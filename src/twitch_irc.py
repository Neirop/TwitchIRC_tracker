import asyncio
import collections
import logging
import random
import re
import string
import textwrap
import time
import traceback
import typing
import uuid
from datetime import datetime
from distutils.util import strtobool
from enum import Enum, auto

import pydle
from pydle.features.ircv3.tags import TaggedMessage

import response_handler
from database_model import Streamer
from response_handler import ResponseStruct, RoomstateResponse, MessageResponse, \
    BanResponse, ClearmsgResponse, CheerResponse, SubResponse

pydle.features.ircv3.tags.TAGGED_MESSAGE_LENGTH_LIMIT = 4096
pydle.features.rfc1459.protocol.MESSAGE_LENGTH_LIMIT = 1024

LOGGER = logging.getLogger(__name__)

POLL_TIME_STREAMERS = 15

BUFFER_SIZE = pow(2, 14)

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 8
WRITE_TIMEOUT = 5
WAITING_RESPONSE_TIMEOUT = 3

ENCODING_FORMAT = "utf-8"
IRC_END_MESSAGE = "\r\n"

TWITCH_CAPABILITIES = {"twitch.tv/membership",
                       "twitch.tv/commands",
                       "twitch.tv/tags"
                       }

IGNORED_COMMAND = {"001", "002", "003", "004", "353", "366", "372", "375",
                   "USERSTATE", "GLOBALUSERSTATE", "HOSTTARGET"
                   }

IGNORED_NOTICE = {"raid", "unraid", "bitsbadgetier", "ritual", "charity",
                  "rewardgift", "primecommunitygiftreceived",
                  "submysterygift", "communitypayforward", "standardpayforward",
                  "giftpaidupgrade", "anongiftpaidupgrade", "primepaidupgrade"
                  }

# submysterygift: Gift random subs
#  user is gifting 5 Tier 1 Subs to CHANNEL's community! They've gifted a total of 117 in the channel

# communitypayforward: user received a gifted sub, and gift random subs (submysterygift)
#  user1 is paying forward the Gift they got from user2 to the community!

# standardpayforward: user received a gifted sub, and gift a sub to someone
#  user1 is paying forward the Gift they got from user2 to user3!

# paidupgrade usernotice seems to not be used anymore
# giftpaidupgrade, anongiftpaidupgrade: user upgrade from a (anon) gifted sub
#  user1 is continuing the Gift Sub they got from user2

# primepaidupgrade: user upgrade Twitch Prime sub to paid sub
#  user converted from a Twitch Prime sub to a Tier 1 sub!


LIMIT_JOIN_PER_SOCKET = 80
LIMIT_TIME_REQ = 0.30

SUB_TYPE_INT = {"Prime": 0, "1000": 1, "2000": 2, "3000": 3}


class PendingResultEnum(Enum):
    SUCCESS = auto()
    WAITING = auto()
    TIMEOUT = auto()
    ERROR = auto()


class PendingCommandEnum(Enum):
    LOGIN = auto()
    CAP = auto()
    JOIN = auto()
    PART = auto()
    ROOMSTATE = auto()


class PendingCommandResult:
    class PendingFuture(typing.NamedTuple):
        future: asyncio.Future
        expected_result: typing.Any

    def __init__(self, event_loop: asyncio.AbstractEventLoop):
        self.event_loop = event_loop

        self._pending_future = {c.value: dict() for c in PendingCommandEnum}

    async def _wait_future(self, cmd: PendingCommandEnum, cmd_arg: typing.Union[str, None] = None,
                           expected_result: typing.Any = None) -> PendingResultEnum:
        cmd_value = cmd.value
        if cmd_arg in self._pending_future[cmd_value]:
            return PendingResultEnum.WAITING
        else:
            self._pending_future[cmd_value][cmd_arg] = self.PendingFuture(self.event_loop.create_future(),
                                                                          expected_result)

        try:
            await asyncio.wait_for(asyncio.shield(self._pending_future[cmd_value][cmd_arg].future),
                                   timeout=WAITING_RESPONSE_TIMEOUT)
        except asyncio.TimeoutError:
            pass

        # Pending future may be deleted from dict if it was cleared
        pending_future = self._pending_future[cmd_value].pop(cmd_arg, None)
        if pending_future is None:
            return PendingResultEnum.ERROR
        elif pending_future.future.done():
            return pending_future.future.result()
        else:
            return PendingResultEnum.TIMEOUT

    def create_pending_future_task(self, cmd: PendingCommandEnum, cmd_arg: typing.Union[str, None] = None,
                                   expected_result: typing.Any = None) -> asyncio.Task:
        return asyncio.create_task(self._wait_future(cmd, cmd_arg, expected_result))

    def get_pending_future(self, cmd: PendingCommandEnum,
                           cmd_arg: typing.Union[str, None] = None) -> typing.Union[PendingFuture, None]:
        cmd_value = cmd.value
        if cmd_arg in self._pending_future[cmd_value] and not self._pending_future[cmd_value][cmd_arg].future.done():
            return self._pending_future[cmd_value][cmd_arg]
        else:
            return None

    def clear(self):
        for d in self._pending_future.values():
            d.clear()


# TODO Debug option to print IRC on file
class IRCTwitchClient:
    _last_request_timestamp = 0

    response_types = {MessageResponse: "message",
                      BanResponse: "ban",
                      ClearmsgResponse: "clearmsg",
                      RoomstateResponse: "roomstate",
                      CheerResponse: "cheer",
                      SubResponse: "sub"}

    def __init__(self, client_num: int, event_loop: asyncio.AbstractEventLoop, response_stack: ResponseStruct):

        # Nickname for read only chat
        self.nickname = "justinfan" + "".join(random.choices(string.digits, k=10))
        self.client_num = client_num
        self.peer_address = ""

        self.event_loop = event_loop
        self.response_stack = response_stack

        self.reader = None
        self.writer = None
        self._drain_lock = asyncio.Lock()

        self.connected = False
        self._disconnect_event = asyncio.Event()

        self._mes_handler_task = None

        self.pending_cmd_result = PendingCommandResult(self.event_loop)

        self.channel_joined = dict()

        self.connection_datetime = datetime.min
        self.disconnection_counter = 0
        self.stats = collections.Counter()

    @classmethod
    async def _wait_limit_send_command(cls):
        current_time = time.time()
        if cls._last_request_timestamp < current_time:

            interval_time = current_time - cls._last_request_timestamp
            if interval_time < LIMIT_TIME_REQ:
                time_to_wait = LIMIT_TIME_REQ - interval_time
                cls._last_request_timestamp = current_time + time_to_wait
                await asyncio.sleep(time_to_wait)
            else:
                cls._last_request_timestamp = current_time

        else:
            cls._last_request_timestamp += LIMIT_TIME_REQ
            await asyncio.sleep(cls._last_request_timestamp - current_time)

    async def _send_message(self, message: str, wait_limit: bool = False) -> bool:
        if wait_limit:
            await self._wait_limit_send_command()

        message_encoded = bytes(message + IRC_END_MESSAGE, encoding=ENCODING_FORMAT)

        if self.connected and not self._disconnect_event.is_set():
            try:
                self.writer.write(message_encoded)

                # drain() cannot be called concurrently by multiple coroutines:
                # http://bugs.python.org/issue29930
                async with self._drain_lock:
                    await asyncio.wait_for(self.writer.drain(), timeout=WRITE_TIMEOUT)

                return True
            except OSError as err:
                LOGGER.error("Client %d - %s: Send error: %s", self.client_num, self.peer_address, err)
                self._disconnect_event.set()
            except asyncio.TimeoutError:
                LOGGER.error("Client %d - %s: Send error: Timeout", self.client_num, self.peer_address)
                self._disconnect_event.set()

        return False

    async def _recv_messages(self) -> bytearray:
        # bytearray is better for concatenation because is mutable
        data = bytearray()
        buffer_full_counter = 0
        ping_sent = False
        error = False

        while True:
            try:
                part = await asyncio.wait_for(self.reader.read(BUFFER_SIZE), timeout=READ_TIMEOUT)
                if part == b"":
                    if not await self.ping():
                        error = True
                        break

            except OSError as err:
                LOGGER.error("Client %d - %s: Recv error: %s", self.client_num, self.peer_address, err)
                error = True
                break

            except asyncio.TimeoutError:
                if not ping_sent:
                    if not await self.ping():
                        error = True
                        break
                    ping_sent = True
                    continue
                else:
                    LOGGER.error("Client %d - %s: No response for ping", self.client_num, self.peer_address)
                    error = True
                    break

            data += part

            if len(part) == BUFFER_SIZE:
                buffer_full_counter += 1
                if buffer_full_counter % 10 == 0:
                    LOGGER.warning(
                        "Client %d - %s: Recv: Buffer size reached %d times in a row", self.client_num,
                        self.peer_address, buffer_full_counter)

            if data.endswith(IRC_END_MESSAGE.encode(ENCODING_FORMAT)):
                break

        if error:
            self._disconnect_event.set()
            # Keep only complete IRC message
            data, _, _ = data.rpartition(IRC_END_MESSAGE.encode(ENCODING_FORMAT))

        return data

    def _parse_source_user(self, source: str) -> typing.Union[str, None]:
        if "!" in source:
            return source.split("!", 1)[0]
        else:
            return None

    def _check_streamer_id(self, channel: str, streamer_id: int) -> bool:
        # Check if in channel joined
        if channel in self.channel_joined:
            if self.channel_joined[channel] == streamer_id:
                return True
            else:
                LOGGER.warning("Client %d - %s: Message received from a channel [%s] that has changed its login name",
                               self.client_num, self.peer_address, channel)
                asyncio.create_task(self.part(channel))
                return False
        else:
            # Other command (PRIVMSG) could be received before the channel is considered as joined
            pending_future = self.pending_cmd_result.get_pending_future(PendingCommandEnum.JOIN, channel)
            if pending_future is None:
                LOGGER.warning("Client %d - %s: Message received from a channel [%s] not joined", self.client_num,
                               self.peer_address, channel)
                asyncio.create_task(self.part(channel))

            return False

    def _append_response_stack(self, response, channel: str):
        res_type = IRCTwitchClient.response_types[type(response)]

        # Add to response stack
        getattr(self.response_stack, res_type)[channel].append(response)
        # Increment statistics
        self.stats[res_type] += 1

    async def connect(self):
        while not self.connected:
            try:
                self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection('irc.chat.twitch.tv', 6667),
                                                                  timeout=CONNECT_TIMEOUT)
                self.connected = True
            except asyncio.TimeoutError:
                continue
            except OSError as err:
                LOGGER.error("Client %d: Connection error to Twitch IRC: %s", self.client_num, err)
                await asyncio.sleep(5)
                continue

            self.peer_address = self.writer.transport.get_extra_info("peername")

            # Launch message handler task
            self._mes_handler_task = asyncio.create_task(self.handle_messages())

            # Request capabilities and login
            for coro in (self.request_twitch_capabilities, self.login):
                if not await coro():
                    await self.disconnect()
                    await asyncio.sleep(5)
                    break

        self.connection_datetime = datetime.now()
        LOGGER.info("Client %d - %s: Successful connection to Twitch IRC", self.client_num, self.peer_address)

    async def disconnect(self):
        self.connected = False

        if self._mes_handler_task is not None:
            # Wait end of message handler task
            await self._mes_handler_task

        if self.writer is not None:
            self.writer.close()
            try:
                await asyncio.wait_for(self.writer.wait_closed(), timeout=WRITE_TIMEOUT)
            except (OSError, asyncio.TimeoutError):
                pass

        self.reader = None
        self.writer = None

        self.channel_joined.clear()
        self.pending_cmd_result.clear()
        self.stats.clear()

        self.disconnection_counter += 1
        self._disconnect_event.clear()

    async def login(self) -> bool:
        pending_task = self.pending_cmd_result.create_pending_future_task(PendingCommandEnum.LOGIN)

        await self._send_message("NICK {0}".format(self.nickname))
        command_result = await pending_task

        if command_result == PendingResultEnum.SUCCESS:
            return True
        else:
            LOGGER.error("Client %d - %s: Unsuccessful login", self.client_num, self.peer_address)
            return False

    async def request_twitch_capabilities(self) -> bool:
        pending_task_dict = dict()

        for cap in TWITCH_CAPABILITIES:
            pending_task_dict[cap] = self.pending_cmd_result.create_pending_future_task(PendingCommandEnum.CAP, cap)

        await self._send_message("CAP REQ :{0}".format(" ".join(TWITCH_CAPABILITIES)))

        for cap, task in pending_task_dict.items():
            cap_result = await task

            if cap_result != PendingResultEnum.SUCCESS:
                LOGGER.error("Client %d - %s: Capability %s not acknowledged", self.client_num, self.peer_address, cap)
                return False

        await self._send_message("CAP END")
        return True

    async def join(self, channel: str, streamer_id: int) -> bool:
        join_pending_task = self.pending_cmd_result.create_pending_future_task(PendingCommandEnum.JOIN, channel)
        roomstate_pending_task = self.pending_cmd_result.create_pending_future_task(PendingCommandEnum.ROOMSTATE,
                                                                                    channel,
                                                                                    streamer_id)

        res = await self._send_message("JOIN #" + channel, True)
        join_result, roomstate_result = await asyncio.gather(join_pending_task, roomstate_pending_task)

        if join_result == PendingResultEnum.SUCCESS and roomstate_result == PendingResultEnum.SUCCESS:
            return True
        else:
            LOGGER.warning("Client %d - %s: Join channel [%s] failed (%s)",
                           self.client_num, self.peer_address, channel,
                           "Send error" if not res
                           else "Join result={}, Roomstate result={}".format(join_result.name,
                                                                             getattr(roomstate_result, "name", None)))
            return False

    async def part(self, channel: str) -> bool:
        pending_task = self.pending_cmd_result.create_pending_future_task(PendingCommandEnum.PART, channel)

        res = await self._send_message("PART #" + channel, False)
        command_result = await pending_task

        if command_result == PendingResultEnum.SUCCESS:
            return True
        else:
            LOGGER.warning("Client %d - %s: Part channel [%s] failed (%s)",
                           self.client_num, self.peer_address, channel,
                           "Send error" if not res else "Part result={}".format(command_result.name))
            return False

    async def ping(self, ping_id: str = "0") -> bool:
        return await self._send_message("PING " + ping_id)

    def is_joined(self, channel: str) -> bool:
        return channel in self.channel_joined

    def nb_channel_joined(self) -> int:
        return len(self.channel_joined)

    def on_ping(self, message: TaggedMessage):
        asyncio.create_task(self._send_message("PONG :tmi.twitch.tv"))

    def on_pong(self, message: TaggedMessage):
        pass

    def on_join(self, message: TaggedMessage):
        channel = message.params[0].replace("#", "")
        user = self._parse_source_user(message.source)

        # Successful JOIN command
        if user == self.nickname:
            pending_future = self.pending_cmd_result.get_pending_future(PendingCommandEnum.JOIN, channel)
            if pending_future is not None:
                pending_future.future.set_result(PendingResultEnum.SUCCESS)

    def on_part(self, message: TaggedMessage):
        channel = message.params[0].replace("#", "")
        user = self._parse_source_user(message.source)

        # Successful PART command
        if user == self.nickname:
            pending_future = self.pending_cmd_result.get_pending_future(PendingCommandEnum.PART, channel)
            if pending_future is not None:
                pending_future.future.set_result(PendingResultEnum.SUCCESS)
            else:
                LOGGER.warning("Client %d - %s: No pending command for PART [%s]", self.client_num, self.peer_address,
                               channel)
            self.channel_joined.pop(channel, None)

    def on_376(self, message: TaggedMessage):
        # Successful login
        pending_future = self.pending_cmd_result.get_pending_future(PendingCommandEnum.LOGIN)
        if pending_future is not None:
            pending_future.future.set_result(PendingResultEnum.SUCCESS)

    def on_cap(self, message: TaggedMessage):
        cap_command = message.params[1]
        cap_param = message.params[2]

        # Cap acknowledgement
        if cap_command == "ACK":
            for cap in cap_param.split(" "):
                pending_future = self.pending_cmd_result.get_pending_future(PendingCommandEnum.CAP, cap)
                if pending_future is not None:
                    pending_future.future.set_result(PendingResultEnum.SUCCESS)

    def on_reconnect(self, message: TaggedMessage):
        LOGGER.warning("Client %d - %s: Received RECONNECT request: %s", self.client_num, self.peer_address,
                       message._raw)
        self._disconnect_event.set()

    def on_roomstate(self, message: TaggedMessage):
        channel = message.params[0].replace("#", "")
        streamer_id = int(message.tags["room-id"])

        if channel not in self.channel_joined:
            pending_future = self.pending_cmd_result.get_pending_future(PendingCommandEnum.ROOMSTATE, channel)
            if pending_future is not None:
                if streamer_id == pending_future.expected_result:
                    self.channel_joined[channel] = streamer_id
                    pending_future.future.set_result(PendingResultEnum.SUCCESS)
                else:
                    # Channel with a different streamer id is joined,
                    # it happens when a streamer changes its login name and a user takes its previous login name
                    asyncio.create_task(self.part(channel))
                    pending_future.future.set_result(PendingResultEnum.ERROR)
                    return
            else:
                # There is no pending join for the channel (join result after pending timeout)
                # Streamer id verification can't be processed, then part of channel
                LOGGER.warning("Client %d - %s: No pending command for JOIN [%s]", self.client_num, self.peer_address,
                               channel)
                asyncio.create_task(self.part(channel))
                return

        if not self._check_streamer_id(channel, streamer_id):
            return

        roomstate_resp = RoomstateResponse(streamer_id=streamer_id,
                                           change_datetime=datetime.utcnow(),
                                           emote_only=strtobool(message.tags["emote-only"])
                                           if "emote-only" in message.tags else None,
                                           followers_only=int(message.tags["followers-only"])
                                           if "followers-only" in message.tags else None,
                                           r9k=strtobool(message.tags["r9k"])
                                           if "r9k" in message.tags else None,
                                           slow=int(message.tags["slow"])
                                           if "slow" in message.tags else None,
                                           subs_only=strtobool(message.tags["subs-only"])
                                           if "subs-only" in message.tags else None
                                           )
        self._append_response_stack(roomstate_resp, channel)

    def on_privmsg(self, message: TaggedMessage):
        channel = message.params[0].replace("#", "")
        streamer_id = int(message.tags["room-id"])
        login_name = message.source.split("!", 1)[0]

        if not self._check_streamer_id(channel, streamer_id):
            return

        message_resp = MessageResponse(message_id=message.tags["id"],
                                       streamer_id=streamer_id,
                                       user_id=int(message.tags["user-id"]),
                                       login_name=login_name,
                                       display_name=message.tags["display-name"]
                                       if message.tags["display-name"].lower() != login_name else None,
                                       message_datetime=datetime.utcfromtimestamp(
                                           float(message.tags["tmi-sent-ts"]) / 1000),
                                       message_content=message.params[1],
                                       subscribed=int(
                                           re.search(r'(?:subscriber|founder)/(\d+)', message.tags["badge-info"]).group(
                                               1))
                                       if any(s in message.tags["badge-info"] for s in {"subscriber", "founder"})
                                       else 0
                                       )
        self._append_response_stack(message_resp, channel)

        if "bits" in message.tags:
            cheer_resp = CheerResponse(streamer_id=streamer_id,
                                       user_id=int(message.tags["user-id"]),
                                       cheer_datetime=datetime.utcfromtimestamp(
                                           float(message.tags["tmi-sent-ts"]) / 1000),
                                       nb_bits=int(message.tags["bits"]))
            self._append_response_stack(cheer_resp, channel)

    def on_clearchat(self, message: TaggedMessage):
        channel = message.params[0].replace("#", "")
        streamer_id = int(message.tags["room-id"])

        if not self._check_streamer_id(channel, streamer_id):
            return

        # Don't treat /clear command
        if len(message.params) == 2:
            ban_resp = BanResponse(ban_id=str(uuid.uuid4()),
                                   streamer_id=streamer_id,
                                   user_id=int(message.tags["target-user-id"]),
                                   ban_datetime=datetime.utcfromtimestamp(
                                       float(message.tags["tmi-sent-ts"]) / 1000),
                                   ban_duration=int(message.tags.get("ban-duration", "0"))
                                   )
            self._append_response_stack(ban_resp, channel)

    def on_clearmsg(self, message: TaggedMessage):
        channel = message.params[0].replace("#", "")

        clearmsg_resp = ClearmsgResponse(target_msg_id=message.tags["target-msg-id"],
                                         clear_datetime=datetime.utcfromtimestamp(
                                             float(message.tags["tmi-sent-ts"]) / 1000)
                                         )
        self._append_response_stack(clearmsg_resp, channel)

    def on_usernotice(self, message: TaggedMessage):
        channel = message.params[0].replace("#", "")
        streamer_id = int(message.tags["room-id"])
        msg_id = message.tags["msg-id"]

        if not self._check_streamer_id(channel, streamer_id):
            return

        if msg_id in {"sub", "resub"}:
            month_tenure = int(message.tags["msg-param-cumulative-months"])

            # Resub from multi month subgift
            if all(t in message.tags for t in ["msg-param-gift-month-being-redeemed", "msg-param-gift-months"]) and \
                    int(message.tags["msg-param-gift-month-being-redeemed"]) < \
                    int(message.tags["msg-param-gift-months"]):
                future_month_tenure = month_tenure - int(message.tags["msg-param-gift-month-being-redeemed"]) + int(
                    message.tags["msg-param-gift-months"])
            else:
                # TODO New tag msg-param-multimonth-tenure and msg-param-multimonth-duration are added for multi month
                #  but not documented and can't understand how they work
                future_month_tenure = None

            sub_resp = SubResponse(streamer_id=streamer_id,
                                   user_id=int(message.tags["user-id"]),
                                   sub_datetime=datetime.utcfromtimestamp(
                                       float(message.tags["tmi-sent-ts"]) / 1000),
                                   month_tenure=month_tenure,
                                   future_month_tenure=future_month_tenure,
                                   sub_type=SUB_TYPE_INT[message.tags["msg-param-sub-plan"]],
                                   gifted=strtobool(message.tags["msg-param-was-gifted"]))
            self._append_response_stack(sub_resp, channel)

            # Add sub message
            if len(message.params) == 2:
                message_resp = MessageResponse(message_id=message.tags["id"],
                                               streamer_id=streamer_id,
                                               user_id=int(message.tags["user-id"]),
                                               login_name=message.tags["login"],
                                               display_name=message.tags["display-name"]
                                               if message.tags["display-name"].lower() != message.tags["login"]
                                               else None,
                                               message_datetime=datetime.utcfromtimestamp(
                                                   float(message.tags["tmi-sent-ts"]) / 1000),
                                               message_content=message.params[1],
                                               subscribed=month_tenure
                                               )
                self._append_response_stack(message_resp, channel)

        elif msg_id in {"subgift", "anonsubgift"}:
            sub_resp = SubResponse(streamer_id=streamer_id,
                                   user_id=int(message.tags["msg-param-recipient-id"]),
                                   sub_datetime=datetime.utcfromtimestamp(
                                       float(message.tags["tmi-sent-ts"]) / 1000),
                                   month_tenure=int(message.tags["msg-param-months"]),
                                   sub_type=SUB_TYPE_INT[message.tags["msg-param-sub-plan"]],
                                   gifted=True,
                                   future_month_tenure=int(message.tags["msg-param-months"]) +
                                                       (int(message.tags["msg-param-gift-months"]) - 1)
                                   if int(message.tags["msg-param-gift-months"]) != 1
                                   else None
                                   )
            self._append_response_stack(sub_resp, channel)

        elif msg_id == "extendsub":
            # extendsub: user extended their Tier 1 subscription through July
            #  https://discuss.dev.twitch.tv/t/ios-sub-tokens-launch/22794
            current_month = datetime.utcfromtimestamp(float(message.tags["tmi-sent-ts"]) / 1000).month
            end_month = int(message.tags["msg-param-sub-benefit-end-month"])
            future_month_tenure = None

            # The next month is only if the sub expires in the current month
            if current_month < end_month:
                future_month_tenure = int(message.tags["msg-param-cumulative-months"]) + (end_month - current_month)
            elif current_month > end_month:
                future_month_tenure = int(message.tags["msg-param-cumulative-months"]) + (
                        12 - current_month + end_month)
            elif current_month == end_month:
                future_month_tenure = int(message.tags["msg-param-cumulative-months"]) + 12

            sub_resp = SubResponse(streamer_id=streamer_id,
                                   user_id=int(message.tags["user-id"]),
                                   sub_datetime=datetime.utcfromtimestamp(
                                       float(message.tags["tmi-sent-ts"]) / 1000),
                                   month_tenure=int(message.tags["msg-param-cumulative-months"]),
                                   sub_type=SUB_TYPE_INT[message.tags["msg-param-sub-plan"]],
                                   gifted=False,
                                   future_month_tenure=future_month_tenure)
            self._append_response_stack(sub_resp, channel)

        elif msg_id not in IGNORED_NOTICE:
            LOGGER.warning("Unknown msg id notice received: %s", msg_id)

    def on_notice(self, message: TaggedMessage):
        # Server global notice
        if message.params[0] == "*":
            LOGGER.warning("Client %d - %s: Server notice received: %s", self.client_num, self.peer_address,
                           message.params[1])
            return

        channel = message.params[0].replace("#", "")
        msg_id = message.tags.get("msg_id", None)
        if msg_id is not None:

            # Can't join channel
            if any(tag == msg_id for tag in {"msg_channel_suspended", "tos_ban"}):
                pending_future = self.pending_cmd_result.get_pending_future(PendingCommandEnum.JOIN, channel)
                if pending_future is not None:
                    pending_future.future.set_result(PendingResultEnum.ERROR)
                else:
                    # In this case, channel was already joined when the ban has occurred
                    pass

    async def handle_messages(self):
        while self.connected and not self._disconnect_event.is_set():

            messages = await self._recv_messages()

            for message in messages.split(IRC_END_MESSAGE.encode(ENCODING_FORMAT)):

                # Empty message
                if not message:
                    continue

                # Parsing message
                try:
                    tagged_message = TaggedMessage.parse(message, encoding=ENCODING_FORMAT)
                except Exception as err:
                    LOGGER.error("Error (%s) when parsing message: %s", err, message)
                    continue

                if not tagged_message._valid:
                    LOGGER.error("Invalid parsed message: %s", message)
                    continue

                # Call command method
                if isinstance(tagged_message.command, int):
                    command = str(tagged_message.command).zfill(3)
                else:
                    command = tagged_message.command

                if command not in IGNORED_COMMAND:
                    method = getattr(self, "on_" + command.lower(), None)
                    if method is not None:
                        try:
                            method(tagged_message)
                        except Exception as e:
                            traceback.print_exc()
                            LOGGER.error("Exception %s occurred when handling message: %s", e, message)
                    else:
                        LOGGER.error("No method for IRC command: %s", command)

    async def run(self):
        # connect() must be called before run()
        while self.connected:
            # Wait on disconnection event
            await self._disconnect_event.wait()
            await self.disconnect()

            # Reconnection
            await self.connect()


class IRCHandler:
    def __init__(self):
        # Must init Lock in the event loop
        IRCHandler._limit_request_lock = asyncio.Lock()

        self.event_loop = asyncio.get_event_loop()

        self.channel_irc_queue = asyncio.Queue()

        self.clients_pool = list()
        self.client_counter = 0

        self.channel_joined_map = collections.ChainMap()

        self.response_stack = ResponseStruct()

        self.response_handler = response_handler.ResponseHandler(self.response_stack, self.event_loop)

    async def start_new_client(self) -> IRCTwitchClient:
        irc_client = IRCTwitchClient(self.client_counter, self.event_loop, self.response_stack)
        self.client_counter += 1
        self.clients_pool.append(irc_client)
        self.channel_joined_map.maps.append(irc_client.channel_joined)

        await irc_client.connect()
        asyncio.create_task(irc_client.run())

        return irc_client

    async def handle_clients(self):
        while True:
            streamer_name: str
            streamer_id: int
            (streamer_name, streamer_id) = await self.channel_irc_queue.get()

            # Join the channel
            if streamer_id:
                # Check if channel is already joined by one client
                if streamer_name not in self.channel_joined_map:
                    client = next((client for client in self.clients_pool
                                   if client.nb_channel_joined() < LIMIT_JOIN_PER_SOCKET), None)
                    if client is None:
                        client = await self.start_new_client()

                    await client.join(streamer_name, streamer_id)
            # Part the channel
            else:
                for client in self.clients_pool:
                    if client.is_joined(streamer_name):
                        await client.part(streamer_name)
                        break

    async def poll_streamers(self):
        while True:
            if self.channel_irc_queue.qsize():
                # Wait for current channel join/part processing
                await asyncio.sleep(POLL_TIME_STREAMERS)
                continue

            streamers = await self.event_loop.run_in_executor(None, Streamer.get_all_record_of_table)

            for streamer in streamers:
                if not streamer.banned:
                    # Join channel
                    await self.channel_irc_queue.put((streamer.login_name, streamer.streamer_id))
                else:
                    # Part channel
                    await self.channel_irc_queue.put((streamer.login_name, 0))

            # Case for login change: Part for old login
            streamers_login = {streamer.login_name for streamer in streamers}
            for channel in self.channel_joined_map:
                if channel not in streamers_login:
                    await self.channel_irc_queue.put((channel, 0))

            await asyncio.sleep(POLL_TIME_STREAMERS)

    async def get_info_stats(self, client_num: int = None) -> str:
        def client_resume(c: IRCTwitchClient) -> str:
            return "Client {0}: connected to {1} for {2} ({3} disconnections), {4} channels joined\n" \
                   "  message: {stats[message]:,}\n" \
                   "  ban: {stats[ban]:,}\n" \
                   "  clearmsg: {stats[clearmsg]:,}\n" \
                   "  roomstate: {stats[roomstate]:,}\n" \
                   "  cheer: {stats[cheer]:,}\n" \
                   "  sub: {stats[sub]:,}" \
                .format(c.client_num,
                        c.peer_address,
                        str(datetime.now() - c.connection_datetime).split(".")[0],
                        c.disconnection_counter,
                        c.nb_channel_joined(),
                        stats=c.stats)

        info = ""
        if client_num is None:
            info += "{0} IRC client, {1} channels joined\n".format(len(self.clients_pool),
                                                                   len(self.channel_joined_map))
            info += "{:=<50}\n".format("")
            for client in self.clients_pool:
                if client.connected:
                    info += client_resume(client)
                else:
                    info += "Client {0}: not connected ({1} disconnections)".format(client.client_num,
                                                                                    client.disconnection_counter)
                info += "\n{:-<80}\n".format("")
        else:
            client = next((client for client in self.clients_pool
                           if client.client_num == client_num), None)
            if client is None:
                info += "Client {0} doesn't exist".format(client_num)
            else:
                info += client_resume(client)
                info += "\n{:-<80}\n".format("")
                info += "Channel joined:\n{}".format(textwrap.fill(", ".join(client.channel_joined.keys()),
                                                                   width=80,
                                                                   initial_indent="  ",
                                                                   subsequent_indent="  "))

        return info

    async def reset_stats(self):
        for client in self.clients_pool:
            client.stats.clear()

    def start_loop(self):
        # Start response handler thread
        self.event_loop.run_in_executor(None, self.response_handler.start_handler)

        self.event_loop.create_task(self.poll_streamers())

        self.event_loop.create_task(self.handle_clients())

        # self.event_loop.set_debug(True)
        self.event_loop.run_forever()
