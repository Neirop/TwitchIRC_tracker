import asyncio
import collections
import copy
import functools
import logging
import statistics
import threading
import time
import typing
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from database_model import RoomState, UserBanned, UserMessage, Cheer, Sub, database

LOGGER = logging.getLogger(__name__)

# Time between iteration for treating and storing data in database
HANDLE_ITERATION_TIME = 5
# Time between last message and ban to define if it was a ban by a bot
BOT_BAN_DETECTION_TIME = 1
# Minimal ban time to record the ban if there is no message
IGNORE_BAN_TIME = 30
# Last number of message concerned by CLEARCHAT response
# (Currently, there are 150 message displayed in Twitch chat)
NB_MESSAGES_CLEARCHAT = 150


@dataclass
class MessageResponse:
    message_id: str
    streamer_id: int
    user_id: int
    login_name: str
    display_name: typing.Union[str, None]
    message_datetime: datetime
    message_content: str
    subscribed: int
    deleted: bool = False


@dataclass
class BanResponse:
    ban_id: str
    streamer_id: int
    user_id: int
    ban_datetime: datetime
    ban_duration: int
    ban_by_bot: typing.Union[bool, None] = None
    cleared_messages: list = field(default_factory=list)


@dataclass
class ClearmsgResponse:
    target_msg_id: str
    clear_datetime: datetime


@dataclass
class RoomstateResponse:
    streamer_id: int
    change_datetime: datetime
    emote_only: typing.Union[bool, None]
    followers_only: typing.Union[int, None]
    r9k: typing.Union[bool, None]
    slow: typing.Union[int, None]
    subs_only: typing.Union[bool, None]


@dataclass
class CheerResponse:
    streamer_id: int
    user_id: int
    cheer_datetime: datetime
    nb_bits: int


@dataclass
class SubResponse:
    streamer_id: int
    user_id: int
    sub_datetime: datetime
    month_tenure: int
    sub_type: int
    gifted: bool
    future_month_tenure: typing.Union[int, None] = None


class ResponseStruct(typing.NamedTuple):
    # Storage of response in a list per channel
    message: dict = collections.defaultdict(list)
    ban: dict = collections.defaultdict(list)
    clearmsg: dict = collections.defaultdict(list)
    roomstate: dict = collections.defaultdict(list)
    cheer: dict = collections.defaultdict(list)
    sub: dict = collections.defaultdict(list)


class HandlerStatistics:
    def __init__(self):
        self._stats_deque = collections.deque(maxlen=250)
        self.iteration_stats = dict()

    def exec_time(save_iteration_stats: bool = False):
        def wrap(func):
            @functools.wraps(func)
            def _wrap(self, *args, **kwargs):
                start_time = time.time()
                result = func(self, *args, **kwargs)
                self.iteration_stats[func.__name__ + "_exectime_ms"] = round((time.time() - start_time) * 1000)
                if save_iteration_stats:
                    self._stats_deque.append(self.iteration_stats.copy())
                return result

            return _wrap

        return wrap

    exec_time = staticmethod(exec_time)

    def generate_stats(self, verbose: bool = False) -> str:
        stats = ""
        if len(self._stats_deque) > 1:
            fields = self._stats_deque[0].keys()
            for f in fields:
                if verbose or not f.startswith("_"):
                    stat = [s[f] for s in self._stats_deque]
                    stats += "  {}: min={} / avg={:.2f} / max={} / mdev={:.2f}\n".format(
                        f[1:] if f.startswith("_") else f,
                        min(stat),
                        statistics.mean(stat),
                        max(stat),
                        statistics.stdev(stat))
        return stats


class ResponseHandler(HandlerStatistics):
    def __init__(self, response_stack: ResponseStruct, irc_handler_event_loop: asyncio.AbstractEventLoop):
        super().__init__()
        self.response_stack = response_stack
        self.irc_handler_event_loop = irc_handler_event_loop

        self.response_data = None

        self.roomstate_hander = RoomstateHandler()
        self.ban_handler = BanHandler()
        self.clearmsg_handler = ClearmsgHandler()
        self.message_handler = MessageHandler()
        self.cheer_handler = CheerHandler()
        self.sub_handler = SubHandler()

    def start_handler(self):
        while True:
            start_time = time.time()

            future = asyncio.run_coroutine_threadsafe(self.fetch_response_data(), self.irc_handler_event_loop)
            future.result()

            self.handle_response()

            elapsed_time = time.time() - start_time

            if elapsed_time < HANDLE_ITERATION_TIME:
                time.sleep(HANDLE_ITERATION_TIME - elapsed_time)
            elif elapsed_time > HANDLE_ITERATION_TIME * 2:
                LOGGER.warning("Handler execution time took %.2f seconds", elapsed_time)

    @HandlerStatistics.exec_time(save_iteration_stats=True)
    def handle_response(self):
        if len(self.response_data.roomstate) > 0:
            self.roomstate_hander.handle_roomstate(self.response_data.roomstate)
        # Bans and clearmsgs must be handled before messages
        if len(self.response_data.ban) > 0:
            self.ban_handler.handle_ban(self.response_data.ban, self.response_data.message)
        if len(self.response_data.clearmsg) > 0:
            self.clearmsg_handler.handle_clearmsg(self.response_data.clearmsg, self.response_data.message)
        if len(self.response_data.message) > 0:
            self.message_handler.handle_message(self.response_data.message)
        if len(self.response_data.cheer) > 0:
            self.cheer_handler.handle_cheer(self.response_data.cheer)
        if len(self.response_data.sub) > 0:
            self.sub_handler.handle_sub(self.response_data.sub)

    async def fetch_response_data(self):
        self.response_data = copy.deepcopy(self.response_stack)

        for d in self.response_stack:
            d.clear()

    def get_stats(self, verbose: bool = False) -> str:
        def underlined(s: str) -> str:
            return "\n{1}\n{0}\n".format("=" * len(s), s)

        stats = ""
        stats += underlined("Global")
        stats += self.generate_stats(verbose)
        stats += underlined("Roomstate")
        stats += self.roomstate_hander.generate_stats(verbose)
        stats += underlined("Ban")
        stats += self.ban_handler.generate_stats(verbose)
        stats += underlined("Clearmsg")
        stats += self.clearmsg_handler.generate_stats(verbose)
        stats += underlined("Message")
        stats += self.message_handler.generate_stats(verbose)
        stats += underlined("Cheer")
        stats += self.cheer_handler.generate_stats(verbose)
        stats += underlined("Sub")
        stats += self.sub_handler.generate_stats(verbose)

        return stats


class BanMessageMixin:
    @classmethod
    def _is_sent_during_ban(cls, message_datetime: datetime, ban_datetime: datetime, ban_duration: int) -> bool:
        # Sometimes, a message can pass in the next second (approximately) of the timeout/ban and before one second
        # of the end of timeout, in these cases, user is not unban
        if (message_datetime - ban_datetime).total_seconds() > 1 and (
                (ban_datetime + timedelta(seconds=ban_duration) - message_datetime).total_seconds() > 1 or
                ban_duration == 0):
            return True
        else:
            return False


class RoomstateHandler(HandlerStatistics):
    @HandlerStatistics.exec_time(save_iteration_stats=True)
    def handle_roomstate(self, roomstate_data: dict):
        roomstate_fields = {"emote_only",
                            "followers_only",
                            "r9k",
                            "slow",
                            "subs_only"}
        roomstate_list = list()

        for channel, channel_roomstate in roomstate_data.items():
            # Store one roomstate (last) per channel to avoid abuse command
            roomstate = max(channel_roomstate, key=lambda x: x.change_datetime)

            # Get the last roomstate of the stream in the database
            last_roomstate = RoomState.get_last_room_state_of_streamer(
                roomstate.streamer_id)

            if last_roomstate is None:
                # Check if is the first roomstate for the channel
                if any(getattr(roomstate, f) is None for f in roomstate_fields):
                    # Check if the first roomstate is present in romstate_data
                    roomstate = next((r for r in channel_roomstate if
                                      all(getattr(r, f) is not None for f in roomstate_fields)), None)
                    if roomstate is None:
                        LOGGER.error("No first roomstate found for [%s]", channel)
                        continue

            # Check if there is a change and merge with the last roomstate
            if last_roomstate is not None:
                changed = False
                for f in roomstate_fields:
                    if getattr(roomstate, f) is None:
                        setattr(roomstate, f, getattr(last_roomstate, f))
                    elif getattr(roomstate, f) != getattr(last_roomstate, f):
                        changed = True

                # If there are no changes, no need to store in database
                if not changed:
                    continue

            roomstate_list.append(roomstate)

        RoomState.insert_many_row([vars(rs) for rs in roomstate_list])

        # Statistics
        self.iteration_stats["nb_roomstate"] = len(roomstate_list)


class BanHandler(HandlerStatistics, BanMessageMixin):
    @HandlerStatistics.exec_time(save_iteration_stats=True)
    def handle_ban(self, ban_data: dict, message_data: dict):
        # Get all messages concerned by ban
        self._fetch_messages_cleared(ban_data, message_data)

        # Detect if the ban was performed by bot (may not be accurate)
        self._check_bot_bans(ban_data)

        # Some channels have bots who ban 2 times (with same ban duration) for 1 message or sometimes 2 mods can ban
        # at the same time. Store new record only if there is no active ban, else update ban
        valid_ban_list = self._verify_bans(ban_data)

        # Remove pointless bans and flag messages not in database as deleted
        self._remove_pointless_bans(valid_ban_list)

        # Store in db and return list of bans
        UserBanned.insert_many_row([{k: v for k, v in vars(ban).items() if k not in ["cleared_messages"]}
                                    for ban in valid_ban_list])

        # Flag messages in database as deleted
        UserMessage.flag_messages_as_deleted([m.message_id for b in valid_ban_list for m in b.cleared_messages])

        # Statistics
        self.iteration_stats["nb_ban"] = sum(len(b) for b in ban_data.values())

    @HandlerStatistics.exec_time()
    def _fetch_messages_cleared(self, ban_data: dict, message_data: dict):
        ban_tmp_dict = dict()
        param_list = list()

        for channel, channel_ban in ban_data.items():
            for ban in channel_ban:
                nb_message_checked = 0

                # Get last messages from message_data
                for message in sorted(message_data.get(channel, []),
                                      key=lambda x: x.message_datetime,
                                      reverse=True):
                    if message.message_datetime < ban.ban_datetime and \
                            nb_message_checked < NB_MESSAGES_CLEARCHAT:
                        nb_message_checked += 1
                        if message.user_id == ban.user_id:
                            ban.cleared_messages.append(message)

                if nb_message_checked < NB_MESSAGES_CLEARCHAT:
                    user_param = UserMessage.UserParamMessage(ban.ban_id,
                                                              ban.streamer_id,
                                                              ban.user_id,
                                                              NB_MESSAGES_CLEARCHAT -
                                                              nb_message_checked)
                    ban_tmp_dict[ban.ban_id] = ban
                    param_list.append(user_param)

        # Get last messages from the database
        if len(param_list) > 0:
            message_db_list = UserMessage.get_last_messages_from_many_users(param_list)

            for message in message_db_list:
                message_response = MessageResponse(message_id=str(message.message_id),
                                                   streamer_id=message.streamer_id,
                                                   user_id=message.user_id,
                                                   login_name=message.login_name,
                                                   display_name=message.display_name,
                                                   message_datetime=message.message_datetime,
                                                   message_content=message.message_content,
                                                   subscribed=message.subscribed,
                                                   deleted=message.deleted
                                                   )
                ban_tmp_dict[message.pid].cleared_messages.append(message_response)

        # Statistics
        self.iteration_stats["_nb_deleted_message"] = sum(len(b.cleared_messages)
                                                          for cb in ban_data.values() for b in cb)

    def _check_bot_bans(self, ban_data: dict):
        for channel, channel_ban in ban_data.items():
            for ban in channel_ban:
                last_message = max(ban.cleared_messages, key=lambda x: x.message_datetime, default=None)

                # If there is no message, ban by bot is undetermined (None)
                if last_message is not None:
                    if (ban.ban_datetime - last_message.message_datetime).total_seconds() < BOT_BAN_DETECTION_TIME:
                        ban.ban_by_bot = True
                    else:
                        ban.ban_by_bot = False

    @HandlerStatistics.exec_time()
    def _verify_bans(self, ban_data: dict) -> typing.List[BanResponse]:
        valid_ban_tmp = collections.defaultdict(list)

        # Check among bans in ban_data
        for channel, channel_ban in ban_data.items():
            # Iterate over a sorted list by ban datetime
            for ban in sorted(channel_ban, key=lambda x: x.ban_datetime):

                prev_ban = next((pb for pb in reversed(valid_ban_tmp.get((ban.streamer_id, ban.user_id), []))),
                                None)

                if prev_ban is not None:
                    # Check if the previous ban still active when the new ban had occurred
                    if (prev_ban.ban_datetime + timedelta(seconds=prev_ban.ban_duration)) > ban.ban_datetime or \
                            prev_ban.ban_duration == 0:
                        if self._compare_and_update_ban(prev_ban, ban) != 1:
                            continue

                valid_ban_tmp[(ban.streamer_id, ban.user_id)].append(ban)

        # Convert dict of list in list of ban
        valid_ban_tmp = [b for bl in valid_ban_tmp.values() for b in bl]

        # Check among bans in database
        param_list = [
            UserBanned.UserParamBan(ban.ban_id,
                                    ban.streamer_id,
                                    ban.user_id,
                                    ban.ban_datetime)
            for ban in valid_ban_tmp]

        if len(param_list) > 0:
            active_ban_db_dict = {b.pid: b for b in UserBanned.get_many_active_ban(param_list)}
        else:
            active_ban_db_dict = dict()

        valid_ban_list = list()
        ban_db_to_update_list = list()

        for ban in valid_ban_tmp:
            prev_ban = active_ban_db_dict.get(ban.ban_id, None)
            if prev_ban is not None:
                res = self._compare_and_update_ban(prev_ban, ban)
                if res == 0:
                    ban_db_to_update_list.append(prev_ban)
                    continue
                elif res == -1:
                    continue

            valid_ban_list.append(ban)

        # Update previous ban in database
        # TODO Use cte update
        for ban in ban_db_to_update_list:
            UserBanned.update_user_banned(ban.ban_id,
                                          ban_datetime=ban.ban_datetime,
                                          ban_duration=ban.ban_duration,
                                          ban_by_bot=ban.ban_by_bot)

        # Statistics
        self.iteration_stats["_nb_valid_ban"] = len(valid_ban_list)

        return valid_ban_list

    def _remove_pointless_bans(self, ban_list: list):
        for ban in ban_list.copy():
            # Remove ban without messages and with ban duration < 30 sec to avoid storing pointless bans
            if not any(m.deleted is False for m in ban.cleared_messages) and \
                    ban.ban_duration != 0 and ban.ban_duration <= IGNORE_BAN_TIME:
                ban_list.remove(ban)
            else:
                # Flag messages not in database as deleted
                for message in ban.cleared_messages:
                    message.deleted = True

    # Return 1 if new ban is valid, 0 if previous ban is updated and -1 if new ban is invalid
    def _compare_and_update_ban(self, prev_ban: typing.Union[BanResponse,UserBanned],
                                new_ban: BanResponse) -> int:
        # Check if there are no messages between new ban and previous ban
        if any(self._is_sent_during_ban(mes.message_datetime,
                                        prev_ban.ban_datetime,
                                        prev_ban.ban_duration) for mes in new_ban.cleared_messages):
            return 1
        else:
            # Update the previous ban, no need to update ban if previous ban and new ban are a permaban
            if not (prev_ban.ban_duration == 0 and new_ban.ban_duration == 0):
                prev_ban.ban_datetime = new_ban.ban_datetime
                # Update ban_by_bot field only if the ban duration is different because the "second"
                # bots are often late
                if new_ban.ban_duration != prev_ban.ban_duration:
                    prev_ban.ban_duration = new_ban.ban_duration
                    prev_ban.ban_by_bot = new_ban.ban_by_bot
                return 0
            else:
                return -1


class ClearmsgHandler(HandlerStatistics):
    @HandlerStatistics.exec_time(save_iteration_stats=True)
    def handle_clearmsg(self, clearmsg_data: dict, message_data: dict):
        message_id_list = list()

        # Convert list of message in dict of message with id as key,
        # no need to add messages from channel who has no clearmsg
        message_tmp_dict = {mes.message_id: mes
                            for channel, c_mes in message_data.items()
                            if channel in clearmsg_data
                            for mes in c_mes}

        for channel, channel_clearmsg in clearmsg_data.items():
            for clearmsg in channel_clearmsg:
                # Search message in tmp dict
                message = message_tmp_dict.get(clearmsg.target_msg_id, None)
                if message is not None:
                    message.deleted = True
                else:
                    # Add message id in a list for database updating
                    message_id_list.append(clearmsg.target_msg_id)

        # Flag message in database
        if len(message_id_list) > 0:
            nb_updated = UserMessage.flag_messages_as_deleted(message_id_list)

        # Statistics
        self.iteration_stats["nb_clearmsg"] = sum(len(clr) for c, clr in clearmsg_data.items())


class MessageHandler(HandlerStatistics, BanMessageMixin):
    @HandlerStatistics.exec_time(save_iteration_stats=True)
    def handle_message(self, message_data: dict):
        message_list = list()

        for channel, channel_message in message_data.items():
            message_list += channel_message

        # Insert messages in database
        UserMessage.insert_many_row([vars(mes) for mes in message_list])

        # Check if there are users who have been unbanned
        self._check_unban(message_list)

        # Statistics
        self.iteration_stats["nb_message"] = len(message_list)

    @HandlerStatistics.exec_time()
    def _check_unban(self, message_list: typing.List[MessageResponse]):
        user_param_list = [
            UserBanned.UserParamBan(mes.message_id, mes.streamer_id, mes.user_id,
                                    mes.message_datetime) for mes in message_list]

        user_banned_dict = {b.pid: b for b in UserBanned.get_many_active_ban(user_param_list)}
        message_dict = {message.message_id: message for message in message_list}

        # For storing ban id that are marked unban to avoid multiple update
        unban_set = set()

        for pid, user_banned in user_banned_dict.items():
            message = message_dict[pid]

            if user_banned.ban_id not in unban_set:

                if self._is_sent_during_ban(message.message_datetime, user_banned.ban_datetime,
                                            user_banned.ban_duration):
                    LOGGER.debug("User id [%d] with ban duration of %d, was unban manually after %.2f seconds",
                                 user_banned.user_id, user_banned.ban_duration,
                                 (message.message_datetime - user_banned.ban_datetime).total_seconds())

                    unban_set.add(user_banned.ban_id)

        # Set manually unban to the ban record
        if len(unban_set) > 0:
            UserBanned.set_multiple_unban(unban_set)

        # Statistics
        self.iteration_stats["_nb_unban"] = len(unban_set)


class CheerHandler(HandlerStatistics):
    @HandlerStatistics.exec_time(save_iteration_stats=True)
    def handle_cheer(self, cheer_data: dict):
        Cheer.insert_many_row([vars(b) for bc in cheer_data.values() for b in bc])

        # Statistics
        self.iteration_stats["nb_cheer"] = sum(len(ch) for ch in cheer_data.values())


class SubHandler(HandlerStatistics):
    @HandlerStatistics.exec_time(save_iteration_stats=True)
    def handle_sub(self, sub_data: dict):
        new_subs = dict()

        # Check among subs in sub_data
        for channel, channel_sub in sub_data.items():
            for sub in sorted(channel_sub, key=lambda x: x.sub_datetime):
                prev_sub = new_subs.get((sub.streamer_id, sub.user_id), None)

                # If there is sub for streamer/user, update it
                if prev_sub is not None:
                    self._update_prev_sub(prev_sub, sub)
                else:
                    new_subs[(sub.streamer_id, sub.user_id)] = sub

        # Check among subs in database
        sub_to_update = list()
        if len(new_subs) > 0:
            sub_database_list = Sub.get_many_subs([(s.streamer_id, s.user_id)
                                                   for s in new_subs.values()])

            for sub in sub_database_list:
                self._update_prev_sub(sub, new_subs.pop((sub.streamer_id, sub.user_id)))
                sub_to_update.append(sub)

            if len(sub_to_update) > 0:
                # TODO Use cte update
                for sub in sub_to_update:
                    Sub.update_sub(sub.streamer_id, sub.user_id,
                                   sub_datetime=sub.sub_datetime,
                                   month_tenure=sub.month_tenure,
                                   sub_type=sub.sub_type,
                                   gifted=sub.gifted,
                                   future_month_tenure=sub.future_month_tenure)

            Sub.insert_many_row([vars(s) for s in new_subs.values()])

        # Statistics
        self.iteration_stats["nb_new_sub"] = len(new_subs)
        self.iteration_stats["nb_update_sub"] = len(sub_to_update)

    def _update_prev_sub(self, prev_sub, new_sub):
        if prev_sub.future_month_tenure is not None:
            # It's possible to have a sub with future_month_tenure field not None and expired
            # but never month tenure >= future month tenure
            # Replace by none future month tenure of prev sub if it has expired
            if new_sub.month_tenure >= prev_sub.future_month_tenure:
                prev_sub.future_month_tenure = None

        if new_sub.future_month_tenure is not None:
            prev_sub.future_month_tenure = new_sub.future_month_tenure

        if new_sub.month_tenure > prev_sub.month_tenure or new_sub.sub_type != prev_sub.sub_type:
            prev_sub.sub_datetime = new_sub.sub_datetime
            prev_sub.month_tenure = new_sub.month_tenure
            prev_sub.sub_type = new_sub.sub_type
            prev_sub.gifted = new_sub.gifted
