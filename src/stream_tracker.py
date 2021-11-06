import asyncio
import collections
import logging
import time
from datetime import datetime, timedelta
from typing import Tuple

import global_data
import twitch_api
import utils
from database_model import Streamer, Stream, StreamState, StreamStatsCount, StreamViewerCount, \
    RoomState, UserMessage, UserBanned, Cheer, Sub, SubStatsCount

# fetch_new_streams
TRACK_ONLY_VERIFIED = True
NB_FETCH_NEW_STREAMS = 200
POLL_TIME_NEW_STREAMS = 10 * 60

# check_streams_change
INTERVAL_COUNT_STREAM = 15 * 60
MIN_INTERVAL_VIEWER_COUNT = 8 * 60
POLL_TIME_CHECK_STREAMS = 5 * 60

# check_streamers_change
POLL_TIME_CHECK_STREAMERS = 15 * 60

# handle_sub_stats_count
POLL_TIME_HANDLE_SUB_STATS = 3 * 60 * 60

# clean_old_stats
CLEAN_TIME_OFFSET = 30 * 60
CLEAN_MESSAGE_OFFSET = 150
CLEAN_DELETED_MESSAGE_OFFSET = 100
CLEAN_BAN_OFFSET = 100
POLL_TIME_CLEAN_STATS = 15 * 60

LOGGER = logging.getLogger(__name__)


class StreamTracker:
    def __init__(self):
        self.event_loop = asyncio.new_event_loop()

    async def fetch_new_streams(self):
        start_time = time.time()

        nb_streamers_tracked = Streamer.count_number_of_row()
        if nb_streamers_tracked >= global_data.ARGS.MAX_STREAMERS_TRACKED:
            return

        first_streams_list = twitch_api.get_first_streams(NB_FETCH_NEW_STREAMS)

        # Retrieve only streamer's stream not already tracked
        streamer_id_tracked_set = {stream.streamer_id for stream in
                                   Streamer.get_streamers(streamer_id_list=[s.streamer_id
                                                                            for s in first_streams_list])}
        new_streams_dict = {stream.streamer_id: stream for stream in first_streams_list
                            if stream.streamer_id not in streamer_id_tracked_set}

        # Retrieve user info
        new_users_list = twitch_api.get_many_users(user_id_list=list(new_streams_dict.keys()))

        new_streamers_tracked_counter = 0
        for user in new_users_list:
            if TRACK_ONLY_VERIFIED and user.broadcaster_type is None:
                continue

            if nb_streamers_tracked + new_streamers_tracked_counter < global_data.ARGS.MAX_STREAMERS_TRACKED:
                new_streamers_tracked_counter += 1
            else:
                LOGGER.debug("Limit number of streamer tracked reached (tracked=%d, limit=%d), stop tracking "
                             "new streamers",
                             nb_streamers_tracked + new_streamers_tracked_counter,
                             global_data.ARGS.MAX_STREAMERS_TRACKED)
                break

            stream = new_streams_dict[user.streamer_id]

            # Save streamer in database
            Streamer.insert_streamer(**vars(user))

            # Save stream in database
            Stream.insert_stream(stream_id=stream.stream_id,
                                 streamer_id=stream.streamer_id,
                                 started_datetime=stream.started_datetime,
                                 language_stream=stream.language)

            # Save stream state in database
            StreamState.insert_stream_state(stream_id=stream.stream_id,
                                            change_datetime=stream.started_datetime,
                                            title=stream.title,
                                            game_id=stream.game_id)

            # Save current number of viewers
            StreamViewerCount.insert_viewer_count(stream_id=stream.stream_id,
                                                  count_datetime=stream.response_datetime,
                                                  nb_viewers=stream.nb_viewers)

            LOGGER.debug("Track [%s] stream (%s viewers)", user.login_name, stream.nb_viewers)

        LOGGER.info("Fetch new streams processed in %.2f seconds, %d new streams tracked, %d total streams tracked",
                    time.time() - start_time,
                    new_streamers_tracked_counter,
                    nb_streamers_tracked + new_streamers_tracked_counter)

    @staticmethod
    def stream_change(stream_data: twitch_api.StreamAPI, new_stream: bool) -> bool:
        if new_stream:
            Stream.insert_stream(streamer_id=stream_data.streamer_id,
                                 stream_id=stream_data.stream_id,
                                 started_datetime=stream_data.started_datetime,
                                 language_stream=stream_data.language)

            change_datetime = stream_data.started_datetime
            last_stream_state = None
        else:
            change_datetime = stream_data.response_datetime
            last_stream_state = StreamState.get_last_stream_state(stream_data.stream_id)

        if last_stream_state is not None:
            # Check if there is a difference with the last stream state
            if last_stream_state.change_datetime <= change_datetime and (
                    last_stream_state.game_id != stream_data.game_id or
                    last_stream_state.title != stream_data.title):
                # If the last stream state is in the last minute, update it, otherwise insert new steam state
                if (change_datetime - last_stream_state.change_datetime).total_seconds() < 60:
                    StreamState.update_stream_state((last_stream_state.stream_id,
                                                     last_stream_state.change_datetime),
                                                    change_datetime=change_datetime,
                                                    title=stream_data.title,
                                                    game_id=stream_data.game_id
                                                    )
                else:
                    StreamState.insert_stream_state(stream_id=stream_data.stream_id,
                                                    change_datetime=change_datetime,
                                                    title=stream_data.title,
                                                    game_id=stream_data.game_id
                                                    )
                return True
            else:
                return False
        else:
            StreamState.insert_stream_state(stream_id=stream_data.stream_id,
                                            change_datetime=change_datetime,
                                            title=stream_data.title,
                                            game_id=stream_data.game_id
                                            )
            return True

    @staticmethod
    def _compile_stream_stats_time_series(stream_id: int, datetime_list: list,
                                          message_stats: dict, ban_stats: dict,
                                          cheer_stats: dict, sub_stats: dict):
        time_series_stats = {dt: {"stream_id": stream_id,
                                  "start_count_datetime": dt} for dt in datetime_list}

        for dt, stats in message_stats.items():
            time_series_stats[dt]["nb_message"] = stats["nb_total_message"]
            time_series_stats[dt]["nb_deleted_message"] = stats["nb_deleted_message"]

        for dt, stats in ban_stats.items():
            time_series_stats[dt]["nb_permaban"] = stats["nb_permaban"]
            time_series_stats[dt]["nb_timeout"] = stats["nb_timeout"]

        for dt, stats in cheer_stats.items():
            time_series_stats[dt]["nb_bit"] = stats["nb_total_bit"]

        for dt, stats in sub_stats.items():
            time_series_stats[dt]["nb_paid_sub"] = stats["nb_total_sub"] - stats["nb_gifted_sub"]
            time_series_stats[dt]["nb_gifted_sub"] = stats["nb_gifted_sub"]

        StreamStatsCount.insert_many_row([s for s in time_series_stats.values()])

    @staticmethod
    def compile_stream_stats(streamer_id: int, streamer_name: str, stream_id: int, started_datetime: datetime,
                             ended_datetime: datetime):
        def sum_stats(stats_per_datetime: dict) -> dict:
            s = collections.defaultdict(int)
            for stats in stats_per_datetime.values():
                for k, v in stats.items():
                    s[k] += v
            return s

        start_time = time.time()

        # Generate time-series datetime
        nb_datetime = int((ended_datetime - started_datetime).total_seconds() / INTERVAL_COUNT_STREAM) + 1
        datetime_list = [started_datetime + timedelta(seconds=i * INTERVAL_COUNT_STREAM)
                         for i in range(nb_datetime)]

        interval_list = list()
        for idx, dt in enumerate(datetime_list):
            interval_list.append((dt, datetime_list[idx + 1]) if idx + 1 < len(datetime_list) else (dt, ended_datetime))

        # Get stats
        message_stats = UserMessage.count_messages_per_interval(streamer_id, interval_list)
        ban_stats = UserBanned.count_bans_per_interval(streamer_id, interval_list)
        cheer_stats = Cheer.count_cheers_per_interval(streamer_id, interval_list)
        sub_stats = Sub.count_subs_per_interval(streamer_id, interval_list)

        StreamTracker._compile_stream_stats_time_series(stream_id, datetime_list,
                                                        message_stats, ban_stats,
                                                        cheer_stats, sub_stats)

        Stream.update_stream(stream_id,
                             ended_datetime=ended_datetime,
                             avg_viewers=StreamViewerCount.get_average_viewers(stream_id),
                             **RoomState.count_stream_room_state(
                                 streamer_id,
                                 started_datetime,
                                 ended_datetime),
                             **sum_stats(message_stats),
                             **UserMessage.count_unique_chatters(
                                 streamer_id,
                                 started_datetime,
                                 ended_datetime,
                             ),
                             **sum_stats(ban_stats),
                             avg_duration_timeout=UserBanned.count_average_timeout(
                                 streamer_id,
                                 started_datetime,
                                 ended_datetime
                             ),
                             **sum_stats(cheer_stats),
                             **sum_stats(sub_stats),
                             )

        LOGGER.debug("Compile and save stats of [%s]'s stream in %.2f secs",
                     streamer_name, time.time() - start_time)

    # Check all streams that are online in database are not gone offline and if there are new streams
    async def check_streams_change(self):
        stats = collections.Counter()
        start_time = time.time()

        stream_id_online_dict = {stream.stream_id: stream for stream in twitch_api.get_many_streams(
            [s.streamer_id for s in Streamer.get_all_streamers_tracked()])}
        streamer_id_online_dict = {stream.streamer_id: stream for stream in stream_id_online_dict.values()}

        if len(stream_id_online_dict) > 0:
            # Get response_datetime of the last requests
            response_datetime_end = max(s.response_datetime for s in stream_id_online_dict.values())
        else:
            response_datetime_end = datetime.utcnow()

        # Check streams that went offline
        active_stream_db_list = Stream.get_active_streams()
        for stream_db in active_stream_db_list:
            streamer_id_db = stream_db.streamer_id.streamer_id
            if stream_db.stream_id not in stream_id_online_dict:
                # Check if a stream was restarted in meantime
                if streamer_id_db in streamer_id_online_dict:
                    # If restarted, update stream id
                    Stream.update_stream(stream_id=stream_db.stream_id,
                                         new_stream_id=streamer_id_online_dict[streamer_id_db].stream_id)
                    stats["restarted"] += 1
                else:
                    self.compile_stream_stats(streamer_id_db,
                                              stream_db.streamer_id.login_name,
                                              stream_db.stream_id,
                                              stream_db.started_datetime,
                                              response_datetime_end)
                    stats["ended"] += 1

        # Check new online streams
        stream_id_db_set = {s.stream_id for s in Stream.get_many_streams(list(stream_id_online_dict.keys()))}
        for stream_id, online_stream in stream_id_online_dict.items():
            if stream_id not in stream_id_db_set:
                LOGGER.debug("New stream from [%s]", online_stream.login_name)
                self.stream_change(online_stream, True)
                stats["started"] += 1
            else:
                if self.stream_change(online_stream, False):
                    LOGGER.debug("Stream change from [%s]", online_stream.login_name)
                    stats["changed"] += 1

        # Insert viewer_count of current streams in database only if previous viewer count is different
        viewer_count_list = [{"stream_id": stream.stream_id,
                              "count_datetime": stream.response_datetime,
                              "nb_viewers": stream.nb_viewers}
                             for stream_id, stream in stream_id_online_dict.items()]
        last_viewer_count_dict = {v.stream_id: v for v in StreamViewerCount.get_last_viewer_counts()}
        row_list = list()

        for viewer_count in viewer_count_list:
            last_viewer_count = last_viewer_count_dict.get(viewer_count["stream_id"], None)
            if last_viewer_count is None:
                row_list.append(viewer_count)
            elif viewer_count["nb_viewers"] != last_viewer_count.nb_viewers and \
                    (viewer_count["count_datetime"] - last_viewer_count.count_datetime).total_seconds() > \
                    MIN_INTERVAL_VIEWER_COUNT:
                row_list.append(viewer_count)

        StreamViewerCount.insert_many_row(row_list)

        LOGGER.info("Check stream processed in %.2f seconds (%d started, %d restarted, %d changed, %d ended)",
                    time.time() - start_time, stats["started"], stats["restarted"], stats["changed"], stats["ended"])

    async def check_streamers_change(self):
        stats = collections.Counter()
        start_time = time.time()

        streamers_list = Streamer.get_all_streamers_tracked()

        streamers_update = twitch_api.get_many_users(user_id_list=[s.streamer_id for s in streamers_list])
        streamers_update = {s.streamer_id: s for s in streamers_update}

        for streamer in streamers_list:
            # Check channel banned, channels don't appear in get users API request if there are banned
            if streamer.streamer_id not in streamers_update:
                if not streamer.banned:
                    Streamer.update_streamer(streamer_id=streamer.streamer_id, banned=True)
                    LOGGER.debug("Streamer [%s] has been banned", streamer.login_name)
                    stats["banned"] += 1
                continue

            streamer_update = streamers_update[streamer.streamer_id]
            update = False

            # Check channel unbanned
            if streamer.banned:
                update = True
                LOGGER.debug("Streamer [%s] has been unbanned", streamer.login_name)
                stats["unbanned"] += 1

            # Check info changes
            info_changed = False
            for field in {"login_name",
                          "display_name",
                          "broadcaster_type",
                          "description",
                          "profile_image",
                          "offline_image"}:
                if getattr(streamer, field) != getattr(streamer_update, field):
                    update = True
                    info_changed = True
                    if field == "login_name":
                        LOGGER.debug("Streamer [%s] has changed its login name to [%s]",
                                     streamer.login_name, streamer_update.login_name)
            if info_changed:
                LOGGER.debug("Streamer [%s] has changed profile info", streamer.login_name)
                stats["info_changed"] += 1

            if update:
                Streamer.update_streamer(**vars(streamer_update), banned=False)

        LOGGER.info("Check streamer processed in %.2f seconds (%d info_changed, %d banned, %d unbanned)",
                    time.time() - start_time, stats["info_changed"], stats["banned"], stats["unbanned"])

    async def handle_sub_stats_count(self):
        current_datetime = datetime.utcnow()
        start_time = time.time()
        row_list = list()

        sub_count_list = Sub.count_active_subs_all_channels(current_datetime)
        last_sub_count_dict = {s.streamer_id: s for s in SubStatsCount.get_last_stats_counts()}

        for stats in sub_count_list:
            # Only store in database if there is a change with previous count
            if any(stats[field] != getattr(last_sub_count_dict.get(stats["streamer_id"], None), field, None)
                   for field in stats.keys() if field not in ("streamer_id",
                                                              "count_datetime",
                                                              "average_month_tenure")):
                row_list.append(stats)

        # Store in database
        SubStatsCount.insert_many_row(row_list)

        LOGGER.info("Handle sub stats count for %d channels in %.2f seconds",
                    len(row_list),
                    time.time() - start_time)

    async def clean_old_stats(self):
        start_time = time.time()
        nb_mes_del = await self.event_loop.run_in_executor(None, UserMessage.delete_old_messages,
                                                           CLEAN_TIME_OFFSET,
                                                           CLEAN_MESSAGE_OFFSET,
                                                           CLEAN_DELETED_MESSAGE_OFFSET)

        nb_ban_del = await self.event_loop.run_in_executor(None, UserBanned.delete_old_bans,
                                                           CLEAN_TIME_OFFSET,
                                                           CLEAN_BAN_OFFSET)

        LOGGER.info("Clean old stats processed in %.2f seconds (%d UserMessage deleted, %d UserBanned deleted)",
                    time.time() - start_time, nb_mes_del, nb_ban_del)

    async def track_streamers(self, login_list: list) -> Tuple[list, list]:
        already_tracked_list = list()
        no_exist_list = list()

        streamer_dict = {s.login_name: s for s in twitch_api.get_many_users(login_list=login_list)}
        streamer_database_dict = {st.streamer_id: st
                                  for st in Streamer.get_streamers(streamer_id_list=[s.streamer_id
                                                                                     for s in streamer_dict.values()])}

        for login in login_list:
            streamer = streamer_dict.get(login, None)
            if streamer is not None:
                if streamer.streamer_id not in streamer_database_dict:
                    Streamer.insert_streamer(**vars(streamer))
                elif streamer_database_dict[streamer.streamer_id].tracked is False:
                    Streamer.update_streamer(streamer.streamer_id, tracked=True)
                else:
                    already_tracked_list.append(login)
            else:
                no_exist_list.append(login)

        return already_tracked_list, no_exist_list

    async def untrack_streamers(self, login_list: list) -> list:
        already_untracked_list = list()

        streamer_dict = {s.login_name: s for s in Streamer.get_streamers(login_list=login_list)}
        for login in login_list:
            streamer = streamer_dict.get(login, None)
            if streamer is not None and streamer.tracked:
                Streamer.update_streamer(streamer.streamer_id, tracked=False)
            else:
                already_untracked_list.append(login)

        return already_untracked_list

    def start(self):
        asyncio.set_event_loop(self.event_loop)

        if global_data.ARGS.NO_AUTO_TRACK is False:
            self.event_loop.create_task(utils.periodic_task(self.fetch_new_streams,
                                                            POLL_TIME_NEW_STREAMS))
        self.event_loop.create_task(utils.periodic_task(self.check_streams_change,
                                                        POLL_TIME_CHECK_STREAMS))
        self.event_loop.create_task(utils.periodic_task(self.check_streamers_change,
                                                        POLL_TIME_CHECK_STREAMERS))
        self.event_loop.create_task(utils.periodic_task(self.handle_sub_stats_count,
                                                        POLL_TIME_HANDLE_SUB_STATS))
        if global_data.ARGS.NO_AUTO_CLEAN is False:
            self.event_loop.create_task(utils.periodic_task(self.clean_old_stats,
                                                            POLL_TIME_CLEAN_STATS))

        self.event_loop.run_forever()
