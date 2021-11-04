import collections
from datetime import datetime, timedelta
import typing
from typing import Type, List, Union, TypeVar, NamedTuple, Iterable

from peewee import *
from playhouse import shortcuts

LIMIT_BULK_OPERATIONS = 1000

database = PostgresqlDatabase(None)


class BaseModel(Model):
    """A base model that will use our database."""

    class Meta:
        database = database

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="BaseModel")

    @classmethod
    def get_all_record_of_table(cls) -> List[Type[T]]:
        return [row for row in cls.select().iterator()]

    @classmethod
    def count_number_of_row(cls) -> int:
        return cls.select().count()

    @classmethod
    def insert(cls, **kwargs) -> None:
        # If field is a empty string, put None in database instead
        for field in kwargs:
            if isinstance(kwargs[field], str) and kwargs[field] == "":
                kwargs[field] = None
        super().insert(**kwargs).execute()

    @classmethod
    def insert_thread_safe(cls, **kwargs):
        with database.atomic():
            try:
                cls.insert(**kwargs)
            except IntegrityError:
                pass

    @classmethod
    def insert_many_row(cls, row_dict_list: list):
        with database.atomic():
            for batch in chunked(row_dict_list, LIMIT_BULK_OPERATIONS):
                cls.insert_many(batch).execute()

    def convert_model_to_dict(self) -> dict:
        return shortcuts.model_to_dict(self, recurse=False)


class Streamer(BaseModel):
    streamer_id = BigIntegerField(primary_key=True)
    login_name = TextField()
    display_name = TextField()
    broadcaster_type = TextField(null=True)
    description = TextField(null=True)
    profile_image = TextField()
    offline_image = TextField(null=True)
    banned = BooleanField(default=False)
    tracked = BooleanField(default=True)

    class Meta:
        table_name = 'streamer'

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="Streamer")

    @classmethod
    def get_all_streamers_tracked(cls):
        return [s for s in cls.select().where(cls.tracked == True)]

    @classmethod
    def get_streamers(cls, streamer_id_list: list = None, login_list: list = None) -> List[T]:
        streamer_id_list = list() if streamer_id_list is None else streamer_id_list
        login_list = list() if login_list is None else login_list
        return [s for s in cls.select().where(cls.streamer_id.in_(streamer_id_list) |
                                              (cls.login_name.in_(login_list)))]

    @classmethod
    def insert_streamer(cls, streamer_id: int, login_name: str, display_name: str, profile_image: str,
                        broadcaster_type: str = None, description: str = None,
                        offline_image: str = None) -> None:
        kwargs = locals()
        kwargs.pop("cls")
        cls.insert(**kwargs)

    @classmethod
    def update_streamer(cls, streamer_id: int, **kwargs) -> None:
        cls.update(**kwargs).where(cls.streamer_id == streamer_id).execute()

    @classmethod
    def delete_streamer(cls, user_id: int) -> None:
        # Also delete all relational data (ON DELETE CASCADE)
        cls.delete().where(cls.streamer_id == user_id).execute()


class Stream(BaseModel):
    stream_id = BigIntegerField(primary_key=True)
    streamer_id = ForeignKeyField(column_name='streamer_id', field='streamer_id', model=Streamer,
                                  on_delete='CASCADE', on_update='CASCADE', index=False, lazy_load=False)
    started_datetime = DateTimeField()
    ended_datetime = DateTimeField(default=None, null=True)
    language_stream = TextField()
    # Roomstate stats
    emote_only_pct = FloatField(default=None, null=True)
    r9k_pct = FloatField(default=None, null=True)
    slow_average = FloatField(default=None, null=True)
    subs_only_pct = FloatField(default=None, null=True)
    followers_only_state = IntegerField(default=None, null=True)
    # Message stats
    nb_total_message = IntegerField(default=0)
    nb_sub_message = IntegerField(default=0)
    nb_deleted_message = IntegerField(default=0)
    nb_unique_chatter = IntegerField(default=0)
    nb_sub_unique_chatter = IntegerField(default=0)
    # Ban stats
    nb_permaban = IntegerField(default=0)
    nb_timeout = IntegerField(default=0)
    nb_ban_by_bot = IntegerField(default=0)
    avg_duration_timeout = IntegerField(default=0)
    # Bit stats
    nb_cheer = IntegerField(default=0)
    nb_total_bit = IntegerField(default=0)
    # Sub stats
    nb_total_sub = IntegerField(default=0)
    nb_first_sub_no_gift = IntegerField(default=0)
    nb_gifted_sub = IntegerField(default=0)
    nb_prime_sub = IntegerField(default=0)
    nb_tier1_sub = IntegerField(default=0)
    nb_tier2_sub = IntegerField(default=0)
    nb_tier3_sub = IntegerField(default=0)

    class Meta:
        table_name = 'stream'
        indexes = (
            (('streamer_id', 'started_datetime'), False),
        )

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="Stream")

    @classmethod
    def insert_stream(cls, streamer_id: int, stream_id: int, started_datetime: datetime, language_stream: str) -> None:
        kwargs = locals()
        kwargs.pop("cls")
        cls.insert(**kwargs)

    @classmethod
    def update_stream(cls, stream_id: int, new_stream_id: int = None, **kwargs) -> None:
        cls.update(stream_id=new_stream_id if new_stream_id is not None else stream_id, **kwargs) \
            .where(cls.stream_id == stream_id).execute()

    @classmethod
    def get_many_streams(cls, stream_id_list: List[int]) -> List[T]:
        query = cls.select().where(cls.stream_id.in_(stream_id_list))

        return [stream for stream in query]

    @classmethod
    def get_active_streams(cls) -> List[T]:
        query = cls.select(cls, Streamer) \
            .join(Streamer) \
            .where(cls.ended_datetime.is_null(True))

        return [stream for stream in query]

    @classmethod
    def get_last_streams(cls) -> List[T]:
        cls_alias = cls.alias()
        subquery = cls_alias \
            .select(cls_alias.streamer_id, fn.MAX(cls_alias.started_datetime).alias("last_dt")) \
            .group_by(cls_alias.streamer_id)

        query = cls.select() \
            .join(subquery, on=(
                (cls.started_datetime == subquery.c.last_dt) &
                (cls.streamer_id == subquery.c.streamer_id)))

        return [s for s in query]


class StreamViewerCount(BaseModel):
    stream_id = ForeignKeyField(column_name='stream_id', field='stream_id', model=Stream, on_delete='CASCADE',
                                index=False, lazy_load=False)
    count_datetime = DateTimeField()
    nb_viewers = IntegerField()

    class Meta:
        table_name = 'stream_viewer_count'
        primary_key = CompositeKey('stream_id', 'count_datetime')

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="StreamViewerCount")

    @classmethod
    def insert_viewer_count(cls, stream_id: int, count_datetime: datetime, nb_viewers: int) -> None:
        kwargs = locals()
        kwargs.pop("cls")
        cls.insert(**kwargs)

    @classmethod
    def get_last_viewer_counts(cls) -> List[T]:
        cls_alias = cls.alias()
        subquery = cls_alias \
            .select(cls_alias.stream_id, fn.MAX(cls_alias.count_datetime).alias("last_dt")) \
            .join(Stream) \
            .where(Stream.ended_datetime.is_null(True)) \
            .group_by(cls_alias.stream_id)

        query = cls.select() \
            .join(subquery, on=(
                (cls.count_datetime == subquery.c.last_dt) &
                (cls.stream_id == subquery.c.stream_id)))

        return [s for s in query]


class Game(BaseModel):
    game_id = BigIntegerField(primary_key=True)
    game_title = TextField()
    box_art_url = TextField()

    class Meta:
        table_name = 'game'

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="Game")

    @classmethod
    def get_game(cls, game_id: int) -> Union[T, None]:
        return cls.get_or_none(cls.game_id == game_id)

    @classmethod
    def insert_game(cls, game_id: int, game_title: str, box_art_url: str) -> None:
        kwargs = locals()
        kwargs.pop("cls")
        cls.insert_thread_safe(**kwargs)

    @classmethod
    def update_game(cls, game_id: int, **kwargs) -> None:
        cls.update(**kwargs).where(cls.game_id == game_id).execute()


class StreamState(BaseModel):
    stream_id = ForeignKeyField(column_name='stream_id', field='stream_id', model=Stream, on_delete='CASCADE',
                                index=False, lazy_load=False)
    change_datetime = DateTimeField()
    title = TextField(null=True)
    game_id = ForeignKeyField(column_name='game_id', field='game_id', model=Game, null=True, lazy_load=False)

    class Meta:
        table_name = 'stream_state'
        primary_key = CompositeKey('stream_id', 'change_datetime')

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="StreamState")

    @classmethod
    def insert_stream_state(cls, stream_id: int, change_datetime: datetime, title: str = None,
                            game_id: int = None) -> None:
        kwargs = locals()
        kwargs.pop("cls")
        cls.insert(**kwargs)

    @classmethod
    def update_stream_state(cls, stream_id_change_dt: typing.Tuple[int, datetime], **kwargs) -> None:
        cls.update(**kwargs) \
            .where((cls.stream_id == stream_id_change_dt[0]) &
                   (cls.change_datetime == stream_id_change_dt[1])) \
            .execute()

    @classmethod
    def get_last_stream_state(cls, stream_id: int) -> Union[T, None]:
        try:
            last_stream_state = cls.select() \
                .where((cls.stream_id == stream_id)) \
                .order_by(cls.change_datetime.desc()) \
                .get()
            return last_stream_state
        except DoesNotExist:
            return None


class StreamStatsCount(BaseModel):
    stream_id = ForeignKeyField(column_name='stream_id', field='stream_id', model=Stream, on_delete='CASCADE',
                                index=False, lazy_load=False)
    start_count_datetime = DateTimeField()
    # Message
    nb_message = IntegerField(default=0)
    nb_deleted_message = IntegerField(default=0)
    # Ban
    nb_permaban = IntegerField(default=0)
    nb_timeout = IntegerField(default=0)
    # Sub
    nb_paid_sub = IntegerField(default=0)
    nb_gifted_sub = IntegerField(default=0)
    # Bit
    nb_bit = IntegerField(default=0)

    class Meta:
        table_name = 'stream_stats_count'
        primary_key = CompositeKey('stream_id', 'start_count_datetime')

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="StreamStatsCount")


class RoomState(BaseModel):
    streamer_id = ForeignKeyField(column_name='streamer_id', field='streamer_id', model=Streamer, on_delete='CASCADE',
                                  index=False, lazy_load=False)
    change_datetime = DateTimeField()
    emote_only = BooleanField()
    followers_only = IntegerField()
    r9k = BooleanField()
    slow = SmallIntegerField()
    subs_only = BooleanField()

    class Meta:
        table_name = 'room_state'
        primary_key = CompositeKey('streamer_id', 'change_datetime')

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="RoomState")

    @classmethod
    def insert_room_state(cls, streamer_id: int, change_datetime: datetime, emote_only: bool, followers_only: int,
                          r9k: bool, slow: int, subs_only: bool) -> None:
        kwargs = locals()
        kwargs.pop("cls")
        cls.insert(**kwargs)

    @classmethod
    def get_last_room_state_of_streamer(cls, streamer_id: int) -> Union[T, None]:
        try:
            return cls.select().where(cls.streamer_id == streamer_id).order_by(
                cls.change_datetime.desc()).get()
        except DoesNotExist:
            return None

    @classmethod
    def count_stream_room_state(cls, streamer_id: int, start_datetime: datetime, end_datetime: datetime) -> dict:
        # In case where there is not roomstate before stream
        try:
            # Previous roomstate datetime
            previous_datetime = cls.select() \
                .where((cls.streamer_id == streamer_id) &
                       (cls.change_datetime <= start_datetime)) \
                .order_by(cls.change_datetime.desc()) \
                .get().change_datetime
        except DoesNotExist:
            previous_datetime = datetime.min

        # Get all roomstate during time stream
        query = cls.select().where((cls.streamer_id == streamer_id) &
                                   (cls.change_datetime >= previous_datetime) &
                                   (cls.change_datetime < end_datetime)) \
            .order_by(cls.change_datetime)

        result = collections.defaultdict(float)
        total_time = (end_datetime - start_datetime).total_seconds()
        followers_only_duration = collections.defaultdict(float)

        for index, roomstate in enumerate(query):
            # Case where there is only one roomstate for the stream
            if len(query) == 1:
                interval_time = total_time
            # First stream roomstate
            elif index == 0:
                interval_time = (query[index + 1].change_datetime - start_datetime).total_seconds()
            # Last stream roomstate
            elif index == len(query) - 1:
                interval_time = (end_datetime - roomstate.change_datetime).total_seconds()
            else:
                interval_time = (query[index + 1].change_datetime - roomstate.change_datetime).total_seconds()

            try:
                result["emote_only_pct"] += interval_time / total_time * 100 if roomstate.emote_only else 0
                result["r9k_pct"] += interval_time / total_time * 100 if roomstate.r9k else 0
                result["slow_average"] += roomstate.slow * interval_time / total_time
                result["subs_only_pct"] += interval_time / total_time * 100 if roomstate.subs_only else 0
            except ZeroDivisionError:
                pass
            followers_only_duration[roomstate.followers_only] += interval_time

        # Keep followers only state with the longest duration
        result["followers_only_state"] = max(followers_only_duration, key=followers_only_duration.get, default=None)

        return result


class UserBanned(BaseModel):
    # Require peewee version >=3.13.3 otherwise there is a bug with bulk update if using UUIDField
    ban_id = UUIDField(index=True)
    streamer_id = ForeignKeyField(column_name='streamer_id', field='streamer_id', model=Streamer, on_delete='CASCADE',
                                  index=False, lazy_load=False)
    user_id = BigIntegerField()
    ban_datetime = DateTimeField(index=True)
    ban_duration = IntegerField()
    ban_by_bot = BooleanField(null=True)
    manually_unban = BooleanField(default=False)

    class Meta:
        table_name = 'user_banned'
        primary_key = False
        indexes = (
            (('streamer_id', 'user_id'), False),
        )

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="UserBanned")

    @classmethod
    def update_user_banned(cls, ban_id: str, **kwargs) -> None:
        cls.update(**kwargs).where(cls.ban_id == ban_id).execute()

    @classmethod
    def set_multiple_unban(cls, ban_id_list: Iterable[str]):
        cls.update(manually_unban=True).where(UserBanned.ban_id.in_(ban_id_list))

    @classmethod
    def delete_old_bans(cls, time_offset: int, nb_ban_offset: int) -> int:
        dt_max = datetime.utcnow() - timedelta(seconds=time_offset)
        streamer_id_dt = list()

        for stream in Stream.get_last_streams():
            streamer_id_dt.append((stream.streamer_id,
                                   dt_max if stream.ended_datetime is not None
                                   else stream.started_datetime - timedelta(seconds=time_offset)))

        val_list = ValuesList(streamer_id_dt).cte("streamer_offset", columns=["sid", "dt"])
        cls_alias1 = cls.alias()
        subquery = cls_alias1.select(cls_alias1.ban_id,
                                     fn.RANK().over(order_by=[cls_alias1.ban_datetime.desc()],
                                                    partition_by=[cls_alias1.streamer_id]).alias("rank")) \
            .join(val_list, on=((cls_alias1.streamer_id == val_list.c.sid) &
                                (cls_alias1.ban_datetime <= val_list.c.dt))) \
            .with_cte(val_list) \
            .alias("subq")

        cls_alias2 = cls.alias()
        query = cls_alias2.select(subquery.c.ban_id) \
            .from_(subquery) \
            .where(subquery.c.rank > nb_ban_offset)

        return cls.delete().where(cls.ban_id.in_(query)).execute()

    class UserParamBan(NamedTuple):
        param_id: str
        streamer_id: int
        user_id: int
        datetime: datetime

    @classmethod
    def get_many_active_ban(cls, user_param_list: List[UserParamBan]) -> List[T]:
        val_list = ValuesList(user_param_list).cte("streamer_user_dt", columns=["pid", "sid", "uid", "dt"])
        query = cls.select(cls, val_list.c.pid) \
            .join(val_list, on=((cls.streamer_id == val_list.c.sid) &
                                (cls.user_id == val_list.c.uid) &
                                (cls.manually_unban == False) &
                                (cls.ban_datetime < val_list.c.dt) &
                                ((cls.ban_duration == 0) |
                                 ((cls.ban_datetime +
                                   (SQL("INTERVAL '1 second'") * cls.ban_duration)) > val_list.c.dt)
                                 )
                                )
                  ) \
            .with_cte(val_list)

        ban_list = list()
        # Add pid in in attribute
        for ban in query:
            ban.pid = ban.streamer_user_dt["pid"]
            ban_list.append(ban)

        return ban_list

    @classmethod
    def count_bans_per_interval(cls, streamer_id: int, interval_list: list) -> dict:
        interval_count = dict()

        val_list = ValuesList(interval_list).cte("interval", columns=["sdt", "edt"])
        query = cls.select(fn.SUM(Case(None, [(cls.ban_duration == 0, 1)], 0)).alias("permaban"),
                           fn.SUM(Case(None, [(cls.ban_duration > 0, 1)], 0)).alias("timeout"),
                           fn.SUM(Case(None, [(cls.ban_by_bot == True, 1)], 0)).alias("ban_by_bot"),
                           val_list.c.sdt) \
            .join(val_list, on=((cls.streamer_id == streamer_id) &
                                (cls.ban_datetime >= val_list.c.sdt) &
                                (cls.ban_datetime < val_list.c.edt) &
                                (cls.manually_unban == False))) \
            .with_cte(val_list) \
            .group_by(val_list.c.sdt)

        for count in query:
            interval_count[count.interval["sdt"]] = {"nb_permaban": count.permaban,
                                                     "nb_timeout": count.timeout,
                                                     "nb_ban_by_bot": count.ban_by_bot
                                                     }

        return interval_count

    @classmethod
    def count_average_timeout(cls, streamer_id: int, start_datetime: datetime, end_datetime: datetime) -> float:
        query = cls.select(fn.AVG(Case(None, [(cls.ban_duration > 0, cls.ban_duration)]))
                           .alias("avg_duration_timeout")) \
            .where((cls.streamer_id == streamer_id) &
                   (cls.ban_datetime >= start_datetime) &
                   (cls.ban_datetime <= end_datetime) &
                   (cls.manually_unban == False)) \
            .get()

        return query.avg_duration_timeout if query.avg_duration_timeout is not None else 0


class UserMessage(BaseModel):
    # Require peewee version >=3.13.3 otherwise there is a bug with bulk update if using UUIDField
    message_id = UUIDField(index=True)
    streamer_id = ForeignKeyField(column_name='streamer_id', field='streamer_id', model=Streamer, on_delete='CASCADE',
                                  index=False, lazy_load=False)
    user_id = BigIntegerField()
    login_name = TextField()
    # Display name is stored only if is different of login name (case ignored), typically for japanese, korean... users
    display_name = TextField(null=True)
    message_datetime = DateTimeField()
    message_content = TextField()
    subscribed = SmallIntegerField()
    deleted = BooleanField()

    class Meta:
        table_name = 'user_message'
        primary_key = False
        indexes = (
            (('streamer_id', 'message_datetime'), False),
        )

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="UserMessage")

    @classmethod
    def flag_messages_as_deleted(cls, message_id_list: list) -> int:
        return cls.update(deleted=True).where(cls.message_id.in_(message_id_list)).execute()

    @classmethod
    def delete_old_messages(cls, time_offset: int, nb_message_offset: int, nb_deleted_message_offset: int) -> int:
        dt_max = datetime.utcnow() - timedelta(seconds=time_offset)
        streamer_id_dt = list()

        for stream in Stream.get_last_streams():
            streamer_id_dt.append((stream.streamer_id,
                                   dt_max if stream.ended_datetime is not None
                                   else stream.started_datetime - timedelta(seconds=time_offset)))

        val_list = ValuesList(streamer_id_dt).cte("streamer_offset", columns=["sid", "dt"])
        cls_alias1 = cls.alias()
        subquery = cls_alias1.select(cls_alias1.message_id,
                                     cls_alias1.deleted,
                                     fn.RANK().over(order_by=[cls_alias1.message_datetime.desc()],
                                                    partition_by=[cls_alias1.streamer_id,
                                                                  cls_alias1.deleted]).alias("rank")) \
            .join(val_list, on=((cls_alias1.streamer_id == val_list.c.sid) &
                                (cls_alias1.message_datetime <= val_list.c.dt))) \
            .with_cte(val_list) \
            .alias("subq")

        cls_alias2 = cls.alias()
        query = cls_alias2.select(subquery.c.message_id) \
            .from_(subquery) \
            .where(((subquery.c.rank > nb_message_offset) & (subquery.c.deleted == False) |
                    ((subquery.c.rank > nb_deleted_message_offset) & (subquery.c.deleted == True))))

        return cls.delete().where(cls.message_id.in_(query)).execute()

    class UserParamMessage(NamedTuple):
        param_id: str
        streamer_id: int
        user_id: int
        nb_last_messages: int

    @classmethod
    def get_last_messages_from_many_users(cls, user_param_list: List[UserParamMessage]) -> List[T]:
        message_list = list()
        streamer_set = {user_param.streamer_id for user_param in user_param_list}

        for streamer_id in streamer_set:
            cls_alias = cls.alias()
            user_param_list_aggr = [user_param for user_param in user_param_list if
                                    user_param.streamer_id == streamer_id]

            subquery = cls_alias \
                .select(cls_alias, fn.RANK().over(order_by=[cls_alias.message_datetime.desc()])
                        .alias("rank")) \
                .where(cls_alias.streamer_id == streamer_id) \
                .order_by(cls_alias.message_datetime.desc()) \
                .limit(max(p.nb_last_messages for p in user_param_list_aggr))

            user_cte = ValuesList(user_param_list_aggr).cte("streamer_user_limit",
                                                            columns=["pid", "sid", "uid", "limit"])
            query = cls \
                .select(user_cte.c.pid, subquery.c.message_id, subquery.c.streamer_id, subquery.c.user_id,
                        subquery.c.login_name, subquery.c.display_name, subquery.c.message_datetime,
                        subquery.c.message_content, subquery.c.subscribed, subquery.c.deleted,
                        subquery.c.rank) \
                .from_(subquery) \
                .join(user_cte, on=(
                    (subquery.c.user_id == user_cte.c.uid) &
                    (subquery.c.rank <= user_cte.c.limit))) \
                .with_cte(user_cte)

            for mes in query:
                message_list.append(mes)

        return message_list

    @classmethod
    def count_messages_per_interval(cls, streamer_id: int, interval_list: list) -> dict:
        interval_count = dict()

        val_list = ValuesList(interval_list).cte("interval", columns=["sdt", "edt"])
        query = cls.select(fn.COUNT(cls.message_id).alias("total_mes"),
                           fn.SUM(Case(None, [(cls.subscribed > 0, 1)], 0)).alias("mes_sub"),
                           fn.SUM(Case(None, [(cls.deleted == True, 1)], 0)).alias("mes_deleted"),
                           val_list.c.sdt) \
            .join(val_list, on=((cls.streamer_id == streamer_id) &
                                (cls.message_datetime >= val_list.c.sdt) &
                                (cls.message_datetime < val_list.c.edt))) \
            .with_cte(val_list) \
            .group_by(val_list.c.sdt)

        for count in query:
            interval_count[count.interval["sdt"]] = {"nb_total_message": count.total_mes,
                                                     "nb_sub_message": count.mes_sub,
                                                     "nb_deleted_message": count.mes_deleted
                                                     }

        return interval_count

    @classmethod
    def count_unique_chatters(cls, streamer_id: int, start_datetime: datetime, end_datetime: datetime) -> dict:
        query = cls.select(fn.COUNT(cls.user_id.distinct()).alias("unique_chatter"),
                           fn.COUNT(Case(None, [(cls.subscribed > 0, cls.user_id)], 0).distinct())
                           .alias("sub_unique_chatter"),
                           ) \
            .where((cls.streamer_id == streamer_id) &
                   (cls.message_datetime >= start_datetime) &
                   (cls.message_datetime <= end_datetime)) \
            .get()

        return {"nb_unique_chatter": query.unique_chatter,
                "nb_sub_unique_chatter": query.sub_unique_chatter
                }


class Cheer(BaseModel):
    streamer_id = ForeignKeyField(column_name='streamer_id', field='streamer_id', model=Streamer, on_delete='CASCADE',
                                  index=True, lazy_load=False)
    user_id = IntegerField()
    cheer_datetime = DateTimeField()
    nb_bits = IntegerField()

    class Meta:
        table_name = 'cheer'
        primary_key = False

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="Cheer")

    @classmethod
    def count_cheers_per_interval(cls, streamer_id: int, interval_list: list) -> dict:
        interval_count = dict()

        val_list = ValuesList(interval_list).cte("interval", columns=["sdt", "edt"])
        query = Cheer.select(fn.COUNT(cls).alias("nb_cheer"),
                             fn.SUM(cls.nb_bits).alias("nb_total_bit"),
                             val_list.c.sdt) \
            .join(val_list, on=((cls.streamer_id == streamer_id) &
                                (cls.cheer_datetime >= val_list.c.sdt) &
                                (cls.cheer_datetime < val_list.c.edt))) \
            .with_cte(val_list) \
            .group_by(val_list.c.sdt)

        for count in query:
            interval_count[count.interval["sdt"]] = {"nb_cheer": count.nb_cheer,
                                                     "nb_total_bit": count.nb_total_bit
                                                     if count.nb_total_bit is not None else 0
                                                     }

        return interval_count


class Sub(BaseModel):
    streamer_id = ForeignKeyField(column_name='streamer_id', field='streamer_id', model=Streamer, on_delete='CASCADE',
                                  index=False, lazy_load=False)
    user_id = BigIntegerField()
    sub_datetime = DateTimeField()
    month_tenure = SmallIntegerField()
    sub_type = SmallIntegerField()
    gifted = BooleanField()
    future_month_tenure = SmallIntegerField(null=True)

    class Meta:
        table_name = 'sub'
        primary_key = CompositeKey('streamer_id', 'user_id')
        indexes = (
            (('streamer_id', 'sub_datetime'), False),
        )

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="Sub")

    @classmethod
    def update_sub(cls, streamer_id: int, user_id: int, **kwargs) -> None:
        cls.update(**kwargs).where((cls.streamer_id == streamer_id) & (cls.user_id == user_id))

    @classmethod
    def get_many_subs(cls, streamer_user_id_list: List[tuple]) -> List[T]:
        val_list = ValuesList(streamer_user_id_list).cte("streamer_user", columns=["sid", "uid"])
        query = cls.select() \
            .join(val_list, on=((cls.streamer_id == val_list.c.sid) &
                                (cls.user_id == val_list.c.uid))) \
            .with_cte(val_list)

        return [sub for sub in query]

    @classmethod
    def count_subs_per_interval(cls, streamer_id: int, interval_list: list) -> dict:
        interval_count = dict()

        val_list = ValuesList(interval_list).cte("interval", columns=["sdt", "edt"])
        query = cls.select(fn.COUNT(cls.user_id).alias("total_sub"),
                           fn.SUM(Case(None, [((cls.month_tenure == 1) & (cls.gifted == False), 1)], 0))
                           .alias("first_sub"),
                           fn.SUM(Case(None, [(cls.gifted == True, 1)], 0)).alias("gifted"),
                           fn.SUM(Case(None, [(cls.sub_type == 0, 1)], 0)).alias("prime"),
                           fn.SUM(Case(None, [(cls.sub_type == 1, 1)], 0)).alias("tier1"),
                           fn.SUM(Case(None, [(cls.sub_type == 2, 1)], 0)).alias("tier2"),
                           fn.SUM(Case(None, [(cls.sub_type == 3, 1)], 0)).alias("tier3"),
                           val_list.c.sdt) \
            .join(val_list, on=((cls.streamer_id == streamer_id) &
                                (cls.sub_datetime >= val_list.c.sdt) &
                                (cls.sub_datetime < val_list.c.edt))) \
            .with_cte(val_list) \
            .group_by(val_list.c.sdt)

        for count in query:
            interval_count[count.interval["sdt"]] = {"nb_total_sub": count.total_sub,
                                                     "nb_first_sub_no_gift": count.first_sub,
                                                     "nb_gifted_sub": count.gifted,
                                                     "nb_prime_sub": count.prime,
                                                     "nb_tier1_sub": count.tier1,
                                                     "nb_tier2_sub": count.tier2,
                                                     "nb_tier3_sub": count.tier3
                                                     }

        return interval_count

    @classmethod
    def count_active_subs_all_channels(cls, from_datetime: datetime) -> list:
        query = cls.select(cls.streamer_id,
                           fn.COUNT(cls.user_id).alias("total_sub"),
                           Value(fn.AVG(cls.month_tenure).alias("avg_month_tenure")),
                           fn.SUM(Case(None, [(cls.gifted == True, 1)], 0)).alias("gifted"),
                           fn.SUM(Case(None, [(cls.sub_type == 0, 1)], 0)).alias("prime"),
                           fn.SUM(Case(None, [(cls.sub_type == 1, 1)], 0)).alias("tier1"),
                           fn.SUM(Case(None, [(cls.sub_type == 2, 1)], 0)).alias("tier2"),
                           fn.SUM(Case(None, [(cls.sub_type == 3, 1)], 0)).alias("tier3"),
                           ) \
            .where((cls.sub_datetime > (from_datetime - SQL("INTERVAL '1 month'"))) |
                   (
                           (cls.future_month_tenure.is_null(False)) &
                           (cls.sub_datetime >
                            from_datetime - (
                                    (cls.future_month_tenure - cls.month_tenure + 1) * SQL("INTERVAL '1 month'")))
                   )
                   ) \
            .group_by(cls.streamer_id)

        return [{"streamer_id": count.streamer_id,
                 "count_datetime": from_datetime,
                 "nb_active_total_sub": count.total_sub,
                 "average_month_tenure": count.avg_month_tenure,
                 "nb_active_gifted_sub": count.gifted,
                 "nb_active_prime_sub": count.prime,
                 "nb_active_tier1_sub": count.tier1,
                 "nb_active_tier2_sub": count.tier2,
                 "nb_active_tier3_sub": count.tier3
                 } for count in query]


class SubStatsCount(BaseModel):
    streamer_id = ForeignKeyField(column_name='streamer_id', field='streamer_id', model=Streamer, on_delete='CASCADE',
                                  index=False, lazy_load=False)
    count_datetime = DateTimeField()
    nb_active_total_sub = IntegerField()
    average_month_tenure = FloatField()
    nb_active_gifted_sub = IntegerField()
    nb_active_prime_sub = IntegerField()
    nb_active_tier1_sub = IntegerField()
    nb_active_tier2_sub = IntegerField()
    nb_active_tier3_sub = IntegerField()

    class Meta:
        table_name = 'sub_stats_count'
        primary_key = False
        indexes = (
            (('streamer_id', 'count_datetime'), False),
        )

    # ------------------------------------------------------------------------------------------------------------------
    T = TypeVar('T', bound="SubStatsCount")

    @classmethod
    def get_last_stats_counts(cls) -> List[T]:
        cls_alias = cls.alias()
        subquery = cls_alias \
            .select(cls_alias.streamer_id, fn.MAX(cls_alias.count_datetime).alias("last_dt")) \
            .group_by(cls_alias.streamer_id)

        query = cls.select() \
            .join(subquery, on=(
                (cls.count_datetime == subquery.c.last_dt) &
                (cls.streamer_id == subquery.c.streamer_id)))

        return [s for s in query]


def reindex_table(table_name: str):
    if table_name in database.get_tables():
        database.execute_sql("REINDEX TABLE {}".format(table_name))


def init_db(database_name: str, host: str, port: int, user: str, password: str):
    database.init(database_name, host=host, port=port, user=user, password=password)
    database.connect()

    # Create table if not exists
    database.create_tables(
        [Streamer, Stream, StreamViewerCount, Game, StreamState, StreamStatsCount, RoomState,
         UserBanned, UserMessage, Cheer, Sub, SubStatsCount])
