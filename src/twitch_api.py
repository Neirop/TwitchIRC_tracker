import logging
import re
import time
import typing
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime

import global_data
import utils
from database_model import Game

LOGGER = logging.getLogger(__name__)


@dataclass
class UserAPI:
    streamer_id: int
    login_name: str
    display_name: str
    broadcaster_type: typing.Union[str, None]
    description: typing.Union[str, None]
    profile_image: str
    offline_image: typing.Union[str, None]


@dataclass
class StreamAPI:
    stream_id: int
    streamer_id: int
    login_name: str
    display_name: str
    language: str
    title: typing.Union[str, None]
    started_datetime: datetime
    game_id: typing.Union[int, None]
    game_title: typing.Union[str, None]
    nb_viewers: int
    response_datetime: datetime

@dataclass
class GameAPI:
    game_id: int
    game_title: str
    box_art_url: str


def _request_get_api(url: str) -> typing.Union[dict, None]:
    headers = {"Client-ID": global_data.API_CLIENT_ID, "Authorization": "Bearer " + global_data.API_APP_ACCESS_TOKEN}

    response = utils.request_get(url, headers=headers)

    # Too many requests
    while response.status_code == 429:
        time_to_wait = int(response.headers["ratelimit-reset"]) - time.time()
        LOGGER.warning("Too many API requests, waiting " + "%.2f" % time_to_wait + " seconds")
        time.sleep(time_to_wait)
        # Retry request after the ratelimit reset is reached
        response = utils.request_get(url, headers=headers)

    if response.status_code != 200:
        error_message = "no message"
        try:
            error_message = response.json()["message"]
        except (ValueError, KeyError):
            pass
        LOGGER.error("Error %d (%s) for request: %s", response.status_code, error_message, url)
        return None

    resp_json = response.json()
    # Add current datetime to know when request was achieved
    try:
        resp_json["response_datetime"] = datetime.utcfromtimestamp(
            float(re.findall(r'S(\d+.\d+),', response.headers["X-Timer"])[0]))
    except (KeyError, IndexError):
        resp_json["response_datetime"] = datetime.utcnow()

    return resp_json


def _requests_with_pagination(number: int, url_endpoint: str) -> typing.Union[list, None]:
    json_list = list()
    cursor = None

    while number != 0:
        if number > 100:
            first = 100
        else:
            first = number
        number -= first

        if cursor is None:
            req_url = url_endpoint + "?first=" + str(first)
        else:
            req_url = url_endpoint + "?first=" + str(first) + "&after=" + cursor

        resp_json = _request_get_api(req_url)
        if resp_json is None:
            return None

        json_list.append(resp_json)

        # In case that is the end of data
        if "cursor" in resp_json["pagination"]:
            cursor = resp_json["pagination"]["cursor"]
        else:
            break

    return json_list


def parse_data_stream(stream_data: dict, resp_datetime: datetime) -> StreamAPI:
    stream = StreamAPI(stream_id=int(stream_data["id"]),
                       streamer_id=int(stream_data["user_id"]),
                       login_name=stream_data["user_login"],
                       display_name=stream_data["user_name"],
                       language=stream_data["language"],
                       title=stream_data["title"] if stream_data["title"] != "" else None,
                       started_datetime=utils.convert_str_date_to_datetime(stream_data["started_at"]),
                       game_id=int(stream_data["game_id"]) if stream_data["game_id"] != "" else None,
                       game_title=stream_data["game_name"] if stream_data["game_name"] != "" else None,
                       nb_viewers=stream_data["viewer_count"],
                       response_datetime=resp_datetime,
                       )

    # Save game in database
    if stream.game_id is not None:
        if not _fetch_game_title(stream.game_id, stream.game_title):
            stream.game_id = None
            stream.game_title = None

    return stream


def get_first_streams(number: int) -> list:
    # Reference: https://dev.twitch.tv/docs/api/reference#get-streams
    stream_list = list()

    json_list = None
    while json_list is None:
        json_list = _requests_with_pagination(number, "https://api.twitch.tv/helix/streams")

    for js in json_list:
        for data in js["data"]:
            stream_list.append(parse_data_stream(data, js["response_datetime"]))

    return stream_list


def get_many_streams(streamer_id_list: list) -> list:
    # Reference: https://dev.twitch.tv/docs/api/reference#get-streams
    future_list = list()
    stream_list = list()
    base_req_url = "https://api.twitch.tv/helix/streams?"

    req_url = base_req_url
    nb_id = 0

    with ThreadPoolExecutor(max_workers=16) as thread_pool:
        for index, user_id in enumerate(streamer_id_list):
            req_url += "user_id=" + str(user_id) + "&"
            nb_id += 1

            # Max 100 id per request
            if nb_id == 100 or index == len(streamer_id_list) - 1:
                future_list.append(thread_pool.submit(_request_get_api, req_url))
                req_url = base_req_url
                nb_id = 0

    for future in future_list:
        resp_json = future.result()
        for stream in resp_json["data"]:
            stream_list.append(parse_data_stream(stream, resp_json["response_datetime"]))

    return stream_list


def _fetch_game_title(game_id: int, game_title: str) -> bool:
    game_row = Game.get_game(game_id)
    # Check if game is in database or changed
    if game_row is None or game_row.game_title != game_title:
        game = get_game(game_id)
        if game is None:
            return False
        if game_row is None:
            Game.insert_game(**vars(game))
        else:
            Game.update_game(**vars(game))

    return True

def parse_game_data(game_data: dict) -> GameAPI:
    return GameAPI(game_id=int(game_data["id"]),
                   game_title=game_data["name"],
                   box_art_url=game_data["box_art_url"],
                   )


def get_game(game_id: int) -> typing.Union[GameAPI, None]:
    # Reference: https://dev.twitch.tv/docs/api/reference#get-games
    req_url = "https://api.twitch.tv/helix/games?id=" + str(game_id)
    resp_json = _request_get_api(req_url)

    if len(resp_json["data"]) == 1:
        return parse_game_data(resp_json["data"][0])
    else:
        return None


def parse_data_users(user_data: dict) -> UserAPI:
    return UserAPI(streamer_id=int(user_data["id"]),
                   login_name=user_data["login"],
                   display_name=user_data["display_name"],
                   broadcaster_type=user_data["broadcaster_type"] if user_data["broadcaster_type"] != "" else None,
                   description=user_data["description"] if user_data["description"] != "" else None,
                   profile_image=user_data["profile_image_url"],
                   offline_image=user_data["offline_image_url"] if user_data["offline_image_url"] != "" else None,
                   )


def get_many_users(user_id_list: list) -> list:
    # Reference: https://dev.twitch.tv/docs/api/reference#get-users
    future_list = list()
    user_list = list()
    base_req_url = "https://api.twitch.tv/helix/users?"

    req_url = base_req_url
    nb_id = 0

    with ThreadPoolExecutor(max_workers=16) as thread_pool:
        for index, user_id in enumerate(user_id_list):
            req_url += "id=" + str(user_id) + "&"
            nb_id += 1

            # Max 100 id per request
            if nb_id == 100 or index == len(user_id_list) - 1:
                future_list.append(thread_pool.submit(_request_get_api, req_url))

                req_url = base_req_url
                nb_id = 0

    for future in future_list:
        resp_json = future.result()
        for user in resp_json["data"]:
            user_list.append(parse_data_users(user))

    return user_list
