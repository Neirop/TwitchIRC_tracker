import time
import typing
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

MAX_RETRY_REQUESTS = 10


def convert_str_date_to_datetime(str_date: str) -> datetime:
    return datetime.strptime(str_date[:19], "%Y-%m-%dT%H:%M:%S")


def convert_str_date_to_timestamp(str_date: str) -> float:
    return datetime.strptime(str_date[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


def sleep_until(end_datetime: datetime):
    while True:
        diff = (end_datetime - datetime.now()).total_seconds()
        if diff < 0:
            return
        time.sleep(diff / 2)
        if diff <= 0.1:
            return


def _create_session() -> requests.Session:
    session = requests.Session()
    # TODO Catch potential exception
    retry = Retry(total=MAX_RETRY_REQUESTS, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session


def request_get(url: str, headers: dict = None) -> requests.Response:
    return _create_session().get(url, headers=headers)


def request_post(url: str, headers: dict = None, data: str = None) -> requests.Response:
    return _create_session().post(url, headers=headers, data=data)


def get_external_ip() -> typing.Union[str, None]:
    response = request_get("https://ipinfo.io/ip")
    if response.status_code != 200:
        print("Error to get IP: %d", response.status_code)
        return None

    ip = response.text.replace("\n", "")
    return ip
