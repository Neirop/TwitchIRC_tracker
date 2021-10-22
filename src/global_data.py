import argparse
import configparser
import logging
import os
import threading
from datetime import datetime, timedelta

import utils

ARGS: argparse.Namespace

# API_auth
API_CLIENT_ID: str = ""
API_CLIENT_SECRET: str = ""
API_APP_ACCESS_TOKEN: str = ""

# Database config
DATABASE_HOST: str = ""
DATABASE_PORT: int = 0
DATABASE_USER: str = ""
DATABASE_PASSWORD: str = ""
DATABASE_NAME: str = ""

LOGGER = logging.getLogger(__name__)


def parse_args():
    global ARGS

    parser = argparse.ArgumentParser(description="Collect twitch channels and chat statistics",
                                     formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("-c", "--config-file", type=os.path.abspath,
                        default="config.cfg",
                        dest="CONFIG_FILE",
                        help="Configuration file that contains API authentication and database access "
                             "configuration")
    parser.add_argument("-l", "--log-level", type=str,
                        choices=["debug", "info", "warning", "error"],
                        default="info",
                        dest="LOG_LEVEL",
                        help="Define logging level")
    parser.add_argument("--max-streamers-tracked", type=int,
                        default=5000,
                        dest="MAX_STREAMERS_TRACKED",
                        help="Maximum number of streamer tracked")
    # parser.add_argument("--debug-irc-usernotice", type=str,
    #                     default=None,
    #                     dest="DEBUG_IRC_USERNOTICE",
    #                     help="Print USERNOTICE IRC response to file for debugging purpose")
    # automatic fetch new streams option
    ARGS = parser.parse_args()


def parse_cfg_options():
    cfg = configparser.ConfigParser()
    retval = cfg.read(ARGS.CONFIG_FILE)
    if len(retval) == 0:
        print("Error: {} file doesn't exist or can't be read".format(ARGS.CONFIG_FILE))
        exit(-1)

    try:
        global API_CLIENT_ID, API_CLIENT_SECRET, \
            DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME

        API_CLIENT_ID = cfg.get("API_auth", "client_id")
        API_CLIENT_SECRET = cfg.get("API_auth", "client_secret")

        DATABASE_HOST = cfg.get("database", "host")
        DATABASE_PORT = cfg.get("database", "port")
        DATABASE_USER = cfg.get("database", "user")
        DATABASE_PASSWORD = cfg.get("database", "password")
        DATABASE_NAME = cfg.get("database", "database_name")

    except Exception as err:
        print("Error: {}".format(err))
        exit(-1)


def get_app_access_token(token_ready: threading.Event):
    global API_APP_ACCESS_TOKEN

    while True:
        url = "https://id.twitch.tv/oauth2/token?client_id=" + API_CLIENT_ID + "&client_secret=" \
              + API_CLIENT_SECRET + "&grant_type=client_credentials"
        req = utils.request_post(url)
        if req.status_code != 200:
            error_message = "no message"
            try:
                error_message = req.json()["message"]
            except (ValueError, KeyError):
                pass
            LOGGER.error("Error %d (%s) to get app access token", req.status_code, error_message)
            expire_datetime = datetime.now() + timedelta(seconds=5)
        else:
            req_json = req.json()
            API_APP_ACCESS_TOKEN = req_json["access_token"]
            # TODO Check API errors response
            # "error": "Unauthorized",
            # "status": 401,
            # "message": "Invalid OAuth token"

            # Refresh token 12 hours before expiration
            expire_datetime = datetime.now() + timedelta(seconds=req_json["expires_in"]) - timedelta(hours=12)
            LOGGER.info("App access token requested [%s], expires %s",
                        API_APP_ACCESS_TOKEN, expire_datetime)

        token_ready.set()

        # Wait until token expiration
        utils.sleep_until(expire_datetime)


def revoke_app_access_token(token: str):
    url = "https://id.twitch.tv/oauth2/revoke?client_id=" + API_CLIENT_ID + "&token=" + token
    req = utils.request_post(url)
    if req.status_code != 200:
        error_message = "no message"
        try:
            error_message = req.json()["message"]
        except (ValueError, KeyError):
            pass
        LOGGER.error("Error %d (%s) to revoke app access token", req.status_code, error_message)
