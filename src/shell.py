import asyncio
import cmd
import os
import re
import signal
import typing

import global_data


class Shell(cmd.Cmd):
    intro = "Shell to interact and print some values in runtime. Type help or ? to list commands.\n"
    prompt = "shell# "

    def __init__(self, stream_tracker, irc_handler):
        super().__init__()
        self.stream_tracker = stream_tracker
        self.irc_handler = irc_handler

    def emptyline(self):
        pass

    def do_api_access_token(self, args):
        """usage: api_access_token
        Print API access token and remaining time"""
        print("API app access token: {}".format(global_data.API_APP_ACCESS_TOKEN))

    def do_irc_client_stats(self, args):
        """usage: irc_client_stats [CLIENT_NUMBER]
        Print IRC client information and statistics, specify client number for detailed statistics"""
        args = self.parse_args(args, int, 0, 1)
        if args is None:
            return
        elif args:
            print(asyncio.run_coroutine_threadsafe(self.irc_handler.get_info_stats(args[0]),
                                                   self.irc_handler.event_loop).result())
        else:
            print(asyncio.run_coroutine_threadsafe(self.irc_handler.get_info_stats(),
                                                   self.irc_handler.event_loop).result())

    def do_irc_client_stats_reset(self, args):
        """usage: irc_client_stats_reset
        Reset IRC clients statistics"""
        asyncio.run_coroutine_threadsafe(self.irc_handler.reset_stats(),
                                         self.irc_handler.event_loop).result()
        print("IRC stats reset")

    def do_max_streamers_tracked(self, args):
        """usage: max_streamers_tracked [NB_MAX_STREAMER]
        Print max number of streamer tracked or change it"""
        args = self.parse_args(args, int, 0, 1)
        if args is None:
            return
        elif args:
            global_data.ARGS.MAX_STREAMERS_TRACKED = args[0]

        print("Max number of streamer tracked: {}".format(global_data.ARGS.MAX_STREAMERS_TRACKED))

    def do_response_handler_stats(self, args):
        """usage: response_handler_stats [-v]
        Print statistics about response handler"""
        args = self.parse_args(args, str, 0, 1)
        if args is None:
            return
        print(self.irc_handler.response_handler.get_stats(args[0] == "-v" if args else False))

    def do_track_streamer(self, args):
        """usage: track_streamer STREAMER_LOGIN_NAME [STREAMER_LOGIN_NAME ...]
                Track new streamers (not affected by the limit number of streamer tracked)"""
        args = self.parse_args(args, str, 1)
        login_list = list()
        for login in args:
            if re.match(r'^[\w]*$', login) is not None:
                login_list.append(login)
            else:
                print("[{}] is an invalid login name".format(login))

        already_tracked_list, no_exist_list = asyncio.run_coroutine_threadsafe(
            self.stream_tracker.track_streamers(login_list), self.stream_tracker.event_loop).result()
        for login in login_list:
            if login in already_tracked_list:
                print("[{}] is already tracked".format(login))
            elif login in no_exist_list:
                print("[{}] doesn't exist".format(login))
            else:
                print("[{}] tracked".format(login))

    def do_exit(self, args):
        """usage: exit
        Exit the program"""
        os.kill(os.getpid(), signal.SIGTERM)

    def do_EOF(self, args):
        print("Use exit command or CTRL-C to exit program")

    @staticmethod
    def parse_args(args: str, arg_type: type, nb_args_min: int, nb_args_max: int = None) -> typing.Union[tuple, None]:
        s = args.split()
        if len(s) < nb_args_min:
            print("Error: too few arguments (provided={0}, min_expected={1})".format(len(s), nb_args_min))
            return None
        if nb_args_max is not None and len(s) > nb_args_max:
            print("Error: too many arguments (provided={0}, max_expected={1})".format(len(s), nb_args_max))
            return None

        try:
            return tuple(map(arg_type, s))
        except:
            print("Error: arguments type expected are {}".format(arg_type.__name__))
            return None
