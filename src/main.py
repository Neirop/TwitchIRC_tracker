import logging
import os
import signal
import threading
import time

import database_model
import global_data
import shell
import stream_tracker
import twitch_irc

LOG_FILENAME = "twitchIRC_tracker.log"


def main():
    global_data.parse_args()

    # Set logging
    root_logger = logging.getLogger()
    root_logger.setLevel(global_data.ARGS.LOG_LEVEL.upper())
    handler = logging.FileHandler(LOG_FILENAME, "a")
    handler.setFormatter(logging.Formatter(fmt="[%(levelname)s] %(asctime)s - %(name)s: %(message)s",
                                           datefmt="%m/%d/%y %H:%M:%S"))
    root_logger.addHandler(handler)

    # Retrieve API and database configs from cfg file
    global_data.parse_cfg_options()

    # Init and connect to database
    database_model.init_db(database_name=global_data.DATABASE_NAME,
                           host=global_data.DATABASE_HOST,
                           port=global_data.DATABASE_PORT,
                           user=global_data.DATABASE_USER,
                           password=global_data.DATABASE_PASSWORD)

    # Get an app access token and launch thread that will change token after expiration time
    token_ready = threading.Event()
    handle_token_thread = threading.Thread(target=global_data.get_app_access_token, args=(token_ready,))
    handle_token_thread.start()
    token_ready.wait()
    if global_data.API_APP_ACCESS_TOKEN == "":
        print("Error: Can't get API access token (check log)")
        os.kill(os.getpid(), signal.SIGTERM)

    # Create instances
    stream_tracker_ins = stream_tracker.StreamTracker()
    irc_handler_ins = twitch_irc.IRCHandler()

    # Launch threads
    stream_tracker_thread = threading.Thread(target=stream_tracker_ins.start, name="stream_tracker")
    stream_tracker_thread.start()

    irc_handler_thread = threading.Thread(target=irc_handler_ins.start_loop, name="irc_handler")
    irc_handler_thread.start()

    while True:
        try:
            shell.Shell(irc_handler_ins).cmdloop()
        except KeyboardInterrupt:
            try:
                print("\nCTRL-C caught, press CTRL-C a second time to exit program")
                time.sleep(0.5)
            except KeyboardInterrupt:
                os.kill(os.getpid(), signal.SIGTERM)


if __name__ == "__main__":
    main()
