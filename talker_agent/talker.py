#!/usr/bin/python


"""
Talker Agent (Server-Side)
==========================

* Important:
    - keep this free of dependencies (there's only a redis dependency)
    - keep this compatible with python2.6+ (no dict comprehension)

* Packaging:
    - update the 'TALKER' version in version_info
    - ./teka pack talker

* Testing:
    - See ./wepy/devops/talker.py (client-side)

"""

import atexit
import json
import logging
import os
import os.path
import subprocess
import sys

from talker_agent.agent import TalkerAgent
from talker_agent.config import logger, EXCEPTION_FILENAME, CONFIG_FILENAME, VERSION_FILENAME
from talker_agent.utils import set_logging_to_file


def handle_exception(*exc_info):
    if issubclass(exc_info[0], KeyboardInterrupt):
        sys.__excepthook__(*exc_info)
        return
    logger.error("Uncaught exception", exc_info=exc_info)


def setup_logging(verbose):
    sys.excepthook = handle_exception
    for handler in logging.root.handlers:
        logging.root.removeHandler(handler)

    logging.root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s|%(levelname)-5s|%(funcName)-15s|%(message)s", datefmt="%H:%M:%S"))
    if not verbose:
        handler.setLevel(logging.INFO)
    logging.root.addHandler(handler)


def load_configuration():
    try:
        with open(CONFIG_FILENAME, 'r') as f:
            config = json.load(f)
    except (IOError, OSError):
        logger.warning("No configuration, exiting")
        sys.exit(1)

    logger.info("Loaded configuration: %s", CONFIG_FILENAME)
    for p in sorted(config.items()):
        logger.info("   %s: %s", *p)
    return config


def main(*args):
    setup_logging(verbose="-v" in args)
    no_restart = "--no-restart" in args

    version = open(VERSION_FILENAME).read()
    logger.info("Starting Talker: %s", version)

    config = load_configuration()
    set_logging_to_file(config.pop("logpath", "/var/log/talker.log"))

    # to help with WEKAPP-74054
    os.system("df")
    os.system("df -i")

    open("/var/run/talker.pid", "w").write(str(os.getpid()))
    atexit.register(os.unlink, "/var/run/talker.pid")

    try:
        agent = TalkerAgent()
        agent.setup(**config)
        agent.start()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        logger.info("(user interrupted)")
        return 2
    except Exception as exc:
        from traceback import format_exception
        try:
            with open(EXCEPTION_FILENAME, "w") as f:
                f.writelines(format_exception(*sys.exc_info()))
        except:  # noqa
            pass
        if not no_restart:
            logger.error("Uncaught exception - committing suicide, and restarting in 3 seconds")
            subprocess.Popen(["bash", "-ce",
                              'sleep 3 && service talker restart'])  # make sure this matches the pattern in 'install_talker()'
        raise
    else:
        if not no_restart:
            logger.info("Restarting in 1 seconds")
            subprocess.Popen(["bash", "-ce",
                              'sleep 1 && service talker restart'])  # make sure this matches the pattern in 'install_talker()'


if __name__ == '__main__':
    args = sys.argv[1:]
    if "--ut" in args:
        print("Talker don't need no UT")
    else:
        sys.exit(main(*args))
