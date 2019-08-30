import json
import logging
import os
import shutil
import sys
import time
from logging.handlers import RotatingFileHandler
from textwrap import dedent

from talker_agent.config import logger, CONFIG_FILENAME

FILE_LOG_HANDLER = None

PY3 = sys.version_info[0] == 3

# ===========================================================================================
# Define a python2/3 compatible 'reraise' function for re-raising exceptions properly
# Since the syntax is different and would not compile between versions, we need to using 'exec'

if PY3:
    def reraise(tp, value, tb=None):
        if value is None:
            value = tp()
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value
else:
    def exec_(_code_, _globs_=None, _locs_=None):
        """Execute code in a namespace."""
        if _globs_ is None:
            frame = sys._getframe(1)
            _globs_ = frame.f_globals
            if _locs_ is None:
                _locs_ = frame.f_locals
            del frame
        elif _locs_ is None:
            _locs_ = _globs_
        exec("""exec _code_ in _globs_, _locs_""")


    exec_(dedent("""
        def reraise(tp, value, tb=None):
            raise tp, value, tb
        """))


# ===========================================================================================


def set_logging_to_file(logpath):
    global FILE_LOG_HANDLER
    if FILE_LOG_HANDLER:
        prev_logpath = FILE_LOG_HANDLER.stream.name
        logger.info("closing %s; switched to %s", prev_logpath, logpath)
        logging.root.removeHandler(FILE_LOG_HANDLER)
        FILE_LOG_HANDLER.close()
        shutil.move(prev_logpath, logpath)

    dirname = os.path.dirname(logpath)
    for i in range(60):
        if os.path.isdir(dirname):
            break
        logger.info("%s not found, waiting to be mounted...", dirname)
        time.sleep(2)
    else:
        raise Exception("logpath directory not found: %s" % dirname)

    handler = RotatingFileHandler(logpath, encoding='utf-8', maxBytes=50 * 1024 * 1024, backupCount=2)
    handler.setFormatter(
        logging.Formatter("%(asctime)s|%(threadName)-15s|%(name)-38s|%(levelname)-5s|%(funcName)-15s|%(message)s"))
    logging.root.addHandler(handler)

    FILE_LOG_HANDLER = handler

    with open(CONFIG_FILENAME, 'r') as f:
        config = json.load(f)

    with open(CONFIG_FILENAME, 'w') as f:
        config.update(logpath=logpath)
        json.dump(config, f)
