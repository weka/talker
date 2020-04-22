from datetime import timedelta
import logging

from easypy.units import MINUTE, SECOND, MiB
from easypy.threadtree import ThreadContexts


AGENT_SEND_TIMEOUT = 2 * MINUTE  # the duration we allow the client for sending our command to redis
AGENT_ACK_TIMEOUT = 20 * MINUTE  # the duration we allow the server/agent to receive and start our commands
IS_ALIVE_ACK_TIMEOUT = 5
IS_ALIVE_TIMEOUT = 5
MAX_OUTPUT_PER_CHANNEL = MiB * 10

# needs to be below AGENT_SEND_TIMEOUT, so that we don't confuse a redis timeout with a reactor timeout
REDIS_SOCKET_TIMEOUT = 120
REDIS_SOCKET_CONNECT_TIMEOUT = 60
REDIS_RETRY_ON_TIMEOUT = True
REDIS_HEALTH_CHECK_INTERVAL = 30

JOB_PID_TIMEOUT = 3 * SECOND
TALKER_COMMAND_LOST_RETRY_ATTEMPTS = 1
COMMANDS_KEY_TIMEOUT = timedelta(days=1)

TALKER_CONTEXT = ThreadContexts(defaults=dict(talker_quiet=False, ack_timeout=AGENT_ACK_TIMEOUT))

_logger = logging.getLogger(__name__)
_verbose_logger = logging.getLogger(__name__ + ".verbose")


def get_logger():
    return _verbose_logger if TALKER_CONTEXT.talker_quiet else _logger
