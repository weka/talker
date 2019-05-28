import logging

from easypy.units import MINUTE, SECOND, MiB
from easypy.threadtree import ThreadContexts


AGENT_SEND_TIMEOUT = 2 * MINUTE  # the duration we allow the client for sending our command to redis
AGENT_ACK_TIMEOUT = 20 * MINUTE  # the duration we allow the server/agent to receive and start our commands
MAX_OUTPUT_PER_CHANNEL = MiB * 10
# needs to be below AGENT_SEND_TIMEOUT, so that we don't confuse a redis timeout with a reactor timeout
REDIS_SOCKET_TIMEOUT = 1.5 * MINUTE
JOB_PID_TIMEOUT = 1 * SECOND
TALKER_COMMAND_LOST_RETRY_ATTEMPTS = 1

TALKER_CONTEXT = ThreadContexts(defaults=dict(talker_quiet=False, ack_timeout=AGENT_ACK_TIMEOUT))

_logger = logging.getLogger(__name__)
_verbose_logger = logging.getLogger(__name__ + ".verbose")


def get_logger():
    return _verbose_logger if TALKER_CONTEXT.talker_quiet else _logger


def ack_timeout(timeout):
    return TALKER_CONTEXT(ack_timeout=timeout)
