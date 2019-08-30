import os
from logging import getLogger
import select


CONFIG_FILENAME = '/root/talker/conf.json'
REBOOT_FILENAME = '/root/talker/reboot.id'
EXCEPTION_FILENAME = '/root/talker/last_exception'
VERSION_FILENAME = '/root/talker/version'
JOBS_DIR = '/root/talker/jobs'
JOBS_SEEN = os.path.join(JOBS_DIR, 'eos.json')

logger = getLogger()

CYCLE_DURATION_MS = 100.0
CYCLE_DURATION = CYCLE_DURATION_MS / 1000.0
READ_ONLY = select.POLLIN | select.POLLPRI | select.POLLHUP | select.POLLERR

SIMULATE_DROPS_RATE = 0.0  # set a float (0.0 - 1.0), that will cause the server to simulate TalkerCommandLost

FINAL_RESULT_EXPIRATION = 3600

# let command output expire after some time.
# we assume that any output a command write is read shortly after, and if it isn't - it is not needed, except for troubleshooting
CMD_OUTPUT_EXPIRATION = 3600 * 24

MAX_OUTPUT_PER_CHANNEL = 10 * 2 ** 20
DEFAULT_GRACEFUL_TIMEOUT = 3

JOBS_EXPIRATION = 15  # 20 * 60  # how long to keep job ids in the EOS registry (exactly-once-semantics)

