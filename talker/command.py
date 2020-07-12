import time
import json
import logging
from uuid import uuid4
from contextlib import ExitStack

from easypy.units import DAY, HOUR, Duration
from easypy.logging import log_context
from easypy.humanize import compact
from easypy.properties import safe_property
from easypy.timing import Timer

from talker.config import (
    _logger, _verbose_logger, get_logger,
    MAX_OUTPUT_PER_CHANNEL, JOB_PID_TIMEOUT, AGENT_SEND_TIMEOUT,
    AGENT_ACK_TIMEOUT, TALKER_COMMAND_LOST_RETRY_ATTEMPTS, COMMANDS_KEY_TIMEOUT
)
from talker.errors import (
    CommandTimeout, UnknownChannel, TalkerCommandLost, HostDidNotRecover, HostStillAlive,
    TalkerClientSendTimeout, TalkerServerTimeout, ClientCommandTimeoutError, CommandTimeoutError,
    CommandAbortedByReboot, CommandAbortedByOverflow, CommandOrphaned,
    CommandLineTimeout, CommandExecutionError, UnknownExitCode, TalkerError, CommandPidTimeoutError, CommandAlreadyDone
)


class AbortedBy:
    timeout = 'timeout'
    reboot = 'reboot'
    overflowed = 'overflowed'
    orphaned = 'orphaned'
    error = 'error'
    hanging = 'hanging'
    line_timeout = 'line_timeout'


class Cmd(object):
    """
    A class to represent a command to run on a host

    :param [str] host_id: Host ID to run the command on
    :param [TalkerClient] talker: The Talker client that is used to send the command
    :param [bool] raise_on_failure: Should the command raise an exception on failure
    :param [Tuple[int]] retcode: What return codes are considered success
    :param [List[str]] args: List of the command arguments
    :param [int, float] ack_timeout: Agent ack timeout in seconds
    :param [float, int] timeout: Command timeout in seconds
    :param [float, int] server_timeout: Command timeout on the agent in seconds
    :param [float, int] line_timeout: Command output timeout in seconds
    :param [str] log_file: Log file path to write command output to in the host
    :param [str] name: Command name for logging purposes
    :param [int] max_output_per_channel: Maximum size of the command output to generate in bytes
    :param [str] set_new_logpath: Set a new path for the Talker agent log file
    """

    def __init__(self,
                 host_id,
                 talker,
                 raise_on_failure=True,
                 retcode=(0,),
                 args=None,
                 ack_timeout=AGENT_ACK_TIMEOUT,
                 timeout=HOUR,
                 server_timeout=True,
                 line_timeout=None,
                 log_file=None,
                 name=None,
                 max_output_per_channel=None,
                 set_new_logpath=None,
                 ):
        self.name = name
        self.job_id = str(uuid4())
        self.talker = talker
        self.retcode = None
        self.aborted_by = None
        self.ack = None
        self.stdout = []
        self.stderr = []
        self.raise_on_failure = raise_on_failure
        self.good_codes = retcode
        self.args = args
        self.host_id = host_id
        self.hostname = host_id.partition('.')[-1]
        self.ack_timer = None
        self.handling_timer = None
        self.ack_timeout = ack_timeout or AGENT_ACK_TIMEOUT
        self.timeout = timeout or HOUR  # No command should run over hour, unless specified explicitly
        self.server_timeout = server_timeout
        self.line_timeout = Duration(line_timeout) if line_timeout else None
        self.log_file = log_file
        self.max_output_per_channel = max_output_per_channel or MAX_OUTPUT_PER_CHANNEL
        self.set_new_logpath = set_new_logpath
        self.client_timer = Timer(expiration=self.timeout if not server_timeout else self.timeout + 5)
        self._line_timeout_synced = False
        self.attempts = 0  # in case of TalkerCommandLost

    def __repr__(self):
        return "%s(<%s>%s)" % (self.__class__.__name__, self.job_id, "!" if not self.ack else "")

    @property
    def _result_key(self):
        return "result-%s" % self.job_id

    @property
    def _ack_key(self):
        return "%s-ack" % self._result_key

    @property
    def _stdout_key(self):
        return "%s-stdout" % self._result_key

    @property
    def _stderr_key(self):
        return "%s-stderr" % self._result_key

    @property
    def _exit_code_key(self):
        return "%s-retcode" % self._result_key

    @safe_property
    def _pid_key(self):
        return "%s-pid" % self._result_key

    @property
    def _commands_key(self):
        return "commands-%s" % self.host_id

    @property
    def timed_out(self):
        """
        Is this command timed out

        :rtype: bool
        """
        return self.aborted_by == AbortedBy.timeout

    @property
    def since_started(self):
        """
        Get the time since the command acked

        :rtype: Duration
        """
        return Duration(time.time() - self.ack) if self.ack else None

    def get_pid(self, timeout=JOB_PID_TIMEOUT, wait_for_ack=True):
        """
        Get the command process ID

        :param [int, float] timeout: After how many seconds should we throw a timeout error
        :param [bool] wait_for_ack: Wait for ack before checking for the command PID

        :returns: The command PID
        :rtype: int
        """
        if wait_for_ack:
            self.wait(for_ack=True)

        res = self.talker.reactor.get(self._pid_key, timeout=timeout, _cmd_id=self.job_id)
        if res is None:
            if self.retcode is not None:
                raise CommandAlreadyDone()
            else:
                raise CommandPidTimeoutError('redis timeout after {} when trying to get pid'.format(timeout))

        return int(res)

    def put_command(self, *, job_repr, **job_data):
        """
        Put the command in the commands queue
        For internal use

        :param [str] job_repr: A string to print the command for logging purposes
        :param [dict] job_data: Command data to send to the Talker agent
        """
        get_logger().debug("submitting job [%s] (%s)", job_repr, self.job_id)
        self.talker.reactor.rpush(self._commands_key, json.dumps(job_data), _callback=self.on_sent, _cmd_id=self.job_id)
        # making expiration long enough so it's unlikely to expire in the middle pushing a new command - see WEKAPP-86513
        # a 5m expiration could fall exactly when we push a new command, causing it to expire before the agent pops it out;
        # a 1d expiration is long enough since new commands are likely to be pushed in the interim, extending the expiration
        # we can't call 'expire' before 'rpush', since it will be a no-op if the key doesn't exist yet
        self.talker.reactor.expire(self._commands_key, COMMANDS_KEY_TIMEOUT, _cmd_id=self.job_id)
        self.handling_timer = Timer(expiration=AGENT_SEND_TIMEOUT)

    @log_context(host="{.hostname}")
    def send(self):
        """
        Send the command to the Talker agent on the host
        """
        args = [str(a) for a in self.args]
        command_string = ' '.join(x if ' ' not in x else '"%s"' % x for x in args)
        timeout = self.timeout if self.server_timeout else None

        extras = {}
        extras.update(
            max_output_per_channel=self.max_output_per_channel,
            set_new_logpath=self.set_new_logpath,
            line_timeout=self.line_timeout,
            log_file=self.log_file,
        )

        self.put_command(id=self.job_id, cmd=args, timeout=timeout, job_repr=compact(command_string, 100), **extras)

    def on_sent(self):
        if not self.ack_timer:
            self.ack_timer = Timer(expiration=self.ack_timeout)  # this is expiration on the agent ack'ing the command

    @property
    def is_sent(self):
        """
        Is the command sent to the host

        :rtype: bool
        """
        return bool(self.ack_timer)

    @log_context(host="{.hostname}")
    def reset_server_timeout(self):
        """
        Reset timeout on the Talker agent for this command
        """
        _logger.debug("Reseting timeout, %s, %s", self.job_id, self.timeout)
        self.client_timer.reset()
        self.talker.reactor.rpush(self._commands_key, json.dumps(dict(
            id=self.job_id,
            cmd="reset_timeout",
            new_timeout=self.timeout
        )), _cmd_id=self.job_id)
        self.talker.reactor.expire(self._commands_key, COMMANDS_KEY_TIMEOUT, _cmd_id=self.job_id)

    @log_context(host="{.hostname}")
    def send_signal(self, sig):
        """
        Send a signal for the process running te command

        :param [int] sig: The signal to send to the command
        """
        self.talker.reactor.rpush(self._commands_key, json.dumps(dict(
            id=self.job_id,
            cmd="signal",
            signal=sig,
        )), _cmd_id=self.job_id)
        self.talker.reactor.expire(self._commands_key, COMMANDS_KEY_TIMEOUT, _cmd_id=self.job_id)

    @log_context(host="{.hostname}")
    def kill(self, graceful_timeout=3):
        """
        Kill the process running the command

        This will send a SIGTERM signal to the process, wait for graceful_timeout seconds,
        check if the process died and if not, send the SIGKILL signal.

        :param [int, float] graceful_timeout: Seconds to wait between the SIGTERM and SIGKILL signals
        """
        self.talker.reactor.rpush(self._commands_key, json.dumps(dict(
            id=self.job_id,
            cmd="kill",
            graceful_timeout=graceful_timeout,
        )), _cmd_id=self.job_id)
        self.talker.reactor.expire(self._commands_key, COMMANDS_KEY_TIMEOUT, _cmd_id=self.job_id)

    def _sync_timeout(self, line_timeout):
        if self._line_timeout_synced:
            return
        else:
            self._line_timeout_synced = True

        if line_timeout and line_timeout > self.timeout:
            self.timeout = int(line_timeout * 1.15)
            self.reset_server_timeout()

    def iter_results(self, line_timeout=DAY, timeout=DAY):
        """
        Iterate return code and output results

        :param [int, float] line_timeout: Timeout for new output to be recieved in seconds
        :param [int, float] timeout: Timout for command to finish in seconds

        :returns: A Tuple with the channel name and value. Channel names are `retcode`, `stdout` and `stderr`
        :rtype: Tuple[str, str]
        """
        timeout = timeout or DAY
        line_timeout = line_timeout or DAY
        blpop_timeout = min(line_timeout, timeout // 10, self.timeout // 10, AGENT_ACK_TIMEOUT // 10)

        session_timer = Timer(expiration=timeout)  # timeout for this invocation (not entire command duration)
        line_timer = Timer(expiration=line_timeout)
        timeout_reset_timer = Timer(expiration=max(line_timeout // 10, 1))
        self._sync_timeout(line_timeout=line_timeout)

        while self.retcode is None:

            if line_timer.expired:
                self.raise_exception(exception_cls=CommandLineTimeout, line_timeout=Duration(line_timeout))

            if session_timer.expired:
                self.raise_exception(exception_cls=CommandTimeout, timeout=timeout)

            channels = {
                self._ack_key: 'ack', self._stderr_key: 'stderr',
                self._stdout_key: 'stdout', self._exit_code_key: 'retcode'
            }
            res = self.talker.reactor.blpop(
                [self._ack_key, self._stderr_key, self._stdout_key, self._exit_code_key],
                timeout=blpop_timeout, _cmd_id=self.job_id
            )
            if not res:
                if not self.ack:
                    # no point checking these timers timeout if the command hasn't been acknowledged yet
                    line_timer.reset()
                    session_timer.reset()

                self.check_client_timeout()
                continue
            else:
                channel_name, value = res
                channel_name = channels[channel_name.decode('utf-8')]

            line_timer.reset()
            if timeout_reset_timer.expired:
                timeout_reset_timer.reset()
                self.reset_server_timeout()

            if channel_name == 'stderr':
                self.on_output(self.stderr, (value,))
            elif channel_name == 'stdout':
                self.on_output(self.stdout, (value,))
            elif channel_name == 'retcode':
                # see if any leftovers after receiving exit code
                stdout, stderr = self.get_output()
                for data_channel_name, values in (('stdout', stdout), ('stderr', stderr)):
                    for data_value in values:
                        yield (data_channel_name, data_value)
                value = self.set_retcode(value)
            elif channel_name == 'ack':
                self.on_ack(value)
                continue

            yield (channel_name, value)

            self.raise_if_needed()

    def iter_lines(self, line_timeout=DAY, timeout=DAY):
        """
        Iterate command output

        :param [int, float] line_timeout: Timeout for new output to be recieved in seconds
        :param [int, float] timeout: Timout for command to finish in seconds

        :returns: A Tuple containing a line from stdout or None and a line from stderr or None
        :rtype: Tuple[str, str]
        """
        done = False
        parts = {
            'stdout': '',
            'stderr': ''
        }

        def return_for_channel(channel, line):
            if channel == 'stdout':
                return (line, None)
            elif channel == 'stderr':
                return (None, line)

        def yield_channel(channel):
            lines = parts[channel].splitlines(keepends=True)
            parts[channel] = ''
            for line in lines:
                if line.endswith('\n'):
                    yield return_for_channel(channel, line.strip('\n'))
                else:
                    parts[channel] = line
            if done:
                yield return_for_channel(channel, parts[channel])

        for channel, value in self.iter_results(line_timeout=line_timeout, timeout=timeout):
            if channel in ['stdout', 'stderr']:
                try:
                    parts[channel] += value.decode('utf-8')
                except UnicodeDecodeError:
                    _logger.warning("Unable to decode %s to utf-8!", value)
                    parts[channel] += "%s" % value
                yield from yield_channel(channel)
            elif channel == 'retcode':
                done = True

    @log_context(host="{.hostname}")
    def log_pipe(self, logger, stdout_lvl=logging.INFO, stderr_lvl=logging.ERROR, line_timeout=DAY):
        """
        Pipe command output to a logger

        :param [logging.Logger] logger: The logger to use to log command output
        :param [int] stdout_lvl: Logging level to use in order to log stdout
        :param [int] stderr_lvl: Logging level to use in order to log stderr
        :param [int, float] line_timeout: Timeout for new output to be recieved in seconds
        """
        for stdout, stderr in self.iter_lines(line_timeout=line_timeout):
            if stdout_lvl is not None and stdout:
                logger.log(stdout_lvl, stdout)
            if stderr_lvl is not None and stderr:
                logger.log(stderr_lvl, stderr)

    @log_context(host="{.hostname}")
    def foreground(self, stdout=True, stderr=True):
        """
        Print command output as it is recieved from the agent

        :param [bool] stdout: Should stdout be printed
        :param [bool] stderr: Should stderr be printed

        :returns: The command return code when done
        :rtype: int
        """
        should_print = dict(stdout=stdout, stderr=stderr)
        for channel, value in self.iter_results():
            if channel in ['stdout', 'stderr'] and should_print[channel]:
                print(value.decode('utf-8'), end='')
            elif channel == 'retcode':
                return self.retcode
            else:
                self.raise_exception(exception_cls=UnknownChannel, channel=channel)

    def check_client_timeout(self):
        """
        Check if the command has timed out

        This will raise an appropriate exception depends on what has timed out:
        * If the reactor haven't sent the command before the handling timer expired: TalkerClientSendTimeout
        * If the command has yet to be acked and the agent is alive: TalkerCommandLost
        * If the command has yet to be acked and the agent is not responding: TalkerServerTimeout
        * If ack is not supported and the command has expired: ClientCommandTimeoutError
        """
        if not self.is_sent:
            # client/reactor did not send command yet
            if self.handling_timer.expired:
                self.raise_exception(exception_cls=TalkerClientSendTimeout, timeout=self.handling_timer.elapsed)
        elif not self.ack:
            is_talker_alive_name = 'is_talker_alive'

            # client did not receive command - check all timers
            if self.ack_timer.expired or self.client_timer.expired:
                talker_alive = False
                if self.name != is_talker_alive_name:  # prevent recursion
                    if self.talker.is_alive(self.host_id, is_talker_alive_name):
                        # check again, since talker may have just booted
                        self.poll(check_client_timeout=False)
                        if self.ack:
                            # seems like our command was finally answered
                            _logger.debug("we thought TalkerCommandLost, but finally not so... (%s)", self)
                            return
                        talker_alive = True
                        if self.attempts < TALKER_COMMAND_LOST_RETRY_ATTEMPTS:
                            self.attempts += 1
                            self.ack_timer = None
                            self.send()
                            return

                exception_cls = TalkerCommandLost if talker_alive else TalkerServerTimeout
                self.raise_exception(exception_cls=exception_cls, timeout=self.ack_timer.elapsed)
        elif self.client_timer.expired:
            self.raise_exception(
                exception_cls=ClientCommandTimeoutError, timeout=self.ack_timer.elapsed, started=self.since_started
            )

    def raise_if_needed(self):
        """
        Raise an error for the command failure. If the command succeeded, do nothing
        """
        if self.aborted_by == AbortedBy.timeout:
            self.raise_exception(exception_cls=CommandTimeoutError, timeout=Duration(self.timeout))
        if self.aborted_by == AbortedBy.reboot:
            self.raise_exception(exception_cls=CommandAbortedByReboot)
        if self.aborted_by == AbortedBy.overflowed:
            self.raise_exception(exception_cls=CommandAbortedByOverflow, max_output_per_channel=self.max_output_per_channel)
        if self.aborted_by == AbortedBy.orphaned:
            self.raise_exception(exception_cls=CommandOrphaned)
        if self.aborted_by == AbortedBy.line_timeout:
            self.raise_exception(exception_cls=CommandLineTimeout, line_timeout=self.line_timeout)
        if self.aborted_by == AbortedBy.error:
            _, traceback = self.get_output(new=False)
            traceback = self._decode_output(traceback, safe=True).strip()
            exception = traceback.splitlines()[-1]
            raise TalkerError(
                host_id=self.host_id, hostname=self.hostname, fulltb=traceback, talker=self.talker, _exception=exception
            )
        if self.retcode is None:
            return
        if self.raise_on_failure and self.retcode not in self.good_codes:
            self.raise_exception()

    def raise_exception(self, exception_cls=CommandExecutionError, **params):
        """
        Raise an exception for this command
        """

        def process_output(d):
            text = self._decode_output(d, safe=True)
            if len(text) > 1010:
                text = "...%s" % text[-1000:]
            return text

        log_files = self.log_file if isinstance(self.log_file, (tuple, list)) else [self.log_file] * 2
        stds = [None] * 2 if all(log_files) else (process_output(std) for std in self.get_output(new=False))
        for (param, std, log_file) in zip("stdout stderr".split(), stds, log_files):
            params[param] = ("<%s>" % log_file) if log_file else std

        raise exception_cls(
            cmd=self.job_id,
            name=self.name or "(unnamed command)",
            args=" ".join(map(str, self.args)),
            host_id=self.host_id,
            hostname=self.hostname,
            retcode=self.retcode,
            since_started=self.since_started,
            talker=self.talker,
            **params
        )

    def on_polled(self, exit_code):
        """
        Report return code for the command 

        :param [int] exit_code: The command return code

        :returns: The command return code or None if not yet set
        :rtype: int
        """
        if self.retcode is not None:
            return self.retcode
        if exit_code is None:
            return
        _verbose_logger.debug("%s: exit-code received (%s)", self, exit_code)
        self.set_retcode(exit_code)
        return self.retcode

    def poll(self, check_client_timeout=True):
        """
        Check if the command is finished and return it's return code

        :param [bool] check_client_timeout: Also check if the command timed out

        :returns: The command return code if done
        :rtype: int
        """
        if self.retcode is not None:
            return self.retcode

        if not self.ack:
            ack = self.talker.reactor.lpop(self._ack_key, _cmd_id=self.job_id)
            if ack is None:
                if check_client_timeout:
                    self.check_client_timeout()
                return
            self.on_ack(ack)

        exit_code = self.talker.reactor.lpop(self._exit_code_key, _cmd_id=self.job_id)
        if exit_code is None:
            if check_client_timeout:
                self.check_client_timeout()
        return self.on_polled(exit_code)

    def set_retcode(self, exit_code):
        """
        Set the command return code

        :param [int] exit_code: The command return code
        """
        if exit_code is None:
            return
        try:
            exit_code = int(exit_code)
        except ValueError:
            exit_code = exit_code.decode('utf-8')

        self.on_sent()  # sometimes we get here before the callback is called
        self.ack_timer.stop()

        if isinstance(exit_code, int):
            self.retcode = exit_code
        elif hasattr(AbortedBy, exit_code):
            self.aborted_by = getattr(AbortedBy, exit_code)
            self.retcode = -1
        else:
            self.raise_exception(exception_cls=UnknownExitCode, exit_code=exit_code)
        self.raise_if_needed()

    def wait(self, for_ack=False):
        """
        Wait until the command is done

        :param [bool] for_ack: Wait for the command to be acked instead of command to finish

        :returns: The command return code when done
        :rtype: int
        """
        while True:

            if for_ack:
                if self.ack is not None:
                    return self.ack

            if self.retcode is not None:
                return self.retcode

            # we don't want to wait too long, cause we want to raise timeout exceptions promptly
            blpop_timeout = (
                1 if not self.is_sent
                else self.ack_timer.expiration // 10 if not self.ack
                else self.client_timer.expiration // 10)

            result = self.talker.reactor.blpop(
                [self._exit_code_key, self._ack_key], timeout=blpop_timeout, _cmd_id=self.job_id)
            if result is None:
                self.check_client_timeout()
                continue

            key, value = result
            key = key.decode()
            if key == self._ack_key:
                _verbose_logger.debug("%s: ack received (%s)", self, value)
                self.on_ack(value)
                continue

            return self.on_polled(result[1])

    def result(self, decode='utf8'):
        """
        Wait until the command is done and return it's stdout output

        :param [str] decode: Expected output encoding

        :returns: The command stdout output
        :rtype: str
        """
        self.wait()
        stdout, _ = self.get_output(new=False)
        return self._decode_output(stdout, decode=decode)

    def _decode_output(self, output, decode='utf8', safe=False):
        ret = b''.join(output)
        if decode:
            ret = ret.decode(decode, errors='replace' if safe else 'strict')
        return ret

    def _request_outputs(self, pipeline):
        pipeline.lrange(self._stdout_key, 0, -1)
        pipeline.lrange(self._stderr_key, 0, -1)

    def _fetch_outputs(self):
        with self.talker.reactor.pipeline() as p:
            self._request_outputs(p)
            new_stdout, new_stderr = p.execute()
        return new_stdout, new_stderr

    def _trim_outputs(self, stdout, stderr, pipeline=None):
        len_stdout = len(stdout)
        len_stderr = len(stderr)
        with ExitStack() as stack:
            if pipeline is not None:
                p = pipeline
            else:
                p = stack.enter_context(self.talker.reactor.pipeline())
            for data_len, redis_key in \
                    ((len_stdout, self._stdout_key),
                     (len_stderr, self._stderr_key)):
                if data_len:
                    p.ltrim(redis_key, data_len, -1)
            if pipeline is None:
                p.execute()

    def on_output(self, channel, data):
        """
        Update the command output

        :param [List[bytes]] channel: The channel to update. One of `self.stdout` or `self.stderr`
        :param [bytes] data: New data to add to one of the channels
        """
        if data:
            channel.extend(data)

    def on_ack(self, ack):
        """
        Update the command ack

        :param [bytes] ack: The command start time on the host
        """
        self.ack = float(ack.decode())

    def get_output(self, new=True):
        """
        Get raw command output

        :param [bool] new: Get only new output

        :returns: Stdout and stderr from the command
        :rtype: Tuple[bytes, bytes]
        """
        new_stdout, new_stderr = self._fetch_outputs()
        self.on_output(self.stdout, new_stdout)
        self.on_output(self.stderr, new_stderr)
        self._trim_outputs(stdout=new_stdout, stderr=new_stderr)
        if new:
            return new_stdout, new_stderr
        else:
            return self.stdout, self.stderr


class RebootCmd(Cmd):
    DEFAULT_FORCE = False

    """
    A command to handle host reboot

    :param [bool] force: Should reboot forcefully (might skip sync and other host cleanups)
    """

    def __init__(self, *args, force=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.force = RebootCmd.DEFAULT_FORCE if force is None else force
        self.args = ['reboot', 'force=%s' % force]

    def send(self):
        job_repr = "reboot %r, force=%s" % (self.host_id, self.force)
        self.put_command(id=self.job_id, job_repr=job_repr, cmd='reboot', force=self.force, timeout=None)

    def check_client_timeout(self):
        try:
            super().check_client_timeout()
        except ClientCommandTimeoutError as exc:
            raise HostDidNotRecover(**exc._params)

    def raise_if_needed(self):
        if self.retcode == 1:
            raise HostStillAlive(hostname=self.hostname, elapsed=self.ack_timer.elapsed)
        super().raise_if_needed()
