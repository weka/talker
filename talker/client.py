import json

from functools import partial

from redis import StrictRedis
import redis.exceptions

from easypy.caching import cached_property, locking_cache
from easypy.collections import chunkify
from easypy.concurrency import MultiObject
from easypy.timing import wait, BackoffTimer, Timer, TimeoutException
from easypy.units import DAY, HOUR, MINUTE
from easypy.humanize import compact

from talker.reactor import TalkerReactor
from talker.command import Cmd, RebootCmd
from talker.errors import RedisTimeoutError, TalkerServerTimeout, CommandTimeoutError, ClientCommandTimeoutError
from talker.config import (
    REDIS_SOCKET_TIMEOUT, REDIS_SOCKET_CONNECT_TIMEOUT, REDIS_RETRY_ON_TIMEOUT,
    REDIS_HEALTH_CHECK_INTERVAL, AGENT_ACK_TIMEOUT, _logger, _verbose_logger, IS_ALIVE_ACK_TIMEOUT, IS_ALIVE_TIMEOUT
)


@locking_cache
def get_redis(host, password, port):
    return StrictRedis(
        host=host, password=password, port=port,
        socket_timeout=REDIS_SOCKET_TIMEOUT,
        socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
        retry_on_timeout=REDIS_RETRY_ON_TIMEOUT,
        health_check_interval=REDIS_HEALTH_CHECK_INTERVAL
    )


@locking_cache
def get_talker(host, password, port, agent_version=None, name=None):
    return Talker(host, password, port, agent_version=agent_version, name=name)


class Talker(object):
    """
    The client to communicate with Talker agents via the same backend (Redis).

    :param [str] host: The backend host's address
    :param [str] password: The backend user's password
    :param [int] port: The backend port
    :param [str] user: The backend username
    :param [str] agent_version: The Talker agent version we are connecting to
    :param [str] name: Name to use for the Talker client (for logging purposes)
    """

    def __init__(self, host, password, port, agent_version, name):
        self._host = host
        self._password = password
        self._port = port
        self._name = name or "anon"
        self._journal_saved = False
        self.reactor = TalkerReactor(self)
        _logger.debug("%s: initialized", self)

    @property
    def redis_params(self):
        """
        Get the Redis backend's connection parameters

        :returns: The Redis backend's connection parameters
        :rtype: Dict
        """
        return dict(
            host=self._host,
            password=self._password,
            port=int(self._port)
        )

    @cached_property
    def redis(self):
        """
        Get the Redis backend this client is connected to

        :returns: The Redis backend of this Talker client
        :rtype: StrictRedis
        """
        return get_redis(**self.redis_params)

    def __repr__(self):
        return "<{0.__class__.__name__} {0._name}/{0._host}:{0._port}>".format(self)

    def _pipeline_flush_log(self, pipeline):
        _verbose_logger.debug("Sending pipeline: %s", pipeline.command_stack)

    def get_runner_for(self, host_id):
        return partial(self.run, host_id)

    __getitem__ = get_runner_for

    def run(self, host_id, *args,
            name=None,
            ack_timeout=AGENT_ACK_TIMEOUT,
            timeout=HOUR,
            server_timeout=True,
            line_timeout=None,
            log_file=None,
            raise_on_failure=True,
            max_output_per_channel=None, set_new_logpath=None):

        """
        Run a command on a specific host

        :param [str] host_id: The ID of the host to send the command to
        :param [List[str]] args: The command to run on the host (e.g. 'echo', 'hello')
        :param [str] name: A name to give to the command (for logging pruposes)
        :param [int, float] ack_timeout: Agent ack timeout in seconds
        :param [int, float] timeout: Command timeout in seconds
        :param [int, float] server_timeout: Command timeout in seconds to be enforced in the agent
        :param [int, float] line_timeout: Command output timeout in seconds
        :param [str] log_file: Log file path for writing command output on the remote host
        :param [bool] raise_on_failure: Should the client raise an appropriate exception on failure
        :param [int] max_output_per_channel: Maximum command output in bytes
        :param [str] set_new_logpath: Reconfigure the remote agent's log path

        :returns: The command running on the remote host
        :rtype: Cmd
        """

        if not name:
            lines = " ".join(args).strip().splitlines()
            name = compact(lines.pop(0), 60, suffix_length=10)
            if lines:
                name += " ..."
            name = "`%s`" % name

        cmd = Cmd(
            talker=self, host_id=host_id, raise_on_failure=raise_on_failure,
            line_timeout=line_timeout, log_file=log_file,
            args=args, ack_timeout=ack_timeout, timeout=timeout, server_timeout=server_timeout, name=name,
            max_output_per_channel=max_output_per_channel, set_new_logpath=set_new_logpath)
        cmd.send()
        return cmd

    def reboot(self, host_id, timeout=10 * MINUTE, force=RebootCmd.DEFAULT_FORCE, raise_on_failure=True):
        """
        Send a reboot command to the remote host

        :param [str] host_id: The ID of the host to send the command to
        :param [int, float] timeout: Command timeout in seconds
        :param [boot] force: Whether to reboot forcefully (without calling sync)
        :param [bool] raise_on_failure: Should the client raise an appropriate exception on failure

        :returns: The reboot command running on the remote host
        :rtype: RebootCmd
        """
        cmd = RebootCmd(
            force=force,
            talker=self, host_id=host_id, raise_on_failure=raise_on_failure,
            timeout=timeout, server_timeout=False, name="reboot")
        cmd.send()
        return cmd

    def reset_server_error(self, host_id):
        """
        Reset the last error the agent encountered on

        :param [str] host_id: The ID of the host to send the command to
        """
        _logger.debug("Reseting pending error")
        self.reactor.rpush("commands-%s" % host_id, json.dumps(dict(cmd="reset_error")))

    def poll(self, cmds):
        results = {cmd: dict(ack=cmd.ack, retcode=cmd.retcode) for cmd in cmds}

        with self.redis.pipeline() as p:
            res_idx_to_i = {}
            res_idx = 0
            for cmd in cmds:
                if cmd.ack is None:
                    p.lpop(cmd._ack_key)
                    res_idx_to_i[res_idx] = cmd, 'ack'
                    res_idx += 1

                if cmd.retcode is None:
                    p.lpop(cmd._exit_code_key)
                    res_idx_to_i[res_idx] = cmd, 'retcode'
                    res_idx += 1

            if res_idx:
                self._pipeline_flush_log(p)

                with RedisTimeoutError.on_exception(redis.exceptions.TimeoutError, redis=self):
                    pipeline_results = p.execute()

                for (i, result) in enumerate(pipeline_results):
                    cmd, slot = res_idx_to_i[i]
                    results[cmd][slot] = result

        def on_poll(cmd):
            ack = results[cmd]['ack']
            retcode = results[cmd]['retcode']

            if cmd.ack is None and ack:
                cmd.on_ack(ack)

            if retcode is None:
                cmd.check_client_timeout()

            return cmd.on_polled(retcode)

        # Using MultiObject allows to raise multiexception, not just exception on single cmd
        return MultiObject(cmds).call(on_poll)

    def is_done(self, cmds):
        """
        Check if a list of commands are all done

        :param [List[Cmd]] cmds: The list of command to check if are done

        :returns: True if all commands are done else False
        :rtype: bool
        """
        return len(list(filter(lambda i: i is None, self.poll(cmds)))) == 0

    def is_alive(self, host_id, name='is_alive'):
        """
        Do a naive check to see if the agent is responsive.
        If the function returns False it doesn't mean for sure that the agent/machine is down

        :param [str] host_id: The ID of the host to send the command to
        :param [str] name: A name to give to the command (for logging pruposes)
        :return: True if we get ack, False otherwise
        :rtype: bool
        """

        cmd = self.run(host_id, 'true', name=name, timeout=IS_ALIVE_TIMEOUT, ack_timeout=IS_ALIVE_ACK_TIMEOUT)
        try:
            cmd.wait()
        except CommandTimeoutError as exc:
            _logger.debug('%s:is_alive resulted with %s returning true', cmd.job_id, exc.__class__.__name__)
            return True
        except (TalkerServerTimeout, ClientCommandTimeoutError) as exc:
            _logger.debug('%s:is_alive resulted with %s ignoring ...', cmd.job_id, exc.__class__.__name__)
            pass
        return bool(cmd.ack)

    def wait(self, cmds, sleep=0.5, timeout=DAY, initial_log_interval=12):
        """
        Wait for commands to finish running

        :param [List[Cmd]] cmds: The list of command to wait on
        :param [int, float] sleep: How long to sleep between checks
        :param [int, float] timeout: After how many seconds should we throw a timeout error
        :param [int, float] initial_log_interval: Interval between logging

        :returns: True if all commands are done else False
        :rtype: bool
        """
        for _ in self.as_completed(cmds, sleep, timeout, initial_log_interval):
            pass
        if not all(cmd.retcode in cmd.good_codes for cmd in cmds):  # Not spawning threads without need
            MultiObject(cmds).raise_if_needed()  # as completed omitted all exception, therefore need to re-raise
        return self.poll(cmds)

    def as_completed(self, cmds, sleep=0.5, timeout=DAY, initial_log_interval=12):
        """
        Yields completed commands, until all are completed or the timeout expires (if provided).
        If timeout is 0, this will effectively stop the generator instead of blocking on remaining commands

        :param [List[Cmd]] cmds: The list of command to wait on
        :param [int, float] sleep: How long to sleep between checks
        :param [int, float] timeout: After how many seconds should we throw a timeout error
        :param [int, float] initial_log_interval: Interval between logging

        :returns: Yields commands and their results as a tuple
        :rtype: Tuple[Cmd, result???]
        """
        pending = list(cmds)

        # We prevent exceptions from being raised from this iterator, so that we can keep iterating
        # through all cmds. we'll let the caller deal with raising exceptions mid-iteration if it so desires
        # We also need to deal with the case of re-entering this iterator with incomplete cmds, so we must
        # keep the original 'raise_on_failure' setting across invocations
        for cmd in pending:
            cmd._should_raise_on_failure = getattr(cmd, "_should_raise_on_failure", cmd.raise_on_failure)
            cmd.raise_on_failure = False

        log_interval = initial_log_interval
        logging_timer = BackoffTimer(expiration=log_interval, max_interval=HOUR)
        timeout_timer = Timer(expiration=timeout)

        had_results = False

        def _poll():
            nonlocal had_results
            results = self.poll(pending)

            done_idxes = []
            for i, result in enumerate(results):
                cmd = pending[i]
                if result is not None:
                    done_idxes.append(i)
                    cmd.raise_on_failure = cmd._should_raise_on_failure
                    yield cmd, result

            for i in sorted(done_idxes,
                            reverse=True):  # Alternative would be to copy pending list, but this is more efficient
                pending.pop(i)

            if pending:
                if logging_timer.expired:
                    logging_timer.backoff()
                    MultiObject(pending).check_client_timeout()
                    hosts = sorted(set(cmd.hostname for cmd in pending))
                    _logger.info("Waiting on %s command(s) on %s host(s): %s", len(pending), len(hosts), ", ".join(hosts))
                    for cmd in pending:
                        since_started = (
                            "acked {:ago}".format(cmd.since_started) if cmd.ack else
                            "not-started")
                        _logger.debug("   job-id: %s (%s)", cmd.job_id, since_started)
            if done_idxes:
                had_results = True

        while True:
            yield from _poll()
            if not pending:
                return
            if timeout == 0:
                if had_results:
                    return
                else:
                    continue
            if timeout_timer.expired:
                raise TimeoutException(
                    "Commands (%s) did not complete within timeout" % len(pending),
                    timeout=timeout, pending=pending
                )
            wait(sleep)

    def results(self, cmds, decode='utf-8'):
        """
        Wait for commands to finish and get their results
        This will raise for failed commands if needed

        :param [List[Cmd]] cmds: The list of commands to get their results
        :param [str] decode: Expected output encoding

        :returns: A MultiObject of tuples containing stdout and stderr
        :rtype: MultiObject[Tuple[str, str]]
        """
        self.wait(cmds)
        ret = []
        self.get_output(cmds)
        for i, cmd in enumerate(cmds):
            ret.append(cmd._decode_output(cmd.stdout, decode='utf-8'))
        return ret

    def get_output(self, cmds, decode='utf-8'):
        """
        Get commands' output so far

        :param [List[Cmd]] cmds: The list of commands to get their output
        :param [str] decode: Expected output encoding

        :returns: A MultiObject of tuples containing stdout and stderr
        :rtype: MultiObject[Tuple[str, str]]
        """
        ret = []
        with self.redis.pipeline() as p:
            for cmd in cmds:
                cmd._request_outputs(p)
            self._pipeline_flush_log(p)
            results = p.execute()
            p.reset()
            for i, (stdout, stderr) in enumerate(chunkify(results, 2)):
                cmd = cmds.L[i]
                cmd.on_output(cmd.stdout, stdout)
                cmd.on_output(cmd.stderr, stderr)
                cmd._trim_outputs(stdout, stderr, pipeline=p)
                if decode:
                    (stdout, stderr) = (cmd._decode_output(out, decode) for out in (stdout, stderr))
                ret.append((stdout, stderr))
            self._pipeline_flush_log(p)
            p.execute()
        return MultiObject(ret)

    def iter_output(self, cmds, sleep=1.0, decode='utf-8'):
        """
        Iterate commands output

        :param [List[Cmd]] cmds: The list of commands to iterate on their output
        :param [int, float] sleep: How long to sleep between pollings
        :param [str] decode: Expected output encoding

        :returns: A MultiObject of tuples containing stdout and stderr
        :rtype: MultiObject[Tuple[str, str]]
        """
        while not self.is_done(cmds):
            yield self.get_output(cmds, decode=decode)
            wait(sleep)
        yield self.get_output(cmds, decode=decode)
