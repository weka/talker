import errno
import fcntl
import os
import random
import signal
import subprocess
import sys
import threading
import time
from logging import getLogger

from talker_agent.config import MAX_OUTPUT_PER_CHANNEL, DEFAULT_GRACEFUL_TIMEOUT, CMD_OUTPUT_EXPIRATION, \
    FINAL_RESULT_EXPIRATION, REBOOT_FILENAME, JOBS_DIR
from talker_agent.utils import set_logging_to_file
from talker_agent.utils import reraise


def wait_proc(proc, timeout):
    t = threading.Thread(target=proc.wait)
    t.start()
    t.join(timeout)
    return not t.is_alive()


class LineTimeout(Exception):
    pass


class JobHanging(Exception):
    pass


class JobTimeout(Exception):
    pass


class Job(object):
    class OutputChannel(object):
        __slots__ = "name", "chunks", "closed", "total", "expiration", "fdno", "job"

        def __init__(self, name, job):
            self.name = name
            self.chunks = []
            self.closed = False
            self.total = 0
            self.expiration = 0  # expiration of data in redis
            self.fdno = None
            self.job = job

        def close(self):
            self.closed = True

        def write(self, data):
            byte_count = len(data)
            self.job.logger.debug("Got data from %s (fd=%s), len=%s", self.name, self.fdno, byte_count)

            self.total += byte_count
            self.job.last_read = time.time()
            if byte_count == 0:
                self.close()
                return True

            return self._write(data)

        def _write(self, data):
            if self.total > self.job.max_output_per_channel:
                self.job.set_result("overflowed")
                self.job.finalize()
                self.job.kill()
                return True

            self.chunks.append(data)
            return False

    class LoggedOutputChannel(OutputChannel):
        __slots__ = "log"

        def __init__(self, *args, **kwargs):
            log_file = kwargs.pop('log_file')
            super(Job.LoggedOutputChannel, self).__init__(*args, **kwargs)

            dirname = os.path.dirname(log_file)
            if dirname:
                try:
                    os.makedirs(dirname)
                except OSError as e:
                    if e.errno != errno.EEXIST or not os.path.isdir(dirname):
                        raise

            self.log = open(log_file, "ab")
            self.job.logger.info("Logging %s to '%s' (%s)", self.job.job_id, log_file, self.name)

        def close(self):
            super(Job.LoggedOutputChannel, self).close()
            self.log.close()

        def _write(self, data):
            self.log.write(data)
            return False

        def __del__(self):
            self.log.close()

    def create_channel(self, name, log_file):
        params = dict(name=name, job=self)
        cls = self.OutputChannel

        if log_file:
            params.update(log_file=log_file)
            cls = self.LoggedOutputChannel

        return cls(**params)

    def __init__(
            self, id, cmd, agent, timeout=None, buffer_time=100,
            log_file=None,  # a path or paths (stdout, stderr) for sending output to
            line_timeout=None,  # timeout for output (in either stdout or stderr) to be read from process
            set_new_logpath=None,  # used to tell talker to move its own logfile to a new location
            max_output_per_channel=MAX_OUTPUT_PER_CHANNEL,  # don't allow more than this amount of data to be sent back
            **kw):

        self.job_id = id
        self.logger = getLogger(self.job_id)

        self.logger.debug("Got job, cmd %s", cmd)
        if kw:
            # we want to allow new talker-clients to talk to older talker-agents, so we
            # politely swallow any unknown parameters that the client might have sent us
            self.logger.warning("got unsupported params: %s", tuple(map(str, sorted(kw.keys()))))

        self.cmd = cmd
        self.popen = None
        self.job_fn = None
        self.buffer_time = buffer_time  # buffer time in ms, will poll at most with this rate
        self.timeout = timeout
        self.line_timeout = line_timeout
        self.last_read = None

        out_log, err_log = log_file if isinstance(log_file, (tuple, list)) else ([log_file] * 2)
        self.stdout = self.create_channel(name="stdout", log_file=out_log)
        self.stderr = self.create_channel(name="stderr", log_file=err_log)
        self.channels = (self.stdout, self.stderr)

        self.exit_code = None
        self.killed_by = False
        self.last_send = 0
        self.agent = agent
        self.start_time = time.time()
        self.reset_timeout()
        self.max_output_per_channel = max_output_per_channel
        self.set_new_logpath = set_new_logpath

        self.acknowledge()

    def acknowledge(self):
        pipeline = self.agent.redis.pipeline()
        pipeline.rpush("result-%s-ack" % self.job_id, self.start_time)
        pipeline.expire("result-%s-ack" % self.job_id, FINAL_RESULT_EXPIRATION)
        pipeline.execute()

    def start(self):
        already_seen = self.agent.check_if_seen(self.job_id)
        if already_seen:
            self.logger.debug("already seen at %s", time.ctime(already_seen))
            return

        self.logger.debug("starting job")
        attempts = 0

        if self.agent.pending_exception:
            self.stderr.chunks.append(self.agent.pending_exception)
            self.set_result("error")
            self.finalize()
            return

        while True:
            attempts += 1
            try:
                self.popen = subprocess.Popen([u"%s" % arg for arg in self.cmd], stdout=subprocess.PIPE,
                                              stderr=subprocess.PIPE, preexec_fn=os.setsid)
            except OSError as e:
                self.logger.error("OS Error:%s", e, exc_info=True)

                if attempts < 3:
                    if e.args[0] == errno.EAGAIN:
                        time.sleep(random.random() * 2)
                        continue

                self.stderr.chunks.append(e.strerror)
                self.set_result(e.errno)
                self.finalize()
                return
            else:
                break

        self.job_fn = "%s/job.%s.%s" % (JOBS_DIR, self.job_id, self.popen.pid)
        with open(self.job_fn, "w") as f:
            try:
                f.write(repr(self.cmd))
            except IOError:
                # to help with WEKAPP-74054
                os.system("df")
                os.system("df -i")
                raise

        self.agent.current_processes[self.job_id] = self
        for channel in self.channels:
            pipe = getattr(self.popen, channel.name)
            channel.fdno = fileno = pipe.fileno()
            fcntl.fcntl(fileno, fcntl.F_SETFL, os.O_NONBLOCK)
            self.agent.fds_to_channels[fileno] = channel
            self.agent.fds_to_files[fileno] = pipe
            self.agent.fds_poller.register(fileno)

        self.last_read = time.time()
        self.logger.debug("job started: pid=%s", self.popen.pid)
        pipeline = self.agent.redis.pipeline()
        pipeline.set("result-%s-pid" % self.job_id, self.popen.pid)
        pipeline.execute()

    def finalize(self, pipeline=None):
        own_pipeline = pipeline is None
        if pipeline is None:
            pipeline = self.agent.redis.pipeline()
        self.close()
        self.send_data(throttled=False, pipeline=pipeline)
        self.send_result(pipeline)
        if own_pipeline:
            pipeline.execute()

    def reset_timeout(self, new_timeout=None):
        if new_timeout:
            self.timeout = new_timeout
        self.logger.debug("Reseting timeout to %s", self.timeout)
        if new_timeout == -1:
            self.timeout = 0
            self.timeout_at = None
            return
        if self.timeout:
            self.timeout_at = time.time() + self.timeout
        else:
            self.timeout_at = None

    def check_timed_out(self):
        now = time.time()
        if not self.line_timeout or not self.last_read:
            pass
        elif now - self.last_read > self.line_timeout:
            raise LineTimeout()

        if not self.timeout_at:
            pass
        elif self.timeout_at > now:
            pass
        elif self.killed_by:
            raise JobHanging()
        else:
            raise JobTimeout()

    def send_data(self, pipeline, throttled=True):
        now = time.time()

        if throttled:
            if now - self.last_send < (self.buffer_time / 1000):
                return
            else:
                self.last_send = now

        for channel in self.channels:
            if isinstance(channel, self.LoggedOutputChannel):
                continue
            chunks = []
            while True:
                try:
                    chunks.append(channel.chunks.pop(0))
                except IndexError:
                    break
            if chunks:
                to_send = b"".join(chunks)
                self.logger.debug("Sending data from %s, len=%s", channel.name, len(to_send))
                cmd = 'result-%s-%s' % (self.job_id, channel.name)
                pipeline.rpush(cmd, to_send)
                if (channel.expiration - now) < 3600:
                    pipeline.expire(cmd, CMD_OUTPUT_EXPIRATION)
                    pipeline.expire("result-%s-pid" % self.job_id, CMD_OUTPUT_EXPIRATION)
                    channel.expiration = now + CMD_OUTPUT_EXPIRATION

    def send_result(self, pipeline):
        self.logger.debug("Job done: %s", self.exit_code)
        pipeline.rpush("result-%s-retcode" % self.job_id, self.exit_code)
        pipeline.expire("result-%s-retcode" % self.job_id, FINAL_RESULT_EXPIRATION)
        pipeline.expire("result-%s-stderr" % self.job_id, FINAL_RESULT_EXPIRATION)
        pipeline.expire("result-%s-stdout" % self.job_id, FINAL_RESULT_EXPIRATION)
        pipeline.expire("result-%s-pid" % self.job_id, FINAL_RESULT_EXPIRATION)
        if self.set_new_logpath:
            self.logger.info("Got request to set a new logpath: %s", self.set_new_logpath)
            if self.exit_code == 0:
                set_logging_to_file(self.set_new_logpath)
            else:
                self.logger.info("Skipping due to non-zero exit code")

    def set_result(self, result):
        self.logger.debug("Setting result, %s", result)
        if self.exit_code is None:
            self.exit_code = result

    def close(self):
        if self.popen:
            for fileno in (self.popen.stdout.fileno(), self.popen.stderr.fileno()):
                channel = self.agent.fds_to_channels.pop(fileno, None)
                if channel:
                    channel.close()
            self.close_popen()
        if self.job_fn:
            os.remove(self.job_fn)
        self.agent.current_processes.pop(self.job_id, None)

    def close_popen(self):
        if self.popen.stdout:
            self.popen.stdout.close()
        if self.popen.stderr:
            self.popen.stderr.close()
        if self.popen.stdin:
            self.popen.stdin.close()

    def sync_progress(self, pipeline, now):
        if all(c.closed for c in self.channels) and self.popen.poll() is not None:
            self.set_result(self.popen.poll())
            self.finalize(pipeline)

        elif self.exit_code is None:
            try:
                self.check_timed_out()
            except JobHanging:
                self.logger.error("Job did not die")
                self.set_result('hanging')
                self.finalize(pipeline)
            except JobTimeout:
                self.logger.error("Job timed out")
                self.set_result('timeout')
                self.finalize(pipeline)
                self.kill()
            except LineTimeout:
                self.logger.info("No output for too long (%s)", self.line_timeout)
                self.set_result('line_timeout')
                self.finalize(pipeline)
                self.kill()
            else:
                self.send_data(pipeline)
        else:
            self.send_data(pipeline)

    def send_signal(self, sig):
        self.logger.debug("Sending signal %s", sig)
        try:
            os.killpg(os.getpgid(self.popen.pid), sig)
        except OSError as e:
            if e.errno != errno.ESRCH:
                reraise(*sys.exc_info())

    def kill(self, graceful_timeout=DEFAULT_GRACEFUL_TIMEOUT):
        self.logger.debug("Killing job (%s)", self.popen.pid)

        def _kill():
            try:
                self.killed_by = signal.SIGTERM
                self.send_signal(signal.SIGTERM)
                time.sleep(graceful_timeout)
                if self.popen.poll() is None:
                    self.killed_by = signal.SIGKILL
                    self.send_signal(signal.SIGKILL)
            except Exception as e:
                self.logger.error(e)

        thread = threading.Thread(target=_kill, name="killer-%s" % self.job_id)
        thread.daemon = True
        thread.start()
        self.reset_timeout(new_timeout=graceful_timeout + 10)


class RebootJob(Job):
    def __init__(self, *args, **kwargs):
        self.force = kwargs.pop('force')
        super(RebootJob, self).__init__(*args, **kwargs)

    def start(self):
        threading.Thread(target=self.reboot_host, name="Reboot").start()

    def reboot_host(self):
        with open(REBOOT_FILENAME, 'w') as f:
            f.write(self.job_id)
        self.agent.current_processes[self.job_id] = self  # So results will be send by sender thread
        self.agent.stop_for_reboot(requested_by=self)

        proc = subprocess.Popen(['sync'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if wait_proc(proc, 60):
            self.log("Sync finished successfully")
        else:
            self.log("Sync timed out")

        self.log("Starting reboot")
        if self.force:
            self.log("Reboot using sysrq")
            proc = subprocess.Popen(
                ['bash', '-ce', 'echo 1 > /proc/sys/kernel/sysrq; echo b > /proc/sysrq-trigger'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            self.log("Reboot with reboot cmd")
            proc = subprocess.Popen(['reboot'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        wait_proc(proc, timeout=60)
        time.sleep(60)

        self.log("Talker still alive, raising HostStillAlive(1)")
        time.sleep(1)  # Giving chance to report message
        self.set_result(1)
        self.finalize()
        raise Exception("Reboot did not occur")

    def log(self, s, *args):
        self.logger.info(s, *args)
        if args:
            s %= args
        self.stdout.chunks.append((s + "\n").encode('utf-8'))
