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

import fcntl
import json
import logging
import os
import os.path
import errno
import select
import signal
import subprocess
import sys
import threading
import time
import shutil
import glob
import atexit
import random
from textwrap import dedent
from contextlib import contextmanager
from logging import getLogger
from logging.handlers import RotatingFileHandler
from threading import Lock
try:
    from configparser import ConfigParser, NoSectionError
except:  # python 2.7
    from ConfigParser import ConfigParser, NoSectionError


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

CONFIG_FILENAME = '/root/talker/config.ini'
REBOOT_FILENAME = '/root/talker/reboot.id'
EXCEPTION_FILENAME = '/root/talker/last_exception'
VERSION_FILENAME = '/root/talker/version'
JOBS_DIR = '/root/talker/jobs'
JOBS_SEEN = os.path.join(JOBS_DIR, 'eos.json')
JOBS_SEEN_TMP = JOBS_SEEN + '.tmp'

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

config = None
first_exception_info = None
safe_thread_lock = Lock()


class SafeThread(threading.Thread):
    def __init__(self, target, name, args=(), kwargs=None, daemon=None):
        super(SafeThread, self).__init__(None, target, name, args, kwargs)
        self.setDaemon(daemon)

    def run(self):
        global first_exception_info
        try:
            if PY3:
                self._target(*self._args, **self._kwargs)
            else:
                self._Thread__target(*self._Thread__args, **self._Thread__kwargs)
        except:
            exc_info = sys.exc_info()
            logger.info("exception in '%s'", self.name, exc_info=exc_info)
            with safe_thread_lock:
                if not first_exception_info:
                    first_exception_info = sys.exc_info()
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            if PY3:
                del self._target, self._args, self._kwargs
            else:
                del self._Thread__target, self._Thread__args, self._Thread__kwargs


class LineTimeout(Exception):
    pass


class JobTimeout(Exception):
    pass


class Config:
    def __init__(self, filename=CONFIG_FILENAME):
        self._filename = filename
        self._parser = ConfigParser()
        self.load_configuration()

    @property
    def parser(self):
        return self._parser

    def load_configuration(self):
        try:
            self._parser.read(self._filename)
        except (IOError, OSError) as e:
            logger.error(e, exc_info=True)
            sys.exit(1)

        logger.info("Loaded configuration: %s", self._filename)

        self.validate_config()

    def update(self, section, **kwargs):
        for key, val in kwargs.items():
            self._parser.set(section, key, val)

        with open(self._filename, 'w') as configfile:
            self._parser.write(configfile)

    def validate_config(self):
        if not self._parser.get('agent', 'host_id'):
            raise Exception('config agent.host_id is missing')


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
                                              stderr=subprocess.PIPE, preexec_fn=os.setsid, close_fds=True)
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
                self.send_signal(signal.SIGTERM)
                time.sleep(graceful_timeout)
                if self.popen.poll() is None:
                    self.send_signal(signal.SIGKILL)
            except Exception as e:
                self.logger.error(e)

        SafeThread(target=_kill, name="killer-%s" % self.job_id, daemon=True).start()
        self.reset_timeout()


class RebootJob(Job):
    def __init__(self, *args, **kwargs):
        self.force = kwargs.pop('force')
        super(RebootJob, self).__init__(*args, **kwargs)

    def start(self):
        SafeThread(target=self.reboot_host, name="Reboot").start()

    def reboot_host(self):
        with open(REBOOT_FILENAME, 'w') as f:
            f.write(self.job_id)
        self.agent.current_processes[self.job_id] = self  # So results will be send by sender thread
        self.agent.stop_for_reboot(requested_by=self)

        proc = subprocess.Popen(['bash', '-ce', 'sync'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if wait_proc(proc, 60):
            self.log("Sync finished successfully")
        else:
            self.log("Sync timed out")

        self.log("Starting reboot")
        if self.force:
            self.log("Reboot using sysrq")
            proc = subprocess.Popen(
                ['bash', '-ce', 'echo 1 > /proc/sys/kernel/sysrq; echo b > /proc/sysrq-trigger'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        else:
            self.log("Reboot with reboot cmd")
            proc = subprocess.Popen(['bash', '-ce', 'reboot'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)

        flush_logger()
        wait_proc(proc, timeout=60)
        time.sleep(60)

        proc_result = dict()
        communicate_proc(proc, 0.01, proc_result)
        proc_stdout = proc_result.get('stdout')
        proc_stderr = proc_result.get('stderr')
        if proc_stdout:
            self.log('Reboot stdout: %s', proc_stdout)
        if proc_stderr:
            self.log('Reboot stderr: %s', proc_stderr)

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


class TalkerAgent(object):

    def __init__(self):
        self.output_lock = threading.RLock()
        self.redis = None
        self.host_id = None
        self.job_poller = None
        self.fds_poller = select.poll()
        self.fds_to_channels = {}
        self.fds_to_files = {}
        self.seen_jobs = {}
        self.stop_fetching = threading.Event()
        self.fetching_stopped = threading.Event()
        self.stop_agent = threading.Event()
        self.current_processes = {}
        self.exc_info = None
        self.last_scrubbed = 0

    def check_if_seen(self, job_id):
        now = time.time()
        first_seen = self.seen_jobs.setdefault(job_id, now)
        if first_seen == now:
            return
        return first_seen

    def kill(self):
        self.stop_fetching.set()
        self.stop_agent.set()

    def fetch_new_jobs(self):
        """
        format of job
            dict(
                id=str(uuid4()),
                cmd=["/bin/bash", '-c', "for i in $(seq 2);do sleep 1; echo $i; done"],
            )
        """
        last_reported = 0
        jobs_key = 'commands-%s' % self.host_id
        while not self.stop_fetching.is_set():
            new_jobs = []
            ret = self.redis.blpop([jobs_key], timeout=1)
            if not ret:
                now = time.time()
                self.scrub_seen_jobs(now=now)
                if now - last_reported > 10:
                    logger.debug("No new jobs...")
                    last_reported = now
                continue
            _, job_data_raw = ret
            new_jobs.append(job_data_raw)

            # Could be that multiple jobs were sent, checking with lrange
            additional_jobs = self.redis.lrange(jobs_key, 0, -1)
            if len(additional_jobs):
                self.redis.ltrim(jobs_key, len(additional_jobs), -1)

            new_jobs.extend(additional_jobs)
            logger.debug("Got %s jobs", len(new_jobs))
            for job in new_jobs:
                self.start_job(job.decode("utf-8"))

        self.fetching_stopped.set()
        logger.info("stopped fetching new jobs")

    def scrub_seen_jobs(self, now):
        if now - self.last_scrubbed < 10:
            return
        # for back-compat with python2.6, we don't use dict-comprehension or tuple unpacking
        self.seen_jobs = dict((job_id, last_seen) for job_id, last_seen in self.seen_jobs.items() if (now - last_seen) < JOBS_EXPIRATION)
        logger.debug("Dumping seen-jobs registry (%s items)", len(self.seen_jobs))
        with open(JOBS_SEEN_TMP, "w") as f:
            # sudden reboot may corrupt this file
            # the safe approach is to write into a temp file and then rename (-atomic) it
            json.dump(self.seen_jobs, f)
            f.flush()
            os.fsync(f.fileno())

        os.rename(JOBS_SEEN_TMP, JOBS_SEEN)
        self.last_scrubbed = now

    def reset_timeout(self, job_id, new_timeout=None, delayed=False):
        job = self.current_processes.get(job_id)
        if job is None:
            if not delayed:
                t = threading.Timer(5, self.reset_timeout, kwargs=dict(
                    job_id=job_id,
                    new_timeout=new_timeout,
                    delayed=True))
                t.start()
                logger.error("Received timeout reset request, but job_id %s was not found, delaying for 5 seconds", job_id)
            else:
                logger.error("Delayed by 5 seconds, but job_id %s still not found", job_id)
            return
        job.reset_timeout(new_timeout=new_timeout)

    def signal_job(self, job_id, signal):
        job = self.current_processes.get(job_id)
        if job is None:
            return
        job.send_signal(signal)

    def kill_job(self, job_id, graceful_timeout):
        job = self.current_processes.get(job_id)
        if job is None:
            return
        job.kill(graceful_timeout=graceful_timeout)

    def stop_for_reboot(self, requested_by):
        self.stop_fetching.set()
        requested_by.log("Waiting for polling to stop")
        self.fetching_stopped.wait(120)
        assert self.fetching_stopped.is_set(), "Polling did not stop"
        requested_by.log("Polling stopped")
        wait_start = time.time()

        while True:
            pending = set(self.current_processes).difference((requested_by.job_id,))  # for python2 compatibility
            if not pending:
                requested_by.log("All jobs are done")
                return

            requested_by.log("Waiting for %s jobs to finish", len(pending))
            for job_id in pending:
                logger.debug("- %s", job_id)

            time.sleep(1)

            if time.time() - wait_start > 60:
                break

        requested_by.log("Some jobs not yet finished, setting exit code to 'reboot' and proceeding")
        with self.pipeline() as pipeline:
            for job_id, job in list(self.current_processes.items()):
                if job_id == requested_by.job_id:
                    continue
                job.set_result('reboot')
                job.finalize(pipeline)

    def finalize_previous_session(self):
        self.pending_exception = None
        if os.path.exists(EXCEPTION_FILENAME):
            logger.info("Found exception from previous run")
            with open(EXCEPTION_FILENAME, 'rb') as f:
                self.pending_exception = f.read()
            os.remove(EXCEPTION_FILENAME)

        with self.pipeline() as pipeline:
            for fn in glob.glob("%s/job.*.*" % JOBS_DIR):
                _, job_id, pid = fn.split(".")
                job = Job(id=job_id, cmd=None, agent=self)
                job.logger.info("Found orphaned job: %s, pid=%s", job_id, pid)
                if self.pending_exception:
                    job.stderr.chunks.append(self.pending_exception)
                    job.send_data(pipeline=pipeline, throttled=False)
                job.set_result('orphaned')
                job.finalize(pipeline=pipeline)
                os.renames(fn, "%s/orphaned.%s.%s" % (JOBS_DIR, job_id, pid))

        if os.path.exists(REBOOT_FILENAME):
            with open(REBOOT_FILENAME, 'r') as f:
                job_id = f.read()
                job = Job(id=job_id, cmd=None, agent=self)
                job.logger.info("Recovered from reboot %s", job_id)
                job.set_result(0)
                job.finalize()
            os.remove(REBOOT_FILENAME)

    def start_job(self, job_data_raw):
        job_data = json.loads(job_data_raw)
        cmd = job_data['cmd']
        if isinstance(cmd, list):
            if SIMULATE_DROPS_RATE and cmd != ['true'] and random.random() > SIMULATE_DROPS_RATE:
                logger.warning("dropping job: %(id)s", job_data)
                return
            job = Job(agent=self, **job_data)
            job.start()
        elif cmd == 'reset_timeout':
            new_timeout = job_data.get('new_timeout')
            self.reset_timeout(job_data['id'], new_timeout=new_timeout)
        elif cmd == 'reset_error':
            logger.info("Got reset error request, clearing %s", "error" if self.pending_exception else "nothing")
            self.pending_exception = None
        elif cmd == 'signal':
            sig = job_data.get('signal', signal.SIGTERM)
            self.signal_job(job_data['id'], signal=sig)
        elif cmd == 'kill':
            graceful_timeout = job_data.get('graceful_timeout', DEFAULT_GRACEFUL_TIMEOUT)
            self.kill_job(job_data['id'], graceful_timeout=graceful_timeout)
        elif cmd == 'reboot':
            logger.info("Got reboot request, force=%s", job_data.get("force"))
            job = RebootJob(agent=self, **job_data)
            job.start()
        else:
            logger.error("Cmd %s not supported, skipping", cmd)

    def get_jobs_outputs(self):
        if not self.fds_to_channels:
            return False
        to_read = self.fds_poller.poll(CYCLE_DURATION_MS)
        read_fds = set()
        for fdno, _ in to_read:
            read_fds.add(fdno)
            while True:
                file = self.fds_to_files.get(fdno)
                is_done = False

                if not file:  # closed by timeout
                    break

                try:
                    data = file.read()
                    if data is None:
                        raise IOError()  # Python3 has None on empty read, python2 raises IOError
                except IOError:
                    # no data to read
                    break
                except ValueError:
                    data = b''  # leads to cleanly closing the channel

                is_done |= len(data) == 0

                channel = self.fds_to_channels.get(fdno)  # ensuring that not closed by timeout

                # sending EOL to write func for it to handle close and breaking only then
                if channel:
                    is_done |= channel.write(data)

                if is_done:
                    self.unregister_fileno(fdno)
                    break

        return True

    @contextmanager
    def pipeline(self):
        pipeline = self.redis.pipeline()
        yield pipeline
        if len(pipeline):
            logger.debug("Sending %s commands via pipeline", len(pipeline))
            pipeline.execute()
            logger.debug("Done sending")

    def sync_jobs_progress(self):
        pipeline = self.redis.pipeline()
        now = time.time()
        while True:
            for job in list(self.current_processes.values()):
                job.sync_progress(pipeline, now=now)
            if len(pipeline):
                logger.debug("Sending %s commands via pipeline", len(pipeline))
                pipeline.execute()
                logger.debug("Done sending")
                pipeline.reset()
            else:
                time.sleep(CYCLE_DURATION)

    def start(self):
        global first_exception_info
        first_exception_info = None

        self.finalize_previous_session()
        if os.path.isfile(JOBS_SEEN):
            with open(JOBS_SEEN, "r") as f:
                try:
                    self.seen_jobs = json.load(f)
                except ValueError:
                    logger.error("Couldn't read %s, removing file", JOBS_SEEN)
                    os.remove(JOBS_SEEN)

        SafeThread(target=self.fetch_new_jobs, name="RedisFetcher", daemon=True).start()
        SafeThread(target=self.sync_jobs_progress, name="JobProgress", daemon=True).start()

        while not self.stop_agent.is_set():
            if not self.get_jobs_outputs():
                time.sleep(CYCLE_DURATION / 10.0)
            if first_exception_info:
                logger.debug("re-raising exception from worker")
                reraise(*first_exception_info)
                assert False, "exception should have been raised"

    def setup(self):
        host_id = config.parser.get('agent', 'host_id')
        host = config.parser.get('redis', 'host')
        port = config.parser.getint('redis', 'port')
        try:
            password = config.parser.get('redis', 'password')
        except:
            password = None

        socket_timeout = config.parser.getfloat('redis', 'socket_timeout')
        socket_connect_timeout = config.parser.getfloat('redis', 'socket_connect_timeout')
        retry_on_timeout = config.parser.getboolean('redis', 'retry_on_timeout')
        health_check_interval = config.parser.getfloat('redis', 'health_check_interval')

        logger.info("Connecting to redis %s:%s", host, port)
        import redis  # deferring so that importing talker (for ut) doesn't immediately fail if package not available
        self.redis = redis.StrictRedis(
            host=host, port=port, db=0, password=password,
            socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout,
            retry_on_timeout=retry_on_timeout, health_check_interval=health_check_interval
        )

        while True:
            try:
                self.redis.ping()
            except redis.ConnectionError as exc:
                logger.info("Redis not ready (%s)", exc)
                time.sleep(10)
            else:
                break

        logger.info("Connected to redis as %s", host_id)
        self.host_id = host_id

        if not os.path.isdir(JOBS_DIR):
            os.makedirs(JOBS_DIR)

    def unregister_fileno(self, fileno):
        try:
            self.fds_poller.unregister(fileno)
        except KeyError:
            pass  # closed while processing data


def run_diagnostics():
    try:
        script_path = config.parser.get('diagnostics', 'script_path')
    except NoSectionError:
        return

    if os.path.isfile(script_path):
        logger.info("Running diagnostics")
        try:
            subprocess.Popen(["bash", "-ce", script_path], close_fds=True)
        except:
            logger.exception("Running diagnostics failed")
    else:
        logger.warning("Diagnostics script file does not exist")


def wait_proc(proc, timeout):
    t = SafeThread(target=proc.wait, name='wait_proc')
    t.start()
    t.join(timeout)
    return not t.is_alive()


def communicate_proc(proc, timeout, proc_result):
    def target():
        stdout, stderr = proc.communicate()
        proc_result['stdout'] = stdout.decode('utf-8').strip()
        proc_result['stderr'] = stderr.decode('utf-8').strip()

    t = SafeThread(target=target, name='proc_output')
    t.start()
    t.join(timeout)
    if t.is_alive():
        logger.error("Reboot didn't finish")


def handle_exception(*exc_info):
    if issubclass(exc_info[0], KeyboardInterrupt):
        sys.__excepthook__(*exc_info)
        return
    logger.error("Uncaught exception", exc_info=exc_info)


FILE_LOG_HANDLER = None


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
    handler.setFormatter(logging.Formatter("%(asctime)s|%(threadName)-15s|%(name)-38s|%(levelname)-5s|%(funcName)-15s|%(message)s"))
    logging.root.addHandler(handler)

    FILE_LOG_HANDLER = handler

    config.update('logging', logpath=logpath)


def flush_logger():
    if not FILE_LOG_HANDLER:
        return

    FILE_LOG_HANDLER.flush()
    logfile_fd = FILE_LOG_HANDLER.stream.fileno()
    os.fsync(logfile_fd)


def setup_logging(verbose):
    sys.excepthook = handle_exception
    for handler in logging.root.handlers:
        logging.root.removeHandler(handler)

    logging.root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s|%(levelname)-5s|%(funcName)-15s|%(message)s", datefmt="%H:%M:%S"))
    if not verbose:
        handler.setLevel(logging.INFO)
    logging.root.addHandler(handler)


def main(*args):
    global config

    setup_logging(verbose="-v" in args)
    no_restart = "--no-restart" in args

    version = open(VERSION_FILENAME).read()
    logger.info("Starting Talker: %s", version)

    config = Config()
    set_logging_to_file(config.parser.get('logging', 'logpath'))

    # to help with WEKAPP-74054
    os.system("df")
    os.system("df -i")

    open("/var/run/talker.pid", "w").write(str(os.getpid()))
    atexit.register(os.unlink, "/var/run/talker.pid")

    try:
        run_diagnostics()
        agent = TalkerAgent()
        agent.setup()
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

        run_diagnostics()

        if not no_restart:
            logger.error("Uncaught exception - committing suicide, and restarting in 3 seconds")
            subprocess.Popen(["bash", "-ce", 'sleep 3 && service talker restart'], close_fds=True)  # make sure this matches the pattern in 'install_talker()'
        raise
    else:
        if not no_restart:
            logger.info("Restarting in 1 seconds")
            subprocess.Popen(["bash", "-ce", 'sleep 1 && service talker restart'], close_fds=True)  # make sure this matches the pattern in 'install_talker()'


if __name__ == '__main__':
    args = sys.argv[1:]
    if "--ut" in args:
        print("Talker don't need no UT")
    else:
        sys.exit(main(*args))
