import glob
import json
import os
import random
import select
import signal
import sys
import threading
import time
from contextlib import contextmanager

from talker_agent.config import logger, JOBS_SEEN, REBOOT_FILENAME, EXCEPTION_FILENAME, JOBS_DIR, SIMULATE_DROPS_RATE, \
    DEFAULT_GRACEFUL_TIMEOUT, CYCLE_DURATION_MS, CYCLE_DURATION, JOBS_EXPIRATION
from talker_agent.job import Job, RebootJob
from talker_agent.utils import reraise


class TalkerAgent(object):

    def __init__(self):
        self.output_lock = threading.RLock()
        self.redis = None
        self.host_id = None
        self.redis_fetcher = None
        self.redis_sender = None
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
        self.seen_jobs = dict(
            (job_id, last_seen) for job_id, last_seen in self.seen_jobs.items() if (now - last_seen) < JOBS_EXPIRATION)
        logger.debug("Dumping seen-jobs registry (%s items)", len(self.seen_jobs))
        with open(JOBS_SEEN, "w") as f:
            json.dump(self.seen_jobs, f)
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
                logger.error("Received timeout reset request, but job_id %s was not found, delaying for 5 seconds",
                             job_id)
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
            for job_id, job in self.current_processes.items():
                if job_id == requested_by.job_id:
                    continue
                job.set_result('reboot')
                job.finalize(pipeline)

    def finalize_previous_session(self):
        if os.path.exists(REBOOT_FILENAME):
            with open(REBOOT_FILENAME, 'r') as f:
                job_id = f.read()
                job = Job(id=job_id, cmd=None, agent=self)
                job.logger.info("Recovered from reboot %s", job_id)
                job.set_result(0)
                job.finalize()
            os.remove(REBOOT_FILENAME)

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

    def start_worker(self, worker, name):

        def safe_run():
            try:
                return worker()
            except:  # noqa
                self.exc_info = sys.exc_info()
                logger.debug("exception in '%s'", name, exc_info=self.exc_info)

        t = threading.Thread(target=safe_run, name=name)
        t.daemon = True
        t.start()
        return t

    def start(self):
        self.finalize_previous_session()
        if os.path.isfile(JOBS_SEEN):
            with open(JOBS_SEEN, "r") as f:
                self.seen_jobs = json.load(f)

        self.redis_fetcher = self.start_worker(self.fetch_new_jobs, name="RedisFetcher")
        self.redis_sender = self.start_worker(self.sync_jobs_progress, name="JobProgress")

        while not self.stop_agent.is_set():
            if not self.get_jobs_outputs():
                time.sleep(CYCLE_DURATION / 10.0)
            if self.exc_info:
                logger.debug("re-raising exception from worker")
                reraise(*self.exc_info)
                assert False, "exception should have been raised"

    def setup(self, host_id, host, port, password, socket_timeout=10, retry_on_timeout=True):
        logger.info("Connecting to redis %s:%s", host, port)
        import redis  # deferring so that importing talker (for ut) doesn't immediately fail if package not available
        self.redis = redis.StrictRedis(
            host=host, port=port, db=0, password=password,
            socket_timeout=socket_timeout, retry_on_timeout=retry_on_timeout)

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
