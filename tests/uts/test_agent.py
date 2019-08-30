import json
import os
import unittest
import uuid
from threading import Thread
from time import sleep, time

from fakeredis import FakeStrictRedis
from mock import patch

from talker_agent.agent import TalkerAgent


def get_uuid():
    return str(uuid.uuid4())


def kill_process_before_signaling(self, job_id, signal):
    job = self.current_processes.get(job_id)
    os.killpg(os.getpgid(job.popen.pid), signal)
    while job.popen.poll() is None:
        pass
    job.send_signal(signal)


def raise_file_not_found(*args, **kwargs):
    raise OSError(2, 'No such file or directory')


JOBS_DIR = '/tmp/talker/jobs'


@patch('talker_agent.job.JOBS_DIR', JOBS_DIR)
class TestClient(unittest.TestCase):

    def setUp(self):
        super(TestClient, self).setUp()

        if not os.path.isdir(JOBS_DIR):
            os.makedirs(JOBS_DIR)

        self.agent = TalkerAgent()
        self.agent.host_id = get_uuid()
        self.agent.redis = FakeStrictRedis()
        self.agent.pending_exception = None
        self.agent_exception = None
        self.agent_thread = Thread(target=self.run_agent)
        self.agent_thread.start()

    def tearDown(self):
        self.agent.kill()
        self.agent_thread.join()

    def run_agent(self):
        try:
            self.agent.start()
        except Exception as e:
            self.agent_exception = e

    def assert_agent_exception(self, exception_class, timeout=1):
        iteration_sleep = 0.1
        begin = time()
        while not self.agent_exception and (time() - begin) < timeout:
            sleep(iteration_sleep)

        if not self.agent_exception:
            raise TimeoutError("agent didn't raise {}".format(exception_class.__name__))

        with self.assertRaises(exception_class):
            raise self.agent_exception

    def get_commands_key(self):
        return "commands-%s" % self.agent.host_id

    def run_cmd_on_agent(self, cmd, job_id=None):
        jobs_key = self.get_commands_key()
        if not job_id:
            job_id = get_uuid()

        job_data = {'id': job_id, 'cmd': cmd, 'timeout': 3600.0}
        self.agent.redis.rpush(jobs_key, json.dumps(job_data))
        return job_id

    @patch('talker_agent.agent.TalkerAgent.signal_job', kill_process_before_signaling)
    def test_signal_existed_process(self):
        job_id = self.run_cmd_on_agent(['bash', '-ce', 'sleep 5'])
        job_id = self.run_cmd_on_agent('signal', job_id)
        _, ret_code = self.agent.redis.blpop('result-{}-retcode'.format(job_id))
        self.assertEqual(ret_code.decode('utf-8'), '-15')

    @patch('os.killpg', raise_file_not_found)
    def test_signal_job_unexpected_errno(self):
        job_id = self.run_cmd_on_agent(['bash', '-ce', 'sleep 5'])
        self.run_cmd_on_agent('signal', job_id)
        self.assert_agent_exception(OSError)
        self.assertEqual(2, self.agent_exception.errno)
