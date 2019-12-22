import json
import os
import sys
import unittest
import uuid
from collections import namedtuple
from threading import Thread
from time import sleep, time
from traceback import format_exception

from fakeredis import FakeStrictRedis
from mock import patch
from redis.exceptions import ConnectionError

from talker_agent.talker import MAX_OUTPUT_PER_CHANNEL
from talker_agent.talker import TalkerAgent
from tests.utils import get_retcode, get_stdout, retry


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


class RebootMockException(Exception):
    pass


def reboot_mock_exception(*args):
    raise RebootMockException("This is reboot mock exception")


JOBS_DIR = '/tmp/talker/jobs'
EXCEPTION_FILENAME = '/tmp/talker/last_exception'
JOBS_SEEN = os.path.join(JOBS_DIR, 'eos.json')
REBOOT_FILENAME = '/tmp/talker/reboot.id'


@patch('talker_agent.talker.JOBS_DIR', JOBS_DIR)
@patch('talker_agent.talker.JOBS_SEEN', JOBS_SEEN)
@patch('talker_agent.talker.EXCEPTION_FILENAME', EXCEPTION_FILENAME)
@patch('talker_agent.talker.REBOOT_FILENAME', REBOOT_FILENAME)
class TestAgent(unittest.TestCase):

    def setUp(self):
        super(TestAgent, self).setUp()

        if not os.path.isdir(JOBS_DIR):
            os.makedirs(JOBS_DIR)
        if os.path.isfile(EXCEPTION_FILENAME):
            os.remove(EXCEPTION_FILENAME)

        self.run_agent()

    def tearDown(self):
        self.terminate_agent()

    def terminate_agent(self):
        self.agent.kill()
        self.agent_thread.join()

    def run_agent(self):

        def start_agent_safely():
            try:
                self.agent.start()
            except Exception as e:
                self.agent_exception = e
                with open(EXCEPTION_FILENAME, "w") as f:
                    f.writelines(format_exception(*sys.exc_info()))

        self.agent = TalkerAgent()
        self.agent.host_id = get_uuid()
        self.agent.redis = FakeStrictRedis()
        self.agent.pending_exception = None
        self.agent_exception = None
        self.agent_thread = Thread(target=start_agent_safely)
        self.agent_thread.start()

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

    def run_cmd_on_agent(self, cmd, job_id=None, **kwargs):
        jobs_key = self.get_commands_key()
        if not job_id:
            job_id = get_uuid()

        job_data = dict(id=job_id, cmd=cmd, **kwargs)
        self.agent.redis.rpush(jobs_key, json.dumps(job_data))
        return job_id

    @patch('talker_agent.talker.TalkerAgent.signal_job', kill_process_before_signaling)
    def test_signal_existed_process(self):
        job_id = self.run_cmd_on_agent(['bash', '-ce', 'sleep 5'])
        signal_job_id = self.run_cmd_on_agent('signal', job_id)
        _, ret_code = self.agent.redis.blpop('result-{}-retcode'.format(signal_job_id), timeout=1)
        self.assertEqual(ret_code.decode('utf-8'), '-15')

    @patch('os.killpg', raise_file_not_found)
    def test_signal_job_unexpected_errno(self):
        job_id = self.run_cmd_on_agent(['bash', '-ce', 'sleep 5'])
        self.run_cmd_on_agent('signal', job_id)
        self.assert_agent_exception(OSError)
        self.assertEqual(2, self.agent_exception.errno)

    def test_command_orphaned(self):
        job_id = self.run_cmd_on_agent(['bash', '-ce', 'sleep 5'])
        ret = retry(lambda: self.agent.redis.get("result-{}-pid".format(job_id)), timeout=1)
        self.assertIsNotNone(ret)
        self.terminate_agent()
        self.run_agent()
        _, retcode = self.agent.redis.blpop('result-{}-retcode'.format(job_id), timeout=1)
        self.assertEqual(retcode.decode('utf-8'), 'orphaned')

    def test_last_exception(self):
        self.agent.redis = FakeStrictRedis(connected=False)
        self.assert_agent_exception(ConnectionError, timeout=2)
        self.run_agent()
        job_id = self.run_cmd_on_agent(['bash', '-ce', 'echo hello'])
        _, retcode = self.agent.redis.blpop('result-{}-retcode'.format(job_id), timeout=1)
        self.assertEqual(retcode.decode('utf-8'), 'error')
        _, err = self.agent.redis.blpop('result-{}-stderr'.format(job_id), timeout=1)
        self.assertIn('ConnectionError', err.decode('utf-8'))

    def test_max_output_per_channel(self):
        TestCase = namedtuple('TestCase', ['val', 'val_repeats', 'max_output_per_channel', 'expected_ret'])
        test_cases = [
            TestCase('123', 1, 3, '0'),
            TestCase('1\\n', 1, 3, '0'),
            TestCase('12\\n', 1, 3, '0'),
            TestCase('1234', 1, 3, 'overflowed'),
            TestCase('123\\n', 1, 3, 'overflowed'),
            TestCase('12\\n3', 1, 3, 'overflowed'),
            TestCase('1', MAX_OUTPUT_PER_CHANNEL, MAX_OUTPUT_PER_CHANNEL, '0'),
            TestCase('1', MAX_OUTPUT_PER_CHANNEL, None, '0'),
            TestCase('1', MAX_OUTPUT_PER_CHANNEL + 1, MAX_OUTPUT_PER_CHANNEL, 'overflowed'),
            TestCase('1', MAX_OUTPUT_PER_CHANNEL + 1, None, 'overflowed'),
            TestCase('1', MAX_OUTPUT_PER_CHANNEL + 1, MAX_OUTPUT_PER_CHANNEL + 1, '0'),
        ]

        kwargs = {}

        for val, val_repeats, max_output_per_channel, expected_ret in test_cases:
            if max_output_per_channel is not None:
                kwargs = dict(max_output_per_channel=max_output_per_channel)

            job_id = self.run_cmd_on_agent(
                ['bash', '-ce', 'python -c "import sys; sys.stdout.write(\'{}\' * {})"'.format(val, val_repeats)],
                **kwargs)
            retcode = get_retcode(self.agent.redis, job_id)
            self.assertEqual(retcode, expected_ret)

            if expected_ret == '0':
                res = get_stdout(self.agent.redis, job_id)
                expected_val = val.replace('\\n', '\n') * val_repeats
                self.assertEqual(res, expected_val)

    @patch('talker_agent.talker.RebootJob.reboot_host', reboot_mock_exception)
    def test_safe_thread(self):
        _ = self.run_cmd_on_agent('reboot', force=True)
        self.assert_agent_exception(RebootMockException)
