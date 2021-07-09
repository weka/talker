import io
import json
import logging
import unittest
import uuid
from threading import Thread
from time import sleep, time
from typing import Union

from easypy.concurrency import initialize_exception_listener
from easypy.units import Duration
from fakeredis import FakeStrictRedis
from mock import patch

from talker.client import get_talker
from talker.config import AGENT_ACK_TIMEOUT
from talker.errors import ClientCommandTimeoutError, CommandTimeoutError, CommandExecutionError, CommandPidTimeoutError, \
    CommandAlreadyDone, \
    TalkerServerTimeout, TalkerCommandLost, RedisConnectionError
from tests.utils import get_version


def get_uuid() -> str:
    return str(uuid.uuid4())


def my_get_redis(*args, **kwargs) -> FakeStrictRedis:
    return FakeStrictRedis()


def get_logger(log_stream: io.StringIO) -> logging.Logger:
    stream_handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter(fmt='%(message)s')
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.WARNING)
    logger = logging.getLogger(get_uuid())
    logger.addHandler(stream_handler)
    return logger


def dummy_pipeline_flush_log(*args, **kwargs):
    """
    this mock method is defined since fakeredis does not support pipeline.command_stack
    and we get error when Talker._pipeline_flush_log is called
    :return:
    """
    pass


def is_alive_mock(*args):
    return True


class TestClient(unittest.TestCase):

    @patch('talker.client.get_redis', my_get_redis)
    def setUp(self):
        super().setUp()
        self.client = get_talker('test_host', 'password', 1111, get_version())
        self.redis = self.client.redis
        self.host_id = get_uuid()

    def deferred_agent_response_mock(self, **kwargs):
        Thread(target=self.mock_agent_response, kwargs=kwargs, daemon=True).start()

    def mock_agent_response(
            self, job_id: str, stdout_val: str = '', stderr_val: str = '',
            retcode: Union[int, str] = 0, pid: str = '1234', delay: float = None):

        self.mock_agent_ack(job_id)
        self.redis.set('result-{}-pid'.format(job_id), pid)

        if delay:
            sleep(delay)

        self.redis.rpush('result-{}-stdout'.format(job_id), stdout_val)
        self.redis.rpush('result-{}-stderr'.format(job_id), stderr_val)
        self.redis.rpush('result-{}-retcode'.format(job_id), retcode)

    def mock_agent_ack(self, job_id):
        ack = time()
        self.redis.rpush('result-{}-ack'.format(job_id), ack)

    def delete_pid_key(self, job_id):
        self.redis.delete('result-{}-pid'.format(job_id))

    def get_command(self) -> dict:
        cmd_key = 'commands-{}'.format(self.host_id)
        ret = self.redis.blpop([cmd_key], timeout=1)
        _, job_data_raw = ret
        result = json.loads(job_data_raw.decode('utf-8'))
        return result

    @staticmethod
    def generate_command(
            id, cmd, line_timeout=None, log_file=None, max_output_per_channel=10485760,
            set_new_logpath=None, timeout=3600.0):

        return dict(
            id=id, cmd=cmd, line_timeout=line_timeout, log_file=log_file,
            max_output_per_channel=max_output_per_channel, set_new_logpath=set_new_logpath, timeout=timeout)

    def assert_command_serialization(self, bash_command, **kwargs):
        cmd = self.client.run(self.host_id, *bash_command, **kwargs)
        result = self.get_command()
        expected_value = self.generate_command(id=cmd.job_id, cmd=bash_command, **kwargs)
        self.assertEqual(result, expected_value)

    def test_command_serialization(self):
        self.assert_command_serialization(['bash', '-ce', 'echo hello'])
        self.assert_command_serialization(['bash', '-ce', 'echo hello'], max_output_per_channel=10)

    def test_command_result_from_agent(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'echo hello')
        self.mock_agent_response(cmd.job_id, 'hello\n')
        res = cmd.result()
        self.assertEqual(res, 'hello\n')

    def test_client_wait_command(self):
        delay = 0.5
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'sleep {}'.format(delay))
        before = time()
        self.deferred_agent_response_mock(job_id=cmd.job_id, delay=delay)
        ret = cmd.wait()
        after = time()
        self.assertEqual(ret, 0)
        self.assertAlmostEqual(after - before, delay, delta=delay * 0.1)

    def test_client_timeout_error(self):
        delay = 5
        cmd = self.client.run(
            self.host_id, 'bash', '-ce', 'sleep {}'.format(delay), timeout=delay * 0.0001, server_timeout=False)
        self.deferred_agent_response_mock(job_id=cmd.job_id, delay=delay)
        self.assertRaises(ClientCommandTimeoutError, cmd.wait)

    def test_agent_timeout_error(self):
        cmd = self.client.run(
            self.host_id, 'bash', '-ce', 'true', timeout=5, server_timeout=False)
        self.mock_agent_response(cmd.job_id, retcode='timeout')
        self.assertRaises(CommandTimeoutError, cmd.wait)

    def test_command_fail(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'exit 2')
        self.mock_agent_response(cmd.job_id, retcode=2)
        self.assertRaises(CommandExecutionError, cmd.result)

    def test_ignoring_command_fail(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'exit 2', raise_on_failure=False)
        self.mock_agent_response(cmd.job_id, retcode=2)
        cmd.result()

    def test_logging_output(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'for i in {0..4}; do echo $i; sleep 0.05; done')
        expected_value = '\n'.join([str(i) for i in range(5)]) + '\n'
        for i in range(5):
            self.mock_agent_response(cmd.job_id, stdout_val='{}\n'.format(i))

        log_stream = io.StringIO()
        logger = get_logger(log_stream)
        cmd.log_pipe(logger=logger, stdout_lvl=logging.WARNING)
        output = log_stream.getvalue()
        self.assertEqual(expected_value, output)

    def test_client_reboot(self):
        timeout = 60.0
        cmd = self.client.reboot(self.host_id, timeout=Duration(timeout), force=False, raise_on_failure=True)
        result = self.get_command()
        expected_value = {
            'id': cmd.job_id,
            'cmd': 'reboot',
            'force': False,
            'timeout': None}
        self.assertEqual(result, expected_value)

    def test_reset_server_error(self):
        self.client.reset_server_error(self.host_id)
        sleep(0.1)
        result = self.get_command()
        expected_value = {'cmd': 'reset_error'}
        self.assertEqual(result, expected_value)

    @patch('talker.client.Talker._pipeline_flush_log', dummy_pipeline_flush_log)
    def test_client_poll(self):
        cmd1 = self.client.run(self.host_id, 'bash', '-ce', 'echo hello')
        cmd2 = self.client.run(self.host_id, 'bash', '-ce', 'exit 2', raise_on_failure=False)
        cmds = [cmd1, cmd2]
        polling_result = self.client.poll(cmds).L
        self.assertEqual([None, None], polling_result)
        self.mock_agent_response(cmd1.job_id, 'hello\n')
        polling_result = self.client.poll(cmds).L
        self.assertEqual([0, None], polling_result)
        self.mock_agent_response(cmd2.job_id, retcode=2)
        polling_result = self.client.poll(cmds).L
        self.assertEqual([0, 2], polling_result)

    def test_pid_not_found(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'true')
        self.mock_agent_ack(cmd.job_id)
        self.assertRaises(CommandPidTimeoutError, cmd.get_pid)

        cmd = self.client.run(self.host_id, 'bash', '-ce', 'echo hello', server_timeout=False)
        self.mock_agent_response(cmd.job_id, 'hello\n')
        res = cmd.result()
        self.assertEqual(res, 'hello\n')
        self.delete_pid_key(cmd.job_id)
        self.assertRaises(CommandAlreadyDone, cmd.get_pid)

    @patch('talker.client.IS_ALIVE_ACK_TIMEOUT', 0.01)
    def test_ack_timeout(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'sleep 20', ack_timeout=0.01)
        with self.assertRaises(TalkerServerTimeout) as exc:
            cmd.result()

        # The total timeout should actually be 2 * 0.01.
        # When we don't get ack we check if the machine is alive with ack_timeout=IS_ALIVE_ACK_TIMEOUT
        self.assertLess(exc.exception.timeout, 5)

    @patch('talker.client.Talker.is_alive', is_alive_mock)
    @patch('talker.client.IS_ALIVE_ACK_TIMEOUT', 0.01)
    def test_command_lost(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'sleep 20', ack_timeout=0.01)
        self.assertRaises(TalkerCommandLost, cmd.result)

    @patch('talker.client.Talker.is_alive', is_alive_mock)
    def test_command_ack_delayed(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'sleep 20', ack_timeout=0.01, timeout=0.1)
        t = Thread(target=cmd.result)
        t.start()
        sleep(2)
        self.mock_agent_response(cmd.job_id, '')
        t.join()

    def test_redis_connection_timeout(self):
        initialize_exception_listener()  # needed in order to make the exception be raised in main thread

        self.client.redis = FakeStrictRedis(connected=False)

        with self.assertRaises(RedisConnectionError):
            self.client.run(self.host_id, 'bash', '-ce', 'true')
            sleep(0.1)  # we need a small sleep until the thread gets to the exception

        self.client.redis = FakeStrictRedis()

    def test_ack_timeout_value(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'echo hello', ack_timeout=None)
        self.assertEqual(cmd.ack_timeout, AGENT_ACK_TIMEOUT)
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'echo hello')
        self.assertEqual(cmd.ack_timeout, AGENT_ACK_TIMEOUT)
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'echo hello', ack_timeout=1)
        self.assertEqual(cmd.ack_timeout, 1)
