import unittest

from talker_agent.talker import Config
from talker.errors import CommandAbortedByOverflow, CommandExecutionError
from tests.utils import get_talker_client, get_retcode, get_stdout


class IntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.host_id = Config('talker_agent/config.ini').parser.get('agent', 'host_id')

    def setUp(self):
        super().setUp()
        self.client = get_talker_client()
        self.client.redis.ping()

    def tearDown(self) -> None:
        self.client.redis.flushall()

    def test_echo_hello_command(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'echo hello')
        result = cmd.result()
        self.assertEqual(result, 'hello\n')

    def test_max_output_exceeds_maximum(self):
        cmd = self.client.run(self.host_id, 'bash', '-ce', 'yes')
        with self.assertRaises(CommandAbortedByOverflow):
            cmd.result()

    def test_max_output_per_channel_set(self):
        max_output_per_channel = 3
        for val, expected_ret in [('123', '0'), ('1234', 'overflowed')]:
            cmd = self.client.run(
                self.host_id, 'bash', '-ce', 'echo -n {}'.format(val), max_output_per_channel=max_output_per_channel)
            ret = get_retcode(self.client.redis, cmd.job_id)
            self.assertEqual(ret, expected_ret)
            if expected_ret == 0:
                res = get_stdout(self.client.redis, cmd.job_id)
                self.assertEqual(res, val)

    def test_bad_command(self):
        cmd = self.client.run(self.host_id, 'foo')
        with self.assertRaises(CommandExecutionError):
            cmd.result()
