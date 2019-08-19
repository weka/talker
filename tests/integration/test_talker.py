import subprocess
import unittest

from talker.client import get_talker
from tests.utils import get_talker_client, restart_redis_container


class IntegrationTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.client = get_talker_client()
        self.client.redis.ping()

    def tearDown(self) -> None:
        self.client.redis.flushall()

    def test_echo_hello_command(self):
        cmd = self.client.run('MyHostId', 'bash', '-ce', 'echo hello')
        result = cmd.result()
        self.assertEqual(result, 'hello\n')

    def test_redis_fail(self):
        restart_redis_container()
        cmd = self.client.run('MyHostId', 'bash', '-ce', 'echo hello')
        result = cmd.result()
        self.assertEqual(result, 'hello\n')
