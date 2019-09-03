import subprocess
import unittest

from talker.client import get_talker
from talker.errors import CommandAbortedByOverflow


class IntegrationTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        redis_container_id = subprocess.check_output(
            'cd tests/integration; docker-compose ps -q redis', shell=True,  stderr=subprocess.PIPE).decode('utf-8')
        if not redis_container_id:
            raise Exception('No Redis Container')

        shell_cmd = \
            "docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' " + redis_container_id
        redis_container_ip = subprocess.check_output(shell_cmd, shell=True, stderr=subprocess.PIPE)
        self.client = get_talker(redis_container_ip.strip(), None, 6379)
        self.client.redis.ping()

    def tearDown(self) -> None:
        self.client.redis.flushall()

    def test_echo_hello_command(self):
        cmd = self.client.run('MyHostId', 'bash', '-ce', 'echo hello')
        result = cmd.result()
        self.assertEqual(result, 'hello\n')

    def test_max_output(self):
        cmd = self.client.run('MyHostId', 'bash', '-ce', 'yes')
        with self.assertRaises(CommandAbortedByOverflow):
            cmd.result()
