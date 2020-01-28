import json
import unittest

from talker_agent.talker import Config
from talker.errors import CommandAbortedByOverflow
from tests.utils import get_talker_client, get_retcode, get_stdout


class IntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.host_id = Config('talker_agent/config.ini').parser.get('agent', 'host_id')
        cls.diagnostics_script = Config('talker_agent/config.ini').parser.get('diagnostics', 'script_path')

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

    def test_on_startup_diagnostics(self):
        # the expected result of the Dockerfile diagnostics script
        default_output_filename = '/opt/diagnostics.txt'

        result = self.client.run(self.host_id, 'bash', '-ce', 'ls {}'.format(default_output_filename)).result().strip()
        self.assertEqual(result, default_output_filename)

    def trigger_uncaught_exception(self):
        # this illegal command will cause agent uncaught exception
        self.client.redis.rpush("commands-%s" % self.host_id, json.dumps({'cmd': ['']}))
        self.client.reset_server_error(self.host_id)

    def test_uncaught_exception_diagnostics(self):
        output_filename = 'test_uncaught_exception_diagnostics.txt'

        self.client.run(
            self.host_id, 'bash', '-ce',
            'echo touch {output_filename} > {script_path}; chmod +x {script_path}'.format(
                output_filename=output_filename, script_path=self.diagnostics_script)).result()

        self.trigger_uncaught_exception()
        result = self.client.run(self.host_id, 'bash', '-ce', 'ls {}'.format(output_filename)).result().strip()
        self.assertEqual(result, output_filename)

        #  check resilience of diagnostics on uncaught_exception
        self.client.run(self.host_id, 'bash', '-ce', 'chmod 444 {}'.format(self.diagnostics_script)).result()
        self.trigger_uncaught_exception()
        self.client.run(self.host_id, 'bash', '-ce', 'true'.format(self.diagnostics_script)).result()

        self.client.run(self.host_id, 'bash', '-ce', 'rm {}'.format(self.diagnostics_script)).result()
        self.trigger_uncaught_exception()
        self.client.run(self.host_id, 'bash', '-ce', 'true'.format(self.diagnostics_script)).result()
