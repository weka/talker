import os
import subprocess
import sys
import textwrap
import unittest
from concurrent.futures import ThreadPoolExecutor
from time import monotonic, sleep

from talker.reactor import Event

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class ReactorEventTest(unittest.TestCase):

    def test_wait_times_out_while_unset(self):
        event = Event()
        start = monotonic()
        self.assertFalse(event.wait(timeout=0.2))
        self.assertGreaterEqual(monotonic() - start, 0.2)
        self.assertFalse(event.is_set())

    def test_set_from_another_native_thread_wakes_the_waiter(self):
        event = Event()

        def set_later():
            sleep(0.1)
            event.set()

        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(set_later)
            start = monotonic()
            self.assertTrue(event.wait(timeout=5))
        self.assertLess(monotonic() - start, 1)
        self.assertTrue(event.is_set())
        self.assertTrue(event.wait(timeout=0))  # stays set for later waiters
        event.set()  # a second set is harmless

    def test_under_gevent_patching_the_lock_is_a_native_one(self):
        code = textwrap.dedent("""
            from gevent import monkey
            monkey.patch_all()
            from concurrent.futures import ThreadPoolExecutor
            from talker.reactor import Event
            event = Event()
            assert type(event._lock).__module__ == '_thread', type(event._lock)
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(event.set)
                assert event.wait(timeout=5)
            print('ok')
        """)
        env = dict(os.environ, PYTHONPATH=REPO_ROOT)
        run = subprocess.run([sys.executable, '-c', code], cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(run.stdout.strip(), 'ok')
