from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import partial
from queue import Queue, Empty
from threading import RLock, Event, Semaphore

import redis.exceptions

from easypy.concurrency import _check_exiting, concurrent, _run_with_exception_logging, raise_in_main_thread
from easypy.timing import wait, Timer
from easypy.units import MINUTE

from talker.errors import NoResponseForRedisCommand
from talker.config import _logger, _verbose_logger, REDIS_SOCKET_TIMEOUT
from talker.utils import retry_redis_op


class TalkerReactor():
    ASYNC_COMMANDS = {'rpush', 'expire'}
    BLOCKING_COMMANDS = {'blpop', 'brpop', 'get'}

    CmdItem = namedtuple("CmdItem", "cmd_idx cmd_id cmd args kwargs callback event results")

    def __init__(self, talker):
        self.talker = talker
        self._max_workers = 5
        self._executors = ThreadPoolExecutor(max_workers=self._max_workers)
        self._commands_queue = Queue()
        self._commands = dict()
        self._lock = RLock()
        self._current_workers = Semaphore(self._max_workers)
        self._cmd_idx = 0

        reactor_loop = _logger.context(host="TLKR-reactor-loop")(self._get_main_loop)
        self._main_loop = concurrent(func=reactor_loop, threadname="TLKR-reactor")
        self._main_loop.start()

    @staticmethod
    def _log_cmd(cmd, cmd_id=None):
        log_message = 'reactor got command {}'.format(cmd)
        if cmd_id:
            log_message = '{}: {}'.format(log_message, cmd_id)

        _verbose_logger.debug(log_message)

    def _get_main_loop(self):
        while True:
            items = []
            items.append(self._commands_queue.get())
            t = Timer(expiration=10.0 / 1000.0)
            while not t.expired:
                try:
                    items.append(self._commands_queue.get(timeout=t.remain))
                except Empty:
                    break
            if not items:
                continue
            self._current_workers.acquire(timeout=10)
            self._executors.submit(_run_with_exception_logging, self._send_data, (items,), {}, {})

    @raise_in_main_thread()
    def _send_data(self, items):
        try:
            @retry_redis_op
            def execute_pipeline_with_retry(pipeline_obj, log_context_extra=None):
                return pipeline_obj.execute()

            with self.talker.redis.pipeline() as pipeline:
                for item in items:
                    redis_func = getattr(pipeline, item.cmd)
                    redis_func(*item.args, **item.kwargs)

                # Prepare context for logging within the retry decorator
                cmds_summary = ", ".join(item.cmd for item in items)
                log_extra_for_retry = {
                    'reactor_operation': 'execute_pipeline',
                    'commands_summary': cmds_summary[:100],
                    'num_items': len(items)
                }
                
                results = execute_pipeline_with_retry(
                    pipeline,
                    log_context_extra=log_extra_for_retry
                )

                assert len(results) == len(items), "Our redis pipeline got out of sync?"

                for item, result in zip(items, results):
                    if item.callback:
                        item.callback()

                    if item.event:  # non-async
                        item.results.append(result)
                        item.event.set()
        finally:
            self._current_workers.release()

    def send(self, cmd, *args, _async=False, _callback=None, _cmd_id=None, **kwargs):
        with self._lock:
            self._cmd_idx += 1
            cmd_idx = self._cmd_idx

        event = None if cmd in self.ASYNC_COMMANDS else Event()
        item = self.CmdItem(
            cmd_idx=cmd_idx, cmd_id=_cmd_id, cmd=cmd,
            args=args, kwargs=kwargs, callback=_callback,
            event=event, results=[])

        if cmd not in self.ASYNC_COMMANDS:
            self._commands[cmd_idx] = item

        if cmd not in self.BLOCKING_COMMANDS:
            self._log_cmd(cmd, _cmd_id)

        self._commands_queue.put(item)

        if _async:
            return cmd_idx
        if event is not None:
            return self.get_response(cmd_idx)

    def get_response(self, cmd_idx):
        item = self._commands.pop(cmd_idx)

        def has_response():
            _check_exiting()
            if not item.event.wait(timeout=0.5):
                raise NoResponseForRedisCommand(talker=self.talker, **item._asdict())
            return True

        wait(REDIS_SOCKET_TIMEOUT + MINUTE, has_response, message=False, progressbar=False, sleep=0)
        [response] = item.results
        return response

    _FALSE = object()

    def send_blocking(self, cmd, *args, timeout=MINUTE, **kwargs):
        assert timeout is not None
        timeout = timeout or 1

        def _send():
            res = self.send(cmd, *args, **kwargs)
            if res is None:
                return False
            if res is False:
                return self._FALSE
            return res

        self._log_cmd(cmd, kwargs.get('_cmd_id'))
        res = wait(timeout, _send, sleep=0.01, progressbar=False, throw=False)
        if res == self._FALSE:
            res = False
        return res

    class PipelineExecutor():
        def __init__(self, reactor):
            self.reactor = reactor
            self.commands = []

        def __getattr__(self, item):
            def runner(cmd, *args, **kwargs):
                send = getattr(self.reactor, item)
                self.commands.append(send(cmd, *args, _async=True, **kwargs))

            return runner

        def reset(self):
            self.commands = []

        def execute(self):
            results = [self.reactor.get_response(command) for command in self.commands]
            self.reset()
            return results

    @contextmanager
    def pipeline(self):
        executor = self.PipelineExecutor(self)
        yield executor
        executor.reset()

    def __getattr__(self, cmd):
        if cmd in self.BLOCKING_COMMANDS:
            return partial(self.send_blocking, cmd)
        return partial(self.send, cmd)
