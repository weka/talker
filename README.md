[![Build Status](https://travis-ci.com/weka-io/talker.svg?branch=master)](https://travis-ci.com/weka-io/talker) [![PyPI version](https://badge.fury.io/py/talker.svg)](https://badge.fury.io/py/talker)

# Talker

> The almighty remote command executor for Unix machines

## Basic Usage

### Connecting a client to a Redis backend

    In [1]: from talker.client import get_talker

    In [2]: params = dict(
        host='redis.example.com',
        password='*********',
        port=8415,
        name='SomeRedisServer')

    In [2]: client = get_talker(**params)

    In [3]: client
    Out[3]: <Talker SomeRedisServer/redis.example.com:8415>

### Running a command

    In [1]: cmd = client.run(host_id, 'bash', '-ce', 'echo hello')

    In [2]: cmd.result()
    Out[2]: 'hello\n'

### Waiting for a command

    In [1]: cmd = client.run(host_id, 'bash', '-ce', 'sleep 10')

    In [2]: cmd.wait()
    Out[2]: 0

### Command timeout

    In [1]: cmd = client.run(host_id, 'bash', '-ce', 'sleep 10', timeout=2)

    In [2]: cmd.wait()
    CommandTimeoutError: Command timed out after 2s: `bash -ce sleep 10`
        args = bash -ce sleep 10
        cmd = f812c749-bdf1-4761-bae3-a279b07c64c4
        host_id = 827605a7-791e-4c6c-8f5c-eca52d7849f5.talker-0
        hostname = talker-0
        name = `bash -ce sleep 10`
        retcode = -1
        since_started = 2.2s
        stderr =
        stdout =
        talker = <Talker SomeRedisServer/redis.example.com:8415>
        timeout = 2s
        timestamp = 2019-06-24T15:41:10.112050

### Command failures

    In [25]: cmd = client.run(host_id, 'bash', '-ce', 'exit 2')

    In [26]: cmd.result()
    CommandExecutionError: Command execution error: `bash -ce exit 2`
        args = bash -ce exit 2
        cmd = 58564019-88cd-4c1b-8067-b0dada118ac4
        host_id = 827605a7-791e-4c6c-8f5c-eca52d7849f5.talker-0
        hostname = talker-0
        name = `bash -ce exit 2`
        retcode = 2
        since_started = None
        stderr =
        stdout =
        talker = <Talker SomeRedisServer/redis.example.com:8415>
        timestamp = 2019-06-24T15:42:00.020477

### Ignoring command failures

    In [1]: cmd = client.run(host_id, 'bash', '-ce', 'exit 2', raise_on_failure=False)

    In [2]: cmd.result()
    Out[2]: ''

### Logging command output while it's running

    In [43]: cmd = client.run(host_id, 'bash', '-ce', 'for i in {1..5}; do date; sleep 1; done')

    In [44]: cmd.log_pipe(logger=logging.getLogger(), stdout_lvl=logging.WARNING)
    Mon Jun 24 15:47:43 IDT 2019
    Mon Jun 24 15:47:44 IDT 2019
    Mon Jun 24 15:47:45 IDT 2019
    Mon Jun 24 15:47:46 IDT 2019
    Mon Jun 24 15:47:47 IDT 2019
