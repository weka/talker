import subprocess
from time import sleep, time

from fakeredis import FakeStrictRedis


def get_container_id(container_name):
    return subprocess.check_output(
        'cd tests/integration; docker-compose ps -q {}'.format(container_name),
        shell=True, stderr=subprocess.PIPE).decode('utf-8').strip()


def get_redis_container_id():
    return get_container_id('redis')


def get_agent_container_id():
    return get_container_id('talker')


def get_redis_container_ip(redis_container_id=None):
    if not redis_container_id:
        redis_container_id = get_redis_container_id()
    shell_cmd = \
        "docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' " + redis_container_id
    res = subprocess.check_output(shell_cmd, shell=True, stderr=subprocess.PIPE).decode('utf-8').strip()
    if not res:
        raise Exception('redis container does not exist')
    return res


def stop_redis_container(redis_container_id=None):
    if not redis_container_id:
        redis_container_id = get_redis_container_id()
    res = subprocess.check_output('docker stop {}'.format(redis_container_id),
                                  shell=True, stderr=subprocess.PIPE).decode('utf-8').strip()
    assert (res == redis_container_id)
    print('redis container was killed')


def start_redis_container(redis_container_id):
    res = subprocess.check_output('docker start {}'.format(redis_container_id),
                                  shell=True, stderr=subprocess.PIPE).decode('utf-8').strip()
    assert (res == redis_container_id)
    print('redis container was started')


def restart_redis_container(redis_container_id=None):
    if not redis_container_id:
        redis_container_id = get_redis_container_id()
    stop_redis_container(redis_container_id)
    start_redis_container(redis_container_id)


def get_talker_client():
    from talker.client import get_talker
    redis_container_ip = get_redis_container_ip()
    return get_talker(redis_container_ip, None, 6379, get_version())


def my_get_redis(*args, **kwargs):
    return FakeStrictRedis()


def get_version():
    return subprocess.check_output('cat VERSION', shell=True, stderr=subprocess.PIPE).decode('utf-8').strip()


def sanity(host_id=''):
    cl = get_talker_client()
    cmd = cl.run(host_id, 'bash', '-ce', 'echo hello')
    result = cmd.result()
    assert (result == 'hello\n')


def retry(func, timeout, interval=0.1):
    end_time = time() + timeout
    res = func()
    while res is None and time() < end_time:
        sleep(interval)
        res = func()
    return res


def get_blpop_value(redis, key, timeout):
    _, ret_code = redis.blpop(key, timeout)
    return ret_code.decode('utf-8')


def get_retcode(redis, job_id, timeout=1):
    return get_blpop_value(redis, 'result-{}-retcode'.format(job_id), timeout)


def get_stdout(redis, job_id, timeout=1):
    return get_blpop_value(redis, 'result-{}-stdout'.format(job_id), timeout)


def configure_logging():
    import logging.config
    import os

    log_folder = 'tmp'
    if not os.path.isdir(log_folder):
        os.makedirs(log_folder)

    configuration = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'detailed': {
                'format': '%(asctime)s|%(process)2s:%(threadName)-25s|%(name)-40s|%(levelname)-5s|'
                          '%(funcName)-30s |%(host)-35s|%(message)s',
            }
        },
        'handlers': {
            'main_file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': os.path.join(log_folder, 'test.log'),
                'mode': 'w',
                'formatter': 'detailed',
                'level': 'DEBUG',
                'maxBytes': 2 ** 20 * 10,
                'delay': True,
                'encoding': 'utf-8',
            }
        },
        'root': {
            'level': 'NOTSET',
            'handlers': ['main_file']
        },

    }

    logging.config.dictConfig(configuration)
