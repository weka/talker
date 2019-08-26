import subprocess

from fakeredis import FakeStrictRedis

from talker.client import get_talker


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
