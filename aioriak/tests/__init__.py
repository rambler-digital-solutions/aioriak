import os


DOCKER_CLUSTER = int(os.environ.get('DOCKER_CLUSTER', '0'))

if not DOCKER_CLUSTER:
    HOST = os.environ.get('RIAK_TEST_HOST', '127.0.0.1')
    PORT = int(os.environ.get('RIAK_TEST_PORT', '8087'))
else:
    HOST = '172.17.0.31'
    PORT = 8087
