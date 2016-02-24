import os
from commands import get_node_ip


DOCKER_CLUSTER = int(os.environ.get('DOCKER_CLUSTER', '0'))

if not DOCKER_CLUSTER:
    HOST = os.environ.get('RIAK_TEST_HOST', '127.0.0.1')
    PORT = int(os.environ.get('RIAK_TEST_PORT', '8087'))
else:
    HOST = get_node_ip()
    PORT = 8087
