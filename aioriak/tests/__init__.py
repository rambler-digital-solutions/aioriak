import os


USE_TEST_SERVER = int(os.environ.get('USE_TEST_SERVER', '0'))

if not USE_TEST_SERVER:
    HOST = os.environ.get('RIAK_TEST_HOST', '127.0.0.1')
    PORT = int(os.environ.get('RIAK_TEST_PORT', '8087'))
else:
    # init docker cluster here
    print('init docker cluster')
    HOST = '127.0.0.1'
    PORT = 8087
