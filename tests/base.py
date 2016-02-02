from aioriak import RiakClient


class IntegrationTest:
    @classmethod
    def create_client(cls, host=None, port=None, **client_args):
        host = host or 'localhost'
        port = port or '8098'
        return RiakClient(host, port, **client_args)

    def setUp(self):
        self.client = self.create_client()

    def tearDown(self):
        self.client.close()
