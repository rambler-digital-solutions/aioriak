import asyncio
from aioriak import RiakClient
from aioriak.tests import HOST, PORT
import unittest


class IntegrationTest:
    def tearDown(self):
        self.client.close()


class AsyncUnitTestCase(unittest.TestCase):
    def create_client(self, host=None, port=None, **client_args):
        host = host or HOST
        port = port or PORT
        return self.loop.run_until_complete(
            RiakClient.create(host, port,
                              loop=self.loop, **client_args))

    def setUp(self):
        super().setUp()
        self.loop = asyncio.get_event_loop()
        self.client = self.create_client()

    def tearDown(self):
        self.loop.stop()
