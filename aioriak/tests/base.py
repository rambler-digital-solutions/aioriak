import asyncio
from aioriak import RiakClient
from aioriak.tests import HOST, PORT
import unittest
import random


class IntegrationTest:
    @classmethod
    def randname(cls, length=12):
        out = ''
        for i in range(length):
            out += chr(random.randint(ord('a'), ord('z')))
        return out

    def setUp(self):
        super().setUp()
        self.bucket_name = self.randname()
        self.key_name = self.randname()


class AsyncUnitTestCase(unittest.TestCase):
    def create_client(self, host=None, port=None, **client_args):
        host = host or HOST
        port = port or PORT
        return self.loop.run_until_complete(
            RiakClient.create(host, port,
                              loop=self.loop, **client_args))

    def setUp(self):
        super().setUp()
        self.loop = asyncio.new_event_loop()
        self.client = self.create_client()

    def tearDown(self):
        super().tearDown()
        self.client.close()
        self.loop.stop()
        self.loop.close()
