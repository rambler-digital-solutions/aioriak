from aioriak.tests.base import IntegrationTest, AsyncUnitTestCase


class ClientTests(IntegrationTest, AsyncUnitTestCase):
    def test_uses_client_id(self):
        async def go():
            zero_client_id = b'\0\0\0\0'
            await self.client.set_client_id(zero_client_id)
            self.assertEqual(zero_client_id,
                             (await self.client.get_client_id()))
        self.loop.run_until_complete(go())

    def test_non_zero_client_id(self):
        async def go():
            non_zero_client_id = b'\0\0\1\0'
            await self.client.set_client_id(non_zero_client_id)
            self.assertEqual(non_zero_client_id,
                             (await self.client.get_client_id()))
        self.loop.run_until_complete(go())

    def test_list_of_hosts(self):
        async def go():
            hosts = [self.client._host, self.client._host]
            client = await self.async_create_client(hosts)
            self.assertTrue((await client.ping()))
            self.assertIn(client._host, hosts)
        self.loop.run_until_complete(go())
