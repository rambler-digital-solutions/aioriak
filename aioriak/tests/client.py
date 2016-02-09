from aioriak import RiakClient
from aioriak.tests.base import IntegrationTest, AsyncUnitTestCase
import asyncio


class ClientTests(IntegrationTest, AsyncUnitTestCase):
    def test_uses_client_id(self):
        async def go():
            zero_client_id = b'\0\0\0\0'
            await self.client.set_client_id(zero_client_id)
            self.assertEqual(zero_client_id,
                             (await self.client.get_client_id()))
        self.loop.run_until_complete(go())

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    async def test():
        client = await RiakClient.create('localhost', loop=loop)
        await client.ping()
        await client.ping()
        res = await client.get_client_id()
        print('client id:', res)
        res = await client.set_client_id(b'test')
        print(res)
        res = await client.get_client_id()
        print(res)
        bucket_type = client.bucket_type('counter_map')
        print(await bucket_type.get_properties())
        await bucket_type.set_property('n_val', 3)
        res = await client.get_bucket_type_props(bucket_type)
        print(res)
        bucket = (await bucket_type.get_buckets())[0]
        print(bucket)
        keys = await bucket.get_keys()
        print('keys count', len(keys))
        res = await client.get_client_id()
        obj = await bucket.get(keys[0])
        print(obj)
        print(obj.sets[list(obj.sets.keys())[0]])
        print(obj.key)

    loop.run_until_complete(test())
