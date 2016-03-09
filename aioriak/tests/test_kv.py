from .base import IntegrationTest, AsyncUnitTestCase


class BasicKVTests(IntegrationTest, AsyncUnitTestCase):
    def test_no_returnbody(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            o = await bucket.new(self.key_name, "bar")
            await o.store(return_body=False)
            self.assertEqual(o.vclock, None)
        self.loop.run_until_complete(go())

    def test_is_alive(self):
        self.assertTrue(self.client.is_alive())

    def test_store_and_get(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            rand = self.randint()
            obj = await bucket.new('foo', rand)
            await obj.store()
            obj = await bucket.get('foo')
            self.assertTrue(obj.exists)
            self.assertEqual(obj.bucket.name, self.bucket_name)
            self.assertEqual(obj.key, 'foo')
            self.assertEqual(obj.data, rand)
        self.loop.run_until_complete(go())
