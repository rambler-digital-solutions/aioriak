from .base import IntegrationTest, AsyncUnitTestCase
from aioriak.bucket import Bucket


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

            obj2 = await bucket.new('baz', rand, 'application/json')
            obj2.charset = 'UTF-8'
            await obj2.store()
            obj2 = await bucket.get('baz')
            self.assertEqual(obj2.data, rand)
        self.loop.run_until_complete(go())

    def test_store_object_with_unicode(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            data = {'føø': u'éå'}
            obj = await bucket.new('foo', data)
            await obj.store()
            obj = await bucket.get('foo')
            self.assertEqual(obj.data, data)
        self.loop.run_until_complete(go())

    def test_store_unicode_string(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            data = 'some unicode data: \u00c6'
            obj = await bucket.new(self.key_name,
                                   encoded_data=data.encode('utf-8'),
                                   content_type='text/plain')
            obj.charset = 'utf-8'
            await obj.store()
            obj2 = await bucket.get(self.key_name)
            self.assertEqual(data, obj2.encoded_data.decode('utf-8'))
        self.loop.run_until_complete(go())

    def test_string_bucket_name(self):
        async def go():
            # Things that are not strings cannot be bucket names
            for bad in (12345, True, None, {}, []):
                with self.assertRaisesRegexp(
                        TypeError, 'must be a string'):
                    self.client.bucket(bad)

                with self.assertRaisesRegexp(
                        TypeError, 'must be a string'):
                    Bucket(self.client, bad, None)
            self.client.bucket('føø')
            self.client.bucket('ASCII')
        self.loop.run_until_complete(go())
