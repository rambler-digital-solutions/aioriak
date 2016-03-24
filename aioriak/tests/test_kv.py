from .base import IntegrationTest, AsyncUnitTestCase
from aioriak.bucket import Bucket
import json
import pickle
import copy


testrun_props_bucket = 'propsbucket'


class NotJsonSerializable(object):
    def __init__(self, *args, **kwargs):
        self.args = list(args)
        self.kwargs = kwargs

    def __eq__(self, other):
        if len(self.args) != len(other.args):
            return False
        if len(self.kwargs) != len(other.kwargs):
            return False
        for name, value in self.kwargs.items():
            if other.kwargs[name] != value:
                return False
        value1_args = copy.copy(self.args)
        value2_args = copy.copy(other.args)
        value1_args.sort()
        value2_args.sort()
        for i in range(len(value1_args)):
            if value1_args[i] != value2_args[i]:
                return False
        return True


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

    def test_generate_key(self):
        async def go():
            # Ensure that Riak generates a random key when
            # the key passed to bucket.new() is None.
            bucket = self.client.bucket('random_key_bucket')
            existing_keys = await bucket.get_keys()
            o = await bucket.new(None, data={})
            self.assertIsNone(o.key)
            await o.store()
            self.assertIsNotNone(o.key)
            self.assertNotIn('/', o.key)
            self.assertNotIn(o.key, existing_keys)
            self.assertEqual(len(await bucket.get_keys()),
                             len(existing_keys) + 1)
        self.loop.run_until_complete(go())

    def test_bad_key(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            obj = await bucket.new()
            with self.assertRaises(TypeError):
                await bucket.get(None)

            with self.assertRaises(TypeError):
                await self.client.get(obj)

            with self.assertRaises(TypeError):
                await bucket.get(1)
        self.loop.run_until_complete(go())

    def test_binary_store_and_get(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            # Store as binary, retrieve as binary, then compare...
            rand = str(self.randint())
            rand = bytes(rand, 'utf-8')
            obj = await bucket.new(self.key_name, encoded_data=rand,
                                   content_type='text/plain')
            await obj.store()
            obj = await bucket.get(self.key_name)
            self.assertTrue(obj.exists)
            self.assertEqual(obj.encoded_data, rand)
            # Store as JSON, retrieve as binary, JSON-decode, then compare...
            data = [self.randint(), self.randint(), self.randint()]
            key2 = self.randname()
            obj = await bucket.new(key2, data)
            await obj.store()
            obj = await bucket.get(key2)
            self.assertEqual(data, json.loads(obj.encoded_data.decode()))
        self.loop.run_until_complete(go())

    def test_blank_binary_204(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)

            # this should *not* raise an error
            empty = ""
            empty = bytes(empty, 'utf-8')
            obj = await bucket.new('foo2', encoded_data=empty,
                                   content_type='text/plain')
            await obj.store()
            obj = await bucket.get('foo2')
            self.assertTrue(obj.exists)
            self.assertEqual(obj.encoded_data, empty)
        self.loop.run_until_complete(go())

    def test_custom_bucket_encoder_decoder(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            # Teach the bucket how to pickle
            bucket.set_encoder('application/x-pickle', pickle.dumps)
            bucket.set_decoder('application/x-pickle', pickle.loads)
            data = {'array': [1, 2, 3],
                    'badforjson': NotJsonSerializable(1, 3)}
            obj = await bucket.new(self.key_name, data, 'application/x-pickle')
            await obj.store()
            obj2 = await bucket.get(self.key_name)
            self.assertEqual(data, obj2.data)
        self.loop.run_until_complete(go())

    def test_custom_client_encoder_decoder(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            # Teach the client how to pickle
            self.client.set_encoder('application/x-pickle', pickle.dumps)
            self.client.set_decoder('application/x-pickle', pickle.loads)
            data = {'array': [1, 2, 3],
                    'badforjson': NotJsonSerializable(1, 3)}
            obj = await bucket.new(self.key_name, data, 'application/x-pickle')
            await obj.store()
            obj2 = await bucket.get(self.key_name)
            self.assertEqual(data, obj2.data)
        self.loop.run_until_complete(go())

    def test_unknown_content_type_encoder_decoder(self):
        async def go():
            # Bypass the content_type encoders
            bucket = self.client.bucket(self.bucket_name)
            data = "some funny data"
            data = data.encode()
            obj = await bucket.new(self.key_name,
                                   encoded_data=data,
                                   content_type='application/x-frobnicator')
            await obj.store()
            obj2 = await bucket.get(self.key_name)
            self.assertEqual(data, obj2.encoded_data)
        self.loop.run_until_complete(go())

    def test_text_plain_encoder_decoder(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            data = 'some funny data'
            obj = await bucket.new(self.key_name, data,
                                   content_type='text/plain')
            await obj.store()
            obj2 = await bucket.get(self.key_name)
            self.assertEqual(data, obj2.data)
        self.loop.run_until_complete(go())

    def test_missing_object(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            obj = await bucket.get(self.key_name)
            self.assertFalse(obj.exists)
            # Object with no siblings should not raise the ConflictError
            self.assertIsNone(obj.data)
        self.loop.run_until_complete(go())

    def test_delete(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            rand = self.randint()
            obj = await bucket.new(self.key_name, rand)
            await obj.store()
            obj = await bucket.get(self.key_name)
            self.assertTrue(obj.exists)

            await obj.delete()
            await obj.reload()
            self.assertFalse(obj.exists)
        self.loop.run_until_complete(go())

    def test_bucket_delete(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            rand = self.randint()
            obj = await bucket.new(self.key_name, rand)
            await obj.store()

            await bucket.delete(self.key_name)
            await obj.reload()
            self.assertFalse(obj.exists)
        self.loop.run_until_complete(go())

    def test_set_bucket_properties(self):
        async def go():
            bucket = self.client.bucket(testrun_props_bucket)
            # Test setting allow mult...
            bucket.allow_mult = True
            # Test setting nval...
            bucket.n_val = 1

            c2 = self.create_client()
            bucket2 = c2.bucket(testrun_props_bucket)
            self.assertTrue(bucket2.allow_mult)
            self.assertEqual(bucket2.n_val, 1)
            # Test setting multiple properties...
            await bucket.set_properties({"allow_mult": False, "n_val": 2})

            c3 = self.create_client()
            bucket3 = c3.bucket(testrun_props_bucket)
            self.assertFalse(bucket3.allow_mult)
            self.assertEqual(bucket3.n_val, 2)

            # clean up!
            await c2.close()
            await c3.close()
        self.loop.run_until_complete(go())

    def test_if_none_match(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            obj = await bucket.get(self.key_name)
            await obj.delete()

            await obj.reload()
            self.assertFalse(obj.exists)
            obj.data = ["first store"]
            obj.content_type = 'application/json'
            await obj.store()

            obj.data = ["second store"]
            with self.assertRaises(Exception):
                await obj.store(if_none_match=True)
        self.loop.run_until_complete(go())
