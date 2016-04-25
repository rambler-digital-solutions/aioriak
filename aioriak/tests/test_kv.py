from .base import IntegrationTest, AsyncUnitTestCase
from aioriak.bucket import Bucket
from aioriak.error import ConflictError
from riak.resolver import default_resolver, last_written_resolver
import asyncio
import json
import pickle
import copy


testrun_props_bucket = 'propsbucket'
testrun_sibs_bucket = 'sibsbucket'


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
        async def go():
            self.assertTrue(await self.client.is_alive())
        self.loop.run_until_complete(go())

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

    def test_big_object(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            obj = await bucket.new(self.key_name)
            data = '0' * 1024 * 1024
            obj.data = data
            await obj.store()
            obj2 = await bucket.get(self.key_name)
            self.assertEqual(obj2.data, data)
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
            await bucket.set_property('allow_mult', True)
            # Test setting nval...
            await bucket.set_property('n_val', 1)

            c2 = await self.async_create_client()
            bucket2 = c2.bucket(testrun_props_bucket)
            self.assertTrue(await bucket2.get_property('allow_mult'))
            self.assertEqual(await bucket2.get_property('n_val'), 1)
            # Test setting multiple properties...
            await bucket.set_properties({"allow_mult": False, "n_val": 2})

            c3 = await self.async_create_client()
            bucket3 = c3.bucket(testrun_props_bucket)
            self.assertFalse(await bucket3.get_property('allow_mult'))
            self.assertEqual(await bucket3.get_property('n_val'), 2)

            # clean up!
            c2.close()
            c3.close()
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

    async def generate_siblings(self, original, count=5, delay=None):
        vals = []
        for _ in range(count):
            while True:
                randval = str(self.randint())
                if randval not in vals:
                    break

            other_obj = await original.bucket.new(key=original.key,
                                                  data=randval,
                                                  content_type='text/plain')
            other_obj.vclock = original.vclock
            await other_obj.store()
            vals.append(randval)
            if delay:
                await asyncio.sleep(delay)
        return vals

    def test_siblings(self):
        async def go():
            # Set up the bucket, clear any existing object...
            bucket = self.client.bucket(testrun_sibs_bucket)
            obj = await bucket.get(self.key_name)
            await bucket.set_property('allow_mult', True)

            # Even if it previously existed,let's store a base resolved version
            # from which we can diverge by sending a stale vclock.
            obj.data = 'start'
            obj.content_type = 'text/plain'
            await obj.store()

            vals = set(await self.generate_siblings(obj, count=5))

            # Make sure the object has five siblings...
            obj = await bucket.get(self.key_name)
            await obj.reload()
            self.assertEqual(len(obj.siblings), 5)

            # When the object is in conflict, using the shortcut methods
            # should raise the ConflictError
            with self.assertRaises(ConflictError):
                obj.data

            # Get each of the values - make sure they match what was
            # assigned
            vals2 = set([sibling.data for sibling in obj.siblings])
            self.assertEqual(vals, vals2)

            # Resolve the conflict, and then do a get...
            resolved_sibling = obj.siblings[3]
            obj.siblings = [resolved_sibling]
            await obj.store()

            await obj.reload()
            self.assertEqual(len(obj.siblings), 1)
            self.assertEqual(obj.data, resolved_sibling.data)
        self.loop.run_until_complete(go())

    def test_resolution(self):
        async def go():
            bucket = self.client.bucket(testrun_sibs_bucket)
            obj = await bucket.get(self.key_name)
            await bucket.set_property('allow_mult', True)

            # Even if it previously existed, let's store a base resolved
            # version from which we can diverge by sending a stale vclock.
            obj.data = 'start'
            obj.content_type = 'text/plain'
            await obj.store()

            vals = await self.generate_siblings(obj, count=5)

            # Make sure the object has five siblings when using the
            # default resolver
            obj = await bucket.get(self.key_name)
            await obj.reload()
            self.assertEqual(len(obj.siblings), 5)

            # Setting the resolver on the client object to use the
            # "last-write-wins" behavior
            self.client.resolver = last_written_resolver
            await obj.reload()
            self.assertEqual(obj.resolver, last_written_resolver)
            self.assertEqual(1, len(obj.siblings))
            self.assertEqual(obj.data, vals[-1])

            # Set the resolver on the bucket to the default resolver,
            # overriding the resolver on the client
            bucket.resolver = default_resolver
            await obj.reload()
            self.assertEqual(obj.resolver, default_resolver)
            self.assertEqual(len(obj.siblings), 5)

            # Define our own custom resolver on the object that returns
            # the maximum value, overriding the bucket and client resolvers
            def max_value_resolver(obj):
                obj.siblings = [max(obj.siblings, key=lambda s: s.data), ]

            obj.resolver = max_value_resolver
            await obj.reload()
            self.assertEqual(obj.resolver, max_value_resolver)
            self.assertEqual(obj.data, max(vals))

            # Setting the resolver to None on all levels reverts to the
            # default resolver.
            obj.resolver = None
            self.assertEqual(obj.resolver, default_resolver)  # set by bucket
            bucket.resolver = None
            self.assertEqual(obj.resolver, last_written_resolver)  # by client
            self.client.resolver = None
            self.assertEqual(obj.resolver, default_resolver)  # reset
            self.assertEqual(bucket.resolver, default_resolver)  # reset
            self.assertEqual(self.client.resolver, default_resolver)  # reset
        self.loop.run_until_complete(go())

    def test_resolution_default(self):
        async def go():
            # If no resolver is setup, be sure to resolve to default_resolver
            bucket = self.client.bucket(testrun_sibs_bucket)
            self.assertEqual(self.client.resolver, default_resolver)
            self.assertEqual(bucket.resolver, default_resolver)
        self.loop.run_until_complete(go())

    def test_tombstone_siblings(self):
        async def go():
            # Set up the bucket, clear any existing object...
            bucket = self.client.bucket(testrun_sibs_bucket)
            obj = await bucket.get(self.key_name)
            await bucket.set_property('allow_mult', True)

            obj.data = 'start'
            obj.content_type = 'text/plain'
            await obj.store(return_body=True)

            await obj.delete()

            vals = set(await self.generate_siblings(obj, count=4))

            obj = await bucket.get(self.key_name)

            siblen = len(obj.siblings)
            self.assertTrue(siblen == 5)

            non_tombstones = 0
            for sib in obj.siblings:
                if sib.exists:
                    non_tombstones += 1
                self.assertTrue(not sib.exists or sib.data in vals)
            self.assertEqual(non_tombstones, 4)
        self.loop.run_until_complete(go())

    def test_store_of_missing_object(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            # for json objects
            o = await bucket.get(self.key_name)
            self.assertEqual(o.exists, False)
            o.data = {"foo": "bar"}
            o.content_type = 'application/json'

            o = await o.store()
            self.assertEqual(o.data, {"foo": "bar"})
            self.assertEqual(o.content_type, "application/json")
            await o.delete()
            # for binary objects
            o = await bucket.get(self.randname())
            self.assertEqual(o.exists, False)
            o.encoded_data = '1234567890'.encode()
            o.content_type = 'application/octet-stream'

            o = await o.store()
            self.assertEqual(o.encoded_data, '1234567890'.encode())
            self.assertEqual(o.content_type, 'application/octet-stream')
            await o.delete()
        self.loop.run_until_complete(go())

    def test_store_metadata(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            rand = self.randint()
            obj = await bucket.new(self.key_name, rand)
            obj.usermeta = {'custom': 'some metadata'}
            await obj.store()
            obj = await bucket.get(self.key_name)
            self.assertEqual('some metadata', obj.usermeta['custom'])
        self.loop.run_until_complete(go())

    def test_list_buckets(self):
        async def go():
            bucket = self.client.bucket(self.bucket_name)
            obj = await bucket.new("one", {"foo": "one", "bar": "red"})
            await obj.store()
            buckets = await self.client.get_buckets()
            self.assertTrue(self.bucket_name in [x.name for x in buckets])
        self.loop.run_until_complete(go())
