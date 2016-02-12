from aioriak.tests.base import IntegrationTest, AsyncUnitTestCase
from aioriak.bucket import BucketType, Bucket
from aioriak.error import RiakError


class BucketTypeTests(IntegrationTest, AsyncUnitTestCase):
    def test_btype_init(self):
        btype = self.client.bucket_type('foo')
        self.assertIsInstance(btype, BucketType)
        self.assertEqual('foo', btype.name)
        self.assertIs(btype, self.client.bucket_type('foo'))

    def test_btype_get_bucket(self):
        btype = self.client.bucket_type('foo')
        bucket = btype.bucket(self.bucket_name)
        self.assertIsInstance(bucket, Bucket)
        self.assertIs(btype, bucket.bucket_type)
        self.assertIs(bucket,
                      self.client.bucket_type('foo').bucket(self.bucket_name))
        self.assertEqual(btype, BucketType(self.client, 'foo'))
        self.assertIsNot(bucket, self.client.bucket(self.bucket_name))

    def test_btype_default(self):
        defbtype = self.client.bucket_type('default')
        othertype = self.client.bucket_type('foo')
        self.assertTrue(defbtype.is_default())
        self.assertFalse(othertype.is_default())

    def test_btype_repr(self):
        defbtype = self.client.bucket_type("default")
        othertype = self.client.bucket_type("foo")
        self.assertEqual('<BucketType default>', str(defbtype))
        self.assertEqual('<BucketType foo>', str(othertype))
        self.assertEqual('<BucketType default>', repr(defbtype))
        self.assertEqual('<BucketType foo>', repr(othertype))

    def test_btype_get_props(self):
        async def go():
            defbtype = self.client.bucket_type('default')
            btype = self.client.bucket_type('pytest')
            props = await defbtype.get_properties()
            self.assertIsInstance(props, dict)
            self.assertIn('n_val', props)
            self.assertEqual(3, props['n_val'])

            props = await btype.get_properties()
            self.assertIsInstance(props, dict)
            self.assertIn('n_val', props)
            self.assertEqual(3, props['n_val'])
        self.loop.run_until_complete(go())

    def test_btype_set_props(self):
        async def go():
            defbtype = self.client.bucket_type("default")
            btype = self.client.bucket_type("pytest")
            with self.assertRaises(RiakError):
                await defbtype.set_properties({'allow_mult': True})

            oldprops = await btype.get_properties()
            try:
                await btype.set_properties({'allow_mult': True})
                newprops = await btype.get_properties()
                self.assertIsInstance(newprops, dict)
                self.assertIn('allow_mult', newprops)
                self.assertTrue(newprops['allow_mult'])
                if 'claimant' in oldprops:  # HTTP hack
                    del oldprops['claimant']
            finally:
                await btype.set_properties(oldprops)
        self.loop.run_until_complete(go())

    def test_btype_set_props_immutable(self):
        async def go():
            btype = self.client.bucket_type("pytest-maps")
            with self.assertRaises(RiakError):
                await btype.set_property('datatype', 'counter')
        self.loop.run_until_complete(go())

    def test_btype_list_buckets(self):
        async def go():
            btype = self.client.bucket_type('py-test')
            bucket = btype.bucket(self.bucket_name)
            obj = await bucket.new(self.key_name)
            obj.data = [1, 2, 3]
            await obj.store()

            self.assertIn(bucket, await btype.get_buckets())
        self.loop.run_until_complete(go())

    def test_btype_list_keys(self):
        async def go():
            btype = self.client.bucket_type('py-test')
            bucket = btype.bucket(self.bucket_name)

            obj = await bucket.new(self.key_name)
            obj.data = [1, 2, 3]
            await obj.store()

            self.assertIn(self.key_name, await bucket.get_keys())
        self.loop.run_until_complete(go())

    def test_default_btype_list_buckets(self):
        async def go():
            default_btype = self.client.bucket_type('default')
            bucket = default_btype.bucket(self.bucket_name)
            obj = await bucket.new(self.key_name)
            obj.data = [1, 2, 3]
            await obj.store()

            buckets = await default_btype.get_buckets()
            self.assertIn(bucket, buckets)
            self.assertCountEqual(buckets, await self.client.get_buckets())
            # self.assertListEqual(buckets, await self.client.get_buckets())
        self.loop.run_until_complete(go())

    def test_default_btype_list_keys(self):
        async def go():
            btype = self.client.bucket_type('default')
            bucket = btype.bucket(self.bucket_name)

            obj = await bucket.new(self.key_name)
            obj.data = [1, 2, 3]
            await obj.store()

            keys = await bucket.get_keys()
            self.assertIn(self.key_name, keys)

            oldapikeys = await self.client.get_keys(
                self.client.bucket(self.bucket_name))
            self.assertCountEqual(keys, oldapikeys)
        self.loop.run_until_complete(go())
