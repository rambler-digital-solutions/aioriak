from aioriak.tests.base import IntegrationTest, AsyncUnitTestCase
from aioriak.bucket import BucketType, Bucket


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
        self.assertIs(btype, BucketType(self.client, 'foo'))
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
            defbtype = self.client.bucket_type("default")
            btype = self.client.bucket_type("pytest")
            with self.assertRaises(ValueError):
                await defbtype.get_properties()

            props = await btype.get_properties()
            self.assertIsInstance(props, dict)
            self.assertIn('n_val', props)
            self.assertEqual(3, props['n_val'])
        self.loop.run_until_complete(go())
