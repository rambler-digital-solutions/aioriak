import unittest
from aioriak.bucket import Bucket, BucketType
from aioriak import datatypes
from aioriak.tests.base import IntegrationTest, AsyncUnitTestCase


class DatatypeUnitTestBase:
    dtype = None
    bucket = Bucket(None, 'test', BucketType(None, 'datatypes'))

    def op(self, dtype):
        raise NotImplementedError

    def check_op_output(self, op):
        raise NotImplementedError

    def test_new_type_is_clean(self):
        newtype = self.dtype(self.bucket, 'key')
        self.assertIsNone(newtype.to_op())

    def test_modified_type_has_op(self):
        newtype = self.dtype(self.bucket, 'key')
        self.op(newtype)
        self.assertIsNotNone(newtype.to_op())

    def test_protected_attrs_not_settable(self):
        newtype = self.dtype(self.bucket, 'key')
        for i in ('value', 'context'):
            with self.assertRaises(AttributeError):
                setattr(newtype, i, 'foo')

    def test_modified_type_has_unmodified_value(self):
        newtype = self.dtype(self.bucket, 'key')
        oldvalue = newtype.value
        self.op(newtype)
        self.assertEqual(oldvalue, newtype.value)

    def test_op_output(self):
        newtype = self.dtype(self.bucket, 'key')
        self.op(newtype)
        op = newtype.to_op()
        self.check_op_output(op)


class CounterUnitTests(DatatypeUnitTestBase, unittest.TestCase):
    dtype = datatypes.Counter

    def op(self, dtype):
        dtype.increment(5)

    def check_op_output(self, op):
        self.assertEqual(('increment', 5), op)


class DatatypeIntegrationTests(IntegrationTest,
                               AsyncUnitTestCase):
    def test_dt_counter(self):
        async def go():
            btype = self.client.bucket_type('pytest-counters')
            bucket = btype.bucket(self.bucket_name)
            mycount = datatypes.Counter(bucket, self.key_name)
            mycount.increment(5)
            await mycount.store()

            othercount = await bucket.get(self.key_name)
            self.assertEqual(5, othercount.value)
            print(type(mycount), type(othercount))

            othercount.decrement(3)
            await othercount.store()

            await mycount.reload()
            self.assertEqual(2, mycount.value)
        self.loop.run_until_complete(go())
