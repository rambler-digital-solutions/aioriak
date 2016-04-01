import unittest
from aioriak.bucket import Bucket, BucketType
from aioriak import datatypes
from aioriak.tests.base import IntegrationTest, AsyncUnitTestCase
from aioriak import error
from aioriak.riak_object import RiakObject


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


class FlagUnitTests(DatatypeUnitTestBase, unittest.TestCase):
    dtype = datatypes.Flag

    def op(self, dtype):
        dtype.enable()

    def check_op_output(self, op):
        self.assertEqual('enable', op)

    def test_disables_require_context(self):
        dtype = self.dtype(self.bucket, 'key')
        with self.assertRaises(error.ContextRequired):
            dtype.disable()

        dtype._context = 'blah'
        dtype.disable()
        self.assertTrue(dtype.modified)


class RegisterUnitTests(DatatypeUnitTestBase, unittest.TestCase):
    dtype = datatypes.Register

    def op(self, dtype):
        dtype.assign('foobarbaz')

    def check_op_output(self, op):
        self.assertEqual(('assign', 'foobarbaz'), op)


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

            othercount.decrement(3)
            await othercount.store()

            await mycount.reload()
            self.assertEqual(2, mycount.value)
        self.loop.run_until_complete(go())

    def test_dt_set(self):
        async def go():
            btype = self.client.bucket_type('pytest-sets')
            bucket = btype.bucket(self.bucket_name)
            myset = datatypes.Set(bucket, self.key_name)
            myset.add('Sean')
            myset.add('Brett')
            await myset.store()

            otherset = await bucket.get(self.key_name)

            self.assertIn('Sean', otherset)
            self.assertIn('Brett', otherset)

            otherset.add('Russell')
            otherset.discard('Sean')
            await otherset.store()

            await myset.reload()
            self.assertIn('Russell', myset)
            self.assertIn('Brett', myset)
            self.assertNotIn('Sean', myset)
        self.loop.run_until_complete(go())

    def test_dt_set_remove_without_context(self):
        async def go():
            btype = self.client.bucket_type('pytest-sets')
            bucket = btype.bucket(self.bucket_name)
            set = datatypes.Set(bucket, self.key_name)

            set.add("X")
            set.add("Y")
            set.add("Z")
            with self.assertRaises(error.ContextRequired):
                set.discard("Y")
        self.loop.run_until_complete(go())

    def test_dt_set_remove_fetching_context(self):
        async def go():
            btype = self.client.bucket_type('pytest-sets')
            bucket = btype.bucket(self.bucket_name)
            set = datatypes.Set(bucket, self.key_name)

            set.add('X')
            set.add('Y')
            await set.store()

            await set.reload()
            set.discard('bogus')
            await set.store()

            set2 = await bucket.get(self.key_name)
            self.assertSetEqual({'X', 'Y'}, set2.value)
        self.loop.run_until_complete(go())

    def test_dt_set_add_twice(self):
        async def go():
            btype = self.client.bucket_type('pytest-sets')
            bucket = btype.bucket(self.bucket_name)
            set = datatypes.Set(bucket, self.key_name)

            set.add('X')
            set.add('Y')
            await set.store()

            await set.reload()
            set.add('X')
            await set.store()

            set2 = await bucket.get(self.key_name)
            self.assertSetEqual({'X', 'Y'}, set2.value)
        self.loop.run_until_complete(go())

    def test_dt_set_add_wins_in_same_op(self):
        async def go():
            btype = self.client.bucket_type('pytest-sets')
            bucket = btype.bucket(self.bucket_name)
            set = datatypes.Set(bucket, self.key_name)

            set.add('X')
            set.add('Y')
            await set.store()

            await set.reload()
            set.add('X')
            set.discard('X')
            await set.store()

            set2 = await bucket.get(self.key_name)
            self.assertSetEqual({'X', 'Y'}, set2.value)
        self.loop.run_until_complete(go())

    def test_dt_set_add_wins_in_same_op_reversed(self):
        async def go():
            btype = self.client.bucket_type('pytest-sets')
            bucket = btype.bucket(self.bucket_name)
            set = datatypes.Set(bucket, self.key_name)

            set.add('X')
            set.add('Y')
            await set.store()

            await set.reload()
            set.discard('X')
            set.add('X')
            await set.store()

            set2 = await bucket.get(self.key_name)
            self.assertSetEqual({'X', 'Y'}, set2.value)
        self.loop.run_until_complete(go())

    def test_dt_set_remove_old_context(self):
        async def go():
            btype = self.client.bucket_type('pytest-sets')
            bucket = btype.bucket(self.bucket_name)
            set = datatypes.Set(bucket, self.key_name)

            set.add('X')
            set.add('Y')
            await set.store()

            await set.reload()

            set_parallel = datatypes.Set(bucket, self.key_name)
            set_parallel.add('Z')
            await set_parallel.store()

            set.discard('Z')
            await set.store()

            set2 = await bucket.get(self.key_name)
            self.assertSetEqual({'X', 'Y', 'Z'}, set2.value)
        self.loop.run_until_complete(go())

    def test_dt_set_remove_updated_context(self):
        async def go():
            btype = self.client.bucket_type('pytest-sets')
            bucket = btype.bucket(self.bucket_name)
            set = datatypes.Set(bucket, self.key_name)

            set.add('X')
            set.add('Y')
            await set.store()

            set_parallel = datatypes.Set(bucket, self.key_name)
            set_parallel.add('Z')
            await set_parallel.store()

            await set.reload()
            set.discard('Z')
            await set.store()

            set2 = await bucket.get(self.key_name)
            self.assertSetEqual({'X', 'Y'}, set2.value)
        self.loop.run_until_complete(go())

    def test_dt_set_return_body_true_default(self):
        async def go():
            btype = self.client.bucket_type('pytest-sets')
            bucket = btype.bucket(self.bucket_name)
            myset = await bucket.new(self.key_name)
            myset.add('X')
            await myset.store(return_body=False)
            with self.assertRaises(error.ContextRequired):
                myset.discard('X')

            myset.add('Y')
            await myset.store()
            self.assertSetEqual(myset.value, {'X', 'Y'})

            myset.discard('X')
            await myset.store()
            self.assertSetEqual(myset.value, {'Y'})
        self.loop.run_until_complete(go())

    def test_delete_datatype(self):
        async def go():
            ctype = self.client.bucket_type('pytest-counters')
            cbucket = ctype.bucket(self.bucket_name)
            counter = await cbucket.new(self.key_name)
            counter.increment(5)
            await counter.store()

            stype = self.client.bucket_type('pytest-sets')
            sbucket = stype.bucket(self.bucket_name)
            set_ = await sbucket.new(self.key_name)
            set_.add("Brett")
            await set_.store()

            '''mtype = self.client.bucket_type('pytest-maps')
            mbucket = mtype.bucket(self.bucket_name)
            map_ = await mbucket.new(self.key_name)
            map_.sets['people'].add('Sean')
            await map_.store()'''

            for t in [counter, set_]:
                await t.delete()
                obj = RiakObject(self.client, t.bucket, t.key)
                await self.client.get(obj)
                self.assertFalse(
                    obj.exists,
                    '{0} exists after deletion'.format(t.type_name))
        self.loop.run_until_complete(go())
