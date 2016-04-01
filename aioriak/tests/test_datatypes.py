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


class MapUnitTests(DatatypeUnitTestBase, unittest.TestCase):
    dtype = datatypes.Map

    def op(self, dtype):
        dtype.counters['a'].increment(2)
        dtype.registers['b'].assign('testing')
        dtype.flags['c'].enable()
        dtype.maps['d'][('e', 'set')].add('deep value')
        dtype.maps['f'].counters['g']
        dtype.maps['h'].maps['i'].flags['j']

    def check_op_output(self, op):
        self.assertIn(('update', ('a', 'counter'), ('increment', 2)), op)
        self.assertIn(('update', ('b', 'register'), ('assign', 'testing')), op)
        self.assertIn(('update', ('c', 'flag'), 'enable'), op)
        self.assertIn(('update', ('d', 'map'), [('update', ('e', 'set'),
                                                 {'adds': ['deep value']})]),
                      op)
        self.assertNotIn(('update', ('f', 'map'), None), op)
        self.assertNotIn(('update', ('h', 'map'), [('update', ('i', 'map'),
                                                    None)]), op)

    def test_removes_require_context(self):
        dtype = self.dtype(self.bucket, 'key')
        with self.assertRaises(error.ContextRequired):
            del dtype.sets['foo']

        with self.assertRaises(error.ContextRequired):
            dtype.sets['bar'].discard('xyz')

        with self.assertRaises(error.ContextRequired):
            del dtype.maps['baz'].registers['quux']

        dtype._context = 'blah'
        del dtype.sets['foo']
        self.assertTrue(dtype.modified)


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

            mtype = self.client.bucket_type('pytest-maps')
            mbucket = mtype.bucket(self.bucket_name)
            map_ = await mbucket.new(self.key_name)
            map_.sets['people'].add('Sean')
            await map_.store()

            for t in [counter, set_, map_]:
                await t.delete()
                obj = RiakObject(self.client, t.bucket, t.key)
                await self.client.get(obj)
                self.assertFalse(
                    obj.exists,
                    '{0} exists after deletion'.format(t.type_name))
        self.loop.run_until_complete(go())

    def test_dt_map(self):
        async def go():
            btype = self.client.bucket_type('pytest-maps')
            bucket = btype.bucket(self.bucket_name)
            mymap = datatypes.Map(bucket, self.key_name)

            mymap.counters['a'].increment(2)
            mymap.registers['b'].assign('testing')
            mymap.flags['c'].enable()
            mymap.maps['d'][('e', 'set')].add('deep value')
            await mymap.store()

            othermap = await bucket.get(self.key_name)

            self.assertIn('a', othermap.counters)
            self.assertIn('b', othermap.registers)
            self.assertIn('c', othermap.flags)
            self.assertIn('d', othermap.maps)

            self.assertEqual(2, othermap.counters['a'].value)
            self.assertEqual('testing', othermap.registers['b'].value)
            self.assertTrue(othermap.flags['c'].value)
            self.assertEqual({('e', 'set'): frozenset(['deep value'])},
                             othermap.maps['d'].value)
            self.assertEqual(frozenset([]), othermap.sets['f'].value)

            othermap.sets['f'].add('thing1')
            othermap.sets['f'].add('thing2')
            del othermap.counters['a']
            await othermap.store(return_body=True)

            await mymap.reload()
            self.assertNotIn('a', mymap.counters)
            self.assertIn('f', mymap.sets)
            self.assertSetEqual({'thing1', 'thing2'}, mymap.sets['f'].value)
        self.loop.run_until_complete(go())

    def test_dt_map_remove_set_update_same_op(self):
        async def go():
            btype = self.client.bucket_type('pytest-maps')
            bucket = btype.bucket(self.bucket_name)
            map = datatypes.Map(bucket, self.key_name)

            map.sets['set'].add('X')
            map.sets['set'].add('Y')
            await map.store()

            await map.reload()
            del map.sets['set']
            map.sets['set'].add('Z')
            await map.store()

            map2 = await bucket.get(self.key_name)
            self.assertSetEqual({'Z'}, map2.sets['set'].value)
        self.loop.run_until_complete(go())

    def test_dt_map_remove_counter_increment_same_op(self):
        async def go():
            btype = self.client.bucket_type('pytest-maps')
            bucket = btype.bucket(self.bucket_name)
            map = datatypes.Map(bucket, self.key_name)

            map.counters['counter'].increment(5)
            await map.store()

            await map.reload()
            self.assertEqual(5, map.counters['counter'].value)
            map.counters['counter'].increment(2)
            del map.counters['counter']
            await map.store()

            map2 = await bucket.get(self.key_name)
            self.assertEqual(2, map2.counters['counter'].value)
        self.loop.run_until_complete(go())

    def test_dt_map_remove_map_update_same_op(self):
        async def go():
            btype = self.client.bucket_type('pytest-maps')
            bucket = btype.bucket(self.bucket_name)
            map = datatypes.Map(bucket, self.key_name)

            map.maps['map'].sets['set'].add("X")
            map.maps['map'].sets['set'].add("Y")
            await map.store()

            await map.reload()
            del map.maps['map']
            map.maps['map'].sets['set'].add("Z")
            await map.store()

            map2 = await bucket.get(self.key_name)
            self.assertSetEqual({"Z"}, map2.maps['map'].sets['set'].value)
        self.loop.run_until_complete(go())

    def test_dt_map_return_body_true_default(self):
        async def go():
            btype = self.client.bucket_type('pytest-maps')
            bucket = btype.bucket(self.bucket_name)
            mymap = await bucket.new(self.key_name)
            mymap.sets['a'].add('X')
            await mymap.store(return_body=False)
            with self.assertRaises(error.ContextRequired):
                mymap.sets['a'].discard('X')
            with self.assertRaises(error.ContextRequired):
                del mymap.sets['a']

            mymap.sets['a'].add('Y')
            await mymap.store()
            self.assertSetEqual(mymap.sets['a'].value, {'X', 'Y'})

            mymap.sets['a'].discard('X')
            await mymap.store()
            self.assertSetEqual(mymap.sets['a'].value, {'Y'})

            del mymap.sets['a']
            await mymap.store()

            self.assertEqual(mymap.value, {})
        self.loop.run_until_complete(go())
