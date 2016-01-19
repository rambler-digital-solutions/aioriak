import os
import logging
os.environ['PYTHONASYNCIODEBUG'] = '1'
# logging.basicConfig(level=logging.DEBUG)

import asyncio
from weakref import WeakValueDictionary
from transport import create_transport
from bucket import BucketType, Bucket


logger = logging.getLogger('aioriak.client')


class RiakClient:
    def __init__(self, host='localhost', port=8087, loop=None):
        self._host = host
        self._port = port
        self._loop = loop
        self._bucket_types = WeakValueDictionary()
        self._buckets = WeakValueDictionary()

    async def _create_transport(self):
        self._transport = await create_transport(
            self._host, self._port, self._loop)

    @classmethod
    async def create(cls, host='localhost', port=8087, loop=None):
        '''
        Return initialized instance of RiakClient since
        RiakClient.__init__() can not ne async.

        client =  await RiakClient.create('localhost', loop)
        :rtupe: RiakClient
        '''
        client = cls(host, port, loop)
        await client._create_transport()
        return client

    async def ping(self):
        '''
        Check if the Riak server for this ``RiakClient`` instance is alive.
        @TODO: .. note:: This request is automatically retried :attr:`retries`
           times if it fails due to network error.
        :rtype: boolean
        '''
        return await self._transport.ping()

    async def get_client_id(self):
        'Client ID for this RiakClient instance'
        return await self._transport.get_client_id()

    async def set_client_id(self, id):
        'Set Client ID for this RiakClient instance'
        return await self._transport.set_client_id(id)

    async def get_buckets(self, bucket_type=None):
        """
        get_buckets(bucket_type=None)
        Get the list of buckets as :class:`Bucket
        <aioriak.bucket.Bucket>` instances.
        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.
        :param bucket_type: the optional containing bucket type
        :type bucket_type: :class:`~riak.bucket.BucketType`
        :rtype: list of :class:`Bucket <aioriak.bucket.Bucket>`
                instances
        """
        if bucket_type:
            maker = bucket_type.bucket
        else:
            maker = self.bucket

        return [maker(name.decode())
                for name in await self._transport.get_buckets(
                    bucket_type=bucket_type)]

    def bucket_type(self, name):
        """
        Gets the bucket-type by the specified name. Bucket-types do
        not always exist (unlike buckets), but this will always return
        a :class:`BucketType <riak.bucket.BucketType>` object.
        :param name: the bucket name
        :type name: str
        :rtype: :class:`BucketType <riak.bucket.BucketType>`
        """
        if not isinstance(name, str):
            raise TypeError('Bucket name must be a string')

        if name in self._bucket_types:
            return self._bucket_types[name]
        else:
            btype = BucketType(self, name)
            self._bucket_types[name] = btype
            return btype

    def bucket(self, name, bucket_type='default'):
        """
        Get the bucket by the specified name. Since buckets always exist,
        this will always return a
        :class:`RiakBucket <aioriak.bucket.RiakBucket>`.
        If you are using a bucket that is contained in a bucket type, it is
        preferable to access it from the bucket type object::
            # Preferred:
            client.bucket_type("foo").bucket("bar")
            # Equivalent, but not preferred:
            client.bucket("bar", bucket_type="foo")
        :param name: the bucket name
        :type name: str
        :param bucket_type: the parent bucket-type
        :type bucket_type: :class:`BucketType <riak.bucket.BucketType>`
              or str
        :rtype: :class:`RiakBucket <riak.bucket.RiakBucket>`
        """
        if not isinstance(name, str):
            raise TypeError('Bucket name must be a string')

        if isinstance(bucket_type, str):
            bucket_type = self.bucket_type(bucket_type)
        elif not isinstance(bucket_type, BucketType):
            raise TypeError('bucket_type must be a string '
                            'or riak.bucket.BucketType')

        return self._buckets.setdefault((bucket_type, name),
                                        Bucket(self, name, bucket_type))

    async def get_bucket_type_props(self, bucket_type):
        """
        get_bucket_type_props(bucket_type)
        Fetches properties for the given bucket-type.
        :param bucket_type: the bucket-type whose properties will be fetched
        :type bucket_type: BucketType
        :rtype: dict
        """
        return await self._transport.get_bucket_type_props(bucket_type)

    async def set_bucket_type_props(self, bucket_type, props):
        """
        set_bucket_type_props(bucket_type, props)
        Sets properties for the given bucket-type.
        :param bucket_type: the bucket-type whose properties will be set
        :type bucket_type: BucketType
        :param props: the properties to set
        :type props: dict
        """
        return await self._transport.set_bucket_type_props(bucket_type, props)

    async def get_keys(self, bucket):
        """
        get_keys(bucket)
        Lists all keys in a bucket.
        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.
        :param bucket: the bucket whose keys are fetched
        :type bucket: Bucket
        :rtype: list
        """
        return await self._transport.get_keys(bucket)

    async def get(self, robj):
        '''
        get(robj)
        Fetches the contents of a Riak object.
        :param robj: the object to fetch
        :type robj: RiakObject
        '''
        if not isinstance(robj.key, str):
            raise TypeError(
                'key must be a string, instead got {0}'.format(repr(robj.key)))

        return await self._transport.get(robj)


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
        '''bucket = (await bucket_type.get_buckets())[0]
        print(bucket)
        keys = await bucket.get_keys()
        print(keys)
        res = await client.get_client_id()'''
        obj = await bucket_type.bucket('counters').get(
            '2016-01-02-5e43e39ff4d644fbbc805f2370660276-lenta')
        print(obj)

        '''res = await conn.get(
            bucket_type=b'counter_map', bucket=b'counters',
            key=b'2015-12-51-578c8259a3d946dab994abb97717a469-lenta')
        print(res)'''

    loop.run_until_complete(test())
