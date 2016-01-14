# from riak import bucket


class Bucket:
    """
    The ``Bucket`` object allows you to access and change information
    about a Riak bucket, and provides async methods to create or retrieve
    objects within the bucket.
    """

    def __init__(self, client, name, bucket_type):
        """
        Returns a new ``Bucket`` instance.
        :param client: A :class:`RiakClient <aioriak.client.RiakClient>`
            instance
        :type client: :class:`RiakClient <aioriak.client.RiakClient>`
        :param name: The bucket name
        :type name: string
        :param bucket_type: The parent bucket type of this bucket
        :type bucket_type: :class:`BucketType`
        """
        try:
            if isinstance(name, str):
                name = name.encode('ascii').decode()
            else:
                raise TypeError('Bucket name must be a string')
        except UnicodeError:
            raise TypeError('Unicode bucket names are not supported.')

        if not isinstance(bucket_type, BucketType):
            raise TypeError('Parent bucket type must be a BucketType instance')

        self._client = client
        self.name = name
        self.bucket_type = bucket_type
        self._encoders = {}
        self._decoders = {}
        self._resolver = None

    async def get_keys(self):
        """
        Return all keys within the bucket.
        :rtype: list of keys
        """
        return await self._client.get_keys(self)

    def __repr__(self):
        if self.bucket_type.is_default():
            return '<RiakBucket {}>'.format(self.name)
        else:
            return '<RiakBucket {}/{}>'.format(self.bucket_type.name,
                                               self.name)


class BucketType:
    '''
    The ``BucketType`` object allows you to access and change
    properties on a Riak bucket type and access buckets within its
    namespace.

    Async implementation of riak.bucket.BucketType
    '''
    def __init__(self, client, name):
        '''
        Returns a new ``BucketType`` instance.
        :param client: A :class:`RiakClient <aioriak.client.RiakClient>`
               instance
        :type client: :class:`RiakClient <aioriak.client.RiakClient>`
        :param name: The bucket-type's name
        :type name: string
        '''
        self._client = client
        self.name = name

    def __repr__(self):
        return "<BucketType {0}>".format(self.name)

    def is_default(self):
        '''
        Whether this bucket type is the default type, or a user-defined type.
        :rtype: bool
        '''
        return self.name == 'default'

    async def get_property(self, key):
        '''
        Retrieve a bucket-type property.
        :param key: The property to retrieve.
        :type key: string
        :rtype: mixed
        '''
        return await self.get_properties()[key]

    async def set_property(self, key, value):
        '''
        Set a bucket-type property.
        :param key: Property to set.
        :type key: string
        :param value: Property value.
        :type value: mixed
        '''
        await self.set_properties({key: value})

    async def get_properties(self):
        '''
        Retrieve a dict of all bucket-type properties.
        :rtype: dict
        '''
        return await self._client.get_bucket_type_props(self)

    async def set_properties(self, props):
        '''
        Set multiple bucket-type properties in one call.
        :param props: A dictionary of properties
        :type props: dict
        '''
        await self._client.set_bucket_type_props(self, props)

    def bucket(self, name):
        '''
        Gets a bucket that belongs to this bucket-type.
        :param name: the bucket name
        :type name: str
        :rtype: :class:`RiakBucket`
        '''
        return self._client.bucket(name, self)

    async def get_buckets(self):
        '''
        Get the list of buckets under this bucket-type as
        :class:`Bucket <aioriak.bucket.Bucket>` instances.
        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.
        :rtype: list of :class:`Bucket <riak.bucket.Bucket>`
                instances
        '''
        return await self._client.get_buckets(bucket_type=self)

    '''def stream_buckets(self, timeout=None):
        """
        Streams the list of buckets under this bucket-type. This is a
        generator method that should be iterated over.
        The caller must close the stream when finished.  See
        :meth:`RiakClient.stream_buckets()
        <riak.client.RiakClient.stream_buckets>` for more details.
        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.
        :param timeout: a timeout value in milliseconds
        :type timeout: int
        :rtype: iterator that yields lists of :class:`RiakBucket
             <riak.bucket.RiakBucket>` instances
        """
        return self._client.stream_buckets(bucket_type=self, timeout=timeout)

    @lazy_property
    def datatype(self):
        """
        The assigned datatype for this bucket type, if present.
        :rtype: None or string
        """
        if self.is_default():
            return None
        else:
            return self.get_properties().get('datatype')

    def __str__(self):
        return "<BucketType {0!r}>".format(self.name)

    __repr__ = __str__

    def __hash__(self):
        return hash((self.name, self._client))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return hash(self) == hash(other)
        else:
            return False

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return hash(self) != hash(other)
        else:
            return True'''
