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

    def get_decoder(self, content_type):
        """
        Get the decoding function for the provided content type for
        this bucket.
        :param content_type: the requested media type
        :type content_type: str
        :rtype: function
        """
        if content_type in self._decoders:
            return self._decoders[content_type]
        else:
            return self._client.get_decoder(content_type)

    async def get_keys(self):
        """
        Return all keys within the bucket.
        :rtype: list of keys
        """
        return await self._client.get_keys(self)

    async def get(self, key):
        '''
        Retrieve an :class:`~aioriak.riak_object.RiakObject` or
        :class:`~aioriak.datatypes.Datatype`, based on the presence and value
        of the :attr:`datatype <BucketType.datatype>` bucket property.
        :param key: Name of the key.
        :type key: string
        :rtype: :class:`RiakObject <aioriak.riak_object.RiakObject>` or
           :class:`~aioriak.datatypes.Datatype`
        '''
        if await self.bucket_type.get_datatype():
            return await self._client.fetch_datatype(self, key)
        from riak_object import RiakObject
        obj = RiakObject(self._client, self, key)
        return await obj.reload()

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

    def _get_resolver(self):
        if callable(self._resolver):
            return self._resolver
        elif self._resolver is None:
            return self._client.resolver
        else:
            raise TypeError("resolver is not a function")

    def _set_resolver(self, value):
        if value is None or callable(value):
            self._resolver = value
        else:
            raise TypeError("resolver is not a function")

    resolver = property(_get_resolver, _set_resolver,
                        doc='''The sibling-resolution function for this
                        bucket. If the resolver is not set, the
                        client's resolver will be used.''')

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

    async def get_datatype(self):
        '''
        The assigned datatype for this bucket type, if present.
        :rtype: None or string
        '''
        if not hasattr(self, '_datatype'):
            self._datatype = (await self.get_properties()).get('datatype')
        return self._datatype
