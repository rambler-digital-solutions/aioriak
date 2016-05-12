from aioriak.datatypes import TYPES


def bucket_property(name, doc=None):
    def _prop_getter(self):
        return self.get_property(name)

    def _prop_setter(self, value):
        return self.set_property(name, value)

    return property(_prop_getter, _prop_setter, doc=doc)


class Bucket:
    '''
    The ``Bucket`` object allows you to access and change information
    about a Riak bucket, and provides async methods to create or retrieve
    objects within the bucket.
    '''

    def __init__(self, client, name, bucket_type):
        '''
        Returns a new ``Bucket`` instance.

        :param client: A :class:`RiakClient <aioriak.client.RiakClient>`
            instance
        :type client: :class:`RiakClient <aioriak.client.RiakClient>`
        :param name: The bucket name
        :type name: string
        :param bucket_type: The parent bucket type of this bucket
        :type bucket_type: :class:`BucketType`
        '''
        if not isinstance(name, str):
            raise TypeError('Bucket name must be a string')

        if not isinstance(bucket_type, BucketType):
            raise TypeError('Parent bucket type must be a BucketType instance')

        self._client = client
        self.name = name
        self.bucket_type = bucket_type
        self._encoders = {}
        self._decoders = {}
        self._resolver = None

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

    allow_mult = bucket_property(
        'allow_mult',
        doc='''If set to True, then writes with conflicting data will be stored
        and returned to the client.
        :type bool: boolean''')

    def get_decoder(self, content_type):
        '''
        Get the decoding function for the provided content type for
        this bucket.

        :param content_type: the requested media type
        :type content_type: str
        :rtype: function
        '''
        if content_type in self._decoders:
            return self._decoders[content_type]
        else:
            return self._client.get_decoder(content_type)

    def get_encoder(self, content_type):
        '''
        Get the encoding function for the provided content type for
        this bucket.

        :param content_type: the requested media type
        :type content_type: str
        :param content_type: Content type requested
        '''
        if content_type in self._encoders:
            return self._encoders[content_type]
        else:
            return self._client.get_encoder(content_type)

    def set_encoder(self, content_type, encoder):
        '''
        Set the encoding function for the provided content type for
        this bucket.

        :param content_type: the requested media type
        :type content_type: str
        :param encoder: an encoding function, takes a single object
            argument and returns a string data as single argument.
        :type encoder: function
        '''
        self._encoders[content_type] = encoder
        return self

    def set_decoder(self, content_type, decoder):
        '''
        Set the decoding function for the provided content type for
        this bucket.

        :param content_type: the requested media type
        :type content_type: str
        :param decoder: a decoding function, takes a string and
            returns a Python type
        :type decoder: function
        '''
        self._decoders[content_type] = decoder
        return self

    async def set_property(self, key, value):
        '''
        Set a bucket property.

        :param key: Property to set.
        :type key: string
        :param value: Property value.
        :type value: mixed
        '''
        return await self.set_properties({key: value})

    async def get_property(self, key):
        '''
        Retrieve a bucket property.

        :param key: The property to retrieve.
        :type key: string
        :rtype: mixed
        '''
        return (await self.get_properties())[key]

    async def set_properties(self, props):
        '''
        Set multiple bucket properties in one call.

        :param props: A dictionary of properties
        :type props: dict
        '''
        await self._client.set_bucket_props(self, props)

    async def get_properties(self):
        '''
        Retrieve a dict of all bucket properties.

        :rtype: dict
        '''
        return await self._client.get_bucket_props(self)

    async def get_keys(self):
        '''
        Return all keys within the bucket.

        :rtype: list of keys
        '''
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
        from aioriak.riak_object import RiakObject
        obj = RiakObject(self._client, self, key)
        return await obj.reload()

    async def new(self, key=None, data=None, content_type='application/json',
                  encoded_data=None):
        '''
        A shortcut for manually instantiating a new
        :class:`~aioriak.riak_object.RiakObject` or a new
        :class:`~aioriak.datatypes.Datatype`, based on the presence and value
        of the :attr:`datatype <BucketType.datatype>` bucket property. When
        the bucket contains a :class:`~aioriak.datatypes.Datatype`, all
        arguments are ignored except ``key``, otherwise they are used to
        initialize the :class:`~aioriak.riak_object.RiakObject`.

        :param key: Name of the key. Leaving this to be None (default)
                    will make Riak generate the key on store.
        :type key: str
        :param data: The data to store in a
           :class:`~aioriak.riak_object.RiakObject`, see
           :attr:`RiakObject.data <aioriak.riak_object.RiakObject.data>`.
        :type data: object
        :param content_type: The media type of the data stored in the
           :class:`~aioriak.riak_object.RiakObject`, see
           :attr:`RiakObject.content_type
           <aioriak.riak_object.RiakObject.content_type>`.
        :type content_type: str
        :param encoded_data: The encoded data to store in a
           :class:`~aioriak.riak_object.RiakObject`, see
           :attr:`RiakObject.encoded_data
           <aioriak.riak_object.RiakObject.encoded_data>`.
        :type encoded_data: str
        :rtype: :class:`~aioriak.riak_object.RiakObject` or
                :class:`~aioriak.datatypes.Datatype`
        '''
        from aioriak import RiakObject
        datatype = await self.bucket_type.get_datatype()
        if datatype:
            return TYPES[datatype](bucket=self, key=key)

        obj = RiakObject(self._client, self, key)
        obj.content_type = content_type
        if data is not None:
            obj.data = data
        if encoded_data is not None:
            obj.encoded_data = encoded_data
        return obj

    async def delete(self, key, **kwargs):
        '''
        Deletes a key from Riak. Short hand for
        ``bucket.new(key).delete()``. See :meth:`RiakClient.delete()
        <riak.client.RiakClient.delete>` for options.

        :param key: The key for the object
        :type key: string
        :rtype: RiakObject
        '''
        return await (await self.new(key)).delete(**kwargs)

    def __repr__(self):
        if self.bucket_type.is_default():
            return '<Bucket {}>'.format(self.name)
        else:
            return '<Bucket {}/{}>'.format(self.bucket_type.name,
                                           self.name)


class BucketType:
    '''
    The ``BucketType`` object allows you to access and change
    properties on a Riak bucket type and access buckets within its
    namespace.

    Async implementation of :class:`riak.bucket.BucketType`
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
            return True

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
        :rtype: :class:`Bucket`
        '''
        return self._client.bucket(name, self)

    async def get_buckets(self):
        '''
        Get the list of buckets under this bucket-type as
        :class:`Bucket <aioriak.bucket.Bucket>` instances.

        .. warning:: Do not use this in production, as it requires
           traversing through all keys stored in a cluster.

        :rtype: list of :class:`Bucket <aioriak.bucket.Bucket>`
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
