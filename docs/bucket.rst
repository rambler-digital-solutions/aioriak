.. _bucket_types:

======================
Buckets & Bucket Types
======================

.. currentmodule:: aioriak.bucket

**Buckets** are both namespaces for the key-value pairs you store in
Riak, and containers for properties that apply to that namespace. In
older versions of Riak, this was the only logical organization
available. Now a higher-level collection called a **Bucket Type** can
group buckets together. They allow for efficiently setting properties
on a group of buckets at the same time.

Unlike buckets, Bucket Types must be `explicitly created
<http://docs.basho.com/riak/2.0.0/dev/advanced/bucket-types/#Managing-Bucket-Types-Through-the-Command-Line>`_
and activated before being used::

   riak-admin bucket-type create n_equals_1 '{"props":{"n_val":1}}'
   riak-admin bucket-type activate n_equals_1

Bucket Type creation and activation is only supported via the
``riak-admin bucket-type`` command-line tool. Riak 2.0 does not
include an API to perform these actions, but the Python client *can*
:meth:`retrieve <BucketType.get_properties>` and :meth:`set
<BucketType.set_properties>` bucket-type properties.

If Bucket Types are not specified, the *default* bucket
type is used.  These buckets should be created via the :meth:`bucket()
<aioriak.client.RiakClient.bucket>` method on the client object, like so::

    import aioriak

    async def go():
        client = await aioriak.RiakClient.create()
        mybucket = client.bucket('mybucket')

Buckets with a user-specified Bucket Type can also be created via the same
:meth:`bucket()<aioriak.client.RiakClient.bucket>` method with
an additional parameter or explicitly via
:meth:`bucket_type()<aioriak.client.RiakClient.bucket_type>`::

    othertype = client.bucket_type('othertype')
    otherbucket = othertype.bucket('otherbucket')

    # Alternate way to get a bucket within a bucket-type
    mybucket = client.bucket('mybucket', bucket_type='mybuckettype')

For more detailed discussion, see `Using Bucket Types
<http://docs.basho.com/riak/2.0.0/dev/advanced/bucket-types/>`_.

--------------
Bucket objects
--------------

.. autoclass:: Bucket

    .. automethod:: __init__ 

    .. attribute:: name

        The name of the bucket, a string.

    .. attribute:: bucket_type

        The parent :class:`BucketType` for the bucket.

    .. autoattribute:: resolver

-----------------
Bucket properties
-----------------

Bucket properties are flags and defaults that apply to all keys in the
bucket.

.. autocomethod:: Bucket.get_properties
.. autocomethod:: Bucket.set_properties
.. autocomethod:: Bucket.get_property
.. autocomethod:: Bucket.set_property

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Shortcuts for common properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some of the most commonly-used bucket properties are exposed as object
properties as well. The getters and setters simply call
:meth:`Bucket.get_property` and :meth:`Bucket.set_property`
respectively.

.. autoattribute:: Bucket.allow_mult

-----------------
Working with keys
-----------------

The primary purpose of buckets is to act as namespaces for keys. As
such, you can use the bucket object to create, fetch and delete
:class:`objects <aioriak.riak_object.RiakObject>`.

.. autocomethod:: Bucket.new
.. autocomethod:: Bucket.get
.. autocomethod:: Bucket.delete

-------------
Serialization
-------------

Similar to :class:`RiakClient <aioriak.client.RiakClient>`, buckets can
register custom transformation functions for media-types. When
undefined on the bucket, :meth:`Bucket.get_encoder` and
:meth:`Bucket.get_decoder` will delegate to the client associated
with the bucket.

.. automethod:: Bucket.get_encoder
.. automethod:: Bucket.set_encoder
.. automethod:: Bucket.get_decoder
.. automethod:: Bucket.set_decoder

------------
Listing keys
------------

Shortcuts for :meth:`RiakClient.get_keys()
<aioriak.client.RiakClient.get_keys>` are exposed on the bucket
object. The same admonitions for these operation apply.

.. autocomethod:: Bucket.get_keys

-------------------
Bucket Type objects
-------------------

.. autoclass:: BucketType

    .. automethod:: __init__

    .. attribute:: name

        The name of the Bucket Type, a string.

.. automethod:: BucketType.is_default

.. automethod:: BucketType.bucket

----------------------
Bucket Type properties
----------------------

Bucket Type properties are flags and defaults that apply to all buckets in the
Bucket Type.

.. autocomethod:: BucketType.get_properties
.. autocomethod:: BucketType.set_properties
.. autocomethod:: BucketType.get_property
.. autocomethod:: BucketType.set_property
.. attribute:: BucketType.datatype

    The assigned datatype for this bucket type, if present.

---------------
Listing buckets
---------------

Shortcut for :meth:`RiakClient.get_buckets()
<aioriak.client.RiakClient.get_buckets>` is exposed on the bucket
type object.  This is similar to `Listing keys`_ on buckets.

.. autocomethod:: BucketType.get_buckets
