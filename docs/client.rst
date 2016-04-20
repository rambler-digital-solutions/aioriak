.. highlight:: python

.. module:: aioriak.client

====================
Client & Connections
====================

To connect to a Riak cluster, you must create a
:py:class:`~aioriak.client.RiakClient` object. The default configuration
connects to a Riak node on ``localhost`` with the default
ports. The below instantiation statements are all equivalent::

    from aioriak import RiakClient


    client = RiakClient()

    async def go():
        client = await RiakClient.create(host='127.0.0.1', port=8087)

.. note:: Connections are not established until you attempt to perform
   an operation. If the host or port are incorrect, you will not get
   an error raised immediately.

--------------
Client objects
--------------

.. currentmodule:: aioriak.client
.. autoclass:: RiakClient

    .. autoattribute:: resolver

-----------------------
Client-level Operations
-----------------------

Some operations are not scoped by buckets or bucket types and can be
performed on the client directly:

.. autocomethod:: RiakClient.ping
.. autocomethod:: RiakClient.get_buckets

----------------------------------
Accessing Bucket Types and Buckets
----------------------------------

Most client operations are on :py:class:`bucket type objects
<aioriak.bucket.BucketType>`, the :py:class:`bucket objects
<aioriak.bucket.Bucket>` they contain or keys within those buckets. Use the
``bucket_type`` or ``bucket`` methods for creating bucket types and buckets
that will proxy operations to the called client.

.. automethod:: RiakClient.bucket_type
.. automethod:: RiakClient.bucket
