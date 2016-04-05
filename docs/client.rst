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

   .. autoattribute:: client_id
   .. autoattribute:: resolver
