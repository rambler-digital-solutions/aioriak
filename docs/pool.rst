.. highlight:: python

.. currentmodule:: aioriak.pool

===============
Connection Pool
===============

For Python 3.7+, aioriak brings a connection pool class which
internally manages a configured amount of Riak connections. Connections
are opened on-demand and a semaphore is used to ensure that connections
are used only by one caller at a time.

----------------------
ConnectionPool objects
----------------------

.. autoclass:: ConnectionPool
   :members: __init__, acquire, establish_new_connection
