import asyncio
import logging
import zlib
from collections.abc import Mapping
try:
    from contextlib import asynccontextmanager
except ImportError as err:  # pragma: no cover - unreachable on 3.7
    raise ImportError(
        "Python 3.7 is required for the connection pool "
        "via `contextlib.asynccontextmanager`"
    ) from err

from .client import RiakClient


logger = logging.getLogger('aioriak.pool')


class ConnectionPool:
    '''
    A connection pool for client connections.

    This class maintains an internal list of connections
    and establishes new connections on demand up
    until a certain bound.
    '''
    def __init__(self, max_connections, **connection_kwargs):
        '''
        Set up the connection pool.

        :param max_connections: The maximum amount of connections to keep \
        internally at any time. If this amount of connections is reached \
        and the attempt to acquire a connection is made, the caller will \
        be blocked until a connection from the pool becomes available.
        :type max_connections: int

        Any further keyword arguments are passed to riak clients when
        a new pool connection is to be established.
        '''
        self.total_connections = 0
        self.max_connections = max_connections

        self._connection_kwargs = connection_kwargs
        self._ready_connections = []
        self._semaphore = asyncio.Semaphore(value=self.max_connections)

    def __del__(self):
        '''Clean up connections to Riak.'''
        for conn in self._ready_connections:
            conn.close()

    async def establish_new_connection(self):
        '''
        Set up a new connection using the connection factory passed
        in the constructor and add it to the currently ready connections.

        You normally do not need to call this manually, it is called
        by the pool to set up a connection in `acquire`.
        '''
        new_connection = await RiakClient.create(**self._connection_kwargs)
        self.total_connections += 1
        logger.debug(
            "Established new Riak connection for pool (%d/%d).",
            self.total_connections, self.max_connections
        )
        self._ready_connections.append(new_connection)

    @asynccontextmanager
    async def acquire(self):
        '''
        Acquire a connection from the pool.
        If the pool is not at its full capacity, add a new connection to it.
        Otherwise, return one as soon as it becomes available.

        :Example:

        .. code-block:: python

            async with my_pool.acquire() as conn:
                bucket = conn.bucket('example')
                obj = await bucket.get('users:by-name:testuser')
        '''
        async with self._semaphore:

            if self.total_connections < self.max_connections:
                await self.establish_new_connection()

            acquired_connection = self._ready_connections.pop()
            try:
                yield acquired_connection
            finally:
                self._ready_connections.append(acquired_connection)
