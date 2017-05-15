import asyncio
from collections import deque


POOL_SIZE = 10


class Connection:
    def __init__(self, reader, writer, node=None, loop=None):
        self.reader = reader
        self.writer = writer
        self._lock = asyncio.Lock()
        self._node = node
        self._loop = loop or asyncio.get_event_loop()

    @classmethod
    async def create(cls, node, loop=None):
        reader, writer = await asyncio.open_connection(
            node.host, node.port, loop=loop)
        return cls(reader, writer)


class ConnectionPool:
    def __init__(self, nodes, loop=None):
        self._nodes = nodes
        self._connections = deque()
        self._used_connections = set()
        self._loop = loop or asyncio.get_event_loop()

    async def acquire(self):
        if self.connections:
            connection = self._connections.popleft()
        else:
            connection = await Connection.create()
        self._used_connections.add(connection)
        return connection
