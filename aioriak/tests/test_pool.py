import sys
import unittest

from aioriak.tests.base import AsyncUnitTestCase, IntegrationTest


@unittest.skipIf(
    sys.version_info < (3, 7),
    "requires Python 3.7 for contextlib.asynccontextmanager"
)
class PoolTests(IntegrationTest, AsyncUnitTestCase):
    def test_does_not_exceed_pool_size(self):
        async def go():
            from aioriak.pool import ConnectionPool

            pool = ConnectionPool(
                max_connections=2,
                host=self.client._host
            )
            for _ in range(5):
                async with pool.acquire() as conn:
                    pong = await conn.ping()
                    self.assertTrue(pong)

            self.assertEqual(pool.total_connections, 2)
            self.assertEqual(pool.max_connections, 2)
            self.assertEqual(len(pool._ready_connections), 2)

        self.loop.run_until_complete(go())

    def test_pops_from_ready_connections(self):
        async def go():
            from aioriak.pool import ConnectionPool

            pool = ConnectionPool(
                max_connections=2,
                host=self.client._host
            )

            async with pool.acquire() as conn:
                self.assertNotIn(conn, pool._ready_connections)
            self.assertIn(conn, pool._ready_connections)

        self.loop.run_until_complete(go())

    def test_adds_connections_one_by_one(self):
        async def go():
            from aioriak.pool import ConnectionPool

            pool = ConnectionPool(
                max_connections=10,
                host=self.client._host
            )

            for n in range(10):
                async with pool.acquire() as conn:
                    pong = await conn.ping()
                    self.assertTrue(pong)
                    self.assertEqual(pool.total_connections, n + 1)

        self.loop.run_until_complete(go())
