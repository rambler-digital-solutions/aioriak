import pytest
from aioriak.cluster import Cluster, Node


class TestCluster:
    @pytest.mark.asyncio
    async def test_init_default(self):
        cluster = Cluster()
        assert len(cluster._nodes) == 1
        assert cluster._nodes[0].host == 'localhost'

    @pytest.mark.asyncio
    async def test_init_by_list(self):
        cluster = Cluster(['127.0.0.1', '127.0.0.2'])
        assert len(cluster._nodes) == 2
        assert cluster._nodes[1].host == '127.0.0.2'

    @pytest.mark.asyncio
    async def test_init_by_tuple(self):
        cluster = Cluster(('127.0.0.1', '127.0.0.2'))
        assert len(cluster._nodes) == 2
        assert cluster._nodes[1].host == '127.0.0.2'

    @pytest.mark.asyncio
    async def test_init_by_tuple_with_port(self):
        cluster = Cluster((('127.0.0.1', '8087'), ('127.0.0.2', 8088)))
        assert len(cluster._nodes) == 2
        assert cluster._nodes[1].host == '127.0.0.2'
        assert cluster._nodes[1].port == 8088

    @pytest.mark.asyncio
    async def test_init_by_list_with_port(self):
        cluster = Cluster([('127.0.0.1', '8087'), ['127.0.0.2', 8088]])
        assert len(cluster._nodes) == 2
        assert cluster._nodes[1].host == '127.0.0.2'
        assert cluster._nodes[1].port == 8088

    @pytest.mark.asyncio
    async def test_init_by_nodes(self):
        cluster = Cluster((Node('localhost', 8087), Node('127.0.0.2', 8089)))
        assert len(cluster._nodes) == 2
        assert cluster._nodes[1].host == '127.0.0.2'
        assert cluster._nodes[1].port == 8089

    def test_init_by_invalid_data(self):
        with pytest.raises(ValueError):
            Cluster('invalid_hostname')


class TestNode:
    def test_repr(self):
        node = Node('google.ru', 1488)
        assert repr(node) == "Node('google.ru', 1488)"
