from .pool import ConnectionPool


class Node:
    def __init__(self, host='localhost', port=8087):
        self.host = host
        self.port = port

    def __repr__(self):
        return "Node('{}', {})".format(self.host, self.port)


class Cluster:
    def __init__(self, nodes=None):
        if not nodes:
            nodes = [Node()]
        elif not isinstance(nodes, (list, tuple, set)):
            raise ValueError('Expected list or tuple of cleuster nodes')

        self._nodes = []

        for node in nodes:
            if isinstance(node, str):
                self._nodes.append(Node(node))
            elif isinstance(node, (list, tuple)):
                self._nodes.append(Node(node[0], node[1]))
            elif isinstance(node, Node):
                self._nodes.append(node)

        self._pool = ConnectionPool(self._nodes)

    def connect(self):
        return self._pool.acquire()
