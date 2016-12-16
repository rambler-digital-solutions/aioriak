__version__ = '0.0.1'

from .client import RiakClient
from .riak_object import RiakObject
from .mapreduce import RiakMapReduce


__all__ = ('RiakClient', 'RiakObject', 'RiakMapReduce')
