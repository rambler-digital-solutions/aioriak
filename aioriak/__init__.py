__version__ = '0.2.0'

from .client import RiakClient
from .riak_object import RiakObject
from .mapreduce import RiakMapReduce


__all__ = ('RiakClient', 'RiakObject', 'RiakMapReduce')
