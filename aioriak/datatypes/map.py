from .datatype import Datatype
from . import TYPES
from collections import Mapping


class Map(Mapping, Datatype):
    '''A convergent datatype that acts as a key-value datastructure. Keys
    are pairs of ``(name, datatype)`` where ``name`` is a string and
    ``datatype`` is the datatype name. Values are other convergent
    datatypes, represented by any concrete type in this module.
    You cannot set values in the map directly (it does not implement
    ``__setitem__``), but you may add new empty values or access
    non-existing values directly via bracket syntax. If a key is not in the
    original value of the map when accessed, fetching the key will cause
    its associated value to be created.::
        map[('name', 'register')]
    Keys and their associated values may be deleted from the map as
    you would in a dict::
        del map[('emails', 'set')]
    Convenience accessors exist that partition the map's keys by
    datatype and implement the :class:`~collections.Mapping`
    behavior as well as supporting deletion::
        map.sets['emails']
        map.registers['name']
        del map.counters['likes']
    '''

    type_name = 'map'


TYPES['map'] = Map
