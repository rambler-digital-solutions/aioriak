class RiakError(Exception):
    '''
    Base class for exceptions generated in the Riak API.
    '''
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class ConflictError(RiakError):
    '''
    Raised when an operation is attempted on a
    :class:`~aioriak.riak_object.RiakObject` that has more than one
    sibling.
    '''
    def __init__(self, message='Object in conflict'):
        super(ConflictError, self).__init__(message)
