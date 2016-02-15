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


class ContextRequired(RiakError):
    '''
    This exception is raised when removals of map fields and set
    entries are attempted and the datatype hasn't been initialized
    with a context.
    '''

    _default_message = ('A context is required for remove operations, '
                        'fetch the datatype first')

    def __init__(self, message=None):
        super(ContextRequired, self).__init__(message or
                                              self._default_message)
