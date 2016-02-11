class RiakError(Exception):
    '''
    Base class for exceptions generated in the Riak API.
    '''
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
