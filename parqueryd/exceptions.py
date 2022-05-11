# TODO: move to utils maybe?
WORKER_MAX_MEMORY_KB = 2 * (2 ** 20)  # Max memory of 2GB, in Kilobytes

class RPCError(Exception):
    """Base class for exceptions in this module."""
    _message = "RPCError: There was an exception when performing the RPC action"

    def __init__(self):
        super(RPCError, self).__init__()

    def __str__(self):
        return repr(self._message)    

class FileTooBigError(RPCError):    
    _message = "RPCError: File size is too big. Files can't take more than {} KB of memory.".format(WORKER_MAX_MEMORY_KB)

class ResourceTemporarilyUnavailableError(RPCError):
    _message = "RPCError: Resource temporarily unavailable."

class TimeOutError(RPCError):
    _message = "RPCError: RPC action timed out."

class StateError(RPCError):
    _message = "RPCError: RPC operation cannot be accomplished in current state."

class UnableToConnect(RPCError):
    _message = "RPCError: Unable to connect"


class MissingDimensionError(RPCError):
    _message = "RPCError: Given dimension {} not found."

    def __init__(self, dimension_id):
        super(RPCError, self).__init__(dimension_id)
        self.dimension_id = dimension_id
        
    def __str__(self):
        m = self._message
        return m.format(self.dimension_id)        
