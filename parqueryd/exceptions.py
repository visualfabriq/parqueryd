from __future__ import annotations

# TODO: move to utils maybe?
WORKER_MAX_MEMORY_KB: int = 2 * (2**20)  # Max memory of 2GB, in Kilobytes


class RPCError(Exception):
    """Base class for exceptions in this module."""

    _message: str = "RPCError: There was an exception when performing the RPC action"

    def __init__(self, message: str | None = None) -> None:
        self.message: str | None = message
        super().__init__()

    def __str__(self) -> str:
        return str(self.message or self._message)


class FileTooBigError(RPCError):
    _message: str = f"RPCError: File size is too big. Files can't take more than {WORKER_MAX_MEMORY_KB} KB of memory."


class ResourceTemporarilyUnavailableError(RPCError):
    _message: str = "RPCError: Resource temporarily unavailable."


class TimeOutError(RPCError):
    _message: str = "RPCError: RPC action timed out."


class StateError(RPCError):
    _message: str = "RPCError: RPC operation cannot be accomplished in current state."


class UnableToConnect(RPCError):
    _message: str = "RPCError: Unable to connect"


class MissingDimensionError(RPCError):
    _message: str = "RPCError: Given dimension {} not found."

    def __init__(self, dimension_id: str) -> None:
        super().__init__(dimension_id)
        self.dimension_id: str = dimension_id

    def __str__(self) -> str:
        return self._message.format(self.dimension_id)


class RetriesExceededError(RPCError):
    _message: str = "No response from DQE, retries {} exceeded"

    def __init__(self, max_retries: int) -> None:
        super().__init__(str(max_retries))
        self.max_retries: int = max_retries

    def __str__(self) -> str:
        return self._message.format(self.max_retries)
