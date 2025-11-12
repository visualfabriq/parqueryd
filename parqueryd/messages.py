from __future__ import annotations

import base64
import json
import pickle
import time
from typing import Any

from parqueryd.tool import ens_bytes


def msg_factory(msg: bytes | dict[str, Any] | None) -> Message:
    """Factory function to create appropriate Message subclass from bytes or dict."""
    if isinstance(msg, bytes):
        try:
            msg = json.loads(msg.decode())
        except Exception:
            msg = None
    if not msg:
        return Message()
    msg_mapping: dict[str | None, type[Message]] = {
        "calc": CalcMessage,
        "rpc": RPCMessage,
        "error": ErrorMessage,
        "worker_register": WorkerRegisterMessage,
        "busy": BusyMessage,
        "done": DoneMessage,
        "ticketdone": TicketDoneMessage,
        "stop": StopMessage,
        None: Message,
    }
    msg_class = msg_mapping.get(msg.get("msg_type"))
    return msg_class(msg) if msg_class else Message(msg)


class MalformedMessage(Exception):
    """Exception for malformed messages."""

    pass


class Message(dict):
    """Base message class that extends dict."""

    msg_type: str | None = None

    def __init__(self, datadict: dict[str, Any] | None = None) -> None:
        if datadict is None:
            datadict = {}
        self.update(datadict)
        self["payload"] = datadict.get("payload")
        self["version"] = datadict.get("version", 1)
        self["msg_type"] = self.msg_type
        self["created"] = time.time()

    def copy(self) -> Message:
        """Create a copy of this message."""
        newme = super().copy()
        return msg_factory(newme)

    def isa(self, payload_or_instance: Any) -> bool:
        """Check if message matches payload or instance type."""
        if self.msg_type == getattr(payload_or_instance, "msg_type", "_"):
            return True
        return self.get("payload") == payload_or_instance

    def get_from_binary(self, key: str, default: Any = None) -> Any:
        """Get and unpickle a binary value from message."""
        buf = self.get(key)
        if not buf:
            return default
        return pickle.loads(base64.b64decode(buf))

    def to_json(self) -> bytes:
        """Serialize message to JSON bytes."""
        # We could do some serialization fixes in here for things like datetime or other binary non-json-serializable members
        return ens_bytes(json.dumps(self))

    def set_args_kwargs(self, args: list[Any], kwargs: dict[str, Any]) -> None:
        """Set args and kwargs in message parameters."""
        params = {"args": args, "kwargs": kwargs}
        self["params"] = params

    def get_args_kwargs(self) -> tuple[list[Any], dict[str, Any]]:
        """Get args and kwargs from message parameters."""
        params = self.get("params", {})
        kwargs = params.get("kwargs", {})
        args = params.get("args", [])
        return args, kwargs


class WorkerRegisterMessage(Message):
    """Message sent by workers to register with controller."""

    msg_type: str = "worker_register"


class CalcMessage(Message):
    """Message for calculation requests."""

    msg_type: str = "calc"


class RPCMessage(Message):
    """Message for RPC requests."""

    msg_type: str = "rpc"


class ErrorMessage(Message):
    """Message for error responses."""

    msg_type: str = "error"


class BusyMessage(Message):
    """Message indicating worker is busy."""

    msg_type: str = "busy"


class DoneMessage(Message):
    """Message indicating task is complete."""

    msg_type: str = "done"


class StopMessage(Message):
    """Message to stop processing."""

    msg_type: str = "stop"


class TicketDoneMessage(Message):
    """Message indicating a ticket is complete."""

    msg_type: str = "ticketdone"
