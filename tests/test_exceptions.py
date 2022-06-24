import pytest

from parqueryd.exceptions import RPCError, RetriesExceededError

def test_RPCError():
    e = RPCError("Something happened")
    assert e

def test_RetriesExceededError():
    e = RetriesExceededError(5)
    assert e.max_retries == 5