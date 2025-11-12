from parqueryd.exceptions import MissingDimensionError, RetriesExceededError, RPCError


def test_RPCError():
    e = RPCError("Something happened")
    assert str(e) == "Something happened"

    e = RPCError()
    assert str(e) == "RPCError: There was an exception when performing the RPC action"


def test_RetriesExceededError():
    e = RetriesExceededError(5)
    assert e.max_retries == 5
    assert str(e) == "No response from DQE, retries 5 exceeded"


def test_MissingDimensionError():
    e = MissingDimensionError(10)
    assert e.dimension_id == 10

    e = MissingDimensionError(-4)
    assert e.dimension_id == -4

    assert str(e) == "RPCError: Given dimension -4 not found."
