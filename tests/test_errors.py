from __future__ import annotations

import pickle

from timedbatch import BatcherClosed, BatcherFull, FlushError


def test_flush_error_carries_batches_errors_and_cause() -> None:
    boom = ValueError("boom")
    error = FlushError([[1, 2], [3]], [boom, KeyError("k")])

    assert error.batches == [[1, 2], [3]]
    assert error.errors[0] is boom
    assert error.__cause__ is boom
    assert str(error) == "2 batches (3 items) failed to flush: ValueError('boom')"


def test_flush_error_message_for_one_batch() -> None:
    assert str(FlushError([[1]], [OSError("x")])) == (
        "1 batch (1 item) failed to flush: OSError('x')"
    )


def test_flush_error_survives_pickling() -> None:
    copy = pickle.loads(pickle.dumps(FlushError([["a"]], [ValueError("v")])))

    assert copy.batches == [["a"]]
    assert repr(copy.errors[0]) == "ValueError('v')"


def test_error_families() -> None:
    assert issubclass(BatcherFull, TimeoutError)
    assert issubclass(BatcherClosed, RuntimeError)
    assert issubclass(FlushError, Exception)
