"""How TimedBatcher hands back batches whose delivery failed (no thread, fake clock)."""

from __future__ import annotations

import pytest
from conftest import make

from timedbatch import FlushError


def test_the_sink_gets_its_own_list() -> None:
    kept: list[list[int]] = []

    def sink(batch: list[int]) -> None:
        batch.clear()
        raise ValueError("after clearing")

    batcher = make(sink, max_items=2, on_error=lambda exc, batch: kept.append(batch))
    batcher.add(1)
    batcher.add(2)

    assert kept == [[1, 2]]


def test_on_error_receives_the_exception_and_the_batch() -> None:
    seen: list[tuple[str, list[int]]] = []

    def sink(batch: list[int]) -> None:
        raise ValueError(f"bad {batch}")

    batcher = make(sink, max_items=1, on_error=lambda exc, batch: seen.append((str(exc), batch)))
    batcher.add(1)
    batcher.add(2)

    assert seen == [("bad [1]", [1]), ("bad [2]", [2])]
    assert batcher.pending == 0


def test_without_on_error_the_failed_batch_comes_back_in_a_flush_error() -> None:
    def sink(batch: list[int]) -> None:
        raise ValueError("down")

    batcher = make(sink, max_items=2)
    batcher.add(1)
    batcher.add(2)  # accepted; the failure of its batch is reported by the next call
    with pytest.raises(FlushError) as caught:
        batcher.add(3)

    assert caught.value.batches == [[1, 2]]
    assert isinstance(caught.value.__cause__, ValueError)
    assert batcher.pending == 0  # 3 was not accepted
    batcher.add(3)  # reported once, then the batcher carries on
    assert batcher.pending == 1


def test_a_failing_on_error_is_reported_with_the_batch() -> None:
    def sink(batch: list[int]) -> None:
        raise ValueError("sink")

    def handler(exc: Exception, batch: list[int]) -> None:
        raise RuntimeError("handler")

    batcher = make(sink, max_items=1, on_error=handler)
    batcher.add(1)
    with pytest.raises(FlushError) as caught:
        batcher.poll()

    assert caught.value.batches == [[1]]
    assert repr(caught.value.errors[0]) == "RuntimeError('handler')"


def test_an_interrupt_in_the_sink_propagates_and_the_batch_is_reported_later() -> None:
    calls = 0

    def sink(batch: list[int]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KeyboardInterrupt

    batcher = make(sink, max_items=1)
    with pytest.raises(KeyboardInterrupt):
        batcher.add(1)
    with pytest.raises(FlushError) as caught:
        batcher.add(2)

    assert caught.value.batches == [[1]]
    assert batcher.pending == 0
