"""TimedBatcher without a thread: every delivery happens in the caller, on a fake clock."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from conftest import FakeClock

from timedbatch import BatcherClosed, BatcherFull, FlushError, TimedBatcher


def make(
    sink: Callable[[list[int]], object],
    clock: FakeClock | None = None,
    **options: object,
) -> TimedBatcher[int]:
    settings: dict[str, object] = {"max_items": 3, "max_seconds": 5.0}
    settings.update(options)
    return TimedBatcher(
        sink,
        clock=clock or FakeClock(),
        background=False,
        flush_at_exit=False,
        **settings,  # type: ignore[arg-type]
    )


def test_a_full_batch_is_delivered_by_the_add_that_fills_it() -> None:
    out: list[list[int]] = []
    batcher = make(out.append)

    for value in range(4):
        batcher.add(value)

    assert out == [[0, 1, 2]]
    assert batcher.pending == 1


def test_an_old_batch_is_delivered_by_poll_without_new_items() -> None:
    clock, out = FakeClock(), list[list[int]]()
    batcher = make(out.append, clock)
    batcher.add(1)
    clock.now = 4.9
    batcher.poll()
    assert out == []

    clock.now = 5.0
    batcher.poll()

    assert out == [[1]]


def test_age_is_counted_from_the_oldest_item_of_each_batch() -> None:
    clock, out = FakeClock(), list[list[int]]()
    batcher = make(out.append, clock, max_items=100)
    batcher.add(1)
    clock.now = 3.0
    batcher.add(2)
    clock.now = 5.0
    batcher.add(3)
    clock.now = 9.9
    batcher.poll()

    assert out == [[1, 2, 3]]


def test_flush_delivers_the_partial_batch_at_once() -> None:
    out: list[list[int]] = []
    batcher = make(out.append, max_items=2)
    for value in range(5):
        batcher.add(value)

    batcher.flush()

    assert out == [[0, 1], [2, 3], [4]]
    assert batcher.pending == 0


def test_close_delivers_the_rest_and_refuses_new_items() -> None:
    out: list[list[int]] = []
    batcher = make(out.append)
    batcher.add(7)

    batcher.close()
    batcher.close()

    assert out == [[7]]
    assert batcher.closed
    with pytest.raises(BatcherClosed):
        batcher.add(8)


def test_the_context_manager_closes() -> None:
    out: list[list[int]] = []
    with make(out.append) as batcher:
        batcher.add(1)

    assert out == [[1]]
    assert batcher.closed


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


def test_add_from_inside_the_sink_is_buffered_not_delivered_reentrantly() -> None:
    out: list[list[int]] = []

    def sink(batch: list[int]) -> None:
        out.append(batch)
        if batch == [0]:
            batcher.add(99)

    batcher = make(sink, max_items=1)
    batcher.add(0)

    assert out == [[0], [99]]


@pytest.mark.parametrize("call", ["poll", "flush", "close"])
def test_control_calls_from_inside_the_sink_raise(call: str) -> None:
    errors: list[Exception] = []

    def sink(batch: list[int]) -> None:
        try:
            getattr(batcher, call)()
        except RuntimeError as exc:
            errors.append(exc)

    batcher = make(sink, max_items=1)
    batcher.add(1)

    assert len(errors) == 1
    assert "inside the flush callback" in str(errors[0])


def test_add_inside_the_sink_with_no_room_raises_instead_of_deadlocking() -> None:
    errors: list[Exception] = []

    def sink(batch: list[int]) -> None:
        try:
            batcher.add(5)
        except BatcherFull as exc:
            errors.append(exc)

    batcher = make(sink, max_items=1, max_pending=1)
    batcher.add(1)

    assert len(errors) == 1
