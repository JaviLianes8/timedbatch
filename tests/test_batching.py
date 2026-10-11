from __future__ import annotations

from collections.abc import Iterator

import pytest

from timedbatch import batched_by_time


class FakeClock:
    """A clock whose value is set by the test, so no real time passes."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def timed_source(clock: FakeClock, steps: list[tuple[object, float]]) -> Iterator[object]:
    """Yield each value after moving the clock to its timestamp."""
    for value, moment in steps:
        clock.now = moment
        yield value


def test_flushes_when_the_count_limit_is_reached() -> None:
    clock = FakeClock()
    source = timed_source(clock, [("a", 0.0), ("b", 0.1), ("c", 0.2)])

    batches = list(batched_by_time(source, max_items=2, max_seconds=100.0, clock=clock))

    assert batches == [["a", "b"], ["c"]]


def test_flushes_when_the_time_window_elapses() -> None:
    clock = FakeClock()
    source = timed_source(clock, [("a", 0.0), ("b", 6.0), ("c", 6.1)])

    batches = list(batched_by_time(source, max_items=10, max_seconds=5.0, clock=clock))

    assert batches == [["a", "b"], ["c"]]


def test_flushes_the_remainder_at_the_end() -> None:
    clock = FakeClock()
    source = timed_source(clock, [("a", 0.0), ("b", 0.1), ("c", 0.2)])

    batches = list(batched_by_time(source, max_items=5, max_seconds=100.0, clock=clock))

    assert batches == [["a", "b", "c"]]


def test_empty_iterable_yields_nothing() -> None:
    clock = FakeClock()

    empty: list[object] = []
    batches = list(batched_by_time(empty, max_items=3, max_seconds=1.0, clock=clock))

    assert batches == []


def test_single_item_is_flushed_as_one_batch() -> None:
    clock = FakeClock()

    batches = list(batched_by_time(["only"], max_items=3, max_seconds=1.0, clock=clock))

    assert batches == [["only"]]


def test_each_batch_restarts_its_own_window() -> None:
    clock = FakeClock()
    source = timed_source(clock, [("a", 0.0), ("b", 6.0), ("c", 6.5), ("d", 12.0)])

    batches = list(batched_by_time(source, max_items=10, max_seconds=5.0, clock=clock))

    assert batches == [["a", "b"], ["c", "d"]]


def test_preserves_the_item_type() -> None:
    numbers: list[int] = [1, 2, 3, 4]

    batches: list[list[int]] = list(batched_by_time(numbers, max_items=2, max_seconds=1.0))

    assert batches == [[1, 2], [3, 4]]
    assert all(isinstance(value, int) for batch in batches for value in batch)


def test_rejects_a_non_positive_count() -> None:
    with pytest.raises(ValueError):
        list(batched_by_time([1], max_items=0, max_seconds=1.0))


def test_rejects_a_non_positive_window() -> None:
    with pytest.raises(ValueError):
        list(batched_by_time([1], max_items=1, max_seconds=0.0))
