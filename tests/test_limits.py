from __future__ import annotations

import math
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest

from timedbatch import AsyncTimedBatcher, TimedBatcher, abatched_by_time, batched_by_time


def _noop(batch: list[int]) -> None:
    return None


async def _anoop(batch: list[int]) -> None:
    return None


async def _empty() -> AsyncIterator[int]:
    return
    yield


def _all_entry_points(max_items: Any, max_seconds: Any) -> list[Callable[[], object]]:
    return [
        lambda: batched_by_time([], max_items, max_seconds),
        lambda: abatched_by_time(_empty(), max_items, max_seconds),
        lambda: TimedBatcher(
            _noop,
            max_items=max_items,
            max_seconds=max_seconds,
            background=False,
            flush_at_exit=False,
        ),
        lambda: AsyncTimedBatcher(_anoop, max_items=max_items, max_seconds=max_seconds),
    ]


@pytest.mark.parametrize(
    ("max_items", "max_seconds", "error"),
    [
        (0, 1.0, ValueError),
        (-1, 1.0, ValueError),
        (True, 1.0, TypeError),
        (2.0, 1.0, TypeError),
        ("3", 1.0, TypeError),
        (1, 0.0, ValueError),
        (1, -0.5, ValueError),
        (1, math.nan, ValueError),
        (1, True, TypeError),
        (1, "1", TypeError),
        (1, None, TypeError),
    ],
)
def test_every_entry_point_rejects_bad_limits_at_once(
    max_items: object, max_seconds: object, error: type[Exception]
) -> None:
    for build in _all_entry_points(max_items, max_seconds):
        with pytest.raises(error):
            build()


def test_an_infinite_window_means_count_only() -> None:
    batches = list(batched_by_time(range(5), 2, math.inf))

    assert batches == [[0, 1], [2, 3], [4]]


def test_integer_windows_and_int_subclasses_are_accepted() -> None:
    class Count(int):
        pass

    assert list(batched_by_time(range(3), Count(2), 5)) == [[0, 1], [2]]


@pytest.mark.parametrize(("pending", "error"), [(1, ValueError), (0, ValueError), (2.5, TypeError)])
def test_max_pending_must_hold_at_least_one_batch(pending: object, error: type[Exception]) -> None:
    with pytest.raises(error):
        TimedBatcher(
            _noop,
            max_items=2,
            max_seconds=1.0,
            max_pending=pending,  # type: ignore[arg-type]
            background=False,
            flush_at_exit=False,
        )


def test_flush_must_be_callable() -> None:
    with pytest.raises(TypeError):
        TimedBatcher("sink", max_items=1, max_seconds=1.0, flush_at_exit=False)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        AsyncTimedBatcher("sink", max_items=1, max_seconds=1.0)  # type: ignore[arg-type]
