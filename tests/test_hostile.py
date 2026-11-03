"""Hostile sizes and values: huge numbers, huge timeouts, huge backlogs, huge errors."""

from __future__ import annotations

import asyncio
import math
import time
from decimal import Decimal
from fractions import Fraction

import pytest
from conftest import FakeClock, make

from timedbatch import AsyncTimedBatcher, FlushError, TimedBatcher, batched_by_time

HUGE_WINDOWS = [10**400, Fraction(10**400, 3), Decimal("1e400"), math.inf]
HUGE_TIMEOUTS = [10**400, Fraction(10**400), Decimal("1e400"), math.inf, 1e12, 4294968.0]


@pytest.mark.parametrize("window", HUGE_WINDOWS, ids=repr)
def test_a_window_beyond_the_float_range_means_no_age_limit(window: object) -> None:
    clock = FakeClock()
    sent: list[list[int]] = []
    batcher = make(sent.append, clock, max_items=2, max_seconds=window)
    batcher.add(1)
    clock.now = 1e300
    batcher.poll()
    assert sent == []
    batcher.add(2)
    assert sent == [[1, 2]]
    assert list(batched_by_time([1, 2, 3], 2, window)) == [[1, 2], [3]]  # type: ignore[arg-type]


@pytest.mark.parametrize("window", [-(10**400), Fraction(-(10**400)), Decimal("-1e400")])
def test_a_window_below_the_float_range_is_refused(window: object) -> None:
    with pytest.raises(ValueError, match=r"^max_seconds must be greater than 0$"):
        batched_by_time([1], 1, window)  # type: ignore[arg-type]


@pytest.mark.parametrize("nan", [Decimal("NaN"), Decimal("sNaN"), Decimal("-NaN"), math.nan])
def test_every_nan_is_refused_with_the_same_message(nan: object) -> None:
    with pytest.raises(ValueError, match=r"^max_seconds must not be NaN$"):
        batched_by_time([1], 1, nan)  # type: ignore[arg-type]


@pytest.mark.parametrize("timeout", HUGE_TIMEOUTS, ids=repr)
def test_huge_timeouts_mean_wait_as_long_as_needed(timeout: float) -> None:
    sent: list[list[str]] = []
    batcher = TimedBatcher(sent.append, max_items=5, max_seconds=60.0, max_pending=5)
    batcher.add("a", timeout=timeout)
    batcher.flush(timeout=timeout)
    batcher.add("b", timeout=timeout)
    batcher.close(timeout=timeout)
    assert sent == [["a"], ["b"]]
    assert not batcher._thread.is_alive()  # type: ignore[union-attr]


@pytest.mark.parametrize("timeout", HUGE_TIMEOUTS, ids=repr)
def test_huge_timeouts_work_in_the_async_batcher(timeout: float) -> None:
    sent: list[list[str]] = []

    async def send(batch: list[str]) -> None:
        sent.append(batch)

    async def main() -> None:
        async with AsyncTimedBatcher(send, max_items=1, max_seconds=1e300, max_pending=1) as out:
            await out.add("a", timeout=timeout)
            await out.add("b", timeout=timeout)
            await out.flush(timeout=timeout)

    asyncio.run(main())
    assert sent == [["a"], ["b"]]


def _drain_seconds(backlog: int) -> float:
    """Time to deliver ``backlog`` one-item batches that piled up behind a busy sink."""
    batcher: TimedBatcher[int]
    calls = 0

    def sink(batch: list[int]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            for value in range(backlog):
                batcher.add(value)  # buffered while this delivery runs

    batcher = make(sink, max_items=1, max_seconds=60.0)
    started = time.perf_counter()
    batcher.add(-1)
    elapsed = time.perf_counter() - started
    assert calls == backlog + 1
    return elapsed


def test_draining_a_backlog_takes_linear_time() -> None:
    small = min(_drain_seconds(20_000) for _ in range(3))
    large = min(_drain_seconds(80_000) for _ in range(3))
    # Four times the backlog: about 4x when linear, 16x when quadratic.
    assert large / small < 9, (small, large)


def test_a_flush_error_from_a_huge_batch_has_a_short_message() -> None:
    def sink(batch: list[int]) -> None:
        raise ValueError(repr(batch))

    batcher = make(sink, max_items=10_000)
    for _ in range(10_000):
        batcher.add(10**100)  # the last one fills the batch, which fails
    with pytest.raises(FlushError) as caught:
        batcher.close()
    assert len(str(caught.value)) < 300
    assert len(caught.value.batches[0]) == 10_000
