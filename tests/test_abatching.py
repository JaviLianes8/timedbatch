"""abatched_by_time: idle flushes, no lost reads, clean shutdown."""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from collections.abc import AsyncIterator

import pytest
from conftest import FakeClock

from timedbatch import abatched_by_time


async def collect(source: AsyncIterator[list[int]]) -> list[list[int]]:
    return [batch async for batch in source]


async def paced(*steps: tuple[int, float]) -> AsyncIterator[int]:
    """Yield each value after sleeping the given real seconds."""
    for value, pause in steps:
        await asyncio.sleep(pause)
        yield value


def test_count_and_end_of_source() -> None:
    batches = asyncio.run(collect(abatched_by_time(paced(*[(i, 0) for i in range(5)]), 2, 60.0)))

    assert batches == [[0, 1], [2, 3], [4]]


def test_a_partial_batch_is_yielded_while_the_source_is_idle() -> None:
    async def main() -> list[tuple[list[int], float]]:
        started = time.monotonic()
        out = []
        async for batch in abatched_by_time(paced((1, 0), (2, 0.6)), 10, 0.15):
            out.append((batch, time.monotonic() - started))
        return out

    (first, first_at), (second, _) = asyncio.run(main())

    assert (first, second) == ([1], [2])
    assert 0.15 <= first_at < 0.55  # flushed on age, well before the next item came


def test_the_read_in_flight_survives_every_idle_flush() -> None:
    steps = [(value, 0.07 if value % 3 else 0.0) for value in range(30)]
    batches = asyncio.run(collect(abatched_by_time(paced(*steps), 4, 0.02)))

    assert [item for batch in batches for item in batch] == list(range(30))
    assert all(1 <= len(batch) <= 4 for batch in batches)


def test_age_on_arrival_uses_the_injected_clock() -> None:
    clock = FakeClock()

    async def source() -> AsyncIterator[int]:
        for value, moment in [(1, 0.0), (2, 6.0), (3, 6.1)]:
            clock.now = moment
            yield value

    batches = asyncio.run(collect(abatched_by_time(source(), 10, 5.0, clock=clock)))

    assert batches == [[1, 2], [3]]


def test_items_read_before_a_source_error_are_yielded_first() -> None:
    async def source() -> AsyncIterator[int]:
        yield 1
        raise OSError("gone")

    async def main() -> tuple[list[int], BaseException | None]:
        batches = abatched_by_time(source(), 10, 60.0)
        first = await batches.__anext__()
        try:
            await batches.__anext__()
        except OSError as exc:
            return first, exc
        return first, None

    first, error = asyncio.run(main())

    assert first == [1]
    assert isinstance(error, OSError)


def test_closing_early_cancels_the_pending_read() -> None:
    cancelled = asyncio.Event()

    async def source() -> AsyncIterator[int]:
        yield 1
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        yield 2

    async def main() -> list[int]:
        batches = abatched_by_time(source(), 10, 0.05)
        async with contextlib.aclosing(batches):
            first = await batches.__anext__()
        await asyncio.wait_for(cancelled.wait(), 5)
        return first

    assert asyncio.run(main()) == [1]


def test_an_infinite_window_never_flushes_on_age() -> None:
    batches = asyncio.run(collect(abatched_by_time(paced((1, 0), (2, 0.2)), 5, math.inf)))

    assert batches == [[1, 2]]


def test_rejects_a_non_async_iterable() -> None:
    with pytest.raises(TypeError):
        abatched_by_time([1, 2], 2, 1.0)  # type: ignore[arg-type]
