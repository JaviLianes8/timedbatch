"""AsyncTimedBatcher: the TimedBatcher promises on one event loop."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import pytest

from timedbatch import AsyncTimedBatcher, BatcherClosed, BatcherFull, FlushError

Sink = Callable[[list[int]], Awaitable[object]]


def recorder() -> tuple[list[list[int]], Sink]:
    out: list[list[int]] = []

    async def sink(batch: list[int]) -> None:
        out.append(batch)

    return out, sink


def test_size_flush_and_close() -> None:
    out, sink = recorder()

    async def main() -> None:
        async with AsyncTimedBatcher(sink, max_items=2, max_seconds=60.0) as batcher:
            for value in range(5):
                await batcher.add(value)
            await batcher.flush()
            assert batcher.pending == 0
            await batcher.add(5)
        assert batcher.closed
        with pytest.raises(BatcherClosed):
            await batcher.add(6)
        await batcher.aclose()

    asyncio.run(main())
    assert out == [[0, 1], [2, 3], [4], [5]]


def test_an_idle_partial_batch_is_delivered_after_max_seconds() -> None:
    out, sink = recorder()

    async def main() -> float:
        batcher = AsyncTimedBatcher(sink, max_items=10, max_seconds=0.15)
        started = time.monotonic()
        await batcher.add(1)
        while not out:
            await asyncio.sleep(0.01)
        elapsed = time.monotonic() - started
        await batcher.aclose()
        return elapsed

    elapsed = asyncio.run(main())
    assert out == [[1]]
    assert 0.15 <= elapsed < 5


def test_back_pressure_and_add_timeout() -> None:
    async def main() -> None:
        release = asyncio.Event()

        async def sink(batch: list[int]) -> None:
            await release.wait()

        batcher = AsyncTimedBatcher(sink, max_items=1, max_seconds=60.0, max_pending=2)
        await batcher.add(1)
        await batcher.add(2)
        with pytest.raises(BatcherFull):
            await batcher.add(3, timeout=0.05)
        assert batcher.pending == 2
        with pytest.raises(TimeoutError):
            await batcher.flush(timeout=0.05)
        release.set()
        await batcher.add(3, timeout=5)
        await batcher.aclose()
        assert batcher.pending == 0

    asyncio.run(main())


def test_an_add_waiting_for_room_is_refused_when_the_batcher_closes() -> None:
    async def main() -> None:
        release = asyncio.Event()

        async def sink(batch: list[int]) -> None:
            await release.wait()

        batcher = AsyncTimedBatcher(sink, max_items=1, max_seconds=60.0, max_pending=1)
        await batcher.add(1)
        waiting = asyncio.ensure_future(batcher.add(2))
        await asyncio.sleep(0.05)
        closing = asyncio.ensure_future(batcher.aclose())
        await asyncio.sleep(0.05)
        release.set()
        await closing
        with pytest.raises(BatcherClosed):
            await waiting
        assert batcher.pending == 0

    asyncio.run(main())


def test_failures_go_to_on_error_sync_or_async() -> None:
    seen: list[tuple[str, list[int]]] = []

    async def sink(batch: list[int]) -> None:
        raise ValueError(str(batch))

    async def async_handler(exc: Exception, batch: list[int]) -> None:
        seen.append((str(exc), batch))

    async def main() -> None:
        for handler in (lambda exc, batch: seen.append((str(exc), batch)), async_handler):
            async with AsyncTimedBatcher(
                sink, max_items=1, max_seconds=60.0, on_error=handler
            ) as batcher:
                await batcher.add(1)

    asyncio.run(main())
    assert seen == [("[1]", [1]), ("[1]", [1])]


def test_without_on_error_the_failed_batch_comes_back_once() -> None:
    async def sink(batch: list[int]) -> None:
        batch.clear()
        raise ValueError("down")

    async def main() -> None:
        batcher = AsyncTimedBatcher(sink, max_items=2, max_seconds=60.0)
        await batcher.add(1)
        await batcher.add(2)
        with pytest.raises(FlushError) as caught:
            await batcher.flush()
        assert caught.value.batches == [[1, 2]]
        await batcher.add(3)
        with pytest.raises(FlushError) as again:
            await batcher.aclose()
        assert again.value.batches == [[3]]
        await batcher.aclose()

    asyncio.run(main())


def test_a_failure_is_raised_by_add_before_accepting() -> None:
    async def sink(batch: list[int]) -> None:
        raise ValueError("down")

    async def main() -> None:
        batcher = AsyncTimedBatcher(sink, max_items=1, max_seconds=60.0)
        await batcher.add(1)
        while batcher.pending:
            await asyncio.sleep(0.01)
        with pytest.raises(FlushError):
            await batcher.add(2)
        assert batcher.pending == 0
        await batcher.aclose()

    asyncio.run(main())


def test_calls_from_inside_the_sink_raise_instead_of_deadlocking() -> None:
    errors: list[str] = []

    async def main() -> None:
        async def sink(batch: list[int]) -> None:
            for call in (batcher.flush, batcher.aclose):
                try:
                    await call()
                except RuntimeError as exc:
                    errors.append(str(exc))
            try:
                await batcher.add(9)
            except BatcherFull:
                errors.append("full")

        batcher = AsyncTimedBatcher(sink, max_items=1, max_seconds=60.0, max_pending=1)
        await batcher.add(1)
        await batcher.flush()
        await batcher.aclose()

    asyncio.run(main())
    assert errors[2] == "full"
    assert all("inside the flush callback" in text for text in errors[:2])
