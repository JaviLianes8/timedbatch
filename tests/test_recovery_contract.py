"""Hand-written contracts for retained failures, capacity and task termination."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable

import pytest

from timedbatch import AsyncTimedBatcher, FlushError, TimedBatcher


def fail(batch: list[int]) -> None:
    raise ValueError("sink")


def test_sync_failures_remain_pending_until_handback() -> None:
    out = TimedBatcher(fail, max_items=1, max_seconds=1, max_pending=1, background=False)
    out.add(7)
    assert out.pending == 1
    with pytest.raises(FlushError) as caught:
        out.add(8, timeout=0)
    assert caught.value.batches == [[7]]
    assert out.pending == 0
    out.close()


def test_async_failures_remain_pending_until_handback() -> None:
    async def main() -> None:
        attempted = asyncio.Event()

        def sink(batch: list[int]) -> None:
            attempted.set()
            fail(batch)

        out = AsyncTimedBatcher(sink, max_items=1, max_seconds=1, max_pending=1)
        await out.add(7)
        await attempted.wait()
        assert out.pending == 1
        with pytest.raises(FlushError) as caught:
            await out.add(8, timeout=0)
        assert caught.value.batches == [[7]]
        assert out.pending == 0
        await out.aclose()

    asyncio.run(main())


def test_sync_waiting_producer_receives_failure_without_accepting_item() -> None:
    gate = threading.Event()
    entered_wait = threading.Event()

    def sink(batch: list[int]) -> None:
        assert gate.wait(5)
        fail(batch)

    out = TimedBatcher(sink, max_items=1, max_seconds=1, max_pending=1)
    original_wait = out._progress.wait

    def wait(timeout: float | None = None) -> bool:
        entered_wait.set()
        return original_wait(timeout)

    out._progress.wait = wait  # type: ignore[method-assign]
    recovered: list[list[int]] = []

    def produce() -> None:
        try:
            out.add(8, timeout=5)
        except FlushError as error:
            recovered.extend(error.batches)

    out.add(7)
    producer = threading.Thread(target=produce)
    producer.start()
    try:
        assert entered_wait.wait(5)
    finally:
        gate.set()
        producer.join(5)
    assert not producer.is_alive()
    assert recovered == [[7]]
    assert out.pending == 0
    out.close()


def test_async_waiting_producer_receives_failure_without_accepting_item() -> None:
    async def main() -> None:
        gate = asyncio.Event()

        async def sink(batch: list[int]) -> None:
            await gate.wait()
            fail(batch)

        out = AsyncTimedBatcher(sink, max_items=1, max_seconds=1, max_pending=1)
        await out.add(7)
        producer = asyncio.create_task(out.add(8, timeout=5))
        await asyncio.sleep(0)
        assert not producer.done()
        gate.set()
        with pytest.raises(FlushError) as caught:
            await producer
        assert caught.value.batches == [[7]]
        assert out.pending == 0
        await out.aclose()

    asyncio.run(main())


def test_closed_observes_cancelled_task_without_consuming_failures() -> None:
    async def main() -> None:
        attempted = asyncio.Event()

        def sink(batch: list[int]) -> None:
            attempted.set()
            raise asyncio.CancelledError

        out = AsyncTimedBatcher(sink, max_items=1, max_seconds=1)
        await out.add(7)
        await out.add(8)
        await attempted.wait()
        assert out.closed
        assert out.closed and out.pending == 2
        with pytest.raises(FlushError) as caught:
            await out.aclose()
        assert caught.value.batches == [[7], [8]]
        assert all(isinstance(e, asyncio.CancelledError) for e in caught.value.errors)
        assert out.pending == 0
        await out.aclose()

    asyncio.run(main())


def destructive_handler(error: Exception, batch: list[int]) -> None:
    batch.clear()
    raise RuntimeError("handler")


def test_sync_handler_cannot_destroy_recovery_copy() -> None:
    out = TimedBatcher(
        fail, max_items=1, max_seconds=1, background=False, on_error=destructive_handler
    )
    out.add(7)
    assert out.pending == 1
    with pytest.raises(FlushError) as caught:
        out.close()
    assert caught.value.batches == [[7]]
    assert isinstance(caught.value.errors[0], RuntimeError)
    assert out.pending == 0
    out.close()


@pytest.mark.parametrize("awaited", [False, True])
def test_async_handler_cannot_destroy_recovery_copy(awaited: bool) -> None:
    async def main() -> None:
        attempted = asyncio.Event()

        def handler(error: Exception, batch: list[int]) -> None:
            attempted.set()
            destructive_handler(error, batch)

        async def async_handler(error: Exception, batch: list[int]) -> None:
            handler(error, batch)

        callback: Callable[[Exception, list[int]], object] = async_handler if awaited else handler
        out = AsyncTimedBatcher(fail, max_items=1, max_seconds=1, on_error=callback)
        await out.add(7)
        await attempted.wait()
        assert out.pending == 1
        with pytest.raises(FlushError) as caught:
            await out.aclose()
        assert caught.value.batches == [[7]]
        assert isinstance(caught.value.errors[0], RuntimeError)
        assert out.pending == 0
        await out.aclose()

    asyncio.run(main())
