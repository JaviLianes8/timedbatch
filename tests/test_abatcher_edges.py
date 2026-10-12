"""AsyncTimedBatcher edge cases: no items, huge windows."""

from __future__ import annotations

import asyncio
import contextlib
import math

import pytest

from timedbatch import AsyncTimedBatcher, BatcherClosed, FlushError


def recorder() -> tuple[list[list[int]], AsyncTimedBatcher[int]]:
    out: list[list[int]] = []

    async def sink(batch: list[int]) -> None:
        out.append(batch)

    return out, AsyncTimedBatcher(sink, max_items=10, max_seconds=1e12)


def test_close_before_any_item_is_a_no_op() -> None:
    async def main() -> None:
        _, batcher = recorder()
        await batcher.aclose()
        assert batcher.closed

    asyncio.run(main())


def test_a_huge_window_still_flushes_on_demand() -> None:
    out, batcher = recorder()

    async def main() -> None:
        async with batcher:
            await batcher.add(1)
            await asyncio.sleep(0.05)
            await batcher.flush(timeout=5)

    asyncio.run(main())
    assert out == [[1]]


async def _cancelled_future_sink(batch: list[str]) -> None:
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    future.cancel()
    await future


def test_a_sink_cancellation_closes_the_batcher_and_hands_everything_back() -> None:
    async def main() -> None:
        batcher = AsyncTimedBatcher(_cancelled_future_sink, max_items=1, max_seconds=60.0)
        await batcher.add("a")
        await asyncio.sleep(0.05)
        with pytest.raises(FlushError) as caught:
            await batcher.add("b")
        assert caught.value.batches == [["a"]]
        with pytest.raises(BatcherClosed):
            await batcher.add("c")
        assert batcher.closed and batcher.pending == 0
        await batcher.aclose()  # the task's own cancellation does not leak out

    asyncio.run(main())


def test_items_buffered_when_the_task_is_cancelled_are_handed_back() -> None:
    async def main() -> None:
        _, batcher = recorder()
        await batcher.add(1)
        await batcher.add(2)
        assert batcher._task is not None
        batcher._task.cancel()
        await asyncio.sleep(0.05)
        with pytest.raises(FlushError) as caught:
            await batcher.add(3)
        assert caught.value.batches == [[1, 2]]
        assert isinstance(caught.value.errors[0], asyncio.CancelledError)
        with pytest.raises(BatcherClosed):
            await batcher.add(4)

    asyncio.run(main())


def test_items_left_when_the_loop_ends_come_back_in_the_next_loop() -> None:
    _, batcher = recorder()
    asyncio.run(batcher.add(1))

    async def later() -> None:
        with pytest.raises(FlushError) as caught:
            await batcher.add(2)
        assert caught.value.batches == [[1]]
        await batcher.aclose()

    asyncio.run(later())
    assert batcher.pending == 0


def test_add_waiting_for_room_sees_the_task_stop() -> None:
    async def main() -> None:
        gate = asyncio.Event()

        async def sink(batch: list[int]) -> None:
            await gate.wait()

        batcher = AsyncTimedBatcher(sink, max_items=1, max_seconds=60.0, max_pending=1)
        await batcher.add(1)
        waiting = asyncio.ensure_future(batcher.add(2))
        await asyncio.sleep(0.05)
        assert batcher._task is not None
        batcher._task.cancel()
        with pytest.raises(FlushError):
            await waiting
        assert batcher.pending == 0

    asyncio.run(main())


@pytest.mark.parametrize("bad", [math.nan, "1", True])
def test_bad_timeouts_are_rejected(bad: object) -> None:
    async def main() -> None:
        _, batcher = recorder()
        with pytest.raises((TypeError, ValueError)):
            await batcher.add(1, timeout=bad)  # type: ignore[arg-type]
        with pytest.raises((TypeError, ValueError)):
            await batcher.flush(timeout=bad)  # type: ignore[arg-type]
        assert batcher.pending == 0

    asyncio.run(main())


def test_aclose_hands_back_instead_of_raising_when_the_task_is_cancelled() -> None:
    async def main() -> None:
        gate = asyncio.Event()

        async def sink(batch: list[int]) -> None:
            await gate.wait()

        batcher = AsyncTimedBatcher(sink, max_items=1, max_seconds=60.0)
        await batcher.add(1)
        await batcher.add(2)
        closing = asyncio.ensure_future(batcher.aclose())
        await asyncio.sleep(0.05)
        assert batcher._task is not None
        batcher._task.cancel()
        with pytest.raises(FlushError) as caught:
            await closing
        assert caught.value.batches == [[1], [2]]
        assert batcher.pending == 0

    asyncio.run(main())


def test_an_async_on_error_that_raises_is_reported_with_the_batch() -> None:
    async def sink(batch: list[int]) -> None:
        raise ValueError("sink")

    async def handler(exc: Exception, batch: list[int]) -> None:
        raise RuntimeError("handler")

    async def main() -> None:
        batcher = AsyncTimedBatcher(sink, max_items=1, max_seconds=60.0, on_error=handler)
        await batcher.add(1)
        with pytest.raises(FlushError) as caught:
            await batcher.aclose()
        assert caught.value.batches == [[1]]
        assert repr(caught.value.errors[0]) == "RuntimeError('handler')"

    asyncio.run(main())


class _Halt(BaseException):
    pass


def test_a_base_exception_from_the_sink_closes_and_hands_back_the_rest() -> None:
    async def sink(batch: list[int]) -> None:
        raise _Halt

    async def main() -> None:
        batcher = AsyncTimedBatcher(sink, max_items=2, max_seconds=60.0)
        for value in range(5):
            with contextlib.suppress(_Halt):
                await batcher.add(value)
        await asyncio.sleep(0.05)
        with pytest.raises(FlushError) as caught:
            await batcher.add(9)
        assert [x for batch in caught.value.batches for x in batch] == [0, 1, 2, 3, 4]
        with pytest.raises(BatcherClosed):
            await batcher.add(9)

    asyncio.run(main())
