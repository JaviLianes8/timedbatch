"""AsyncTimedBatcher edge cases: no items, huge windows."""

from __future__ import annotations

import asyncio

from timedbatch import AsyncTimedBatcher


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
