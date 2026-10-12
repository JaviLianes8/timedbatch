"""Many producers, a slow and failing sink, a tight max_pending: the invariants hold."""

from __future__ import annotations

import asyncio
import random
import threading
import time

import pytest

from timedbatch import AsyncTimedBatcher, FlushError, TimedBatcher

PRODUCERS, PER_PRODUCER, MAX_ITEMS, MAX_PENDING = 8, 600, 7, 20


class Audit:
    """Collects what the sink saw and checks every invariant at the end."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.delivered: list[tuple[int, int]] = []
        self.failed: list[tuple[int, int]] = []
        self.max_pending_seen = 0
        self.overlap = 0
        self.inside = 0
        self.rng = random.Random(7)

    def enter(self, batch: list[tuple[int, int]], pending: int) -> bool:
        with self.lock:
            self.inside += 1
            self.overlap = max(self.overlap, self.inside)
            self.max_pending_seen = max(self.max_pending_seen, pending)
            assert 1 <= len(batch) <= MAX_ITEMS
            return self.rng.random() < 0.15

    def leave(self, batch: list[tuple[int, int]], failed: bool) -> None:
        with self.lock:
            self.inside -= 1
            (self.failed if failed else self.delivered).extend(batch)

    def check(self) -> None:
        expected = {(p, i) for p in range(PRODUCERS) for i in range(PER_PRODUCER)}
        seen = self.delivered + self.failed
        assert len(seen) == len(expected), "an item was lost or delivered twice"
        assert set(seen) == expected
        assert self.failed, "the failing path was never exercised"
        assert self.overlap == 1, "two batches were in the sink at once"
        assert self.max_pending_seen <= MAX_PENDING
        for producer in range(PRODUCERS):
            order = [i for p, i in self.delivered if p == producer]
            assert order == sorted(order), "items of one producer were reordered"


@pytest.mark.parametrize("background", [True, False])
def test_threads_never_lose_duplicate_or_reorder(background: bool) -> None:
    audit = Audit()
    handed_back: list[tuple[int, int]] = []

    def sink(batch: list[tuple[int, int]]) -> None:
        fail = audit.enter(batch, batcher.pending)
        time.sleep(audit.rng.choice([0, 0, 0, 0.001]))
        audit.leave(batch, fail)
        if fail:
            raise OSError("flaky")

    batcher: TimedBatcher[tuple[int, int]] = TimedBatcher(
        sink,
        max_items=MAX_ITEMS,
        max_seconds=0.002,
        max_pending=MAX_PENDING,
        background=background,
        flush_at_exit=False,
    )

    def producer(index: int) -> None:
        for item in range(PER_PRODUCER):
            while True:
                try:
                    batcher.add((index, item))
                    assert batcher.pending <= MAX_PENDING
                    break
                except FlushError as exc:
                    with audit.lock:
                        handed_back.extend(x for batch in exc.batches for x in batch)

    threads = [threading.Thread(target=producer, args=(n,)) for n in range(PRODUCERS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    try:
        batcher.close(timeout=60)
    except FlushError as exc:
        handed_back.extend(x for batch in exc.batches for x in batch)

    assert sorted(handed_back) == sorted(audit.failed), "a failed batch was not handed back once"
    assert batcher.pending == 0
    audit.check()


def test_tasks_never_lose_duplicate_or_reorder() -> None:
    audit = Audit()
    handed_back: list[tuple[int, int]] = []

    async def main() -> None:
        async def sink(batch: list[tuple[int, int]]) -> None:
            fail = audit.enter(batch, batcher.pending)
            await asyncio.sleep(audit.rng.choice([0, 0, 0.001]))
            audit.leave(batch, fail)
            if fail:
                raise OSError("flaky")

        batcher: AsyncTimedBatcher[tuple[int, int]] = AsyncTimedBatcher(
            sink, max_items=MAX_ITEMS, max_seconds=0.002, max_pending=MAX_PENDING
        )

        async def producer(index: int) -> None:
            for item in range(PER_PRODUCER):
                while True:
                    try:
                        await batcher.add((index, item))
                        assert batcher.pending <= MAX_PENDING
                        break
                    except FlushError as exc:
                        handed_back.extend(x for batch in exc.batches for x in batch)
                if item % 50 == 0:
                    await asyncio.sleep(0)

        await asyncio.gather(*(producer(n) for n in range(PRODUCERS)))
        try:
            await batcher.aclose()
        except FlushError as exc:
            handed_back.extend(x for batch in exc.batches for x in batch)
        assert batcher.pending == 0

    asyncio.run(main())
    assert sorted(handed_back) == sorted(audit.failed)
    audit.check()
