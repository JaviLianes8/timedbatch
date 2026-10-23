"""Seeded random runs: every accepted item is delivered or handed back exactly once, in order."""

from __future__ import annotations

import asyncio
import math
import random
from collections.abc import Callable

import pytest
from conftest import FakeClock

from timedbatch import AsyncTimedBatcher, BatcherFull, FlushError, TimedBatcher

SEEDS = range(200)


class Ledger:
    """What the sink, the error handler and the raised FlushErrors saw."""

    def __init__(self, rng: random.Random, failure_rate: float) -> None:
        self.rng = rng
        self.failure_rate = failure_rate
        self.calls: list[list[int]] = []
        self.delivered: list[int] = []
        self.handed_back: list[int] = []

    def sink(self, batch: list[int]) -> None:
        self.calls.append(list(batch))
        if self.rng.random() < self.failure_rate:
            raise ValueError("down")
        self.delivered.extend(batch)

    def on_error(self, error: Exception, batch: list[int]) -> None:
        assert isinstance(error, ValueError)
        if self.rng.random() < 0.3:
            raise KeyError("handler down")
        self.handed_back.extend(batch)

    def caught(self, error: FlushError) -> None:
        assert len(error.batches) == len(error.errors) > 0
        for batch in error.batches:
            self.handed_back.extend(batch)

    def check(self, accepted: list[int], size: int, finished: bool) -> None:
        assert all(1 <= len(batch) <= size for batch in self.calls)
        # Batches leave in acceptance order, never overlapping and never skipping.
        called = [item for batch in self.calls for item in batch]
        assert called == accepted[: len(called)]
        settled = sorted(self.delivered + self.handed_back)
        assert len(settled) == len(set(settled)), "an item was settled twice"
        if finished:
            assert settled == sorted(accepted)


def _sync_options(rng: random.Random, ledger: Ledger) -> tuple[int, dict[str, object]]:
    size = rng.randint(1, 5)
    options: dict[str, object] = {
        "max_items": size,
        "max_seconds": rng.choice([0.5, 1.0, 3.0, math.inf]),
        "max_pending": rng.choice([None, size, size + 3]),
        "on_error": rng.choice([None, ledger.on_error]),
    }
    return size, options


@pytest.mark.parametrize("seed", SEEDS)
def test_the_thread_free_batcher_settles_every_item_once(seed: int) -> None:
    rng = random.Random(seed)
    ledger = Ledger(rng, rng.choice([0.0, 0.2, 0.6]))
    size, options = _sync_options(rng, ledger)
    clock = FakeClock()
    batcher: TimedBatcher[int] = TimedBatcher(
        ledger.sink,
        clock=clock,
        background=False,
        flush_at_exit=False,
        **options,  # type: ignore[arg-type]
    )
    accepted: list[int] = []
    for value in range(rng.randint(0, 60)):
        operation = rng.choice(["add", "add", "add", "poll", "flush", "tick"])
        try:
            if operation == "add":
                batcher.add(value)
                accepted.append(value)
            elif operation == "poll":
                batcher.poll()
            elif operation == "flush":
                batcher.flush()
            else:
                clock.now += rng.choice([0.1, 0.5, 2.0])
        except FlushError as error:
            ledger.caught(error)
        assert batcher.pending == len(accepted) - sum(map(len, ledger.calls))
        ledger.check(accepted, size, finished=False)
    try:
        batcher.close()
    except FlushError as error:
        ledger.caught(error)
    assert batcher.pending == 0
    ledger.check(accepted, size, finished=True)


async def _async_run(rng: random.Random, ledger: Ledger) -> tuple[list[int], int]:
    size, options = _sync_options(rng, ledger)
    options["max_seconds"] = math.inf  # the event loop waits in real time on a window

    async def send(batch: list[int]) -> None:
        if rng.random() < 0.5:
            await asyncio.sleep(0)
        ledger.sink(batch)

    sinks: list[Callable[[list[int]], object]] = [send, ledger.sink]
    batcher = AsyncTimedBatcher(rng.choice(sinks), **options)  # type: ignore[arg-type]
    accepted: list[int] = []
    for value in range(rng.randint(0, 60)):
        operation = rng.choice(["add", "add", "add", "yield", "flush"])
        try:
            if operation == "add":
                await batcher.add(value, timeout=rng.choice([0, 0.001, None]))
                accepted.append(value)
            elif operation == "yield":
                await asyncio.sleep(0)
            else:
                await batcher.flush()
        except FlushError as error:
            ledger.caught(error)
        except BatcherFull:
            pass
        # A batch whose async delivery is still running is called but not yet settled.
        unsettled = batcher.pending - (len(accepted) - sum(map(len, ledger.calls)))
        assert 0 <= unsettled <= size
        ledger.check(accepted, size, finished=False)
    try:
        await batcher.aclose()
    except FlushError as error:
        ledger.caught(error)
    assert batcher.pending == 0
    return accepted, size


@pytest.mark.parametrize("seed", SEEDS)
def test_the_async_batcher_settles_every_item_once(seed: int) -> None:
    rng = random.Random(seed)
    ledger = Ledger(rng, rng.choice([0.0, 0.2, 0.6]))
    accepted, size = asyncio.run(_async_run(rng, ledger))
    ledger.check(accepted, size, finished=True)
