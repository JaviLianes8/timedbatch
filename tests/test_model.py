"""Seeded random timelines checked against small reference models written for the tests."""

from __future__ import annotations

import random
from collections.abc import Iterator

import pytest
from conftest import FakeClock

from timedbatch import TimedBatcher, batched_by_time

SEEDS = range(300)


def _iterator_model(steps: list[tuple[int, float]], size: int, window: float) -> list[list[int]]:
    batches: list[list[int]] = []
    current: list[int] = []
    start = 0.0
    for value, moment in steps:
        if not current:
            start = moment
        current.append(value)
        if len(current) == size or moment - start >= window:
            batches.append(current)
            current = []
    return [*batches, current] if current else batches


def _push_model(
    events: list[tuple[str, float]], size: int, window: float
) -> list[tuple[int, list[int]]]:
    """Expected (event index, batch) pairs: which call delivered which batch."""
    delivered: list[tuple[int, list[int]]] = []
    buffer: list[tuple[int, float]] = []
    value = 0
    for index, (kind, moment) in enumerate(events):
        if kind == "add":
            buffer.append((value, moment))
            value += 1
        while buffer and (len(buffer) >= size or moment - buffer[0][1] >= window):
            delivered.append((index, [item for item, _ in buffer[:size]]))
            buffer = buffer[size:]
    return delivered


def _timeline(rng: random.Random, count: int) -> list[float]:
    moments, now = [], 0.0
    for _ in range(count):
        now += rng.choice([0.0, 0.0, 0.1, 0.5, 1.0, 2.5, 7.0])
        moments.append(now)
    return moments


@pytest.mark.parametrize("seed", SEEDS)
def test_batched_by_time_matches_the_model(seed: int) -> None:
    rng = random.Random(seed)
    size, window = rng.randint(1, 6), rng.choice([0.5, 1.0, 3.0, 10.0])
    steps = list(enumerate(_timeline(rng, rng.randint(0, 40))))
    clock = FakeClock()

    def source() -> Iterator[int]:
        for value, moment in steps:
            clock.now = moment
            yield value

    batches = list(batched_by_time(source(), size, window, clock=clock))

    assert batches == _iterator_model(steps, size, window)


@pytest.mark.parametrize("seed", SEEDS)
def test_push_batcher_matches_the_model(seed: int) -> None:
    rng = random.Random(seed)
    size, window = rng.randint(1, 6), rng.choice([0.5, 1.0, 3.0, 10.0])
    events = [(rng.choice(["add", "add", "poll"]), t) for t in _timeline(rng, rng.randint(0, 50))]
    clock = FakeClock()
    seen: list[tuple[int, list[int]]] = []
    index = 0
    batcher: TimedBatcher[int] = TimedBatcher(
        lambda batch: seen.append((index, batch)),
        max_items=size,
        max_seconds=window,
        clock=clock,
        background=False,
        flush_at_exit=False,
    )
    value = 0
    for index, (kind, moment) in enumerate(events):  # noqa: B007 (read by the sink)
        clock.now = moment
        if kind == "add":
            batcher.add(value)
            value += 1
        else:
            batcher.poll()

    assert seen == _push_model(events, size, window)
    batcher.close()
    assert [item for _, batch in seen for item in batch] == list(range(value))
