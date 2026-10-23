"""Independent implementations run side by side with timedbatch (skipped when not installed).

``more-itertools`` ``chunked`` is a count-only reference. ``reactivex``
``buffer_with_time_or_count`` is the best known count-or-time buffer; the README says how
its windows differ, and the last test shows that difference on one timeline.
"""

from __future__ import annotations

import math
import random
from typing import Any

import pytest
from conftest import FakeClock, make

from timedbatch import batched_by_time


def test_count_only_batching_matches_more_itertools_chunked() -> None:
    more_itertools = pytest.importorskip("more_itertools")
    rng = random.Random(7)
    for _ in range(2_000):
        data = [rng.random() for _ in range(rng.randint(0, 40))]
        size = rng.randint(1, 12)
        expected = list(more_itertools.chunked(data, size))
        assert list(batched_by_time(data, size, math.inf)) == expected


def test_reactivex_windows_run_on_a_fixed_schedule_and_timedbatch_ages_from_the_first_item() -> (
    None
):
    operators = pytest.importorskip("reactivex.operators")
    testing = pytest.importorskip("reactivex.testing")
    ReactiveTest, TestScheduler = testing.ReactiveTest, testing.TestScheduler

    arrivals = [(210, "a"), (300, "b"), (900, "c")]
    scheduler = TestScheduler()
    source = scheduler.create_hot_observable(
        *(ReactiveTest.on_next(tick, item) for tick, item in arrivals),
        ReactiveTest.on_completed(2000),
    )
    window = scheduler.to_seconds(500)
    observed = scheduler.start(
        lambda: source.pipe(operators.buffer_with_time_or_count(timespan=window, count=10)),
        created=100,
        subscribed=200,
        disposed=5000,
    )
    rx: list[tuple[float, Any]] = [
        (message.time, message.value.value)
        for message in observed.messages
        if message.value.kind == "N"
    ]
    # Windows open at subscription (200) and every 500 ticks after; empty ones are emitted.
    assert rx == [(700.0, ["a", "b"]), (1200.0, ["c"]), (1700.0, []), (2000.0, [])]

    clock = FakeClock()
    ours: list[tuple[float, list[Any]]] = []
    # The fake clock counts ticks, as the scheduler above does.
    batcher = make(
        lambda batch: ours.append((clock.now, batch)), clock, max_items=10, max_seconds=500
    )
    pending = dict(arrivals)
    for tick in range(200, 2001, 10):
        clock.now = tick
        if tick in pending:
            batcher.add(pending[tick])  # type: ignore[arg-type]
        batcher.poll()
    # Each batch is due 500 ticks after its own first item; nothing is sent while empty.
    assert ours == [(710, ["a", "b"]), (1400, ["c"])]
