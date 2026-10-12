"""Batch an iterable by both a maximum count and a maximum time window."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from typing import TypeVar

_T = TypeVar("_T")


def batched_by_time(
    iterable: Iterable[_T],
    max_items: int,
    max_seconds: float,
    *,
    clock: Callable[[], float] = time.monotonic,
) -> Iterator[list[_T]]:
    """Yield batches of items, flushing when either limit is reached.

    A batch is emitted as soon as it holds ``max_items`` items or ``max_seconds``
    have elapsed since the batch started, whichever comes first. The window is
    checked only when an item arrives, so a batch is never cut mid wait. Any
    remaining items are flushed when the source is exhausted.

    The ``clock`` returns a monotonic timestamp in seconds. Inject it to make the
    behaviour deterministic in tests; it defaults to :func:`time.monotonic`.
    """
    if max_items < 1:
        raise ValueError("max_items must be at least 1")
    if max_seconds <= 0:
        raise ValueError("max_seconds must be greater than 0")

    batch: list[_T] = []
    started = 0.0
    for item in iterable:
        if not batch:
            started = clock()
        batch.append(item)
        if len(batch) >= max_items or clock() - started >= max_seconds:
            yield batch
            batch = []
    if batch:
        yield batch
