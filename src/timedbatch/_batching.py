"""Batch an iterable by both a maximum count and a maximum time window."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Iterator
from typing import TypeVar

from ._limits import check_limits

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
    have elapsed since its first item arrived, whichever comes first. The window is
    checked only when an item arrives, so an idle source holds its partial batch
    until the next item (use :func:`abatched_by_time` or :class:`TimedBatcher` to
    flush on time while idle). Remaining items are flushed when the source is
    exhausted. If the source raises, the items already read are yielded first and the
    exception is raised on the next step. ``max_seconds`` may be ``math.inf`` to batch
    by count only.

    The ``clock`` returns a monotonic timestamp in seconds. Inject it to make the
    behaviour deterministic in tests; it defaults to :func:`time.monotonic`.
    """
    items, seconds, _ = check_limits(max_items, max_seconds)
    return _batches(iter(iterable), items, seconds, clock)


def _batches(
    source: Iterator[_T], max_items: int, max_seconds: float, clock: Callable[[], float]
) -> Iterator[list[_T]]:
    batch: list[_T] = []
    started = 0.0
    while True:
        try:
            item = next(source)
        except StopIteration:
            break
        except Exception:
            if batch:
                yield batch
            raise
        if not batch:
            started = clock()
        batch.append(item)
        if len(batch) >= max_items or clock() - started >= max_seconds:
            yield batch
            batch = []
    if batch:
        yield batch
