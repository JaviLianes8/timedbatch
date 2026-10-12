"""Batch an async iterable by count and age, flushing on time even while it is idle."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import AsyncIterable, AsyncIterator, Callable
from typing import Any, TypeVar

from ._limits import check_limits

_T = TypeVar("_T")


def abatched_by_time(
    aiterable: AsyncIterable[_T],
    max_items: int,
    max_seconds: float,
    *,
    clock: Callable[[], float] = time.monotonic,
) -> AsyncIterator[list[_T]]:
    """Yield batches from an async iterable, flushing on ``max_items`` or ``max_seconds``.

    Unlike :func:`batched_by_time`, the age limit fires while the source is idle: a
    partial batch is yielded ``max_seconds`` after its first item even if no further
    item arrives. The pending read of the source is kept across those flushes, never
    cancelled, so no item is lost; it is cancelled only when the iterator is closed. If
    the source raises, the items already read are yielded first. ``clock`` must tick in
    seconds; real waiting uses the event loop.
    """
    items, seconds, _ = check_limits(max_items, max_seconds)
    return _abatches(aiterable.__aiter__(), items, seconds, clock)


async def _abatches(
    source: AsyncIterator[_T], max_items: int, max_seconds: float, clock: Callable[[], float]
) -> AsyncIterator[list[_T]]:
    batch: list[_T] = []
    started = 0.0
    pending: asyncio.Future[Any] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(source.__anext__())
            timeout = None
            if batch and not math.isinf(max_seconds):
                timeout = max(0.0, started + max_seconds - clock())
            # asyncio.wait, unlike wait_for, leaves the read running when it times out.
            done, _ = await asyncio.wait({pending}, timeout=timeout)
            if not done:
                yield batch
                batch = []
                continue
            read, pending = pending, None
            try:
                item = read.result()
            except StopAsyncIteration:
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
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.wait({pending})
            if not pending.cancelled():
                pending.exception()
