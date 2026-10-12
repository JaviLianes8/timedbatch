"""Batch an async iterable by count and age, flushing on time even while it is idle."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Callable
from typing import Any, TypeVar

from ._limits import check_limits

_T = TypeVar("_T")


def abatched_by_time(
    aiterable: AsyncIterable[_T],
    max_items: int,
    max_seconds: float,
    *,
    clock: Callable[[], float] = time.monotonic,
) -> AsyncGenerator[list[_T], None]:
    """Yield batches from an async iterable, flushing on ``max_items`` or ``max_seconds``.

    Unlike :func:`batched_by_time`, the age limit fires while the source is idle: a
    partial batch is yielded ``max_seconds`` after its first item even if no further
    item arrives. The pending read of the source is kept across those flushes, never
    cancelled, so no item is lost; it is cancelled only when the iterator is closed. If
    the source raises, the items already read are yielded first. ``clock`` must tick in
    seconds; real waiting uses the event loop. Close it early with ``aclose()`` (or
    :func:`contextlib.aclosing`): the read in flight is cancelled, and an item it had
    already taken from the source is dropped, together with the partial batch. Each read
    of the source runs in its own task, so context variables set by the source do not
    carry over from one item to the next.
    """
    items, seconds, _ = check_limits(max_items, max_seconds)
    if not isinstance(aiterable, AsyncIterable):
        raise TypeError(f"expected an async iterable, not {type(aiterable).__name__}")
    return _abatches(aiterable.__aiter__(), items, seconds, clock)


async def _abatches(
    source: AsyncIterator[_T], max_items: int, max_seconds: float, clock: Callable[[], float]
) -> AsyncGenerator[list[_T], None]:
    batch: list[_T] = []
    started = 0.0
    pending: asyncio.Future[Any] | None = None
    try:
        while True:
            if pending is None:
                try:
                    step = source.__anext__()
                except StopAsyncIteration:
                    break
                except Exception:
                    if batch:
                        yield batch
                    raise
                pending = asyncio.ensure_future(step)
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
