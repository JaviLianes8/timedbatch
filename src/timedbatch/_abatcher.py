"""The asyncio counterpart of TimedBatcher, for a single event loop."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Generic

from ._core import Buffer, Ledger, T
from ._errors import BatcherClosed, BatcherFull
from ._limits import check_limits


class AsyncTimedBatcher(Generic[T]):
    """Collect items from coroutines and await ``flush(batch)`` in batches.

    The same promises as :class:`TimedBatcher`: delivery on size, age (also while
    idle), :meth:`flush` and :meth:`aclose`; one batch at a time, in order; every
    accepted item delivered once or handed back once through ``on_error`` or a
    :class:`FlushError`; ``max_pending`` makes :meth:`add` wait. Use it from one event
    loop only, and close it (``async with``): there is no exit hook for coroutines.
    """

    def __init__(
        self,
        flush: Callable[[list[T]], Awaitable[object]],
        *,
        max_items: int,
        max_seconds: float,
        max_pending: int | None = None,
        on_error: Callable[[Exception, list[T]], object] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not callable(flush):
            raise TypeError("flush must be callable")
        items, seconds, self._max_pending = check_limits(max_items, max_seconds, max_pending)
        self._sink = flush
        self._on_error = on_error
        self._buffer: Buffer[T] = Buffer(items, seconds, clock)
        self._ledger: Ledger[T] = Ledger()
        self._accepted = self._done = self._wanted = 0
        self._closed = False
        self._task: asyncio.Task[None] | None = None
        self._wake: asyncio.Event | None = None
        self._progress: asyncio.Event | None = None

    @property
    def pending(self) -> int:
        """Items accepted and not yet delivered or handed back."""
        return self._accepted - self._done

    @property
    def closed(self) -> bool:
        """Whether :meth:`aclose` has been called."""
        return self._closed

    async def add(self, item: T, timeout: float | None = None) -> None:
        """Accept ``item``, waiting up to ``timeout`` seconds for room under ``max_pending``."""
        if self._closed:
            raise BatcherClosed("the batcher is closed")
        self._ledger.raise_any()
        self._start()
        limit = self._max_pending
        if limit is not None and self.pending >= limit:
            if self._inside():
                raise BatcherFull("add() inside the flush callback cannot wait for room")
            await self._wait(lambda: self.pending < limit, timeout, BatcherFull)
            if self._closed:
                raise BatcherClosed("the batcher closed while add() was waiting")
        self._buffer.append(item)
        self._accepted += 1
        self._signal()

    async def flush(self, timeout: float | None = None) -> None:
        """Deliver every item accepted so far; raise ``TimeoutError`` if it takes longer."""
        self._refuse_inside("flush")
        self._start()
        target = self._wanted = max(self._wanted, self._accepted)
        self._signal()
        await self._wait(lambda: self._done >= target, timeout, TimeoutError)
        self._ledger.raise_any()

    async def aclose(self) -> None:
        """Stop accepting items, deliver the rest and stop the task. Idempotent."""
        self._refuse_inside("aclose")
        self._closed = True
        if self._task is not None:
            self._signal()
            await asyncio.shield(self._task)
        self._ledger.raise_any()

    async def __aenter__(self) -> AsyncTimedBatcher[T]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def _start(self) -> None:
        if self._task is None:
            self._wake, self._progress = asyncio.Event(), asyncio.Event()
            self._task = asyncio.get_running_loop().create_task(self._run())

    def _signal(self) -> None:
        if self._wake is not None:
            self._wake.set()

    def _inside(self) -> bool:
        return self._task is not None and asyncio.current_task() is self._task

    def _refuse_inside(self, name: str) -> None:
        if self._inside():
            raise RuntimeError(f"{name}() cannot be called from inside the flush callback")

    async def _wait(
        self, ready: Callable[[], bool], timeout: float | None, error: type[Exception]
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while not ready():
            assert self._task is not None and self._progress is not None
            if self._task.done():
                raise RuntimeError("the flush task has stopped")
            event = self._progress
            left = None if deadline is None else deadline - loop.time()
            if left is not None and left <= 0:
                raise error("timed out waiting for the flush task")
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(event.wait(), left)

    async def _run(self) -> None:
        assert self._wake is not None and self._progress is not None
        while True:
            batch = self._buffer.take(self._closed or self._wanted > self._done)
            if batch is None:
                if self._closed:
                    return
                self._wake.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), self._buffer.remaining())
                continue
            try:
                await self._sink(list(batch))
            except Exception as error:
                await self._report(error, batch)
            except BaseException as error:
                self._ledger.record(error, batch)
                self._finish(batch)
                raise
            self._finish(batch)

    async def _report(self, error: Exception, batch: list[T]) -> None:
        if self._on_error is None:
            self._ledger.record(error, batch)
            return
        try:
            outcome = self._on_error(error, list(batch))
            if inspect.isawaitable(outcome):
                await outcome
        except BaseException as handler_error:
            self._ledger.record(handler_error, batch)
            if not isinstance(handler_error, Exception):
                self._finish(batch)
                raise

    def _finish(self, batch: list[T]) -> None:
        assert self._progress is not None
        self._done += len(batch)
        # Waiters hold the old event; swapping in a fresh one wakes them all exactly once.
        self._progress.set()
        self._progress = asyncio.Event()
