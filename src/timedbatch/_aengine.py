"""The task, waits and failure handling behind AsyncTimedBatcher."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Generic

from ._core import Buffer, Ledger, T, wait_event
from ._errors import BatcherClosed


class AsyncEngine(Generic[T]):
    """Buffer, counters and the flush task of one event loop."""

    def __init__(
        self,
        sink: Callable[[list[T]], object],
        on_error: Callable[[Exception, list[T]], object] | None,
        buffer: Buffer[T],
        max_pending: int | None,
    ) -> None:
        self._sink = sink
        self._on_error = on_error
        self._buffer = buffer
        self._max_pending = max_pending
        self._ledger: Ledger[T] = Ledger()
        self._accepted = self._done = self._wanted = 0
        self._closed = False
        self._stopped: BaseException | None = None
        self._task: asyncio.Task[None] | None = None
        self._wake = self._progress = asyncio.Event()

    def _reap(self) -> None:
        # A task that stopped abnormally (cancelled, even before it ever ran, or killed
        # by a BaseException from the sink) closes the batcher and hands back its items.
        task = self._task
        if task is None or not task.done() or self._stopped is not None:
            return
        error = asyncio.CancelledError() if task.cancelled() else task.exception()
        if error is None:
            return
        self._stopped, self._closed = error, True
        while (batch := self._buffer.take(True)) is not None:
            self._ledger.record(error, batch)
            self._finish(batch)

    def _check_open(self) -> None:
        self._reap()
        self._ledger.raise_any()
        if self._stopped is not None:
            raise BatcherClosed("the flush task stopped") from self._stopped
        if self._closed:
            raise BatcherClosed("the batcher is closed")

    def _start(self) -> None:
        if self._task is None:
            self._wake, self._progress = asyncio.Event(), asyncio.Event()
            self._task = asyncio.get_running_loop().create_task(self._loop())

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
            assert self._task is not None
            if self._task.done():
                self._check_open()
            left = None if deadline is None else deadline - loop.time()
            if left is not None and left <= 0:
                raise error("timed out waiting for the flush task")
            await wait_event(self._progress, left)

    async def _loop(self) -> None:
        while True:
            batch = self._buffer.take(self._closed or self._wanted > self._done)
            if batch is None:
                if self._closed:
                    return
                self._wake.clear()
                await wait_event(self._wake, self._buffer.remaining())
                continue
            try:
                # A plain function has already done its work when it returns, so only an
                # awaitable result is awaited; anything else counts as delivered.
                outcome = self._sink(list(batch))
                if inspect.isawaitable(outcome):
                    await outcome
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
        self._done += len(batch)
        # Waiters hold the old event; swapping in a fresh one wakes them all exactly once.
        self._progress.set()
        self._progress = asyncio.Event()
