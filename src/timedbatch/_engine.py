"""The locking and delivery machinery behind TimedBatcher."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Generic

from ._core import Buffer, Ledger, T, call_sink
from ._errors import BatcherClosed, BatcherFull


class Engine(Generic[T]):
    """Buffer, counters and the one-at-a-time delivery loop, guarded by one lock."""

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
        self._lock = threading.Lock()
        self._wake = threading.Condition(self._lock)
        self._progress = threading.Condition(self._lock)
        self._order = threading.Lock()
        self._accepted = self._done = self._wanted = 0
        self._closed = False
        self._delivering: int | None = None

    def _refuse_inside(self, name: str) -> None:
        with self._lock:
            if self._delivering == threading.get_ident():
                raise RuntimeError(f"{name}() cannot be called from inside the flush callback")

    def _raise_failures(self) -> None:
        with self._lock:
            self._ledger.raise_any()

    def _wait_for_room(self, timeout: float | None, inside: bool) -> None:
        limit = self._max_pending
        if limit is None or self._accepted - self._done < limit:
            return
        if inside:
            raise BatcherFull("add() inside the flush callback cannot wait for room")
        # Waiting uses the real clock: the injected one only measures batch age.
        deadline = None if timeout is None else time.monotonic() + timeout
        while self._accepted - self._done >= limit:
            left = None if deadline is None else deadline - time.monotonic()
            if left is not None and left <= 0:
                raise BatcherFull(f"no room under max_pending={limit} after {timeout} seconds")
            self._progress.wait(_bounded(left))
            if self._closed:
                raise BatcherClosed("the batcher closed while add() was waiting")

    def _wait_until_done(self, target: int, timeout: float | None) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while self._done < target:
                left = None if deadline is None else deadline - time.monotonic()
                if left is not None and left <= 0:
                    raise TimeoutError("flush() timed out before every item was delivered")
                self._progress.wait(_bounded(left))

    def _next_batch(self) -> list[T] | None:
        batch = self._buffer.take(self._closed or self._wanted > self._done)
        if batch is not None:
            self._delivering = threading.get_ident()
        return batch

    def _deliver(self, batch: list[T]) -> BaseException | None:
        error = call_sink(self._sink, self._on_error, batch)
        with self._lock:
            self._delivering = None
            if error is not None:
                self._ledger.record(error, batch)
            self._done += len(batch)
            self._progress.notify_all()
        return error

    def _drain(self) -> None:
        with self._order:
            while True:
                with self._lock:
                    batch = self._next_batch()
                if batch is None:
                    return
                error = self._deliver(batch)
                if error is not None and not isinstance(error, Exception):
                    raise error

    def _run(self) -> None:
        with self._lock:
            while True:
                batch = self._next_batch()
                if batch is None:
                    if self._closed:
                        return
                    self._wake.wait(_bounded(self._buffer.remaining()))
                    continue
                self._lock.release()
                try:
                    self._deliver(batch)
                finally:
                    self._lock.acquire()


def _bounded(seconds: float | None) -> float | None:
    # Condition.wait raises OverflowError past TIMEOUT_MAX, which would kill the thread.
    return None if seconds is None else min(seconds, threading.TIMEOUT_MAX)
