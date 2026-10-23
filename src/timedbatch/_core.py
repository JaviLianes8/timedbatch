"""State shared by the push-style batchers: the buffer and the failure ledger."""

from __future__ import annotations

import asyncio
import inspect
import math
from collections import deque
from collections.abc import Callable
from typing import Generic, TypeVar

from ._errors import FlushError

T = TypeVar("T")


class Buffer(Generic[T]):
    """Items waiting for delivery, with the time each one arrived. Not thread-safe."""

    def __init__(self, max_items: int, max_seconds: float, clock: Callable[[], float]) -> None:
        self.max_items = max_items
        self.max_seconds = max_seconds
        self.clock = clock
        # Deques: a slow sink can leave many batches waiting, and taking each one from the
        # front of a list would make draining them quadratic.
        self._items: deque[T] = deque()
        # One stamp per item: after a slow delivery the buffer can hold more than one
        # batch, and the next batch's age must come from its own oldest item.
        self._stamps: deque[float] = deque()

    def __len__(self) -> int:
        return len(self._items)

    def append(self, item: T) -> None:
        self._stamps.append(self.clock())
        self._items.append(item)

    def remaining(self) -> float | None:
        """Seconds until the oldest item is due, or ``None`` when nothing will come due."""
        if not self._items or math.isinf(self.max_seconds):
            return None
        return self._stamps[0] + self.max_seconds - self.clock()

    def take(self, force: bool) -> list[T] | None:
        """Remove and return the next batch if it is due (or ``force``), else ``None``."""
        if not self._items:
            return None
        if (
            not force
            and len(self._items) < self.max_items
            and self.clock() - self._stamps[0] < self.max_seconds
        ):
            return None
        size = min(self.max_items, len(self._items))
        batch = [self._items.popleft() for _ in range(size)]
        for _ in range(size):
            self._stamps.popleft()
        return batch


class Ledger(Generic[T]):
    """Failed batches not yet reported to the producer."""

    def __init__(self) -> None:
        self._batches: list[list[T]] = []
        self._errors: list[BaseException] = []

    def record(self, error: BaseException, batch: list[T]) -> None:
        self._batches.append(batch)
        self._errors.append(error)

    def raise_any(self) -> None:
        if self._batches:
            batches, errors = self._batches, self._errors
            self._batches, self._errors = [], []
            raise FlushError(list(batches), errors)


def call_sink(
    sink: Callable[[list[T]], object],
    on_error: Callable[[Exception, list[T]], object] | None,
    batch: list[T],
) -> BaseException | None:
    """Deliver ``batch``; return the exception that still needs reporting, if any."""
    try:
        # The sink gets its own list, so a sink that clears it cannot empty the copy we
        # hand back on failure.
        _plain(sink(list(batch)))
    except Exception as error:
        if on_error is None:
            return error
        try:
            _plain(on_error(error, list(batch)))
        except BaseException as handler_error:
            return handler_error
    except BaseException as error:
        return error
    return None


def is_async(function: object) -> bool:
    """Whether calling ``function`` returns a coroutine (functions and callable objects)."""
    call = type(function).__call__
    return inspect.iscoroutinefunction(function) or inspect.iscoroutinefunction(call)


def _plain(outcome: object) -> None:
    # A coroutine that is never awaited does nothing: treat it as a failed delivery.
    if inspect.isawaitable(outcome):
        if inspect.iscoroutine(outcome):
            outcome.close()
        raise TypeError("the flush function returned an awaitable; use AsyncTimedBatcher")


async def wait_event(event: asyncio.Event, timeout: float | None) -> None:
    """Wait for ``event`` at most ``timeout`` seconds, without raising on timeout."""
    # asyncio.wait instead of wait_for: on 3.10 and 3.11 wait_for can swallow a
    # cancellation that arrives as the event fires.
    waiter = asyncio.ensure_future(event.wait())
    try:
        await asyncio.wait({waiter}, timeout=timeout)
    finally:
        waiter.cancel()
