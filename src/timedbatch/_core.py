"""State shared by the push-style batchers: the buffer and the failure ledger."""

from __future__ import annotations

import math
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
        self._items: list[T] = []
        # One stamp per item: after a slow delivery the buffer can hold more than one
        # batch, and the next batch's age must come from its own oldest item.
        self._stamps: list[float] = []

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
        if not force and len(self._items) < self.max_items:
            left = self.remaining()
            if left is None or left > 0:
                return None
        batch = self._items[: self.max_items]
        del self._items[: self.max_items]
        del self._stamps[: self.max_items]
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
        sink(list(batch))
    except Exception as error:
        if on_error is None:
            return error
        try:
            on_error(error, list(batch))
        except BaseException as handler_error:
            return handler_error
    except BaseException as error:
        return error
    return None
