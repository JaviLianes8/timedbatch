"""Exceptions raised by the batchers."""

from __future__ import annotations

from typing import Any


class FlushError(Exception):
    """One or more batches could not be delivered.

    ``batches`` holds every failed batch, in delivery order, and ``errors`` the exception
    each one raised, so the caller can retry or store them: no item is dropped silently.
    The batches hold the items exactly as they were added (``list[list[Any]]``: an
    exception type cannot carry the batcher's item type). Messages contain only counts;
    original exceptions are available only through ``errors``, without traceback chaining.

    >>> from timedbatch import FlushError
    >>> error = FlushError([["a", "b"]], [ConnectionError("down")])
    >>> print(error)
    1 batch (2 items) failed to flush
    >>> error.batches, isinstance(error.errors[0], ConnectionError)
    ([['a', 'b']], True)
    """

    def __init__(self, batches: list[list[Any]], errors: list[BaseException]) -> None:
        if not errors or len(batches) != len(errors):
            raise ValueError("FlushError needs one error per failed batch, and at least one")
        count = sum(len(batch) for batch in batches)
        noun = "batch" if len(batches) == 1 else "batches"
        items = "item" if count == 1 else "items"
        super().__init__(f"{len(batches)} {noun} ({count} {items}) failed to flush")
        self.batches = batches
        self.errors = errors
        self.__suppress_context__ = True

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.batches, self.errors))


class BatcherClosed(RuntimeError):
    """The batcher was closed, so it accepts no more items.

    >>> from timedbatch import BatcherClosed, TimedBatcher
    >>> batcher = TimedBatcher(print, max_items=10, max_seconds=60.0, background=False)
    >>> batcher.close()
    >>> try:
    ...     batcher.add("late")
    ... except BatcherClosed as error:
    ...     print(error)
    the batcher is closed
    """


class BatcherFull(TimeoutError):
    """``add`` gave up waiting for room under ``max_pending``; the item was not accepted.

    >>> import threading
    >>> from timedbatch import BatcherFull, TimedBatcher
    >>> gate = threading.Event()
    >>> batcher = TimedBatcher(
    ...     lambda batch: gate.wait(), max_items=1, max_seconds=60.0, max_pending=1
    ... )
    >>> batcher.add("a")
    >>> try:
    ...     batcher.add("b", timeout=0)
    ... except BatcherFull as error:
    ...     print(error)
    no room under max_pending=1 within 0.0 seconds
    >>> gate.set()
    >>> batcher.close()
    """
