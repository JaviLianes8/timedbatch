"""Exceptions raised by the batchers."""

from __future__ import annotations

from typing import Any


class FlushError(Exception):
    """One or more batches could not be delivered.

    ``batches`` holds every failed batch, in delivery order, and ``errors`` the exception
    each one raised, so the caller can retry or store them: no item is dropped silently.
    """

    def __init__(self, batches: list[list[Any]], errors: list[BaseException]) -> None:
        count = sum(len(batch) for batch in batches)
        noun = "batch" if len(batches) == 1 else "batches"
        super().__init__(f"{len(batches)} {noun} ({count} items) failed to flush: {errors[0]!r}")
        self.batches = batches
        self.errors = errors
        self.__cause__ = errors[0]

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.batches, self.errors))


class BatcherClosed(RuntimeError):
    """The batcher was closed, so it accepts no more items."""


class BatcherFull(TimeoutError):
    """``add`` gave up waiting for room under ``max_pending``; the item was not accepted."""
