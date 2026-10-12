"""Batch items by a max count and a max age, flushing when either limit is hit."""

from __future__ import annotations

from ._abatcher import AsyncTimedBatcher
from ._abatching import abatched_by_time
from ._batcher import TimedBatcher
from ._batching import batched_by_time
from ._errors import BatcherClosed, BatcherFull, FlushError

__version__ = "0.2.0"

__all__ = [
    "AsyncTimedBatcher",
    "BatcherClosed",
    "BatcherFull",
    "FlushError",
    "TimedBatcher",
    "abatched_by_time",
    "batched_by_time",
]
