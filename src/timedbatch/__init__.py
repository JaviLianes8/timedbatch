"""Batch items by a max count and a max age, flushing when either limit is hit."""

from __future__ import annotations

from ._abatcher import AsyncTimedBatcher
from ._abatching import abatched_by_time
from ._batcher import TimedBatcher
from ._batching import batched_by_time
from ._errors import BatcherClosed, BatcherFull, FlushError

__version__ = "1.0.0"

__all__ = [
    "AsyncTimedBatcher",
    "BatcherClosed",
    "BatcherFull",
    "FlushError",
    "TimedBatcher",
    "abatched_by_time",
    "batched_by_time",
]

# The public names report the package as their home, so reprs, pickles and docs never
# depend on the private module layout, which is free to change.
for _name in __all__:
    globals()[_name].__module__ = __name__
del _name
