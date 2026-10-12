"""Batch an iterable by a max count and a max time window, flushing on either limit."""

from __future__ import annotations

from ._batching import batched_by_time

__version__ = "0.1.0"

__all__ = ["batched_by_time"]
