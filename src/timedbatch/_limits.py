"""Validation of the batching limits shared by every entry point."""

from __future__ import annotations

import math
from numbers import Real


def _count(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an int, not {type(value).__name__}")
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return int(value)


def check_limits(
    max_items: object, max_seconds: object, max_pending: object = None
) -> tuple[int, float, int | None]:
    """Return the limits as exact types, or raise ``TypeError`` / ``ValueError``."""
    items = _count("max_items", max_items)
    if isinstance(max_seconds, bool) or not isinstance(max_seconds, Real):
        raise TypeError(f"max_seconds must be a real number, not {type(max_seconds).__name__}")
    seconds = float(max_seconds)
    # NaN compares false with everything, so it would silently disable the age limit.
    if math.isnan(seconds) or seconds <= 0:
        raise ValueError("max_seconds must be greater than 0")
    if max_pending is None:
        return items, seconds, None
    pending = _count("max_pending", max_pending)
    if pending < items:
        raise ValueError("max_pending must be at least max_items")
    return items, seconds, pending
