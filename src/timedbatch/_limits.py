"""Validation of the batching limits shared by every entry point."""

from __future__ import annotations

import math
import operator
from decimal import Decimal
from fractions import Fraction
from numbers import Real
from typing import TypeAlias

Seconds: TypeAlias = float | Decimal | Fraction | Real


def _count(name: str, value: object) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be an int, not bool")
    try:
        # operator.index accepts int and integer types such as NumPy's, never floats.
        count = operator.index(value)  # type: ignore[arg-type]
    except TypeError:
        raise TypeError(f"{name} must be an integer supporting __index__") from None
    if count < 1:
        raise ValueError(f"{name} must be at least 1")
    return count


def _seconds(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        raise TypeError(f"{name} must be a real number")
    # NaN compares false with everything, so it would silently disable a limit.
    if isinstance(value, Decimal) and value.is_nan():
        raise ValueError(f"{name} must not be NaN")
    try:
        seconds = float(value)
    except OverflowError:
        # An int or Fraction beyond the float range: as long as any float can measure.
        seconds = math.inf if value > 0 else -math.inf  # type: ignore[operator]
    if math.isnan(seconds):
        raise ValueError(f"{name} must not be NaN")
    if seconds == 0 and value > 0:  # type: ignore[operator]
        seconds = math.nextafter(0.0, 1.0)
    return seconds


def check_limits(
    max_items: object, max_seconds: object, max_pending: object = None
) -> tuple[int, float, int | None]:
    """Return the limits as exact types, or raise ``TypeError`` / ``ValueError``."""
    items = _count("max_items", max_items)
    seconds = _seconds("max_seconds", max_seconds)
    if seconds <= 0:
        raise ValueError("max_seconds must be greater than 0")
    if max_pending is None:
        return items, seconds, None
    pending = _count("max_pending", max_pending)
    if pending < items:
        raise ValueError("max_pending must be at least max_items")
    return items, seconds, pending


def check_timeout(timeout: object) -> float | None:
    """Return a wait timeout in seconds (``None`` waits forever); negative means now."""
    return None if timeout is None else _seconds("timeout", timeout)


def check_callables(**functions: object) -> None:
    """Require callables, allowing ``None`` only for the optional error handler."""
    for name, function in functions.items():
        if not callable(function) and not (name == "on_error" and function is None):
            raise TypeError(f"{name} must be callable")
