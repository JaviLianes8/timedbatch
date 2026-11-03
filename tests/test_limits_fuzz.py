"""Seeded hostile limit values: each entry point accepts exactly what an independent rule says."""

from __future__ import annotations

import math
import operator
import random
from decimal import Decimal
from fractions import Fraction
from numbers import Real

import pytest

from timedbatch import AsyncTimedBatcher, TimedBatcher, abatched_by_time, batched_by_time


class Index:
    """An integer type that is not an int, like NumPy's."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __index__(self) -> int:
        return self.value


POOL: list[object] = [
    *(0, 1, 2, -1, 7, 10**30, -(10**30), 10**400, True, False),
    *(0.0, -0.0, 0.5, 1.0, 2.0, -1.5, 5e-324, 1e308, math.inf, -math.inf, math.nan),
    *(Decimal("0"), Decimal("1"), Decimal("0.25"), Decimal("-3"), Decimal("1e400")),
    *(Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), Decimal("-Infinity")),
    *(Fraction(1, 3), Fraction(-1, 3), Fraction(10**400), Fraction(0)),
    *(Index(3), Index(0), Index(-2), Index(10**400)),
    *("3", b"3", None, [], 3j, object(), "x" * 10_000),
]


def _count_rule(value: object) -> type[Exception] | None:
    if isinstance(value, bool):
        return TypeError
    try:
        number = operator.index(value)  # type: ignore[arg-type]
    except TypeError:
        return TypeError
    return ValueError if number < 1 else None


def _window_rule(value: object) -> type[Exception] | None:
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        return TypeError
    if isinstance(value, Decimal):
        return ValueError if value.is_nan() or value <= 0 else None
    if isinstance(value, float) and math.isnan(value):
        return ValueError
    return ValueError if value <= 0 else None


def _expected(items: object, seconds: object, pending: object) -> type[Exception] | None:
    error = _count_rule(items) or _window_rule(seconds)
    if error is None and pending is not None:
        error = _count_rule(pending)
        if error is None and operator.index(pending) < operator.index(items):  # type: ignore[arg-type]
            error = ValueError
    return error


def _build(kind: str, items: object, seconds: object, pending: object) -> None:
    if kind == "iter":
        batched_by_time([], items, seconds)  # type: ignore[arg-type]
    elif kind == "aiter":
        abatched_by_time(_Empty(), items, seconds)  # type: ignore[arg-type]
    elif kind == "thread":
        TimedBatcher(
            print,
            max_items=items,  # type: ignore[arg-type]
            max_seconds=seconds,  # type: ignore[arg-type]
            max_pending=pending,  # type: ignore[arg-type]
            background=False,
            flush_at_exit=False,
        )
    else:
        AsyncTimedBatcher(print, max_items=items, max_seconds=seconds, max_pending=pending)  # type: ignore[arg-type]


class _Empty:
    def __aiter__(self) -> _Empty:
        return self

    async def __anext__(self) -> int:
        raise StopAsyncIteration


@pytest.mark.parametrize("seed", range(40))
def test_limits_are_accepted_or_refused_exactly_as_the_rule_says(seed: int) -> None:
    rng = random.Random(seed)
    for _ in range(200):
        kind = rng.choice(["iter", "aiter", "thread", "async"])
        items, seconds = rng.choice(POOL), rng.choice(POOL)
        pending = rng.choice([None, *POOL]) if kind in ("thread", "async") else None
        expected = _expected(items, seconds, pending)
        if expected is None:
            _build(kind, items, seconds, pending)
            continue
        with pytest.raises(expected) as caught:
            _build(kind, items, seconds, pending)
        assert type(caught.value) is expected
        # Messages name the parameter and never quote the value.
        message = str(caught.value)
        assert len(message) < 80, message
        assert message.startswith(("max_items ", "max_seconds ", "max_pending ")), message
