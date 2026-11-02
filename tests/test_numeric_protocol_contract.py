"""Hand-written numeric range and async protocol boundary contracts."""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterator
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest

from timedbatch import AsyncTimedBatcher, TimedBatcher, abatched_by_time, batched_by_time


async def nothing() -> AsyncIterator[int]:
    return
    yield


@pytest.mark.parametrize("duration", [Decimal("1e-1000"), Fraction(1, 10**1000)])
def test_positive_underflow_is_accepted_by_every_interface(duration: Any) -> None:
    assert list(batched_by_time([], 2, duration)) == []
    out: TimedBatcher[int] = TimedBatcher(
        print, max_items=2, max_seconds=duration, background=False
    )
    out.close()

    async def main() -> None:
        assert [batch async for batch in abatched_by_time(nothing(), 2, duration)] == []
        async with AsyncTimedBatcher(print, max_items=2, max_seconds=duration):
            pass

    asyncio.run(main())


def test_underflow_rounds_up_to_smallest_positive_float() -> None:
    now = 0.0
    sent: list[list[int]] = []
    out = TimedBatcher(
        sent.append,
        max_items=2,
        max_seconds=Decimal("1e-1000"),
        clock=lambda: now,
        background=False,
    )
    out.add(7)
    out.poll()
    assert sent == []
    now = math.nextafter(0.0, 1.0)
    out.poll()
    assert sent == [[7]]
    out.close()


@pytest.mark.parametrize(
    "argument", ["max_items", "max_seconds", "max_pending", "clock", "on_error"]
)
def test_validation_does_not_echo_dynamic_type_names(argument: str) -> None:
    hostile = type("password=secret-" + "x" * 100_000, (), {})()
    options: dict[str, Any] = {"max_items": 1, "max_seconds": 1, argument: hostile}
    for cls in (TimedBatcher, AsyncTimedBatcher):
        with pytest.raises(TypeError) as caught:
            cls(print, **options)
        assert "secret" not in str(caught.value)
        assert len(str(caught.value)) < 80


@pytest.mark.parametrize("result", [None, 1, [], object()])
def test_malformed_async_iterators_raise_typeerror(result: object) -> None:
    class Malformed:
        def __aiter__(self) -> Any:
            return result

    async def main() -> None:
        with pytest.raises(TypeError):
            async for _ in abatched_by_time(Malformed(), 1, 1):
                pass

    asyncio.run(main())


def test_async_source_rejection_does_not_echo_dynamic_type_names() -> None:
    hostile = type("password=secret-" + "x" * 100_000, (), {})()
    with pytest.raises(TypeError) as caught:
        abatched_by_time(hostile, 1, 1)
    assert "secret" not in str(caught.value)
    assert len(str(caught.value)) < 80


def test_async_iterator_flushes_at_exact_age() -> None:
    async def main() -> None:
        now = 0.0

        async def arrivals() -> AsyncIterator[int]:
            nonlocal now
            for moment, item in [(0.0, 1), (5.0, 2), (5.0, 3)]:
                now = moment
                yield item

        batches = [batch async for batch in abatched_by_time(arrivals(), 10, 5, clock=lambda: now)]
        assert batches == [[1, 2], [3]]

    asyncio.run(main())
