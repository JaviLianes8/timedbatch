"""Clock validation must happen before a producer can accept any items."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from timedbatch import AsyncTimedBatcher, TimedBatcher, abatched_by_time, batched_by_time


async def _source() -> AsyncIterator[int]:
    yield 1


def test_none_is_not_a_clock() -> None:
    options = {"clock": None}
    with pytest.raises(TypeError, match=r"^clock must be callable$"):
        batched_by_time([1], 2, 1.0, **options)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=r"^clock must be callable$"):
        abatched_by_time(_source(), 2, 1.0, **options)  # type: ignore[arg-type]
    for cls in (TimedBatcher, AsyncTimedBatcher):
        with pytest.raises(TypeError, match=r"^clock must be callable$"):
            cls(print, max_items=2, max_seconds=1.0, **options)  # type: ignore[arg-type]
