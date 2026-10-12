"""Review regressions: async callables in TimedBatcher, timeouts, integer types, errors."""

from __future__ import annotations

import math
import warnings
from collections.abc import Coroutine
from decimal import Decimal
from typing import Any

import pytest
from conftest import make

from timedbatch import FlushError, TimedBatcher, batched_by_time


async def _async_sink(batch: list[int]) -> None:
    return None


class _AsyncCallable:
    async def __call__(self, batch: list[int]) -> None:
        return None


@pytest.mark.parametrize("sink", [_async_sink, _AsyncCallable()])
def test_an_async_flush_function_is_rejected(sink: Any) -> None:
    with pytest.raises(TypeError, match="AsyncTimedBatcher"):
        TimedBatcher(sink, max_items=1, max_seconds=1.0, flush_at_exit=False)


def test_an_async_on_error_is_rejected() -> None:
    with pytest.raises(TypeError):
        make(print, on_error=_async_sink)


def test_a_sink_returning_a_coroutine_fails_the_batch_instead_of_dropping_it() -> None:
    def sneaky(batch: list[int]) -> Coroutine[Any, Any, None]:
        return _async_sink(batch)

    batcher = make(sneaky, max_items=1)
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # the coroutine is closed, never left unawaited
        batcher.add(1)
        with pytest.raises(FlushError) as caught:
            batcher.poll()

    assert caught.value.batches == [[1]]
    assert isinstance(caught.value.errors[0], TypeError)


@pytest.mark.parametrize("bad", [math.nan, "1", True])
def test_bad_timeouts_are_rejected_without_accepting(bad: Any) -> None:
    batcher = make(print)
    with pytest.raises((TypeError, ValueError)):
        batcher.add(1, timeout=bad)
    with pytest.raises((TypeError, ValueError)):
        batcher.flush(timeout=bad)
    with pytest.raises((TypeError, ValueError)):
        batcher.close(timeout=bad)
    assert batcher.pending == 0


def test_integer_like_counts_and_decimal_windows_are_accepted() -> None:
    class Index:
        def __index__(self) -> int:
            return 2

    batches = list(batched_by_time(range(3), Index(), Decimal("1.5")))  # type: ignore[arg-type]

    assert batches == [[0, 1], [2]]


def test_a_flush_error_needs_one_error_per_batch() -> None:
    with pytest.raises(ValueError):
        FlushError([], [])
    with pytest.raises(ValueError):
        FlushError([[1], [2]], [OSError()])
