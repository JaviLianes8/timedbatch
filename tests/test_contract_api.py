"""The stable 1.0 surface: signatures, error families, module names and argument checks."""

from __future__ import annotations

import asyncio
import inspect
import pickle
from collections.abc import AsyncIterator, Coroutine
from typing import Any

import pytest

import timedbatch
from timedbatch import (
    AsyncTimedBatcher,
    BatcherClosed,
    BatcherFull,
    FlushError,
    TimedBatcher,
    abatched_by_time,
    batched_by_time,
)

LIMITS = "max_items: 'int', max_seconds: 'float', max_pending: 'int | None' = None"
HANDLERS = (
    "on_error: 'Callable[[Exception, list[T]], object] | None' = None, "
    "clock: 'Callable[[], float]' = <built-in function monotonic>"
)
SIGNATURES = {
    "AsyncTimedBatcher": f"(flush: 'Callable[[list[T]], object]', *, {LIMITS}, {HANDLERS})"
    " -> 'None'",
    "TimedBatcher": f"(flush: 'Callable[[list[T]], object]', *, {LIMITS}, {HANDLERS}, "
    "background: 'bool' = True, flush_at_exit: 'bool' = True) -> 'None'",
    "abatched_by_time": "(aiterable: 'AsyncIterable[_T]', max_items: 'int', max_seconds: "
    "'float', *, clock: 'Callable[[], float]' = <built-in function monotonic>) -> "
    "'AsyncGenerator[list[_T], None]'",
    "batched_by_time": "(iterable: 'Iterable[_T]', max_items: 'int', max_seconds: 'float', *, "
    "clock: 'Callable[[], float]' = <built-in function monotonic>) -> 'Iterator[list[_T]]'",
    "FlushError": "(batches: 'list[list[Any]]', errors: 'list[BaseException]') -> 'None'",
    "TimedBatcher.add": "(self, item: 'T', timeout: 'float | None' = None) -> 'None'",
    "TimedBatcher.poll": "(self) -> 'None'",
    "TimedBatcher.flush": "(self, timeout: 'float | None' = None) -> 'None'",
    "TimedBatcher.close": "(self, timeout: 'float | None' = None) -> 'None'",
    "AsyncTimedBatcher.add": "(self, item: 'T', timeout: 'float | None' = None) -> 'None'",
    "AsyncTimedBatcher.flush": "(self, timeout: 'float | None' = None) -> 'None'",
    "AsyncTimedBatcher.aclose": "(self) -> 'None'",
}


@pytest.mark.parametrize("path", sorted(SIGNATURES))
def test_signatures_are_pinned(path: str) -> None:
    target: object = timedbatch
    for part in path.split("."):
        target = getattr(target, part)
    assert str(inspect.signature(target)) == SIGNATURES[path]  # type: ignore[arg-type]


def test_the_batchers_expose_the_same_read_only_state() -> None:
    for cls in (TimedBatcher, AsyncTimedBatcher):
        for name in ("pending", "closed"):
            attribute = inspect.getattr_static(cls, name)
            assert isinstance(attribute, property) and attribute.fset is None, (cls, name)
    assert inspect.iscoroutinefunction(AsyncTimedBatcher.add)
    assert not hasattr(AsyncTimedBatcher, "poll") and not hasattr(AsyncTimedBatcher, "close")


def test_error_families_are_stable() -> None:
    assert issubclass(FlushError, Exception) and not issubclass(FlushError, RuntimeError)
    assert issubclass(BatcherClosed, RuntimeError)
    assert issubclass(BatcherFull, TimeoutError)


def test_every_public_name_reports_the_package_as_its_module() -> None:
    for name in timedbatch.__all__:
        assert getattr(timedbatch, name).__module__ == "timedbatch", name
    error = FlushError([[1]], [ValueError("x")])
    assert b"timedbatch._" not in pickle.dumps(error)
    assert repr(pickle.loads(pickle.dumps(error))).startswith("FlushError(")


def test_flush_error_quotes_at_most_200_characters_of_the_first_error() -> None:
    secret = "x" * 1_000_000
    error = FlushError([[secret]], [ValueError(secret)])
    message = str(error)
    assert message.startswith("1 batch (1 item) failed to flush: ValueError('xxx")
    assert message.endswith("...")
    assert len(message) == len("1 batch (1 item) failed to flush: ") + 200


def test_flush_error_survives_an_error_whose_repr_raises() -> None:
    class Broken(Exception):
        def __repr__(self) -> str:
            raise RuntimeError("no repr")

    error = FlushError([[1], [2, 3]], [Broken(), ValueError()])
    assert str(error) == "2 batches (3 items) failed to flush: <Broken with an unprintable repr>"
    assert error.batches == [[1], [2, 3]]


def test_a_broken_repr_from_the_sink_still_hands_the_batch_back() -> None:
    class Broken(Exception):
        def __repr__(self) -> str:
            raise RuntimeError("no repr")

    def sink(batch: list[int]) -> None:
        raise Broken

    batcher = TimedBatcher(sink, max_items=2, max_seconds=60.0, background=False)
    batcher.add(1)
    batcher.add(2)
    with pytest.raises(FlushError) as caught:
        batcher.close()
    assert caught.value.batches == [[1, 2]]


@pytest.mark.parametrize("name", ["on_error", "clock"])
def test_handlers_that_are_not_callable_are_refused_at_once(name: str) -> None:
    options: dict[str, object] = {"max_items": 1, "max_seconds": 1.0, name: 5}
    with pytest.raises(TypeError, match=f"^{name} must be callable, not int$"):
        TimedBatcher(print, background=False, flush_at_exit=False, **options)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=f"^{name} must be callable, not int$"):
        AsyncTimedBatcher(print, **options)  # type: ignore[arg-type]
    if name == "clock":
        with pytest.raises(TypeError, match=r"^clock must be callable, not int$"):
            batched_by_time([1], 1, 1.0, clock=5)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match=r"^clock must be callable, not int$"):
            abatched_by_time(_nothing(), 1, 1.0, clock=5)  # type: ignore[arg-type]


async def _nothing() -> AsyncIterator[int]:
    return
    yield


def test_the_async_batcher_counts_a_plain_flush_function_as_delivered() -> None:
    sent: list[list[int]] = []

    def plain(batch: list[int]) -> None:
        sent.append(batch)

    async def main() -> None:
        async with AsyncTimedBatcher(plain, max_items=2, max_seconds=60.0) as batcher:
            for value in range(3):
                await batcher.add(value)
            await batcher.flush()
            assert batcher.pending == 0

    asyncio.run(main())
    assert sent == [[0, 1], [2]]


def test_the_async_batcher_awaits_any_awaitable_a_plain_function_returns() -> None:
    sent: list[list[int]] = []

    async def send(batch: list[int]) -> None:
        sent.append(batch)

    def deferred(batch: list[int]) -> Coroutine[Any, Any, None]:
        return send(batch)

    async def main() -> None:
        async with AsyncTimedBatcher(deferred, max_items=5, max_seconds=60.0) as out:
            await out.add(1)

    asyncio.run(main())
    assert sent == [[1]]
