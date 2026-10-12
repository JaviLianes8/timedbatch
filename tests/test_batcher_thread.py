"""TimedBatcher with its flusher thread, on the real clock with generous margins."""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Iterator

import pytest

from timedbatch import BatcherClosed, BatcherFull, FlushError, TimedBatcher

WAIT = 10.0  # upper bound for anything that should happen promptly


@pytest.fixture
def opened() -> Iterator[list[TimedBatcher[int]]]:
    batchers: list[TimedBatcher[int]] = []
    yield batchers
    for batcher in batchers:
        with contextlib.suppress(FlushError):
            batcher.close(timeout=WAIT)


def test_an_idle_partial_batch_is_delivered_after_max_seconds(
    opened: list[TimedBatcher[int]],
) -> None:
    arrived = threading.Event()
    out: list[list[int]] = []

    def sink(batch: list[int]) -> None:
        out.append(batch)
        arrived.set()

    batcher: TimedBatcher[int] = TimedBatcher(
        sink, max_items=100, max_seconds=0.2, flush_at_exit=False
    )
    opened.append(batcher)
    started = time.monotonic()
    batcher.add(1)

    assert arrived.wait(WAIT)
    assert time.monotonic() - started >= 0.2
    assert out == [[1]]


def test_a_full_batch_is_delivered_without_waiting_for_the_window(
    opened: list[TimedBatcher[int]],
) -> None:
    arrived = threading.Event()
    batcher: TimedBatcher[int] = TimedBatcher(
        lambda batch: arrived.set(), max_items=2, max_seconds=3600.0, flush_at_exit=False
    )
    opened.append(batcher)
    batcher.add(1)
    batcher.add(2)

    assert arrived.wait(WAIT)


def test_flush_returns_once_everything_accepted_is_delivered() -> None:
    out: list[list[int]] = []
    batcher: TimedBatcher[int] = TimedBatcher(
        out.append, max_items=100, max_seconds=3600.0, flush_at_exit=False
    )
    for value in range(5):
        batcher.add(value)

    batcher.flush(timeout=WAIT)

    assert out == [[0, 1, 2, 3, 4]]
    assert batcher.pending == 0
    batcher.close()


def test_close_delivers_the_rest_stops_the_thread_and_is_idempotent() -> None:
    out: list[list[int]] = []
    batcher: TimedBatcher[int] = TimedBatcher(
        out.append, max_items=100, max_seconds=3600.0, flush_at_exit=False
    )
    batcher.add(1)

    batcher.close(timeout=WAIT)
    batcher.close(timeout=WAIT)

    assert out == [[1]]
    assert batcher._thread is not None and not batcher._thread.is_alive()
    with pytest.raises(BatcherClosed):
        batcher.add(2)


def test_add_blocks_under_back_pressure_until_the_sink_catches_up() -> None:
    release = threading.Event()
    batcher: TimedBatcher[int] = TimedBatcher(
        lambda batch: release.wait(WAIT),
        max_items=1,
        max_seconds=3600.0,
        max_pending=2,
        flush_at_exit=False,
    )
    batcher.add(1)
    batcher.add(2)
    with pytest.raises(BatcherFull):
        batcher.add(3, timeout=0.1)
    assert batcher.pending == 2

    release.set()
    batcher.add(3, timeout=WAIT)
    batcher.close(timeout=WAIT)
    assert batcher.pending == 0


def test_an_add_waiting_for_room_is_refused_when_the_batcher_closes() -> None:
    release = threading.Event()
    batcher: TimedBatcher[int] = TimedBatcher(
        lambda batch: release.wait(WAIT),
        max_items=1,
        max_seconds=3600.0,
        max_pending=1,
        flush_at_exit=False,
    )
    batcher.add(1)
    errors: list[BaseException] = []

    def producer() -> None:
        try:
            batcher.add(2)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=producer)
    thread.start()
    time.sleep(0.1)
    closer = threading.Thread(target=batcher.close)
    closer.start()
    thread.join(WAIT)
    release.set()
    closer.join(WAIT)

    assert len(errors) == 1 and isinstance(errors[0], BatcherClosed)
    assert batcher.pending == 0


def test_a_background_failure_is_raised_by_the_next_call_without_accepting() -> None:
    failed = threading.Event()

    def sink(batch: list[int]) -> None:
        failed.set()
        raise ValueError("down")

    batcher: TimedBatcher[int] = TimedBatcher(
        sink, max_items=1, max_seconds=3600.0, flush_at_exit=False
    )
    batcher.add(1)
    assert failed.wait(WAIT)
    deadline = time.monotonic() + WAIT
    while batcher.pending and time.monotonic() < deadline:
        time.sleep(0.01)

    with pytest.raises(FlushError) as caught:
        batcher.add(2)

    assert caught.value.batches == [[1]]
    assert batcher.pending == 0
    batcher.close()


def test_flush_times_out_on_a_stuck_sink() -> None:
    release = threading.Event()
    batcher: TimedBatcher[int] = TimedBatcher(
        lambda batch: release.wait(WAIT), max_items=1, max_seconds=3600.0, flush_at_exit=False
    )
    batcher.add(1)

    with pytest.raises(TimeoutError):
        batcher.flush(timeout=0.1)
    with pytest.raises(TimeoutError):
        batcher.close(timeout=0.1)
    release.set()
    batcher.close(timeout=WAIT)


def test_close_unregisters_the_exit_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    registered: list[object] = []
    monkeypatch.setattr("atexit.register", registered.append)
    monkeypatch.setattr("atexit.unregister", registered.remove)

    batcher: TimedBatcher[int] = TimedBatcher(print, max_items=1, max_seconds=1.0)
    assert len(registered) == 1
    batcher.close()

    assert registered == []
