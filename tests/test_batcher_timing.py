"""TimedBatcher deadlines on the real clock: from the oldest item, and past TIMEOUT_MAX."""

from __future__ import annotations

import threading
import time

from timedbatch import TimedBatcher

WAIT = 10.0


def test_a_window_beyond_the_thread_timeout_limit_keeps_the_thread_alive() -> None:
    out: list[list[int]] = []
    batcher: TimedBatcher[int] = TimedBatcher(
        out.append, max_items=10, max_seconds=threading.TIMEOUT_MAX * 10, flush_at_exit=False
    )
    batcher.add(1)
    time.sleep(0.1)

    assert batcher._thread is not None and batcher._thread.is_alive()
    batcher.flush(timeout=WAIT)
    assert out == [[1]]
    batcher.close(timeout=WAIT)


def test_the_idle_deadline_comes_from_the_oldest_item() -> None:
    arrived = threading.Event()
    batcher: TimedBatcher[int] = TimedBatcher(
        lambda batch: arrived.set(), max_items=10, max_seconds=1.0, flush_at_exit=False
    )
    started = time.monotonic()
    batcher.add(1)
    time.sleep(0.6)
    batcher.add(2)  # must not push the deadline to 1.6 s
    batcher.poll()  # wakes the thread, which recomputes its deadline

    assert arrived.wait(WAIT)
    assert time.monotonic() - started < 1.45
    batcher.close(timeout=WAIT)
