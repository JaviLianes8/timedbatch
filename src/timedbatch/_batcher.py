"""A thread-safe push batcher that flushes on size, age, demand, close and exit."""

from __future__ import annotations

import atexit
import threading
import time
from collections.abc import Callable
from types import TracebackType

from ._core import Buffer, T, is_async
from ._engine import Engine
from ._errors import BatcherClosed
from ._limits import check_callables, check_limits, check_timeout


class TimedBatcher(Engine[T]):
    """Collect items from any thread and hand them to ``flush`` in batches.

    A batch is delivered when it holds ``max_items`` items, when its oldest item is
    ``max_seconds`` old, on :meth:`flush` and on :meth:`close`. Batches go to ``flush``
    one at a time, in the order the items were accepted, and never exceed ``max_items``.
    Every accepted item is delivered once or handed back once: to ``on_error(exc, batch)``
    or inside a :class:`FlushError` raised by the next ``add``, ``poll``, ``flush`` or
    ``close``. ``max_pending`` bounds items accepted but not yet delivered; ``add`` blocks
    for room. With ``background=False`` there is no thread: due batches are delivered by
    the caller on ``add`` and ``poll``, which makes an injected ``clock`` fully
    deterministic. ``flush_at_exit`` closes a forgotten batcher when the interpreter exits.

    >>> from timedbatch import TimedBatcher
    >>> now = 0.0
    >>> sent: list[list[str]] = []
    >>> batcher = TimedBatcher(
    ...     sent.append, max_items=2, max_seconds=5.0, clock=lambda: now, background=False
    ... )
    >>> for word in ["a", "b", "c"]:
    ...     batcher.add(word)
    >>> sent, batcher.pending
    ([['a', 'b']], 1)
    >>> now = 5.0
    >>> batcher.poll()
    >>> sent
    [['a', 'b'], ['c']]
    >>> batcher.close()
    """

    def __init__(
        self,
        flush: Callable[[list[T]], object],
        *,
        max_items: int,
        max_seconds: float,
        max_pending: int | None = None,
        on_error: Callable[[Exception, list[T]], object] | None = None,
        clock: Callable[[], float] = time.monotonic,
        background: bool = True,
        flush_at_exit: bool = True,
    ) -> None:
        if not callable(flush):
            raise TypeError("flush must be callable")
        check_callables(on_error=on_error, clock=clock)
        if is_async(flush) or is_async(on_error):
            raise TypeError("flush and on_error must be plain functions; see AsyncTimedBatcher")
        items, seconds, pending = check_limits(max_items, max_seconds, max_pending)
        super().__init__(flush, on_error, Buffer(items, seconds, clock), pending)
        self._at_exit = flush_at_exit
        self._thread: threading.Thread | None = None
        if background:
            self._thread = threading.Thread(target=self._run, name="timedbatch", daemon=True)
            self._thread.start()
        if flush_at_exit:
            atexit.register(self._close_at_exit)

    @property
    def pending(self) -> int:
        """Items accepted and not yet delivered or handed back."""
        with self._lock:
            return self._accepted - self._done

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` has been called."""
        return self._closed

    def add(self, item: T, timeout: float | None = None) -> None:
        """Accept ``item``, waiting up to ``timeout`` seconds for room under ``max_pending``.

        Every exception it raises means the item was not accepted: :class:`BatcherClosed`,
        :class:`BatcherFull`, or :class:`FlushError` for earlier failures not reported yet.
        """
        timeout = check_timeout(timeout)
        with self._lock:
            self._ledger.raise_any()
            if self._closed:
                raise BatcherClosed("the batcher is closed")
            inside = self._delivering == threading.get_ident()
            self._wait_for_room(timeout, inside)
            self._buffer.append(item)
            self._accepted += 1
            if self._thread is not None:
                if len(self._buffer) in (1, self._buffer.max_items):
                    self._wake.notify()
                return
        if not inside:
            # A batch failing here is reported by the next call, so that a FlushError from
            # add() always means the item was not accepted.
            self._drain()

    def poll(self) -> None:
        """Deliver every batch that is due now and report failures not reported yet."""
        self._refuse_inside("poll")
        if self._thread is None:
            self._drain()
        else:
            with self._lock:
                self._wake.notify()
        self._raise_failures()

    def flush(self, timeout: float | None = None) -> None:
        """Deliver every item accepted so far; raise ``TimeoutError`` if it takes longer.

        Without a background thread the caller delivers, so ``timeout`` has no effect.
        """
        timeout = check_timeout(timeout)
        self._refuse_inside("flush")
        with self._lock:
            target = self._wanted = max(self._wanted, self._accepted)
            self._wake.notify()
        if self._thread is None:
            self._drain()
        else:
            self._wait_until_done(target, timeout)
        self._raise_failures()

    def close(self, timeout: float | None = None) -> None:
        """Stop accepting items, deliver the rest and stop the thread. Idempotent.

        If this times out or is interrupted, the exit hook stays registered, so what is
        left is still delivered when the interpreter exits.
        """
        self._shutdown(check_timeout(timeout), unregister=True)

    def __enter__(self) -> TimedBatcher[T]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _close_at_exit(self) -> None:
        # The atexit docs leave unregistering from inside a handler undefined.
        self._shutdown(None, unregister=False)

    def _shutdown(self, timeout: float | None, unregister: bool) -> None:
        self._refuse_inside("close")
        with self._lock:
            self._closed = True
            self._wake.notify()
            self._progress.notify_all()
        if self._thread is None:
            self._drain()
        else:
            # join() raises OverflowError past the platform limit (about 49 days on
            # Windows); a timeout that long means no limit at all.
            forever = timeout is None or timeout >= threading.TIMEOUT_MAX
            self._thread.join(None if forever else timeout)
            if self._thread.is_alive():
                raise TimeoutError("the flush thread is still delivering")
        # Only now: an interrupted or timed out close must leave the exit flush in place.
        if unregister and self._at_exit:
            atexit.unregister(self._close_at_exit)
        self._raise_failures()
