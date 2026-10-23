"""The asyncio counterpart of TimedBatcher, for a single event loop."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from types import TracebackType

from ._aengine import AsyncEngine
from ._core import Buffer, T
from ._errors import BatcherFull
from ._limits import check_callables, check_limits, check_timeout


class AsyncTimedBatcher(AsyncEngine[T]):
    """Collect items from coroutines and await ``flush(batch)`` in batches.

    The same promises as :class:`TimedBatcher`: delivery on size, age (also while
    idle), :meth:`flush` and :meth:`aclose`; one batch at a time, in order; every
    accepted item delivered once or handed back once through ``on_error`` or a
    :class:`FlushError`; ``max_pending`` makes :meth:`add` wait. ``flush`` is normally
    an ``async def`` function; when it returns something that is not awaitable (a plain
    function) the batch counts as delivered. ``on_error`` may be sync or async. Use it
    from one event loop only, and close it (``async with``): there is no exit hook for
    coroutines. If the flush task is cancelled (for example because the loop ends), the
    batcher closes and the items it still held are handed back by the next call.

    >>> import asyncio
    >>> from timedbatch import AsyncTimedBatcher
    >>> async def send(batch: list[int]) -> None:
    ...     print("send", batch)
    >>> async def main() -> None:
    ...     async with AsyncTimedBatcher(send, max_items=2, max_seconds=60.0) as out:
    ...         for number in range(3):
    ...             await out.add(number)
    >>> asyncio.run(main())
    send [0, 1]
    send [2]
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
    ) -> None:
        if not callable(flush):
            raise TypeError("flush must be callable")
        check_callables(on_error=on_error, clock=clock)
        items, seconds, pending = check_limits(max_items, max_seconds, max_pending)
        super().__init__(flush, on_error, Buffer(items, seconds, clock), pending)

    @property
    def pending(self) -> int:
        """Items accepted and not yet delivered or handed back."""
        return self._accepted - self._done

    @property
    def closed(self) -> bool:
        """Whether :meth:`aclose` has been called or the flush task has stopped."""
        return self._closed

    async def add(self, item: T, timeout: float | None = None) -> None:
        """Accept ``item``, waiting up to ``timeout`` seconds for room under ``max_pending``.

        Every exception it raises means the item was not accepted: :class:`BatcherClosed`,
        :class:`BatcherFull`, or :class:`FlushError` for earlier failures not reported yet.
        """
        timeout = check_timeout(timeout)
        self._check_open()
        self._start()
        limit = self._max_pending
        if limit is not None and self.pending >= limit:
            if self._inside():
                raise BatcherFull("add() inside the flush callback cannot wait for room")
            await self._wait(lambda: self.pending < limit, timeout, BatcherFull)
            self._check_open()
        self._buffer.append(item)
        self._accepted += 1
        self._wake.set()

    async def flush(self, timeout: float | None = None) -> None:
        """Deliver every item accepted so far; raise ``TimeoutError`` if it takes longer."""
        timeout = check_timeout(timeout)
        self._refuse_inside("flush")
        if self._task is not None:
            target = self._wanted = max(self._wanted, self._accepted)
            self._wake.set()
            await self._wait(lambda: self._done >= target, timeout, TimeoutError)
        self._ledger.raise_any()

    async def aclose(self) -> None:
        """Stop accepting items, deliver the rest and stop the task. Idempotent."""
        self._refuse_inside("aclose")
        self._closed = True
        if self._task is not None and not self._task.done():
            self._wake.set()
            # wait() neither cancels the task if this caller is cancelled nor re-raises
            # the task's own cancellation into a caller that was not cancelled.
            await asyncio.wait({self._task})
        self._reap()
        self._ledger.raise_any()

    async def __aenter__(self) -> AsyncTimedBatcher[T]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()
