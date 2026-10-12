# timedbatch improvement round

Progress:
- [x] Brainstorm ranked improvements (this file)
- [ ] Shared limits validation and error types (`FlushError`, `BatcherClosed`, `BatcherFull`)
- [ ] `batched_by_time`: strict limits, partial batch kept when the source raises
- [ ] `TimedBatcher`: push style, background age flush, back-pressure, close and exit flush
- [ ] `abatched_by_time` and `AsyncTimedBatcher` for asyncio
- [ ] Contracts: reference model, stress invariants, exit flush in a child process, typing, README examples, installed wheel e2e
- [ ] Docs: README, CHANGELOG, docstrings, packaging metadata
- [ ] Gates: ruff format, ruff check, mypy --strict, pytest
- [ ] Independent review findings fixed with regression tests
- [ ] Pull request opened, guard clean

## Starting point

`0.1.0` ships one function, `batched_by_time(iterable, max_items, max_seconds, clock=)`.
It is correct for what it does, but reading it as a user who wants to flush logs, events
or metrics turns up these gaps:

- Pull only. The age limit is checked when an item arrives, so an idle stream holds its
  partial batch forever. Most callers have the opposite shape: many places call
  `add(item)` and a sink should receive batches on time even when traffic stops.
- No story for concurrency, back-pressure, a slow or failing sink, or shutdown. Each of
  those is where hand-rolled batchers lose or duplicate items.
- If the source raises, the items already read into the partial batch are lost.
- `max_seconds=float("nan")` passes validation and silently disables the age limit;
  `max_items=True` and `max_items=2.5` are accepted.
- Nothing for asyncio, where an idle-flushing batcher is easy to get subtly wrong
  (`asyncio.wait_for` cancels the awaited `__anext__`).
- Packaging: no Python version classifiers, no wheel test, README examples never run.

## Sources (fetched 2026-10-12, docs.python.org/3 served the 3.15 pages)

- `atexit`, https://docs.python.org/3/library/atexit.html: handlers run upon normal
  termination (including `sys.exit()` and the main module completing) in reverse order of
  registration; they do not run when the program is killed by an unhandled signal, on a
  fatal internal error or on `os._exit()`; an exception in a handler is printed and the
  remaining handlers still run; since 3.12 starting a thread in a handler raises
  `RuntimeError`; `unregister` compares with `==`.
- `threading`, https://docs.python.org/3/library/threading.html: the program exits when
  only daemon threads are left; daemon threads are abruptly stopped at shutdown;
  `Condition.wait(timeout)` releases the lock, returns on notify or timeout and
  re-acquires the lock; an exception escaping `run()` goes to `threading.excepthook`.
- `asyncio-task`, https://docs.python.org/3/library/asyncio-task.html: `wait_for`
  cancels the awaitable on timeout; `wait` does not cancel on timeout and does not cancel
  its futures when it is itself cancelled; 3.11 made the timeout `TimeoutError` (on 3.10
  it is the distinct `asyncio.TimeoutError`).
- `asyncio-sync`, https://docs.python.org/3/library/asyncio-sync.html: asyncio
  primitives are not thread-safe.
- `time`, https://docs.python.org/3/library/time.html: `monotonic` cannot go backwards,
  only differences between calls are meaningful.

Consequences for the design: the flusher thread is a daemon (a forgotten batcher must not
hang the program), so the exit flush is an `atexit` handler that stops and joins it, never
one that starts a thread. The async iterator waits with `asyncio.wait` and keeps the
pending `__anext__` across timeouts, and cancels it itself on close. The exit flush is
proven by running real child interpreters, not assumed from the text.

## Ranked improvements

1. **`TimedBatcher`, push style with a background flusher.** `add(item)` from any number
   of threads; one flusher thread hands batches to `flush(batch)` serially and in order,
   when a batch reaches `max_items`, when its oldest item is `max_seconds` old (even if
   no more items arrive), on `flush()`, and on `close()`. Batches never exceed
   `max_items`; per-item timestamps keep the age promise when a slow sink lets the buffer
   grow past one batch.
2. **No lost and no duplicated items.** Every accepted item is delivered exactly once or
   handed back once, through `on_error(exc, batch)` or a `FlushError` carrying the failed
   batches. `add` that raises `BatcherFull` or `BatcherClosed` did not accept the item.
3. **Exceptions in the sink.** With `on_error` the handler decides; without it, failures
   are kept and raised as `FlushError` from the next `add`, `poll`, `flush` or `close`
   (before accepting a new item), so a background failure is never silent.
4. **Back-pressure.** `max_pending` bounds items accepted but not yet delivered; `add`
   blocks, or raises `BatcherFull` (a `TimeoutError`) after `timeout`. A call from inside
   the sink that would wait on itself raises instead of deadlocking.
5. **Flush on close and exit.** `close()` is idempotent, delivers the rest and joins the
   thread; context manager; `flush_at_exit=True` registers an `atexit` handler so a
   forgotten batcher still drains on normal exit, `sys.exit` and an uncaught exception.
6. **Injectable clock, deterministic tests.** `background=False` runs without a thread:
   due batches are delivered in the caller on `add` and `poll()`, so a fake clock drives
   the whole age logic with no sleeping.
7. **asyncio.** `abatched_by_time(aiterable, ...)` flushes on age while the source is idle;
   `AsyncTimedBatcher` mirrors `TimedBatcher` with an async sink, `await add()`,
   `await flush()`, `await aclose()`, `async with`.
8. **Stricter `batched_by_time`.** Reject `bool`, non-integer counts, NaN windows; allow
   `math.inf` as "count only"; when the source raises, yield the partial batch first and
   then raise.
9. **Contracts that bite.** A reference model compared on thousands of seeded random
   timelines; stress tests with many producers, a slow and failing sink and a tight
   `max_pending` checking exactly-once, order per producer, batch size and the pending
   bound; child interpreters for the exit flush; mypy on a snippet; README examples run in
   the suite; installed wheel e2e.
10. **Docs and packaging.** README with runnable examples and the guarantees spelled out,
    CHANGELOG, classifiers, sdist without this file.

Left out on purpose: retries and dead letter queues (policy belongs to the caller, who gets
the failed batch back), multiprocessing, and a size limit in bytes.
