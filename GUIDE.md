# timedbatch guide

The full contract of the seven public names. Everything here is covered by the test
suite; the examples below run as tests and print what their comments say.

## The rule

A batch is delivered as soon as it holds `max_items` items or its oldest item is
`max_seconds` old, whichever comes first. Age is measured from the oldest item still
waiting, never from a fixed schedule: nothing is sent while there is nothing to send, and
a batch never leaves sooner than `max_seconds` after its first item unless it is full or
you flush or close. Batches never hold more than `max_items` items, and items keep the order in
which they were accepted.

| Entry point | Shape | Age checked |
|---|---|---|
| `batched_by_time(iterable, max_items, max_seconds)` | iterator of lists | when an item arrives |
| `abatched_by_time(aiterable, max_items, max_seconds)` | async iterator of lists | also while the source is idle |
| `TimedBatcher(flush, max_items=, max_seconds=)` | push, any thread | by a background thread, or by `add` and `poll` |
| `AsyncTimedBatcher(flush, max_items=, max_seconds=)` | push, one event loop | by a background task |

## Limits

- `max_items` (and `max_pending`) take an `int` of at least 1, or any integer type with
  `__index__` (NumPy integers). `bool`, floats and strings raise `TypeError`; zero and
  negative counts raise `ValueError`.
- `max_seconds` takes any real number (`int`, `float`, `Fraction`, `Decimal`) greater
  than 0. `math.inf`, or a number too large for a float, means count only. NaN raises
  `ValueError`.
- `max_pending`, when given, must be at least `max_items`, so a full batch always fits.
- A `timeout` argument is `None` (wait as long as needed), a real number of seconds
  (zero or negative means "do not wait"), or a number too large for a float (no limit).
- `flush`, `on_error` and `clock` must be callable. All of this is checked when you
  create the batcher or call the function, before anything runs.

## Iterables: `batched_by_time`

The age is checked when an item arrives, because a plain iterator cannot be interrupted
while it waits for the next item. An idle source therefore holds its partial batch until
the next item or its end. Items are read one at a time, never ahead of the consumer. If
the source raises, the items already read are yielded first and the exception follows on
the next step.

## Async iterables: `abatched_by_time`

A partial batch is yielded `max_seconds` after its first item even while the source is
waiting. The read in flight is kept across those flushes (it is never cancelled by a
flush), so no item is lost. Closing the iterator early (`aclose()` or
`contextlib.aclosing`) cancels that read: an item it had already taken from the source is
dropped, together with the partial batch. Each read runs in its own task, so context
variables set by the source do not carry from one item to the next.

## Threads: `TimedBatcher`

`add(item)` may be called from any number of threads. One background thread hands
batches to `flush(batch)`, one at a time and in order. Each call gets its own list.

- `flush(timeout=None)` returns once every item accepted before the call is delivered or
  handed back, or raises `TimeoutError`. Without a thread the caller delivers, so the
  timeout has nothing to wait for.
- `close(timeout=None)` stops accepting items, delivers the rest and stops the thread.
  It is idempotent, and the `with` statement calls it. If it times out (`TimeoutError`) or
  is interrupted, the exit flush below stays in place.
- `poll()` reports failures not yet reported; without a thread it also delivers what is
  due.
- `pending` counts items accepted and not yet delivered or handed back; `closed` says
  whether `close` was called.
- `max_pending` caps `pending`: `add` waits for room, up to `timeout`, then raises
  `BatcherFull`. Waiting uses the real clock even when you inject one.
- Inside the flush function, `add` is allowed (the item joins a later batch), but an
  `add` that would have to wait raises `BatcherFull`, and `flush`, `poll` and `close`
  raise `RuntimeError` instead of deadlocking.
- `flush_at_exit=True` (the default) closes a forgotten batcher when the interpreter exits
  normally, through `sys.exit` or an uncaught exception. That flush waits for your
  function, so give it its own timeouts. Nothing can run after `os._exit`, a kill signal
  or a fatal error, and a child created by `os.fork` has no flush thread.
- `background=False` removes the thread: `add` delivers a full batch at once and `poll`
  delivers what is due, in the calling thread. With an injected `clock` every age
  decision is then deterministic (see Testing).
- An `async def` flush function is refused with `TypeError`; a plain function that
  returns an awaitable fails that batch with `TypeError` instead of dropping it.

## asyncio: `AsyncTimedBatcher`

The same promises for one event loop: `await add(item, timeout=None)`,
`await flush(timeout=None)`, `await aclose()` and `async with`. The flush function is
normally `async def`; a plain function also works, and its batch counts as delivered when
it returns (it runs on the event loop thread, so keep it short). `on_error` may be sync or
async. There is no exit hook for coroutines: close it yourself. If the flush task is
cancelled or stops (for example because the loop ended first), the batcher closes and the
next call hands back everything it held in a `FlushError`, then raises `BatcherClosed`.

## Failures

Every accepted item is delivered once or handed back once, never both and never twice.

- With `on_error(exc, batch)`, a batch whose flush raised an `Exception` is passed to it,
  with its own copy of the list. If `on_error` raises, the batch comes back in a
  `FlushError` with that error.
- Without `on_error`, failed batches are kept and raised as one `FlushError` by the next
  `add`, `poll`, `flush`, `close` or `aclose`. Its `batches` and `errors` line up, in
  delivery order; the first error is also its `__cause__`. A `FlushError` raised by `add`
  means the new item was not accepted. It pickles.
- An interrupt (`KeyboardInterrupt`, `SystemExit`) inside the flush function is never
  passed to `on_error`. With a background thread its batch comes back in the next
  `FlushError`; with `background=False` it passes through the call that was delivering,
  and its batch comes back in the next `FlushError`. In `AsyncTimedBatcher` it stops the
  task (asyncio also re-raises it from the event loop), which closes the batcher as above.

```python
from timedbatch import FlushError, TimedBatcher

attempts: list[list[int]] = []


def store(batch: list[int]) -> None:
    attempts.append(batch)
    if len(attempts) == 1:
        raise ConnectionError("database is down")


batcher = TimedBatcher(store, max_items=2, max_seconds=60.0, background=False)
batcher.add(1)
batcher.add(2)  # this batch fails; the error is kept for the next call
try:
    batcher.add(3)
except FlushError as error:
    print(error.batches, error.errors)  # [[1, 2]] [ConnectionError('database is down')]
    for batch in error.batches:  # retry the failed items
        for item in batch:
            batcher.add(item)
    batcher.add(3)  # 3 was not accepted by the add that raised
batcher.close()
print(attempts)  # [[1, 2], [1, 2], [3]]
```

## Errors

| Exception | Raised by | Meaning |
|---|---|---|
| `FlushError` | `add`, `poll`, `flush`, `close`, `aclose` | Earlier batches failed; from `add`, the new item was not accepted. |
| `BatcherClosed` (a `RuntimeError`) | `add` | Closed, or the async flush task stopped (then it is chained to the reason). |
| `BatcherFull` (a `TimeoutError`) | `add` | No room under `max_pending` within `timeout`, or no room inside the flush function. |
| `TimeoutError` | `flush`, `close` | The work did not finish within `timeout`. |
| `RuntimeError` | `flush`, `poll`, `close`, `aclose` | Called from inside the flush function. |
| `TypeError`, `ValueError` | every entry point, `timeout` arguments | Invalid limit, handler or timeout. |

The library's own messages never include your items. A `FlushError` quotes at most 200
characters of the first error's `repr`, and an error whose `repr` raises is named by its
type instead.

## Testing with an injected clock

`clock` is any function returning seconds from a monotonic source. With
`background=False`, a fake clock drives every age decision of `TimedBatcher`, so tests
never sleep:

```python
from timedbatch import TimedBatcher

now = 0.0
sent: list[list[str]] = []
batcher = TimedBatcher(
    sent.append, max_items=10, max_seconds=5.0, clock=lambda: now, background=False
)
batcher.add("first")
now = 3.0
batcher.add("second")
now = 5.0
batcher.poll()  # "first" is 5 seconds old: the whole batch goes
print(sent)  # [['first', 'second']]
batcher.close()
```

`batched_by_time` reads the clock when items arrive, so a source that moves a fake clock
before each item makes it deterministic too (see its docstring). The asyncio entry
points use the clock to measure age but wait on the event loop's own timer.

## Stability

From 1.0.0 the seven names in `timedbatch.__all__`, their signatures, parameter names,
defaults, exception types and the rules above follow Semantic Versioning. Modules whose
names start with an underscore are private and may change in any release.
