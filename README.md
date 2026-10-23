# timedbatch

[![Lianes8 Certified](https://lianes8studio.com/certified/lianes8-certified-badge.svg)](https://lianes8studio.com/certified)

Batch items by a max count and a max age, flushing when either limit is hit. Typed, no
dependencies, for plain iterables, for threads and for asyncio.

## Install

```
pip install timedbatch
```

## Which one to use

| You have | Use | Flushes while idle |
|---|---|---|
| An iterable you loop over | `batched_by_time` | No: the age is checked when an item arrives |
| An async iterable | `abatched_by_time` | Yes |
| Many places calling `add(item)` from threads | `TimedBatcher` | Yes, from a background thread |
| Many coroutines calling `await add(item)` | `AsyncTimedBatcher` | Yes, from a background task |

A batch is delivered as soon as it holds `max_items` items or its oldest item is
`max_seconds` old, whichever comes first. `max_seconds` may be `math.inf` to batch by
count only. The [guide](https://github.com/JaviLianes8/timedbatch/blob/main/GUIDE.md)
has the full contract: every rule, every error and how to test code that uses it.

## Iterables

```python
from timedbatch import batched_by_time

now = 0.0


def clock() -> float:  # inject a clock and tests never sleep
    return now


def events():
    global now
    for moment, name in [(0.0, "a"), (0.5, "b"), (0.6, "c"), (9.0, "d"), (9.1, "e")]:
        now = moment
        yield name


for batch in batched_by_time(events(), max_items=2, max_seconds=5.0, clock=clock):
    print(batch)  # ['a', 'b']
    # ['c', 'd']
    # ['e']
```

`['c', 'd']` closes on age: `d` arrives 8.4 seconds after `c`. If the source raises, the
items already read are yielded first and the exception follows.

## Threads: `TimedBatcher`

```python
from timedbatch import TimedBatcher


def write_rows(batch: list[int]) -> None:
    print("insert", batch)


with TimedBatcher(write_rows, max_items=2, max_seconds=1.0) as batcher:
    for row in range(5):
        batcher.add(row)  # safe from any number of threads
# insert [0, 1]
# insert [2, 3]
# insert [4]
```

What it promises:

- **Size and age.** A full batch goes out at once; a partial one goes out `max_seconds`
  after its oldest item, even if nothing else is added.
- **Order and no overlap.** One background thread calls your function, one batch at a
  time, in the order the items were accepted. A batch never exceeds `max_items`.
- **Nothing lost, nothing twice.** Every accepted item is delivered once or handed back
  once. When `add` raises, the item was not accepted (one exception: with
  `background=False`, an interrupt raised inside your function passes through `add`,
  and its batch comes back in the next `FlushError`).
- **Failures.** Give `on_error(exc, batch)` to decide what to do with a failed batch.
  Without it, failed batches are kept and raised as a `FlushError` (with `.batches` and
  `.errors`) by the next `add`, `poll`, `flush` or `close`.
- **Back-pressure.** `max_pending` caps items accepted but not yet delivered. `add` waits
  for room, or raises `BatcherFull` (a `TimeoutError`) after `timeout` seconds.
- **Shutdown.** `close()` (or leaving the `with` block) delivers the rest and stops the
  thread; later calls to `add` raise `BatcherClosed`. A batcher you forget to close, or
  whose `close(timeout=...)` timed out, is closed when the interpreter exits normally,
  through `sys.exit` or an uncaught exception (`flush_at_exit=True`). That exit flush
  waits for your function, so give it its own timeouts. Nothing can flush after
  `os._exit`, a kill signal, or in a child created by `os.fork` (it has no thread).
- **No deadlocks from inside.** Calling `flush`, `poll` or `close` from the flush
  function raises `RuntimeError`, and an `add` there that would wait raises `BatcherFull`.

Failures come back with their items:

```python
from timedbatch import FlushError, TimedBatcher


def flaky(batch: list[str]) -> None:
    raise ConnectionError("database is down")


batcher = TimedBatcher(flaky, max_items=10, max_seconds=60.0)
batcher.add("order-1")
batcher.add("order-2")
try:
    batcher.close()
except FlushError as error:
    print(error.batches)  # [['order-1', 'order-2']]
    print(error)  # 1 batch (2 items) failed to flush: ConnectionError('database is down')
```

### Deterministic tests

`background=False` removes the thread: due batches are delivered by the caller inside
`add` and `poll()`, so an injected clock drives every age decision (and the `timeout`
of `flush` and `close` has nothing to wait for).

```python
from timedbatch import TimedBatcher

now = 0.0
sent: list[list[str]] = []
batcher = TimedBatcher(
    sent.append, max_items=100, max_seconds=5.0, clock=lambda: now, background=False
)
batcher.add("x")
now = 4.9
batcher.poll()
print(sent)  # []
now = 5.0
batcher.poll()
print(sent)  # [['x']]
```

## asyncio

`abatched_by_time` yields a partial batch on time even while the source waits. It keeps
the pending read across those flushes (it uses `asyncio.wait`, which does not cancel on
timeout), so no item is lost. Each read runs in its own task, so context variables the
source sets do not carry from one item to the next. Closing it early drops the partial
batch and the read in flight.

```python
import asyncio
from timedbatch import abatched_by_time


async def ticks():
    yield 1
    yield 2
    await asyncio.sleep(0.3)  # the source goes quiet
    yield 3


async def main() -> None:
    async for batch in abatched_by_time(ticks(), max_items=10, max_seconds=0.1):
        print(batch)  # [1, 2]
        # [3]


asyncio.run(main())
```

`AsyncTimedBatcher` is `TimedBatcher` for one event loop, with an async flush function
(a plain function also works and counts as delivered when it returns; `on_error` may be
sync or async). Close it with `async with` or `await aclose()`:
coroutines cannot run at interpreter exit. If its flush task is cancelled, for example
because the loop ended first, the batcher closes and the next call hands back what it
held. `TimedBatcher` refuses an `async def` flush function instead of dropping batches.

```python
import asyncio
from timedbatch import AsyncTimedBatcher


async def send(batch: list[str]) -> None:
    print("send", batch)


async def main() -> None:
    async with AsyncTimedBatcher(send, max_items=3, max_seconds=1.0, max_pending=100) as out:
        for word in ["a", "b", "c", "d"]:
            await out.add(word)  # waits when 100 items are already pending


asyncio.run(main())
# send ['a', 'b', 'c']
# send ['d']
```

## Errors

| Exception | Raised by | Meaning |
|---|---|---|
| `FlushError` | `add`, `poll`, `flush`, `close`, `aclose` | Earlier batches failed; `.batches` and `.errors` hold them. From `add`, the new item was not accepted. |
| `BatcherClosed` (a `RuntimeError`) | `add` | The batcher is closed, or its async flush task stopped. |
| `BatcherFull` (a `TimeoutError`) | `add` | No room under `max_pending` within `timeout`. |
| `TimeoutError` | `flush`, `close` | The sink did not finish within `timeout`. |
| `RuntimeError` | `flush`, `poll`, `close`, `aclose` | Called from inside the flush function. |
| `TypeError`, `ValueError` | every entry point, `timeout` arguments | A limit, a handler or a timeout is invalid; nothing was created or accepted. |

The library's own messages never include your items; a `FlushError`
quotes at most 200 characters of the first error's `repr`.

## Why

Count-based batching is everywhere: `itertools.batched` (Python 3.12+) and
`more-itertools` `chunked` cut an iterable into groups of `n`, but neither closes a batch
on elapsed time. `reactivex` has `buffer_with_time_or_count`, which bounds both, but its
time windows run on a fixed schedule from the moment you subscribe, so a batch can close
an instant after its first item arrived, and it emits empty lists while the source is
quiet; it also brings the observable model and its schedulers. Code that ships logs, events or
metrics usually wants the latency bound measured from the oldest waiting item, with
nothing sent while idle, and it needs the rest done right: flushing an idle buffer, not
losing items when the sink fails or the program exits, slowing producers down when the
sink falls behind. `timedbatch` does that once, with tests that run hundreds of random
timelines against a reference model, hammer the batchers from many threads and tasks,
and check count-only batching against CPython's own `batched`.

## License

MIT. See [LICENSE](https://github.com/JaviLianes8/timedbatch/blob/main/LICENSE).
Changes are listed in the [changelog](https://github.com/JaviLianes8/timedbatch/blob/main/CHANGELOG.md).

## Links

- Website: https://lianes8studio.com
- GitHub: https://github.com/JaviLianes8
- LinkedIn: https://www.linkedin.com/in/jlianes/
