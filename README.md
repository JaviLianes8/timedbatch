# timedbatch

Batch items from an iterable by a max count and a max time window, flushing when either limit is hit

## Install

```
pip install timedbatch
```

## Usage

`batched_by_time` groups an iterable into lists, emitting a batch as soon as it
holds `max_items` items or `max_seconds` have passed since the batch started,
whichever comes first. The leftover items are flushed when the source ends.

```python
from timedbatch import batched_by_time

events = read_events()  # any iterable, finite or streaming

for batch in batched_by_time(events, max_items=500, max_seconds=5.0):
    flush(batch)  # at most 500 items, at most 5 seconds of latency
```

The clock is injectable, so tests stay deterministic and never really sleep:

```python
now = 0.0

def clock() -> float:
    return now

batches = batched_by_time(source, max_items=100, max_seconds=2.0, clock=clock)
```

The time window is checked only when an item arrives, so a batch is never cut
off mid wait; an idle stream holds its partial batch until the next item.

## Why

Count-based batching is everywhere (`itertools`-style recipes, `more-itertools`
`batched` and `chunked`), but none of them close a batch on elapsed time. Code
that flushes logs, events or metrics almost always wants both bounds: cap the
size so a batch never grows without limit, and cap the latency so a slow stream
still drains on schedule. That second bound gets hand-rolled over and over with
ad hoc timestamp bookkeeping. `timedbatch` does it in one typed, zero-dependency
function with an injectable clock for deterministic tests.

## License

MIT. See [LICENSE](LICENSE).

## Links

- Website: https://lianes8studio.com
- GitHub: https://github.com/JaviLianes8
- LinkedIn: https://www.linkedin.com/in/jlianes/
