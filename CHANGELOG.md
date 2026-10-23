# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-10-23

The first release on PyPI (0.1.0 and 0.2.0 were never published there). The
public API is now stable: the seven names in `timedbatch.__all__`, their signatures,
parameter names, defaults and the exception types they raise follow Semantic Versioning
from here on.

### Changed

- `AsyncTimedBatcher` awaits what `flush` returns only when it is awaitable. A plain
  function that returns `None` used to deliver the batch and then report it as failed
  (`TypeError` from `await`), so the same items were both delivered and handed back; such
  a batch now counts as delivered. The `flush` parameter is typed
  `Callable[[list[T]], object]`, like `on_error`.
- Every entry point raises `TypeError` at once for an `on_error` or `clock` that is not
  callable, instead of failing on the first batch.
- The `FlushError` message quotes at most 200 characters of the first error's `repr`, and
  a `repr` that raises no longer turns the report of failed batches into another error
  (the batches were lost in that case).
- The public names report `timedbatch` as their module, so their `repr` and pickles no
  longer depend on private module names.

### Fixed

- Draining many waiting batches took quadratic time: each batch was cut from the front
  of a list, so the time to catch up grew with the square of the backlog. It is now
  linear.

### Added

- A runnable example in the docstring of every public name, run by the test suite.
- A guide (`GUIDE.md`) with the full contract: delivery rules, errors, threads and
  asyncio, shutdown and testing with an injected clock.

## [0.2.0] - 2026-10-12

### Added

- `TimedBatcher`: a thread-safe push batcher. A background thread delivers batches on
  size, on age (also while idle), on `flush()` and on `close()`, one at a time and in
  order. `on_error` or `FlushError` hands every failed batch back; `max_pending` adds
  back-pressure (`BatcherFull` after `timeout`); a forgotten batcher is closed at
  interpreter exit; `background=False` runs without a thread for deterministic tests.
- `AsyncTimedBatcher`: the same for one asyncio event loop, with an async flush function.
- `abatched_by_time`: batch an async iterable, yielding partial batches on time while the
  source is idle without losing the read in flight.
- `FlushError`, `BatcherClosed` and `BatcherFull`.

### Changed

- `batched_by_time` checks its limits when called instead of on the first step, rejects
  `bool`, floats and other non-integer counts and a NaN window, and accepts `math.inf`
  for count-only batching. Integer types such as NumPy's (anything with `__index__`)
  and `Decimal` windows keep working.
- When the source raises, `batched_by_time` yields the items already read before the
  exception.

## [0.1.0] - 2026-10-12

- First release.
- `batched_by_time`: batch an iterable by a max count and a max time window,
  flushing when either limit is reached and at the end, with an injectable clock.
