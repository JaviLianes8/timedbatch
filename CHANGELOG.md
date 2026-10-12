# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0]

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
  `bool` and non-integer counts and a NaN window, and accepts `math.inf` for count-only
  batching.
- When the source raises, `batched_by_time` yields the items already read before the
  exception.

## [0.1.0]

- First release.
- `batched_by_time`: batch an iterable by a max count and a max time window,
  flushing when either limit is reached and at the end, with an injectable clock.
