# timedbatch 1.0.0 certification report

The 1.0.0 certification bar (19 items) applied to timedbatch. Nothing was ever published to
PyPI (`https://pypi.org/pypi/timedbatch/json` answered 404 on 2026-10-23), so 1.0.0 is the
first release there and there is no upgrade path to test; every change against `main`
at 1a0c415 (version 0.2.0) is still listed in the CHANGELOG.

Progress:
- [x] Branch `release/1.0.0` created from `main` (1a0c415, 0.2.0)
- [x] Baseline gates on 0.2.0
- [x] API review and 1.0.0 changes (item 1)
- [x] `__all__`, docstrings with examples, typing, errors (items 2 to 5)
- [x] Contracts, ground truth, fuzz (items 6, 7, 9)
- [x] Docs: README, GUIDE, CHANGELOG, pyproject (items 11 to 13)
- [x] Wheel and sdist contents, real e2e, every Python on this PC (items 8, 10, 14)
- [x] Author mutation round (item 18)
- [x] Gates (item 15)
- [ ] Five QA lenses (item 19)
- [ ] Independent reviewer (item 16)
- [ ] PR merged and published by the maintainer (item 17); the PR and trace guard are done

## Scorecard

| # | Item | Result |
| --- | --- | --- |
| 1 | Public API reviewed for stability | PASS: the seven names, signatures, parameter names and defaults of 0.2.0 kept and pinned by `tests/test_contract_api.py`; behaviour changes in the CHANGELOG |
| 2 | `__all__` exact, no private imports in docs | PASS: every public name reports `timedbatch` as its module |
| 3 | Docstrings with runnable examples | PASS: all seven, run by `tests/test_doctests.py`; README (6) and GUIDE (2) examples run by `tests/test_docs.py` |
| 4 | Typed end to end | PASS: `mypy --strict src tests`, `py.typed`, consumer snippet checked by mypy; `FlushError.batches` is `list[list[Any]]`, documented |
| 5 | Typed, documented, bounded errors | PASS: errors table in README and GUIDE; messages never include items; `FlushError` quotes at most 200 characters of the first error and survives a raising `repr` |
| 6 | Contract tests of every public name | PASS |
| 7 | Real ground truth | PASS: CPython v3.14.0 sources fetched (hash manifest), the C `itertools.batched`, more-itertools 11.2.1, reactivex 6.0.0 |
| 8 | Real e2e from the wheel | PASS: wheel installed in a fresh venv and driven from an empty folder; no CLI; no PyPI release to upgrade from |
| 9 | Seeded fuzz, no crash, hang or blowup | PASS |
| 10 | Supported Pythons | PASS: CPython 3.10.19, 3.11.5, 3.12.12, 3.13.2, 3.14.2 and 3.14.2t |
| 11 | README and `twine check` | PASS: `twine check --strict` on wheel and sdist, absolute links (tested), badge, errors, honest comparison |
| 12 | Dated CHANGELOG section | PASS: 1.0.0, 2026-10-22 |
| 13 | pyproject 1.0.0, Production/Stable | PASS |
| 14 | sdist and wheel contents | PASS |
| 15 | Gates each exit 0 | PASS: see Gates |
| 16 | Independent reviewer | pending |
| 17 | PR and trace guard | PR from `release/1.0.0`, trace guard CLEAN; merge and publish left to the maintainer |
| 18 | Contracts bite (mutations) | PASS: 40 author mutants, 39 killed at once, 1 survivor killed by a new contract |
| 19 | Five independent QA lenses | pending |

## Baseline on 0.2.0

`ruff format --check .`, `ruff check .` and `mypy --strict src tests` exit 0 at 1a0c415
(this PC, CPython 3.13.2).
Defects found in 0.2.0 by reading and then reproducing:

- Draining a backlog was quadratic: each batch was cut from the front of a list
  (20,000 one-item batches 0.053 s, 80,000 batches 0.824 s; now 0.008 s and 0.043 s).
- `AsyncTimedBatcher` with a plain flush function delivered the batch, then failed the
  `await` and handed the same batch back: delivered and handed back.
- `close(timeout=math.inf)` (or any timeout past the platform limit) raised
  `OverflowError` from `Thread.join` after the batcher had been closed.
- `max_seconds=10**400` or `Fraction(10**400)` raised `OverflowError` instead of meaning
  "no age limit" (a `Decimal("1e400")` already worked); `Decimal("sNaN")` gave a
  different message from the other NaNs.
- A `FlushError` message quoted the whole `repr` of the first error, and a `repr` that
  raised made the ledger lose the failed batches.
- `on_error=5` or `clock=5` were accepted and failed only on the first batch.
- No docstring had a runnable example, the README linked LICENSE relatively, there was no
  GUIDE, and `scripts/check.ps1` did not stop on a failing native command.

## API review (item 1)

Kept: `AsyncTimedBatcher`, `BatcherClosed`, `BatcherFull`, `FlushError`, `TimedBatcher`,
`abatched_by_time`, `batched_by_time`, every signature and default of 0.2.0, the error
families (`BatcherClosed` is a `RuntimeError`, `BatcherFull` a `TimeoutError`, `FlushError`
an `Exception`). Changed, all in the CHANGELOG: the async flush function is typed
`Callable[[list[T]], object]` and a non-awaitable result counts as delivered;
`on_error` and `clock` are checked when called; bounded `FlushError` messages; windows and
timeouts beyond the float range mean "no limit"; public names report `timedbatch` as
their module. Internal: `AsyncTimedBatcher` split into a private engine
(`_aengine.py`) to keep every file under 200 lines with its examples.

## Ground truth (item 7)

No standard or reference implementation defines batching by count and age together, so
the age rules are pinned by the hand-written reference models of `tests/test_model.py`
(labelled as written for the tests) and by real interpreter behaviour. Where an external
truth exists it is used:

- CPython v3.14.0 sources, fetched on 2026-10-23 by `scripts/fetch_upstream.py` from
  `https://raw.githubusercontent.com/python/cpython/v3.14.0/` into the ignored
  `tests/upstream` folder; `tests/upstream/MANIFEST.json` records URL, tag, size, sha256
  and licence, and the tests check the hash before reading. Nothing fetched is committed
  or shipped.
  - `Doc/library/itertools.rst`: the `batched` recipe is cut from the text and run;
    count-only batching (`max_seconds=math.inf`) gives the same batches and the same
    error families on the inputs of CPython's own recipe test.
  - `Lib/test/test_itertools.py`: the three literal `test_batched` vectors are read with
    `ast` (never imported) and met.
  - `Doc/library/asyncio-task.rst`, `atexit.rst`, `threading.rst`: the sentences the
    design relies on (`wait()` does not cancel on timeout, no exit handlers after
    `os._exit`, `OverflowError` past `TIMEOUT_MAX`) are located in the text, and the
    first is checked against the running interpreter.
- The C `itertools.batched` (3.12+) run live on every input above plus `range(100)`;
  timedbatch refuses `True` as a count on purpose (asserted).
- more-itertools 11.2.1 `chunked`: 2,000 seeded inputs, identical.
- reactivex 6.0.0 `buffer_with_time_or_count` on a virtual-time scheduler: its windows
  close on a fixed schedule from subscription and emit empty lists, while timedbatch
  ages each batch from its first item and sends nothing while idle. This is the
  comparison the README makes, asserted by `tests/test_oracles.py`.
- Real program output: child interpreters prove the exit flush (`tests/test_exit.py`).

## Fuzz and hostile input (item 9)

`tests/test_fuzz.py`: 200 seeds each for `TimedBatcher` without a thread (random add,
poll, flush and clock steps, failing sinks, failing `on_error`, `max_pending`) and for
`AsyncTimedBatcher` (async and plain sinks, zero timeouts, failures): after every step
`pending` matches, batches leave in order without overlap, every item is settled at most
once, and after close exactly once (6,491 items, 1,626 handed back, 3,585 sink calls in
one run). `tests/test_limits_fuzz.py`: 40 seeds of 200 constructions with hostile limit
values against an independent acceptance rule; refusals are exactly `TypeError` or
`ValueError`, name the parameter and stay under 80 characters. `tests/test_hostile.py`:
windows and timeouts beyond the float range, 1e12-second timeouts, linear draining
(4x backlog under 9x time), a 10,000-item failed batch with a short message.
`tests/test_stress.py` (many threads and tasks) is unchanged.

## Author mutation round (item 18)

40 mutants, each applied alone to a copy of the pushed branch at 49e6005 on another
machine (the laptop, CPython 3.14.6), full suite with `-x`, fetched sources and both
oracle packages present, 60 s per test. Mutants whose run ended in a hang or showed no
failing test were run again on Elena's PC (CPython 3.14.2, 120 s per test) with the full
output kept.

| Mutant | Change | Caught by |
| --- | --- | --- |
| M1 | `_core` full check `<` to `<=` | test_abatcher_edges: sink cancellation hands everything back |
| M2 | `_core` age check `<` to `<=` | test_batcher_sync: old batch delivered by poll |
| M3 | `_core` take newest items first | test_abatcher: size flush and close |
| M4 | `_core` deadline from the newest stamp | test_batcher_timing: idle deadline from the oldest item |
| M5 | sink gets the shared list | test_batcher_failures: sink gets its own list |
| M6 | failing `on_error` swallowed | test_batcher_failures: failing on_error reported |
| M7 | ledger not cleared after raising | test_abatcher: failed batch comes back once |
| M8 | awaitable from a plain sink accepted | test_plain_sinks: coroutine fails the batch |
| M9 | room check `<` to `<=` | test_batcher_sync: add inside the sink with no room (hang, per-test timeout) |
| M10 | done counts batches, not items | test_batcher_failures: failed batch in a FlushError |
| M11 | force `>` to `>=` (always flush) | test_batcher_failures: sink gets its own list |
| M12 | interrupt not re-raised by drain | test_batcher_failures: interrupt propagates |
| M13 | no wake-up on a full batch | test_batcher_thread: full batch without waiting |
| M14 | closed batcher accepts items | test_batcher_sync: close refuses new items |
| M15 | exit hook never registered | test_exit: buffered items flushed at exit |
| M16 | huge close timeout passed to `join` | test_hostile: huge timeouts |
| M17 | count `< 1` to `< 0` | test_batching: non-positive count refused |
| M18 | `max_pending` may be below `max_items` | test_limits: max_pending holds one batch |
| M19 | huge negative window becomes infinite | test_hostile: window below the float range refused |
| M20 | no Decimal NaN check | test_hostile: every NaN refused with one message |
| M21 | callable check inverted | test_abatcher: size flush and close |
| M22 | quote limit 200 to 2000 | test_contract_api: at most 200 characters |
| M23 | broken `repr` not caught | test_contract_api: error whose repr raises |
| M24 | cause is the last error | test_errors: batches, errors and cause |
| M25 | sync iterator ages from the newest item | test_batching: window elapses |
| M26 | partial batch dropped on source error | test_batching: items read before an error yielded |
| M27 | count check `>=` to `>` | test_batching: count limit reached |
| M28 | async idle wait ignores elapsed time | survived; killed by the new test_abatching: idle wait counts from the first item |
| M29 | read in flight cancelled on idle flush | test_abatching: partial batch while idle |
| M30 | pending read not cancelled on close | test_abatching: closing early cancels the read (hang, per-test timeout) |
| M31 | plain async sink awaited | test_contract_api: plain flush counts as delivered |
| M32 | stopped task loses buffered items | test_abatcher_edges: items buffered at cancel handed back |
| M33 | progress event never set | test_abatcher: size flush and close (hang, per-test timeout) |
| M34 | async room check `>=` to `>` | test_abatcher: back-pressure and add timeout |
| M35 | `aclose` does not reap the task | test_abatcher_edges: aclose hands back on cancel |
| M36 | names keep private modules | test_contract_api: package reported as module |
| M37 | one stamp too few removed | test_model: push batcher matches the model |
| M38 | progress not notified after delivery | test_hostile: huge timeouts (hang, per-test timeout) |
| M39 | async `on_error` bypassed | test_abatcher: failures go to on_error |
| M40 | handlers not checked in `TimedBatcher` | test_contract_api: handlers refused at once |

M28 survived because no test waited for an idle flush after a second item arrived late
in the window; `test_the_idle_wait_counts_from_the_first_item_not_from_the_latest` now
fails with it (checked with the mutant applied) and passes without it.

## Packaging, e2e and Pythons (items 8, 10, 13, 14)

- Wheel `timedbatch-1.0.0-py3-none-any.whl`: the ten modules, `py.typed`, METADATA,
  WHEEL, RECORD and `licenses/LICENSE`; Production/Stable; only dev extras require
  anything. Sdist: sources, tests, scripts, README, GUIDE, CHANGELOG, CONTRIBUTING,
  LICENSE and the upstream manifest only (no fetched files, no `.github`, no reports).
  Both asserted by `tests/test_e2e_wheel.py`; `twine check --strict` passes on both.
- At d1bcf97 on this PC, from a fresh clone: the wheel and sdist built once, then for
  each interpreter the sdist's own suite with `src` removed, run against the wheel
  installed in a fresh venv, with the fetched sources, both oracle packages,
  `TIMEDBATCH_E2E=1` and `TIMEDBATCH_WHEEL` (the e2e tests install that wheel in another
  fresh venv and run the consumer program from an empty folder with `-I`):
  CPython 3.10.19 and 3.11.5 1261 passed, 1 skipped (`itertools.batched` is 3.12+);
  3.12.12, 3.13.2, 3.14.2 and 3.14.2 free-threaded 1262 passed. 3.8 and 3.9 are installed
  but below `requires-python`.

## Gates (item 15)

At 49e6005 on Elena's PC from a fresh clone (CPython 3.14.2), each command on its own,
with the fetched sources, both oracle packages and `TIMEDBATCH_E2E=1`:
`ruff format --check .`, `ruff check .`, `mypy --strict src`, `mypy --strict src tests`
exit 0 and `pytest` 1261 passed. The final head is checked the same way before the PR is
handed over (see the PR).
