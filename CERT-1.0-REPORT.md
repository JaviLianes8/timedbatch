# timedbatch 1.0.0 certification report

The 1.0.0 certification bar (19 items) applied to timedbatch. On 2026-11-02,
`https://pypi.org/pypi/timedbatch/json` lists 0.2.0, uploaded on 2026-11-01. Its
package files are byte-for-byte identical to `main` at 1a0c415. The installed-wheel
upgrade test covers this actual release; every behavior change is in the CHANGELOG.
The earlier preparation evidence below is retained, with current results recorded in
the 2026-11-02 section. Five QA lenses and independent review remain pending.

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
- [ ] PR #3 merged and published by the maintainer (item 17); the PR and trace guard are done

## Scorecard

| # | Item | Result |
| --- | --- | --- |
| 1 | Public API reviewed for stability | PASS: the seven names, parameter names and defaults kept; widened numeric annotations pinned by `tests/test_contract_api.py`; behaviour changes in the CHANGELOG |
| 2 | `__all__` exact, no private imports in docs | PASS: every public name reports `timedbatch` as its module |
| 3 | Docstrings with runnable examples | PASS: all seven, run by `tests/test_doctests.py`; README (6) and GUIDE (2) examples run by `tests/test_docs.py` |
| 4 | Typed end to end | PASS: `mypy --strict src tests`, `py.typed`, consumer snippet checked by mypy; `FlushError.batches` is `list[list[Any]]`, documented |
| 5 | Typed, documented, bounded errors | PASS: errors table in README and GUIDE; count-only `FlushError` diagnostics, no sink exception formatting or chaining; see QA round 1 |
| 6 | Contract tests of every public name | PASS |
| 7 | Real ground truth | PASS: CPython v3.14.0 sources fetched (hash manifest), the C `itertools.batched`, more-itertools 11.2.1, reactivex 6.1.0 |
| 8 | Real e2e from the wheel | PASS: wheel installed in a fresh venv and driven from an empty folder; upgrade from actual PyPI 0.2.0; no CLI |
| 9 | Seeded fuzz, no crash, hang or blowup | PASS |
| 10 | Supported Pythons | PASS: CPython 3.14.6 is the only available interpreter on this laptop; earlier matrix retained below; other supported versions unavailable here |
| 11 | README and `twine check` | PASS: `twine check --strict` on wheel and sdist, absolute links (tested), badge, errors, honest comparison |
| 12 | Dated CHANGELOG section | PASS: 1.0.0, 2026-11-02 |
| 13 | pyproject 1.0.0, Production/Stable | PASS |
| 14 | sdist and wheel contents | PASS |
| 15 | Gates each exit 0 | PASS: see Gates |
| 16 | Independent reviewer | pending |
| 17 | PR and trace guard | PR #3 from `release/1.0.0`, trace guard CLEAN; merge and publish left to the maintainer |
| 18 | Contracts bite (mutations) | PASS: original 40 mutants retained; ten more on the updated validation module, all killed |
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
  start their timer at window creation and emit empty lists, while timedbatch ages
  each batch from its first item and sends nothing while idle. Count-triggered closures
  open a new window and reset the timer, as verified in QA round 1 below.
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
- Earlier run at d1bcf97 on the original certification PC, from a fresh clone:
  the wheel and sdist built once, then for
  each interpreter the sdist's own suite with `src` removed, run against the wheel
  installed in a fresh venv, with the fetched sources, both oracle packages,
  `TIMEDBATCH_E2E=1` and `TIMEDBATCH_WHEEL` (the e2e tests install that wheel in another
  fresh venv and run the consumer program from an empty folder with `-I`):
  CPython 3.10.19 and 3.11.5 1261 passed, 1 skipped (`itertools.batched` is 3.12+);
  3.12.12, 3.13.2, 3.14.2 and 3.14.2 free-threaded 1262 passed. 3.8 and 3.9 are installed
  but below `requires-python`.

## Earlier gates (item 15)

At bda3760 (later commits change only this report and the CHANGELOG date, after which `tests/test_docs.py`, ruff and `mypy --strict src` were rerun and exit 0) on the laptop
from a fresh clone (CPython 3.14.6), each command on its own, with the fetched sources,
both oracle packages and `TIMEDBATCH_E2E=1`: `ruff format --check .`, `ruff check .`,
`mypy --strict src` and `mypy --strict src tests` exit 0, and `pytest` 1262 passed
(exit 0). The same commands at 49e6005 on Elena's PC (CPython 3.14.2): exit 0, 1261 passed.

## Preparation refresh, 2026-11-02

Resumed from 4ddf744 without discarding any earlier commits or evidence. Items 16, 17
(merge and publication) and 19 remain for subsequent review. No prior lens verdicts
or independent reviewer approval were recorded.

### Published baseline and upgrade

The live PyPI JSON response now lists 0.2.0, uploaded 2026-11-01T02:11:50 UTC.
Downloaded its wheel using `pip download --no-deps timedbatch==0.2.0` and checked its
SHA-256 against that response:
`c56c5af7a10f5da72f7ea857a4ebbb3e3becf6fc79044d1e61f84a6f82923322`.
Every package file matches `git show main:src/timedbatch/<file>` byte for byte.

`test_upgrade_from_pypi_020` installs that wheel in a fresh venv, verifies version
0.2.0, then installs 1.0.0 with `pip install --upgrade` and verifies the new version.
Before and after, the same consumer program checks 2,000 seeded count-only cases,
200 deterministic push timelines and an async sink. Outputs match exactly. A
`FlushError` pickled by 0.2.0 loads with the same batches and errors after the upgrade.
Finally, the complete 1.0.0 consumer program runs using `-I` from an empty directory:
threaded and async delivery, idle flushing, closure, typed resources, bounded errors,
plain async sinks, huge windows and a real interpreter-exit flush. No CLI is provided.

### Changes and current ground truth

The dated CHANGELOG now describes the upgrade and includes the previously omitted
huge-window, timeout and signaling-NaN changes. The only new runtime change is refusing
`clock=None` immediately, consistently across all four entry points. Optional
`on_error=None` is unchanged. `test_none_is_not_a_clock` pins the error type and message.

All five CPython v3.14.0 files were fetched again on 2026-11-02 from the URLs in
`tests/upstream/MANIFEST.json`; sizes and hashes match. `TIMEDBATCH_UPSTREAM` now lets
the fetch script and tests use an external directory. The manifest records today's
fetch date; third-party content is neither committed nor included in the distributions.
The documented recipe, literal upstream vectors, live `itertools.batched`, interpreter
timeout semantics, more-itertools 11.2.1 and reactivex 6.1.0 all pass their comparisons.
The existing seeded model, hostile-input and fuzz tests also pass.

### Additional mutation round

Ten individual mutations of the updated validation module, each in an isolated copy
on this laptop with CPython 3.14.6, bytecode disabled and a 300-second per-test timeout.
Each run used the clock, limit, hostile-input and API contract tests with `-x`.
All ten exited 1 with an assertion failure; none survived or timed out. The original
40-mutation round above is retained.

| Mutant | Change | Contract that failed |
| --- | --- | --- |
| N1 | Allow `None` clocks again | `test_none_is_not_a_clock` |
| N2 | Reject optional `None` error handlers | `test_none_is_not_a_clock` (wrong parameter error) |
| N3 | Reject valid callables instead of invalid ones | `test_none_is_not_a_clock` |
| N4 | Accept count zero | `test_every_entry_point_rejects_bad_limits_at_once` |
| N5 | Accept boolean counts | `test_every_entry_point_rejects_bad_limits_at_once` |
| N6 | Accept a zero window | `test_every_entry_point_rejects_bad_limits_at_once` |
| N7 | Reverse the pending-capacity comparison | `test_max_pending_must_hold_at_least_one_batch` |
| N8 | Skip Decimal NaN normalization | `test_every_nan_is_refused_with_the_same_message` |
| N9 | Reverse the sign of overflowed numeric limits | `test_a_window_beyond_the_float_range_means_no_age_limit` |
| N10 | Accept floating NaN | `test_every_entry_point_rejects_bad_limits_at_once` |

### Current packaging, interpreters and gates

CPython 3.14.6 is the only available interpreter on this laptop. The other entries from
`py -0p` refer to paths that no longer exist. PATH and the standard installation and
managed-interpreter directories contain no additional runtimes. Python 3.10, 3.11,
3.12, 3.13 and free-threaded 3.14 could not be rerun here; the earlier matrix above is
historical evidence, not a claim about the current laptop.

The wheel was built from the sdist using `python -m build --no-isolation`. Both pass
`twine check --strict` and the archive-content contracts. The README badge matches the
catalogue badge exactly; all README and GUIDE examples and public doctests pass.
All Python source files have at most 200 lines.

With the fetched sources, both reference packages, `TIMEDBATCH_E2E=1`,
`TIMEDBATCH_WHEEL` and `TIMEDBATCH_PYPI_WHEEL`, the full suite reports **1264 passed**.
The final sdist's own suite, extracted without `src` and run against the installed
wheel in the isolated dependency environment, also reports **1264 passed**, including
both installed-wheel consumer tests and the PyPI upgrade test.
`ruff format --check .`, `ruff check .`, `mypy --strict src` and
`mypy --strict src tests` each exit 0. Temporary directories, build artifacts, dependency
environments and mutation logs are kept outside the repository. Every pytest run uses
an explicit scratch `--basetemp` and `--timeout=300`.


## QA lenses, round 1, 2026-11-02

Reviewed base: `ef35b302e4bcce852769625b35fad5b53a84c017` on `release/1.0.0`.
Implementation commit: `322f16781e59ef5e61f4d8e104999a73c7256cba`
(`fix failure recovery and numeric contracts for 1.0.0`).

All 19 reported findings are addressed. Independent lens re-verification remains pending;
this fix round does not substitute for those verdicts.

| Findings | Change and contract evidence |
| --- | --- |
| A1 | Counts use `SupportsIndex`; durations and timeouts include `Decimal`, `Fraction` and `Real`. The strict numeric-consumer contract covers all four interfaces and all timeout methods. |
| A2, B1 | Retained failures count toward `pending` and `max_pending` until handback. Separate delivery completion counters keep flush and capacity waits working. Sync and async contracts cover retained items, waiting producers, exactly-once handback and final zero pending. The seeded accounting model now counts settled items. |
| A3 | `TimedBatcher.add` documents that synchronous sink or handler interrupts may escape after acceptance, with later failure handback. |
| A4, B2 | `closed` reconciles stopped async tasks without consuming failures. The cancellation contract checks repeated property reads and recovery of both attempted and buffered items. |
| A5, C-02 | Reporting never formats original errors, and the ledger is cleared only after report construction succeeds. Both batchers retain recovery data after a simulated construction failure; interrupting `repr` cannot discard it. |
| A6, C-01 | `FlushError` messages contain only batch and item counts. Original exceptions remain in `errors`; automatic cause and context chaining are suppressed, including stopped-task `BatcherClosed`. Direct and sink-driven contracts check secret-bearing errors and default traceback output. README, GUIDE, doctests and CHANGELOG describe the confidentiality boundary. |
| A7 | README describes window creation and count-triggered timer resets. The ReactiveX scheduler contract observes count closure at 202 and the next timeout at 212, rather than 210. |
| B3 | Positive float underflow rounds up to the smallest positive float. Contracts cover Decimal and Fraction across all four interfaces and an injected-clock boundary. |
| C-03 | No user exception formatting occurs during reporting under the lock. A bounded subprocess contract exercises an exception whose `repr` reads `pending`. |
| C-04 | Validation messages omit dynamic type names. Contracts use long, secret-bearing type names for count, window, pending, callback and async-source rejection. |
| C-05 | `aiter()` validates async sources at construction, with a bounded `TypeError` for malformed return values. Four malformed protocols are covered. |
| E-01 | A deterministic async iterator contract asserts complete batches for arrivals at 0, 5 and 5 with a five-second window. |
| E-02 | Three distinct errors are checked by identity against every corresponding batch. |
| E-03, E-04 | Destructive, failing error handlers cannot change recovery copies. Contracts cover the synchronous batcher and both plain and async handlers in the async batcher, including pending and exactly-once handback. |

### Regression and mutation evidence

Before implementation changes, the new focused contracts reported **29 failed, 12 passed**.
After the fixes they reported **41 passed**. The mutation checks ran sequentially in an
isolated scratch copy; each mutation failed with an assertion, not a timeout.

| Reviewer mutation | Contract that now rejects it |
| --- | --- |
| M06: async age `>=` becomes `>` | `test_async_iterator_flushes_at_exact_age` |
| M32: reverse errors after the first | `test_direct_report_suppresses_implicit_context_and_keeps_all_associations` |
| M34: synchronous handler receives retained list | `test_sync_handler_cannot_destroy_recovery_copy` |
| M35: async batcher's handler receives retained list | `test_async_handler_cannot_destroy_recovery_copy`, both handler forms |

### Gates

On CPython 3.14.6: `ruff format --check .`, `ruff check .`, `mypy --strict src`
and `mypy --strict src tests` each exit 0. The full suite exits 0 with **1296 passed**,
including wheel/sdist inspection, installed-wheel consumers, upgrade from PyPI 0.2.0,
fetched upstream sources and both reference packages. Every pytest run uses a
300-second per-test timeout and a scratch basetemp. All Python files remain at most
200 lines. README/GUIDE examples and public doctests pass.

The first full run was stopped after identifying an old test that waited for failed
items to disappear from `pending`. Both old delivery-failure waits now synchronize on
delivery completion and assert that retained items stay pending. The final full run
includes these updated contracts.

### Disputed findings

None.

## QA lenses, round 2, 2026-11-02

Reviewed base: `5cb0a393af99e5b080c9ca87978d066cc68b5fbd` on `release/1.0.0`.
Implementation commit: `3e151465a2b4012b28c36c6ea5e432df382bbdbe`
(`add async source traceback secrecy contract`).

Lens E confirmed E-01 through E-04 fixed and reported E-05, a missing regression
contract. E-05 is addressed; independent lens re-verification remains pending.

| Finding | Change and contract evidence |
| --- | --- |
| E-05 | Extended `test_async_source_rejection_does_not_echo_dynamic_type_names` to format the complete exception traceback and assert that both the hostile type name and its payload are absent. The bounded replacement message is checked exactly. Existing `from None` suppression is preserved; runtime behavior and the public API are unchanged. CHANGELOG records the added coverage. |

### Regression and mutation evidence

The extended contract passes against the unchanged implementation. In an isolated
scratch copy, N15 removes only `from None` from the invalid async-source `TypeError`
in `_abatching.py`. The same repository contract then fails with an assertion because
the formatted traceback contains the secret-bearing protocol error. The mutation was
restored byte for byte after the check. This is a coverage fix, not a runtime defect.

| Reviewer mutation | Contract that now rejects it |
| --- | --- |
| N15: remove async-source error context suppression | `tests/test_numeric_protocol_contract.py::test_async_source_rejection_does_not_echo_dynamic_type_names` |

### Gates

On CPython 3.14.6, `ruff format --check .`, `ruff check .`, `mypy --strict src`
and `mypy --strict src tests` each exit 0. The complete suite exits 0 with
**1296 passed**, including freshly built wheel/sdist inspection, installed-wheel
consumers, upgrade from PyPI 0.2.0, upstream sources and both reference packages.
All pytest invocations use a 300-second per-test timeout and scratch basetemps.
Python files remain at most 200 lines. Evidence is retained under
`C:/lianes8-build/codex-pylib/scratch/timedbatch-cert/fix-r2/`:
`baseline.log`, `N15.log` and `full.log`.

### Disputed findings

None.
