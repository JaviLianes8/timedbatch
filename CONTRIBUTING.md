# Contributing

Thanks for taking the time. This project keeps a small, predictable shape.

## Setup

```
pip install -r requirements-dev.txt
```

## Before you open a pull request

- Keep public API in `src/timedbatch/__init__.py`; internals live in `_`-prefixed modules.
- Add a test for the behaviour you change. Tests inject time, disk and network where
  they can. Three files use the real clock or real processes on purpose:
  `test_batcher_thread.py` and the asyncio tests (idle flushes need real waiting, with
  wide margins), `test_stress.py` (many threads and tasks), and `test_exit.py` (child
  interpreters prove the exit flush).
- `TIMEDBATCH_E2E=1 pytest tests/test_e2e_wheel.py` builds the wheel, installs it in a
  fresh virtual environment and drives the installed package.
- Run `scripts/check.ps1` (format, lint, types, tests) and make sure it passes.
- One logical change per commit, imperative subject line.

Open an issue first if the change is large or shifts the public API.
