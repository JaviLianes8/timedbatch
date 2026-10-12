"""Build the wheel, install it in a throwaway venv and drive the installed package.

Slow and needs the build backend, so it runs only with TIMEDBATCH_E2E=1.
"""

from __future__ import annotations

import os
import subprocess
import sys
import venv
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.skipif(
    os.environ.get("TIMEDBATCH_E2E") != "1", reason="set TIMEDBATCH_E2E=1 to build and install"
)

PROGRAM = """
import asyncio, importlib.metadata, importlib.resources, pickle, threading
import timedbatch
from timedbatch import (
    AsyncTimedBatcher, BatcherClosed, FlushError, TimedBatcher, abatched_by_time,
    batched_by_time,
)

assert "site-packages" in timedbatch.__file__, timedbatch.__file__
assert importlib.metadata.version("timedbatch") == timedbatch.__version__
assert importlib.resources.files("timedbatch").joinpath("py.typed").is_file()
assert list(batched_by_time(range(5), 2, 60.0)) == [[0, 1], [2, 3], [4]]

out = []
idle = threading.Event()
with TimedBatcher(lambda b: (out.append(b), idle.set()), max_items=3, max_seconds=0.05) as b:
    b.add(1)
    assert idle.wait(10), "idle batch never flushed"
    for value in range(2, 6):
        b.add(value)
assert out == [[1], [2, 3, 4], [5]], out
try:
    b.add(6)
except BatcherClosed:
    pass
else:
    raise AssertionError("closed batcher accepted an item")

def fail(batch):
    raise ValueError("down")

failing = TimedBatcher(fail, max_items=5, max_seconds=60.0)
failing.add("x")
try:
    failing.close()
except FlushError as error:
    assert pickle.loads(pickle.dumps(error)).batches == [["x"]]

async def main():
    got = []
    async def send(batch):
        got.append(batch)
    async with AsyncTimedBatcher(send, max_items=2, max_seconds=60.0) as a:
        for value in range(3):
            await a.add(value)
    assert got == [[0, 1], [2]], got
    async def ticks():
        yield 1
        await asyncio.sleep(0.3)
        yield 2
    assert [x async for x in abatched_by_time(ticks(), 10, 0.05)] == [[1], [2]]

asyncio.run(main())

forgotten = TimedBatcher(lambda batch: print("exit", batch), max_items=10, max_seconds=60.0)
forgotten.add("late")
print("e2e ok")
"""


def _run(*cmd: str | Path, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(c) for c in cmd], cwd=cwd, capture_output=True, text=True, check=False
    )


def test_installed_wheel_end_to_end(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    built = _run(sys.executable, "-m", "build", "--wheel", "--outdir", dist, ROOT)
    assert built.returncode == 0, built.stderr
    (wheel,) = dist.glob("timedbatch-*.whl")

    env_dir = tmp_path / "venv"
    venv.create(env_dir, with_pip=True)
    bindir = env_dir / ("Scripts" if sys.platform == "win32" else "bin")
    python = bindir / "python"
    installed = _run(python, "-m", "pip", "install", "-q", wheel)
    assert installed.returncode == 0, installed.stderr

    # Run from an empty folder so the source tree cannot shadow the install.
    work = tmp_path / "work"
    work.mkdir()
    result = _run(python, "-I", "-c", PROGRAM, cwd=work)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["e2e ok", "exit ['late']"]
