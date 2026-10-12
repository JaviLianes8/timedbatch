"""A forgotten batcher still drains when a real interpreter exits."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"

PROGRAM = """
import sys
sys.path.insert(0, {src!r})
from timedbatch import TimedBatcher

def sink(batch):
    print("batch", batch, flush=True)

batcher = TimedBatcher(sink, max_items=1000, max_seconds=3600.0, {options})
for value in range(5):
    batcher.add(value)
{ending}
"""


def run(ending: str, options: str = "") -> subprocess.CompletedProcess[str]:
    code = PROGRAM.format(src=str(SRC), options=options, ending=textwrap.dedent(ending))
    return subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=60, check=False
    )


@pytest.mark.parametrize(
    ("ending", "code"),
    [("", 0), ("sys.exit(3)", 3), ("raise RuntimeError('crash')", 1)],
    ids=["normal", "sys-exit", "uncaught"],
)
@pytest.mark.parametrize("options", ["", "background=False"])
def test_buffered_items_are_flushed_at_exit(ending: str, code: int, options: str) -> None:
    result = run(ending, options)

    assert result.returncode == code, result.stderr
    assert result.stdout == "batch [0, 1, 2, 3, 4]\n"


def test_without_the_exit_hook_the_items_are_lost() -> None:
    result = run("", "flush_at_exit=False")

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


def test_a_closed_batcher_does_not_flush_again_at_exit() -> None:
    result = run("batcher.close()\nprint('closed', flush=True)")

    assert result.stdout == "batch [0, 1, 2, 3, 4]\nclosed\n"


def test_an_unreported_failure_is_printed_at_exit() -> None:
    code = f"""
import sys
sys.path.insert(0, {str(SRC)!r})
from timedbatch import TimedBatcher

def sink(batch):
    raise ValueError("sink down")

batcher = TimedBatcher(sink, max_items=1000, max_seconds=3600.0)
batcher.add("kept")
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, timeout=60, check=False
    )

    assert "FlushError: 1 batch (1 item) failed to flush: ValueError('sink down')" in result.stderr
