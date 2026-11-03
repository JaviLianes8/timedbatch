"""Hand-written hostile errors must preserve recovery data and safe diagnostics."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import traceback
from pathlib import Path

import pytest

from timedbatch import AsyncTimedBatcher, FlushError, TimedBatcher


class Unprintable(ValueError):
    def __repr__(self) -> str:
        raise KeyboardInterrupt("repr must not run")

    def __str__(self) -> str:
        raise KeyboardInterrupt("str must not run")


@pytest.mark.parametrize("hostile", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_reporting_never_formats_sink_errors(hostile: bool, asynchronous: bool) -> None:
    secret = "password=example-secret"
    failure = Unprintable(secret) if hostile else ValueError(secret)

    def sink(batch: list[str]) -> None:
        raise failure

    def check(error: FlushError) -> None:
        assert error.batches == [[secret]]
        assert error.errors[0] is failure
        assert str(error) == "1 batch (1 item) failed to flush"
        assert error.__cause__ is None
        rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        assert secret not in rendered

    async def main() -> None:
        out = AsyncTimedBatcher(sink, max_items=1, max_seconds=1)
        await out.add(secret)
        with pytest.raises(FlushError) as caught:
            await out.aclose()
        check(caught.value)
        assert out.pending == 0
        await out.aclose()

    try:
        if asynchronous:
            asyncio.run(main())
        else:
            out = TimedBatcher(sink, max_items=1, max_seconds=1, background=False)
            out.add(secret)
            with pytest.raises(FlushError) as caught:
                out.poll()
            check(caught.value)
            assert out.pending == 0
            out.close()
    except KeyboardInterrupt:
        pytest.fail("reporting called hostile exception formatting")


def test_direct_report_suppresses_implicit_context_and_keeps_all_associations() -> None:
    failures: list[BaseException] = [
        ValueError("first-secret"),
        KeyError("second-secret"),
        OSError("third-secret"),
    ]
    try:
        raise failures[0]
    except ValueError:
        with pytest.raises(FlushError) as caught:
            raise FlushError([[1], [2], [3]], failures)  # noqa: B904 - exercise implicit context
    error = caught.value
    for batch, actual, expected in zip(error.batches, error.errors, failures, strict=True):
        assert batch == [failures.index(expected) + 1]
        assert actual is expected
    assert error.__cause__ is None
    rendered = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert "first-secret" not in rendered


def test_report_does_not_deadlock_when_repr_reads_pending() -> None:
    program = """
from timedbatch import FlushError, TimedBatcher
class Failure(Exception):
    def __repr__(self):
        return str(out.pending)
def sink(batch):
    raise Failure()
out = TimedBatcher(sink, max_items=1, max_seconds=1,
                   background=False, flush_at_exit=False)
out.add(7)
try:
    out.poll()
except FlushError as error:
    assert error.batches == [[7]]
out.close()
"""
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
    result = subprocess.run(
        [sys.executable, "-c", program], env=env, capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("asynchronous", [False, True])
def test_report_construction_failure_preserves_ledger(
    monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    def broken_report(*args: object) -> None:
        raise MemoryError("report allocation")

    def sink(batch: list[int]) -> None:
        raise ValueError("sink")

    async def main() -> None:
        out = AsyncTimedBatcher(sink, max_items=1, max_seconds=1)
        await out.add(7)
        with monkeypatch.context() as patch:
            patch.setattr("timedbatch._core.FlushError", broken_report)
            with pytest.raises(MemoryError):
                await out.aclose()
        with pytest.raises(FlushError) as caught:
            await out.aclose()
        assert caught.value.batches == [[7]]
        assert out.pending == 0

    if asynchronous:
        asyncio.run(main())
    else:
        out = TimedBatcher(sink, max_items=1, max_seconds=1, background=False)
        out.add(7)
        with monkeypatch.context() as patch:
            patch.setattr("timedbatch._core.FlushError", broken_report)
            with pytest.raises(MemoryError):
                out.poll()
        with pytest.raises(FlushError) as caught:
            out.close()
        assert caught.value.batches == [[7]]
        assert out.pending == 0
