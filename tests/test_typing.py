"""The public API is typed: mypy infers results and rejects bad calls."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

api = pytest.importorskip("mypy.api")

SRC = Path(__file__).resolve().parents[1] / "src"

SNIPPET = """
from collections.abc import AsyncIterator
from timedbatch import AsyncTimedBatcher, FlushError, TimedBatcher
from timedbatch import abatched_by_time, batched_by_time
async def numbers() -> AsyncIterator[int]:
    yield 1
async def send(batch: list[int]) -> None: ...
def sink(batch: list[int]) -> None: ...
reveal_type(batched_by_time([1, 2], 2, 1.0))
reveal_type(abatched_by_time(numbers(), 2, 1.0))
reveal_type(TimedBatcher(sink, max_items=2, max_seconds=1.0))
reveal_type(AsyncTimedBatcher(send, max_items=2, max_seconds=1.0))
reveal_type(FlushError([[1]], [ValueError()]).batches)
TimedBatcher(sink, max_items=2, max_seconds=1.0).add("text")
TimedBatcher(sink, 2, 1.0)
AsyncTimedBatcher(send, max_items="2", max_seconds=1.0)
batched_by_time([1], 2, 1.0, clock=1.0)
AsyncTimedBatcher(sink, max_items=2, max_seconds=1.0)
"""


@pytest.fixture(scope="module")
def mypy_output(tmp_path_factory: pytest.TempPathFactory) -> str:
    folder = tmp_path_factory.mktemp("typing")
    target = folder / "snippet.py"
    target.write_text(SNIPPET, encoding="utf-8")
    config = folder / "mypy.ini"
    config.write_text(f"[mypy]\nmypy_path = {SRC.as_posix()}\n", encoding="utf-8")
    cache = str(folder / "cache")
    out, err, _ = api.run(
        ["--strict", "--config-file", str(config), "--cache-dir", cache, str(target)]
    )
    return str(out) + str(err)


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (9, "Iterator[list[int]]"),
        (10, "AsyncGenerator[list[int], None]"),
        (11, "TimedBatcher[int]"),
        (12, "AsyncTimedBatcher[int]"),
        (13, "list[list[Any]]"),
    ],
)
def test_results_are_typed(mypy_output: str, line: int, expected: str) -> None:
    match = re.search(rf"snippet\.py:{line}: note: Revealed type is \"(.+)\"", mypy_output)
    assert match, mypy_output
    # Strip module prefixes, which differ between mypy releases.
    revealed = re.sub(r"\b[a-z_][\w]*\.", "", match.group(1))
    assert revealed == expected


@pytest.mark.parametrize(
    ("line", "code"),
    [(14, "arg-type"), (15, "call-arg"), (16, "arg-type"), (17, "arg-type")],
)
def test_wrong_calls_are_rejected(mypy_output: str, line: int, code: str) -> None:
    assert re.search(rf"snippet\.py:{line}: error: .*\[{code}\]", mypy_output), mypy_output


def test_a_plain_flush_function_is_accepted_by_the_async_batcher(mypy_output: str) -> None:
    assert "snippet.py:18:" not in mypy_output, mypy_output
