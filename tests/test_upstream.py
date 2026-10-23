"""Ground truth from CPython v3.14.0, fetched by scripts/fetch_upstream.py (hash checked).

Count-only batching (``max_seconds=math.inf``) must agree with ``itertools.batched``: its
documented pure Python recipe (cut from the fetched docs), its own test vectors (read
from the fetched CPython test file), and the C implementation where the running Python
has it. The platform facts the design leans on are located in the fetched docs and then
checked against the running interpreter.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import itertools
import json
import math
import re
import textwrap
from collections.abc import Callable, Iterator
from itertools import product
from pathlib import Path
from typing import Any

import pytest

from timedbatch import batched_by_time

FOLDER = Path(__file__).resolve().parent / "upstream"
MANIFEST = json.loads((FOLDER / "MANIFEST.json").read_text(encoding="utf-8"))
ITERABLES: list[Any] = ["", "a", "ab", "abc", "abcd", "abcde", "abcdef", "abcdefg", None]
SIZES: list[Any] = [-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, None, "hello", 2.0, True]


def _fetched(name: str) -> str:
    path = FOLDER / name
    if not path.is_file():
        pytest.skip("run scripts/fetch_upstream.py to fetch the CPython sources")
    body = path.read_bytes()
    assert hashlib.sha256(body).hexdigest() == MANIFEST[name]["sha256"], name
    return body.decode("utf-8")


def _recipe() -> Callable[[Any, Any], Iterator[tuple[Any, ...]]]:
    text = _fetched("itertools.rst")
    section = text[text.index(".. function:: batched(") :]
    start = section.index("def batched(")
    lines = section[start:].splitlines()
    indent = len(re.match(r"\s*", section[section.rindex("\n", 0, start) + 1 :]).group())  # type: ignore[union-attr]
    body = [lines[0]]
    for line in lines[1:]:
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        body.append(line)
    source = textwrap.dedent(" " * indent + "\n".join(body))
    namespace: dict[str, Any] = {"islice": itertools.islice}
    exec(compile(source, "itertools.rst", "exec"), namespace)
    return namespace["batched"]  # type: ignore[no-any-return]


def _outcome(
    function: Callable[[Any, Any], Iterator[Any]], iterable: Any, size: Any
) -> tuple[type[BaseException] | None, Any]:
    try:
        return None, [list(batch) for batch in function(iterable, size)]
    except Exception as error:
        return type(error), None


def _ours(iterable: Any, size: Any) -> Iterator[list[Any]]:
    return batched_by_time(iterable, size, math.inf)


def test_count_only_batching_matches_the_documented_recipe() -> None:
    recipe = _recipe()
    for iterable, size in product(ITERABLES, SIZES):
        if isinstance(size, (bool, float)):
            continue  # the recipe leaves these to islice; covered against C below
        expected = _outcome(recipe, iterable, size)
        assert _outcome(_ours, iterable, size) == expected, (iterable, size)


@pytest.mark.skipif(not hasattr(itertools, "batched"), reason="itertools.batched is 3.12+")
def test_count_only_batching_matches_the_c_implementation() -> None:
    _fetched("itertools.rst")
    batched: Callable[[Any, Any], Iterator[tuple[Any, ...]]] = itertools.batched  # type: ignore[attr-defined]
    for iterable, size in product([*ITERABLES, range(100), [None] * 9], SIZES):
        expected = _outcome(batched, iterable, size)
        if isinstance(size, bool) and iterable is not None:
            # batched() takes True as 1; timedbatch refuses bool counts on purpose.
            assert expected[0] is None
            expected = (TypeError, None)
        assert _outcome(_ours, iterable, size) == expected, (iterable, size)


def test_count_only_batching_meets_the_cpython_test_vectors() -> None:
    tree = ast.parse(_fetched("cpython-test_itertools.py.txt"))
    test = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "test_batched"
    )
    checked = 0
    for node in ast.walk(test):
        if not (isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "assertEqual"):
            continue
        actual, expected = node.args
        call = actual.args[0] if isinstance(actual, ast.Call) and actual.args else None
        if not (isinstance(call, ast.Call) and getattr(call.func, "id", "") == "batched"):
            continue
        if call.keywords:  # strict=True has no counterpart
            continue
        data, size = (ast.literal_eval(arg) for arg in call.args)
        assert list(_ours(data, size)) == [list(batch) for batch in ast.literal_eval(expected)]
        checked += 1
    assert checked == 3


def test_the_docs_still_say_what_the_design_relies_on() -> None:
    asyncio_text = " ".join(_fetched("asyncio-task.rst").split())
    assert "Unlike :func:`~asyncio.wait_for`, ``wait()`` does not cancel the futures" in (
        asyncio_text
    )
    atexit_text = " ".join(_fetched("atexit.rst").split())
    assert "or when :func:`os._exit` is called" in atexit_text
    threading_text = " ".join(_fetched("threading.rst").split())
    assert "Specifying a timeout greater than this value will raise an :exc:`OverflowError`" in (
        threading_text
    )


def test_the_interpreter_behaves_as_the_docs_say() -> None:
    _fetched("asyncio-task.rst")

    async def main() -> tuple[bool, bool]:
        loop = asyncio.get_running_loop()
        kept: asyncio.Future[None] = loop.create_future()
        await asyncio.wait({kept}, timeout=0.01)
        cancelled: asyncio.Future[None] = loop.create_future()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(cancelled, timeout=0.01)
        return kept.done(), cancelled.cancelled()

    assert asyncio.run(main()) == (False, True)
