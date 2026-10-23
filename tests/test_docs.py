"""The README and GUIDE examples run and print what they say; metadata agrees with the code."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import timedbatch

ROOT = Path(__file__).resolve().parents[1]

OUTPUTS = [
    ["['a', 'b']", "['c', 'd']", "['e']"],
    ["insert [0, 1]", "insert [2, 3]", "insert [4]"],
    [
        "[['order-1', 'order-2']]",
        "1 batch (2 items) failed to flush: ConnectionError('database is down')",
    ],
    ["[]", "[['x']]"],
    ["[1, 2]", "[3]"],
    ["send ['a', 'b', 'c']", "send ['d']"],
]
GUIDE_OUTPUTS = [
    ["[[1, 2]] [ConnectionError('database is down')]", "[[1, 2], [1, 2], [3]]"],
    ["[['first', 'second']]"],
]
DOCS = {"README.md": OUTPUTS, "GUIDE.md": GUIDE_OUTPUTS}
CASES = [(name, index) for name, outputs in DOCS.items() for index in range(len(outputs))]


def _read(name: str = "README.md") -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def _blocks(name: str) -> list[str]:
    return re.findall(r"```python\n(.*?)```", _read(name), flags=re.DOTALL)


@pytest.mark.parametrize("name", sorted(DOCS))
def test_there_is_one_expected_output_per_example(name: str) -> None:
    assert len(_blocks(name)) == len(DOCS[name])


@pytest.mark.parametrize(("name", "index"), CASES)
def test_each_example_prints_its_output(
    name: str, index: int, capsys: pytest.CaptureFixture[str]
) -> None:
    exec(compile(_blocks(name)[index], name, "exec"), {"__name__": "docs"})

    assert capsys.readouterr().out.splitlines() == DOCS[name][index]


@pytest.mark.parametrize(("name", "index"), CASES)
def test_each_printed_line_is_shown_as_a_comment(name: str, index: int) -> None:
    comments = re.findall(r"#\s(.+)$", _blocks(name)[index], flags=re.MULTILINE)
    shown = [text.strip() for text in comments]

    for line in DOCS[name][index]:
        assert line in shown, line


@pytest.mark.parametrize("name", sorted(DOCS))
def test_every_public_name_appears_in_the_docs(name: str) -> None:
    for public in timedbatch.__all__:
        assert f"`{public}`" in _read(name), public


def test_the_readme_links_are_absolute() -> None:
    links = re.findall(r"\]\(([^)]+)\)", _read())
    assert links
    assert all(link.startswith("https://") for link in links), links


def test_version_matches_pyproject() -> None:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', text, flags=re.MULTILINE)
    assert match is not None
    assert match.group(1) == timedbatch.__version__


def test_changelog_has_an_entry_for_this_version() -> None:
    text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert re.search(
        rf"^## \[{re.escape(timedbatch.__version__)}\] - \d{{4}}-\d{{2}}-\d{{2}}$", text, re.M
    )


def test_no_em_dash_in_any_shipped_text() -> None:
    files = [*ROOT.glob("*.md"), *ROOT.glob("src/**/*.py"), *ROOT.glob("tests/*.py")]
    for path in files:
        assert chr(0x2014) not in path.read_text(encoding="utf-8"), path
