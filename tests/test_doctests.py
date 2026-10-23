"""Every public name carries a runnable example in its docstring, and every example passes."""

from __future__ import annotations

import doctest

import pytest

import timedbatch


def _examples(name: str) -> list[doctest.DocTest]:
    # module=False: the public names report the package as their module, which the default
    # finder would read as "defined elsewhere" and skip.
    finder = doctest.DocTestFinder(recurse=True)
    found = finder.find(getattr(timedbatch, name), name, module=False, globs={})
    return [test for test in found if test.examples]


@pytest.mark.parametrize("name", timedbatch.__all__)
def test_the_docstring_has_an_example(name: str) -> None:
    obj = getattr(timedbatch, name)
    assert obj.__doc__ and obj.__doc__.strip(), name
    assert any(test.name == name for test in _examples(name)), name


@pytest.mark.parametrize("name", timedbatch.__all__)
def test_the_examples_pass(name: str) -> None:
    runner = doctest.DocTestRunner(optionflags=doctest.ELLIPSIS)
    for test in _examples(name):
        runner.run(test)
    results = runner.summarize(verbose=False)
    assert results.attempted > 0
    assert results.failed == 0, name
