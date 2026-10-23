"""Build the wheel and sdist, inspect them, install the wheel in a throwaway venv and use it.

Slow and needs the build backend, so it runs only with TIMEDBATCH_E2E=1. Set
TIMEDBATCH_WHEEL to the path of an already built wheel to test that wheel instead of
building one (the contents test then needs the matching sdist next to it).
"""

from __future__ import annotations

import os
import subprocess
import sys
import tarfile
import venv
import zipfile
from pathlib import Path

import pytest
from e2e_program import PROGRAM

ROOT = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.skipif(
    os.environ.get("TIMEDBATCH_E2E") != "1", reason="set TIMEDBATCH_E2E=1 to build and install"
)

MODULES = {
    "__init__.py", "_abatcher.py", "_abatching.py", "_aengine.py", "_batcher.py",
    "_batching.py", "_core.py", "_engine.py", "_errors.py", "_limits.py", "py.typed",
}  # fmt: skip


def _run(*cmd: str | Path, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(c) for c in cmd], cwd=cwd, capture_output=True, text=True, check=False
    )


@pytest.fixture(scope="module")
def dist(tmp_path_factory: pytest.TempPathFactory) -> Path:
    given = os.environ.get("TIMEDBATCH_WHEEL")
    if given:
        return Path(given).resolve().parent
    folder = tmp_path_factory.mktemp("dist")
    built = _run(sys.executable, "-m", "build", "--outdir", folder, ROOT)
    assert built.returncode == 0, built.stderr
    return folder


def _wheel(dist: Path) -> Path:
    given = os.environ.get("TIMEDBATCH_WHEEL")
    if given:
        return Path(given).resolve()
    (wheel,) = dist.glob("timedbatch-*.whl")
    return wheel


def test_wheel_and_sdist_contents(dist: Path) -> None:
    names = zipfile.ZipFile(_wheel(dist)).namelist()
    package = {name.split("/", 1)[1] for name in names if name.startswith("timedbatch/")}
    assert package == MODULES
    info = [name for name in names if not name.startswith("timedbatch/")]
    assert all(".dist-info/" in name for name in info), info
    assert any(name.endswith("licenses/LICENSE") for name in info), info
    metadata = zipfile.ZipFile(_wheel(dist)).read(
        next(name for name in info if name.endswith("METADATA"))
    )
    assert b"Classifier: Development Status :: 5 - Production/Stable" in metadata
    requires = [line for line in metadata.splitlines() if line.startswith(b"Requires-Dist:")]
    assert requires and all(b"extra ==" in line for line in requires), requires  # dev only

    (sdist,) = dist.glob("timedbatch-*.tar.gz")
    with tarfile.open(sdist) as archive:
        members = {name.split("/", 1)[1] for name in archive.getnames() if "/" in name}
    for required in ("LICENSE", "README.md", "CHANGELOG.md", "GUIDE.md", "pyproject.toml"):
        assert required in members, required
    assert {f"src/timedbatch/{module}" for module in MODULES} <= members
    assert "tests/upstream/MANIFEST.json" in members
    upstream = {name for name in members if name.startswith("tests/upstream/")}
    assert upstream == {"tests/upstream/MANIFEST.json"}, upstream
    for name in members:
        assert "__pycache__" not in name and not name.endswith(".pyc"), name
        assert not name.startswith((".github", "IMPROVEMENTS", "CERT-1.0")), name


def test_installed_wheel_end_to_end(dist: Path, tmp_path: Path) -> None:
    env_dir = tmp_path / "venv"
    venv.create(env_dir, with_pip=True)
    bindir = env_dir / ("Scripts" if sys.platform == "win32" else "bin")
    python = bindir / "python"
    installed = _run(python, "-m", "pip", "install", "-q", _wheel(dist))
    assert installed.returncode == 0, installed.stderr

    # Run from an empty folder so the source tree cannot shadow the install.
    work = tmp_path / "work"
    work.mkdir()
    result = _run(python, "-I", "-c", PROGRAM, cwd=work)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ["e2e ok", "exit ['late']"]
