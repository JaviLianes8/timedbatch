"""Fetch the third-party files listed in tests/upstream/MANIFEST.json.

The files (CPython documentation sources and the CPython itertools tests at a fixed tag)
are not part of the repository. Their tests skip until this script has fetched them; each
download is checked against the size and sha256 in the manifest.

    python scripts/fetch_upstream.py
"""

from __future__ import annotations

import hashlib
import json
import sys
import urllib.request
from pathlib import Path

FOLDER = Path(__file__).resolve().parents[1] / "tests" / "upstream"
MANIFEST = FOLDER / "MANIFEST.json"


def main() -> int:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    failed = 0
    for name, entry in manifest.items():
        request = urllib.request.Request(entry["url"], headers={"User-Agent": "timedbatch-tests"})
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
        digest = hashlib.sha256(body).hexdigest()
        if len(body) != entry["size"] or digest != entry["sha256"]:
            print(f"{name}: got {len(body)} bytes, sha256 {digest}; manifest differs")
            failed += 1
            continue
        (FOLDER / name).write_bytes(body)
        print(f"{name}: ok")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
