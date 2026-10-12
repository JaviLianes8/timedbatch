from __future__ import annotations


class FakeClock:
    """A clock whose value is set by the test, so no real time passes."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now
