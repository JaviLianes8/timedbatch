from __future__ import annotations

from collections.abc import Callable

from timedbatch import TimedBatcher


class FakeClock:
    """A clock whose value is set by the test, so no real time passes."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def make(
    sink: Callable[[list[int]], object],
    clock: FakeClock | None = None,
    **options: object,
) -> TimedBatcher[int]:
    settings: dict[str, object] = {"max_items": 3, "max_seconds": 5.0}
    settings.update(options)
    return TimedBatcher(
        sink,
        clock=clock or FakeClock(),
        background=False,
        flush_at_exit=False,
        **settings,  # type: ignore[arg-type]
    )
