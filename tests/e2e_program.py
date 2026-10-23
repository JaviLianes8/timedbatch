"""The program the e2e test runs with the installed wheel, as an outside consumer would."""

PROGRAM = """
import asyncio, importlib.metadata, importlib.resources, pickle, threading
import timedbatch
from timedbatch import (
    AsyncTimedBatcher, BatcherClosed, FlushError, TimedBatcher, abatched_by_time,
    batched_by_time,
)

assert "site-packages" in timedbatch.__file__, timedbatch.__file__
assert importlib.metadata.version("timedbatch") == timedbatch.__version__
assert importlib.resources.files("timedbatch").joinpath("py.typed").is_file()
assert list(batched_by_time(range(5), 2, 60.0)) == [[0, 1], [2, 3], [4]]

out = []
idle = threading.Event()
with TimedBatcher(lambda b: (out.append(b), idle.set()), max_items=3, max_seconds=0.05) as b:
    b.add(1)
    assert idle.wait(10), "idle batch never flushed"
    for value in range(2, 6):
        b.add(value)
assert out == [[1], [2, 3, 4], [5]], out
try:
    b.add(6)
except BatcherClosed:
    pass
else:
    raise AssertionError("closed batcher accepted an item")

def fail(batch):
    raise ValueError("down")

failing = TimedBatcher(fail, max_items=5, max_seconds=60.0)
failing.add("x")
try:
    failing.close()
except FlushError as error:
    assert pickle.loads(pickle.dumps(error)).batches == [["x"]]

async def main():
    got = []
    async def send(batch):
        got.append(batch)
    async with AsyncTimedBatcher(send, max_items=2, max_seconds=60.0) as a:
        for value in range(3):
            await a.add(value)
    assert got == [[0, 1], [2]], got
    async def ticks():
        yield 1
        await asyncio.sleep(0.3)
        yield 2
    assert [x async for x in abatched_by_time(ticks(), 10, 0.05)] == [[1], [2]]

asyncio.run(main())

async def plain_sink():
    got = []
    async with AsyncTimedBatcher(got.append, max_items=2, max_seconds=60.0) as a:
        for value in range(3):
            await a.add(value)
    assert got == [[0, 1], [2]], got

asyncio.run(plain_sink())
assert all(getattr(timedbatch, name).__module__ == "timedbatch" for name in timedbatch.__all__)
assert len(str(FlushError([["x"]], [ValueError("y" * 10_000)]))) < 300
assert list(batched_by_time(range(3), 2, 10**400)) == [[0, 1], [2]]

forgotten = TimedBatcher(lambda batch: print("exit", batch), max_items=10, max_seconds=60.0)
forgotten.add("late")
print("e2e ok")
"""
