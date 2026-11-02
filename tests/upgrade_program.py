"""Deterministic consumer behavior shared by PyPI 0.2.0 and 1.0.0."""

from __future__ import annotations

PROGRAM = """
import asyncio, json, math, pickle, random
from pathlib import Path
from timedbatch import AsyncTimedBatcher, FlushError, TimedBatcher, batched_by_time

rng = random.Random(20261102)
results = []
for _ in range(2000):
    data = [rng.randrange(1000) for _ in range(rng.randrange(100))]
    size = rng.randrange(1, 20)
    batches = list(batched_by_time(data, size, math.inf))
    assert batches == [data[i:i+size] for i in range(0, len(data), size)]
    results.append(batches)

for _ in range(200):
    now = 0.0
    sent = []
    with TimedBatcher(sent.append, max_items=7, max_seconds=3.0,
                      background=False, clock=lambda: now) as batcher:
        for value in range(50):
            now += rng.random()
            batcher.add(value)
            batcher.poll()
    assert [x for batch in sent for x in batch] == list(range(50))
    results.append(sent)

async def main():
    sent = []
    async def sink(batch):
        sent.append(batch)
    async with AsyncTimedBatcher(sink, max_items=3, max_seconds=math.inf) as batcher:
        for value in range(20):
            await batcher.add(value)
    assert [x for batch in sent for x in batch] == list(range(20))
    results.append(sent)

asyncio.run(main())
saved = Path('failure.pickle')
if saved.exists():
    failure = pickle.loads(saved.read_bytes())
    assert failure.batches == [['pending']]
    assert isinstance(failure.errors[0], ValueError)
else:
    saved.write_bytes(pickle.dumps(FlushError([['pending']], [ValueError('down')])))
print(json.dumps(results))
"""
