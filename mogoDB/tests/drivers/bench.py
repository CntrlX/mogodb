"""Tiny throughput probe: python tests/drivers/bench.py <port>. Prints ops/sec per phase."""

import sys
import time

import pymongo

port = int(sys.argv[1])
c = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}/")
coll = c.bench.docs
coll.drop()
N = 100_000


def timed(label, fn, n):
    t = time.perf_counter()
    fn()
    dt = time.perf_counter() - t
    print(f"{label:<28} {n / dt:>10.0f} ops/s  ({dt:.2f}s)")


timed("insert_many (1k batches)", lambda: [coll.insert_many([{"i": i, "g": i % 100, "s": f"user{i}", "tags": ["a", "b"]} for i in range(b, b + 1000)]) for b in range(0, N, 1000)], N)
timed("create_index i", lambda: coll.create_index("i"), 1)
timed("find_one by index x5k", lambda: [coll.find_one({"i": i * 17 % N}) for i in range(5000)], 5000)
timed("full scan filter x10", lambda: [list(coll.find({"g": 7})) for _ in range(10)], 10)
timed("aggregate group x10", lambda: [list(coll.aggregate([{"$group": {"_id": "$g", "n": {"$sum": 1}}}])) for _ in range(10)], 10)
timed("update_one by index x5k", lambda: [coll.update_one({"i": i}, {"$inc": {"c": 1}}) for i in range(5000)], 5000)
timed("insert_one x5k", lambda: [coll.insert_one({"x": i}) for i in range(5000)], 5000)
