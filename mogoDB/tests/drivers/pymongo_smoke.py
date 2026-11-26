"""pymongo smoke test: python tests/drivers/pymongo_smoke.py [port]. Exits non-zero on failure."""

import datetime
import sys
import threading
import time

import gridfs
import pymongo
from bson import Decimal128, ObjectId
from pymongo import ReturnDocument, UpdateOne, InsertOne, DeleteMany
from pymongo.errors import DuplicateKeyError, OperationFailure

port = int(sys.argv[1]) if len(sys.argv) > 1 else 27017
checks = 0


def check(cond, what):
    global checks
    checks += 1
    if not cond:
        print(f"FAIL: {what}")
        sys.exit(1)


for comp in ("snappy", "zstd", "zlib"):
    c = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}/?compressors={comp}", serverSelectionTimeoutMS=3000)
    coll = c.py_smoke[f"comp_{comp}"]
    coll.drop()
    coll.insert_one({"_id": 1, "blob": "z" * 100_000})
    check(len(coll.find_one({"_id": 1})["blob"]) == 100_000, f"{comp} round trip")
    c.close()

client = pymongo.MongoClient(f"mongodb://127.0.0.1:{port}/", serverSelectionTimeoutMS=3000)
db = client.py_smoke
check(client.server_info()["version"].startswith("9."), "server version")

# CRUD + types
c = db.items
c.drop()
r = c.insert_many([{"n": i, "d": Decimal128("1.10"), "t": datetime.datetime(2024, 1, i + 1)} for i in range(10)])
check(len(r.inserted_ids) == 10 and isinstance(r.inserted_ids[0], ObjectId), "insert_many ids")
check(c.count_documents({"n": {"$gte": 5}}) == 5, "count_documents")
doc = c.find_one({"n": 3})
check(doc["d"] == Decimal128("1.10") and doc["t"] == datetime.datetime(2024, 1, 4), "types round trip")
check([d["n"] for d in c.find({}, {"n": 1}).sort("n", -1).limit(3)] == [9, 8, 7], "sort/limit")
u = c.update_many({"n": {"$lt": 3}}, {"$inc": {"n": 100}})
check(u.matched_count == 3 and u.modified_count == 3, "update_many")
f = c.find_one_and_update({"n": 102}, {"$set": {"x": 1}}, return_document=ReturnDocument.AFTER)
check(f["x"] == 1, "find_one_and_update")
up = c.update_one({"n": 999}, {"$set": {"y": 2}}, upsert=True)
check(up.upserted_id is not None, "upsert")
check(c.delete_many({"n": {"$gte": 100}}).deleted_count == 4, "delete_many")
c.create_index("n", unique=True)
try:
    c.insert_one({"n": 5})
    check(False, "dup key should raise")
except DuplicateKeyError as e:
    check(e.code == 11000 and e.details.get("keyValue") == {"n": 5}, "DuplicateKeyError details")
try:
    c.find_one({"n": {"$bogus": 1}})
    check(False, "bad operator should raise")
except OperationFailure as e:
    check(e.code == 2, "OperationFailure code")

# bulk
res = c.bulk_write([InsertOne({"n": 50}), UpdateOne({"n": 50}, {"$set": {"b": 1}}), DeleteMany({"n": {"$lt": 4}})], ordered=True)
check(res.inserted_count == 1 and res.modified_count == 1 and res.deleted_count == 1, f"bulk_write {res.bulk_api_result}")

# cursors across batches
big = db.big
big.drop()
big.insert_many([{"i": i} for i in range(5000)])
check(sum(1 for _ in big.find({}, batch_size=100)) == 5000, "batched cursor")
check(list(big.aggregate([{"$group": {"_id": None, "s": {"$sum": "$i"}}}]))[0]["s"] == sum(range(5000)), "aggregate sum")
check(big.estimated_document_count() == 5000, "estimated count")
check(sorted(big.distinct("i", {"i": {"$lt": 3}})) == [0, 1, 2], "distinct")

# GridFS (exercises indexes, sorting, chunks)
fs = gridfs.GridFS(db)
payload = b"0123456789" * 50_000
fid = fs.put(payload, filename="blob.bin")
check(fs.get(fid).read() == payload, "gridfs round trip")
check(fs.find_one({"filename": "blob.bin"}).length == len(payload), "gridfs metadata")

# transactions
acct = db.acct
acct.drop()
acct.insert_many([{"_id": 1, "bal": 100}, {"_id": 2, "bal": 0}])


def transfer(session):
    acct.update_one({"_id": 1}, {"$inc": {"bal": -40}}, session=session)
    acct.update_one({"_id": 2}, {"$inc": {"bal": 40}}, session=session)


with client.start_session() as s:
    s.with_transaction(transfer)
check([d["bal"] for d in acct.find().sort("_id")] == [60, 40], "with_transaction commit")
with client.start_session() as s:
    s.start_transaction()
    acct.insert_one({"_id": 3}, session=s)
    s.abort_transaction()
check(acct.count_documents({"_id": 3}) == 0, "abort")

# change streams (background writer)
events = []
with acct.watch(full_document="updateLookup", max_await_time_ms=500) as stream:
    def writer():
        time.sleep(0.3)
        acct.insert_one({"_id": 10, "bal": 1})
        acct.update_one({"_id": 10}, {"$inc": {"bal": 1}})
    threading.Thread(target=writer).start()
    deadline = time.time() + 10
    while len(events) < 2 and time.time() < deadline:
        ev = stream.try_next()
        if ev:
            events.append(ev)
check([e["operationType"] for e in events] == ["insert", "update"], f"change stream {events}")
check(events[1]["fullDocument"]["bal"] == 2, "updateLookup")

# retryable writes on by default: ensure no errors with txnNumber
db.retry.insert_one({"a": 1})
check(db.retry.count_documents({}) == 1, "retryable insert")

# listCollections / indexes / drop
check("items" in db.list_collection_names(), "list_collection_names")
check(any(ix["name"] == "n_1" for ix in c.list_indexes()), "list_indexes")
client.drop_database("py_smoke")
check("py_smoke" not in client.list_database_names(), "drop_database")
print(f"pymongo {pymongo.version}: all {checks} checks passed")
