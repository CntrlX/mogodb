Building a drop-in, feature-complete MongoDB clone in Rust with a minimal memory and binary footprint requires replacing MongoDB’s heavyweight C++ components—WiredTiger’s multi-gigabyte cache, full BSON AST deserialization, and the embedded SpiderMonkey JavaScript engine—with **zero-copy BSON byte-slicing**, an **embedded pure-Rust MVCC storage engine**, and a **stream-based execution pipeline**.

While existing Rust embedded document databases like PoloDB offer partial wire protocol compatibility, they rely on C++ `librocksdb-sys` bindings and lack full distributed and aggregation parity. A pure-Rust architecture can idle at under 15 MB of RAM, compile to a single static binary under 20 MB, and still talk natively to official MongoDB drivers (`mongodb`, `mongoose`, `pymongo`, `mongosh`).

---

## 1. Core Architecture & Zero-Bloat Crate Stack

| Subsystem | Standard `mongod` (C++) | Lightweight Rust Spec | Why It Cuts Overhead |
| --- | --- | --- | --- |
| **Network & Wire Protocol** | Custom ASIO thread-per-connection | `tokio` + `bytes` (or `monoio` for `io_uring`) | Multiplexes thousands of driver connections on a few async worker threads with zero-copy ring buffers. |
| **Document Memory Model** | `BSONObj` / heap-allocated AST | `bson::RawDocument` & `RawDocumentBuf` | Validates length and trailing null byte in $O(1)$; reads fields directly from byte slices without allocating maps or strings. |
| **Storage Engine & MVCC** | WiredTiger (B-Tree + heavy page cache) | `fjall` (Pure-Rust LSM + MVCC) | 100% safe Rust, cross-keyspace serializable transactions, built-in LZ4 compression, and optional blob key-value separation. |
| **Full-Text & Vector Index** | Dedicated `mongot` / Atlas Java process | `tantivy` (BM25) + `instant-distance` (HNSW) | Embeds `$text` and `$vectorSearch` directly in-process using memory-mapped inverted indexes. |
| **Geospatial (`2dsphere`)** | S2 Geometry C++ library | `s2` + `geo` crates | Maps GeoJSON polygons/points to `u64` Hilbert curve cell IDs stored in standard range-searchable index trees. |
| **Replication & Consensus** | Custom Raft-derived oplog protocol | `openraft` | Handles leader election, log compaction, and quorum commits while translating state to MongoDB `hello` and `replSetGetStatus` responses. |
| **Server-Side JS (`$where`)** | SpiderMonkey (`mozjs`, ~40 MB) | `boa_engine` or `rquickjs` (Feature-flagged) | Keeps the core binary tiny; spins up a micro-context (~300 KB) only when a legacy `$accumulator` or `$function` stage is invoked. |

---

## 2. Wire Protocol & Zero-Copy BSON Engine

Modern MongoDB drivers (MongoDB 5.1+) communicate exclusively over TCP port `27017` using the `OP_MSG` opcode (`2013`) and `OP_COMPRESSED` (`2012`), having deprecated all legacy opcodes (`OP_QUERY`, `OP_INSERT`).

### Wire Frame Specification

Every wire frame begins with a 16-byte little-endian `MsgHeader` (`messageLength`, `requestID`, `responseTo`, `opCode`) followed by the `OP_MSG` payload:

* **`flagBits` (`u32`):** Bit `0` (`checksumPresent`) indicates a trailing 4-byte CRC-32C checksum; Bit `1` (`moreToCome`) enables streaming responses without waiting for client ACKs; Bit `16` (`exhaustAllowed`) signals client support for streaming cursor bursts.
* **`Kind 0` (Body Section):** A single `RawDocument` containing the command (`find`, `insert`, `aggregate`, `hello`, `lsid`, `$db`).
* **`Kind 1` (Document Sequence Section):** Used by drivers for high-throughput bulk writes (`documents`, `updates`, `deletes`). Consists of a `u32` section size, a null-terminated C-string identifier (e.g., `"documents\0"`), and back-to-back raw BSON payloads with no array wrapper.
* **`OP_COMPRESSED` (`2012`):** Wraps `OP_MSG` with `originalOpcode`, `uncompressedSize`, and `compressorId` (`1` = Snappy, `2` = Zlib, `3` = Zstd). Negotiated during the initial `hello` / `isMaster` handshake.

### Zero-Copy BSON Inspection

Deserializing every incoming BSON payload into a heap-allocated `bson::Document` (`BTreeMap`) destroys throughput and inflates memory. Instead, keep documents as contiguous byte slices (`RawDocument`):

* Because BSON prefixes strings, subdocuments, and arrays with their byte lengths, scanning for a nested dot-path like `"user.billing.tier"` uses `RawDocument::iter_elements()` to hop across unrelated subtrees by adding their 4-byte length prefix to the cursor offset—zero heap allocations required.

```rust
use bson::raw::{RawDocument, RawElement, RawBsonRef};
use bytes::BytesMut;

pub struct OpMsgFrame<'a> {
    pub request_id: i32,
    pub response_to: i32,
    pub flags: u32,
    pub body: &'a RawDocument,
    pub doc_sequences: Vec<(&'a str, Vec<&'a RawDocument>)>,
}

/// Evaluates a top-level or nested dot-path directly on raw BSON bytes without heap allocation
pub fn resolve_dot_path<'a>(
    doc: &'a RawDocument,
    path: &str,
) -> Result<Option<RawBsonRef<'a>>, bson::raw::Error> {
    let mut segments = path.split('.').peekable();
    let mut current_doc = doc;

    while let Some(segment) = segments.next() {
        let Some(elem) = current_doc.get(segment)? else {
            return Ok(None);
        };
        if segments.peek().is_none() {
            return Ok(Some(elem));
        }
        match elem {
            RawBsonRef::Document(subdoc) => current_doc = subdoc,
            _ => return Ok(None), // Intermediate path is not a subdocument (or requires array traversal)
        }
    }
    Ok(None)
}

```

---

## 3. Storage Keyspace Layout & Secondary Indexes

Using `fjall` as the underlying storage engine provides isolated LSM keyspaces (partitions), atomic cross-keyspace write batches, and point-in-time MVCC snapshots.

### Physical Keyspaces

1. **`_catalog`:** Stores collection definitions, UUIDs, validator schemas (`$jsonSchema`), and index metadata.
2. **`_docs_<coll_id>`:** Stores the primary BSON payloads.
* **Key:** `[TypeTag (1B)][Memcomparable _id Bytes]`
* **Value:** Raw BSON document bytes (with `fjall` key-value blob separation enabled for documents approaching the 16 MB BSON limit).


3. **`_idx_<index_id>`:** Stores secondary indexes (Single-field, Compound, Multikey, Sparse, Partial, TTL, and `2dsphere`).
4. **`_oplog`:** Ring-buffered keyspace keyed by Hybrid Logical Clock (HLC) `Bson::Timestamp(epoch_sec, ordinal)` for replication and Change Streams.

### Type-Bracketed Memcomparable Key Encoding

MongoDB enforces a strict cross-type BSON comparison order (`MinKey` $\rightarrow$ `Null` $\rightarrow$ `Numbers (int32/int64/double/decimal128 unified)` $\rightarrow$ `String` $\rightarrow$ `Object` $\rightarrow$ `Array` $\rightarrow$ `BinData` $\rightarrow$ `ObjectId` $\rightarrow$ `Boolean` $\rightarrow$ `Date` $\rightarrow$ `Timestamp` $\rightarrow$ `RegExp` $\rightarrow$ `MaxKey`). Since LSM trees sort keys lexicographically by raw bytes, index keys must be encoded into a byte-sortable format:

```
+---------------+----------------------+-------------------+--------------------+
| Field1 TypeTag| Field1 Encoded Bytes | Direction (ASC/DSC)| Primary _id Suffix |
|     (1 Byte)  |  (Variable, Escaped) |   (Bitwise XOR)   |  (Tie-breaker)     |
+---------------+----------------------+-------------------+--------------------+

```

* **Unified Numeric Encoding:** Convert `i32`, `i64`, and `f64` into canonical IEEE-754 64-bit floats (with special handling for large `i64` precision edges), flip the sign bit if positive or bitwise-NOT all bits if negative, and write in big-endian order. This guarantees `-100.5` sorts before `-10`, which sorts before `0`, which sorts before `42`.
* **Descending Indexes (`-1`):** Bitwise invert (`^ 0xFF`) the encoded bytes of any descending field so forward LSM prefix/range iterators naturally scan in reverse order.
* **Multikey (Array) Indexes:** When an indexed field resolves to a `RawBsonRef::Array`, emit one index entry per element pointing to the same `_id`. Set a `is_multikey = true` flag on the index metadata so the query executor wraps index scans in a deduplicating `RoaringBitmap` or `AHashSet` filter.
* **TTL Worker:** A background `tokio` task wakes every 60 seconds, reads `_idx_<ttl_id>` using a range scan from `[DateTag][0]` to `[DateTag][now - expire_after_secs]`, and commits a batch delete across `_docs` and `_idx`.

---

## 4. Query Planner, Aggregation & Change Streams

### MQL Query Engine & Cost-Based Planner

1. **Normalization & CNF Conversion:** Parse the incoming filter `RawDocument` into an expression tree, normalizing shorthand queries (`{"a": 1}` $\rightarrow$ `{"a": {"$eq": 1}}`) and pushing `$not` inward via De Morgan's laws.
2. **Index Interval Extraction:** Match filter predicates (`$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$elemMatch`, `$regex` prefix) against active index definitions on the collection to produce byte-range bounds `Vec<(Bound<Bytes>, Bound<Bytes>)>`.
3. **Histogram Selectivity over Plan Racing:** Instead of MongoDB’s heavy multi-plan trial executor (which runs candidate index plans concurrently and caches winners), maintain a lightweight 256-bucket equi-depth histogram per index updated asynchronously during `fjall` LSM compactions. Pick the lowest-cardinality range scan in microseconds.
4. **Covered Queries:** If a query specifies a projection (`{"_id": 0, "email": 1}`) where all requested fields exist in the index key and the index is not multikey, reconstruct the output `RawDocumentBuf` directly from the decoded index key without touching the `_docs` keyspace.

### Pull-Based Streaming Aggregation Pipeline

Implement the aggregation pipeline as a chain of composable async operators implementing a pull-based trait:

```rust
#[async_trait::async_trait]
pub trait PipelineStage: Send {
    async fn next_batch(&mut self) -> Result<Option<Vec<bson::RawDocumentBuf>>, EngineError>;
}

```

* **Optimizer Passes (Before Execution):**
* **Predicate Pushdown:** Hoist `$match` stages ahead of `$project`, `$addFields`, and `$unwind` whenever they don't depend on computed fields, fusing the earliest `$match` + `$sort` + `$limit` directly into the storage index scan.
* **Field Dependency Pruning:** Walk the pipeline backward from the final stage to compute the minimal set of BSON paths required, stripping unused payload bytes at the source scan.


* **Memory-Bounded Blocking Stages (`$group`, `$sort`, `$lookup`, `$facet`):**
* Track per-stage memory allocations via an atomic byte counter.
* Enforce the 100 MB stage limit by default; when `allowDiskUse: true` is set (or by default in lightweight mode at a lower threshold like 16 MB), spill sorted runs or hash partitions to temporary LZ4-compressed files and merge them via a $k$-way min-heap iterator.



### Sessions, ACID Transactions & Change Streams

* **Multi-Document ACID Transactions:**
* Drivers pass `lsid` (Logical Session ID), `txnNumber`, `startTransaction: true`, and `autocommit: false` inside `OP_MSG` command bodies.
* Maintain an in-memory `DashMap<(Uuid, i64), Arc<Mutex<fjall::Transaction>>>`. Every CRUD command with that `lsid` and `txnNumber` executes against the open `fjall` transaction workspace.
* When `commitTransaction` arrives, write all staged mutations plus a single atomic `_oplog` entry and call `.commit()`. If the session times out (`transactionLifetimeLimitSeconds`, default 60s), drop the handle to roll back automatically.


* **Zero-Polling Change Streams (`$changeStream`):**
* Every committed write appends an oplog document (`op: "i" | "u" | "d"`, `ns`, `o`, `o2`, `ts`) to the `_oplog` keyspace and broadcasts the event over a lock-free `tokio::sync::broadcast` channel.
* When a client opens an `aggregate` cursor with `[{"$changeStream": {}}]`, the cursor first replays any historical `_oplog` entries if `resumeAfter` or `startAtOperationTime` is specified, then subscribes directly to the broadcast receiver when a `getMore` command awaits new events (`maxTimeMS`).



---

## 5. Security, Auth & Compatibility Checklist

To ensure existing applications and ORMs (`Prisma`, `Mongoose`, `MongoDB Rust Driver`) connect without errors, your command router must implement these foundational wire commands from day one:

1. **Handshake & Topology Discovery:** `hello`, `isMaster`, `buildInfo`, `getParameter`, `ping`, `whatsmyuri`, and `endSessions`. Return `maxBsonObjectSize: 16777216`, `maxMessageSizeBytes: 48000000`, `maxWriteBatchSize: 100000`, and `minWireVersion: 13` / `maxWireVersion: 21` (MongoDB 7.0+ protocol level).
2. **Authentication:** Implement `saslStart` and `saslContinue` supporting **SCRAM-SHA-256** (using the `sha2`, `hmac`, and `pbkdf2` crates) backed by the `admin.system.users` collection.
3. **Core CRUD & Cursor Lifecycle:** `insert`, `find`, `update`, `delete`, `findAndModify`, `countDocuments` (via `aggregate`), `distinct`, `getMore`, and `killCursors`. Store active cursors in a sharded LRU map with a 10-minute idle eviction timer (`cursorTimeoutMillis`).
4. **DDL & Index Management:** `create`, `drop`, `listCollections`, `listDatabases`, `createIndexes`, `dropIndexes`, and `listIndexes`.
