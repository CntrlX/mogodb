<p align="center">
  <img src="../assets/logo.svg" alt="mogoDB logo" width="160" height="160">
</p>

# mogoDB

A lightweight MongoDB-compatible server in pure Rust (`mogod`). Official drivers, `mongosh`,
and ODMs connect to it unmodified: it speaks the MongoDB wire protocol (OP_MSG, OP_COMPRESSED,
legacy OP_QUERY handshake) and reports itself as MongoDB 9.0.4 (wire version 29, FCV 9.0).

| | mogod (release) | mongod 9.0.4 |
|---|---|---|
| Binary | 17.1 MB, single file (libc only; 8.0 MB without the `js` feature) | 306.6 MB |
| Idle RSS | ~8 MiB | ~115 MiB |
| RSS after 100k-doc load (bench below) | ~104 MiB | ~181 MiB |

## Build & run

```sh
cargo build --release
./target/release/mogod --dbpath ./data/db              # standalone on 127.0.0.1:27017
./target/release/mogod --dbpath ./data/db --replSet rs0  # single-node replica set (PRIMARY)
./target/release/mogod --dbpath ./data/db --auth         # require SCRAM authentication
mongosh mongodb://127.0.0.1:27017
```

Options: `--port`, `--bind_ip a,b`, `--bind_ip_all`, `--dbpath`, `--auth`, `--replSet <name>`,
`--cacheSizeMB` (block cache, default 16), `--oplogSizeMB` (default 64), `-v` (log every command).
`--replSet` makes `hello` report a PRIMARY of a one-member set; use it for clients that require a
replica set (e.g. Prisma transactions). Cargo features (both default): `js` (server-side
JavaScript on the pure-Rust Boa engine) and `zstd` (the only C dependency).
`--no-default-features --features zstd` drops JavaScript; `$where`/`$function`/`$accumulator`/`mapReduce`
then fail with code 31264.

With `--auth` and no users, the localhost exception allows creating the first user on `admin`.

## Architecture

| Spec subsystem | Implementation |
|---|---|
| Network | `tokio` multi-thread runtime (≤4 workers); `src/wire.rs` frames OP_MSG (kind 0 body, kind 1 sequences, CRC flag, `moreToCome`), OP_COMPRESSED (snappy, zlib, zstd), OP_QUERY/OP_REPLY for legacy handshakes. |
| Document model | Documents are stored and scanned as raw BSON. `src/deps.rs` computes the top-level fields a filter/pipeline reads; scans skip all other elements by their length prefix and decode only those fields, fully decoding a document only when it is returned. |
| Storage & MVCC | `fjall` 3 (pure-Rust LSM) with optimistic serializable transactions. Keyspaces: `_catalog`, `_docs_<id>` (RecordId → BSON, so natural order is insertion order), `_idx_<id>`, `_oplog`. 8 MiB memtables. |
| Index keys | `src/keys.rs`: type-bracketed memcomparable encoding in MongoDB's cross-type order; numbers unified (double + exact int64 remainder, so large int64s stay unique), descending fields bit-inverted, multikey fan-out, sparse, partial, unique, compound, TTL. `_id` is a real unique index. |
| Planner | `src/planner.rs`: `$eq`/`$in`/range/anchored-`$regex` bounds on index prefixes (with MongoDB's `$or`→`$in` canonicalization), costed with per-index 256-bucket equi-depth histograms rebuilt in the background after enough writes (key-count probe fallback). Plans can provide the requested sort (forward or backward scans; no blocking sort, lazy so `limit` stops early) and can be covered (index entries store per-field type bits, so documents are rebuilt from keys with exact int/long/double types; `totalDocsExamined: 0`). `explain` reproduces 9.0's stage trees (`EXPRESS_IXSCAN`, `FETCH`/`IXSCAN`, `PROJECTION_SIMPLE`/`DEFAULT`/`COVERED`, `SORT`, `SKIP`, `LIMIT`, `SUBPLAN`, `EOF`), index bounds strings, residual filters, rejected plans and `executionStats` counters. |
| Collation | `src/collation.rs`: ICU-style UCA (strength 1–5, `caseLevel`, `caseFirst`, `numericOrdering`, `alternate`/`maxVariable`, `backwards`, `normalization`) for find/aggregate/update/delete/indexes/`$group` keys/views; collection defaults, per-index collations, collated index keys; an index serves string bounds only when its collation matches the query's. |
| Time series | `src/timeseries.rs`: 9.0 single-namespace collections (`timeField`/`metaField`/granularity/custom bucket spans, normalized options, default `{meta, time}` index, no `_id` index, meta-only multi updates, index restrictions, `collMod` granularity transitions, collection-level TTL). Measurements are stored as ordinary documents. |
| Server-side JS | `src/js.rs`: `$where`, `$function`, `$accumulator`, `mapReduce` (inline/replace/merge/reduce outputs) on Boa, with BSON↔JS type mapping, UTC local dates (as 9.0's WASM engine) and runaway-script limits. |
| Search | `src/search.rs`: `$search`/`$searchMeta` (text, phrase, autocomplete, wildcard/regex, compound, equals, range, exists, in, near, facets, BM25 scoring, highlights-free), `$listSearchIndexes`, search-index commands, `$rankFusion` (reciprocal-rank fusion, weights, `scoreDetails`, views). |
| Query/update | `src/query/matcher.rs` (all MQL operators incl. `$expr`, `$jsonSchema`, bitwise, geo, `$text`), `src/update.rs` (all update operators, positional `$`, `$[]`, `$[id]` + `arrayFilters`, pipeline updates, upsert seeding). |
| Aggregation | Pull-based iterator pipeline (`src/agg/pipeline.rs`): 40+ stages incl. `$lookup` (both forms), `$graphLookup`, `$facet`, `$unionWith`, `$setWindowFields`, `$densify`, `$fill`, `$bucketAuto`, `$merge`, `$out`, `$geoNear`, `$vectorSearch`, `$documents`. Leading `$match` is pushed into the planner; `$sort`+`$limit` fuse into top-k; `$sort` spills LZ4 runs to disk with a k-way merge under `allowDiskUse`; 100 MiB per-stage limit (error 292). ~150 expression operators (`src/agg/expr*.rs`). Decimal128 add/sub/mul are exact. |
| Transactions | Sessions keyed by `lsid`; each multi-document transaction holds a fjall write transaction. First writer wins via a document lock table (immediate `WriteConflict` + `TransientTransactionError`); 60 s lifetime; retryable-write de-duplication by `txnNumber`. |
| Change streams | Every commit appends oplog entries in the same transaction and broadcasts them (`tokio::sync::broadcast`). Streams replay from the oplog for `resumeAfter`/`startAfter`/`startAtOperationTime`, then await events in `getMore` (`maxTimeMS`). Collection/database/cluster scope, `updateLookup`, pre-images when `changeStreamPreAndPostImages` is enabled, `drop`/`rename`/`invalidate`. |
| Auth | SCRAM-SHA-256 and SCRAM-SHA-1 (`src/auth.rs`), speculative authentication, `saslSupportedMechs`, users/roles in `admin.system.users` / `admin.system.roles`, built-in role authorization. |
| Text / vector / geo | `$text` with stemming tokenizer and weighted scoring over text-indexed fields; `$vectorSearch` brute-force cosine; GeoJSON + legacy `$near`, `$geoWithin`, `$geoIntersects`, `$geoNear` (haversine / planar). |
| Time zones | `src/tz.rs` reads the system zoneinfo database (TZif v1–v3 plus POSIX footer rules) for Olson names; date operators resolve the offset per instant, and calendar arithmetic (`$dateAdd`, `$dateTrunc`, `$dateFromParts`) works in local wall-clock time across DST. |
| Background | TTL monitor (60 s), cursor idle eviction (`cursorTimeoutMillis`, 10 min), session/transaction reaping, oplog ring-buffer trimming. |

Commands: handshake (`hello`/`isMaster`, `buildInfo`, `ping`, `whatsmyuri`, `getParameter`, `getCmdLineOpts`, `getLog`, …), CRUD
(`insert`, `find`, `update` incl. `sort`, `delete`, `findAndModify`, `bulkWrite`, `count`, `distinct`, `aggregate`, `mapReduce`, `getMore`,
`killCursors`, `explain`), query settings (`setQuerySettings`, `removeQuerySettings`, `$querySettings`), search indexes
(`createSearchIndexes`, `updateSearchIndex`, `dropSearchIndex`),
DDL (`create` incl. views/capped/time series/validators with the 9.0 `constraint` level, `drop`, `dropDatabase`, `renameCollection`, `collMod`, `createIndexes`,
`dropIndexes`, `listIndexes`, `listCollections`, `listDatabases`), transactions, sessions, users/roles, and diagnostics
(`serverStatus`, `dbStats`, `collStats`, `validate`, `compact`, `autoCompact`, `currentOp`, `killOp`, `fsync`,
`replSetGetStatus`, …).

## Compatibility testing

`tests/compat/run.sh` runs each `tests/compat/t_*.js` mongosh script against a real `mongod`
(`REF_PORT`, default 27018) and `mogod` (`TEST_PORT`, default 27017) and diffs the canonical
Extended JSON output (so int32/int64/double/decimal types and error codes are compared exactly).
Current status: 17 files, 2,168 cases, all identical against MongoDB 9.0.4 (CRUD, query and update
operators, aggregation stages and ~200 expressions incl. the 8.x/9.0 additions, `$convert`
matrix, collation, planner/explain, time series, server-side JS, `$rankFusion`, `bulkWrite`,
query settings, 9.0 behaviour changes, DDL, validation levels, transactions, change streams).

`tests/drivers/pymongo_smoke.py <port>` exercises pymongo 4.19 (snappy/zstd/zlib compression,
GridFS, `with_transaction`, change streams, retryable writes, error mapping);
`tests/drivers/mongoose_smoke.js <port>` exercises Mongoose 8 (schema indexes, unique/validation
errors, populate, Decimal128, timestamps, aggregation, `withTransaction`);
`tests/drivers/bench.py <port>` is a small throughput probe.

Unit tests: `cargo test`.

## Not implemented / known differences

- Replication and sharding: no Raft or secondaries; `--replSet` emulates a single PRIMARY.
- `$search`/`$vectorSearch`/search-index commands run in-process; a self-managed mongod 9.0
  without mongot answers them with 31082 `SearchNotEnabled`. `$vectorSearch` is exact brute
  force, not ANN; `$search` scoring is BM25 over a per-query in-memory index (scores differ
  from Lucene's).
- Time series buckets are not materialized: storage is row-oriented, `collStats.timeseries`
  bucket counters are synthetic, and read-back field order is insertion order (mongod's is
  hash-based and changes across restarts).
- Explain: memory counters (`totalDataSizeSorted`, `peakTrackedMemBytes`) are approximations;
  plan hashes are deterministic but not MongoDB's; no SBE `slotBasedPlan`; no `$or` index union.
- Server-side JS error messages come from Boa, not MongoDB's engine (codes match).
- Schema-validation failures report `errInfo.details: {operatorName: "$jsonSchema"}` instead of
  MongoDB's per-operator explanation.
- Write replies on `--replSet` omit `opTime`/`electionId`.
- Geo queries scan (no S2 cell index); `$text` scans with tokenization (no persistent
  inverted index); `$text` scores and `$sample` order differ from MongoDB's.
- Decimal128 division and transcendental functions go through f64.
- Regular expressions use Rust `regex` syntax (no backreferences/lookaround).
- Legacy OP_QUERY is only supported for `$cmd` handshakes (drivers older than wire version 13 are refused).
