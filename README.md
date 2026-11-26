<p align="center">
  <img src="assets/logo.svg" alt="mogoDB logo: an isometric wireframe cube filled with a dithered ribbon of light" width="220" height="220">
</p>

<h1 align="center">mogoDB</h1>

<p align="center">
  A lightweight, MongoDB-compatible database server in pure Rust, with a GUI and a client library.
</p>

Official MongoDB drivers, `mongosh`, and ODMs connect to **mogoDB** unmodified. It speaks the
MongoDB wire protocol and reports itself as MongoDB 9.0.4 (wire version 29, FCV 9.0). It is verified
against a real `mongod` 9.0.4.

| | mogod (release) | mongod 9.0.4 |
|---|---|---|
| Binary | 17.1 MB, single file | 306.6 MB |
| Idle RSS | ~8 MiB | ~115 MiB |
| RSS after 100k-doc load | ~104 MiB | ~181 MiB |

## What's in this repository

| Crate | Binary | Description |
|---|---|---|
| [`mogoDB`](mogoDB) | `mogod` | The database server: wire protocol (OP_MSG, OP_COMPRESSED), LSM storage with serializable transactions, cost-based query planner, aggregation, change streams, SCRAM authentication, TTL, geo and text search |
| [`mogo-client`](mogo-client) | n/a | Wire-protocol client library (OP_MSG, SCRAM-SHA-1/256, cursors, change streams) |
| [`compass`](compass) | `mogocompass` | Desktop GUI (egui/eframe) for browsing and querying a server |

## Quick start

```sh
cargo build --release
./target/release/mogod --dbpath ./data/db        # listens on 127.0.0.1:27017
mongosh mongodb://127.0.0.1:27017                # connect with any MongoDB client
```

Useful flags: `--port`, `--bind_ip`, `--auth` (require SCRAM), `--replSet <name>` (single-node
replica set, for clients that need transactions), `--cacheSizeMB`, `--oplogSizeMB`.
See [`mogoDB/README.md`](mogoDB/README.md) for the full option list, architecture, benchmarks,
compatibility testing, and known differences.

### GUI

```sh
cargo run -p mogocompass
```

A servers, databases and collections sidebar with Documents (filter, sort, projection, JSON and
table views, insert and edit), Aggregation, Schema, Indexes, Explain and Performance tabs. Saved
connections are stored in `~/.config/mogocompass/connections.json` (no passwords on disk).

> **Linux note:** `eframe` must be built with features `["glow","default_fonts","x11","wayland"]`,
> and the workspace patches `winit` to `vendor/winit`. Without this the GUI panics at startup.

## Tests

```sh
cargo test --workspace --lib
```

Live `mogo-client` tests need `MOGO_TEST_URI`, `MOGO_TEST_RS_URI` (replica set) and
`MOGO_TEST_AUTH_URI` (auth-enabled server).
