# mogo

Client tools for **mogoDB**, plus the wire-protocol client they use.

| Crate | Binary | What it is |
|---|---|---|
| `compass` | `mogocompass` | MongoDB Compass clone (egui/eframe GUI) |
| `mogo-client` | — | MongoDB wire-protocol client (OP_MSG, SCRAM-SHA-1/256, cursors, change streams) |

The GUI talks to a real `mongod` (verified against 9.0.4) over `mogo-client`;
it does not embed the engine.

## Build

```sh
cargo build -p mogocompass
cargo build --release
```

## mogocompass

```sh
cargo run -p mogocompass
```

egui/eframe GUI with a servers → databases → collections sidebar, Documents tab
(filter/sort/projection, JSON + table views, insert/edit), Aggregation, Schema, Indexes,
Explain and Performance tabs. Saved connections live in
`~/.config/mogocompass/connections.json` (no passwords on disk).

Headless drive mode for testing:

```sh
MOGO_COMPASS_DRIVE=mogodb://127.0.0.1:27017 target/debug/mogocompass
```

> **Linux note:** `eframe` must be built with features `["glow","default_fonts","x11","wayland"]`
> and the workspace patches `winit` to `vendor/winit` (build script forces the `x11_platform` /
> `wayland_platform` cfgs). Without this the GUI panics at startup on Linux.

## Tests

```sh
cargo test --workspace --lib           # unit tests (mogo-client, mogod)
```

Live `mogo-client` tests need `MOGO_TEST_URI`, `MOGO_TEST_RS_URI` (replica set) and
`MOGO_TEST_AUTH_URI` (auth-enabled server).
