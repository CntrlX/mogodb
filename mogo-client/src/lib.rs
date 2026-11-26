//! `mogo-client` — MongoDB wire-protocol client.
//!
//! Speaks OP_MSG (plus the legacy OP_QUERY handshake), authenticates with SCRAM-SHA-1/256,
//! pools connections, and exposes cursors and change streams.

pub mod client;
pub mod conn;
pub mod error;
pub mod scram;
pub mod uri;
pub mod wire;

pub use client::{
    AggregateOptions, ChangeStream, Client, ClientOptions, Collection, Cursor, Db, FindOptions, InsertResult, PooledConn,
    UpdateResult, WatchOptions,
};
pub use conn::{CursorReply, ServerInfo};
pub use error::{Error, ErrorKind, Result};

/// Convenience: connect and return a client (async).
pub async fn connect(uri: &str) -> Result<Client> {
    Client::connect(uri).await
}

/// Current protocol version this client speaks.
pub const WIRE_VERSION: i32 = 17;
