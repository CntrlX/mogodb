//! Server/collection data model and the request channel to the async worker.

use bson::Document;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Instant;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
pub fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// A saved or connected server. Passwords are kept in memory only (never written to disk).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerInfo {
    pub name: String,
    pub uri: String,
    #[serde(skip)]
    pub password: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct DatabaseInfo {
    pub name: String,
    pub collections: Vec<CollectionInfo>,
    pub size_on_disk: i64,
    pub empty: bool,
}

#[derive(Clone, Debug, Default)]
pub struct CollectionInfo {
    pub name: String,
    pub kind: CollectionKind,
    pub document_count: i64,
    pub storage_size: i64,
    pub indexes: Vec<IndexInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CollectionKind {
    Collection,
    View,
    TimeSeries,
}

impl Default for CollectionKind {
    fn default() -> Self {
        CollectionKind::Collection
    }
}

#[derive(Clone, Debug, Default)]
pub struct IndexInfo {
    pub name: String,
    pub key: Document,
    pub unique: bool,
    pub sparse: bool,
    pub collation: Option<Document>,
}

/// One unit of work for the worker thread.
pub enum Cmd {
    Connect { server: usize, uri: String, password: Option<String> },
    Disconnect(usize),
    ListDatabases(usize),
    ListCollections(usize, String),
    CollectionMeta(usize, String, String),
    RunQuery(usize, QueryRequest),
    CountDocuments(usize, QueryRequest),
    Distinct(usize, QueryRequest, String),
    Explain(usize, QueryRequest),
    Insert(usize, QueryRequest, Vec<Document>),
    UpdateMany(usize, QueryRequest, Document),
    DeleteMany(usize, QueryRequest),
    DropCollection(usize, QueryRequest),
    CreateCollection(usize, String, String, Document),
    DropDatabase(usize, String),
    CreateIndex(usize, QueryRequest, Document, Document),
    DropIndex(usize, QueryRequest, String),
    ServerStatus(usize),
    CurrentOps(usize),
    KillOp(usize, i64),
    Command(usize, String, Document),
}

#[derive(Clone, Debug)]
pub struct QueryRequest {
    pub db: String,
    pub coll: String,
    pub filter: Document,
    pub sort: Option<Document>,
    pub projection: Option<Document>,
    pub skip: i64,
    pub limit: i64,
    pub collation: Option<Document>,
}

impl QueryRequest {
    pub fn new(db: &str, coll: &str) -> QueryRequest {
        QueryRequest { db: db.to_string(), coll: coll.to_string(), filter: Document::new(), sort: None, projection: None, skip: 0, limit: 50, collation: None }
    }
}

/// Result of one command; `id` ties it back to the issuing tab.
#[derive(Debug)]
pub struct Response {
    pub id: u64,
    pub started: Instant,
    pub result: Result<Reply, String>,
}

#[derive(Debug)]
pub enum Reply {
    Unit,
    Docs(Vec<Document>),
    Databases(Vec<DatabaseInfo>),
    Collections(String, Vec<CollectionInfo>),
    Collection(CollectionInfo),
    Count(u64),
    Values(Vec<bson::Bson>),
    Explain(Document),
    Status(Document),
    Ops(Vec<Document>),
    ServerInfo(Box<ServerInfo>),
}

/// The request channel plus a monotonically increasing id for correlation.
#[derive(Clone)]
pub struct Backend {
    tx: Sender<(u64, Cmd)>,
    resp: Arc<Mutex<Option<Sender<Response>>>>,
}

impl Backend {
    pub fn new() -> (Backend, Receiver<(u64, Cmd)>, Receiver<Response>) {
        let (cmd_tx, cmd_rx) = channel();
        let (resp_tx, resp_rx) = channel();
        let backend = Backend { tx: cmd_tx, resp: Arc::new(Mutex::new(Some(resp_tx))) };
        (backend, cmd_rx, resp_rx)
    }

    /// The sender given to the worker, taken once.
    pub fn take_sender(&self) -> Option<Sender<Response>> {
        self.resp.lock().ok()?.take()
    }

    pub fn send(&self, cmd: Cmd) -> Option<u64> {
        let id = next_id();
        self.tx.send((id, cmd)).ok()?;
        Some(id)
    }
}

pub fn reply_docs(r: &Reply) -> Option<&Vec<Document>> {
    match r {
        Reply::Docs(d) => Some(d),
        _ => None,
    }
}

pub type Shared<T> = Arc<T>;