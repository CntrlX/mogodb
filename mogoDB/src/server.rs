//! TCP server, connection state, command routing, handshake commands.

use crate::bsonutil;
use crate::cursor::CursorMgr;
use crate::error::{Error, Result};
use crate::session::{self, Session, SessionMgr, TxnArgs};
use crate::storage::{Store, Txn};
use crate::wire::{self, Compressor, Op};
use bson::{Bson, Document, doc, oid::ObjectId};
use fjall::Snapshot;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

pub const VERSION: &str = "9.0.4";
pub const MIN_WIRE: i32 = 0;
pub const MAX_WIRE: i32 = 29;

#[derive(Clone, Debug)]
pub struct Config {
    pub port: u16,
    pub bind_ip: Vec<String>,
    pub dbpath: std::path::PathBuf,
    pub auth: bool,
    pub repl_set: Option<String>,
    pub cache_mb: u64,
    pub oplog_mb: u64,
    pub verbose: bool,
    pub argv: Vec<String>,
}

#[derive(Default)]
pub struct Counters {
    pub insert: AtomicI64,
    pub query: AtomicI64,
    pub update: AtomicI64,
    pub delete: AtomicI64,
    pub getmore: AtomicI64,
    pub command: AtomicI64,
    pub bytes_in: AtomicI64,
    pub bytes_out: AtomicI64,
    pub requests: AtomicI64,
    pub conns_current: AtomicI64,
    pub conns_total: AtomicI64,
}

/// An operation in progress (for `currentOp` / `killOp`).
pub struct OpInfo {
    pub opid: i64,
    pub conn_id: i64,
    pub ns: String,
    pub command: Document,
    pub started: Instant,
    pub client: String,
    pub killed: Arc<std::sync::atomic::AtomicBool>,
}

pub struct Server {
    pub store: Arc<Store>,
    pub cursors: CursorMgr,
    pub sessions: SessionMgr,
    pub cfg: Config,
    pub started: Instant,
    pub process_id: ObjectId,
    pub election_id: ObjectId,
    pub counters: Counters,
    pub ops: Mutex<Vec<OpInfo>>,
    next_conn: AtomicI64,
    next_op: AtomicI64,
    pub parameters: Mutex<Document>,
    pub log: Mutex<std::collections::VecDeque<String>>,
    pub shutdown: tokio::sync::Notify,
    pub fsync_locked: AtomicU64,
}

/// Authenticated identity on a connection.
#[derive(Clone, Debug)]
pub struct AuthUser {
    pub user: String,
    pub db: String,
    /// (role, db)
    pub roles: Vec<(String, String)>,
}

pub struct Conn {
    pub id: i64,
    pub peer: String,
    pub local: String,
    pub users: Vec<AuthUser>,
    pub sasl: std::collections::HashMap<i32, crate::auth::ScramConversation>,
    pub next_conv: i32,
    pub compressors: Vec<Compressor>,
    pub client_meta: Option<Document>,
    pub last_error: Option<Document>,
    /// Conversations that requested `skipEmptyExchange`.
    pub skip_empty: std::collections::HashSet<i32>,
}

impl Conn {
    pub fn is_localhost(&self) -> bool {
        self.peer.starts_with("127.") || self.peer.starts_with("[::1]") || self.peer.starts_with("localhost")
    }
}

/// Source of reads for a command: a fresh snapshot or the session's open transaction.
pub enum ReadSrc<'a> {
    Snap(Snapshot),
    Txn(&'a mut Txn),
}
/// Dispatch a generic read over either source (`Readable` is not object safe).
#[macro_export]
macro_rules! with_reader {
    ($src:expr, |$r:ident| $body:expr) => {
        match $src {
            $crate::server::ReadSrc::Snap(s) => {
                let $r = &s;
                $body
            }
            $crate::server::ReadSrc::Txn(t) => {
                let $r = &t.tx;
                $body
            }
        }
    };
}

/// Per-command execution context handed to command implementations.
pub struct CmdCtx<'a> {
    pub srv: &'a Arc<Server>,
    pub conn: &'a mut Conn,
    pub db: String,
    pub name: String,
    pub cmd: Document,
    pub txn: TxnArgs,
    pub session: Option<Arc<Mutex<Session>>>,
    guard: Option<MutexGuard<'a, Session>>,
    pub opid: i64,
    pub killed: Arc<std::sync::atomic::AtomicBool>,
}

impl<'a> CmdCtx<'a> {
    pub fn store(&self) -> &Arc<Store> {
        &self.srv.store
    }

    pub fn in_txn(&self) -> bool {
        self.txn.in_txn()
    }

    fn sess(&mut self) -> Option<&mut Session> {
        self.guard.as_deref_mut()
    }

    /// The open session transaction (only inside multi-document transactions).
    pub fn session_txn(&mut self) -> Option<&mut Txn> {
        if !self.txn.in_txn() {
            return None;
        }
        self.sess()?.txn.as_mut()?.txn.as_mut()
    }

    /// Run `f` on the command's logical session (error if the command has no `lsid`).
    pub fn with_session<T>(&mut self, f: impl FnOnce(&mut Session) -> Result<T>) -> Result<T> {
        let s = self.sess().ok_or_else(|| Error::bad_value("this command requires a logical session (lsid)"))?;
        f(s)
    }

    /// Read source for this command.
    pub fn reader(&mut self) -> ReadSrc<'_> {
        if self.txn.in_txn() {
            let store = self.srv.store.clone();
            if let Some(t) = self.session_txn() {
                return ReadSrc::Txn(t);
            }
            return ReadSrc::Snap(store.snapshot());
        }
        ReadSrc::Snap(self.srv.store.snapshot())
    }

    /// Run `f` inside the session transaction, or in its own retried auto-commit transaction.
    pub fn write<T>(&mut self, mut f: impl FnMut(&Store, &mut Txn) -> Result<T>) -> Result<T> {
        let store = self.srv.store.clone();
        if self.txn.in_txn() {
            let t = self.session_txn().ok_or_else(|| session::no_such_txn(0))?;
            return f(&store, t);
        }
        let lsid = self.txn.lsid.clone();
        let tn = self.txn.txn_number;
        store.with_retry(|txn| {
            txn.lsid = lsid.clone();
            txn.txn_number = tn;
            f(&store, txn)
        })
    }

    pub fn check_killed(&self) -> Result<()> {
        if self.killed.load(Ordering::Relaxed) {
            return Err(Error::new(11601, "operation was interrupted"));
        }
        Ok(())
    }

    /// Collection name from the command's first field.
    pub fn coll_name(&self) -> Result<String> {
        match self.cmd.get(&self.name) {
            Some(Bson::String(s)) => Ok(s.clone()),
            Some(Bson::Binary(b)) if b.subtype == bson::spec::BinarySubtype::Uuid => self
                .srv
                .store
                .list_colls(Some(&self.db))
                .into_iter()
                .find(|c| c.uuid.bytes == b.bytes)
                .map(|c| c.name.clone())
                .ok_or_else(|| Error::ns_not_found("UUID not found")),
            _ => Err(Error::new(73, format!("collection name has invalid type {}", self.cmd.get(&self.name).map(bsonutil::type_name).unwrap_or("missing")))),
        }
    }

    pub fn ns(&self, coll: &str) -> String {
        format!("{}.{coll}", self.db)
    }
}

impl Server {
    pub fn new(store: Arc<Store>, cfg: Config) -> Arc<Server> {
        Arc::new(Server {
            store,
            cursors: CursorMgr::default(),
            sessions: SessionMgr::default(),
            cfg,
            started: Instant::now(),
            process_id: ObjectId::new(),
            election_id: ObjectId::parse_str("7fffffff0000000000000001").expect("oid"),
            counters: Counters::default(),
            ops: Mutex::new(Vec::new()),
            next_conn: AtomicI64::new(1),
            next_op: AtomicI64::new(1),
            parameters: Mutex::new(doc! {
                "featureCompatibilityVersion": {"version": "9.0"},
                "transactionLifetimeLimitSeconds": 60,
                "cursorTimeoutMillis": 600000_i64,
                "internalQueryMaxBlockingSortMemoryUsageBytes": 104857600_i64,
                "logLevel": 0,
                "authenticationMechanisms": ["SCRAM-SHA-1", "SCRAM-SHA-256"],
                "maxTransactionLockRequestTimeoutMillis": 5,
                "maxConcurrentMultiDocumentTransactions": 10000,
                "internalQueryMaxMemoryUsageBytesPerOperation": crate::server::per_op_memory_limit(),
                "internalQueryMaxSingleExpressionMemoryUsageBytes": 104857600_i64,
                "internalQueryStatsSampleRate": 0.01,
                "internalQueryStatsWriteCmdSampleRate": 0.01,
                "upsertMaxRetryAttemptsOnDuplicateKeyError": 100,
                "enableAutoCompaction": false,
                "notablescan": false,
                "quiet": false,
            }),
            log: Mutex::new(std::collections::VecDeque::new()),
            shutdown: tokio::sync::Notify::new(),
            fsync_locked: AtomicU64::new(0),
        })
    }

    pub fn log(&self, msg: impl Into<String>) {
        let line = format!("{} {}", bson::DateTime::now().try_to_rfc3339_string().unwrap_or_default(), msg.into());
        eprintln!("{line}");
        let mut l = self.log.lock().expect("log");
        if l.len() >= 1024 {
            l.pop_front();
        }
        l.push_back(line);
    }

    pub fn host(&self) -> String {
        let h = self.cfg.bind_ip.first().map(String::as_str).unwrap_or("127.0.0.1");
        let h = if h == "0.0.0.0" || h == "::" { "localhost" } else { h };
        format!("{h}:{}", self.cfg.port)
    }
}

// ============================ networking ============================

pub async fn run(srv: Arc<Server>) -> std::io::Result<()> {
    let mut listeners = Vec::new();
    for ip in &srv.cfg.bind_ip {
        let addr = if ip.contains(':') { format!("[{ip}]:{}", srv.cfg.port) } else { format!("{ip}:{}", srv.cfg.port) };
        let l = TcpListener::bind(&addr).await?;
        srv.log(format!("Waiting for connections on {addr}"));
        listeners.push(l);
    }
    for l in listeners {
        let srv = srv.clone();
        tokio::spawn(async move {
            loop {
                match l.accept().await {
                    Ok((s, peer)) => {
                        let _ = s.set_nodelay(true);
                        let srv = srv.clone();
                        tokio::spawn(async move {
                            let local = s.local_addr().map(|a| a.to_string()).unwrap_or_default();
                            handle_conn(srv, s, peer.to_string(), local).await;
                        });
                    }
                    Err(e) => {
                        srv.log(format!("accept error: {e}"));
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });
    }
    srv.shutdown.notified().await;
    Ok(())
}

async fn handle_conn(srv: Arc<Server>, mut s: TcpStream, peer: String, local: String) {
    let id = srv.next_conn.fetch_add(1, Ordering::Relaxed);
    srv.counters.conns_current.fetch_add(1, Ordering::Relaxed);
    srv.counters.conns_total.fetch_add(1, Ordering::Relaxed);
    if srv.cfg.verbose {
        srv.log(format!("connection accepted from {peer} #{id}"));
    }
    let mut conn = Conn {
        id,
        peer,
        local,
        users: Vec::new(),
        sasl: Default::default(),
        next_conv: 1,
        compressors: Vec::new(),
        client_meta: None,
        last_error: None,
        skip_empty: Default::default(),
    };
    let (mut rd, mut wr) = s.split();
    loop {
        let frame = match wire::read_frame(&mut rd).await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                if srv.cfg.verbose {
                    srv.log(format!("conn{id} read error: {e}"));
                }
                break;
            }
        };
        srv.counters.bytes_in.fetch_add(frame.len() as i64, Ordering::Relaxed);
        srv.counters.requests.fetch_add(1, Ordering::Relaxed);
        let req = match wire::parse(&frame) {
            Ok(r) => r,
            Err(e) => {
                srv.log(format!("conn{id} bad message: {e}"));
                break;
            }
        };
        let rid = (rand::random::<u32>() >> 1) as i32;
        let (out, more_to_come) = match req.op {
            Op::Msg(m) => {
                let more = m.flags & wire::FLAG_MORE_TO_COME != 0;
                let mut body = m.body;
                for (k, docs) in m.sequences {
                    body.insert(k, docs.into_iter().map(Bson::Document).collect::<Vec<_>>());
                }
                let reply = dispatch(&srv, &mut conn, body).await;
                (wire::encode_msg(rid, req.request_id, 0, &reply, req.compressor), more)
            }
            Op::Query(q) => {
                let (db, coll) = q.collection.split_once('.').unwrap_or((&q.collection, ""));
                let mut body = q.query;
                // `{$query: {...}}` wrapping used by some legacy clients.
                if let Ok(inner) = body.get_document("$query") {
                    body = inner.clone();
                }
                let reply = if coll != "$cmd" {
                    Error::new(352, "Unsupported OP_QUERY command: legacy find is not supported").to_doc()
                } else {
                    body.insert("$db", db);
                    dispatch(&srv, &mut conn, body).await
                };
                (wire::encode_reply(rid, req.request_id, &reply, req.compressor), false)
            }
        };
        if more_to_come {
            continue; // fire-and-forget (w:0): no reply
        }
        srv.counters.bytes_out.fetch_add(out.len() as i64, Ordering::Relaxed);
        if wr.write_all(&out).await.is_err() {
            break;
        }
    }
    srv.counters.conns_current.fetch_sub(1, Ordering::Relaxed);
    if srv.cfg.verbose {
        srv.log(format!("connection ended #{id}"));
    }
}

/// Commands allowed before authentication.
const PRE_AUTH: &[&str] = &[
    "hello", "isMaster", "ismaster", "saslStart", "saslContinue", "authenticate", "getnonce", "ping", "buildInfo",
    "buildinfo", "endSessions", "logout", "whatsmyuri", "connectionStatus", "getLastError", "getlasterror",
];

/// Route one command document to its implementation and build the reply.
pub async fn dispatch(srv: &Arc<Server>, conn: &mut Conn, body: Document) -> Document {
    let Some(name) = body.keys().next().cloned() else {
        return Error::new(59, "no such command: ''").to_doc();
    };
    let db = body.get_str("$db").unwrap_or("admin").to_string();
    srv.counters.command.fetch_add(1, Ordering::Relaxed);
    if srv.cfg.verbose {
        srv.log(format!("conn{} {db} {name}: {}", conn.id, truncate(&body.to_string(), 400)));
    }
    // Async-aware commands.
    match name.as_str() {
        "hello" | "isMaster" | "ismaster" => {
            if let (Some(_), Some(wait)) = (body.get("topologyVersion"), body.get("maxAwaitTimeMS").and_then(bsonutil::num_i64)) {
                // Awaitable hello: no topology changes ever happen on this node, so wait it out.
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(wait.clamp(0, 10_000) as u64)) => {}
                    _ = srv.shutdown.notified() => {}
                }
            }
        }
        "getMore" => {
            if let Some(r) = crate::changestream::maybe_get_more(srv, conn, &db, &body).await {
                return finish_reply(srv, r);
            }
        }
        _ => {}
    }
    let srv2 = srv.clone();
    let result = tokio::task::block_in_place(|| run_sync(&srv2, conn, name, db, body));
    finish_reply(srv, result)
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        let mut end = n;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

fn finish_reply(srv: &Server, r: Result<Document>) -> Document {
    let mut d = match r {
        Ok(d) => d,
        Err(e) => e.to_doc(),
    };
    if srv.cfg.repl_set.is_some() {
        let ts = srv.store.cluster_time();
        d.insert("$clusterTime", doc! {"clusterTime": ts, "signature": {"hash": bsonutil::binary(bson::spec::BinarySubtype::Generic, vec![0; 20]), "keyId": 0_i64}});
        d.insert("operationTime", ts);
    }
    d
}

fn run_sync(srv: &Arc<Server>, conn: &mut Conn, name: String, db: String, cmd: Document) -> Result<Document> {
    if srv.cfg.auth && conn.users.is_empty() && !PRE_AUTH.contains(&name.as_str()) {
        let localhost_exception = name == "createUser" && db == "admin" && conn.is_localhost() && !crate::cmd::users::any_users(srv);
        if !localhost_exception {
            return Err(Error::new(13, format!("Command {name} requires authentication")));
        }
    }
    if srv.cfg.auth && !conn.users.is_empty() {
        crate::cmd::users::authorize(srv, conn, &db, &name, &cmd)?;
    }
    let txn = TxnArgs::from_cmd(&cmd);
    let session = txn.lsid.as_ref().map(|l| srv.sessions.get(l));
    // Retryable write de-duplication.
    let retryable = !txn.in_txn() && txn.txn_number.is_some() && matches!(name.as_str(), "insert" | "update" | "delete" | "findAndModify" | "findandmodify");
    let opid = srv.next_op.fetch_add(1, Ordering::Relaxed);
    let killed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    srv.ops.lock().expect("ops").push(OpInfo {
        opid,
        conn_id: conn.id,
        ns: format!("{db}.{}", cmd.get(&name).and_then(|v| v.as_str()).unwrap_or("$cmd")),
        command: cmd.clone(),
        started: Instant::now(),
        client: conn.peer.clone(),
        killed: killed.clone(),
    });
    let result = (|| {
        let sess_arc = session.clone();
        let guard = match &sess_arc {
            Some(s) => Some(s.lock().expect("session")),
            None => None,
        };
        // SAFETY of lifetimes: guard borrows from sess_arc which outlives ctx in this closure.
        let guard: Option<MutexGuard<'_, Session>> = guard;
        let mut ctx = CmdCtx { srv, conn, db, name: name.clone(), cmd, txn: txn.clone(), session: session.clone(), guard, opid, killed: killed.clone() };
        if retryable {
            if let (Some(n), Some(s)) = (txn.txn_number, ctx.sess()) {
                if let Some((last, reply)) = &s.retry {
                    if *last == n {
                        return Ok(reply.clone());
                    }
                }
            }
        }
        if txn.in_txn() && !matches!(name.as_str(), "commitTransaction" | "abortTransaction") {
            if !crate::cmd::txn_allowed(&name) {
                return Err(Error::new(263, format!("Cannot run '{name}' in a multi-document transaction.")));
            }
            let store = srv.store.clone();
            let s = ctx.sess().ok_or_else(|| Error::internal("missing session"))?;
            session::prepare_txn(&store, s, &txn)?;
        }
        let r = crate::cmd::route(&mut ctx);
        if let Err(e) = &r {
            if ctx.in_txn() && !matches!(name.as_str(), "commitTransaction" | "abortTransaction") && e.code != 50 {
                if let Some(s) = ctx.sess() {
                    session::abort_on_error(s);
                }
            }
        }
        if retryable {
            if let (Ok(reply), Some(n)) = (&r, txn.txn_number) {
                if let Some(s) = ctx.sess() {
                    s.retry = Some((n, reply.clone()));
                }
            }
        }
        r
    })();
    srv.ops.lock().expect("ops").retain(|o| o.opid != opid);
    if let Err(e) = &result {
        if srv.cfg.verbose {
            srv.log(format!("conn{} {name} failed: {e}", conn.id));
        }
    }
    result
}

// ============================ handshake ============================

pub fn hello(ctx: &mut CmdCtx) -> Result<Document> {
    let srv = ctx.srv;
    let cmd = &ctx.cmd;
    if let Ok(c) = cmd.get_array("compression") {
        ctx.conn.compressors = c.iter().filter_map(|v| v.as_str()).filter_map(Compressor::from_name).collect();
    }
    if let Ok(m) = cmd.get_document("client") {
        ctx.conn.client_meta = Some(m.clone());
    }
    let legacy = ctx.name != "hello";
    let mut d = Document::new();
    if cmd.contains_key("helloOk") || ctx.name == "hello" {
        d.insert("helloOk", true);
    }
    d.insert(if legacy { "ismaster" } else { "isWritablePrimary" }, true);
    d.insert("topologyVersion", doc! {"processId": srv.process_id, "counter": 0_i64});
    if let Some(set) = &srv.cfg.repl_set {
        let host = srv.host();
        let ts = srv.store.cluster_time();
        let now = bson::DateTime::now();
        d.insert("hosts", vec![host.clone()]);
        d.insert("setName", set.clone());
        d.insert("setVersion", 1);
        d.insert("secondary", false);
        d.insert("primary", host.clone());
        d.insert("me", host);
        d.insert("electionId", srv.election_id);
        d.insert("lastWrite", doc! {"opTime": {"ts": ts, "t": 1_i64}, "lastWriteDate": now, "majorityOpTime": {"ts": ts, "t": 1_i64}, "majorityWriteDate": now});
    }
    d.insert("maxBsonObjectSize", 16_777_216);
    d.insert("maxMessageSizeBytes", 48_000_000);
    d.insert("maxWriteBatchSize", 100_000);
    d.insert("localTime", bson::DateTime::now());
    d.insert("logicalSessionTimeoutMinutes", 30);
    d.insert("connectionId", ctx.conn.id as i32);
    d.insert("minWireVersion", MIN_WIRE);
    d.insert("maxWireVersion", MAX_WIRE);
    d.insert("readOnly", false);
    if !ctx.conn.compressors.is_empty() {
        d.insert("compression", ctx.conn.compressors.iter().map(|c| Bson::String(c.name().into())).collect::<Vec<_>>());
    }
    if let Ok(u) = cmd.get_str("saslSupportedMechs") {
        if let Some((udb, user)) = u.split_once('.') {
            if let Some(mechs) = crate::cmd::authcmd::supported_mechs(srv, udb, user) {
                d.insert("saslSupportedMechs", mechs);
            }
        }
    }
    if let Ok(spec) = cmd.get_document("speculativeAuthenticate") {
        if let Ok(r) = crate::cmd::authcmd::speculative(ctx, spec.clone()) {
            d.insert("speculativeAuthenticate", r);
        }
    }
    d.insert("ok", 1.0);
    Ok(d)
}

pub fn build_info() -> Document {
    doc! {
        "version": VERSION,
        "gitVersion": "mogodb",
        "modules": [],
        "allocator": "system",
        "javascriptEngine": if cfg!(feature = "js") { "mozjs-wasm" } else { "none" },
        "sysinfo": "deprecated",
        "versionArray": [9, 0, 4, 0],
        "openssl": {"running": "disabled", "compiled": "disabled"},
        "buildEnvironment": {"distmod": "mogodb", "target_arch": std::env::consts::ARCH, "target_os": std::env::consts::OS},
        "bits": 64,
        "debug": false,
        "maxBsonObjectSize": 16_777_216,
        "storageEngines": ["fjall"],
        "ok": 1.0,
    }
}

/// 9.0 per-operation memory limit default: max(1 GiB, 20% of physical RAM).
pub fn per_op_memory_limit() -> i64 {
    let total = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|m| m.lines().find(|l| l.starts_with("MemTotal:")).and_then(|l| l.split_whitespace().nth(1)?.parse::<i64>().ok()))
        .map(|kb| kb * 1024)
        .unwrap_or(0);
    (total / 5).max(1 << 30)
}
