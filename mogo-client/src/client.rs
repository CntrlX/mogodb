//! Client, connection pool, database/collection handles, cursors and change streams.

use crate::conn::{self, Connection, CursorReply};
use crate::error::{Error, ErrorKind, Result};
use crate::uri;
use bson::{Bson, Document};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

pub use crate::conn::ServerInfo;


#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub hosts: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub auth_source: String,
    pub replica_set: Option<String>,
    pub app_name: String,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
    pub max_pool_size: usize,
    pub default_db: Option<String>,
    pub min_pool_size: usize,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            hosts: vec!["127.0.0.1:27017".into()],
            username: None,
            password: None,
            auth_source: "admin".into(),
            replica_set: None,
            app_name: "mogo-client".into(),
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
            max_pool_size: 16,
            default_db: None,
            min_pool_size: 0,
        }
    }
}

impl ClientOptions {
    pub fn from_uri(uri: &str) -> Result<ClientOptions> {
        let u = uri::parse(uri)?;
        Ok(ClientOptions {
            hosts: u.hosts.clone(),
            username: u.username.clone(),
            password: u.password.clone(),
            auth_source: u.auth_source(),
            replica_set: u.replica_set(),
            app_name: u.app_name(),
            default_db: u.default_db.clone(),
            ..Default::default()
        })
    }
}

struct PoolInner {
    opts: ClientOptions,
    idle: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
}

impl PoolInner {
    /// Return a connection to the idle list (dropping it when the pool is full).
    fn release(&self, conn: Connection) {
        if let Ok(mut idle) = self.idle.try_lock() {
            if idle.len() < self.opts.max_pool_size {
                idle.push(conn);
                return;
            }
        }
        drop(conn);
    }
}

pub struct Pool {
    inner: Arc<PoolInner>,
}

impl Pool {
    fn new(opts: ClientOptions) -> Pool {
        let permits = Arc::new(Semaphore::new(opts.max_pool_size.max(1)));
        Pool { inner: Arc::new(PoolInner { opts, idle: Mutex::new(Vec::new()), permits }) }
    }

    /// Check out a connection, reusing an idle one or connecting a new one.
    pub async fn checkout(&self) -> Result<PooledConn> {
        let permit = self.inner.permits.clone().acquire_owned().await.map_err(|_| Error::io("pool closed"))?;
        let mut idle = self.inner.idle.lock().await;
        if let Some(mut conn) = idle.pop() {
            drop(idle);
            // mongod authorizes per connection, so a reused connection re-runs SCRAM.
            if let (Some(user), Some(pass)) = (&self.inner.opts.username, &self.inner.opts.password) {
                conn.authenticate(user, pass)
                    .await
                    .map_err(|e| Error::io(format!("re-authentication on pooled connection failed: {e}")))?;
            }
            return Ok(PooledConn { conn: Some(conn), inner: self.inner.clone(), _permit: permit });
        }
        drop(idle);
        let mut conn = self.connect_new().await?;
        conn.set_timeout(self.inner.opts.operation_timeout);
        Ok(PooledConn { conn: Some(conn), inner: self.inner.clone(), _permit: permit })
    }

    async fn connect_new(&self) -> Result<Connection> {
        let opts = &self.inner.opts;
        let mut last: Option<Error> = None;
        // Reuse an authenticated idle connection for the same user when possible.
        if opts.username.is_some() {
            let mut idle = self.inner.idle.lock().await;
            if let Some(pos) = idle.iter().position(|c| c.authenticated_user() == opts.username.as_deref()) {
                return Ok(idle.remove(pos));
            }
        }
        for host in &opts.hosts {
            match Connection::connect(host, opts.connect_timeout).await {
                Ok(mut conn) => {
                    conn.auth_source = opts.auth_source.clone();
                    let user = opts.username.as_deref();
                    if let Err(e) = conn.handshake(&opts.app_name, user).await {
                        last = Some(e);
                        continue;
                    }
                    if let Some(set) = &opts.replica_set {
                        match conn.info.set_name.as_deref() {
                            Some(name) if name == set => {}
                            Some(name) => {
                                last = Some(Error::config(format!("host {host} is in replica set {name:?}, expected {set:?}")));
                                continue;
                            }
                            None => {
                                last = Some(Error::config(format!("host {host} is not in a replica set (expected {set:?})")));
                                continue;
                            }
                        }
                    }
                    if let (Some(user), Some(pass)) = (&opts.username, &opts.password) {
                        if let Err(e) = conn.authenticate(user, pass).await {
                            last = Some(e);
                            continue;
                        }
                    }
                    return Ok(conn);
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| Error::io("no reachable hosts")))
    }
}

/// A checked-out connection; returns to the pool on drop.
pub struct PooledConn {
    conn: Option<Connection>,
    inner: Arc<PoolInner>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Deref for PooledConn {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.conn.as_ref().expect("checked out")
    }
}

impl DerefMut for PooledConn {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("checked out")
    }
}

impl Drop for PooledConn {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.inner.release(conn);
        }
    }
}

pub struct Client {
    pool: Pool,
    opts: ClientOptions,
}

impl Client {
    pub async fn connect(uri: &str) -> Result<Client> {
        Client::with_options(ClientOptions::from_uri(uri)?).await
    }

    pub async fn with_options(opts: ClientOptions) -> Result<Client> {
        let client = Client { pool: Pool::new(opts.clone()), opts };
        let conn = client.pool.checkout().await?;
        let info = conn.info.clone();
        drop(conn);
        if info.max_wire_version == 0 && info.hello.is_empty() {
            return Err(Error::io("server did not answer the handshake"));
        }
        if client.opts.min_pool_size > 0 {
            for _ in 0..client.opts.min_pool_size {
                if let Ok(c) = client.pool.checkout().await {
                    drop(c);
                }
            }
        }
        Ok(client)
    }

    pub fn options(&self) -> &ClientOptions {
        &self.opts
    }

    pub async fn ping(&self) -> Result<()> {
        let mut c = self.pool.checkout().await?;
        c.command("admin", bson::doc! {"ping": 1}).await?;
        Ok(())
    }

    pub async fn server_info(&self) -> Result<ServerInfo> {
        let c = self.pool.checkout().await?;
        Ok(c.info.clone())
    }

    pub async fn build_info(&self) -> Result<Document> {
        let mut c = self.pool.checkout().await?;
        c.command("admin", bson::doc! {"buildInfo": 1}).await
    }

    pub fn database(&self, name: &str) -> Db<'_> {
        Db { client: self, name: name.to_string() }
    }

    pub fn default_database(&self) -> Option<&str> {
        self.opts.default_db.as_deref()
    }

    /// Check out a connection for multi-step work.
    pub async fn conn(&self) -> Result<PooledConn> {
        self.pool.checkout().await
    }

    pub async fn run_admin(&self, cmd: Document) -> Result<Document> {
        let mut c = self.pool.checkout().await?;
        c.command("admin", cmd).await
    }

    pub async fn list_database_names(&self) -> Result<Vec<String>> {
        let d = self.run_admin(bson::doc! {"listDatabases": 1, "nameOnly": true}).await?;
        Ok(d.get_array("databases")
            .map(|a| a.iter().filter_map(Bson::as_document).filter_map(|x| x.get_str("name").ok()).map(str::to_string).collect())
            .unwrap_or_default())
    }

    pub fn lsid(&self) -> Document {
        bson::doc! {"id": Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes: uuid::Uuid::new_v4().as_bytes().to_vec() })}
    }
}

pub struct Db<'a> {
    client: &'a Client,
    name: String,
}

impl<'a> Db<'a> {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn collection(&self, name: &str) -> Collection<'a> {
        Collection { client: self.client, db: self.name.clone(), name: name.to_string() }
    }
    pub async fn command(&self, cmd: Document) -> Result<Document> {
        let mut c = self.client.conn().await?;
        c.command(&self.name, cmd).await
    }
    pub async fn create_collection(&self, name: &str, options: Document) -> Result<Document> {
        let mut cmd = bson::doc! {"create": name};
        for (k, v) in options {
            cmd.insert(k, v);
        }
        self.command(cmd).await
    }
    pub async fn drop_collection(&self, name: &str) -> Result<Document> {
        self.command(bson::doc! {"drop": name}).await
    }
    pub async fn list_collection_names(&self) -> Result<Vec<String>> {
        let d = self.command(bson::doc! {"listCollections": 1, "nameOnly": true}).await?;
        Ok(d.get_document("cursor")
            .ok()
            .and_then(|c| c.get_array("firstBatch").ok())
            .map(|a| a.iter().filter_map(Bson::as_document).filter_map(|x| x.get_str("name").ok()).map(str::to_string).collect())
            .unwrap_or_default())
    }
    pub async fn list_collections(&self, filter: Document) -> Result<Vec<Document>> {
        let d = self.command(bson::doc! {"listCollections": 1, "filter": filter}).await?;
        Ok(cursor_docs(&d))
    }
}

pub struct Collection<'a> {
    client: &'a Client,
    db: String,
    name: String,
}

impl<'a> Collection<'a> {
    pub fn db(&self) -> &str {
        &self.db
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn namespace(&self) -> String {
        format!("{}.{}", self.db, self.name)
    }

    pub async fn find(&self, filter: Document, opts: FindOptions) -> Result<Cursor> {
        let mut c = self.client.conn().await?;
    let mut cmd = bson::doc! {"find": self.name.clone(), "filter": filter, "batchSize": opts.batch_size.unwrap_or(101)};
        if let Some(p) = &opts.projection {
            cmd.insert("projection", p.clone());
        }
        if let Some(s) = &opts.sort {
            cmd.insert("sort", s.clone());
        }
        if opts.skip > 0 {
            cmd.insert("skip", opts.skip as i64);
        }
        if opts.limit != 0 {
            cmd.insert("limit", opts.limit);
        }
        if opts.single_batch {
            cmd.insert("singleBatch", true);
        }
        if let Some(ca) = &opts.collation {
            cmd.insert("collation", ca.clone());
        }
        if let Some(h) = &opts.hint {
            cmd.insert("hint", h.clone());
        }
        if opts.max_time_ms > 0 {
            cmd.insert("maxTimeMS", opts.max_time_ms);
        }
        if opts.show_record_id {
            cmd.insert("showRecordId", true);
        }
        if opts.return_key {
            cmd.insert("returnKey", true);
        }
        if let Some(l) = &opts.let_vars {
            cmd.insert("let", l.clone());
        }
        if let Some(cm) = &opts.comment {
            cmd.insert("comment", cm.clone());
        }
        let reply = c.command(&self.db, cmd).await?;
        let cur = conn::parse_cursor(&reply)?;
        let exhausted = cur.id == 0;
        Ok(Cursor { conn: c, cursor: cur, exhausted })
    }

    pub async fn find_one(&self, filter: Document, opts: FindOptions) -> Result<Option<Document>> {
        let mut o = opts;
        o.limit = 1;
        o.single_batch = true;
        o.batch_size = Some(1);
        let mut cur = self.find(filter, o).await?;
        Ok(cur.next().await?)
    }

    pub async fn find_all(&self, filter: Document, opts: FindOptions) -> Result<Vec<Document>> {
        let limit = if opts.limit == 0 { i64::MAX } else { opts.limit };
        let mut cur = self.find(filter, opts).await?;
        let mut out = Vec::new();
        loop {
            match cur.next().await? {
                Some(d) => {
                    out.push(d);
                    if limit != 0 && out.len() as i64 >= limit {
                        break;
                    }
                }
                None => break,
            }
        }
        Ok(out)
    }

    pub async fn count(&self, filter: Document) -> Result<i64> {
        let mut c = self.client.conn().await?;
        let d = c.command(&self.db, bson::doc! {"count": self.name.clone(), "query": filter}).await?;
        Ok(d.get_i64("n").or_else(|_| d.get_i32("n").map(|i| i as i64)).unwrap_or(0))
    }

    pub async fn distinct(&self, field: &str, filter: Document) -> Result<Vec<Bson>> {
        let mut c = self.client.conn().await?;
        let d = c.command(&self.db, bson::doc! {"distinct": self.name.clone(), "key": field, "query": filter}).await?;
        Ok(d.get_array("values").cloned().unwrap_or_default())
    }

    pub async fn insert_one(&self, doc: Document) -> Result<Bson> {
        let id = doc.get("_id").cloned();
        let r = self.insert_many(vec![doc], true).await?;
        if let Some(id) = id {
            return Ok(id);
        }
        Ok(r.inserted_ids.into_iter().next().unwrap_or(Bson::Null))
    }

    pub async fn insert_many(&self, docs: Vec<Document>, ordered: bool) -> Result<InsertResult> {
        let mut c = self.client.conn().await?;
        let mut cmd = bson::doc! {"insert": self.name.clone(), "documents": docs};
        if !ordered {
            cmd.insert("ordered", false);
        }
        let r = c.command(&self.db, cmd).await?;
        parse_insert(&r)
    }

    pub async fn update_one(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.update(filter, update, false, false, None).await
    }
    pub async fn update_many(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.update(filter, update, true, false, None).await
    }
    pub async fn replace_one(&self, filter: Document, replacement: Document) -> Result<UpdateResult> {
        self.update(filter, replacement, false, true, None).await
    }
    pub async fn update(&self, filter: Document, update: Document, multi: bool, replacement: bool, upsert: Option<bool>) -> Result<UpdateResult> {
        let mut c = self.client.conn().await?;
        let u = if replacement { update.clone() } else { update };
        let mut stmt = bson::doc! {"q": filter, "u": u};
        if multi {
            stmt.insert("multi", true);
        }
        if let Some(up) = upsert {
            stmt.insert("upsert", up);
        }
        let r = c.command(&self.db, bson::doc! {"update": self.name.clone(), "updates": [stmt]}).await?;
        parse_update(&r)
    }

    pub async fn find_and_modify(&self, cmd: Document) -> Result<Document> {
        let mut c = self.client.conn().await?;
        c.command(&self.db, cmd).await
    }

    pub async fn delete_one(&self, filter: Document) -> Result<i64> {
        self.delete(filter, false).await
    }
    pub async fn delete_many(&self, filter: Document) -> Result<i64> {
        self.delete(filter, true).await
    }
    async fn delete(&self, filter: Document, multi: bool) -> Result<i64> {
        let mut c = self.client.conn().await?;
        // The MongoDB delete statement has no `multi` field: `limit: 0` means "all", `1` means one.
        let stmt = bson::doc! {"q": filter, "limit": if multi { 0 } else { 1 }};
        let r = c.command(&self.db, bson::doc! {"delete": self.name.clone(), "deletes": [stmt]}).await?;
        Ok(r.get_i32("n").map(|i| i as i64).or_else(|_| r.get_i64("n")).unwrap_or(0))
    }

    pub async fn aggregate(&self, pipeline: Vec<Document>, opts: AggregateOptions) -> Result<Cursor> {
        let mut c = self.client.conn().await?;
        let mut cmd = bson::doc! {"aggregate": self.name.clone(), "pipeline": pipeline, "cursor": {"batchSize": opts.batch_size.unwrap_or(101)}};
        if opts.allow_disk_use {
            cmd.insert("allowDiskUse", true);
        }
        if opts.max_time_ms > 0 {
            cmd.insert("maxTimeMS", opts.max_time_ms);
        }
        if let Some(h) = &opts.hint {
            cmd.insert("hint", h.clone());
        }
        if let Some(ca) = &opts.collation {
            cmd.insert("collation", ca.clone());
        }
        if let Some(l) = &opts.let_vars {
            cmd.insert("let", l.clone());
        }
        let reply = c.command(&self.db, cmd).await?;
        let cur = conn::parse_cursor(&reply)?;
        let exhausted = cur.id == 0;
        Ok(Cursor { conn: c, cursor: cur, exhausted })
    }

    pub async fn aggregate_all(&self, pipeline: Vec<Document>, opts: AggregateOptions) -> Result<Vec<Document>> {
        let mut cur = self.aggregate(pipeline, opts).await?;
        let mut out = Vec::new();
        while let Some(d) = cur.next().await? {
            out.push(d);
        }
        Ok(out)
    }

    /// Open a change stream (`$changeStream` must be the first pipeline stage).
    pub async fn watch(&self, pipeline: Vec<Document>, opts: WatchOptions) -> Result<ChangeStream> {
        let mut stages = vec![bson::doc! {"$changeStream": opts.change_stream_doc()}];
        stages.extend(pipeline);
        let agg = AggregateOptions {
            batch_size: Some(opts.batch_size.unwrap_or(1)),
            max_time_ms: opts.max_time_ms,
            ..Default::default()
        };
        let cur = self.aggregate(stages, agg).await?;
        if cur.is_exhausted() {
            return Err(Error::new(ErrorKind::Command, "change stream closed immediately"));
        }
        Ok(ChangeStream { cursor: cur, resume: None })
    }

    pub async fn create_index(&self, spec: Document, opts: Document) -> Result<Document> {
        let mut c = self.client.conn().await?;
        let mut cmd = bson::doc! {"createIndexes": self.name.clone(), "indexes": [spec]};
        for (k, v) in opts {
            cmd.insert(k, v);
        }
        c.command(&self.db, cmd).await
    }

    pub async fn indexes(&self) -> Result<Vec<Document>> {
        let mut c = self.client.conn().await?;
        let d = c.command(&self.db, bson::doc! {"listIndexes": self.name.clone()}).await?;
        Ok(cursor_docs(&d))
    }

    pub async fn drop_index(&self, name: &str) -> Result<Document> {
        let mut c = self.client.conn().await?;
        c.command(&self.db, bson::doc! {"dropIndexes": self.name.clone(), "index": name}).await
    }

    pub async fn explain(&self, cmd: Document) -> Result<Document> {
        let mut c = self.client.conn().await?;
        c.command(&self.db, bson::doc! {"explain": cmd, "verbosity": "allPlansExecution"}).await
    }

    pub async fn stats(&self) -> Result<Document> {
        let mut c = self.client.conn().await?;
        c.command(&self.db, bson::doc! {"collStats": self.name.clone()}).await
    }

    pub async fn validate(&self) -> Result<Document> {
        let mut c = self.client.conn().await?;
        c.command(&self.db, bson::doc! {"validate": self.name.clone()}).await
    }
}

#[derive(Debug, Clone, Default)]
pub struct FindOptions {
    pub projection: Option<Document>,
    pub sort: Option<Document>,
    pub skip: i64,
    pub limit: i64,
    pub batch_size: Option<u32>,
    pub single_batch: bool,
    pub collation: Option<Document>,
    pub hint: Option<Document>,
    pub max_time_ms: i64,
    pub show_record_id: bool,
    pub return_key: bool,
    pub let_vars: Option<Document>,
    pub comment: Option<Bson>,
}

impl FindOptions {
    pub fn sort(mut self, spec: Document) -> Self {
        self.sort = Some(spec);
        self
    }
    pub fn limit(mut self, n: i64) -> Self {
        self.limit = n;
        self
    }
    pub fn skip(mut self, n: i64) -> Self {
        self.skip = n;
        self
    }
    pub fn projection(mut self, p: Document) -> Self {
        self.projection = Some(p);
        self
    }
    pub fn hint(mut self, h: Document) -> Self {
        self.hint = Some(h);
        self
    }
    pub fn collation(mut self, c: Document) -> Self {
        self.collation = Some(c);
        self
    }
    pub fn max_time_ms(mut self, ms: i64) -> Self {
        self.max_time_ms = ms;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct AggregateOptions {
    pub batch_size: Option<u32>,
    pub allow_disk_use: bool,
    pub max_time_ms: i64,
    pub hint: Option<Document>,
    pub collation: Option<Document>,
    pub let_vars: Option<Document>,
}

#[derive(Debug, Clone, Default)]
pub struct WatchOptions {
    /// Extra `$changeStream` options (`fullDocument`, `resumeAfter`, `startAtOperationTime`, ...).
    pub full_document: Option<String>,
    pub resume_after: Option<Document>,
    pub start_at_operation_time: Option<Bson>,
    pub batch_size: Option<u32>,
    pub max_time_ms: i64,
}

impl WatchOptions {
    fn change_stream_doc(&self) -> Document {
        let mut d = Document::new();
        if let Some(f) = &self.full_document {
            d.insert("fullDocument", f.clone());
        }
        if let Some(r) = &self.resume_after {
            d.insert("resumeAfter", r.clone());
        }
        if let Some(t) = &self.start_at_operation_time {
            d.insert("startAtOperationTime", t.clone());
        }
        d
    }
}

#[derive(Debug, Clone, Default)]
pub struct InsertResult {
    pub acknowledged: bool,
    pub inserted_ids: Vec<Bson>,
    pub upserted_ids: Vec<(i64, Bson)>,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateResult {
    pub matched: i64,
    pub modified: i64,
    pub upserted_id: Option<Bson>,
}

/// Cursor over `find`/`aggregate` results, backed by one pooled connection.
pub struct Cursor {
    conn: PooledConn,
    cursor: CursorReply,
    exhausted: bool,
}

impl Cursor {
    pub fn namespace(&self) -> &str {
        &self.cursor.ns
    }
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    /// Next document, fetching batches as needed.
    pub async fn next(&mut self) -> Result<Option<Document>> {
        loop {
            if let Some(d) = self.buffer_pop() {
                return Ok(Some(d));
            }
            if self.exhausted {
                return Ok(None);
            }
            self.fetch_batch().await?;
        }
    }

    fn buffer_pop(&mut self) -> Option<Document> {
        // `docs` doubles as the read buffer: batches are appended and consumed from the front.
        if self.cursor.docs.is_empty() {
            None
        } else {
            Some(self.cursor.docs.remove(0))
        }
    }

    async fn fetch_batch(&mut self) -> Result<()> {
        let id = self.cursor.id;
        let (db, coll) = self.cursor.ns.split_once('.').unwrap_or(("admin", ""));
        let r = self.conn.command(db, bson::doc! {"getMore": id, "collection": coll, "batchSize": 101}).await?;
        let (new_id, docs) = conn::next_batch(&r)?;
        self.cursor.id = new_id;
        self.cursor.docs.extend(docs);
        if new_id == 0 || self.cursor.docs.is_empty() {
            self.exhausted = true;
        }
        Ok(())
    }
}

/// Change stream: `next()` waits up to `max_time_ms` for the next event.
pub struct ChangeStream {
    cursor: Cursor,
    resume: Option<Document>,
}

impl ChangeStream {
    pub async fn next(&mut self, max_time_ms: i64) -> Result<Option<Document>> {
        if let Some(d) = self.cursor.buffer_pop() {
            self.remember(&d);
            return Ok(Some(d));
        }
        if self.cursor.exhausted {
            return Ok(None);
        }
        let id = self.cursor.cursor.id;
        let ns = self.cursor.cursor.ns.clone();
        let (db, coll) = ns.split_once('.').unwrap_or(("admin", ""));
        let r = self.cursor.conn.command(db, bson::doc! {"getMore": id, "collection": coll, "batchSize": 1, "maxTimeMS": max_time_ms}).await?;
        let (new_id, docs) = conn::next_batch(&r)?;
        self.cursor.cursor.id = new_id;
        if new_id == 0 {
            self.cursor.exhausted = true;
        }
        if let Some(d) = docs.into_iter().next() {
            self.remember(&d);
            Ok(Some(d))
        } else {
            Ok(None)
        }
    }

    fn remember(&mut self, d: &Document) {
        if let Ok(t) = d.get_document("_id") {
            self.resume = Some(t.clone());
        }
    }

    pub fn resume_token(&self) -> Option<&Document> {
        self.resume.as_ref()
    }
}

/// Extract documents from a cursor-style reply.
pub fn cursor_docs(d: &Document) -> Vec<Document> {
    d.get_document("cursor")
        .ok()
        .and_then(|c| c.get_array("firstBatch").ok())
        .map(|a| a.iter().filter_map(Bson::as_document).cloned().collect())
        .unwrap_or_default()
}

fn parse_insert(d: &Document) -> Result<InsertResult> {
    if let Some(Bson::Array(errs)) = d.get("writeErrors") {
        if let Some(first) = errs.first().and_then(Bson::as_document) {
            let code = first.get_i32("code").unwrap_or(0);
            let msg = first.get_str("errmsg").unwrap_or("write failed").to_string();
            return Err(Error::command(code, msg));
        }
    }
    let mut inserted = vec![Bson::Null; d.get_array("insertedIds").map(|a| a.len()).or_else(|_| d.get_document("insertedIds").map(|x| x.len())).unwrap_or(0)];
    match d.get("insertedIds") {
        Some(Bson::Document(m)) => {
            for (k, v) in m {
                if let Ok(i) = k.parse::<usize>() {
                    if i < inserted.len() {
                        inserted[i] = v.clone();
                    }
                }
            }
        }
        Some(Bson::Array(a)) => {
            for (i, v) in a.iter().enumerate() {
                if i < inserted.len() {
                    inserted[i] = v.clone();
                }
            }
        }
        _ => {}
    }
    Ok(InsertResult {
        acknowledged: d.get_bool("ok").unwrap_or(true),
        inserted_ids: inserted,
        upserted_ids: d
            .get_array("upserted")
            .map(|a| {
                a.iter()
                    .filter_map(Bson::as_document)
                    .filter_map(|x| Some((x.get_i64("index").ok()?, x.get("_id").cloned()?)))
                    .collect()
            })
            .unwrap_or_default(),
    })
}fn parse_update(d: &Document) -> Result<UpdateResult> {
    if let Some(Bson::Array(errs)) = d.get("writeErrors") {
        if let Some(first) = errs.first().and_then(Bson::as_document) {
            return Err(Error::command(first.get_i32("code").unwrap_or(0), first.get_str("errmsg").unwrap_or("update failed").to_string()));
        }
    }
    Ok(UpdateResult {
        matched: d.get_i32("n").map(|i| i as i64).or_else(|_| d.get_i64("n")).unwrap_or(0),
        modified: d.get_i32("nModified").map(|i| i as i64).or_else(|_| d.get_i64("nModified")).unwrap_or(0),
        upserted_id: d.get_array("upserted").ok().and_then(|a| a.first()).and_then(Bson::as_document).and_then(|x| x.get("_id").cloned()),
    })
}
