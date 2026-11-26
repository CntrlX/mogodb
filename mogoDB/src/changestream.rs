//! Change streams: oplog replay + zero-polling broadcast subscription.

use crate::agg::expr::Vars;
use crate::agg::pipeline::{self, AggOptions};
use crate::bsonutil;
use crate::cursor::{self, Cursor, CursorKind};
use crate::error::{Error, Result};
use crate::server::{CmdCtx, Conn, Server};
use bson::{Bson, Document, doc};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::{self, error::RecvError, error::TryRecvError};

#[derive(Clone, Debug, PartialEq)]
pub enum Scope {
    Coll { db: String, coll: String },
    Db(String),
    Cluster,
}

pub struct ChangeStream {
    pub scope: Scope,
    pub rx: broadcast::Receiver<Arc<Document>>,
    /// Timestamp of the last oplog entry examined.
    pub last_ts: bson::Timestamp,
    pub full_document: String,
    pub before_change: String,
    pub expanded: bool,
    pub stages: Vec<Document>,
    pub pending: VecDeque<Document>,
    pub invalidated: bool,
    pub db: String,
    /// Resume token of the last event returned (postBatchResumeToken).
    pub last_token: Document,
}

pub fn token(ts: bson::Timestamp) -> Document {
    let v = ((ts.time as u64) << 32) | ts.increment as u64;
    doc! {"_data": format!("{v:016X}")}
}

pub fn parse_token(t: &Document) -> Result<bson::Timestamp> {
    let s = t.get_str("_data").map_err(|_| Error::bad_value("invalid resume token: missing _data"))?;
    let v = u64::from_str_radix(s.get(..16).unwrap_or(s), 16).map_err(|_| Error::bad_value(format!("invalid resume token: {s}")))?;
    Ok(bson::Timestamp { time: (v >> 32) as u32, increment: v as u32 })
}

fn ts_minus_one(ts: bson::Timestamp) -> bson::Timestamp {
    if ts.increment > 0 {
        bson::Timestamp { time: ts.time, increment: ts.increment - 1 }
    } else if ts.time > 0 {
        bson::Timestamp { time: ts.time - 1, increment: u32::MAX }
    } else {
        ts
    }
}

fn ts_gt(a: bson::Timestamp, b: bson::Timestamp) -> bool {
    (a.time, a.increment) > (b.time, b.increment)
}

/// MongoDB-style `updateDescription` diff of two documents.
pub fn update_description(old: &Document, new: &Document) -> Document {
    let mut updated = Document::new();
    let mut removed = Vec::new();
    fn walk(prefix: &str, old: &Document, new: &Document, up: &mut Document, rm: &mut Vec<Bson>) {
        for (k, nv) in new {
            let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
            match old.get(k) {
                Some(Bson::Document(od)) => {
                    if let Bson::Document(nd) = nv {
                        walk(&path, od, nd, up, rm);
                    } else {
                        up.insert(path, nv.clone());
                    }
                }
                Some(Bson::Array(oa)) => {
                    if let Bson::Array(na) = nv {
                        if na.len() >= oa.len() && oa.iter().zip(na.iter()).all(|(a, b)| bsonutil::bson_eq(a, b) && bsonutil::type_order(a) == bsonutil::type_order(b)) {
                            for (i, e) in na.iter().enumerate().skip(oa.len()) {
                                up.insert(format!("{path}.{i}"), e.clone());
                            }
                            continue;
                        }
                    }
                    if bsonutil::to_bytes(&doc! {"v": Bson::Array(oa.clone())}) != bsonutil::to_bytes(&doc! {"v": nv.clone()}) {
                        up.insert(path, nv.clone());
                    }
                }
                Some(ov) => {
                    if bsonutil::to_bytes(&doc! {"v": ov.clone()}) != bsonutil::to_bytes(&doc! {"v": nv.clone()}) {
                        up.insert(path, nv.clone());
                    }
                }
                None => {
                    up.insert(path, nv.clone());
                }
            }
        }
        for k in old.keys() {
            if !new.contains_key(k) {
                rm.push(Bson::String(if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") }));
            }
        }
    }
    walk("", old, new, &mut updated, &mut removed);
    doc! {"updatedFields": updated, "removedFields": removed, "truncatedArrays": []}
}

impl ChangeStream {
    fn in_scope(&self, ns: &str) -> bool {
        let (db, coll) = ns.split_once('.').unwrap_or((ns, ""));
        match &self.scope {
            Scope::Coll { db: d, coll: c } => d == db && (c == coll || coll == "$cmd"),
            Scope::Db(d) => d == db && !coll.starts_with("system."),
            Scope::Cluster => !matches!(db, "admin" | "config" | "local"),
        }
    }

    /// Convert an oplog entry into zero or more change events.
    fn convert(&mut self, srv: &Server, e: &Document) -> Vec<Document> {
        let ns = e.get_str("ns").unwrap_or_default();
        let ts = match e.get("ts") {
            Some(Bson::Timestamp(t)) => *t,
            _ => return Vec::new(),
        };
        if !self.in_scope(ns) {
            return Vec::new();
        }
        let (db, coll) = ns.split_once('.').unwrap_or((ns, ""));
        let op = e.get_str("op").unwrap_or_default();
        let mut ev = doc! {"_id": token(ts)};
        let base_ns = doc! {"db": db, "coll": coll};
        let wall = e.get("wall").cloned().unwrap_or(Bson::Null);
        let key = e.get_document("o2").ok().cloned().or_else(|| e.get_document("o").ok().cloned()).unwrap_or_default();
        match op {
            "i" => {
                ev.insert("operationType", "insert");
                ev.insert("clusterTime", ts);
                ev.insert("wallTime", wall);
                ev.insert("fullDocument", e.get("o").cloned().unwrap_or(Bson::Null));
                ev.insert("ns", base_ns);
                ev.insert("documentKey", key);
            }
            "u" => {
                let repl = e.get_bool("repl").unwrap_or(false);
                ev.insert("operationType", if repl { "replace" } else { "update" });
                ev.insert("clusterTime", ts);
                ev.insert("wallTime", wall);
                if repl {
                    ev.insert("fullDocument", e.get("full").cloned().unwrap_or(Bson::Null));
                } else if matches!(self.full_document.as_str(), "updateLookup" | "whenAvailable" | "required") {
                    let id = key.get("_id").cloned().unwrap_or(Bson::Null);
                    let ctx = crate::cmd::crud::ServerAggCtx { store: srv.store.clone() };
                    let found = crate::agg::pipeline::AggContext::find(&ctx, db, coll, &doc! {"_id": id}).ok().and_then(|v| v.into_iter().next());
                    ev.insert("fullDocument", found.map(Bson::Document).unwrap_or(Bson::Null));
                }
                ev.insert("ns", base_ns);
                ev.insert("documentKey", key);
                if !repl {
                    ev.insert("updateDescription", e.get("ud").cloned().unwrap_or_else(|| Bson::Document(doc! {"updatedFields": {}, "removedFields": [], "truncatedArrays": []})));
                }
                if matches!(self.before_change.as_str(), "whenAvailable" | "required") {
                    ev.insert("fullDocumentBeforeChange", e.get("pre").cloned().unwrap_or(Bson::Null));
                }
            }
            "d" => {
                ev.insert("operationType", "delete");
                ev.insert("clusterTime", ts);
                ev.insert("wallTime", wall);
                ev.insert("ns", base_ns);
                ev.insert("documentKey", e.get("o").cloned().unwrap_or(Bson::Null));
                if matches!(self.before_change.as_str(), "whenAvailable" | "required") {
                    ev.insert("fullDocumentBeforeChange", e.get("pre").cloned().unwrap_or(Bson::Null));
                }
            }
            "c" => {
                let o = e.get_document("o").cloned().unwrap_or_default();
                let mut out = Vec::new();
                if let Ok(c) = o.get_str("drop") {
                    if matches!(&self.scope, Scope::Coll { coll, .. } if coll != c) {
                        return Vec::new();
                    }
                    ev.insert("operationType", "drop");
                    ev.insert("clusterTime", ts);
                    ev.insert("wallTime", wall);
                    ev.insert("ns", doc! {"db": db, "coll": c});
                    out.push(ev);
                    if matches!(self.scope, Scope::Coll { .. }) {
                        out.push(self.invalidate(ts));
                    }
                    return out;
                }
                if let Ok(from) = o.get_str("renameCollection") {
                    let (_, fc) = from.split_once('.').unwrap_or(("", from));
                    if matches!(&self.scope, Scope::Coll { coll, .. } if coll != fc) {
                        return Vec::new();
                    }
                    let to = o.get_str("to").unwrap_or_default();
                    let (tdb, tc) = to.split_once('.').unwrap_or((to, ""));
                    ev.insert("operationType", "rename");
                    ev.insert("clusterTime", ts);
                    ev.insert("wallTime", wall);
                    ev.insert("ns", doc! {"db": db, "coll": fc});
                    ev.insert("to", doc! {"db": tdb, "coll": tc});
                    out.push(ev);
                    if matches!(self.scope, Scope::Coll { .. }) {
                        out.push(self.invalidate(ts));
                    }
                    return out;
                }
                if o.contains_key("dropDatabase") {
                    if matches!(self.scope, Scope::Coll { .. }) {
                        return Vec::new();
                    }
                    ev.insert("operationType", "dropDatabase");
                    ev.insert("clusterTime", ts);
                    ev.insert("wallTime", wall);
                    ev.insert("ns", doc! {"db": db});
                    out.push(ev);
                    if matches!(self.scope, Scope::Db(_)) {
                        out.push(self.invalidate(ts));
                    }
                    return out;
                }
                if let Ok(c) = o.get_str("create") {
                    if !self.expanded || matches!(&self.scope, Scope::Coll { coll, .. } if coll != c) {
                        return Vec::new();
                    }
                    ev.insert("operationType", "create");
                    ev.insert("clusterTime", ts);
                    ev.insert("wallTime", wall);
                    ev.insert("ns", doc! {"db": db, "coll": c});
                    return vec![ev];
                }
                return Vec::new();
            }
            _ => return Vec::new(),
        }
        if let Some(l) = e.get("lsid") {
            ev.insert("lsid", l.clone());
        }
        if let Some(t) = e.get("txnNumber") {
            ev.insert("txnNumber", t.clone());
        }
        if self.expanded {
            if let Some(u) = e.get("ui") {
                ev.insert("collectionUUID", u.clone());
            }
            if let Ok(ud) = ev.get_document_mut("updateDescription") {
                if !ud.contains_key("disambiguatedPaths") {
                    ud.insert("disambiguatedPaths", Document::new());
                }
            }
        }
        vec![ev]
    }

    fn invalidate(&mut self, ts: bson::Timestamp) -> Document {
        self.invalidated = true;
        doc! {"_id": token(ts), "operationType": "invalidate", "clusterTime": ts}
    }

    fn ingest(&mut self, srv: &Server, e: &Document) {
        if let Some(Bson::Timestamp(ts)) = e.get("ts") {
            if !ts_gt(*ts, self.last_ts) {
                return;
            }
            self.last_ts = *ts;
        }
        if self.invalidated {
            return;
        }
        for ev in self.convert(srv, e) {
            self.pending.push_back(ev);
        }
    }

    fn catch_up_from_oplog(&mut self, srv: &Server) -> Result<()> {
        for e in srv.store.oplog_since(Some(self.last_ts))? {
            self.ingest(srv, &e);
        }
        Ok(())
    }

    /// Drain buffered broadcast events without waiting.
    fn poll(&mut self, srv: &Server) -> Result<()> {
        loop {
            match self.rx.try_recv() {
                Ok(e) => self.ingest(srv, &e),
                Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => return Ok(()),
                Err(TryRecvError::Lagged(_)) => self.catch_up_from_oplog(srv)?,
            }
        }
    }

    /// Apply the user pipeline to pending events, returning up to `n` results.
    fn take_batch(&mut self, n: Option<usize>) -> Result<Vec<Bson>> {
        let limit = n.unwrap_or(usize::MAX);
        let mut evs = Vec::new();
        while evs.len() < limit {
            let Some(e) = self.pending.pop_front() else { break };
            evs.push(e);
        }
        if let Some(last) = evs.last() {
            if let Ok(t) = last.get_document("_id") {
                self.last_token = t.clone();
            }
        }
        if self.stages.is_empty() {
            return Ok(evs.into_iter().map(Bson::Document).collect());
        }
        let opts = Arc::new(AggOptions {
            db: self.db.clone(),
            coll: None,
            allow_disk_use: false,
            max_memory_bytes: crate::cmd::crud::MAX_SORT_BYTES,
            vars: Vars::new(),
            ctx: Arc::new(NoCtx),
        });
        let it = pipeline::run_pipeline(Box::new(evs.into_iter().map(Ok)), &self.stages, opts)?;
        it.map(|r| r.map(Bson::Document)).collect()
    }

    fn pbrt(&self, srv: &Server) -> Document {
        if self.pending.is_empty() {
            let ct = srv.store.cluster_time();
            if ts_gt(ct, parse_token(&self.last_token).unwrap_or(ct)) {
                return token(self.last_ts.max_ts(ct));
            }
        }
        self.last_token.clone()
    }
}

trait MaxTs {
    fn max_ts(self, o: bson::Timestamp) -> bson::Timestamp;
}
impl MaxTs for bson::Timestamp {
    fn max_ts(self, o: bson::Timestamp) -> bson::Timestamp {
        if ts_gt(o, self) { o } else { self }
    }
}

/// Change-stream post-processing has no collection access.
struct NoCtx;
impl crate::agg::pipeline::AggContext for NoCtx {
    fn scan(&self, _: &str, _: &str) -> Result<crate::agg::pipeline::DocIter> {
        Err(Error::new(40324, "stage not allowed in a change stream pipeline"))
    }
    fn find(&self, _: &str, _: &str, _: &Document) -> Result<Vec<Document>> {
        Err(Error::new(40324, "stage not allowed in a change stream pipeline"))
    }
    fn replace_collection(&self, _: &str, _: &str, _: Vec<Document>) -> Result<()> {
        Err(Error::new(40324, "stage not allowed in a change stream pipeline"))
    }
    fn insert(&self, _: &str, _: &str, _: Document) -> Result<()> {
        Err(Error::new(40324, "stage not allowed in a change stream pipeline"))
    }
    fn replace_by_id(&self, _: &str, _: &str, _: &Bson, _: Document) -> Result<()> {
        Err(Error::new(40324, "stage not allowed in a change stream pipeline"))
    }
    fn coll_stats(&self, _: &str, _: &str, _: &Document) -> Result<Document> {
        Err(Error::new(40324, "stage not allowed in a change stream pipeline"))
    }
    fn index_stats(&self, _: &str, _: &str) -> Result<Vec<Document>> {
        Err(Error::new(40324, "stage not allowed in a change stream pipeline"))
    }
    fn temp_dir(&self) -> std::path::PathBuf {
        std::env::temp_dir()
    }
}

const ALLOWED: &[&str] = &["$match", "$project", "$addFields", "$set", "$unset", "$replaceRoot", "$replaceWith", "$redact"];

/// `aggregate` with a leading `$changeStream` stage.
pub fn open(ctx: &mut CmdCtx, coll: Option<String>, stages: &[Document], batch_size: Option<usize>) -> Result<Document> {
    let spec = stages[0].get_document("$changeStream").map_err(|_| Error::failed_to_parse("$changeStream must be an object"))?.clone();
    let rest: Vec<Document> = stages[1..].to_vec();
    for s in &rest {
        let k = s.keys().next().map(String::as_str).unwrap_or("");
        if !ALLOWED.contains(&k) {
            return Err(Error::new(31134, format!("Stage {k} is not permitted in a $changeStream pipeline")));
        }
    }
    let all = bsonutil::as_bool(spec.get("allChangesForCluster"));
    let scope = match (&coll, all) {
        (_, true) => {
            if ctx.db != "admin" {
                return Err(Error::bad_value("$changeStream may only be opened on the 'admin' database if allChangesForCluster is set"));
            }
            Scope::Cluster
        }
        (Some(c), _) => Scope::Coll { db: ctx.db.clone(), coll: c.clone() },
        (None, _) => {
            if ctx.db == "admin" {
                return Err(Error::new(73, "$changeStream may not be opened on the internal admin database"));
            }
            Scope::Db(ctx.db.clone())
        }
    };
    if let Scope::Coll { db, coll } = &scope {
        if let Some(c) = ctx.srv.store.get_coll(&format!("{db}.{coll}")) {
            if c.is_view() {
                return Err(Error::new(166, format!("Namespace {db}.{coll} is a view, not a collection")));
            }
        }
    }
    let rx = ctx.srv.store.events.subscribe();
    let resume = spec.get_document("resumeAfter").ok().or_else(|| spec.get_document("startAfter").ok());
    let start = if let Some(t) = resume {
        let ts = parse_token(t)?;
        if let Some(first) = ctx.srv.store.oplog_first_ts() {
            if ts_gt(first, ts) && ts != ts_minus_one(first) {
                return Err(Error::new(286, "Resume of change stream was not possible, as the resume point may no longer be in the oplog."));
            }
        }
        ts
    } else if let Some(Bson::Timestamp(t)) = spec.get("startAtOperationTime") {
        ts_minus_one(*t)
    } else {
        ctx.srv.store.cluster_time()
    };
    let mut cs = ChangeStream {
        scope,
        rx,
        last_ts: start,
        full_document: spec.get_str("fullDocument").unwrap_or("default").to_string(),
        before_change: spec.get_str("fullDocumentBeforeChange").unwrap_or("off").to_string(),
        expanded: bsonutil::as_bool(spec.get("showExpandedEvents")),
        stages: rest,
        pending: VecDeque::new(),
        invalidated: false,
        db: ctx.db.clone(),
        last_token: token(start),
    };
    cs.catch_up_from_oplog(ctx.srv)?;
    let ns = match &coll {
        Some(c) => format!("{}.{c}", ctx.db),
        None => format!("{}.$cmd.aggregate", ctx.db),
    };
    let batch = cs.take_batch(batch_size)?;
    let pbrt = cs.pbrt(ctx.srv);
    let mut cur = Cursor::new(ns.clone(), Box::new(std::iter::empty()));
    cur.lsid = ctx.cmd.get("lsid").cloned();
    let done = cs.invalidated && cs.pending.is_empty();
    cur.kind = CursorKind::ChangeStream(Box::new(cs));
    let id = if done { 0 } else { ctx.srv.cursors.register(cur) };
    let mut r = cursor::cursor_reply(&ns, id, batch, true);
    if let Ok(c) = r.get_document_mut("cursor") {
        c.insert("postBatchResumeToken", pbrt);
    }
    Ok(r)
}

/// `getMore` on a change stream cursor: waits up to `maxTimeMS` for events.
pub async fn maybe_get_more(srv: &Arc<Server>, _conn: &mut Conn, db: &str, body: &Document) -> Option<Result<Document>> {
    let id = match body.get("getMore") {
        Some(Bson::Int64(i)) => *i,
        _ => return None,
    };
    let mut cur = srv.cursors.take(id).ok()?;
    let CursorKind::ChangeStream(_) = &cur.kind else {
        srv.cursors.put_back(cur);
        return None;
    };
    let coll = body.get_str("collection").unwrap_or_default();
    let ns = format!("{db}.{coll}");
    if cur.ns != ns {
        let cns = cur.ns.clone();
        srv.cursors.put_back(cur);
        return Some(Err(Error::new(13, format!("Requested getMore on namespace '{ns}', but cursor belongs to a different namespace {cns}"))));
    }
    let wait = body.get("maxTimeMS").and_then(bsonutil::num_i64).unwrap_or(1000).clamp(0, 60_000) as u64;
    let batch_size = body.get("batchSize").and_then(bsonutil::num_i64).filter(|b| *b > 0).map(|b| b as usize);
    let res = async {
        let CursorKind::ChangeStream(cs) = &mut cur.kind else { unreachable!() };
        cs.poll(srv)?;
        if cs.pending.is_empty() && !cs.invalidated {
            let deadline = tokio::time::Instant::now() + Duration::from_millis(wait);
            while cs.pending.is_empty() && !cs.invalidated {
                match tokio::time::timeout_at(deadline, cs.rx.recv()).await {
                    Err(_) => break,
                    Ok(Ok(e)) => {
                        cs.ingest(srv, &e);
                        cs.poll(srv)?;
                    }
                    Ok(Err(RecvError::Lagged(_))) => cs.catch_up_from_oplog(srv)?,
                    Ok(Err(RecvError::Closed)) => break,
                }
            }
        }
        let batch = tokio::task::block_in_place(|| cs.take_batch(batch_size))?;
        let pbrt = cs.pbrt(srv);
        let done = cs.invalidated && cs.pending.is_empty();
        Ok::<_, Error>((batch, pbrt, done))
    }
    .await;
    Some(match res {
        Ok((batch, pbrt, done)) => {
            let rid = if done { 0 } else { id };
            let ns = cur.ns.clone();
            if !done {
                srv.cursors.put_back(cur);
            }
            let mut r = cursor::cursor_reply(&ns, rid, batch, false);
            if let Ok(c) = r.get_document_mut("cursor") {
                c.insert("postBatchResumeToken", pbrt);
            }
            Ok(r)
        }
        Err(e) => Err(e),
    })
}
