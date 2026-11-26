//! Server-side cursors: lazily pulled document streams with idle eviction.

use crate::agg::pipeline::DocIter;
use crate::error::{Error, Result};
use bson::{Bson, Document, doc};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Reply batches are capped near the 16 MiB BSON limit.
pub const MAX_BATCH_BYTES: usize = 16 * 1024 * 1024 - 64 * 1024;
pub const DEFAULT_FIRST_BATCH: usize = 101;

pub enum CursorKind {
    Normal,
    ChangeStream(Box<crate::changestream::ChangeStream>),
}

pub struct Cursor {
    pub id: i64,
    pub ns: String,
    pub iter: DocIter,
    pub kind: CursorKind,
    pub last_used: Instant,
    pub lsid: Option<Bson>,
    pub no_timeout: bool,
    pub created: Instant,
    pub n_returned: u64,
    pub batch_size: Option<usize>,
    /// Collation active when the cursor was opened; re-installed for every batch.
    pub collation: Option<std::sync::Arc<crate::collation::Collation>>,
}

impl Cursor {
    pub fn new(ns: String, iter: DocIter) -> Cursor {
        Cursor {
            id: 0,
            ns,
            iter,
            kind: CursorKind::Normal,
            last_used: Instant::now(),
            lsid: None,
            no_timeout: false,
            created: Instant::now(),
            n_returned: 0,
            batch_size: None,
            collation: crate::collation::current(),
        }
    }

    /// Pull up to `n` documents (bounded by bytes). Returns (batch, exhausted).
    pub fn next_batch(&mut self, n: Option<usize>) -> Result<(Vec<Bson>, bool)> {
        let _collation = crate::collation::enter(self.collation.clone());
        let limit = n.unwrap_or(usize::MAX);
        let mut out = Vec::new();
        let mut bytes = 0usize;
        while out.len() < limit {
            match self.iter.next() {
                None => return Ok((out, true)),
                Some(Err(e)) => return Err(e),
                Some(Ok(mut d)) => {
                    d.remove(crate::bsonutil::META_KEY);
                    bytes += crate::bsonutil::doc_size(&d);
                    out.push(Bson::Document(d));
                    self.n_returned += 1;
                    if bytes >= MAX_BATCH_BYTES {
                        break;
                    }
                }
            }
        }
        Ok((out, false))
    }
}

#[derive(Default)]
pub struct CursorMgr {
    map: Mutex<HashMap<i64, Cursor>>,
}

impl CursorMgr {
    pub fn register(&self, mut c: Cursor) -> i64 {
        let mut id: i64;
        let mut m = self.map.lock().expect("cursors");
        loop {
            id = (rand::random::<u64>() >> 2) as i64;
            if id != 0 && !m.contains_key(&id) {
                break;
            }
        }
        c.id = id;
        c.last_used = Instant::now();
        m.insert(id, c);
        id
    }

    /// Remove a cursor for exclusive use (re-insert with `put_back`).
    pub fn take(&self, id: i64) -> Result<Cursor> {
        self.map.lock().expect("cursors").remove(&id).ok_or_else(|| Error::new(43, format!("cursor id {id} not found")))
    }

    pub fn put_back(&self, mut c: Cursor) {
        c.last_used = Instant::now();
        self.map.lock().expect("cursors").insert(c.id, c);
    }

    pub fn kill(&self, id: i64) -> bool {
        self.map.lock().expect("cursors").remove(&id).is_some()
    }

    pub fn kill_for_session(&self, lsid: &Bson) {
        self.map.lock().expect("cursors").retain(|_, c| c.lsid.as_ref() != Some(lsid));
    }

    pub fn count(&self) -> usize {
        self.map.lock().expect("cursors").len()
    }

    /// Evict cursors idle longer than `timeout`.
    pub fn reap(&self, timeout: Duration) -> usize {
        let mut m = self.map.lock().expect("cursors");
        let before = m.len();
        m.retain(|_, c| c.no_timeout || c.last_used.elapsed() < timeout);
        before - m.len()
    }

    pub fn describe(&self) -> Vec<Document> {
        self.map
            .lock()
            .expect("cursors")
            .values()
            .map(|c| doc! {"cursorId": c.id, "ns": &c.ns, "nDocsReturned": c.n_returned as i64, "noCursorTimeout": c.no_timeout})
            .collect()
    }
}

/// `{cursor: {firstBatch|nextBatch, id, ns}, ok: 1}`
pub fn cursor_reply(ns: &str, id: i64, batch: Vec<Bson>, first: bool) -> Document {
    let key = if first { "firstBatch" } else { "nextBatch" };
    doc! {"cursor": {key: batch, "id": id, "ns": ns}, "ok": 1.0}
}

/// Serve a first batch from `cur`, registering it if more results remain.
pub fn first_batch(mgr: &CursorMgr, mut cur: Cursor, batch_size: Option<usize>, single_batch: bool) -> Result<Document> {
    let n = batch_size.unwrap_or(DEFAULT_FIRST_BATCH);
    let (batch, done) = if n == 0 { (Vec::new(), false) } else { cur.next_batch(Some(n))? };
    let ns = cur.ns.clone();
    let id = if done || single_batch { 0 } else { mgr.register(cur) };
    Ok(cursor_reply(&ns, id, batch, true))
}
