//! Storage: catalog, documents, secondary indexes, oplog on top of fjall (LSM + MVCC).
//!
//! Keyspaces:
//! * `_catalog`   — `ns` -> collection metadata (BSON)
//! * `_docs_<id>` — RecordId (u64 BE) -> raw BSON document (natural order = insertion order)
//! * `_idx_<id>`  — unique: `key -> rid`; non-unique: `key ‖ rid -> []`
//! * `_oplog`     — Timestamp (u64 BE) -> oplog entry (BSON)

use crate::bsonutil::{self, get_path};
use crate::error::{Error, Result};
use crate::keys;
use crate::collation::Collation;
use crate::planner::{Histogram, Interval, Plan};
use crate::query::matcher::Matcher;
use bson::{Bson, Document, doc, spec::BinarySubtype};
use fjall::{
    KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable, Snapshot, OptimisticWriteTx as WriteTransaction,
};
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;

pub const MAX_BSON_SIZE: usize = 16 * 1024 * 1024;

/// A record: RecordId plus decoded document.
pub type Rec = (u64, Document);
pub type RecIter = Box<dyn Iterator<Item = Result<Rec>> + Send>;
/// A record with its undecoded BSON bytes.
pub type RawRec = (u64, fjall::Slice);
pub type RawIter = Box<dyn Iterator<Item = Result<RawRec>> + Send>;

#[derive(Clone, Debug, PartialEq)]
pub enum IndexKind {
    Regular,
    Text { weights: Vec<(String, f64)> },
    Geo2dsphere,
    Geo2d,
    Hashed,
    Wildcard,
}

pub struct Index {
    pub id: u64,
    pub name: String,
    pub key: Document,
    /// (path, descending)
    pub fields: Vec<(String, bool)>,
    /// Full spec as returned by `listIndexes`.
    pub spec: Document,
    pub unique: bool,
    pub sparse: bool,
    pub partial: Option<Matcher>,
    pub kind: IndexKind,
    pub expire_after: Option<f64>,
    pub multikey: AtomicBool,
    pub ks: OptimisticTxKeyspace,
    /// Ops served (for `$indexStats`).
    pub accesses: AtomicI64,
    pub since: bson::DateTime,
    /// Index collation (string keys are collation sort keys). `None` = simple.
    pub collation: Option<Arc<Collation>>,
    /// Entry writes since the histogram was last rebuilt.
    pub mods: AtomicU64,
    /// Index fields observed holding arrays (explain `multiKeyPaths`).
    mk_paths: Mutex<std::collections::BTreeSet<String>>,
    hist: RwLock<Option<Arc<Histogram>>>,
}

impl Index {
    /// Paths that made this index multikey. Unknown after a restart: every field then.
    pub fn multikey_paths(&self) -> Vec<String> {
        if !self.multikey.load(Ordering::Relaxed) {
            return Vec::new();
        }
        let set = self.mk_paths.lock().expect("mk paths");
        if set.is_empty() {
            return self.fields.iter().map(|(f, _)| f.clone()).collect();
        }
        set.iter().cloned().collect()
    }

    pub fn histogram(&self) -> Option<Arc<Histogram>> {
        self.hist.read().expect("hist").clone()
    }

    /// Index maintains key entries (text/geo/hashed/wildcard are metadata-only and served by scans).
    pub fn has_entries(&self) -> bool {
        self.kind == IndexKind::Regular
    }
}

pub struct Coll {
    pub ns: String,
    pub db: String,
    pub name: String,
    pub id: u64,
    pub uuid: bson::Binary,
    pub options: Document,
    /// `None` for views.
    pub docs: Option<OptimisticTxKeyspace>,
    pub indexes: RwLock<Vec<Arc<Index>>>,
    pub next_rid: AtomicU64,
    pub count: AtomicI64,
    pub size: AtomicI64,
    pub validator: Option<Matcher>,
    /// Serializes index DDL against writes on this collection.
    pub ddl: RwLock<()>,
    /// Collection default collation.
    pub collation: Option<Arc<Collation>>,
}

impl Coll {
    pub fn is_view(&self) -> bool {
        self.docs.is_none()
    }
    pub fn docs(&self) -> Result<&OptimisticTxKeyspace> {
        self.docs.as_ref().ok_or_else(|| Error::new(166, format!("Namespace {} is a view, not a collection", self.ns)))
    }
    pub fn indexes(&self) -> Vec<Arc<Index>> {
        self.indexes.read().expect("indexes lock").clone()
    }
    pub fn capped(&self) -> Option<(i64, Option<i64>)> {
        if !bsonutil::as_bool(self.options.get("capped")) {
            return None;
        }
        let size = bsonutil::as_i64_opt(self.options.get("size")).unwrap_or(4096);
        let max = bsonutil::as_i64_opt(self.options.get("max")).filter(|m| *m > 0);
        Some((size, max))
    }
    pub fn text_index(&self) -> Option<Arc<Index>> {
        self.indexes().into_iter().find(|i| matches!(i.kind, IndexKind::Text { .. }))
    }
    /// `changeStreamPreAndPostImages: {enabled: true}`.
    pub fn pre_images(&self) -> bool {
        self.options.get_document("changeStreamPreAndPostImages").map(|d| bsonutil::as_bool(d.get("enabled"))).unwrap_or(false)
    }
}

/// Document write locks held by open write transactions (first writer wins).
#[derive(Default)]
pub struct LockTable {
    map: Mutex<HashMap<(u64, u64), u64>>,
    next: AtomicU64,
}

/// Releases a transaction's document locks when the transaction ends (commit, abort or drop).
pub struct LockGuard {
    table: Arc<LockTable>,
    token: u64,
    keys: Vec<(u64, u64)>,
}

impl LockGuard {
    fn acquire(&mut self, key: (u64, u64)) -> Result<()> {
        let mut m = self.table.map.lock().expect("locks");
        match m.get(&key) {
            Some(t) if *t != self.token => Err(conflict()),
            Some(_) => Ok(()),
            None => {
                m.insert(key, self.token);
                self.keys.push(key);
                Ok(())
            }
        }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        if self.keys.is_empty() {
            return;
        }
        let mut m = self.table.map.lock().expect("locks");
        for k in &self.keys {
            if m.get(k) == Some(&self.token) {
                m.remove(k);
            }
        }
    }
}

/// A write transaction plus staged oplog events and stats deltas.
pub struct Txn {
    pub tx: WriteTransaction,
    pub events: Vec<Document>,
    deltas: HashMap<u64, (Arc<Coll>, i64, i64)>,
    /// Session info stamped into oplog entries.
    pub lsid: Option<Document>,
    pub txn_number: Option<i64>,
    /// Counts toward `maxConcurrentMultiDocumentTransactions` while alive.
    pub slot: Option<crate::session::OpenTxnSlot>,
    locks: LockGuard,
}

pub struct Store {
    pub db: OptimisticTxDatabase,
    catalog_ks: OptimisticTxKeyspace,
    pub oplog_ks: OptimisticTxKeyspace,
    colls: RwLock<HashMap<String, Arc<Coll>>>,
    next_ks_id: AtomicU64,
    clock: Mutex<(u32, u32)>,
    commit_lock: Mutex<()>,
    pub events: broadcast::Sender<Arc<Document>>,
    pub dbpath: PathBuf,
    pub oplog_max_bytes: u64,
    oplog_bytes: AtomicU64,
    ddl_lock: Mutex<()>,
    locks: Arc<LockTable>,
}

pub struct StoreOptions {
    pub cache_mb: u64,
    pub oplog_mb: u64,
}

/// Lightweight defaults: 8 MiB memtables (fjall's default is 64 MiB per keyspace).
fn ks_opts() -> KeyspaceCreateOptions {
    KeyspaceCreateOptions::default().max_memtable_size(8 * 1024 * 1024)
}

fn rid_key(rid: u64) -> [u8; 8] {
    rid.to_be_bytes()
}

fn rid_from(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    let n = b.len();
    if n >= 8 {
        a.copy_from_slice(&b[n - 8..]);
    }
    u64::from_be_bytes(a)
}

pub fn split_ns(ns: &str) -> (&str, &str) {
    ns.split_once('.').unwrap_or((ns, ""))
}

pub fn new_uuid() -> bson::Binary {
    let mut b: [u8; 16] = rand::random();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    bson::Binary { subtype: BinarySubtype::Uuid, bytes: b.to_vec() }
}

pub fn validate_coll_name(db: &str, name: &str) -> Result<()> {
    if db.is_empty() || db.contains(['/', '\\', '.', ' ', '"', '$', '\0']) || db.len() > 63 {
        return Err(Error::new(73, format!("Invalid database name: '{db}'")));
    }
    if name.is_empty() || name.contains('\0') || name.starts_with('.') || name.contains('$') {
        return Err(Error::new(73, format!("Invalid collection name: {name}")));
    }
    Ok(())
}

fn ts_key(ts: bson::Timestamp) -> [u8; 8] {
    (((ts.time as u64) << 32) | ts.increment as u64).to_be_bytes()
}

pub fn ts_from_key(k: &[u8]) -> bson::Timestamp {
    let v = rid_from(k);
    bson::Timestamp { time: (v >> 32) as u32, increment: v as u32 }
}

impl Store {
    pub fn open(path: &Path, opts: StoreOptions) -> Result<Arc<Store>> {
        std::fs::create_dir_all(path)?;
        let db = OptimisticTxDatabase::builder(path.join("data"))
            .cache_size(opts.cache_mb.max(1) * 1024 * 1024)
            
            .worker_threads(2)
            .open()?;
        let catalog_ks = db.keyspace("_catalog", ks_opts)?;
        let oplog_ks = db.keyspace("_oplog", ks_opts)?;
        let (events, _) = broadcast::channel(4096);
        let store = Store {
            db,
            catalog_ks,
            oplog_ks,
            colls: RwLock::new(HashMap::new()),
            next_ks_id: AtomicU64::new(1),
            clock: Mutex::new((0, 0)),
            commit_lock: Mutex::new(()),
            events,
            dbpath: path.to_path_buf(),
            oplog_max_bytes: opts.oplog_mb.max(1) * 1024 * 1024,
            oplog_bytes: AtomicU64::new(0),
            ddl_lock: Mutex::new(()),
            locks: Arc::new(LockTable::default()),
        };
        store.load_catalog()?;
        Ok(Arc::new(store))
    }

    fn load_catalog(&self) -> Result<()> {
        let snap = self.db.read_tx();
        let mut max_id = 0;
        let mut colls = HashMap::new();
        for g in snap.iter(&self.catalog_ks) {
            let (k, v) = g.into_inner()?;
            if k.starts_with(b"meta\0") {
                continue;
            }
            let meta = bsonutil::from_bytes(&v)?;
            let coll = self.coll_from_meta(&meta)?;
            max_id = max_id.max(coll.id);
            for i in coll.indexes() {
                max_id = max_id.max(i.id);
            }
            colls.insert(coll.ns.clone(), Arc::new(coll));
        }
        // Initialize counters by scanning (count, size, next rid).
        for c in colls.values() {
            if let Some(ks) = &c.docs {
                let (mut n, mut sz, mut last) = (0i64, 0i64, 0u64);
                for g in snap.iter(ks) {
                    let (k, v) = g.into_inner()?;
                    n += 1;
                    sz += v.len() as i64;
                    last = last.max(rid_from(&k));
                }
                c.count.store(n, Ordering::Relaxed);
                c.size.store(sz, Ordering::Relaxed);
                c.next_rid.store(last + 1, Ordering::Relaxed);
            }
        }
        let mut ob = 0u64;
        let mut last_ts = bson::Timestamp { time: 0, increment: 0 };
        for g in snap.iter(&self.oplog_ks) {
            let (k, v) = g.into_inner()?;
            ob += v.len() as u64;
            last_ts = ts_from_key(&k);
        }
        self.oplog_bytes.store(ob, Ordering::Relaxed);
        *self.clock.lock().expect("clock") = (last_ts.time, last_ts.increment);
        self.next_ks_id.store(max_id + 1, Ordering::Relaxed);
        *self.colls.write().expect("catalog") = colls;
        Ok(())
    }

    fn coll_from_meta(&self, meta: &Document) -> Result<Coll> {
        let ns = meta.get_str("ns").map_err(|_| Error::internal("corrupt catalog"))?.to_string();
        let id = meta.get_i64("id").map_err(|_| Error::internal("corrupt catalog"))? as u64;
        let options = meta.get_document("options").cloned().unwrap_or_default();
        let uuid = match meta.get("uuid") {
            Some(Bson::Binary(b)) => b.clone(),
            _ => new_uuid(),
        };
        let is_view = options.contains_key("viewOn");
        let docs = if is_view { None } else { Some(self.db.keyspace(&format!("_docs_{id}"), ks_opts)?) };
        let mut indexes = Vec::new();
        if let Ok(arr) = meta.get_array("indexes") {
            for e in arr {
                if let Bson::Document(d) = e {
                    let iid = d.get_i64("id").unwrap_or(0) as u64;
                    let spec = d.get_document("spec").cloned().unwrap_or_default();
                    let multikey = d.get_bool("multikey").unwrap_or(false);
                    let idx = self.build_index(iid, spec)?;
                    idx.multikey.store(multikey, Ordering::Relaxed);
                    indexes.push(Arc::new(idx));
                }
            }
        }
        let (db, name) = split_ns(&ns);
        let validator = match options.get_document("validator") {
            Ok(v) if !v.is_empty() => Some(Matcher::new(v)?),
            _ => None,
        };
        let collation = match options.get_document("collation") {
            Ok(c) => Collation::parse(c)?,
            Err(_) => None,
        };
        Ok(Coll {
            db: db.to_string(),
            name: name.to_string(),
            ns: ns.clone(),
            id,
            uuid,
            options,
            docs,
            indexes: RwLock::new(indexes),
            next_rid: AtomicU64::new(1),
            count: AtomicI64::new(0),
            size: AtomicI64::new(0),
            validator,
            ddl: RwLock::new(()),
            collation,
        })
    }

    fn coll_meta(&self, c: &Coll) -> Document {
        let idx: Vec<Bson> = c
            .indexes()
            .iter()
            .map(|i| {
                Bson::Document(doc! {"id": i.id as i64, "spec": i.spec.clone(), "multikey": i.multikey.load(Ordering::Relaxed)})
            })
            .collect();
        doc! {"ns": &c.ns, "id": c.id as i64, "uuid": Bson::Binary(c.uuid.clone()), "options": c.options.clone(), "indexes": idx}
    }

    fn persist_meta(&self, c: &Coll) -> Result<()> {
        self.catalog_ks.insert(c.ns.as_bytes(), bsonutil::to_bytes(&self.coll_meta(c)))?;
        Ok(())
    }

    /// Persist the multikey flag after it flips.
    pub fn persist_multikey(&self, c: &Coll) {
        let _ = self.persist_meta(c);
    }

    pub fn build_index(&self, id: u64, spec: Document) -> Result<Index> {
        let key = spec.get_document("key").map_err(|_| Error::new(9, "index specification must contain a 'key' document"))?.clone();
        let name = spec.get_str("name").map_err(|_| Error::new(9, "index specification must have a 'name'"))?.to_string();
        let mut fields = Vec::new();
        let mut kind = IndexKind::Regular;
        for (k, v) in &key {
            match v {
                Bson::String(s) if s == "text" => {
                    let mut w: Vec<(String, f64)> = Vec::new();
                    if let Ok(ws) = spec.get_document("weights") {
                        for (f, wv) in ws {
                            w.push((f.clone(), bsonutil::num_f64(wv).unwrap_or(1.0)));
                        }
                    }
                    if k != "_fts" && !w.iter().any(|(f, _)| f == k) {
                        w.push((k.clone(), 1.0));
                    }
                    if let IndexKind::Text { weights } = &mut kind {
                        for e in w {
                            if !weights.iter().any(|(f, _)| *f == e.0) {
                                weights.push(e);
                            }
                        }
                    } else {
                        kind = IndexKind::Text { weights: w };
                    }
                }
                Bson::String(s) if s == "2dsphere" => kind = IndexKind::Geo2dsphere,
                Bson::String(s) if s == "2d" => kind = IndexKind::Geo2d,
                Bson::String(s) if s == "hashed" => kind = IndexKind::Hashed,
                Bson::String(s) => {
                    return Err(Error::new(67, format!("Unknown index plugin '{s}'")));
                }
                _ if k.ends_with("$**") || k == "$**" => kind = IndexKind::Wildcard,
                n if bsonutil::is_number(n) => {
                    let f = bsonutil::num_f64(n).unwrap_or(1.0);
                    if f == 0.0 {
                        return Err(Error::new(67, format!("Values in the index key pattern can't be 0: {key}")));
                    }
                    fields.push((k.clone(), f < 0.0));
                }
                _ => return Err(Error::new(67, format!("Values in v:2 index key pattern cannot be of type {}. Only numbers > 0, numbers < 0, and strings are allowed.", bsonutil::type_name(v)))),
            }
        }
        if key.is_empty() {
            return Err(Error::new(67, "Index keys cannot be empty."));
        }
        let partial = match spec.get_document("partialFilterExpression") {
            Ok(p) => Some(Matcher::new(p)?),
            Err(_) => None,
        };
        let ks = self.db.keyspace(&format!("_idx_{id}"), ks_opts)?;
        let index_collation = match spec.get_document("collation") {
            Ok(c) => Collation::parse(c)?,
            Err(_) => None,
        };
        Ok(Index {
            id,
            name,
            fields,
            unique: bsonutil::as_bool(spec.get("unique")) || key == doc! {"_id": 1} || key == doc! {"_id": 1_i64} || key == doc! {"_id": 1.0},
            sparse: bsonutil::as_bool(spec.get("sparse")),
            partial,
            expire_after: spec.get("expireAfterSeconds").and_then(bsonutil::num_f64),
            kind,
            key,
            spec,
            multikey: AtomicBool::new(false),
            ks,
            accesses: AtomicI64::new(0),
            since: bson::DateTime::now(),
            collation: index_collation,
            mods: AtomicU64::new(0),
            mk_paths: Mutex::new(Default::default()),
            hist: RwLock::new(None),
        })
    }

    // ---------------- server metadata ----------------

    /// Read a server metadata document (search indexes, query settings, ...).
    pub fn get_meta(&self, key: &str) -> Result<Option<Document>> {
        match self.catalog_ks.get(format!("meta\0{key}"))? {
            Some(v) => Ok(Some(bsonutil::from_bytes(&v)?)),
            None => Ok(None),
        }
    }

    /// Write (or with `None`, delete) a server metadata document.
    pub fn put_meta(&self, key: &str, doc: Option<&Document>) -> Result<()> {
        let k = format!("meta\0{key}");
        match doc {
            Some(d) => self.catalog_ks.insert(k, bsonutil::to_bytes(d))?,
            None => self.catalog_ks.remove(k)?,
        }
        Ok(())
    }

    // ---------------- catalog ----------------

    pub fn get_coll(&self, ns: &str) -> Option<Arc<Coll>> {
        self.colls.read().expect("catalog").get(ns).cloned()
    }

    pub fn list_colls(&self, db: Option<&str>) -> Vec<Arc<Coll>> {
        let mut v: Vec<_> = self
            .colls
            .read()
            .expect("catalog")
            .values()
            .filter(|c| db.is_none_or(|d| c.db == d))
            .cloned()
            .collect();
        v.sort_by(|a, b| a.ns.cmp(&b.ns));
        v
    }

    pub fn list_dbs(&self) -> Vec<String> {
        let mut v: Vec<String> = self.colls.read().expect("catalog").values().map(|c| c.db.clone()).collect();
        v.sort();
        v.dedup();
        v
    }

    /// Create a collection (or view). Errors 48 NamespaceExists if present.
    pub fn create_coll(&self, ns: &str, options: Document) -> Result<Arc<Coll>> {
        let _g = self.ddl_lock.lock().expect("ddl");
        let (db, name) = split_ns(ns);
        validate_coll_name(db, name)?;
        if self.get_coll(ns).is_some() {
            return Err(Error::new(48, format!("Collection {ns} already exists.")));
        }
        self.create_locked(ns, options)
    }

    fn create_locked(&self, ns: &str, mut options: Document) -> Result<Arc<Coll>> {
        // Store the fully specified collation; `{locale: "simple"}` means none.
        if let Ok(c) = options.get_document("collation") {
            match Collation::parse(c)? {
                Some(col) => {
                    options.insert("collation", col.to_doc());
                }
                None => {
                    options.remove("collation");
                }
            }
        }
        let id = self.next_ks_id.fetch_add(1, Ordering::Relaxed);
        let is_view = options.contains_key("viewOn");
        let mut indexes = Vec::new();
        let is_ts = options.contains_key("timeseries");
        if let Some(spec) = crate::timeseries::default_index(&options) {
            let iid = self.next_ks_id.fetch_add(1, Ordering::Relaxed);
            indexes.push(doc! {"id": iid as i64, "spec": spec});
        }
        if !is_view && !is_ts {
            let iid = self.next_ks_id.fetch_add(1, Ordering::Relaxed);
            let mut spec = doc! {"v": 2, "key": {"_id": 1}, "name": "_id_"};
            if let Ok(c) = options.get_document("collation") {
                spec.insert("collation", c.clone());
            }
            indexes.push(doc! {"id": iid as i64, "spec": spec});
        }
        let meta = doc! {"ns": ns, "id": id as i64, "uuid": Bson::Binary(new_uuid()), "options": options, "indexes": indexes};
        if is_view {
            self.put_meta(&format!("sysviews.{}", split_ns(ns).0), Some(&doc! {"v": 1}))?;
        }
        let coll = Arc::new(self.coll_from_meta(&meta)?);
        self.persist_meta(&coll)?;
        self.colls.write().expect("catalog").insert(ns.to_string(), coll.clone());
        let (db, name) = split_ns(ns);
        self.log_command(db, doc! {"create": name})?;
        Ok(coll)
    }

    /// Get or implicitly create a collection for writes.
    pub fn coll_for_write(&self, ns: &str) -> Result<Arc<Coll>> {
        if let Some(c) = self.get_coll(ns) {
            if c.is_view() {
                return Err(Error::new(166, format!("Namespace {ns} is a view, not a collection")));
            }
            return Ok(c);
        }
        let _g = self.ddl_lock.lock().expect("ddl");
        if let Some(c) = self.get_coll(ns) {
            return Ok(c);
        }
        let (db, name) = split_ns(ns);
        validate_coll_name(db, name)?;
        self.create_locked(ns, Document::new())
    }

    pub fn drop_coll(&self, ns: &str) -> Result<bool> {
        let _g = self.ddl_lock.lock().expect("ddl");
        let Some(c) = self.colls.write().expect("catalog").remove(ns) else {
            return Ok(false);
        };
        let _w = c.ddl.write().expect("ddl");
        self.catalog_ks.remove(ns.as_bytes())?;
        if let Some(ks) = &c.docs {
            self.db.inner().delete_keyspace(ks.inner().clone())?;
        }
        for i in c.indexes() {
            self.db.inner().delete_keyspace(i.ks.inner().clone())?;
        }
        self.log_command(&c.db, doc! {"drop": &c.name})?;
        Ok(true)
    }

    /// Drop every collection and view of `db`, then log a `dropDatabase` oplog entry.
    /// `system.views` exists once a view was ever created in `db` (until the db is dropped).
    pub fn has_system_views(&self, db: &str) -> bool {
        self.list_colls(Some(db)).iter().any(|c| c.is_view()) || self.get_meta(&format!("sysviews.{db}")).ok().flatten().is_some()
    }

    pub fn drop_database(&self, db: &str) -> Result<()> {
        self.put_meta(&format!("sysviews.{db}"), None)?;
        for c in self.list_colls(Some(db)) {
            self.drop_coll(&c.ns)?;
        }
        self.log_command(db, doc! {"dropDatabase": 1})
    }

    pub fn rename_coll(&self, from: &str, to: &str, drop_target: bool) -> Result<()> {
        if self.get_coll(to).is_some() {
            if !drop_target {
                return Err(Error::new(48, "target namespace exists"));
            }
            self.drop_coll(to)?;
        }
        let _g = self.ddl_lock.lock().expect("ddl");
        let (db, name) = split_ns(to);
        validate_coll_name(db, name)?;
        let c = self.colls.write().expect("catalog").remove(from).ok_or_else(|| Error::ns_not_found("source namespace does not exist"))?;
        let mut meta = self.coll_meta(&c);
        meta.insert("ns", to);
        self.catalog_ks.remove(from.as_bytes())?;
        self.catalog_ks.insert(to.as_bytes(), bsonutil::to_bytes(&meta))?;
        let nc = self.coll_from_meta(&meta)?;
        nc.count.store(c.count.load(Ordering::Relaxed), Ordering::Relaxed);
        nc.size.store(c.size.load(Ordering::Relaxed), Ordering::Relaxed);
        nc.next_rid.store(c.next_rid.load(Ordering::Relaxed), Ordering::Relaxed);
        for (a, b) in nc.indexes().iter().zip(c.indexes().iter()) {
            a.multikey.store(b.multikey.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        self.colls.write().expect("catalog").insert(to.to_string(), Arc::new(nc));
        let (fdb, fname) = split_ns(from);
        self.log_command(fdb, doc! {"renameCollection": format!("{fdb}.{fname}"), "to": to})?;
        Ok(())
    }

    /// Replace collection options (collMod) and recompile validator.
    pub fn set_coll_options(&self, ns: &str, options: Document) -> Result<()> {
        let _g = self.ddl_lock.lock().expect("ddl");
        let c = self.get_coll(ns).ok_or_else(|| Error::ns_not_found(format!("ns does not exist: {ns}")))?;
        let mut meta = self.coll_meta(&c);
        meta.insert("options", options);
        let nc = self.coll_from_meta(&meta)?;
        nc.count.store(c.count.load(Ordering::Relaxed), Ordering::Relaxed);
        nc.size.store(c.size.load(Ordering::Relaxed), Ordering::Relaxed);
        nc.next_rid.store(c.next_rid.load(Ordering::Relaxed), Ordering::Relaxed);
        for (a, b) in nc.indexes().iter().zip(c.indexes().iter()) {
            a.multikey.store(b.multikey.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        self.persist_meta(&nc)?;
        self.colls.write().expect("catalog").insert(ns.to_string(), Arc::new(nc));
        Ok(())
    }

    /// Set `expireAfterSeconds` / `hidden` etc. on an index spec (collMod index).
    pub fn modify_index(&self, ns: &str, name_or_key: &Bson, changes: &Document) -> Result<Document> {
        let _g = self.ddl_lock.lock().expect("ddl");
        let c = self.get_coll(ns).ok_or_else(|| Error::ns_not_found(format!("ns does not exist: {ns}")))?;
        let mut idxs = c.indexes.write().expect("indexes");
        let pos = idxs
            .iter()
            .position(|i| match name_or_key {
                Bson::String(n) => &i.name == n,
                Bson::Document(k) => &i.key == k,
                _ => false,
            })
            .ok_or_else(|| Error::new(27, "cannot find index"))?;
        let old = idxs[pos].clone();
        let mut spec = old.spec.clone();
        let mut out = Document::new();
        for (k, v) in changes {
            if let Some(prev) = spec.get(k) {
                out.insert(format!("{k}_old"), prev.clone());
            }
            out.insert(format!("{k}_new"), v.clone());
            spec.insert(k.clone(), v.clone());
        }
        let ni = self.build_index(old.id, spec)?;
        ni.multikey.store(old.multikey.load(Ordering::Relaxed), Ordering::Relaxed);
        idxs[pos] = Arc::new(ni);
        drop(idxs);
        self.persist_meta(&c)?;
        Ok(out)
    }

    /// Build a new index over existing data. Returns false if an identical index exists.
    pub fn create_index(&self, ns: &str, mut spec: Document) -> Result<bool> {
        let c = self.coll_for_write(ns)?;
        // Indexes inherit the collection default collation; store fully specified, simple = absent.
        match spec.get_document("collation") {
            Ok(cd) => match Collation::parse(cd)? {
                Some(col) => {
                    spec.insert("collation", col.to_doc());
                }
                None => {
                    spec.remove("collation");
                }
            },
            Err(_) => {
                if let Some(col) = &c.collation {
                    spec.insert("collation", col.to_doc());
                }
            }
        }
        let _g = self.ddl_lock.lock().expect("ddl");
        let _w = c.ddl.write().expect("ddl");
        let key = spec.get_document("key").map_err(|_| Error::new(9, "index spec missing key"))?;
        let name = spec.get_str("name").map_err(|_| Error::new(9, "index spec missing name"))?;
        for i in c.indexes() {
            if i.name == name {
                if &i.key == key {
                    return Ok(false);
                }
                return Err(Error::new(86, format!("An existing index has the same name as the requested index. When index names are not specified, they are auto generated and can cause conflicts. Please refer to our documentation. Requested index: {spec}, existing index: {}", i.spec)));
            }
            if &i.key == key && i.spec.get("collation") == spec.get("collation") {
                let same = i.unique == bsonutil::as_bool(spec.get("unique")) && i.sparse == bsonutil::as_bool(spec.get("sparse"));
                if same && i.spec.get("partialFilterExpression") == spec.get("partialFilterExpression") {
                    return Ok(false);
                }
                return Err(Error::new(85, format!("An equivalent index already exists with a different name and options. Requested index: {spec}, existing index: {}", i.spec)));
            }
        }
        let id = self.next_ks_id.fetch_add(1, Ordering::Relaxed);
        let idx = Arc::new(self.build_index(id, spec)?);
        if idx.has_entries() {
            // Backfill in bounded batches.
            let snap = self.db.read_tx();
            let mut txn = self.begin();
            let mut n = 0;
            let mut seen_unique: HashMap<Vec<u8>, u64> = HashMap::new();
            for g in snap.iter(c.docs()?) {
                let (k, v) = g.into_inner()?;
                let rid = rid_from(&k);
                let d = bsonutil::from_bytes(&v)?;
                if let Err(e) = self.index_insert(&mut txn.tx, &c, &idx, rid, &d, &mut seen_unique) {
                    txn.tx.rollback();
                    self.db.inner().delete_keyspace(idx.ks.inner().clone())?;
                    return Err(e);
                }
                n += 1;
                if n % 2000 == 0 {
                    let full = std::mem::replace(&mut txn, self.begin());
                    full.tx.commit()?.map_err(|_| conflict())?;
                }
            }
            txn.tx.commit()?.map_err(|_| conflict())?;
        }
        c.indexes.write().expect("indexes").push(idx);
        self.persist_meta(&c)?;
        Ok(true)
    }

    pub fn drop_index(&self, ns: &str, name: &str) -> Result<()> {
        let _g = self.ddl_lock.lock().expect("ddl");
        let c = self.get_coll(ns).ok_or_else(|| Error::ns_not_found(format!("ns not found {ns}")))?;
        let _w = c.ddl.write().expect("ddl");
        if name == "_id_" {
            return Err(Error::new(72, "cannot drop _id index"));
        }
        let mut idxs = c.indexes.write().expect("indexes");
        let pos = idxs.iter().position(|i| i.name == name).ok_or_else(|| Error::new(27, format!("index not found with name [{name}]")))?;
        let i = idxs.remove(pos);
        drop(idxs);
        self.db.inner().delete_keyspace(i.ks.inner().clone())?;
        self.persist_meta(&c)?;
        Ok(())
    }

    // ---------------- transactions ----------------

    pub fn begin(&self) -> Txn {
        Txn {
            tx: self.db.write_tx().expect("write tx"),
            events: Vec::new(),
            deltas: HashMap::new(),
            lsid: None,
            txn_number: None,
            slot: None,
            locks: LockGuard { table: self.locks.clone(), token: self.locks.next.fetch_add(1, Ordering::Relaxed) + 1, keys: Vec::new() },
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.db.read_tx()
    }

    pub fn next_ts(&self) -> bson::Timestamp {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as u32).unwrap_or(0);
        let mut c = self.clock.lock().expect("clock");
        if now > c.0 {
            *c = (now, 1);
        } else {
            c.1 += 1;
        }
        bson::Timestamp { time: c.0, increment: c.1 }
    }

    pub fn cluster_time(&self) -> bson::Timestamp {
        let c = self.clock.lock().expect("clock");
        bson::Timestamp { time: c.0, increment: c.1 }
    }

    /// Commit: stamp oplog entries, write them in the same transaction, then broadcast.
    pub fn commit(&self, mut txn: Txn) -> Result<Option<bson::Timestamp>> {
        let _g = self.commit_lock.lock().expect("commit");
        let mut committed = Vec::with_capacity(txn.events.len());
        let mut last = None;
        let wall = bson::DateTime::now();
        let n = txn.events.len();
        for (i, mut e) in std::mem::take(&mut txn.events).into_iter().enumerate() {
            let ts = self.next_ts();
            e.insert("ts", ts);
            e.insert("wall", wall);
            if let Some(l) = &txn.lsid {
                e.insert("lsid", l.clone());
            }
            if let Some(t) = txn.txn_number {
                e.insert("txnNumber", t);
                e.insert("txnOpIndex", i as i64);
                e.insert("txnOps", n as i64);
            }
            let bytes = bsonutil::to_bytes(&e);
            self.oplog_bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
            txn.tx.insert(&self.oplog_ks, ts_key(ts), bytes);
            last = Some(ts);
            committed.push(Arc::new(e));
        }
        match txn.tx.commit()? {
            Ok(()) => {}
            Err(_) => return Err(conflict()),
        }
        for (c, dn, ds) in txn.deltas.into_values() {
            c.count.fetch_add(dn, Ordering::Relaxed);
            c.size.fetch_add(ds, Ordering::Relaxed);
        }
        for e in committed {
            let _ = self.events.send(e);
        }
        Ok(last)
    }

    /// Run `f` in a fresh transaction and commit, retrying write conflicts.
    pub fn with_retry<T>(&self, mut f: impl FnMut(&mut Txn) -> Result<T>) -> Result<T> {
        let mut attempt = 0;
        loop {
            let mut txn = self.begin();
            let out = match f(&mut txn) {
                Ok(v) => v,
                Err(e) => {
                    txn.tx.rollback();
                    if e.code == 112 && attempt < 2000 {
                        attempt += 1;
                        std::thread::sleep(std::time::Duration::from_micros((50u64 << attempt.min(8)).min(5_000)));
                        continue;
                    }
                    return Err(e);
                }
            };
            match self.commit(txn) {
                Ok(_) => return Ok(out),
                Err(e) if e.code == 112 && attempt < 2000 => {
                    attempt += 1;
                    std::thread::sleep(std::time::Duration::from_micros((50u64 << attempt.min(8)).min(5_000)));
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn log_command(&self, db: &str, o: Document) -> Result<()> {
        let mut txn = self.begin();
        txn.events.push(doc! {"op": "c", "ns": format!("{db}.$cmd"), "o": o});
        self.commit(txn)?;
        Ok(())
    }

    // ---------------- document writes ----------------

    fn delta(txn: &mut Txn, c: &Arc<Coll>, dn: i64, ds: i64) {
        let e = txn.deltas.entry(c.id).or_insert_with(|| (c.clone(), 0, 0));
        e.1 += dn;
        e.2 += ds;
    }

    /// Insert a document (adds `_id` if missing). Returns the stored document.
    pub fn insert(&self, txn: &mut Txn, c: &Arc<Coll>, mut d: Document) -> Result<Document> {
        let _r = c.ddl.read().expect("ddl");
        let docs = c.docs()?;
        prepare_for_insert(&mut d)?;
        if let Some(ts) = crate::timeseries::spec(&c.options) {
            d = crate::timeseries::prepare_insert(&ts, d)?;
        }
        if let Some(v) = &c.validator {
            validate(c, v, &d)?;
        }
        let bytes = bsonutil::to_bytes(&d);
        if bytes.len() > MAX_BSON_SIZE {
            return Err(Error::new(10334, format!("BSONObj size: {} (0x{:x}) is invalid. Size must be between 0 and 16793600(16MB)", bytes.len(), bytes.len())));
        }
        let rid = c.next_rid.fetch_add(1, Ordering::Relaxed);
        let mut seen = HashMap::new();
        for idx in c.indexes() {
            self.index_insert(&mut txn.tx, c, &idx, rid, &d, &mut seen)?;
        }
        Self::delta(txn, c, 1, bytes.len() as i64);
        txn.tx.insert(docs, rid_key(rid), bytes);
        txn.events.push(doc! {"op": "i", "ns": &c.ns, "ui": Bson::Binary(c.uuid.clone()), "o": d.clone(), "o2": {"_id": d.get("_id").cloned().unwrap_or(Bson::Null)}});
        if let Some((size, max)) = c.capped() {
            self.cap_trim(txn, c, size, max)?;
        }
        Ok(d)
    }

    fn cap_trim(&self, txn: &mut Txn, c: &Arc<Coll>, size: i64, max: Option<i64>) -> Result<()> {
        let (dn, ds) = txn.deltas.get(&c.id).map(|e| (e.1, e.2)).unwrap_or((0, 0));
        let mut count = c.count.load(Ordering::Relaxed) + dn;
        let mut bytes = c.size.load(Ordering::Relaxed) + ds;
        if count <= max.unwrap_or(i64::MAX) && bytes <= size {
            return Ok(());
        }
        let docs = c.docs()?;
        let mut victims = Vec::new();
        for g in txn.tx.iter(docs) {
            if count <= max.unwrap_or(i64::MAX) && bytes <= size || count <= 1 {
                break;
            }
            let (k, v) = g.into_inner()?;
            count -= 1;
            bytes -= v.len() as i64;
            victims.push((rid_from(&k), bsonutil::from_bytes(&v)?));
        }
        for (rid, d) in victims {
            self.delete(txn, c, rid, &d)?;
        }
        Ok(())
    }

    /// Replace the document at `rid` (old → new), maintaining indexes.
    pub fn update(&self, txn: &mut Txn, c: &Arc<Coll>, rid: u64, old: &Document, new: Document, event: Document) -> Result<()> {
        let _r = c.ddl.read().expect("ddl");
        let docs = c.docs()?;
        txn.locks.acquire((c.id, rid))?;
        if let Some(v) = &c.validator {
            if c.options.get_str("validationLevel").unwrap_or("strict") != "moderate" || v.matches(old).unwrap_or(false) {
                validate(c, v, &new)?;
            }
        }
        let bytes = bsonutil::to_bytes(&new);
        if bytes.len() > MAX_BSON_SIZE {
            return Err(Error::new(17419, format!("Resulting document after update is larger than {MAX_BSON_SIZE}")));
        }
        for idx in c.indexes() {
            if !idx.has_entries() {
                continue;
            }
            let old_keys = self.index_keys(c, &idx, old)?;
            let new_keys = self.index_keys(c, &idx, &new)?;
            for (k, _) in old_keys.iter().filter(|(k, _)| !new_keys.iter().any(|(n, _)| n == k)) {
                txn.tx.remove(&idx.ks, self.entry_key(&idx, k, rid));
                idx.mods.fetch_add(1, Ordering::Relaxed);
            }
            // New keys, and keys whose type bits changed (e.g. 1 -> 1.0).
            let added: Vec<(Vec<u8>, Vec<u8>)> = new_keys.iter().filter(|e| !old_keys.contains(e)).cloned().collect();
            self.put_keys(&mut txn.tx, c, &idx, rid, &new, added)?;
        }
        let old_len = bsonutil::doc_size(old) as i64;
        Self::delta(txn, c, 0, bytes.len() as i64 - old_len);
        txn.tx.insert(docs, rid_key(rid), bytes);
        let mut ev = event;
        ev.insert("op", "u");
        ev.insert("ns", &c.ns);
        ev.insert("ui", Bson::Binary(c.uuid.clone()));
        ev.insert("o2", doc! {"_id": new.get("_id").cloned().unwrap_or(Bson::Null)});
        if ev.get_bool("repl").unwrap_or(false) {
            ev.insert("full", new);
        }
        if c.pre_images() {
            ev.insert("pre", old.clone());
        }
        txn.events.push(ev);
        Ok(())
    }

    pub fn delete(&self, txn: &mut Txn, c: &Arc<Coll>, rid: u64, old: &Document) -> Result<()> {
        let _r = c.ddl.try_read();
        let docs = c.docs()?;
        txn.locks.acquire((c.id, rid))?;
        for idx in c.indexes() {
            if !idx.has_entries() {
                continue;
            }
            for (k, _) in self.index_keys(c, &idx, old)? {
                txn.tx.remove(&idx.ks, self.entry_key(&idx, &k, rid));
                idx.mods.fetch_add(1, Ordering::Relaxed);
            }
        }
        Self::delta(txn, c, -1, -(bsonutil::doc_size(old) as i64));
        txn.tx.remove(docs, rid_key(rid));
        let mut ev = doc! {"op": "d", "ns": &c.ns, "ui": Bson::Binary(c.uuid.clone()), "o": {"_id": old.get("_id").cloned().unwrap_or(Bson::Null)}};
        if c.pre_images() {
            ev.insert("pre", old.clone());
        }
        txn.events.push(ev);
        Ok(())
    }

    fn entry_key(&self, idx: &Index, k: &[u8], rid: u64) -> Vec<u8> {
        if idx.unique {
            k.to_vec()
        } else {
            let mut v = k.to_vec();
            v.extend_from_slice(&rid_key(rid));
            v
        }
    }

    /// Encoded keys (without rid suffix) a document contributes to `idx`. Empty if excluded (sparse/partial).
    pub fn index_keys(&self, c: &Coll, idx: &Index, d: &Document) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if idx.kind == IndexKind::Geo2dsphere {
            for (f, kind) in &idx.key {
                if kind.as_str() != Some("2dsphere") {
                    continue;
                }
                if let Some(v) = get_path(d, f) {
                    if let Err((reason, elem)) = crate::geo::check_2dsphere(v) {
                        let id = d.get("_id").map(crate::cmd::admin::shell_repr).unwrap_or_default();
                        return Err(Error::new(510, format!("Could not extract geo keys at path '{f}' for document with _id {id}: {reason}"))
                            .with_extra(doc! {"errInfo": {"failingPath": f.as_str(), "underlyingCode": 2, "underlyingReason": reason, "failingElement": elem}}));
                    }
                }
            }
        }
        if !idx.has_entries() {
            return Ok(Vec::new());
        }
        if let Some(p) = &idx.partial {
            if !p.matches(d)? {
                return Ok(Vec::new());
            }
        }
        if idx.sparse && !idx.fields.iter().any(|(f, _)| path_exists(d, f)) {
            return Ok(Vec::new());
        }
        let mut per_field: Vec<Vec<Bson>> = Vec::with_capacity(idx.fields.len());
        let mut present: Vec<bool> = Vec::with_capacity(idx.fields.len());
        let mut array_field: Option<&str> = None;
        for (f, _) in &idx.fields {
            present.push(path_exists(d, f));
            let (vals, is_arr) = index_values(d, f);
            if is_arr {
                if let Some(prev) = array_field {
                    return Err(Error::new(171, format!("cannot index parallel arrays [{f}] [{prev}]")));
                }
                array_field = Some(f);
                idx.mk_paths.lock().expect("mk paths").insert(f.clone());
                if !idx.multikey.swap(true, Ordering::Relaxed) {
                    self.persist_multikey(c);
                }
            }
            per_field.push(vals);
        }
        let coll = idx.collation.as_deref();
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = vec![(Vec::new(), Vec::new())];
        for ((vals, (_, desc)), here) in per_field.iter().zip(&idx.fields).zip(&present) {
            let mut next = Vec::with_capacity(out.len() * vals.len());
            for (prefix, tb) in &out {
                for v in vals {
                    let mut k = prefix.clone();
                    keys::encode_dir_c(&mut k, v, *desc, coll);
                    let mut t = tb.clone();
                    t.push(keys::type_bit(v, *here, coll.is_some()));
                    next.push((k, t));
                }
            }
            out = next;
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out.dedup_by(|a, b| a.0 == b.0);
        Ok(out)
    }

    fn index_insert(&self, tx: &mut WriteTransaction, c: &Coll, idx: &Index, rid: u64, d: &Document, _seen: &mut HashMap<Vec<u8>, u64>) -> Result<()> {
        let ks = self.index_keys(c, idx, d)?;
        self.put_keys(tx, c, idx, rid, d, ks)
    }

    /// Write index entries. Values: unique `rid ‖ typebits`, non-unique `typebits`.
    fn put_keys(&self, tx: &mut WriteTransaction, c: &Coll, idx: &Index, rid: u64, d: &Document, ks: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        for (k, tb) in ks {
            idx.mods.fetch_add(1, Ordering::Relaxed);
            if idx.unique {
                if let Some(v) = tx.get(&idx.ks, k.as_slice())? {
                    if unique_rid(&v) != rid {
                        return Err(dup_key_error(c, idx, d));
                    }
                }
                let mut val = rid_key(rid).to_vec();
                val.extend_from_slice(&tb);
                tx.insert(&idx.ks, k, val);
            } else {
                tx.insert(&idx.ks, self.entry_key(idx, &k, rid), tb);
            }
        }
        Ok(())
    }

    /// Record id of an index entry.
    fn entry_rid(idx: &Index, k: &[u8], v: &[u8]) -> u64 {
        if idx.unique { unique_rid(v) } else { rid_from(k) }
    }

    /// Type bits stored in an index entry value (empty for entries written before type bits).
    fn entry_typebits<'a>(idx: &Index, v: &'a [u8]) -> &'a [u8] {
        if idx.unique { v.get(8..).unwrap_or(&[]) } else { v }
    }

    /// Reconstruct `{indexed fields}` from an entry for covered plans; `None` when the key is
    /// not decodable (falls back to fetching the document).
    fn covered_doc(idx: &Index, k: &[u8], v: &[u8]) -> Option<Document> {
        let tb = Self::entry_typebits(idx, v);
        if tb.len() != idx.fields.len() {
            return None;
        }
        let key = if idx.unique { k } else { &k[..k.len().checked_sub(8)?] };
        keys::decode_fields(key, &idx.fields, tb)
    }

    /// Covered-plan fallback: the indexed fields of the stored document.
    fn project_index_fields(idx: &Index, doc: &Document) -> Document {
        let mut out = Document::new();
        for (f, _) in &idx.fields {
            // Covered plans read the key, where a missing field is null.
            let v = get_path(doc, f).cloned().unwrap_or(Bson::Null);
            let _ = bsonutil::set_path(&mut out, f, v);
        }
        out
    }

    /// Index entries within `ranges` in index order (descending when `reverse`), eagerly.
    fn entries_eager<R: Readable>(&self, r: &R, idx: &Index, ranges: &[Interval], reverse: bool) -> Result<Vec<(fjall::Slice, fjall::Slice)>> {
        let mut out = Vec::new();
        let ordered: Vec<&Interval> = if reverse { ranges.iter().rev().collect() } else { ranges.iter().collect() };
        for (lo, hi) in ordered {
            let it = r.range(&idx.ks, (lo.clone(), hi.clone()));
            if reverse {
                for g in it.rev() {
                    out.push(g.into_inner()?);
                }
            } else {
                for g in it {
                    out.push(g.into_inner()?);
                }
            }
        }
        Ok(out)
    }

    /// Rebuild stale index histograms (cheap no-op when nothing changed much).
    pub fn refresh_histograms(&self) -> Result<()> {
        for c in self.list_colls(None) {
            for idx in c.indexes() {
                if !idx.has_entries() {
                    continue;
                }
                let mods = idx.mods.load(Ordering::Relaxed);
                let stale = match idx.histogram() {
                    None => mods > 0 || c.count.load(Ordering::Relaxed) > 0,
                    Some(h) => mods > 1000.max(h.total / 10),
                };
                if !stale {
                    continue;
                }
                idx.mods.store(0, Ordering::Relaxed);
                let snap = self.snapshot();
                let unique = idx.unique;
                let keys = snap.iter(&idx.ks).filter_map(|g| g.key().ok()).map(|k| {
                    if unique { k.to_vec() } else { k[..k.len().saturating_sub(8)].to_vec() }
                });
                let h = Histogram::build(keys, c.count.load(Ordering::Relaxed).max(1) as u64);
                *idx.hist.write().expect("hist") = Some(Arc::new(h));
            }
        }
        Ok(())
    }

    // ---------------- reads ----------------

    pub fn get_rid<R: Readable>(&self, r: &R, c: &Coll, rid: u64) -> Result<Option<Document>> {
        match r.get(c.docs()?, rid_key(rid))? {
            Some(v) => Ok(Some(bsonutil::from_bytes(&v)?)),
            None => Ok(None),
        }
    }

    /// Full collection scan in natural (RecordId) order.
    pub fn scan<R: Readable>(&self, r: &R, c: &Coll, reverse: bool) -> Result<RecIter> {
        let it = r.iter(c.docs()?);
        let map = |g: fjall::Guard| -> Result<Rec> {
            let (k, v) = g.into_inner()?;
            Ok((rid_from(&k), bsonutil::from_bytes(&v)?))
        };
        Ok(if reverse { Box::new(it.rev().map(map)) } else { Box::new(it.map(map)) })
    }

    /// Full collection scan yielding raw bytes (callers decode only what they need).
    pub fn scan_raw<R: Readable>(&self, r: &R, c: &Coll, reverse: bool) -> Result<RawIter> {
        let it = r.iter(c.docs()?);
        let map = |g: fjall::Guard| -> Result<RawRec> {
            let (k, v) = g.into_inner()?;
            Ok((rid_from(&k), v))
        };
        Ok(if reverse { Box::new(it.rev().map(map)) } else { Box::new(it.map(map)) })
    }

    /// Raw-bytes plan execution (index scans fetch eagerly from `r`).
    pub fn execute_raw<R: Readable>(&self, r: &R, c: &Arc<Coll>, plan: &Plan) -> Result<RawIter> {
        match plan {
            Plan::Empty => Ok(Box::new(std::iter::empty())),
            Plan::CollScan { reverse } => self.scan_raw(r, c, *reverse),
            Plan::IxScan(p) => {
                p.index.accesses.fetch_add(1, Ordering::Relaxed);
                let docs = c.docs()?;
                let dedupe = p.index.multikey.load(Ordering::Relaxed);
                let mut seen = HashSet::new();
                let mut out = Vec::new();
                for (k, v) in self.entries_eager(r, &p.index, &p.ranges, p.reverse)? {
                    p.keys_examined.fetch_add(1, Ordering::Relaxed);
                    let rid = Self::entry_rid(&p.index, &k, &v);
                    if dedupe && !seen.insert(rid) {
                        continue;
                    }
                    if p.covered {
                        if let Some(d) = Self::covered_doc(&p.index, &k, &v) {
                            out.push(Ok((rid, fjall::Slice::from(bsonutil::to_bytes(&d)))));
                            continue;
                        }
                    }
                    if let Some(raw) = r.get(docs, rid_key(rid))? {
                        let raw = if p.covered { fjall::Slice::from(bsonutil::to_bytes(&Self::project_index_fields(&p.index, &bsonutil::from_bytes(&raw)?))) } else { raw };
                        out.push(Ok((rid, raw)));
                    }
                }
                Ok(Box::new(out.into_iter()))
            }
        }
    }

    /// Lazily executed raw plan over a snapshot (for cursors): index scans stream in index
    /// order, so sort-providing plans with a limit stop early.
    pub fn execute_snapshot_raw(self: &Arc<Self>, snap: Snapshot, c: &Arc<Coll>, plan: &Plan) -> Result<RawIter> {
        let Plan::IxScan(p) = plan else { return self.execute_raw(&snap, c, plan) };
        p.index.accesses.fetch_add(1, Ordering::Relaxed);
        let docs = c.docs()?.clone();
        let idx = p.index.clone();
        let covered = p.covered;
        let dedupe = idx.multikey.load(Ordering::Relaxed);
        let mut seen = HashSet::new();
        let keys_examined = p.keys_examined.clone();
        let entries = IndexEntries { snap: snap.clone(), idx: idx.clone(), ranges: p.ranges.clone().into(), reverse: p.reverse, cur: None };
        Ok(Box::new(entries.filter_map(move |e| {
            let (k, v) = match e {
                Ok(x) => x,
                Err(err) => return Some(Err(err)),
            };
            keys_examined.fetch_add(1, Ordering::Relaxed);
            let rid = Self::entry_rid(&idx, &k, &v);
            if dedupe && !seen.insert(rid) {
                return None;
            }
            if covered {
                if let Some(d) = Self::covered_doc(&idx, &k, &v) {
                    return Some(Ok((rid, fjall::Slice::from(bsonutil::to_bytes(&d)))));
                }
            }
            match snap.get(&docs, rid_key(rid)) {
                Ok(Some(raw)) if covered => Some(bsonutil::from_bytes(&raw).map(|d| (rid, fjall::Slice::from(bsonutil::to_bytes(&Self::project_index_fields(&idx, &d)))))),
                Ok(Some(raw)) => Some(Ok((rid, raw))),
                Ok(None) => None,
                Err(e) => Some(Err(e.into())),
            }
        })))
    }

    /// Record ids within index ranges, in ascending index order, de-duplicated.
    pub fn index_rids<R: Readable>(&self, r: &R, idx: &Index, ranges: &[Interval], limit: Option<usize>) -> Result<Vec<u64>> {
        idx.accesses.fetch_add(1, Ordering::Relaxed);
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let dedupe = idx.multikey.load(Ordering::Relaxed) || ranges.len() > 1;
        for (lo, hi) in ranges {
            for g in r.range(&idx.ks, (lo.clone(), hi.clone())) {
                let (k, v) = g.into_inner()?;
                let rid = Self::entry_rid(idx, &k, &v);
                if !dedupe || seen.insert(rid) {
                    out.push(rid);
                    if limit.is_some_and(|l| out.len() >= l) {
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Execute a plan returning candidate records (a superset; callers must re-filter).
    pub fn execute<R: Readable>(&self, r: &R, c: &Arc<Coll>, plan: &Plan) -> Result<RecIter> {
        match plan {
            Plan::CollScan { reverse } => self.scan(r, c, *reverse),
            _ => Ok(Box::new(self.execute_raw(r, c, plan)?.map(|x| x.and_then(|(rid, raw)| Ok((rid, bsonutil::from_bytes(&raw)?)))))),
        }
    }

    // ---------------- oplog ----------------

    /// Oplog entries strictly after `after` (or all when None), in order.
    pub fn oplog_since(&self, after: Option<bson::Timestamp>) -> Result<Vec<Document>> {
        let snap = self.db.read_tx();
        let lo = match after {
            Some(ts) => Bound::Excluded(ts_key(ts).to_vec()),
            None => Bound::Unbounded,
        };
        let mut out = Vec::new();
        for g in snap.range::<Vec<u8>, _>(&self.oplog_ks, (lo, Bound::Unbounded)) {
            let (_, v) = g.into_inner()?;
            out.push(bsonutil::from_bytes(&v)?);
        }
        Ok(out)
    }

    pub fn oplog_first_ts(&self) -> Option<bson::Timestamp> {
        let snap = self.db.read_tx();
        snap.first_key_value(&self.oplog_ks).and_then(|g| g.key().ok()).map(|k| ts_from_key(&k))
    }

    /// Trim the oplog ring buffer to its configured size.
    pub fn trim_oplog(&self) -> Result<()> {
        if self.oplog_bytes.load(Ordering::Relaxed) <= self.oplog_max_bytes {
            return Ok(());
        }
        let snap = self.db.read_tx();
        let target = self.oplog_max_bytes * 9 / 10;
        let mut cur = self.oplog_bytes.load(Ordering::Relaxed);
        for g in snap.iter(&self.oplog_ks) {
            if cur <= target {
                break;
            }
            let (k, v) = g.into_inner()?;
            self.oplog_ks.remove(k)?;
            cur = cur.saturating_sub(v.len() as u64);
        }
        self.oplog_bytes.store(cur, Ordering::Relaxed);
        Ok(())
    }

    pub fn disk_size(&self) -> u64 {
        self.db.disk_space().unwrap_or(0)
    }

    pub fn flush(&self) -> Result<()> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }
}

pub fn conflict() -> Error {
    Error::new(112, "Write conflict during plan execution and yielding is disabled. :: Please retry your operation or multi-document transaction.")
        .with_label("TransientTransactionError")
}

thread_local! {
    static BYPASS_VALIDATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Restores the previous `bypassDocumentValidation` state when dropped.
pub struct BypassGuard(bool);

impl Drop for BypassGuard {
    fn drop(&mut self) {
        BYPASS_VALIDATION.with(|b| b.set(self.0));
    }
}

/// Enable `bypassDocumentValidation` for writes on this thread until the guard drops.
pub fn bypass_validation(on: bool) -> BypassGuard {
    BypassGuard(BYPASS_VALIDATION.with(|b| b.replace(on)))
}

fn validate(c: &Coll, v: &Matcher, d: &Document) -> Result<()> {
    let level = c.options.get_str("validationLevel").unwrap_or("strict");
    if BYPASS_VALIDATION.with(|b| b.get()) {
        if level == "constraint" {
            return Err(Error::new(2, "bypassDocumentValidation is not permitted with 'constraint' validationLevel"));
        }
        if bsonutil::as_bool(c.options.get("prepareConstraintValidationLevel")) {
            return Err(Error::new(2, format!("bypassDocumentValidation is not permitted while prepareConstraintValidationLevel is set. To allow bypassDocumentValidation, first run: db.runCommand({{collMod: \"{}\", prepareConstraintValidationLevel: false}})", c.name)));
        }
        return Ok(());
    }
    if level == "off" {
        return Ok(());
    }
    if !v.matches(d)? {
        if c.options.get_str("validationAction").unwrap_or("error") == "warn" {
            return Ok(());
        }
        return Err(Error::new(121, "Document failed validation").with_extra(doc! {"errInfo": {"failingDocumentId": d.get("_id").cloned().unwrap_or(Bson::Null), "details": {"operatorName": "$jsonSchema"}}}));
    }
    Ok(())
}

fn dup_key_error(c: &Coll, idx: &Index, d: &Document) -> Error {
    let mut kv = Document::new();
    for (f, _) in &idx.fields {
        kv.insert(f.clone(), get_path(d, f).cloned().unwrap_or(Bson::Null));
    }
    let shown: Vec<String> = kv.iter().map(|(k, v)| format!("{k}: {}", shell_repr(v))).collect();
    Error::new(
        11000,
        format!("E11000 duplicate key error collection: {} index: {} dup key: {{ {} }}", c.ns, idx.name, shown.join(", ")),
    )
    .with_extra(doc! {"keyPattern": idx.key.clone(), "keyValue": kv})
}

fn shell_repr(v: &Bson) -> String {
    match v {
        Bson::String(s) => format!("\"{s}\""),
        Bson::ObjectId(o) => format!("ObjectId('{o}')"),
        other => other.to_string(),
    }
}

/// Ensure `_id` exists and is first; reject invalid `_id` and `$`-prefixed top-level fields.
pub fn prepare_for_insert(d: &mut Document) -> Result<()> {
    match d.get("_id") {
        None => {
            let mut nd = Document::new();
            nd.insert("_id", bson::oid::ObjectId::new());
            for (k, v) in std::mem::take(d) {
                nd.insert(k, v);
            }
            *d = nd;
        }
        Some(Bson::Array(_)) => return Err(Error::new(2, "can't use an array for _id")),
        Some(Bson::RegularExpression(_)) => return Err(Error::new(2, "can't use a regex for _id")),
        Some(Bson::Undefined) => return Err(Error::new(2, "can't use a undefined for _id")),
        Some(_) => {
            if d.keys().next().map(String::as_str) != Some("_id") {
                let id = d.remove("_id").expect("present");
                let mut nd = Document::new();
                nd.insert("_id", id);
                for (k, v) in std::mem::take(d) {
                    nd.insert(k, v);
                }
                *d = nd;
            }
        }
    }
    if let Some(Bson::Document(idd)) = d.get("_id") {
        if idd.keys().any(|k| k.starts_with('$')) {
            return Err(Error::new(52, "_id fields may not contain '$'-prefixed fields"));
        }
    }
    Ok(())
}

fn path_exists(d: &Document, path: &str) -> bool {
    fn walk(v: &Bson, segs: &[&str]) -> bool {
        let Some((first, rest)) = segs.split_first() else { return true };
        match v {
            Bson::Document(d) => d.get(*first).is_some_and(|x| walk(x, rest)),
            Bson::Array(a) => a.iter().any(|e| walk(e, segs)) || first.parse::<usize>().ok().and_then(|i| a.get(i)).is_some_and(|x| walk(x, rest)),
            _ => false,
        }
    }
    let segs: Vec<&str> = path.split('.').collect();
    d.get(segs[0]).is_some_and(|v| walk(v, &segs[1..]))
}

/// Values at `path` for index keys, with array fan-out. Returns (values, traversed_array).
pub fn index_values(d: &Document, path: &str) -> (Vec<Bson>, bool) {
    fn collect(v: &Bson, segs: &[&str], out: &mut Vec<Bson>, arr: &mut bool) {
        let Some((first, rest)) = segs.split_first() else {
            match v {
                Bson::Array(a) => {
                    *arr = true;
                    if a.is_empty() {
                        out.push(Bson::Undefined);
                    } else {
                        out.extend(a.iter().cloned());
                    }
                }
                other => out.push(other.clone()),
            }
            return;
        };
        match v {
            Bson::Document(d) => match d.get(*first) {
                Some(x) => collect(x, rest, out, arr),
                None => out.push(Bson::Null),
            },
            Bson::Array(a) => {
                *arr = true;
                if let Some(x) = first.parse::<usize>().ok().and_then(|i| a.get(i)) {
                    collect(x, rest, out, arr);
                    return;
                }
                let before = out.len();
                for e in a {
                    if let Bson::Document(_) = e {
                        collect(e, segs, out, arr);
                    }
                }
                if out.len() == before {
                    out.push(Bson::Null);
                }
            }
            _ => out.push(Bson::Null),
        }
    }
    let segs: Vec<&str> = path.split('.').collect();
    let mut out = Vec::new();
    let mut arr = false;
    match d.get(segs[0]) {
        Some(v) => collect(v, &segs[1..], &mut out, &mut arr),
        None => out.push(Bson::Null),
    }
    out.sort_by(bsonutil::cmp_bson);
    out.dedup_by(|a, b| keys::encode_one(a) == keys::encode_one(b));
    (out, arr)
}

/// Record id stored at the front of a unique index entry value.
fn unique_rid(v: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    if let Some(b) = v.get(..8) {
        a.copy_from_slice(b);
    }
    u64::from_be_bytes(a)
}

/// Lazy, ordered iteration over index entries of a snapshot.
struct IndexEntries {
    snap: Snapshot,
    idx: Arc<Index>,
    ranges: std::collections::VecDeque<Interval>,
    reverse: bool,
    cur: Option<Box<dyn Iterator<Item = fjall::Guard> + Send>>,
}

impl Iterator for IndexEntries {
    type Item = Result<(fjall::Slice, fjall::Slice)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(cur) = self.cur.as_mut() {
                if let Some(g) = cur.next() {
                    return Some(g.into_inner().map_err(Error::from));
                }
                self.cur = None;
            }
            let (lo, hi) = if self.reverse { self.ranges.pop_back()? } else { self.ranges.pop_front()? };
            let it = self.snap.range(&self.idx.ks, (lo, hi));
            self.cur = Some(if self.reverse { Box::new(it.rev()) } else { Box::new(it) });
        }
    }
}
