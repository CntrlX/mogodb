//! Query settings (`setQuerySettings` / `removeQuerySettings`), the
//! `$querySettings` and `$listClusterCatalog` aggregation sources, and
//! query-shape hashing.

use std::sync::{Arc, Mutex};

use bson::{doc, Bson, Document};
use sha2::{Digest, Sha256};

use crate::agg::pipeline::{AggOptions, DocIter};
use crate::bsonutil;
use crate::error::{Error, Result};
use crate::server::CmdCtx;
use crate::storage::{Coll, Store};

const META_KEY: &str = "querySettings";
const KNOWN_FIELDS: [&str; 6] = [
    "indexHints",
    "queryFramework",
    "reject",
    "maxTimeMS",
    "comment",
    "queryKnobs",
];
const FIND_FIELDS: &[&str] = &[
    "find",
    "filter",
    "sort",
    "projection",
    "hint",
    "skip",
    "limit",
    "batchSize",
    "singleBatch",
    "comment",
    "maxTimeMS",
    "readConcern",
    "max",
    "min",
    "returnKey",
    "showRecordId",
    "tailable",
    "oplogReplay",
    "noCursorTimeout",
    "awaitData",
    "collation",
    "let",
    "$db",
    "lsid",
    "txnNumber",
    "autocommit",
    "startTransaction",
    "$clusterTime",
    "allowDiskUse",
    "allowPartialResults",
    "explain",
    "querySettings",
];
const AGG_FIELDS: &[&str] = &[
    "aggregate",
    "pipeline",
    "cursor",
    "explain",
    "allowDiskUse",
    "maxTimeMS",
    "bypassDocumentValidation",
    "readConcern",
    "collation",
    "hint",
    "comment",
    "writeConcern",
    "let",
    "$db",
    "lsid",
    "txnNumber",
    "autocommit",
    "startTransaction",
    "$clusterTime",
    "querySettings",
];
const DISTINCT_FIELDS: &[&str] = &[
    "distinct",
    "key",
    "query",
    "collation",
    "readConcern",
    "maxTimeMS",
    "comment",
    "hint",
    "$db",
    "lsid",
    "txnNumber",
    "autocommit",
    "startTransaction",
    "$clusterTime",
    "querySettings",
];

/// Serialises read-modify-write cycles on the stored settings document.
static META_LOCK: Mutex<()> = Mutex::new(());

/// A parsed `setQuerySettings` / `removeQuerySettings` argument.
struct QArg {
    hash: String,
    /// Representative query, present only when the argument was an object.
    rep: Option<Document>,
}

pub fn route_cmd(ctx: &mut CmdCtx) -> Result<Document> {
    if ctx.db != "admin" {
        return Err(Error::new(
            13,
            format!("{} may only be run against the admin database.", ctx.name),
        ));
    }
    let name = ctx.name.clone();
    match name.as_str() {
        "setQuerySettings" => set_cmd(ctx),
        "removeQuerySettings" => remove_cmd(ctx),
        other => Err(Error::new(59, format!("no such command: '{other}'"))),
    }
}

fn set_cmd(ctx: &mut CmdCtx) -> Result<Document> {
    let arg = query_arg(ctx)?;
    let settings = match ctx.cmd.get("settings") {
        Some(Bson::Document(d)) => d.clone(),
        Some(other) => {
            return Err(Error::new(
                14,
                format!(
                    "BSON field 'setQuerySettings.settings' is the wrong type '{}', expected type 'object'",
                    bsonutil::type_name(other)
                ),
            ));
        }
        None => {
            return Err(Error::new(
                40414,
                "BSON field 'setQuerySettings.settings' is missing but a required field",
            ));
        }
    };
    if !KNOWN_FIELDS.iter().any(|k| settings.contains_key(k)) {
        return Err(Error::new(
            8727502,
            "settings field in setQuerySettings command cannot be empty",
        ));
    }
    let _guard = META_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    ctx.write(|store, _| set_entry(store, &arg, &settings))
}

fn remove_cmd(ctx: &mut CmdCtx) -> Result<Document> {
    let arg = query_arg(ctx)?;
    let _guard = META_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    ctx.write(|store, _| {
        let mut entries = stored_settings(store)?;
        let before = entries.len();
        entries.retain(|e| e.get_str("queryShapeHash").ok() != Some(arg.hash.as_str()));
        if entries.len() != before {
            save(store, &entries)?;
        }
        Ok(doc! { "ok": 1.0 })
    })
}

/// Reads the command argument (named after the command) as either a hash
/// string or a representative query object.
fn query_arg(ctx: &CmdCtx) -> Result<QArg> {
    let name = ctx.name.as_str();
    match ctx.cmd.get(name) {
        Some(Bson::String(s)) => Ok(QArg {
            hash: parse_hash(s)?,
            rep: None,
        }),
        Some(Bson::Document(d)) => {
            let shape = validate_representative(d)?;
            Ok(QArg {
                hash: hash_shape(&shape),
                rep: Some(d.clone()),
            })
        }
        Some(other) => Err(Error::new(
            14,
            format!(
                "BSON field '{name}.{name}' is the wrong type '{}', expected types '[string, object]'",
                bsonutil::type_name(other)
            ),
        )),
        None => Err(Error::new(
            40414,
            format!("BSON field '{name}.{name}' is missing but a required field"),
        )),
    }
}

fn parse_hash(s: &str) -> Result<String> {
    if s.is_empty() || s.len() % 2 != 0 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Error::new(2, "Hash input is not a hex string"));
    }
    let n = s.len() / 2;
    if n != 32 {
        return Err(Error::new(
            16,
            format!("Unsupported SHA256Block hash length: {n}"),
        ));
    }
    Ok(s.to_ascii_uppercase())
}

fn set_entry(store: &Store, arg: &QArg, settings: &Document) -> Result<Document> {
    let mut entries = stored_settings(store)?;
    let pos = entries
        .iter()
        .position(|e| e.get_str("queryShapeHash").ok() == Some(arg.hash.as_str()));
    if pos.is_none() && arg.rep.is_none() {
        return Err(Error::new(
            7746604,
            "no query settings exist for this hash; a representative query is required to create them",
        ));
    }
    let base = match pos {
        Some(i) => entries[i]
            .get_document("settings")
            .cloned()
            .unwrap_or_default(),
        None => Document::new(),
    };
    let new_settings = ordered(apply_settings(base, settings)?);
    if new_settings.keys().all(|k| k == "comment") {
        return Err(Error::new(
            7746604,
            "the resulting settings cannot be empty or contain only default values",
        ));
    }
    let mut resp = doc! { "queryShapeHash": arg.hash.clone(), "settings": new_settings.clone() };
    match pos {
        Some(i) => {
            let e = &mut entries[i];
            e.insert("settings", new_settings);
            if let Some(rq) = &arg.rep {
                e.insert("representativeQuery", rq.clone());
            }
        }
        None => {
            let mut e = doc! { "queryShapeHash": arg.hash.clone(), "settings": new_settings };
            if let Some(rq) = &arg.rep {
                e.insert("representativeQuery", rq.clone());
                resp.insert("representativeQuery", rq.clone());
            }
            entries.push(e);
        }
    }
    save(store, &entries)?;
    resp.insert("ok", 1.0);
    Ok(resp)
}

/// Canonical field order of stored settings (as mongod 9.0 returns them).
fn ordered(d: Document) -> Document {
    const ORDER: [&str; 6] = ["indexHints", "queryFramework", "reject", "comment", "maxTimeMS", "queryKnobs"];
    let mut out = Document::new();
    for k in ORDER {
        if let Some(v) = d.get(k) {
            out.insert(k, v.clone());
        }
    }
    for (k, v) in d {
        if !ORDER.contains(&k.as_str()) {
            out.insert(k, v);
        }
    }
    out
}

/// Merges the user-supplied settings onto the existing ones. Defaults remove
/// the field; unknown fields are ignored.
fn apply_settings(mut base: Document, input: &Document) -> Result<Document> {
    for (k, v) in input {
        match k.as_str() {
            "indexHints" => {
                let hints = parse_hints(v)?;
                if hints.is_empty() {
                    base.remove(k);
                } else {
                    base.insert(k, Bson::Array(hints));
                }
            }
            "queryFramework" => match v {
                Bson::String(s) => match s.as_str() {
                    "classic" | "sbe" => {
                        base.insert(k, s.clone());
                    }
                    _ => {
                        return Err(Error::new(
                            2,
                            format!(
                                "Invalid value for 'queryFramework': expected classic or sbe, but got {s}"
                            ),
                        ));
                    }
                },
                other => {
                    return Err(Error::new(
                        14,
                        format!(
                            "BSON field 'setQuerySettings.settings.queryFramework' is the wrong type '{}', expected type 'string'",
                            bsonutil::type_name(other)
                        ),
                    ));
                }
            },
            "reject" => match v {
                Bson::Boolean(false) => {
                    base.remove(k);
                }
                Bson::Boolean(true) => {
                    base.insert(k, true);
                }
                other => {
                    return Err(Error::new(
                        14,
                        format!(
                            "Field 'reject' should be a boolean value, but found: {}",
                            bsonutil::type_name(other)
                        ),
                    ));
                }
            },
            "maxTimeMS" => {
                let n = bsonutil::num_i64(v).ok_or_else(|| {
                    Error::new(
                        14,
                        format!(
                            "BSON field 'setQuerySettings.settings.maxTimeMS' is the wrong type '{}', expected types '[decimal, double, long, int]'",
                            bsonutil::type_name(v)
                        ),
                    )
                })?;
                if n < 0 {
                    return Err(Error::new(
                        2,
                        format!("BSON field 'maxTimeMS' value must be >= 0, actual value '{n}'"),
                    ));
                }
                if n == 0 {
                    base.remove(k);
                } else {
                    base.insert(k, Bson::Int64(n));
                }
            }
            "comment" => {
                base.insert(k, v.clone());
            }
            "queryKnobs" => match v {
                Bson::Document(knobs) => {
                    if let Some((name, _)) = knobs.iter().next() {
                        return Err(Error::new(
                            12194501,
                            format!(
                                "failed to parse query knob {name}: query knob not settable via QuerySettings: {name}"
                            ),
                        ));
                    }
                }
                other => {
                    return Err(Error::new(
                        14,
                        format!(
                            "BSON field 'setQuerySettings.settings.queryKnobs' is the wrong type '{}', expected type 'object'",
                            bsonutil::type_name(other)
                        ),
                    ));
                }
            },
            _ => {}
        }
    }
    Ok(base)
}

fn parse_hints(v: &Bson) -> Result<Vec<Bson>> {
    let items: Vec<Bson> = match v {
        Bson::Document(d) => vec![Bson::Document(d.clone())],
        Bson::Array(a) => a.clone(),
        _ => {
            return Err(Error::new(
                9,
                "'indexHints' must be an object or an array",
            ));
        }
    };
    items
        .iter()
        .map(|item| {
            let Bson::Document(h) = item else {
                return Err(Error::new(10065, "invalid parameter: expected an object (0)"));
            };
            let ns = match h.get("ns") {
                Some(Bson::Document(n)) => n,
                Some(other) => {
                    return Err(Error::new(
                        14,
                        format!(
                            "BSON field 'IndexHintSpec.ns' is the wrong type '{}', expected type 'object'",
                            bsonutil::type_name(other)
                        ),
                    ));
                }
                None => {
                    return Err(Error::new(
                        40414,
                        "BSON field 'IndexHintSpec.ns' is missing but a required field",
                    ));
                }
            };
            let coll = ns.get_str("coll").map_err(|_| {
                Error::new(8727501, "invalid index hint: 'ns.coll' field is missing")
            })?;
            let db = ns.get_str("db").map_err(|_| {
                Error::new(8727501, "invalid index hint: 'ns.db' field is missing")
            })?;
            let allowed = match h.get("allowedIndexes") {
                Some(Bson::Array(a)) => a,
                Some(other) => {
                    return Err(Error::new(
                        14,
                        format!(
                            "BSON field 'IndexHintSpec.allowedIndexes' is the wrong type '{}', expected type 'array'",
                            bsonutil::type_name(other)
                        ),
                    ));
                }
                None => {
                    return Err(Error::new(
                        40414,
                        "BSON field 'IndexHintSpec.allowedIndexes' is missing but a required field",
                    ));
                }
            };
            let mut names = Vec::with_capacity(allowed.len());
            for x in allowed {
                match x {
                    Bson::String(_) | Bson::Document(_) => names.push(x.clone()),
                    _ => return Err(Error::new(9, "Hint must be a string or an object")),
                }
            }
            Ok(Bson::Document(doc! {
                "ns": { "db": db, "coll": coll },
                "allowedIndexes": Bson::Array(names),
            }))
        })
        .collect()
}

fn save(store: &Store, entries: &[Document]) -> Result<()> {
    if entries.is_empty() {
        return store.put_meta(META_KEY, None);
    }
    let arr: Vec<Bson> = entries.iter().cloned().map(Bson::Document).collect();
    store.put_meta(META_KEY, Some(&doc! { "entries": Bson::Array(arr) }))
}

/// All stored query-settings entries.
pub fn stored_settings(store: &Store) -> Result<Vec<Document>> {
    let Some(meta) = store.get_meta(META_KEY)? else {
        return Ok(Vec::new());
    };
    Ok(match meta.get("entries") {
        Some(Bson::Array(a)) => a
            .iter()
            .filter_map(|b| match b {
                Bson::Document(d) => Some(d.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    })
}

/// Settings that apply to a query command: its inline `querySettings`
/// field, overlaid by the stored settings for its query shape.
pub fn effective_settings(ctx: &CmdCtx, cmd: &Document) -> Result<Document> {
    let mut out = match cmd.get("querySettings") {
        Some(Bson::Document(d)) => d.clone(),
        _ => Document::new(),
    };
    let Ok(shape) = shape_of(cmd) else {
        return Ok(out);
    };
    let hash = hash_shape(&shape);
    for e in stored_settings(&ctx.srv.store)? {
        if e.get_str("queryShapeHash").ok() == Some(hash.as_str()) {
            if let Ok(s) = e.get_document("settings") {
                for (k, v) in s {
                    out.insert(k.clone(), v.clone());
                }
            }
        }
    }
    Ok(out)
}

fn hash_shape(shape: &Document) -> String {
    Sha256::digest(bsonutil::to_bytes(shape))
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect()
}

fn shape_err(q: &Document) -> Error {
    Error::new(
        7746402,
        format!("QueryShape can not be computed for command: {q:?}"),
    )
}

fn validate_representative(rq: &Document) -> Result<Document> {
    let Some(name) = rq.keys().next() else {
        return Err(shape_err(rq));
    };
    let (allowed, prefix) = match name.as_str() {
        "find" => (FIND_FIELDS, "findCommandRequest"),
        "aggregate" => (AGG_FIELDS, "aggregateCommandRequest"),
        "distinct" => (DISTINCT_FIELDS, "distinctCommandRequest"),
        _ => return Err(shape_err(rq)),
    };
    for k in rq.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(Error::new(
                40415,
                format!("BSON field '{prefix}.{k}' is an unknown field."),
            ));
        }
    }
    match rq.get("$db") {
        Some(Bson::String(_)) => {}
        Some(other) => {
            return Err(Error::new(
                14,
                format!(
                    "BSON field '{prefix}.$db' is the wrong type '{}', expected type 'string'",
                    bsonutil::type_name(other)
                ),
            ));
        }
        None => {
            return Err(Error::new(
                40414,
                format!("BSON field '{prefix}.$db' is missing but a required field"),
            ));
        }
    }
    shape_of(rq)
}

/// Debug shape of a query command: literals replaced by type placeholders,
/// keyed by the command's namespace.
pub fn shape_of(query: &Document) -> Result<Document> {
    let Some(name) = query.keys().next() else {
        return Err(shape_err(query));
    };
    let db = query.get_str("$db").unwrap_or("");
    let coll = match query.get(name) {
        Some(Bson::String(c)) => c.as_str(),
        _ => "",
    };
    let ns = format!("{db}.{coll}");
    match name.as_str() {
        "find" => Ok(find_shape(query, ns)),
        "aggregate" => Ok(aggregate_shape(query, ns)),
        "distinct" => Ok(distinct_shape(query, ns)),
        _ => Err(shape_err(query)),
    }
}

/// True when the query projects `$meta`; such queries have no debug shape.
pub fn has_meta_projection(q: &Document) -> bool {
    q.values().any(contains_meta)
}

fn contains_meta(b: &Bson) -> bool {
    match b {
        Bson::Document(d) => {
            d.keys().any(|k| k.as_str() == "$meta") || d.values().any(contains_meta)
        }
        Bson::Array(a) => a.iter().any(contains_meta),
        _ => false,
    }
}

fn find_shape(q: &Document, ns: String) -> Document {
    let mut d = doc! { "cmdNs": ns, "command": "find" };
    if let Some(Bson::Document(f)) = q.get("filter") {
        if !f.is_empty() {
            d.insert("filter", shape_filter(f));
        }
    }
    if let Some(s) = q.get("sort") {
        d.insert("sort", s.clone());
    }
    if let Some(Bson::Document(p)) = q.get("projection") {
        d.insert("projection", shape_projection(p));
    }
    for k in ["skip", "limit"] {
        if q.contains_key(k) {
            d.insert(k, tag("?number"));
        }
    }
    if let Some(s) = q.get("singleBatch") {
        d.insert("singleBatch", s.clone());
    }
    for k in ["min", "max", "let"] {
        if let Some(v) = q.get(k) {
            d.insert(k, replace_top(v));
        }
    }
    if let Some(c) = q.get("collation") {
        d.insert("collation", c.clone());
    }
    d
}

fn aggregate_shape(q: &Document, ns: String) -> Document {
    let stages: Vec<Bson> = match q.get("pipeline") {
        Some(Bson::Array(a)) => a.iter().map(shape_stage).collect(),
        _ => Vec::new(),
    };
    let mut d = doc! { "cmdNs": ns, "command": "aggregate", "pipeline": Bson::Array(stages) };
    if let Some(c) = q.get("collation") {
        d.insert("collation", c.clone());
    }
    if let Some(v) = q.get("let") {
        d.insert("let", replace_top(v));
    }
    d
}

fn shape_stage(b: &Bson) -> Bson {
    let Bson::Document(stage) = b else {
        return b.clone();
    };
    let Some((name, val)) = stage.iter().next() else {
        return b.clone();
    };
    let shaped = match (name.as_str(), val) {
        ("$match", Bson::Document(f)) => Bson::Document(shape_filter(f)),
        ("$project", Bson::Document(p)) => Bson::Document(shape_projection(p)),
        ("$limit" | "$skip", _) => tag("?number"),
        ("$unwind", Bson::String(path)) => Bson::Document(doc! { "path": path.clone() }),
        ("$group", _) => replace(val, true),
        ("$sort" | "$lookup", _) => val.clone(),
        _ => replace(val, false),
    };
    Bson::Document(one(name, shaped))
}

fn distinct_shape(q: &Document, ns: String) -> Document {
    let mut d = doc! { "cmdNs": ns, "command": "distinct" };
    if let Some(k) = q.get("key") {
        d.insert("key", k.clone());
    }
    if let Some(Bson::Document(f)) = q.get("query") {
        d.insert("query", shape_filter(f));
    }
    d
}

fn shape_projection(p: &Document) -> Document {
    let mut out = Document::new();
    let mut inclusion = false;
    for (k, v) in p {
        let nv = match v {
            Bson::Boolean(b) => Bson::Boolean(*b),
            Bson::Int32(n) => Bson::Boolean(*n != 0),
            Bson::Int64(n) => Bson::Boolean(*n != 0),
            Bson::Double(f) => Bson::Boolean(*f != 0.0),
            other => other.clone(),
        };
        if matches!(nv, Bson::Boolean(true)) && k.as_str() != "_id" {
            inclusion = true;
        }
        out.insert(k.clone(), nv);
    }
    if inclusion && !out.contains_key("_id") {
        out.insert("_id", true);
    }
    out
}

fn shape_filter(f: &Document) -> Document {
    let mut parts: Vec<Bson> = Vec::new();
    for (k, v) in f {
        if k.starts_with('$') {
            let nv = match v {
                Bson::Array(a) => Bson::Array(
                    a.iter()
                        .map(|x| match x {
                            Bson::Document(d) => Bson::Document(shape_filter(d)),
                            o => replace(o, false),
                        })
                        .collect(),
                ),
                o => replace(o, false),
            };
            parts.push(Bson::Document(one(k, nv)));
            continue;
        }
        match v {
            Bson::Document(ops) if !ops.is_empty() && ops.keys().all(|o| o.starts_with('$')) => {
                for (op, val) in ops {
                    parts.push(Bson::Document(op_part(k, op, val)));
                }
            }
            _ => parts.push(Bson::Document(one(
                k,
                Bson::Document(one("$eq", replace(v, false))),
            ))),
        }
    }
    match parts.len() {
        0 => Document::new(),
        1 => match parts.pop() {
            Some(Bson::Document(d)) => d,
            _ => Document::new(),
        },
        _ => one("$and", Bson::Array(parts)),
    }
}

fn op_part(k: &str, op: &str, val: &Bson) -> Document {
    let inner = match op {
        "$ne" => one("$not", Bson::Document(one("$eq", replace(val, false)))),
        "$exists" => match val {
            Bson::Boolean(false) => one("$not", Bson::Document(one("$exists", tag("?bool")))),
            _ => one("$exists", tag("?bool")),
        },
        _ => one(op, replace(val, false)),
    };
    one(k, Bson::Document(inner))
}

/// Replaces literal values with type placeholders. With `keep_paths`, `$`
/// field-path strings and nested documents/arrays are kept structurally.
fn replace(v: &Bson, keep_paths: bool) -> Bson {
    match v {
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
            tag("?number")
        }
        Bson::String(s) => {
            if keep_paths && s.starts_with('$') {
                v.clone()
            } else {
                tag("?string")
            }
        }
        Bson::Boolean(_) => tag("?bool"),
        Bson::DateTime(_) => tag("?date"),
        Bson::Null => tag("?null"),
        Bson::RegularExpression(_) => Bson::Document(doc! { "$regex": "?string" }),
        Bson::Document(d) => {
            if keep_paths {
                Bson::Document(replace_doc(d))
            } else {
                tag("?object")
            }
        }
        Bson::Array(a) => {
            if keep_paths {
                Bson::Array(a.iter().map(|x| replace(x, true)).collect())
            } else {
                array_shape(a)
            }
        }
        other => tag(&format!("?{}", bsonutil::type_name(other))),
    }
}

/// Literal replacement for a value that may be a document of variables.
fn replace_top(v: &Bson) -> Bson {
    match v {
        Bson::Document(d) => Bson::Document(replace_doc(d)),
        other => replace(other, false),
    }
}

fn replace_doc(d: &Document) -> Document {
    let mut out = Document::new();
    for (k, v) in d {
        out.insert(k.clone(), replace(v, true));
    }
    out
}

fn array_shape(a: &[Bson]) -> Bson {
    let reps: Vec<Bson> = a.iter().map(|x| replace(x, false)).collect();
    let inner = match reps.first() {
        Some(Bson::String(s)) if reps.iter().all(|r| *r == reps[0]) => s.clone(),
        _ => String::new(),
    };
    tag(&format!("?array<{inner}>"))
}

fn tag(s: &str) -> Bson {
    Bson::String(s.to_string())
}

fn one(k: &str, v: Bson) -> Document {
    let mut d = Document::new();
    d.insert(k, v);
    d
}

/// All catalog entries (collections and views) in `db`, or in every database
/// when `db` is `None`, sorted by namespace.
pub fn catalog_docs(store: &Store, db: Option<&str>) -> Result<Vec<Document>> {
    let mut colls = store.list_colls(db);
    colls.sort_by(|a, b| a.ns.cmp(&b.ns));
    let mut out: Vec<Document> = colls.iter().map(|c| catalog_doc(c)).collect();
    let mut dbs: Vec<String> = colls.iter().map(|c| c.db.clone()).collect();
    if let Some(d) = db {
        dbs.push(d.to_string());
    }
    dbs.sort();
    dbs.dedup();
    for d in dbs {
        if store.has_system_views(&d) {
            out.push(doc! {
                "ns": format!("{d}.system.views"),
                "db": d.as_str(),
                "type": "collection",
                "options": {},
                "info": {"readOnly": false},
                "idIndex": { "v": 2, "key": { "_id": 1 }, "name": "_id_" },
                "sharded": false,
            });
        }
    }
    Ok(out)
}

fn catalog_doc(c: &Coll) -> Document {
    let view = c.is_view();
    let kind = if view { "view" } else { "collection" };
    let mut d = doc! {
        "ns": c.ns.clone(),
        "db": c.db.clone(),
        "type": kind,
        "options": Bson::Document(c.options.clone()),
    };
    if view {
        d.insert("info", doc! { "readOnly": true });
    } else {
        d.insert(
            "info",
            doc! { "readOnly": false, "uuid": Bson::Binary(c.uuid.clone()) },
        );
        d.insert("idIndex", doc! { "v": 2, "key": { "_id": 1 }, "name": "_id_" });
    }
    d.insert("sharded", false);
    d
}

/// Builds the source stage for `$querySettings` and `$listClusterCatalog`.
/// Both are source stages: the incoming stream is not consumed.
pub fn build_stage(
    _input: DocIter,
    name: &str,
    arg: &Bson,
    opts: &Arc<AggOptions>,
) -> Result<DocIter> {
    let docs = match name {
        "$querySettings" => query_settings_stage(arg, opts)?,
        "$listClusterCatalog" => cluster_catalog_stage(arg, opts)?,
        other => {
            return Err(Error::new(
                40324,
                format!("Unrecognized pipeline stage name: '{other}'"),
            ));
        }
    };
    Ok(Box::new(docs.into_iter().map(|d| Ok::<Document, Error>(d))))
}

fn query_settings_stage(arg: &Bson, opts: &AggOptions) -> Result<Vec<Document>> {
    let Bson::Document(spec) = arg else {
        return Err(Error::new(
            7746800,
            "$querySettings stage expects a document as argument",
        ));
    };
    let mut show = false;
    for (k, v) in spec {
        match k.as_str() {
            "showDebugQueryShape" => match v {
                Bson::Boolean(b) => show = *b,
                other => {
                    return Err(Error::new(
                        14,
                        format!(
                            "Field 'showDebugQueryShape' should be a boolean value, but found: {}",
                            bsonutil::type_name(other)
                        ),
                    ));
                }
            },
            _ => {
                return Err(Error::new(
                    40415,
                    format!("BSON field '$querySettings.{k}' is an unknown field."),
                ));
            }
        }
    }
    let mut out = opts.ctx.query_settings()?;
    if show {
        for e in &mut out {
            let rq = match e.get("representativeQuery") {
                Some(Bson::Document(r)) => r.clone(),
                _ => continue,
            };
            if has_meta_projection(&rq) {
                continue;
            }
            let dbg = shape_of(&rq)?;
            e.insert("debugQueryShape", dbg);
        }
    }
    Ok(out)
}

fn cluster_catalog_stage(arg: &Bson, opts: &AggOptions) -> Result<Vec<Document>> {
    let Bson::Document(spec) = arg else {
        return Err(Error::new(
            9621302,
            format!("$listClusterCatalog must take an object but found: {arg:?}"),
        ));
    };
    let mut shards = false;
    for (k, v) in spec {
        match k.as_str() {
            "shards" => match v {
                Bson::Boolean(b) => shards = *b,
                other => {
                    return Err(Error::new(
                        14,
                        format!(
                            "Field 'shards' should be a boolean value, but found: {}",
                            bsonutil::type_name(other)
                        ),
                    ));
                }
            },
            "balancingConfiguration" => {}
            _ => {
                return Err(Error::new(
                    40415,
                    format!("BSON field '$listClusterCatalog.{k}' is an unknown field."),
                ));
            }
        }
    }
    let db = if opts.db == "admin" {
        None
    } else {
        Some(opts.db.as_str())
    };
    let mut docs = opts.ctx.cluster_catalog(db)?;
    if shards {
        for d in &mut docs {
            d.insert("shards", Bson::Array(Vec::new()));
        }
    }
    Ok(docs)
}
