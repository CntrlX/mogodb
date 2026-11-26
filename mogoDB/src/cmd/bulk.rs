//! `bulkWrite` command (MongoDB 8.0+): mixed inserts/updates/deletes across namespaces.
//!
//! Each op runs through the same primitives as `insert`/`update`/`delete`; per-op results are
//! returned through a cursor on `admin.$cmd.bulkWrite`.

use crate::bsonutil;
use crate::cmd::admin::shell_doc;
use crate::cmd::crud::{self, UpdateArgs};
use crate::cursor::{self, Cursor};
use crate::error::{Error, Result};
use crate::server::CmdCtx;
use bson::{Bson, Document, doc};

const INSERT_FIELDS: &[&str] = &["insert", "document"];
const UPDATE_FIELDS: &[&str] = &["update", "filter", "updateMods", "arrayFilters", "multi", "hint", "upsert", "sort", "collation", "constants"];
const DELETE_FIELDS: &[&str] = &["delete", "filter", "multi", "hint", "collation"];

enum Op<'a> {
    Insert { ns: String, doc: &'a Document },
    Update { ns: String, op: &'a Document },
    Delete { ns: String, op: &'a Document },
}

fn missing(f: &str) -> Error {
    Error::new(40414, format!("BSON field 'bulkWrite.ops.{f}' is missing but a required field"))
}

/// Validate one op entry (shape, fields, namespace index).
fn parse_op<'a>(op: &'a Document, ns_info: &[String]) -> Result<Op<'a>> {
    let Some((kind, idx)) = op.iter().next() else {
        return Err(Error::failed_to_parse("BulkWrite ops entry must not be empty"));
    };
    let allowed = match kind.as_str() {
        "insert" => INSERT_FIELDS,
        "update" => UPDATE_FIELDS,
        "delete" => DELETE_FIELDS,
        other => return Err(Error::new(40415, format!("BSON field 'bulkWrite.{other}' is an unknown field."))),
    };
    for k in op.keys().skip(1) {
        if !allowed.contains(&k.as_str()) {
            return Err(Error::new(40415, format!("BSON field 'bulkWrite.ops.{k}' is an unknown field.")));
        }
    }
    let ns = bsonutil::num_i64(idx)
        .and_then(|i| usize::try_from(i).ok())
        .and_then(|i| ns_info.get(i))
        .cloned()
        .ok_or_else(|| Error::bad_value(format!("BulkWrite ops entry {} has an invalid nsInfo index.", shell_doc(op))))?;
    Ok(match kind.as_str() {
        "insert" => Op::Insert { ns, doc: op.get_document("document").map_err(|_| missing("document"))? },
        "update" => {
            op.get_document("filter").map_err(|_| missing("filter"))?;
            op.get("updateMods").ok_or_else(|| missing("updateMods"))?;
            Op::Update { ns, op }
        }
        _ => {
            op.get_document("filter").map_err(|_| missing("filter"))?;
            Op::Delete { ns, op }
        }
    })
}

/// Per-op failure entry: `{ok: 0, idx, code, errmsg, <extra>, n: 0[, nModified: 0]}`.
fn error_entry(idx: usize, e: &Error, update: bool) -> Document {
    let mut d = doc! {"ok": 0, "idx": idx as i32, "code": e.code, "errmsg": e.msg.clone()};
    if let Some(x) = &e.extra {
        for (k, v) in x {
            if k != "writeErrors" {
                d.insert(k.clone(), v.clone());
            }
        }
    }
    d.insert("n", 0);
    if update {
        d.insert("nModified", 0);
    }
    d
}

pub fn bulk_write(ctx: &mut CmdCtx) -> Result<Document> {
    if ctx.db != "admin" {
        return Err(Error::new(13, "bulkWrite may only be run against the admin database."));
    }
    let cmd = ctx.cmd.clone();
    let ops: Vec<&Document> = match cmd.get("ops") {
        Some(Bson::Array(a)) => a.iter().map(|o| o.as_document().ok_or_else(|| Error::type_mismatch("BSON field 'bulkWrite.ops' must be an array of objects"))).collect::<Result<_>>()?,
        Some(_) => return Err(Error::type_mismatch("BSON field 'bulkWrite.ops' is the wrong type, expected type 'array'")),
        None => return Err(Error::new(40414, "BSON field 'bulkWrite.ops' is missing but a required field")),
    };
    if ops.is_empty() || ops.len() > 100_000 {
        return Err(Error::new(16, format!("Write batch sizes must be between 1 and 100000. Got {} operations.", ops.len())));
    }
    let ns_info: Vec<String> = match cmd.get("nsInfo") {
        Some(Bson::Array(a)) => a
            .iter()
            .map(|e| e.as_document().and_then(|d| d.get_str("ns").ok()).map(str::to_string).ok_or_else(|| missing_ns()))
            .collect::<Result<_>>()?,
        Some(_) => return Err(Error::type_mismatch("BSON field 'bulkWrite.nsInfo' is the wrong type, expected type 'array'")),
        None => return Err(Error::new(40414, "BSON field 'bulkWrite.nsInfo' is missing but a required field")),
    };
    // Every entry is validated before anything executes.
    let parsed: Vec<Op> = ops.iter().map(|o| parse_op(o, &ns_info)).collect::<Result<_>>()?;
    let ordered = cmd.get_bool("ordered").unwrap_or(true);
    let errors_only = bsonutil::as_bool(cmd.get("errorsOnly"));
    let _bypass = crate::storage::bypass_validation(bsonutil::as_bool(cmd.get("bypassDocumentValidation")));
    let vars = crud::command_vars(ctx, &cmd)?;

    let (mut n_ins, mut n_match, mut n_mod, mut n_ups, mut n_del, mut n_err) = (0i64, 0i64, 0i64, 0i64, 0i64, 0i64);
    let mut results: Vec<Bson> = Vec::new();
    for (idx, op) in parsed.iter().enumerate() {
        let is_update = matches!(op, Op::Update { .. });
        let r: Result<Document> = match op {
            Op::Insert { ns, doc } => insert_one(ctx, ns, doc).map(|_| {
                n_ins += 1;
                doc! {"ok": 1, "idx": idx as i32, "n": 1}
            }),
            Op::Update { ns, op } => run_update(ctx, ns, op, &vars).map(|(o, entry)| {
                n_match += o.matched;
                n_mod += o.modified;
                if o.upserted.is_some() {
                    n_ups += 1;
                }
                let mut e = doc! {"ok": 1, "idx": idx as i32};
                e.extend(entry);
                e
            }),
            Op::Delete { ns, op } => run_delete(ctx, ns, op, &vars).map(|n| {
                n_del += n;
                doc! {"ok": 1, "idx": idx as i32, "n": small(n)}
            }),
        };
        match r {
            Ok(d) => {
                if !errors_only {
                    results.push(Bson::Document(d));
                }
            }
            Err(e) => {
                if ctx.in_txn() {
                    return Err(e);
                }
                n_err += 1;
                results.push(Bson::Document(error_entry(idx, &e, is_update)));
                if ordered {
                    break;
                }
            }
        }
    }
    let batch_size = cmd.get_document("cursor").ok().and_then(|c| c.get("batchSize")).and_then(bsonutil::num_i64).map(|n| n.max(0) as usize);
    let cur = Cursor::new("admin.$cmd.bulkWrite".into(), Box::new(results.into_iter().map(|b| match b {
        Bson::Document(d) => Ok(d),
        _ => Ok(Document::new()),
    })));
    let reply = cursor::first_batch(&ctx.srv.cursors, cur, Some(batch_size.unwrap_or(usize::MAX)), false)?;
    // bulkWrite's cursor document lists `id` before `firstBatch`.
    let c = reply.get_document("cursor").cloned().unwrap_or_default();
    let mut out = doc! {"cursor": {
        "id": c.get("id").cloned().unwrap_or(Bson::Int64(0)),
        "firstBatch": c.get("firstBatch").cloned().unwrap_or(Bson::Array(Vec::new())),
        "ns": "admin.$cmd.bulkWrite",
    }};
    out.insert("nErrors", small(n_err));
    out.insert("nInserted", small(n_ins));
    out.insert("nMatched", small(n_match));
    out.insert("nModified", small(n_mod));
    out.insert("nUpserted", small(n_ups));
    out.insert("nDeleted", small(n_del));
    out.insert("ok", 1.0);
    Ok(out)
}

fn missing_ns() -> Error {
    Error::new(40414, "BSON field 'bulkWrite.nsInfo.ns' is missing but a required field")
}

fn small(n: i64) -> Bson {
    i32::try_from(n).map(Bson::Int32).unwrap_or(Bson::Int64(n))
}

fn insert_one(ctx: &mut CmdCtx, ns: &str, d: &Document) -> Result<()> {
    let c = ctx.srv.store.coll_for_write(ns)?;
    ctx.write(|s, t| s.insert(t, &c, d.clone()).map(|_| ()))
}

fn run_update(ctx: &mut CmdCtx, ns: &str, op: &Document, vars: &crate::agg::expr::Vars) -> Result<(crud::UpdateOutcome, Document)> {
    let _coll = match op.get("collation") {
        Some(v) => Some(crate::collation::enter(crud::parse_collation(v)?)),
        None => None,
    };
    let q = op.get_document("filter").cloned().unwrap_or_default();
    let u = op.get("updateMods").cloned().unwrap_or(Bson::Null);
    let multi = bsonutil::as_bool(op.get("multi"));
    let sort = op.get_document("sort").ok().filter(|s| !s.is_empty());
    if multi && sort.is_some() {
        return Err(Error::failed_to_parse("Cannot specify sort with multi=true"));
    }
    let mut v = vars.clone();
    if let Ok(c) = op.get_document("constants") {
        for (k, x) in c {
            v.insert(k.clone(), x.clone());
        }
    }
    let o = crud::exec_update(
        ctx,
        &UpdateArgs {
            ns,
            q: &q,
            u: &u,
            upsert: bsonutil::as_bool(op.get("upsert")),
            multi,
            array_filters: op.get_array("arrayFilters").ok(),
            hint: op.get("hint").filter(|h| !matches!(h, Bson::Document(d) if d.is_empty())),
            sort,
            vars: &v,
        },
    )?;
    let mut entry = doc! {};
    match &o.upserted {
        Some(id) => {
            entry.insert("n", 1);
            entry.insert("nModified", 0);
            entry.insert("upserted", doc! {"_id": id.clone()});
        }
        None => {
            entry.insert("n", small(o.matched));
            entry.insert("nModified", small(o.modified));
        }
    }
    Ok((o, entry))
}

fn run_delete(ctx: &mut CmdCtx, ns: &str, op: &Document, vars: &crate::agg::expr::Vars) -> Result<i64> {
    let _coll = match op.get("collation") {
        Some(v) => Some(crate::collation::enter(crud::parse_collation(v)?)),
        None => None,
    };
    let q = op.get_document("filter").cloned().unwrap_or_default();
    let hint = op.get("hint").filter(|h| !matches!(h, Bson::Document(d) if d.is_empty()));
    Ok(crud::exec_delete(ctx, ns, &q, bsonutil::as_bool(op.get("multi")), hint, None, vars)?.0)
}
