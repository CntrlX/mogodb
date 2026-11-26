//! CRUD commands, aggregation entry point, cursors and explain.

use crate::agg::expr::Vars;
use crate::agg::pipeline::{self, AggContext, AggOptions, DocIter};
use crate::bsonutil::{self, META_KEY};
use crate::cursor::{self, Cursor};
use crate::error::{Error, Result};
use crate::query::matcher::Matcher;
use crate::query::projection::{Projection, ProjectionKind};
use crate::query::sort::SortSpec;
use crate::server::{CmdCtx, ReadSrc};
use crate::deps::{self, Fields};
use crate::planner::{self, Plan, QueryShape};
use crate::storage::{Coll, RawIter, Store};
use crate::update::{self, UpdateDoc};
use crate::with_reader;
use bson::{Bson, Document, doc};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub const MAX_SORT_BYTES: usize = 100 * 1024 * 1024;

pub fn route(ctx: &mut CmdCtx) -> Result<Document> {
    let col = command_collation(ctx)?;
    let _collation = crate::collation::enter(col);
    let _bypass = crate::storage::bypass_validation(bsonutil::as_bool(ctx.cmd.get("bypassDocumentValidation")));
    match ctx.name.as_str() {
        "find" => find(ctx),
        "insert" => insert(ctx),
        "update" => update_cmd(ctx),
        "delete" => delete_cmd(ctx),
        "findAndModify" | "findandmodify" => find_and_modify(ctx),
        "count" => count(ctx),
        "distinct" => distinct(ctx),
        "aggregate" => aggregate(ctx),
        "getMore" => get_more(ctx),
        "killCursors" => kill_cursors(ctx),
        "explain" => explain(ctx),
        n => Err(Error::command_not_found(n)),
    }
}

// ------------------------------------------------------------ helpers

/// Parse a `collation` option value (`None` for simple).
pub fn parse_collation(v: &Bson) -> Result<Option<Arc<crate::collation::Collation>>> {
    match v {
        Bson::Document(d) => crate::collation::Collation::parse(d),
        other => Err(Error::type_mismatch(format!("BSON field 'collation' is the wrong type '{}', expected type 'object'", bsonutil::type_name(other)))),
    }
}

/// The command's collation: explicit `collation` option, else the target collection's default.
fn command_collation(ctx: &CmdCtx) -> Result<Option<Arc<crate::collation::Collation>>> {
    if let Some(v) = ctx.cmd.get("collation") {
        return parse_collation(v);
    }
    if matches!(ctx.name.as_str(), "find" | "count" | "distinct" | "aggregate" | "update" | "delete" | "findAndModify" | "findandmodify") {
        if let Ok(cn) = ctx.coll_name() {
            if let Some(c) = ctx.srv.store.get_coll(&ctx.ns(&cn)) {
                return Ok(c.collation.clone());
            }
        }
    }
    Ok(None)
}

pub fn opt_doc(cmd: &Document, k: &str) -> Result<Option<Document>> {
    match cmd.get(k) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::Document(d)) => Ok(Some(d.clone())),
        Some(v) => Err(Error::type_mismatch(format!("BSON field '{k}' is the wrong type '{}', expected type 'object'", bsonutil::type_name(v)))),
    }
}

fn opt_i64(cmd: &Document, k: &str) -> Result<Option<i64>> {
    match cmd.get(k) {
        None | Some(Bson::Null) => Ok(None),
        Some(v) => bsonutil::num_i64(v)
            .map(Some)
            .ok_or_else(|| Error::type_mismatch(format!("BSON field '{k}' is the wrong type '{}', expected types '[long, int, decimal, double]'", bsonutil::type_name(v)))),
    }
}

fn opt_nonneg(cmd: &Document, k: &str) -> Result<Option<i64>> {
    let v = opt_i64(cmd, k)?;
    if let Some(n) = v {
        if n < 0 {
            return Err(Error::new(51024, format!("BSON field '{k}' value must be >= 0, actual value '{n}'")));
        }
    }
    Ok(v)
}

/// System + `let` variables.
pub fn command_vars(ctx: &CmdCtx, cmd: &Document) -> Result<Vars> {
    let mut vars = Vars::new();
    vars.insert("NOW".into(), Bson::DateTime(bson::DateTime::now()));
    // `$$CLUSTER_TIME` exists only on replica sets (8.2+ errors on standalone).
    if ctx.srv.cfg.repl_set.is_some() {
        vars.insert("CLUSTER_TIME".into(), Bson::Timestamp(ctx.srv.store.cluster_time()));
    }
    if let Some(l) = opt_doc(cmd, "let")? {
        let empty = Document::new();
        for (k, v) in &l {
            let val = crate::agg::expr::eval(v, &empty, &vars)?;
            vars.insert(k.clone(), val);
        }
    }
    Ok(vars)
}

fn docs_array(cmd: &Document, k: &str) -> Result<Vec<Document>> {
    match cmd.get(k) {
        Some(Bson::Array(a)) => a
            .iter()
            .map(|v| match v {
                Bson::Document(d) => Ok(d.clone()),
                o => Err(Error::type_mismatch(format!("BSON field '{k}' element is the wrong type '{}', expected type 'object'", bsonutil::type_name(o)))),
            })
            .collect(),
        None => Err(Error::new(40414, format!("BSON field '{k}' is missing but a required field"))),
        Some(v) => Err(Error::type_mismatch(format!("BSON field '{k}' is the wrong type '{}', expected type 'array'", bsonutil::type_name(v)))),
    }
}

pub fn write_error(i: usize, e: &Error) -> Bson {
    let mut d = doc! {"index": i as i32, "code": e.code, "errmsg": e.msg.clone()};
    if let Some(x) = &e.extra {
        for (k, v) in x {
            d.insert(k.clone(), v.clone());
        }
    }
    Bson::Document(d)
}

fn text_fields(c: &Coll) -> Option<Vec<(String, f64)>> {
    c.text_index().and_then(|i| match &i.kind {
        crate::storage::IndexKind::Text { weights } => Some(weights.clone()),
        _ => None,
    })
}

pub fn build_matcher(c: Option<&Coll>, filter: &Document, vars: &Vars) -> Result<Matcher> {
    let mut m = Matcher::with_vars(filter, vars.clone())?;
    if m.has_text() {
        match c.and_then(text_fields) {
            Some(f) => m.set_text_fields(f),
            None => return Err(Error::new(27, "text index required for $text query")),
        }
    }
    Ok(m)
}

/// Item flowing through find execution: record id, document, positional match index.
type Item = (u64, Document, Option<usize>);
type ItemIter = Box<dyn Iterator<Item = Result<Item>> + Send>;

/// Detect a top-level `$near`/`$nearSphere` predicate: (path, spec).
fn find_near(filter: &Document) -> Result<Option<(String, crate::geo::NearSpec)>> {
    for (k, v) in filter {
        if let Bson::Document(d) = v {
            for op in ["$near", "$nearSphere"] {
                if let Some(val) = d.get(op) {
                    let mut spec = crate::geo::parse_near(op, val)?;
                    if let Some(m) = d.get("$maxDistance").and_then(bsonutil::num_f64) {
                        spec.max_distance = Some(m);
                    }
                    if let Some(m) = d.get("$minDistance").and_then(bsonutil::num_f64) {
                        spec.min_distance = Some(m);
                    }
                    return Ok(Some((k.clone(), spec)));
                }
            }
        }
    }
    Ok(None)
}

/// Candidate records (raw bytes) for a plan, from the snapshot or the session transaction.
fn base_raw(ctx: &mut CmdCtx, c: &Arc<Coll>, plan: &Plan) -> Result<RawIter> {
    let store = ctx.srv.store.clone();
    match ctx.reader() {
        ReadSrc::Snap(s) => store.execute_snapshot_raw(s, c, plan),
        ReadSrc::Txn(t) => {
            let v: Vec<_> = store.execute_raw(&t.tx, c, plan)?.collect();
            Ok(Box::new(v.into_iter()))
        }
    }
}

fn query_items(ctx: &mut CmdCtx, c: &Arc<Coll>, filter: &Document, hint: Option<&Bson>, vars: &Vars, reverse_natural: bool) -> Result<(ItemIter, Plan)> {
    query_items_with(ctx, c, filter, hint, vars, reverse_natural, None, QueryShape::default())
}

/// Filtered stream (with `$text` score and `$near` distance metadata, `$near` sorted).
///
/// Zero-copy filtering: only the fields the filter reads are decoded to evaluate it;
/// matching documents are then decoded fully, or only `out_fields` when given.
#[allow(clippy::too_many_arguments)]
fn query_items_with(ctx: &mut CmdCtx, c: &Arc<Coll>, filter: &Document, hint: Option<&Bson>, vars: &Vars, reverse_natural: bool, out_fields: Option<Arc<Fields>>, shape: QueryShape) -> Result<(ItemIter, Plan)> {
    let matcher = build_matcher(Some(c), filter, vars)?;
    let near = find_near(filter)?;
    if near.is_some() && !c.indexes().iter().any(|i| matches!(i.kind, crate::storage::IndexKind::Geo2dsphere | crate::storage::IndexKind::Geo2d)) {
        return Err(Error::new(291, "error processing query: planner returned error :: caused by :: unable to find index for $geoNear query"));
    }
    let shape = if near.is_some() { QueryShape::default() } else { shape };
    let mut plan = ctx.srv.store.plan_query(c, filter, hint, shape)?.0;
    if reverse_natural {
        if let Plan::CollScan { reverse } = &mut plan {
            *reverse = true;
        }
    }
    let base = base_raw(ctx, c, &plan)?;
    let has_text = matcher.has_text();
    let filter_fields = if filter.is_empty() { Some(Fields::new()) } else { deps::filter_fields(filter) };
    let full_out = out_fields.is_none();
    let decode_out = move |raw: &[u8]| match &out_fields {
        Some(f) => bsonutil::from_bytes_fields(raw, f),
        None => bsonutil::from_bytes(raw),
    };
    let it: ItemIter = Box::new(base.filter_map(move |r| {
        let (rid, raw) = match r {
            Ok(x) => x,
            Err(e) => return Some(Err(e)),
        };
        let step = || -> Result<Option<Item>> {
            let (pos, full) = match &filter_fields {
                Some(ff) if ff.is_empty() => (None, None),
                Some(ff) => {
                    let partial = bsonutil::from_bytes_fields(&raw, ff)?;
                    match matcher.matches_with_position(&partial)? {
                        (true, pos) => (pos, None),
                        (false, _) => return Ok(None),
                    }
                }
                None => {
                    let d = bsonutil::from_bytes(&raw)?;
                    match matcher.matches_with_position(&d)? {
                        (true, pos) => (pos, Some(d)),
                        (false, _) => return Ok(None),
                    }
                }
            };
            // `$text` filters always take the full-decode path, so `full` is set for scoring.
            let score = match (&full, has_text) {
                (Some(fd), true) => Some(matcher.text_score(fd)),
                _ => None,
            };
            let mut d = match full {
                Some(d) if full_out => d,
                _ => decode_out(&raw)?,
            };
            if let Some(s) = score {
                set_meta(&mut d, "textScore", Bson::Double(s));
            }
            Ok(Some((rid, d, pos)))
        };
        step().transpose()
    }));
    if let Some((path, spec)) = near {
        let mut v = Vec::new();
        for r in it {
            let (rid, mut d, pos) = r?;
            let dist = crate::agg::expr::resolve_field(&d, &path);
            if let Some(dd) = crate::geo::near_distance(&dist, &spec)
                .filter(|dd| spec.max_distance.is_none_or(|m| *dd <= m) && spec.min_distance.is_none_or(|m| *dd >= m))
            {
                set_meta(&mut d, "geoNearDistance", Bson::Double(dd));
                v.push((dd, (rid, d, pos)));
            }
        }
        v.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        return Ok((Box::new(v.into_iter().map(|(_, x)| Ok(x))), plan));
    }
    Ok((it, plan))
}

pub fn set_meta(d: &mut Document, k: &str, v: Bson) {
    match d.get_mut(META_KEY) {
        Some(Bson::Document(m)) => {
            m.insert(k, v);
        }
        _ => {
            d.insert(META_KEY, doc! {k: v});
        }
    }
}

fn sort_items(items: ItemIter, spec: &SortSpec, allow_disk: bool, top: Option<usize>) -> Result<ItemIter> {
    let mut v: Vec<(Vec<Bson>, Item)> = Vec::new();
    let mut bytes = 0usize;
    for r in items {
        let it = r?;
        bytes += bsonutil::doc_size(&it.1);
        if bytes > MAX_SORT_BYTES && !allow_disk {
            return Err(Error::new(292, format!("Executor error during find command :: caused by :: Sort exceeded memory limit of {MAX_SORT_BYTES} bytes, but did not opt in to external sorting.")));
        }
        v.push((spec.sort_key(&it.1), it));
        // Bounded top-k: periodically trim.
        if let Some(k) = top {
            if v.len() >= k.max(1) * 2 + 1024 {
                v.sort_by(|a, b| spec.compare_keys(&a.0, &b.0));
                v.truncate(k);
            }
        }
    }
    v.sort_by(|a, b| spec.compare_keys(&a.0, &b.0));
    if let Some(k) = top {
        v.truncate(k);
    }
    Ok(Box::new(v.into_iter().map(|(_, x)| Ok(x))))
}

fn coll_or_view(ctx: &CmdCtx, ns: &str) -> Option<Arc<Coll>> {
    ctx.srv.store.get_coll(ns)
}

/// Resolve a view chain to (source collection name, prefix pipeline).
pub fn resolve_view(store: &Store, db: &str, coll: &str) -> Result<(String, Vec<Document>)> {
    let mut name = coll.to_string();
    let mut pipeline: Vec<Document> = Vec::new();
    for _ in 0..20 {
        match store.get_coll(&format!("{db}.{name}")) {
            Some(c) if c.is_view() => {
                let src = c.options.get_str("viewOn").unwrap_or_default().to_string();
                let mut p: Vec<Document> = c.options.get_array("pipeline").map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect()).unwrap_or_default();
                p.extend(pipeline);
                pipeline = p;
                name = src;
            }
            _ => return Ok((name, pipeline)),
        }
    }
    Err(Error::new(17, "View depth too deep or view cycle detected; maximum depth is 20"))
}

// ------------------------------------------------------------ find

fn find(ctx: &mut CmdCtx) -> Result<Document> {
    let cname = ctx.coll_name()?;
    let ns = ctx.ns(&cname);
    let cmd = ctx.cmd.clone();
    let filter = opt_doc(&cmd, "filter")?.unwrap_or_default();
    let sort = opt_doc(&cmd, "sort")?.filter(|s| !s.is_empty());
    let projection = opt_doc(&cmd, "projection")?.filter(|p| !p.is_empty());
    let skip = opt_nonneg(&cmd, "skip")?.unwrap_or(0) as usize;
    let raw_limit = opt_i64(&cmd, "limit")?.unwrap_or(0);
    let mut single_batch = bsonutil::as_bool(cmd.get("singleBatch"));
    if raw_limit < 0 {
        single_batch = true;
    }
    let limit = raw_limit.unsigned_abs() as usize;
    let batch_size = opt_nonneg(&cmd, "batchSize")?.map(|b| b as usize);
    let vars = command_vars(ctx, &cmd)?;
    ctx.srv.counters.query.fetch_add(1, Ordering::Relaxed);
    let Some(c) = coll_or_view(ctx, &ns) else {
        return Ok(cursor::cursor_reply(&ns, 0, Vec::new(), true));
    };
    if c.is_view() {
        let mut stages = vec![doc! {"$match": filter}];
        if let Some(s) = sort {
            stages.push(doc! {"$sort": s});
        }
        if skip > 0 {
            stages.push(doc! {"$skip": skip as i64});
        }
        if limit > 0 {
            stages.push(doc! {"$limit": limit as i64});
        }
        if let Some(p) = projection {
            stages.push(doc! {"$project": p});
        }
        let it = run_agg_on(ctx, &cname, stages, &cmd, vars)?;
        let mut cur = Cursor::new(ns, it);
        cur.lsid = cmd.get("lsid").cloned();
        return cursor::first_batch(&ctx.srv.cursors, cur, batch_size, single_batch);
    }
    let it = find_iter(ctx, &c, &cmd, filter, sort, projection, skip, limit, vars)?;
    let mut cur = Cursor::new(ns, it);
    cur.lsid = cmd.get("lsid").cloned();
    cur.no_timeout = bsonutil::as_bool(cmd.get("noCursorTimeout"));
    if ctx.in_txn() {
        // Transaction reads are materialized; the cursor stays valid after commit.
        let v: Vec<Result<Document>> = cur.iter.by_ref().collect();
        cur.iter = Box::new(v.into_iter());
    }
    cursor::first_batch(&ctx.srv.cursors, cur, batch_size, single_batch)
}

#[allow(clippy::too_many_arguments)]
fn find_iter(ctx: &mut CmdCtx, c: &Arc<Coll>, cmd: &Document, filter: Document, sort: Option<Document>, projection: Option<Document>, skip: usize, limit: usize, vars: Vars) -> Result<DocIter> {
    {
        let mut probe = vec![doc! {"$match": filter.clone()}];
        if let Some(s) = &sort {
            probe.push(doc! {"$sort": s.clone()});
        }
        if let Some(p) = &projection {
            probe.push(doc! {"$project": p.clone()});
        }
        crate::agg::pipeline::check_meta(&probe, &Default::default())?;
    }
    let hint = cmd.get("hint").filter(|h| !matches!(h, Bson::Document(d) if d.is_empty()) && !matches!(h, Bson::Null));
    let natural_sort = sort.as_ref().and_then(|s| s.get("$natural")).and_then(bsonutil::num_f64);
    let sort = sort.filter(|_| natural_sort.is_none());
    let proj = projection.as_ref().map(|p| Projection::new(p, ProjectionKind::Find)).transpose()?;
    let sort_spec = sort.as_ref().map(SortSpec::new).transpose()?;
    let allow_disk = bsonutil::as_bool(cmd.get("allowDiskUse")) || cmd.get("allowDiskUse").is_none();
    // `$natural` sort forces a collection scan in that direction.
    let natural_hint = natural_sort.map(|n| Bson::Document(doc! {"$natural": if n < 0.0 { -1 } else { 1 }}));
    let hint = natural_hint.as_ref().or(hint);
    let shape = QueryShape { sort: sort.as_ref(), projection: projection.as_ref(), limit: (limit > 0).then_some(skip + limit) };
    let (mut items, plan) = query_items_with(ctx, c, &filter, hint, &vars, false, None, shape)?;
    if let Some(spec) = sort_spec.as_ref().filter(|_| !plan.provides_sort()) {
        let top = (limit > 0).then_some(skip + limit);
        items = sort_items(items, spec, allow_disk, top)?;
    }
    let mut it: ItemIter = items;
    if skip > 0 {
        it = Box::new(it.skip(skip));
    }
    if limit > 0 {
        it = Box::new(it.take(limit));
    }
    let show_rid = bsonutil::as_bool(cmd.get("showRecordId"));
    let return_key = bsonutil::as_bool(cmd.get("returnKey"));
    let out: DocIter = Box::new(it.map(move |r| {
        let (rid, d, pos) = r?;
        let mut d = match &proj {
            Some(p) => p.apply(&d, pos, &vars)?,
            None => d,
        };
        if return_key {
            d = Document::new();
        }
        if show_rid {
            d.insert("$recordId", rid as i64);
        }
        Ok(d)
    }));
    Ok(out)
}

// ------------------------------------------------------------ count / distinct

fn count(ctx: &mut CmdCtx) -> Result<Document> {
    let cname = ctx.coll_name()?;
    let ns = ctx.ns(&cname);
    let cmd = ctx.cmd.clone();
    let filter = opt_doc(&cmd, "query")?.unwrap_or_default();
    let skip = opt_nonneg(&cmd, "skip")?.unwrap_or(0) as usize;
    let limit = opt_i64(&cmd, "limit")?.unwrap_or(0).unsigned_abs() as usize;
    let Some(c) = coll_or_view(ctx, &ns) else { return Ok(doc! {"n": 0, "ok": 1.0}) };
    let vars = command_vars(ctx, &cmd)?;
    let n = if c.is_view() {
        let mut stages = vec![doc! {"$match": filter}];
        if skip > 0 {
            stages.push(doc! {"$skip": skip as i64});
        }
        if limit > 0 {
            stages.push(doc! {"$limit": limit as i64});
        }
        let mut n = 0usize;
        for r in run_agg_on(ctx, &cname, stages, &cmd, vars)? {
            r?;
            n += 1;
        }
        n
    } else if filter.is_empty() && !ctx.in_txn() {
        let total = c.count.load(Ordering::Relaxed).max(0) as usize;
        let after = total.saturating_sub(skip);
        if limit > 0 { after.min(limit) } else { after }
    } else {
        let hint = cmd.get("hint").filter(|h| !matches!(h, Bson::Document(d) if d.is_empty()));
        let (items, _) = query_items(ctx, &c, &filter, hint, &vars, false)?;
        let mut n = 0usize;
        let mut seen = 0usize;
        for r in items {
            r?;
            seen += 1;
            if seen <= skip {
                continue;
            }
            n += 1;
            if limit > 0 && n >= limit {
                break;
            }
        }
        n
    };
    Ok(doc! {"n": n as i32, "ok": 1.0})
}

fn distinct(ctx: &mut CmdCtx) -> Result<Document> {
    let cname = ctx.coll_name()?;
    let ns = ctx.ns(&cname);
    let cmd = ctx.cmd.clone();
    let key = match cmd.get("key") {
        Some(Bson::String(k)) => k.clone(),
        Some(v) => return Err(Error::type_mismatch(format!("BSON field 'distinct.key' is the wrong type '{}', expected type 'string'", bsonutil::type_name(v)))),
        None => return Err(Error::new(40414, "BSON field 'distinct.key' is missing but a required field")),
    };
    let filter = opt_doc(&cmd, "query")?.unwrap_or_default();
    let Some(c) = coll_or_view(ctx, &ns) else { return Ok(doc! {"values": [], "ok": 1.0}) };
    let vars = command_vars(ctx, &cmd)?;
    let docs: DocIter = if c.is_view() {
        run_agg_on(ctx, &cname, vec![doc! {"$match": filter}], &cmd, vars)?
    } else {
        let (items, _) = query_items(ctx, &c, &filter, cmd.get("hint"), &vars, false)?;
        Box::new(items.map(|r| r.map(|x| x.1)))
    };
    let mut vals: Vec<Bson> = Vec::new();
    let push = |v: Bson, vals: &mut Vec<Bson>| {
        if !bsonutil::is_missing(&v) && !vals.iter().any(|x| bsonutil::bson_eq(x, &v) && bsonutil::type_order(x) == bsonutil::type_order(&v)) {
            vals.push(v);
        }
    };
    let segs: Vec<&str> = key.split('.').collect();
    for r in docs {
        let d = r?;
        let mut found = Vec::new();
        distinct_values(&Bson::Document(d), &segs, &mut found);
        for v in found {
            push(v, &mut vals);
        }
    }
    vals.sort_by(bsonutil::cmp_bson);
    Ok(doc! {"values": vals, "ok": 1.0})
}

/// Values at a dotted path for `distinct`: arrays met along the path are traversed
/// (document elements only), and a leaf array contributes each element.
fn distinct_values(v: &Bson, segs: &[&str], out: &mut Vec<Bson>) {
    let Some((first, rest)) = segs.split_first() else {
        match v {
            Bson::Array(a) => out.extend(a.iter().cloned()),
            other => out.push(other.clone()),
        }
        return;
    };
    match v {
        Bson::Document(d) => {
            if let Some(x) = d.get(*first) {
                distinct_values(x, rest, out);
            }
        }
        Bson::Array(a) => {
            if let Some(x) = first.parse::<usize>().ok().and_then(|i| a.get(i)) {
                distinct_values(x, rest, out);
            }
            for e in a {
                if let Bson::Document(_) = e {
                    distinct_values(e, segs, out);
                }
            }
        }
        _ => {}
    }
}

// ------------------------------------------------------------ insert

fn insert(ctx: &mut CmdCtx) -> Result<Document> {
    let cname = ctx.coll_name()?;
    let ns = ctx.ns(&cname);
    let docs = docs_array(&ctx.cmd, "documents")?;
    if docs.len() > 100_000 {
        return Err(Error::new(10334, format!("Write batch sizes must be between 1 and 100000. Got {} operations.", docs.len())));
    }
    if cname.starts_with("system.") && !matches!(cname.as_str(), "system.users" | "system.roles" | "system.js") {
        return Err(Error::new(73, format!("cannot write to '{ns}'")));
    }
    let ordered = ctx.cmd.get_bool("ordered").unwrap_or(true);
    let c = match ctx.srv.store.coll_for_write(&ns) {
        Ok(c) => c,
        // Namespace-level failures (e.g. views) are reported as a write error on the first document.
        Err(e) if e.code == 166 && !ctx.in_txn() => return Ok(doc! {"n": 0, "writeErrors": [write_error(0, &e)], "ok": 1.0}),
        Err(e) => return Err(e),
    };
    ctx.srv.counters.insert.fetch_add(docs.len() as i64, Ordering::Relaxed);
    let mut n = 0i32;
    let mut errors = Vec::new();
    if ctx.in_txn() {
        for (i, d) in docs.into_iter().enumerate() {
            ctx.write(|s, t| s.insert(t, &c, d.clone()).map(|_| ()))
                .map_err(|e| { let we = write_error(i, &e); e.with_extra(doc! {"writeErrors": [we]}) })?;
            n += 1;
        }
        return Ok(doc! {"n": n, "ok": 1.0});
    }
    // Batch documents into shared transactions; fall back to per-document on error.
    const CHUNK: usize = 256;
    let mut i = 0;
    let total = docs.len();
    'outer: while i < total {
        let end = (i + CHUNK).min(total);
        let chunk = &docs[i..end];
        let batch = ctx.write(|s, t| {
            for d in chunk {
                s.insert(t, &c, d.clone())?;
            }
            Ok(())
        });
        match batch {
            Ok(()) => n += chunk.len() as i32,
            Err(_) => {
                for (j, d) in chunk.iter().enumerate() {
                    match ctx.write(|s, t| s.insert(t, &c, d.clone()).map(|_| ())) {
                        Ok(()) => n += 1,
                        Err(e) => {
                            errors.push(write_error(i + j, &e));
                            if ordered {
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }
        i = end;
    }
    let mut out = doc! {"n": n};
    if !errors.is_empty() {
        out.insert("writeErrors", errors);
    }
    out.insert("ok", 1.0);
    Ok(out)
}

// ------------------------------------------------------------ update / delete

pub struct UpdateOutcome {
    pub matched: i64,
    pub modified: i64,
    pub upserted: Option<Bson>,
    pub old: Option<Document>,
    pub new: Option<Document>,
}

pub struct UpdateArgs<'a> {
    pub ns: &'a str,
    pub q: &'a Document,
    pub u: &'a Bson,
    pub upsert: bool,
    pub multi: bool,
    pub array_filters: Option<&'a Vec<Bson>>,
    pub hint: Option<&'a Bson>,
    pub sort: Option<&'a Document>,
    pub vars: &'a Vars,
}

/// Record ids of documents matching `q`, in plan (or `sort`) order.
fn matching_rids(ctx: &mut CmdCtx, c: &Arc<Coll>, q: &Document, hint: Option<&Bson>, sort: Option<&Document>, vars: &Vars, first_only: bool) -> Result<Vec<u64>> {
    let (mut items, _) = query_items(ctx, c, q, hint, vars, false)?;
    if let Some(s) = sort.filter(|s| !s.is_empty()) {
        let spec = SortSpec::new(s)?;
        items = sort_items(items, &spec, true, first_only.then_some(1))?;
    }
    let mut out = Vec::new();
    for r in items {
        out.push(r?.0);
        if first_only {
            break;
        }
    }
    Ok(out)
}

pub fn exec_update(ctx: &mut CmdCtx, a: &UpdateArgs) -> Result<UpdateOutcome> {
    let upd = UpdateDoc::new(a.u, a.array_filters)?;
    if upd.is_replacement() && a.multi {
        return Err(Error::failed_to_parse("multi update is not supported for replacement-style update"));
    }
    let mut out = UpdateOutcome { matched: 0, modified: 0, upserted: None, old: None, new: None };
    let c = match ctx.srv.store.get_coll(a.ns) {
        Some(c) if c.is_view() => return Err(Error::new(166, format!("Namespace {} is a view, not a collection", a.ns))),
        Some(c) => Some(c),
        None if a.upsert => Some(ctx.srv.store.coll_for_write(a.ns)?),
        None => None,
    };
    let Some(c) = c else { return Ok(out) };
    if let Some(ts) = crate::timeseries::spec(&c.options) {
        crate::timeseries::check_update(&ts, a.q, a.u, a.multi, a.upsert)?;
    }
    let matcher = build_matcher(Some(&c), a.q, a.vars)?;
    let rids = matching_rids(ctx, &c, a.q, a.hint, a.sort, a.vars, !a.multi)?;
    let repl = upd.is_replacement();
    for rid in rids {
        ctx.check_killed()?;
        let r = ctx.write(|s, t| {
            let Some(doc) = s.get_rid(&t.tx, &c, rid)? else { return Ok(None) };
            let (ok, pos) = matcher.matches_with_position(&doc)?;
            if !ok {
                return Ok(None);
            }
            let new = upd.apply(&doc, pos, false, a.vars)?;
            check_id_unchanged(&doc, &new)?;
            let changed = bsonutil::to_bytes(&doc) != bsonutil::to_bytes(&new);
            if changed {
                let ud = crate::changestream::update_description(&doc, &new);
                let mut ev = doc! {"ud": ud};
                if repl {
                    ev.insert("repl", true);
                }
                s.update(t, &c, rid, &doc, new.clone(), ev).map_err(|e| if e.code == 510 { Error { msg: format!("Plan executor error during update :: caused by :: {}", e.msg), ..e } } else { e })?;
            }
            Ok(Some((doc, new, changed)))
        })?;
        if let Some((old, new, changed)) = r {
            out.matched += 1;
            if changed {
                out.modified += 1;
            }
            if out.old.is_none() {
                out.old = Some(old);
                out.new = Some(new);
            }
        }
    }
    if out.matched == 0 && a.upsert {
        let seed = update::upsert_seed(a.q)?;
        let mut new = upd.apply(&seed, None, true, a.vars)?;
        if repl {
            if let (Some(id), false) = (seed.get("_id"), new.contains_key("_id")) {
                let mut nd = doc! {"_id": id.clone()};
                nd.extend(new);
                new = nd;
            }
        }
        let stored = ctx.write(|s, t| s.insert(t, &c, new.clone()))?;
        out.upserted = stored.get("_id").cloned();
        out.new = Some(stored);
    }
    Ok(out)
}

fn check_id_unchanged(old: &Document, new: &Document) -> Result<()> {
    match (old.get("_id"), new.get("_id")) {
        (Some(a), Some(b)) if bsonutil::bson_eq(a, b) && bsonutil::type_order(a) == bsonutil::type_order(b) => Ok(()),
        (None, _) => Ok(()),
        (Some(_), b) => Err(Error::new(
            66,
            format!("After applying the update, the (immutable) field '_id' was found to have been altered to _id: {}", b.map(|v| v.to_string()).unwrap_or_else(|| "missing".into())),
        )),
    }
}

fn update_cmd(ctx: &mut CmdCtx) -> Result<Document> {
    let cname = ctx.coll_name()?;
    let ns = ctx.ns(&cname);
    let cmd = ctx.cmd.clone();
    let stmts = docs_array(&cmd, "updates")?;
    let ordered = cmd.get_bool("ordered").unwrap_or(true);
    let vars = command_vars(ctx, &cmd)?;
    let (mut n, mut nmod) = (0i64, 0i64);
    let mut upserted = Vec::new();
    let mut errors = Vec::new();
    ctx.srv.counters.update.fetch_add(stmts.len() as i64, Ordering::Relaxed);
    for st in stmts.iter() {
        match st.get("sort") {
            None | Some(Bson::Document(_)) => {}
            Some(v) => return Err(Error::type_mismatch(format!("BSON field 'update.updates.sort' is the wrong type '{}', expected type 'object'", bsonutil::type_name(v)))),
        }
        if bsonutil::as_bool(st.get("multi")) && st.get_document("sort").is_ok_and(|s| !s.is_empty()) {
            return Err(Error::failed_to_parse("Cannot specify sort with multi=true"));
        }
    }
    for (i, st) in stmts.iter().enumerate() {
        let r = (|| {
            let _stmt_collation = match st.get("collation") {
                Some(v) => Some(crate::collation::enter(parse_collation(v)?)),
                None => None,
            };
            let q = opt_doc(st, "q")?.unwrap_or_default();
            let u = st.get("u").ok_or_else(|| Error::new(40414, "BSON field 'update.updates.u' is missing but a required field"))?;
            let mut svars = vars.clone();
            if let Some(c) = opt_doc(st, "c")? {
                for (k, v) in c {
                    svars.insert(k, v);
                }
            }
            exec_update(
                ctx,
                &UpdateArgs {
                    ns: &ns,
                    q: &q,
                    u,
                    upsert: bsonutil::as_bool(st.get("upsert")),
                    multi: bsonutil::as_bool(st.get("multi")),
                    array_filters: st.get_array("arrayFilters").ok(),
                    hint: st.get("hint").filter(|h| !matches!(h, Bson::Document(d) if d.is_empty())),
                    sort: st.get_document("sort").ok(),
                    vars: &svars,
                },
            )
        })();
        match r {
            Ok(o) => {
                n += o.matched;
                nmod += o.modified;
                if let Some(id) = o.upserted {
                    n += 1;
                    upserted.push(Bson::Document(doc! {"index": i as i32, "_id": id}));
                }
            }
            Err(e) => {
                if ctx.in_txn() {
                    return Err(e);
                }
                errors.push(write_error(i, &e));
                if ordered {
                    break;
                }
            }
        }
    }
    let mut out = doc! {"n": n as i32, "nModified": nmod as i32};
    if !upserted.is_empty() {
        out.insert("upserted", upserted);
    }
    if !errors.is_empty() {
        out.insert("writeErrors", errors);
    }
    out.insert("ok", 1.0);
    Ok(out)
}

pub fn exec_delete(ctx: &mut CmdCtx, ns: &str, q: &Document, multi: bool, hint: Option<&Bson>, sort: Option<&Document>, vars: &Vars) -> Result<(i64, Option<Document>)> {
    let Some(c) = ctx.srv.store.get_coll(ns) else { return Ok((0, None)) };
    if c.is_view() {
        return Err(Error::new(166, format!("Namespace {ns} is a view, not a collection")));
    }
    if c.capped().is_some() {
        return Err(Error::new(20, format!("cannot remove from a capped collection: {ns}")));
    }
    let matcher = build_matcher(Some(&c), q, vars)?;
    let rids = matching_rids(ctx, &c, q, hint, sort, vars, !multi)?;
    let mut n = 0;
    let mut first = None;
    for rid in rids {
        ctx.check_killed()?;
        let r = ctx.write(|s, t| {
            let Some(doc) = s.get_rid(&t.tx, &c, rid)? else { return Ok(None) };
            if !matcher.matches(&doc)? {
                return Ok(None);
            }
            s.delete(t, &c, rid, &doc)?;
            Ok(Some(doc))
        })?;
        if let Some(d) = r {
            n += 1;
            if first.is_none() {
                first = Some(d);
            }
        }
    }
    Ok((n, first))
}

fn delete_cmd(ctx: &mut CmdCtx) -> Result<Document> {
    let cname = ctx.coll_name()?;
    let ns = ctx.ns(&cname);
    let cmd = ctx.cmd.clone();
    let stmts = docs_array(&cmd, "deletes")?;
    let ordered = cmd.get_bool("ordered").unwrap_or(true);
    let vars = command_vars(ctx, &cmd)?;
    let mut n = 0i64;
    let mut errors = Vec::new();
    ctx.srv.counters.delete.fetch_add(stmts.len() as i64, Ordering::Relaxed);
    for (i, st) in stmts.iter().enumerate() {
        let r = (|| {
            let _stmt_collation = match st.get("collation") {
                Some(v) => Some(crate::collation::enter(parse_collation(v)?)),
                None => None,
            };
            let q = opt_doc(st, "q")?.unwrap_or_default();
            let limit = st.get("limit").and_then(bsonutil::num_i64).ok_or_else(|| Error::new(40414, "BSON field 'delete.deletes.limit' is missing but a required field"))?;
            if limit != 0 && limit != 1 {
                return Err(Error::failed_to_parse(format!("The limit field in delete objects must be 0 or 1. Got {limit}")));
            }
            exec_delete(ctx, &ns, &q, limit == 0, st.get("hint").filter(|h| !matches!(h, Bson::Document(d) if d.is_empty())), None, &vars)
        })();
        match r {
            Ok((k, _)) => n += k,
            Err(e) => {
                if ctx.in_txn() {
                    return Err(e);
                }
                errors.push(write_error(i, &e));
                if ordered {
                    break;
                }
            }
        }
    }
    let mut out = doc! {"n": n as i32};
    if !errors.is_empty() {
        out.insert("writeErrors", errors);
    }
    out.insert("ok", 1.0);
    Ok(out)
}

fn find_and_modify(ctx: &mut CmdCtx) -> Result<Document> {
    let cname = ctx.coll_name()?;
    let ns = ctx.ns(&cname);
    if ctx.srv.store.get_coll(&ns).is_some_and(|c| c.options.contains_key("timeseries")) {
        if ctx.cmd.contains_key("txnNumber") && !ctx.in_txn() {
            return Err(Error::new(7314600, "Retryable findAndModify on a timeseries is not supported"));
        }
        return Err(Error::new(72, "Cannot perform a non-multi update on a time-series collection"));
    }
    let cmd = ctx.cmd.clone();
    let q = opt_doc(&cmd, "query")?.unwrap_or_default();
    let sort = opt_doc(&cmd, "sort")?;
    let remove = bsonutil::as_bool(cmd.get("remove"));
    let upd = cmd.get("update");
    let upsert = bsonutil::as_bool(cmd.get("upsert"));
    let return_new = bsonutil::as_bool(cmd.get("new"));
    let fields = opt_doc(&cmd, "fields")?.filter(|f| !f.is_empty());
    let vars = command_vars(ctx, &cmd)?;
    if remove && upd.is_some() {
        return Err(Error::failed_to_parse("Cannot specify both an update and remove=true"));
    }
    if !remove && upd.is_none() {
        return Err(Error::failed_to_parse("Either an update or remove=true must be specified"));
    }
    if remove && upsert {
        return Err(Error::failed_to_parse("Cannot specify both upsert=true and remove=true "));
    }
    if remove && return_new {
        return Err(Error::failed_to_parse("Cannot specify both new=true and remove=true; 'remove' always returns the deleted document"));
    }
    let project = |d: Option<Document>| -> Result<Bson> {
        match (d, &fields) {
            (None, _) => Ok(Bson::Null),
            (Some(d), Some(f)) => Ok(Bson::Document(Projection::new(f, ProjectionKind::Find)?.apply(&d, None, &vars)?)),
            (Some(d), None) => Ok(Bson::Document(d)),
        }
    };
    if remove {
        ctx.srv.counters.delete.fetch_add(1, Ordering::Relaxed);
        let (n, old) = exec_delete(ctx, &ns, &q, false, cmd.get("hint"), sort.as_ref(), &vars)?;
        return Ok(doc! {"lastErrorObject": {"n": n as i32}, "value": project(old)?, "ok": 1.0});
    }
    ctx.srv.counters.update.fetch_add(1, Ordering::Relaxed);
    let u = upd.expect("checked above");
    let o = exec_update(
        ctx,
        &UpdateArgs {
            ns: &ns,
            q: &q,
            u,
            upsert,
            multi: false,
            array_filters: cmd.get_array("arrayFilters").ok(),
            hint: cmd.get("hint"),
            sort: sort.as_ref(),
            vars: &vars,
        },
    )?;
    let mut leo = doc! {"n": (o.matched + o.upserted.is_some() as i64) as i32, "updatedExisting": o.matched > 0};
    if let Some(id) = &o.upserted {
        leo.insert("upserted", id.clone());
    }
    let value = if return_new { o.new } else if o.upserted.is_some() { None } else { o.old };
    Ok(doc! {"lastErrorObject": leo, "value": project(value)?, "ok": 1.0})
}

// ------------------------------------------------------------ aggregate

pub struct ServerAggCtx {
    pub store: Arc<Store>,
}

impl ServerAggCtx {
    fn find_rid(&self, c: &Arc<Coll>, id: &Bson) -> Result<Option<(u64, Document)>> {
        let snap = self.store.snapshot();
        let filter = doc! {"_id": id.clone()};
        let plan = self.store.plan(c, &filter, None)?;
        for r in self.store.execute(&snap, c, &plan)? {
            let (rid, d) = r?;
            if d.get("_id").is_some_and(|x| bsonutil::bson_eq(x, id)) {
                return Ok(Some((rid, d)));
            }
        }
        Ok(None)
    }
}

impl AggContext for ServerAggCtx {
    fn scan(&self, db: &str, coll: &str) -> Result<DocIter> {
        let (src, view_pipe) = resolve_view(&self.store, db, coll)?;
        let Some(c) = self.store.get_coll(&format!("{db}.{src}")) else { return Ok(Box::new(std::iter::empty())) };
        let snap = self.store.snapshot();
        let it: DocIter = Box::new(self.store.scan(&snap, &c, false)?.map(|r| r.map(|x| x.1)));
        if view_pipe.is_empty() {
            return Ok(it);
        }
        let opts = Arc::new(AggOptions {
            db: db.to_string(),
            coll: Some(src),
            allow_disk_use: true,
            max_memory_bytes: MAX_SORT_BYTES,
            vars: Vars::new(),
            ctx: Arc::new(ServerAggCtx { store: self.store.clone() }),
        });
        pipeline::run_pipeline(it, &view_pipe, opts)
    }

    fn find(&self, db: &str, coll: &str, filter: &Document) -> Result<Vec<Document>> {
        let Some(c) = self.store.get_coll(&format!("{db}.{coll}")) else { return Ok(Vec::new()) };
        if c.is_view() {
            let m = Matcher::new(filter)?;
            return self.scan(db, coll)?.filter(|r| r.as_ref().map(|d| m.matches(d).unwrap_or(false)).unwrap_or(true)).collect();
        }
        let snap = self.store.snapshot();
        let plan = self.store.plan(&c, filter, None)?;
        let m = Matcher::new(filter)?;
        let mut out = Vec::new();
        for r in self.store.execute(&snap, &c, &plan)? {
            let (_, d) = r?;
            if m.matches(&d)? {
                out.push(d);
            }
        }
        Ok(out)
    }

    fn replace_collection(&self, db: &str, coll: &str, docs: Vec<Document>) -> Result<()> {
        let ns = format!("{db}.{coll}");
        let c = self.store.coll_for_write(&ns)?;
        let snap = self.store.snapshot();
        let existing: Vec<(u64, Document)> = self.store.scan(&snap, &c, false)?.collect::<Result<_>>()?;
        self.store.with_retry(|t| {
            for (rid, d) in &existing {
                self.store.delete(t, &c, *rid, d)?;
            }
            for d in &docs {
                let mut d = d.clone();
                d.remove(META_KEY);
                self.store.insert(t, &c, d)?;
            }
            Ok(())
        })
    }

    fn insert(&self, db: &str, coll: &str, mut doc: Document) -> Result<()> {
        let c = self.store.coll_for_write(&format!("{db}.{coll}"))?;
        doc.remove(META_KEY);
        self.store.with_retry(|t| self.store.insert(t, &c, doc.clone()).map(|_| ()))
    }

    fn replace_by_id(&self, db: &str, coll: &str, id: &Bson, mut doc: Document) -> Result<()> {
        let c = self.store.coll_for_write(&format!("{db}.{coll}"))?;
        doc.remove(META_KEY);
        let Some((rid, old)) = self.find_rid(&c, id)? else {
            return self.insert(db, coll, doc);
        };
        let ud = crate::changestream::update_description(&old, &doc);
        self.store.with_retry(|t| self.store.update(t, &c, rid, &old, doc.clone(), doc! {"ud": ud.clone(), "repl": true}))
    }

    fn coll_stats(&self, db: &str, coll: &str, spec: &Document) -> Result<Document> {
        let ns = format!("{db}.{coll}");
        let c = self.store.get_coll(&ns).ok_or_else(|| Error::ns_not_found(format!("Collection [{ns}] not found.")))?;
        let mut d = doc! {"ns": &ns, "host": "localhost", "localTime": bson::DateTime::now()};
        if spec.contains_key("latencyStats") {
            d.insert("latencyStats", doc! {"reads": {"latency": 0_i64, "ops": 0_i64}, "writes": {"latency": 0_i64, "ops": 0_i64}, "commands": {"latency": 0_i64, "ops": 0_i64}, "transactions": {"latency": 0_i64, "ops": 0_i64}});
        }
        if spec.contains_key("storageStats") {
            let count = c.count.load(Ordering::Relaxed);
            let size = c.size.load(Ordering::Relaxed);
            let idx: Vec<Document> = c.indexes().iter().map(|i| doc! {"name": &i.name}).collect();
            let mut sizes = Document::new();
            for i in c.indexes() {
                sizes.insert(i.name.clone(), 0_i64);
            }
            d.insert(
                "storageStats",
                doc! {"size": size, "count": count, "avgObjSize": if count > 0 { size / count } else { 0 }, "storageSize": size, "freeStorageSize": 0_i64, "capped": c.capped().is_some(), "nindexes": idx.len() as i32, "indexDetails": {}, "indexBuilds": [], "totalIndexSize": 0_i64, "indexSizes": sizes, "totalSize": size},
            );
        }
        if spec.contains_key("count") {
            d.insert("count", c.count.load(Ordering::Relaxed));
        }
        Ok(d)
    }

    fn index_stats(&self, db: &str, coll: &str) -> Result<Vec<Document>> {
        let Some(c) = self.store.get_coll(&format!("{db}.{coll}")) else { return Ok(Vec::new()) };
        Ok(c.indexes()
            .iter()
            .map(|i| doc! {"name": &i.name, "key": i.key.clone(), "host": "localhost:27017", "accesses": {"ops": i.accesses.load(Ordering::Relaxed), "since": i.since}, "spec": i.spec.clone()})
            .collect())
    }

    fn temp_dir(&self) -> PathBuf {
        let p = self.store.dbpath.join("_tmp");
        let _ = std::fs::create_dir_all(&p);
        p
    }

    fn search_indexes(&self, db: &str, coll: &str) -> Result<Vec<Document>> {
        crate::search::stored_indexes(&self.store, db, coll)
    }

    fn cluster_catalog(&self, db: Option<&str>) -> Result<Vec<Document>> {
        crate::cmd::qsettings::catalog_docs(&self.store, db)
    }

    fn query_settings(&self) -> Result<Vec<Document>> {
        crate::cmd::qsettings::stored_settings(&self.store)
    }
}

fn agg_opts(ctx: &CmdCtx, coll: Option<String>, cmd: &Document, vars: Vars) -> Arc<AggOptions> {
    Arc::new(AggOptions {
        db: ctx.db.clone(),
        coll,
        allow_disk_use: cmd.get("allowDiskUse").map(bsonutil::truthy).unwrap_or(true),
        max_memory_bytes: op_memory_limit(ctx),
        vars,
        ctx: Arc::new(ServerAggCtx { store: ctx.srv.store.clone() }),
    })
}

/// Run `stages` over collection/view `coll` with leading-`$match` pushdown into the planner.
pub fn run_agg_on(ctx: &mut CmdCtx, coll: &str, stages: Vec<Document>, cmd: &Document, vars: Vars) -> Result<DocIter> {
    let (src, prefix) = resolve_view(&ctx.srv.store, &ctx.db, coll)?;
    let fusion_first = stages.first().is_some_and(|s| s.contains_key("$rankFusion") || s.contains_key("$scoreFusion"));
    let mut full = if fusion_first && !prefix.is_empty() {
        let mut v = vec![crate::search::with_view_prefix(&stages[0], &prefix)];
        v.extend(stages.into_iter().skip(1));
        v
    } else {
        let mut v = prefix;
        v.extend(stages);
        v
    };
    if full.iter().skip(1).any(|s| s.contains_key("$rankFusion") || s.contains_key("$scoreFusion")) {
        return Err(Error::new(10170100, "$rankFusion/$scoreFusion can only be the first stage of an aggregation pipeline."));
    }
    crate::agg::pipeline::check_meta(&full, &Default::default())?;
    let ns = ctx.ns(&src);
    let opts = agg_opts(ctx, Some(src.clone()), cmd, vars.clone());
    let first_name = full.first().and_then(|s| s.keys().next().cloned()).unwrap_or_default();
    let input: DocIter = match ctx.srv.store.get_coll(&ns) {
        None => Box::new(std::iter::empty()),
        Some(c) => {
            // Predicate pushdown: a leading $match drives index selection.
            let pushed = if first_name == "$match" && full[0].len() == 1 {
                let f = full[0].get_document("$match").cloned().unwrap_or_default();
                full.remove(0);
                Some(f)
            } else {
                None
            };
            // Field dependency pruning: decode only the fields the remaining stages read.
            let needed = deps::pipeline_fields(&full).map(Arc::new);
            match pushed {
                Some(filter) => {
                    let (items, _) = query_items_with(ctx, &c, &filter, cmd.get("hint"), &vars, false, needed, QueryShape::default())?;
                    Box::new(items.map(|r| r.map(|x| x.1)))
                }
                None => {
                    let plan = match cmd.get("hint") {
                        Some(h) => ctx.srv.store.plan(&c, &Document::new(), Some(h))?,
                        None => Plan::CollScan { reverse: false },
                    };
                    let raw = base_raw(ctx, &c, &plan)?;
                    Box::new(raw.map(move |r| {
                        r.and_then(|(_, v)| match &needed {
                            Some(f) => bsonutil::from_bytes_fields(&v, f),
                            None => bsonutil::from_bytes(&v),
                        })
                    }))
                }
            }
        }
    };
    pipeline::run_pipeline(input, &full, opts)
}

fn aggregate(ctx: &mut CmdCtx) -> Result<Document> {
    let cmd = ctx.cmd.clone();
    let stages = docs_array(&cmd, "pipeline")?;
    let is_explain = bsonutil::as_bool(cmd.get("explain"));
    let cursor_opt = match cmd.get("cursor") {
        Some(Bson::Document(d)) => Some(d.clone()),
        None if is_explain => None,
        None => return Err(Error::failed_to_parse("The 'cursor' option is required, except for aggregate with the explain argument")),
        Some(v) => return Err(Error::type_mismatch(format!("cursor field must be missing or an object, not {}", bsonutil::type_name(v)))),
    };
    let batch_size = cursor_opt.as_ref().and_then(|c| c.get("batchSize")).and_then(bsonutil::num_i64).map(|b| b.max(0) as usize);
    let coll: Option<String> = match cmd.get("aggregate") {
        Some(Bson::String(s)) => Some(s.clone()),
        Some(v) if bsonutil::num_i64(v) == Some(1) => None,
        _ => Some(ctx.coll_name()?),
    };
    let vars = command_vars(ctx, &cmd)?;
    let first = stages.first().and_then(|s| s.keys().next().cloned()).unwrap_or_default();
    if first == "$changeStream" {
        return crate::changestream::open(ctx, coll, &stages, batch_size);
    }
    for (i, s) in stages.iter().enumerate() {
        if let Some(k) = s.keys().next() {
            if (k == "$out" || k == "$merge") && i + 1 != stages.len() {
                return Err(Error::new(if k == "$out" { 40601 } else { 6083800 }, format!("{k} can only be the final stage in the pipeline")));
            }
            if k == "$changeStream" && i > 0 {
                return Err(Error::new(40602, "$changeStream is only valid as the first stage in a pipeline"));
            }
        }
    }
    if is_explain {
        let filter = stages.first().and_then(|s| s.get_document("$match").ok()).cloned().unwrap_or_default();
        let ns = ctx.ns(coll.as_deref().unwrap_or("$cmd.aggregate"));
        let c = coll.as_ref().and_then(|c| ctx.srv.store.get_coll(&ctx.ns(c))).filter(|c| !c.is_view());
        let (win, rejected) = match &c {
            Some(c) => ctx.srv.store.plan_query(c, &filter, cmd.get("hint"), QueryShape::default())?,
            None => (Plan::Empty, Vec::new()),
        };
        let eq = planner::ExplainQuery { ns: &ns, filter: &filter, sort: None, projection: None, skip: 0, limit: 0 };
        let qp = doc! {
            "namespace": &ns,
            "parsedQuery": planner::parsed_query(&filter),
            "indexFilterSet": false,
            "cursorType": "regular",
            "maxIndexedOrSolutionsReached": false,
            "maxIndexedAndSolutionsReached": false,
            "maxScansToExplodeReached": false,
            "prunedSimilarIndexes": false,
            "winningPlan": planner::explain_tree(&win, &eq, None),
            "rejectedPlans": rejected.iter().map(|p| Bson::Document(planner::explain_tree(p, &eq, None))).collect::<Vec<_>>(),
        };
        let mut out_stages = vec![Bson::Document(doc! {"$cursor": {"queryPlanner": qp}})];
        let skip_first = usize::from(stages.first().is_some_and(|s| s.contains_key("$match")));
        for s in stages.iter().skip(skip_first) {
            out_stages.push(Bson::Document(s.clone()));
        }
        let mut command = cmd.clone();
        command.remove("explain");
        command.remove("$__verbosity");
        return Ok(doc! {
            "explainVersion": "1",
            "stages": out_stages,
            "queryShapeHash": planner::shape_hash(&[&filter]).repeat(8),
            "serverInfo": {"host": ctx.srv.host(), "port": ctx.srv.cfg.port as i32, "version": crate::server::VERSION},
            "serverParameters": explain_server_parameters(),
            "command": command,
            "ok": 1.0,
        });
    }
    let (ns, it): (String, DocIter) = match &coll {
        Some(c) => (ctx.ns(c), run_agg_on(ctx, c, stages.clone(), &cmd, vars)?),
        None => {
            // Collection-less aggregate: $documents, $currentOp, $listLocalSessions.
            if !matches!(first.as_str(), "$documents" | "$currentOp" | "$listLocalSessions" | "$querySettings" | "$listClusterCatalog" | "$changeStream" | "$queryStats" | "$listSessions" | "$listSearchIndexes" | "$rankFusion") {
                return Err(Error::new(73, format!("{{aggregate: 1}} is not valid for '{first}'; a collection is required.")));
            }
            let mut stages = stages.clone();
            let input: DocIter = match first.as_str() {
                "$currentOp" => {
                    stages.remove(0);
                    Box::new(crate::cmd::admin::current_op_docs(ctx.srv, &Document::new()).into_iter().map(Ok))
                }
                "$listLocalSessions" => {
                    stages.remove(0);
                    Box::new(std::iter::empty())
                }
                _ => Box::new(std::iter::empty()),
            };
            let opts = agg_opts(ctx, None, &cmd, vars);
            (format!("{}.$cmd.aggregate", ctx.db), pipeline::run_pipeline(input, &stages, opts)?)
        }
    };
    let last = stages.last().and_then(|s| s.keys().next().cloned()).unwrap_or_default();
    let mut cur = Cursor::new(ns, it);
    cur.lsid = cmd.get("lsid").cloned();
    if last == "$out" || last == "$merge" {
        for r in cur.iter.by_ref() {
            r?;
        }
        return Ok(cursor::cursor_reply(&cur.ns, 0, Vec::new(), true));
    }
    if ctx.in_txn() {
        let v: Vec<Result<Document>> = cur.iter.by_ref().collect();
        cur.iter = Box::new(v.into_iter());
    }
    cursor::first_batch(&ctx.srv.cursors, cur, batch_size, false)
}

// ------------------------------------------------------------ cursors

fn get_more(ctx: &mut CmdCtx) -> Result<Document> {
    let id = match ctx.cmd.get("getMore") {
        Some(Bson::Int64(i)) => *i,
        Some(v) => return Err(Error::type_mismatch(format!("Field 'getMore' must be of type long in: {}", bsonutil::type_name(v)))),
        None => return Err(Error::failed_to_parse("missing getMore")),
    };
    let coll = ctx.cmd.get_str("collection").map_err(|_| Error::new(40414, "BSON field 'getMore.collection' is missing but a required field"))?.to_string();
    let ns = ctx.ns(&coll);
    let batch = opt_nonneg(&ctx.cmd, "batchSize")?.filter(|b| *b > 0).map(|b| b as usize);
    ctx.srv.counters.getmore.fetch_add(1, Ordering::Relaxed);
    let mut cur = ctx.srv.cursors.take(id)?;
    if cur.ns != ns {
        let cns = cur.ns.clone();
        ctx.srv.cursors.put_back(cur);
        return Err(Error::new(13, format!("Requested getMore on namespace '{ns}', but cursor belongs to a different namespace {cns}")));
    }
    let (docs, done) = match cur.next_batch(batch) {
        Ok(x) => x,
        Err(e) => return Err(e),
    };
    let rid = if done { 0 } else { id };
    let cns = cur.ns.clone();
    if !done {
        ctx.srv.cursors.put_back(cur);
    }
    Ok(cursor::cursor_reply(&cns, rid, docs, false))
}

fn kill_cursors(ctx: &mut CmdCtx) -> Result<Document> {
    let ids = ctx.cmd.get_array("cursors").map_err(|_| Error::new(40414, "BSON field 'killCursors.cursors' is missing but a required field"))?;
    let mut killed = Vec::new();
    let mut missing = Vec::new();
    for v in ids {
        let Some(id) = bsonutil::num_i64(v) else { continue };
        if ctx.srv.cursors.kill(id) {
            killed.push(Bson::Int64(id));
        } else {
            missing.push(Bson::Int64(id));
        }
    }
    Ok(doc! {"cursorsKilled": killed, "cursorsNotFound": missing, "cursorsAlive": [], "cursorsUnknown": [], "ok": 1.0})
}

// ------------------------------------------------------------ explain

fn explain(ctx: &mut CmdCtx) -> Result<Document> {
    let inner = ctx.cmd.get_document("explain").map_err(|_| Error::type_mismatch("explain command requires a nested object"))?.clone();
    let verbosity = ctx.cmd.get_str("verbosity").unwrap_or("allPlansExecution").to_string();
    if !matches!(verbosity.as_str(), "queryPlanner" | "executionStats" | "allPlansExecution") {
        return Err(Error::new(2, format!("Enumeration value '{verbosity}' for field 'explain.verbosity' is not a valid value.")));
    }
    let Some(cname) = inner.keys().next().cloned() else { return Err(Error::failed_to_parse("explain: empty command")) };
    let coll = inner.get_str(&cname).unwrap_or_default().to_string();
    let ns = ctx.ns(&coll);
    if cname == "aggregate" {
        let mut agg = inner.clone();
        agg.insert("explain", true);
        agg.insert("$__verbosity", verbosity.as_str());
        let saved = std::mem::replace(&mut ctx.cmd, agg);
        let saved_name = std::mem::replace(&mut ctx.name, "aggregate".into());
        let r = aggregate(ctx);
        ctx.cmd = saved;
        ctx.name = saved_name;
        return r;
    }
    let first_stmt = |k: &str, q: &str| -> Document {
        inner.get_array(k).ok().and_then(|a| a.first()).and_then(|u| u.as_document()).and_then(|u| u.get_document(q).ok()).cloned().unwrap_or_default()
    };
    let (filter, sort, projection, skip, limit) = match cname.as_str() {
        "find" => (
            inner.get_document("filter").cloned().unwrap_or_default(),
            inner.get_document("sort").ok().filter(|s| !s.is_empty()).cloned(),
            inner.get_document("projection").ok().filter(|p| !p.is_empty()).cloned(),
            bsonutil::as_i64_opt(inner.get("skip")).unwrap_or(0).max(0) as usize,
            bsonutil::as_i64_opt(inner.get("limit")).unwrap_or(0).unsigned_abs() as usize,
        ),
        "count" => (inner.get_document("query").cloned().unwrap_or_default(), None, None, 0, 0),
        "distinct" => (inner.get_document("query").cloned().unwrap_or_default(), None, None, 0, 0),
        "findAndModify" | "findandmodify" => (inner.get_document("query").cloned().unwrap_or_default(), inner.get_document("sort").ok().cloned(), None, 0, 1),
        "update" => (first_stmt("updates", "q"), None, None, 0, 0),
        "delete" => (first_stmt("deletes", "q"), None, None, 0, 0),
        other => return Err(Error::new(59, format!("Explain failed due to unknown command: {other}"))),
    };
    let hint = inner.get("hint").filter(|h| !matches!(h, Bson::Document(d) if d.is_empty())).cloned();
    let c = ctx.srv.store.get_coll(&ns).filter(|c| !c.is_view());
    let shape = QueryShape { sort: sort.as_ref(), projection: projection.as_ref(), limit: (limit > 0).then_some(skip + limit) };
    let (win, rejected) = match &c {
        Some(c) => ctx.srv.store.plan_query(c, &filter, hint.as_ref(), shape)?,
        None => (Plan::Empty, Vec::new()),
    };
    let eq = planner::ExplainQuery { ns: &ns, filter: &filter, sort: sort.as_ref(), projection: projection.as_ref(), skip, limit };
    let wrap_count = |tree: Document| if cname == "count" { doc! {"isCached": false, "stage": "COUNT", "inputStage": strip_cached(tree)} } else { tree };
    let win_doc = wrap_count(planner::explain_tree(&win, &eq, None));
    let rej_docs: Vec<Bson> = rejected.iter().map(|p| Bson::Document(wrap_count(planner::explain_tree(p, &eq, None)))).collect();
    let hash = planner::shape_hash(&[&filter, sort.as_ref().unwrap_or(&Document::new()), projection.as_ref().unwrap_or(&Document::new())]);
    let mut qp = doc! {"namespace": &ns, "parsedQuery": planner::parsed_query(&filter), "indexFilterSet": false};
    if c.is_some() {
        qp.insert("queryHash", &hash);
        qp.insert("planCacheShapeHash", &hash);
        qp.insert("planCacheKey", &hash);
        qp.insert("optimizationTimeMillis", 0);
        qp.insert("optimizationTimeMicros", 0);
    }
    qp.insert("maxIndexedOrSolutionsReached", false);
    qp.insert("maxIndexedAndSolutionsReached", false);
    qp.insert("maxScansToExplodeReached", false);
    qp.insert("prunedSimilarIndexes", false);
    qp.insert("winningPlan", win_doc);
    qp.insert("rejectedPlans", rej_docs);
    let mut out = doc! {"explainVersion": "1", "queryPlanner": qp};
    if verbosity != "queryPlanner" {
        let start = std::time::Instant::now();
        let stats = match &c {
            Some(c) => simulate(ctx, c, &win, &filter, &inner, sort.is_some() && !win.provides_sort(), skip, limit)?,
            None => planner::ExecStats::default(),
        };
        let stages = wrap_count(planner::explain_tree(&win, &eq, Some(&stats)));
        let mut es = doc! {
            "executionSuccess": true,
            "nReturned": planner::num32(if cname == "count" { 0 } else { stats.returned }),
            "executionTimeMillis": planner::num32(start.elapsed().as_millis() as u64),
            "totalKeysExamined": planner::num32(stats.keys),
            "totalDocsExamined": planner::num32(stats.docs),
            "executionStages": stages,
        };
        if verbosity == "allPlansExecution" {
            es.insert("allPlansExecution", Vec::<Bson>::new());
        }
        out.insert("executionStats", es);
    }
    out.insert("queryShapeHash", planner::shape_hash(&[&filter, &doc! {"cmd": &cname}]).repeat(8));
    let mut command = inner.clone();
    command.insert("$db", ctx.db.clone());
    out.insert("command", command);
    out.insert("serverInfo", doc! {"host": ctx.srv.host(), "port": ctx.srv.cfg.port as i32, "version": crate::server::VERSION, "gitVersion": "mogodb"});
    out.insert("serverParameters", explain_server_parameters());
    out.insert("ok", 1.0);
    Ok(out)
}

fn strip_cached(mut d: Document) -> Document {
    d.remove("isCached");
    d
}

fn explain_server_parameters() -> Document {
    doc! {
        "internalQueryFacetBufferSizeBytes": 104857600,
        "internalQueryFacetMaxOutputDocSizeBytes": 104857600,
        "internalLookupStageIntermediateDocumentMaxSizeBytes": 104857600,
        "internalDocumentSourceGroupMaxMemoryBytes": 104857600,
        "internalQueryMaxBlockingSortMemoryUsageBytes": 104857600,
        "internalQueryProhibitBlockingMergeOnMongoS": 0,
        "internalQueryMaxAddToSetBytes": 104857600,
        "internalDocumentSourceSetWindowFieldsMaxMemoryBytes": 104857600,
        "internalQueryFrameworkControl": "trySbeRestricted",
        "internalQueryPlannerIgnoreIndexWithCollationForRegex": 1,
    }
}

/// Execute `plan` the way the find command would, collecting per-stage counters.
#[allow(clippy::too_many_arguments)]
fn simulate(ctx: &mut CmdCtx, c: &Arc<Coll>, plan: &Plan, filter: &Document, cmd: &Document, blocking_sort: bool, skip: usize, limit: usize) -> Result<planner::ExecStats> {
    let vars = command_vars(ctx, cmd)?;
    let m = build_matcher(Some(c), filter, &vars)?;
    let mut st = planner::ExecStats::default();
    let base = base_raw(ctx, c, plan)?;
    let fetches = !plan.is_covered();
    let mut bytes = 0u64;
    for r in base {
        let (_, raw) = r?;
        st.scanned += 1;
        if fetches {
            st.docs += 1;
        }
        if m.matches(&bsonutil::from_bytes(&raw)?)? {
            st.matched += 1;
            bytes += raw.len() as u64;
            if !blocking_sort && limit > 0 && st.matched as usize >= skip + limit {
                break;
            }
        }
    }
    st.sorted = if limit > 0 { st.matched.min((skip + limit) as u64) } else { st.matched };
    st.skipped_out = st.sorted.saturating_sub(skip as u64);
    st.returned = if limit > 0 { st.skipped_out.min(limit as u64) } else { st.skipped_out };
    if planner::skip_below_fetch(plan, filter, blocking_sort, skip) {
        st.docs = st.docs.saturating_sub(skip as u64);
    }
    if blocking_sort && st.matched > 0 {
        st.sort_bytes = bytes / st.matched * st.sorted;
    }
    if let Plan::IxScan(p) = plan {
        st.keys = p.keys_examined.load(Ordering::Relaxed);
    }
    Ok(st)
}

#[allow(unused)]
fn _assert_reader(r: ReadSrc) -> usize {
    with_reader!(r, |x| { let _ = x; 0 })
}

/// Blocking-stage memory budget: the stage limit capped by the 9.0 per-operation limit.
fn op_memory_limit(ctx: &CmdCtx) -> usize {
    let per_op = ctx.srv.parameters.lock().expect("parameters").get("internalQueryMaxMemoryUsageBytesPerOperation").and_then(bsonutil::num_i64).unwrap_or(i64::MAX);
    MAX_SORT_BYTES.min(per_op.max(0) as usize)
}
