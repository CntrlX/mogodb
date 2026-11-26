//! Pull-based streaming aggregation pipeline.
use crate::agg::accum::{
    bucket_auto_stage, bucket_stage, densify_stage, fill_stage, group_stage, set_window_fields_stage,
};
use crate::agg::expr::{eval, Vars};
use crate::bsonutil::{
    bson_eq, doc_size, from_bytes, get_path, is_missing, num_f64, num_i64, remove_path, set_path, to_bytes,
    META_KEY,
};
use crate::error::{Error, Result};
use crate::geo::{near_distance, parse_geometry, Geometry, NearSpec};
use crate::query::matcher::Matcher;
use crate::query::projection::{add_fields, Projection, ProjectionKind};
use crate::query::sort::SortSpec;
use bson::{doc, Bson, Document};
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use rand::seq::SliceRandom;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

/// Lazily pulled document stream. Every stage is an iterator adapter over its input.
pub type DocIter = Box<dyn Iterator<Item = Result<Document>> + Send>;

/// Storage access for stages that touch other collections. Implemented by the server.
pub trait AggContext: Send + Sync {
    /// Stream all documents of `db.coll` (empty if absent).
    fn scan(&self, db: &str, coll: &str) -> Result<DocIter>;
    /// Documents of `db.coll` matching `filter` (may use indexes).
    fn find(&self, db: &str, coll: &str, filter: &Document) -> Result<Vec<Document>>;
    /// `$out`: atomically replace `db.coll` with `docs`.
    fn replace_collection(&self, db: &str, coll: &str, docs: Vec<Document>) -> Result<()>;
    /// Insert one document (error 11000 on duplicate key). Creates the collection if needed.
    fn insert(&self, db: &str, coll: &str, doc: Document) -> Result<()>;
    /// Replace the document whose `_id` equals `id`.
    fn replace_by_id(&self, db: &str, coll: &str, id: &Bson, doc: Document) -> Result<()>;
    /// `$collStats` output document.
    fn coll_stats(&self, db: &str, coll: &str, spec: &Document) -> Result<Document>;
    /// `$indexStats` output documents.
    fn index_stats(&self, db: &str, coll: &str) -> Result<Vec<Document>>;
    /// Directory for spill files.
    fn temp_dir(&self) -> PathBuf;
    /// Search index definitions of `db.coll` (as returned by `$listSearchIndexes`).
    fn search_indexes(&self, db: &str, coll: &str) -> Result<Vec<Document>> {
        let _ = (db, coll);
        Ok(Vec::new())
    }
    /// `$listClusterCatalog` entries for `db` (`None` = every database, admin only).
    fn cluster_catalog(&self, db: Option<&str>) -> Result<Vec<Document>> {
        let _ = db;
        Ok(Vec::new())
    }
    /// Stored query settings (`$querySettings`).
    fn query_settings(&self) -> Result<Vec<Document>> {
        Ok(Vec::new())
    }
}

pub struct AggOptions {
    pub db: String,
    /// Source collection name (None for `aggregate: 1`).
    pub coll: Option<String>,
    pub allow_disk_use: bool,
    /// Per blocking stage memory limit in bytes (default 100 MiB).
    pub max_memory_bytes: usize,
    /// `let` + system variables (NOW, CLUSTER_TIME).
    pub vars: Vars,
    pub ctx: Arc<dyn AggContext>,
}

const KNOWN_STAGES: &[&str] = &[
    "$addFields",
    "$set",
    "$bucket",
    "$bucketAuto",
    "$changeStream",
    "$collStats",
    "$count",
    "$currentOp",
    "$densify",
    "$documents",
    "$facet",
    "$fill",
    "$geoNear",
    "$graphLookup",
    "$group",
    "$indexStats",
    "$limit",
    "$listLocalSessions",
    "$listSessions",
    "$lookup",
    "$match",
    "$merge",
    "$out",
    "$project",
    "$redact",
    "$replaceRoot",
    "$replaceWith",
    "$sample",
    "$setWindowFields",
    "$skip",
    "$sort",
    "$sortByCount",
    "$unionWith",
    "$unset",
    "$unwind",
    "$vectorSearch",
    "$search",
    "$searchMeta",
    "$listSearchIndexes",
    "$rankFusion",
    "$listClusterCatalog",
    "$querySettings",
];

/// Stages that must be the first stage of a pipeline.
const FIRST_ONLY_STAGES: &[&str] = &[
    "$documents",
    "$geoNear",
    "$vectorSearch",
    "$collStats",
    "$indexStats",
    "$listSessions",
    "$listLocalSessions",
    "$currentOp",
    "$search",
    "$searchMeta",
    "$listSearchIndexes",
    "$rankFusion",
    "$listClusterCatalog",
    "$querySettings",
];

static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

/// Build the stage chain over `input`. Validation errors (unknown stage etc.) are returned eagerly.
pub fn run_pipeline(input: DocIter, stages: &[Document], opts: Arc<AggOptions>) -> Result<DocIter> {
    let parsed = validate_stages(stages)?;
    let mut it = input;
    let mut i = 0;
    while i < parsed.len() {
        let (name, arg) = parsed[i];
        if name == "$sort" {
            if let Some(&("$limit", limit_arg)) = parsed.get(i + 1) {
                let n = parse_limit(limit_arg)?;
                let spec = SortSpec::new(expect_doc(name, arg)?)?;
                it = sort_stage(it, spec, Some(n), Arc::clone(&opts));
                i += 2;
                continue;
            }
        }
        it = build_stage(it, name, arg, &opts)?;
        i += 1;
    }
    Ok(it)
}

/// Apply update-pipeline stages (`$addFields`/`$set`, `$project`, `$unset`, `$replaceRoot`, `$replaceWith`) to one document.
pub fn apply_simple_stages(doc: Document, stages: &[Document], vars: &Vars) -> Result<Document> {
    let mut cur = doc;
    for st in stages {
        let (name, arg) = single_stage(st)?;
        cur = match name {
            "$addFields" | "$set" => add_fields(&cur, expect_doc(name, arg)?, vars)?,
            "$project" => Projection::new(expect_doc(name, arg)?, ProjectionKind::Agg)?.apply(&cur, None, vars)?,
            "$unset" => match unset_projection(arg)? {
                Some(p) => p.apply(&cur, None, vars)?,
                None => cur,
            },
            "$replaceRoot" => {
                let expr = parse_replace_root(arg)?;
                replace_root_with(&cur, &expr, vars)?
            }
            "$replaceWith" => replace_root_with(&cur, arg, vars)?,
            _ if KNOWN_STAGES.contains(&name) => {
                return Err(Error::location(72, format!("{name} is not allowed to be used within an update")));
            }
            _ => return Err(unrecognized_stage(name)),
        };
    }
    Ok(cur)
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

fn unrecognized_stage(name: &str) -> Error {
    Error::new(40324, format!("Unrecognized pipeline stage name: '{name}'"))
}

fn single_stage(st: &Document) -> Result<(&str, &Bson)> {
    let mut entries = st.iter();
    match (entries.next(), entries.next()) {
        (Some((name, arg)), None) => Ok((name.as_str(), arg)),
        _ => Err(Error::new(40323, "A pipeline stage specification object must contain exactly one field.")),
    }
}

fn validate_stages(stages: &[Document]) -> Result<Vec<(&str, &Bson)>> {
    let n = stages.len();
    let mut out = Vec::with_capacity(n);
    for (i, st) in stages.iter().enumerate() {
        let (name, arg) = single_stage(st)?;
        if !KNOWN_STAGES.contains(&name) {
            return Err(unrecognized_stage(name));
        }
        if matches!(name, "$out" | "$merge") && i + 1 != n {
            return Err(Error::location(40601, format!("{name} can only be the final stage in the pipeline")));
        }
        if name == "$changeStream" {
            let msg = if i == 0 {
                "$changeStream is not supported by this server"
            } else {
                "$changeStream is only valid as the first stage in a pipeline"
            };
            return Err(Error::location(40602, msg));
        }
        if i != 0 && matches!(name, "$rankFusion" | "$scoreFusion") {
            return Err(Error::location(10170100, "$rankFusion/$scoreFusion can only be the first stage of an aggregation pipeline."));
        }
        if i != 0 && FIRST_ONLY_STAGES.contains(&name) {
            return Err(Error::location(40602, format!("{name} is only valid as the first stage in a pipeline")));
        }
        out.push((name, arg));
    }
    Ok(out)
}

/// Rejects stages that may not appear inside `$lookup`, `$facet`, or `$unionWith` sub-pipelines.
fn check_nested(stages: &[Document], outer: &str) -> Result<()> {
    for st in stages {
        let (name, _) = single_stage(st)?;
        if matches!(name, "$out" | "$merge" | "$facet") {
            return Err(Error::location(40600, format!("{name} is not allowed to be used within a {outer} stage")));
        }
    }
    Ok(())
}

fn expect_doc<'a>(name: &str, arg: &'a Bson) -> Result<&'a Document> {
    match arg {
        Bson::Document(d) => Ok(d),
        _ if name == "$sort" => Err(Error::location(15973, "the $sort key specification must be an object")),
        _ => Err(Error::failed_to_parse(format!("{name} specification must be an object"))),
    }
}

fn string_field(stage: &str, field: &str, v: &Bson) -> Result<String> {
    match v {
        Bson::String(s) => Ok(s.clone()),
        _ => Err(Error::failed_to_parse(format!("{stage} option '{field}' must be a string"))),
    }
}

fn pipeline_docs(stage: &str, v: &Bson) -> Result<Vec<Document>> {
    let Bson::Array(items) = v else {
        return Err(Error::failed_to_parse(format!("{stage} 'pipeline' must be an array")));
    };
    items
        .iter()
        .map(|i| match i {
            Bson::Document(d) => Ok(d.clone()),
            _ => Err(Error::failed_to_parse(format!("{stage} pipeline stages must be objects"))),
        })
        .collect()
}

/// `(db, coll)` from a string (same db) or `{db, coll}` document.
fn target_ns(v: &Bson, default_db: &str, code: i32, stage: &str) -> Result<(String, String)> {
    let bad = || Error::location(code, format!("{stage} requires a string or an object with a 'coll' field"));
    match v {
        Bson::String(c) => Ok((default_db.to_string(), c.clone())),
        Bson::Document(spec) => {
            let db = match spec.get("db") {
                None => default_db.to_string(),
                Some(Bson::String(s)) => s.clone(),
                Some(_) => return Err(bad()),
            };
            let coll = match spec.get("coll") {
                Some(Bson::String(s)) => s.clone(),
                _ => return Err(bad()),
            };
            Ok((db, coll))
        }
        _ => Err(bad()),
    }
}

/// Shell-style rendering of a stage argument for `$limit`/`$skip` messages.
fn arg_repr(v: &Bson) -> String {
    match v {
        Bson::String(s) => format!("\"{s}\""),
        Bson::Double(d) => format!("{d}"),
        Bson::Int64(i) => i.to_string(),
        Bson::Int32(i) => i.to_string(),
        Bson::Decimal128(d) => d.to_string(),
        other => other.to_string(),
    }
}

/// Non-negative integral `$limit`/`$skip` amount (9.0 codes 5107201 / 5107200).
fn stage_amount(stage: &str, code: i32, arg: &Bson) -> Result<i64> {
    let bad = |what: &str| Error::location(code, format!("invalid argument to {stage} stage: {what} {stage}: {}", arg_repr(arg)));
    if !crate::bsonutil::is_number(arg) {
        return Err(bad("Expected a number in:"));
    }
    let f = crate::bsonutil::num_f64(arg).unwrap_or(f64::NAN);
    if f.is_nan() || f.fract() != 0.0 {
        return Err(bad("Expected an integer:"));
    }
    if f < 0.0 {
        return Err(bad("Expected a non-negative number in:"));
    }
    Ok(num_i64(arg).unwrap_or(i64::MAX))
}

fn parse_limit(arg: &Bson) -> Result<usize> {
    match stage_amount("$limit", 5107201, arg)? {
        0 => Err(Error::location(15958, "the limit must be positive")),
        n => Ok(n as usize),
    }
}

fn parse_skip(arg: &Bson) -> Result<usize> {
    Ok(stage_amount("$skip", 5107200, arg)? as usize)
}

// ---------------------------------------------------------------------------
// $meta availability (parse-time, like MongoDB's dependency analysis)
// ---------------------------------------------------------------------------

const META_NAMES: &[&str] = &[
    "textScore", "randVal", "searchScore", "searchHighlights", "geoNearDistance", "geoNearPoint", "recordId", "indexKey", "sortKey",
    "searchScoreDetails", "searchSequenceToken", "vectorSearchScore", "score", "scoreDetails", "timeseriesBucketMinTime",
    "timeseriesBucketMaxTime", "searchRootDocumentId", "changeStreamControlEvent", "stream",
];

/// Human name used in "query requires X metadata"; `None` = always available.
fn meta_label(name: &str) -> Option<&'static str> {
    Some(match name {
        "textScore" => "text score",
        "score" => "score",
        "scoreDetails" => "scoreDetails",
        "searchScore" => "$search score",
        "searchHighlights" => "$search highlights",
        "searchScoreDetails" => "$search score details",
        "vectorSearchScore" => "$vectorSearch score",
        "geoNearDistance" => "$geoNear distance",
        "geoNearPoint" => "$geoNear point",
        _ => return None,
    })
}

/// 40218 "query requires X metadata, but it is not available".
pub fn meta_unavailable(name: &str) -> Option<Error> {
    meta_label(name).map(|l| Error::location(40218, format!("query requires {l} metadata, but it is not available")))
}

fn has_text(filter: &Document) -> bool {
    filter.iter().any(|(k, v)| {
        k == "$text" || (k == "$and" && v.as_array().is_some_and(|a| a.iter().any(|e| e.as_document().is_some_and(has_text))))
    })
}

fn scan_meta(stage: &str, v: &Bson, avail: &std::collections::HashSet<&str>) -> Result<()> {
    match v {
        Bson::Document(d) => {
            if d.len() == 1 {
                if let Some(Bson::String(name)) = d.get("$meta") {
                    if !META_NAMES.contains(&name.as_str()) {
                        let pre = if matches!(stage, "$project" | "$addFields" | "$set") { format!("Invalid {stage} :: caused by :: ") } else { String::new() };
                        return Err(Error::location(17308, format!("{pre}Unsupported $meta field: {name}")));
                    }
                    if !avail.contains(name.as_str()) {
                        if let Some(e) = meta_unavailable(name) {
                            return Err(e);
                        }
                    }
                    return Ok(());
                }
                if d.contains_key("$literal") {
                    return Ok(());
                }
            }
            d.values().try_for_each(|x| scan_meta(stage, x, avail))
        }
        Bson::Array(a) => a.iter().try_for_each(|x| scan_meta(stage, x, avail)),
        _ => Ok(()),
    }
}

fn sub_pipeline(v: Option<&Bson>) -> Vec<Document> {
    v.and_then(Bson::as_array).map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect()).unwrap_or_default()
}

/// Reject `{$meta: X}` references to metadata no earlier stage produces (40218) and unknown
/// `$meta` names (17308). `inherited` is what the enclosing pipeline makes available.
pub fn check_meta(stages: &[Document], inherited: &std::collections::HashSet<&'static str>) -> Result<()> {
    let mut avail = inherited.clone();
    for st in stages {
        let Some((name, arg)) = st.iter().next() else { continue };
        let flag = |k: &str| arg.as_document().is_some_and(|d| crate::bsonutil::as_bool(d.get(k)));
        match name.as_str() {
            "$facet" => {
                if let Some(d) = arg.as_document() {
                    for p in d.values() {
                        check_meta(&sub_pipeline(Some(p)), &avail)?;
                    }
                }
            }
            "$lookup" | "$unionWith" => {
                if let Some(d) = arg.as_document() {
                    check_meta(&sub_pipeline(d.get("pipeline")), &Default::default())?;
                }
            }
            "$rankFusion" | "$scoreFusion" => {
                if let Some(ps) = arg.as_document().and_then(|d| d.get_document("input").ok()).and_then(|i| i.get_document("pipelines").ok()) {
                    for p in ps.values() {
                        check_meta(&sub_pipeline(Some(p)), &Default::default())?;
                    }
                }
                avail.insert("score");
                if flag("scoreDetails") {
                    avail.insert("scoreDetails");
                }
            }
            "$search" => {
                avail.extend(["searchScore", "searchHighlights", "score", "searchSequenceToken"]);
                if flag("scoreDetails") {
                    avail.extend(["searchScoreDetails", "scoreDetails"]);
                }
            }
            "$vectorSearch" => avail.extend(["vectorSearchScore", "score"]),
            "$geoNear" => avail.extend(["geoNearDistance", "geoNearPoint"]),
            "$score" => {
                scan_meta(name, arg, &avail)?;
                avail.insert("score");
                if flag("scoreDetails") {
                    avail.insert("scoreDetails");
                }
            }
            "$searchMeta" | "$listSearchIndexes" => {}
            "$match" => {
                if arg.as_document().is_some_and(has_text) {
                    avail.extend(["textScore", "score"]);
                }
                scan_meta(name, arg, &avail)?;
            }
            _ => scan_meta(name, arg, &avail)?,
        }
    }
    Ok(())
}

fn count_value(n: usize) -> Bson {
    i32::try_from(n).map_or(Bson::Int64(n as i64), Bson::Int32)
}

fn collection_name(opts: &AggOptions, stage: &str) -> Result<String> {
    opts.coll
        .clone()
        .ok_or_else(|| Error::bad_value(format!("{stage} must be run against a collection")))
}

fn sub_options(opts: &AggOptions, coll: &str, vars: Vars) -> Arc<AggOptions> {
    Arc::new(AggOptions {
        db: opts.db.clone(),
        coll: Some(coll.to_string()),
        allow_disk_use: opts.allow_disk_use,
        max_memory_bytes: opts.max_memory_bytes,
        vars,
        ctx: Arc::clone(&opts.ctx),
    })
}

// ---------------------------------------------------------------------------
// Iterator plumbing
// ---------------------------------------------------------------------------

fn empty_iter() -> DocIter {
    Box::new(std::iter::empty())
}

fn docs_iter(docs: Vec<Document>) -> DocIter {
    Box::new(docs.into_iter().map(Ok))
}

fn collect_docs(input: DocIter) -> Result<Vec<Document>> {
    input.collect()
}

type InitFn = Box<dyn FnOnce() -> Result<DocIter> + Send>;

/// Defers `init` until the first pull, so blocking stages consume input only when drained.
struct Lazy {
    init: Option<InitFn>,
    inner: Option<DocIter>,
}

impl Iterator for Lazy {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(init) = self.init.take() {
            match init() {
                Ok(it) => self.inner = Some(it),
                Err(e) => return Some(Err(e)),
            }
        }
        self.inner.as_mut()?.next()
    }
}

fn lazy(f: impl FnOnce() -> Result<DocIter> + Send + 'static) -> DocIter {
    Box::new(Lazy { init: Some(Box::new(f)), inner: None })
}

fn map_docs<F>(input: DocIter, mut f: F) -> DocIter
where
    F: FnMut(Document) -> Result<Option<Document>> + Send + 'static,
{
    Box::new(input.filter_map(move |r| match r {
        Ok(d) => f(d).transpose(),
        Err(e) => Some(Err(e)),
    }))
}

fn flat_docs<F>(input: DocIter, mut f: F) -> DocIter
where
    F: FnMut(Document) -> Result<Vec<Document>> + Send + 'static,
{
    Box::new(input.flat_map(move |r| -> Vec<Result<Document>> {
        match r.and_then(|d| f(d)) {
            Ok(v) => v.into_iter().map(Ok).collect(),
            Err(e) => vec![Err(e)],
        }
    }))
}

struct Limit {
    inner: DocIter,
    remaining: usize,
}

impl Iterator for Limit {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let item = self.inner.next()?;
        if item.is_ok() {
            self.remaining -= 1;
        }
        Some(item)
    }
}

struct Skip {
    inner: DocIter,
    remaining: usize,
}

impl Iterator for Skip {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.inner.next()?;
            match item {
                Ok(_) if self.remaining > 0 => self.remaining -= 1,
                other => return Some(other),
            }
        }
    }
}

fn limit_stage(input: DocIter, n: usize) -> DocIter {
    Box::new(Limit { inner: input, remaining: n })
}

fn skip_stage(input: DocIter, n: usize) -> DocIter {
    Box::new(Skip { inner: input, remaining: n })
}

// ---------------------------------------------------------------------------
// Stage construction
// ---------------------------------------------------------------------------

fn build_stage(input: DocIter, name: &str, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let o = Arc::clone(opts);
    match name {
        "$match" => {
            let Bson::Document(filter) = arg else {
                return Err(Error::location(15959, "the match filter must be an expression in an object"));
            };
            let m = Matcher::with_vars(filter, opts.vars.clone())?;
            Ok(map_docs(input, move |d| Ok(m.matches(&d)?.then_some(d))))
        }
        "$project" => {
            let p = Projection::new(
                expect_project_doc(arg)?,
                ProjectionKind::Agg,
            )?;
            Ok(map_docs(input, move |d| Ok(Some(p.apply(&d, None, &o.vars)?))))
        }
        "$addFields" | "$set" => {
            let spec = expect_doc(name, arg)?.clone();
            Ok(map_docs(input, move |d| Ok(Some(add_fields(&d, &spec, &o.vars)?))))
        }
        "$unset" => match unset_projection(arg)? {
            None => Ok(input),
            Some(p) => Ok(map_docs(input, move |d| Ok(Some(p.apply(&d, None, &o.vars)?)))),
        },
        "$unwind" => {
            let (path, index, preserve) = parse_unwind(arg)?;
            Ok(flat_docs(input, move |d| unwind_doc(d, &path, index.as_deref(), preserve)))
        }
        "$limit" => Ok(limit_stage(input, parse_limit(arg)?)),
        "$skip" => Ok(skip_stage(input, parse_skip(arg)?)),
        "$sort" => Ok(sort_stage(input, SortSpec::new(expect_doc(name, arg)?)?, None, Arc::clone(opts))),
        "$sample" => {
            let spec = expect_doc(name, arg)?;
            let size = match spec.get("size") {
                Some(v) => num_i64(v).ok_or_else(|| Error::location(28746, "size argument to $sample must be a number"))?,
                None => return Err(Error::location(28745, "$sample stage must specify a size")),
            };
            if size < 0 {
                return Err(Error::location(28747, "size argument to $sample must not be negative"));
            }
            let n = size as usize;
            Ok(lazy(move || {
                let mut docs = collect_docs(input)?;
                docs.shuffle(&mut rand::thread_rng());
                docs.truncate(n);
                Ok(docs_iter(docs))
            }))
        }
        "$count" => {
            let Bson::String(field) = arg else {
                return Err(Error::location(40157, "the count field must be a string"));
            };
            if field.is_empty() {
                return Err(Error::location(40156, "the count field must be a non-empty string"));
            }
            if field.starts_with('$') {
                return Err(Error::location(40159, "the count field cannot be a $-prefixed path"));
            }
            if field.contains('.') {
                return Err(Error::location(40158, "the count field cannot contain '.'"));
            }
            let field = field.clone();
            Ok(lazy(move || {
                let mut n = 0usize;
                for r in input {
                    r?;
                    n += 1;
                }
                if n == 0 {
                    return Ok(empty_iter());
                }
                let mut out = Document::new();
                out.insert(field, count_value(n));
                Ok(docs_iter(vec![out]))
            }))
        }
        "$sortByCount" => {
            let expr = arg.clone();
            Ok(lazy(move || {
                let mut groups: Vec<(Bson, usize)> = Vec::new();
                let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
                for r in input {
                    let d = r?;
                    let v = match eval(&expr, &d, &o.vars)? {
                        v if is_missing(&v) => Bson::Null,
                        v => v,
                    };
                    let key = to_bytes(&doc! {"k": group_key_value(&v)});
                    match index.get(&key) {
                        Some(&i) => groups[i].1 += 1,
                        None => {
                            index.insert(key, groups.len());
                            groups.push((v, 1));
                        }
                    }
                }
                groups.sort_by(|a, b| b.1.cmp(&a.1));
                let docs = groups
                    .into_iter()
                    .map(|(id, n)| doc! {"_id": id, "count": count_value(n)})
                    .collect();
                Ok(docs_iter(docs))
            }))
        }
        "$group" => group_stage(input, expect_doc(name, arg)?, opts),
        "$bucket" => bucket_stage(input, expect_doc(name, arg)?, opts),
        "$bucketAuto" => bucket_auto_stage(input, expect_doc(name, arg)?, opts),
        "$setWindowFields" => set_window_fields_stage(input, expect_doc(name, arg)?, opts),
        "$densify" => densify_stage(input, expect_doc(name, arg)?, opts),
        "$fill" => fill_stage(input, expect_doc(name, arg)?, opts),
        "$facet" => build_facet(input, arg, opts),
        "$lookup" => build_lookup(input, arg, opts),
        "$graphLookup" => build_graph_lookup(input, arg, opts),
        "$unionWith" => build_union_with(input, arg, opts),
        "$redact" => build_redact(input, arg, opts),
        "$replaceRoot" => {
            let expr = parse_replace_root(arg)?;
            Ok(map_docs(input, move |d| Ok(Some(replace_root_with(&d, &expr, &o.vars)?))))
        }
        "$replaceWith" => {
            let expr = arg.clone();
            Ok(map_docs(input, move |d| Ok(Some(replace_root_with(&d, &expr, &o.vars)?))))
        }
        "$out" => build_out(input, arg, opts),
        "$merge" => build_merge(input, arg, opts),
        "$geoNear" => build_geo_near(input, arg, opts),
        "$vectorSearch" => build_vector_search(input, arg, opts),
        "$documents" => {
            drop(input);
            let v = eval(arg, &Document::new(), &opts.vars)?;
            let Bson::Array(items) = v else {
                return Err(Error::location(40516, "$documents must evaluate to an array of objects"));
            };
            let docs = items
                .into_iter()
                .map(|i| match i {
                    Bson::Document(d) => Ok(d),
                    _ => Err(Error::location(40516, "$documents must evaluate to an array of objects")),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(docs_iter(docs))
        }
        "$collStats" => {
            let spec = expect_doc(name, arg)?.clone();
            let coll = collection_name(opts, name)?;
            drop(input);
            let db = opts.db.clone();
            let ctx = Arc::clone(&opts.ctx);
            Ok(lazy(move || Ok(docs_iter(vec![ctx.coll_stats(&db, &coll, &spec)?]))))
        }
        "$indexStats" => {
            let coll = collection_name(opts, name)?;
            drop(input);
            let db = opts.db.clone();
            let ctx = Arc::clone(&opts.ctx);
            Ok(lazy(move || Ok(docs_iter(ctx.index_stats(&db, &coll)?))))
        }
        "$listSessions" | "$listLocalSessions" => {
            drop(input);
            Ok(empty_iter())
        }
        "$search" | "$searchMeta" | "$listSearchIndexes" | "$rankFusion" => crate::search::build_stage(input, name, arg, opts),
        "$listClusterCatalog" | "$querySettings" => crate::cmd::qsettings::build_stage(input, name, arg, opts),
        "$currentOp" => Err(Error::location(40602, "$currentOp is not supported by this server")),
        "$changeStream" => Err(Error::location(40602, "$changeStream is not supported by this server")),
        _ => Err(unrecognized_stage(name)),
    }
}

fn expect_project_doc(arg: &Bson) -> Result<&Document> {
    match arg {
        Bson::Document(d) => Ok(d),
        _ => Err(Error::location(15969, "$project specification must be an object")),
    }
}

fn unset_projection(arg: &Bson) -> Result<Option<Projection>> {
    let fields: Vec<&str> = match arg {
        Bson::String(s) => vec![s.as_str()],
        Bson::Array(items) => items
            .iter()
            .map(|v| match v {
                Bson::String(s) => Ok(s.as_str()),
                _ => Err(Error::location(31120, "$unset specification must be a string or an array containing only string values")),
            })
            .collect::<Result<Vec<_>>>()?,
        _ => {
            return Err(Error::location(
                31120,
                "$unset specification must be a string or an array containing only string values",
            ));
        }
    };
    if fields.is_empty() {
        return Ok(None);
    }
    let mut spec = Document::new();
    for f in fields {
        spec.insert(f, 0i32);
    }
    Ok(Some(Projection::new(&spec, ProjectionKind::Agg)?))
}

fn parse_replace_root(arg: &Bson) -> Result<Bson> {
    let spec = expect_doc("$replaceRoot", arg)?;
    if let Some(k) = spec.keys().find(|k| k.as_str() != "newRoot") {
        return Err(Error::location(40230, format!("unrecognized option to $replaceRoot stage: {k}")));
    }
    spec.get("newRoot")
        .cloned()
        .ok_or_else(|| Error::location(40231, "no newRoot specified for the $replaceRoot stage"))
}

fn replace_root_with(d: &Document, expr: &Bson, vars: &Vars) -> Result<Document> {
    match eval(expr, d, vars)? {
        Bson::Document(nd) => Ok(nd),
        other => Err(Error::location(
            40228,
            format!("'newRoot' expression must evaluate to an object, but resulting value was: {other}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// $unwind
// ---------------------------------------------------------------------------

fn unwind_path(s: &str) -> Result<String> {
    match s.strip_prefix('$') {
        Some(p) if !p.is_empty() => Ok(p.to_string()),
        _ => Err(Error::location(
            28818,
            format!("path option to $unwind stage should be prefixed with a '$': {s}"),
        )),
    }
}

fn parse_unwind(arg: &Bson) -> Result<(String, Option<String>, bool)> {
    match arg {
        Bson::String(s) => Ok((unwind_path(s)?, None, false)),
        Bson::Document(spec) => {
            let path = match spec.get("path") {
                Some(Bson::String(p)) => unwind_path(p)?,
                Some(_) => return Err(Error::location(28808, "expected a string as the path for $unwind stage")),
                None => return Err(Error::location(15981, "no path specified to $unwind stage")),
            };
            let index = match spec.get("includeArrayIndex") {
                None | Some(Bson::Null) => None,
                Some(Bson::String(s)) => Some(s.clone()),
                Some(_) => {
                    return Err(Error::location(
                        28823,
                        "includeArrayIndex option to $unwind stage should be a string",
                    ));
                }
            };
            let preserve = match spec.get("preserveNullAndEmptyArrays") {
                None => false,
                Some(Bson::Boolean(b)) => *b,
                Some(_) => {
                    return Err(Error::location(
                        28809,
                        "preserveNullAndEmptyArrays option to $unwind stage should be a bool value",
                    ));
                }
            };
            if let Some(k) = spec
                .keys()
                .find(|k| !matches!(k.as_str(), "path" | "includeArrayIndex" | "preserveNullAndEmptyArrays"))
            {
                return Err(Error::location(28811, format!("unrecognized option to $unwind stage: {k}")));
            }
            Ok((path, index, preserve))
        }
        _ => Err(Error::location(28808, "expected a string or object as the argument to $unwind stage")),
    }
}

fn unwind_doc(d: Document, path: &str, index: Option<&str>, preserve: bool) -> Result<Vec<Document>> {
    let keep_as_is = |mut out: Document| -> Result<Vec<Document>> {
        if let Some(ix) = index {
            set_path(&mut out, ix, Bson::Null)?;
        }
        Ok(vec![out])
    };
    let value = get_path(&d, path).cloned();
    match value {
        Some(Bson::Array(items)) if !items.is_empty() => {
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.into_iter().enumerate() {
                let mut nd = d.clone();
                set_path(&mut nd, path, item)?;
                if let Some(ix) = index {
                    set_path(&mut nd, ix, Bson::Int64(i as i64))?;
                }
                out.push(nd);
            }
            Ok(out)
        }
        Some(Bson::Array(_)) => {
            if preserve {
                let mut out = d;
                remove_path(&mut out, path);
                keep_as_is(out)
            } else {
                Ok(vec![])
            }
        }
        Some(v) if !is_missing(&v) && !matches!(v, Bson::Null) => keep_as_is(d),
        _ => {
            if preserve {
                keep_as_is(d)
            } else {
                Ok(vec![])
            }
        }
    }
}

// ---------------------------------------------------------------------------
// $sort (in memory, top-k, and external merge sort)
// ---------------------------------------------------------------------------

fn sort_stage(input: DocIter, spec: SortSpec, limit: Option<usize>, opts: Arc<AggOptions>) -> DocIter {
    lazy(move || sort_collect(input, spec, limit, &opts))
}

fn sort_collect(input: DocIter, spec: SortSpec, limit: Option<usize>, opts: &AggOptions) -> Result<DocIter> {
    let spec = Arc::new(spec);
    match limit {
        Some(n) => top_k(input, &spec, n, opts),
        None => external_sort(input, spec, opts),
    }
}

fn over_limit(opts: &AggOptions) -> Error {
    Error::location(
        292,
        format!(
            "Sort exceeded memory limit of {} bytes, but did not opt in to external sorting.",
            opts.max_memory_bytes
        ),
    )
}

fn sort_in_memory(mem: &mut [(Vec<Bson>, Document)], spec: &SortSpec) {
    mem.sort_by(|a, b| spec.compare_keys(&a.0, &b.0));
}

/// `$sort` immediately followed by `$limit n`: bounded buffer, never spills.
fn top_k(input: DocIter, spec: &SortSpec, n: usize, opts: &AggOptions) -> Result<DocIter> {
    let cap = n.saturating_mul(2);
    let mut buf: Vec<(Vec<Bson>, Document)> = Vec::new();
    let mut bytes = 0usize;
    for r in input {
        let d = r?;
        bytes += doc_size(&d);
        let key = spec.sort_key(&d);
        buf.push((key, d));
        if buf.len() > cap {
            sort_in_memory(&mut buf, spec);
            buf.truncate(n);
            bytes = buf.iter().map(|(_, d)| doc_size(d)).sum();
        }
        if bytes > opts.max_memory_bytes && !opts.allow_disk_use {
            return Err(over_limit(opts));
        }
    }
    sort_in_memory(&mut buf, spec);
    buf.truncate(n);
    let it: DocIter = Box::new(buf.into_iter().map(|(_, d)| Ok(d)));
    Ok(it)
}

/// Removes spill files when dropped.
struct TempFiles(Vec<PathBuf>);

impl Drop for TempFiles {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::remove_file(p);
        }
    }
}

fn io_error(e: std::io::Error) -> Error {
    Error::internal(format!("sort spill file I/O failed: {e}"))
}

fn spill_path(opts: &AggOptions) -> Result<PathBuf> {
    let dir = opts.ctx.temp_dir();
    std::fs::create_dir_all(&dir).map_err(io_error)?;
    let seq = SPILL_SEQ.fetch_add(1, AtomicOrdering::Relaxed);
    Ok(dir.join(format!("mogo-sort-{}-{}.lz4", std::process::id(), seq)))
}

/// Writes `docs` as length-prefixed BSON records inside one LZ4 frame.
fn write_run(path: &Path, docs: impl Iterator<Item = Document>) -> Result<()> {
    let file = File::create(path).map_err(io_error)?;
    let mut enc = FrameEncoder::new(BufWriter::new(file));
    for d in docs {
        let bytes = to_bytes(&d);
        let len = u32::try_from(bytes.len()).map_err(|_| Error::internal("sort spill record exceeds 4 GiB"))?;
        enc.write_all(&len.to_le_bytes()).map_err(io_error)?;
        enc.write_all(&bytes).map_err(io_error)?;
    }
    let mut out = enc
        .finish()
        .map_err(|e| Error::internal(format!("sort spill frame write failed: {e:?}")))?;
    out.flush().map_err(io_error)
}

struct RunReader {
    dec: FrameDecoder<BufReader<File>>,
}

impl Iterator for RunReader {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_buf = [0u8; 4];
        match self.dec.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(io_error(e))),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        if let Err(e) = self.dec.read_exact(&mut buf) {
            return Some(Err(io_error(e)));
        }
        Some(from_bytes(&buf))
    }
}

fn read_run(path: &Path) -> Result<DocIter> {
    let file = File::open(path).map_err(io_error)?;
    let it: DocIter = Box::new(RunReader { dec: FrameDecoder::new(BufReader::new(file)) });
    Ok(it)
}

struct Entry {
    key: Vec<Bson>,
    src: usize,
    doc: Document,
    spec: Arc<SortSpec>,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Entry {}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.spec
            .compare_keys(&self.key, &other.key)
            .then_with(|| self.src.cmp(&other.src))
    }
}

/// K-way merge over sorted sources. Ties go to the lower source index (earlier input).
struct Merge {
    sources: Vec<DocIter>,
    heap: BinaryHeap<Reverse<Entry>>,
    spec: Arc<SortSpec>,
    started: bool,
    _files: TempFiles,
}

impl Merge {
    fn refill(&mut self, src: usize) -> Result<()> {
        if let Some(item) = self.sources[src].next() {
            let doc = item?;
            let key = self.spec.sort_key(&doc);
            self.heap.push(Reverse(Entry { key, src, doc, spec: Arc::clone(&self.spec) }));
        }
        Ok(())
    }
}

impl Iterator for Merge {
    type Item = Result<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
            for i in 0..self.sources.len() {
                if let Err(e) = self.refill(i) {
                    return Some(Err(e));
                }
            }
        }
        let Reverse(entry) = self.heap.pop()?;
        if let Err(e) = self.refill(entry.src) {
            return Some(Err(e));
        }
        Some(Ok(entry.doc))
    }
}

fn external_sort(input: DocIter, spec: Arc<SortSpec>, opts: &AggOptions) -> Result<DocIter> {
    let mut files = TempFiles(Vec::new());
    let mut mem: Vec<(Vec<Bson>, Document)> = Vec::new();
    let mut bytes = 0usize;
    for r in input {
        let d = r?;
        let size = doc_size(&d);
        if bytes + size > opts.max_memory_bytes && !mem.is_empty() {
            if !opts.allow_disk_use {
                return Err(over_limit(opts));
            }
            sort_in_memory(&mut mem, &spec);
            let path = spill_path(opts)?;
            files.0.push(path.clone());
            write_run(&path, mem.drain(..).map(|(_, d)| d))?;
            bytes = 0;
        }
        bytes += size;
        let key = spec.sort_key(&d);
        mem.push((key, d));
    }
    sort_in_memory(&mut mem, &spec);
    if files.0.is_empty() {
        let it: DocIter = Box::new(mem.into_iter().map(|(_, d)| Ok(d)));
        return Ok(it);
    }
    let mut sources: Vec<DocIter> = Vec::with_capacity(files.0.len() + 1);
    for path in &files.0 {
        sources.push(read_run(path)?);
    }
    if !mem.is_empty() {
        let tail: DocIter = Box::new(mem.into_iter().map(|(_, d)| Ok(d)));
        sources.push(tail);
    }
    let it: DocIter = Box::new(Merge {
        sources,
        heap: BinaryHeap::new(),
        spec,
        started: false,
        _files: files,
    });
    Ok(it)
}

// ---------------------------------------------------------------------------
// Grouping helpers
// ---------------------------------------------------------------------------

/// Normalizes numbers so that equal values share one grouping key (Int32 1 == Double 1.0).
fn group_key_value(v: &Bson) -> Bson {
    match v {
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => match num_i64(v) {
            Some(i) => Bson::Int64(i),
            None => num_f64(v).map_or_else(|| v.clone(), Bson::Double),
        },
        other => other.clone(),
    }
}

/// Values of `field` in `d` with arrays expanded; missing yields nothing.
fn present_values(d: &Document, field: &str) -> Vec<Bson> {
    match get_path(d, field) {
        None => vec![],
        Some(Bson::Array(items)) => items.iter().filter(|v| !is_missing(v)).cloned().collect(),
        Some(v) if is_missing(v) => vec![],
        Some(v) => vec![v.clone()],
    }
}

/// `$eq`-style candidates for an equality match: missing -> null, arrays expanded.
fn field_candidates(d: &Document, field: &str) -> Vec<Bson> {
    match get_path(d, field) {
        None => vec![Bson::Null],
        Some(v) if is_missing(v) => vec![Bson::Null],
        Some(Bson::Array(items)) => items
            .iter()
            .map(|i| if is_missing(i) { Bson::Null } else { i.clone() })
            .collect(),
        Some(v) => vec![v.clone()],
    }
}

fn take_unseen(values: Vec<Bson>, seen: &mut HashSet<Vec<u8>>) -> Vec<Bson> {
    values
        .into_iter()
        .filter(|v| seen.insert(to_bytes(&doc! {"k": group_key_value(v)})))
        .collect()
}

// ---------------------------------------------------------------------------
// $facet
// ---------------------------------------------------------------------------

fn build_facet(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let Bson::Document(facets) = arg else {
        return Err(Error::location(40169, "the $facet specification must be an object"));
    };
    if facets.is_empty() {
        return Err(Error::location(40169, "the $facet specification must be a non-empty object"));
    }
    let mut plans: Vec<(String, Arc<Vec<Document>>)> = Vec::with_capacity(facets.len());
    for (name, v) in facets {
        let stages = pipeline_docs("$facet", v)?;
        check_nested(&stages, "$facet")?;
        drop(run_pipeline(empty_iter(), &stages, Arc::clone(opts))?);
        plans.push((name.clone(), Arc::new(stages)));
    }
    let o = Arc::clone(opts);
    Ok(lazy(move || {
        let docs = collect_docs(input)?;
        let mut out = Document::new();
        for (name, stages) in plans {
            let sub = run_pipeline(docs_iter(docs.clone()), stages.as_slice(), Arc::clone(&o))?;
            let values = sub.map(|r| r.map(|mut x| {
                x.remove(META_KEY);
                Bson::Document(x)
            })).collect::<Result<Vec<Bson>>>()?;
            out.insert(name, Bson::Array(values));
        }
        Ok(docs_iter(vec![out]))
    }))
}

// ---------------------------------------------------------------------------
// $lookup
// ---------------------------------------------------------------------------

struct LookupSpec {
    from: String,
    as_: String,
    local: Option<String>,
    foreign: Option<String>,
    lets: Vec<(String, Bson)>,
    pipeline: Option<Vec<Document>>,
}

fn parse_lookup(arg: &Bson) -> Result<LookupSpec> {
    let spec = expect_doc("$lookup", arg)?;
    let mut from: Option<String> = None;
    let mut as_: Option<String> = None;
    let mut local: Option<String> = None;
    let mut foreign: Option<String> = None;
    let mut lets: Vec<(String, Bson)> = Vec::new();
    let mut pipeline: Option<Vec<Document>> = None;
    for (k, v) in spec {
        match k.as_str() {
            "from" => from = Some(string_field("$lookup", "from", v)?),
            "as" => as_ = Some(string_field("$lookup", "as", v)?),
            "localField" => local = Some(string_field("$lookup", "localField", v)?),
            "foreignField" => foreign = Some(string_field("$lookup", "foreignField", v)?),
            "let" => {
                let l = expect_doc("$lookup", v)?;
                lets = l.iter().map(|(n, e)| (n.clone(), e.clone())).collect();
            }
            "pipeline" => pipeline = Some(pipeline_docs("$lookup", v)?),
            other => return Err(Error::location(40415, format!("unknown argument to $lookup: {other}"))),
        }
    }
    Ok(LookupSpec {
        from: from.ok_or_else(|| Error::location(40449, "$lookup requires 'from' to be specified"))?,
        as_: as_.ok_or_else(|| Error::location(40449, "$lookup requires 'as' to be specified"))?,
        local,
        foreign,
        lets,
        pipeline,
    })
}

fn build_lookup(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let LookupSpec { from, as_, local, foreign, lets, pipeline } = parse_lookup(arg)?;
    match (local, foreign, pipeline) {
        (Some(local), Some(foreign), None) => equality_lookup(input, from, as_, local, foreign, opts),
        (local, foreign, Some(pipeline)) => {
            pipeline_lookup(input, from, as_, local, foreign, lets, pipeline, opts)
        }
        (None, None, None) => Err(Error::location(
            40321,
            "$lookup requires either 'pipeline' or both 'localField' and 'foreignField' to be specified",
        )),
        _ => Err(Error::failed_to_parse("$lookup requires both 'localField' and 'foreignField' to be specified")),
    }
}

fn equality_lookup(
    input: DocIter,
    from: String,
    as_: String,
    local: String,
    foreign: String,
    opts: &Arc<AggOptions>,
) -> Result<DocIter> {
    let o = Arc::clone(opts);
    Ok(lazy(move || {
        let foreign_docs = o.ctx.scan(&o.db, &from)?.collect::<Result<Vec<Document>>>()?;
        let index = EqIndex::build(foreign_docs, &foreign);
        Ok(map_docs(input, move |mut d| {
            let found = index.lookup(&field_candidates(&d, &local));
            let matches: Vec<Bson> = found.into_iter().map(|i| Bson::Document(index.docs[i].clone())).collect();
            set_path(&mut d, &as_, Bson::Array(matches))?;
            Ok(Some(d))
        }))
    }))
}

/// `{$match: {$expr: {$eq: [$foreign, $$__local]}}}` with missing treated as null.
fn correlation_stage(foreign: &str) -> Document {
    let foreign_side = doc! {"$ifNull": [format!("${foreign}"), Bson::Null]};
    let local_side = doc! {"$ifNull": ["$$__local", Bson::Null]};
    let eq = doc! {"$eq": [Bson::Document(foreign_side), Bson::Document(local_side)]};
    doc! {"$match": {"$expr": Bson::Document(eq)}}
}

#[allow(clippy::too_many_arguments)]
fn pipeline_lookup(
    input: DocIter,
    from: String,
    as_: String,
    local: Option<String>,
    foreign: Option<String>,
    lets: Vec<(String, Bson)>,
    pipeline: Vec<Document>,
    opts: &Arc<AggOptions>,
) -> Result<DocIter> {
    if local.is_some() != foreign.is_some() {
        return Err(Error::failed_to_parse("$lookup requires both 'localField' and 'foreignField' to be specified"));
    }
    check_nested(&pipeline, "$lookup")?;
    let mut stages = Vec::with_capacity(pipeline.len() + 1);
    if let Some(ff) = &foreign {
        stages.push(correlation_stage(ff));
    }
    stages.extend(pipeline);
    let stages = Arc::new(stages);
    drop(run_pipeline(empty_iter(), stages.as_slice(), sub_options(opts, &from, opts.vars.clone()))?);
    let o = Arc::clone(opts);
    Ok(map_docs(input, move |mut d| {
        let mut vars = o.vars.clone();
        for (name, expr) in &lets {
            let v = eval(expr, &d, &o.vars)?;
            vars.insert(name.clone(), v);
        }
        if let Some(lf) = &local {
            let lv = get_path(&d, lf).filter(|v| !is_missing(v)).cloned().unwrap_or(Bson::Null);
            vars.insert("__local".to_string(), lv);
        }
        let sub = sub_options(&o, &from, vars);
        let found = run_pipeline(o.ctx.scan(&o.db, &from)?, stages.as_slice(), sub)?
            .map(|r| r.map(|mut x| {
                x.remove(META_KEY);
                Bson::Document(x)
            }))
            .collect::<Result<Vec<Bson>>>()?;
        set_path(&mut d, &as_, Bson::Array(found))?;
        Ok(Some(d))
    }))
}

/// Canonical hashable form of a scalar; `None` for arrays/documents/other types.
#[derive(PartialEq, Eq, Hash)]
enum CanonKey {
    Null,
    Bool(bool),
    Num(u64),
    Str(String),
    Oid([u8; 12]),
    Date(i64),
}

fn float_bits(f: f64) -> u64 {
    if f.is_nan() {
        f64::NAN.to_bits()
    } else if f == 0.0 {
        0
    } else {
        f.to_bits()
    }
}

fn canon_key(v: &Bson) -> Option<CanonKey> {
    Some(match v {
        Bson::Null => CanonKey::Null,
        Bson::Boolean(b) => CanonKey::Bool(*b),
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
            CanonKey::Num(float_bits(num_f64(v)?))
        }
        Bson::String(s) => CanonKey::Str(s.clone()),
        Bson::ObjectId(o) => CanonKey::Oid(o.bytes()),
        Bson::DateTime(d) => CanonKey::Date(d.timestamp_millis()),
        _ => return None,
    })
}

/// Hash index over the foreign collection for `$lookup` equality matching.
struct EqIndex {
    docs: Vec<Document>,
    vals: Vec<Vec<Bson>>,
    keyed: HashMap<CanonKey, Vec<usize>>,
    unkeyed: Vec<usize>,
}

impl EqIndex {
    fn build(docs: Vec<Document>, field: &str) -> EqIndex {
        let mut vals = Vec::with_capacity(docs.len());
        let mut keyed: HashMap<CanonKey, Vec<usize>> = HashMap::new();
        let mut unkeyed = Vec::new();
        for (i, d) in docs.iter().enumerate() {
            let vs = field_candidates(d, field);
            let mut any_unkeyed = false;
            for v in &vs {
                match canon_key(v) {
                    Some(k) => keyed.entry(k).or_default().push(i),
                    None => any_unkeyed = true,
                }
            }
            if any_unkeyed {
                unkeyed.push(i);
            }
            vals.push(vs);
        }
        EqIndex { docs, vals, keyed, unkeyed }
    }

    /// Indices of foreign docs with a value equal to any candidate, in foreign order.
    fn lookup(&self, cands: &[Bson]) -> Vec<usize> {
        let mut idx: BTreeSet<usize> = BTreeSet::new();
        if cands.iter().any(|c| canon_key(c).is_none()) {
            idx.extend(0..self.docs.len());
        } else {
            for c in cands {
                if let Some(k) = canon_key(c) {
                    if let Some(hits) = self.keyed.get(&k) {
                        idx.extend(hits.iter().copied());
                    }
                }
            }
            idx.extend(self.unkeyed.iter().copied());
        }
        idx.into_iter()
            .filter(|&i| self.vals[i].iter().any(|v| cands.iter().any(|c| bson_eq(v, c))))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// $graphLookup
// ---------------------------------------------------------------------------

struct GraphSpec {
    from: String,
    start_with: Bson,
    connect_from: String,
    connect_to: String,
    as_: String,
    max_depth: Option<i64>,
    depth_field: Option<String>,
    restrict: Option<Document>,
}

fn parse_graph(arg: &Bson) -> Result<GraphSpec> {
    let spec = expect_doc("$graphLookup", arg)?;
    let mut from: Option<String> = None;
    let mut start: Option<Bson> = None;
    let mut connect_from: Option<String> = None;
    let mut connect_to: Option<String> = None;
    let mut as_: Option<String> = None;
    let mut max_depth: Option<i64> = None;
    let mut depth_field: Option<String> = None;
    let mut restrict: Option<Document> = None;
    for (k, v) in spec {
        match k.as_str() {
            "from" => from = Some(string_field("$graphLookup", "from", v)?),
            "startWith" => start = Some(v.clone()),
            "connectFromField" => connect_from = Some(string_field("$graphLookup", "connectFromField", v)?),
            "connectToField" => connect_to = Some(string_field("$graphLookup", "connectToField", v)?),
            "as" => as_ = Some(string_field("$graphLookup", "as", v)?),
            "maxDepth" => {
                max_depth = Some(
                    num_i64(v)
                        .filter(|n| *n >= 0)
                        .ok_or_else(|| Error::location(40100, "maxDepth must be a non-negative integer"))?,
                )
            }
            "depthField" => depth_field = Some(string_field("$graphLookup", "depthField", v)?),
            "restrictSearchWithMatch" => restrict = Some(expect_doc("$graphLookup", v)?.clone()),
            other => return Err(Error::location(40415, format!("unknown argument to $graphLookup: {other}"))),
        }
    }
    let missing = |f: &str| Error::location(40352, format!("Missing '{f}' parameter to $graphLookup"));
    Ok(GraphSpec {
        from: from.ok_or_else(|| missing("from"))?,
        start_with: start.ok_or_else(|| missing("startWith"))?,
        connect_from: connect_from.ok_or_else(|| missing("connectFromField"))?,
        connect_to: connect_to.ok_or_else(|| missing("connectToField"))?,
        as_: as_.ok_or_else(|| missing("as"))?,
        max_depth,
        depth_field,
        restrict,
    })
}

fn build_graph_lookup(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let g = parse_graph(arg)?;
    let o = Arc::clone(opts);
    Ok(map_docs(input, move |mut d| {
        let found = graph_search(&o, &g, &d)?;
        set_path(&mut d, &g.as_, Bson::Array(found.into_iter().map(Bson::Document).collect()))?;
        Ok(Some(d))
    }))
}

/// Breadth-first search over `from`, following `connectFromField` -> `connectToField` edges.
fn graph_search(o: &AggOptions, g: &GraphSpec, d: &Document) -> Result<Vec<Document>> {
    let start = match eval(&g.start_with, d, &o.vars)? {
        Bson::Array(items) => items,
        v => vec![v],
    };
    let mut seen_vals: HashSet<Vec<u8>> = HashSet::new();
    let mut seen_docs: HashSet<Vec<u8>> = HashSet::new();
    let mut frontier = take_unseen(start, &mut seen_vals);
    let mut results: Vec<Document> = Vec::new();
    let mut depth: i64 = 0;
    while !frontier.is_empty() {
        let mut clause = Document::new();
        clause.insert(g.connect_to.clone(), Bson::Document(doc! {"$in": Bson::Array(frontier.clone())}));
        let filter = match &g.restrict {
            Some(r) => doc! {"$and": [Bson::Document(r.clone()), Bson::Document(clause)]},
            None => clause,
        };
        let mut next = Vec::new();
        for mut nd in o.ctx.find(&o.db, &g.from, &filter)? {
            if !seen_docs.insert(to_bytes(&nd)) {
                continue;
            }
            if let Some(f) = &g.depth_field {
                set_path(&mut nd, f, Bson::Int64(depth))?;
            }
            next.extend(present_values(&nd, &g.connect_from));
            results.push(nd);
        }
        if g.max_depth.is_some_and(|m| depth >= m) {
            break;
        }
        frontier = take_unseen(next, &mut seen_vals);
        depth += 1;
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// $unionWith
// ---------------------------------------------------------------------------

fn build_union_with(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let (coll, pipeline) = match arg {
        Bson::String(c) => (c.clone(), Vec::new()),
        Bson::Document(spec) => {
            let mut coll: Option<String> = None;
            let mut pipeline: Vec<Document> = Vec::new();
            for (k, v) in spec {
                match k.as_str() {
                    "coll" => coll = Some(string_field("$unionWith", "coll", v)?),
                    "pipeline" => pipeline = pipeline_docs("$unionWith", v)?,
                    other => return Err(Error::location(40415, format!("unknown argument to $unionWith: {other}"))),
                }
            }
            let coll = coll.ok_or_else(|| Error::location(40414, "$unionWith requires a 'coll' field"))?;
            (coll, pipeline)
        }
        _ => return Err(Error::failed_to_parse("$unionWith requires a string or an object")),
    };
    check_nested(&pipeline, "$unionWith")?;
    let sub = sub_options(opts, &coll, opts.vars.clone());
    if !pipeline.is_empty() {
        drop(run_pipeline(empty_iter(), &pipeline, Arc::clone(&sub))?);
    }
    let o = Arc::clone(opts);
    let stages = Arc::new(pipeline);
    let other = lazy(move || {
        let scanned = o.ctx.scan(&o.db, &coll)?;
        if stages.is_empty() {
            Ok(scanned)
        } else {
            run_pipeline(scanned, stages.as_slice(), sub)
        }
    });
    let it: DocIter = Box::new(input.chain(other));
    Ok(it)
}

// ---------------------------------------------------------------------------
// $redact
// ---------------------------------------------------------------------------

enum Redact {
    Keep,
    Prune,
    Descend,
}

fn redact_decision(v: &Bson) -> Result<Redact> {
    match v {
        Bson::String(s) if s == "keep" => Ok(Redact::Keep),
        Bson::String(s) if s == "prune" => Ok(Redact::Prune),
        Bson::String(s) if s == "descend" => Ok(Redact::Descend),
        _ => Err(Error::location(
            17053,
            "$redact's expression should evaluate to one of the following: $$DESCEND, $$PRUNE, $$KEEP",
        )),
    }
}

fn redact_doc(d: &Document, expr: &Bson, vars: &Vars) -> Result<Option<Document>> {
    match redact_decision(&eval(expr, d, vars)?)? {
        Redact::Keep => Ok(Some(d.clone())),
        Redact::Prune => Ok(None),
        Redact::Descend => {
            let mut out = Document::new();
            for (k, v) in d {
                if let Some(nv) = redact_value(v, expr, vars)? {
                    out.insert(k.clone(), nv);
                }
            }
            Ok(Some(out))
        }
    }
}

fn redact_value(v: &Bson, expr: &Bson, vars: &Vars) -> Result<Option<Bson>> {
    match v {
        Bson::Document(sub) => Ok(redact_doc(sub, expr, vars)?.map(Bson::Document)),
        Bson::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                if let Some(x) = redact_value(item, expr, vars)? {
                    out.push(x);
                }
            }
            Ok(Some(Bson::Array(out)))
        }
        other => Ok(Some(other.clone())),
    }
}

fn build_redact(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let expr = arg.clone();
    let mut vars = opts.vars.clone();
    vars.insert("DESCEND".to_string(), Bson::String("descend".to_string()));
    vars.insert("PRUNE".to_string(), Bson::String("prune".to_string()));
    vars.insert("KEEP".to_string(), Bson::String("keep".to_string()));
    Ok(map_docs(input, move |d| redact_doc(&d, &expr, &vars)))
}

// ---------------------------------------------------------------------------
// $out / $merge
// ---------------------------------------------------------------------------

fn build_out(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let (db, coll) = target_ns(arg, &opts.db, 16994, "$out")?;
    let ctx = Arc::clone(&opts.ctx);
    Ok(lazy(move || {
        let docs = collect_docs(input)?;
        ctx.replace_collection(&db, &coll, docs)?;
        Ok(empty_iter())
    }))
}

enum WhenMatched {
    Replace,
    KeepExisting,
    Merge,
    Fail,
    Pipeline(Arc<Vec<Document>>),
}

enum WhenNotMatched {
    Insert,
    Discard,
    Fail,
}

struct MergeSpec {
    db: String,
    coll: String,
    on: Vec<String>,
    when_matched: WhenMatched,
    when_not_matched: WhenNotMatched,
}

fn parse_when_matched(v: &Bson) -> Result<WhenMatched> {
    Ok(match v {
        Bson::String(s) => match s.as_str() {
            "replace" => WhenMatched::Replace,
            "keepExisting" => WhenMatched::KeepExisting,
            "merge" => WhenMatched::Merge,
            "fail" => WhenMatched::Fail,
            _ => return Err(Error::location(51191, format!("unrecognized value for whenMatched: {s}"))),
        },
        Bson::Array(_) => WhenMatched::Pipeline(Arc::new(pipeline_docs("$merge", v)?)),
        _ => return Err(Error::location(51191, "whenMatched must be a string or an array")),
    })
}

fn parse_when_not_matched(v: &Bson) -> Result<WhenNotMatched> {
    match v {
        Bson::String(s) => match s.as_str() {
            "insert" => Ok(WhenNotMatched::Insert),
            "discard" => Ok(WhenNotMatched::Discard),
            "fail" => Ok(WhenNotMatched::Fail),
            _ => Err(Error::location(51189, format!("unrecognized value for whenNotMatched: {s}"))),
        },
        _ => Err(Error::location(51189, "whenNotMatched must be a string")),
    }
}

fn parse_merge(arg: &Bson, default_db: &str) -> Result<MergeSpec> {
    let spec = expect_doc("$merge", arg)?;
    let mut into: Option<&Bson> = None;
    let mut on: Option<&Bson> = None;
    let mut when_matched = WhenMatched::Merge;
    let mut when_not_matched = WhenNotMatched::Insert;
    for (k, v) in spec {
        match k.as_str() {
            "into" => into = Some(v),
            "on" => on = Some(v),
            "whenMatched" => when_matched = parse_when_matched(v)?,
            "whenNotMatched" => when_not_matched = parse_when_not_matched(v)?,
            other => return Err(Error::location(40415, format!("unrecognized option to $merge: {other}"))),
        }
    }
    let into = into.ok_or_else(|| Error::location(51178, "$merge requires an 'into' field"))?;
    let (db, coll) = target_ns(into, default_db, 51178, "$merge")?;
    let on = match on {
        None => vec!["_id".to_string()],
        Some(Bson::String(s)) => vec![s.clone()],
        Some(Bson::Array(items)) if !items.is_empty() => items
            .iter()
            .map(|x| match x {
                Bson::String(s) => Ok(s.clone()),
                _ => Err(Error::location(51186, "$merge 'on' field must be a string or an array of strings")),
            })
            .collect::<Result<Vec<_>>>()?,
        Some(_) => return Err(Error::location(51186, "$merge 'on' field must be a string or an array of strings")),
    };
    Ok(MergeSpec { db, coll, on, when_matched, when_not_matched })
}

fn build_merge(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let m = parse_merge(arg, &opts.db)?;
    let o = Arc::clone(opts);
    let it: DocIter = Box::new(input.filter_map(move |r| match r {
        Ok(d) => match merge_one(&o, &m, d) {
            Ok(()) => None,
            Err(e) => Some(Err(e)),
        },
        Err(e) => Some(Err(e)),
    }));
    Ok(it)
}

fn merge_one(o: &AggOptions, m: &MergeSpec, d: Document) -> Result<()> {
    let mut filter = Document::new();
    for f in &m.on {
        let v = get_path(&d, f).filter(|v| !is_missing(v)).cloned().unwrap_or(Bson::Null);
        filter.insert(f.clone(), v);
    }
    let existing = o.ctx.find(&m.db, &m.coll, &filter)?.into_iter().next();
    let Some(old) = existing else {
        return match m.when_not_matched {
            WhenNotMatched::Insert => o.ctx.insert(&m.db, &m.coll, d),
            WhenNotMatched::Discard => Ok(()),
            WhenNotMatched::Fail => Err(Error::location(13113, "$merge could not insert a new document")),
        };
    };
    let id = old.get("_id").cloned().unwrap_or(Bson::Null);
    let replacement = match &m.when_matched {
        WhenMatched::KeepExisting => return Ok(()),
        WhenMatched::Fail => {
            return Err(Error::location(
                11000,
                format!("E11000 duplicate key error collection: {}.{}", m.db, m.coll),
            ));
        }
        WhenMatched::Replace => {
            let mut nd = doc! {"_id": id.clone()};
            for (k, v) in d {
                if k != "_id" {
                    nd.insert(k, v);
                }
            }
            nd
        }
        WhenMatched::Merge => {
            let mut merged = old;
            for (k, v) in d {
                if k != "_id" {
                    merged.insert(k, v);
                }
            }
            merged.insert("_id", id.clone());
            merged
        }
        WhenMatched::Pipeline(stages) => {
            let mut vars = o.vars.clone();
            vars.insert("new".to_string(), Bson::Document(d));
            let mut res = apply_simple_stages(old, stages.as_slice(), &vars)?;
            res.insert("_id", id.clone());
            res
        }
    };
    o.ctx.replace_by_id(&m.db, &m.coll, &id, replacement)
}

// ---------------------------------------------------------------------------
// $geoNear / $vectorSearch
// ---------------------------------------------------------------------------

/// Distance and matched geo value of the nearest geo field (or of `key` when given).
fn nearest(d: &Document, key: Option<&str>, spec: &NearSpec) -> Option<(f64, Bson)> {
    let candidates: Vec<&Bson> = match key {
        Some(k) => get_path(d, k).into_iter().collect(),
        None => d.values().collect(),
    };
    candidates
        .into_iter()
        .filter_map(|v| near_distance(v, spec).map(|dist| (dist, v.clone())))
        .min_by(|a, b| a.0.total_cmp(&b.0))
}

fn build_geo_near(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let spec = expect_doc("$geoNear", arg)?;
    let mut near: Option<Bson> = None;
    let mut distance_field: Option<String> = None;
    let mut spherical = false;
    let mut max_distance: Option<f64> = None;
    let mut min_distance: Option<f64> = None;
    let mut query: Option<Document> = None;
    let mut multiplier = 1.0f64;
    let mut include_locs: Option<String> = None;
    let mut key: Option<String> = None;
    for (k, v) in spec {
        match k.as_str() {
            "near" => near = Some(v.clone()),
            "distanceField" => distance_field = Some(string_field("$geoNear", "distanceField", v)?),
            "spherical" => match v {
                Bson::Boolean(b) => spherical = *b,
                _ => return Err(Error::bad_value("$geoNear 'spherical' must be a boolean")),
            },
            "maxDistance" => {
                max_distance = Some(num_f64(v).ok_or_else(|| Error::bad_value("maxDistance must be a number"))?)
            }
            "minDistance" => {
                min_distance = Some(num_f64(v).ok_or_else(|| Error::bad_value("minDistance must be a number"))?)
            }
            "query" => query = Some(expect_doc("$geoNear", v)?.clone()),
            "distanceMultiplier" => {
                multiplier = num_f64(v).ok_or_else(|| Error::bad_value("distanceMultiplier must be a number"))?
            }
            "includeLocs" => include_locs = Some(string_field("$geoNear", "includeLocs", v)?),
            "key" => key = Some(string_field("$geoNear", "key", v)?),
            other => return Err(Error::location(40415, format!("unknown argument to $geoNear: {other}"))),
        }
    }
    let near = near.ok_or_else(|| Error::location(5860204, "$geoNear requires a 'near' option as an argument."))?;
    let distance_field = distance_field
        .ok_or_else(|| Error::location(16603, "$geoNear requires a 'distanceField' option as a String"))?;
    let center = match parse_geometry(&near) {
        Some(Geometry::Point(p)) => p,
        _ => return Err(Error::location(16602, "$geoNear 'near' must be a point")),
    };
    let is_geojson = matches!(&near, Bson::Document(d) if d.contains_key("type"));
    let nspec = NearSpec {
        point: center,
        spherical: spherical || is_geojson,
        max_distance,
        min_distance,
        radians: false,
    };
    let matcher = query.map(|q| Matcher::with_vars(&q, opts.vars.clone())).transpose()?;
    Ok(lazy(move || {
        let mut hits: Vec<(f64, Bson, Document)> = Vec::new();
        for r in input {
            let d = r?;
            if let Some(m) = &matcher {
                if !m.matches(&d)? {
                    continue;
                }
            }
            let Some((dist, loc)) = nearest(&d, key.as_deref(), &nspec) else {
                continue;
            };
            if nspec.max_distance.is_some_and(|mx| dist > mx) || nspec.min_distance.is_some_and(|mn| dist < mn) {
                continue;
            }
            hits.push((dist, loc, d));
        }
        hits.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut out = Vec::with_capacity(hits.len());
        for (dist, loc, mut d) in hits {
            set_path(&mut d, &distance_field, Bson::Double(dist * multiplier))?;
            if let Some(il) = &include_locs {
                set_path(&mut d, il, loc)?;
            }
            out.push(d);
        }
        Ok(docs_iter(out))
    }))
}

fn norm(v: &[f64]) -> f64 {
    v.iter().map(|x| x * x).sum::<f64>().sqrt()
}

fn doc_vector(d: &Document, path: &str) -> Option<Vec<f64>> {
    match get_path(d, path)? {
        Bson::Array(items) => items.iter().map(num_f64).collect(),
        _ => None,
    }
}

fn set_meta(d: &mut Document, field: &str, value: Bson) {
    let mut meta = match d.get(META_KEY) {
        Some(Bson::Document(m)) => m.clone(),
        _ => Document::new(),
    };
    meta.insert(field, value);
    d.insert(META_KEY, meta);
}

fn build_vector_search(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let spec = expect_doc("$vectorSearch", arg)?;
    let mut path: Option<String> = None;
    let mut query: Option<Vec<f64>> = None;
    let mut limit: Option<usize> = None;
    let mut filter: Option<Document> = None;
    for (k, v) in spec {
        match k.as_str() {
            "path" => path = Some(string_field("$vectorSearch", "path", v)?),
            "queryVector" => {
                let Bson::Array(items) = v else {
                    return Err(Error::bad_value("$vectorSearch queryVector must be an array of numbers"));
                };
                let vals = items
                    .iter()
                    .map(|x| num_f64(x).ok_or_else(|| Error::bad_value("$vectorSearch queryVector must be numeric")))
                    .collect::<Result<Vec<f64>>>()?;
                query = Some(vals);
            }
            "limit" => limit = Some(parse_limit(v)?),
            "filter" => filter = Some(expect_doc("$vectorSearch", v)?.clone()),
            "index" | "numCandidates" | "exact" => {}
            other => return Err(Error::location(40415, format!("unknown argument to $vectorSearch: {other}"))),
        }
    }
    let path = path.ok_or_else(|| Error::bad_value("$vectorSearch requires a 'path' field"))?;
    let query = query.ok_or_else(|| Error::bad_value("$vectorSearch requires a 'queryVector' field"))?;
    if query.is_empty() {
        return Err(Error::bad_value("$vectorSearch queryVector must not be empty"));
    }
    let limit = limit.ok_or_else(|| Error::bad_value("$vectorSearch requires a 'limit' field"))?;
    let qn = norm(&query);
    if qn == 0.0 {
        return Err(Error::bad_value("$vectorSearch queryVector must have non-zero magnitude"));
    }
    let matcher = filter.map(|f| Matcher::with_vars(&f, opts.vars.clone())).transpose()?;
    Ok(lazy(move || {
        let mut scored: Vec<(f64, Document)> = Vec::new();
        for r in input {
            let d = r?;
            if let Some(m) = &matcher {
                if !m.matches(&d)? {
                    continue;
                }
            }
            let Some(v) = doc_vector(&d, &path) else {
                continue;
            };
            if v.len() != query.len() {
                continue;
            }
            let dn = norm(&v);
            if dn == 0.0 {
                continue;
            }
            let dot: f64 = v.iter().zip(&query).map(|(a, b)| a * b).sum();
            let cos = dot / (dn * qn);
            scored.push(((1.0 + cos) / 2.0, d));
        }
        scored.sort_by(|a, b| b.0.total_cmp(&a.0));
        scored.truncate(limit);
        let mut out = Vec::with_capacity(scored.len());
        for (score, mut d) in scored {
            set_meta(&mut d, "vectorSearchScore", Bson::Double(score));
            out.push(d);
        }
        Ok(docs_iter(out))
    }))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MemCtx {
        colls: Mutex<HashMap<String, Vec<Document>>>,
        temp: PathBuf,
    }

    fn ns(db: &str, coll: &str) -> String {
        format!("{db}.{coll}")
    }

    impl MemCtx {
        fn get(&self, db: &str, coll: &str) -> Vec<Document> {
            self.colls.lock().unwrap().get(&ns(db, coll)).cloned().unwrap_or_default()
        }
    }

    impl AggContext for MemCtx {
        fn scan(&self, db: &str, coll: &str) -> Result<DocIter> {
            Ok(docs_iter(self.get(db, coll)))
        }

        fn find(&self, db: &str, coll: &str, filter: &Document) -> Result<Vec<Document>> {
            let m = Matcher::new(filter)?;
            let mut out = Vec::new();
            for d in self.get(db, coll) {
                if m.matches(&d)? {
                    out.push(d);
                }
            }
            Ok(out)
        }

        fn replace_collection(&self, db: &str, coll: &str, docs: Vec<Document>) -> Result<()> {
            self.colls.lock().unwrap().insert(ns(db, coll), docs);
            Ok(())
        }

        fn insert(&self, db: &str, coll: &str, doc: Document) -> Result<()> {
            let mut g = self.colls.lock().unwrap();
            let v = g.entry(ns(db, coll)).or_default();
            if let Some(id) = doc.get("_id") {
                if v.iter().any(|d| d.get("_id") == Some(id)) {
                    return Err(Error::location(11000, "E11000 duplicate key error"));
                }
            }
            v.push(doc);
            Ok(())
        }

        fn replace_by_id(&self, db: &str, coll: &str, id: &Bson, doc: Document) -> Result<()> {
            let mut g = self.colls.lock().unwrap();
            let v = g.entry(ns(db, coll)).or_default();
            match v.iter_mut().find(|d| d.get("_id") == Some(id)) {
                Some(slot) => {
                    *slot = doc;
                    Ok(())
                }
                None => Err(Error::bad_value("no document with that _id")),
            }
        }

        fn coll_stats(&self, db: &str, coll: &str, _spec: &Document) -> Result<Document> {
            Ok(doc! {"ns": ns(db, coll)})
        }

        fn index_stats(&self, _db: &str, _coll: &str) -> Result<Vec<Document>> {
            Ok(Vec::new())
        }

        fn temp_dir(&self) -> PathBuf {
            self.temp.clone()
        }
    }

    fn ctx(dir: &str) -> Arc<MemCtx> {
        Arc::new(MemCtx { colls: Mutex::new(HashMap::new()), temp: std::env::temp_dir().join(dir) })
    }

    fn put(c: &MemCtx, coll: &str, docs: Vec<Document>) {
        c.colls.lock().unwrap().insert(ns("test", coll), docs);
    }

    fn opts(c: &Arc<MemCtx>, max: usize, allow: bool) -> Arc<AggOptions> {
        Arc::new(AggOptions {
            db: "test".into(),
            coll: Some("src".into()),
            allow_disk_use: allow,
            max_memory_bytes: max,
            vars: Vars::new(),
            ctx: c.clone(),
        })
    }

    fn run(input: Vec<Document>, stages: Vec<Document>, o: Arc<AggOptions>) -> Result<Vec<Document>> {
        run_pipeline(docs_iter(input), &stages, o)?.collect()
    }

    fn ids(docs: &[Document]) -> Vec<String> {
        docs.iter()
            .map(|d| match d.get("_id") {
                Some(Bson::String(s)) => s.clone(),
                Some(Bson::Int32(i)) => i.to_string(),
                other => format!("{other:?}"),
            })
            .collect()
    }

    fn arr_docs(d: &Document, key: &str) -> Vec<Document> {
        match d.get(key) {
            Some(Bson::Array(v)) => v
                .iter()
                .filter_map(|x| if let Bson::Document(m) = x { Some(m.clone()) } else { None })
                .collect(),
            _ => panic!("missing array {key}"),
        }
    }

    #[test]
    fn match_sort_limit_add_fields() {
        let c = ctx("mogo-pipeline-t1");
        let input = vec![doc! {"_id": 1, "a": 3}, doc! {"_id": 2, "a": 1}, doc! {"_id": 3, "a": 2}, doc! {"_id": 4, "a": 5}];
        let stages = vec![
            doc! {"$match": {"a": {"$gt": 1}}},
            doc! {"$sort": {"a": -1}},
            doc! {"$limit": 2},
            doc! {"$addFields": {"c": {"$add": ["$a", 1]}}},
        ];
        let out = run(input, stages, opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(ids(&out), vec!["4", "1"]);
        assert_eq!(num_f64(out[0].get("c").unwrap()), Some(6.0));
    }

    #[test]
    fn unwind_options() {
        let c = ctx("mogo-pipeline-t2");
        let input = vec![
            doc! {"_id": 1, "t": ["a", "b"]},
            doc! {"_id": 2, "t": []},
            doc! {"_id": 3},
            doc! {"_id": 4, "t": "s"},
            doc! {"_id": 5, "t": Bson::Null},
        ];
        let plain = run(input.clone(), vec![doc! {"$unwind": "$t"}], opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(ids(&plain), vec!["1", "1", "4"]);
        let kept = run(
            input,
            vec![doc! {"$unwind": {"path": "$t", "includeArrayIndex": "i", "preserveNullAndEmptyArrays": true}}],
            opts(&c, 1 << 20, false),
        )
        .unwrap();
        assert_eq!(ids(&kept), vec!["1", "1", "2", "3", "4", "5"]);
        assert_eq!(kept[0].get("i"), Some(&Bson::Int64(0)));
        assert_eq!(kept[2].get("i"), Some(&Bson::Null));
        assert!(kept[2].get("t").is_none());
    }

    #[test]
    fn lookup_equality_matches_arrays_and_null() {
        let c = ctx("mogo-pipeline-t3");
        put(
            &c,
            "f",
            vec![doc! {"_id": "a", "k": 1}, doc! {"_id": "b", "k": 2}, doc! {"_id": "c"}, doc! {"_id": "d", "k": [2, 3]}],
        );
        let input = vec![doc! {"_id": 1, "fk": [1, 2]}, doc! {"_id": 2, "fk": Bson::Null}, doc! {"_id": 3}];
        let stages = vec![doc! {"$lookup": {"from": "f", "localField": "fk", "foreignField": "k", "as": "m"}}];
        let out = run(input, stages, opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(ids(&arr_docs(&out[0], "m")), vec!["a", "b", "d"]);
        assert_eq!(ids(&arr_docs(&out[1], "m")), vec!["c"]);
        assert_eq!(ids(&arr_docs(&out[2], "m")), vec!["c"]);
    }

    #[test]
    fn lookup_pipeline_with_let_and_expr() {
        let c = ctx("mogo-pipeline-t4");
        put(&c, "f", vec![doc! {"k": 1, "v": "a"}, doc! {"k": 2, "v": "b"}]);
        let stages = vec![doc! {"$lookup": {
            "from": "f",
            "let": {"lx": "$x"},
            "pipeline": [{"$match": {"$expr": {"$eq": ["$k", "$$lx"]}}}],
            "as": "m"
        }}];
        let out = run(vec![doc! {"_id": 1, "x": 2}], stages, opts(&c, 1 << 20, false)).unwrap();
        let m = arr_docs(&out[0], "m");
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].get_str("v").unwrap(), "b");
    }

    #[test]
    fn facet_runs_each_branch() {
        let c = ctx("mogo-pipeline-t5");
        let input = vec![doc! {"a": 1}, doc! {"a": 2}, doc! {"a": 3}];
        let stages = vec![doc! {"$facet": {"even": [{"$match": {"a": 2}}], "all": [{"$count": "n"}]}}];
        let out = run(input, stages, opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(arr_docs(&out[0], "even").len(), 1);
        let all = arr_docs(&out[0], "all");
        assert_eq!(num_i64(all[0].get("n").unwrap()), Some(3));
    }

    #[test]
    fn external_sort_spills_and_cleans_up() {
        let c = ctx("mogo-pipeline-t6");
        let _ = std::fs::remove_dir_all(&c.temp);
        let input: Vec<Document> = (0..200)
            .map(|i| doc! {"a": (i * 37) % 200, "pad": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"})
            .collect();
        let out = run(input.clone(), vec![doc! {"$sort": {"a": 1}}], opts(&c, 4096, true)).unwrap();
        let vals: Vec<i64> = out.iter().map(|d| num_i64(d.get("a").unwrap()).unwrap()).collect();
        assert_eq!(vals, (0..200).collect::<Vec<i64>>());
        let leftover = std::fs::read_dir(&c.temp).map(|rd| rd.count()).unwrap_or(0);
        assert_eq!(leftover, 0);

        let err = run(input.clone(), vec![doc! {"$sort": {"a": 1}}], opts(&c, 4096, false)).unwrap_err();
        assert_eq!(err.code, 292);

        let top = run(input, vec![doc! {"$sort": {"a": -1}}, doc! {"$limit": 3}], opts(&c, 1 << 20, false)).unwrap();
        let vals: Vec<i64> = top.iter().map(|d| num_i64(d.get("a").unwrap()).unwrap()).collect();
        assert_eq!(vals, vec![199, 198, 197]);
    }

    #[test]
    fn merge_modes() {
        let c = ctx("mogo-pipeline-t7");
        put(&c, "t", vec![doc! {"_id": 1, "v": "old", "keep": true}]);
        let input = vec![doc! {"_id": 1, "v": "new"}, doc! {"_id": 2, "v": "x"}];
        let out = run(
            input.clone(),
            vec![doc! {"$merge": {"into": "t", "whenMatched": "merge", "whenNotMatched": "insert"}}],
            opts(&c, 1 << 20, false),
        )
        .unwrap();
        assert!(out.is_empty());
        let t = c.get("test", "t");
        assert_eq!(t.len(), 2);
        assert_eq!(t[0].get_str("v").unwrap(), "new");
        assert!(t[0].get_bool("keep").unwrap());

        run(
            input.clone(),
            vec![doc! {"$merge": {"into": "t", "whenMatched": "replace"}}],
            opts(&c, 1 << 20, false),
        )
        .unwrap();
        let t = c.get("test", "t");
        assert!(t[0].get("keep").is_none());

        let err = run(
            input,
            vec![doc! {"$merge": {"into": "t", "whenMatched": "fail"}}],
            opts(&c, 1 << 20, false),
        )
        .unwrap_err();
        assert_eq!(err.code, 11000);
    }

    #[test]
    fn out_replaces_collection() {
        let c = ctx("mogo-pipeline-t8");
        let input = vec![doc! {"_id": 1}, doc! {"_id": 2}];
        let out = run(input, vec![doc! {"$out": "o"}], opts(&c, 1 << 20, false)).unwrap();
        assert!(out.is_empty());
        assert_eq!(c.get("test", "o").len(), 2);
    }

    #[test]
    fn graph_lookup_depth() {
        let c = ctx("mogo-pipeline-t9");
        put(
            &c,
            "g",
            vec![
                doc! {"name": "a", "boss": Bson::Null},
                doc! {"name": "b", "boss": "a"},
                doc! {"name": "c", "boss": "b"},
            ],
        );
        let input = vec![doc! {"_id": 1, "who": "a"}];
        let stages = vec![doc! {"$graphLookup": {
            "from": "g",
            "startWith": "$who",
            "connectFromField": "name",
            "connectToField": "boss",
            "as": "reports",
            "depthField": "depth"
        }}];
        let out = run(input.clone(), stages, opts(&c, 1 << 20, false)).unwrap();
        let reports = arr_docs(&out[0], "reports");
        let names: Vec<String> = reports.iter().map(|d| d.get_str("name").unwrap().to_string()).collect();
        assert_eq!(names, vec!["b", "c"]);
        assert_eq!(num_i64(reports[1].get("depth").unwrap()), Some(1));

        let limited = vec![doc! {"$graphLookup": {
            "from": "g",
            "startWith": "$who",
            "connectFromField": "name",
            "connectToField": "boss",
            "as": "reports",
            "maxDepth": 0
        }}];
        let out = run(input, limited, opts(&c, 1 << 20, false)).unwrap();
        let reports = arr_docs(&out[0], "reports");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].get_str("name").unwrap(), "b");
    }

    #[test]
    fn redact_prunes_and_descends() {
        let c = ctx("mogo-pipeline-t10");
        let input = vec![doc! {"_id": 1, "level": 1, "s": {"level": 5, "v": "hidden"}, "t": {"level": 1, "v": "shown"}}];
        let stages = vec![doc! {"$redact": {"$cond": {
            "if": {"$lte": ["$level", 3]},
            "then": "$$DESCEND",
            "else": "$$PRUNE"
        }}}];
        let out = run(input, stages, opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].get("s").is_none());
        let t = out[0].get_document("t").unwrap();
        assert_eq!(t.get_str("v").unwrap(), "shown");
    }

    #[test]
    fn count_and_sort_by_count() {
        let c = ctx("mogo-pipeline-t11");
        let input = vec![doc! {"a": "x"}, doc! {"a": "y"}, doc! {"a": "x"}, doc! {"a": "x"}];
        let out = run(input.clone(), vec![doc! {"$sortByCount": "$a"}], opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(ids(&out), vec!["x", "y"]);
        assert_eq!(num_i64(out[0].get("count").unwrap()), Some(3));

        let out = run(input.clone(), vec![doc! {"$count": "n"}], opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(num_i64(out[0].get("n").unwrap()), Some(4));

        let out = run(vec![], vec![doc! {"$count": "n"}], opts(&c, 1 << 20, false)).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn union_with_appends_collection() {
        let c = ctx("mogo-pipeline-t12");
        put(&c, "u", vec![doc! {"_id": 9}]);
        let out = run(vec![doc! {"_id": 1}], vec![doc! {"$unionWith": "u"}], opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(ids(&out), vec!["1", "9"]);
    }

    #[test]
    fn geo_near_orders_by_distance() {
        let c = ctx("mogo-pipeline-t13");
        let input = vec![doc! {"_id": "far", "loc": [10.0, 10.0]}, doc! {"_id": "near", "loc": [0.1, 0.0]}];
        let stages = vec![doc! {"$geoNear": {"near": [0.0, 0.0], "distanceField": "dist"}}];
        let out = run(input, stages, opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(ids(&out), vec!["near", "far"]);
        assert!(num_f64(out[0].get("dist").unwrap()).unwrap() < 1.0);
    }

    #[test]
    fn vector_search_orders_by_similarity() {
        let c = ctx("mogo-pipeline-t14");
        let input = vec![
            doc! {"_id": "a", "emb": [1.0, 0.0]},
            doc! {"_id": "b", "emb": [0.0, 1.0]},
            doc! {"_id": "c", "emb": [1.0, 1.0]},
        ];
        let stages = vec![doc! {"$vectorSearch": {"path": "emb", "queryVector": [1.0, 0.0], "limit": 2}}];
        let out = run(input, stages, opts(&c, 1 << 20, false)).unwrap();
        assert_eq!(ids(&out), vec!["a", "c"]);
        let meta = out[0].get_document(META_KEY).unwrap();
        assert!((meta.get_f64("vectorSearchScore").unwrap() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn stage_validation_errors() {
        let c = ctx("mogo-pipeline-t15");
        let o = || opts(&c, 1 << 20, false);
        assert_eq!(run(vec![], vec![doc! {"$foo": 1}], o()).unwrap_err().code, 40324);
        assert_eq!(run(vec![], vec![doc! {"$match": {}, "$limit": 1}], o()).unwrap_err().code, 40323);
        assert_eq!(run(vec![], vec![doc! {"$out": "x"}, doc! {"$limit": 1}], o()).unwrap_err().code, 40601);
        assert_eq!(run(vec![], vec![doc! {"$facet": {"x": [{"$out": "y"}]}}], o()).unwrap_err().code, 40600);
        assert_eq!(run(vec![], vec![doc! {"$limit": 0}], o()).unwrap_err().code, 15958);
    }

    #[test]
    fn apply_update_stages() {
        let vars = Vars::new();
        let out = apply_simple_stages(
            doc! {"a": 1, "b": 2},
            &[doc! {"$set": {"c": 3}}, doc! {"$unset": "a"}],
            &vars,
        )
        .unwrap();
        assert!(out.get("a").is_none());
        assert_eq!(num_i64(out.get("c").unwrap()), Some(3));
        assert!(out.contains_key("b"));
        let err = apply_simple_stages(doc! {}, &[doc! {"$match": {}}], &vars).unwrap_err();
        assert_eq!(err.code, 72);
    }
}
