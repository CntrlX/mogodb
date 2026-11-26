//! Atlas Search emulation: `$search`, `$searchMeta`, `$listSearchIndexes`, `$rankFusion`
//! and the search-index commands (`createSearchIndexes`, `updateSearchIndex`,
//! `dropSearchIndex`).
//!
//! Index definitions live in server metadata under `search\0<db.coll>`. Queries run
//! in-process: each query tokenizes the scanned documents with the index analyzers and
//! derives BM25 statistics (document count, average length, document frequency) from them,
//! so no inverted index is persisted.

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use bson::{doc, oid::ObjectId, Bson, Document};
use regex::Regex;

use crate::agg::pipeline::{run_pipeline, AggOptions, DocIter};
use crate::bsonutil::{bson_eq, cmp_bson, get_path, is_number, num_f64, num_i64, to_bytes, type_name, META_KEY};
use crate::error::{Error, Result};
use crate::server::CmdCtx;
use crate::storage::Store;

const K1: f64 = 1.2;
const B: f64 = 0.75;
const RRF_K: f64 = 60.0;
const SCORE_KEY: &str = "$searchScore";
const DEFAULT_INDEX: &str = "default";
const DEFAULT_BUCKETS: usize = 10;
const OPERATORS: &[&str] = &[
    "text",
    "phrase",
    "autocomplete",
    "wildcard",
    "regex",
    "compound",
    "equals",
    "range",
    "exists",
    "in",
    "near",
];
const RANKED_START: &[&str] = &["$search", "$vectorSearch", "$geoNear"];
const RANK_ALLOWED: &[&str] = &["$search", "$vectorSearch", "$match", "$sort", "$limit", "$skip", "$sample"];
const RANK_DESCRIPTION: &str = "value output by reciprocal rank fusion algorithm, computed as sum of (weight * (1 / (60 + rank))) across input pipelines from which this document is output, from:";
const RANK_NOT_RANKED: &str = "Pipeline did not begin with a ranked stage and did not contain an explicit $sort stage. All input pipelines to the $rankFusion stage must begin with one of $search, $vectorSearch, $geoNear, or have a $sort in the pipeline.";
const RANK_EMPTY: &str = "$rankFusion input pipeline cannot be empty. All input pipelines to the $rankFusion stage must begin with one of $search, $vectorSearch, $geoNear, or have a $sort in the pipeline.";
const RANK_NESTED: &str = "$rankFusion input pipeline has a nested hybrid search stage ($rankFusion/$scoreFusion). All input pipelines to the $rankFusion stage must begin with one of $search, $vectorSearch, $geoNear, or have a $sort in the pipeline.";
const RANK_NAME_PREFIX: &str = "$rankFusion pipeline names must follow the naming rules of field path expressions. :: caused by :: ";
const RANK_PARSE_PREFIX: &str = "Error parsing $rankFusion/$scoreFusion.input.pipelines :: caused by :: ";

/// Mirrors the aggregation stage names known to `agg::pipeline`.
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

const ENGLISH_STOP: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of",
    "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with",
];

// ---------------------------------------------------------------------------
// Stage entry points
// ---------------------------------------------------------------------------

/// Build one of the search stages. `input` is the full collection scan (these stages are first-only).
pub fn build_stage(input: DocIter, name: &str, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    match name {
        "$search" => build_search(input, arg, opts),
        "$searchMeta" => build_search_meta(input, arg, opts),
        "$listSearchIndexes" => build_list_indexes(input, arg, opts),
        "$rankFusion" => build_rank_fusion(input, arg, opts),
        _ => Err(Error::new(40324, format!("Unrecognized pipeline stage name: '{name}'"))),
    }
}

/// Search index definitions of `db.coll` in `$listSearchIndexes` shape.
/// Aggregating a view with a leading `$rankFusion`: the view pipeline runs inside every
/// input pipeline instead of before the stage.
pub fn with_view_prefix(stage: &Document, prefix: &[Document]) -> Document {
    let mut st = stage.clone();
    let name = if st.contains_key("$rankFusion") { "$rankFusion" } else { "$scoreFusion" };
    if let Ok(spec) = st.get_document_mut(name) {
        if let Ok(input) = spec.get_document_mut("input") {
            if let Ok(ps) = input.get_document_mut("pipelines") {
                for (_, p) in ps.iter_mut() {
                    if let Bson::Array(a) = p {
                        let mut n: Vec<Bson> = prefix.iter().cloned().map(Bson::Document).collect();
                        n.append(a);
                        *a = n;
                    }
                }
            }
        }
    }
    st
}

pub fn stored_indexes(store: &Store, db: &str, coll: &str) -> Result<Vec<Document>> {
    load_indexes(store, &format!("{db}.{coll}"))
}

/// Commands `createSearchIndexes`, `updateSearchIndex`, `dropSearchIndex`.
pub fn route_cmd(ctx: &mut CmdCtx) -> Result<Document> {
    let coll = ctx.coll_name()?;
    let ns = ctx.ns(&coll);
    match ctx.name.as_str() {
        "createSearchIndexes" => create_indexes(ctx, &ns),
        "updateSearchIndex" => update_index(ctx, &ns),
        "dropSearchIndex" => drop_index(ctx, &ns),
        other => Err(Error::new(59, format!("no such command: '{other}'"))),
    }
}

// ---------------------------------------------------------------------------
// Index definitions and persistence
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct FieldCfg {
    kind: String,
    analyzer: String,
    search_analyzer: String,
}

#[derive(Clone)]
struct IndexCfg {
    dynamic: bool,
    analyzer: String,
    search_analyzer: String,
    fields: BTreeMap<String, FieldCfg>,
}

impl IndexCfg {
    /// Parse and validate a `definition` document.
    fn parse(def: &Document) -> Result<IndexCfg> {
        let analyzer = opt_str(def, "analyzer", "definition")?.unwrap_or("lucene.standard").to_string();
        let search_analyzer = opt_str(def, "searchAnalyzer", "definition")?
            .map(String::from)
            .unwrap_or_else(|| analyzer.clone());
        check_analyzer(&analyzer)?;
        check_analyzer(&search_analyzer)?;
        let mappings = match def.get("mappings") {
            None => {
                return Ok(IndexCfg { dynamic: false, analyzer, search_analyzer, fields: BTreeMap::new() });
            }
            Some(Bson::Document(m)) => m,
            Some(o) => return Err(wrong_type("definition.mappings", o, "object")),
        };
        let dynamic = match mappings.get("dynamic") {
            None => false,
            Some(Bson::Boolean(b)) => *b,
            Some(o) => return Err(wrong_type("definition.mappings.dynamic", o, "bool")),
        };
        let mut fields = BTreeMap::new();
        match mappings.get("fields") {
            None => {}
            Some(Bson::Document(f)) => add_fields(&mut fields, "", f, &analyzer, &search_analyzer)?,
            Some(o) => return Err(wrong_type("definition.mappings.fields", o, "object")),
        }
        Ok(IndexCfg { dynamic, analyzer, search_analyzer, fields })
    }

    /// Field configuration for `path`; `None` when the path is not indexed.
    /// Analyzer used to index and to query `path`; `None` when the path is not text-indexed.
    fn analyzed(&self, path: &str) -> Option<(String, String)> {
        let f = self.field(path)?;
        match f.kind.as_str() {
            "string" | "autocomplete" => Some((f.analyzer, f.search_analyzer)),
            "token" => Some(("lucene.keyword".into(), "lucene.keyword".into())),
            _ => None,
        }
    }

    fn field(&self, path: &str) -> Option<FieldCfg> {
        if let Some(f) = self.fields.get(path) {
            return Some(f.clone());
        }
        if self.dynamic {
            return Some(FieldCfg {
                kind: "string".into(),
                analyzer: self.analyzer.clone(),
                search_analyzer: self.search_analyzer.clone(),
            });
        }
        None
    }
}

fn add_fields(
    out: &mut BTreeMap<String, FieldCfg>,
    prefix: &str,
    fields: &Document,
    an: &str,
    san: &str,
) -> Result<()> {
    for (name, spec) in fields {
        let path = if prefix.is_empty() { name.clone() } else { format!("{prefix}.{name}") };
        let Bson::Document(sd) = spec else {
            return Err(wrong_type(&format!("definition.mappings.fields.{path}"), spec, "object"));
        };
        let kind = match sd.get("type") {
            Some(Bson::String(k)) => k.as_str(),
            _ => return Err(Error::bad_value(format!("field '{path}' requires a string 'type'"))),
        };
        match kind {
            "document" => {
                if let Some(Bson::Document(sub)) = sd.get("fields") {
                    add_fields(out, &path, sub, an, san)?;
                }
            }
            "string" | "autocomplete" | "token" | "number" | "date" | "boolean" | "objectId" => {
                let (analyzer, search_analyzer) = match opt_str(sd, "analyzer", &path)? {
                    Some(a) => {
                        let sa = opt_str(sd, "searchAnalyzer", &path)?.unwrap_or(a).to_string();
                        (a.to_string(), sa)
                    }
                    None => {
                        let sa = opt_str(sd, "searchAnalyzer", &path)?.map(String::from).unwrap_or_else(|| san.to_string());
                        (an.to_string(), sa)
                    }
                };
                check_analyzer(&analyzer)?;
                check_analyzer(&search_analyzer)?;
                out.insert(path, FieldCfg { kind: kind.to_string(), analyzer, search_analyzer });
            }
            other => return Err(Error::bad_value(format!("unsupported field type '{other}' for '{path}'"))),
        }
    }
    Ok(())
}

fn check_analyzer(name: &str) -> Result<()> {
    analyze(name, "").map(|_| ())
}

fn search_key(ns: &str) -> String {
    format!("search\0{ns}")
}

fn load_indexes(store: &Store, ns: &str) -> Result<Vec<Document>> {
    let Some(doc) = store.get_meta(&search_key(ns))? else {
        return Ok(Vec::new());
    };
    Ok(match doc.get("indexes") {
        Some(Bson::Array(items)) => items
            .iter()
            .filter_map(|x| match x {
                Bson::Document(d) => Some(d.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    })
}

fn save_indexes(store: &Store, ns: &str, list: Vec<Document>) -> Result<()> {
    if list.is_empty() {
        return store.put_meta(&search_key(ns), None);
    }
    let mut d = Document::new();
    d.insert("indexes", Bson::Array(list.into_iter().map(Bson::Document).collect()));
    store.put_meta(&search_key(ns), Some(&d))
}

fn index_entry(id: &str, name: &str, definition: Document) -> Document {
    doc! {
        "id": id,
        "name": name,
        "status": "READY",
        "queryable": true,
        "latestDefinition": definition,
    }
}

fn entry_name(e: &Document) -> Option<&str> {
    match e.get("name") {
        Some(Bson::String(s)) => Some(s.as_str()),
        _ => None,
    }
}

fn entry_id(e: &Document) -> Option<&str> {
    match e.get("id") {
        Some(Bson::String(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Position of the entry matching `name` (preferred) or `id`.
fn position(list: &[Document], name: Option<&str>, id: Option<&str>) -> Option<usize> {
    list.iter().position(|e| match (name, id) {
        (Some(n), _) => entry_name(e) == Some(n),
        (None, Some(i)) => entry_id(e) == Some(i),
        (None, None) => false,
    })
}

fn selector_fields(cmd: &Document) -> Result<(Option<String>, Option<String>)> {
    let name = match cmd.get("name") {
        None => None,
        Some(Bson::String(s)) => Some(s.clone()),
        Some(o) => return Err(wrong_type("name", o, "string")),
    };
    let id = match cmd.get("id") {
        None => None,
        Some(Bson::String(s)) => Some(s.clone()),
        Some(o) => return Err(wrong_type("id", o, "string")),
    };
    Ok((name, id))
}

fn not_found() -> Error {
    Error::new(27, "Search index not found")
}

fn create_indexes(ctx: &CmdCtx, ns: &str) -> Result<Document> {
    let items = match ctx.cmd.get("indexes") {
        Some(Bson::Array(a)) => a,
        Some(o) => return Err(wrong_type("createSearchIndexes.indexes", o, "array")),
        None => return Err(missing("createSearchIndexes.indexes")),
    };
    let mut list = load_indexes(ctx.store(), ns)?;
    let mut created = Vec::new();
    for item in items {
        let Bson::Document(ix) = item else {
            return Err(wrong_type("createSearchIndexes.indexes", item, "object"));
        };
        let name = match ix.get("name") {
            None => DEFAULT_INDEX.to_string(),
            Some(Bson::String(s)) => s.clone(),
            Some(o) => return Err(wrong_type("createSearchIndexes.indexes.name", o, "string")),
        };
        let def = match ix.get("definition") {
            Some(Bson::Document(d)) => d,
            Some(o) => return Err(wrong_type("createSearchIndexes.indexes.definition", o, "object")),
            None => return Err(missing("createSearchIndexes.indexes.definition")),
        };
        IndexCfg::parse(def)?;
        if list.iter().any(|e| entry_name(e) == Some(name.as_str())) {
            return Err(Error::new(68, format!("Index with name: {name} already exists")));
        }
        let id = ObjectId::new().to_hex();
        created.push(Bson::Document(doc! {"id": id.clone(), "name": name.clone()}));
        list.push(index_entry(&id, &name, def.clone()));
    }
    save_indexes(ctx.store(), ns, list)?;
    Ok(doc! {"indexesCreated": Bson::Array(created), "ok": 1.0})
}

fn update_index(ctx: &CmdCtx, ns: &str) -> Result<Document> {
    let def = match ctx.cmd.get("definition") {
        Some(Bson::Document(d)) => d,
        Some(o) => return Err(wrong_type("updateSearchIndex.definition", o, "object")),
        None => return Err(missing("updateSearchIndex.definition")),
    };
    IndexCfg::parse(def)?;
    let (name, id) = selector_fields(&ctx.cmd)?;
    if name.is_none() && id.is_none() {
        return Err(Error::bad_value("either id or name must be specified"));
    }
    let mut list = load_indexes(ctx.store(), ns)?;
    let pos = position(&list, name.as_deref(), id.as_deref()).ok_or_else(not_found)?;
    list[pos].insert("latestDefinition", def.clone());
    save_indexes(ctx.store(), ns, list)?;
    Ok(doc! {"ok": 1.0})
}

fn drop_index(ctx: &CmdCtx, ns: &str) -> Result<Document> {
    let (name, id) = selector_fields(&ctx.cmd)?;
    if name.is_none() && id.is_none() {
        return Err(Error::bad_value("either id or name must be specified"));
    }
    let mut list = load_indexes(ctx.store(), ns)?;
    let pos = position(&list, name.as_deref(), id.as_deref()).ok_or_else(not_found)?;
    list.remove(pos);
    save_indexes(ctx.store(), ns, list)?;
    Ok(doc! {"ok": 1.0})
}

fn load_index(opts: &AggOptions, index: &str) -> Result<Option<IndexCfg>> {
    let coll = coll_of(opts);
    for d in opts.ctx.search_indexes(&opts.db, &coll)? {
        if matches!(d.get("name"), Some(Bson::String(n)) if n == index) {
            let def = match d.get("latestDefinition") {
                Some(Bson::Document(def)) => def.clone(),
                _ => Document::new(),
            };
            return IndexCfg::parse(&def).map(Some);
        }
    }
    Ok(None)
}

fn coll_of(opts: &AggOptions) -> String {
    opts.coll.clone().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

fn missing(field: &str) -> Error {
    Error::new(40414, format!("BSON field '{field}' is missing but a required field"))
}

fn unknown_field(field: &str) -> Error {
    Error::new(40415, format!("BSON field '{field}' is an unknown field."))
}

fn wrong_type(field: &str, v: &Bson, expected: &str) -> Error {
    Error::type_mismatch(format!(
        "BSON field '{field}' is the wrong type '{}', expected type '{expected}'",
        type_name(v)
    ))
}

fn opt_str<'a>(d: &'a Document, key: &str, what: &str) -> Result<Option<&'a str>> {
    match d.get(key) {
        None => Ok(None),
        Some(Bson::String(s)) => Ok(Some(s.as_str())),
        Some(o) => Err(wrong_type(&format!("{what}.{key}"), o, "string")),
    }
}

fn str_value<'a>(stage: &str, key: &str, v: &'a Bson) -> Result<&'a str> {
    match v {
        Bson::String(s) => Ok(s.as_str()),
        _ => Err(Error::bad_value(format!("{stage} '{key}' must be a string"))),
    }
}

fn doc_value<'a>(stage: &str, key: &str, v: &'a Bson) -> Result<&'a Document> {
    match v {
        Bson::Document(d) => Ok(d),
        _ => Err(Error::bad_value(format!("{stage} '{key}' must be an object"))),
    }
}

fn required<'a>(spec: &'a Document, key: &str, op: &str) -> Result<&'a Bson> {
    spec.get(key).ok_or_else(|| Error::bad_value(format!("{op} requires a '{key}' field")))
}

fn spec_doc<'a>(op: &str, v: &'a Bson) -> Result<&'a Document> {
    match v {
        Bson::Document(d) => Ok(d),
        _ => Err(Error::bad_value(format!("operator '{op}' must be an object"))),
    }
}

fn single_query(op: &str, spec: &Document) -> Result<String> {
    match required(spec, "query", op)? {
        Bson::String(s) => Ok(s.clone()),
        _ => Err(Error::bad_value(format!("{op} query must be a string"))),
    }
}

fn query_list(spec: &Document) -> Result<Vec<String>> {
    match required(spec, "query", "text")? {
        Bson::String(s) => Ok(vec![s.clone()]),
        Bson::Array(items) => items
            .iter()
            .map(|x| match x {
                Bson::String(s) => Ok(s.clone()),
                _ => Err(Error::bad_value("text query array must contain only strings")),
            })
            .collect(),
        _ => Err(Error::bad_value("text query must be a string or array of strings")),
    }
}

fn match_all(spec: &Document) -> Result<bool> {
    match spec.get("matchCriteria") {
        None => Ok(false),
        Some(Bson::String(s)) if s == "any" => Ok(false),
        Some(Bson::String(s)) if s == "all" => Ok(true),
        Some(_) => Err(Error::bad_value("matchCriteria must be 'any' or 'all'")),
    }
}

fn score_number(b: &Bson) -> Result<f64> {
    match b {
        Bson::Document(d) => d
            .get("value")
            .and_then(num_f64)
            .ok_or_else(|| Error::bad_value("score modifier requires a numeric 'value'")),
        _ => Err(Error::bad_value("score modifier must be an object")),
    }
}

/// Apply a `score: {boost: {value}}` or `score: {constant: {value}}` modifier from an operator value.
fn apply_score(v: &Bson, score: f64) -> Result<f64> {
    let Bson::Document(spec) = v else {
        return Ok(score);
    };
    let Some(modifier) = spec.get("score") else {
        return Ok(score);
    };
    let Bson::Document(m) = modifier else {
        return Err(Error::bad_value("score must be an object"));
    };
    if let Some(b) = m.get("boost") {
        return Ok(score * score_number(b)?);
    }
    if let Some(c) = m.get("constant") {
        return score_number(c);
    }
    Err(Error::bad_value("score must specify 'boost' or 'constant'"))
}

// ---------------------------------------------------------------------------
// Analyzers
// ---------------------------------------------------------------------------

/// Tokenize `text` with a Lucene analyzer name.
fn analyze(analyzer: &str, text: &str) -> Result<Vec<String>> {
    Ok(match analyzer {
        "lucene.standard" => text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase)
            .collect(),
        "lucene.simple" => text
            .split(|c: char| !c.is_alphabetic())
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase)
            .collect(),
        "lucene.whitespace" => text.split_whitespace().map(str::to_string).collect(),
        "lucene.keyword" => {
            if text.is_empty() {
                Vec::new()
            } else {
                vec![text.to_string()]
            }
        }
        "lucene.english" => text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .map(str::to_lowercase)
            .filter(|s| !ENGLISH_STOP.contains(&s.as_str()))
            .collect(),
        other => return Err(Error::bad_value(format!("Unknown analyzer: {other}"))),
    })
}

// ---------------------------------------------------------------------------
// Document field access
// ---------------------------------------------------------------------------

/// All values at a dotted path, fanning out through arrays (leaves are flattened).
fn values_at<'a>(doc: &'a Document, path: &str) -> Vec<&'a Bson> {
    let segs: Vec<&str> = path.split('.').collect();
    let mut out = Vec::new();
    walk_doc(doc, &segs, &mut out);
    out
}

fn walk_doc<'a>(doc: &'a Document, segs: &[&str], out: &mut Vec<&'a Bson>) {
    let Some(v) = doc.get(segs[0]) else {
        return;
    };
    if segs.len() == 1 {
        push_leaves(v, out);
    } else {
        walk(v, &segs[1..], out);
    }
}

fn walk<'a>(v: &'a Bson, segs: &[&str], out: &mut Vec<&'a Bson>) {
    match v {
        Bson::Array(a) => {
            for x in a {
                walk(x, segs, out);
            }
        }
        Bson::Document(d) => walk_doc(d, segs, out),
        _ => {}
    }
}

fn push_leaves<'a>(v: &'a Bson, out: &mut Vec<&'a Bson>) {
    match v {
        Bson::Array(a) => {
            for x in a {
                push_leaves(x, out);
            }
        }
        _ => out.push(v),
    }
}

fn string_leaves<'a>(doc: &'a Document, path: &str) -> Vec<&'a str> {
    values_at(doc, path)
        .into_iter()
        .filter_map(|v| match v {
            Bson::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect()
}

fn collect_paths(doc: &Document, prefix: &str, set: &mut BTreeSet<String>) {
    for (k, v) in doc {
        let p = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        set.insert(p.clone());
        collect_value_paths(v, &p, set);
    }
}

fn collect_value_paths(v: &Bson, p: &str, set: &mut BTreeSet<String>) {
    match v {
        Bson::Document(d) => collect_paths(d, p, set),
        Bson::Array(a) => {
            for x in a {
                collect_value_paths(x, p, set);
            }
        }
        _ => {}
    }
}

fn as_number(b: &Bson) -> Option<f64> {
    match b {
        Bson::DateTime(dt) => Some(dt.timestamp_millis() as f64),
        other => num_f64(other),
    }
}

fn same_class(a: &Bson, b: &Bson) -> bool {
    (is_number(a) && is_number(b)) || std::mem::discriminant(a) == std::mem::discriminant(b)
}

fn in_bounds(x: &Bson, bounds: &[(&str, &Bson)]) -> bool {
    bounds.iter().all(|&(op, b)| {
        if !same_class(x, b) {
            return false;
        }
        let o = cmp_bson(x, b);
        match op {
            "gt" => o == Ordering::Greater,
            "gte" => o != Ordering::Less,
            "lt" => o == Ordering::Less,
            _ => o != Ordering::Greater,
        }
    })
}

// ---------------------------------------------------------------------------
// Glob / regex
// ---------------------------------------------------------------------------

fn glob_regex(pat: &str) -> Result<Regex> {
    let mut re = String::from("^");
    for c in pat.chars() {
        match c {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            _ => re.push_str(&regex::escape(&c.to_string())),
        }
    }
    re.push('$');
    Regex::new(&re).map_err(|e| Error::bad_value(format!("invalid wildcard pattern '{pat}': {e}")))
}

fn anchored_regex(pat: &str) -> Result<Regex> {
    Regex::new(&format!("^(?:{pat})$"))
        .map_err(|e| Error::bad_value(format!("invalid regex '{pat}': {e}")))
}

// ---------------------------------------------------------------------------
// Per-path token statistics and BM25
// ---------------------------------------------------------------------------

/// Tokens of one field across all documents, plus corpus statistics for BM25.
struct PathStats {
    /// Per document: one token list per string value at the path.
    tokens: Vec<Vec<Vec<String>>>,
    doc_len: Vec<usize>,
    n: f64,
    avgdl: f64,
    df: HashMap<String, usize>,
}

impl PathStats {
    fn build(docs: &[Document], path: &str, analyzer: &str) -> Result<PathStats> {
        let mut tokens = Vec::with_capacity(docs.len());
        let mut doc_len = Vec::with_capacity(docs.len());
        let mut df: HashMap<String, usize> = HashMap::new();
        let (mut n, mut total) = (0usize, 0usize);
        for d in docs {
            let leaves: Vec<Vec<String>> = string_leaves(d, path)
                .into_iter()
                .map(|s| analyze(analyzer, s))
                .collect::<Result<_>>()?;
            let len: usize = leaves.iter().map(Vec::len).sum();
            if len > 0 {
                n += 1;
                total += len;
            }
            let uniq: HashSet<&String> = leaves.iter().flatten().collect();
            for t in uniq {
                *df.entry(t.clone()).or_insert(0) += 1;
            }
            doc_len.push(len);
            tokens.push(leaves);
        }
        let avgdl = if n == 0 { 0.0 } else { total as f64 / n as f64 };
        Ok(PathStats { tokens, doc_len, n: n as f64, avgdl, df })
    }

    fn tf(&self, i: usize, term: &str) -> usize {
        self.tokens[i].iter().flatten().filter(|t| t.as_str() == term).count()
    }

    fn has_phrase(&self, i: usize, q: &[String]) -> bool {
        !q.is_empty() && self.tokens[i].iter().any(|leaf| leaf.windows(q.len()).any(|w| w == q))
    }

    fn prefix_match(&self, i: usize, q: &[String]) -> bool {
        let Some((last, init)) = q.split_last() else {
            return false;
        };
        let toks = || self.tokens[i].iter().flatten();
        init.iter().all(|t| toks().any(|x| x == t)) && toks().any(|x| x.starts_with(last.as_str()))
    }

    fn bm25(&self, i: usize, term: &str) -> f64 {
        let tf = self.tf(i, term) as f64;
        if tf == 0.0 {
            return 0.0;
        }
        let df = self.df.get(term).copied().unwrap_or(0) as f64;
        let idf = (1.0 + (self.n - df + 0.5) / (df + 0.5)).ln();
        let dl = self.doc_len[i] as f64;
        let norm = if self.avgdl > 0.0 { K1 * (1.0 - B + B * dl / self.avgdl) } else { K1 };
        idf * tf * (K1 + 1.0) / (tf + norm)
    }
}

// ---------------------------------------------------------------------------
// Search evaluation
// ---------------------------------------------------------------------------

struct SearchCtx {
    docs: Vec<Document>,
    cfg: IndexCfg,
    stats: HashMap<(String, String), PathStats>,
    paths: BTreeSet<String>,
}

impl SearchCtx {
    fn new(docs: Vec<Document>, cfg: IndexCfg) -> SearchCtx {
        let mut paths = BTreeSet::new();
        for d in &docs {
            collect_paths(d, "", &mut paths);
        }
        SearchCtx { docs, cfg, stats: HashMap::new(), paths }
    }

    /// Every matching document index with its score, in collection order.
    fn run(&mut self, op: &Document) -> Result<Vec<(usize, f64)>> {
        let mut hits = Vec::new();
        for i in 0..self.docs.len() {
            if let Some(s) = self.clause(op, i)? {
                hits.push((i, s));
            }
        }
        Ok(hits)
    }

    fn clause(&mut self, c: &Document, i: usize) -> Result<Option<f64>> {
        let mut it = c.iter();
        let (name, v) = match (it.next(), it.next()) {
            (Some((k, v)), None) => (k.as_str(), v),
            _ => return Err(Error::bad_value("each search clause must contain exactly one operator")),
        };
        let Some(s) = self.op(name, v, i)? else {
            return Ok(None);
        };
        apply_score(v, s).map(Some)
    }

    fn op(&mut self, name: &str, v: &Bson, i: usize) -> Result<Option<f64>> {
        let spec = spec_doc(name, v)?;
        match name {
            "text" => self.text(spec, i),
            "phrase" => self.phrase(spec, i),
            "autocomplete" => self.autocomplete(spec, i),
            "wildcard" => {
                let q = single_query("wildcard", spec)?;
                let re = glob_regex(&q)?;
                self.token_match(spec, &re, i)
            }
            "regex" => {
                let q = single_query("regex", spec)?;
                let re = anchored_regex(&q)?;
                self.token_match(spec, &re, i)
            }
            "compound" => self.compound(spec, i),
            "equals" => self.equals(spec, i),
            "range" => self.range(spec, i),
            "exists" => self.exists(spec, i),
            "in" => self.in_list(spec, i),
            "near" => self.near(spec, i),
            other => Err(Error::bad_value(format!("unsupported search operator '{other}'"))),
        }
    }

    fn expand_paths(&self, v: &Bson, out: &mut Vec<String>) -> Result<()> {
        match v {
            Bson::String(s) => out.push(s.clone()),
            Bson::Array(a) => {
                for x in a {
                    self.expand_paths(x, out)?;
                }
            }
            Bson::Document(d) => {
                let pat = match d.get("wildcard") {
                    Some(Bson::String(p)) => p,
                    _ => return Err(Error::bad_value("path wildcard must be a string")),
                };
                let re = glob_regex(pat)?;
                out.extend(self.paths.iter().filter(|p| re.is_match(p.as_str())).cloned());
            }
            _ => return Err(Error::bad_value("path must be a string, an array, or a wildcard object")),
        }
        Ok(())
    }

    fn field_paths(&self, spec: &Document) -> Result<Vec<String>> {
        let v = spec.get("path").ok_or_else(|| Error::bad_value("operator requires a 'path' field"))?;
        let mut out = Vec::new();
        self.expand_paths(v, &mut out)?;
        Ok(out)
    }

    fn analyzed(&self, path: &str) -> Option<(String, String)> {
        self.cfg.analyzed(path)
    }

    fn path_stats(&mut self, path: &str, analyzer: &str) -> Result<&PathStats> {
        let key = (path.to_string(), analyzer.to_string());
        match self.stats.entry(key) {
            Entry::Occupied(e) => Ok(e.into_mut()),
            Entry::Vacant(e) => {
                let ps = PathStats::build(&self.docs, path, analyzer)?;
                Ok(e.insert(ps))
            }
        }
    }

    fn text(&mut self, spec: &Document, i: usize) -> Result<Option<f64>> {
        let all = match_all(spec)?;
        let queries = query_list(spec)?;
        let paths = self.field_paths(spec)?;
        let mut total = 0.0;
        let mut hit = false;
        for p in &paths {
            let Some((an, san)) = self.analyzed(p) else {
                continue;
            };
            let mut terms: Vec<String> = Vec::new();
            for q in &queries {
                for t in analyze(&san, q)? {
                    if !terms.contains(&t) {
                        terms.push(t);
                    }
                }
            }
            if terms.is_empty() {
                continue;
            }
            let ps = self.path_stats(p, &an)?;
            let found: Vec<&String> = terms.iter().filter(|t| ps.tf(i, t.as_str()) > 0).collect();
            if found.is_empty() || (all && found.len() != terms.len()) {
                continue;
            }
            hit = true;
            for t in found {
                total += ps.bm25(i, t.as_str());
            }
        }
        Ok(hit.then_some(total))
    }

    fn phrase(&mut self, spec: &Document, i: usize) -> Result<Option<f64>> {
        let q = single_query("phrase", spec)?;
        let paths = self.field_paths(spec)?;
        let mut total = 0.0;
        let mut hit = false;
        for p in &paths {
            let Some((an, san)) = self.analyzed(p) else {
                continue;
            };
            let qt = analyze(&san, &q)?;
            if qt.is_empty() {
                continue;
            }
            let ps = self.path_stats(p, &an)?;
            if !ps.has_phrase(i, &qt) {
                continue;
            }
            hit = true;
            let uniq: BTreeSet<&String> = qt.iter().collect();
            for t in uniq {
                total += ps.bm25(i, t.as_str());
            }
        }
        Ok(hit.then_some(total))
    }

    fn autocomplete(&mut self, spec: &Document, i: usize) -> Result<Option<f64>> {
        let q = single_query("autocomplete", spec)?;
        let paths = self.field_paths(spec)?;
        let mut total = 0.0;
        let mut hit = false;
        for p in &paths {
            let Some((an, san)) = self.analyzed(p) else {
                continue;
            };
            let qt = analyze(&san, &q)?;
            if qt.is_empty() {
                continue;
            }
            let ps = self.path_stats(p, &an)?;
            if ps.prefix_match(i, &qt) {
                hit = true;
                total += qt.len() as f64;
            }
        }
        Ok(hit.then_some(total))
    }

    /// Shared by `wildcard` and `regex`: matches indexed tokens against a compiled pattern.
    fn token_match(&mut self, spec: &Document, re: &Regex, i: usize) -> Result<Option<f64>> {
        let paths = self.field_paths(spec)?;
        let mut total = 0.0;
        let mut hit = false;
        for p in &paths {
            let Some((an, _)) = self.analyzed(p) else {
                continue;
            };
            let ps = self.path_stats(p, &an)?;
            let mut matched: BTreeSet<String> = BTreeSet::new();
            for t in ps.tokens[i].iter().flatten() {
                if re.is_match(t.as_str()) {
                    matched.insert(t.clone());
                }
            }
            for t in &matched {
                hit = true;
                total += ps.bm25(i, t.as_str());
            }
        }
        Ok(hit.then_some(total))
    }

    fn compound(&mut self, spec: &Document, i: usize) -> Result<Option<f64>> {
        for (k, _) in spec {
            if !matches!(k.as_str(), "must" | "mustNot" | "should" | "filter" | "minimumShouldMatch" | "score") {
                return Err(Error::bad_value(format!("unknown compound clause '{k}'")));
            }
        }
        let must = clauses(spec, "must")?;
        let filter = clauses(spec, "filter")?;
        let must_not = clauses(spec, "mustNot")?;
        let should = clauses(spec, "should")?;
        if must.is_empty() && filter.is_empty() && must_not.is_empty() && should.is_empty() {
            return Err(Error::bad_value("compound must have at least one clause"));
        }
        let min_should = match spec.get("minimumShouldMatch") {
            None => {
                if !should.is_empty() && must.is_empty() && filter.is_empty() {
                    1
                } else {
                    0
                }
            }
            Some(v) => num_i64(v)
                .filter(|n| *n >= 0)
                .map(|n| n as usize)
                .ok_or_else(|| Error::bad_value("minimumShouldMatch must be a non-negative integer"))?,
        };
        let mut score = 0.0;
        for c in &must {
            match self.clause(c, i)? {
                Some(s) => score += s,
                None => return Ok(None),
            }
        }
        for c in &filter {
            if self.clause(c, i)?.is_none() {
                return Ok(None);
            }
        }
        for c in &must_not {
            if self.clause(c, i)?.is_some() {
                return Ok(None);
            }
        }
        let mut hits = 0usize;
        for c in &should {
            if let Some(s) = self.clause(c, i)? {
                score += s;
                hits += 1;
            }
        }
        if hits < min_should {
            return Ok(None);
        }
        Ok(Some(score))
    }

    fn equals(&mut self, spec: &Document, i: usize) -> Result<Option<f64>> {
        let paths = self.field_paths(spec)?;
        let value = required(spec, "value", "equals")?;
        let doc = &self.docs[i];
        let hit = paths.iter().any(|p| values_at(doc, p).into_iter().any(|x| bson_eq(x, value)));
        Ok(hit.then_some(1.0))
    }

    fn range(&mut self, spec: &Document, i: usize) -> Result<Option<f64>> {
        let paths = self.field_paths(spec)?;
        let mut bounds: Vec<(&str, &Bson)> = Vec::new();
        for key in ["gt", "gte", "lt", "lte"] {
            if let Some(b) = spec.get(key) {
                bounds.push((key, b));
            }
        }
        if bounds.is_empty() {
            return Err(Error::bad_value("range requires at least one of gt, gte, lt, lte"));
        }
        let doc = &self.docs[i];
        let hit = paths
            .iter()
            .any(|p| values_at(doc, p).into_iter().any(|x| in_bounds(x, &bounds)));
        Ok(hit.then_some(1.0))
    }

    fn exists(&mut self, spec: &Document, i: usize) -> Result<Option<f64>> {
        let paths = self.field_paths(spec)?;
        let doc = &self.docs[i];
        let hit = paths.iter().any(|p| !values_at(doc, p).is_empty());
        Ok(hit.then_some(1.0))
    }

    fn in_list(&mut self, spec: &Document, i: usize) -> Result<Option<f64>> {
        let paths = self.field_paths(spec)?;
        let list: Vec<&Bson> = match required(spec, "value", "in")? {
            Bson::Array(a) => a.iter().collect(),
            other => vec![other],
        };
        let doc = &self.docs[i];
        let hit = paths
            .iter()
            .any(|p| values_at(doc, p).into_iter().any(|x| list.iter().any(|v| bson_eq(x, v))));
        Ok(hit.then_some(1.0))
    }

    fn near(&mut self, spec: &Document, i: usize) -> Result<Option<f64>> {
        let paths = self.field_paths(spec)?;
        let origin = as_number(required(spec, "origin", "near")?)
            .ok_or_else(|| Error::bad_value("near origin must be a number or date"))?;
        let pivot = num_f64(required(spec, "pivot", "near")?)
            .filter(|p| *p > 0.0)
            .ok_or_else(|| Error::bad_value("near pivot must be a positive number"))?;
        let doc = &self.docs[i];
        let mut best: Option<f64> = None;
        for p in &paths {
            for x in values_at(doc, p) {
                if let Some(v) = as_number(x) {
                    let s = pivot / (pivot + (v - origin).abs());
                    best = Some(best.map_or(s, |b| b.max(s)));
                }
            }
        }
        Ok(best)
    }
}

fn clauses<'a>(spec: &'a Document, key: &str) -> Result<Vec<&'a Document>> {
    match spec.get(key) {
        None => Ok(Vec::new()),
        Some(Bson::Document(d)) => Ok(vec![d]),
        Some(Bson::Array(items)) => items
            .iter()
            .map(|x| match x {
                Bson::Document(d) => Ok(d),
                _ => Err(Error::bad_value(format!("compound '{key}' clauses must be objects"))),
            })
            .collect(),
        Some(_) => Err(Error::bad_value(format!("compound '{key}' must be an object or array"))),
    }
}

// ---------------------------------------------------------------------------
// $search / $searchMeta / $listSearchIndexes
// ---------------------------------------------------------------------------

struct FacetSpec {
    path: String,
    buckets: usize,
}

struct SearchArgs {
    index: String,
    op: Document,
    sort: Vec<(String, i32)>,
    count: Option<Document>,
    facet: Option<Document>,
}

impl SearchArgs {
    fn parse(stage: &str, arg: &Bson, meta: bool) -> Result<SearchArgs> {
        let Bson::Document(spec) = arg else {
            return Err(Error::failed_to_parse(format!(
                "{stage} value must be an object. Found: {}",
                type_name(arg)
            )));
        };
        let mut args = SearchArgs {
            index: DEFAULT_INDEX.to_string(),
            op: Document::new(),
            sort: Vec::new(),
            count: None,
            facet: None,
        };
        let mut has_op = false;
        for (k, v) in spec {
            match k.as_str() {
                "index" => args.index = str_value(stage, k, v)?.to_string(),
                "sort" if !meta => args.sort = parse_sort(v)?,
                "count" => args.count = Some(doc_value(stage, k, v)?.clone()),
                "facet" if meta => args.facet = Some(doc_value(stage, k, v)?.clone()),
                "returnStoredSource" | "tracking" | "concurrent" => {}
                name if OPERATORS.contains(&name) => {
                    if has_op {
                        return Err(Error::bad_value(format!("{stage} requires exactly one operator")));
                    }
                    has_op = true;
                    args.op.insert(name, v.clone());
                }
                other => return Err(Error::bad_value(format!("unknown field '{other}' in {stage}"))),
            }
        }
        if !has_op {
            return Err(Error::bad_value(format!("{stage} requires an operator")));
        }
        Ok(args)
    }
}

fn parse_sort(v: &Bson) -> Result<Vec<(String, i32)>> {
    let Bson::Document(d) = v else {
        return Err(Error::bad_value("sort must be an object"));
    };
    let mut out = Vec::new();
    for (field, spec) in d {
        match spec {
            Bson::Document(m) => {
                match m.get("$meta") {
                    Some(Bson::String(s)) if s.as_str() == "searchScore" => {}
                    _ => return Err(Error::bad_value(format!("unsupported sort metadata for '{field}'"))),
                }
                let order = m.get("order").and_then(num_i64).unwrap_or(1);
                out.push((SCORE_KEY.to_string(), sort_sign(order)?));
            }
            other => {
                let order = num_i64(other).ok_or_else(|| Error::bad_value("sort order must be 1 or -1"))?;
                out.push((field.clone(), sort_sign(order)?));
            }
        }
    }
    Ok(out)
}

fn sort_sign(n: i64) -> Result<i32> {
    match n {
        1 => Ok(1),
        -1 => Ok(-1),
        _ => Err(Error::bad_value("sort order must be 1 or -1")),
    }
}

fn field_value(doc: &Document, path: &str) -> Bson {
    get_path(doc, path).cloned().unwrap_or(Bson::Undefined)
}

fn order_hits(hits: &mut [(usize, f64)], docs: &[Document], sort: &[(String, i32)]) {
    if sort.is_empty() {
        hits.sort_by(|a, b| b.1.total_cmp(&a.1));
        return;
    }
    hits.sort_by(|a, b| {
        for (field, dir) in sort {
            let ord = if field == SCORE_KEY {
                a.1.total_cmp(&b.1)
            } else {
                cmp_bson(&field_value(&docs[a.0], field), &field_value(&docs[b.0], field))
            };
            let ord = if *dir < 0 { ord.reverse() } else { ord };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
}

fn set_meta(d: &mut Document, field: &str, value: Bson) {
    let mut meta = match d.get(META_KEY) {
        Some(Bson::Document(m)) => m.clone(),
        _ => Document::new(),
    };
    meta.insert(field, value);
    d.insert(META_KEY, meta);
}

fn docs_iter(docs: Vec<Document>) -> DocIter {
    Box::new(docs.into_iter().map(Ok))
}

/// Iterator that runs `job` on first pull and then yields its rows (errors surface on that pull).
fn deferred<F>(job: F) -> DocIter
where
    F: FnOnce() -> Result<Vec<Document>> + Send + 'static,
{
    let mut job = Some(job);
    let mut rows: std::vec::IntoIter<Document> = Vec::new().into_iter();
    Box::new(std::iter::from_fn(move || {
        if let Some(f) = job.take() {
            match f() {
                Ok(v) => rows = v.into_iter(),
                Err(e) => return Some(Err(e)),
            }
        }
        rows.next().map(Ok)
    }))
}

fn build_search(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let args = SearchArgs::parse("$search", arg, false)?;
    let cfg = load_index(opts, &args.index)?;
    Ok(deferred(move || {
        let docs: Vec<Document> = input.collect::<Result<_>>()?;
        let Some(cfg) = cfg else {
            return Ok(Vec::new());
        };
        let mut ctx = SearchCtx::new(docs, cfg);
        let mut hits = ctx.run(&args.op)?;
        order_hits(&mut hits, &ctx.docs, &args.sort);
        Ok(hits
            .into_iter()
            .map(|(i, s)| {
                let mut d = ctx.docs[i].clone();
                set_meta(&mut d, "searchScore", Bson::Double(s));
                d
            })
            .collect())
    }))
}

fn parse_facets(f: &Document) -> Result<Vec<(String, FacetSpec)>> {
    let facets = match f.get("facets") {
        Some(Bson::Document(d)) => d,
        Some(o) => return Err(wrong_type("$searchMeta.facet.facets", o, "object")),
        None => return Err(missing("$searchMeta.facet.facets")),
    };
    let mut out = Vec::new();
    for (name, s) in facets {
        let Bson::Document(s) = s else {
            return Err(wrong_type(&format!("$searchMeta.facet.facets.{name}"), s, "object"));
        };
        let ty = match s.get("type") {
            Some(Bson::String(t)) => t.as_str(),
            _ => return Err(missing(&format!("$searchMeta.facet.facets.{name}.type"))),
        };
        if ty != "string" {
            return Err(Error::bad_value(format!("unsupported facet type '{ty}'")));
        }
        let path = match s.get("path") {
            Some(Bson::String(p)) => p.clone(),
            Some(o) => return Err(wrong_type(&format!("$searchMeta.facet.facets.{name}.path"), o, "string")),
            None => return Err(missing(&format!("$searchMeta.facet.facets.{name}.path"))),
        };
        let buckets = match s.get("numBuckets") {
            None => DEFAULT_BUCKETS,
            Some(v) => num_i64(v)
                .filter(|n| *n > 0)
                .map(|n| n as usize)
                .ok_or_else(|| Error::bad_value("numBuckets must be a positive integer"))?,
        };
        out.push((name.clone(), FacetSpec { path, buckets }));
    }
    Ok(out)
}

fn facet_buckets(docs: &[Document], hits: &[(usize, f64)], spec: &FacetSpec) -> Bson {
    let mut counts: HashMap<String, i64> = HashMap::new();
    for (i, _) in hits {
        let uniq: BTreeSet<&str> = string_leaves(&docs[*i], &spec.path).into_iter().collect();
        for v in uniq {
            *counts.entry(v.to_string()).or_insert(0) += 1;
        }
    }
    let mut rows: Vec<(String, i64)> = counts.into_iter().collect();
    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    rows.truncate(spec.buckets);
    let buckets: Vec<Bson> = rows
        .into_iter()
        .map(|(v, n)| Bson::Document(doc! {"_id": v, "count": Bson::Int64(n)}))
        .collect();
    Bson::Document(doc! {"buckets": Bson::Array(buckets)})
}

fn build_search_meta(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    let args = SearchArgs::parse("$searchMeta", arg, true)?;
    let cfg = load_index(opts, &args.index)?;
    if let Some(c) = &args.count {
        match c.get("type") {
            None => {}
            Some(Bson::String(t)) if t == "total" => {}
            Some(_) => return Err(Error::bad_value("$searchMeta count type must be 'total'")),
        }
    }
    let facets = match &args.facet {
        Some(f) => Some(parse_facets(f)?),
        None => None,
    };
    let include_count = args.count.is_some() || facets.is_none();
    Ok(deferred(move || {
        let docs: Vec<Document> = input.collect::<Result<_>>()?;
        let (docs, hits) = match cfg {
            Some(cfg) => {
                let mut ctx = SearchCtx::new(docs, cfg);
                let hits = ctx.run(&args.op)?;
                (ctx.docs, hits)
            }
            None => (Vec::new(), Vec::new()),
        };
        let mut out = Document::new();
        if include_count {
            out.insert("count", doc! {"total": Bson::Int64(hits.len() as i64)});
        }
        if let Some(fs) = &facets {
            let mut f = Document::new();
            for (name, spec) in fs {
                f.insert(name.as_str(), facet_buckets(&docs, &hits, spec));
            }
            out.insert("facet", f);
        }
        Ok(vec![out])
    }))
}

fn build_list_indexes(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    drop(input);
    let Bson::Document(spec) = arg else {
        return Err(Error::bad_value("$listSearchIndexes must be an object"));
    };
    let (name, id) = selector_fields(spec)?;
    let coll = coll_of(opts);
    let rows: Vec<Document> = opts
        .ctx
        .search_indexes(&opts.db, &coll)?
        .into_iter()
        .filter(|e| name.as_deref().is_none_or(|n| entry_name(e) == Some(n)))
        .filter(|e| id.as_deref().is_none_or(|i| entry_id(e) == Some(i)))
        .collect();
    Ok(docs_iter(rows))
}

// ---------------------------------------------------------------------------
// $rankFusion
// ---------------------------------------------------------------------------

struct FusionSpec {
    pipelines: Vec<(String, Vec<Document>)>,
    weights: HashMap<String, Bson>,
    details: bool,
}

struct Pipe {
    name: String,
    weight: Bson,
    w: f64,
    it: DocIter,
}

struct Fused {
    doc: Document,
    score: f64,
    ranks: Vec<Option<usize>>,
}

fn build_rank_fusion(input: DocIter, arg: &Bson, opts: &Arc<AggOptions>) -> Result<DocIter> {
    drop(input);
    let Bson::Document(spec) = arg else {
        return Err(Error::failed_to_parse(format!(
            "$rankFusion must take a nested object but found: $rankFusion: {arg}"
        )));
    };
    let fusion = parse_fusion(spec)?;
    let coll = coll_of(opts);
    let sub = Arc::new(AggOptions {
        db: opts.db.clone(),
        coll: Some(coll.clone()),
        allow_disk_use: opts.allow_disk_use,
        max_memory_bytes: opts.max_memory_bytes,
        vars: opts.vars.clone(),
        ctx: Arc::clone(&opts.ctx),
    });
    let mut pipes = Vec::with_capacity(fusion.pipelines.len());
    for (name, stages) in fusion.pipelines {
        check_fusion_pipeline(&stages)?;
        let weight = fusion.weights.get(&name).cloned().unwrap_or(Bson::Int32(1));
        let w = num_f64(&weight).unwrap_or(1.0);
        let scan = opts.ctx.scan(&opts.db, &coll)?;
        let it = run_pipeline(scan, &stages, Arc::clone(&sub))?;
        pipes.push(Pipe { name, weight, w, it });
    }
    let details = fusion.details;
    Ok(deferred(move || fuse(pipes, details)))
}

fn missing_or_wrong(field: &str) -> Error {
    missing(field)
}

fn parse_fusion(spec: &Document) -> Result<FusionSpec> {
    let mut input = None;
    let mut combination = None;
    let mut details = false;
    for (k, v) in spec {
        match k.as_str() {
            "input" => input = Some(v),
            "combination" => combination = Some(v),
            "scoreDetails" => match v {
                Bson::Boolean(b) => details = *b,
                Bson::Null => details = false,
                other => return Err(wrong_type("$rankFusion.scoreDetails", other, "bool")),
            },
            other => return Err(unknown_field(&format!("$rankFusion.{other}"))),
        }
    }
    let input = match input {
        Some(Bson::Document(d)) => d,
        Some(o) => return Err(wrong_type("$rankFusion.input", o, "object")),
        None => return Err(missing_or_wrong("$rankFusion.input")),
    };
    let mut pipelines_val = None;
    for (k, v) in input {
        match k.as_str() {
            "pipelines" => pipelines_val = Some(v),
            other => return Err(unknown_field(&format!("$rankFusion.input.{other}"))),
        }
    }
    let pipelines = match pipelines_val {
        Some(Bson::Document(d)) => d,
        Some(o) => return Err(wrong_type("$rankFusion.input.pipelines", o, "object")),
        None => return Err(missing("$rankFusion.input.pipelines")),
    };
    if pipelines.is_empty() {
        return Err(Error::bad_value("A hybrid scoring stage should be run with at least one pipeline."));
    }
    let mut out: Vec<(String, Vec<Document>)> = Vec::new();
    for (name, v) in pipelines {
        if name.is_empty() {
            return Err(Error::location(
                15998,
                format!("{RANK_NAME_PREFIX}FieldPath field names may not be empty strings."),
            ));
        }
        if name.starts_with('$') {
            return Err(Error::location(
                16410,
                format!("{RANK_NAME_PREFIX}FieldPath field names may not start with '$', given '{name}'."),
            ));
        }
        if name.contains('.') {
            return Err(Error::location(
                16412,
                format!("{RANK_NAME_PREFIX}FieldPath field names may not contain '.', given '{name}'."),
            ));
        }
        let Bson::Array(items) = v else {
            return Err(Error::type_mismatch(format!("{RANK_PARSE_PREFIX}A pipeline must be an array of objects")));
        };
        let mut stages = Vec::with_capacity(items.len());
        for item in items {
            match item {
                Bson::Document(d) => stages.push(d.clone()),
                _ => {
                    return Err(Error::type_mismatch(format!(
                        "{RANK_PARSE_PREFIX}Each element of the 'pipeline' array must be an object"
                    )));
                }
            }
        }
        out.push((name.clone(), stages));
    }

    let mut weights = HashMap::new();
    if let Some(c) = combination {
        let Bson::Document(c) = c else {
            return Err(wrong_type("$rankFusion.combination", c, "object"));
        };
        let mut weights_val = None;
        for (k, v) in c {
            match k.as_str() {
                "weights" => weights_val = Some(v),
                other => return Err(unknown_field(&format!("$rankFusion.combination.{other}"))),
            }
        }
        let w = match weights_val {
            Some(Bson::Document(w)) => w,
            Some(o) => return Err(wrong_type("$rankFusion.combination.weights", o, "object")),
            None => return Err(missing("$rankFusion.combination.weights")),
        };
        let names: Vec<&str> = out.iter().map(|(n, _)| n.as_str()).collect();
        if w.len() > out.len() {
            let extra: Vec<&str> = w.keys().map(String::as_str).filter(|k| !names.contains(k)).collect();
            return Err(Error::location(
                12559403,
                format!(
                    "$rankFusion input has more weights ({}) than pipelines ({}). If 'combination.weights' is specified, there must be a less or equal number of weights as pipelines, each of which is unique and existing. Possible extraneous specified weights = [{}]",
                    w.len(),
                    out.len(),
                    extra.join(", ")
                ),
            ));
        }
        for (k, v) in w {
            let Some(x) = num_f64(v) else {
                return Err(Error::location(
                    12559404,
                    format!("$rankFusion's pipeline weight must be numeric, but given non-numeric value for pipeline '{k}'."),
                ));
            };
            if x.is_nan() || x < 0.0 {
                return Err(Error::location(
                    12559401,
                    format!(
                        "$rankFusion's pipeline weight must be non-negative, but given {} for pipeline '{k}'.",
                        weight_text(v)
                    ),
                ));
            }
            weights.insert(k.clone(), v.clone());
        }
        let mut parts = Vec::new();
        for k in w.keys() {
            if !names.contains(&k.as_str()) {
                let best = names.iter().map(|n| levenshtein(k, n)).min().unwrap_or(0);
                let close: Vec<&str> = names.iter().copied().filter(|n| levenshtein(k, n) == best).collect();
                let suggestion = match close.as_slice() {
                    [only] => format!("suggested: '{only}'"),
                    _ => format!("suggestions: [{}]", close.join(", ")),
                };
                parts.push(format!("(provided: '{k}' -> {suggestion})"));
            }
        }
        if !parts.is_empty() {
            return Err(Error::location(
                9967500,
                format!(
                    "$$rankFusion stage contained ({}) weight(s) in 'combination.weights' that did not reference valid pipeline names. Suggestions for valid pipeline names: {}",
                    parts.len(),
                    parts.join(", ")
                ),
            ));
        }
    }
    Ok(FusionSpec { pipelines: out, weights, details })
}

fn weight_text(v: &Bson) -> String {
    match v {
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) if f.is_nan() => "nan".to_string(),
        Bson::Double(f) => f.to_string(),
        other => format!("{other}"),
    }
}

fn levenshtein(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != *cb);
            cur[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        prev = cur;
    }
    prev[b.len()]
}

/// Validate one input pipeline in the order `$rankFusion` reports errors.
fn check_fusion_pipeline(stages: &[Document]) -> Result<()> {
    if stages.is_empty() {
        return Err(Error::new(12108700, RANK_EMPTY));
    }
    let mut parsed: Vec<(&str, &Bson)> = Vec::with_capacity(stages.len());
    for st in stages {
        let (name, arg) = single_stage(st)?;
        if !KNOWN_STAGES.contains(&name) {
            return Err(Error::new(40324, format!("Unrecognized pipeline stage name: '{name}'")));
        }
        if name == "$sort" && !matches!(arg, Bson::Document(_)) {
            return Err(Error::new(12559415, "Expected $sort spec value to be an object"));
        }
        parsed.push((name, arg));
    }
    for &(name, arg) in &parsed {
        if matches!(name, "$rankFusion" | "$scoreFusion") {
            return match arg {
                Bson::Document(d) if name == "$rankFusion" => {
                    parse_fusion(d)?;
                    Err(Error::new(12108701, RANK_NESTED))
                }
                Bson::Document(d) if d.contains_key("input") => Err(Error::new(12108701, RANK_NESTED)),
                Bson::Document(_) => Err(missing(&format!("{name}.input"))),
                other if name == "$rankFusion" => Err(Error::failed_to_parse(format!(
                    "$rankFusion must take a nested object but found: $rankFusion: {other}"
                ))),
                _ => Err(missing(&format!("{name}.input"))),
            };
        }
    }
    if matches!(parsed[0].0, "$search" | "$vectorSearch") {
        return Err(Error::new(31082, "Using $search and $vectorSearch aggregation stages requires additional configuration. Please connect to Atlas or an AtlasCLI local deployment to enable.For more information on how to connect, see https://dochub.mongodb.org/core/atlas-cli-deploy-local-reqs."));
    }
    let ranked = RANKED_START.contains(&parsed[0].0) || parsed.iter().any(|(n, _)| *n == "$sort");
    if !ranked {
        return Err(Error::new(12108702, RANK_NOT_RANKED));
    }
    for &(name, _) in &parsed {
        if !RANK_ALLOWED.contains(&name) {
            return Err(Error::new(
                12108704,
                format!(
                    "$rankFusion input pipelines must not contain {name} because it modifies or transforms the input documents. Only stages that retrieve, limit, or order documents are allowed."
                ),
            ));
        }
    }
    Ok(())
}

fn single_stage(st: &Document) -> Result<(&str, &Bson)> {
    let mut it = st.iter();
    match (it.next(), it.next()) {
        (Some((k, v)), None) => Ok((k.as_str(), v)),
        _ => Err(Error::new(40323, "A pipeline stage specification object must contain exactly one field.")),
    }
}

fn dedupe_key(d: &Document) -> Vec<u8> {
    match d.get("_id") {
        Some(id) => {
            let mut k = Document::new();
            k.insert("_id", id.clone());
            to_bytes(&k)
        }
        None => to_bytes(d),
    }
}

/// Reciprocal rank fusion over the already-validated input pipeline outputs.
fn fuse(pipes: Vec<Pipe>, details: bool) -> Result<Vec<Document>> {
    let n = pipes.len();
    let names: Vec<String> = pipes.iter().map(|p| p.name.clone()).collect();
    let weights: Vec<Bson> = pipes.iter().map(|p| p.weight.clone()).collect();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut rows: Vec<Fused> = Vec::new();
    for (pi, p) in pipes.into_iter().enumerate() {
        let w = p.w;
        for (r, item) in p.it.enumerate() {
            let d = item?;
            let key = dedupe_key(&d);
            let slot = match index.get(&key) {
                Some(&s) => s,
                None => {
                    rows.push(Fused { doc: d, score: 0.0, ranks: vec![None; n] });
                    index.insert(key, rows.len() - 1);
                    rows.len() - 1
                }
            };
            let row = &mut rows[slot];
            if row.ranks[pi].is_none() {
                let rank = r + 1;
                row.ranks[pi] = Some(rank);
                row.score += w * (1.0 / (RRF_K + rank as f64));
            }
        }
    }
    rows.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| cmp_bson(a.doc.get("_id").unwrap_or(&Bson::Null), b.doc.get("_id").unwrap_or(&Bson::Null)))
    });
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let score = narrow_score(row.score);
        let mut d = row.doc;
        set_meta(&mut d, "score", score.clone());
        if details {
            set_meta(&mut d, "scoreDetails", score_details(&score, &names, &weights, &row.ranks));
        }
        out.push(d);
    }
    Ok(out)
}

/// Integral scores become Int32 or Int64 when they fit; everything else stays double.
fn narrow_score(score: f64) -> Bson {
    if score.is_finite() && score.fract() == 0.0 {
        if score >= i32::MIN as f64 && score <= i32::MAX as f64 {
            return Bson::Int32(score as i32);
        }
        if score >= -9.223_372_036_854_776e18 && score < 9.223_372_036_854_776e18 {
            return Bson::Int64(score as i64);
        }
    }
    Bson::Double(score)
}

fn score_details(score: &Bson, names: &[String], weights: &[Bson], ranks: &[Option<usize>]) -> Bson {
    let details: Vec<Bson> = names
        .iter()
        .zip(weights)
        .zip(ranks)
        .map(|((name, weight), rank)| {
            let mut e = Document::new();
            e.insert("inputPipelineName", name.as_str());
            match rank {
                Some(r) => {
                    e.insert("rank", *r as i32);
                    e.insert("weight", weight.clone());
                    e.insert("details", Bson::Array(Vec::new()));
                }
                None => {
                    e.insert("rank", "NA");
                }
            }
            Bson::Document(e)
        })
        .collect();
    let mut d = Document::new();
    d.insert("value", score.clone());
    d.insert("description", RANK_DESCRIPTION);
    d.insert("details", Bson::Array(details));
    Bson::Document(d)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(def: Document) -> IndexCfg {
        IndexCfg::parse(&def).expect("valid definition")
    }

    fn dynamic_cfg() -> IndexCfg {
        cfg(doc! {"mappings": {"dynamic": true}})
    }

    /// Runs `op` and returns `(_id, score)` pairs in result order.
    fn run_hits(docs: Vec<Document>, index: IndexCfg, op: Document) -> Vec<(i32, f64)> {
        let mut ctx = SearchCtx::new(docs, index);
        let mut hits = ctx.run(&op).expect("search runs");
        order_hits(&mut hits, &ctx.docs, &[]);
        hits.into_iter().map(|(i, s)| (ctx.docs[i].get_i32("_id").unwrap(), s)).collect()
    }

    fn ids(hits: &[(i32, f64)]) -> Vec<i32> {
        hits.iter().map(|h| h.0).collect()
    }

    #[test]
    fn analyzers_tokenize_as_documented() {
        assert_eq!(analyze("lucene.standard", "Hello, World-42").unwrap(), vec!["hello", "world", "42"]);
        assert_eq!(analyze("lucene.simple", "abc123 Def").unwrap(), vec!["abc", "def"]);
        assert_eq!(analyze("lucene.whitespace", "Foo  bar").unwrap(), vec!["Foo", "bar"]);
        assert_eq!(analyze("lucene.keyword", "Foo Bar").unwrap(), vec!["Foo Bar"]);
        assert!(analyze("lucene.keyword", "").unwrap().is_empty());
        assert_eq!(analyze("lucene.english", "The cat and the hat").unwrap(), vec!["cat", "hat"]);
        assert!(analyze("lucene.nope", "x").is_err());
    }

    #[test]
    fn bm25_ranks_higher_term_frequency_first() {
        let docs = vec![
            doc! {"_id": 1, "t": "apple apple apple"},
            doc! {"_id": 2, "t": "apple banana cherry date"},
            doc! {"_id": 3, "t": "banana"},
        ];
        let op = doc! {"text": {"query": "apple", "path": "t"}};
        let hits = run_hits(docs, dynamic_cfg(), op);
        assert_eq!(ids(&hits), vec![1, 2]);
        assert!(hits[0].1 > hits[1].1);
    }

    #[test]
    fn text_match_criteria() {
        let docs = vec![doc! {"_id": 1, "t": "red blue"}, doc! {"_id": 2, "t": "red"}];
        let any = doc! {"text": {"query": "red blue", "path": "t"}};
        assert_eq!(ids(&run_hits(docs.clone(), dynamic_cfg(), any)), vec![1, 2]);
        let all = doc! {"text": {"query": "red blue", "path": "t", "matchCriteria": "all"}};
        assert_eq!(ids(&run_hits(docs, dynamic_cfg(), all)), vec![1]);
    }

    #[test]
    fn phrase_requires_adjacent_terms() {
        let docs = vec![doc! {"_id": 1, "t": "quick brown fox"}, doc! {"_id": 2, "t": "brown quick fox"}];
        let op = doc! {"phrase": {"query": "quick brown", "path": "t"}};
        assert_eq!(ids(&run_hits(docs, dynamic_cfg(), op)), vec![1]);
    }

    #[test]
    fn autocomplete_matches_prefix_of_last_term() {
        let docs = vec![doc! {"_id": 1, "t": "apple pie"}, doc! {"_id": 2, "t": "banana"}];
        let ok = doc! {"autocomplete": {"query": "app", "path": "t"}};
        assert_eq!(ids(&run_hits(docs.clone(), dynamic_cfg(), ok)), vec![1]);
        let two = doc! {"autocomplete": {"query": "apple pi", "path": "t"}};
        assert_eq!(ids(&run_hits(docs, dynamic_cfg(), two)), vec![1]);
    }

    #[test]
    fn wildcard_and_regex_match_tokens() {
        let docs = vec![doc! {"_id": 1, "t": "banana"}, doc! {"_id": 2, "t": "cherry"}];
        let w = doc! {"wildcard": {"query": "b?n*", "path": "t"}};
        assert_eq!(ids(&run_hits(docs.clone(), dynamic_cfg(), w)), vec![1]);
        let r = doc! {"regex": {"query": "ch.rry", "path": "t"}};
        assert_eq!(ids(&run_hits(docs, dynamic_cfg(), r)), vec![2]);
    }

    #[test]
    fn equals_range_exists_in_on_numbers() {
        let docs = vec![
            doc! {"_id": 1, "n": 1, "tag": "a"},
            doc! {"_id": 2, "n": 2},
            doc! {"_id": 3, "n": 3, "tag": "b"},
        ];
        let eq = doc! {"equals": {"path": "n", "value": 2}};
        assert_eq!(ids(&run_hits(docs.clone(), dynamic_cfg(), eq)), vec![2]);
        let range = doc! {"range": {"path": "n", "gte": 2, "lt": 3}};
        assert_eq!(ids(&run_hits(docs.clone(), dynamic_cfg(), range)), vec![2]);
        let exists = doc! {"exists": {"path": "tag"}};
        assert_eq!(ids(&run_hits(docs.clone(), dynamic_cfg(), exists)), vec![1, 3]);
        let inn = doc! {"in": {"path": "n", "value": [1, 3]}};
        assert_eq!(ids(&run_hits(docs, dynamic_cfg(), inn)), vec![1, 3]);
    }

    #[test]
    fn near_scores_closer_values_higher() {
        let docs = vec![doc! {"_id": 1, "n": 10}, doc! {"_id": 2, "n": 50}];
        let op = doc! {"near": {"path": "n", "origin": 10, "pivot": 5}};
        let hits = run_hits(docs, dynamic_cfg(), op);
        assert_eq!(ids(&hits), vec![1, 2]);
        assert!((hits[0].1 - 1.0).abs() < 1e-9);
    }

    #[test]
    fn compound_must_not_and_should() {
        let docs = vec![
            doc! {"_id": 1, "t": "apple pie"},
            doc! {"_id": 2, "t": "apple"},
            doc! {"_id": 3, "t": "banana"},
        ];
        let op = doc! {"compound": {
            "must": [{"text": {"query": "apple", "path": "t"}}],
            "mustNot": [{"text": {"query": "pie", "path": "t"}}],
        }};
        assert_eq!(ids(&run_hits(docs.clone(), dynamic_cfg(), op)), vec![2]);
        let should = doc! {"compound": {"should": [
            {"text": {"query": "pie", "path": "t"}},
            {"text": {"query": "banana", "path": "t"}},
        ]}};
        // BM25 length normalization: the single-token "banana" doc outscores "apple pie".
        assert_eq!(ids(&run_hits(docs, dynamic_cfg(), should)), vec![3, 1]);
    }

    #[test]
    fn score_boost_and_constant_modifiers() {
        let docs = vec![doc! {"_id": 1, "n": 1}];
        let boost = doc! {"equals": {"path": "n", "value": 1, "score": {"boost": {"value": 3}}}};
        assert_eq!(run_hits(docs.clone(), dynamic_cfg(), boost)[0].1, 3.0);
        let constant = doc! {"equals": {"path": "n", "value": 1, "score": {"constant": {"value": 7}}}};
        assert_eq!(run_hits(docs, dynamic_cfg(), constant)[0].1, 7.0);
    }

    #[test]
    fn search_meta_facets_and_count() {
        let docs = vec![
            doc! {"_id": 1, "color": "red"},
            doc! {"_id": 2, "color": "blue"},
            doc! {"_id": 3, "color": "red"},
        ];
        let mut ctx = SearchCtx::new(docs, dynamic_cfg());
        let hits = ctx.run(&doc! {"exists": {"path": "color"}}).unwrap();
        let spec = FacetSpec { path: "color".into(), buckets: 10 };
        let buckets = facet_buckets(&ctx.docs, &hits, &spec);
        let expected = Bson::Document(doc! {"buckets": [
            {"_id": "red", "count": Bson::Int64(2)},
            {"_id": "blue", "count": Bson::Int64(1)},
        ]});
        assert_eq!(buckets, expected);
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn index_definition_validation() {
        assert!(IndexCfg::parse(&doc! {"analyzer": "lucene.nope"}).is_err());
        assert!(IndexCfg::parse(&doc! {"mappings": {"dynamic": "yes"}}).is_err());
        assert!(IndexCfg::parse(&doc! {"mappings": {"fields": {"t": {"type": "bogus"}}}}).is_err());
        let c = cfg(doc! {"mappings": {"fields": {"t": {"type": "string", "analyzer": "lucene.keyword"}}}});
        assert_eq!(c.analyzed("t").unwrap().0, "lucene.keyword");
        assert!(c.analyzed("other").is_none());
    }

    #[test]
    fn index_entry_selection() {
        let list = vec![index_entry("id1", "a", Document::new()), index_entry("id2", "b", Document::new())];
        assert_eq!(position(&list, Some("b"), None), Some(1));
        assert_eq!(position(&list, None, Some("id1")), Some(0));
        assert_eq!(position(&list, Some("zz"), None), None);
    }

    #[test]
    fn rank_fusion_pipeline_validation_codes() {
        let sort = doc! {"$sort": {"a": 1}};
        assert!(check_fusion_pipeline(&[sort.clone()]).is_ok());
        assert_eq!(check_fusion_pipeline(&[]).unwrap_err().code, 12108700);
        assert_eq!(check_fusion_pipeline(&[doc! {"$foo": 1}]).unwrap_err().code, 40324);
        assert_eq!(check_fusion_pipeline(&[doc! {"$match": {}}]).unwrap_err().code, 12108702);
        assert_eq!(check_fusion_pipeline(&[sort.clone(), doc! {"$group": {}}]).unwrap_err().code, 12108704);
        assert_eq!(check_fusion_pipeline(&[doc! {"$group": {}}, sort]).unwrap_err().code, 12108704);
    }

    #[test]
    fn rank_fusion_scores_and_order() {
        let pipe = |name: &str, docs: Vec<Document>, weight: Bson| Pipe {
            name: name.to_string(),
            weight,
            w: 0.0,
            it: Box::new(docs.into_iter().map(Ok)),
        };
        let mut p1 = pipe("p1", vec![doc! {"_id": 1}, doc! {"_id": 2}], Bson::Int32(1));
        p1.w = 1.0;
        let mut p2 = pipe("p2", vec![doc! {"_id": 2}], Bson::Int32(1));
        p2.w = 1.0;
        let out = fuse(vec![p1, p2], true).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].get_i32("_id").unwrap(), 2);
        let meta = out[0].get_document(META_KEY).unwrap();
        // _id 2 is rank 2 in p1 and rank 1 in p2: 1/(60+2) + 1/(60+1).
        assert!(matches!(meta.get("score"), Some(Bson::Double(s)) if (s - (1.0 / 62.0 + 1.0 / 61.0)).abs() < 1e-12));
        let details = meta.get_document("scoreDetails").unwrap();
        let list = details.get_array("details").unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn narrowing_and_missing_pipeline_details() {
        assert!(matches!(narrow_score(0.0), Bson::Int32(0)));
        assert!(matches!(narrow_score(1073741824.0), Bson::Int32(1073741824)));
        assert!(matches!(narrow_score(2147483648.0), Bson::Int64(2147483648)));
        assert!(matches!(narrow_score(0.5), Bson::Double(_)));
        let d = score_details(&Bson::Int32(0), &["p1".into(), "p2".into()], &[Bson::Int32(1), Bson::Int32(1)], &[None, Some(1)]);
        let Bson::Document(d) = d else { panic!("expected document") };
        let details = d.get_array("details").unwrap();
        let Bson::Document(first) = &details[0] else { panic!("expected document") };
        assert_eq!(first.get_str("rank").unwrap(), "NA");
    }

    #[test]
    fn levenshtein_distance() {
        assert_eq!(levenshtein("p9", "p1"), 1);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
    }
}
