use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

use bson::{Bson, DateTime, Document, Timestamp};

use crate::agg::expr::Vars;
use crate::agg::pipeline::apply_simple_stages;
use crate::bsonutil::{
    bson_eq, cmp_bson, f64_to_decimal, get_path, is_number, num_add, num_f64, num_i64, num_mul,
    remove_path, set_path, type_name,
};
use crate::error::{Error, Result};
use crate::query::matcher::{Matcher, value_matches};
use crate::query::sort::SortSpec;

/// A parsed update: operator document, replacement document, or pipeline.
pub struct UpdateDoc {
    kind: Kind,
    filters: HashMap<String, ArrayFilter>,
}

enum Kind {
    Replacement(Document),
    Pipeline(Vec<Document>),
    Operators(Vec<Entry>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Op {
    Set,
    SetOnInsert,
    Unset,
    Inc,
    Mul,
    Min,
    Max,
    Rename,
    CurrentDate,
    Push,
    AddToSet,
    Pop,
    Pull,
    PullAll,
    Bit,
}

impl Op {
    fn from_name(name: &str) -> Option<Op> {
        Some(match name {
            "$set" => Op::Set,
            "$setOnInsert" => Op::SetOnInsert,
            "$unset" => Op::Unset,
            "$inc" => Op::Inc,
            "$mul" => Op::Mul,
            "$min" => Op::Min,
            "$max" => Op::Max,
            "$rename" => Op::Rename,
            "$currentDate" => Op::CurrentDate,
            "$push" => Op::Push,
            "$addToSet" => Op::AddToSet,
            "$pop" => Op::Pop,
            "$pull" => Op::Pull,
            "$pullAll" => Op::PullAll,
            "$bit" => Op::Bit,
            _ => return None,
        })
    }
}

struct Entry {
    op: Op,
    target: Path,
    /// Destination path, only for `$rename`.
    dst: Option<Path>,
    arg: Bson,
    prep: Prep,
}

impl Entry {
    fn paths(&self) -> Vec<&Path> {
        let mut v = vec![&self.target];
        v.extend(self.dst.as_ref());
        v
    }
}

/// Argument data prepared once at parse time.
enum Prep {
    Plain,
    Push(PushSpec),
    Pop(i64),
    Pull(PullCond),
    PullAll(Vec<Bson>),
    Bit(Vec<(BitOp, i64, bool)>),
    CurrentDate(bool),
}

#[derive(Clone, Copy)]
enum BitOp {
    And,
    Or,
    Xor,
}

enum SortMode {
    /// `true` = ascending.
    Value(bool),
    Doc(SortSpec),
}

struct PushSpec {
    each: Vec<Bson>,
    position: Option<i64>,
    slice: Option<i64>,
    sort: Option<SortMode>,
}

enum PullCond {
    Doc(Matcher),
    Value(Bson),
}

enum ArrayFilter {
    Value(Bson),
    Doc(Matcher),
}

#[derive(Clone, Debug)]
struct Path {
    raw: String,
    segs: Vec<Seg>,
}

#[derive(Clone, Debug)]
enum Seg {
    Field(String),
    /// `$`
    Pos,
    /// `$[]`
    All,
    /// `$[id]`
    Filtered(String),
}

enum Action {
    Keep,
    Set(Bson),
    Unset,
}

enum Shape {
    Missing,
    Doc,
    Arr,
    Other,
}

type LeafFn<'a> = dyn FnMut(Option<&Bson>) -> Result<Action> + 'a;

struct Cx<'a> {
    doc: &'a Document,
    position: Option<usize>,
    filters: &'a HashMap<String, ArrayFilter>,
    raw: &'a str,
}

impl UpdateDoc {
    /// `update`: operator document, replacement document, or array (pipeline update).
    pub fn new(update: &Bson, array_filters: Option<&Vec<Bson>>) -> Result<UpdateDoc> {
        let has_filters = array_filters.is_some_and(|f| !f.is_empty());
        match update {
            Bson::Array(stages) => {
                if has_filters {
                    return Err(Error::failed_to_parse(
                        "Cannot use array filters in a pipeline-style update",
                    ));
                }
                let mut docs = Vec::with_capacity(stages.len());
                for stage in stages {
                    match stage {
                        Bson::Document(d) => docs.push(d.clone()),
                        _ => {
                            return Err(Error::failed_to_parse(
                                "Each element of the 'pipeline' array must be an object",
                            ));
                        }
                    }
                }
                Ok(UpdateDoc { kind: Kind::Pipeline(docs), filters: HashMap::new() })
            }
            Bson::Document(d) if d.keys().next().is_some_and(|k| k.starts_with('$')) => {
                let mut entries = parse_operators(d)?;
                let filters = parse_array_filters(array_filters)?;
                check_filter_usage(&entries, &filters)?;
                check_conflicts(&entries)?;
                entries.sort_by(|a, b| a.target.texts().cmp(&b.target.texts()));
                Ok(UpdateDoc { kind: Kind::Operators(entries), filters })
            }
            Bson::Document(d) => {
                if has_filters {
                    return Err(Error::failed_to_parse(
                        "Cannot use array filters in a replacement-style update",
                    ));
                }
                if let Some(k) = d.keys().find(|k| k.starts_with('$')) {
                    return Err(Error::new(
                        52,
                        format!(
                            "The dollar ($) prefixed field '{k}' in '{k}' is not allowed in the context of an update's replacement document. Consider using an aggregation pipeline with $replaceWith."
                        ),
                    ));
                }
                Ok(UpdateDoc { kind: Kind::Replacement(d.clone()), filters: HashMap::new() })
            }
            other => Err(Error::failed_to_parse(format!(
                "Modifiers operate on fields but we found type {} instead. For example: {{$mod: {{<field>: ...}}}} not {{$set: {}}}",
                type_name(other),
                fmt_bson(other)
            ))),
        }
    }

    pub fn is_replacement(&self) -> bool {
        matches!(self.kind, Kind::Replacement(_))
    }

    /// Apply to `doc`. `position`: matched array index for `$`. `is_insert`: applying to an upsert seed.
    pub fn apply(
        &self,
        doc: &Document,
        position: Option<usize>,
        is_insert: bool,
        vars: &Vars,
    ) -> Result<Document> {
        match &self.kind {
            Kind::Replacement(rep) => apply_replacement(doc, rep),
            Kind::Pipeline(stages) => {
                let out = apply_simple_stages(doc.clone(), stages, vars)?;
                check_pipeline_id(doc, &out)?;
                Ok(ensure_id_first(out))
            }
            Kind::Operators(entries) => {
                let mut out = doc.clone();
                for e in entries {
                    if e.op == Op::SetOnInsert && !is_insert {
                        continue;
                    }
                    let cx = Cx { doc, position, filters: &self.filters, raw: &e.target.raw };
                    apply_entry(&mut out, e, &cx)?;
                }
                check_operator_id(doc, &out)?;
                Ok(ensure_id_first(out))
            }
        }
    }
}

/// Build the base document for an upsert insert from the query filter's equality predicates.
pub fn upsert_seed(filter: &Document) -> Result<Document> {
    let mut out = Document::new();
    collect_equalities(filter, &mut out)?;
    Ok(ensure_id_first(out))
}

fn collect_equalities(filter: &Document, out: &mut Document) -> Result<()> {
    for (key, value) in filter {
        if key.as_str() == "$and" {
            if let Bson::Array(items) = value {
                for item in items {
                    if let Bson::Document(d) = item {
                        collect_equalities(d, out)?;
                    }
                }
            }
            continue;
        }
        if key.starts_with('$') || matches!(value, Bson::RegularExpression(_)) {
            continue;
        }
        let eq = match value {
            Bson::Document(d) if d.keys().next().is_some_and(|k| k.starts_with('$')) => {
                match (d.get("$eq"), d.get("$in")) {
                    (Some(e), _) => e.clone(),
                    // A single-element `$in` is an equality for upsert purposes.
                    (None, Some(Bson::Array(a))) if a.len() == 1 && d.len() == 1 => a[0].clone(),
                    _ => continue,
                }
            }
            other => other.clone(),
        };
        let conflict = || {
            Error::bad_value(format!(
                "cannot infer query fields to set, path '{key}' is matched twice"
            ))
        };
        match get_path(out, key).map(|existing| bson_eq(existing, &eq)) {
            Some(true) => {}
            Some(false) => return Err(conflict()),
            None => set_path(out, key, eq).map_err(|_| conflict())?,
        }
    }
    Ok(())
}

fn apply_replacement(doc: &Document, rep: &Document) -> Result<Document> {
    let mut out = rep.clone();
    if let Some(before) = doc.get("_id") {
        match out.get("_id").cloned() {
            None => {
                out.insert("_id", before.clone());
            }
            Some(after) if !bson_eq(&after, before) => return Err(id_altered(&after)),
            Some(_) => {}
        }
    }
    Ok(ensure_id_first(out))
}

fn id_altered(after: &Bson) -> Error {
    Error::new(
        66,
        format!(
            "After applying the update, the (immutable) field '_id' was found to have been altered to _id: {}",
            fmt_bson(after)
        ),
    )
}

fn check_pipeline_id(before: &Document, after: &Document) -> Result<()> {
    match before.get("_id") {
        None => Ok(()),
        Some(b) => match after.get("_id") {
            None => Err(Error::new(
                66,
                "After applying the update, the (immutable) field '_id' was found to have been removed",
            )),
            Some(a) if bson_eq(a, b) => Ok(()),
            Some(a) => Err(id_altered(a)),
        },
    }
}

fn check_operator_id(before: &Document, after: &Document) -> Result<()> {
    match before.get("_id") {
        None => Ok(()),
        Some(b) => match after.get("_id") {
            Some(a) if bson_eq(a, b) => Ok(()),
            _ => Err(Error::new(
                66,
                "Performing an update on the path '_id' would modify the immutable field '_id'",
            )),
        },
    }
}

/// Move `_id` to the front of the document, if present.
fn ensure_id_first(doc: Document) -> Document {
    if !doc.contains_key("_id") || doc.keys().next().is_some_and(|k| k.as_str() == "_id") {
        return doc;
    }
    let mut out = Document::new();
    if let Some(v) = doc.get("_id") {
        out.insert("_id", v.clone());
    }
    for (k, v) in &doc {
        if k.as_str() != "_id" {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

fn parse_operators(d: &Document) -> Result<Vec<Entry>> {
    let mut entries = Vec::new();
    for (k, v) in d {
        if !k.starts_with('$') {
            return Err(Error::failed_to_parse(format!(
                "Unknown modifier: {k}. Expected a valid update modifier or pipeline-style update specified as an array"
            )));
        }
        let op = Op::from_name(k)
            .ok_or_else(|| Error::failed_to_parse(format!("Unknown modifier: {k}")))?;
        let Bson::Document(fields) = v else {
            return Err(Error::failed_to_parse(format!(
                "Modifiers operate on fields but we found type {} instead. For example: {{$mod: {{<field>: ...}}}} not {{{k}: {}}}",
                type_name(v),
                fmt_bson(v)
            )));
        };
        for (field, arg) in fields {
            entries.push(parse_entry(op, k, field, arg)?);
        }
    }
    Ok(entries)
}

fn parse_entry(op: Op, op_name: &str, field: &str, arg: &Bson) -> Result<Entry> {
    let (target, dst) = if op == Op::Rename {
        rename_paths(field, arg)?
    } else {
        (Path::parse(field)?, None)
    };
    let prep = match op {
        Op::Inc | Op::Mul => {
            if !is_number(arg) {
                let verb = if op == Op::Inc { "increment" } else { "multiply" };
                return Err(Error::type_mismatch(format!(
                    "Cannot {verb} with non-numeric argument: {{{field}: {}}}",
                    fmt_bson(arg)
                )));
            }
            Prep::Plain
        }
        Op::Push => Prep::Push(parse_push(op_name, arg, false)?),
        Op::AddToSet => Prep::Push(parse_push(op_name, arg, true)?),
        Op::Pop => Prep::Pop(pop_dir(arg)?),
        Op::Pull => Prep::Pull(parse_pull(arg)?),
        Op::PullAll => match arg {
            Bson::Array(vals) => Prep::PullAll(vals.clone()),
            other => {
                return Err(Error::bad_value(format!(
                    "$pullAll requires an array argument but was given a {}",
                    type_name(other)
                )));
            }
        },
        Op::Bit => Prep::Bit(parse_bit(arg)?),
        Op::CurrentDate => Prep::CurrentDate(parse_current_date(arg)?),
        _ => Prep::Plain,
    };
    Ok(Entry { op, target, dst, arg: arg.clone(), prep })
}

fn rename_paths(field: &str, arg: &Bson) -> Result<(Path, Option<Path>)> {
    let Bson::String(to) = arg else {
        return Err(Error::bad_value(format!(
            "The 'to' field for $rename must be a string: {field}: {}",
            fmt_bson(arg)
        )));
    };
    if is_dynamic(field) {
        return Err(Error::bad_value(format!(
            "The source field for $rename may not be dynamic: {field}"
        )));
    }
    if is_dynamic(to) {
        return Err(Error::bad_value(format!(
            "The destination field for $rename may not be dynamic: {to}"
        )));
    }
    if field == to {
        return Err(Error::bad_value(format!(
            "The source and target field for $rename must differ: {field}"
        )));
    }
    Ok((Path::parse(field)?, Some(Path::parse(to)?)))
}

fn parse_push(name: &str, arg: &Bson, add_to_set: bool) -> Result<PushSpec> {
    let d = match arg {
        Bson::Document(d) if d.keys().next().is_some_and(|k| k.starts_with('$')) => d,
        _ => {
            return Ok(PushSpec {
                each: vec![arg.clone()],
                position: None,
                slice: None,
                sort: None,
            });
        }
    };
    let mut each = None;
    let mut position = None;
    let mut slice = None;
    let mut sort = None;
    for (k, v) in d {
        match k.as_str() {
            "$each" => match v {
                Bson::Array(a) => each = Some(a.clone()),
                other => {
                    return Err(Error::bad_value(format!(
                        "The argument to $each in {name} must be an array but it was of type {}",
                        type_name(other)
                    )));
                }
            },
            "$slice" if !add_to_set => slice = Some(int_modifier(k, v)?),
            "$position" if !add_to_set => position = Some(int_modifier(k, v)?),
            "$sort" if !add_to_set => sort = Some(parse_sort(v)?),
            _ => return Err(Error::bad_value(format!("Unrecognized clause in {name}: {k}"))),
        }
    }
    let each = each.ok_or_else(|| {
        Error::bad_value(format!("{name} with modifiers requires an $each clause"))
    })?;
    Ok(PushSpec { each, position, slice, sort })
}

fn int_modifier(k: &str, v: &Bson) -> Result<i64> {
    num_i64(v).ok_or_else(|| {
        Error::bad_value(format!(
            "The value for {k} must be an integer value but was given as {}",
            type_name(v)
        ))
    })
}

fn parse_sort(v: &Bson) -> Result<SortMode> {
    match v {
        Bson::Document(sd) => Ok(SortMode::Doc(SortSpec::new(sd)?)),
        other => match num_f64(other) {
            Some(n) if n == 1.0 => Ok(SortMode::Value(true)),
            Some(n) if n == -1.0 => Ok(SortMode::Value(false)),
            _ => Err(Error::bad_value(
                "The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields",
            )),
        },
    }
}

fn pop_dir(arg: &Bson) -> Result<i64> {
    match num_f64(arg) {
        Some(n) if n == 1.0 => Ok(1),
        Some(n) if n == -1.0 => Ok(-1),
        _ => Err(Error::failed_to_parse(format!(
            "$pop expects 1 or -1, found: {}",
            fmt_bson(arg)
        ))),
    }
}

fn parse_pull(arg: &Bson) -> Result<PullCond> {
    match arg {
        Bson::Document(d) if !d.keys().next().is_some_and(|k| k.starts_with('$')) => {
            Ok(PullCond::Doc(Matcher::new(d)?))
        }
        _ => {
            value_matches(&Bson::Null, arg)?;
            Ok(PullCond::Value(arg.clone()))
        }
    }
}

fn parse_bit(arg: &Bson) -> Result<Vec<(BitOp, i64, bool)>> {
    let Bson::Document(d) = arg else {
        return Err(Error::failed_to_parse(format!(
            "$bit expects an object argument, found: {}",
            fmt_bson(arg)
        )));
    };
    let mut ops = Vec::with_capacity(d.len());
    for (k, v) in d {
        let op = match k.as_str() {
            "and" => BitOp::And,
            "or" => BitOp::Or,
            "xor" => BitOp::Xor,
            _ => return Err(Error::failed_to_parse(format!("Unknown bit operation: {k}"))),
        };
        let (n, long) = match v {
            Bson::Int32(i) => (i64::from(*i), false),
            Bson::Int64(i) => (*i, true),
            other => {
                return Err(Error::bad_value(format!(
                    "The $bit operand must be an integer value but was given as {}",
                    type_name(other)
                )));
            }
        };
        ops.push((op, n, long));
    }
    Ok(ops)
}

/// Returns `true` for `$currentDate` with `$type: "timestamp"`, `false` for date.
fn parse_current_date(arg: &Bson) -> Result<bool> {
    match arg {
        Bson::Boolean(true) => Ok(false),
        Bson::Document(d) if d.len() == 1 => match d.get_str("$type") {
            Ok("date") => Ok(false),
            Ok("timestamp") => Ok(true),
            _ => Err(Error::bad_value(
                "$currentDate: value must be true or {$type: 'date' | 'timestamp'}",
            )),
        },
        _ => Err(Error::bad_value(
            "$currentDate: value must be true or {$type: 'date' | 'timestamp'}",
        )),
    }
}

fn parse_array_filters(list: Option<&Vec<Bson>>) -> Result<HashMap<String, ArrayFilter>> {
    let mut map: HashMap<String, ArrayFilter> = HashMap::new();
    for item in list.into_iter().flatten() {
        let Bson::Document(doc) = item else {
            return Err(Error::failed_to_parse(
                "Error parsing array filter: each array filter must be an object",
            ));
        };
        let mut id: Option<&str> = None;
        let mut value_cond = None;
        let mut rest = Document::new();
        for (k, v) in doc {
            let (head, tail) = match k.split_once('.') {
                Some((h, t)) => (h, Some(t)),
                None => (k.as_str(), None),
            };
            if !valid_ident(head) {
                return Err(Error::failed_to_parse(format!(
                    "Error parsing array filter: The top-level field name must be an alphanumeric string beginning with a lowercase letter, found {head}"
                )));
            }
            match id {
                Some(prev) if prev != head => {
                    return Err(Error::failed_to_parse(
                        "Error parsing array filter: all keys in an array filter must share the same top-level field name",
                    ));
                }
                _ => id = Some(head),
            }
            match tail {
                Some(t) => {
                    rest.insert(t, v.clone());
                }
                None => value_cond = Some(v.clone()),
            }
        }
        let Some(id) = id else {
            return Err(Error::failed_to_parse("Error parsing array filter: empty array filter"));
        };
        let filter = match value_cond {
            Some(cond) => {
                if doc.len() != 1 {
                    return Err(Error::failed_to_parse(
                        "Error parsing array filter: a condition on the identifier cannot be combined with other keys",
                    ));
                }
                ArrayFilter::Value(cond)
            }
            None => ArrayFilter::Doc(Matcher::new(&rest)?),
        };
        if map.insert(id.to_string(), filter).is_some() {
            return Err(Error::failed_to_parse(format!(
                "Found multiple array filters with the same top-level field name {id}"
            )));
        }
    }
    Ok(map)
}

fn check_filter_usage(entries: &[Entry], filters: &HashMap<String, ArrayFilter>) -> Result<()> {
    let mut used: HashSet<&str> = HashSet::new();
    for e in entries {
        for p in e.paths() {
            for seg in &p.segs {
                if let Seg::Filtered(id) = seg {
                    if !filters.contains_key(id) {
                        return Err(Error::bad_value(format!(
                            "No array filter found for identifier '{id}' in path '{}'",
                            p.raw
                        )));
                    }
                    used.insert(id.as_str());
                }
            }
        }
    }
    for id in filters.keys() {
        if !used.contains(id.as_str()) {
            return Err(Error::failed_to_parse(format!(
                "The array filter for identifier '{id}' was not used in the update"
            )));
        }
    }
    Ok(())
}

/// Reject pairs of paths where one is a prefix of the other (error 40).
fn check_conflicts(entries: &[Entry]) -> Result<()> {
    let mut all: Vec<(usize, &Path)> = Vec::new();
    for (i, e) in entries.iter().enumerate() {
        for p in e.paths() {
            all.push((i, p));
        }
    }
    for (a, (ea, pa)) in all.iter().enumerate() {
        for (eb, pb) in all.iter().skip(a + 1) {
            if ea == eb {
                continue;
            }
            let (ta, tb) = (pa.texts(), pb.texts());
            if !(ta.starts_with(&tb) || tb.starts_with(&ta)) {
                continue;
            }
            let (subject, at) = if ta.len() > tb.len() { (pa, pb) } else { (pb, pa) };
            return Err(Error::new(
                40,
                format!(
                    "Updating the path '{}' would create a conflict at '{}'",
                    subject.raw, at.raw
                ),
            ));
        }
    }
    Ok(())
}

impl Path {
    fn parse(raw: &str) -> Result<Path> {
        if raw.is_empty() {
            return Err(Error::new(56, "An empty update path is not valid."));
        }
        let mut segs = Vec::new();
        for (i, part) in raw.split('.').enumerate() {
            if part.is_empty() {
                return Err(Error::new(
                    56,
                    format!(
                        "The update path '{raw}' contains an empty field name, which is not allowed."
                    ),
                ));
            }
            if !part.starts_with('$') {
                segs.push(Seg::Field(part.to_string()));
                continue;
            }
            let seg = match part {
                "$" if i > 0 => Seg::Pos,
                "$[]" if i > 0 => Seg::All,
                _ => match part.strip_prefix("$[").and_then(|s| s.strip_suffix(']')) {
                    Some(id) if i > 0 => {
                        if !valid_ident(id) {
                            return Err(Error::bad_value(format!(
                                "Error parsing array filter: The top-level field name must be an alphanumeric string beginning with a lowercase letter, found {id}"
                            )));
                        }
                        Seg::Filtered(id.to_string())
                    }
                    _ => {
                        return Err(Error::new(
                            52,
                            format!(
                                "The dollar ($) prefixed field '{part}' in '{raw}' is not valid for storage."
                            ),
                        ));
                    }
                },
            };
            segs.push(seg);
        }
        Ok(Path { raw: raw.to_string(), segs })
    }

    fn texts(&self) -> Vec<String> {
        self.segs.iter().map(seg_text).collect()
    }
}

fn seg_text(s: &Seg) -> String {
    match s {
        Seg::Field(name) => name.clone(),
        Seg::Pos => "$".to_string(),
        Seg::All => "$[]".to_string(),
        Seg::Filtered(id) => format!("$[{id}]"),
    }
}

fn last_text(segs: &[Seg]) -> String {
    segs.last().map(seg_text).unwrap_or_default()
}

fn valid_ident(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_alphanumeric())
}

fn is_dynamic(raw: &str) -> bool {
    raw.split('.').any(|p| p.starts_with('$'))
}

fn apply_entry(out: &mut Document, e: &Entry, cx: &Cx<'_>) -> Result<()> {
    let Some(target) = Some(&e.target) else {
        return Err(Error::internal("update entry without a target path"));
    };
    let segs = &target.segs;
    match (e.op, &e.prep) {
        (Op::Set | Op::SetOnInsert, _) => {
            let mut leaf = |_: Option<&Bson>| -> Result<Action> { Ok(Action::Set(e.arg.clone())) };
            apply_doc(out, segs, cx, true, &mut leaf)
        }
        (Op::Unset, _) => {
            let mut leaf = |cur: Option<&Bson>| -> Result<Action> {
                Ok(if cur.is_some() { Action::Unset } else { Action::Keep })
            };
            apply_doc(out, segs, cx, false, &mut leaf)
        }
        (Op::Inc, _) => {
            let last = last_text(segs);
            let mut leaf = |cur: Option<&Bson>| -> Result<Action> {
                match cur {
                    None => Ok(Action::Set(e.arg.clone())),
                    Some(v) if is_number(v) => num_add(v, &e.arg)
                        .map(Action::Set)
                        .ok_or_else(|| arith_error(cx.raw, &e.arg)),
                    Some(v) => Err(non_numeric("$inc", cx.doc, &last, v)),
                }
            };
            apply_doc(out, segs, cx, true, &mut leaf)
        }
        (Op::Mul, _) => {
            let last = last_text(segs);
            let mut leaf = |cur: Option<&Bson>| -> Result<Action> {
                match cur {
                    None => Ok(Action::Set(zero_like(&e.arg))),
                    Some(v) if is_number(v) => num_mul(v, &e.arg)
                        .map(Action::Set)
                        .ok_or_else(|| arith_error(cx.raw, &e.arg)),
                    Some(v) => Err(non_numeric("$mul", cx.doc, &last, v)),
                }
            };
            apply_doc(out, segs, cx, true, &mut leaf)
        }
        (Op::Min | Op::Max, _) => {
            let want = if e.op == Op::Min { Ordering::Less } else { Ordering::Greater };
            let mut leaf = |cur: Option<&Bson>| -> Result<Action> {
                Ok(match cur {
                    None => Action::Set(e.arg.clone()),
                    Some(v) if cmp_bson(&e.arg, v) == want => Action::Set(e.arg.clone()),
                    Some(_) => Action::Keep,
                })
            };
            apply_doc(out, segs, cx, true, &mut leaf)
        }
        (Op::Rename, _) => match &e.dst {
            Some(dst) => apply_rename(out, &e.target, dst),
            None => Err(Error::internal("$rename without a destination")),
        },
        (Op::CurrentDate, Prep::CurrentDate(ts)) => {
            let ts = *ts;
            let mut leaf =
                |_: Option<&Bson>| -> Result<Action> { Ok(Action::Set(current_value(ts))) };
            apply_doc(out, segs, cx, true, &mut leaf)
        }
        (Op::Push | Op::AddToSet, Prep::Push(spec)) => {
            let add_to_set = e.op == Op::AddToSet;
            let mut leaf = |cur: Option<&Bson>| -> Result<Action> {
                let mut arr = array_for_update(cur, cx)?;
                if add_to_set {
                    for v in &spec.each {
                        if !arr.iter().any(|x| bson_eq(x, v)) {
                            arr.push(v.clone());
                        }
                    }
                } else {
                    push_modify(&mut arr, spec);
                }
                Ok(Action::Set(Bson::Array(arr)))
            };
            apply_doc(out, segs, cx, true, &mut leaf)
        }
        (Op::Pop, Prep::Pop(dir)) => {
            let dir = *dir;
            let mut leaf = |cur: Option<&Bson>| -> Result<Action> {
                match cur {
                    None => Ok(Action::Keep),
                    Some(Bson::Array(items)) => {
                        if items.is_empty() {
                            return Ok(Action::Keep);
                        }
                        let mut items = items.clone();
                        if dir > 0 {
                            items.pop();
                        } else {
                            items.remove(0);
                        }
                        Ok(Action::Set(Bson::Array(items)))
                    }
                    Some(other) => Err(Error::type_mismatch(format!(
                        "Path '{}' contains an element of non-array type '{}'",
                        cx.raw,
                        type_name(other)
                    ))),
                }
            };
            apply_doc(out, segs, cx, false, &mut leaf)
        }
        (Op::Pull, Prep::Pull(cond)) => {
            let mut leaf = |cur: Option<&Bson>| -> Result<Action> {
                match cur {
                    None => Ok(Action::Keep),
                    Some(Bson::Array(items)) => {
                        let mut kept = Vec::with_capacity(items.len());
                        let mut removed = false;
                        for item in items {
                            if pull_matches(item, cond)? {
                                removed = true;
                            } else {
                                kept.push(item.clone());
                            }
                        }
                        Ok(if removed { Action::Set(Bson::Array(kept)) } else { Action::Keep })
                    }
                    Some(_) => Err(Error::bad_value("Cannot apply $pull to a non-array value")),
                }
            };
            apply_doc(out, segs, cx, false, &mut leaf)
        }
        (Op::PullAll, Prep::PullAll(vals)) => {
            let mut leaf = |cur: Option<&Bson>| -> Result<Action> {
                match cur {
                    None => Ok(Action::Keep),
                    Some(Bson::Array(items)) => {
                        let kept: Vec<Bson> = items
                            .iter()
                            .filter(|x| !vals.iter().any(|v| bson_eq(x, v)))
                            .cloned()
                            .collect();
                        if kept.len() == items.len() {
                            Ok(Action::Keep)
                        } else {
                            Ok(Action::Set(Bson::Array(kept)))
                        }
                    }
                    Some(_) => Err(Error::bad_value("Cannot apply $pullAll to a non-array value")),
                }
            };
            apply_doc(out, segs, cx, false, &mut leaf)
        }
        (Op::Bit, Prep::Bit(ops)) => {
            let mut leaf = |cur: Option<&Bson>| -> Result<Action> {
                let (base, mut long) = match cur {
                    None => (0_i64, false),
                    Some(Bson::Int32(v)) => (i64::from(*v), false),
                    Some(Bson::Int64(v)) => (*v, true),
                    Some(_) => {
                        return Err(Error::type_mismatch(
                            "Cannot apply $bit to a value of non-integral type.",
                        ));
                    }
                };
                let mut acc = base;
                for &(op, n, l) in ops {
                    long |= l;
                    acc = match op {
                        BitOp::And => acc & n,
                        BitOp::Or => acc | n,
                        BitOp::Xor => acc ^ n,
                    };
                }
                Ok(Action::Set(if long { Bson::Int64(acc) } else { Bson::Int32(acc as i32) }))
            };
            apply_doc(out, segs, cx, true, &mut leaf)
        }
        _ => Err(Error::internal("update operator was prepared inconsistently")),
    }
}

fn pull_matches(elem: &Bson, cond: &PullCond) -> Result<bool> {
    match cond {
        PullCond::Doc(m) => match elem {
            Bson::Document(d) => m.matches(d),
            _ => Ok(false),
        },
        PullCond::Value(c) => value_matches(elem, c),
    }
}

fn current_value(timestamp: bool) -> Bson {
    if timestamp {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Bson::Timestamp(Timestamp { time: secs as u32, increment: 0 })
    } else {
        Bson::DateTime(DateTime::now())
    }
}

fn array_for_update(cur: Option<&Bson>, cx: &Cx<'_>) -> Result<Vec<Bson>> {
    match cur {
        None => Ok(Vec::new()),
        Some(Bson::Array(a)) => Ok(a.clone()),
        Some(other) => Err(Error::bad_value(format!(
            "The field '{}' must be an array but is of type {} in document {}",
            cx.raw,
            type_name(other),
            fmt_id(cx.doc)
        ))),
    }
}

fn push_modify(arr: &mut Vec<Bson>, spec: &PushSpec) {
    let len = arr.len() as i64;
    let at = match spec.position {
        None => arr.len(),
        Some(p) if p < 0 => (len + p).max(0) as usize,
        Some(p) => p.min(len) as usize,
    };
    let tail = arr.split_off(at);
    arr.extend(spec.each.iter().cloned());
    arr.extend(tail);
    match &spec.sort {
        None => {}
        Some(SortMode::Value(asc)) => arr.sort_by(|a, b| {
            let o = cmp_bson(a, b);
            if *asc { o } else { o.reverse() }
        }),
        Some(SortMode::Doc(sp)) => {
            let empty = Document::new();
            arr.sort_by(|a, b| sp.compare(as_doc(a, &empty), as_doc(b, &empty)));
        }
    }
    if let Some(n) = spec.slice {
        if n >= 0 {
            arr.truncate(n as usize);
        } else {
            let keep = n.unsigned_abs() as usize;
            if arr.len() > keep {
                let cut = arr.len() - keep;
                let kept = arr.split_off(cut);
                *arr = kept;
            }
        }
    }
}

fn as_doc<'a>(b: &'a Bson, empty: &'a Document) -> &'a Document {
    match b {
        Bson::Document(d) => d,
        _ => empty,
    }
}

fn apply_rename(out: &mut Document, src: &Path, dst: &Path) -> Result<()> {
    check_rename_intermediates(out, src, "source")?;
    check_rename_intermediates(out, dst, "destination")?;
    let Some(val) = get_path(out, &src.raw).cloned() else {
        return Ok(());
    };
    remove_path(out, &src.raw);
    set_path(out, &dst.raw, val)
}

fn check_rename_intermediates(doc: &Document, path: &Path, role: &str) -> Result<()> {
    let names: Vec<&str> = path.raw.split('.').collect();
    let mut cur = doc;
    for (i, name) in names.iter().enumerate() {
        if i + 1 == names.len() {
            break;
        }
        match cur.get(*name) {
            Some(Bson::Document(d)) => cur = d,
            Some(Bson::Array(_)) => {
                return Err(Error::bad_value(format!(
                    "The {role} field cannot be an array element, '{}' in doc with {} has an array field called '{name}'",
                    path.raw,
                    fmt_id(doc)
                )));
            }
            _ => break,
        }
    }
    Ok(())
}

fn shape_of(v: Option<&Bson>) -> Shape {
    match v {
        None => Shape::Missing,
        Some(Bson::Document(_)) => Shape::Doc,
        Some(Bson::Array(_)) => Shape::Arr,
        Some(_) => Shape::Other,
    }
}

fn must_exist(cx: &Cx<'_>) -> Error {
    Error::bad_value(format!(
        "The path '{}' must exist in the document in order to apply array updates.",
        cx.raw
    ))
}

fn filter_matches(f: &ArrayFilter, elem: &Bson) -> Result<bool> {
    match f {
        ArrayFilter::Value(cond) => value_matches(elem, cond),
        ArrayFilter::Doc(m) => match elem {
            Bson::Document(d) => m.matches(d),
            _ => Ok(false),
        },
    }
}

/// Walk `segs` from a document. `creating`: missing intermediates are created.
fn apply_doc(
    doc: &mut Document,
    segs: &[Seg],
    cx: &Cx<'_>,
    creating: bool,
    leaf: &mut LeafFn<'_>,
) -> Result<()> {
    let (name, rest) = match segs.split_first() {
        Some((Seg::Field(name), rest)) => (name.as_str(), rest),
        _ => {
            return Err(Error::bad_value(format!(
                "The positional operator in the update path '{}' must follow an array field.",
                cx.raw
            )));
        }
    };
    if rest.is_empty() {
        match leaf(doc.get(name))? {
            Action::Keep => {}
            Action::Set(v) => {
                doc.insert(name, v);
            }
            Action::Unset => {
                doc.remove(name);
            }
        }
        return Ok(());
    }
    let next_is_field = matches!(rest.first(), Some(Seg::Field(_)));
    match shape_of(doc.get(name)) {
        Shape::Missing => {
            if !creating {
                return Ok(());
            }
            if !next_is_field {
                return Err(must_exist(cx));
            }
            let mut child = Document::new();
            apply_doc(&mut child, rest, cx, creating, leaf)?;
            doc.insert(name, child);
            Ok(())
        }
        Shape::Doc => match doc.get_mut(name) {
            Some(Bson::Document(child)) => apply_doc(child, rest, cx, creating, leaf),
            _ => Ok(()),
        },
        Shape::Arr => match doc.get_mut(name) {
            Some(Bson::Array(arr)) => apply_arr(arr, rest, cx, creating, leaf),
            _ => Ok(()),
        },
        Shape::Other => {
            if !creating {
                return Ok(());
            }
            let field = rest.first().map(seg_text).unwrap_or_default();
            Err(Error::new(
                28,
                format!("Cannot create field '{field}' in element {}", fmt_doc(doc)),
            ))
        }
    }
}

/// Walk `segs` from an array value.
fn apply_arr(
    arr: &mut Vec<Bson>,
    segs: &[Seg],
    cx: &Cx<'_>,
    creating: bool,
    leaf: &mut LeafFn<'_>,
) -> Result<()> {
    let Some((head, rest)) = segs.split_first() else {
        return Ok(());
    };
    match head {
        Seg::Field(name) => match name.parse::<usize>() {
            Ok(i) => {
                let fresh = i >= arr.len();
                if fresh {
                    if !creating {
                        return Ok(());
                    }
                    while arr.len() <= i {
                        arr.push(Bson::Null);
                    }
                }
                apply_elem(arr, i, fresh, rest, cx, creating, leaf)
            }
            Err(_) => {
                if creating {
                    return Err(Error::new(
                        28,
                        format!(
                            "cannot use the part ({name} of {}) to traverse the element ({})",
                            cx.raw,
                            fmt_bson(&Bson::Array(arr.clone()))
                        ),
                    ));
                }
                Ok(())
            }
        },
        Seg::Pos => {
            let Some(p) = cx.position else {
                return Err(Error::bad_value(
                    "The positional operator did not find the match needed from the query.",
                ));
            };
            if p < arr.len() {
                apply_elem(arr, p, false, rest, cx, creating, leaf)
            } else {
                Ok(())
            }
        }
        Seg::All => {
            for i in 0..arr.len() {
                apply_elem(arr, i, false, rest, cx, creating, leaf)?;
            }
            Ok(())
        }
        Seg::Filtered(id) => {
            let Some(filter) = cx.filters.get(id) else {
                return Err(Error::bad_value(format!(
                    "No array filter found for identifier '{id}' in path '{}'",
                    cx.raw
                )));
            };
            for i in 0..arr.len() {
                if filter_matches(filter, &arr[i])? {
                    apply_elem(arr, i, false, rest, cx, creating, leaf)?;
                }
            }
            Ok(())
        }
    }
}

/// Apply to element `i` of `arr`. `fresh`: element was just padded and does not exist yet.
fn apply_elem(
    arr: &mut Vec<Bson>,
    i: usize,
    fresh: bool,
    rest: &[Seg],
    cx: &Cx<'_>,
    creating: bool,
    leaf: &mut LeafFn<'_>,
) -> Result<()> {
    if rest.is_empty() {
        let action = leaf(if fresh { None } else { Some(&arr[i]) })?;
        match action {
            Action::Keep => {}
            Action::Set(v) => arr[i] = v,
            Action::Unset => arr[i] = Bson::Null,
        }
        return Ok(());
    }
    if fresh {
        if !matches!(rest.first(), Some(Seg::Field(_))) {
            return Err(must_exist(cx));
        }
        arr[i] = Bson::Document(Document::new());
    }
    match shape_of(Some(&arr[i])) {
        Shape::Doc => match &mut arr[i] {
            Bson::Document(d) => apply_doc(d, rest, cx, creating, leaf),
            _ => Ok(()),
        },
        Shape::Arr => match &mut arr[i] {
            Bson::Array(a) => apply_arr(a, rest, cx, creating, leaf),
            _ => Ok(()),
        },
        Shape::Missing | Shape::Other => {
            if !creating {
                return Ok(());
            }
            let field = rest.first().map(seg_text).unwrap_or_default();
            Err(Error::new(
                28,
                format!(
                    "Cannot create field '{field}' in element {}",
                    fmt_bson(&Bson::Array(arr.clone()))
                ),
            ))
        }
    }
}

fn non_numeric(op: &str, doc: &Document, field: &str, v: &Bson) -> Error {
    Error::type_mismatch(format!(
        "Cannot apply {op} to a value of non-numeric type. {} has the field '{field}' of non-numeric type {}",
        fmt_id(doc),
        type_name(v)
    ))
}

fn arith_error(raw: &str, arg: &Bson) -> Error {
    Error::bad_value(format!(
        "Cannot apply arithmetic update to '{raw}' with {}",
        fmt_bson(arg)
    ))
}

fn zero_like(arg: &Bson) -> Bson {
    match arg {
        Bson::Int64(_) => Bson::Int64(0),
        Bson::Double(_) => Bson::Double(0.0),
        Bson::Decimal128(_) => Bson::Decimal128(f64_to_decimal(0.0)),
        _ => Bson::Int32(0),
    }
}

fn fmt_bson(v: &Bson) -> String {
    match v {
        Bson::Document(d) => fmt_doc(d),
        Bson::Array(a) => format!(
            "[{}]",
            a.iter().map(fmt_bson).collect::<Vec<_>>().join(", ")
        ),
        Bson::String(s) => format!("\"{s}\""),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => f.to_string(),
        Bson::Boolean(b) => b.to_string(),
        Bson::Null => "null".to_string(),
        Bson::ObjectId(oid) => format!("ObjectId('{}')", oid.to_hex()),
        other => format!("{other:?}"),
    }
}

fn fmt_doc(d: &Document) -> String {
    format!(
        "{{{}}}",
        d.iter()
            .map(|(k, v)| format!("{k}: {}", fmt_bson(v)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// `{_id: <value>}` when the document has an `_id`, otherwise the whole document.
fn fmt_id(d: &Document) -> String {
    match d.get("_id") {
        Some(id) => format!("{{_id: {}}}", fmt_bson(id)),
        None => fmt_doc(d),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn run(update: Document, doc: Document) -> Result<Document> {
        UpdateDoc::new(&Bson::Document(update), None)?.apply(&doc, None, false, &Vars::new())
    }

    fn err_code(r: Result<Document>) -> i32 {
        match r {
            Ok(d) => panic!("expected error, got {d:?}"),
            Err(e) => e.code,
        }
    }

    fn ints(v: &[i32]) -> Bson {
        Bson::Array(v.iter().map(|i| Bson::Int32(*i)).collect())
    }

    #[test]
    fn replacement_keeps_original_id() {
        let u = UpdateDoc::new(&Bson::Document(doc! {"a": 1}), None).unwrap();
        assert!(u.is_replacement());
        let out = u.apply(&doc! {"_id": 1, "b": 2}, None, false, &Vars::new()).unwrap();
        assert_eq!(out, doc! {"_id": 1, "a": 1});
        assert_eq!(out.keys().next().map(String::as_str), Some("_id"));
    }

    #[test]
    fn replacement_rejects_id_change() {
        let u = UpdateDoc::new(&Bson::Document(doc! {"_id": 2}), None).unwrap();
        assert_eq!(err_code(u.apply(&doc! {"_id": 1}, None, false, &Vars::new())), 66);
    }

    #[test]
    fn replacement_rejects_dollar_fields() {
        assert_eq!(err_code(run(doc! {"a": 1, "$x": 1}, doc! {"_id": 1})), 52);
    }

    #[test]
    fn set_creates_nested_and_orders_new_fields() {
        let out = run(doc! {"$set": {"b.c": 1, "a": 2}}, doc! {"_id": 1}).unwrap();
        let keys: Vec<&str> = out.keys().map(String::as_str).collect();
        assert_eq!(keys, ["_id", "a", "b"]);
        assert_eq!(out.get_document("b").unwrap().get_i32("c").unwrap(), 1);
    }

    #[test]
    fn set_through_scalar_fails_with_code_28() {
        match run(doc! {"$set": {"a.b.c": 5}}, doc! {"_id": 1, "a": {"b": 1}}) {
            Err(e) => {
                assert_eq!(e.code, 28);
                assert_eq!(e.msg, "Cannot create field 'c' in element {b: 1}");
            }
            Ok(d) => panic!("expected error, got {d:?}"),
        }
    }

    #[test]
    fn set_on_id_with_different_value_fails() {
        assert_eq!(err_code(run(doc! {"$set": {"_id": 2}}, doc! {"_id": 1})), 66);
    }

    #[test]
    fn unset_array_element_leaves_null() {
        let out = run(doc! {"$unset": {"a.1": ""}}, doc! {"_id": 1, "a": [1, 2, 3]}).unwrap();
        assert_eq!(out.get("a"), Some(&ints_with_null()));
    }

    fn ints_with_null() -> Bson {
        Bson::Array(vec![Bson::Int32(1), Bson::Null, Bson::Int32(3)])
    }

    #[test]
    fn inc_promotes_and_rejects_strings() {
        let out = run(doc! {"$inc": {"n": 2.5}}, doc! {"_id": 1, "n": 1}).unwrap();
        assert_eq!(out.get("n"), Some(&Bson::Double(3.5)));
        let out = run(doc! {"$inc": {"n": 2}}, doc! {"_id": 1}).unwrap();
        assert_eq!(out.get("n"), Some(&Bson::Int32(2)));
        assert_eq!(err_code(run(doc! {"$inc": {"n": 1}}, doc! {"_id": 1, "n": "x"})), 14);
    }

    #[test]
    fn mul_on_missing_field_sets_zero() {
        let out = run(doc! {"$mul": {"n": 3}}, doc! {"_id": 1}).unwrap();
        assert_eq!(out.get("n"), Some(&Bson::Int32(0)));
    }

    #[test]
    fn min_and_max_respect_ordering() {
        let out = run(doc! {"$min": {"n": 2}, "$max": {"m": 7}}, doc! {"_id": 1, "n": 5}).unwrap();
        assert_eq!(out.get("n"), Some(&Bson::Int32(2)));
        assert_eq!(out.get("m"), Some(&Bson::Int32(7)));
    }

    #[test]
    fn rename_moves_field() {
        let out = run(doc! {"$rename": {"a": "b"}}, doc! {"_id": 1, "a": 1}).unwrap();
        assert_eq!(out, doc! {"_id": 1, "b": 1});
    }

    #[test]
    fn push_each_slice_and_position() {
        let out = run(
            doc! {"$push": {"a": {"$each": [3, 4], "$slice": -2}}},
            doc! {"_id": 1, "a": [1, 2]},
        )
        .unwrap();
        assert_eq!(out.get("a"), Some(&ints(&[3, 4])));
        let out = run(
            doc! {"$push": {"a": {"$each": [9], "$position": 1}}},
            doc! {"_id": 1, "a": [1, 2]},
        )
        .unwrap();
        assert_eq!(out.get("a"), Some(&ints(&[1, 9, 2])));
    }

    #[test]
    fn push_sort_descending() {
        let out = run(
            doc! {"$push": {"a": {"$each": [5, 0], "$sort": -1}}},
            doc! {"_id": 1, "a": [1, 2]},
        )
        .unwrap();
        assert_eq!(out.get("a"), Some(&ints(&[5, 2, 1, 0])));
    }

    #[test]
    fn add_to_set_dedupes() {
        let out = run(
            doc! {"$addToSet": {"a": {"$each": [2, 3, 3]}}},
            doc! {"_id": 1, "a": [1, 2]},
        )
        .unwrap();
        assert_eq!(out.get("a"), Some(&ints(&[1, 2, 3])));
    }

    #[test]
    fn pop_first_and_last() {
        let out = run(doc! {"$pop": {"a": 1}}, doc! {"_id": 1, "a": [1, 2, 3]}).unwrap();
        assert_eq!(out.get("a"), Some(&ints(&[1, 2])));
        let out = run(doc! {"$pop": {"a": -1}}, doc! {"_id": 1, "a": [1, 2, 3]}).unwrap();
        assert_eq!(out.get("a"), Some(&ints(&[2, 3])));
    }

    #[test]
    fn pull_value_and_document_condition() {
        let out = run(doc! {"$pull": {"a": 2}}, doc! {"_id": 1, "a": [1, 2, 3, 2]}).unwrap();
        assert_eq!(out.get("a"), Some(&ints(&[1, 3])));
        let out = run(
            doc! {"$pull": {"a": {"b": {"$gt": 1}}}},
            doc! {"_id": 1, "a": [{"b": 1}, {"b": 2}]},
        )
        .unwrap();
        assert_eq!(
            out.get("a"),
            Some(&Bson::Array(vec![Bson::Document(doc! {"b": 1})]))
        );
    }

    #[test]
    fn pull_all_removes_listed_values() {
        let out = run(doc! {"$pullAll": {"a": [1, 3]}}, doc! {"_id": 1, "a": [1, 2, 3]}).unwrap();
        assert_eq!(out.get("a"), Some(&ints(&[2])));
    }

    #[test]
    fn positional_uses_matched_index() {
        let d = doc! {"_id": 1, "a": [{"x": 1}, {"x": 2}]};
        let u = UpdateDoc::new(&Bson::Document(doc! {"$set": {"a.$.x": 9}}), None).unwrap();
        let out = u.apply(&d, Some(1), false, &Vars::new()).unwrap();
        assert_eq!(
            out.get("a"),
            Some(&Bson::Array(vec![
                Bson::Document(doc! {"x": 1}),
                Bson::Document(doc! {"x": 9}),
            ]))
        );
        assert_eq!(err_code(u.apply(&d, None, false, &Vars::new())), 2);
    }

    #[test]
    fn all_positional_updates_every_element() {
        let out = run(
            doc! {"$set": {"a.$[].x": 0}},
            doc! {"_id": 1, "a": [{"x": 1}, {"x": 2}]},
        )
        .unwrap();
        assert_eq!(
            out.get("a"),
            Some(&Bson::Array(vec![
                Bson::Document(doc! {"x": 0}),
                Bson::Document(doc! {"x": 0}),
            ]))
        );
    }

    #[test]
    fn filtered_positional_uses_array_filters() {
        let filters = vec![Bson::Document(doc! {"e.x": {"$gt": 1}})];
        let u = UpdateDoc::new(&Bson::Document(doc! {"$set": {"a.$[e].x": 0}}), Some(&filters))
            .unwrap();
        let out = u
            .apply(&doc! {"_id": 1, "a": [{"x": 1}, {"x": 2}]}, None, false, &Vars::new())
            .unwrap();
        assert_eq!(
            out.get("a"),
            Some(&Bson::Array(vec![
                Bson::Document(doc! {"x": 1}),
                Bson::Document(doc! {"x": 0}),
            ]))
        );
    }

    #[test]
    fn conflicting_paths_fail_with_code_40() {
        assert_eq!(err_code(run(doc! {"$set": {"a": 1, "a.b": 2}}, doc! {"_id": 1})), 40);
    }

    #[test]
    fn unknown_modifier_fails_with_code_9() {
        assert_eq!(err_code(run(doc! {"$foo": {"a": 1}}, doc! {"_id": 1})), 9);
        assert_eq!(err_code(run(doc! {"$set": {"a": 1}, "b": 2}, doc! {"_id": 1})), 9);
    }

    #[test]
    fn set_on_insert_only_applies_on_insert() {
        let u = UpdateDoc::new(&Bson::Document(doc! {"$setOnInsert": {"n": 1}}), None).unwrap();
        let seed = doc! {"_id": 1};
        assert_eq!(u.apply(&seed, None, false, &Vars::new()).unwrap(), seed);
        let inserted = u.apply(&seed, None, true, &Vars::new()).unwrap();
        assert_eq!(inserted.get("n"), Some(&Bson::Int32(1)));
    }

    #[test]
    fn bit_and_applies_to_int() {
        let out = run(doc! {"$bit": {"n": {"and": 3}}}, doc! {"_id": 1, "n": 6}).unwrap();
        assert_eq!(out.get("n"), Some(&Bson::Int32(2)));
    }

    #[test]
    fn current_date_sets_date() {
        let out = run(doc! {"$currentDate": {"t": true}}, doc! {"_id": 1}).unwrap();
        assert!(matches!(out.get("t"), Some(Bson::DateTime(_))));
    }

    #[test]
    fn upsert_seed_collects_equalities() {
        let seed = upsert_seed(&doc! {
            "c": {"$eq": 2},
            "a.b": 1,
            "_id": 5,
            "d": {"$gt": 3},
            "$and": [{"e": 4}],
        })
        .unwrap();
        assert_eq!(seed.keys().next().map(String::as_str), Some("_id"));
        assert_eq!(seed.get("c"), Some(&Bson::Int32(2)));
        assert_eq!(seed.get_document("a").unwrap().get_i32("b").unwrap(), 1);
        assert_eq!(seed.get("e"), Some(&Bson::Int32(4)));
        assert!(seed.get("d").is_none());
    }
}
