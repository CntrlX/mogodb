//! Query planner: index bounds, histogram-based selectivity, sort-providing and covered
//! index plans, and MongoDB-shaped explain output.

use crate::bsonutil;
use crate::collation::Collation;
use crate::error::{Error, Result};
use crate::keys;
use crate::query::sort::{SortKey, SortSpec};
use crate::storage::{Coll, Index, IndexKind, Store};
use bson::{Bson, Document, doc};
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub type Interval = (Bound<Vec<u8>>, Bound<Vec<u8>>);

/// An index access path.
#[derive(Clone)]
pub struct IxPlan {
    pub index: Arc<Index>,
    /// Key intervals in ascending byte order.
    pub ranges: Vec<Interval>,
    /// Scan the ranges backward (descending key order).
    pub reverse: bool,
    /// Explain `indexBounds` (`{field: ["[3, 3]"]}`).
    pub bounds: Document,
    /// Index order satisfies the requested sort.
    pub provides_sort: bool,
    /// Documents are reconstructed from index keys (no fetch).
    pub covered: bool,
    /// `_id` point lookup (8.0+ express path).
    pub express: bool,
    /// Index fields whose predicates are fully enforced by the bounds.
    pub bound_fields: Vec<String>,
    /// Index entries visited (explain `keysExamined`).
    pub keys_examined: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Clone)]
pub enum Plan {
    Empty,
    CollScan { reverse: bool },
    IxScan(IxPlan),
}

impl Plan {
    pub fn index_name(&self) -> Option<&str> {
        match self {
            Plan::IxScan(p) => Some(&p.index.name),
            _ => None,
        }
    }
    pub fn provides_sort(&self) -> bool {
        matches!(self, Plan::IxScan(p) if p.provides_sort)
    }
    pub fn is_covered(&self) -> bool {
        matches!(self, Plan::IxScan(p) if p.covered)
    }
}

/// Optional query context that lets the planner use an index for sorting or covering.
#[derive(Default, Clone, Copy)]
pub struct QueryShape<'a> {
    pub sort: Option<&'a Document>,
    pub projection: Option<&'a Document>,
    /// skip + limit when a limit applies.
    pub limit: Option<usize>,
}

// ------------------------------------------------------------------ predicates

#[derive(Default, Debug, Clone)]
struct FieldPred {
    points: Option<Vec<Bson>>,
    /// Ascending-encoded single-field interval.
    range: Option<Interval>,
    /// Source values of the range (value, inclusive) for explain.
    lo: Option<(Bson, bool)>,
    hi: Option<(Bson, bool)>,
    /// Anchored regex prefix used as bounds, and the regex itself (a stored regex value
    /// equal to the pattern also matches).
    regex_prefix: Option<String>,
    regex_value: Option<Bson>,
    /// The regex is exactly `^prefix` without options: bounds are tight.
    regex_exact: bool,
    empty: bool,
    /// A bound value contains a string (collation-sensitive).
    has_string: bool,
}

fn indexable_point(v: &Bson) -> bool {
    !matches!(v, Bson::Array(_) | Bson::Null | Bson::Undefined | Bson::RegularExpression(_) | Bson::MinKey | Bson::MaxKey)
        && !matches!(v, Bson::Document(d) if d.keys().any(|k| k.starts_with('$')))
}

fn collect_preds(filter: &Document, out: &mut HashMap<String, FieldPred>, coll: Option<&Collation>) {
    for (k, v) in filter {
        if k == "$and" {
            if let Bson::Array(a) = v {
                for e in a {
                    if let Bson::Document(d) = e {
                        collect_preds(d, out, coll);
                    }
                }
            }
            continue;
        }
        if k.starts_with('$') {
            continue;
        }
        let p = out.entry(k.clone()).or_default();
        match v {
            Bson::Document(d) if d.keys().next().is_some_and(|f| f.starts_with('$')) => {
                for (op, arg) in d {
                    match op.as_str() {
                        "$eq" if indexable_point(arg) => {
                            p.has_string |= keys::has_string(arg);
                            p.points = Some(vec![arg.clone()]);
                        }
                        "$in" => {
                            if let Bson::Array(a) = arg {
                                if a.iter().all(indexable_point) && p.points.is_none() {
                                    p.has_string |= a.iter().any(keys::has_string);
                                    let mut pts = a.clone();
                                    pts.sort_by(bsonutil::cmp_bson);
                                    pts.dedup_by(|x, y| bsonutil::bson_eq(x, y));
                                    p.points = Some(pts);
                                }
                            }
                        }
                        "$gt" | "$gte" | "$lt" | "$lte" if indexable_point(arg) && !matches!(arg, Bson::Document(_)) => {
                            p.has_string |= keys::has_string(arg);
                            let enc = keys::encode_one_c(arg, coll);
                            let tag = keys::tag_of(arg);
                            let (lo, hi): Interval = match op.as_str() {
                                "$gt" => (succ_incl(&enc), Bound::Excluded(vec![tag + 1])),
                                "$gte" => (Bound::Included(enc), Bound::Excluded(vec![tag + 1])),
                                "$lt" => (Bound::Included(vec![tag]), Bound::Excluded(enc)),
                                _ => (Bound::Included(vec![tag]), succ_excl(&enc)),
                            };
                            match op.as_str() {
                                "$gt" | "$gte" => p.lo = Some((arg.clone(), op == "$gte")),
                                _ => p.hi = Some((arg.clone(), op == "$lte")),
                            }
                            p.range = Some(match p.range.take() {
                                None => (lo, hi),
                                Some((l0, h0)) => (max_lo(l0, lo), min_hi(h0, hi)),
                            });
                            if let Some((l, h)) = &p.range {
                                if interval_empty(l, h) {
                                    p.empty = true;
                                }
                            }
                        }
                        "$regex" if coll.is_none() => {
                            let opts = d.get_str("$options").unwrap_or("");
                            match arg {
                                Bson::String(pat) => set_regex(p, pat, opts),
                                Bson::RegularExpression(r) => set_regex(p, &r.pattern, &r.options),
                                _ => {}
                            }
                        }
                        _ => {}
                    }
                }
            }
            Bson::RegularExpression(r) if coll.is_none() => set_regex(p, &r.pattern, &r.options),
            v if indexable_point(v) => {
                p.has_string |= keys::has_string(v);
                p.points = Some(vec![v.clone()]);
            }
            _ => {}
        }
    }
}

/// Paths a filter constrains, or `None` if it uses operators that need whole documents
/// (`$or`, `$expr`, `$text`, `$where`, ...). Used for covered-plan eligibility.
fn filter_paths(filter: &Document, out: &mut Vec<String>) -> bool {
    for (k, v) in filter {
        match k.as_str() {
            "$and" => {
                let Bson::Array(a) = v else { return false };
                for e in a {
                    let Bson::Document(d) = e else { return false };
                    if !filter_paths(d, out) {
                        return false;
                    }
                }
            }
            "$comment" => {}
            k if k.starts_with('$') => return false,
            k => {
                // Null predicates must fetch to tell a null value from a missing field.
                let is_null = |x: &Bson| matches!(x, Bson::Null | Bson::Undefined);
                if is_null(v) {
                    return false;
                }
                if let Bson::Document(d) = v {
                    if d.keys().any(|op| matches!(op.as_str(), "$elemMatch" | "$size" | "$all" | "$near" | "$nearSphere" | "$geoWithin" | "$geoIntersects" | "$exists" | "$type")) {
                        return false;
                    }
                    if d.iter().any(|(op, x)| match op.as_str() {
                        "$eq" | "$ne" | "$gte" | "$lte" => is_null(x),
                        "$in" | "$nin" => x.as_array().is_some_and(|a| a.iter().any(is_null)),
                        _ => false,
                    }) {
                        return false;
                    }
                }
                out.push(k.to_string());
            }
        }
    }
    true
}

fn set_regex(p: &mut FieldPred, pat: &str, opts: &str) {
    if let Some(pre) = regex_prefix(pat, opts) {
        if p.range.is_none() {
            let mut lo = keys::escaped_prefix(pre.as_bytes());
            let iv = prefix_interval(&lo);
            lo.clear();
            p.range = Some(iv);
            p.regex_exact = opts.is_empty() && pat.len() == pre.len() + 1 && pre.chars().all(|c| c.is_alphanumeric() || c == ' ' || c == '_');
            p.regex_value = Some(Bson::RegularExpression(bson::Regex { pattern: pat.to_string(), options: opts.to_string() }));
            p.regex_prefix = Some(pre);
        }
    }
}

/// Literal prefix of an anchored regex (`^abc...`) without case-insensitive/extended options.
fn regex_prefix(pat: &str, opts: &str) -> Option<String> {
    if opts.contains('i') || opts.contains('x') || opts.contains('m') {
        return None;
    }
    let rest = pat.strip_prefix('^')?;
    let mut pre = String::new();
    for ch in rest.chars() {
        if "\\^$.|?*+()[]{}".contains(ch) {
            break;
        }
        pre.push(ch);
    }
    if let Some(next) = rest[pre.len()..].chars().next() {
        if "?*{".contains(next) {
            pre.pop();
        }
    }
    (!pre.is_empty()).then_some(pre)
}

fn prefix_interval(p: &[u8]) -> Interval {
    (Bound::Included(p.to_vec()), succ_excl(p))
}

fn succ_incl(p: &[u8]) -> Bound<Vec<u8>> {
    match keys::prefix_successor(p) {
        Some(s) => Bound::Included(s),
        None => Bound::Excluded(p.to_vec()),
    }
}

pub(crate) fn succ_excl(p: &[u8]) -> Bound<Vec<u8>> {
    match keys::prefix_successor(p) {
        Some(s) => Bound::Excluded(s),
        None => Bound::Unbounded,
    }
}

fn bound_val(b: &Bound<Vec<u8>>) -> Option<&Vec<u8>> {
    match b {
        Bound::Included(v) | Bound::Excluded(v) => Some(v),
        Bound::Unbounded => None,
    }
}

fn max_lo(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (bound_val(&a), bound_val(&b)) {
        (None, _) => b,
        (_, None) => a,
        (Some(x), Some(y)) => {
            if x > y || (x == y && matches!(a, Bound::Excluded(_))) { a } else { b }
        }
    }
}

fn min_hi(a: Bound<Vec<u8>>, b: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match (bound_val(&a), bound_val(&b)) {
        (None, _) => b,
        (_, None) => a,
        (Some(x), Some(y)) => {
            if x < y || (x == y && matches!(a, Bound::Excluded(_))) { a } else { b }
        }
    }
}

fn interval_empty(lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) -> bool {
    match (lo, hi) {
        (Bound::Included(l), Bound::Included(h)) => l > h,
        (Bound::Included(l) | Bound::Excluded(l), Bound::Excluded(h)) | (Bound::Excluded(l), Bound::Included(h)) => l >= h,
        _ => false,
    }
}

fn concat_bound(prefix: &[u8], b: Bound<Vec<u8>>, is_hi: bool) -> Bound<Vec<u8>> {
    match b {
        Bound::Included(v) => Bound::Included([prefix, &v].concat()),
        Bound::Excluded(v) => Bound::Excluded([prefix, &v].concat()),
        Bound::Unbounded if prefix.is_empty() => Bound::Unbounded,
        Bound::Unbounded if is_hi => succ_excl(prefix),
        Bound::Unbounded => Bound::Included(prefix.to_vec()),
    }
}

// ------------------------------------------------------------------ explain bounds

/// MongoDB's rendering of a bound value (`3`, `"abc"`, `inf`, `MinKey`, `ObjectId('..')`).
fn fmt_value(v: &Bson) -> String {
    match v {
        Bson::Double(d) if d.is_infinite() => if *d > 0.0 { "inf".into() } else { "-inf".into() },
        Bson::Double(d) if d.is_nan() => "nan".into(),
        Bson::Double(d) => {
            if d.fract() == 0.0 && d.abs() < 1e15 { format!("{}.0", *d as i64) } else { format!("{d}") }
        }
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Decimal128(d) => d.to_string(),
        Bson::String(s) => format!("\"{s}\""),
        Bson::MinKey => "MinKey".into(),
        Bson::MaxKey => "MaxKey".into(),
        Bson::Null => "null".into(),
        Bson::Boolean(b) => b.to_string(),
        Bson::ObjectId(o) => format!("ObjectId('{o}')"),
        Bson::DateTime(d) => format!("new Date({})", d.timestamp_millis()),
        Bson::Timestamp(t) => format!("Timestamp({}, {})", t.time, t.increment),
        Bson::Document(d) if d.is_empty() => "{}".into(),
        Bson::Array(a) if a.is_empty() => "[]".into(),
        other => other.to_string(),
    }
}

/// Lowest / highest value of the type bracket of `v` (MongoDB's bound endpoints).
fn bracket_ends(v: &Bson) -> (Bson, bool, Bson, bool) {
    match v {
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => (Bson::Double(f64::NEG_INFINITY), true, Bson::Double(f64::INFINITY), true),
        Bson::String(_) | Bson::Symbol(_) => (Bson::String(String::new()), true, Bson::Document(Document::new()), false),
        Bson::DateTime(_) => (Bson::DateTime(bson::DateTime::MIN), true, Bson::DateTime(bson::DateTime::MAX), true),
        Bson::Boolean(_) => (Bson::Boolean(false), true, Bson::Boolean(true), true),
        _ => (Bson::MinKey, true, Bson::MaxKey, true),
    }
}

fn interval_str(lo: (&Bson, bool), hi: (&Bson, bool)) -> String {
    format!("{}{}, {}{}", if lo.1 { "[" } else { "(" }, fmt_value(lo.0), fmt_value(hi.0), if hi.1 { "]" } else { ")" })
}

/// Explain bound strings for one index field in ascending order.
fn field_bound_strings(p: Option<&FieldPred>, use_points: bool, use_range: bool) -> Vec<(String, String, bool, bool)> {
    // (lo, hi, lo_incl, hi_incl) rendered values, ascending.
    let full = || vec![("MinKey".to_string(), "MaxKey".to_string(), true, true)];
    let Some(p) = p else { return full() };
    if use_points {
        if let Some(pts) = &p.points {
            return pts.iter().map(|v| (fmt_value(v), fmt_value(v), true, true)).collect();
        }
    }
    if use_range {
        if let Some(pre) = &p.regex_prefix {
            let mut next = pre.clone();
            let last = next.pop().map(|c| char::from_u32(c as u32 + 1).unwrap_or(c));
            if let Some(c) = last {
                next.push(c);
            }
            let mut v = vec![(format!("\"{pre}\""), format!("\"{next}\""), true, false)];
            if let Some(Bson::RegularExpression(r)) = &p.regex_value {
                let rs = format!("/{}/{}", r.pattern, r.options);
                v.push((rs.clone(), rs, true, true));
            }
            return v;
        }
        if p.lo.is_some() || p.hi.is_some() {
            let sample = p.lo.as_ref().or(p.hi.as_ref()).map(|x| x.0.clone()).unwrap_or(Bson::Null);
            let (bl, bli, bh, bhi) = bracket_ends(&sample);
            let (lo, lo_i) = p.lo.clone().unwrap_or((bl, bli));
            let (hi, hi_i) = p.hi.clone().unwrap_or((bh, bhi));
            return vec![(fmt_value(&lo), fmt_value(&hi), lo_i, hi_i)];
        }
    }
    full()
}

fn render_bounds(index: &Index, preds: &HashMap<String, FieldPred>, eq_fields: usize, range_field: bool, reverse: bool) -> Document {
    let mut out = Document::new();
    for (i, (f, desc)) in index.fields.iter().enumerate() {
        let p = preds.get(f);
        let use_points = i < eq_fields;
        let use_range = range_field && i == eq_fields;
        let mut ivs = field_bound_strings(p.filter(|_| use_points || use_range), use_points, use_range);
        // Descending fields and backward scans flip interval direction.
        let flip = *desc != reverse;
        if flip {
            ivs.reverse();
        }
        let strs: Vec<Bson> = ivs
            .into_iter()
            .map(|(lo, hi, li, hi_i)| {
                let (a, ai, b, bi) = if flip { (hi, hi_i, lo, li) } else { (lo, li, hi, hi_i) };
                Bson::String(format!("{}{}, {}{}", if ai { "[" } else { "(" }, a, b, if bi { "]" } else { ")" }))
            })
            .collect();
        out.insert(f.clone(), strs);
    }
    let _ = interval_str;
    out
}

fn bound_fields_n(eq: usize, has_range: bool) -> usize {
    eq + has_range as usize
}

// ------------------------------------------------------------------ candidates

struct Candidate {
    plan: IxPlan,
    eq_fields: usize,
    bound_fields: usize,
    /// Estimated index keys examined.
    est: f64,
}

fn same_key(a: &Document, b: &Document) -> bool {
    a == b || a.iter().map(|(k, v)| (k, bsonutil::num_f64(v))).eq(b.iter().map(|(k, v)| (k, bsonutil::num_f64(v))))
}

/// Bounds for `index` from `preds`. `None` if the index does not apply; `Some(None ranges)`
/// (empty interval) when the predicates are unsatisfiable.
fn bounds_for(index: &Arc<Index>, preds: &HashMap<String, FieldPred>, qcoll: Option<&Collation>) -> Option<(Vec<Interval>, usize, bool, bool)> {
    let same_collation = index.collation.as_deref() == qcoll;
    let mut prefixes: Vec<Vec<u8>> = vec![Vec::new()];
    let mut tail: Vec<Interval> = Vec::new();
    let mut eq_fields = 0;
    for (f, is_desc) in &index.fields {
        let Some(p) = preds.get(f) else { break };
        if p.has_string && !same_collation {
            break;
        }
        if p.empty {
            return Some((Vec::new(), eq_fields, true, true));
        }
        if let Some(pts) = &p.points {
            let mut encs: Vec<Vec<u8>> = pts
                .iter()
                .map(|v| {
                    let mut k = Vec::new();
                    keys::encode_dir_c(&mut k, v, *is_desc, index.collation.as_deref());
                    k
                })
                .collect();
            encs.sort();
            encs.dedup();
            if prefixes.len() * encs.len() > 2000 {
                break;
            }
            prefixes = prefixes.iter().flat_map(|pre| encs.iter().map(move |e| [pre.as_slice(), e].concat())).collect();
            eq_fields += 1;
            continue;
        }
        if let Some(r) = &p.range {
            if *is_desc {
                tail.extend(desc_interval(p, index.collation.as_deref()));
            } else {
                tail.push(r.clone());
                if let Some(rv) = &p.regex_value {
                    let e = keys::encode_one(rv);
                    tail.push((Bound::Included(e.clone()), succ_excl(&e)));
                }
            }
        }
        break;
    }
    let has_range = !tail.is_empty();
    let mut ranges: Vec<Interval> = Vec::new();
    for pre in prefixes {
        if tail.is_empty() {
            ranges.push((if pre.is_empty() { Bound::Unbounded } else { Bound::Included(pre.clone()) }, succ_excl(&pre)));
        }
        for (lo, hi) in &tail {
            ranges.push((concat_bound(&pre, lo.clone(), false), concat_bound(&pre, hi.clone(), true)));
        }
    }
    ranges.sort_by(|a, b| bound_val(&a.0).cmp(&bound_val(&b.0)));
    Some((ranges, eq_fields, has_range, false))
}

/// Interval on a descending field's (byte-inverted) encodings, built from the predicate values.
/// `None` when the predicate cannot be expressed (anchored regex prefixes).
fn desc_interval(p: &FieldPred, coll: Option<&Collation>) -> Option<Interval> {
    if p.regex_prefix.is_some() {
        return None;
    }
    let sample = p.lo.as_ref().or(p.hi.as_ref())?.0.clone();
    let inv = |v: &Bson| keys::encode_one_c(v, coll).iter().map(|b| !b).collect::<Vec<u8>>();
    // All values of the type bracket share the first (inverted tag) byte.
    let it = !keys::tag_of(&sample);
    let mut lo = Bound::Included(vec![it]);
    let mut hi = succ_excl(&[it]);
    if let Some((v, incl)) = &p.hi {
        lo = if *incl { Bound::Included(inv(v)) } else { succ_incl(&inv(v)) };
    }
    if let Some((v, incl)) = &p.lo {
        hi = if *incl { succ_excl(&inv(v)) } else { Bound::Excluded(inv(v)) };
    }
    Some((lo, hi))
}

/// Does scanning `index` (after `eq_fields` single-point prefix fields) yield `sort` order?
/// Returns `Some(reverse)`.
fn sort_direction(index: &Index, preds: &HashMap<String, FieldPred>, eq_fields: usize, sort: &SortSpec, qcoll: Option<&Collation>) -> Option<bool> {
    if index.multikey.load(Ordering::Relaxed) || index.sparse || index.partial.is_some() || index.collation.as_deref() != qcoll {
        return None;
    }
    // Equality prefix fields must each be a single point; they are constant in the output.
    for (f, _) in index.fields.iter().take(eq_fields) {
        if preds.get(f).and_then(|p| p.points.as_ref()).is_none_or(|p| p.len() != 1) {
            return None;
        }
    }
    let constant: Vec<&str> = index.fields.iter().take(eq_fields).map(|(f, _)| f.as_str()).collect();
    let wanted: Vec<&(String, SortKey)> = sort.keys.iter().filter(|(p, _)| !constant.contains(&p.as_str())).collect();
    if wanted.is_empty() {
        return Some(false);
    }
    let rest = &index.fields[eq_fields..];
    if wanted.len() > rest.len() {
        return None;
    }
    let mut dir: Option<bool> = None;
    for ((sp, sk), (f, desc)) in wanted.iter().map(|x| (&x.0, &x.1)).zip(rest) {
        if sp != f {
            return None;
        }
        let want_desc = match sk {
            SortKey::Asc => false,
            SortKey::Desc => true,
            SortKey::TextScore => return None,
        };
        let rev = want_desc != *desc;
        match dir {
            None => dir = Some(rev),
            Some(d) if d != rev => return None,
            _ => {}
        }
    }
    dir
}

impl Store {
    /// Choose an access path for `filter`. `hint` is an index name or key pattern (or `{$natural: ±1}`).
    pub fn plan(&self, c: &Arc<Coll>, filter: &Document, hint: Option<&Bson>) -> Result<Plan> {
        Ok(self.plan_query(c, filter, hint, QueryShape::default())?.0)
    }

    /// Plan with sort/projection/limit context. Returns (winning plan, rejected plans).
    pub fn plan_query(&self, c: &Arc<Coll>, filter: &Document, hint: Option<&Bson>, q: QueryShape) -> Result<(Plan, Vec<Plan>)> {
        let normalized = normalize(filter);
        let filter = &normalized;
        let qcoll = crate::collation::current();
        let qc = qcoll.as_deref();
        let mut preds = HashMap::new();
        collect_preds(filter, &mut preds, qc);
        let sort = match q.sort.filter(|s| !s.is_empty() && !s.contains_key("$natural")) {
            Some(s) => Some(SortSpec::new(s)?),
            None => None,
        };
        let mut fpaths = Vec::new();
        let filter_ok = filter_paths(filter, &mut fpaths);
        let text_or_near = filter.contains_key("$text")
            || filter.values().any(|v| matches!(v, Bson::Document(d) if d.contains_key("$near") || d.contains_key("$nearSphere")));

        let make = |index: &Arc<Index>, ranges: Vec<Interval>, eq: usize, has_range: bool, force_full: bool| -> Candidate {
            let sort_rev = sort.as_ref().and_then(|s| sort_direction(index, &preds, eq, s, qc));
            let reverse = sort_rev.unwrap_or(false);
            let covered = q.projection.is_some_and(|p| covers(index, p, if filter_ok { Some(&fpaths) } else { None }, sort.as_ref(), text_or_near));
            let bound_n = eq + has_range as usize;
            let bounds = if force_full { render_bounds(index, &HashMap::new(), 0, false, reverse) } else { render_bounds(index, &preds, eq, has_range, reverse) };
            let express = index.name == "_id_" && eq == 1 && filter.len() == 1 && !has_range && ranges.len() == 1 && sort.is_none() && q.projection.is_none();
            let bound_fields: Vec<String> = if force_full { Vec::new() } else { index.fields.iter().take(bound_fields_n(eq, has_range)).map(|(f, _)| f.clone()).collect() };
            let mut bounds = bounds;
            if ranges.is_empty() {
                if let Some((f, _)) = index.fields.get(eq) {
                    bounds.insert(f.clone(), Vec::<Bson>::new());
                }
            }
            Candidate {
                plan: IxPlan { index: index.clone(), ranges, reverse, bounds, provides_sort: sort_rev.is_some(), covered, express, bound_fields, keys_examined: Default::default() },
                eq_fields: eq,
                bound_fields: bound_n,
                est: 0.0,
            }
        };

        if let Some(h) = hint {
            if let Bson::Document(hd) = h {
                if let Some(n) = hd.get("$natural") {
                    return Ok((Plan::CollScan { reverse: bsonutil::num_f64(n).unwrap_or(1.0) < 0.0 }, Vec::new()));
                }
            }
            let idx = c
                .indexes()
                .into_iter()
                .find(|i| match h {
                    Bson::String(n) => &i.name == n,
                    Bson::Document(k) => same_key(&i.key, k),
                    _ => false,
                })
                .ok_or_else(|| {
                    if c.options.contains_key("timeseries") && matches!(h, Bson::String(n) if n == "_id_") {
                        let tree = if filter.is_empty() { "$and".to_string() } else { parsed_query(filter).to_string() };
                        Error::bad_value(format!("error enumerating plans for query: ns={}Tree: {tree}\nSort: {{}}\nProj: {{}}\nCollation: {{ locale: \"simple\" }}\n planner returned error :: caused by :: cannot hint on '_id_' for timeseries collections", c.ns))
                    } else {
                        Error::bad_value("planner returned error :: caused by :: hint provided does not correspond to an existing index")
                    }
                })?;
            if !idx.has_entries() {
                return Ok((Plan::CollScan { reverse: false }, Vec::new()));
            }
            let cand = match bounds_for(&idx, &preds, qc) {
                Some((r, eq, hr, _)) if eq > 0 || hr || r.is_empty() => make(&idx, r, eq, hr, false),
                _ => make(&idx, vec![(Bound::Unbounded, Bound::Unbounded)], 0, false, true),
            };
            return Ok((Plan::IxScan(cand.plan), Vec::new()));
        }

        let mut cands: Vec<Candidate> = Vec::new();
        for index in c.indexes() {
            if !index.has_entries() || index.partial.is_some() {
                continue;
            }
            match bounds_for(&index, &preds, qc) {
                Some((r, eq, hr, _)) if eq > 0 || hr || r.is_empty() => {
                    if index.sparse && eq == 0 && !hr {
                        continue;
                    }
                    cands.push(make(&index, r, eq, hr, false));
                }
                _ => {
                    // Index usable only to provide the sort order (full scan).
                    if let Some(s) = &sort {
                        if sort_direction(&index, &preds, 0, s, qc).is_some() {
                            cands.push(make(&index, vec![(Bound::Unbounded, Bound::Unbounded)], 0, false, true));
                        }
                    }
                }
            }
        }
        if cands.is_empty() {
            return Ok((Plan::CollScan { reverse: false }, Vec::new()));
        }
        let total = c.count.load(Ordering::Relaxed).max(1) as f64;
        for cand in &mut cands {
            cand.est = self.estimate_keys(&cand.plan.index, &cand.plan.ranges)?;
        }
        // Cost: keys examined, plus an in-memory sort penalty; a sort-providing plan with a
        // limit stops early.
        let cost = |c: &Candidate| -> f64 {
            let mut k = c.est;
            if sort.is_some() {
                if c.plan.provides_sort {
                    if let Some(l) = q.limit {
                        let selectivity = (c.est / total).max(1.0 / total);
                        k = k.min(l as f64 / selectivity.min(1.0)).max(l.min(c.est as usize) as f64);
                    }
                } else {
                    k += k * (k.max(2.0)).log2() * 0.1 + 1.0;
                }
            }
            if !c.plan.covered {
                k *= 1.05;
            }
            k
        };
        cands.sort_by(|a, b| {
            cost(a)
                .partial_cmp(&cost(b))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.bound_fields.cmp(&a.bound_fields))
                .then_with(|| b.plan.index.unique.cmp(&a.plan.index.unique))
                .then_with(|| a.plan.index.fields.len().cmp(&b.plan.index.fields.len()))
                .then_with(|| b.eq_fields.cmp(&a.eq_fields))
        });
        let mut it = cands.into_iter();
        let win = it.next().expect("non-empty");
        let rejected = it.map(|c| Plan::IxScan(c.plan)).collect();
        Ok((Plan::IxScan(win.plan), rejected))
    }

    /// Estimated index entries within `ranges`: histogram when available, else a bounded probe.
    fn estimate_keys(&self, index: &Index, ranges: &[Interval]) -> Result<f64> {
        if let Some(h) = index.histogram() {
            return Ok(ranges.iter().map(|(lo, hi)| h.estimate(lo, hi)).sum::<f64>().max(0.5));
        }
        const PROBE: usize = 1024;
        let snap = self.snapshot();
        let n = self.index_rids(&snap, index, ranges, Some(PROBE))?.len();
        index.accesses.fetch_sub(1, Ordering::Relaxed);
        Ok(if n >= PROBE { PROBE as f64 * 4.0 } else { n as f64 })
    }
}

/// Can `projection` be answered from `index` keys alone?
fn covers(index: &Index, projection: &Document, filter_paths: Option<&Vec<String>>, sort: Option<&SortSpec>, text_or_near: bool) -> bool {
    if text_or_near || index.kind != IndexKind::Regular || index.multikey.load(Ordering::Relaxed) || index.collation.is_some() || index.sparse || index.partial.is_some() {
        return false;
    }
    let Some(fp) = filter_paths else { return false };
    let has = |p: &str| index.fields.iter().any(|(f, _)| f == p);
    let mut include_id = true;
    let mut any = false;
    for (k, v) in projection {
        let on = match v {
            Bson::Boolean(b) => *b,
            v if bsonutil::is_number(v) => bsonutil::truthy(v),
            _ => return false,
        };
        if k == "_id" {
            include_id = on;
            continue;
        }
        if !on || k.starts_with('$') || !has(k) {
            return false;
        }
        any = true;
    }
    if !any && !include_id {
        return false;
    }
    if include_id && !has("_id") {
        return false;
    }
    if !fp.iter().all(|p| has(p)) {
        return false;
    }
    if let Some(s) = sort {
        if !s.keys.iter().all(|(p, _)| has(p)) {
            return false;
        }
    }
    true
}

// ------------------------------------------------------------------ histograms

/// 256-bucket equi-depth histogram over an index's key prefixes (record id stripped).
pub struct Histogram {
    pub total: u64,
    /// Inclusive upper bound of each bucket.
    pub uppers: Vec<Vec<u8>>,
    pub depth: Vec<u64>,
    /// Distinct key prefixes per bucket.
    pub ndv: Vec<u64>,
}

pub const HISTOGRAM_BUCKETS: u64 = 256;

impl Histogram {
    /// Build from keys in ascending order.
    pub fn build(keys: impl Iterator<Item = Vec<u8>>, total_hint: u64) -> Histogram {
        let per = total_hint.div_ceil(HISTOGRAM_BUCKETS).max(1);
        let mut h = Histogram { total: 0, uppers: Vec::new(), depth: Vec::new(), ndv: Vec::new() };
        let (mut n, mut ndv) = (0u64, 0u64);
        let mut last: Option<Vec<u8>> = None;
        for k in keys {
            h.total += 1;
            n += 1;
            if last.as_ref() != Some(&k) {
                ndv += 1;
            }
            // Close a bucket only between distinct keys so equal keys share a bucket.
            if n >= per && last.as_ref() != Some(&k) {
                h.uppers.push(k.clone());
                h.depth.push(n);
                h.ndv.push(ndv);
                n = 0;
                ndv = 0;
            }
            last = Some(k);
        }
        if n > 0 {
            h.uppers.push(last.unwrap_or_default());
            h.depth.push(n);
            h.ndv.push(ndv.max(1));
        }
        h
    }

    fn below(b: &[u8], lo: &Bound<Vec<u8>>) -> bool {
        match lo {
            Bound::Included(l) => b < l.as_slice(),
            Bound::Excluded(l) => b <= l.as_slice(),
            Bound::Unbounded => false,
        }
    }

    fn above(b: &[u8], hi: &Bound<Vec<u8>>) -> bool {
        match hi {
            Bound::Included(h) => b > h.as_slice(),
            Bound::Excluded(h) => b >= h.as_slice(),
            Bound::Unbounded => false,
        }
    }

    /// Estimated keys within `[lo, hi)`.
    pub fn estimate(&self, lo: &Bound<Vec<u8>>, hi: &Bound<Vec<u8>>) -> f64 {
        let mut est = 0.0;
        let mut prev: Option<&[u8]> = None;
        for i in 0..self.uppers.len() {
            let upper = self.uppers[i].as_slice();
            let d = self.depth[i] as f64;
            // Bucket covers (prev, upper].
            if Self::below(upper, lo) {
                prev = Some(upper);
                continue;
            }
            if let Some(p) = prev {
                if Self::above(p, hi) || matches!(hi, Bound::Excluded(h) if p >= h.as_slice()) {
                    break;
                }
            }
            let lo_inside = match (lo, prev) {
                (Bound::Unbounded, _) => true,
                (_, None) => false,
                (Bound::Included(l) | Bound::Excluded(l), Some(p)) => l.as_slice() <= p,
            };
            let hi_inside = !Self::above(upper, hi) && !matches!(hi, Bound::Excluded(h) if upper >= h.as_slice());
            est += if lo_inside && hi_inside {
                d
            } else if !lo_inside && !hi_inside {
                // Narrow interval inside one bucket: about one distinct value's share.
                d / self.ndv[i].max(1) as f64
            } else {
                d / 2.0
            };
            prev = Some(upper);
        }
        est
    }
}

// ------------------------------------------------------------------ explain

/// MongoDB MatchExpression type rank used for canonical ordering of conjuncts.
fn match_rank(op: &str) -> u8 {
    match op {
        "$and" => 0,
        "$or" => 1,
        "$eq" => 2,
        "$lte" => 3,
        "$lt" => 4,
        "$gt" => 5,
        "$gte" => 6,
        "$regex" => 7,
        "$mod" => 8,
        "$exists" => 9,
        "$in" => 10,
        "$bitsAllSet" => 11,
        "$bitsAllClear" => 12,
        "$bitsAnySet" => 13,
        "$bitsAnyClear" => 14,
        "$not" => 15,
        "$elemMatch" => 16,
        "$size" => 18,
        "$type" => 19,
        "$geoWithin" | "$geoIntersects" => 20,
        "$where" => 21,
        "$expr" => 22,
        "$alwaysFalse" => 23,
        "$alwaysTrue" => 24,
        "$near" | "$nearSphere" => 25,
        "$text" => 26,
        "$nor" => 27,
        _ => 30,
    }
}

/// One normalized conjunct: (rank, path, rendered predicate).
fn conjuncts(filter: &Document, out: &mut Vec<(u8, String, Document)>) {
    for (k, v) in filter {
        if k == "$and" {
            if let Bson::Array(a) = v {
                for e in a {
                    if let Bson::Document(d) = e {
                        conjuncts(d, out);
                    }
                }
            }
            continue;
        }
        if k.starts_with('$') {
            out.push((match_rank(k), String::new(), doc! {k.clone(): v.clone()}));
            continue;
        }
        match v {
            Bson::Document(d) if d.keys().next().is_some_and(|f| f.starts_with('$')) => {
                for (op, arg) in d {
                    let (rank, pred) = match op.as_str() {
                        "$ne" => (match_rank("$not"), doc! {"$not": {"$eq": arg.clone()}}),
                        "$nin" => (match_rank("$not"), doc! {"$not": {"$in": arg.clone()}}),
                        "$options" => continue,
                        "$regex" => {
                            let opts = d.get_str("$options").unwrap_or("");
                            let r = match arg {
                                Bson::String(p) => Bson::RegularExpression(bson::Regex { pattern: p.clone(), options: opts.to_string() }),
                                o => o.clone(),
                            };
                            (match_rank("$regex"), doc! {"$regex": r})
                        }
                        "$in" => {
                            let mut a = arg.as_array().cloned().unwrap_or_default();
                            a.sort_by(bsonutil::cmp_bson);
                            a.dedup_by(|x, y| bsonutil::bson_eq(x, y) && bsonutil::type_order(x) == bsonutil::type_order(y));
                            (match_rank("$in"), doc! {"$in": a})
                        }
                        other => (match_rank(other), doc! {other: arg.clone()}),
                    };
                    out.push((rank, k.clone(), doc! {k.clone(): pred}));
                }
            }
            Bson::RegularExpression(_) => out.push((match_rank("$regex"), k.clone(), doc! {k.clone(): {"$regex": v.clone()}})),
            other => out.push((match_rank("$eq"), k.clone(), doc! {k.clone(): {"$eq": other.clone()}})),
        }
    }
}

fn join(mut parts: Vec<(u8, String, Document)>) -> Document {
    parts.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    match parts.len() {
        0 => Document::new(),
        1 => parts.pop().expect("one").2,
        _ => doc! {"$and": parts.into_iter().map(|p| Bson::Document(p.2)).collect::<Vec<_>>()},
    }
}

/// Normalized `parsedQuery` as MongoDB prints it.
pub fn parsed_query(filter: &Document) -> Document {
    let mut parts = Vec::new();
    conjuncts(&normalize(filter), &mut parts);
    join(parts)
}

/// Conjuncts not enforced by the index bounds (FETCH `filter`).
fn residual(filter: &Document, p: &IxPlan) -> Document {
    let mut parts = Vec::new();
    conjuncts(&normalize(filter), &mut parts);
    let tight = |path: &str, pred: &Document| -> bool {
        p.bound_fields.iter().any(|f| f == path)
            && pred.get_document(path).is_ok_and(|ops| ops.keys().all(|o| matches!(o.as_str(), "$eq" | "$in" | "$gt" | "$gte" | "$lt" | "$lte") || (o == "$regex" && exact_regex(ops.get("$regex")))))
    };
    parts.retain(|(_, path, pred)| !tight(path, pred));
    join(parts)
}

fn exact_regex(v: Option<&Bson>) -> bool {
    let Some(Bson::RegularExpression(r)) = v else { return false };
    let mut p = FieldPred::default();
    set_regex(&mut p, &r.pattern, &r.options);
    p.regex_exact
}

/// Rewrite `$or` branches that are plain equalities on one path into `$in` (MongoDB's
/// canonicalization), recursively inside `$and`.
pub fn normalize(filter: &Document) -> Document {
    let mut out = Document::new();
    let mut extra: Vec<Bson> = Vec::new();
    for (k, v) in filter {
        match (k.as_str(), v) {
            ("$or", Bson::Array(branches)) => {
                let mut path: Option<String> = None;
                let mut vals = Vec::new();
                let ok = !branches.is_empty() && branches.iter().all(|b| {
                    let Bson::Document(d) = b else { return false };
                    if d.len() != 1 {
                        return false;
                    }
                    let (bk, bv) = d.iter().next().expect("one");
                    if bk.starts_with('$') || path.as_ref().is_some_and(|p| p != bk) {
                        return false;
                    }
                    let val = match bv {
                        Bson::Document(x) if x.len() == 1 && x.contains_key("$eq") => x.get("$eq").cloned().expect("eq"),
                        Bson::Document(x) if x.keys().any(|k| k.starts_with('$')) => return false,
                        Bson::RegularExpression(_) | Bson::Array(_) => return false,
                        other => other.clone(),
                    };
                    if matches!(val, Bson::RegularExpression(_) | Bson::Array(_)) {
                        return false;
                    }
                    path = Some(bk.clone());
                    vals.push(val);
                    true
                });
                match (ok, path) {
                    (true, Some(p)) if !out.contains_key(&p) => {
                        out.insert(p, doc! {"$in": vals});
                    }
                    (true, Some(p)) => extra.push(Bson::Document(doc! {p: {"$in": vals}})),
                    _ => {
                        out.insert(k.clone(), v.clone());
                    }
                }
            }
            ("$and", Bson::Array(a)) => {
                let n: Vec<Bson> = a.iter().map(|e| match e {
                    Bson::Document(d) => Bson::Document(normalize(d)),
                    o => o.clone(),
                }).collect();
                out.insert("$and", n);
            }
            _ => {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    if !extra.is_empty() {
        let mut a = match out.remove("$and") {
            Some(Bson::Array(a)) => a,
            _ => Vec::new(),
        };
        a.extend(extra);
        out.insert("$and", a);
    }
    out
}

/// Per-stage counters observed while executing the winning plan.
#[derive(Default, Clone)]
pub struct ExecStats {
    /// Records yielded by the access stage (after index de-duplication).
    pub scanned: u64,
    /// Documents passing the filter.
    pub matched: u64,
    pub sorted: u64,
    pub skipped_out: u64,
    pub returned: u64,
    pub keys: u64,
    pub docs: u64,
    /// Bytes held by a blocking sort.
    pub sort_bytes: u64,
}

/// `appendNumber`-style counter: int32 when it fits.
pub fn num32(n: u64) -> Bson {
    i32::try_from(n).map(Bson::Int32).unwrap_or(Bson::Int64(n as i64))
}

/// Does the explain tree place SKIP below FETCH (so skipped entries are never fetched)?
pub fn skip_below_fetch(plan: &Plan, filter: &Document, blocking_sort: bool, skip: usize) -> bool {
    match plan {
        Plan::IxScan(p) if !p.covered && !p.express => skip > 0 && !blocking_sort && residual(filter, p).is_empty(),
        _ => false,
    }
}

/// Query features that shape the explain stage tree.
pub struct ExplainQuery<'a> {
    pub ns: &'a str,
    pub filter: &'a Document,
    pub sort: Option<&'a Document>,
    pub projection: Option<&'a Document>,
    pub skip: usize,
    pub limit: usize,
}

fn projection_stage(p: &Document, covered: bool) -> &'static str {
    if covered {
        return "PROJECTION_COVERED";
    }
    let simple = p.iter().all(|(k, v)| !k.contains('.') && !k.starts_with('$') && (matches!(v, Bson::Boolean(_)) || bsonutil::is_number(v)));
    if simple { "PROJECTION_SIMPLE" } else { "PROJECTION_DEFAULT" }
}

/// Build the MongoDB stage tree for `plan` (optionally annotated with execution stats).
pub fn explain_tree(plan: &Plan, q: &ExplainQuery, stats: Option<&ExecStats>) -> Document {
    let pq = parsed_query(q.filter);
    // MongoDB's layout: `nReturned` after `stage` (or after `filter`, or for express plans after
    // `indexName`); stage counters go right before `inputStage`, or at the end of leaf stages.
    let with = |d: Document, n: u64, extra: &[(&str, Bson)]| -> Document {
        if stats.is_none() {
            return d;
        }
        let anchor = if d.contains_key("filter") {
            "filter"
        } else if d.get_str("stage") == Ok("EXPRESS_IXSCAN") {
            "indexName"
        } else {
            "stage"
        };
        let mut out = Document::new();
        let mut extras_done = false;
        for (k, v) in d {
            if k == "inputStage" {
                for (ek, ev) in extra {
                    out.insert(*ek, ev.clone());
                }
                extras_done = true;
            }
            let is_anchor = k == anchor;
            out.insert(k, v);
            if is_anchor {
                out.insert("nReturned", num32(n));
            }
        }
        if !extras_done {
            for (ek, ev) in extra {
                out.insert(*ek, ev.clone());
            }
        }
        out
    };
    let st = stats.cloned().unwrap_or_default();
    let mut fetch_has_filter = false;
    let mut needs_sort = q.sort.is_some_and(|s| !s.is_empty() && !s.contains_key("$natural"));
    let mut tree = match plan {
        Plan::Empty => doc! {"stage": "EOF", "type": "nonExistentNamespace"},
        Plan::CollScan { reverse } => {
            let mut d = doc! {"stage": "COLLSCAN"};
            if !pq.is_empty() {
                d.insert("filter", pq.clone());
            }
            d.insert("nss", q.ns);
            d.insert("direction", if *reverse { "backward" } else { "forward" });
            with(d, st.matched, &[("docsExamined", num32(st.docs)), ("seeks", Bson::Int32(0))])
        }
        Plan::IxScan(p) if p.express => {
            needs_sort = false;
            with(doc! {"stage": "EXPRESS_IXSCAN", "keyPattern": p.index.key.clone(), "indexName": &p.index.name}, st.matched, &[("keysExamined", num32(st.keys)), ("docsExamined", num32(st.docs))])
        }
        Plan::IxScan(p) => {
            if p.provides_sort {
                needs_sort = false;
            }
            let mk = p.index.multikey_paths();
            let mut multikey_paths = Document::new();
            for (f, _) in &p.index.fields {
                let paths: Vec<Bson> = mk.iter().filter(|m| f == *m || f.starts_with(&format!("{m}."))).map(|m| Bson::String(m.clone())).collect();
                multikey_paths.insert(f.clone(), paths);
            }
            let mut ix = doc! {"stage": "IXSCAN", "nss": q.ns, "keyPattern": p.index.key.clone(), "indexName": &p.index.name};
            if let Some(c) = &p.index.collation {
                ix.insert("collation", c.to_doc());
            }
            ix.insert("isMultiKey", p.index.multikey.load(Ordering::Relaxed));
            if p.index.name != "_id_" {
                ix.insert("multiKeyPaths", multikey_paths);
            }
            ix.insert("isUnique", p.index.unique);
            ix.insert("isSparse", p.index.sparse);
            ix.insert("isPartial", p.index.partial.is_some());
            ix.insert("indexVersion", 2);
            ix.insert("direction", if p.reverse { "backward" } else { "forward" });
            ix.insert("indexBounds", p.bounds.clone());
            let dups = if p.index.multikey.load(Ordering::Relaxed) { st.keys } else { 0 };
            let ix = with(ix, st.scanned, &[
                ("keysExamined", num32(st.keys)),
                ("seeks", num32(p.ranges.len().max(1) as u64)),
                ("dupsTested", num32(dups)),
                ("dupsDropped", num32(dups.saturating_sub(st.scanned))),
                ("peakTrackedMemBytes", Bson::Int32(0)),
            ]);
            if p.covered {
                ix
            } else {
                let res = residual(q.filter, p);
                fetch_has_filter = !res.is_empty();
                let mut f = doc! {"stage": "FETCH"};
                if fetch_has_filter {
                    f.insert("filter", res);
                }
                f.insert("nss", q.ns);
                let skip_below = !fetch_has_filter && !needs_sort && q.skip > 0;
                let inner = if skip_below {
                    with(doc! {"stage": "SKIP", "skipAmount": q.skip as i32, "inputStage": ix}, st.scanned.saturating_sub(q.skip as u64), &[])
                } else {
                    ix
                };
                f.insert("inputStage", inner);
                let docs = st.docs;
                let fetched_n = if skip_below { st.matched.saturating_sub(q.skip as u64) } else { st.matched };
                with(f, fetched_n, &[("docsExamined", num32(docs)), ("alreadyHasObj", Bson::Int32(0))])
            }
        }
    };
    if normalize(q.filter).contains_key("$or") && matches!(plan, Plan::CollScan { .. }) {
        tree = with(doc! {"stage": "SUBPLAN", "inputStage": tree}, st.matched, &[]);
    }
    let skip_done_below = matches!(plan, Plan::IxScan(p) if !p.covered && !p.express) && !fetch_has_filter && !needs_sort && q.skip > 0;
    let mut limit_done = false;
    if needs_sort {
        let mut d = doc! {"stage": "SORT", "sortPattern": q.sort.cloned().unwrap_or_default(), "memLimit": 104857600_i32};
        if q.limit > 0 {
            d.insert("limitAmount", (q.skip + q.limit) as i32);
            limit_done = true;
        }
        d.insert("type", "simple");
        d.insert("inputStage", tree);
        tree = with(d, st.sorted, &[
            ("totalDataSizeSorted", num32(st.sort_bytes)),
            ("usedDisk", Bson::Boolean(false)),
            ("spills", Bson::Int32(0)),
            ("spilledRecords", Bson::Int32(0)),
            ("spilledBytes", Bson::Int32(0)),
            ("spilledDataStorageSize", Bson::Int32(0)),
            ("peakTrackedMemBytes", num32(st.sort_bytes)),
        ]);
    }
    if q.skip > 0 && !skip_done_below {
        tree = with(doc! {"stage": "SKIP", "skipAmount": q.skip as i32, "inputStage": tree}, st.skipped_out, &[]);
    }
    if let Some(p) = q.projection.filter(|p| !p.is_empty()) {
        let covered = plan.is_covered();
        tree = with(doc! {"stage": projection_stage(p, covered), "transformBy": p.clone(), "inputStage": tree}, st.skipped_out.min(if q.limit > 0 { q.limit as u64 } else { u64::MAX }), &[]);
    }
    if q.limit > 0 && !limit_done {
        let mut d = doc! {"stage": "LIMIT", "limitAmount": q.limit as i32, "inputStage": tree};
        d = with(std::mem::take(&mut d), st.returned, &[]);
        tree = d;
    }
    // Root stage carries `isCached` first.
    let mut root = doc! {"isCached": false};
    root.extend(tree);
    root
}

/// Short hash rendering used for `queryHash` / `planCacheShapeHash` / `planCacheKey`.
pub fn shape_hash(parts: &[&Document]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for p in parts {
        // Values are replaced by their type to describe the shape.
        h.update(shape_of(p).to_string().as_bytes());
    }
    let out = h.finalize();
    out[..4].iter().map(|b| format!("{b:02X}")).collect()
}

fn shape_of(d: &Document) -> Document {
    let mut out = Document::new();
    for (k, v) in d {
        let s = match v {
            Bson::Document(x) => Bson::Document(shape_of(x)),
            Bson::Array(a) => Bson::Array(a.iter().map(|e| match e {
                Bson::Document(x) => Bson::Document(shape_of(x)),
                o => Bson::String(format!("?{}", bsonutil::type_name(o))),
            }).collect()),
            o => Bson::String(format!("?{}", bsonutil::type_name(o))),
        };
        out.insert(k.clone(), s);
    }
    out
}
