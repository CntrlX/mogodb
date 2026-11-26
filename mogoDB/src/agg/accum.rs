//! Accumulators and the blocking aggregation stages built on them.
//!
//! Every stage here consumes its whole input eagerly (these stages are blocking in MongoDB too)
//! and returns an owned iterator over the result. Errors are returned as `Err`, never panics.
use crate::agg::expr::{Vars, eval};
use crate::agg::pipeline::{AggOptions, DocIter};
use crate::bsonutil::{
    MISSING, cmp_bson, doc_size, get_path, is_missing, is_nullish, is_number, num_add, num_f64, num_i64,
    set_path, type_name, type_order,
};
use crate::error::{Error, Result};
use crate::query::sort::{SortKey, SortSpec};
use bson::{Bson, Document};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

/// Estimated `$group` overhead per distinct group, beyond its key bytes.
const GROUP_OVERHEAD: usize = 64;
/// Upper bound on the number of candidate values `$densify` steps through per partition.
const MAX_DENSIFY_STEPS: u64 = 10_000_000;
/// 9.0 `$group` spill-refusal error (292).
fn group_mem_error(opts: &AggOptions) -> Error {
    let coll = opts.coll.as_deref().unwrap_or("$cmd.aggregate");
    Error::new(292, format!("Executor error during aggregate command on namespace: {}.{coll} :: caused by :: Exceeded memory limit for group, but didn't allow external spilling; pass allowDiskUse:true to opt in", opts.db))
}
const YEAR_MS: f64 = 31_556_952_000.0;
const MONTH_MS: f64 = YEAR_MS / 12.0;

/// A single accumulation state (one per group, per output field, or per window position).
pub trait Accumulator: Send {
    /// Feed one input document. The accumulator evaluates its own argument expression.
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()>;
    /// Current value. Non-destructive: may be called again after further `process` calls.
    fn finish(&mut self) -> Result<Bson>;
    /// Approximate bytes retained; feeds the `$group` memory limit.
    fn mem_bytes(&self) -> usize {
        0
    }
}

/// Build a fresh accumulator for `op` with its (unevaluated) argument expression.
///
/// `for_window` does not change accumulator semantics: window-only operators are dispatched by
/// `set_window_fields_stage` before reaching this function.
pub fn make_accumulator(op: &str, arg: &Bson, for_window: bool) -> Result<Box<dyn Accumulator>> {
    let _ = for_window;
    let acc: Box<dyn Accumulator> = match op {
        "$sum" => Box::new(Sum { arg: arg.clone(), total: Bson::Int32(0), dec: None }),
        "$avg" => Box::new(Avg { arg: arg.clone(), sum: 0.0, n: 0, decimal: false }),
        "$min" | "$max" => Box::new(MinMax { arg: arg.clone(), is_min: op == "$min", best: None }),
        "$first" => Box::new(First { arg: arg.clone(), val: None }),
        "$last" => Box::new(Last { arg: arg.clone(), val: None }),
        "$push" => Box::new(Push { arg: arg.clone(), vals: Vec::new(), mem: 0 }),
        "$addToSet" => Box::new(AddToSet { arg: arg.clone(), seen: HashSet::new(), vals: Vec::new(), mem: 0 }),
        "$count" => {
            if !matches!(arg, Bson::Document(d) if d.is_empty()) {
                return Err(Error::bad_value("Expression $count takes no arguments"));
            }
            Box::new(Count { n: 0 })
        }
        "$stdDevPop" | "$stdDevSamp" => {
            Box::new(StdDev { arg: arg.clone(), pop: op == "$stdDevPop", n: 0, mean: 0.0, m2: 0.0 })
        }
        "$mergeObjects" => Box::new(MergeObjects { arg: arg.clone(), out: Document::new(), mem: 0 }),
        "$firstN" => nlist(NOp::First, op, arg)?,
        "$lastN" => nlist(NOp::Last, op, arg)?,
        "$maxN" => nlist(NOp::Max, op, arg)?,
        "$minN" => nlist(NOp::Min, op, arg)?,
        "$top" => top(op, arg, true, false)?,
        "$bottom" => top(op, arg, true, true)?,
        "$topN" => top(op, arg, false, false)?,
        "$bottomN" => top(op, arg, false, true)?,
        "$median" => {
            let d = doc_arg(op, arg)?;
            check_method(d, op)?;
            Box::new(Percentiles {
                input: field(op, d, "input")?.clone(),
                ps: vec![0.5],
                single: true,
                vals: Vec::new(),
            })
        }
        "$percentile" => {
            let d = doc_arg(op, arg)?;
            check_method(d, op)?;
            let ps = match field(op, d, "p")? {
                Bson::Array(a) if !a.is_empty() => a
                    .iter()
                    .map(|p| match num_f64(p) {
                        Some(x) if (0.0..=1.0).contains(&x) => Ok(x),
                        _ => Err(Error::bad_value("$percentile requires 'p' values in the range [0.0, 1.0]")),
                    })
                    .collect::<Result<Vec<f64>>>()?,
                _ => return Err(Error::bad_value("$percentile requires 'p' to be a non-empty array")),
            };
            Box::new(Percentiles {
                input: field(op, d, "input")?.clone(),
                ps,
                single: false,
                vals: Vec::new(),
            })
        }
        "$accumulator" => crate::js::accumulator(arg)?,
        _ => return Err(Error::new(15952, format!("unknown group operator '{op}'"))),
    };
    Ok(acc)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn doc_arg<'a>(op: &str, arg: &'a Bson) -> Result<&'a Document> {
    match arg {
        Bson::Document(d) => Ok(d),
        _ => Err(Error::bad_value(format!("{op} requires an object argument"))),
    }
}

fn field<'a>(op: &str, d: &'a Document, key: &str) -> Result<&'a Bson> {
    d.get(key).ok_or_else(|| Error::bad_value(format!("Missing '{key}' parameter to {op}")))
}

fn check_method(d: &Document, op: &str) -> Result<()> {
    match d.get("method") {
        None => Ok(()),
        Some(Bson::String(m)) if m.as_str() == "approximate" => Ok(()),
        Some(_) => Err(Error::bad_value(format!("{op} supports only method 'approximate'"))),
    }
}

fn sort_spec(v: &Bson) -> Result<SortSpec> {
    match v {
        Bson::Document(d) => SortSpec::new(d),
        _ => Err(Error::bad_value("sortBy must be an object")),
    }
}

fn as_doc<'a>(name: &str, v: &'a Bson) -> Result<&'a Document> {
    match v {
        Bson::Document(d) => Ok(d),
        _ => Err(Error::bad_value(format!("The field '{name}' must be an object"))),
    }
}

/// Missing values become null (MongoDB never returns "absent" from an accumulator/stage output).
fn nn(v: Bson) -> Bson {
    if is_missing(&v) { Bson::Null } else { v }
}

fn eval_all(expr: &Bson, docs: &[Document], vars: &Vars) -> Result<Vec<Bson>> {
    docs.iter().map(|d| eval(expr, d, vars)).collect()
}

fn string_list(v: Option<&Bson>, name: &str) -> Result<Vec<String>> {
    match v {
        None => Ok(Vec::new()),
        Some(Bson::Array(a)) => a
            .iter()
            .map(|x| match x {
                Bson::String(s) => Ok(s.clone()),
                _ => Err(Error::bad_value(format!("'{name}' must be an array of field names"))),
            })
            .collect(),
        Some(_) => Err(Error::bad_value(format!("'{name}' must be an array of field names"))),
    }
}

fn unit_ms(u: &str) -> Result<f64> {
    Ok(match u {
        "millisecond" => 1.0,
        "second" => 1_000.0,
        "minute" => 60_000.0,
        "hour" => 3_600_000.0,
        "day" => 86_400_000.0,
        "week" => 604_800_000.0,
        // Calendar months/quarters/years are approximated by fixed durations.
        "month" => MONTH_MS,
        "quarter" => 3.0 * MONTH_MS,
        "year" => YEAR_MS,
        other => return Err(Error::bad_value(format!("unrecognized time unit: {other}"))),
    })
}

/// Numeric view of a sort/time value: dates become epoch milliseconds.
fn numeric_time(b: &Bson) -> Option<f64> {
    match b {
        Bson::DateTime(t) => Some(t.timestamp_millis() as f64),
        other => num_f64(other),
    }
}

fn is_intlike(b: &Bson) -> bool {
    matches!(b, Bson::Int32(_) | Bson::Int64(_))
}

/// Canonical bytes for grouping/dedup: equal bytes iff the values compare equal under
/// `cmp_bson`. Integral numbers collapse to one i64 form, so `1`, `1.0` and `Int64(1)` agree.
/// Every variant is length-prefixed or fixed-size, so the encoding is injective.
fn key_bytes(v: &Bson) -> Vec<u8> {
    let mut out = Vec::new();
    canon(v, &mut out);
    out
}

fn canon(v: &Bson, out: &mut Vec<u8>) {
    fn put_len(out: &mut Vec<u8>, n: usize) {
        out.extend_from_slice(&(n as u32).to_le_bytes());
    }
    fn put_str(out: &mut Vec<u8>, s: &str) {
        put_len(out, s.len());
        out.extend_from_slice(s.as_bytes());
    }
    match v {
        Bson::Undefined => out.push(b'u'),
        Bson::Null => out.push(b'n'),
        Bson::MinKey => out.push(b'm'),
        Bson::MaxKey => out.push(b'M'),
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => match num_i64(v) {
            Some(i) => {
                out.push(b'N');
                out.extend_from_slice(&i.to_le_bytes());
            }
            None => {
                let f = num_f64(v).unwrap_or(f64::NAN);
                let bits = if f.is_nan() { f64::NAN.to_bits() } else { f.to_bits() };
                out.push(b'F');
                out.extend_from_slice(&bits.to_le_bytes());
            }
        },
        Bson::String(s) | Bson::Symbol(s) => match crate::collation::current() {
            Some(c) => {
                out.push(b'C');
                let k = c.sort_key(s);
                put_len(out, k.len());
                out.extend_from_slice(&k);
            }
            None => {
                out.push(b'S');
                put_str(out, s);
            }
        },
        Bson::Document(d) => {
            out.push(b'D');
            put_len(out, d.len());
            for (k, x) in d {
                put_str(out, k);
                canon(x, out);
            }
        }
        Bson::Array(a) => {
            out.push(b'A');
            put_len(out, a.len());
            for x in a {
                canon(x, out);
            }
        }
        Bson::Boolean(b) => {
            out.push(b'b');
            out.push(u8::from(*b));
        }
        Bson::DateTime(t) => {
            out.push(b'T');
            out.extend_from_slice(&t.timestamp_millis().to_le_bytes());
        }
        Bson::ObjectId(o) => {
            out.push(b'o');
            out.extend_from_slice(&o.bytes());
        }
        Bson::Binary(b) => {
            out.push(b'B');
            out.push(u8::from(b.subtype));
            put_len(out, b.bytes.len());
            out.extend_from_slice(&b.bytes);
        }
        Bson::Timestamp(t) => {
            out.push(b't');
            out.extend_from_slice(&t.time.to_le_bytes());
            out.extend_from_slice(&t.increment.to_le_bytes());
        }
        Bson::RegularExpression(r) => {
            out.push(b'r');
            put_str(out, &r.pattern);
            put_str(out, &r.options);
        }
        other => {
            out.push(b'X');
            put_str(out, &format!("{other:?}"));
        }
    }
}

/// Rough retained size of one value, used for memory accounting.
fn est_bytes(v: &Bson) -> usize {
    match v {
        Bson::String(s) => 16 + s.len(),
        Bson::Document(d) => doc_size(d),
        Bson::Array(a) => 16 + a.iter().map(est_bytes).sum::<usize>(),
        _ => 16,
    }
}

/// Evaluate an `n` argument once; it must be a positive integer.
fn eval_n(e: &Bson, doc: &Document, vars: &Vars, op: &str) -> Result<usize> {
    match num_i64(&eval(e, doc, vars)?) {
        Some(n) if n > 0 => Ok(n as usize),
        _ => Err(Error::new(5787902, format!("Expected a positive integer for 'n' in {op}"))),
    }
}

fn cmp_value_lists(a: &[Bson], b: &[Bson]) -> Ordering {
    for (x, y) in a.iter().zip(b) {
        let o = cmp_bson(x, y);
        if o != Ordering::Equal {
            return o;
        }
    }
    a.len().cmp(&b.len())
}

// ---------------------------------------------------------------------------
// Simple accumulators
// ---------------------------------------------------------------------------

/// MongoDB keeps a non-decimal total and a decimal total; the non-decimal part is
/// converted to Decimal128 exactly (shortest repr) only when combining at the end.
struct Sum {
    arg: Bson,
    total: Bson,
    dec: Option<Bson>,
}

impl Accumulator for Sum {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        let v = eval(&self.arg, doc, vars)?;
        if let Bson::Decimal128(_) = v {
            let cur = self.dec.take().unwrap_or(Bson::Int32(0));
            self.dec = num_add(&cur, &v).or(Some(cur));
        } else if is_number(&v) {
            if let Some(s) = num_add(&self.total, &v) {
                self.total = s;
            }
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        match &self.dec {
            None => Ok(self.total.clone()),
            Some(d) => {
                let part = match &self.total {
                    Bson::Double(f) => Bson::Decimal128(crate::bsonutil::f64_to_decimal(*f)),
                    other => other.clone(),
                };
                Ok(num_add(d, &part).unwrap_or_else(|| d.clone()))
            }
        }
    }
}

struct Avg {
    arg: Bson,
    sum: f64,
    n: u64,
    /// Any Decimal128 input makes the result Decimal128 (MongoDB widening rule).
    decimal: bool,
}

impl Accumulator for Avg {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        let v = eval(&self.arg, doc, vars)?;
        if let Some(x) = num_f64(&v) {
            self.sum += x;
            self.n += 1;
            self.decimal |= matches!(v, Bson::Decimal128(_));
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        if self.n == 0 {
            return Ok(Bson::Null);
        }
        let avg = self.sum / self.n as f64;
        Ok(if self.decimal { Bson::Decimal128(crate::bsonutil::f64_to_decimal(avg)) } else { Bson::Double(avg) })
    }
}

struct MinMax {
    arg: Bson,
    is_min: bool,
    best: Option<Bson>,
}

impl Accumulator for MinMax {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        let v = eval(&self.arg, doc, vars)?;
        if is_nullish(&v) {
            return Ok(());
        }
        let take = match &self.best {
            None => true,
            Some(b) => {
                let o = cmp_bson(&v, b);
                if self.is_min { o == Ordering::Less } else { o == Ordering::Greater }
            }
        };
        if take {
            self.best = Some(v);
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        Ok(self.best.clone().unwrap_or(Bson::Null))
    }
}

struct First {
    arg: Bson,
    val: Option<Bson>,
}

impl Accumulator for First {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        if self.val.is_none() {
            self.val = Some(nn(eval(&self.arg, doc, vars)?));
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        Ok(self.val.clone().unwrap_or(Bson::Null))
    }
}

struct Last {
    arg: Bson,
    val: Option<Bson>,
}

impl Accumulator for Last {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        self.val = Some(nn(eval(&self.arg, doc, vars)?));
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        Ok(self.val.clone().unwrap_or(Bson::Null))
    }
}

struct Push {
    arg: Bson,
    vals: Vec<Bson>,
    mem: usize,
}

impl Accumulator for Push {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        let v = eval(&self.arg, doc, vars)?;
        if !is_missing(&v) {
            self.mem += est_bytes(&v);
            self.vals.push(v);
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        Ok(Bson::Array(self.vals.clone()))
    }
    fn mem_bytes(&self) -> usize {
        self.mem
    }
}

struct AddToSet {
    arg: Bson,
    seen: HashSet<Vec<u8>>,
    vals: Vec<Bson>,
    mem: usize,
}

impl Accumulator for AddToSet {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        let v = eval(&self.arg, doc, vars)?;
        if is_missing(&v) {
            return Ok(());
        }
        if self.seen.insert(key_bytes(&v)) {
            self.mem += est_bytes(&v);
            self.vals.push(v);
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        Ok(Bson::Array(self.vals.clone()))
    }
    fn mem_bytes(&self) -> usize {
        self.mem
    }
}

struct Count {
    n: i32,
}

impl Accumulator for Count {
    fn process(&mut self, _doc: &Document, _vars: &Vars) -> Result<()> {
        self.n = self.n.saturating_add(1);
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        Ok(Bson::Int32(self.n))
    }
}

struct StdDev {
    arg: Bson,
    pop: bool,
    n: u64,
    mean: f64,
    m2: f64,
}

impl Accumulator for StdDev {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        if let Some(x) = num_f64(&eval(&self.arg, doc, vars)?) {
            // Welford's online algorithm.
            self.n += 1;
            let delta = x - self.mean;
            self.mean += delta / self.n as f64;
            self.m2 += delta * (x - self.mean);
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        if self.n == 0 || (!self.pop && self.n < 2) {
            return Ok(Bson::Null);
        }
        let var = if self.pop { self.m2 / self.n as f64 } else { self.m2 / (self.n - 1) as f64 };
        Ok(Bson::Double(var.sqrt()))
    }
}

struct MergeObjects {
    arg: Bson,
    out: Document,
    mem: usize,
}

fn merge_err(other: &Bson) -> Error {
    Error::new(
        40400,
        format!(
            "$mergeObjects requires object inputs, but input {other} is of type {}",
            type_name(other)
        ),
    )
}

impl MergeObjects {
    fn merge(&mut self, d: &Document) {
        for (k, v) in d {
            self.mem += est_bytes(v);
            self.out.insert(k.as_str(), v.clone());
        }
    }
}

impl Accumulator for MergeObjects {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        match eval(&self.arg, doc, vars)? {
            Bson::Document(d) => self.merge(&d),
            Bson::Array(items) => {
                for item in items {
                    match item {
                        Bson::Document(d) => self.merge(&d),
                        Bson::Null | Bson::Undefined => {}
                        other => return Err(merge_err(&other)),
                    }
                }
            }
            Bson::Null | Bson::Undefined => {}
            other => return Err(merge_err(&other)),
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        Ok(Bson::Document(self.out.clone()))
    }
    fn mem_bytes(&self) -> usize {
        self.mem
    }
}

// ---------------------------------------------------------------------------
// $firstN / $lastN / $maxN / $minN
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum NOp {
    First,
    Last,
    Max,
    Min,
}

impl NOp {
    fn name(self) -> &'static str {
        match self {
            NOp::First => "$firstN",
            NOp::Last => "$lastN",
            NOp::Max => "$maxN",
            NOp::Min => "$minN",
        }
    }
}

struct NList {
    op: NOp,
    input: Bson,
    n_expr: Bson,
    n: Option<usize>,
    vals: VecDeque<Bson>,
    mem: usize,
}

impl NList {
    /// Keep only the best `n` values (maxN: largest, minN: smallest). Safe to discard the rest
    /// permanently because they can never enter the final top-n.
    fn compact(&mut self) {
        let n = self.n.unwrap_or(0);
        let desc = matches!(self.op, NOp::Max);
        self.vals.make_contiguous().sort_by(|a, b| if desc { cmp_bson(b, a) } else { cmp_bson(a, b) });
        self.vals.truncate(n);
        self.mem = self.vals.iter().map(est_bytes).sum();
    }
}

fn nlist(op: NOp, name: &str, arg: &Bson) -> Result<Box<dyn Accumulator>> {
    let d = doc_arg(name, arg)?;
    Ok(Box::new(NList {
        op,
        input: field(name, d, "input")?.clone(),
        n_expr: field(name, d, "n")?.clone(),
        n: None,
        vals: VecDeque::new(),
        mem: 0,
    }))
}

impl Accumulator for NList {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        // `n` is evaluated once, against the first document.
        let n = match self.n {
            Some(n) => n,
            None => {
                let n = eval_n(&self.n_expr, doc, vars, self.op.name())?;
                self.n = Some(n);
                n
            }
        };
        let v = eval(&self.input, doc, vars)?;
        match self.op {
            NOp::First => {
                if !is_missing(&v) && self.vals.len() < n {
                    self.mem += est_bytes(&v);
                    self.vals.push_back(v);
                }
            }
            NOp::Last => {
                if !is_missing(&v) {
                    self.mem += est_bytes(&v);
                    self.vals.push_back(v);
                    if self.vals.len() > n {
                        if let Some(old) = self.vals.pop_front() {
                            self.mem = self.mem.saturating_sub(est_bytes(&old));
                        }
                    }
                }
            }
            NOp::Max | NOp::Min => {
                if !is_nullish(&v) {
                    self.mem += est_bytes(&v);
                    self.vals.push_back(v);
                    if self.vals.len() >= 2 * n + 16 {
                        self.compact();
                    }
                }
            }
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        if matches!(self.op, NOp::Max | NOp::Min) {
            self.compact();
        }
        Ok(Bson::Array(self.vals.iter().cloned().collect()))
    }
    fn mem_bytes(&self) -> usize {
        self.mem
    }
}

// ---------------------------------------------------------------------------
// $top / $bottom / $topN / $bottomN
// ---------------------------------------------------------------------------

struct TopN {
    sort: SortSpec,
    bottom: bool,
    single: bool,
    output: Bson,
    n_expr: Option<Bson>,
    n: Option<usize>,
    items: Vec<(Vec<Bson>, Bson)>,
    mem: usize,
}

impl TopN {
    /// Sort so the "best" items come first (for `$bottom*` that is the reverse sort order), with
    /// stable ties (earlier input wins), then keep only the best `n`.
    fn compact(&mut self) {
        let cap = self.n.unwrap_or(0);
        let sort = &self.sort;
        let bottom = self.bottom;
        self.items.sort_by(|a, b| {
            let o = sort.compare_keys(&a.0, &b.0);
            if bottom { o.reverse() } else { o }
        });
        self.items.truncate(cap);
        self.mem = self.items.iter().map(|(_, v)| est_bytes(v)).sum();
    }
}

fn top(name: &str, arg: &Bson, single: bool, bottom: bool) -> Result<Box<dyn Accumulator>> {
    let d = doc_arg(name, arg)?;
    let sort = sort_spec(field(name, d, "sortBy")?)?;
    let output = field(name, d, "output")?.clone();
    let (n, n_expr) = if single {
        (Some(1), None)
    } else {
        (None, Some(field(name, d, "n")?.clone()))
    };
    Ok(Box::new(TopN {
        sort,
        bottom,
        single,
        output,
        n_expr,
        n,
        items: Vec::new(),
        mem: 0,
    }))
}

impl Accumulator for TopN {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        let cap = match self.n {
            Some(n) => n,
            None => {
                let e = self
                    .n_expr
                    .as_ref()
                    .ok_or_else(|| Error::bad_value("topN requires an 'n' argument"))?;
                let n = eval_n(e, doc, vars, "$topN/$bottomN")?;
                self.n = Some(n);
                n
            }
        };
        let key = self.sort.sort_key(doc);
        let out = nn(eval(&self.output, doc, vars)?);
        self.mem += est_bytes(&out);
        self.items.push((key, out));
        if self.items.len() >= 2 * cap + 16 {
            self.compact();
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        self.compact();
        if self.single {
            Ok(self.items.first().map_or(Bson::Null, |(_, v)| v.clone()))
        } else {
            Ok(Bson::Array(self.items.iter().map(|(_, v)| v.clone()).collect()))
        }
    }
    fn mem_bytes(&self) -> usize {
        self.mem
    }
}

// ---------------------------------------------------------------------------
// $median / $percentile (discrete: element at ceil(p*n)-1 of the sorted values)
// ---------------------------------------------------------------------------

struct Percentiles {
    input: Bson,
    ps: Vec<f64>,
    single: bool,
    vals: Vec<f64>,
}

impl Accumulator for Percentiles {
    fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
        if let Some(x) = num_f64(&eval(&self.input, doc, vars)?) {
            self.vals.push(x);
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<Bson> {
        if self.vals.is_empty() {
            return Ok(if self.single {
                Bson::Null
            } else {
                Bson::Array(self.ps.iter().map(|_| Bson::Null).collect())
            });
        }
        self.vals.sort_by(f64::total_cmp);
        let vals = &self.vals;
        let n = vals.len();
        let at = |p: f64| Bson::Double(vals[((p * n as f64).ceil() as usize).saturating_sub(1).min(n - 1)]);
        if self.single {
            Ok(at(self.ps[0]))
        } else {
            Ok(Bson::Array(self.ps.iter().map(|&p| at(p)).collect()))
        }
    }
    fn mem_bytes(&self) -> usize {
        self.vals.len() * 8
    }
}

// ---------------------------------------------------------------------------
// Grouping stage
// ---------------------------------------------------------------------------

/// One parsed `<field>: {<op>: <arg>}` entry of `$group` / `$bucket` / `$bucketAuto` output.
struct AccField {
    name: String,
    op: String,
    arg: Bson,
}

fn not_acc(name: &str) -> Error {
    Error::new(40234, format!("The field '{name}' must be an accumulator object"))
}

fn parse_acc_fields(spec: &Document, skip_id: bool) -> Result<Vec<AccField>> {
    let mut out = Vec::new();
    for (name, v) in spec {
        if skip_id && name == "_id" {
            continue;
        }
        if name.is_empty() {
            return Err(Error::new(12116300, "The field name '' cannot be an empty string"));
        }
        if name.contains('.') {
            return Err(Error::new(40235, format!("The field name '{name}' cannot contain '.'")));
        }
        let Bson::Document(acc) = v else {
            return Err(not_acc(name));
        };
        let mut it = acc.iter();
        match (it.next(), it.next()) {
            (Some((op, arg)), None) if op.starts_with('$') => out.push(AccField {
                name: name.clone(),
                op: op.clone(),
                arg: arg.clone(),
            }),
            _ => return Err(not_acc(name)),
        }
    }
    Ok(out)
}

fn build_accs(fields: &[AccField]) -> Result<Vec<Box<dyn Accumulator>>> {
    fields.iter().map(|f| make_accumulator(&f.op, &f.arg, false)).collect()
}

pub fn group_stage(input: DocIter, spec: &Document, opts: &AggOptions) -> Result<DocIter> {
    let id_expr = spec
        .get("_id")
        .ok_or_else(|| Error::new(15955, "a group specification must include an _id"))?
        .clone();
    let fields = parse_acc_fields(spec, true)?;
    // Validate operators eagerly so unknown operators fail even on empty input.
    build_accs(&fields)?;

    let mut groups: Vec<(Bson, Vec<Box<dyn Accumulator>>)> = Vec::new();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut mem = 0usize;
    for item in input {
        let doc = item?;
        let key = nn(eval(&id_expr, &doc, &opts.vars)?);
        let kb = key_bytes(&key);
        let existing = index.get(&kb).copied();
        let gi = match existing {
            Some(g) => g,
            None => {
                let accs = build_accs(&fields)?;
                mem += kb.len() + GROUP_OVERHEAD;
                groups.push((key, accs));
                index.insert(kb, groups.len() - 1);
                groups.len() - 1
            }
        };
        let accs = &mut groups[gi].1;
        let before: usize = accs.iter().map(|a| a.mem_bytes()).sum();
        for a in accs.iter_mut() {
            a.process(&doc, &opts.vars)?;
        }
        let after: usize = accs.iter().map(|a| a.mem_bytes()).sum();
        mem = mem.saturating_sub(before) + after;
        if mem > opts.max_memory_bytes && !opts.allow_disk_use {
            return Err(group_mem_error(opts));
        }
    }

    let mut out: Vec<Result<Document>> = Vec::with_capacity(groups.len());
    for (id, mut accs) in groups {
        let mut d = Document::new();
        d.insert("_id", id);
        for (f, acc) in fields.iter().zip(accs.iter_mut()) {
            d.insert(f.name.as_str(), acc.finish()?);
        }
        out.push(Ok(d));
    }
    let it: DocIter = Box::new(out.into_iter());
    Ok(it)
}

// ---------------------------------------------------------------------------
// $bucket
// ---------------------------------------------------------------------------

pub fn bucket_stage(input: DocIter, spec: &Document, opts: &AggOptions) -> Result<DocIter> {
    let group_by = spec
        .get("groupBy")
        .ok_or_else(|| Error::new(40198, "$bucket requires a 'groupBy' field"))?
        .clone();
    let boundaries = match spec.get("boundaries") {
        Some(Bson::Array(b)) if b.len() >= 2 => b.clone(),
        _ => {
            return Err(Error::new(
                40192,
                "$bucket requires 'boundaries' to be an array with at least two values",
            ));
        }
    };
    for w in boundaries.windows(2) {
        if type_order(&w[0]) != type_order(&w[1]) {
            return Err(Error::new(40193, "The $bucket 'boundaries' field must all be the same type"));
        }
        if cmp_bson(&w[0], &w[1]) != Ordering::Less {
            return Err(Error::new(
                40194,
                "The $bucket 'boundaries' field must be sorted in ascending order",
            ));
        }
    }
    let default = spec.get("default").cloned();
    if let Some(d) = &default {
        let lo = &boundaries[0];
        let hi = &boundaries[boundaries.len() - 1];
        if cmp_bson(d, lo) != Ordering::Less && cmp_bson(d, hi) == Ordering::Less {
            return Err(Error::new(
                40199,
                "The $bucket 'default' value must be less than the lower boundary or greater than or equal to the upper boundary",
            ));
        }
    }
    let out_fields = match spec.get("output") {
        Some(Bson::Document(o)) => parse_acc_fields(o, false)?,
        None => vec![AccField {
            name: "count".to_string(),
            op: "$sum".to_string(),
            arg: Bson::Int32(1),
        }],
        Some(_) => return Err(Error::bad_value("$bucket 'output' must be an object")),
    };
    build_accs(&out_fields)?;

    let nb = boundaries.len() - 1;
    let total = nb + usize::from(default.is_some());
    let mut buckets: Vec<Option<Vec<Box<dyn Accumulator>>>> = (0..total).map(|_| None).collect();
    for item in input {
        let doc = item?;
        let v = eval(&group_by, &doc, &opts.vars)?;
        let pos = boundaries.partition_point(|b| cmp_bson(b, &v) != Ordering::Greater);
        let idx = if pos == 0 || pos == boundaries.len() {
            if default.is_some() {
                nb
            } else {
                return Err(Error::new(
                    40066,
                    "$bucket could not find a matching branch for an input, and no default was specified.",
                ));
            }
        } else {
            pos - 1
        };
        if buckets[idx].is_none() {
            buckets[idx] = Some(build_accs(&out_fields)?);
        }
        if let Some(accs) = buckets[idx].as_mut() {
            for a in accs.iter_mut() {
                a.process(&doc, &opts.vars)?;
            }
        }
    }

    let mut out: Vec<Result<Document>> = Vec::new();
    for (idx, slot) in buckets.into_iter().enumerate() {
        let Some(mut accs) = slot else { continue };
        let id = if idx < nb {
            boundaries[idx].clone()
        } else {
            default.clone().unwrap_or(Bson::Null)
        };
        let mut d = Document::new();
        d.insert("_id", id);
        for (f, a) in out_fields.iter().zip(accs.iter_mut()) {
            d.insert(f.name.as_str(), a.finish()?);
        }
        out.push(Ok(d));
    }
    let it: DocIter = Box::new(out.into_iter());
    Ok(it)
}

// ---------------------------------------------------------------------------
// $bucketAuto
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Gran {
    PowersOf2,
    OneTwoFive,
}

impl Gran {
    fn parse(s: &str) -> Result<Gran> {
        match s {
            "POWERSOF2" => Ok(Gran::PowersOf2),
            "1-2-5" => Ok(Gran::OneTwoFive),
            other => Err(Error::bad_value(format!("Unsupported granularity: {other}"))),
        }
    }

    /// Largest series value strictly below x, for x > 0 (MongoDB's GranularityRounder::roundDown).
    fn lower_pos(self, x: f64) -> f64 {
        match self {
            Gran::PowersOf2 => {
                let mut v = 2f64.powi(x.log2().floor() as i32);
                while v >= x {
                    v /= 2.0;
                }
                while v * 2.0 < x {
                    v *= 2.0;
                }
                v
            }
            Gran::OneTwoFive => {
                let e = x.log10().floor() as i32;
                series_125(e)
                    .into_iter()
                    .filter(|c| *c < x)
                    .fold(0.0, f64::max)
            }
        }
    }

    /// Smallest series value strictly above x, for x > 0 (MongoDB's roundUp).
    fn upper_pos(self, x: f64) -> f64 {
        match self {
            Gran::PowersOf2 => self.lower_pos(x) * if self.lower_pos(x) * 2.0 > x { 2.0 } else { 4.0 },
            Gran::OneTwoFive => {
                let e = x.log10().floor() as i32;
                series_125(e)
                    .into_iter()
                    .filter(|c| *c > x)
                    .fold(f64::INFINITY, f64::min)
            }
        }
    }

    /// Round toward negative infinity onto the series.
    fn floor(self, x: f64) -> f64 {
        if x == 0.0 {
            0.0
        } else if x > 0.0 {
            self.lower_pos(x)
        } else {
            -self.upper_pos(-x)
        }
    }

    /// Round toward positive infinity onto the series.
    fn ceil(self, x: f64) -> f64 {
        if x == 0.0 {
            0.0
        } else if x > 0.0 {
            self.upper_pos(x)
        } else {
            -self.lower_pos(-x)
        }
    }
}

/// Candidate 1-2-5 series values spanning decade `e` (covers neighbours of x in [10^e, 10^(e+1))).
fn series_125(e: i32) -> Vec<f64> {
    (e - 1..=e + 1)
        .flat_map(|k| [1.0, 2.0, 5.0].map(|m| m * 10f64.powi(k)))
        .collect()
}

pub fn bucket_auto_stage(input: DocIter, spec: &Document, opts: &AggOptions) -> Result<DocIter> {
    let group_by = spec
        .get("groupBy")
        .ok_or_else(|| Error::new(40246, "$bucketAuto requires a 'groupBy' field"))?
        .clone();
    let nb = match spec.get("buckets").and_then(num_i64) {
        Some(b) if b > 0 => b as usize,
        _ => {
            return Err(Error::new(
                40243,
                "The $bucketAuto 'buckets' field must be a positive 32-bit integer",
            ));
        }
    };
    let gran = match spec.get("granularity") {
        None => None,
        Some(Bson::String(g)) => Some(Gran::parse(g)?),
        Some(_) => return Err(Error::bad_value("$bucketAuto 'granularity' must be a string")),
    };
    let out_fields = match spec.get("output") {
        Some(Bson::Document(o)) => parse_acc_fields(o, false)?,
        None => vec![AccField {
            name: "count".to_string(),
            op: "$sum".to_string(),
            arg: Bson::Int32(1),
        }],
        Some(_) => return Err(Error::bad_value("$bucketAuto 'output' must be an object")),
    };
    build_accs(&out_fields)?;

    let mut rows: Vec<(Bson, Document)> = Vec::new();
    for item in input {
        let doc = item?;
        let key = eval(&group_by, &doc, &opts.vars)?;
        rows.push((key, doc));
    }
    rows.sort_by(|a, b| cmp_bson(&a.0, &b.0));

    let n = rows.len();
    let mut out: Vec<Result<Document>> = Vec::new();
    if n > 0 {
        // Greedy partition into buckets of about n/nb documents; equal values stay together.
        let size = n.div_ceil(nb);
        let mut starts: Vec<usize> = Vec::new();
        let mut start = 0usize;
        while start < n {
            starts.push(start);
            let mut end = (start + size).min(n);
            while end < n && cmp_bson(&rows[end].0, &rows[end - 1].0) == Ordering::Equal {
                end += 1;
            }
            start = end;
        }
        let m = starts.len();
        // bounds[k] = min of bucket k; bounds[k+1] = max of bucket k (= next bucket's min).
        let bounds: Vec<Bson> = (0..=m)
            .map(|k| if k < m { rows[starts[k]].0.clone() } else { rows[n - 1].0.clone() })
            .collect();
        let bounds: Vec<Bson> = match gran {
            None => bounds,
            Some(g) => {
                let mut rounded = Vec::with_capacity(bounds.len());
                for (k, b) in bounds.iter().enumerate() {
                    let x = num_f64(b).ok_or_else(|| {
                        Error::bad_value("$bucketAuto with granularity requires numeric groupBy values")
                    })?;
                    let r = if k == m { g.ceil(x) } else { g.floor(x) };
                    rounded.push(Bson::Double(r));
                }
                rounded
            }
        };
        for k in 0..m {
            let lo = starts[k];
            let hi = if k + 1 < m { starts[k + 1] } else { n };
            let mut accs = build_accs(&out_fields)?;
            for (_, doc) in &rows[lo..hi] {
                for a in accs.iter_mut() {
                    a.process(doc, &opts.vars)?;
                }
            }
            let mut id = Document::new();
            id.insert("min", nn(bounds[k].clone()));
            id.insert("max", nn(bounds[k + 1].clone()));
            let mut d = Document::new();
            d.insert("_id", id);
            for (f, a) in out_fields.iter().zip(accs.iter_mut()) {
                d.insert(f.name.as_str(), a.finish()?);
            }
            out.push(Ok(d));
        }
    }
    let it: DocIter = Box::new(out.into_iter());
    Ok(it)
}

// ---------------------------------------------------------------------------
// Partitioning and sort-key helpers shared by $setWindowFields and $fill
// ---------------------------------------------------------------------------

enum PartBy {
    None,
    Expr(Bson),
    Fields(Vec<String>),
}

/// Partition key values for a document (missing becomes null).
fn part_values(doc: &Document, pb: &PartBy, vars: &Vars) -> Result<Vec<Bson>> {
    Ok(match pb {
        PartBy::None => Vec::new(),
        PartBy::Expr(e) => vec![nn(eval(e, doc, vars)?)],
        PartBy::Fields(fs) => fs
            .iter()
            .map(|f| get_path(doc, f).cloned().unwrap_or(Bson::Null))
            .collect(),
    })
}

struct Part {
    docs: Vec<Document>,
    /// Sort-key values per document (empty when there is no sortBy).
    keys: Vec<Vec<Bson>>,
}

/// Split documents into partitions (first-appearance order), each sorted by `sort` (stable).
fn split_partitions(docs: Vec<Document>, pb: &PartBy, sort: Option<&SortSpec>, vars: &Vars) -> Result<Vec<Part>> {
    let mut pkeys: Vec<Vec<u8>> = Vec::with_capacity(docs.len());
    let mut keys_all: Vec<Vec<Bson>> = Vec::with_capacity(docs.len());
    for doc in &docs {
        pkeys.push(key_bytes(&Bson::Array(part_values(doc, pb, vars)?)));
        keys_all.push(sort.map_or_else(Vec::new, |s| s.sort_key(doc)));
    }
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (i, k) in pkeys.into_iter().enumerate() {
        let existing = index.get(&k).copied();
        let g = match existing {
            Some(g) => g,
            None => {
                groups.push(Vec::new());
                index.insert(k, groups.len() - 1);
                groups.len() - 1
            }
        };
        groups[g].push(i);
    }
    let mut docs_slots: Vec<Option<Document>> = docs.into_iter().map(Some).collect();
    let mut parts = Vec::with_capacity(groups.len());
    for mut idx in groups {
        if let Some(s) = sort {
            idx.sort_by(|&a, &b| s.compare_keys(&keys_all[a], &keys_all[b]));
        }
        let mut pd = Vec::with_capacity(idx.len());
        let mut pk = Vec::with_capacity(idx.len());
        for i in idx {
            if let Some(d) = docs_slots[i].take() {
                pd.push(d);
                pk.push(std::mem::take(&mut keys_all[i]));
            }
        }
        parts.push(Part { docs: pd, keys: pk });
    }
    Ok(parts)
}

/// Sort-key facts that window and fill operators need, computed over all partitions.
struct SortCtx<'a> {
    spec: Option<&'a SortSpec>,
    /// Exactly one sortBy key.
    single: bool,
    /// Every sort value is numeric or a date (only meaningful when `single`).
    numeric: bool,
    /// Some sort value is a date.
    date: bool,
    /// +1 ascending, -1 descending (applied to range bounds so windows follow sort direction).
    sign: f64,
}

impl<'a> SortCtx<'a> {
    fn new(spec: Option<&'a SortSpec>, parts: &[Part]) -> SortCtx<'a> {
        let single = spec.is_some_and(|s| s.keys.len() == 1);
        let mut numeric = single;
        let mut date = false;
        if single {
            for p in parts {
                for k in &p.keys {
                    if let Some(v) = k.first() {
                        if matches!(v, Bson::DateTime(_)) {
                            date = true;
                        }
                        if numeric_time(v).is_none() {
                            numeric = false;
                        }
                    }
                }
            }
        }
        let sign = match spec.and_then(|s| s.keys.first()) {
            Some((_, SortKey::Desc)) => -1.0,
            _ => 1.0,
        };
        SortCtx { spec, single, numeric, date, sign }
    }

    /// Divisor that converts raw time (ms for dates) into the window/derivative unit.
    fn time_scale(&self, unit: Option<f64>) -> f64 {
        if self.date { unit.unwrap_or(1.0) } else { 1.0 }
    }

    /// Time value per document divided by `scale`.
    fn times(&self, keys: &[Vec<Bson>], scale: f64) -> Result<Vec<f64>> {
        if !(self.single && self.numeric) {
            return Err(Error::bad_value(
                "this operator requires a single numeric or date sortBy field",
            ));
        }
        keys.iter()
            .map(|k| {
                k.first()
                    .and_then(numeric_time)
                    .map(|t| t / scale)
                    .ok_or_else(|| Error::bad_value("sortBy values must be numeric or dates"))
            })
            .collect()
    }

    /// Sort-direction-adjusted times, ascending in window space.
    fn kvals(&self, keys: &[Vec<Bson>], scale: f64) -> Result<Vec<f64>> {
        Ok(self.times(keys, scale)?.into_iter().map(|t| t * self.sign).collect())
    }
}

// ---------------------------------------------------------------------------
// $setWindowFields
// ---------------------------------------------------------------------------

/// Window bounds: documents offsets, or sort-value offsets (already in sort units).
/// `None` means unbounded on that side.
#[derive(Clone, Copy)]
enum Win {
    Docs(Option<i64>, Option<i64>),
    Range(Option<f64>, Option<f64>),
}

#[derive(Clone)]
enum WinRaw {
    Docs(Option<i64>, Option<i64>),
    Range {
        lo: Option<f64>,
        hi: Option<f64>,
        unit: Option<f64>,
    },
}

#[derive(Clone)]
enum WinSpec {
    Default,
    Explicit(WinRaw),
}

#[derive(Clone)]
enum WOp {
    Acc { op: String, arg: Bson, win: WinSpec },
    Rank,
    DenseRank,
    DocNumber,
    Shift { output: Bson, by: i64, default: Option<Bson> },
    Derivative { input: Bson, unit: Option<f64>, win: WinSpec },
    Integral { input: Bson, unit: Option<f64>, win: WinSpec },
    Ema { input: Bson, alpha: f64 },
    Covariance { x: Bson, y: Bson, pop: bool, win: WinSpec },
    Locf { input: Bson },
    LinearFill { input: Bson },
}

struct WField {
    name: String,
    op: WOp,
}

fn doc_bound(b: &Bson) -> Result<Option<i64>> {
    match b {
        Bson::String(s) if s == "unbounded" => Ok(None),
        Bson::String(s) if s == "current" => Ok(Some(0)),
        other => num_i64(other)
            .map(Some)
            .ok_or_else(|| Error::bad_value("window document bounds must be integers, \"current\" or \"unbounded\"")),
    }
}

fn range_bound(b: &Bson) -> Result<Option<f64>> {
    match b {
        Bson::String(s) if s == "unbounded" => Ok(None),
        Bson::String(s) if s == "current" => Ok(Some(0.0)),
        other => num_f64(other)
            .map(Some)
            .ok_or_else(|| Error::bad_value("window range bounds must be numbers, \"current\" or \"unbounded\"")),
    }
}

fn parse_window(w: &Document) -> Result<WinRaw> {
    let unit = match w.get("unit") {
        None => None,
        Some(Bson::String(u)) => Some(unit_ms(u)?),
        Some(_) => return Err(Error::bad_value("window 'unit' must be a string")),
    };
    match (w.get("documents"), w.get("range")) {
        (Some(Bson::Array(b)), None) => {
            let (lo, hi) = match b.as_slice() {
                [lo, hi] => (doc_bound(lo)?, doc_bound(hi)?),
                _ => return Err(Error::bad_value("window 'documents' must be [lower, upper]")),
            };
            if let (Some(l), Some(h)) = (lo, hi) {
                if l > h {
                    return Err(Error::bad_value("window lower bound must not exceed upper bound"));
                }
            }
            if unit.is_some() {
                return Err(Error::bad_value("window 'unit' is only valid with a 'range' window"));
            }
            Ok(WinRaw::Docs(lo, hi))
        }
        (None, Some(Bson::Array(b))) => {
            let (lo, hi) = match b.as_slice() {
                [lo, hi] => (range_bound(lo)?, range_bound(hi)?),
                _ => return Err(Error::bad_value("window 'range' must be [lower, upper]")),
            };
            Ok(WinRaw::Range { lo, hi, unit })
        }
        _ => Err(Error::bad_value("window must specify exactly one of 'documents' or 'range'")),
    }
}

fn parse_wfield(name: &str, v: &Bson) -> Result<WField> {
    let d = as_doc(name, v)?;
    let mut op_entry: Option<(&String, &Bson)> = None;
    for (k, val) in d {
        if k == "window" {
            continue;
        }
        if op_entry.is_some() {
            return Err(Error::bad_value(format!("The field '{name}' must specify exactly one operator")));
        }
        op_entry = Some((k, val));
    }
    let (op, arg) = op_entry
        .ok_or_else(|| Error::bad_value(format!("The field '{name}' must specify an operator")))?;
    let win = match d.get("window") {
        None => WinSpec::Default,
        Some(Bson::Document(w)) => WinSpec::Explicit(parse_window(w)?),
        Some(_) => return Err(Error::bad_value("'window' must be an object")),
    };
    let wop = match op.as_str() {
        "$rank" => WOp::Rank,
        "$denseRank" => WOp::DenseRank,
        "$documentNumber" => WOp::DocNumber,
        "$shift" => {
            let a = doc_arg(op, arg)?;
            let by = num_i64(field(op, a, "by")?)
                .ok_or_else(|| Error::bad_value("$shift 'by' must be an integer"))?;
            WOp::Shift {
                output: field(op, a, "output")?.clone(),
                by,
                default: a.get("default").cloned(),
            }
        }
        "$derivative" | "$integral" => {
            let a = doc_arg(op, arg)?;
            let input = field(op, a, "input")?.clone();
            let unit = match a.get("unit") {
                None => None,
                Some(Bson::String(u)) => Some(unit_ms(u)?),
                Some(_) => return Err(Error::bad_value("'unit' must be a string")),
            };
            if op.as_str() == "$derivative" {
                WOp::Derivative { input, unit, win }
            } else {
                WOp::Integral { input, unit, win }
            }
        }
        "$expMovingAvg" => {
            let a = doc_arg(op, arg)?;
            let input = field(op, a, "input")?.clone();
            let alpha = match (a.get("N"), a.get("alpha")) {
                (Some(n), None) => match num_i64(n) {
                    Some(n) if n > 0 => 2.0 / (n as f64 + 1.0),
                    _ => return Err(Error::bad_value("$expMovingAvg 'N' must be a positive integer")),
                },
                (None, Some(al)) => match num_f64(al) {
                    Some(x) if x > 0.0 && x < 1.0 => x,
                    _ => return Err(Error::bad_value("$expMovingAvg 'alpha' must be in (0, 1)")),
                },
                _ => return Err(Error::bad_value("$expMovingAvg requires exactly one of 'N' or 'alpha'")),
            };
            WOp::Ema { input, alpha }
        }
        "$covariancePop" | "$covarianceSamp" => {
            let (x, y) = match arg {
                Bson::Array(xy) => match xy.as_slice() {
                    [x, y] => (x.clone(), y.clone()),
                    _ => return Err(Error::bad_value("covariance requires exactly two expressions")),
                },
                _ => return Err(Error::bad_value("covariance requires an array of two expressions")),
            };
            WOp::Covariance {
                x,
                y,
                pop: op.as_str() == "$covariancePop",
                win,
            }
        }
        "$locf" => WOp::Locf { input: arg.clone() },
        "$linearFill" => WOp::LinearFill { input: arg.clone() },
        _ => {
            make_accumulator(op, arg, true)?;
            WOp::Acc {
                op: op.clone(),
                arg: arg.clone(),
                win,
            }
        }
    };
    Ok(WField {
        name: name.to_string(),
        op: wop,
    })
}

fn resolve_win(spec: &WinSpec, ctx: &SortCtx) -> Result<(Win, f64)> {
    match spec {
        WinSpec::Default => match ctx.spec {
            None => Ok((Win::Docs(None, None), 1.0)),
            Some(_) if ctx.single && ctx.numeric => Ok((Win::Range(None, Some(0.0)), 1.0)),
            Some(_) => Ok((Win::Docs(None, Some(0)), 1.0)),
        },
        WinSpec::Explicit(WinRaw::Docs(lo, hi)) => Ok((Win::Docs(*lo, *hi), 1.0)),
        WinSpec::Explicit(WinRaw::Range { lo, hi, unit }) => {
            if !(ctx.single && ctx.numeric) {
                return Err(Error::bad_value(
                    "range windows require a single numeric or date sortBy field",
                ));
            }
            if !ctx.date && unit.is_some() {
                return Err(Error::bad_value("window 'unit' requires a date sortBy field"));
            }
            Ok((Win::Range(*lo, *hi), ctx.time_scale(*unit)))
        }
    }
}

fn window_kv(w: Win, ctx: &SortCtx, keys: &[Vec<Bson>], scale: f64) -> Result<Vec<f64>> {
    match w {
        Win::Range(..) => ctx.kvals(keys, scale),
        Win::Docs(..) => Ok(Vec::new()),
    }
}

/// Inclusive `(start, end)` index range of the window for document `i`, or `None` if empty.
/// `kv` must be sorted ascending (only used by range windows).
fn window_bounds(w: Win, i: usize, n: usize, kv: &[f64]) -> Option<(usize, usize)> {
    match w {
        Win::Docs(lo, hi) => {
            let (i, last) = (i as i64, n as i64 - 1);
            let s = lo.map_or(0, |l| i + l).max(0);
            let e = hi.map_or(last, |h| i + h).min(last);
            (s <= e).then(|| (s as usize, e as usize))
        }
        Win::Range(lo, hi) => {
            let k = kv[i];
            let s = lo.map_or(0, |l| kv.partition_point(|x| *x < k + l));
            let e = hi.map_or(n, |h| kv.partition_point(|x| *x <= k + h));
            (s < e).then(|| (s, e - 1))
        }
    }
}

/// Evaluate a regular accumulator over each document's window.
fn accumulate_windows(
    docs: &[Document],
    op: &str,
    arg: &Bson,
    w: Win,
    kv: &[f64],
    vars: &Vars,
) -> Result<Vec<Bson>> {
    let n = docs.len();
    let mut out = Vec::with_capacity(n);
    let lower_open = matches!(w, Win::Docs(None, _) | Win::Range(None, _));
    if lower_open {
        // Window starts at the partition start and its end never moves backwards: feed each
        // document exactly once into one accumulator.
        let mut acc = make_accumulator(op, arg, true)?;
        let mut fed = 0usize;
        for i in 0..n {
            let target = window_bounds(w, i, n, kv).map_or(0, |(_, e)| e + 1);
            while fed < target {
                acc.process(&docs[fed], vars)?;
                fed += 1;
            }
            out.push(acc.finish()?);
        }
    } else {
        for i in 0..n {
            let mut acc = make_accumulator(op, arg, true)?;
            if let Some((s, e)) = window_bounds(w, i, n, kv) {
                for doc in &docs[s..=e] {
                    acc.process(doc, vars)?;
                }
            }
            out.push(acc.finish()?);
        }
    }
    Ok(out)
}

fn require_sort<'c>(ctx: &SortCtx<'c>, op: &str) -> Result<&'c SortSpec> {
    ctx.spec.ok_or_else(|| Error::bad_value(format!("{op} requires a sortBy")))
}

fn rank_values(ctx: &SortCtx, keys: &[Vec<Bson>], dense: bool, op: &str) -> Result<Vec<Bson>> {
    let spec = require_sort(ctx, op)?;
    let mut out = Vec::with_capacity(keys.len());
    let (mut rank, mut dense_rank) = (0i32, 0i32);
    for i in 0..keys.len() {
        if i == 0 || spec.compare_keys(&keys[i], &keys[i - 1]) != Ordering::Equal {
            rank = i as i32 + 1;
            dense_rank += 1;
        }
        out.push(Bson::Int32(if dense { dense_rank } else { rank }));
    }
    Ok(out)
}

fn shift_values(docs: &[Document], output: &Bson, by: i64, default: Option<&Bson>, vars: &Vars) -> Result<Vec<Bson>> {
    let vals = eval_all(output, docs, vars)?;
    let n = docs.len() as i64;
    let mut out = Vec::with_capacity(docs.len());
    for i in 0..docs.len() {
        let j = i as i64 + by;
        let v = if (0..n).contains(&j) {
            nn(vals[j as usize].clone())
        } else {
            match default {
                Some(d) => nn(eval(d, &docs[i], vars)?),
                None => Bson::Null,
            }
        };
        out.push(v);
    }
    Ok(out)
}

fn derivative_values(ys: &[Bson], times: &[f64], w: Win, kv: &[f64]) -> Vec<Bson> {
    (0..ys.len())
        .map(|i| match window_bounds(w, i, ys.len(), kv) {
            Some((s, e)) if e > s => match (num_f64(&ys[s]), num_f64(&ys[e])) {
                (Some(a), Some(b)) if times[e] != times[s] => Bson::Double((b - a) / (times[e] - times[s])),
                _ => Bson::Null,
            },
            _ => Bson::Null,
        })
        .collect()
}

fn integral_values(ys: &[Bson], times: &[f64], w: Win, kv: &[f64]) -> Vec<Bson> {
    (0..ys.len())
        .map(|i| match window_bounds(w, i, ys.len(), kv) {
            None => Bson::Null,
            Some((s, e)) => {
                let mut sum = 0.0;
                for j in s..e {
                    if let (Some(a), Some(b)) = (num_f64(&ys[j]), num_f64(&ys[j + 1])) {
                        sum += (a + b) / 2.0 * (times[j + 1] - times[j]);
                    }
                }
                Bson::Double(sum)
            }
        })
        .collect()
}

fn covariance_values(xs: &[Bson], ys: &[Bson], pop: bool, w: Win, kv: &[f64]) -> Vec<Bson> {
    let n = xs.len();
    (0..n)
        .map(|i| {
            let Some((s, e)) = window_bounds(w, i, n, kv) else {
                return Bson::Null;
            };
            let pairs: Vec<(f64, f64)> = (s..=e)
                .filter_map(|j| Some((num_f64(&xs[j])?, num_f64(&ys[j])?)))
                .collect();
            let m = pairs.len() as f64;
            if pairs.is_empty() || (!pop && pairs.len() < 2) {
                return Bson::Null;
            }
            let mx = pairs.iter().map(|p| p.0).sum::<f64>() / m;
            let my = pairs.iter().map(|p| p.1).sum::<f64>() / m;
            let c: f64 = pairs.iter().map(|(x, y)| (x - mx) * (y - my)).sum();
            let denom = if pop { m } else { m - 1.0 };
            Bson::Double(c / denom)
        })
        .collect()
}

/// Exponential moving average over the partition prefix. Null inputs yield null and do not
/// update the average.
fn ema_values(ys: &[Bson], alpha: f64) -> Vec<Bson> {
    let mut prev: Option<f64> = None;
    ys.iter()
        .map(|y| match num_f64(y) {
            Some(x) => {
                let e = match prev {
                    None => x,
                    Some(p) => alpha * x + (1.0 - alpha) * p,
                };
                prev = Some(e);
                Bson::Double(e)
            }
            None => Bson::Null,
        })
        .collect()
}

/// Replace null/missing values with the last non-null value before them.
fn fill_locf(vals: &mut [Bson]) {
    let mut last: Option<Bson> = None;
    for v in vals.iter_mut() {
        if is_nullish(v) {
            if let Some(l) = &last {
                *v = l.clone();
            }
        } else {
            last = Some(v.clone());
        }
    }
}

/// Linear interpolation over `times` for null/missing values lying between two numeric values.
/// Leading and trailing gaps stay null.
fn fill_linear(vals: &mut [Bson], times: &[f64]) {
    let known: Vec<usize> = (0..vals.len()).filter(|&i| !is_nullish(&vals[i])).collect();
    for w in known.windows(2) {
        let (a, b) = (w[0], w[1]);
        let (Some(ya), Some(yb)) = (num_f64(&vals[a]), num_f64(&vals[b])) else {
            continue;
        };
        let (ta, tb) = (times[a], times[b]);
        for j in a + 1..b {
            let y = if tb == ta { ya } else { ya + (yb - ya) * (times[j] - ta) / (tb - ta) };
            vals[j] = Bson::Double(y);
        }
    }
}

fn compute_field(f: &WField, part: &Part, ctx: &SortCtx, vars: &Vars) -> Result<Vec<Bson>> {
    let docs = part.docs.as_slice();
    let vals = match &f.op {
        WOp::Acc { op, arg, win } => {
            let (w, scale) = resolve_win(win, ctx)?;
            let kv = window_kv(w, ctx, &part.keys, scale)?;
            accumulate_windows(docs, op, arg, w, &kv, vars)?
        }
        WOp::Rank => rank_values(ctx, &part.keys, false, "$rank")?,
        WOp::DenseRank => rank_values(ctx, &part.keys, true, "$denseRank")?,
        WOp::DocNumber => {
            require_sort(ctx, "$documentNumber")?;
            (1..=docs.len()).map(|i| Bson::Int32(i as i32)).collect()
        }
        WOp::Shift { output, by, default } => shift_values(docs, output, *by, default.as_ref(), vars)?,
        WOp::Derivative { input, unit, win } => {
            let (w, wscale) = resolve_win(win, ctx)?;
            let kv = window_kv(w, ctx, &part.keys, wscale)?;
            let times = ctx.times(&part.keys, ctx.time_scale(*unit))?;
            let ys = eval_all(input, docs, vars)?;
            derivative_values(&ys, &times, w, &kv)
        }
        WOp::Integral { input, unit, win } => {
            let (w, wscale) = resolve_win(win, ctx)?;
            let kv = window_kv(w, ctx, &part.keys, wscale)?;
            let times = ctx.times(&part.keys, ctx.time_scale(*unit))?;
            let ys = eval_all(input, docs, vars)?;
            integral_values(&ys, &times, w, &kv)
        }
        WOp::Ema { input, alpha } => ema_values(&eval_all(input, docs, vars)?, *alpha),
        WOp::Covariance { x, y, pop, win } => {
            let (w, wscale) = resolve_win(win, ctx)?;
            let kv = window_kv(w, ctx, &part.keys, wscale)?;
            let xs = eval_all(x, docs, vars)?;
            let ys = eval_all(y, docs, vars)?;
            covariance_values(&xs, &ys, *pop, w, &kv)
        }
        WOp::Locf { input } => {
            let mut ys = eval_all(input, docs, vars)?;
            fill_locf(&mut ys);
            ys
        }
        WOp::LinearFill { input } => {
            let mut ys = eval_all(input, docs, vars)?;
            let times = ctx.times(&part.keys, 1.0)?;
            fill_linear(&mut ys, &times);
            ys
        }
    };
    Ok(vals.into_iter().map(nn).collect())
}

pub fn set_window_fields_stage(input: DocIter, spec: &Document, opts: &AggOptions) -> Result<DocIter> {
    let mut partition = PartBy::None;
    let mut sort: Option<SortSpec> = None;
    let mut output: Option<&Document> = None;
    for (k, v) in spec {
        match k.as_str() {
            "partitionBy" => partition = PartBy::Expr(v.clone()),
            "sortBy" => sort = Some(sort_spec(v)?),
            "output" => output = Some(as_doc(k, v)?),
            other => {
                return Err(Error::new(
                    40415,
                    format!("BSON field '$setWindowFields.{other}' is an unknown field"),
                ));
            }
        }
    }
    let output = output.ok_or_else(|| Error::bad_value("$setWindowFields requires an 'output' field"))?;
    let fields = output
        .iter()
        .map(|(name, v)| parse_wfield(name, v))
        .collect::<Result<Vec<WField>>>()?;
    let docs = input.collect::<Result<Vec<Document>>>()?;
    let parts = split_partitions(docs, &partition, sort.as_ref(), &opts.vars)?;
    let ctx = SortCtx::new(sort.as_ref(), &parts);

    let mut out: Vec<Result<Document>> = Vec::new();
    for mut part in parts {
        let mut computed = Vec::with_capacity(fields.len());
        for f in &fields {
            computed.push(compute_field(f, &part, &ctx, &opts.vars)?);
        }
        for (f, vals) in fields.iter().zip(computed) {
            for (d, v) in part.docs.iter_mut().zip(vals) {
                set_path(d, &f.name, v)?;
            }
        }
        out.extend(part.docs.into_iter().map(Ok));
    }
    let it: DocIter = Box::new(out.into_iter());
    Ok(it)
}

// ---------------------------------------------------------------------------
// $densify
// ---------------------------------------------------------------------------

enum Bounds {
    Full,
    Partition,
    Explicit(Bson, Bson),
}

struct DItem {
    x: f64,
    int: bool,
    doc: Document,
}

struct DPart {
    key: Vec<Bson>,
    items: Vec<DItem>,
}

fn bound_value(b: &Bson) -> Result<f64> {
    numeric_time(b).ok_or_else(|| Error::bad_value("$densify range bounds must be numeric or dates"))
}

fn norm_bits(x: f64) -> u64 {
    if x == 0.0 { 0 } else { x.to_bits() }
}

pub fn densify_stage(input: DocIter, spec: &Document, _opts: &AggOptions) -> Result<DocIter> {
    let field = match spec.get("field") {
        Some(Bson::String(f)) => f.clone(),
        _ => return Err(Error::bad_value("$densify requires a 'field' string")),
    };
    let part_fields = string_list(spec.get("partitionByFields"), "partitionByFields")?;
    let under = |a: &str, b: &str| a == b || a.starts_with(&format!("{b}."));
    for p in &part_fields {
        if under(p, &field) {
            return Err(Error::new(8993000, format!("partitionByFields '{p}' cannot include field '{field}' that is being densified.")));
        }
        if under(&field, p) {
            return Err(Error::new(9554500, format!("field '{field}' that is being densified cannot include partitionByFields '{p}'.")));
        }
    }
    let range = match spec.get("range") {
        Some(Bson::Document(r)) => r,
        _ => return Err(Error::bad_value("$densify requires a 'range' object")),
    };
    let step = range
        .get("step")
        .and_then(num_f64)
        .filter(|s| *s > 0.0)
        .ok_or_else(|| Error::bad_value("$densify range 'step' must be a positive number"))?;
    let step_int = range.get("step").is_some_and(is_intlike);
    let unit = match range.get("unit") {
        None => None,
        Some(Bson::String(u)) => Some(unit_ms(u)?),
        Some(_) => return Err(Error::bad_value("$densify range 'unit' must be a string")),
    };
    let bounds = match range.get("bounds") {
        Some(Bson::String(s)) if s == "full" => Bounds::Full,
        Some(Bson::String(s)) if s == "partition" => Bounds::Partition,
        Some(Bson::Array(b)) => match b.as_slice() {
            [lo, hi] => Bounds::Explicit(lo.clone(), hi.clone()),
            _ => return Err(Error::bad_value("$densify range 'bounds' array must have two values")),
        },
        _ => {
            return Err(Error::bad_value(
                "$densify range 'bounds' must be \"full\", \"partition\" or [lower, upper]",
            ));
        }
    };

    let mut passthrough: Vec<Document> = Vec::new();
    let mut parts: Vec<DPart> = Vec::new();
    let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut date_field: Option<bool> = None;
    for item in input {
        let doc = item?;
        let raw = get_path(&doc, &field).filter(|v| !is_nullish(v)).cloned();
        let Some(raw) = raw else {
            passthrough.push(doc);
            continue;
        };
        let (x, is_date) = match &raw {
            Bson::DateTime(t) => (t.timestamp_millis() as f64, true),
            other => match num_f64(other) {
                Some(x) => (x, false),
                None => return Err(Error::bad_value("$densify field values must be numeric or dates")),
            },
        };
        if let Some(prev) = date_field {
            if prev != is_date {
                return Err(Error::bad_value("$densify field values must all be numeric or all dates"));
            }
        }
        date_field = Some(is_date);
        let key: Vec<Bson> = part_fields
            .iter()
            .map(|p| get_path(&doc, p).cloned().unwrap_or(Bson::Null))
            .collect();
        let kb = key_bytes(&Bson::Array(key.clone()));
        let existing = index.get(&kb).copied();
        let slot = match existing {
            Some(s) => s,
            None => {
                parts.push(DPart { key, items: Vec::new() });
                index.insert(kb, parts.len() - 1);
                parts.len() - 1
            }
        };
        parts[slot].items.push(DItem {
            x,
            int: matches!(raw, Bson::Int32(_) | Bson::Int64(_)),
            doc,
        });
    }

    let is_date = date_field.unwrap_or(false);
    if is_date && unit.is_none() {
        return Err(Error::bad_value("$densify on a date field requires range.unit"));
    }
    if !is_date && unit.is_some() && date_field.is_some() {
        return Err(Error::bad_value("$densify range.unit is only valid for date fields"));
    }
    let step_v = step * unit.unwrap_or(1.0);

    let (mut gmin, mut gmax) = (f64::INFINITY, f64::NEG_INFINITY);
    let mut gint = true;
    for p in &parts {
        for it in &p.items {
            gmin = gmin.min(it.x);
            gmax = gmax.max(it.x);
            gint &= it.int;
        }
    }
    let (lo_e, hi_e, ints_e) = match &bounds {
        Bounds::Explicit(a, b) => (bound_value(a)?, bound_value(b)?, is_intlike(a) && is_intlike(b)),
        _ => (0.0, 0.0, false),
    };

    let mut done: Vec<(Vec<Bson>, Vec<Document>)> = Vec::with_capacity(parts.len());
    for part in parts {
        let (pmin, pmax) = part
            .items
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(lo, hi), it| (lo.min(it.x), hi.max(it.x)));
        let (lo, hi, hi_incl, int_mode) = match &bounds {
            Bounds::Full => (gmin, gmax, true, step_int && gint),
            Bounds::Partition => (pmin, pmax, true, step_int && part.items.iter().all(|i| i.int)),
            Bounds::Explicit(..) => (lo_e, hi_e, false, step_int && ints_e),
        };
        let existing: HashSet<u64> = part.items.iter().map(|i| norm_bits(i.x)).collect();
        let mut generated: Vec<(f64, Document)> = Vec::new();
        let mut k: u64 = 0;
        loop {
            let v = lo + k as f64 * step_v;
            let past = if hi_incl { v > hi } else { v >= hi };
            if past {
                break;
            }
            k += 1;
            if k > MAX_DENSIFY_STEPS {
                return Err(Error::bad_value(format!(
                    "$densify would generate more than {MAX_DENSIFY_STEPS} documents in one partition"
                )));
            }
            if existing.contains(&norm_bits(v)) {
                continue;
            }
            let value = if is_date {
                Bson::DateTime(bson::DateTime::from_millis(v as i64))
            } else if int_mode && v >= i32::MIN as f64 && v <= i32::MAX as f64 {
                Bson::Int32(v as i32)
            } else if int_mode {
                Bson::Int64(v as i64)
            } else {
                Bson::Double(v)
            };
            let mut d = Document::new();
            for (name, val) in part_fields.iter().zip(&part.key) {
                set_path(&mut d, name, val.clone())?;
            }
            set_path(&mut d, &field, value)?;
            generated.push((v, d));
        }
        let mut all: Vec<(f64, Document)> = part
            .items
            .into_iter()
            .map(|i| (i.x, i.doc))
            .chain(generated)
            .collect();
        all.sort_by(|a, b| a.0.total_cmp(&b.0));
        done.push((part.key, all.into_iter().map(|(_, d)| d).collect()));
    }
    done.sort_by(|a, b| cmp_value_lists(&a.0, &b.0));

    let mut out: Vec<Result<Document>> = Vec::new();
    for (_, docs) in done {
        out.extend(docs.into_iter().map(Ok));
    }
    out.extend(passthrough.into_iter().map(Ok));
    let it: DocIter = Box::new(out.into_iter());
    Ok(it)
}

// ---------------------------------------------------------------------------
// $fill
// ---------------------------------------------------------------------------

enum FillMethod {
    Value(Bson),
    Locf,
    Linear,
}

pub fn fill_stage(input: DocIter, spec: &Document, opts: &AggOptions) -> Result<DocIter> {
    let mut pb = PartBy::None;
    let mut sort: Option<SortSpec> = None;
    let mut output: Option<&Document> = None;
    for (k, v) in spec {
        match k.as_str() {
            "partitionBy" => {
                if !matches!(pb, PartBy::None) {
                    return Err(Error::bad_value("$fill accepts only one of partitionBy or partitionByFields"));
                }
                pb = PartBy::Expr(v.clone());
            }
            "partitionByFields" => {
                if !matches!(pb, PartBy::None) {
                    return Err(Error::bad_value("$fill accepts only one of partitionBy or partitionByFields"));
                }
                pb = PartBy::Fields(string_list(Some(v), "partitionByFields")?);
            }
            "sortBy" => sort = Some(sort_spec(v)?),
            "output" => output = Some(as_doc(k, v)?),
            other => {
                return Err(Error::new(
                    40415,
                    format!("BSON field '$fill.{other}' is an unknown field"),
                ));
            }
        }
    }
    let output = output.ok_or_else(|| Error::bad_value("$fill requires an 'output' field"))?;
    let mut fields: Vec<(String, FillMethod)> = Vec::new();
    for (name, v) in output {
        let m = as_doc(name, v)?;
        let method = match (m.get("value"), m.get("method"), m.len()) {
            (Some(e), None, 1) => FillMethod::Value(e.clone()),
            (None, Some(Bson::String(s)), 1) => match s.as_str() {
                "locf" => FillMethod::Locf,
                "linear" => {
                    if sort.is_none() {
                        return Err(Error::bad_value("$fill method 'linear' requires sortBy"));
                    }
                    FillMethod::Linear
                }
                other => return Err(Error::bad_value(format!("unsupported $fill method: {other}"))),
            },
            _ => {
                return Err(Error::bad_value(format!(
                    "$fill output for '{name}' must specify exactly one of 'value' or 'method'"
                )));
            }
        };
        fields.push((name.clone(), method));
    }

    let docs = input.collect::<Result<Vec<Document>>>()?;
    let parts = split_partitions(docs, &pb, sort.as_ref(), &opts.vars)?;
    let ctx = SortCtx::new(sort.as_ref(), &parts);

    let mut out: Vec<Result<Document>> = Vec::new();
    for mut part in parts {
        for (name, method) in &fields {
            let updates: Vec<Option<Bson>> = match method {
                FillMethod::Value(expr) => {
                    let mut ups = Vec::with_capacity(part.docs.len());
                    for d in &part.docs {
                        let absent = get_path(d, name).is_none_or(is_nullish);
                        ups.push(if absent {
                            Some(nn(eval(expr, d, &opts.vars)?))
                        } else {
                            None
                        });
                    }
                    ups
                }
                FillMethod::Locf | FillMethod::Linear => {
                    let orig: Vec<Bson> = part
                        .docs
                        .iter()
                        .map(|d| get_path(d, name).cloned().unwrap_or(MISSING))
                        .collect();
                    let mut vals = orig.clone();
                    if matches!(method, FillMethod::Locf) {
                        fill_locf(&mut vals);
                    } else {
                        let times = ctx.times(&part.keys, 1.0)?;
                        fill_linear(&mut vals, &times);
                    }
                    orig.iter()
                        .zip(vals)
                        .map(|(o, v)| if is_nullish(o) && !is_nullish(&v) { Some(v) } else { None })
                        .collect()
                }
            };
            for (d, u) in part.docs.iter_mut().zip(updates) {
                if let Some(v) = u {
                    set_path(d, name, v)?;
                }
            }
        }
        out.extend(part.docs.into_iter().map(Ok));
    }
    let it: DocIter = Box::new(out.into_iter());
    Ok(it)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agg::pipeline::AggContext;
    use bson::doc;
    use std::path::PathBuf;
    use std::sync::Arc;

    struct NoCtx;

    impl AggContext for NoCtx {
        fn scan(&self, _db: &str, _coll: &str) -> Result<DocIter> {
            let it: DocIter = Box::new(std::iter::empty());
            Ok(it)
        }
        fn find(&self, _db: &str, _coll: &str, _filter: &Document) -> Result<Vec<Document>> {
            Ok(Vec::new())
        }
        fn replace_collection(&self, _db: &str, _coll: &str, _docs: Vec<Document>) -> Result<()> {
            Ok(())
        }
        fn insert(&self, _db: &str, _coll: &str, _doc: Document) -> Result<()> {
            Ok(())
        }
        fn replace_by_id(&self, _db: &str, _coll: &str, _id: &Bson, _doc: Document) -> Result<()> {
            Ok(())
        }
        fn coll_stats(&self, _db: &str, _coll: &str, _opts: &Document) -> Result<Document> {
            Err(Error::new(26, "no collections in unit tests"))
        }
        fn index_stats(&self, _db: &str, _coll: &str) -> Result<Vec<Document>> {
            Ok(Vec::new())
        }
        fn temp_dir(&self) -> PathBuf {
            std::env::temp_dir()
        }
    }

    type StageFn = fn(DocIter, &Document, &AggOptions) -> Result<DocIter>;

    fn opts(max_memory_bytes: usize, allow_disk_use: bool) -> AggOptions {
        AggOptions {
            db: "test".to_string(),
            coll: Some("c".to_string()),
            allow_disk_use,
            max_memory_bytes,
            vars: Vars::new(),
            ctx: Arc::new(NoCtx),
        }
    }

    fn input(docs: Vec<Document>) -> DocIter {
        Box::new(docs.into_iter().map(Ok))
    }

    fn run(f: StageFn, docs: Vec<Document>, spec: &Document) -> Result<Vec<Document>> {
        let o = opts(usize::MAX, false);
        let it = f(input(docs), spec, &o)?;
        it.collect()
    }

    fn code_of(r: Result<DocIter>) -> i32 {
        match r {
            Err(e) => e.code,
            Ok(_) => panic!("expected an error"),
        }
    }

    #[test]
    fn group_normalizes_numeric_keys() {
        let docs = vec![
            doc! {"g": 1, "v": 1},
            doc! {"g": 1.0, "v": 2},
            doc! {"g": Bson::Int64(1), "v": 3},
            doc! {"g": 2, "v": 4},
        ];
        let out = run(group_stage, docs, &doc! {"_id": "$g", "s": {"$sum": "$v"}}).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].get("s"), Some(&Bson::Int32(6)));
        assert_eq!(out[1].get("s"), Some(&Bson::Int32(4)));
    }

    #[test]
    fn sum_overflow_promotes_to_int64() {
        let docs = vec![doc! {"v": i32::MAX}, doc! {"v": i32::MAX}];
        let out = run(group_stage, docs, &doc! {"_id": null, "s": {"$sum": "$v"}}).unwrap();
        assert_eq!(out[0].get("s"), Some(&Bson::Int64(2 * i32::MAX as i64)));
    }

    #[test]
    fn sum_of_nothing_is_int32_zero() {
        let docs = vec![doc! {"v": "x"}];
        let out = run(group_stage, docs, &doc! {"_id": null, "s": {"$sum": "$v"}}).unwrap();
        assert_eq!(out[0].get("s"), Some(&Bson::Int32(0)));
    }

    #[test]
    fn avg_skips_non_numeric_and_is_double() {
        let docs = vec![doc! {"v": 1}, doc! {"v": Bson::Null}, doc! {"v": 3}, doc! {"v": "x"}];
        let out = run(group_stage, docs, &doc! {"_id": null, "a": {"$avg": "$v"}}).unwrap();
        assert_eq!(out[0].get("a"), Some(&Bson::Double(2.0)));
    }

    #[test]
    fn avg_of_nothing_is_null() {
        let docs = vec![doc! {"v": Bson::Null}];
        let out = run(group_stage, docs, &doc! {"_id": null, "a": {"$avg": "$v"}}).unwrap();
        assert_eq!(out[0].get("a"), Some(&Bson::Null));
    }

    #[test]
    fn push_skips_missing_and_add_to_set_dedupes() {
        let docs = vec![doc! {"v": 1}, doc! {"v": 1.0}, doc! {"v": 2}, doc! {}];
        let out = run(
            group_stage,
            docs,
            &doc! {"_id": null, "p": {"$push": "$v"}, "s": {"$addToSet": "$v"}},
        )
        .unwrap();
        assert_eq!(out[0].get_array("p").unwrap().len(), 3);
        assert_eq!(out[0].get_array("s").unwrap().len(), 2);
    }

    #[test]
    fn first_and_last_map_missing_to_null() {
        let docs = vec![doc! {"v": 1}, doc! {"v": 2}, doc! {}];
        let out = run(
            group_stage,
            docs,
            &doc! {"_id": null, "f": {"$first": "$v"}, "l": {"$last": "$v"}},
        )
        .unwrap();
        assert_eq!(out[0].get("f"), Some(&Bson::Int32(1)));
        assert_eq!(out[0].get("l"), Some(&Bson::Null));
    }

    #[test]
    fn top_n_with_sort_by_descending() {
        let docs = vec![
            doc! {"s": 5, "v": "a"},
            doc! {"s": 9, "v": "b"},
            doc! {"s": 7, "v": "c"},
        ];
        let out = run(
            group_stage,
            docs,
            &doc! {"_id": null, "t": {"$topN": {"n": 2, "sortBy": {"s": -1}, "output": "$v"}}},
        )
        .unwrap();
        assert_eq!(
            out[0].get_array("t").unwrap(),
            &vec![Bson::String("b".into()), Bson::String("c".into())]
        );
    }

    #[test]
    fn top_and_bottom_ties_keep_first() {
        let docs = vec![doc! {"s": 1, "v": "first"}, doc! {"s": 1, "v": "second"}];
        let out = run(
            group_stage,
            docs,
            &doc! {"_id": null, "t": {"$top": {"sortBy": {"s": 1}, "output": "$v"}}},
        )
        .unwrap();
        assert_eq!(out[0].get("t"), Some(&Bson::String("first".into())));
    }

    #[test]
    fn count_returns_int32() {
        let docs = vec![doc! {"v": 1}, doc! {"v": 2}, doc! {"v": 3}];
        let out = run(group_stage, docs, &doc! {"_id": null, "c": {"$count": {}}}).unwrap();
        assert_eq!(out[0].get("c"), Some(&Bson::Int32(3)));
    }

    #[test]
    fn unknown_operator_errors() {
        let r = group_stage(input(vec![]), &doc! {"_id": null, "x": {"$nope": 1}}, &opts(usize::MAX, false));
        assert_eq!(code_of(r), 15952);
    }

    #[test]
    fn missing_id_errors() {
        let r = group_stage(input(vec![]), &doc! {"c": {"$sum": 1}}, &opts(usize::MAX, false));
        assert_eq!(code_of(r), 15955);
    }

    #[test]
    fn group_memory_limit_without_disk_use_errors() {
        let docs = vec![doc! {"v": "some value that is larger than ten bytes"}];
        let o = opts(10, false);
        let r = group_stage(input(docs), &doc! {"_id": null, "p": {"$push": "$v"}}, &o);
        assert_eq!(code_of(r), 292);
    }

    #[test]
    fn bucket_default_bucket_is_last() {
        let docs = vec![doc! {"v": 5}, doc! {"v": 15}, doc! {"v": 25}, doc! {"v": 7}];
        let out = run(
            bucket_stage,
            docs,
            &doc! {"groupBy": "$v", "boundaries": [0, 10, 20], "default": "other"},
        )
        .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].get("_id"), Some(&Bson::Int32(0)));
        assert_eq!(out[0].get("count"), Some(&Bson::Int32(2)));
        assert_eq!(out[2].get("_id"), Some(&Bson::String("other".into())));
        assert_eq!(out[2].get("count"), Some(&Bson::Int32(1)));
    }

    #[test]
    fn bucket_without_default_errors_on_no_match() {
        let docs = vec![doc! {"v": 25}];
        let r = bucket_stage(
            input(docs),
            &doc! {"groupBy": "$v", "boundaries": [0, 10]},
            &opts(usize::MAX, false),
        );
        assert_eq!(code_of(r), 40066);
    }

    #[test]
    fn bucket_auto_distributes_evenly() {
        let docs = (1..=6).map(|v| doc! {"v": v}).collect::<Vec<_>>();
        let out = run(bucket_auto_stage, docs, &doc! {"groupBy": "$v", "buckets": 3}).unwrap();
        assert_eq!(out.len(), 3);
        for d in &out {
            assert_eq!(d.get("count"), Some(&Bson::Int32(2)));
        }
        let id0 = out[0].get_document("_id").unwrap();
        assert_eq!(id0.get("min"), Some(&Bson::Int32(1)));
        assert_eq!(id0.get("max"), Some(&Bson::Int32(3)));
        let id2 = out[2].get_document("_id").unwrap();
        assert_eq!(id2.get("max"), Some(&Bson::Int32(6)));
    }

    #[test]
    fn set_window_fields_cumulative_sum_sorted() {
        let docs = vec![
            doc! {"t": 3, "v": 3},
            doc! {"t": 1, "v": 1},
            doc! {"t": 2, "v": 2},
        ];
        let out = run(
            set_window_fields_stage,
            docs,
            &doc! {
                "sortBy": {"t": 1},
                "output": {"c": {"$sum": "$v", "window": {"documents": vec!["unbounded", "current"]}}}
            },
        )
        .unwrap();
        let cums: Vec<Bson> = out.iter().map(|d| d.get("c").cloned().unwrap()).collect();
        assert_eq!(cums, vec![Bson::Int32(1), Bson::Int32(3), Bson::Int32(6)]);
    }

    #[test]
    fn rank_and_dense_rank_with_ties() {
        let docs = vec![doc! {"v": 10}, doc! {"v": 10}, doc! {"v": 20}];
        let out = run(
            set_window_fields_stage,
            docs,
            &doc! {
                "sortBy": {"v": 1},
                "output": {"r": {"$rank": {}}, "d": {"$denseRank": {}}}
            },
        )
        .unwrap();
        let ranks: Vec<Bson> = out.iter().map(|d| d.get("r").cloned().unwrap()).collect();
        let dense: Vec<Bson> = out.iter().map(|d| d.get("d").cloned().unwrap()).collect();
        assert_eq!(ranks, vec![Bson::Int32(1), Bson::Int32(1), Bson::Int32(3)]);
        assert_eq!(dense, vec![Bson::Int32(1), Bson::Int32(1), Bson::Int32(2)]);
    }

    #[test]
    fn shift_previous_value_with_null_default() {
        let docs = vec![doc! {"t": 1, "v": 1}, doc! {"t": 2, "v": 2}, doc! {"t": 3, "v": 3}];
        let out = run(
            set_window_fields_stage,
            docs,
            &doc! {"sortBy": {"t": 1}, "output": {"p": {"$shift": {"output": "$v", "by": -1}}}},
        )
        .unwrap();
        assert_eq!(out[0].get("p"), Some(&Bson::Null));
        assert_eq!(out[1].get("p"), Some(&Bson::Int32(1)));
        assert_eq!(out[2].get("p"), Some(&Bson::Int32(2)));
    }

    #[test]
    fn locf_carries_last_non_null() {
        let docs = vec![
            doc! {"t": 1, "v": 1},
            doc! {"t": 2, "v": Bson::Null},
            doc! {"t": 3, "v": Bson::Null},
            doc! {"t": 4, "v": 4},
            doc! {"t": 5, "v": Bson::Null},
        ];
        let out = run(
            set_window_fields_stage,
            docs,
            &doc! {"sortBy": {"t": 1}, "output": {"f": {"$locf": "$v"}}},
        )
        .unwrap();
        let f: Vec<Bson> = out.iter().map(|d| d.get("f").cloned().unwrap()).collect();
        assert_eq!(
            f,
            vec![Bson::Int32(1), Bson::Int32(1), Bson::Int32(1), Bson::Int32(4), Bson::Int32(4)]
        );
    }

    #[test]
    fn densify_numeric_full_range() {
        let docs = vec![doc! {"x": 1}, doc! {"x": 3}];
        let out = run(
            densify_stage,
            docs,
            &doc! {"field": "x", "range": {"step": 1, "bounds": "full"}},
        )
        .unwrap();
        let xs: Vec<f64> = out.iter().map(|d| num_f64(d.get("x").unwrap()).unwrap()).collect();
        assert_eq!(xs, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn fill_linear_interpolates_gap() {
        let docs = vec![
            doc! {"t": 0, "v": 1},
            doc! {"t": 1, "v": Bson::Null},
            doc! {"t": 2, "v": 3},
        ];
        let out = run(
            fill_stage,
            docs,
            &doc! {"sortBy": {"t": 1}, "output": {"v": {"method": "linear"}}},
        )
        .unwrap();
        assert_eq!(num_f64(out[1].get("v").unwrap()), Some(2.0));
    }

    #[test]
    fn fill_value_replaces_null_and_missing() {
        let docs = vec![doc! {"v": Bson::Null}, doc! {}, doc! {"v": 5}];
        let out = run(fill_stage, docs, &doc! {"output": {"v": {"value": 0}}}).unwrap();
        assert_eq!(out[0].get("v"), Some(&Bson::Int32(0)));
        assert_eq!(out[1].get("v"), Some(&Bson::Int32(0)));
        assert_eq!(out[2].get("v"), Some(&Bson::Int32(5)));
    }

    #[test]
    fn group_key_canon_distinguishes_strings_from_numbers() {
        assert_ne!(key_bytes(&Bson::String("1".into())), key_bytes(&Bson::Int32(1)));
        assert_eq!(key_bytes(&Bson::Int32(1)), key_bytes(&Bson::Double(1.0)));
    }

    #[test]
    fn gran_125_rounding() {
        assert_eq!(Gran::OneTwoFive.upper_pos(3.0), 5.0);
        assert_eq!(Gran::OneTwoFive.lower_pos(3.0), 2.0);
        assert_eq!(Gran::PowersOf2.lower_pos(9.0), 8.0);
        assert_eq!(Gran::PowersOf2.upper_pos(9.0), 16.0);
    }
}
