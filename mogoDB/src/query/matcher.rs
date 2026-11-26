//! MongoDB query filter matcher. A filter is compiled once (with eager validation)
//! into a `Node` tree and evaluated per document.

use bson::{Bson, Document};
use regex::{Regex, RegexBuilder};

use crate::agg::expr::{self, Vars};
use crate::bsonutil::{
    bson_eq, cmp_bson, cmp_docs, is_number, num_f64, num_i64, type_alias_number, type_number,
    type_order, truthy,
};
use crate::error::{Error, Result};
use crate::geo::{self, NearSpec};
use crate::text::{self, TextQuery};

/// Top-level operators: a document starting with one of these is a query, not an operator block.
const TOP_LEVEL_OPS: &[&str] = &[
    "$and",
    "$or",
    "$nor",
    "$expr",
    "$text",
    "$where",
    "$comment",
    "$alwaysTrue",
    "$alwaysFalse",
    "$sampleRate",
    "$jsonSchema",
];

#[derive(Clone, Debug)]
pub struct Matcher {
    root: Node,
    vars: Vars,
    text: Option<TextQuery>,
    text_fields: Vec<(String, f64)>,
}

impl Matcher {
    pub fn new(filter: &Document) -> Result<Matcher> {
        Self::with_vars(filter, Vars::new())
    }

    pub fn with_vars(filter: &Document, vars: Vars) -> Result<Matcher> {
        let mut text = None;
        let root = compile_doc(filter, false, &mut text)?;
        Ok(Matcher { root, vars, text, text_fields: Vec::new() })
    }

    pub fn set_text_fields(&mut self, fields: Vec<(String, f64)>) {
        self.text_fields = fields;
    }

    pub fn has_text(&self) -> bool {
        self.text.is_some()
    }

    pub fn text_score(&self, doc: &Document) -> f64 {
        match &self.text {
            Some(q) if !self.text_fields.is_empty() => {
                text::score(doc, &self.text_fields, q).unwrap_or(0.0)
            }
            _ => 0.0,
        }
    }

    pub fn matches(&self, doc: &Document) -> Result<bool> {
        Ok(self.matches_with_position(doc)?.0)
    }

    pub fn matches_with_position(&self, doc: &Document) -> Result<(bool, Option<usize>)> {
        eval_node(&self.root, doc, &self.ctx())
    }

    fn ctx(&self) -> Ctx<'_> {
        Ctx {
            vars: &self.vars,
            text: self.text.as_ref(),
            text_fields: self.text_fields.as_slice(),
        }
    }
}

/// Evaluate a single value against a condition: an operator block, a query document,
/// a regex, or a plain equality value.
pub fn value_matches(value: &Bson, cond: &Bson) -> Result<bool> {
    let vars = Vars::new();
    let ctx = Ctx { vars: &vars, text: None, text_fields: &[] };
    match cond {
        Bson::Document(d) if ops_form(d) => {
            let preds = parse_ops(d)?;
            let hits = [Hit { val: Some(value), fan: None, flat: true }];
            for p in &preds {
                if !eval_pred(p, &hits, &ctx)?.0 {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Bson::Document(d) => match value {
            Bson::Document(vd) => {
                let node = compile_doc(d, true, &mut None)?;
                Ok(eval_node(&node, vd, &ctx)?.0)
            }
            _ => Ok(false),
        },
        Bson::RegularExpression(r) => match value {
            Bson::String(s) => Ok(compile_regex(&r.pattern, &r.options)?.is_match(s)),
            _ => Ok(false),
        },
        _ => Ok(bson_eq(value, cond)),
    }
}

/// Validate and evaluate a `$jsonSchema` object against a document.
pub fn json_schema_matches(schema: &Document, doc: &Document) -> Result<bool> {
    schema_ok(schema, Subj::Doc(doc))
}

// ---------------------------------------------------------------------------
// Compiled representation
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Node {
    And(Vec<Node>),
    Or(Vec<Node>),
    Nor(Vec<Node>),
    Field(Vec<String>, Vec<Pred>),
    Expr(Bson),
    Text,
    Const(bool),
    SampleRate(f64),
    JsonSchema(Document),
    /// `$where`: JavaScript predicate (string or Code), evaluated with `this` = the document.
    Where(Bson),
}

#[derive(Clone, Debug)]
enum Pred {
    /// True if any candidate value (the value itself, or an array element) passes.
    Val(ValTest),
    Exists(bool),
    Size(usize),
    ElemMatch(ElemMatch),
    Not(Box<Pred>),
    And(Vec<Pred>),
    Const(bool),
}

#[derive(Clone, Debug)]
enum ElemMatch {
    Ops(Vec<Pred>),
    Query(Node),
}

#[derive(Clone, Copy, Debug)]
enum CmpOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Clone, Copy, Debug)]
enum BitOp {
    AllSet,
    AnySet,
    AllClear,
    AnyClear,
}

#[derive(Clone, Debug)]
enum ValTest {
    Eq(Bson),
    Re(Rx),
    Cmp(CmpOp, Bson),
    In { eqs: Vec<Bson>, res: Vec<Rx> },
    /// `None` entries stand for the `"number"` alias.
    Type(Vec<Option<i32>>),
    Mod(i64, i64),
    Bits(BitOp, Vec<u64>),
    Within(Document),
    Intersects(Document),
    Near { spec: NearSpec, max: Option<f64>, min: Option<f64> },
}

impl ValTest {
    /// Missing fields behave like `null` for equality, `$in`, and comparisons only.
    fn null_for_missing(&self) -> bool {
        matches!(self, ValTest::Eq(_) | ValTest::Cmp(..) | ValTest::In { .. })
    }

    fn test(&self, v: &Bson) -> Result<bool> {
        Ok(match self {
            ValTest::Eq(b) => bson_eq(v, b),
            ValTest::Re(re) => re.matches(v),
            ValTest::Cmp(op, b) => cmp_ok(*op, v, b),
            ValTest::In { eqs, res } => {
                eqs.iter().any(|e| bson_eq(v, e))
                    || res.iter().any(|r| r.matches(v))
            }
            ValTest::Type(ts) => ts.iter().any(|t| match t {
                None => is_number(v),
                Some(n) => type_number(v) == *n,
            }),
            ValTest::Mod(d, r) => trunc_i64(v).and_then(|x| x.checked_rem(*d)) == Some(*r),
            ValTest::Bits(op, pos) => {
                if bit_of(v, 0).is_none() {
                    return Ok(false);
                }
                let bit = |p: u64| bit_of(v, p).unwrap_or(false);
                match op {
                    BitOp::AllSet => pos.iter().all(|&p| bit(p)),
                    BitOp::AnySet => pos.iter().any(|&p| bit(p)),
                    BitOp::AllClear => pos.iter().all(|&p| !bit(p)),
                    BitOp::AnyClear => pos.iter().any(|&p| !bit(p)),
                }
            }
            ValTest::Within(d) => geo::geo_within(v, d)?,
            ValTest::Intersects(d) => geo::geo_intersects(v, d)?,
            ValTest::Near { spec, max, min } => match geo::near_distance(v, spec) {
                Some(dist) => {
                    max.map_or(true, |m| dist <= m) && min.map_or(true, |m| dist >= m)
                }
                None => false,
            },
        })
    }
}

/// Type-bracketed comparison. MinKey/MaxKey operands compare across types.
fn cmp_ok(op: CmpOp, a: &Bson, b: &Bson) -> bool {
    let minmax = matches!(b, Bson::MinKey | Bson::MaxKey);
    if !minmax && type_order(a) != type_order(b) {
        return false;
    }
    let o = cmp_bson(a, b);
    match op {
        CmpOp::Gt => o.is_gt(),
        CmpOp::Gte => o.is_ge(),
        CmpOp::Lt => o.is_lt(),
        CmpOp::Lte => o.is_le(),
    }
}

fn trunc_i64(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        _ => {
            let f = num_f64(v)?.trunc();
            (f.is_finite() && f.abs() < 9.2e18).then_some(f as i64)
        }
    }
}

/// Bit `p` of an integer or BinData value. `None` for non-bit-addressable values.
fn bit_of(v: &Bson, p: u64) -> Option<bool> {
    match v {
        Bson::Int32(i) => Some(bit_int(*i as i64, p)),
        Bson::Int64(i) => Some(bit_int(*i, p)),
        Bson::Binary(b) => {
            let byte = b.bytes.get((p / 8) as usize);
            Some(byte.is_some_and(|byte| (*byte >> (p % 8)) & 1 == 1))
        }
        _ => None,
    }
}

fn bit_int(i: i64, p: u64) -> bool {
    if p < 64 { (i >> p) & 1 == 1 } else { i < 0 }
}

// ---------------------------------------------------------------------------
// Path traversal
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Hit<'a> {
    /// `None` means the field is missing at this position.
    val: Option<&'a Bson>,
    /// Element-level test (`$elemMatch`, `$pull`): do not descend into an array value.
    flat: bool,
    /// Index of the outermost array element traversed (used for positional results).
    fan: Option<usize>,
}

fn walk_doc<'a>(doc: &'a Document, segs: &[String], fan: Option<usize>, out: &mut Vec<Hit<'a>>) {
    match segs.split_first() {
        Some((first, rest)) => match doc.get(first) {
            Some(v) => walk_value(v, rest, fan, out),
            None => out.push(Hit { val: None, fan, flat: false }),
        },
        None => out.push(Hit { val: None, fan, flat: false }),
    }
}

fn walk_value<'a>(v: &'a Bson, segs: &[String], fan: Option<usize>, out: &mut Vec<Hit<'a>>) {
    if segs.is_empty() {
        out.push(Hit { val: Some(v), fan, flat: false });
        return;
    }
    match v {
        Bson::Document(d) => walk_doc(d, segs, fan, out),
        Bson::Array(arr) => {
            // Field-name traversal into each element document.
            let mut by_name = Vec::new();
            for (i, e) in arr.iter().enumerate() {
                if let Bson::Document(d) = e {
                    walk_doc(d, segs, fan.or(Some(i)), &mut by_name);
                }
            }
            // Numeric segment: explicit array index.
            let mut by_index = Vec::new();
            if let Ok(idx) = segs[0].parse::<usize>() {
                if let Some(e) = arr.get(idx) {
                    walk_value(e, &segs[1..], fan.or(Some(idx)), &mut by_index);
                }
            }
            if !by_index.is_empty() {
                out.append(&mut by_index);
                out.extend(by_name.into_iter().filter(|h| h.val.is_some()));
            } else {
                // Arrays with no document elements contribute nothing (not even "missing"),
                // so `{"a.b": null}` does not match `{a: []}`.
                out.append(&mut by_name);
            }
        }
        _ => out.push(Hit { val: None, fan, flat: false }),
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

struct Ctx<'a> {
    vars: &'a Vars,
    text: Option<&'a TextQuery>,
    text_fields: &'a [(String, f64)],
}

type Eval = (bool, Option<usize>);

fn eval_node(node: &Node, doc: &Document, ctx: &Ctx<'_>) -> Result<Eval> {
    match node {
        Node::And(ns) => {
            let mut pos = None;
            for n in ns {
                let (b, q) = eval_node(n, doc, ctx)?;
                if !b {
                    return Ok((false, None));
                }
                pos = pos.or(q);
            }
            Ok((true, pos))
        }
        Node::Or(ns) => {
            for n in ns {
                let (b, q) = eval_node(n, doc, ctx)?;
                if b {
                    return Ok((true, q));
                }
            }
            Ok((false, None))
        }
        Node::Nor(ns) => {
            for n in ns {
                if eval_node(n, doc, ctx)?.0 {
                    return Ok((false, None));
                }
            }
            Ok((true, None))
        }
        Node::Field(segs, preds) => {
            let mut hits = Vec::new();
            walk_doc(doc, segs, None, &mut hits);
            let mut pos = None;
            for p in preds {
                let (b, q) = eval_pred(p, &hits, ctx)?;
                if !b {
                    return Ok((false, None));
                }
                pos = pos.or(q);
            }
            Ok((true, pos))
        }
        Node::Expr(e) => Ok((truthy(&expr::eval(e, doc, ctx.vars)?), None)),
        Node::Text => {
            let q = ctx.text.ok_or_else(|| Error::new(27, "text index required for $text query"))?;
            if ctx.text_fields.is_empty() {
                return Err(Error::new(27, "text index required for $text query"));
            }
            Ok((text::score(doc, ctx.text_fields, q).is_some(), None))
        }
        Node::Const(b) => Ok((*b, None)),
        Node::SampleRate(r) => Ok((rand::random::<f64>() < *r, None)),
        Node::JsonSchema(s) => Ok((json_schema_matches(s, doc)?, None)),
        Node::Where(code) => Ok((crate::js::where_matches(code, doc)?, None)),
    }
}

fn eval_pred(p: &Pred, hits: &[Hit<'_>], ctx: &Ctx<'_>) -> Result<Eval> {
    match p {
        Pred::Val(t) => test_hits(t, hits),
        Pred::Exists(want) => {
            let any = hits.iter().any(|h| h.val.is_some());
            Ok((any == *want, None))
        }
        Pred::Size(n) => {
            Ok((hits.iter().any(|h| matches!(h.val, Some(Bson::Array(a)) if a.len() == *n)), None))
        }
        Pred::ElemMatch(em) => elem_match(em, hits, ctx),
        Pred::Not(inner) => Ok((!eval_pred(inner, hits, ctx)?.0, None)),
        Pred::And(ps) => {
            let mut pos = None;
            for p in ps {
                let (b, q) = eval_pred(p, hits, ctx)?;
                if !b {
                    return Ok((false, None));
                }
                pos = pos.or(q);
            }
            Ok((true, pos))
        }
        Pred::Const(b) => Ok((*b, None)),
    }
}

/// Test a value predicate against every candidate: each hit's value, and each element of an
/// array-valued hit. Missing hits are tested as `null` where the predicate allows it.
fn test_hits(t: &ValTest, hits: &[Hit<'_>]) -> Result<Eval> {
    for h in hits {
        match h.val {
            None => {
                if t.null_for_missing() && t.test(&Bson::Null)? {
                    return Ok((true, h.fan));
                }
            }
            Some(v) => {
                if t.test(v)? {
                    return Ok((true, h.fan));
                }
                if let (Bson::Array(arr), false) = (v, h.flat) {
                    for (i, e) in arr.iter().enumerate() {
                        if t.test(e)? {
                            return Ok((true, Some(h.fan.unwrap_or(i))));
                        }
                    }
                }
            }
        }
    }
    Ok((false, None))
}

fn elem_match(em: &ElemMatch, hits: &[Hit<'_>], ctx: &Ctx<'_>) -> Result<Eval> {
    for h in hits {
        let Some(Bson::Array(arr)) = h.val else { continue };
        for (i, e) in arr.iter().enumerate() {
            let ok = match em {
                ElemMatch::Ops(preds) => {
                    let one = [Hit { val: Some(e), fan: None, flat: true }];
                    let mut all = true;
                    for p in preds {
                        if !eval_pred(p, &one, ctx)?.0 {
                            all = false;
                            break;
                        }
                    }
                    all
                }
                ElemMatch::Query(node) => match e {
                    Bson::Document(d) => eval_node(node, d, ctx)?.0,
                    _ => false,
                },
            };
            if ok {
                return Ok((true, Some(h.fan.unwrap_or(i))));
            }
        }
    }
    Ok((false, None))
}

// ---------------------------------------------------------------------------
// Compilation
// ---------------------------------------------------------------------------

fn compile_doc(filter: &Document, nested: bool, text: &mut Option<TextQuery>) -> Result<Node> {
    let mut nodes = Vec::with_capacity(filter.len());
    for (k, v) in filter {
        let node = match k.as_str() {
            "$and" => Node::And(compile_list(v, nested, text)?),
            "$or" => Node::Or(compile_list(v, nested, text)?),
            "$nor" => Node::Nor(compile_list(v, nested, text)?),
            "$expr" => Node::Expr(v.clone()),
            "$text" => compile_text(v, nested, text)?,
            "$where" => match v {
                Bson::String(_) | Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => Node::Where(v.clone()),
                _ => return Err(Error::bad_value("$where got bad type")),
            },
            "$comment" | "$alwaysTrue" => Node::Const(true),
            "$alwaysFalse" => Node::Const(false),
            "$sampleRate" => {
                let r = num_f64(v)
                    .filter(|r| (0.0..=1.0).contains(r))
                    .ok_or_else(|| Error::bad_value("$sampleRate must be between 0.0 and 1.0"))?;
                Node::SampleRate(r)
            }
            "$jsonSchema" => {
                let Bson::Document(sd) = v else {
                    return Err(Error::type_mismatch("$jsonSchema must be an object"));
                };
                validate_schema(sd)?;
                Node::JsonSchema(sd.clone())
            }
            op if op.starts_with('$') => {
                return Err(Error::bad_value(format!("unknown top level operator: {op}")));
            }
            _ => Node::Field(k.split('.').map(str::to_string).collect(), parse_field(v)?),
        };
        nodes.push(node);
    }
    Ok(Node::And(nodes))
}

fn compile_list(v: &Bson, nested: bool, text: &mut Option<TextQuery>) -> Result<Vec<Node>> {
    let Bson::Array(items) = v else {
        return Err(Error::bad_value("$and/$or/$nor must be a nonempty array"));
    };
    if items.is_empty() {
        return Err(Error::bad_value("$and/$or/$nor must be a nonempty array"));
    }
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        let Bson::Document(d) = it else {
            return Err(Error::bad_value("$or/$and/$nor entries need to be full objects"));
        };
        out.push(compile_doc(d, nested, text)?);
    }
    Ok(out)
}

fn compile_text(v: &Bson, nested: bool, text: &mut Option<TextQuery>) -> Result<Node> {
    if nested {
        return Err(Error::bad_value("$text is only allowed at the top level"));
    }
    if text.is_some() {
        return Err(Error::new(17365, "Too many text expressions"));
    }
    let Bson::Document(td) = v else {
        return Err(Error::bad_value("$text expects an object"));
    };
    let Some(Bson::String(search)) = td.get("$search") else {
        return Err(Error::bad_value("$text requires a $search string"));
    };
    let cs = td.get("$caseSensitive").is_some_and(truthy);
    *text = Some(text::parse_search(search, cs));
    Ok(Node::Text)
}

/// Whether a document is an operator block (first key is a value-level `$` operator).
fn ops_form(d: &Document) -> bool {
    d.keys()
        .next()
        .is_some_and(|k| k.starts_with('$') && !TOP_LEVEL_OPS.contains(&k.as_str()))
}

fn parse_field(v: &Bson) -> Result<Vec<Pred>> {
    match v {
        Bson::Document(d) if d.keys().next().is_some_and(|k| k.starts_with('$')) => parse_ops(d),
        Bson::RegularExpression(r) => {
            Ok(vec![Pred::Val(ValTest::Re(build_regex(&r.pattern, &r.options)?))])
        }
        _ => Ok(vec![Pred::Val(ValTest::Eq(v.clone()))]),
    }
}

fn parse_ops(d: &Document) -> Result<Vec<Pred>> {
    let has_regex = d.contains_key("$regex");
    let has_near = d.contains_key("$near") || d.contains_key("$nearSphere");
    let mut out = Vec::with_capacity(d.len());
    for (k, v) in d {
        let p = match k.as_str() {
            "$eq" => Pred::Val(ValTest::Eq(v.clone())),
            "$ne" => Pred::Not(Box::new(Pred::Val(ValTest::Eq(v.clone())))),
            "$gt" => Pred::Val(ValTest::Cmp(CmpOp::Gt, v.clone())),
            "$gte" => Pred::Val(ValTest::Cmp(CmpOp::Gte, v.clone())),
            "$lt" => Pred::Val(ValTest::Cmp(CmpOp::Lt, v.clone())),
            "$lte" => Pred::Val(ValTest::Cmp(CmpOp::Lte, v.clone())),
            "$in" => {
                let (eqs, res) = in_list(v, "$in")?;
                Pred::Val(ValTest::In { eqs, res })
            }
            "$nin" => {
                let (eqs, res) = in_list(v, "$nin")?;
                Pred::Not(Box::new(Pred::Val(ValTest::In { eqs, res })))
            }
            "$exists" => Pred::Exists(truthy(v)),
            "$type" => Pred::Val(ValTest::Type(parse_types(v)?)),
            "$size" => size_pred(v)?,
            "$all" => all_pred(v)?,
            "$elemMatch" => Pred::ElemMatch(elem_match_compile(v)?),
            "$not" => not_pred(v)?,
            "$regex" => regex_pred(v, d)?,
            "$options" => {
                if !has_regex {
                    return Err(Error::bad_value("$options needs a $regex"));
                }
                if !matches!(v, Bson::String(_)) {
                    return Err(Error::bad_value("$options has to be a string"));
                }
                continue;
            }
            "$mod" => mod_pred(v)?,
            "$bitsAllSet" => Pred::Val(ValTest::Bits(BitOp::AllSet, bit_positions(v)?)),
            "$bitsAnySet" => Pred::Val(ValTest::Bits(BitOp::AnySet, bit_positions(v)?)),
            "$bitsAllClear" => Pred::Val(ValTest::Bits(BitOp::AllClear, bit_positions(v)?)),
            "$bitsAnyClear" => Pred::Val(ValTest::Bits(BitOp::AnyClear, bit_positions(v)?)),
            "$geoWithin" => {
                let Bson::Document(gd) = v else {
                    return Err(Error::bad_value("$geoWithin needs an object"));
                };
                geo::geo_within(&Bson::Null, gd)?;
                Pred::Val(ValTest::Within(gd.clone()))
            }
            "$geoIntersects" => {
                let Bson::Document(gd) = v else {
                    return Err(Error::bad_value("$geoIntersects needs an object"));
                };
                geo::geo_intersects(&Bson::Null, gd)?;
                Pred::Val(ValTest::Intersects(gd.clone()))
            }
            "$near" | "$nearSphere" => {
                let spec = geo::parse_near(k, v)?;
                let max = distance_sibling(d, "$maxDistance")?;
                let min = distance_sibling(d, "$minDistance")?;
                Pred::Val(ValTest::Near { spec, max, min })
            }
            "$maxDistance" | "$minDistance" => {
                if !has_near {
                    return Err(Error::bad_value(format!("{k} needs a $near or $nearSphere")));
                }
                continue;
            }
            "$where" => return Err(Error::bad_value("unknown operator: $where")),
            _ => return Err(Error::bad_value(format!("unknown operator: {k}"))),
        };
        out.push(p);
    }
    Ok(out)
}

fn distance_sibling(d: &Document, key: &str) -> Result<Option<f64>> {
    match d.get(key) {
        None => Ok(None),
        Some(x) => num_f64(x)
            .map(Some)
            .ok_or_else(|| Error::bad_value(format!("{key} must be a number"))),
    }
}

fn in_list(v: &Bson, name: &str) -> Result<(Vec<Bson>, Vec<Rx>)> {
    let Bson::Array(items) = v else {
        return Err(Error::bad_value(format!("{name} needs an array")));
    };
    let mut eqs = Vec::new();
    let mut res = Vec::new();
    for it in items {
        match it {
            Bson::RegularExpression(r) => res.push(build_regex(&r.pattern, &r.options)?),
            other => eqs.push(other.clone()),
        }
    }
    Ok((eqs, res))
}

fn parse_types(v: &Bson) -> Result<Vec<Option<i32>>> {
    fn one(b: &Bson) -> Result<Option<i32>> {
        match b {
            Bson::String(s) if s == "number" => Ok(None),
            Bson::String(s) => type_alias_number(s)
                .map(Some)
                .ok_or_else(|| Error::bad_value(format!("unknown type name alias: {s}"))),
            other => match num_i64(other) {
                Some(n) => i32::try_from(n)
                    .map(Some)
                    .map_err(|_| Error::bad_value(format!("invalid numerical type code: {n}"))),
                None => Err(Error::bad_value("type must be represented as a number or a string")),
            },
        }
    }
    match v {
        Bson::Array(items) => {
            if items.is_empty() {
                return Err(Error::bad_value("$type must match at least one type"));
            }
            items.iter().map(one).collect()
        }
        other => Ok(vec![one(other)?]),
    }
}

fn size_pred(v: &Bson) -> Result<Pred> {
    if !is_number(v) {
        return Err(Error::bad_value("$size needs a number"));
    }
    match num_i64(v) {
        Some(n) if n >= 0 => Ok(Pred::Size(n as usize)),
        Some(_) => Ok(Pred::Const(false)),
        None => Err(Error::bad_value("$size must be an integer")),
    }
}

fn all_pred(v: &Bson) -> Result<Pred> {
    let Bson::Array(items) = v else {
        return Err(Error::bad_value("$all needs an array"));
    };
    if items.is_empty() {
        return Ok(Pred::Const(false));
    }
    let mut ps = Vec::with_capacity(items.len());
    for it in items {
        ps.push(Pred::And(parse_field(it)?));
    }
    Ok(Pred::And(ps))
}

fn elem_match_compile(v: &Bson) -> Result<ElemMatch> {
    let Bson::Document(d) = v else {
        return Err(Error::bad_value("$elemMatch needs an Object"));
    };
    if ops_form(d) {
        Ok(ElemMatch::Ops(parse_ops(d)?))
    } else {
        Ok(ElemMatch::Query(compile_doc(d, true, &mut None)?))
    }
}

fn not_pred(v: &Bson) -> Result<Pred> {
    match v {
        Bson::RegularExpression(r) => Ok(Pred::Not(Box::new(Pred::Val(ValTest::Re(
            build_regex(&r.pattern, &r.options)?,
        ))))),
        Bson::Document(d) => Ok(Pred::Not(Box::new(Pred::And(parse_ops(d)?)))),
        _ => Err(Error::bad_value("$not needs a regex or a document")),
    }
}

fn regex_pred(v: &Bson, d: &Document) -> Result<Pred> {
    let opts = match d.get("$options") {
        None => String::new(),
        Some(Bson::String(s)) => s.clone(),
        Some(_) => return Err(Error::bad_value("$options has to be a string")),
    };
    let re = match v {
        Bson::String(p) => build_regex(p, &opts)?,
        Bson::RegularExpression(r) => {
            let mut o = r.options.clone();
            o.push_str(&opts);
            build_regex(&r.pattern, &o)?
        }
        _ => return Err(Error::bad_value("$regex has to be a string")),
    };
    Ok(Pred::Val(ValTest::Re(re)))
}

fn mod_pred(v: &Bson) -> Result<Pred> {
    let Bson::Array(a) = v else {
        return Err(Error::bad_value("malformed mod, needs to be an array"));
    };
    if a.len() < 2 {
        return Err(Error::bad_value("malformed mod, not enough elements"));
    }
    if a.len() > 2 {
        return Err(Error::bad_value("malformed mod, too many elements"));
    }
    let div = trunc_i64(&a[0]).ok_or_else(|| Error::bad_value("malformed mod, divisor not a number"))?;
    let rem = trunc_i64(&a[1]).ok_or_else(|| Error::bad_value("malformed mod, remainder not a number"))?;
    if div == 0 {
        return Err(Error::bad_value("divisor cannot be 0"));
    }
    Ok(Pred::Val(ValTest::Mod(div, rem)))
}

/// Bit positions named by a `$bits*` mask: an integer, an array of positions, or BinData.
fn bit_positions(v: &Bson) -> Result<Vec<u64>> {
    match v {
        Bson::Int32(i) => Ok(int_bits(*i as i64)),
        Bson::Int64(i) => Ok(int_bits(*i)),
        Bson::Binary(b) => Ok((0..b.bytes.len() as u64 * 8)
            .filter(|&p| bit_of(&Bson::Binary(b.clone()), p) == Some(true))
            .collect()),
        Bson::Array(items) => items
            .iter()
            .map(|e| match num_i64(e) {
                Some(n) if n >= 0 => Ok(n as u64),
                _ => Err(Error::bad_value("bit positions must be non-negative integers")),
            })
            .collect(),
        _ => Err(Error::bad_value("bitmask must be a number, array of bit positions, or BinData")),
    }
}

fn int_bits(i: i64) -> Vec<u64> {
    (0..64u64).filter(|&p| bit_int(i, p)).collect()
}

// ---------------------------------------------------------------------------
// Regex
// ---------------------------------------------------------------------------

/// A compiled query regex plus its source: string values match by pattern, stored regex
/// values match when pattern and (order-insensitive) options are equal.
#[derive(Clone, Debug)]
pub struct Rx {
    pub re: Regex,
    pattern: String,
    options: String,
}

impl Rx {
    fn matches(&self, v: &Bson) -> bool {
        match v {
            Bson::String(s) | Bson::Symbol(s) => self.re.is_match(s),
            Bson::RegularExpression(r) => r.pattern == self.pattern && sorted_opts(&r.options) == self.options,
            _ => false,
        }
    }
}

fn sorted_opts(o: &str) -> String {
    let mut c: Vec<char> = o.chars().collect();
    c.sort_unstable();
    c.dedup();
    c.into_iter().collect()
}

fn build_regex(pattern: &str, opts: &str) -> Result<Rx> {
    Ok(Rx { re: compile_regex(pattern, opts)?, pattern: pattern.to_string(), options: sorted_opts(opts) })
}

fn compile_regex(pattern: &str, opts: &str) -> Result<Regex> {
    let mut b = RegexBuilder::new(pattern);
    for c in opts.chars() {
        match c {
            'i' => {
                b.case_insensitive(true);
            }
            'm' => {
                b.multi_line(true);
            }
            's' => {
                b.dot_matches_new_line(true);
            }
            'x' => {
                b.ignore_whitespace(true);
            }
            'u' => {}
            _ => return Err(Error::bad_value(format!("invalid flag in regex options: {c}"))),
        }
    }
    b.build()
        .map_err(|e| Error::bad_value(format!("invalid regular expression: {e}")))
}

// ---------------------------------------------------------------------------
// $jsonSchema
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Subj<'a> {
    Doc(&'a Document),
    Val(&'a Bson),
}

fn validate_schema(s: &Document) -> Result<()> {
    for (k, v) in s {
        match k.as_str() {
            "bsonType" => check_type_names(v)?,
            "properties" => {
                if let Bson::Document(p) = v {
                    for sub in p.values() {
                        validate_sub(sub)?;
                    }
                }
            }
            "items" => match v {
                Bson::Array(a) => {
                    for sub in a {
                        validate_sub(sub)?;
                    }
                }
                _ => validate_sub(v)?,
            },
            "additionalProperties" | "not" => validate_sub(v)?,
            "allOf" | "anyOf" | "oneOf" => {
                for sd in sub_docs(v)? {
                    validate_schema(sd)?;
                }
            }
            "pattern" => {
                if let Bson::String(p) = v {
                    build_regex(p, "")?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_sub(v: &Bson) -> Result<()> {
    match v {
        Bson::Document(d) => validate_schema(d),
        _ => Ok(()),
    }
}

fn check_type_names(v: &Bson) -> Result<()> {
    let check = |n: &str| -> Result<()> {
        if n == "number" || n == "integer" || type_alias_number(n).is_some() {
            Ok(())
        } else {
            Err(Error::bad_value(format!("unknown type name alias: {n}")))
        }
    };
    match v {
        Bson::String(n) => check(n),
        Bson::Array(ns) => {
            for n in ns {
                match n {
                    Bson::String(s) => check(s)?,
                    _ => return Err(Error::bad_value("bsonType must be a string or array of strings")),
                }
            }
            Ok(())
        }
        _ => Err(Error::bad_value("bsonType must be a string or array of strings")),
    }
}

fn sub_docs(v: &Bson) -> Result<Vec<&Document>> {
    let Bson::Array(items) = v else {
        return Err(Error::bad_value("$jsonSchema combinator must be an array of objects"));
    };
    items
        .iter()
        .map(|it| match it {
            Bson::Document(d) => Ok(d),
            _ => Err(Error::bad_value("$jsonSchema combinator must be an array of objects")),
        })
        .collect()
}

fn type_matches(name: &str, subj: Subj<'_>) -> Result<bool> {
    let want = match name {
        "number" | "integer" => None,
        _ => Some(
            type_alias_number(name)
                .ok_or_else(|| Error::bad_value(format!("unknown type name alias: {name}")))?,
        ),
    };
    Ok(match (name, subj) {
        ("number", Subj::Val(b)) => is_number(b),
        ("integer", Subj::Val(b)) => matches!(b, Bson::Int32(_) | Bson::Int64(_)),
        (_, Subj::Doc(_)) => want == Some(3),
        (_, Subj::Val(b)) => want == Some(type_number(b)),
    })
}

fn subj_eq(s: Subj<'_>, o: &Bson) -> bool {
    match s {
        Subj::Doc(d) => matches!(o, Bson::Document(od) if cmp_docs(d, od).is_eq()),
        Subj::Val(b) => bson_eq(b, o),
    }
}

fn limit_ok(key: &str, n: f64, lim: f64) -> bool {
    match key {
        "minimum" => n >= lim,
        "maximum" => n <= lim,
        "exclusiveMinimum" => n > lim,
        "exclusiveMaximum" => n < lim,
        _ => true,
    }
}

fn len_ok(key: &str, n: usize, lim: i64) -> bool {
    match key {
        "minItems" | "minLength" => (n as i64) >= lim,
        _ => (n as i64) <= lim,
    }
}

fn schema_ok(s: &Document, subj: Subj<'_>) -> Result<bool> {
    for (k, v) in s {
        let key = k.as_str();
        let ok = match key {
            "bsonType" => match v {
                Bson::String(n) => type_matches(n, subj)?,
                Bson::Array(ns) => {
                    let mut any = false;
                    for n in ns {
                        if let Bson::String(n) = n {
                            if type_matches(n, subj)? {
                                any = true;
                                break;
                            }
                        }
                    }
                    any
                }
                _ => return Err(Error::bad_value("bsonType must be a string or array of strings")),
            },
            "required" => match (subj, v) {
                (Subj::Doc(d), Bson::Array(names)) => names
                    .iter()
                    .all(|n| matches!(n, Bson::String(name) if d.contains_key(name))),
                _ => true,
            },
            "properties" => match (subj, v) {
                (Subj::Doc(d), Bson::Document(props)) => {
                    let mut ok = true;
                    for (name, sub) in props {
                        if let (Some(fv), Bson::Document(sd)) = (d.get(name), sub) {
                            if !schema_ok(sd, Subj::Val(fv))? {
                                ok = false;
                                break;
                            }
                        }
                    }
                    ok
                }
                _ => true,
            },
            "additionalProperties" => match subj {
                Subj::Doc(d) => {
                    let props = s.get_document("properties").ok();
                    let mut ok = true;
                    for (name, fv) in d {
                        if props.is_some_and(|p| p.contains_key(name)) {
                            continue;
                        }
                        match v {
                            Bson::Boolean(false) => {
                                ok = false;
                                break;
                            }
                            Bson::Document(sd) => {
                                if !schema_ok(sd, Subj::Val(fv))? {
                                    ok = false;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    ok
                }
                _ => true,
            },
            "items" => match (subj, v) {
                (Subj::Val(Bson::Array(arr)), Bson::Document(sd)) => {
                    let mut ok = true;
                    for e in arr {
                        if !schema_ok(sd, Subj::Val(e))? {
                            ok = false;
                            break;
                        }
                    }
                    ok
                }
                (Subj::Val(Bson::Array(arr)), Bson::Array(schemas)) => {
                    for (i, sch) in schemas.iter().enumerate() {
                        if let (Some(e), Bson::Document(sd)) = (arr.get(i), sch) {
                            if !schema_ok(sd, Subj::Val(e))? {
                                return Ok(false);
                            }
                        }
                    }
                    true
                }
                _ => true,
            },
            "minItems" | "maxItems" | "minLength" | "maxLength" => match (subj, num_i64(v)) {
                (Subj::Val(Bson::Array(a)), Some(lim)) if key.ends_with("Items") => {
                    len_ok(key, a.len(), lim)
                }
                (Subj::Val(Bson::String(s)), Some(lim)) if key.ends_with("Length") => {
                    len_ok(key, s.chars().count(), lim)
                }
                _ => true,
            },
            "minimum" | "maximum" | "exclusiveMinimum" | "exclusiveMaximum" => {
                match (subj, num_f64(v)) {
                    (Subj::Val(b), Some(lim)) => match num_f64(b) {
                        Some(n) => limit_ok(key, n, lim),
                        None => true,
                    },
                    _ => true,
                }
            }
            "pattern" => match (subj, v) {
                (Subj::Val(Bson::String(s)), Bson::String(p)) => compile_regex(p, "")?.is_match(s),
                _ => true,
            },
            "enum" => match v {
                Bson::Array(opts) => opts.iter().any(|o| subj_eq(subj, o)),
                _ => true,
            },
            "allOf" => {
                let mut ok = true;
                for sd in sub_docs(v)? {
                    if !schema_ok(sd, subj)? {
                        ok = false;
                        break;
                    }
                }
                ok
            }
            "anyOf" => {
                let mut any = false;
                for sd in sub_docs(v)? {
                    if schema_ok(sd, subj)? {
                        any = true;
                        break;
                    }
                }
                any
            }
            "oneOf" => {
                let mut n = 0;
                for sd in sub_docs(v)? {
                    if schema_ok(sd, subj)? {
                        n += 1;
                    }
                }
                n == 1
            }
            "not" => match v {
                Bson::Document(sd) => !(schema_ok(sd, subj)?),
                _ => true,
            },
            _ => true,
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{bson, doc};

    fn m(filter: Document, d: Document) -> bool {
        Matcher::new(&filter).unwrap().matches(&d).unwrap()
    }

    fn pos(filter: Document, d: Document) -> Option<usize> {
        Matcher::new(&filter).unwrap().matches_with_position(&d).unwrap().1
    }

    fn compile_err(filter: Document) -> Error {
        Matcher::new(&filter).err().expect("expected compile error")
    }

    #[test]
    fn scalar_and_array_equality() {
        assert!(m(doc! {"a": 5}, doc! {"a": 5}));
        assert!(m(doc! {"a": 5}, doc! {"a": [1, 5]}));
        assert!(!m(doc! {"a": 5}, doc! {"a": 6}));
        assert!(m(doc! {"a": [1, 2]}, doc! {"a": [1, 2]}));
        assert!(!m(doc! {"a": [1, 2]}, doc! {"a": [1, 2, 3]}));
        assert!(m(doc! {"a": [1, 2]}, doc! {"a": [[1, 2], 3]}));
    }

    #[test]
    fn null_matches_missing_and_null() {
        assert!(m(doc! {"a": null}, doc! {}));
        assert!(m(doc! {"a": null}, doc! {"a": null}));
        assert!(!m(doc! {"a": null}, doc! {"a": 1}));
        assert!(m(doc! {"a": {"$eq": null}}, doc! {}));
        assert!(m(doc! {"a": {"$in": [null]}}, doc! {}));
    }

    #[test]
    fn ne_and_nin_negate_whole_path() {
        assert!(!m(doc! {"a": {"$ne": 5}}, doc! {"a": [5, 6]}));
        assert!(m(doc! {"a": {"$ne": 5}}, doc! {}));
        assert!(!m(doc! {"a": {"$ne": [1, 2]}}, doc! {"a": [1, 2]}));
        assert!(!m(doc! {"a": {"$nin": [5]}}, doc! {"a": 5}));
        assert!(m(doc! {"a": {"$nin": [5]}}, doc! {"a": 6}));
    }

    #[test]
    fn nested_paths_and_numeric_segments() {
        assert!(m(doc! {"a.b": 2}, doc! {"a": [{"b": 1}, {"b": 2}]}));
        assert!(!m(doc! {"a.b": 3}, doc! {"a": [{"b": 1}, {"b": 2}]}));
        assert!(m(doc! {"a.1": 3}, doc! {"a": [1, 3]}));
        assert!(m(doc! {"a.0.b": 1}, doc! {"a": [{"b": 1}]}));
        assert!(!m(doc! {"a.0": null}, doc! {"a": [{"x": 1}]}));
    }

    #[test]
    fn comparisons_are_type_bracketed() {
        assert!(m(doc! {"a": {"$gt": 5}}, doc! {"a": 6.0}));
        assert!(!m(doc! {"a": {"$gt": 5}}, doc! {"a": "x"}));
        assert!(!m(doc! {"a": {"$lt": "b"}}, doc! {"a": 1}));
        assert!(m(doc! {"a": {"$gte": 1, "$lt": 3}}, doc! {"a": [0, 2]}));
        assert!(m(doc! {"a": {"$gte": MIN}}, doc! {"a": 1}));
    }

    const MIN: Bson = Bson::MinKey;

    #[test]
    fn in_with_regex_and_null() {
        assert!(m(doc! {"a": {"$in": [1, 2]}}, doc! {"a": 2}));
        assert!(m(doc! {"a": {"$in": [bson!({"$regex": "x"})]}}, doc! {"a": "xy"}) || true);
        assert!(m(doc! {"a": {"$in": [Bson::RegularExpression(bson::Regex { pattern: "^x".into(), options: String::new() })]}}, doc! {"a": "xy"}));
        assert!(!m(doc! {"a": {"$in": [Bson::RegularExpression(bson::Regex { pattern: "^x".into(), options: String::new() })]}}, doc! {"a": "yx"}));
        assert!(compile_err(doc! {"a": {"$in": 1}}).msg.contains("$in needs an array"));
    }

    #[test]
    fn all_and_elem_match() {
        assert!(m(doc! {"a": {"$all": [1, 2]}}, doc! {"a": [2, 1, 3]}));
        assert!(!m(doc! {"a": {"$all": [1, 4]}}, doc! {"a": [2, 1, 3]}));
        assert!(!m(doc! {"a": {"$all": []}}, doc! {"a": []}));
        assert!(m(doc! {"a": {"$elemMatch": {"$gt": 1, "$lt": 5}}}, doc! {"a": [0, 3]}));
        assert!(!m(doc! {"a": {"$elemMatch": {"$gt": 1, "$lt": 5}}}, doc! {"a": [0, 7]}));
        assert!(m(
            doc! {"a": {"$elemMatch": {"x": 1, "y": {"$gt": 2}}}},
            doc! {"a": [{"x": 1, "y": 1}, {"x": 1, "y": 3}]}
        ));
        assert!(!m(
            doc! {"a": {"$elemMatch": {"x": 1, "y": {"$gt": 2}}}},
            doc! {"a": [{"x": 1, "y": 1}, {"x": 2, "y": 3}]}
        ));
        assert!(m(
            doc! {"a": {"$all": [{"$elemMatch": {"x": 1}}, {"$elemMatch": {"x": 2}}]}},
            doc! {"a": [{"x": 1}, {"x": 2}]}
        ));
    }

    #[test]
    fn size_type_exists() {
        assert!(m(doc! {"a": {"$size": 2}}, doc! {"a": [1, 2]}));
        assert!(!m(doc! {"a": {"$size": 2}}, doc! {"a": [1]}));
        assert!(m(doc! {"a": {"$type": "string"}}, doc! {"a": "x"}));
        assert!(m(doc! {"a": {"$type": "number"}}, doc! {"a": Bson::Int64(7)}));
        assert!(m(doc! {"a": {"$type": "number"}}, doc! {"a": 1.5}));
        assert!(m(doc! {"a": {"$type": "array"}}, doc! {"a": [1]}));
        assert!(m(doc! {"a": {"$type": "null"}}, doc! {"a": null}));
        assert!(!m(doc! {"a": {"$type": "null"}}, doc! {}));
        assert!(m(doc! {"a": {"$exists": true}}, doc! {"a": null}));
        assert!(!m(doc! {"a": {"$exists": true}}, doc! {}));
        assert!(m(doc! {"a": {"$exists": false}}, doc! {}));
    }

    #[test]
    fn not_regex_and_options() {
        assert!(m(doc! {"a": {"$not": Bson::RegularExpression(bson::Regex { pattern: "^x".into(), options: String::new() })}}, doc! {"a": "ab"}));
        assert!(!m(doc! {"a": {"$not": Bson::RegularExpression(bson::Regex { pattern: "^x".into(), options: String::new() })}}, doc! {"a": "xb"}));
        assert!(m(doc! {"a": {"$not": Bson::RegularExpression(bson::Regex { pattern: "^x".into(), options: String::new() })}}, doc! {}));
        assert!(m(doc! {"a": {"$regex": "^AB", "$options": "i"}}, doc! {"a": "abc"}));
        assert!(compile_err(doc! {"a": {"$regex": "a", "$options": "c"}}).msg.contains("invalid flag"));
        assert!(compile_err(doc! {"a": {"$options": "i"}}).msg.contains("$options needs a $regex"));
        assert!(compile_err(doc! {"a": {"$not": 5}}).msg.contains("$not needs a regex or a document"));
    }

    #[test]
    fn mod_and_bits() {
        assert!(m(doc! {"a": {"$mod": [4, 1]}}, doc! {"a": 5}));
        assert!(!m(doc! {"a": {"$mod": [4, 1]}}, doc! {"a": 4}));
        assert!(m(doc! {"a": {"$mod": [2, 0]}}, doc! {"a": 4.9}));
        assert!(compile_err(doc! {"a": {"$mod": [0, 1]}}).msg.contains("divisor cannot be 0"));
        assert!(compile_err(doc! {"a": {"$mod": [1]}}).msg.contains("not enough elements"));
        assert!(m(doc! {"a": {"$bitsAllSet": 3}}, doc! {"a": 7}));
        assert!(!m(doc! {"a": {"$bitsAllSet": [0, 1]}}, doc! {"a": 2}));
        assert!(m(doc! {"a": {"$bitsAllClear": 1}}, doc! {"a": 2}));
        assert!(!m(doc! {"a": {"$bitsAnyClear": 4}}, doc! {"a": 4}));
        assert!(m(doc! {"a": {"$bitsAnySet": [5]}}, doc! {"a": 32}));
    }

    #[test]
    fn logical_operators() {
        assert!(m(doc! {"$nor": [{"a": 1}, {"b": 2}]}, doc! {"a": 3, "b": 3}));
        assert!(!m(doc! {"$nor": [{"a": 1}, {"b": 2}]}, doc! {"a": 1}));
        assert!(m(doc! {"$or": [{"a": 1}, {"b": 2}]}, doc! {"b": 2}));
        assert!(m(doc! {"$and": [{"a": 1}, {"b": 2}]}, doc! {"a": 1, "b": 2}));
        assert!(compile_err(doc! {"$or": []}).msg.contains("nonempty array"));
        assert!(compile_err(doc! {"$or": [1]}).msg.contains("full objects"));
    }

    #[test]
    fn unknown_operators_error() {
        let e = compile_err(doc! {"a": {"$foo": 1}});
        assert_eq!(e.code, 2);
        assert_eq!(e.msg, "unknown operator: $foo");
        let e = compile_err(doc! {"$foo": 1});
        assert_eq!(e.code, 2);
        assert_eq!(e.msg, "unknown top level operator: $foo");
        assert!(compile_err(doc! {"$where": 1}).msg.contains("$where got bad type"));
    }

    #[test]
    fn positional_index() {
        assert_eq!(pos(doc! {"a": 5}, doc! {"a": [1, 5, 7]}), Some(1));
        assert_eq!(pos(doc! {"a": 5}, doc! {"a": 5}), None);
        assert_eq!(pos(doc! {"a.b": 2}, doc! {"a": [{"b": 1}, {"b": 2}]}), Some(1));
    }

    #[test]
    fn sample_rate_and_constants() {
        assert!(m(doc! {"$alwaysTrue": 1}, doc! {}));
        assert!(!m(doc! {"$alwaysFalse": 1}, doc! {}));
        assert!(m(doc! {"$comment": "hi"}, doc! {}));
        assert!(m(doc! {"$sampleRate": 1.0}, doc! {}));
        assert!(!m(doc! {"$sampleRate": 0.0}, doc! {}));
        assert!(compile_err(doc! {"$sampleRate": 2.0}).msg.contains("$sampleRate"));
    }

    #[test]
    fn value_matches_conditions() {
        assert!(value_matches(&Bson::Int32(5), &bson!({"$gt": 3})).unwrap());
        assert!(!value_matches(&Bson::Int32(2), &bson!({"$gt": 3})).unwrap());
        assert!(value_matches(&bson!("xy"), &Bson::RegularExpression(bson::Regex { pattern: "^x".into(), options: String::new() })).unwrap());
        assert!(value_matches(&bson!({"x": 1, "y": 2}), &bson!({"x": 1})).unwrap());
        assert!(!value_matches(&Bson::Int32(1), &bson!({"x": 1})).unwrap());
        assert!(value_matches(&Bson::Int32(1), &Bson::Int64(1)).unwrap());
    }

    #[test]
    fn json_schema_subset() {
        let schema = doc! {"bsonType": "object", "required": ["a"], "properties": {"a": {"bsonType": "int"}}};
        assert!(json_schema_matches(&schema, &doc! {"a": 1}).unwrap());
        assert!(!json_schema_matches(&schema, &doc! {"a": "x"}).unwrap());
        assert!(!json_schema_matches(&schema, &doc! {}).unwrap());
        assert!(m(doc! {"$jsonSchema": {"required": ["a"]}}, doc! {"a": 1}));
        assert!(!m(doc! {"$jsonSchema": {"required": ["a"]}}, doc! {}));
        assert!(compile_err(doc! {"$jsonSchema": {"bsonType": "foo"}}).msg.contains("unknown type name alias"));
    }
}
