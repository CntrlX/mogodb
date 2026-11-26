//! Aggregation expressions added in MongoDB 8.x / 9.0 (non-conversion).

use crate::agg::expr::{eval, Vars};
use crate::bsonutil::{binary, cmp_bson, decimal_to_f64, get_path_value, is_nullish, num_f64, type_name};
use crate::error::{Error, Result};
use bson::oid::ObjectId;
use bson::spec::BinarySubtype;
use bson::{Bson, Document};
use md5::Md5;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;

/// Evaluate `op` if it is one of this module's operators; `Ok(None)` otherwise.
pub fn eval_new(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Option<Bson>> {
    let v = match op {
        "$sigmoid" => sigmoid(args, doc, vars)?,
        "$bitAnd" | "$bitOr" | "$bitXor" => bit_op(op, args, doc, vars)?,
        "$bitNot" => bit_not(args, doc, vars)?,
        "$createObjectId" => create_object_id(args)?,
        "$hash" | "$hexHash" => hash_op(op, args, doc, vars)?,
        "$similarityCosine" | "$similarityDotProduct" | "$similarityEuclidean" => similarity(op, args, doc, vars)?,
        "$topN" | "$bottomN" | "$top" | "$bottom" => top_op(op, args, doc, vars)?,
        "$toArray" | "$toObject" | "$toUUID" => to_op(op, args, doc, vars)?,
        "$encStrStartsWith" => enc_str(op, args, "ExpressionEncStrStartsWith", 10111803, ["input", "prefix"])?,
        "$encStrContains" => enc_str(op, args, "ExpressionEncStrContains", 10208800, ["input", "substring"])?,
        "$encStrEndsWith" => enc_str(op, args, "ExpressionEncStrEndsWith", 10120900, ["input", "suffix"])?,
        "$encStrNormalizedEq" => enc_str(op, args, "ExpressionEncStrNormalizedEq", 10255705, ["input", "string"])?,
        _ => return Ok(None),
    };
    Ok(Some(v))
}

// ---------- argument helpers ----------

fn eval_list(args: &Bson, doc: &Document, vars: &Vars) -> Result<Vec<Bson>> {
    match args {
        Bson::Array(items) => items.iter().map(|e| eval(e, doc, vars)).collect(),
        other => Ok(vec![eval(other, doc, vars)?]),
    }
}

fn is_nan_decimal(d: &bson::Decimal128) -> bool {
    format!("{d}") == "NaN"
}

// ---------- $sigmoid ----------

fn sigmoid(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let x = eval(args, doc, vars)?;
    let f = match &x {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        Bson::Int32(i) => *i as f64,
        Bson::Int64(i) => *i as f64,
        Bson::Double(f) => *f,
        Bson::Decimal128(d) => {
            if is_nan_decimal(d) {
                return Ok(x.clone());
            }
            decimal_to_f64(d)
        }
        other => {
            return Err(Error::new(14, format!("$sigmoid only supports numeric types, not {}", type_name(other))));
        }
    };
    Ok(Bson::Double(1.0 / (1.0 + (-f).exp())))
}

// ---------- $bitAnd / $bitOr / $bitXor / $bitNot ----------

/// Integer value of a bit-operator argument and whether it is a long.
fn int_arg(v: &Bson) -> Option<(i64, bool)> {
    match v {
        Bson::Int32(i) => Some((*i as i64, false)),
        Bson::Int64(i) => Some((*i, true)),
        Bson::Double(f) => {
            let in_range = *f >= i32::MIN as f64 && *f <= i32::MAX as f64;
            let neg_zero = *f == 0.0 && f.is_sign_negative();
            if in_range && f.fract() == 0.0 && !neg_zero { Some((*f as i64, false)) } else { None }
        }
        _ => None,
    }
}

fn bit_op(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = eval_list(args, doc, vars)?;
    if vals.iter().any(is_nullish) {
        return Ok(Bson::Null);
    }
    let mut acc: i64 = if op == "$bitAnd" { -1 } else { 0 };
    let mut long = false;
    for v in &vals {
        let (x, is_long) = int_arg(v).ok_or_else(|| {
            Error::new(14, format!("{op} only supports integer and long types, not {}", type_name(v)))
        })?;
        long |= is_long;
        acc = match op {
            "$bitAnd" => acc & x,
            "$bitOr" => acc | x,
            _ => acc ^ x,
        };
    }
    Ok(if long { Bson::Int64(acc) } else { Bson::Int32(acc as i32) })
}

fn bit_not(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = eval_list(args, doc, vars)?;
    if vals.len() != 1 {
        return Err(Error::new(
            16020,
            format!("Expression $bitNot takes exactly 1 arguments. {} were passed in.", vals.len()),
        ));
    }
    let x = &vals[0];
    if is_nullish(x) {
        return Ok(Bson::Null);
    }
    match x {
        Bson::Int32(i) => Ok(Bson::Int32(!i)),
        Bson::Int64(i) => Ok(Bson::Int64(!i)),
        Bson::Double(_) => match int_arg(x) {
            Some((v, _)) => Ok(Bson::Int32(!(v as i32))),
            None => Err(Error::new(14, "$bitNot only supports integer and long types, not double")),
        },
        Bson::Decimal128(_) => Err(Error::new(14, "$bitNot only supports integer and long types, not decimal")),
        other => Err(Error::new(28765, format!("$bitNot only supports numeric types, not {}", type_name(other)))),
    }
}

// ---------- $createObjectId ----------

fn create_object_id(args: &Bson) -> Result<Bson> {
    match args {
        Bson::Document(d) if d.is_empty() => Ok(Bson::ObjectId(ObjectId::new())),
        _ => Err(Error::new(
            9,
            "$createObjectId only accepts the empty object as argument. To convert a value to an ObjectId, use $toObjectId.",
        )),
    }
}

// ---------- $hash / $hexHash ----------

const XXH_P1: u64 = 0x9E37_79B1_85EB_CA87;
const XXH_P2: u64 = 0xC2B2_AE3D_27D4_EB4F;
const XXH_P3: u64 = 0x1656_67B1_9E37_79F9;
const XXH_P4: u64 = 0x85EB_CA77_C2B2_AE63;
const XXH_P5: u64 = 0x27D4_EB2F_1656_67C5;

fn le64(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    u64::from_le_bytes(a)
}

fn le32(b: &[u8]) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[..4]);
    u32::from_le_bytes(a)
}

fn xxh_round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(XXH_P2)).rotate_left(31).wrapping_mul(XXH_P1)
}

fn xxh_merge(acc: u64, val: u64) -> u64 {
    (acc ^ xxh_round(0, val)).wrapping_mul(XXH_P1).wrapping_add(XXH_P4)
}

/// XXH64 with the given seed.
fn xxh64(data: &[u8], seed: u64) -> u64 {
    let len = data.len();
    let mut p = 0usize;
    let mut h: u64;
    if len >= 32 {
        let mut v1 = seed.wrapping_add(XXH_P1).wrapping_add(XXH_P2);
        let mut v2 = seed.wrapping_add(XXH_P2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(XXH_P1);
        while p + 32 <= len {
            v1 = xxh_round(v1, le64(&data[p..]));
            v2 = xxh_round(v2, le64(&data[p + 8..]));
            v3 = xxh_round(v3, le64(&data[p + 16..]));
            v4 = xxh_round(v4, le64(&data[p + 24..]));
            p += 32;
        }
        h = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h = xxh_merge(h, v1);
        h = xxh_merge(h, v2);
        h = xxh_merge(h, v3);
        h = xxh_merge(h, v4);
    } else {
        h = seed.wrapping_add(XXH_P5);
    }
    h = h.wrapping_add(len as u64);
    while p + 8 <= len {
        h ^= xxh_round(0, le64(&data[p..]));
        h = h.rotate_left(27).wrapping_mul(XXH_P1).wrapping_add(XXH_P4);
        p += 8;
    }
    if p + 4 <= len {
        h ^= (le32(&data[p..]) as u64).wrapping_mul(XXH_P1);
        h = h.rotate_left(23).wrapping_mul(XXH_P2).wrapping_add(XXH_P3);
        p += 4;
    }
    while p < len {
        h ^= (data[p] as u64).wrapping_mul(XXH_P5);
        h = h.rotate_left(11).wrapping_mul(XXH_P1);
        p += 1;
    }
    h ^= h >> 33;
    h = h.wrapping_mul(XXH_P2);
    h ^= h >> 29;
    h = h.wrapping_mul(XXH_P3);
    h ^= h >> 32;
    h
}

fn hash_op(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = match args {
        Bson::Document(d) => d,
        other => {
            return Err(Error::new(
                9,
                format!("{op} expects an object of named arguments but found: {}", type_name(other)),
            ));
        }
    };
    for k in d.keys() {
        if k != "input" && k != "algorithm" {
            return Err(Error::new(9, format!("{op} found an unknown argument: {k}")));
        }
    }
    let input_e = d.get("input").ok_or_else(|| Error::new(9, format!("Missing 'input' parameter to {op}")))?;
    let alg_e = d.get("algorithm").ok_or_else(|| Error::new(9, format!("Missing 'algorithm' parameter to {op}")))?;
    let input = eval(input_e, doc, vars)?;
    let alg = eval(alg_e, doc, vars)?;
    if is_nullish(&input) {
        return Ok(Bson::Null);
    }
    let bytes: Vec<u8> = match &input {
        Bson::String(s) => s.as_bytes().to_vec(),
        Bson::Binary(b) => b.bytes.clone(),
        other => {
            return Err(Error::new(
                10754000,
                format!(
                    "{op} requires that 'input' be a valid UTF-8 string or binData, found: {} with value {other}",
                    type_name(other)
                ),
            ));
        }
    };
    let name = match &alg {
        Bson::String(s) => s.as_str(),
        other => {
            return Err(Error::new(
                10754001,
                format!("{op} requires that 'algorithm' be a string, found: {}", type_name(other)),
            ));
        }
    };
    let digest: Vec<u8> = match name {
        "md5" => Md5::digest(&bytes).to_vec(),
        "sha256" => Sha256::digest(&bytes).to_vec(),
        "xxh64" => xxh64(&bytes, 0).to_be_bytes().to_vec(),
        _ => return Err(Error::new(10754002, format!("unsupported hash algorithm: {name}"))),
    };
    if op == "$hexHash" {
        Ok(Bson::String(digest.iter().map(|b| format!("{b:02X}")).collect()))
    } else {
        Ok(binary(BinarySubtype::Generic, digest))
    }
}

// ---------- $similarityCosine / $similarityDotProduct / $similarityEuclidean ----------

fn similarity(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let (exprs, score): (&Vec<Bson>, bool) = match args {
        Bson::Array(items) => {
            if items.len() != 2 {
                return Err(Error::new(10413209, format!("{op} requires exactly two vectors")));
            }
            (items, false)
        }
        Bson::Document(d) => {
            for k in d.keys() {
                if k != "vectors" && k != "score" {
                    return Err(Error::new(10413207, format!("{op} found an unknown argument: {k}")));
                }
            }
            let score = match d.get("score") {
                None => false,
                Some(Bson::Boolean(b)) => *b,
                Some(other) => {
                    return Err(Error::new(
                        10413206,
                        format!("{op} requires 'score' to be a bool, but found: {}", type_name(other)),
                    ));
                }
            };
            let vs = d.get("vectors").ok_or_else(|| Error::new(10413209, format!("{op} requires exactly two vectors")))?;
            let items = match vs {
                Bson::Array(items) => items,
                other => {
                    return Err(Error::new(
                        10413205,
                        format!("{op} vectors must be an array of expressions, but found {}", type_name(other)),
                    ));
                }
            };
            if items.len() != 2 {
                return Err(Error::new(10413209, format!("{op} requires exactly two vectors")));
            }
            (items, score)
        }
        other => {
            return Err(Error::new(
                10413208,
                format!("{op} only supports an object or array as an argument, but found {}", type_name(other)),
            ));
        }
    };
    let a = eval(&exprs[0], doc, vars)?;
    let b = eval(&exprs[1], doc, vars)?;
    if is_nullish(&a) || is_nullish(&b) {
        return Ok(Bson::Null);
    }
    let x = similarity_vector(op, &a, 1)?;
    let y = similarity_vector(op, &b, 2)?;
    if x.len() != y.len() {
        return Err(Error::new(10413202, format!("{op} requires vectors of equal length")));
    }
    if x.is_empty() && !score && op != "$similarityCosine" {
        return Ok(Bson::Int32(0));
    }
    let dot: f64 = x.iter().zip(&y).map(|(p, q)| p * q).sum();
    let r = match op {
        "$similarityCosine" => {
            let na: f64 = x.iter().map(|p| p * p).sum::<f64>().sqrt();
            let nb: f64 = y.iter().map(|q| q * q).sum::<f64>().sqrt();
            let c = if na == 0.0 || nb == 0.0 { 0.0 } else { dot / (na * nb) };
            if score { (1.0 + c) / 2.0 } else { c }
        }
        "$similarityDotProduct" => {
            if score { (1.0 + dot) / 2.0 } else { dot }
        }
        _ => {
            let d: f64 = x.iter().zip(&y).map(|(p, q)| (p - q) * (p - q)).sum::<f64>().sqrt();
            if score { 1.0 / (1.0 + d) } else { d }
        }
    };
    Ok(Bson::Double(r))
}

/// Numeric view of a similarity vector argument; `pos` is 1 or 2 for the error code.
fn similarity_vector(op: &str, v: &Bson, pos: u8) -> Result<Vec<f64>> {
    let items = match v {
        Bson::Array(items) => items,
        Bson::Binary(_) => {
            return Err(Error::new(10506600, format!("{op} binData vectors are not supported")));
        }
        other => {
            return Err(Error::new(
                10413200,
                format!("Arguments to {op} must be arrays or binData vectors, but found type: {}", type_name(other)),
            ));
        }
    };
    items
        .iter()
        .map(|e| match e {
            Bson::Boolean(b) => Ok(if *b { 1.0 } else { 0.0 }),
            other => match num_f64(other) {
                Some(f) => Ok(f),
                None => {
                    let (code, which) = if pos == 1 { (10413203, "first") } else { (10413204, "second") };
                    Err(Error::new(code, format!("All elements in the {which} argument to {op} must be numeric or boolean")))
                }
            },
        })
        .collect()
}

// ---------- $topN / $bottomN / $top / $bottom ----------

enum SortSpec {
    Whole(bool),
    Keys(Vec<(String, bool)>),
}

/// Direction of a sort value: `Some(true)` for 1, `Some(false)` for -1, `None` for other numbers.
fn sort_dir(v: &Bson) -> Option<bool> {
    match num_f64(v)? {
        f if f == 1.0 => Some(true),
        f if f == -1.0 => Some(false),
        _ => None,
    }
}

fn top_op(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let is_n = op == "$topN" || op == "$bottomN";
    let is_bottom = op == "$bottom" || op == "$bottomN";
    let d = match args {
        Bson::Document(d) => d,
        other => {
            let code = match op {
                "$topN" => 721218,
                "$bottomN" => 7212117,
                "$top" => 7212113,
                _ => 7212122,
            };
            return Err(Error::new(code, format!("{op} requires an object as an argument, found: {}", type_name(other))));
        }
    };
    let allowed: &[&str] = if is_n { &["n", "sortBy", "input"] } else { &["sortBy", "input"] };
    for k in d.keys() {
        if !allowed.contains(&k.as_str()) {
            let code = match op {
                "$topN" => 721219,
                "$bottomN" => 7212118,
                "$top" => 7212114,
                _ => 7212123,
            };
            return Err(Error::new(code, format!("{op} found an unknown argument: {k}")));
        }
    }
    let missing = |key: &str| {
        let code = match (op, key) {
            ("$topN", "n") => 7212110,
            ("$topN", _) => 7212111,
            ("$bottomN", "n") => 7212119,
            ("$bottomN", _) => 7212120,
            ("$top", _) => 7212115,
            _ => 7212124,
        };
        Error::new(code, format!("{op} requires '{key}' to be specified"))
    };
    let n_expr = if is_n {
        Some(d.get("n").ok_or_else(|| missing("n"))?)
    } else {
        None
    };
    let input_expr = d.get("input").ok_or_else(|| missing("input"))?;

    let n: Option<i64> = match n_expr {
        None => None,
        Some(e) => {
            let v = eval(e, doc, vars)?;
            if is_nullish(&v) {
                return Ok(Bson::Null);
            }
            let as_int = match &v {
                Bson::Int32(i) => Some(*i as i64),
                Bson::Int64(i) => Some(*i),
                Bson::Double(f) if f.fract() == 0.0 && f.is_finite() => Some(*f as i64),
                Bson::Decimal128(dd) => {
                    let f = decimal_to_f64(dd);
                    if f.fract() == 0.0 && f.is_finite() { Some(f as i64) } else { None }
                }
                _ => None,
            };
            let code = if op == "$topN" { 721210 } else { 721214 };
            Some(as_int.ok_or_else(|| {
                Error::new(
                    code,
                    format!("The 'n' argument to {op} must be an integer, but was of type: {}", type_name(&v)),
                )
            })?)
        }
    };

    let input = eval(input_expr, doc, vars)?;
    if is_nullish(&input) {
        return Ok(Bson::Null);
    }
    let items = match &input {
        Bson::Array(items) => items,
        other => {
            let code = match op {
                "$topN" => 721211,
                "$bottomN" => 721215,
                "$top" => 721213,
                _ => 721217,
            };
            return Err(Error::new(
                code,
                format!("The input argument to {op} must be an array, but was of type: {}", type_name(other)),
            ));
        }
    };
    if let Some(k) = n {
        if k < 0 {
            let code = if op == "$topN" { 721212 } else { 721216 };
            return Err(Error::new(code, format!("The 'n' argument to {op} must be non-negative")));
        }
    }

    let spec = match d.get("sortBy") {
        None => SortSpec::Whole(true),
        Some(Bson::Document(sd)) => {
            if sd.is_empty() {
                return Err(Error::new(2942505, "BadValue: The sort pattern is empty when it should be a set of fields."));
            }
            let mut keys = Vec::new();
            for (k, v) in sd {
                let asc = sort_dir(v).ok_or_else(|| {
                    Error::new(2942505, "BadValue: The sort element value must be either 1 or -1")
                })?;
                keys.push((k.clone(), asc));
            }
            SortSpec::Keys(keys)
        }
        Some(v) => match num_f64(v) {
            Some(_) => SortSpec::Whole(sort_dir(v).ok_or_else(|| {
                Error::new(2942506, "The $sort element value must be either 1 or -1")
            })?),
            None => {
                return Err(Error::new(
                    2942507,
                    "The $sort is invalid: use 1/-1 to sort the whole element, or {field:1/-1} to sort embedded fields",
                ));
            }
        },
    };

    let mut sorted: Vec<&Bson> = items.iter().collect();
    let null = Bson::Null;
    match &spec {
        SortSpec::Whole(asc) => sorted.sort_by(|a, b| {
            let o = cmp_bson(a, b);
            if *asc { o } else { o.reverse() }
        }),
        SortSpec::Keys(keys) => sorted.sort_by(|a, b| {
            for (k, asc) in keys {
                let av = get_path_value(a, k).unwrap_or(&null);
                let bv = get_path_value(b, k).unwrap_or(&null);
                let o = cmp_bson(av, bv);
                let o = if *asc { o } else { o.reverse() };
                if o != Ordering::Equal {
                    return o;
                }
            }
            Ordering::Equal
        }),
    }

    match n {
        None => {
            let pick = if is_bottom { sorted.last() } else { sorted.first() };
            Ok(pick.map(|b| (*b).clone()).unwrap_or(Bson::Null))
        }
        Some(k) => {
            let k = k as usize;
            let len = sorted.len();
            let picked: Vec<Bson> = if is_bottom {
                sorted[len.saturating_sub(k)..].iter().map(|b| (*b).clone()).collect()
            } else {
                sorted[..k.min(len)].iter().map(|b| (*b).clone()).collect()
            };
            Ok(Bson::Array(picked))
        }
    }
}

// ---------- $toArray / $toObject / $toUUID ----------

fn to_op(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = eval_list(args, doc, vars)?;
    if vals.len() != 1 {
        return Err(Error::new(50723, format!("{op} requires a single argument, got {}", vals.len())));
    }
    let v = &vals[0];
    if is_nullish(v) {
        return Ok(Bson::Null);
    }
    match op {
        "$toArray" => match v {
            Bson::String(s) => match parse_json(s) {
                Some(j @ Bson::Array(_)) => Ok(j),
                _ => Err(Error::new(241, "Input doesn't represent a valid array in $convert")),
            },
            other => Err(Error::new(
                241,
                format!("Unsupported conversion from {} to array in $convert with no onError value", type_name(other)),
            )),
        },
        "$toObject" => match v {
            Bson::String(s) => match parse_json(s) {
                Some(j @ Bson::Document(_)) => Ok(j),
                _ => Err(Error::new(241, "Input doesn't represent a valid object in $convert")),
            },
            other => Err(Error::new(
                241,
                format!("Unsupported conversion from {} to object in $convert with no onError value", type_name(other)),
            )),
        },
        _ => to_uuid(v),
    }
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Parse a canonical UUID string `8-4-4-4-12` into 16 bytes.
fn uuid_from_str(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if b.len() != 36 {
        return None;
    }
    let mut out = Vec::with_capacity(16);
    let mut i = 0;
    while i < 36 {
        if [8, 13, 18, 23].contains(&i) {
            if b[i] != b'-' {
                return None;
            }
            i += 1;
            continue;
        }
        out.push((hex_val(b[i])? << 4) | hex_val(b[i + 1])?);
        i += 2;
    }
    Some(out)
}

fn to_uuid(v: &Bson) -> Result<Bson> {
    let bytes: Vec<u8> = match v {
        Bson::Int32(i) => i.to_le_bytes().to_vec(),
        Bson::Int64(i) => i.to_le_bytes().to_vec(),
        Bson::Double(f) => f.to_le_bytes().to_vec(),
        Bson::Document(d) => {
            let mut buf = Vec::new();
            d.to_writer(&mut buf)
                .map_err(|e| Error::new(241, format!("Unsupported conversion from object to binData: {e}")))?;
            buf
        }
        Bson::String(s) => uuid_from_str(s).ok_or_else(|| Error::new(241, format!("Invalid UUID string: {s}")))?,
        Bson::Binary(b) => {
            if b.subtype == BinarySubtype::Uuid {
                return Ok(v.clone());
            }
            return Err(Error::new(241, "Conversions between different BinData subtypes are not supported"));
        }
        other => {
            return Err(Error::new(
                241,
                format!("Unsupported conversion from {} to binData in $convert with no onError value", type_name(other)),
            ));
        }
    };
    Ok(binary(BinarySubtype::Uuid, bytes))
}

/// Strict JSON parser for `$toArray` / `$toObject`; extended-JSON wrappers stay plain documents.
struct JsonParser<'a> {
    s: &'a [u8],
    i: usize,
}

fn parse_json(text: &str) -> Option<Bson> {
    let mut p = JsonParser { s: text.as_bytes(), i: 0 };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != p.s.len() {
        return None;
    }
    Some(v)
}

impl JsonParser<'_> {
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }

    fn lit(&mut self, word: &str) -> bool {
        if self.s[self.i..].starts_with(word.as_bytes()) {
            self.i += word.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Option<Bson> {
        match self.peek()? {
            b'{' => {
                self.i += 1;
                let mut d = Document::new();
                self.ws();
                if self.peek()? == b'}' {
                    self.i += 1;
                    return Some(Bson::Document(d));
                }
                loop {
                    self.ws();
                    if self.peek()? != b'"' {
                        return None;
                    }
                    let k = self.string()?;
                    self.ws();
                    if self.peek()? != b':' {
                        return None;
                    }
                    self.i += 1;
                    self.ws();
                    let v = self.value()?;
                    d.insert(k, v);
                    self.ws();
                    match self.peek()? {
                        b',' => self.i += 1,
                        b'}' => {
                            self.i += 1;
                            return Some(Bson::Document(d));
                        }
                        _ => return None,
                    }
                }
            }
            b'[' => {
                self.i += 1;
                let mut items = Vec::new();
                self.ws();
                if self.peek()? == b']' {
                    self.i += 1;
                    return Some(Bson::Array(items));
                }
                loop {
                    self.ws();
                    items.push(self.value()?);
                    self.ws();
                    match self.peek()? {
                        b',' => self.i += 1,
                        b']' => {
                            self.i += 1;
                            return Some(Bson::Array(items));
                        }
                        _ => return None,
                    }
                }
            }
            b'"' => self.string().map(Bson::String),
            b't' if self.lit("true") => Some(Bson::Boolean(true)),
            b'f' if self.lit("false") => Some(Bson::Boolean(false)),
            b'n' if self.lit("null") => Some(Bson::Null),
            b'-' | b'0'..=b'9' => self.number(),
            _ => None,
        }
    }

    fn hex4(&self, at: usize) -> Option<u32> {
        let mut v = 0u32;
        for k in 0..4 {
            v = (v << 4) | hex_val(*self.s.get(at + k)?)? as u32;
        }
        Some(v)
    }

    fn string(&mut self) -> Option<String> {
        self.i += 1; // opening quote
        let mut buf: Vec<u8> = Vec::new();
        loop {
            let c = self.peek()?;
            self.i += 1;
            match c {
                b'"' => break,
                b'\\' => {
                    let e = self.peek()?;
                    self.i += 1;
                    let ch = match e {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let hi = self.hex4(self.i)?;
                            self.i += 4;
                            let code = if (0xD800..0xDC00).contains(&hi) {
                                if self.s.get(self.i) != Some(&b'\\') || self.s.get(self.i + 1) != Some(&b'u') {
                                    return None;
                                }
                                let lo = self.hex4(self.i + 2)?;
                                self.i += 6;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return None;
                                }
                                0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                            } else {
                                hi
                            };
                            char::from_u32(code)?
                        }
                        _ => return None,
                    };
                    let mut tmp = [0u8; 4];
                    buf.extend_from_slice(ch.encode_utf8(&mut tmp).as_bytes());
                }
                c if c < 0x20 => return None,
                c => buf.push(c),
            }
        }
        String::from_utf8(buf).ok()
    }

    fn number(&mut self) -> Option<Bson> {
        let start = self.i;
        let digits = |p: &mut Self| {
            let s = p.i;
            while p.i < p.s.len() && p.s[p.i].is_ascii_digit() {
                p.i += 1;
            }
            p.i > s
        };
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        if !digits(self) {
            return None;
        }
        let mut float = false;
        if self.peek() == Some(b'.') {
            self.i += 1;
            float = true;
            if !digits(self) {
                return None;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.i += 1;
            float = true;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.i += 1;
            }
            if !digits(self) {
                return None;
            }
        }
        let text = std::str::from_utf8(&self.s[start..self.i]).ok()?;
        if float {
            return text.parse::<f64>().ok().map(Bson::Double);
        }
        match text.parse::<i64>() {
            Ok(v) if v >= i32::MIN as i64 && v <= i32::MAX as i64 => Some(Bson::Int32(v as i32)),
            Ok(v) => Some(Bson::Int64(v)),
            Err(_) => text.parse::<f64>().ok().map(Bson::Double),
        }
    }
}

// ---------- $encStr* (queryable encryption) ----------

/// Validates the argument shape, then reports the executor error: these operators need a binary payload
/// that only the encrypted-collection path provides.
fn enc_str(op: &str, args: &Bson, expr: &str, code: i32, fields: [&str; 2]) -> Result<Bson> {
    let d = match args {
        Bson::Document(d) => d,
        _ => return Err(Error::new(40415, format!("BSON field '{op}.0' is an unknown field."))),
    };
    for k in d.keys() {
        if !fields.contains(&k.as_str()) {
            return Err(Error::new(40415, format!("BSON field '{op}.{k}' is an unknown field.")));
        }
    }
    for f in fields {
        if matches!(d.get(f), None | Some(Bson::Null)) {
            return Err(Error::new(40414, format!("BSON field '{op}.{f}' is missing but a required field")));
        }
    }
    Err(Error::new(code, format!("{expr} can't be evaluated without binary payload")))
}
