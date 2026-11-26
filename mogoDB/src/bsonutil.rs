//! Shared BSON helpers: MongoDB total ordering, numeric promotion, dot paths.
//!
//! Conventions used across the engine:
//! * `MISSING` (`Bson::Undefined`) means "field absent" inside expression evaluation.
//! * Comparison follows MongoDB's canonical cross-type order.

use crate::error::{Error, Result};
use bson::{Bson, Document, spec::BinarySubtype};
use std::cmp::Ordering;

/// Reserved field carrying per-document metadata through a pipeline
/// (`{textScore, geoNearDistance, vectorSearchScore, ...}`); stripped before replying.
pub const META_KEY: &str = "$__meta";

/// Sentinel for "missing" in expression evaluation (never persisted).
pub const MISSING: Bson = Bson::Undefined;

#[inline]
pub fn is_missing(b: &Bson) -> bool {
    matches!(b, Bson::Undefined)
}

/// `true` for null, undefined/missing.
#[inline]
pub fn is_nullish(b: &Bson) -> bool {
    matches!(b, Bson::Null | Bson::Undefined)
}

/// Canonical type rank used for cross-type comparison.
pub fn type_order(b: &Bson) -> u8 {
    match b {
        Bson::MinKey => 0,
        Bson::Undefined => 1,
        Bson::Null => 2,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => 3,
        Bson::String(_) | Bson::Symbol(_) => 4,
        Bson::Document(_) => 5,
        Bson::Array(_) => 6,
        Bson::Binary(_) => 7,
        Bson::ObjectId(_) => 8,
        Bson::Boolean(_) => 9,
        Bson::DateTime(_) => 10,
        Bson::Timestamp(_) => 11,
        Bson::RegularExpression(_) => 12,
        Bson::DbPointer(_) => 13,
        Bson::JavaScriptCode(_) => 14,
        Bson::JavaScriptCodeWithScope(_) => 15,
        Bson::MaxKey => 16,
    }
}

#[inline]
pub fn is_number(b: &Bson) -> bool {
    matches!(b, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_))
}

/// Numeric value as f64 (Decimal128 is parsed from its string form).
pub fn num_f64(b: &Bson) -> Option<f64> {
    match b {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(d) => Some(*d),
        Bson::Decimal128(d) => Some(decimal_to_f64(d)),
        _ => None,
    }
}

/// Integral numeric value as i64 if it is exactly representable.
pub fn num_i64(b: &Bson) -> Option<i64> {
    match b {
        Bson::Int32(i) => Some(*i as i64),
        Bson::Int64(i) => Some(*i),
        Bson::Double(d) if d.fract() == 0.0 && d.abs() < 9.223372036854776e18 => Some(*d as i64),
        Bson::Decimal128(d) => {
            let f = decimal_to_f64(d);
            (f.fract() == 0.0 && f.abs() < 9.223372036854776e18).then_some(f as i64)
        }
        _ => None,
    }
}

pub fn decimal_to_f64(d: &bson::Decimal128) -> f64 {
    let s = d.to_string();
    match s.as_str() {
        "NaN" | "-NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        _ => s.parse::<f64>().unwrap_or(f64::NAN),
    }
}

pub fn f64_to_decimal(f: f64) -> bson::Decimal128 {
    let s = if f.is_nan() {
        "NaN".to_string()
    } else if f.is_infinite() {
        if f > 0.0 { "Infinity".into() } else { "-Infinity".into() }
    } else {
        format!("{f}")
    };
    s.parse().unwrap_or_else(|_| "NaN".parse().expect("NaN decimal"))
}

fn cmp_i64_f64(i: i64, f: f64) -> Ordering {
    if f.is_nan() {
        return Ordering::Greater;
    }
    if f >= 9.223372036854775807e18 {
        return Ordering::Less;
    }
    if f < -9.223372036854775808e18 {
        return Ordering::Greater;
    }
    let t = f.trunc();
    match i.cmp(&(t as i64)) {
        Ordering::Equal => {
            let fr = f - t;
            if fr > 0.0 {
                Ordering::Less
            } else if fr < 0.0 {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        o => o,
    }
}

fn cmp_f64(a: f64, b: f64) -> Ordering {
    // MongoDB: NaN equals NaN and sorts below every other number.
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

pub fn cmp_numbers(a: &Bson, b: &Bson) -> Ordering {
    use Bson::*;
    match (a, b) {
        (Int32(x), Int32(y)) => x.cmp(y),
        (Int32(x), Int64(y)) => (*x as i64).cmp(y),
        (Int64(x), Int32(y)) => x.cmp(&(*y as i64)),
        (Int64(x), Int64(y)) => x.cmp(y),
        (Int32(x), Double(y)) => cmp_i64_f64(*x as i64, *y),
        (Int64(x), Double(y)) => cmp_i64_f64(*x, *y),
        (Double(x), Int32(y)) => cmp_i64_f64(*y as i64, *x).reverse(),
        (Double(x), Int64(y)) => cmp_i64_f64(*y, *x).reverse(),
        _ => cmp_f64(num_f64(a).unwrap_or(f64::NAN), num_f64(b).unwrap_or(f64::NAN)),
    }
}

/// MongoDB total order over BSON values.
pub fn cmp_bson(a: &Bson, b: &Bson) -> Ordering {
    let (ta, tb) = (type_order(a), type_order(b));
    if ta != tb {
        return ta.cmp(&tb);
    }
    use Bson::*;
    match (a, b) {
        (MinKey, MinKey) | (MaxKey, MaxKey) | (Null, Null) | (Undefined, Undefined) => Ordering::Equal,
        _ if ta == 3 => cmp_numbers(a, b),
        _ if ta == 4 => crate::collation::cmp_current(str_of(a), str_of(b)),
        (Document(x), Document(y)) => cmp_docs(x, y),
        (Array(x), Array(y)) => {
            for (ea, eb) in x.iter().zip(y.iter()) {
                let o = cmp_bson(ea, eb);
                if o != Ordering::Equal {
                    return o;
                }
            }
            x.len().cmp(&y.len())
        }
        (Binary(x), Binary(y)) => x
            .bytes
            .len()
            .cmp(&y.bytes.len())
            .then_with(|| u8::from(x.subtype).cmp(&u8::from(y.subtype)))
            .then_with(|| x.bytes.cmp(&y.bytes)),
        (ObjectId(x), ObjectId(y)) => x.bytes().cmp(&y.bytes()),
        (Boolean(x), Boolean(y)) => x.cmp(y),
        (DateTime(x), DateTime(y)) => x.timestamp_millis().cmp(&y.timestamp_millis()),
        (Timestamp(x), Timestamp(y)) => (x.time, x.increment).cmp(&(y.time, y.increment)),
        (RegularExpression(x), RegularExpression(y)) => {
            x.pattern.cmp(&y.pattern).then_with(|| x.options.cmp(&y.options))
        }
        (JavaScriptCode(x), JavaScriptCode(y)) => x.cmp(y),
        (JavaScriptCodeWithScope(x), JavaScriptCodeWithScope(y)) => {
            x.code.cmp(&y.code).then_with(|| cmp_docs(&x.scope, &y.scope))
        }
        (DbPointer(x), DbPointer(y)) => format!("{x:?}").cmp(&format!("{y:?}")),
        _ => Ordering::Equal,
    }
}

fn str_of(b: &Bson) -> &str {
    match b {
        Bson::String(s) | Bson::Symbol(s) => s,
        _ => "",
    }
}

/// Document comparison: per element, canonical type, then field name, then value.
pub fn cmp_docs(x: &Document, y: &Document) -> Ordering {
    let mut ix = x.iter();
    let mut iy = y.iter();
    loop {
        match (ix.next(), iy.next()) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some((ka, va)), Some((kb, vb))) => {
                let o = type_order(va)
                    .cmp(&type_order(vb))
                    .then_with(|| ka.as_bytes().cmp(kb.as_bytes()))
                    .then_with(|| cmp_bson(va, vb));
                if o != Ordering::Equal {
                    return o;
                }
            }
        }
    }
}

#[inline]
pub fn bson_eq(a: &Bson, b: &Bson) -> bool {
    cmp_bson(a, b) == Ordering::Equal
}

/// Aggregation truthiness: false, null, missing, and numeric zero are false.
pub fn truthy(b: &Bson) -> bool {
    match b {
        Bson::Boolean(x) => *x,
        Bson::Null | Bson::Undefined => false,
        Bson::Int32(i) => *i != 0,
        Bson::Int64(i) => *i != 0,
        Bson::Double(d) => *d != 0.0,
        Bson::Decimal128(d) => decimal_to_f64(d) != 0.0,
        _ => true,
    }
}

/// BSON type number as used by `$type`.
pub fn type_number(b: &Bson) -> i32 {
    match b {
        Bson::Double(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::Undefined => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Null => 10,
        Bson::RegularExpression(_) => 11,
        Bson::DbPointer(_) => 12,
        Bson::JavaScriptCode(_) => 13,
        Bson::Symbol(_) => 14,
        Bson::JavaScriptCodeWithScope(_) => 15,
        Bson::Int32(_) => 16,
        Bson::Timestamp(_) => 17,
        Bson::Int64(_) => 18,
        Bson::Decimal128(_) => 19,
        Bson::MinKey => -1,
        Bson::MaxKey => 127,
    }
}

/// Type alias as returned by aggregation `$type` (`"missing"` for MISSING).
pub fn type_name(b: &Bson) -> &'static str {
    match b {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "missing",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::DbPointer(_) => "dbPointer",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::Symbol(_) => "symbol",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
    }
}

/// Map a `$type` alias to its type number; `"number"` returns `None` (handled by callers).
pub fn type_alias_number(alias: &str) -> Option<i32> {
    Some(match alias {
        "double" => 1,
        "string" => 2,
        "object" => 3,
        "array" => 4,
        "binData" => 5,
        "undefined" => 6,
        "objectId" => 7,
        "bool" => 8,
        "date" => 9,
        "null" => 10,
        "regex" => 11,
        "dbPointer" => 12,
        "javascript" => 13,
        "symbol" => 14,
        "javascriptWithScope" => 15,
        "int" => 16,
        "timestamp" => 17,
        "long" => 18,
        "decimal" => 19,
        "minKey" => -1,
        "maxKey" => 127,
        _ => return None,
    })
}

// ---------- numeric arithmetic with MongoDB type promotion ----------

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum NumRank {
    I32,
    I64,
    F64,
    Dec,
}

fn rank(b: &Bson) -> Option<NumRank> {
    Some(match b {
        Bson::Int32(_) => NumRank::I32,
        Bson::Int64(_) => NumRank::I64,
        Bson::Double(_) => NumRank::F64,
        Bson::Decimal128(_) => NumRank::Dec,
        _ => return None,
    })
}

type DecOp = fn((i128, i32), (i128, i32)) -> Option<(i128, i32)>;

fn arith(a: &Bson, b: &Bson, ii: fn(i64, i64) -> Option<i64>, ff: fn(f64, f64) -> f64, dd: DecOp) -> Option<Bson> {
    let r = rank(a)?.max(rank(b)?);
    match r {
        NumRank::I32 | NumRank::I64 => {
            let (x, y) = (num_i64(a)?, num_i64(b)?);
            match ii(x, y) {
                Some(v) if r == NumRank::I32 && v >= i32::MIN as i64 && v <= i32::MAX as i64 => Some(Bson::Int32(v as i32)),
                Some(v) => Some(Bson::Int64(v)),
                None => Some(Bson::Double(ff(x as f64, y as f64))),
            }
        }
        NumRank::F64 => Some(Bson::Double(ff(num_f64(a)?, num_f64(b)?))),
        NumRank::Dec => {
            if let (Some(x), Some(y)) = (dec_parts(a), dec_parts(b)) {
                if let Some(d) = dd(x, y).and_then(dec_from_parts) {
                    return Some(Bson::Decimal128(d));
                }
            }
            Some(Bson::Decimal128(f64_to_decimal(ff(num_f64(a)?, num_f64(b)?))))
        }
    }
}

/// Largest coefficient Decimal128 can hold exactly (34 digits).
const DEC_MAX_COEF: i128 = 9_999_999_999_999_999_999_999_999_999_999_999;

/// Decompose a finite numeric value into (coefficient, exponent): value = coef * 10^exp.
pub fn dec_parts(b: &Bson) -> Option<(i128, i32)> {
    let s = match b {
        Bson::Int32(i) => return Some((*i as i128, 0)),
        Bson::Int64(i) => return Some((*i as i128, 0)),
        // MongoDB converts doubles to Decimal128 with 15 significant digits.
        Bson::Double(d) if d.is_finite() => format!("{d:.14e}"),
        Bson::Decimal128(d) => d.to_string(),
        _ => return None,
    };
    let (mant, exp) = match s.find(['E', 'e']) {
        Some(i) => (&s[..i], s[i + 1..].parse::<i32>().ok()?),
        None => (s.as_str(), 0),
    };
    let (neg, mant) = match mant.strip_prefix('-') {
        Some(m) => (true, m),
        None => (false, mant.strip_prefix('+').unwrap_or(mant)),
    };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    if !int.bytes().chain(frac.bytes()).all(|c| c.is_ascii_digit()) {
        return None;
    }
    let digits = format!("{int}{frac}");
    let digits = digits.trim_start_matches('0');
    if digits.len() > 34 {
        return None;
    }
    let coef: i128 = if digits.is_empty() { 0 } else { digits.parse().ok()? };
    Some((if neg { -coef } else { coef }, exp - frac.len() as i32))
}

fn dec_from_parts((coef, exp): (i128, i32)) -> Option<bson::Decimal128> {
    if coef.abs() > DEC_MAX_COEF || !(-6176..=6111).contains(&exp) {
        return None;
    }
    format!("{coef}E{exp}").parse().ok()
}

fn pow10(n: u32) -> Option<i128> {
    10i128.checked_pow(n)
}

fn dec_add(a: (i128, i32), b: (i128, i32)) -> Option<(i128, i32)> {
    let e = a.1.min(b.1);
    let x = a.0.checked_mul(pow10((a.1 - e) as u32)?)?;
    let y = b.0.checked_mul(pow10((b.1 - e) as u32)?)?;
    Some((x.checked_add(y)?, e))
}

fn dec_sub(a: (i128, i32), b: (i128, i32)) -> Option<(i128, i32)> {
    dec_add(a, (b.0.checked_neg()?, b.1))
}

fn dec_mul(a: (i128, i32), b: (i128, i32)) -> Option<(i128, i32)> {
    Some((a.0.checked_mul(b.0)?, a.1 + b.1))
}

/// `a + b` with int32 → int64 → double overflow promotion; exact for Decimal128. `None` if non-numeric.
pub fn num_add(a: &Bson, b: &Bson) -> Option<Bson> {
    arith(a, b, i64::checked_add, |x, y| x + y, dec_add)
}
pub fn num_sub(a: &Bson, b: &Bson) -> Option<Bson> {
    arith(a, b, i64::checked_sub, |x, y| x - y, dec_sub)
}
pub fn num_mul(a: &Bson, b: &Bson) -> Option<Bson> {
    arith(a, b, i64::checked_mul, |x, y| x * y, dec_mul)
}

// ---------- dot paths ----------

/// Resolve a dotted path without array fan-out. Numeric segments index arrays (`"a.0.b"`).
pub fn get_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut it = path.split('.');
    let first = it.next()?;
    let mut cur = doc.get(first)?;
    for seg in it {
        cur = match cur {
            Bson::Document(d) => d.get(seg)?,
            Bson::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// Same as [`get_path`] but rooted at any value.
pub fn get_path_value<'a>(v: &'a Bson, path: &str) -> Option<&'a Bson> {
    if path.is_empty() {
        return Some(v);
    }
    match v {
        Bson::Document(d) => get_path(d, path),
        Bson::Array(a) => {
            let (seg, rest) = path.split_once('.').unwrap_or((path, ""));
            let e = a.get(seg.parse::<usize>().ok()?)?;
            get_path_value(e, rest)
        }
        _ => None,
    }
}

fn path_err(path: &str, seg: &str, v: &Bson) -> Error {
    Error::new(
        28,
        format!("Cannot create field '{seg}' in element {{{path}: {v}}}"),
    )
}

/// Set a dotted path, creating intermediate documents. Numeric segments into arrays
/// assign by index (padding with null). Errors with code 28 (PathNotViable).
pub fn set_path(doc: &mut Document, path: &str, value: Bson) -> Result<()> {
    let (head, rest) = match path.split_once('.') {
        None => {
            doc.insert(path.to_string(), value);
            return Ok(());
        }
        Some(x) => x,
    };
    if !doc.contains_key(head) {
        doc.insert(head.to_string(), Bson::Document(Document::new()));
    }
    let child = doc.get_mut(head).expect("inserted above");
    set_path_value(child, rest, value, head)
}

fn set_path_value(cur: &mut Bson, path: &str, value: Bson, parent: &str) -> Result<()> {
    let (seg, rest) = match path.split_once('.') {
        Some((s, r)) => (s, Some(r)),
        None => (path, None),
    };
    match cur {
        Bson::Document(d) => match rest {
            None => {
                d.insert(seg.to_string(), value);
                Ok(())
            }
            Some(r) => {
                if !d.contains_key(seg) {
                    d.insert(seg.to_string(), Bson::Document(Document::new()));
                }
                set_path_value(d.get_mut(seg).expect("present"), r, value, seg)
            }
        },
        Bson::Array(a) => {
            let idx: usize = seg.parse().map_err(|_| path_err(parent, seg, &Bson::Array(a.clone())))?;
            while a.len() <= idx {
                a.push(Bson::Null);
            }
            match rest {
                None => {
                    a[idx] = value;
                    Ok(())
                }
                Some(r) => {
                    if matches!(a[idx], Bson::Null) {
                        a[idx] = Bson::Document(Document::new());
                    }
                    set_path_value(&mut a[idx], r, value, seg)
                }
            }
        }
        other => Err(path_err(parent, seg, other)),
    }
}

/// Remove a dotted path. Array elements addressed by index are set to null (MongoDB `$unset`).
/// Returns the removed value.
pub fn remove_path(doc: &mut Document, path: &str) -> Option<Bson> {
    match path.split_once('.') {
        None => doc.remove(path),
        Some((head, rest)) => remove_path_value(doc.get_mut(head)?, rest),
    }
}

fn remove_path_value(cur: &mut Bson, path: &str) -> Option<Bson> {
    let (seg, rest) = match path.split_once('.') {
        Some((s, r)) => (s, Some(r)),
        None => (path, None),
    };
    match cur {
        Bson::Document(d) => match rest {
            None => d.remove(seg),
            Some(r) => remove_path_value(d.get_mut(seg)?, r),
        },
        Bson::Array(a) => {
            let idx: usize = seg.parse().ok()?;
            let e = a.get_mut(idx)?;
            match rest {
                None => Some(std::mem::replace(e, Bson::Null)),
                Some(r) => remove_path_value(e, r),
            }
        }
        _ => None,
    }
}

// ---------- serialization ----------

pub fn to_bytes(doc: &Document) -> Vec<u8> {
    let mut v = Vec::with_capacity(64);
    doc.to_writer(&mut v).expect("in-memory document serialization");
    v
}

/// Decode a full document directly from raw bytes (no intermediate copy).
pub fn from_bytes(b: &[u8]) -> Result<Document> {
    let raw = bson::RawDocument::from_bytes(b).map_err(|e| Error::new(22, format!("invalid BSON: {e}")))?;
    Document::try_from(raw).map_err(|e| Error::new(22, format!("invalid BSON: {e}")))
}

/// Byte length of an element value of BSON type `t` starting at `b[at..]`.
fn value_len(t: u8, b: &[u8], at: usize) -> Option<usize> {
    let i32_at = |p: usize| b.get(p..p + 4).map(|s| i32::from_le_bytes([s[0], s[1], s[2], s[3]]) as usize);
    let cstr_at = |p: usize| b.get(p..)?.iter().position(|&c| c == 0).map(|n| n + 1);
    Some(match t {
        0x01 | 0x09 | 0x11 | 0x12 => 8,
        0x02 | 0x0D | 0x0E => 4 + i32_at(at)?,
        0x03 | 0x04 | 0x0F => i32_at(at)?,
        0x05 => 5 + i32_at(at)?,
        0x06 | 0x0A | 0xFF | 0x7F => 0,
        0x07 => 12,
        0x08 => 1,
        0x0B => {
            let p = cstr_at(at)?;
            p + cstr_at(at + p)?
        }
        0x0C => 4 + i32_at(at)? + 12,
        0x10 => 4,
        0x13 => 16,
        _ => return None,
    })
}

/// Decode only the listed top-level fields. Unwanted elements are skipped by their
/// length prefix without validation or allocation; wanted elements are gathered into
/// one compact buffer and decoded in a single pass.
pub fn from_bytes_fields(b: &[u8], fields: &std::collections::HashSet<String>) -> Result<Document> {
    let bad = || Error::new(22, "invalid BSON document");
    if fields.is_empty() {
        return Ok(Document::new());
    }
    if b.len() < 5 {
        return Err(bad());
    }
    let end = b.len() - 1;
    let mut sub: Vec<u8> = vec![0; 4];
    let mut found = 0;
    let mut at = 4;
    while at < end {
        let start = at;
        let t = b[at];
        let key_len = b[at + 1..].iter().position(|&c| c == 0).ok_or_else(bad)?;
        let key = std::str::from_utf8(&b[at + 1..at + 1 + key_len]).map_err(|_| bad())?;
        let vstart = at + 2 + key_len;
        let vlen = value_len(t, b, vstart).ok_or_else(bad)?;
        at = vstart + vlen;
        if at > end {
            return Err(bad());
        }
        if fields.contains(key) {
            sub.extend_from_slice(&b[start..at]);
            found += 1;
            if found == fields.len() {
                break;
            }
        }
    }
    sub.push(0);
    let n = sub.len() as i32;
    sub[..4].copy_from_slice(&n.to_le_bytes());
    from_bytes(&sub)
}

/// Serialized size of a document in bytes.
pub fn doc_size(doc: &Document) -> usize {
    to_bytes(doc).len()
}

pub fn binary(subtype: BinarySubtype, bytes: Vec<u8>) -> Bson {
    Bson::Binary(bson::Binary { subtype, bytes })
}

/// Coerce a numeric-ish option (`limit`, `batchSize`, ...) to i64.
pub fn as_i64_opt(b: Option<&Bson>) -> Option<i64> {
    b.and_then(num_i64)
}

/// Interpret a BSON value as an option boolean (numbers are truthy when non-zero).
pub fn as_bool(b: Option<&Bson>) -> bool {
    b.map(truthy).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{bson, doc};

    #[test]
    fn cross_type_order() {
        let v = [
            Bson::MinKey,
            Bson::Null,
            bson!(-1.5),
            bson!(2_i32),
            bson!("a"),
            bson!({"a": 1}),
            bson!([1]),
            Bson::Boolean(false),
            Bson::MaxKey,
        ];
        for w in v.windows(2) {
            assert_eq!(cmp_bson(&w[0], &w[1]), Ordering::Less, "{:?} < {:?}", w[0], w[1]);
        }
        assert_eq!(cmp_bson(&bson!(1_i32), &bson!(1.0)), Ordering::Equal);
        assert_eq!(cmp_bson(&bson!(i64::MAX), &bson!(9.3e18)), Ordering::Less);
    }

    #[test]
    fn paths() {
        let mut d = doc! {"a": {"b": [1, {"c": 2}]}};
        assert_eq!(get_path(&d, "a.b.1.c"), Some(&bson!(2)));
        set_path(&mut d, "a.b.3", bson!(9)).unwrap();
        assert_eq!(get_path(&d, "a.b.2"), Some(&Bson::Null));
        set_path(&mut d, "x.y", bson!(1)).unwrap();
        assert!(set_path(&mut d, "x.y.z", bson!(1)).is_err());
        assert_eq!(remove_path(&mut d, "a.b.0"), Some(bson!(1)));
        assert_eq!(get_path(&d, "a.b.0"), Some(&Bson::Null));
    }

    #[test]
    fn promotion() {
        assert_eq!(num_add(&bson!(i32::MAX), &bson!(1_i32)), Some(Bson::Int64(i32::MAX as i64 + 1)));
        assert_eq!(num_add(&bson!(1_i32), &bson!(0.5)), Some(Bson::Double(1.5)));
        assert!(matches!(num_mul(&bson!(i64::MAX), &bson!(2_i64)), Some(Bson::Double(_))));
    }
}
