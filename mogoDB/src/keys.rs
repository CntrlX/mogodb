//! Type-bracketed memcomparable encoding of BSON values for LSM index keys.
//!
//! Byte order of encoded values equals MongoDB's BSON comparison order. Encodings are
//! prefix-free, so concatenating several encoded fields (plus a record-id suffix) keeps
//! lexicographic order field by field. Descending fields are bitwise inverted.

use crate::collation::Collation;
use bson::Bson;

pub const TAG_MINKEY: u8 = 0x01;
pub const TAG_UNDEFINED: u8 = 0x04;
pub const TAG_NULL: u8 = 0x05;
pub const TAG_NUMBER: u8 = 0x10;
pub const TAG_STRING: u8 = 0x14;
pub const TAG_OBJECT: u8 = 0x18;
pub const TAG_ARRAY: u8 = 0x1C;
pub const TAG_BINARY: u8 = 0x20;
pub const TAG_OID: u8 = 0x24;
pub const TAG_BOOL: u8 = 0x28;
pub const TAG_DATE: u8 = 0x2C;
pub const TAG_TIMESTAMP: u8 = 0x30;
pub const TAG_REGEX: u8 = 0x34;
pub const TAG_DBPOINTER: u8 = 0x38;
pub const TAG_CODE: u8 = 0x3C;
pub const TAG_CODE_WS: u8 = 0x40;
pub const TAG_MAXKEY: u8 = 0x7F;

/// Type tag for a value (all numeric types share one bracket).
pub fn tag_of(v: &Bson) -> u8 {
    match v {
        Bson::MinKey => TAG_MINKEY,
        Bson::Undefined => TAG_UNDEFINED,
        Bson::Null => TAG_NULL,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => TAG_NUMBER,
        Bson::String(_) | Bson::Symbol(_) => TAG_STRING,
        Bson::Document(_) => TAG_OBJECT,
        Bson::Array(_) => TAG_ARRAY,
        Bson::Binary(_) => TAG_BINARY,
        Bson::ObjectId(_) => TAG_OID,
        Bson::Boolean(_) => TAG_BOOL,
        Bson::DateTime(_) => TAG_DATE,
        Bson::Timestamp(_) => TAG_TIMESTAMP,
        Bson::RegularExpression(_) => TAG_REGEX,
        Bson::DbPointer(_) => TAG_DBPOINTER,
        Bson::JavaScriptCode(_) => TAG_CODE,
        Bson::JavaScriptCodeWithScope(_) => TAG_CODE_WS,
        Bson::MaxKey => TAG_MAXKEY,
    }
}

fn put_escaped(out: &mut Vec<u8>, s: &[u8]) {
    for &b in s {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0, 0]);
}

/// Escape string bytes without the terminator (for `$regex` prefix bounds).
pub fn escaped_prefix(s: &[u8]) -> Vec<u8> {
    let mut out = vec![TAG_STRING];
    for &b in s {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out
}

fn f64_key(f: f64) -> [u8; 8] {
    if f.is_nan() {
        return [0; 8];
    }
    let f = if f == 0.0 { 0.0 } else { f };
    let bits = f.to_bits();
    let t = if bits >> 63 == 0 { bits | (1 << 63) } else { !bits };
    t.to_be_bytes()
}

fn i64_key(i: i64) -> [u8; 8] {
    ((i as u64) ^ (1 << 63)).to_be_bytes()
}

fn put_number(out: &mut Vec<u8>, v: &Bson) {
    // Rounded double followed by the exact integer remainder, so int64 values that
    // collide after rounding still order (and stay unique) correctly.
    let (f, rem) = match v {
        Bson::Int32(i) => (*i as f64, 0i64),
        Bson::Int64(i) => {
            let f = *i as f64;
            let rem = if f >= 9.223372036854775807e18 {
                i.wrapping_sub(i64::MAX)
            } else {
                i.wrapping_sub(f as i64)
            };
            (f, rem)
        }
        Bson::Double(d) => (*d, 0),
        other => (crate::bsonutil::num_f64(other).unwrap_or(f64::NAN), 0),
    };
    out.extend_from_slice(&f64_key(f));
    out.extend_from_slice(&i64_key(rem));
}

/// Append the ascending encoding of `v` (tag + body).
pub fn encode(out: &mut Vec<u8>, v: &Bson) {
    encode_c(out, v, None)
}

/// Append the ascending encoding of `v`; strings (also nested ones) are encoded by their
/// collation sort key when `coll` is given, as MongoDB stores collated index keys.
pub fn encode_c(out: &mut Vec<u8>, v: &Bson, coll: Option<&Collation>) {
    out.push(tag_of(v));
    match v {
        Bson::String(s) | Bson::Symbol(s) if coll.is_some() => put_escaped(out, &coll.expect("checked").sort_key(s)),
        Bson::MinKey | Bson::MaxKey | Bson::Null | Bson::Undefined => {}
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => put_number(out, v),
        Bson::String(s) | Bson::Symbol(s) => put_escaped(out, s.as_bytes()),
        Bson::Document(d) => {
            for (k, val) in d {
                out.push(tag_of(val));
                put_escaped(out, k.as_bytes());
                // value body re-encoded with its tag for prefix-freeness
                encode_c(out, val, coll);
            }
            out.push(0);
        }
        Bson::Array(a) => {
            for e in a {
                encode_c(out, e, coll);
            }
            out.push(0);
        }
        Bson::Binary(b) => {
            out.extend_from_slice(&(b.bytes.len() as u32).to_be_bytes());
            out.push(u8::from(b.subtype));
            out.extend_from_slice(&b.bytes);
        }
        Bson::ObjectId(o) => out.extend_from_slice(&o.bytes()),
        Bson::Boolean(b) => out.push(*b as u8),
        Bson::DateTime(d) => out.extend_from_slice(&i64_key(d.timestamp_millis())),
        Bson::Timestamp(t) => {
            out.extend_from_slice(&t.time.to_be_bytes());
            out.extend_from_slice(&t.increment.to_be_bytes());
        }
        Bson::RegularExpression(r) => {
            put_escaped(out, r.pattern.as_bytes());
            put_escaped(out, r.options.as_bytes());
        }
        Bson::JavaScriptCode(c) => put_escaped(out, c.as_bytes()),
        Bson::JavaScriptCodeWithScope(c) => {
            put_escaped(out, c.code.as_bytes());
            encode(out, &Bson::Document(c.scope.clone()));
        }
        Bson::DbPointer(p) => put_escaped(out, format!("{p:?}").as_bytes()),
    }
}

/// Encode with direction: descending fields are bitwise inverted.
pub fn encode_dir(out: &mut Vec<u8>, v: &Bson, descending: bool) {
    encode_dir_c(out, v, descending, None)
}

/// [`encode_dir`] under an optional collation.
pub fn encode_dir_c(out: &mut Vec<u8>, v: &Bson, descending: bool, coll: Option<&Collation>) {
    let start = out.len();
    encode_c(out, v, coll);
    if descending {
        for b in &mut out[start..] {
            *b = !*b;
        }
    }
}

pub fn encode_one(v: &Bson) -> Vec<u8> {
    encode_one_c(v, None)
}

pub fn encode_one_c(v: &Bson, coll: Option<&Collation>) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    encode_c(&mut out, v, coll);
    out
}

/// True if `v` is or contains a string (collation-sensitive).
pub fn has_string(v: &Bson) -> bool {
    match v {
        Bson::String(_) | Bson::Symbol(_) => true,
        Bson::Array(a) => a.iter().any(has_string),
        Bson::Document(d) => d.values().any(has_string),
        _ => false,
    }
}

/// Smallest byte string greater than every string having `p` as a prefix.
/// `None` when no such string exists (all 0xFF).
pub fn prefix_successor(p: &[u8]) -> Option<Vec<u8>> {
    let mut v = p.to_vec();
    while let Some(last) = v.pop() {
        if last != 0xFF {
            v.push(last + 1);
            return Some(v);
        }
    }
    None
}

// ------------------------------------------------------------------ type bits / decoding

/// Field was absent from the document (key encodes null).
pub const TB_MISSING: u8 = 0x10;
/// Double negative zero (the key encoding folds -0.0 into 0).
pub const TB_NEG_ZERO: u8 = 0x0D;
/// Value cannot be reconstructed from the key (collated strings, arrays, documents, ...).
pub const TB_OPAQUE: u8 = 0xFF;

/// Per-field type bits stored with an index entry: the original numeric type (the key
/// encoding unifies numbers), absence, or whether the key is undecodable.
pub fn type_bit(v: &Bson, present: bool, collated: bool) -> u8 {
    if !present {
        return TB_MISSING;
    }
    match v {
        Bson::Int32(_) => 0x01,
        Bson::Int64(_) => 0x02,
        Bson::Double(d) if *d == 0.0 && d.is_sign_negative() => TB_NEG_ZERO,
        Bson::Double(_) => 0x03,
        Bson::String(_) if !collated => 0x04,
        Bson::Null => 0x05,
        Bson::Boolean(_) => 0x06,
        Bson::ObjectId(_) => 0x07,
        Bson::DateTime(_) => 0x08,
        Bson::Timestamp(_) => 0x09,
        Bson::MinKey => 0x0A,
        Bson::MaxKey => 0x0B,
        Bson::Binary(_) => 0x0C,
        _ => TB_OPAQUE,
    }
}

struct Rd<'a> {
    b: &'a [u8],
    at: usize,
    mask: u8,
}

impl Rd<'_> {
    fn byte(&mut self) -> Option<u8> {
        let v = *self.b.get(self.at)? ^ self.mask;
        self.at += 1;
        Some(v)
    }
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let mut a = [0u8; N];
        for x in &mut a {
            *x = self.byte()?;
        }
        Some(a)
    }
    fn escaped(&mut self) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let c = self.byte()?;
            if c == 0 {
                match self.byte()? {
                    0 => return Some(out),
                    0xFF => out.push(0),
                    _ => return None,
                }
            } else {
                out.push(c);
            }
        }
    }
}

fn f64_from_key(k: [u8; 8]) -> f64 {
    let t = u64::from_be_bytes(k);
    if t == 0 {
        return f64::NAN;
    }
    f64::from_bits(if t >> 63 == 1 { t & !(1 << 63) } else { !t })
}

fn i64_from_key(k: [u8; 8]) -> i64 {
    (u64::from_be_bytes(k) ^ (1 << 63)) as i64
}

fn decode_value(r: &mut Rd, tb: u8) -> Option<Bson> {
    let tag = r.byte()?;
    Some(match (tag, tb) {
        (TAG_NUMBER, TB_NEG_ZERO) => {
            r.take::<16>()?;
            Bson::Double(-0.0)
        }
        (TAG_NUMBER, 0x01 | 0x02 | 0x03) => {
            let f = f64_from_key(r.take::<8>()?);
            let rem = i64_from_key(r.take::<8>()?);
            match tb {
                0x01 => Bson::Int32(f as i32),
                0x02 => Bson::Int64(if f >= 9.223372036854775807e18 { i64::MAX.wrapping_add(rem) } else { (f as i64).wrapping_add(rem) }),
                _ => Bson::Double(f),
            }
        }
        (TAG_STRING, 0x04) => Bson::String(String::from_utf8(r.escaped()?).ok()?),
        (TAG_NULL, 0x05 | TB_MISSING) => Bson::Null,
        (TAG_BOOL, 0x06) => Bson::Boolean(r.byte()? != 0),
        (TAG_OID, 0x07) => Bson::ObjectId(bson::oid::ObjectId::from_bytes(r.take::<12>()?)),
        (TAG_DATE, 0x08) => Bson::DateTime(bson::DateTime::from_millis(i64_from_key(r.take::<8>()?))),
        (TAG_TIMESTAMP, 0x09) => {
            let t = u32::from_be_bytes(r.take::<4>()?);
            let i = u32::from_be_bytes(r.take::<4>()?);
            Bson::Timestamp(bson::Timestamp { time: t, increment: i })
        }
        (TAG_MINKEY, 0x0A) => Bson::MinKey,
        (TAG_MAXKEY, 0x0B) => Bson::MaxKey,
        (TAG_BINARY, 0x0C) => {
            let n = u32::from_be_bytes(r.take::<4>()?) as usize;
            let st = r.byte()?;
            let mut bytes = Vec::with_capacity(n);
            for _ in 0..n {
                bytes.push(r.byte()?);
            }
            Bson::Binary(bson::Binary { subtype: st.into(), bytes })
        }
        _ => return None,
    })
}

/// Rebuild `{field: value}` for every indexed field from an index key (record id stripped)
/// and its type bits. `None` when any field is not reconstructible.
pub fn decode_fields(key: &[u8], fields: &[(String, bool)], tb: &[u8]) -> Option<bson::Document> {
    let mut out = bson::Document::new();
    let mut at = 0;
    for ((f, desc), &bit) in fields.iter().zip(tb) {
        if bit == TB_OPAQUE {
            return None;
        }
        let mut r = Rd { b: key, at, mask: if *desc { 0xFF } else { 0 } };
        let v = decode_value(&mut r, bit)?;
        at = r.at;
        crate::bsonutil::set_path(&mut out, f, v).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bsonutil::cmp_bson;
    use bson::{bson, oid::ObjectId};

    #[test]
    fn order_matches_bson_order() {
        let vals = vec![
            Bson::MinKey,
            Bson::Null,
            bson!(f64::NAN),
            bson!(f64::NEG_INFINITY),
            bson!(-100.5),
            bson!(-10_i32),
            bson!(0_i32),
            bson!(0.5),
            bson!(42_i64),
            bson!(9007199254740992_i64),
            bson!(9007199254740993_i64),
            bson!(i64::MAX),
            bson!(""),
            bson!("a"),
            bson!("a\u{0}b"),
            bson!("ab"),
            bson!({}),
            bson!({"a": 1}),
            bson!({"a": 1, "b": 1}),
            bson!({"b": 0}),
            bson!([]),
            bson!([1]),
            bson!([1, 2]),
            Bson::ObjectId(ObjectId::from_bytes([0; 12])),
            bson!(false),
            bson!(true),
            Bson::DateTime(bson::DateTime::from_millis(-5)),
            Bson::DateTime(bson::DateTime::from_millis(5)),
            Bson::MaxKey,
        ];
        for w in vals.windows(2) {
            let (a, b) = (encode_one(&w[0]), encode_one(&w[1]));
            assert!(a < b, "{:?} < {:?}", w[0], w[1]);
            if !(matches!(w[0], Bson::Double(f) if f.is_nan())) {
                assert_ne!(cmp_bson(&w[0], &w[1]), std::cmp::Ordering::Greater, "{:?} vs {:?}", w[0], w[1]);
            }
        }
        assert_eq!(encode_one(&bson!(1_i32)), encode_one(&bson!(1.0)));
        assert_eq!(encode_one(&bson!(-0.0)), encode_one(&bson!(0_i64)));
    }

    #[test]
    fn descending_inverts() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        encode_dir(&mut a, &bson!(1), true);
        encode_dir(&mut b, &bson!(2), true);
        assert!(a > b);
    }

    #[test]
    fn decode_roundtrip() {
        let vals = [bson!(5_i32), bson!(-7_i64), bson!(i64::MAX), bson!(9007199254740993_i64), bson!(-2.5), bson!("a\u{0}b"), Bson::Boolean(true),
            Bson::ObjectId(ObjectId::from_bytes([7; 12])), Bson::DateTime(bson::DateTime::from_millis(-123)), Bson::Null, Bson::MinKey];
        for desc in [false, true] {
            for v in &vals {
                let mut k = Vec::new();
                encode_dir(&mut k, v, desc);
                encode_dir(&mut k, &bson!(1), !desc);
                let fields = vec![("x".to_string(), desc), ("y".to_string(), !desc)];
                let d = decode_fields(&k, &fields, &[type_bit(v, true, false), 0x01]).unwrap();
                assert_eq!(d.get("x"), Some(v), "{v:?} desc={desc}");
                assert_eq!(d.get("y"), Some(&bson!(1)));
            }
        }
    }
}
