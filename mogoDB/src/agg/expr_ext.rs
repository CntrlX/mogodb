//! String, regex and date aggregation operators.
use crate::agg::expr::{eval, Vars};
use crate::bsonutil::type_name;
use crate::error::{Error, Result};
use bson::{doc, Bson, Document};
use std::cmp::Ordering;
use std::collections::HashMap;

const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;
const MIN_MS: i64 = 60_000;
const SEC_MS: i64 = 1_000;
const DEFAULT_DATE_FORMAT: &str = "%Y-%m-%dT%H:%M:%S.%LZ";
const DEFAULT_WS: &[char] = &['\0', '\t', '\n', '\u{b}', '\u{c}', '\r', ' '];

/// Evaluate operator `op` (e.g. "$concat") with raw `args`. `Ok(None)` when `op` is not handled here.
pub fn eval_ext(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Option<Bson>> {
    let v = match op {
        "$concat" => concat(args, doc, vars)?,
        "$toLower" | "$toUpper" => case_op(op, args, doc, vars)?,
        "$strLenBytes" | "$strLenCP" => str_len(op, args, doc, vars)?,
        "$substrBytes" | "$substr" | "$substrCP" => substr(op, args, doc, vars)?,
        "$strcasecmp" => strcasecmp(args, doc, vars)?,
        "$split" => split(args, doc, vars)?,
        "$trim" => trim(op, args, doc, vars, true, true)?,
        "$ltrim" => trim(op, args, doc, vars, true, false)?,
        "$rtrim" => trim(op, args, doc, vars, false, true)?,
        "$indexOfBytes" => index_of(op, args, doc, vars, false)?,
        "$indexOfCP" => index_of(op, args, doc, vars, true)?,
        "$replaceOne" => replace(op, args, doc, vars, false)?,
        "$replaceAll" => replace(op, args, doc, vars, true)?,
        "$regexMatch" => regex_op(op, RxKind::Match, args, doc, vars)?,
        "$regexFind" => regex_op(op, RxKind::Find, args, doc, vars)?,
        "$regexFindAll" => regex_op(op, RxKind::FindAll, args, doc, vars)?,
        "$year" | "$month" | "$dayOfMonth" | "$dayOfWeek" | "$dayOfYear" | "$hour" | "$minute"
        | "$second" | "$millisecond" | "$week" | "$isoWeek" | "$isoWeekYear" | "$isoDayOfWeek" => {
            date_part(op, args, doc, vars)?
        }
        "$dateToString" => date_to_string(args, doc, vars)?,
        "$dateFromString" => date_from_string(args, doc, vars)?,
        "$dateToParts" => date_to_parts(args, doc, vars)?,
        "$dateFromParts" => date_from_parts(args, doc, vars)?,
        "$dateAdd" => date_add(op, args, doc, vars, 1)?,
        "$dateSubtract" => date_add(op, args, doc, vars, -1)?,
        "$dateDiff" => date_diff(args, doc, vars)?,
        "$dateTrunc" => date_trunc(args, doc, vars)?,
        _ => return Ok(None),
    };
    Ok(Some(v))
}

type Named = HashMap<&'static str, Bson>;

// ---------- argument helpers ----------

fn eval_list(args: &Bson, doc: &Document, vars: &Vars) -> Result<Vec<Bson>> {
    match args {
        Bson::Array(items) => items.iter().map(|e| eval(e, doc, vars)).collect(),
        other => Ok(vec![eval(other, doc, vars)?]),
    }
}

fn eval_range(op: &str, args: &Bson, min: usize, max: usize, doc: &Document, vars: &Vars) -> Result<Vec<Bson>> {
    let v = eval_list(args, doc, vars)?;
    if v.len() < min || v.len() > max {
        return Err(Error::new(
            16020,
            format!("Expression {op} takes {} arguments. {} were passed in.", if min == max { format!("exactly {min}") } else { format!("between {min} and {max}") }, v.len()),
        ));
    }
    Ok(v)
}

fn eval_n(op: &str, args: &Bson, n: usize, doc: &Document, vars: &Vars) -> Result<Vec<Bson>> {
    eval_range(op, args, n, n, doc, vars)
}

/// Reference codes per operator: (missing-required, unknown-field, arity).
fn op_codes(op: &str) -> (i32, i32, i32) {
    match op {
        "$dateToParts" => (40522, 40520, 40524),
        "$dateFromString" => (40542, 40541, 40541),
        "$dateToString" => (18628, 18534, 18629),
        "$dateAdd" | "$dateSubtract" => (5166402, 5166401, 5166400),
        "$dateDiff" => (5166301, 5166302, 5166301),
        "$dateTrunc" => (5439009, 5439008, 5439007),
        "$dateFromParts" => (40414, 40518, 40519),
        _ => (40539, 40535, 40536),
    }
}

fn named(op: &str, args: &Bson, doc: &Document, vars: &Vars, allowed: &[&'static str], required: &[&str], single: Option<&'static str>) -> Result<Named> {
    let (missing, unknown, arity) = op_codes(op);
    let mut out: Named = HashMap::new();
    match args {
        Bson::Document(d) => {
            for (k, v) in d.iter() {
                let key = allowed
                    .iter()
                    .copied()
                    .find(|a| *a == k.as_str())
                    .ok_or_else(|| Error::new(unknown, format!("Unrecognized parameter to {op}: {k}")))?;
                out.insert(key, eval(v, doc, vars)?);
            }
        }
        other => {
            let Some(key) = single else {
                return Err(Error::new(arity, format!("Expression {op} requires an object argument")));
            };
            let mut v = eval_list(other, doc, vars)?;
            if v.len() != 1 {
                return Err(Error::new(arity, format!("Expression {op} takes exactly 1 arguments. {} were passed in.", v.len())));
            }
            out.insert(key, v.remove(0));
        }
    }
    for r in required {
        if !out.contains_key(*r) {
            return Err(Error::new(missing, format!("Missing '{r}' parameter to {op}")));
        }
    }
    Ok(out)
}

fn get(m: &Named, k: &str) -> Bson {
    m.get(k).cloned().unwrap_or(Bson::Null)
}

fn as_int(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 && f.is_finite() && f.abs() < 9.0e18 => Some(*f as i64),
        _ => None,
    }
}

fn coerce_str(v: &Bson) -> Result<String> {
    match v {
        Bson::Null | Bson::Undefined => Ok(String::new()),
        Bson::String(s) => Ok(s.clone()),
        Bson::Int32(i) => Ok(i.to_string()),
        Bson::Int64(i) => Ok(i.to_string()),
        Bson::Double(f) => Ok(f.to_string()),
        Bson::Boolean(b) => Ok(b.to_string()),
        Bson::ObjectId(o) => Ok(o.to_hex()),
        other => Err(Error::new(16007, format!("can't convert from BSON type {} to String", type_name(other)))),
    }
}

// ---------- string operators ----------

fn concat(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let mut out = String::new();
    let mut has_null = false;
    for v in eval_list(args, doc, vars)? {
        match v {
            Bson::Null | Bson::Undefined => has_null = true,
            Bson::String(s) => out.push_str(&s),
            other => {
                return Err(Error::new(
                    16702,
                    format!("$concat only supports strings and null, not {}", type_name(&other)),
                ));
            }
        }
    }
    Ok(if has_null { Bson::Null } else { Bson::String(out) })
}

fn case_op(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let v = eval_n(op, args, 1, doc, vars)?.remove(0);
    let s = match &v {
        Bson::Boolean(_) => {
            return Err(Error::new(16007, "can't convert from BSON type bool to String"));
        }
        Bson::DateTime(dt) => format_date(DEFAULT_DATE_FORMAT, dt.timestamp_millis(), 0)?,
        other => coerce_str(other)?,
    };
    Ok(Bson::String(if op == "$toUpper" { s.to_ascii_uppercase() } else { s.to_ascii_lowercase() }))
}

fn str_len(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let v = eval_n(op, args, 1, doc, vars)?.remove(0);
    let cp = op == "$strLenCP";
    match &v {
        Bson::String(s) => {
            let n = if cp { s.chars().count() } else { s.len() };
            Ok(Bson::Int32(n as i32))
        }
        other => Err(Error::new(
            if cp { 34471 } else { 34473 },
            format!("{op} requires a string argument, found: {}", type_name(other)),
        )),
    }
}

fn unit_to_byte(s: &str, u: usize, cp: bool) -> Result<usize> {
    if cp {
        Ok(s.char_indices().map(|(i, _)| i).chain(std::iter::once(s.len())).nth(u).unwrap_or(s.len()))
    } else if s.is_char_boundary(u) {
        Ok(u)
    } else {
        Err(Error::new(28656, "Invalid range, starting index is a UTF-8 continuation byte."))
    }
}

fn substr_impl(s: &str, start: i64, len: i64, cp: bool) -> Result<String> {
    if start < 0 {
        return Err(Error::new(50752, "$substrBytes: starting index must be non-negative"));
    }
    let start = start as usize;
    if cp {
        if len < 0 {
            return Err(Error::new(34454, "$substrCP: length must be non-negative"));
        }
        return Ok(s.chars().skip(start).take(len as usize).collect());
    }
    let n = s.len();
    if start >= n {
        return Ok(String::new());
    }
    if !s.is_char_boundary(start) {
        return Err(Error::new(28656, "Invalid range, starting index is a UTF-8 continuation byte."));
    }
    let end = if len < 0 { n } else { start.saturating_add(len as usize).min(n) };
    if !s.is_char_boundary(end) {
        return Err(Error::new(28657, "Invalid range, ending index is in the middle of a UTF-8 character."));
    }
    Ok(s[start..end].to_string())
}

/// Numeric argument truncated toward zero (fractional values are accepted).
fn as_trunc(v: &Bson) -> Option<i64> {
    match v {
        Bson::Double(f) if f.is_finite() && f.abs() < 9.0e18 => Some(f.trunc() as i64),
        other => as_int(other),
    }
}

fn substr(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let v = eval_n(op, args, 3, doc, vars)?;
    let s = coerce_str(&v[0])?;
    let cp = op == "$substrCP";
    let start = as_trunc(&v[1]).ok_or_else(|| {
        if cp {
            Error::new(34450, format!("{op}: starting index must be a numeric type (is BSON type {})", type_name(&v[1])))
        } else {
            Error::new(16034, format!("{op}: starting index must be a numeric type (is BSON type {})", type_name(&v[1])))
        }
    })?;
    let len = as_trunc(&v[2]).ok_or_else(|| {
        Error::new(16035, format!("{op}: length must be a numeric type (is BSON type {})", type_name(&v[2])))
    })?;
    Ok(Bson::String(substr_impl(&s, start, len, cp)?))
}

fn strcasecmp_impl(a: &str, b: &str) -> i32 {
    match a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase()) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn strcasecmp(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let v = eval_n("$strcasecmp", args, 2, doc, vars)?;
    let a = coerce_str(&v[0])?;
    let b = coerce_str(&v[1])?;
    Ok(Bson::Int32(strcasecmp_impl(&a, &b)))
}

fn split(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let v = eval_n("$split", args, 2, doc, vars)?;
    let s = match &v[0] {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        Bson::String(s) => s,
        other => {
            return Err(Error::new(
                40085,
                format!("$split requires an expression that evaluates to a string as a first argument, found: {}", type_name(other)),
            ));
        }
    };
    let d = match &v[1] {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        Bson::String(d) => d,
        other => {
            return Err(Error::new(
                40086,
                format!("$split requires an expression that evaluates to a string as a second argument, found: {}", type_name(other)),
            ));
        }
    };
    if d.is_empty() {
        return Err(Error::new(40087, "$split requires a non-empty separator"));
    }
    Ok(Bson::Array(s.split(d.as_str()).map(|p| Bson::String(p.to_string())).collect()))
}

fn trim_impl(s: &str, set: &[char], left: bool, right: bool) -> String {
    let pred = |c: char| set.contains(&c);
    let out = match (left, right) {
        (true, true) => s.trim_matches(pred),
        (true, false) => s.trim_start_matches(pred),
        _ => s.trim_end_matches(pred),
    };
    out.to_string()
}

fn trim(op: &str, args: &Bson, doc: &Document, vars: &Vars, left: bool, right: bool) -> Result<Bson> {
    let Bson::Document(d) = args else {
        return Err(Error::new(50696, format!("{op} expects an object")));
    };
    let mut input = None;
    let mut chars = None;
    for (k, v) in d.iter() {
        match k.as_str() {
            "input" => input = Some(eval(v, doc, vars)?),
            "chars" => chars = Some(eval(v, doc, vars)?),
            other => return Err(Error::new(50694, format!("Unrecognized parameter to {op}: {other}"))),
        }
    }
    let Some(input) = input else {
        return Err(Error::new(50695, format!("Missing 'input' parameter to {op}")));
    };
    let s = match input {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        Bson::String(s) => s,
        other => {
            return Err(Error::new(50699, format!("{op} requires 'input' to be a string, found: {}", type_name(&other))));
        }
    };
    let set: Vec<char> = match chars {
        None | Some(Bson::Undefined) => DEFAULT_WS.to_vec(),
        Some(Bson::Null) => return Ok(Bson::Null),
        Some(Bson::String(c)) => {
            if c.len() > 4096 {
                return Err(Error::new(12066800, format!("{op} requires 'chars' to be at most 4096 bytes")));
            }
            c.chars().collect()
        }
        Some(other) => {
            return Err(Error::new(50700, format!("{op} requires 'chars' to be a string, found: {}", type_name(&other))));
        }
    };
    Ok(Bson::String(trim_impl(&s, &set, left, right)))
}

fn index_impl(s: &str, sub: &str, start: i64, end: Option<i64>, cp: bool) -> Result<i64> {
    if start < 0 || end.is_some_and(|e| e < 0) {
        return Err(Error::new(40097, "starting and ending index must be non-negative"));
    }
    if cp {
        let total = s.chars().count() as i64;
        let end = end.unwrap_or(total).min(total);
        if start > end {
            return Ok(-1);
        }
        let bs = unit_to_byte(s, start as usize, true)?;
        let be = unit_to_byte(s, end as usize, true)?;
        return Ok(match s[bs..be].find(sub) {
            None => -1,
            Some(i) => s[..bs + i].chars().count() as i64,
        });
    }
    // Byte mode: positions are raw byte offsets, so a start inside a character is allowed.
    let bytes = s.as_bytes();
    let total = bytes.len() as i64;
    let end = end.unwrap_or(total).min(total);
    if start > end {
        return Ok(-1);
    }
    let hay = &bytes[start as usize..end as usize];
    let needle = sub.as_bytes();
    if needle.is_empty() {
        return Ok(start);
    }
    Ok(match hay.windows(needle.len()).position(|w| w == needle) {
        None => -1,
        Some(i) => start + i as i64,
    })
}

fn index_of(op: &str, args: &Bson, doc: &Document, vars: &Vars, cp: bool) -> Result<Bson> {
    let v = if cp {
        eval_range(op, args, 2, 4, doc, vars)?
    } else {
        let v = eval_list(args, doc, vars)?;
        if !(2..=4).contains(&v.len()) {
            return Err(Error::new(28667, format!("Expression {op} takes between 2 and 4 arguments. {} were passed in.", v.len())));
        }
        v
    };
    let s = match &v[0] {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        Bson::String(s) => s,
        other => {
            return Err(Error::new(40091, format!("{op} requires a string as the first argument, found: {}", type_name(other))));
        }
    };
    let sub = match &v[1] {
        Bson::String(s) => s,
        other => {
            return Err(Error::new(40092, format!("{op} requires a string as the second argument, found: {}", type_name(other))));
        }
    };
    let start = match v.get(2) {
        None => 0,
        Some(x) => as_int(x).ok_or_else(|| Error::new(40096, format!("{op} requires a numeric starting index")))?,
    };
    let end = match v.get(3) {
        None => None,
        Some(x) => Some(as_int(x).ok_or_else(|| Error::new(40094, format!("{op} requires a numeric ending index")))?),
    };
    Ok(Bson::Int32(index_impl(s, sub, start, end, cp)? as i32))
}

fn replace_impl(s: &str, find: &str, rep: &str, all: bool) -> String {
    if all { s.replace(find, rep) } else { s.replacen(find, rep, 1) }
}

fn replace(op: &str, args: &Bson, doc: &Document, vars: &Vars, all: bool) -> Result<Bson> {
    let Bson::Document(d) = args else {
        return Err(Error::new(51751, format!("{op} expects an object")));
    };
    let mut input = None;
    let mut find = None;
    let mut repl = None;
    for (k, v) in d.iter() {
        let val = Some(eval(v, doc, vars)?);
        match k.as_str() {
            "input" => input = val,
            "find" => find = val,
            "replacement" => repl = val,
            other => return Err(Error::new(51750, format!("Unrecognized parameter to {op}: {other}"))),
        }
    }
    let Some(input) = input else {
        return Err(Error::new(51749, format!("Missing 'input' parameter to {op}")));
    };
    let Some(find) = find else {
        return Err(Error::new(51748, format!("Missing 'find' parameter to {op}")));
    };
    let Some(repl) = repl else {
        return Err(Error::new(51747, format!("Missing 'replacement' parameter to {op}")));
    };
    let s = match input {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        Bson::String(s) => s,
        other => return Err(Error::new(10503904, format!("{op} requires that 'input' be a string, found: {}", type_name(&other)))),
    };
    let f = match find {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        Bson::String(f) => f,
        other => return Err(Error::new(10503903, format!("{op} requires that 'find' be a string, found: {}", type_name(&other)))),
    };
    let r = match repl {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        Bson::String(r) => r,
        other => {
            return Err(Error::new(51748, format!("{op} requires that 'replacement' be a string, found: {}", type_name(&other))));
        }
    };
    Ok(Bson::String(replace_impl(&s, &f, &r, all)))
}

// ---------- regex operators ----------

#[derive(Clone, Copy)]
enum RxKind {
    Match,
    Find,
    FindAll,
}

/// Rewrites PCRE's ASCII-only classes (\w \d \s and negations) so the Unicode-aware regex crate matches like MongoDB.
fn translate_ascii(p: &str) -> String {
    let mut out = String::with_capacity(p.len());
    let mut in_class = false;
    let mut chars = p.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            let Some(n) = chars.next() else {
                out.push('\\');
                break;
            };
            let rep = match (n, in_class) {
                ('w', false) => "[0-9A-Za-z_]",
                ('d', false) => "[0-9]",
                ('s', false) => "[\\t\\n\\x0B\\x0C\\r ]",
                ('W', false) => "[^0-9A-Za-z_]",
                ('D', false) => "[^0-9]",
                ('S', false) => "[^\\t\\n\\x0B\\x0C\\r ]",
                ('w', true) => "0-9A-Za-z_",
                ('d', true) => "0-9",
                ('s', true) => "\\t\\n\\x0B\\x0C\\r ",
                _ => {
                    out.push('\\');
                    out.push(n);
                    continue;
                }
            };
            out.push_str(rep);
            continue;
        }
        match c {
            '[' if !in_class => {
                in_class = true;
                out.push(c);
            }
            ']' if in_class => {
                in_class = false;
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

fn compile_regex(op: &str, pat: &Bson, opts: Option<&Bson>) -> Result<regex::Regex> {
    let opts_str = match opts {
        None | Some(Bson::Null) | Some(Bson::Undefined) => String::new(),
        Some(Bson::String(o)) => o.clone(),
        Some(other) => {
            return Err(Error::bad_value(format!("{op} requires 'options' to be a string, found: {}", type_name(other))));
        }
    };
    let (pattern, flags) = match pat {
        Bson::String(p) => (p.clone(), opts_str),
        Bson::RegularExpression(r) => {
            if !opts_str.is_empty() {
                return Err(Error::new(51107, format!("{op}: options must not be set when 'regex' is a regex type")));
            }
            (r.pattern.clone(), r.options.clone())
        }
        other => {
            return Err(Error::new(51105, format!("{op} needs 'regex' to be of type string or regex, found: {}", type_name(other))));
        }
    };
    let mut b = regex::RegexBuilder::new(translate_ascii(&pattern).as_str());
    for ch in flags.chars() {
        match ch {
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
            other => return Err(Error::new(51108, format!("invalid flag in regex options: {other}"))),
        }
    }
    b.build().map_err(|e| Error::new(51111, format!("{op}: invalid regex: {e}")))
}

fn match_doc(s: &str, c: &regex::Captures) -> Bson {
    let (start, text) = match c.get(0) {
        Some(m) => (m.start(), m.as_str()),
        None => (0, ""),
    };
    let idx = s[..start].chars().count() as i32;
    let caps: Vec<Bson> = (1..c.len())
        .map(|i| match c.get(i) {
            Some(g) => Bson::String(g.as_str().to_string()),
            None => Bson::Null,
        })
        .collect();
    Bson::Document(doc! { "match": text, "idx": idx, "captures": Bson::Array(caps) })
}

fn regex_op(op: &str, kind: RxKind, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let missing = match kind {
        RxKind::Match => 31023,
        RxKind::Find => 31022,
        RxKind::FindAll => 31023,
    };
    let Bson::Document(d) = args else {
        return Err(Error::new(missing, format!("{op} expects an object")));
    };
    let mut input = None;
    let mut regex = None;
    let mut options = None;
    for (k, v) in d.iter() {
        let val = Some(eval(v, doc, vars)?);
        match k.as_str() {
            "input" => input = val,
            "regex" => regex = val,
            "options" => options = val,
            other => return Err(Error::new(31024, format!("Unrecognized parameter to {op}: {other}"))),
        }
    }
    let Some(input) = input else {
        return Err(Error::new(missing, format!("Missing 'input' parameter to {op}")));
    };
    let Some(regex) = regex else {
        return Err(Error::new(missing, format!("Missing 'regex' parameter to {op}")));
    };
    let re = compile_regex(op, &regex, options.as_ref())?;
    let s = match input {
        Bson::Null | Bson::Undefined => None,
        Bson::String(s) => Some(s),
        other => {
            let code = match kind {
                RxKind::Match | RxKind::Find => 51104,
                RxKind::FindAll => 51106,
            };
            return Err(Error::new(code, format!("{op} needs 'input' to be of type string, found: {}", type_name(&other))));
        }
    };
    let Some(s) = s else {
        return Ok(match kind {
            RxKind::Match => Bson::Boolean(false),
            RxKind::Find => Bson::Null,
            RxKind::FindAll => Bson::Array(vec![]),
        });
    };
    Ok(match kind {
        RxKind::Match => Bson::Boolean(re.is_match(&s)),
        RxKind::Find => match re.captures(&s) {
            None => Bson::Null,
            Some(c) => match_doc(&s, &c),
        },
        RxKind::FindAll => Bson::Array(
            re.captures_iter(&s)
                // An empty match at the very end of a non-empty input is not a match in MongoDB.
                .filter(|c| !c.get(0).is_some_and(|m| m.is_empty() && m.start() == s.len() && !s.is_empty()))
                .map(|c| match_doc(&s, &c))
                .collect(),
        ),
    })
}

// ---------- civil calendar math ----------

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_in_month(y: i64, m: i64) -> i64 {
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    days_from_civil(ny, nm, 1) - days_from_civil(y, m, 1)
}

/// Sunday = 0.
fn wday0(days: i64) -> i64 {
    (days + 4).rem_euclid(7)
}

/// Monday = 1 ... Sunday = 7.
fn iso_dow(days: i64) -> i64 {
    (days + 3).rem_euclid(7) + 1
}

/// (ISO week-year, ISO week number).
fn iso_week(days: i64) -> (i64, i64) {
    let thu = days - (iso_dow(days) - 1) + 3;
    let (ty, _, _) = civil_from_days(thu);
    (ty, (thu - days_from_civil(ty, 1, 1)) / 7 + 1)
}

/// Index of the week containing `days`, where weeks start on a weekday whose shift is `shift`.
fn week_index(days: i64, shift: i64) -> i64 {
    (days - shift).div_euclid(7)
}

/// Shift so that `days.rem(7) == shift` marks the first day of a week, given weekday index `w` (Sunday = 0).
fn shift_for_weekday(w: i64) -> i64 {
    (w - 4).rem_euclid(7)
}

#[derive(Clone, Copy)]
struct Parts {
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second: i64,
    milli: i64,
    days: i64,
    ms_of_day: i64,
}

fn parts(local: i64) -> Parts {
    let days = local.div_euclid(DAY_MS);
    let msd = local.rem_euclid(DAY_MS);
    let (year, month, day) = civil_from_days(days);
    Parts {
        year,
        month,
        day,
        hour: msd / HOUR_MS,
        minute: msd / MIN_MS % 60,
        second: msd / SEC_MS % 60,
        milli: msd % SEC_MS,
        days,
        ms_of_day: msd,
    }
}

fn to_local(ms: i64, off_min: i64) -> i64 {
    ms.saturating_add(off_min.saturating_mul(MIN_MS))
}

fn checked_local(y: i64, mo: i64, d: i64, h: i64, mi: i64, s: i64, ms: i64) -> Option<i64> {
    if !(1..=12).contains(&mo)
        || d < 1
        || d > days_in_month(y, mo)
        || !(0..24).contains(&h)
        || !(0..60).contains(&mi)
        || !(0..60).contains(&s)
        || !(0..1000).contains(&ms)
    {
        return None;
    }
    Some(days_from_civil(y, mo, d) * DAY_MS + h * HOUR_MS + mi * MIN_MS + s * SEC_MS + ms)
}

fn local_tail(days: i64, h: i64, mi: i64, s: i64, ms: i64) -> Option<i64> {
    days.checked_mul(DAY_MS)?
        .checked_add(h.checked_mul(HOUR_MS)?)?
        .checked_add(mi.checked_mul(MIN_MS)?)?
        .checked_add(s.checked_mul(SEC_MS)?)?
        .checked_add(ms)
}

fn parts_to_local(y: i64, mo: i64, d: i64, h: i64, mi: i64, s: i64, ms: i64) -> Option<i64> {
    let yy = y.checked_add(mo.checked_sub(1)?.div_euclid(12))?;
    let mm = mo.checked_sub(1)?.rem_euclid(12) + 1;
    if !(1..=9999).contains(&yy) {
        return None;
    }
    let days = days_from_civil(yy, mm, 1).checked_add(d.checked_sub(1)?)?;
    local_tail(days, h, mi, s, ms)
}

fn iso_parts_to_local(iy: i64, iw: i64, idow: i64, h: i64, mi: i64, s: i64, ms: i64) -> Option<i64> {
    if !(1..=9999).contains(&iy) {
        return None;
    }
    let base = days_from_civil(iy, 1, 4);
    let monday1 = base - (iso_dow(base) - 1);
    let days = monday1.checked_add(iw.checked_sub(1)?.checked_mul(7)?)?.checked_add(idow.checked_sub(1)?)?;
    local_tail(days, h, mi, s, ms)
}

// ---------- timezone ----------

/// Parse a fixed-offset timezone ("UTC", "+05:30", "-0800", "+02") to minutes.
fn parse_tz(s: &str) -> Result<i64> {
    let up = s.to_ascii_uppercase();
    if matches!(up.as_str(), "UTC" | "GMT") {
        return Ok(0);
    }
    let err = || Error::new(40485, format!("unrecognized time zone identifier: \"{s}\""));
    let b = s.as_bytes();
    let sign = match b.first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return Err(err()),
    };
    let rest = &b[1..];
    let digits = |x: &[u8]| -> Option<i64> {
        if x.is_empty() || !x.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(x).ok()?.parse::<i64>().ok()
    };
    let (hh, mm) = match rest.len() {
        2 => (digits(rest), Some(0)),
        4 => (digits(&rest[..2]), digits(&rest[2..])),
        5 if rest[2] == b':' => (digits(&rest[..2]), digits(&rest[3..])),
        _ => return Err(err()),
    };
    match (hh, mm) {
        (Some(h), Some(m)) if m < 60 => Ok(sign * (h * 60 + m)),
        _ => Err(err()),
    }
}

/// A resolved timezone: fixed offset or an Olson zone whose offset depends on the instant.
#[derive(Clone)]
enum Tz {
    Fixed(i64),
    Zone(std::sync::Arc<crate::tz::Zone>),
}

impl Tz {
    /// Offset in minutes at UTC instant `ms`.
    fn at_utc(&self, ms: i64) -> i64 {
        match self {
            Tz::Fixed(m) => *m,
            Tz::Zone(z) => z.offset_at_utc(ms).div_euclid(60),
        }
    }
    /// Offset in minutes for local wall-clock millis `local`.
    fn for_local(&self, local: i64) -> i64 {
        match self {
            Tz::Fixed(m) => *m,
            Tz::Zone(z) => z.offset_for_local(local).div_euclid(60),
        }
    }
}

fn resolve_tz(s: &str) -> Result<Tz> {
    match parse_tz(s) {
        Ok(m) => Ok(Tz::Fixed(m)),
        Err(e) => crate::tz::load(s).map(Tz::Zone).ok_or(e),
    }
}

/// `Ok(None)` when the timezone evaluates to null (result is null).
fn tz_from(m: &Named) -> Result<Option<Tz>> {
    match m.get("timezone") {
        None => Ok(Some(Tz::Fixed(0))),
        Some(Bson::Null) | Some(Bson::Undefined) => Ok(None),
        Some(Bson::String(s)) => resolve_tz(s).map(Some),
        Some(other) => Err(Error::new(40517, format!("timezone must evaluate to a string, found: {}", type_name(other)))),
    }
}

// ---------- date values ----------

/// `Ok(None)` for null/missing; otherwise epoch milliseconds.
fn date_ms(v: &Bson) -> Result<Option<i64>> {
    Ok(Some(match v {
        Bson::Null | Bson::Undefined => return Ok(None),
        Bson::DateTime(d) => d.timestamp_millis(),
        Bson::Timestamp(t) => i64::from(t.time) * SEC_MS,
        Bson::ObjectId(o) => {
            let b = o.bytes();
            i64::from(u32::from_be_bytes([b[0], b[1], b[2], b[3]])) * SEC_MS
        }
        other => {
            return Err(Error::new(16006, format!("can't convert from BSON type {} to Date", type_name(other))));
        }
    }))
}

fn date_value(ms: i64) -> Bson {
    Bson::DateTime(bson::DateTime::from_millis(ms))
}

fn date_part(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let m = named(op, args, doc, vars, &["date", "timezone"], &["date"], Some("date"))?;
    let Some(ms) = date_ms(&get(&m, "date"))? else { return Ok(Bson::Null) };
    let Some(tz) = tz_from(&m)? else { return Ok(Bson::Null) };
    let p = parts(to_local(ms, tz.at_utc(ms)));
    let wd = wday0(p.days);
    let yd = p.days - days_from_civil(p.year, 1, 1);
    let v = match op {
        "$year" => p.year,
        "$month" => p.month,
        "$dayOfMonth" => p.day,
        "$dayOfWeek" => wd + 1,
        "$dayOfYear" => yd + 1,
        "$hour" => p.hour,
        "$minute" => p.minute,
        "$second" => p.second,
        "$millisecond" => p.milli,
        "$week" => (yd + 7 - wd) / 7,
        "$isoWeek" => iso_week(p.days).1,
        "$isoWeekYear" => iso_week(p.days).0,
        "$isoDayOfWeek" => iso_dow(p.days),
        _ => return Err(Error::internal(format!("unhandled date operator {op}"))),
    };
    // MongoDB returns $isoWeekYear as a long.
    if op == "$isoWeekYear" {
        return Ok(Bson::Int64(v as i64));
    }
    Ok(Bson::Int32(v as i32))
}
const MONTH_SHORT: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const MONTH_LONG: [&str; 12] = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

/// Format a date per MongoDB's `$dateToString` specifiers.
fn format_date(fmt: &str, ms: i64, off: i64) -> Result<String> {
    let local = to_local(ms, off);
    let p = parts(local);
    let wd = wday0(p.days);
    let yd = p.days - days_from_civil(p.year, 1, 1);
    let (iy, iw) = iso_week(p.days);
    let mut out = String::new();
    let mut chars = fmt.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let Some(spec) = chars.next() else {
            return Err(Error::new(18535, "Unmatched '%' in format string"));
        };
        let piece = match spec {
            '%' => "%".to_string(),
            'd' => format!("{:02}", p.day),
            'G' => format!("{:04}", iy),
            'H' => format!("{:02}", p.hour),
            'j' => format!("{:03}", yd + 1),
            'L' => format!("{:03}", p.milli),
            'm' => format!("{:02}", p.month),
            'M' => format!("{:02}", p.minute),
            'S' => format!("{:02}", p.second),
            'u' => iso_dow(p.days).to_string(),
            'U' => format!("{:02}", (yd + 7 - wd) / 7),
            'V' => format!("{:02}", iw),
            'w' => (wd + 1).to_string(),
            'b' => MONTH_SHORT[(p.month - 1) as usize].to_string(),
            'B' => MONTH_LONG[(p.month - 1) as usize].to_string(),
            'Y' => format!("{:04}", p.year),
            'z' => {
                let a = off.abs();
                format!("{}{:02}{:02}", if off < 0 { '-' } else { '+' }, a / 60, a % 60)
            }
            'Z' => off.to_string(),
            other => {
                return Err(Error::new(18536, format!("Invalid format character '%{other}' in format string")));
            }
        };
        out.push_str(&piece);
    }
    Ok(out)
}

fn date_to_string(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let op = "$dateToString";
    let m = named(op, args, doc, vars, &["date", "format", "timezone", "onNull"], &["date"], None)?;
    let Some(ms) = date_ms(&get(&m, "date"))? else {
        return Ok(get(&m, "onNull"));
    };
    let Some(tz) = tz_from(&m)? else { return Ok(Bson::Null) };
    let fmt = match m.get("format") {
        None | Some(Bson::Undefined) => DEFAULT_DATE_FORMAT.to_string(),
        Some(Bson::Null) => return Ok(Bson::Null),
        Some(Bson::String(f)) => f.clone(),
        Some(other) => {
            return Err(Error::new(18533, format!("{op} requires 'format' to be a string, found: {}", type_name(other))));
        }
    };
    Ok(Bson::String(format_date(&fmt, ms, tz.at_utc(ms))?))
}

/// Parse ISO-8601 style text. Returns (local millis, explicit offset in minutes).
fn parse_iso(s: &str) -> Option<(i64, Option<i64>)> {
    let mut c = Cur { rest: s };
    let neg = c.eat('-');
    let y = c.digits(4)?;
    let y = if neg { -y } else { y };
    if !c.eat('-') {
        return None;
    }
    let (y, mo, d) = if c.eat('W') {
        let w = c.digits(2)?;
        let wd = if c.eat('-') { c.digits(1)? } else { 1 };
        if !(1..=53).contains(&w) || !(1..=7).contains(&wd) {
            return None;
        }
        let jan4 = days_from_civil(y, 1, 4);
        let (yy, mm, dd) = civil_from_days(jan4 - (jan4 + 3).rem_euclid(7) + (w - 1) * 7 + (wd - 1));
        (yy, mm, dd)
    } else {
        let mo = c.digits(2)?;
        let d = if c.eat('-') { c.digits(2)? } else { 1 };
        (y, mo, d)
    };
    let (mut h, mut mi, mut sec, mut ms) = (0i64, 0i64, 0i64, 0i64);
    if c.eat('T') || c.eat(' ') {
        h = c.digits(2)?;
        if c.eat(':') {
            mi = c.digits(2)?;
            if c.eat(':') {
                sec = c.digits(2)?;
                if c.eat('.') {
                    ms = c.frac_millis()?;
                }
            }
        }
    }
    let off = if c.eat('Z') || c.eat('z') {
        Some(0)
    } else if let Some(sg) = c.sign() {
        let oh = c.digits(2)?;
        let om = if c.eat(':') || c.peek_digit() { c.digits(2)? } else { 0 };
        Some(sg * (oh * 60 + om))
    } else {
        None
    };
    if !c.rest.is_empty() {
        return None;
    }
    Some((checked_local(y, mo, d, h, mi, sec, ms)?, off))
}

/// Parse text according to a `$dateFromString` format. Err only for invalid format strings.
fn parse_fmt(fmt: &str, s: &str) -> Result<Option<(i64, Option<i64>)>> {
    let mut c = Cur { rest: s };
    let (mut y, mut mo, mut d, mut h, mut mi, mut sec, ms) = (1970i64, 1i64, 1i64, 0i64, 0i64, 0i64, 0i64);
    let mut yday: Option<i64> = None;
    let mut iso_y: Option<i64> = None;
    let mut off: Option<i64> = None;
    let mut has_mo = false;
    let mut has_d = false;
    let mut f = fmt.chars();
    while let Some(ch) = f.next() {
        if ch != '%' {
            if !c.eat(ch) {
                return Ok(None);
            }
            continue;
        }
        let Some(spec) = f.next() else {
            return Err(Error::new(18535, "Unmatched '%' in format string"));
        };
        match spec {
            '%' => {
                if !c.eat('%') {
                    return Ok(None);
                }
            }
            'Y' => match c.digits(4) { Some(v) => y = v, None => return Ok(None) },
            'm' => match c.digits(2) { Some(v) => { mo = v; has_mo = true; } None => return Ok(None) },
            'd' => match c.digits(2) { Some(v) => { d = v; has_d = true; } None => return Ok(None) },
            'H' => match c.digits(2) { Some(v) => h = v, None => return Ok(None) },
            'M' => match c.digits(2) { Some(v) => mi = v, None => return Ok(None) },
            'S' => match c.digits(2) { Some(v) => sec = v, None => return Ok(None) },
            'L' => return Ok(None),
            'j' => match c.digits(3) { Some(v) => yday = Some(v), None => return Ok(None) },
            'G' => match c.digits(4) { Some(v) => iso_y = Some(v), None => return Ok(None) },
            'b' | 'B' => {
                let names = if spec == 'B' { MONTH_LONG } else { MONTH_SHORT };
                let Some(i) = names.iter().position(|n| c.rest.starts_with(*n)) else { return Ok(None) };
                c.rest = &c.rest[names[i].len()..];
                mo = i as i64 + 1;
                has_mo = true;
            }
            'z' => {
                let Some(sg) = c.sign() else { return Ok(None) };
                let Some(hh) = c.digits(2) else { return Ok(None) };
                let Some(mm) = c.digits(2) else { return Ok(None) };
                off = Some(sg * (hh * 60 + mm));
            }
            'Z' => {
                let sg = c.sign().unwrap_or(1);
                let Some(mins) = c.digits_any() else { return Ok(None) };
                off = Some(sg * mins);
            }
            other => {
                return Err(Error::new(18536, format!("Invalid format character '%{other}' in format string")));
            }
        }
    }
    if !c.rest.is_empty() {
        return Ok(None);
    }
    if yday.is_none() && iso_y.is_none() && !(has_mo && has_d) {
        return Ok(None);
    }
    if let Some(iy) = iso_y {
        let jan4 = days_from_civil(iy, 1, 4);
        let (yy, mm, dd) = civil_from_days(jan4 - (jan4 + 3).rem_euclid(7));
        y = yy;
        mo = mm;
        d = dd;
    }
    if let Some(yd) = yday {
        let (yy, mm, dd) = civil_from_days(days_from_civil(y, 1, 1) + yd);
        y = yy;
        mo = mm;
        d = dd;
    }
    Ok(checked_local(y, mo, d, h, mi, sec, ms).map(|l| (l, off)))
}


struct Cur<'a> {
    rest: &'a str,
}

impl Cur<'_> {
    fn digits(&mut self, n: usize) -> Option<i64> {
        let b = self.rest.as_bytes();
        if b.len() < n || !b[..n].iter().all(u8::is_ascii_digit) {
            return None;
        }
        let v = self.rest[..n].parse::<i64>().ok()?;
        self.rest = &self.rest[n..];
        Some(v)
    }

    fn digits_any(&mut self) -> Option<i64> {
        let n = self.rest.bytes().take_while(u8::is_ascii_digit).count();
        if n == 0 { None } else { self.digits(n) }
    }

    fn eat(&mut self, c: char) -> bool {
        if let Some(r) = self.rest.strip_prefix(c) {
            self.rest = r;
            true
        } else {
            false
        }
    }

    fn peek_digit(&self) -> bool {
        self.rest.as_bytes().first().is_some_and(u8::is_ascii_digit)
    }

    fn sign(&mut self) -> Option<i64> {
        if self.eat('+') {
            Some(1)
        } else if self.eat('-') {
            Some(-1)
        } else {
            None
        }
    }

    /// Fractional seconds as milliseconds (first three digits, right-padded).
    fn frac_millis(&mut self) -> Option<i64> {
        let mut v = 0i64;
        let mut n = 0usize;
        while let Some(&b) = self.rest.as_bytes().first() {
            if !b.is_ascii_digit() {
                break;
            }
            if n < 3 {
                v = v * 10 + i64::from(b - b'0');
            }
            n += 1;
            self.rest = &self.rest[1..];
        }
        if n == 0 {
            return None;
        }
        for _ in n.min(3)..3 {
            v *= 10;
        }
        Some(v)
    }
}

fn date_from_string(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let op = "$dateFromString";
    let m = named(op, args, doc, vars, &["dateString", "format", "timezone", "onError", "onNull"], &["dateString"], None)?;
    let s = match get(&m, "dateString") {
        Bson::Null | Bson::Undefined => return Ok(get(&m, "onNull")),
        Bson::String(s) => s,
        other => {
            return Err(Error::new(241, format!("{op} requires that 'dateString' be a string, found: {}", type_name(&other))));
        }
    };
    let Some(tz) = tz_from(&m)? else { return Ok(Bson::Null) };
    let tz_given = matches!(m.get("timezone"), Some(v) if !matches!(v, Bson::Null | Bson::Undefined));
    let parsed = match m.get("format") {
        None | Some(Bson::Undefined) => parse_iso(&s),
        Some(Bson::Null) => return Ok(Bson::Null),
        Some(Bson::String(f)) => parse_fmt(f, &s)?,
        Some(other) => {
            return Err(Error::new(40684, format!("{op} requires 'format' to be a string, found: {}", type_name(other))));
        }
    };
    match parsed {
        Some((_, Some(_))) if tz_given => Err(Error::new(241, format!("{op}: Error parsing date string '{s}'"))),
        Some((local, off)) => {
            let utc = local - off.unwrap_or_else(|| tz.for_local(local)) * MIN_MS;
            Ok(date_value(utc))
        }
        None => match m.get("onError") {
            Some(e) => Ok(e.clone()),
            None => Err(Error::new(241, format!("{op}: Error parsing date string '{s}'"))),
        },
    }
}

fn date_to_parts(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let op = "$dateToParts";
    let m = named(op, args, doc, vars, &["date", "timezone", "iso8601"], &["date"], None)?;
    let Some(ms) = date_ms(&get(&m, "date"))? else { return Ok(Bson::Null) };
    let Some(tz) = tz_from(&m)? else { return Ok(Bson::Null) };
    let iso = match m.get("iso8601") {
        None | Some(Bson::Null) | Some(Bson::Undefined) => false,
        Some(Bson::Boolean(b)) => *b,
        Some(other) => {
            return Err(Error::new(40521, format!("iso8601 must evaluate to a bool, found {}", type_name(other))));
        }
    };
    let p = parts(to_local(ms, tz.at_utc(ms)));
    let out = if iso {
        let (iy, iw) = iso_week(p.days);
        doc! {
            "isoWeekYear": iy as i32,
            "isoWeek": iw as i32,
            "isoDayOfWeek": iso_dow(p.days) as i32,
            "hour": p.hour as i32,
            "minute": p.minute as i32,
            "second": p.second as i32,
            "millisecond": p.milli as i32,
        }
    } else {
        doc! {
            "year": p.year as i32,
            "month": p.month as i32,
            "day": p.day as i32,
            "hour": p.hour as i32,
            "minute": p.minute as i32,
            "second": p.second as i32,
            "millisecond": p.milli as i32,
        }
    };
    Ok(Bson::Document(out))
}

/// Field of `$dateFromParts`: `Ok(None)` when null, `default` when absent.
fn part_field(m: &Named, key: &str, default: Option<i64>) -> Result<Option<i64>> {
    match m.get(key) {
        None => match default {
            Some(d) => Ok(Some(d)),
            None => Err(Error::new(40516, format!("Missing '{key}' parameter to $dateFromParts"))),
        },
        Some(Bson::Null) | Some(Bson::Undefined) => Ok(None),
        Some(v) => as_int(v).map(Some).ok_or_else(|| int_field_err(key, v)),
    }
}

fn int_field_err(key: &str, v: &Bson) -> Error {
    let found = match v {
        Bson::Double(f) => format!("double with value {f}"),
        Bson::String(s) => format!("string with value \"{s}\""),
        other => type_name(other).to_string(),
    };
    Error::new(40515, format!("'{key}' must evaluate to an integer, found {found}"))
}

fn date_from_parts(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let op = "$dateFromParts";
    let m = named(
        op,
        args,
        doc,
        vars,
        &["year", "month", "day", "hour", "minute", "second", "millisecond", "isoWeekYear", "isoWeek", "isoDayOfWeek", "timezone"],
        &[],
        None,
    )?;
    let iso_mode = ["isoWeekYear", "isoWeek", "isoDayOfWeek"].iter().any(|k| m.contains_key(k));
    let cal_mode = ["year", "month", "day"].iter().any(|k| m.contains_key(k));
    if iso_mode && cal_mode {
        return Err(if m.contains_key("year") {
            Error::new(40489, format!("{op} does not allow mixing natural dates with ISO dates"))
        } else {
            Error::new(40525, format!("{op} does not allow mixing ISO dates with natural dates"))
        });
    }
    let Some(tz) = tz_from(&m)? else { return Ok(Bson::Null) };
    let hms = [
        part_field(&m, "hour", Some(0))?,
        part_field(&m, "minute", Some(0))?,
        part_field(&m, "second", Some(0))?,
        part_field(&m, "millisecond", Some(0))?,
    ];
    let local = if iso_mode {
        let f = [part_field(&m, "isoWeekYear", None)?, part_field(&m, "isoWeek", Some(1))?, part_field(&m, "isoDayOfWeek", Some(1))?];
        let Some(v) = f.into_iter().collect::<Option<Vec<i64>>>() else { return Ok(Bson::Null) };
        let Some(hv) = hms.into_iter().collect::<Option<Vec<i64>>>() else { return Ok(Bson::Null) };
        iso_parts_to_local(v[0], v[1], v[2], hv[0], hv[1], hv[2], hv[3])
    } else {
        let f = [part_field(&m, "year", None)?, part_field(&m, "month", Some(1))?, part_field(&m, "day", Some(1))?];
        let Some(v) = f.into_iter().collect::<Option<Vec<i64>>>() else { return Ok(Bson::Null) };
        if !(1..=9999).contains(&v[0]) {
            return Err(Error::new(40523, format!("'year' must evaluate to an integer in the range 1 to 9999, found {}", v[0])));
        }
        let Some(hv) = hms.into_iter().collect::<Option<Vec<i64>>>() else { return Ok(Bson::Null) };
        parts_to_local(v[0], v[1], v[2], hv[0], hv[1], hv[2], hv[3])
    };
    let Some(local) = local else {
        return Err(Error::bad_value(format!("{op}: date field out of range")));
    };
    let utc = local
        .checked_sub(tz.for_local(local) * MIN_MS)
        .ok_or_else(|| Error::bad_value(format!("{op}: date field out of range")))?;
    Ok(date_value(utc))
}

fn unit_of(v: Option<&Bson>) -> Result<Unit> {
    let s = match v {
        Some(Bson::String(s)) => s.as_str(),
        Some(other) => {
            return Err(Error::new(5439013, format!("unit must be a string, found: {}", type_name(other))));
        }
        None => return Err(Error::new(40414, "Missing 'unit' parameter")),
    };
    Ok(match s {
        "millisecond" => Unit::Millisecond,
        "second" => Unit::Second,
        "minute" => Unit::Minute,
        "hour" => Unit::Hour,
        "day" => Unit::Day,
        "week" => Unit::Week,
        "month" => Unit::Month,
        "quarter" => Unit::Quarter,
        "year" => Unit::Year,
        other => return Err(Error::new(9, format!("unrecognized date unit: '{other}'"))),
    })
}

#[derive(Clone, Copy, PartialEq)]
enum Unit {
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl Unit {
    /// Calendar units count months; `None` for fixed-length units.
    fn months(self) -> Option<i64> {
        match self {
            Unit::Month => Some(1),
            Unit::Quarter => Some(3),
            Unit::Year => Some(12),
            _ => None,
        }
    }

    /// Length in milliseconds of fixed units (0 for calendar units).
    fn fixed_ms(self) -> i64 {
        match self {
            Unit::Millisecond => 1,
            Unit::Second => SEC_MS,
            Unit::Minute => MIN_MS,
            Unit::Hour => HOUR_MS,
            Unit::Day => DAY_MS,
            Unit::Week => 7 * DAY_MS,
            Unit::Month | Unit::Quarter | Unit::Year => 0,
        }
    }
}

/// Calendar units (day and larger) are applied to local wall-clock time, so the time of
/// day is preserved across DST changes; smaller units add fixed durations.
fn add_unit(ms: i64, tz: &Tz, u: Unit, n: i64) -> Option<i64> {
    let local = ms.checked_add(tz.at_utc(ms).checked_mul(MIN_MS)?)?;
    let nl = if let Some(months) = u.months() {
        let total_months = months.checked_mul(n)?;
        let p = parts(local);
        let tot = (p.year.checked_mul(12)?.checked_add(p.month - 1)?).checked_add(total_months)?;
        let (ny, nm) = (tot.div_euclid(12), tot.rem_euclid(12) + 1);
        let nd = p.day.min(days_in_month(ny, nm));
        days_from_civil(ny, nm, nd).checked_mul(DAY_MS)?.checked_add(p.ms_of_day)?
    } else if matches!(u, Unit::Day | Unit::Week) {
        local.checked_add(n.checked_mul(u.fixed_ms())?)?
    } else {
        return ms.checked_add(n.checked_mul(u.fixed_ms())?);
    };
    nl.checked_sub(tz.for_local(nl).checked_mul(MIN_MS)?)
}

fn date_add(op: &str, args: &Bson, doc: &Document, vars: &Vars, sign: i64) -> Result<Bson> {
    let m = named(op, args, doc, vars, &["startDate", "unit", "amount", "timezone"], &["startDate", "unit", "amount"], None)?;
    let Some(ms) = date_ms(&get(&m, "startDate")).map_err(|_| Error::new(5166403, format!("{op} requires 'startDate' to be a date")))? else {
        return Ok(Bson::Null);
    };
    let u = unit_of(m.get("unit"))?;
    let amount = match get(&m, "amount") {
        Bson::Null | Bson::Undefined => return Ok(Bson::Null),
        v => as_int(&v).ok_or_else(|| {
            Error::new(5166405, format!("{op} requires 'amount' to be an integer, found: {}", type_name(&v)))
        })?,
    };
    let Some(tz) = tz_from(&m)? else { return Ok(Bson::Null) };
    let n = amount.checked_mul(sign).ok_or_else(|| Error::bad_value(format!("{op}: amount overflow")))?;
    let out = add_unit(ms, &tz, u, n).ok_or_else(|| Error::bad_value(format!("{op}: date out of range")))?;
    Ok(date_value(out))
}

fn diff_units(s: i64, e: i64, tz: &Tz, u: Unit, shift: i64) -> i64 {
    let sl = to_local(s, tz.at_utc(s));
    let el = to_local(e, tz.at_utc(e));
    let (sp, ep) = (parts(sl), parts(el));
    match u {
        Unit::Year => ep.year - sp.year,
        Unit::Quarter => (ep.year * 4 + (ep.month - 1) / 3) - (sp.year * 4 + (sp.month - 1) / 3),
        Unit::Month => (ep.year * 12 + ep.month) - (sp.year * 12 + sp.month),
        Unit::Week => week_index(ep.days, shift) - week_index(sp.days, shift),
        Unit::Day => ep.days - sp.days,
        _ => {
            let f = u.fixed_ms();
            e.div_euclid(f) - s.div_euclid(f)
        }
    }
}

fn week_start_shift(m: &Named) -> Result<i64> {
    let w = match m.get("startOfWeek") {
        None | Some(Bson::Null) | Some(Bson::Undefined) => 0,
        Some(Bson::String(s)) => match s.to_ascii_lowercase().as_str() {
            "sunday" | "sun" => 0,
            "monday" | "mon" => 1,
            "tuesday" | "tue" => 2,
            "wednesday" | "wed" => 3,
            "thursday" | "thu" => 4,
            "friday" | "fri" => 5,
            "saturday" | "sat" => 6,
            _ => return Err(Error::new(5439016, format!("unrecognized day of week for startOfWeek: '{s}'"))),
        },
        Some(other) => {
            return Err(Error::bad_value(format!("startOfWeek must be a string, found: {}", type_name(other))));
        }
    };
    Ok(shift_for_weekday(w))
}

fn date_diff(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let op = "$dateDiff";
    let m = named(op, args, doc, vars, &["startDate", "endDate", "unit", "timezone", "startOfWeek"], &["startDate", "endDate", "unit"], None)?;
    let Some(s) = date_ms(&get(&m, "startDate")).map_err(|_| Error::new(5166307, format!("{op} requires 'startDate' to be a date")))? else {
        return Ok(Bson::Null);
    };
    let Some(e) = date_ms(&get(&m, "endDate")).map_err(|_| Error::new(5166307, format!("{op} requires 'endDate' to be a date")))? else {
        return Ok(Bson::Null);
    };
    let u = unit_of(m.get("unit"))?;
    let Some(tz) = tz_from(&m)? else { return Ok(Bson::Null) };
    if matches!(m.get("startOfWeek"), Some(Bson::Null)) {
        return Ok(Bson::Null);
    }
    let shift = week_start_shift(&m)?;
    Ok(Bson::Int64(diff_units(s, e, &tz, u, shift)))
}

/// Start (local millis) of the truncation bin containing local instant `l`.
fn trunc_local(l: i64, u: Unit, bin: i64, shift: i64) -> Option<i64> {
    // Bins align to 2000-01-01 (local), as MongoDB does for every unit.
    const ORIGIN_DAY: i64 = 10957;
    if let Some(months) = u.months() {
        let p = parts(l);
        let step = months.checked_mul(bin)?;
        let total = (p.year.checked_sub(2000)?).checked_mul(12)?.checked_add(p.month - 1)?;
        let tb = total.div_euclid(step).checked_mul(step)?;
        days_from_civil(2000 + tb.div_euclid(12), tb.rem_euclid(12) + 1, 1).checked_mul(DAY_MS)
    } else if u == Unit::Week {
        let p = parts(l);
        let origin = week_index(ORIGIN_DAY + (shift - ORIGIN_DAY).rem_euclid(7), shift);
        let idx = week_index(p.days, shift);
        let start = (idx - origin).div_euclid(bin).checked_mul(bin)?.checked_add(origin)?;
        start.checked_mul(7)?.checked_add(shift)?.checked_mul(DAY_MS)
    } else {
        let f = u.fixed_ms().checked_mul(bin)?;
        let o = ORIGIN_DAY.checked_mul(DAY_MS)?;
        o.checked_add(l.checked_sub(o)?.div_euclid(f).checked_mul(f)?)
    }
}

fn date_trunc(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let op = "$dateTrunc";
    let m = named(op, args, doc, vars, &["date", "unit", "binSize", "timezone", "startOfWeek"], &["date", "unit"], None)?;
    let Some(ms) = date_ms(&get(&m, "date"))? else { return Ok(Bson::Null) };
    let u = unit_of(m.get("unit"))?;
    let bin = match get(&m, "binSize") {
        Bson::Null | Bson::Undefined if m.contains_key("binSize") => return Ok(Bson::Null),
        Bson::Null | Bson::Undefined => 1,
        v => match as_int(&v) {
            Some(b) if b >= 1 => b,
            Some(_) => return Err(Error::new(5439018, format!("{op} requires 'binSize' to be greater than 0"))),
            None => {
                return Err(Error::new(5439017, format!("{op} requires 'binSize' to be a 64-bit integer, found: {}", type_name(&v))));
            }
        },
    };
    let Some(tz) = tz_from(&m)? else { return Ok(Bson::Null) };
    let shift = week_start_shift(&m)?;
    let start = trunc_local(to_local(ms, tz.at_utc(ms)), u, bin, shift)
        .and_then(|l| l.checked_sub(tz.for_local(l).checked_mul(MIN_MS)?))
        .ok_or_else(|| Error::bad_value(format!("{op}: date out of range")))?;
    Ok(date_value(start))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms_of(y: i64, mo: i64, d: i64) -> i64 {
        days_from_civil(y, mo, d) * DAY_MS
    }

    #[test]
    fn default_format_and_iso_fields() {
        let ms = ms_of(2017, 7, 4) + 12 * HOUR_MS + 123;
        assert_eq!(format_date(DEFAULT_DATE_FORMAT, ms, 0).unwrap(), "2017-07-04T12:00:00.123Z");
    }

    #[test]
    fn week_specifiers_on_2021_01_01() {
        // 2021-01-01 is a Friday, ISO week 53 of 2020; MongoDB's %w is 1 (Sunday) .. 7 (Saturday).
        let ms = ms_of(2021, 1, 1);
        assert_eq!(format_date("%j|%U|%V|%G|%u|%w", ms, 0).unwrap(), "001|00|53|2020|5|6");
    }

    #[test]
    fn timezone_shifts_local_fields() {
        let ms = ms_of(2017, 7, 4) + 12 * HOUR_MS;
        assert_eq!(format_date("%H:%M %z", ms, 330).unwrap(), "17:30 +0530");
        assert_eq!(format_date("%Z", ms, -480).unwrap(), "-480");
    }

    #[test]
    fn invalid_format_specifier_errors() {
        assert_eq!(format_date("%q", 0, 0).unwrap_err().code, 18536);
        assert_eq!(format_date("abc%", 0, 0).unwrap_err().code, 18535);
    }

    #[test]
    fn timezone_parsing() {
        assert_eq!(parse_tz("+05:30").unwrap(), 330);
        assert_eq!(parse_tz("-0800").unwrap(), -480);
        assert_eq!(parse_tz("+02").unwrap(), 120);
        assert_eq!(parse_tz("utc").unwrap(), 0);
        assert_eq!(parse_tz("Nowhere/City").unwrap_err().code, 40485);
    }

    #[test]
    fn iso_parse_with_offset() {
        let (local, off) = parse_iso("2017-07-04T12:00:00.5+02:00").unwrap();
        assert_eq!(local, ms_of(2017, 7, 4) + 12 * HOUR_MS + 500);
        assert_eq!(off, Some(120));
        assert!(parse_iso("2017-02-30").is_none());
    }

    #[test]
    fn fmt_parse_with_numeric_offset() {
        let (local, off) = parse_fmt("%Y-%m-%d %H:%M %z", "2017-07-04 12:00 -0130").unwrap().unwrap();
        assert_eq!(local, ms_of(2017, 7, 4) + 12 * HOUR_MS);
        assert_eq!(off, Some(-90));
        assert!(parse_fmt("%Y-%m-%d", "2017-7-4").unwrap().is_none());
    }

    #[test]
    fn add_month_clamps_day() {
        let jan31 = ms_of(2020, 1, 31);
        assert_eq!(add_unit(jan31, &Tz::Fixed(0), Unit::Month, 1), Some(ms_of(2020, 2, 29)));
        assert_eq!(add_unit(jan31, &Tz::Fixed(0), Unit::Month, -12), Some(ms_of(2019, 1, 31)));
    }

    #[test]
    fn diff_counts_month_boundaries() {
        assert_eq!(diff_units(ms_of(2020, 1, 31), ms_of(2020, 2, 1), &Tz::Fixed(0), Unit::Month, 0), 1);
        assert_eq!(diff_units(ms_of(2020, 1, 1), ms_of(2021, 12, 31), &Tz::Fixed(0), Unit::Year, 0), 1);
        assert_eq!(diff_units(ms_of(2020, 1, 1), ms_of(2020, 1, 8), &Tz::Fixed(0), Unit::Week, shift_for_weekday(0)), 1);
    }

    #[test]
    fn trunc_week_starts_on_sunday() {
        // 2021-05-17 is a Monday; the Sunday before is 2021-05-16.
        let l = ms_of(2021, 5, 17) + 5 * HOUR_MS;
        assert_eq!(trunc_local(l, Unit::Week, 1, shift_for_weekday(0)), Some(ms_of(2021, 5, 16)));
        assert_eq!(trunc_local(l, Unit::Month, 1, 0), Some(ms_of(2021, 5, 1)));
        assert_eq!(trunc_local(l, Unit::Hour, 1, 0), Some(ms_of(2021, 5, 17) + 5 * HOUR_MS));
    }

    #[test]
    fn substr_is_code_point_aware() {
        assert_eq!(substr_impl("héllo", 1, 3, true).unwrap(), "éll");
        assert_eq!(substr_impl("héllo", 1, 1, false).unwrap_err().code, 28657);
        assert_eq!(substr_impl("héllo", 2, 1, false).unwrap_err().code, 28656);
    }

    #[test]
    fn index_cp_vs_bytes() {
        assert_eq!(index_impl("héllo", "l", 0, None, true).unwrap(), 2);
        assert_eq!(index_impl("héllo", "l", 0, None, false).unwrap(), 3);
        assert_eq!(index_impl("abc", "z", 0, None, true).unwrap(), -1);
    }

    #[test]
    fn trim_with_chars() {
        let ws = DEFAULT_WS.to_vec();
        assert_eq!(trim_impl("  ab \n", &ws, true, true), "ab");
        assert_eq!(trim_impl("xxaybxx", &['x', 'y'], true, true), "ayb");
        assert_eq!(trim_impl("xxaybxx", &['x'], false, true), "xxayb");
    }

    #[test]
    fn replace_one_and_all() {
        assert_eq!(replace_impl("aaa", "a", "b", false), "baa");
        assert_eq!(replace_impl("aaa", "a", "b", true), "bbb");
    }

    #[test]
    fn regex_find_reports_cp_index_and_captures() {
        let re = compile_regex("$regexFind", &Bson::String(r"(\d+)-(\d+)".into()), None).unwrap();
        let s = "éé 12-34";
        let c = re.captures(s).unwrap();
        let d = match_doc(s, &c);
        let Bson::Document(d) = d else { panic!("expected document") };
        assert_eq!(d.get_str("match").unwrap(), "12-34");
        assert_eq!(d.get_i32("idx").unwrap(), 3);
        assert_eq!(
            d.get_array("captures").unwrap(),
            &vec![Bson::String("12".into()), Bson::String("34".into())]
        );
    }

    #[test]
    fn regex_flags_and_errors() {
        let re = compile_regex("$regexMatch", &Bson::String("abc".into()), Some(&Bson::String("i".into()))).unwrap();
        assert!(re.is_match("xABCx"));
        assert_eq!(compile_regex("$regexMatch", &Bson::String("a".into()), Some(&Bson::String("q".into()))).err().unwrap().code, 51108);
    }

    #[test]
    fn strcasecmp_ordering() {
        assert_eq!(strcasecmp_impl("ABC", "abd"), -1);
        assert_eq!(strcasecmp_impl("abc", "ABC"), 0);
        assert_eq!(strcasecmp_impl("b", "A"), 1);
    }

    #[test]
    fn parts_to_local_normalizes_overflowing_month() {
        // month 13 of 2020 is January 2021.
        assert_eq!(parts_to_local(2020, 13, 1, 0, 0, 0, 0), Some(ms_of(2021, 1, 1)));
    }

    #[test]
    fn iso_parts_to_local_week_one() {
        // ISO week 1 of 2021 starts Monday 2021-01-04.
        assert_eq!(iso_parts_to_local(2021, 1, 1, 0, 0, 0, 0), Some(ms_of(2021, 1, 4)));
    }
}
