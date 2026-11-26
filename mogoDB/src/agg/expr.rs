//! Aggregation expression evaluator.
use crate::agg::expr_ext;
use crate::bsonutil::{
    META_KEY, MISSING, bson_eq, cmp_bson, decimal_to_f64, doc_size, f64_to_decimal, get_path_value, is_missing,
    is_nullish, is_number, num_add, num_f64, num_mul, num_sub, to_bytes, truthy, type_name,
};
use crate::error::{Error, Result};
use bson::oid::ObjectId;
use bson::{Bson, DateTime, Decimal128, Document};
use std::cmp::Ordering;
use std::collections::HashMap;

/// User/system variables (without the `$$` prefix): e.g. "NOW", "CLUSTER_TIME", `$let`/`$map` bindings.
pub type Vars = HashMap<String, Bson>;

/// Maximum number of elements `$range` may produce.
const RANGE_MAX: usize = 10_000_000;

/// Evaluate `expr` with `doc` as `$$ROOT`/`$$CURRENT`. Missing values are `bsonutil::MISSING`.
pub fn eval(expr: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    match expr {
        Bson::String(s) => {
            if let Some(body) = s.strip_prefix("$$") {
                variable(body, doc, vars)
            } else if let Some(path) = s.strip_prefix('$') {
                Ok(resolve_field(doc, path))
            } else {
                Ok(expr.clone())
            }
        }
        Bson::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for e in items {
                let v = eval(e, doc, vars)?;
                out.push(if is_missing(&v) { Bson::Null } else { v });
            }
            Ok(Bson::Array(out))
        }
        Bson::Document(d) => eval_document(d, doc, vars),
        other => Ok(other.clone()),
    }
}

/// Resolve a field path (without leading `$`) with array fan-out (`"a.b"` over `a: [{b:1},{b:2}]` -> `[1,2]`).
pub fn resolve_field(doc: &Document, path: &str) -> Bson {
    let segs: Vec<&str> = path.split('.').collect();
    path_in_doc(doc, &segs)
}

fn path_in_doc(d: &Document, segs: &[&str]) -> Bson {
    let Some((head, rest)) = segs.split_first() else {
        return Bson::Document(d.clone());
    };
    match d.get(*head) {
        None => MISSING,
        Some(v) => path_in_value(v, rest),
    }
}

fn path_in_value(v: &Bson, segs: &[&str]) -> Bson {
    if segs.is_empty() {
        return v.clone();
    }
    match v {
        Bson::Document(d) => path_in_doc(d, segs),
        Bson::Array(items) => Bson::Array(
            items
                .iter()
                .filter_map(|e| match e {
                    Bson::Document(d) => {
                        let r = path_in_doc(d, segs);
                        (!is_missing(&r)).then_some(r)
                    }
                    _ => None,
                })
                .collect(),
        ),
        _ => MISSING,
    }
}

fn eval_document(d: &Document, doc: &Document, vars: &Vars) -> Result<Bson> {
    if d.len() == 1 {
        if let Some((k, v)) = d.iter().next() {
            if k.starts_with('$') {
                return call_op(k.as_str(), v, doc, vars);
            }
        }
    } else if d.keys().any(|k| k.starts_with('$')) {
        return Err(Error::new(
            15983,
            format!(
                "an expression specification must contain exactly one field, the name of the expression. Found {} fields in {:?}",
                d.len(),
                d
            ),
        ));
    }
    let mut out = Document::new();
    for (k, v) in d {
        let r = eval(v, doc, vars)?;
        if !is_missing(&r) {
            out.insert(k.clone(), r);
        }
    }
    Ok(Bson::Document(out))
}

fn variable(body: &str, doc: &Document, vars: &Vars) -> Result<Bson> {
    let (name, path) = match body.split_once('.') {
        Some((n, p)) => (n, Some(p)),
        None => (body, None),
    };
    let segs: Vec<&str> = path.map(|p| p.split('.').collect()).unwrap_or_default();
    let base = match name {
        "ROOT" | "CURRENT" => {
            if segs.is_empty() {
                return Ok(Bson::Document(doc.clone()));
            }
            return Ok(path_in_doc(doc, &segs));
        }
        "NOW" => vars.get("NOW").cloned().unwrap_or_else(|| Bson::DateTime(DateTime::now())),
        "CLUSTER_TIME" => match vars.get("CLUSTER_TIME") {
            Some(v) => v.clone(),
            None => {
                return Err(Error::new(
                    10071200,
                    "system variable $$CLUSTER_TIME is not available in standalone mode",
                ));
            }
        },
        "REMOVE" => MISSING,
        _ => match vars.get(name) {
            Some(v) => v.clone(),
            None => return Err(Error::new(17276, format!("Use of undefined variable: {name}"))),
        },
    };
    Ok(path_in_value(&base, &segs))
}

// ---------- helpers ----------

fn arity_err(op: &str, want: usize, got: usize) -> Error {
    Error::new(16020, format!("Expression {op} takes exactly {want} arguments. {got} were passed in."))
}

/// Operator arguments as a list: an array is its elements, anything else is a single argument.
fn items(args: &Bson) -> Vec<&Bson> {
    match args {
        Bson::Array(v) => v.iter().collect(),
        other => vec![other],
    }
}

/// Evaluate all operator arguments eagerly.
fn ev_all(args: &Bson, doc: &Document, vars: &Vars) -> Result<Vec<Bson>> {
    items(args).into_iter().map(|e| eval(e, doc, vars)).collect()
}

/// Evaluate all operator arguments and require exactly `N` of them.
fn fixed<const N: usize>(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<[Bson; N]> {
    let v = ev_all(args, doc, vars)?;
    let got = v.len();
    v.try_into().map_err(|_: Vec<Bson>| arity_err(op, N, got))
}

fn doc_arg<'a>(op: &str, args: &'a Bson) -> Result<&'a Document> {
    match args {
        Bson::Document(d) => Ok(d),
        _ => Err(Error::bad_value(format!("{op} requires an object argument"))),
    }
}

fn get_arg<'a>(d: &'a Document, op: &str, key: &str) -> Result<&'a Bson> {
    d.get(key).ok_or_else(|| Error::bad_value(format!("Missing '{key}' parameter to {op}")))
}

fn str_field(op: &str, d: &Document) -> Result<String> {
    match get_arg(d, op, "field")? {
        Bson::String(s) => Ok(s.clone()),
        _ => Err(Error::bad_value(format!("{op} requires 'field' to be a string"))),
    }
}

fn as_name(d: &Document) -> Result<String> {
    match d.get("as") {
        None => Ok("this".to_string()),
        Some(Bson::String(s)) => Ok(s.clone()),
        Some(_) => Err(Error::bad_value("'as' must be a string")),
    }
}

fn check_args(d: &Document, allowed: &[&str], err: impl Fn(&str) -> Error) -> Result<()> {
    for k in d.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(err(k));
        }
    }
    Ok(())
}

/// Validates a user variable name supplied via `arrayIndexAs`/`as`.
fn validate_var_name(s: &str) -> Result<()> {
    let Some(first) = s.chars().next() else {
        return Err(Error::new(9, "empty variable names are not allowed"));
    };
    if s != "CURRENT" && first.is_ascii() && !first.is_ascii_lowercase() {
        return Err(Error::new(9, format!("'{s}' starts with an invalid character for a user variable name")));
    }
    if s.contains('-') {
        return Err(Error::new(9, format!("'{s}' contains an invalid character for a variable name: '-'")));
    }
    Ok(())
}

/// Name of the index variable: `arrayIndexAs` if given (validated), else `IDX`.
/// The bool reports whether the name was given explicitly.
fn index_name(d: &Document) -> Result<(String, bool)> {
    match d.get("arrayIndexAs") {
        None => Ok(("IDX".to_string(), false)),
        Some(Bson::String(s)) => {
            validate_var_name(s)?;
            Ok((s.clone(), true))
        }
        Some(_) => Err(Error::new(9, "empty variable names are not allowed")),
    }
}

fn index_val(i: usize) -> Bson {
    Bson::Int32(i32::try_from(i).unwrap_or(i32::MAX))
}

/// Shell-style rendering of a value for error messages (`{a: 1}`, `"abc"`, `[1, 2]`).
fn shell_repr(v: &Bson) -> String {
    match v {
        Bson::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Bson::Array(a) => format!("[{}]", a.iter().map(shell_repr).collect::<Vec<_>>().join(", ")),
        Bson::Document(d) => {
            format!("{{{}}}", d.iter().map(|(k, x)| format!("{k}: {}", shell_repr(x))).collect::<Vec<_>>().join(", "))
        }
        other => other.to_string(),
    }
}

/// Integral value of a number (ints, or doubles with no fractional part).
fn int_val(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        Bson::Double(d) if d.fract() == 0.0 && d.abs() < 9.0e18 => Some(*d as i64),
        _ => None,
    }
}

fn i64_of(v: &Bson) -> Option<i64> {
    match v {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        _ => None,
    }
}

fn int_bson(i: i64) -> Bson {
    match i32::try_from(i) {
        Ok(v) => Bson::Int32(v),
        Err(_) => Bson::Int64(i),
    }
}

fn num_check(code: i32, op: &str, v: &Bson) -> Result<()> {
    if is_number(v) {
        Ok(())
    } else {
        Err(Error::new(code, format!("{op} only supports numeric types, not {}", type_name(v))))
    }
}

fn f64_of(op: &str, v: &Bson) -> Result<f64> {
    num_f64(v).ok_or_else(|| Error::type_mismatch(format!("{op} only supports numeric types, not {}", type_name(v))))
}

/// Milliseconds carried by a numeric value (for date arithmetic).
fn millis_of(v: &Bson) -> i64 {
    match v {
        Bson::Int32(i) => i64::from(*i),
        Bson::Int64(i) => *i,
        _ => num_f64(v).map(|f| f as i64).unwrap_or(0),
    }
}

fn call_op(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    match op {
        "$add" => op_add(args, doc, vars),
        "$subtract" => op_subtract(args, doc, vars),
        "$multiply" => op_multiply(args, doc, vars),
        "$divide" => op_divide(args, doc, vars),
        "$mod" => op_mod(args, doc, vars),
        "$abs" => op_abs(args, doc, vars),
        "$ceil" => op_ceil_floor(op, args, doc, vars, f64::ceil),
        "$floor" => op_ceil_floor(op, args, doc, vars, f64::floor),
        "$round" => op_round(op, args, doc, vars, false),
        "$trunc" => op_round(op, args, doc, vars, true),
        "$pow" => op_pow(args, doc, vars),
        "$log" => op_log(args, doc, vars),
        "$atan2" => op_atan2(args, doc, vars),
        "$sqrt" => math1(op, args, doc, vars, |x| (x >= 0.0).then(|| x.sqrt()), "greater than or equal to 0"),
        "$exp" => math1(op, args, doc, vars, |x| Some(x.exp()), ""),
        "$ln" => math1(op, args, doc, vars, |x| (x > 0.0).then(|| x.ln()), "a positive number"),
        "$log10" => math1(op, args, doc, vars, |x| (x > 0.0).then(|| x.log10()), "a positive number"),
        "$sin" => math1(op, args, doc, vars, |x| Some(x.sin()), ""),
        "$cos" => math1(op, args, doc, vars, |x| Some(x.cos()), ""),
        "$tan" => math1(op, args, doc, vars, |x| Some(x.tan()), ""),
        "$asin" => math1(op, args, doc, vars, |x| ((-1.0..=1.0).contains(&x)).then(|| x.asin()), "in the range [-1, 1]"),
        "$acos" => math1(op, args, doc, vars, |x| ((-1.0..=1.0).contains(&x)).then(|| x.acos()), "in the range [-1, 1]"),
        "$atan" => math1(op, args, doc, vars, |x| Some(x.atan()), ""),
        "$sinh" => math1(op, args, doc, vars, |x| Some(x.sinh()), ""),
        "$cosh" => math1(op, args, doc, vars, |x| Some(x.cosh()), ""),
        "$tanh" => math1(op, args, doc, vars, |x| Some(x.tanh()), ""),
        "$asinh" => math1(op, args, doc, vars, |x| Some(x.asinh()), ""),
        "$acosh" => math1(op, args, doc, vars, |x| (x >= 1.0).then(|| x.acosh()), "greater than or equal to 1"),
        "$atanh" => math1(op, args, doc, vars, |x| (x > -1.0 && x < 1.0).then(|| x.atanh()), "in the range (-1, 1)"),
        "$degreesToRadians" => math1(op, args, doc, vars, |x| Some(x.to_radians()), ""),
        "$radiansToDegrees" => math1(op, args, doc, vars, |x| Some(x.to_degrees()), ""),
        "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" | "$cmp" => op_compare(op, args, doc, vars),
        "$and" | "$or" => op_and_or(op, args, doc, vars),
        "$not" => {
            let [x] = fixed::<1>(op, args, doc, vars)?;
            Ok(Bson::Boolean(!truthy(&x)))
        }
        "$cond" => op_cond(args, doc, vars),
        "$ifNull" => op_if_null(args, doc, vars),
        "$function" => crate::js::function_expr(args, doc, vars),
        "$switch" => op_switch(args, doc, vars),
        "$arrayElemAt" => op_array_elem_at(args, doc, vars),
        "$first" | "$last" => op_first_last(op, args, doc, vars),
        "$concatArrays" => op_concat_arrays(args, doc, vars),
        "$filter" => op_filter(args, doc, vars),
        "$map" => op_map(args, doc, vars),
        "$reduce" => op_reduce(args, doc, vars),
        "$in" => op_in(args, doc, vars),
        "$indexOfArray" => op_index_of_array(args, doc, vars),
        "$isArray" => {
            let [x] = fixed::<1>(op, args, doc, vars)?;
            Ok(Bson::Boolean(matches!(x, Bson::Array(_))))
        }
        "$range" => op_range(args, doc, vars),
        "$reverseArray" => op_reverse_array(args, doc, vars),
        "$size" => op_size(args, doc, vars),
        "$slice" => op_slice(args, doc, vars),
        "$zip" => op_zip(args, doc, vars),
        "$arrayToObject" => op_array_to_object(args, doc, vars),
        "$objectToArray" => op_object_to_array(args, doc, vars),
        "$sortArray" => op_sort_array(args, doc, vars),
        "$firstN" | "$lastN" | "$maxN" | "$minN" => op_n_elements(op, args, doc, vars),
        "$setEquals" | "$setIntersection" | "$setUnion" => op_set_n(op, args, doc, vars),
        "$setDifference" | "$setIsSubset" => op_set_two(op, args, doc, vars),
        "$anyElementTrue" | "$allElementsTrue" => op_any_all(op, args, doc, vars),
        "$mergeObjects" => op_merge_objects(args, doc, vars),
        "$getField" => op_get_field(args, doc, vars),
        "$setField" => op_set_field(args, doc, vars),
        "$unsetField" => op_unset_field(args, doc, vars),
        "$let" => op_let(args, doc, vars),
        "$literal" => Ok(args.clone()),
        "$type" => {
            let [x] = fixed::<1>(op, args, doc, vars)?;
            Ok(Bson::String(type_name(&x).to_string()))
        }
        "$subtype" => op_subtype(args, doc, vars),
        "$serializeEJSON" => op_serialize_ejson(args, doc, vars),
        "$deserializeEJSON" => op_deserialize_ejson(args, doc, vars),
        "$isNumber" => {
            let [x] = fixed::<1>(op, args, doc, vars)?;
            Ok(Bson::Boolean(is_number(&x)))
        }
        "$convert" => op_convert(args, doc, vars),
        "$toBool" => op_to(op, "bool", args, doc, vars),
        "$toInt" => op_to(op, "int", args, doc, vars),
        "$toLong" => op_to(op, "long", args, doc, vars),
        "$toDouble" => op_to(op, "double", args, doc, vars),
        "$toDecimal" => op_to(op, "decimal", args, doc, vars),
        "$toString" => op_to(op, "string", args, doc, vars),
        "$toObjectId" => op_to(op, "objectId", args, doc, vars),
        "$toDate" => op_to(op, "date", args, doc, vars),
        "$sum" | "$avg" => op_sum_avg(op, args, doc, vars),
        "$min" | "$max" => op_min_max(op, args, doc, vars),
        "$stdDevPop" | "$stdDevSamp" => op_std_dev(op, args, doc, vars),
        "$median" => op_median(args, doc, vars),
        "$percentile" => op_percentile(args, doc, vars),
        "$rand" => Ok(Bson::Double(rand::random::<f64>())),
        "$meta" => op_meta(args, doc),
        "$toHashedIndexKey" => op_hashed_key(args, doc, vars),
        "$binarySize" => op_binary_size(args, doc, vars),
        "$bsonSize" => op_bson_size(args, doc, vars),
        "$tsSecond" | "$tsIncrement" => op_ts_part(op, args, doc, vars),
        _ => match expr_ext::eval_ext(op, args, doc, vars)? {
            Some(v) => Ok(v),
            None => match crate::agg::expr_new::eval_new(op, args, doc, vars)? {
                Some(v) => Ok(v),
                None => Err(Error::new(168, format!("Unrecognized expression '{op}'"))),
            },
        },
    }
}

// ---------- arithmetic ----------

fn op_add(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = ev_all(args, doc, vars)?;
    if vals.iter().any(is_nullish) {
        return Ok(Bson::Null);
    }
    let mut date: Option<DateTime> = None;
    let mut sum = Bson::Int32(0);
    for v in &vals {
        match v {
            Bson::DateTime(d) => {
                if date.is_some() {
                    return Err(Error::new(16612, "only one date allowed in an $add expression"));
                }
                date = Some(*d);
            }
            _ if is_number(v) => {
                if let Some(s) = num_add(&sum, v) {
                    sum = s;
                }
            }
            _ => {
                return Err(Error::type_mismatch(format!(
                    "$add only supports numeric or date types, not {}",
                    type_name(v)
                )));
            }
        }
    }
    match date {
        None => Ok(sum),
        Some(d) => {
            let ms = d
                .timestamp_millis()
                .checked_add(millis_of(&sum))
                .ok_or_else(|| Error::bad_value("date overflow in $add"))?;
            Ok(Bson::DateTime(DateTime::from_millis(ms)))
        }
    }
}

fn op_subtract(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [a, b] = fixed::<2>("$subtract", args, doc, vars)?;
    if is_nullish(&a) || is_nullish(&b) {
        return Ok(Bson::Null);
    }
    let sub_err = || Error::type_mismatch(format!("cant $subtract a {} from a {}", type_name(&b), type_name(&a)));
    match (&a, &b) {
        (Bson::DateTime(x), Bson::DateTime(y)) => {
            Ok(Bson::Int64(x.timestamp_millis().saturating_sub(y.timestamp_millis())))
        }
        (Bson::DateTime(x), n) if is_number(n) => Ok(Bson::DateTime(DateTime::from_millis(
            x.timestamp_millis().saturating_sub(millis_of(n)),
        ))),
        _ if is_number(&a) && is_number(&b) => num_sub(&a, &b).ok_or_else(sub_err),
        _ => Err(sub_err()),
    }
}

fn op_multiply(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = ev_all(args, doc, vars)?;
    if vals.iter().any(is_nullish) {
        return Ok(Bson::Null);
    }
    let mut acc = Bson::Int32(1);
    for v in &vals {
        num_check(16555, "$multiply", v)?;
        if let Some(p) = num_mul(&acc, v) {
            acc = p;
        }
    }
    Ok(acc)
}

fn op_divide(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [a, b] = fixed::<2>("$divide", args, doc, vars)?;
    if is_nullish(&a) || is_nullish(&b) {
        return Ok(Bson::Null);
    }
    num_check(16609, "$divide", &a)?;
    num_check(16609, "$divide", &b)?;
    let (x, y) = (f64_of("$divide", &a)?, f64_of("$divide", &b)?);
    if y == 0.0 {
        return Err(Error::bad_value("can't $divide by zero"));
    }
    let r = x / y;
    if matches!(&a, Bson::Decimal128(_)) || matches!(&b, Bson::Decimal128(_)) {
        Ok(Bson::Decimal128(f64_to_decimal(r)))
    } else {
        Ok(Bson::Double(r))
    }
}

fn op_mod(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [a, b] = fixed::<2>("$mod", args, doc, vars)?;
    if is_nullish(&a) || is_nullish(&b) {
        return Ok(Bson::Null);
    }
    num_check(16611, "$mod", &a)?;
    num_check(16611, "$mod", &b)?;
    if f64_of("$mod", &b)? == 0.0 {
        return Err(Error::bad_value("can't $mod by zero"));
    }
    if let (Some(x), Some(y)) = (i64_of(&a), i64_of(&b)) {
        let r = x.wrapping_rem(y);
        let both_int32 = matches!((&a, &b), (Bson::Int32(_), Bson::Int32(_)));
        return Ok(match i32::try_from(r) {
            Ok(v) if both_int32 => Bson::Int32(v),
            _ => Bson::Int64(r),
        });
    }
    let (x, y) = (f64_of("$mod", &a)?, f64_of("$mod", &b)?);
    Ok(Bson::Double(x % y))
}

fn op_abs(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>("$abs", args, doc, vars)?;
    if is_nullish(&x) {
        return Ok(Bson::Null);
    }
    num_check(14, "$abs", &x)?;
    Ok(match &x {
        Bson::Int32(i) => match i.checked_abs() {
            Some(v) => Bson::Int32(v),
            None => Bson::Int64(i64::from(*i).abs()),
        },
        Bson::Int64(i) => match i.checked_abs() {
            Some(v) => Bson::Int64(v),
            None => Bson::Double((*i as f64).abs()),
        },
        Bson::Decimal128(d) => Bson::Decimal128(f64_to_decimal(decimal_to_f64(d).abs())),
        _ => Bson::Double(f64_of("$abs", &x)?.abs()),
    })
}

fn op_ceil_floor(op: &str, args: &Bson, doc: &Document, vars: &Vars, f: fn(f64) -> f64) -> Result<Bson> {
    let [x] = fixed::<1>(op, args, doc, vars)?;
    if is_nullish(&x) {
        return Ok(Bson::Null);
    }
    num_check(14, op, &x)?;
    Ok(match &x {
        Bson::Int32(_) | Bson::Int64(_) => x.clone(),
        Bson::Decimal128(d) => Bson::Decimal128(f64_to_decimal(f(decimal_to_f64(d)))),
        _ => Bson::Double(f(f64_of(op, &x)?)),
    })
}

fn op_round(op: &str, args: &Bson, doc: &Document, vars: &Vars, trunc: bool) -> Result<Bson> {
    let vals = ev_all(args, doc, vars)?;
    let (x, place) = match vals.as_slice() {
        [x] => (x, 0_i64),
        [x, p] => {
            if is_nullish(p) {
                return Ok(Bson::Null);
            }
            let pl = int_val(p).ok_or_else(|| Error::bad_value(format!("{op} requires place argument to be integral")))?;
            (x, pl)
        }
        _ => return Err(Error::bad_value(format!("Expression {op} takes 1 or 2 arguments."))),
    };
    if is_nullish(x) {
        return Ok(Bson::Null);
    }
    num_check(14, op, x)?;
    let place = place.clamp(-400, 400) as i32;
    if place >= 0 && matches!(x, Bson::Int32(_) | Bson::Int64(_)) {
        return Ok(x.clone());
    }
    let f = f64_of(op, x)?;
    let s = 10f64.powi(place);
    let scaled = f * s;
    // $round is round-half-to-even; $trunc truncates toward zero.
    let rounded = if trunc { scaled.trunc() } else { scaled.round_ties_even() };
    let r = rounded / s;
    Ok(match x {
        Bson::Int32(_) => match i32::try_from(r as i64) {
            Ok(v) => Bson::Int32(v),
            Err(_) => Bson::Int64(r as i64),
        },
        Bson::Int64(_) => Bson::Int64(r as i64),
        Bson::Decimal128(_) => Bson::Decimal128(f64_to_decimal(r)),
        _ => Bson::Double(r),
    })
}

fn math1(
    op: &str,
    args: &Bson,
    doc: &Document,
    vars: &Vars,
    f: fn(f64) -> Option<f64>,
    domain: &str,
) -> Result<Bson> {
    let [x] = fixed::<1>(op, args, doc, vars)?;
    if is_nullish(&x) {
        return Ok(Bson::Null);
    }
    let v = f64_of(op, &x)?;
    match f(v) {
        Some(r) => Ok(Bson::Double(r)),
        None => Err(Error::bad_value(format!("{op}'s argument must be {domain}"))),
    }
}

fn op_pow(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [b, e] = fixed::<2>("$pow", args, doc, vars)?;
    if is_nullish(&b) || is_nullish(&e) {
        return Ok(Bson::Null);
    }
    num_check(14, "$pow", &b)?;
    num_check(14, "$pow", &e)?;
    if let (Some(bi), Some(ei)) = (i64_of(&b), i64_of(&e)) {
        if ei >= 0 {
            if let Some(v) = u32::try_from(ei).ok().and_then(|ex| bi.checked_pow(ex)) {
                return Ok(int_bson(v));
            }
        }
    }
    let (bf, ef) = (f64_of("$pow", &b)?, f64_of("$pow", &e)?);
    if bf == 0.0 && ef < 0.0 {
        return Err(Error::bad_value("$pow cannot raise 0 to a negative power"));
    }
    Ok(Bson::Double(bf.powf(ef)))
}

fn op_log(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [n, b] = fixed::<2>("$log", args, doc, vars)?;
    if is_nullish(&n) || is_nullish(&b) {
        return Ok(Bson::Null);
    }
    num_check(14, "$log", &n)?;
    num_check(14, "$log", &b)?;
    let (nf, bf) = (f64_of("$log", &n)?, f64_of("$log", &b)?);
    if nf <= 0.0 {
        return Err(Error::bad_value("$log's argument must be a positive number"));
    }
    if bf <= 0.0 || bf == 1.0 {
        return Err(Error::bad_value("$log's base must be a positive number not equal to 1"));
    }
    Ok(Bson::Double(nf.log(bf)))
}

fn op_atan2(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [y, x] = fixed::<2>("$atan2", args, doc, vars)?;
    if is_nullish(&y) || is_nullish(&x) {
        return Ok(Bson::Null);
    }
    let (yf, xf) = (f64_of("$atan2", &y)?, f64_of("$atan2", &x)?);
    Ok(Bson::Double(yf.atan2(xf)))
}

// ---------- comparison & boolean ----------

fn op_compare(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [a, b] = fixed::<2>(op, args, doc, vars)?;
    let ord = cmp_bson(&a, &b);
    Ok(match op {
        "$cmp" => Bson::Int32(match ord {
            Ordering::Less => -1,
            Ordering::Equal => 0,
            Ordering::Greater => 1,
        }),
        "$eq" => Bson::Boolean(ord == Ordering::Equal),
        "$ne" => Bson::Boolean(ord != Ordering::Equal),
        "$gt" => Bson::Boolean(ord == Ordering::Greater),
        "$gte" => Bson::Boolean(ord != Ordering::Less),
        "$lt" => Bson::Boolean(ord == Ordering::Less),
        _ => Bson::Boolean(ord != Ordering::Greater), // $lte
    })
}

fn op_and_or(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let is_and = op == "$and";
    for e in items(args) {
        let t = truthy(&eval(e, doc, vars)?);
        if is_and && !t {
            return Ok(Bson::Boolean(false));
        }
        if !is_and && t {
            return Ok(Bson::Boolean(true));
        }
    }
    Ok(Bson::Boolean(is_and))
}

// ---------- conditionals ----------

fn op_cond(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let (c, t, f) = match args {
        Bson::Array(v) => match v.as_slice() {
            [c, t, f] => (c, t, f),
            _ => return Err(arity_err("$cond", 3, v.len())),
        },
        Bson::Document(d) => (
            get_arg(d, "$cond", "if")?,
            get_arg(d, "$cond", "then")?,
            get_arg(d, "$cond", "else")?,
        ),
        _ => return Err(arity_err("$cond", 3, 1)),
    };
    if truthy(&eval(c, doc, vars)?) { eval(t, doc, vars) } else { eval(f, doc, vars) }
}

fn op_if_null(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let list = items(args);
    let Some((last, rest)) = list.split_last() else {
        return Err(Error::bad_value("Expression $ifNull takes at least 2 arguments. 0 were passed in."));
    };
    if rest.is_empty() {
        return Err(Error::bad_value(format!(
            "Expression $ifNull takes at least 2 arguments. {} were passed in.",
            list.len()
        )));
    }
    for e in rest {
        let v = eval(e, doc, vars)?;
        if !is_nullish(&v) {
            return Ok(v);
        }
    }
    eval(last, doc, vars)
}

fn op_switch(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = match args {
        Bson::Document(d) => d,
        other => {
            return Err(Error::new(
                40060,
                format!("$switch requires an object as an argument, found: {}", type_name(other)),
            ));
        }
    };
    let branches = match d.get("branches") {
        Some(Bson::Array(b)) => b,
        _ => return Err(Error::new(40061, "$switch requires an array 'branches'")),
    };
    for br in branches {
        let Bson::Document(b) = br else {
            return Err(Error::new(40062, "$switch requires each branch to be an object"));
        };
        let case = b
            .get("case")
            .ok_or_else(|| Error::new(40067, "$switch requires each branch have a 'case' expression"))?;
        let then = b
            .get("then")
            .ok_or_else(|| Error::new(40068, "$switch requires each branch have a 'then' expression"))?;
        if truthy(&eval(case, doc, vars)?) {
            return eval(then, doc, vars);
        }
    }
    match d.get("default") {
        Some(e) => eval(e, doc, vars),
        None => Err(Error::new(
            40069,
            "$switch could not find a matching branch for an input, and no default was specified.",
        )),
    }
}

// ---------- arrays ----------

fn op_array_elem_at(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [arr, idx] = fixed::<2>("$arrayElemAt", args, doc, vars)?;
    if is_nullish(&arr) || is_nullish(&idx) {
        return Ok(Bson::Null);
    }
    let Bson::Array(a) = &arr else {
        return Err(Error::new(
            28689,
            format!("$arrayElemAt's first argument must be an array, but is {}", type_name(&arr)),
        ));
    };
    let i = int_val(&idx).ok_or_else(|| {
        Error::new(
            28690,
            format!("$arrayElemAt's second argument must be a numeric value, but is {}", type_name(&idx)),
        )
    })?;
    let len = a.len() as i64;
    let pos = if i < 0 { len + i } else { i };
    if pos < 0 {
        return Ok(MISSING);
    }
    Ok(a.get(pos as usize).cloned().unwrap_or(MISSING))
}

fn op_first_last(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>(op, args, doc, vars)?;
    if is_nullish(&x) {
        return Ok(Bson::Null);
    }
    let Bson::Array(a) = &x else {
        return Err(Error::new(
            16461,
            format!("{op}'s argument must be an array, but is {}", type_name(&x)),
        ));
    };
    let found = if op == "$first" { a.first() } else { a.last() };
    Ok(found.cloned().unwrap_or(MISSING))
}

fn op_concat_arrays(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = ev_all(args, doc, vars)?;
    if vals.iter().any(is_nullish) {
        return Ok(Bson::Null);
    }
    let mut out = Vec::new();
    for v in vals {
        match v {
            Bson::Array(a) => out.extend(a),
            other => {
                return Err(Error::type_mismatch(format!(
                    "$concatArrays only supports arrays, not {}",
                    type_name(&other)
                )));
            }
        }
    }
    Ok(Bson::Array(out))
}

fn op_filter(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$filter", args)?;
    check_args(d, &["input", "as", "cond", "limit", "arrayIndexAs"], |k| {
        Error::new(28647, format!("Unrecognized parameter to $filter: {k}"))
    })?;
    let input = eval(get_arg(d, "$filter", "input")?, doc, vars)?;
    if is_nullish(&input) {
        return Ok(Bson::Null);
    }
    let elems = match input {
        Bson::Array(a) => a,
        other => {
            return Err(Error::bad_value(format!("input to $filter must be an array not {}", type_name(&other))));
        }
    };
    let name = as_name(d)?;
    let (index, explicit) = index_name(d)?;
    if explicit && index == name {
        return Err(Error::new(9375802, "'as' and 'arrayIndexAs' cannot have the same name"));
    }
    let cond = get_arg(d, "$filter", "cond")?;
    let limit = match d.get("limit") {
        None => None,
        Some(e) => {
            let v = eval(e, doc, vars)?;
            if is_nullish(&v) {
                None
            } else {
                match int_val(&v) {
                    Some(n) if n > 0 => Some(n as usize),
                    _ => return Err(Error::bad_value("$filter: limit must be a positive integer")),
                }
            }
        }
    };
    let mut scope = vars.clone();
    let mut out = Vec::new();
    for (i, el) in elems.into_iter().enumerate() {
        if limit.is_some_and(|l| out.len() >= l) {
            break;
        }
        scope.insert(name.clone(), el.clone());
        scope.insert(index.clone(), index_val(i));
        if truthy(&eval(cond, doc, &scope)?) {
            out.push(el);
        }
    }
    Ok(Bson::Array(out))
}

fn op_map(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$map", args)?;
    check_args(d, &["input", "as", "in", "arrayIndexAs"], |k| {
        Error::new(16879, format!("Unrecognized parameter to $map: {k}"))
    })?;
    let input = eval(get_arg(d, "$map", "input")?, doc, vars)?;
    if is_nullish(&input) {
        return Ok(Bson::Null);
    }
    let elems = match input {
        Bson::Array(a) => a,
        other => return Err(Error::bad_value(format!("input to $map must be an array not {}", type_name(&other)))),
    };
    let name = as_name(d)?;
    let (index, explicit) = index_name(d)?;
    if explicit && index == name {
        return Err(Error::new(9375801, "'as' and 'arrayIndexAs' cannot have the same name"));
    }
    let body = get_arg(d, "$map", "in")?;
    let mut scope = vars.clone();
    let mut out = Vec::with_capacity(elems.len());
    for (i, el) in elems.into_iter().enumerate() {
        scope.insert(name.clone(), el);
        scope.insert(index.clone(), index_val(i));
        let r = eval(body, doc, &scope)?;
        out.push(if is_missing(&r) { Bson::Null } else { r });
    }
    Ok(Bson::Array(out))
}

fn op_reduce(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$reduce", args)?;
    check_args(d, &["input", "initialValue", "in", "arrayIndexAs"], |k| {
        Error::new(40076, format!("$reduce found an unknown argument: {k}"))
    })?;
    let input = eval(get_arg(d, "$reduce", "input")?, doc, vars)?;
    if is_nullish(&input) {
        return Ok(Bson::Null);
    }
    let elems = match input {
        Bson::Array(a) => a,
        other => {
            return Err(Error::new(
                40080,
                format!("$reduce requires that 'input' be an array, found: {}", shell_repr(&other)),
            ));
        }
    };
    let Some(init) = d.get("initialValue") else {
        return Err(Error::new(40078, "$reduce requires 'initialValue' to be specified"));
    };
    let (index, _) = index_name(d)?;
    let mut value = eval(init, doc, vars)?;
    let body = get_arg(d, "$reduce", "in")?;
    let mut scope = vars.clone();
    for (i, el) in elems.into_iter().enumerate() {
        let prev = std::mem::replace(&mut value, MISSING);
        scope.insert("value".to_string(), prev);
        scope.insert("this".to_string(), el);
        scope.insert(index.clone(), index_val(i));
        value = eval(body, doc, &scope)?;
    }
    Ok(value)
}

fn op_subtype(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>("$subtype", args, doc, vars)?;
    match x {
        Bson::Binary(b) => Ok(Bson::Int32(i32::from(u8::from(b.subtype)))),
        other if is_nullish(&other) => Ok(Bson::Null),
        other => Err(Error::new(
            10389300,
            format!("Provided value: {}, does not have a subtype, but $subtype was used.", shell_repr(&other)),
        )),
    }
}

fn op_in(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [needle, arr] = fixed::<2>("$in", args, doc, vars)?;
    match arr {
        Bson::Array(a) => Ok(Bson::Boolean(a.iter().any(|x| bson_eq(x, &needle)))),
        other => Err(Error::new(
            40081,
            format!("$in requires an array as a second argument, found: {}", type_name(&other)),
        )),
    }
}

fn op_index_of_array(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = ev_all(args, doc, vars)?;
    let (arr, needle, start, end) = match vals.as_slice() {
        [a, n] => (a, n, None, None),
        [a, n, s] => (a, n, Some(s), None),
        [a, n, s, e] => (a, n, Some(s), Some(e)),
        _ => return Err(Error::bad_value("Expression $indexOfArray takes between 2 and 4 arguments.")),
    };
    if is_nullish(arr) {
        return Ok(Bson::Null);
    }
    let Bson::Array(a) = arr else {
        return Err(Error::type_mismatch(format!(
            "$indexOfArray requires an array as a first argument, found: {}",
            type_name(arr)
        )));
    };
    let index_arg = |v: &Bson| -> Result<usize> {
        match int_val(v) {
            Some(i) if i >= 0 => Ok(i as usize),
            _ => Err(Error::bad_value("$indexOfArray requires a non-negative integral index")),
        }
    };
    let s = match start {
        None => 0,
        Some(v) => index_arg(v)?,
    };
    let e = match end {
        None => a.len(),
        Some(v) => index_arg(v)?,
    };
    for (i, x) in a.iter().enumerate().take(e).skip(s) {
        if bson_eq(x, needle) {
            return Ok(Bson::Int32(i as i32));
        }
    }
    Ok(Bson::Int32(-1))
}

fn op_range(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = ev_all(args, doc, vars)?;
    let (s, e, step) = match vals.as_slice() {
        [s, e] => (s, e, None),
        [s, e, st] => (s, e, Some(st)),
        _ => return Err(Error::bad_value("Expression $range takes 2 or 3 arguments.")),
    };
    let start = int_val(s).ok_or_else(|| Error::new(34443, "$range requires a numeric starting value"))?;
    let end = int_val(e).ok_or_else(|| Error::new(34444, "$range requires a numeric ending value"))?;
    let step = match step {
        None => 1,
        Some(v) => int_val(v).ok_or_else(|| Error::new(34445, "$range requires a numeric step value"))?,
    };
    if step == 0 {
        return Err(Error::new(34447, "$range requires a non-zero step value"));
    }
    let mut out = Vec::new();
    let mut cur = start;
    while (step > 0 && cur < end) || (step < 0 && cur > end) {
        if out.len() >= RANGE_MAX {
            return Err(Error::bad_value("$range would produce too many elements"));
        }
        out.push(int_bson(cur));
        cur = match cur.checked_add(step) {
            Some(c) => c,
            None => break,
        };
    }
    Ok(Bson::Array(out))
}

fn op_reverse_array(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>("$reverseArray", args, doc, vars)?;
    match x {
        Bson::Array(mut a) => {
            a.reverse();
            Ok(Bson::Array(a))
        }
        other if is_nullish(&other) => Ok(Bson::Null),
        other => Err(Error::new(
            34435,
            format!("The argument to $reverseArray must be an array, but was of type: {}", type_name(&other)),
        )),
    }
}

fn op_size(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>("$size", args, doc, vars)?;
    match x {
        Bson::Array(a) => Ok(Bson::Int32(a.len() as i32)),
        other => Err(Error::new(
            17124,
            format!("The argument to $size must be an array, but was of type: {}", type_name(&other)),
        )),
    }
}

fn op_slice(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = ev_all(args, doc, vars)?;
    let (arr, pos, n) = match vals.as_slice() {
        [a, n] => (a, None, n),
        [a, p, n] => (a, Some(p), n),
        _ => return Err(Error::bad_value("Expression $slice takes 2 or 3 arguments.")),
    };
    if is_nullish(arr) {
        return Ok(Bson::Null);
    }
    let Bson::Array(a) = arr else {
        return Err(Error::new(
            28724,
            format!("First argument to $slice must be an array, but is of type: {}", type_name(arr)),
        ));
    };
    let len = a.len();
    match pos {
        None => {
            let n = int_val(n).ok_or_else(|| Error::bad_value("Second argument to $slice must be a numeric value"))?;
            if n == 0 {
                return Err(Error::bad_value("Second argument to $slice must be a non-zero integer"));
            }
            if n > 0 {
                Ok(Bson::Array(a.iter().take(n as usize).cloned().collect()))
            } else {
                let k = n.unsigned_abs() as usize;
                Ok(Bson::Array(a.get(len.saturating_sub(k)..).unwrap_or(&[]).to_vec()))
            }
        }
        Some(p) => {
            let pos = int_val(p).ok_or_else(|| Error::bad_value("Second argument to $slice must be a numeric value"))?;
            let n = int_val(n).ok_or_else(|| Error::bad_value("Third argument to $slice must be a numeric value"))?;
            if n <= 0 {
                return Err(Error::bad_value("Third argument to $slice must be positive"));
            }
            let len_i = len as i64;
            let start = if pos < 0 { (len_i + pos).max(0) } else { pos.min(len_i) } as usize;
            Ok(Bson::Array(a.iter().skip(start).take(n as usize).cloned().collect()))
        }
    }
}

fn op_zip(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$zip", args)?;
    let inputs = eval(get_arg(d, "$zip", "inputs")?, doc, vars)?;
    let list = match inputs {
        Bson::Array(l) => l,
        _ => return Err(Error::bad_value("$zip requires 'inputs' to be an array")),
    };
    if list.iter().any(is_nullish) {
        return Ok(Bson::Null);
    }
    let longest = match d.get("useLongestLength") {
        None => false,
        Some(e) => truthy(&eval(e, doc, vars)?),
    };
    let defaults = match d.get("defaults") {
        None => None,
        Some(e) => match eval(e, doc, vars)? {
            Bson::Array(v) => Some(v),
            other if is_nullish(&other) => None,
            _ => return Err(Error::bad_value("$zip requires 'defaults' to be an array")),
        },
    };
    let mut arrays: Vec<Vec<Bson>> = Vec::with_capacity(list.len());
    for v in list {
        match v {
            Bson::Array(a) => arrays.push(a),
            other => {
                return Err(Error::bad_value(format!(
                    "$zip found a non-array argument of type {}",
                    type_name(&other)
                )));
            }
        }
    }
    let lens = arrays.iter().map(|a| a.len());
    let n = if longest { lens.max().unwrap_or(0) } else { lens.min().unwrap_or(0) };
    let defaults = defaults.unwrap_or_else(|| vec![Bson::Null; arrays.len()]);
    if longest && defaults.len() != arrays.len() {
        return Err(Error::bad_value("$zip defaults must have the same number of elements as inputs"));
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let row: Vec<Bson> = arrays
            .iter()
            .enumerate()
            .map(|(j, a)| {
                a.get(i)
                    .cloned()
                    .unwrap_or_else(|| defaults.get(j).cloned().unwrap_or(Bson::Null))
            })
            .collect();
        out.push(Bson::Array(row));
    }
    Ok(Bson::Array(out))
}

fn kv_pair(e: &Bson) -> Option<(&Bson, &Bson)> {
    match e {
        Bson::Array(p) if p.len() == 2 => Some((p.first()?, p.get(1)?)),
        Bson::Document(d) if d.len() == 2 => Some((d.get("k")?, d.get("v")?)),
        _ => None,
    }
}

fn op_array_to_object(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>("$arrayToObject", args, doc, vars)?;
    if is_nullish(&x) {
        return Ok(Bson::Null);
    }
    let Bson::Array(a) = &x else {
        return Err(Error::new(
            40386,
            format!("$arrayToObject requires an array input, found: {}", type_name(&x)),
        ));
    };
    let mut out = Document::new();
    for e in a {
        let Some((k, v)) = kv_pair(e) else {
            return Err(Error::new(
                40397,
                "$arrayToObject requires an array of key-value pairs, where the key is a string",
            ));
        };
        let Bson::String(key) = k else {
            return Err(Error::new(40392, "$arrayToObject requires string keys"));
        };
        out.insert(key.clone(), v.clone());
    }
    Ok(Bson::Document(out))
}

fn op_object_to_array(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>("$objectToArray", args, doc, vars)?;
    match &x {
        Bson::Document(d) => Ok(Bson::Array(
            d.iter()
                .map(|(k, v)| {
                    let mut e = Document::new();
                    e.insert("k", k.clone());
                    e.insert("v", v.clone());
                    Bson::Document(e)
                })
                .collect(),
        )),
        other if is_nullish(other) => Ok(Bson::Null),
        other => Err(Error::new(
            40390,
            format!("$objectToArray requires a document input, found: {}", type_name(other)),
        )),
    }
}

fn op_sort_array(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$sortArray", args)?;
    let input = eval(get_arg(d, "$sortArray", "input")?, doc, vars)?;
    let sort_by = get_arg(d, "$sortArray", "sortBy")?;
    if is_nullish(&input) {
        return Ok(Bson::Null);
    }
    let mut arr = match input {
        Bson::Array(a) => a,
        other => {
            return Err(Error::bad_value(format!(
                "$sortArray input argument must be an array but is {}",
                type_name(&other)
            )));
        }
    };
    match sort_by {
        Bson::Document(spec) => {
            let mut keys: Vec<(String, bool)> = Vec::new();
            for (k, dir) in spec {
                let asc = match int_val(dir) {
                    Some(1) => true,
                    Some(-1) => false,
                    _ => {
                        return Err(Error::bad_value(
                            "$sort key ordering must be 1 (for ascending) or -1 (for descending)",
                        ));
                    }
                };
                keys.push((k.clone(), asc));
            }
            if keys.is_empty() {
                return Err(Error::bad_value("$sortArray requires sortBy to specify at least one field"));
            }
            arr.sort_by(|a, b| {
                for (path, asc) in &keys {
                    let undef = MISSING;
                    let va = get_path_value(a, path).unwrap_or(&undef);
                    let vb = get_path_value(b, path).unwrap_or(&undef);
                    let o = cmp_bson(va, vb);
                    let o = if *asc { o } else { o.reverse() };
                    if o != Ordering::Equal {
                        return o;
                    }
                }
                Ordering::Equal
            });
        }
        other => match int_val(other) {
            Some(1) => arr.sort_by(cmp_bson),
            Some(-1) => arr.sort_by(|a, b| cmp_bson(b, a)),
            _ => return Err(Error::bad_value("$sortArray requires sortBy to be 1, -1, or a sort document")),
        },
    }
    Ok(Bson::Array(arr))
}

fn op_n_elements(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg(op, args)?;
    let input = eval(get_arg(d, op, "input")?, doc, vars)?;
    let n_val = eval(get_arg(d, op, "n")?, doc, vars)?;
    if is_nullish(&input) {
        return Ok(Bson::Null);
    }
    let n = match int_val(&n_val) {
        Some(n) if n > 0 => n as usize,
        _ => return Err(Error::bad_value(format!("{op} requires 'n' to be a positive integer"))),
    };
    let mut arr = match input {
        Bson::Array(a) => a,
        other => {
            return Err(Error::bad_value(format!("{op} requires an array input, found: {}", type_name(&other))));
        }
    };
    match op {
        "$firstN" => arr.truncate(n),
        "$lastN" => {
            let skip = arr.len().saturating_sub(n);
            arr.drain(..skip);
        }
        _ => {
            arr.retain(|x| !is_nullish(x));
            arr.sort_by(cmp_bson);
            if op == "$maxN" {
                arr.reverse();
            }
            arr.truncate(n);
        }
    }
    Ok(Bson::Array(arr))
}

// ---------- sets ----------

fn arr_operand<'a>(op: &str, v: &'a Bson) -> Result<&'a [Bson]> {
    match v {
        Bson::Array(a) => Ok(a.as_slice()),
        _ => Err(Error::new(17044, format!("All operands of {op} must be arrays."))),
    }
}

fn contains(set: &[Bson], x: &Bson) -> bool {
    set.iter().any(|y| bson_eq(x, y))
}

fn dedupe(v: &[Bson]) -> Vec<Bson> {
    let mut out: Vec<Bson> = Vec::new();
    for x in v {
        if !contains(&out, x) {
            out.push(x.clone());
        }
    }
    out
}

fn set_eq(a: &[Bson], b: &[Bson]) -> bool {
    a.iter().all(|x| contains(b, x)) && b.iter().all(|x| contains(a, x))
}

fn op_set_n(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = ev_all(args, doc, vars)?;
    if op != "$setEquals" && vals.iter().any(is_nullish) {
        return Ok(Bson::Null);
    }
    let sets: Vec<&[Bson]> = vals.iter().map(|v| arr_operand(op, v)).collect::<Result<Vec<_>>>()?;
    match op {
        "$setEquals" => {
            let Some((first, rest)) = sets.split_first() else {
                return Ok(Bson::Boolean(true));
            };
            Ok(Bson::Boolean(rest.iter().all(|s| set_eq(first, s))))
        }
        "$setIntersection" => {
            let Some((first, rest)) = sets.split_first() else {
                return Ok(Bson::Array(Vec::new()));
            };
            let out: Vec<Bson> = dedupe(first).into_iter().filter(|x| rest.iter().all(|s| contains(s, x))).collect();
            Ok(Bson::Array(out))
        }
        _ => {
            let all: Vec<Bson> = sets.iter().flat_map(|s| s.iter().cloned()).collect();
            Ok(Bson::Array(dedupe(&all)))
        }
    }
}

fn op_set_two(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [a, b] = fixed::<2>(op, args, doc, vars)?;
    if op == "$setDifference" && (is_nullish(&a) || is_nullish(&b)) {
        return Ok(Bson::Null);
    }
    let a = arr_operand(op, &a)?;
    let b = arr_operand(op, &b)?;
    if op == "$setDifference" {
        Ok(Bson::Array(dedupe(a).into_iter().filter(|x| !contains(b, x)).collect()))
    } else {
        Ok(Bson::Boolean(a.iter().all(|x| contains(b, x))))
    }
}

fn op_any_all(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>(op, args, doc, vars)?;
    let Bson::Array(a) = &x else {
        return Err(Error::type_mismatch(format!(
            "{op}'s argument must be an array, but is {}",
            type_name(&x)
        )));
    };
    Ok(Bson::Boolean(if op == "$anyElementTrue" {
        a.iter().any(truthy)
    } else {
        a.iter().all(truthy)
    }))
}

// ---------- objects & variables ----------

fn op_merge_objects(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let mut out = Document::new();
    for v in ev_all(args, doc, vars)? {
        if is_nullish(&v) {
            continue;
        }
        match v {
            Bson::Document(d) => {
                for (k, x) in d {
                    out.insert(k, x);
                }
            }
            other => {
                return Err(Error::new(
                    40400,
                    format!("$mergeObjects requires object inputs, but input {:?} is of type {}", other, type_name(&other)),
                ));
            }
        }
    }
    Ok(Bson::Document(out))
}

fn op_get_field(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let (field, input) = match args {
        Bson::String(f) => (f.clone(), Bson::Document(doc.clone())),
        Bson::Document(d) => {
            let f = match get_arg(d, "$getField", "field")? {
                Bson::String(s) => s.clone(),
                _ => return Err(Error::bad_value("$getField requires 'field' to be a string")),
            };
            let input = match d.get("input") {
                Some(e) => eval(e, doc, vars)?,
                None => Bson::Document(doc.clone()),
            };
            (f, input)
        }
        _ => return Err(Error::bad_value("$getField requires an object or string argument")),
    };
    match input {
        Bson::Document(d) => Ok(d.get(field.as_str()).cloned().unwrap_or(MISSING)),
        other if is_nullish(&other) => Ok(Bson::Null),
        other => Err(Error::bad_value(format!("$getField requires 'input' to be an object, found {}", type_name(&other)))),
    }
}

fn op_set_field(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$setField", args)?;
    let field = str_field("$setField", d)?;
    let input = eval(get_arg(d, "$setField", "input")?, doc, vars)?;
    let value = eval(get_arg(d, "$setField", "value")?, doc, vars)?;
    match input {
        Bson::Document(mut out) => {
            if is_missing(&value) {
                out.remove(field.as_str());
            } else {
                out.insert(field, value);
            }
            Ok(Bson::Document(out))
        }
        other if is_nullish(&other) => Ok(Bson::Null),
        other => Err(Error::bad_value(format!("$setField requires 'input' to be an object, found {}", type_name(&other)))),
    }
}

fn op_unset_field(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$unsetField", args)?;
    let field = str_field("$unsetField", d)?;
    let input = eval(get_arg(d, "$unsetField", "input")?, doc, vars)?;
    match input {
        Bson::Document(mut out) => {
            out.remove(field.as_str());
            Ok(Bson::Document(out))
        }
        other if is_nullish(&other) => Ok(Bson::Null),
        other => Err(Error::bad_value(format!("$unsetField requires 'input' to be an object, found {}", type_name(&other)))),
    }
}

fn op_let(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$let", args)?;
    let bindings = match get_arg(d, "$let", "vars")? {
        Bson::Document(b) => b,
        _ => return Err(Error::bad_value("$let requires 'vars' to be an object")),
    };
    let mut scope = vars.clone();
    for (k, e) in bindings {
        let v = eval(e, doc, vars)?;
        scope.insert(k.clone(), v);
    }
    eval(get_arg(d, "$let", "in")?, doc, &scope)
}

// ---------- type conversion ----------

fn conv_fail(v: &Bson, alias: &str) -> Error {
    Error::new(
        241,
        format!("Unsupported conversion from {} to {} in $convert with no onError value", type_name(v), alias),
    )
}

fn parse_fail(noun: &str, s: &str, why: &str) -> Error {
    Error::new(
        241,
        format!("Failed to parse {noun} '{s}' in $convert with no onError value. Full error: {why}"),
    )
}

fn overflow_fail() -> Error {
    Error::new(241, "Conversion would overflow target type in $convert with no onError value")
}

fn to_alias(to: &Bson) -> Result<&'static str> {
    if let Bson::String(s) = to {
        return ["double", "string", "objectId", "bool", "date", "int", "long", "decimal", "binData", "array"]
            .into_iter()
            .find(|a| *a == s.as_str())
            .ok_or_else(|| match s.as_str() {
                "null" | "missing" | "undefined" | "minKey" | "maxKey" | "regex" | "javascript" | "timestamp"
                | "symbol" | "object" => Error::new(241, format!("Unsupported conversion target type: {s}")),
                _ => Error::bad_value(format!("Unsupported conversion target type: {s}")),
            });
    }
    let n = int_val(to).ok_or_else(|| Error::new(9, "$convert 'to' must be a type name or type number"))?;
    Ok(match n {
        1 => "double",
        2 => "string",
        5 => "binData",
        7 => "objectId",
        8 => "bool",
        9 => "date",
        16 => "int",
        18 => "long",
        19 => "decimal",
        20 | 22 | 23 | 255 => return Err(Error::new(9, format!("Unsupported conversion target type number: {n}"))),
        _ => return Err(Error::new(241, format!("Unsupported conversion target type number: {n}"))),
    })
}

fn f64_to_i64(d: f64) -> Option<i64> {
    if d.is_finite() && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&d) {
        Some(d.trunc() as i64)
    } else {
        None
    }
}

/// Decimal to integer by exact decimal-string truncation: out-of-range, NaN and Infinity fail.
fn dec_to_i64(d: &Decimal128) -> Result<i64> {
    dec_trunc(&d.to_string()).and_then(|t| i64::try_from(t).ok()).ok_or_else(overflow_fail)
}

/// Integer part of a decimal string such as `-1.5E+3`; `None` for NaN, Infinity or huge values.
fn dec_trunc(s: &str) -> Option<i128> {
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (mant, exp) = match rest.find(['E', 'e']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (ip, fp) = mant.split_once('.').unwrap_or((mant, ""));
    if (ip.is_empty() && fp.is_empty()) || !ip.bytes().chain(fp.bytes()).all(|c| c.is_ascii_digit()) {
        return None;
    }
    let point = ip.len() as i64 + exp;
    if point > 40 {
        return None;
    }
    let digits = format!("{ip}{fp}");
    let int_part = if point <= 0 {
        String::new()
    } else {
        let p = point as usize;
        if p <= digits.len() { digits[..p].to_string() } else { format!("{digits}{}", "0".repeat(p - digits.len())) }
    };
    let t = int_part.trim_start_matches('0');
    if t.is_empty() {
        return Some(0);
    }
    let mag: i128 = t.parse().ok()?;
    Some(if neg { -mag } else { mag })
}

fn to_i64(v: &Bson, alias: &str, base: Option<u32>) -> Result<i64> {
    Ok(match v {
        Bson::Int32(i) => i64::from(*i),
        Bson::Int64(i) => *i,
        Bson::Double(d) => f64_to_i64(*d).ok_or_else(overflow_fail)?,
        Bson::Decimal128(d) => dec_to_i64(d)?,
        Bson::Boolean(b) => i64::from(*b),
        Bson::DateTime(d) => d.timestamp_millis(),
        Bson::String(s) => i64::from_str_radix(s, base.unwrap_or(10))
            .map_err(|e| parse_fail("number", s, &e.to_string()))?,
        _ => return Err(conv_fail(v, alias)),
    })
}

fn to_decimal(v: &Bson, alias: &str) -> Result<Decimal128> {
    match v {
        Bson::Decimal128(d) => Ok(d.clone()),
        Bson::Double(d) => Ok(double_to_decimal(*d)),
        Bson::Int32(_) | Bson::Int64(_) | Bson::Boolean(_) | Bson::DateTime(_) => {
            let s = to_i64(v, alias, None)?.to_string();
            s.parse::<Decimal128>().map_err(|e| parse_fail("number", &s, &format!("{e:?}")))
        }
        Bson::String(s) => s
            .parse::<Decimal128>()
            .map_err(|e| parse_fail("number", s, &format!("{e:?}"))),
        _ => Err(conv_fail(v, alias)),
    }
}

/// Double to Decimal128 as `$convert` produces it: integral values below 1e15 are exact; other finite values
/// keep 15 significant digits in Decimal128's canonical string form.
fn double_to_decimal(d: f64) -> Decimal128 {
    if !d.is_finite() {
        return f64_to_decimal(d);
    }
    if d.fract() == 0.0 && d.abs() < 1e15 {
        return format!("{}", d as i64).parse().unwrap_or_else(|_| f64_to_decimal(d));
    }
    let sci = format!("{:.14e}", d.abs());
    let (mant, exp) = sci.split_once('e').unwrap_or((sci.as_str(), "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let digits: String = mant.chars().filter(|c| c.is_ascii_digit()).collect();
    let sign = if d < 0.0 { "-" } else { "" };
    let s = if exp > 14 || exp < -6 {
        format!("{sign}{}.{}E{}{}", &digits[..1], &digits[1..], if exp < 0 { "-" } else { "+" }, exp.abs())
    } else {
        format!("{sign}{:.*}", (14 - exp) as usize, d.abs())
    };
    s.parse().unwrap_or_else(|_| f64_to_decimal(d))
}

fn to_date(v: &Bson, alias: &str) -> Result<DateTime> {
    match v {
        Bson::DateTime(d) => Ok(*d),
        Bson::String(s) => parse_iso_date(s).map(DateTime::from_millis).ok_or_else(|| {
            Error::new(241, format!("Failed to parse date '{s}' in $convert with no onError value"))
        }),
        Bson::Int64(_) | Bson::Double(_) | Bson::Decimal128(_) => {
            Ok(DateTime::from_millis(to_i64(v, alias, None)?))
        }
        Bson::ObjectId(o) => Ok(o.timestamp()),
        Bson::Timestamp(t) => Ok(DateTime::from_millis(i64::from(t.time) * 1000)),
        _ => Err(conv_fail(v, alias)),
    }
}

fn to_string_val(v: &Bson, alias: &str) -> Result<String> {
    Ok(match v {
        Bson::String(s) => s.clone(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(d) => fmt_double(*d),
        Bson::Decimal128(d) => d.to_string(),
        Bson::Boolean(b) => b.to_string(),
        Bson::ObjectId(o) => o.to_hex(),
        Bson::DateTime(d) => fmt_iso(d.timestamp_millis()),
        Bson::MinKey => "MinKey".into(),
        Bson::MaxKey => "MaxKey".into(),
        Bson::Timestamp(t) => format!("Timestamp({}, {})", t.time, t.increment),
        Bson::Binary(b) if b.subtype == bson::spec::BinarySubtype::Uuid && b.bytes.len() == 16 => fmt_uuid(&b.bytes),
        Bson::Binary(b) => base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &b.bytes),
        Bson::RegularExpression(r) => format!("/{}/{}", r.pattern, r.options),
        Bson::JavaScriptCode(c) => c.clone(),
        Bson::Symbol(s) => s.clone(),
        Bson::Document(_) | Bson::Array(_) => {
            let mut out = String::new();
            json_value(v, &mut out);
            out
        }
        _ => return Err(conv_fail(v, alias)),
    })
}

/// MongoDB's double rendering: shortest round-trip digits, printf-`%g` style placement
/// (exponent form below 1e-4 or from 1e17 on, exponent signed with at least two digits).
fn fmt_double(d: f64) -> String {
    if d.is_nan() {
        return "NaN".into();
    }
    if d.is_infinite() {
        return if d > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    if d == 0.0 {
        return if d.is_sign_negative() { "-0".into() } else { "0".into() };
    }
    let e = format!("{d:e}");
    let (mant, exp) = e.split_once('e').unwrap_or((&e, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    if !(-4..17).contains(&exp) {
        format!("{mant}e{}{:02}", if exp < 0 { '-' } else { '+' }, exp.abs())
    } else {
        format!("{d}")
    }
}

fn fmt_uuid(b: &[u8]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
}

fn json_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Compact JSON used by `$toString` on objects and arrays: numbers, booleans and null
/// are bare (including longs and decimals); every other type is its `$toString` text.
fn json_value(v: &Bson, out: &mut String) {
    match v {
        Bson::Document(d) => {
            out.push('{');
            for (i, (k, x)) in d.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_str(k, out);
                out.push(':');
                json_value(x, out);
            }
            out.push('}');
        }
        Bson::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_value(x, out);
            }
            out.push(']');
        }
        Bson::Null | Bson::Undefined => out.push_str("null"),
        Bson::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        Bson::Int32(i) => out.push_str(&i.to_string()),
        Bson::Int64(i) => out.push_str(&i.to_string()),
        Bson::Decimal128(d) => out.push_str(&d.to_string()),
        Bson::Double(d) if d.is_finite() => out.push_str(&fmt_double(*d)),
        other => json_str(&to_string_val(other, "string").unwrap_or_default(), out),
    }
}

/// `$convert` options: `format`, `byteOrder`, `base`; `subtype` comes from the `to` object.
#[derive(Default)]
struct ConvOpts {
    format: Option<Bson>,
    big: bool,
    base: Option<u32>,
    subtype: u8,
}

fn conv_opts(d: &Document, doc: &Document, vars: &Vars) -> Result<ConvOpts> {
    let mut o = ConvOpts::default();
    if let Some(e) = d.get("format") {
        let f = eval(e, doc, vars)?;
        if !is_nullish(&f) {
            o.format = Some(f);
        }
    }
    if let Some(e) = d.get("byteOrder") {
        let b = eval(e, doc, vars)?;
        o.big = match &b {
            Bson::String(s) if s == "big" => true,
            Bson::String(s) if s == "little" => false,
            Bson::String(s) => {
                return Err(Error::new(9130002, format!("Invalid byteOrder '{s}' in $convert. Expected 'big' or 'little'.")));
            }
            x if is_nullish(x) => false,
            _ => return Err(Error::new(9130001, "$convert 'byteOrder' must be a string")),
        };
    }
    if let Some(e) = d.get("base") {
        let b = eval(e, doc, vars)?;
        o.base = match &b {
            x if is_nullish(x) => None,
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => {
                let n = match &b {
                    Bson::Int32(i) => i64::from(*i),
                    Bson::Int64(i) => *i,
                    Bson::Double(f) if f.is_finite() && f.fract() == 0.0 => *f as i64,
                    _ => return Err(Error::new(3501300, "$convert 'base' must be an integer")),
                };
                match n {
                    2 | 8 | 10 | 16 => Some(n as u32),
                    _ => return Err(Error::new(3501301, format!("Invalid base {n} for $convert. Must be 2, 8, 10 or 16."))),
                }
            }
            _ => return Err(Error::new(3501300, "$convert 'base' must be an integer")),
        };
    }
    Ok(o)
}

fn subtype_of(s: &Bson) -> Result<u8> {
    match s {
        Bson::Int32(i) if (0..=255).contains(i) => Ok(*i as u8),
        Bson::Int64(i) if (0..=255).contains(i) => Ok(*i as u8),
        _ => Err(Error::new(4341115, "$convert 'subtype' must be an integer between 0 and 255")),
    }
}

/// `to` is a type name, a type number, or an object `{type, subtype}`.
fn convert_target(to: &Bson) -> Result<(&'static str, Option<&Bson>)> {
    match to {
        Bson::Document(td) => {
            let t = td.get("type").ok_or_else(|| Error::new(9, "$convert 'to' object requires a 'type' field"))?;
            Ok((to_alias(t)?, td.get("subtype")))
        }
        Bson::Array(_) => Err(Error::new(9, "$convert 'to' must be a type name, type number, or object")),
        _ => Ok((to_alias(to)?, None)),
    }
}

fn format_of(o: &ConvOpts) -> Result<&str> {
    match &o.format {
        None => Err(Error::new(4341115, "$convert requires a 'format' for this conversion")),
        Some(Bson::String(s)) => Ok(s),
        Some(_) => Err(Error::new(4341114, "$convert 'format' must be a string")),
    }
}

fn bad_format(fmt: &str) -> Error {
    Error::new(4341125, format!("Unrecognized format '{fmt}' in $convert"))
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 || !s.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    s.as_bytes()
        .chunks(2)
        .map(|c| std::str::from_utf8(c).ok().and_then(|t| u8::from_str_radix(t, 16).ok()))
        .collect()
}

fn decode_bin_str(s: &str, fmt: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
    let fail = |why: &str| {
        Error::new(241, format!("Failed to parse {fmt} '{s}' in $convert with no onError value. Full error: {why}"))
    };
    Ok(match fmt {
        "base64" => STANDARD.decode(s).map_err(|e| fail(&e.to_string()))?,
        "base64url" => URL_SAFE_NO_PAD.decode(s).map_err(|e| fail(&e.to_string()))?,
        "hex" => unhex(s).ok_or_else(|| fail("invalid hex digits or length"))?,
        "uuid" => return Err(fail("uuid strings are not supported")),
        "auto" => return Err(fail("format 'auto' cannot decode strings")),
        "utf8" => s.as_bytes().to_vec(),
        _ => return Err(bad_format(fmt)),
    })
}

fn encode_bin(b: &bson::Binary, fmt: &str) -> Result<String> {
    use base64::Engine;
    use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
    let uuid = u8::from(b.subtype) == 4 && b.bytes.len() == 16;
    Ok(match fmt {
        "auto" if uuid => fmt_uuid(&b.bytes),
        "auto" | "base64" => STANDARD.encode(&b.bytes),
        "base64url" => URL_SAFE_NO_PAD.encode(&b.bytes),
        "hex" => b.bytes.iter().map(|x| format!("{x:02x}")).collect(),
        "uuid" if uuid => fmt_uuid(&b.bytes),
        "uuid" => return Err(Error::new(241, "Failed to convert binData to uuid string in $convert with no onError value")),
        "utf8" => String::from_utf8(b.bytes.clone())
            .map_err(|_| Error::new(241, "Failed to convert binData to utf8 string in $convert with no onError value"))?,
        _ => return Err(bad_format(fmt)),
    })
}

fn to_bin(v: &Bson, o: &ConvOpts) -> Result<Bson> {
    let bytes = match v {
        Bson::Binary(b) if u8::from(b.subtype) == o.subtype => return Ok(Bson::Binary(b.clone())),
        Bson::Binary(_) => return Err(Error::new(241, "Conversions between different BinData subtypes are not supported")),
        Bson::String(s) => {
            let fmt = format_of(o)?;
            let bytes = decode_bin_str(s, fmt)?;
            return Ok(Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::from(o.subtype), bytes }));
        }
        Bson::Int32(i) => if o.big { i.to_be_bytes().to_vec() } else { i.to_le_bytes().to_vec() },
        Bson::Int64(i) => if o.big { i.to_be_bytes().to_vec() } else { i.to_le_bytes().to_vec() },
        Bson::Double(d) => {
            let bits = d.to_bits();
            if o.big { bits.to_be_bytes().to_vec() } else { bits.to_le_bytes().to_vec() }
        }
        Bson::Document(d) => bson::to_vec(d).map_err(|_| conv_fail(v, "binData"))?,
        Bson::Array(items) => return vector_bin(items, o.big),
        _ => return Err(conv_fail(v, "binData")),
    };
    Ok(Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::from(o.subtype), bytes }))
}

/// Vector BinData (subtype 9) from an array: packed bits, int8 or float32 by element content.
fn vector_bin(items: &[Bson], big: bool) -> Result<Bson> {
    // (value, bit-eligible, integral, Int32 literal)
    let mut vals: Vec<(f64, bool, bool, bool)> = Vec::with_capacity(items.len());
    for it in items {
        vals.push(match it {
            Bson::Boolean(b) => (f64::from(u8::from(*b)), true, true, false),
            Bson::Int32(i) => (f64::from(*i), (0..=1).contains(i), true, true),
            Bson::Double(d) => (*d, *d == 1.0 || (*d == 0.0 && d.is_sign_positive()), d.is_finite() && d.fract() == 0.0, false),
            _ => return Err(Error::new(241, "Converting array to BinData requires an array with numeric or bool content")),
        });
    }
    let in_i8 = |x: f64| (-128.0..=127.0).contains(&x);
    let (dtype, padding, payload): (u8, u8, Vec<u8>) = if vals.iter().all(|e| e.1) {
        let mut bits = vec![0u8; vals.len().div_ceil(8)];
        for (i, e) in vals.iter().enumerate() {
            if e.0 != 0.0 {
                bits[i / 8] |= 0x80 >> (i % 8);
            }
        }
        let pad = bits.len() * 8 - vals.len();
        (0x10, pad as u8, bits)
    } else if vals.iter().all(|e| e.2) && vals.iter().any(|e| e.3 && !in_i8(e.0)) {
        return Err(Error::new(241, "Converting a BSON array with integers larger than INT8 is not supported."));
    } else if vals.iter().all(|e| e.2 && in_i8(e.0)) {
        (0x03, 0, vals.iter().map(|e| e.0 as i8 as u8).collect())
    } else {
        let mut out = Vec::with_capacity(vals.len() * 4);
        for e in &vals {
            let f = e.0 as f32;
            out.extend_from_slice(&if big { f.to_be_bytes() } else { f.to_le_bytes() });
        }
        (0x27, 0, out)
    };
    let mut bytes = vec![dtype, padding];
    bytes.extend(payload);
    Ok(Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::from(9u8), bytes }))
}

/// Array from a vector BinData (subtype 9): booleans, ints or floats per dType.
fn vector_array(b: &bson::Binary, big: bool) -> Result<Bson> {
    if b.bytes.len() < 2 {
        return Err(Error::new(10506601, "binData length invalid"));
    }
    let (dtype, pad, payload) = (b.bytes[0], b.bytes[1], &b.bytes[2..]);
    if !matches!(dtype, 0x10 | 0x03 | 0x27) {
        return Err(Error::new(
            10506600,
            "Invalid dType for provided BinData vector. Valid options are 0x10 for PACKED_BIT, 0x03 for INT8, or 0x27 for FLOAT32.",
        ));
    }
    if pad > 7 {
        return Err(Error::new(241, format!("Invalid Vector: padding must be a value between 0 and 7. found: {pad}")));
    }
    Ok(Bson::Array(match dtype {
        0x10 => {
            let nbits = (payload.len() * 8).saturating_sub(usize::from(pad));
            (0..nbits).map(|i| Bson::Boolean(payload[i / 8] & (0x80 >> (i % 8)) != 0)).collect()
        }
        0x03 => payload.iter().map(|&x| Bson::Int32(i32::from(x as i8))).collect(),
        _ => {
            if payload.len() % 4 != 0 {
                return Err(Error::new(241, "Invalid Vector: Float32 vector must contain a multiple of 4 bytes"));
            }
            payload
                .chunks_exact(4)
                .map(|c| {
                    let a = [c[0], c[1], c[2], c[3]];
                    let f = if big { f32::from_be_bytes(a) } else { f32::from_le_bytes(a) };
                    Bson::Double(f64::from(f))
                })
                .collect()
        }
    }))
}

fn to_array_val(v: &Bson, o: &ConvOpts) -> Result<Bson> {
    match v {
        Bson::Binary(b) if u8::from(b.subtype) == 9 => vector_array(b, o.big),
        Bson::Binary(_) => Err(Error::new(241, "Conversion from binData to array is only supported for bindata with the vector subtype")),
        _ => Err(conv_fail(v, "array")),
    }
}

/// Unsigned integer from 1-8 bytes, honouring byte order; `sizes` lists the accepted lengths.
fn bin_uint(b: &bson::Binary, big: bool, sizes: &[usize], alias: &str) -> Result<u64> {
    if !sizes.contains(&b.bytes.len()) {
        return Err(Error::new(241, format!("Unsupported conversion from binData to {alias} in $convert with no onError value")));
    }
    Ok(if big {
        b.bytes.iter().fold(0u64, |a, &x| (a << 8) | u64::from(x))
    } else {
        b.bytes.iter().rev().fold(0u64, |a, &x| (a << 8) | u64::from(x))
    })
}

fn parse_f64(s: &str) -> Result<f64> {
    let d: f64 = s.parse().map_err(|e: std::num::ParseFloatError| parse_fail("number", s, &e.to_string()))?;
    if d.is_infinite() && !s.to_ascii_lowercase().contains("inf") {
        return Err(parse_fail("number", s, "number is out of range"));
    }
    Ok(d)
}

/// Integer in base 2, 8, 10 or 16 with upper-case digits (MongoDB's `$convert` base output).
fn radix_str(n: i64, base: u32) -> String {
    if base == 10 {
        return n.to_string();
    }
    let mut u = n.unsigned_abs();
    let mut digits = Vec::new();
    loop {
        digits.push(char::from_digit((u % u64::from(base)) as u32, base).unwrap_or('0').to_ascii_uppercase());
        u /= u64::from(base);
        if u == 0 {
            break;
        }
    }
    if n < 0 {
        digits.push('-');
    }
    digits.iter().rev().collect()
}

/// Convert a non-null value to the named target type.
fn convert_value(v: &Bson, alias: &str, o: &ConvOpts) -> Result<Bson> {
    Ok(match alias {
        "double" if o.base.is_some() && matches!(v, Bson::String(_)) => {
            let i = to_i64(v, alias, o.base)?;
            match i32::try_from(i) {
                Ok(x) => Bson::Int32(x),
                Err(_) => Bson::Int64(i),
            }
        }
        "double" => Bson::Double(match v {
            Bson::Double(d) => *d,
            Bson::Int32(i) => f64::from(*i),
            Bson::Int64(i) => *i as f64,
            Bson::Decimal128(d) => decimal_to_f64(d),
            Bson::Boolean(b) => f64::from(u8::from(*b)),
            Bson::DateTime(d) => d.timestamp_millis() as f64,
            Bson::String(s) => parse_f64(s)?,
            Bson::Binary(b) => {
                let bits = bin_uint(b, o.big, &[4, 8], alias)?;
                if b.bytes.len() == 4 { f64::from(f32::from_bits(bits as u32)) } else { f64::from_bits(bits) }
            }
            _ => return Err(conv_fail(v, alias)),
        }),
        "int" => {
            if matches!(v, Bson::DateTime(_)) {
                return Err(conv_fail(v, alias));
            }
            let i = match v {
                Bson::Binary(b) => bin_uint(b, o.big, &[1, 2, 4], alias)? as i64,
                _ => to_i64(v, alias, o.base)?,
            };
            Bson::Int32(i32::try_from(i).map_err(|_| overflow_fail())?)
        }
        "long" => Bson::Int64(match v {
            Bson::Binary(b) => bin_uint(b, o.big, &[1, 2, 4, 8], alias)? as i64,
            _ => to_i64(v, alias, o.base)?,
        }),
        "decimal" => Bson::Decimal128(to_decimal(v, alias)?),
        "string" => Bson::String(match (v, o.base) {
            (Bson::Int32(_) | Bson::Int64(_), Some(base)) => radix_str(to_i64(v, alias, None)?, base),
            (Bson::Binary(b), _) => encode_bin(b, format_of(o)?)?,
            _ => to_string_val(v, alias)?,
        }),
        "bool" => Bson::Boolean(match v {
            Bson::Boolean(b) => *b,
            Bson::Int32(i) => *i != 0,
            Bson::Int64(i) => *i != 0,
            Bson::Double(d) => *d != 0.0,
            Bson::Decimal128(d) => decimal_to_f64(d) != 0.0,
            Bson::DateTime(_) | Bson::ObjectId(_) | Bson::String(_) | Bson::Binary(_) | Bson::Array(_) | Bson::Document(_) => true,
            _ => return Err(conv_fail(v, alias)),
        }),
        "objectId" => match v {
            Bson::ObjectId(o) => Bson::ObjectId(*o),
            Bson::String(s) => Bson::ObjectId(
                ObjectId::parse_str(s).map_err(|e| parse_fail("objectId", s, &e.to_string()))?,
            ),
            _ => return Err(conv_fail(v, alias)),
        },
        "date" => Bson::DateTime(to_date(v, alias)?),
        "array" => to_array_val(v, o)?,
        "binData" => to_bin(v, o)?,
        _ => return Err(Error::bad_value(format!("Unsupported conversion target type: {alias}"))),
    })
}

fn op_convert(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let Bson::Document(d) = args else {
        return Err(Error::new(9, "$convert requires an object argument"));
    };
    for k in d.keys() {
        if !matches!(k.as_str(), "input" | "to" | "onError" | "onNull" | "format" | "byteOrder" | "base") {
            return Err(Error::new(9, format!("Unrecognized parameter to $convert: {k}")));
        }
    }
    let input = eval(d.get("input").ok_or_else(|| Error::new(9, "Missing 'input' parameter to $convert"))?, doc, vars)?;
    let to = eval(d.get("to").ok_or_else(|| Error::new(9, "Missing 'to' parameter to $convert"))?, doc, vars)?;
    if is_nullish(&input) {
        return match d.get("onNull") {
            Some(e) => eval(e, doc, vars),
            None => Ok(Bson::Null),
        };
    }
    if is_nullish(&to) {
        return Ok(Bson::Null);
    }
    let (alias, to_sub) = convert_target(&to)?;
    let mut o = conv_opts(d, doc, vars)?;
    if let Some(s) = to_sub {
        if !is_nullish(s) {
            o.subtype = subtype_of(s)?;
        }
    }
    match convert_value(&input, alias, &o) {
        Ok(v) => Ok(v),
        Err(e) => match d.get("onError") {
            Some(fallback) => eval(fallback, doc, vars),
            None => Err(e),
        },
    }
}

fn op_to(op: &str, alias: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let n = items(args).len();
    if n != 1 {
        return Err(Error::new(50723, format!("Expression {op} takes exactly 1 arguments. {n} were passed in.")));
    }
    let [x] = fixed::<1>(op, args, doc, vars)?;
    if is_nullish(&x) {
        return Ok(Bson::Null);
    }
    convert_value(&x, alias, &ConvOpts::default())
}

/// Named-argument object for `$serializeEJSON`/`$deserializeEJSON`; unknown keys are rejected.
fn ejson_args<'a>(op: &str, args: &'a Bson, allowed: &[&str]) -> Result<&'a Document> {
    let Bson::Document(d) = args else {
        return Err(Error::new(9, format!("{op} expects an object of named arguments but found: {}", type_name(args))));
    };
    for k in d.keys() {
        if !allowed.contains(&k.as_str()) {
            return Err(Error::new(9, format!("{op} found an unknown argument: {k}")));
        }
    }
    Ok(d)
}

fn op_serialize_ejson(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = ejson_args("$serializeEJSON", args, &["input", "relaxed", "onError"])?;
    let input_e = d.get("input").ok_or_else(|| Error::new(9, "Missing 'input' parameter to $serializeEJSON"))?;
    let relaxed = match d.get("relaxed") {
        None => false,
        Some(e) => match eval(e, doc, vars)? {
            Bson::Boolean(b) => b,
            v => return Err(Error::new(2, format!("Unexpected value for relaxed: {}", shell_repr(&v)))),
        },
    };
    // Serialization of supported BSON types cannot fail, so onError is never used here.
    let input = eval(input_e, doc, vars)?;
    Ok(ejson_ser(&input, relaxed))
}

fn op_deserialize_ejson(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = ejson_args("$deserializeEJSON", args, &["input", "onError"])?;
    let input_e = d.get("input").ok_or_else(|| Error::new(9, "Missing 'input' parameter to $deserializeEJSON"))?;
    let input = eval(input_e, doc, vars)?;
    match ejson_de(&input) {
        Ok(v) => Ok(v),
        Err(e) => match d.get("onError") {
            Some(fallback) => eval(fallback, doc, vars),
            None => Err(e),
        },
    }
}

fn ejson_pairs(v: Vec<(&str, Bson)>) -> Bson {
    Bson::Document(v.into_iter().map(|(k, x)| (k.to_string(), x)).collect())
}

fn ejson_one(k: &str, v: Bson) -> Bson {
    ejson_pairs(vec![(k, v)])
}

/// Canonical EJSON (`relaxed` false) or relaxed EJSON (`relaxed` true).
fn ejson_ser(v: &Bson, relaxed: bool) -> Bson {
    use base64::Engine;
    match v {
        Bson::Document(d) => Bson::Document(d.iter().map(|(k, x)| (k.clone(), ejson_ser(x, relaxed))).collect()),
        Bson::Array(a) => Bson::Array(a.iter().map(|x| ejson_ser(x, relaxed)).collect()),
        Bson::Int32(i) if relaxed => Bson::Int32(*i),
        Bson::Int32(i) => ejson_one("$numberInt", Bson::String(i.to_string())),
        Bson::Int64(i) if relaxed => Bson::Int64(*i),
        Bson::Int64(i) => ejson_one("$numberLong", Bson::String(i.to_string())),
        Bson::Double(f) if f.is_nan() => ejson_one("$numberDouble", Bson::String("NaN".into())),
        Bson::Double(f) if f.is_infinite() => {
            let s = if *f > 0.0 { "Infinity" } else { "-Infinity" };
            ejson_one("$numberDouble", Bson::String(s.into()))
        }
        Bson::Double(f) if relaxed => Bson::Double(*f),
        Bson::Double(f) => ejson_one("$numberDouble", Bson::String(format!("{f:?}"))),
        Bson::Decimal128(d) => ejson_one("$numberDecimal", Bson::String(d.to_string())),
        Bson::ObjectId(o) => ejson_one("$oid", Bson::String(o.to_hex())),
        Bson::Boolean(b) => Bson::Boolean(*b),
        Bson::String(_) | Bson::Null => v.clone(),
        Bson::DateTime(dt) => {
            let ms = dt.timestamp_millis();
            if (0..32_535_215_999_000).contains(&ms) {
                let iso = dt.try_to_rfc3339_string().unwrap_or_default();
                let secs = iso.trim_end_matches('Z').split('.').next().unwrap_or_default();
                ejson_one("$date", Bson::String(format!("{secs}.{:03}Z", ms.rem_euclid(1000))))
            } else {
                ejson_one("$date", ejson_one("$numberLong", Bson::String(ms.to_string())))
            }
        }
        Bson::Timestamp(t) => ejson_one(
            "$timestamp",
            ejson_pairs(vec![("t", Bson::Int64(t.time as i64)), ("i", Bson::Int64(t.increment as i64))]),
        ),
        Bson::Binary(b) if u8::from(b.subtype) == 4 && b.bytes.len() == 16 => {
            ejson_one("$uuid", Bson::String(fmt_uuid(&b.bytes)))
        }
        Bson::Binary(b) => ejson_one(
            "$binary",
            ejson_pairs(vec![
                ("base64", Bson::String(base64::engine::general_purpose::STANDARD.encode(&b.bytes))),
                ("subType", Bson::String(format!("{:x}", u8::from(b.subtype)))),
            ]),
        ),
        Bson::RegularExpression(r) => ejson_one(
            "$regularExpression",
            ejson_pairs(vec![("pattern", Bson::String(r.pattern.clone())), ("options", Bson::String(r.options.clone()))]),
        ),
        Bson::JavaScriptCode(c) => ejson_one("$code", Bson::String(c.clone())),
        Bson::JavaScriptCodeWithScope(c) => ejson_pairs(vec![
            ("$code", Bson::String(c.code.clone())),
            ("$scope", ejson_ser(&Bson::Document(c.scope.clone()), relaxed)),
        ]),
        Bson::Symbol(s) => ejson_one("$symbol", Bson::String(s.clone())),
        Bson::MinKey => ejson_one("$minKey", Bson::Int32(1)),
        Bson::MaxKey => ejson_one("$maxKey", Bson::Int32(1)),
        Bson::Undefined => ejson_one("$undefined", Bson::Boolean(true)),
        other => other.clone(),
    }
}

fn ejson_str<'a>(v: &'a Bson, msg: &str) -> Result<&'a str> {
    match v {
        Bson::String(s) => Ok(s),
        _ => Err(Error::new(241, msg)),
    }
}

/// Decodes EJSON wrapper documents (`{$numberLong: "1"}` etc.) recursively.
fn ejson_de(v: &Bson) -> Result<Bson> {
    match v {
        Bson::Array(a) => Ok(Bson::Array(a.iter().map(ejson_de).collect::<Result<Vec<_>>>()?)),
        Bson::Document(d) => {
            if d.len() == 1 {
                if let Some((k, inner)) = d.iter().next() {
                    if let Some(r) = ejson_wrapper(k, inner)? {
                        return Ok(r);
                    }
                }
            }
            Ok(Bson::Document(d.iter().map(|(k, x)| Ok((k.clone(), ejson_de(x)?))).collect::<Result<Document>>()?))
        }
        other => Ok(other.clone()),
    }
}

/// One EJSON wrapper key; `None` when the key is not an EJSON type wrapper.
fn ejson_wrapper(k: &str, v: &Bson) -> Result<Option<Bson>> {
    use base64::Engine;
    let r = match k {
        "$numberLong" => {
            let s = ejson_str(v, "$numberLong value must be of type string")?;
            let n = s.parse::<i64>().map_err(|_| Error::new(241, "$numberLong value must be a string specifying an integer"))?;
            Bson::Int64(n)
        }
        "$numberInt" => {
            let s = ejson_str(v, "$numberInt value must be of type string")?;
            let n = s.parse::<i32>().map_err(|_| Error::new(241, "$numberInt value must be a string specifying an integer"))?;
            Bson::Int32(n)
        }
        "$numberDouble" => {
            let bad = || Error::new(241, "$numberDouble value must be a string specifying a number, NaN, Infinity or -Infinity");
            let f = match v {
                Bson::String(s) => match s.as_str() {
                    "NaN" => f64::NAN,
                    "Infinity" => f64::INFINITY,
                    "-Infinity" => f64::NEG_INFINITY,
                    s if s.bytes().all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'-' | b'+' | b'e' | b'E')) => {
                        s.parse::<f64>().map_err(|_| bad())?
                    }
                    _ => return Err(bad()),
                },
                Bson::Double(f) => *f,
                Bson::Int32(i) => *i as f64,
                Bson::Int64(i) => *i as f64,
                _ => return Err(bad()),
            };
            Bson::Double(f)
        }
        "$numberDecimal" => {
            let s = ejson_str(v, "$numberDecimal value must be of type string")?;
            let d = s.parse::<bson::Decimal128>().map_err(|_| Error::new(241, "$numberDecimal value must be a string specifying a decimal"))?;
            Bson::Decimal128(d)
        }
        "$oid" => {
            let s = ejson_str(v, "$oid must be a 24 character hexadecimal string")?;
            Bson::ObjectId(bson::oid::ObjectId::parse_str(s).map_err(|_| Error::new(241, "$oid must be a 24 character hexadecimal string"))?)
        }
        "$uuid" => {
            let s = ejson_str(v, "$uuid value must be a valid UUID string")?;
            let h: String = s.chars().filter(|c| *c != '-').collect();
            let bytes = match (h.len(), unhex(&h)) {
                (32, Some(b)) => b,
                _ => return Err(Error::new(241, "$uuid value must be a valid UUID string")),
            };
            Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Uuid, bytes })
        }
        "$binary" => {
            let Bson::Document(b) = v else {
                return Err(Error::new(241, "$binary value must be of type object"));
            };
            let b64 = match b.get("base64") {
                Some(Bson::String(s)) => s,
                _ => return Err(Error::new(241, "$binary.base64 value must be of type string")),
            };
            let sub = match b.get("subType") {
                Some(Bson::String(s)) => s,
                _ => return Err(Error::new(241, "$binary.subType value must be of type string")),
            };
            if sub.is_empty() || sub.len() > 2 {
                return Err(Error::new(241, "$binary.subType must be a hex representation of a single byte"));
            }
            let st = u8::from_str_radix(sub, 16).map_err(|_| Error::new(241, "$binary.subType must be a hex representation of a single byte"))?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|_| Error::new(241, "$binary.base64 must be a valid base64 encoded string"))?;
            Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::from(st), bytes })
        }
        "$date" => match v {
            Bson::String(s) => Bson::DateTime(
                bson::DateTime::parse_rfc3339_str(s).map_err(|_| Error::new(241, "$date must be a valid ISO-8601 string"))?,
            ),
            Bson::Document(_) => match ejson_de(v)? {
                Bson::Int64(ms) => Bson::DateTime(bson::DateTime::from_millis(ms)),
                _ => return Err(Error::new(241, "$date must be a valid ISO-8601 string")),
            },
            _ => return Err(Error::new(241, "$date must be a valid ISO-8601 string")),
        },
        "$timestamp" => {
            let Bson::Document(t) = v else {
                return Err(Error::new(241, "$timestamp value must be of type object"));
            };
            let part = |name: &str| -> Result<u32> {
                let x = match t.get(name) {
                    Some(Bson::Int32(x)) => *x as i64,
                    Some(Bson::Int64(x)) => *x,
                    _ => return Err(Error::new(241, format!("$timestamp.{name} value must be of type int or long"))),
                };
                u32::try_from(x).map_err(|_| Error::new(241, format!("$timestamp.{name} value must be non-negative")))
            };
            Bson::Timestamp(bson::Timestamp { time: part("t")?, increment: part("i")? })
        }
        "$regularExpression" => {
            let Bson::Document(r) = v else {
                return Err(Error::new(241, "$regularExpression value must be of type object"));
            };
            let pattern = match r.get("pattern") {
                Some(Bson::String(s)) => s.clone(),
                _ => return Err(Error::new(241, "$regularExpression.pattern value must be of type string")),
            };
            let options = match r.get("options") {
                Some(Bson::String(s)) => s.clone(),
                _ => return Err(Error::new(241, "$regularExpression.options value must be of type string")),
            };
            Bson::RegularExpression(bson::Regex { pattern, options })
        }
        "$minKey" => Bson::MinKey,
        "$maxKey" => Bson::MaxKey,
        "$code" => Bson::JavaScriptCode(ejson_str(v, "$code value must be of type string")?.to_string()),
        "$symbol" => Bson::Symbol(ejson_str(v, "$symbol value must be of type string")?.to_string()),
        "$undefined" => Bson::Undefined,
        _ => return Ok(None),
    };
    Ok(Some(r))
}

// ---------- accumulator-style expressions ----------

/// Values for accumulator expressions: a single array argument is its elements; otherwise the argument values.
fn collect_values(args: &Bson, doc: &Document, vars: &Vars) -> Result<Vec<Bson>> {
    let mut vals = ev_all(args, doc, vars)?;
    if matches!(vals.as_slice(), [Bson::Array(_)]) {
        if let Some(Bson::Array(a)) = vals.pop() {
            return Ok(a);
        }
    }
    Ok(vals)
}

fn op_sum_avg(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = collect_values(args, doc, vars)?;
    if op == "$sum" {
        let mut acc = Bson::Int32(0);
        for v in &vals {
            if is_number(v) {
                if let Some(s) = num_add(&acc, v) {
                    acc = s;
                }
            }
        }
        return Ok(acc);
    }
    let nums: Vec<f64> = vals.iter().filter(|v| is_number(v)).filter_map(num_f64).collect();
    if nums.is_empty() {
        return Ok(Bson::Null);
    }
    Ok(Bson::Double(nums.iter().sum::<f64>() / nums.len() as f64))
}

fn op_min_max(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = collect_values(args, doc, vars)?;
    let mut best: Option<Bson> = None;
    for v in vals {
        if is_nullish(&v) {
            continue;
        }
        best = Some(match best {
            None => v,
            Some(b) => {
                let o = cmp_bson(&v, &b);
                let take_new = if op == "$min" { o == Ordering::Less } else { o == Ordering::Greater };
                if take_new { v } else { b }
            }
        });
    }
    Ok(best.unwrap_or(Bson::Null))
}

fn op_std_dev(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let vals = collect_values(args, doc, vars)?;
    let nums: Vec<f64> = vals.iter().filter(|v| is_number(v)).filter_map(num_f64).collect();
    let pop = op == "$stdDevPop";
    let n = nums.len();
    if n == 0 || (!pop && n < 2) {
        return Ok(Bson::Null);
    }
    let mean = nums.iter().sum::<f64>() / n as f64;
    let ss: f64 = nums.iter().map(|x| (x - mean).powi(2)).sum();
    let denom = if pop { n as f64 } else { (n - 1) as f64 };
    Ok(Bson::Double((ss / denom).sqrt()))
}

fn check_method(d: &Document, op: &str) -> Result<()> {
    match d.get("method") {
        None => Ok(()),
        Some(Bson::String(m)) if m == "approximate" => Ok(()),
        Some(_) => Err(Error::bad_value(format!("{op} requires 'method' to be 'approximate'"))),
    }
}

/// Sorted numeric values of `input`; `None` when input is null/missing.
fn sorted_nums(input: &Bson, op: &str) -> Result<Option<Vec<f64>>> {
    if is_nullish(input) {
        return Ok(None);
    }
    let Bson::Array(a) = input else {
        return Err(Error::bad_value(format!("{op} requires 'input' to be an array")));
    };
    let mut nums: Vec<f64> = a.iter().filter(|v| is_number(v)).filter_map(num_f64).collect();
    nums.sort_by(|x, y| x.total_cmp(y));
    Ok(Some(nums))
}

/// Linear-interpolated percentile of already-sorted values.
fn percentile_of(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let pos = p * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    let (a, b) = (*sorted.get(lo)?, *sorted.get(hi)?);
    Some(a + (b - a) * (pos - lo as f64))
}

fn op_median(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$median", args)?;
    let input = eval(get_arg(d, "$median", "input")?, doc, vars)?;
    check_method(d, "$median")?;
    let Some(sorted) = sorted_nums(&input, "$median")? else {
        return Ok(Bson::Null);
    };
    Ok(percentile_of(&sorted, 0.5).map(Bson::Double).unwrap_or(Bson::Null))
}

fn op_percentile(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let d = doc_arg("$percentile", args)?;
    let input = eval(get_arg(d, "$percentile", "input")?, doc, vars)?;
    check_method(d, "$percentile")?;
    let ps = match get_arg(d, "$percentile", "p")? {
        Bson::Array(p) => p,
        _ => return Err(Error::bad_value("$percentile requires 'p' to be an array")),
    };
    let mut probs = Vec::with_capacity(ps.len());
    for p in ps {
        match num_f64(p) {
            Some(x) if (0.0..=1.0).contains(&x) => probs.push(x),
            _ => return Err(Error::bad_value("$percentile requires 'p' values in the range [0.0, 1.0]")),
        }
    }
    let Some(sorted) = sorted_nums(&input, "$percentile")? else {
        return Ok(Bson::Null);
    };
    Ok(Bson::Array(
        probs
            .into_iter()
            .map(|p| percentile_of(&sorted, p).map(Bson::Double).unwrap_or(Bson::Null))
            .collect(),
    ))
}

// ---------- misc ----------

fn op_meta(args: &Bson, doc: &Document) -> Result<Bson> {
    let name = match args {
        Bson::String(s) => s.as_str(),
        _ => return Err(Error::bad_value("$meta requires a string argument")),
    };
    if !matches!(
        name,
        "textScore" | "searchScore" | "vectorSearchScore" | "geoNearDistance" | "randVal" | "indexKey"
    ) {
        return Err(Error::new(17308, format!("Unsupported argument to $meta: {name}")));
    }
    Ok(match doc.get(META_KEY) {
        Some(Bson::Document(m)) => m.get(name).cloned().unwrap_or(MISSING),
        _ => MISSING,
    })
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

fn op_hashed_key(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>("$toHashedIndexKey", args, doc, vars)?;
    let x = if is_missing(&x) { Bson::Null } else { x };
    let mut wrapper = Document::new();
    wrapper.insert("", x);
    Ok(Bson::Int64(fnv1a(&to_bytes(&wrapper)) as i64))
}

fn op_binary_size(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>("$binarySize", args, doc, vars)?;
    match &x {
        other if is_nullish(other) => Ok(Bson::Null),
        Bson::String(s) => Ok(Bson::Int32(s.len() as i32)),
        Bson::Binary(b) => Ok(Bson::Int32(b.bytes.len() as i32)),
        _ => Err(Error::bad_value("$binarySize requires a string or BinData argument")),
    }
}

fn op_bson_size(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>("$bsonSize", args, doc, vars)?;
    match &x {
        Bson::Document(d) => Ok(Bson::Int32(doc_size(d) as i32)),
        other if is_nullish(other) => Ok(Bson::Null),
        other => Err(Error::bad_value(format!(
            "$bsonSize requires a document input, found: {}",
            type_name(other)
        ))),
    }
}

fn op_ts_part(op: &str, args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let [x] = fixed::<1>(op, args, doc, vars)?;
    match &x {
        Bson::Timestamp(t) => Ok(Bson::Int64(if op == "$tsSecond" {
            i64::from(t.time)
        } else {
            i64::from(t.increment)
        })),
        other if is_nullish(other) => Ok(Bson::Null),
        other => Err(Error::bad_value(format!("{op} requires a timestamp argument, found: {}", type_name(other)))),
    }
}

// ---------- date/time text helpers (ISO-8601, UTC) ----------

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Format epoch milliseconds as `YYYY-MM-DDTHH:MM:SS.mmmZ`.
fn fmt_iso(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    let (h, mi, s, milli) = (rem / 3_600_000, rem / 60_000 % 60, rem / 1000 % 60, rem % 1000);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{milli:03}Z")
}

/// Split a trailing `Z` or `±HH:MM` offset off a time string; returns (time, offset minutes).
fn split_offset(t: &str) -> Option<(&str, i64)> {
    if let Some(rest) = t.strip_suffix('Z').or_else(|| t.strip_suffix('z')) {
        return Some((rest, 0));
    }
    let pos = t.rfind(['+', '-'])?;
    if pos < 5 {
        return Some((t, 0));
    }
    let (body, off) = t.split_at(pos);
    let sign = if off.starts_with('-') { -1 } else { 1 };
    let (oh, om) = off[1..].split_once(':').unwrap_or((&off[1..], "0"));
    let minutes = oh.parse::<i64>().ok()? * 60 + om.parse::<i64>().ok()?;
    Some((body, sign * minutes))
}

/// Parse `YYYY-MM-DD` optionally followed by `T`/space and `HH:MM[:SS[.fff]]` with optional offset.
fn parse_iso_date(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date, time) = match s.find(['T', ' ']) {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    let mut it = date.split('-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    if it.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let mut ms = days_from_civil(y, m, d) * 86_400_000;
    if let Some(t) = time {
        let (t, off_min) = split_offset(t)?;
        let (hms, frac) = match t.split_once('.') {
            Some((a, b)) => (a, Some(b)),
            None => (t, None),
        };
        let parts: Vec<&str> = hms.split(':').collect();
        let (h, mi, sec) = match parts.as_slice() {
            [h, m] => (*h, *m, "0"),
            [h, m, s] => (*h, *m, *s),
            _ => return None,
        };
        let (h, mi, sec): (i64, i64, i64) = (h.parse().ok()?, mi.parse().ok()?, sec.parse().ok()?);
        if !(0..=23).contains(&h) || !(0..=59).contains(&mi) || !(0..=60).contains(&sec) {
            return None;
        }
        ms += (h * 3600 + mi * 60 + sec) * 1000;
        if let Some(f) = frac {
            if f.is_empty() || !f.bytes().all(|c| c.is_ascii_digit()) {
                return None;
            }
            let digits: String = f.chars().take(3).collect();
            let mut v: i64 = digits.parse().ok()?;
            for _ in digits.len()..3 {
                v *= 10;
            }
            ms += v;
        }
        ms -= off_min * 60_000;
    }
    Some(ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{bson, doc};

    fn run(expr: Bson) -> Bson {
        eval(&expr, &doc! {}, &Vars::new()).expect("eval ok")
    }

    fn run_doc(expr: Bson, d: Document) -> Bson {
        eval(&expr, &d, &Vars::new()).expect("eval ok")
    }

    fn err_of(expr: Bson) -> Error {
        eval(&expr, &doc! {}, &Vars::new()).expect_err("expected error")
    }

    #[test]
    fn field_path_fan_out() {
        let d = doc! { "a": [ { "b": 1 }, { "b": 2 }, { "c": 3 }, 5 ] };
        assert_eq!(resolve_field(&d, "a.b"), bson!([1, 2]));
        assert!(is_missing(&resolve_field(&d, "x.y")));
        let nested = doc! { "a": { "b": { "c": "deep" } } };
        assert_eq!(resolve_field(&nested, "a.b.c"), bson!("deep"));
    }

    #[test]
    fn remove_omits_field_in_object_literal() {
        let out = run(bson!({ "a": "$$REMOVE", "b": 1 }));
        assert_eq!(out, Bson::Document(doc! { "b": 1 }));
    }

    #[test]
    fn cluster_time_requires_var() {
        let e = err_of(bson!("$$CLUSTER_TIME"));
        assert_eq!(e.code, 10071200);
        assert_eq!(e.msg, "system variable $$CLUSTER_TIME is not available in standalone mode");
        let mut vars = Vars::new();
        let ts = Bson::Timestamp(bson::Timestamp { time: 7, increment: 1 });
        vars.insert("CLUSTER_TIME".to_string(), ts.clone());
        assert_eq!(eval(&bson!("$$CLUSTER_TIME"), &doc! {}, &vars).expect("eval ok"), ts);
    }

    #[test]
    fn missing_array_elements_become_null() {
        let out = run(bson!(["$$REMOVE", 1]));
        assert_eq!(out, bson!([null, 1]));
    }

    #[test]
    fn add_int_overflow_promotes_to_int64() {
        assert_eq!(run(bson!({ "$add": [2147483647, 1] })), Bson::Int64(2147483648));
    }

    #[test]
    fn add_date_and_number() {
        let d = doc! { "d": DateTime::from_millis(5) };
        assert_eq!(
            run_doc(bson!({ "$add": ["$d", 1000] }), d),
            Bson::DateTime(DateTime::from_millis(1005))
        );
    }

    #[test]
    fn add_missing_is_null() {
        assert_eq!(run(bson!({ "$add": ["$missing", 1] })), Bson::Null);
    }

    #[test]
    fn divide_by_zero_errors() {
        assert_eq!(err_of(bson!({ "$divide": [1, 0] })).code, 2);
    }

    #[test]
    fn cond_both_forms() {
        assert_eq!(run(bson!({ "$cond": [true, "x", "y"] })), bson!("x"));
        assert_eq!(run(bson!({ "$cond": { "if": false, "then": 1, "else": 2 } })), bson!(2));
    }

    #[test]
    fn switch_default_and_no_match() {
        let with_default = bson!({ "$switch": { "branches": [{ "case": false, "then": 1 }], "default": 9 } });
        assert_eq!(run(with_default), bson!(9));
        let no_default = bson!({ "$switch": { "branches": [{ "case": false, "then": 1 }] } });
        assert_eq!(err_of(no_default).code, 40069);
    }

    #[test]
    fn filter_map_reduce_with_vars() {
        assert_eq!(
            run(bson!({ "$filter": { "input": [1, 2, 3, 4], "cond": { "$gt": ["$$this", 2] } } })),
            bson!([3, 4])
        );
        assert_eq!(
            run(bson!({ "$map": { "input": [1, 2], "as": "x", "in": { "$add": ["$$x", 10] } } })),
            bson!([11, 12])
        );
        assert_eq!(
            run(bson!({ "$reduce": { "input": [1, 2, 3], "initialValue": 0, "in": { "$add": ["$$value", "$$this"] } } })),
            bson!(6)
        );
    }

    #[test]
    fn let_binds_variables() {
        assert_eq!(
            run(bson!({ "$let": { "vars": { "x": 5 }, "in": { "$multiply": ["$$x", 2] } } })),
            bson!(10)
        );
    }

    #[test]
    fn array_object_round_trip() {
        let out = run(bson!({ "$arrayToObject": { "$objectToArray": { "a": 1, "b": "x" } } }));
        assert_eq!(out, Bson::Document(doc! { "a": 1, "b": "x" }));
        assert_eq!(
            run(bson!({ "$arrayToObject": [[["k", "v"]]] })),
            Bson::Document(doc! { "k": "v" })
        );
    }

    #[test]
    fn round_half_to_even() {
        assert_eq!(run(bson!({ "$round": [2.5, 0] })), Bson::Double(2.0));
        assert_eq!(run(bson!({ "$round": [3.5, 0] })), Bson::Double(4.0));
    }

    #[test]
    fn convert_on_error_and_failure() {
        let fallback = bson!({ "$convert": { "input": "abc", "to": "int", "onError": -1 } });
        assert_eq!(run(fallback), bson!(-1));
        let failing = bson!({ "$convert": { "input": "abc", "to": "int" } });
        assert_eq!(err_of(failing).code, 241);
    }

    #[test]
    fn set_union_dedupes() {
        assert_eq!(run(bson!({ "$setUnion": [[1, 2, 2], [2, 3]] })), bson!([1, 2, 3]));
    }

    #[test]
    fn merge_objects_skips_null() {
        let out = run(bson!({ "$mergeObjects": [{ "a": 1 }, null, { "a": 2, "b": 3 }] }));
        assert_eq!(out, Bson::Document(doc! { "a": 2, "b": 3 }));
    }

    #[test]
    fn sort_array_by_direction_and_fields() {
        assert_eq!(
            run(bson!({ "$sortArray": { "input": [3, 1, 2], "sortBy": -1 } })),
            bson!([3, 2, 1])
        );
        assert_eq!(
            run(bson!({ "$sortArray": { "input": [{ "a": 2 }, { "a": 1 }], "sortBy": { "a": 1 } } })),
            bson!([{ "a": 1 }, { "a": 2 }])
        );
    }

    #[test]
    fn date_iso_round_trip() {
        let ms = parse_iso_date("2024-01-02T03:04:05.006Z").expect("parses");
        assert_eq!(fmt_iso(ms), "2024-01-02T03:04:05.006Z");
        assert_eq!(parse_iso_date("1970-01-01"), Some(0));
    }

    #[test]
    fn variable_undefined_errors() {
        assert_eq!(err_of(bson!("$$nope")).code, 17276);
    }
}
