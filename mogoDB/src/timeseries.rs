//! Time series collections (MongoDB 9.0 semantics on a single user-facing namespace).
//!
//! Measurements are stored as ordinary documents (time field first, then the meta field,
//! then the measurement fields), so every query/aggregation feature works unchanged. This
//! module owns option validation/normalization, write restrictions, and index rules.

use crate::bsonutil;
use crate::error::{Error, Result};
use bson::{Bson, Document, doc};

#[derive(Clone, Debug, PartialEq)]
pub struct TsSpec {
    pub time_field: String,
    pub meta_field: Option<String>,
}

/// Parsed spec of a collection's `options.timeseries`.
pub fn spec(options: &Document) -> Option<TsSpec> {
    let ts = options.get_document("timeseries").ok()?;
    Some(TsSpec { time_field: ts.get_str("timeField").ok()?.to_string(), meta_field: ts.get_str("metaField").ok().map(str::to_string) })
}

/// (bucketMaxSpanSeconds, bucketRoundingSeconds) defaults per granularity.
fn granularity_spans(g: &str) -> (i32, i32) {
    match g {
        "minutes" => (86400, 3600),
        "hours" => (2_592_000, 86400),
        _ => (3600, 60),
    }
}

fn span_arg(ts: &Document, k: &str) -> Result<Option<i32>> {
    match ts.get(k) {
        None => Ok(None),
        Some(v) => match bsonutil::num_i64(v) {
            Some(n) if (1..=31_536_000).contains(&n) => Ok(Some(n as i32)),
            Some(_) => Err(Error::new(2, format!("BSON field 'create.timeseries.{k}' value must be in the range [1, 31536000]"))),
            None => Err(Error::type_mismatch(format!("BSON field 'create.timeseries.{k}' is the wrong type '{}', expected types '[long, int, decimal, double]'", bsonutil::type_name(v)))),
        },
    }
}

/// Validate and normalize `create` options of a time series collection in place.
pub fn normalize_create(ns: &str, opts: &mut Document) -> Result<()> {
    let ts = match opts.get("timeseries") {
        Some(Bson::Document(d)) => d.clone(),
        Some(v) => return Err(Error::type_mismatch(format!("BSON field 'create.timeseries' is the wrong type '{}', expected type 'object'", bsonutil::type_name(v)))),
        None => return Ok(()),
    };
    for k in ts.keys() {
        if !matches!(k.as_str(), "timeField" | "metaField" | "granularity" | "bucketMaxSpanSeconds" | "bucketRoundingSeconds") {
            return Err(Error::new(40415, format!("BSON field 'create.timeseries.{k}' is an unknown field.")));
        }
    }
    let time = match ts.get("timeField") {
        Some(Bson::String(s)) => s.clone(),
        Some(v) => return Err(Error::type_mismatch(format!("BSON field 'create.timeseries.timeField' is the wrong type '{}', expected type 'string'", bsonutil::type_name(v)))),
        None => return Err(Error::new(40414, "BSON field 'create.timeseries.timeField' is missing but a required field")),
    };
    let meta = match ts.get("metaField") {
        Some(Bson::String(s)) => Some(s.clone()),
        Some(v) => return Err(Error::type_mismatch(format!("BSON field 'create.timeseries.metaField' is the wrong type '{}', expected type 'string'", bsonutil::type_name(v)))),
        None => None,
    };
    let granularity = match ts.get("granularity") {
        None => None,
        Some(Bson::String(g)) if matches!(g.as_str(), "seconds" | "minutes" | "hours") => Some(g.clone()),
        Some(Bson::String(g)) => return Err(Error::new(2, format!("Enumeration value '{g}' for field 'create.timeseries.granularity' is not a valid value."))),
        Some(v) => return Err(Error::type_mismatch(format!("BSON field 'create.timeseries.granularity' is the wrong type '{}', expected type 'string'", bsonutil::type_name(v)))),
    };
    if time.starts_with('$') || meta.as_ref().is_some_and(|m| m.starts_with('$')) {
        return Err(Error::new(2, "The 'timeField' or 'metaField' cannot start with '$'"));
    }
    if meta.as_deref() == Some(time.as_str()) {
        return Err(Error::new(72, "'metaField' cannot be the same as 'timeField'"));
    }
    if opts.contains_key("capped") && bsonutil::as_bool(opts.get("capped")) {
        return Err(Error::new(72, format!("{ns}: 'timeseries' is not allowed with 'capped'")));
    }
    if opts.contains_key("viewOn") {
        return Err(Error::new(72, format!("{ns}: 'timeseries' is not allowed with 'viewOn'")));
    }
    if time.contains('.') {
        return Err(Error::new(72, format!("{ns}: 'timeField' must be a top-level field and not contain a '.'")));
    }
    if meta.as_ref().is_some_and(|m| m.contains('.')) {
        return Err(Error::new(72, format!("{ns}: 'metaField' must be a top-level field and not contain a '.'")));
    }
    let fail = |m: String| Error::new(72, format!("Failed to create timeseries collection :: caused by :: {m}"));
    let max_span = span_arg(&ts, "bucketMaxSpanSeconds")?;
    let rounding = span_arg(&ts, "bucketRoundingSeconds")?;
    let mut out = doc! {"timeField": &time};
    if let Some(m) = &meta {
        out.insert("metaField", m.clone());
    }
    match (max_span, rounding) {
        (None, None) => {
            let g = granularity.unwrap_or_else(|| "seconds".into());
            out.insert("granularity", g.clone());
            out.insert("bucketMaxSpanSeconds", granularity_spans(&g).0);
        }
        (span, round) => {
            if let Some(g) = &granularity {
                let (dspan, dround) = granularity_spans(g);
                if span.is_some_and(|s| s != dspan) {
                    return Err(fail(format!("Time-series 'bucketMaxSpanSeconds' cannot be set to a value other than the default of {dspan} for the provided granularity")));
                }
                if round.is_some_and(|r| r != dround) {
                    return Err(fail(format!("Time-series 'bucketRoundingSeconds' cannot be set to a value other than the default of {dround} for the provided granularity")));
                }
                out.insert("granularity", g.clone());
                out.insert("bucketMaxSpanSeconds", dspan);
            } else {
                if span != round {
                    return Err(fail("Time-series 'bucketRoundingSeconds' must be equal to 'bucketMaxSpanSeconds'".into()));
                }
                out.insert("bucketRoundingSeconds", round.expect("equal and present"));
                out.insert("bucketMaxSpanSeconds", span.expect("equal and present"));
            }
        }
    }
    out.insert("fixedBucketing", true);
    opts.insert("timeseries", out);
    if let Some(v) = opts.get("expireAfterSeconds").cloned() {
        let n = match v {
            v if bsonutil::is_number(&v) => bsonutil::num_f64(&v).unwrap_or(0.0),
            other => return Err(Error::type_mismatch(format!("BSON field 'create.expireAfterSeconds' is the wrong type '{}', expected types '[long, double, int, decimal]'", bsonutil::type_name(&other)))),
        };
        if n < 0.0 {
            return Err(fail("TTL index 'expireAfterSeconds' option cannot be less than 0".into()));
        }
        // Stored (and listed) before `timeseries`.
        let rest = std::mem::take(opts);
        opts.insert("expireAfterSeconds", Bson::Int64(n as i64));
        for (k, v) in rest {
            if k != "expireAfterSeconds" {
                opts.insert(k, v);
            }
        }
    }
    Ok(())
}

/// Default index created with the collection: `{meta: 1, time: 1}` when there is a metaField.
pub fn default_index(options: &Document) -> Option<Document> {
    let s = spec(options)?;
    let m = s.meta_field?;
    Some(doc! {"v": 2, "key": {m.as_str(): 1, s.time_field.as_str(): 1}, "name": format!("{m}_1_{}_1", s.time_field)})
}

/// Validate a measurement and reorder it as time field, meta field, then the rest.
pub fn prepare_insert(s: &TsSpec, mut d: Document) -> Result<Document> {
    match d.get(&s.time_field) {
        Some(Bson::DateTime(_)) => {}
        _ => return Err(Error::new(2, format!("'{}' must be present and contain a valid BSON UTC datetime value", s.time_field))),
    }
    let mut out = Document::new();
    if let Some(t) = d.remove(&s.time_field) {
        out.insert(s.time_field.clone(), t);
    }
    if let Some(m) = &s.meta_field {
        if let Some(v) = d.remove(m) {
            out.insert(m.clone(), v);
        }
    }
    out.extend(d);
    Ok(out)
}

fn meta_path(s: &TsSpec, path: &str) -> bool {
    s.meta_field.as_ref().is_some_and(|m| path == m || path.starts_with(&format!("{m}.")))
}

fn filter_only_meta(s: &TsSpec, f: &Document) -> bool {
    f.iter().all(|(k, v)| match k.as_str() {
        "$and" | "$or" | "$nor" => v.as_array().is_some_and(|a| a.iter().all(|e| e.as_document().is_some_and(|d| filter_only_meta(s, d)))),
        "$comment" => true,
        k if k.starts_with('$') => false,
        k => meta_path(s, k),
    })
}

/// Write restrictions for `update` / `findAndModify` on a time series collection.
pub fn check_update(s: &TsSpec, filter: &Document, update: &Bson, multi: bool, upsert: bool) -> Result<()> {
    if !multi {
        return Err(Error::new(72, "Command error committing internal transaction :: caused by :: Cannot perform a non-multi update on a time-series collection"));
    }
    if upsert {
        return Err(Error::new(72, "Cannot perform an upsert on a time-series collection"));
    }
    let Some(m) = &s.meta_field else {
        return Err(Error::new(72, "Cannot perform an update on a time-series collection that does not have a metaField"));
    };
    if !filter_only_meta(s, filter) {
        return Err(Error::new(72, format!("Cannot perform an update or delete on a time-series collection when querying on a field that is not the metaField '{m}'")));
    }
    let ud = match update {
        Bson::Array(_) => return Err(Error::new(72, "Cannot perform an update on a time-series collection using a pipeline update")),
        Bson::Document(d) => d,
        _ => return Ok(()),
    };
    let not_meta = || Error::new(72, format!("Cannot perform an update on a time-series collection which updates a field that is not the metaField '{m}'"));
    if !ud.keys().next().is_some_and(|k| k.starts_with('$')) {
        return Err(not_meta());
    }
    for (op, args) in ud {
        let Bson::Document(a) = args else { continue };
        for (path, v) in a {
            if !meta_path(s, path) {
                return Err(not_meta());
            }
            if op == "$rename" && !v.as_str().is_some_and(|t| meta_path(s, t)) {
                return Err(not_meta());
            }
        }
    }
    Ok(())
}

/// Index restrictions on time series collections.
pub fn check_index(s: &TsSpec, index: &Document) -> Result<()> {
    let key = index.get_document("key").cloned().unwrap_or_default();
    if index.get_str("name") == Ok("_id_") {
        let parts: Vec<String> = key
            .iter()
            .flat_map(|(k, v)| {
                if meta_path(s, k) {
                    vec![format!("meta{}: {v}", k.strip_prefix(s.meta_field.as_deref().unwrap_or("")).unwrap_or(k))]
                } else {
                    vec![format!("control.min.{k}: {v}"), format!("control.max.{k}: {v}")]
                }
            })
            .collect();
        return Err(Error::new(2, format!("The index name '_id_' is reserved for the _id index, which must have key pattern {{_id: 1}}, found key: {{ {} }}", parts.join(", "))));
    }
    if bsonutil::as_bool(index.get("unique")) {
        return Err(Error::new(72, "Unique indexes are not supported on time-series collections"));
    }
    if bsonutil::as_bool(index.get("sparse")) {
        return Err(Error::new(72, "Sparse indexes are not supported on time-series measurements"));
    }
    if key.values().any(|v| v.as_str() == Some("text")) {
        return Err(Error::new(72, "Text indexes are not supported on time-series collections"));
    }
    if index.contains_key("expireAfterSeconds") && !index.get_document("partialFilterExpression").is_ok_and(|p| p.keys().all(|k| meta_path(s, k))) {
        return Err(Error::new(72, "TTL indexes on time-series collections require a partialFilterExpression on the metaField"));
    }
    Ok(())
}

/// `collMod` of `timeseries` / `expireAfterSeconds`; mutates stored options.
pub fn coll_mod(opts: &mut Document, ts_arg: Option<&Bson>, expire: Option<&Bson>) -> Result<()> {
    let bad = |m: &str| Error::new(72, m.to_string());
    if let Some(arg) = ts_arg {
        let Bson::Document(a) = arg else {
            return Err(Error::type_mismatch("BSON field 'collMod.timeseries' is the wrong type, expected type 'object'"));
        };
        let mut ts = opts.get_document("timeseries").cloned().unwrap_or_default();
        let cur_g = ts.get_str("granularity").ok().map(str::to_string);
        if let Some(g) = a.get_str("granularity").ok() {
            if !matches!(g, "seconds" | "minutes" | "hours") {
                return Err(Error::new(2, format!("Enumeration value '{g}' for field 'collMod.timeseries.granularity' is not a valid value.")));
            }
            let rank = |x: &str| match x {
                "seconds" => 0,
                "minutes" => 1,
                _ => 2,
            };
            match cur_g.as_deref() {
                Some(c) if c == g => {}
                Some(c) if rank(g) == rank(c) + 1 => {
                    let mut n = doc! {"timeField": ts.get("timeField").cloned().unwrap_or(Bson::Null)};
                    if let Some(m) = ts.get("metaField") {
                        n.insert("metaField", m.clone());
                    }
                    n.insert("granularity", g);
                    n.insert("bucketMaxSpanSeconds", granularity_spans(g).0);
                    n.insert("fixedBucketing", false);
                    ts = n;
                }
                _ => return Err(bad("Invalid transition for timeseries.granularity. Can only transition from 'seconds' to 'minutes' or 'minutes' to 'hours'.")),
            }
        }
        let span = a.get("bucketMaxSpanSeconds").and_then(bsonutil::num_i64);
        let round = a.get("bucketRoundingSeconds").and_then(bsonutil::num_i64);
        if span.is_some() || round.is_some() {
            if span != round {
                return Err(bad("Invalid transition for timeseries: 'bucketRoundingSeconds' must be equal to 'bucketMaxSpanSeconds'"));
            }
            let cur_span = ts.get("bucketMaxSpanSeconds").and_then(bsonutil::num_i64).unwrap_or(0);
            let v = span.expect("equal and present");
            if v < cur_span {
                return Err(bad("Invalid transition for timeseries.bucketMaxSpanSeconds, the new value must be greater than or equal to the old value"));
            }
            let mut n = doc! {"timeField": ts.get("timeField").cloned().unwrap_or(Bson::Null)};
            if let Some(m) = ts.get("metaField") {
                n.insert("metaField", m.clone());
            }
            n.insert("bucketRoundingSeconds", v as i32);
            n.insert("bucketMaxSpanSeconds", v as i32);
            n.insert("fixedBucketing", false);
            ts = n;
        }
        opts.insert("timeseries", ts);
    }
    if let Some(e) = expire {
        match e {
            Bson::String(s) if s == "off" => {
                opts.remove("expireAfterSeconds");
            }
            v if bsonutil::is_number(v) => {
                let n = bsonutil::num_i64(v).unwrap_or(0);
                if n < 0 {
                    return Err(Error::new(72, "TTL index 'expireAfterSeconds' option cannot be less than 0"));
                }
                // collMod keeps the key position but stores a long.
                let mut out = Document::new();
                let mut placed = false;
                for (k, x) in std::mem::take(opts) {
                    if k == "timeseries" && !placed {
                        out.insert("expireAfterSeconds", Bson::Int64(n));
                        placed = true;
                    }
                    if k == "expireAfterSeconds" {
                        if !placed {
                            out.insert("expireAfterSeconds", Bson::Int64(n));
                            placed = true;
                        }
                        continue;
                    }
                    out.insert(k, x);
                }
                if !placed {
                    out.insert("expireAfterSeconds", Bson::Int64(n));
                }
                *opts = out;
            }
            other => return Err(Error::type_mismatch(format!("BSON field 'collMod.expireAfterSeconds' is the wrong type '{}'", bsonutil::type_name(other)))),
        }
    }
    Ok(())
}

/// `collStats.timeseries` sub-document.
pub fn coll_stats(ns: &str, count: i64) -> Document {
    // 9.0 stores buckets in the user-facing namespace itself.
    doc! {
        "bucketsNs": ns,
        "bucketCount": count.min(1).max(0) as i32,
        "avgBucketSize": 0,
        "numActiveBuckets": 0,
        "numBucketInserts": 0,
        "numBucketUpdates": 0,
        "numBucketsOpenedDueToMetadata": 0,
        "numBucketsClosedDueToCount": 0,
        "numBucketsClosedDueToSize": 0,
        "numBucketsClosedDueToTimeForward": 0,
        "numBucketsClosedDueToTimeBackward": 0,
        "numBucketsClosedDueToMemoryThreshold": 0,
        "numCommits": 0,
        "numWaits": 0,
        "numMeasurementsCommitted": 0,
    }
}

/// Documents older than `expireAfterSeconds` (TTL monitor filter).
pub fn ttl_filter(options: &Document, now_ms: i64) -> Option<Document> {
    let s = spec(options)?;
    let secs = options.get("expireAfterSeconds").and_then(bsonutil::num_i64)?;
    Some(doc! {s.time_field.as_str(): {"$lt": Bson::DateTime(bson::DateTime::from_millis(now_ms - secs * 1000))}})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_and_orders() {
        let mut o = doc! {"timeseries": {"timeField": "t", "metaField": "m", "granularity": "hours"}, "expireAfterSeconds": 60};
        normalize_create("d.c", &mut o).unwrap();
        assert_eq!(o, doc! {"timeseries": {"timeField": "t", "metaField": "m", "granularity": "hours", "bucketMaxSpanSeconds": 2592000, "fixedBucketing": true}, "expireAfterSeconds": 60_i64});
        let s = spec(&o).unwrap();
        let d = prepare_insert(&s, doc! {"_id": 1, "v": 2, "m": 3, "t": bson::DateTime::from_millis(0)}).unwrap();
        assert_eq!(d.keys().map(String::as_str).collect::<Vec<_>>(), ["t", "m", "_id", "v"]);
        assert_eq!(prepare_insert(&s, doc! {"t": 1}).unwrap_err().code, 2);

    }

    #[test]
    fn update_rules() {
        let s = TsSpec { time_field: "t".into(), meta_field: Some("m".into()) };
        assert!(check_update(&s, &doc! {"m.a": 1}, &Bson::Document(doc! {"$set": {"m.b": 1}}), true, false).is_ok());
        assert_eq!(check_update(&s, &doc! {"v": 1}, &Bson::Document(doc! {"$set": {"m": 1}}), true, false).unwrap_err().code, 72);
        assert!(check_update(&s, &doc! {}, &Bson::Document(doc! {"$set": {"v": 1}}), true, false).unwrap_err().msg.contains("updates a field"));
    }
}
