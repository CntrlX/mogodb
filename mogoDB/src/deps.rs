//! Field dependency analysis: which top-level fields a filter or pipeline reads.
//!
//! Scans decode only these fields from the raw BSON bytes (zero-copy skip over the
//! rest) and fully decode a document only when it is actually needed. `None` means
//! "the whole document is required" (e.g. `$$ROOT`, `$text`, unknown stages).

use bson::{Bson, Document};
use std::collections::HashSet;

pub type Fields = HashSet<String>;

fn first_seg(p: &str) -> &str {
    p.split('.').next().unwrap_or(p)
}

/// Top-level fields read by a query filter.
pub fn filter_fields(f: &Document) -> Option<Fields> {
    let mut out = Fields::new();
    collect_filter(f, &mut out).then_some(out)
}

fn collect_filter(f: &Document, out: &mut Fields) -> bool {
    for (k, v) in f {
        match k.as_str() {
            "$and" | "$or" | "$nor" => {
                let Bson::Array(items) = v else { return false };
                for it in items {
                    let Bson::Document(d) = it else { return false };
                    if !collect_filter(d, out) {
                        return false;
                    }
                }
            }
            "$comment" => {}
            "$expr" => {
                if !collect_expr(v, out) {
                    return false;
                }
            }
            k if k.starts_with('$') => return false,
            k => {
                out.insert(first_seg(k).to_string());
            }
        }
    }
    true
}

/// Fields referenced by an aggregation expression.
pub fn collect_expr(v: &Bson, out: &mut Fields) -> bool {
    match v {
        Bson::String(s) if s.starts_with("$$") => {
            let body = &s[2..];
            let (var, rest) = body.split_once('.').unwrap_or((body, ""));
            if var == "ROOT" || var == "CURRENT" {
                if rest.is_empty() {
                    return false;
                }
                out.insert(first_seg(rest).to_string());
            }
            true
        }
        Bson::String(s) if s.starts_with('$') => {
            out.insert(first_seg(&s[1..]).to_string());
            true
        }
        Bson::Array(a) => a.iter().all(|e| collect_expr(e, out)),
        Bson::Document(d) => {
            for (k, val) in d {
                match k.as_str() {
                    "$literal" => {}
                    // Implicit $$CURRENT access by field name.
                    "$getField" | "$setField" | "$unsetField" => return false,
                    _ => {
                        if !collect_expr(val, out) {
                            return false;
                        }
                    }
                }
            }
            true
        }
        _ => true,
    }
}

fn sort_keys(spec: &Bson, out: &mut Fields) -> bool {
    match spec {
        Bson::Document(d) => {
            for k in d.keys() {
                out.insert(first_seg(k).to_string());
            }
            true
        }
        _ => false,
    }
}

fn accumulators(spec: &Document, skip: &[&str], out: &mut Fields) -> bool {
    for (k, v) in spec {
        if skip.contains(&k.as_str()) {
            continue;
        }
        let Bson::Document(acc) = v else { return false };
        for (_, arg) in acc {
            if let Bson::Document(a) = arg {
                if let Some(s) = a.get("sortBy") {
                    if !sort_keys(s, out) {
                        return false;
                    }
                }
            }
            if !collect_expr(arg, out) {
                return false;
            }
        }
    }
    true
}

fn project_fields(spec: &Document, out: &mut Fields) -> Option<bool> {
    // Returns Some(true) if this is an inclusion/computed projection (shape reset).
    let mut include_id = true;
    let mut inclusion = false;
    for (k, v) in spec {
        let is_flag = matches!(v, Bson::Boolean(_) | Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_));
        if is_flag {
            let on = crate::bsonutil::truthy(v);
            if k == "_id" {
                include_id = on;
                continue;
            }
            if !on {
                return Some(false);
            }
            inclusion = true;
            out.insert(first_seg(k).to_string());
        } else {
            inclusion = true;
            if let Bson::Document(d) = v {
                if !d.keys().any(|x| x.starts_with('$')) {
                    // Nested projection spec rooted at this field.
                    out.insert(first_seg(k).to_string());
                    let mut inner = Fields::new();
                    if !collect_expr(v, &mut inner) {
                        return None;
                    }
                    out.extend(inner);
                    continue;
                }
            }
            if !collect_expr(v, out) {
                return None;
            }
        }
    }
    if include_id {
        out.insert("_id".to_string());
    }
    Some(inclusion)
}

/// Top-level source fields needed by `stages`, or `None` if whole documents are required.
pub fn pipeline_fields(stages: &[Document]) -> Option<Fields> {
    let mut out = Fields::new();
    for s in stages {
        let (name, arg) = s.iter().next()?;
        if s.len() != 1 {
            return None;
        }
        match name.as_str() {
            "$match" => {
                let Bson::Document(f) = arg else { return None };
                if !collect_filter(f, &mut out) {
                    return None;
                }
            }
            "$sort" => {
                if !sort_keys(arg, &mut out) {
                    return None;
                }
            }
            "$limit" | "$skip" | "$sample" | "$unset" => {}
            "$unwind" => match arg {
                Bson::String(p) => {
                    out.insert(first_seg(p.trim_start_matches('$')).to_string());
                }
                Bson::Document(d) => {
                    out.insert(first_seg(d.get_str("path").ok()?.trim_start_matches('$')).to_string());
                }
                _ => return None,
            },
            "$addFields" | "$set" => {
                let Bson::Document(d) = arg else { return None };
                for v in d.values() {
                    if !collect_expr(v, &mut out) {
                        return None;
                    }
                }
            }
            "$lookup" => {
                let Bson::Document(d) = arg else { return None };
                if let Ok(lf) = d.get_str("localField") {
                    out.insert(first_seg(lf).to_string());
                }
                if let Ok(l) = d.get_document("let") {
                    for v in l.values() {
                        if !collect_expr(v, &mut out) {
                            return None;
                        }
                    }
                }
            }
            "$group" => {
                let Bson::Document(d) = arg else { return None };
                if !collect_expr(d.get("_id").unwrap_or(&Bson::Null), &mut out) || !accumulators(d, &["_id"], &mut out) {
                    return None;
                }
                return Some(out);
            }
            "$bucket" | "$bucketAuto" => {
                let Bson::Document(d) = arg else { return None };
                if !collect_expr(d.get("groupBy")?, &mut out) {
                    return None;
                }
                if let Ok(o) = d.get_document("output") {
                    if !accumulators(o, &[], &mut out) {
                        return None;
                    }
                }
                return Some(out);
            }
            "$sortByCount" => {
                return collect_expr(arg, &mut out).then_some(out);
            }
            "$count" => return Some(out),
            "$replaceRoot" => {
                let Bson::Document(d) = arg else { return None };
                return collect_expr(d.get("newRoot")?, &mut out).then_some(out);
            }
            "$replaceWith" => return collect_expr(arg, &mut out).then_some(out),
            "$project" => {
                let Bson::Document(d) = arg else { return None };
                match project_fields(d, &mut out)? {
                    true => return Some(out),
                    false => return None,
                }
            }
            "$facet" => {
                let Bson::Document(d) = arg else { return None };
                for sub in d.values() {
                    let Bson::Array(a) = sub else { return None };
                    let subs: Vec<Document> = a.iter().filter_map(|x| x.as_document().cloned()).collect();
                    out.extend(pipeline_fields(&subs)?);
                }
                return Some(out);
            }
            _ => return None,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn set(v: &[&str]) -> Fields {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn filters() {
        assert_eq!(filter_fields(&doc! {"a.b": 1, "$or": [{"c": 1}, {"d": {"$gt": 1}}]}), Some(set(&["a", "c", "d"])));
        assert_eq!(filter_fields(&doc! {"$expr": {"$gt": ["$x.y", "$$ROOT.z"]}}), Some(set(&["x", "z"])));
        assert_eq!(filter_fields(&doc! {"$text": {"$search": "x"}}), None);
        assert_eq!(filter_fields(&doc! {"$expr": {"$eq": ["$$ROOT", {}]}}), None);
    }

    #[test]
    fn pipelines() {
        let p = vec![doc! {"$match": {"a": 1}}, doc! {"$sort": {"b": 1}}, doc! {"$group": {"_id": "$c", "t": {"$sum": "$d"}, "top": {"$top": {"sortBy": {"e": 1}, "output": "$f"}}}}];
        assert_eq!(pipeline_fields(&p), Some(set(&["a", "b", "c", "d", "e", "f"])));
        assert_eq!(pipeline_fields(&[doc! {"$project": {"x": 1, "y": "$z.w"}}]), Some(set(&["x", "z", "_id"])));
        assert_eq!(pipeline_fields(&[doc! {"$project": {"x": 0}}]), None);
        assert_eq!(pipeline_fields(&[doc! {"$addFields": {"x": 1}}]), None);
        assert_eq!(pipeline_fields(&[doc! {"$replaceWith": "$$ROOT"}]), None);
        assert_eq!(pipeline_fields(&[doc! {"$count": "n"}]), Some(Fields::new()));
    }
}
