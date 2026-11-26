//! Sort specifications with MongoDB array/missing semantics.
use crate::bsonutil::{cmp_bson, num_f64, META_KEY};
use crate::error::{Error, Result};
use bson::{Bson, Document};
use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq)]
pub enum SortKey {
    Asc,
    Desc,
    /// `{$meta: "textScore"}` (descending by score)
    TextScore,
}

#[derive(Clone, Debug)]
pub struct SortSpec {
    pub keys: Vec<(String, SortKey)>,
}

impl SortSpec {
    pub fn new(spec: &Document) -> Result<SortSpec> {
        if spec.is_empty() {
            return Err(Error::new(15976, "$sort stage must have at least one sort key"));
        }
        let mut keys = Vec::with_capacity(spec.len());
        for (path, v) in spec {
            let kind = match v {
                Bson::Document(d) if is_text_score_meta(d) => SortKey::TextScore,
                Bson::Document(_) => {
                    return Err(Error::new(15974, format!("Illegal key in $sort specification: {path}: {v}")));
                }
                other => match num_f64(other) {
                    Some(n) if n == 1.0 => SortKey::Asc,
                    Some(n) if n == -1.0 => SortKey::Desc,
                    _ => {
                        return Err(Error::new(
                            15975,
                            "$sort key ordering must be 1 (for ascending) or -1 (for descending)",
                        ));
                    }
                },
            };
            keys.push((path.clone(), kind));
        }
        Ok(SortSpec { keys })
    }

    /// Extract one key per sort field (array semantics applied).
    pub fn sort_key(&self, doc: &Document) -> Vec<Bson> {
        self.keys
            .iter()
            .map(|(path, kind)| match kind {
                SortKey::TextScore => Bson::Double(text_score(doc)),
                SortKey::Asc | SortKey::Desc => {
                    let segs: Vec<&str> = path.split('.').collect();
                    let mut cands = Vec::new();
                    gather_doc(doc, &segs, &mut cands);
                    pick(cands, *kind == SortKey::Asc)
                }
            })
            .collect()
    }

    pub fn compare_keys(&self, a: &[Bson], b: &[Bson]) -> Ordering {
        for ((_, kind), (x, y)) in self.keys.iter().zip(a.iter().zip(b.iter())) {
            let ord = cmp_bson(x, y);
            let ord = if *kind == SortKey::Asc { ord } else { ord.reverse() };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    pub fn compare(&self, a: &Document, b: &Document) -> Ordering {
        self.compare_keys(&self.sort_key(a), &self.sort_key(b))
    }
}

fn is_text_score_meta(d: &Document) -> bool {
    d.len() == 1 && d.get_str("$meta") == Ok("textScore")
}

fn text_score(doc: &Document) -> f64 {
    doc.get_document(META_KEY)
        .ok()
        .and_then(|m| m.get("textScore"))
        .and_then(num_f64)
        .unwrap_or(0.0)
}

/// Choose the sort key from candidates: smallest for ascending, largest for
/// descending. A path that reaches nothing sorts as null.
fn pick(cands: Vec<Bson>, asc: bool) -> Bson {
    let want = if asc { Ordering::Less } else { Ordering::Greater };
    let mut best: Option<Bson> = None;
    for c in cands {
        let replace = match &best {
            None => true,
            Some(b) => cmp_bson(&c, b) == want,
        };
        if replace {
            best = Some(c);
        }
    }
    best.unwrap_or(Bson::Null)
}

/// Collect every value reached by `segs` from a document. A missing field contributes
/// null (so `{a: [{b: 5}, {}]}` has sort candidates `{5, null}` for `a.b`, as in MongoDB).
fn gather_doc(doc: &Document, segs: &[&str], out: &mut Vec<Bson>) {
    match segs.split_first() {
        None => out.push(Bson::Document(doc.clone())),
        Some((first, rest)) => match doc.get(*first) {
            Some(v) => gather_val(v, rest, out),
            None => out.push(Bson::Null),
        },
    }
}

fn gather_val(v: &Bson, segs: &[&str], out: &mut Vec<Bson>) {
    match (v, segs.is_empty()) {
        // A leaf array sorts by its elements; an empty array sorts as undefined.
        (Bson::Array(items), true) => {
            if items.is_empty() {
                out.push(Bson::Undefined);
            } else {
                out.extend(items.iter().cloned());
            }
        }
        (_, true) => out.push(v.clone()),
        (Bson::Document(d), false) => gather_doc(d, segs, out),
        (Bson::Array(items), false) => gather_array(items, segs, out),
        // A scalar cannot contain the remaining path: missing.
        (_, false) => {}
    }
}

/// Fan out over an array: a numeric segment indexes an element, and the
/// segment also matches the field of each document element. Nested arrays are
/// not traversed.
fn gather_array(items: &[Bson], segs: &[&str], out: &mut Vec<Bson>) {
    if let Some(e) = segs[0].parse::<usize>().ok().and_then(|i| items.get(i)) {
        // Positional path: only elements that actually have the named field also count.
        gather_val(e, &segs[1..], out);
        for e in items {
            if let Bson::Document(d) = e {
                if d.contains_key(segs[0]) {
                    gather_doc(d, segs, out);
                }
            }
        }
        return;
    }
    for e in items {
        if let Bson::Document(d) = e {
            gather_doc(d, segs, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn spec(d: Document) -> SortSpec {
        SortSpec::new(&d).expect("valid sort spec")
    }

    fn key1(s: &SortSpec, d: &Document) -> Bson {
        s.sort_key(d).remove(0)
    }

    #[test]
    fn asc_takes_min_and_desc_takes_max_over_array() {
        let d = doc! { "a": [3, 1, 2] };
        assert!(bson_eq_test(&key1(&spec(doc! { "a": 1 }), &d), &Bson::Int32(1)));
        assert!(bson_eq_test(&key1(&spec(doc! { "a": -1 }), &d), &Bson::Int32(3)));
    }

    #[test]
    fn empty_array_sorts_below_null_and_missing() {
        let s = spec(doc! { "a": 1 });
        let empty = doc! { "a": [] };
        let null = doc! { "a": Bson::Null };
        let missing = doc! {};
        assert_eq!(s.compare(&empty, &missing), Ordering::Less);
        assert_eq!(s.compare(&empty, &null), Ordering::Less);
        assert_eq!(s.compare(&missing, &null), Ordering::Equal);
    }

    #[test]
    fn nested_path_fans_out_over_document_elements() {
        let d = doc! { "a": [ { "b": 5 }, { "b": 2 } ] };
        assert!(bson_eq_test(&key1(&spec(doc! { "a.b": 1 }), &d), &Bson::Int32(2)));
        assert!(bson_eq_test(&key1(&spec(doc! { "a.b": -1 }), &d), &Bson::Int32(5)));
    }

    #[test]
    fn numeric_segment_indexes_array_element() {
        let d = doc! { "a": [ { "b": 1 }, { "b": 9 } ] };
        assert!(bson_eq_test(&key1(&spec(doc! { "a.1.b": 1 }), &d), &Bson::Int32(9)));
    }

    #[test]
    fn missing_key_equals_null_key() {
        let s = spec(doc! { "x": 1 });
        assert_eq!(s.compare(&doc! {}, &doc! { "x": Bson::Null }), Ordering::Equal);
    }

    #[test]
    fn desc_reverses_and_later_keys_break_ties() {
        let s = spec(doc! { "x": 1, "y": -1 });
        let a = doc! { "x": 1, "y": 5 };
        let b = doc! { "x": 1, "y": 7 };
        // Ties on x; y descending puts 7 before 5.
        assert_eq!(s.compare(&a, &b), Ordering::Greater);
        assert_eq!(s.compare(&doc! { "x": 1 }, &doc! { "x": 2 }), Ordering::Less);
    }

    #[test]
    fn text_score_sorts_higher_first() {
        let s = spec(doc! { "s": { "$meta": "textScore" } });
        assert_eq!(s.keys[0].1, SortKey::TextScore);
        let high = doc! { META_KEY: { "textScore": 0.9 } };
        let low = doc! { META_KEY: { "textScore": 0.5 } };
        assert_eq!(s.compare(&high, &low), Ordering::Less);
    }

    #[test]
    fn invalid_specs_return_mongodb_codes() {
        assert_eq!(SortSpec::new(&doc! {}).unwrap_err().code, 15976);
        assert_eq!(SortSpec::new(&doc! { "a": 0 }).unwrap_err().code, 15975);
        assert_eq!(SortSpec::new(&doc! { "a": "x" }).unwrap_err().code, 15975);
        assert_eq!(SortSpec::new(&doc! { "a": { "$meta": "randVal" } }).unwrap_err().code, 15974);
    }

    fn bson_eq_test(a: &Bson, b: &Bson) -> bool {
        cmp_bson(a, b) == Ordering::Equal
    }
}
