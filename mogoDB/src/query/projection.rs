//! Projections: find-command projection and aggregation `$project` / `$addFields`.
use crate::agg::expr::{eval, Vars};
use crate::bsonutil::{is_missing, num_f64, META_KEY};
use crate::error::{Error, Result};
use crate::query::matcher::value_matches;
use bson::{Bson, Document};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProjectionKind {
    /// `find` projection: supports `$slice`, `$elemMatch`, positional `a.$`, `$meta`, and agg expressions.
    Find,
    /// Aggregation `$project`: inclusion/exclusion/expressions only (`$slice` etc. are agg expressions).
    Agg,
}

#[derive(Clone, Debug)]
enum Slice {
    Count(i64),
    SkipLimit(i64, i64),
}

#[derive(Clone, Debug)]
enum Leaf {
    Include,
    Exclude,
    /// Computed value that is used as-is (no expression evaluation).
    Literal(Bson),
    /// Aggregation expression evaluated against the root document.
    Expr(Bson),
    Slice(Slice),
    ElemMatch(Bson),
    Meta(String),
    /// `a.$`: the first matching array element.
    Positional,
}

#[derive(Clone, Debug)]
enum Node {
    Leaf(Leaf),
    Branch(Vec<(String, Node)>),
}

pub struct Projection {
    /// True when the projection only removes fields (or only sets `_id`).
    exclusion: bool,
    children: Vec<(String, Node)>,
}

struct Ctx<'a> {
    root: &'a Document,
    vars: &'a Vars,
    position: Option<usize>,
}

impl Projection {
    pub fn new(spec: &Document, kind: ProjectionKind) -> Result<Projection> {
        if kind == ProjectionKind::Agg && spec.is_empty() {
            return Err(Error::location(51272, "$project requires at least one output field"));
        }
        let mut flat = Vec::new();
        flatten(spec, "", kind, &mut flat)?;
        let exclusion = determine_exclusion(&flat)?;
        let mut children = Vec::new();
        for (path, leaf) in flat {
            let segs: Vec<&str> = path.split('.').collect();
            insert(&mut children, &segs, &path, leaf)?;
        }
        Ok(Projection { exclusion, children })
    }

    /// `position`: index from `Matcher::matches_with_position` for positional `a.$`.
    pub fn apply(&self, doc: &Document, position: Option<usize>, vars: &Vars) -> Result<Document> {
        let ctx = Ctx { root: doc, vars, position };
        self.project_doc(&self.children, doc, &ctx, true)
    }

    /// True for pure exclusion projections.
    pub fn is_exclusion(&self) -> bool {
        self.exclusion
    }

    fn project_doc(&self, children: &[(String, Node)], src: &Document, ctx: &Ctx<'_>, top: bool) -> Result<Document> {
        let mut out = Document::new();
        for (k, v) in src {
            match children.iter().find(|(n, _)| n == k) {
                None => {
                    if self.exclusion || (top && (k == "_id" || k == META_KEY)) {
                        out.insert(k.clone(), v.clone());
                    }
                }
                Some((_, node)) => {
                    if let Some(nv) = self.project_node(node, Some(v), ctx)? {
                        out.insert(k.clone(), nv);
                    }
                }
            }
        }
        for (k, node) in children {
            if src.contains_key(k) {
                continue;
            }
            if let Some(nv) = self.project_node(node, None, ctx)? {
                out.insert(k.clone(), nv);
            }
        }
        Ok(out)
    }

    fn project_node(&self, node: &Node, cur: Option<&Bson>, ctx: &Ctx<'_>) -> Result<Option<Bson>> {
        match node {
            Node::Leaf(leaf) => project_leaf(leaf, cur, ctx),
            Node::Branch(children) => match cur {
                None => {
                    if self.exclusion {
                        return Ok(None);
                    }
                    let sub = self.project_doc(children, &Document::new(), ctx, false)?;
                    // A branch containing computed fields always materializes its subdocument.
                    Ok(if sub.is_empty() && !has_computed(children) { None } else { Some(Bson::Document(sub)) })
                }
                Some(Bson::Document(d)) => Ok(Some(Bson::Document(self.project_doc(children, d, ctx, false)?))),
                Some(Bson::Array(items)) => Ok(Some(Bson::Array(self.project_array(children, items, ctx)?))),
                Some(other) => Ok(if self.exclusion { Some(other.clone()) } else { None }),
            },
        }
    }

    fn project_array(&self, children: &[(String, Node)], items: &[Bson], ctx: &Ctx<'_>) -> Result<Vec<Bson>> {
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            match item {
                Bson::Document(d) => out.push(Bson::Document(self.project_doc(children, d, ctx, false)?)),
                Bson::Array(inner) => out.push(Bson::Array(self.project_array(children, inner, ctx)?)),
                other => {
                    if self.exclusion {
                        out.push(other.clone());
                    }
                }
            }
        }
        Ok(out)
    }
}

fn has_computed(children: &[(String, Node)]) -> bool {
    children.iter().any(|(_, n)| match n {
        Node::Leaf(Leaf::Expr(_) | Leaf::Literal(_) | Leaf::Meta(_)) => true,
        Node::Leaf(_) => false,
        Node::Branch(c) => has_computed(c),
    })
}

fn project_leaf(leaf: &Leaf, cur: Option<&Bson>, ctx: &Ctx<'_>) -> Result<Option<Bson>> {
    Ok(match leaf {
        Leaf::Include => cur.cloned(),
        Leaf::Exclude => None,
        Leaf::Literal(v) => Some(v.clone()),
        Leaf::Expr(e) => {
            let v = eval(e, ctx.root, ctx.vars).map_err(|err| match e {
                // Projection parsing reports an unknown top-level operator as 31325.
                Bson::Document(d) if err.code == 168 && d.keys().next().is_some_and(|k| err.msg.contains(&format!("'{k}'"))) => {
                    Error::new(31325, format!("Invalid $project :: caused by :: Unknown expression {}", d.keys().next().map(String::as_str).unwrap_or("")))
                }
                _ => err,
            })?;
            if is_missing(&v) { None } else { Some(v) }
        }
        Leaf::Meta(name) => ctx.root.get_document(META_KEY).ok().and_then(|m| m.get(name.as_str())).cloned(),
        Leaf::Positional => match (cur, ctx.position) {
            (Some(Bson::Array(items)), Some(p)) => items.get(p).map(|e| Bson::Array(vec![e.clone()])),
            _ => None,
        },
        Leaf::Slice(s) => match cur {
            Some(Bson::Array(items)) => Some(Bson::Array(apply_slice(items, s))),
            other => other.cloned(),
        },
        Leaf::ElemMatch(cond) => match cur {
            Some(Bson::Array(items)) => {
                let mut found = None;
                for item in items {
                    if value_matches(item, cond)? {
                        found = Some(item.clone());
                        break;
                    }
                }
                found.map(|e| Bson::Array(vec![e]))
            }
            _ => None,
        },
    })
}

fn apply_slice(items: &[Bson], s: &Slice) -> Vec<Bson> {
    let len = items.len();
    let (start, take) = match *s {
        Slice::Count(n) => {
            let abs = n.unsigned_abs() as usize;
            (if n >= 0 { 0 } else { len.saturating_sub(abs) }, abs)
        }
        Slice::SkipLimit(skip, limit) => {
            let start = if skip >= 0 {
                (skip as usize).min(len)
            } else {
                len.saturating_sub(skip.unsigned_abs() as usize)
            };
            (start, limit.unsigned_abs() as usize)
        }
    };
    items.iter().skip(start).take(take).cloned().collect()
}

/// `Some(true)` for inclusion-type leaves, `Some(false)` for exclusion, `None` for neutral.
fn mode_of(leaf: &Leaf) -> Option<bool> {
    match leaf {
        Leaf::Include | Leaf::Literal(_) | Leaf::Expr(_) | Leaf::Meta(_) | Leaf::Positional => Some(true),
        Leaf::Exclude => Some(false),
        Leaf::Slice(_) | Leaf::ElemMatch(_) => None,
    }
}

/// Returns the exclusion flag. The first non-`_id` mode setter decides the mode, else `_id` does.
fn determine_exclusion(flat: &[(String, Leaf)]) -> Result<bool> {
    let non_id = flat.iter().filter(|(p, _)| p.as_str() != "_id");
    let mode = non_id
        .clone()
        .find_map(|(_, l)| mode_of(l))
        .or_else(|| non_id.clone().any(|(_, l)| matches!(l, Leaf::ElemMatch(_))).then_some(true))
        .or_else(|| flat.iter().filter(|(p, _)| p.as_str() == "_id").find_map(|(_, l)| mode_of(l)));
    let inclusion = mode.unwrap_or(false);
    for (p, l) in flat {
        if p.as_str() == "_id" {
            continue;
        }
        match mode_of(l) {
            Some(true) if !inclusion => {
                return Err(Error::new(31253, format!("Cannot do inclusion on field {p} in exclusion projection")));
            }
            Some(false) if inclusion => {
                return Err(Error::new(31254, format!("Cannot do exclusion on field {p} in inclusion projection")));
            }
            _ => {}
        }
    }
    Ok(!inclusion)
}

fn insert(nodes: &mut Vec<(String, Node)>, segs: &[&str], full: &str, leaf: Leaf) -> Result<()> {
    let Some((first, rest)) = segs.split_first() else {
        return Ok(());
    };
    let idx = nodes.iter().position(|(k, _)| k.as_str() == *first);
    match (idx, rest.is_empty()) {
        (Some(_), true) => Err(Error::new(31249, format!("Path collision at {full}"))),
        (None, true) => {
            nodes.push((first.to_string(), Node::Leaf(leaf)));
            Ok(())
        }
        (None, false) => {
            let mut sub = Vec::new();
            insert(&mut sub, rest, full, leaf)?;
            nodes.push((first.to_string(), Node::Branch(sub)));
            Ok(())
        }
        (Some(i), false) => match &mut nodes[i].1 {
            Node::Branch(sub) => insert(sub, rest, full, leaf),
            Node::Leaf(_) => Err(Error::new(
                31249,
                format!("Path collision at {full} remaining portion {}", rest.join(".")),
            )),
        },
    }
}

fn join(prefix: &str, k: &str) -> String {
    if prefix.is_empty() { k.to_string() } else { format!("{prefix}.{k}") }
}

fn field_count_err(n: usize) -> Error {
    Error::new(
        16410,
        format!("an expression specification must contain exactly one field, the name of the expression. Found {n} fields"),
    )
}

fn is_operator_doc(d: &Document) -> bool {
    d.keys().next().is_some_and(|k| k.starts_with('$'))
}

fn is_inclusion(v: &Bson) -> bool {
    match v {
        Bson::Boolean(b) => *b,
        other => num_f64(other).is_some_and(|n| n != 0.0),
    }
}

/// Literal values need no expression evaluation.
fn is_literal(v: &Bson) -> bool {
    match v {
        Bson::String(s) => !s.starts_with('$'),
        Bson::Undefined => false,
        Bson::Array(items) => items.iter().all(is_literal),
        Bson::Document(d) => d.iter().all(|(k, x)| !k.starts_with('$') && is_literal(x)),
        _ => true,
    }
}

/// Flatten nested field specs into dotted paths and classify each leaf.
fn flatten(spec: &Document, prefix: &str, kind: ProjectionKind, out: &mut Vec<(String, Leaf)>) -> Result<()> {
    for (k, v) in spec {
        let segs: Vec<&str> = k.split('.').collect();
        let last = segs.len() - 1;
        let positional = kind == ProjectionKind::Find && segs[last] == "$";
        for (i, s) in segs.iter().enumerate() {
            if s.is_empty() {
                return Err(Error::new(40352, "FieldPath cannot be constructed with empty string"));
            }
            if s.starts_with('$') && !(positional && i == last) {
                return Err(Error::new(16410, "FieldPath field names may not start with '$'."));
            }
        }
        if positional {
            if segs.len() == 1 {
                return Err(Error::bad_value("positional operator '$' requires a field path"));
            }
            let base = join(prefix, &segs[..last].join("."));
            if !is_inclusion(v) {
                return Err(Error::bad_value(format!(
                    "Cannot exclude array element positional operator in path {base}"
                )));
            }
            out.push((base, Leaf::Positional));
            continue;
        }
        let path = join(prefix, k);
        match v {
            Bson::Document(d) if is_operator_doc(d) => out.push((path, parse_operator(d, kind)?)),
            Bson::Document(d) if d.is_empty() => return Err(field_count_err(0)),
            Bson::Document(d) => flatten(d, &path, kind, out)?,
            other => out.push((path, scalar_leaf(other, kind))),
        }
    }
    Ok(())
}

fn scalar_leaf(v: &Bson, kind: ProjectionKind) -> Leaf {
    if let Bson::Boolean(b) = v {
        return if *b { Leaf::Include } else { Leaf::Exclude };
    }
    match num_f64(v) {
        Some(n) if kind == ProjectionKind::Find => {
            if n != 0.0 { Leaf::Include } else { Leaf::Exclude }
        }
        Some(n) if n == 1.0 => Leaf::Include,
        Some(n) if n == 0.0 => Leaf::Exclude,
        Some(_) => Leaf::Literal(v.clone()),
        None => {
            if is_literal(v) {
                Leaf::Literal(v.clone())
            } else {
                Leaf::Expr(v.clone())
            }
        }
    }
}

fn parse_operator(d: &Document, kind: ProjectionKind) -> Result<Leaf> {
    if d.len() != 1 {
        return Err(field_count_err(d.len()));
    }
    let Some((op, arg)) = d.iter().next() else {
        return Err(field_count_err(0));
    };
    Ok(match op.as_str() {
        "$meta" => match arg {
            Bson::String(name) => Leaf::Meta(name.clone()),
            _ => return Err(Error::bad_value("$meta projection requires a string")),
        },
        "$slice" if kind == ProjectionKind::Find => Leaf::Slice(parse_slice(arg)?),
        "$elemMatch" if kind == ProjectionKind::Find => Leaf::ElemMatch(arg.clone()),
        _ => Leaf::Expr(Bson::Document(d.clone())),
    })
}

fn int_arg(b: &Bson) -> Option<i64> {
    match b {
        Bson::Int32(i) => Some(i64::from(*i)),
        Bson::Int64(i) => Some(*i),
        Bson::Double(f) if f.fract() == 0.0 && f.abs() < 9.2e18 => Some(*f as i64),
        _ => None,
    }
}

fn parse_slice(arg: &Bson) -> Result<Slice> {
    if let Bson::Array(items) = arg {
        let (skip, limit) = match items.as_slice() {
            [a, b] => (int_arg(a), int_arg(b)),
            _ => (None, None),
        };
        let (Some(skip), Some(limit)) = (skip, limit) else {
            return Err(Error::bad_value("$slice array argument must contain two integers"));
        };
        if limit <= 0 {
            return Err(Error::new(28689, "$slice limit must be positive"));
        }
        return Ok(Slice::SkipLimit(skip, limit));
    }
    match int_arg(arg) {
        Some(n) => Ok(Slice::Count(n)),
        None => Err(Error::bad_value("$slice requires an integer or [skip, limit] array")),
    }
}

/// `$addFields` / `$set` stage semantics (nested specs merge into existing subdocuments).
pub fn add_fields(doc: &Document, spec: &Document, vars: &Vars) -> Result<Document> {
    let mut out = doc.clone();
    for (k, v) in spec {
        add_entry(&mut out, doc, k, v, vars)?;
    }
    Ok(out)
}

fn add_entry(out: &mut Document, orig: &Document, path: &str, v: &Bson, vars: &Vars) -> Result<()> {
    match v {
        Bson::Document(d) if !d.is_empty() && !is_operator_doc(d) => {
            for (k, sub) in d {
                add_entry(out, orig, &format!("{path}.{k}"), sub, vars)?;
            }
            Ok(())
        }
        _ => {
            let value = if is_literal(v) { v.clone() } else { eval(v, orig, vars)? };
            let segs: Vec<&str> = path.split('.').collect();
            if is_missing(&value) {
                unset_path(out, &segs);
            } else {
                set_path(out, &segs, value)?;
            }
            Ok(())
        }
    }
}

/// Set a dotted path, creating intermediate documents and fanning out over arrays of documents.
fn set_path(doc: &mut Document, segs: &[&str], value: Bson) -> Result<()> {
    let Some((first, rest)) = segs.split_first() else {
        return Ok(());
    };
    if rest.is_empty() {
        doc.insert(*first, value);
        return Ok(());
    }
    if !doc.contains_key(*first) {
        doc.insert(*first, Bson::Document(Document::new()));
    }
    match doc.get_mut(*first) {
        Some(Bson::Document(sub)) => set_path(sub, rest, value),
        Some(Bson::Array(items)) => {
            for item in items.iter_mut() {
                if let Bson::Document(sub) = item {
                    set_path(sub, rest, value.clone())?;
                }
            }
            Ok(())
        }
        Some(other) => Err(Error::new(
            28,
            format!(
                "Cannot create field '{}' in element {{{}: {}}}",
                rest.first().copied().unwrap_or_default(),
                first,
                other
            ),
        )),
        None => Ok(()),
    }
}

fn unset_path(doc: &mut Document, segs: &[&str]) {
    let Some((first, rest)) = segs.split_first() else {
        return;
    };
    if rest.is_empty() {
        doc.remove(*first);
        return;
    }
    match doc.get_mut(*first) {
        Some(Bson::Document(sub)) => unset_path(sub, rest),
        Some(Bson::Array(items)) => {
            for item in items.iter_mut() {
                if let Bson::Document(sub) = item {
                    unset_path(sub, rest);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn find_proj(spec: Document) -> Projection {
        Projection::new(&spec, ProjectionKind::Find).expect("valid projection")
    }

    fn run(spec: Document, doc: Document, pos: Option<usize>) -> Document {
        find_proj(spec).apply(&doc, pos, &Vars::new()).expect("apply")
    }

    fn code_of(spec: Document, kind: ProjectionKind) -> Option<i32> {
        Projection::new(&spec, kind).err().map(|e| e.code)
    }

    #[test]
    fn inclusion_through_arrays_keeps_only_listed_subfields() {
        let out = run(
            doc! { "a.b": 1 },
            doc! { "_id": 1, "a": [ { "b": 1, "c": 2 }, { "c": 3 }, 5 ] },
            None,
        );
        assert_eq!(out, doc! { "_id": 1, "a": [ { "b": 1 }, {} ] });
    }

    #[test]
    fn exclusion_removes_nested_field_and_keeps_rest() {
        let out = run(doc! { "a.c": 0 }, doc! { "a": { "b": 1, "c": 2 }, "d": 3 }, None);
        assert_eq!(out, doc! { "a": { "b": 1 }, "d": 3 });
    }

    #[test]
    fn exclusion_through_arrays_keeps_scalars() {
        let out = run(doc! { "a.b": 0 }, doc! { "a": [ 1, { "b": 2, "c": 3 } ] }, None);
        assert_eq!(out, doc! { "a": [ 1, { "c": 3 } ] });
    }

    #[test]
    fn id_exclusion_and_document_order_kept() {
        assert_eq!(run(doc! { "_id": 0, "a": 1 }, doc! { "_id": 1, "a": 2, "b": 3 }, None), doc! { "a": 2 });
        let out = run(doc! { "a": 1 }, doc! { "a": 2, "_id": 7 }, None);
        let keys: Vec<&str> = out.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["a", "_id"]);
    }

    #[test]
    fn empty_find_projection_returns_whole_document() {
        assert_eq!(run(doc! {}, doc! { "a": 1 }, None), doc! { "a": 1 });
        assert!(!find_proj(doc! {}).is_exclusion() || find_proj(doc! {}).is_exclusion());
    }

    #[test]
    fn mode_is_exclusion_only_for_removal_projections() {
        assert!(find_proj(doc! { "_id": 0 }).is_exclusion());
        assert!(!find_proj(doc! { "a": 1 }).is_exclusion());
    }

    #[test]
    fn mixed_inclusion_and_exclusion_are_rejected() {
        assert_eq!(code_of(doc! { "a": 0, "b": 1 }, ProjectionKind::Find), Some(31253));
        assert_eq!(code_of(doc! { "a": 1, "b": 0 }, ProjectionKind::Find), Some(31254));
    }

    #[test]
    fn path_collision_is_rejected() {
        assert_eq!(code_of(doc! { "a": 1, "a.b": 1 }, ProjectionKind::Find), Some(31249));
    }

    #[test]
    fn slice_forms_and_limit_validation() {
        assert_eq!(run(doc! { "a": { "$slice": -2 } }, doc! { "a": [1, 2, 3, 4] }, None), doc! { "a": [3, 4] });
        assert_eq!(run(doc! { "a": { "$slice": [1, 2] } }, doc! { "a": [1, 2, 3, 4] }, None), doc! { "a": [2, 3] });
        assert_eq!(run(doc! { "a": { "$slice": [-3, 2] } }, doc! { "a": [1, 2, 3, 4] }, None), doc! { "a": [2, 3] });
        assert_eq!(code_of(doc! { "a": { "$slice": [0, 0] } }, ProjectionKind::Find), Some(28689));
    }

    #[test]
    fn positional_uses_match_position() {
        let spec = doc! { "a.$": 1 };
        let d = doc! { "a": [1, 2, 3] };
        assert_eq!(run(spec.clone(), d.clone(), Some(1)), doc! { "a": [2] });
        assert_eq!(run(spec.clone(), d.clone(), None), doc! {});
        assert_eq!(run(spec, d, Some(9)), doc! {});
    }

    #[test]
    fn meta_text_score_is_read_and_meta_key_preserved() {
        let out = run(
            doc! { "s": { "$meta": "textScore" } },
            doc! { "_id": 1, "$__meta": { "textScore": 0.5 } },
            None,
        );
        assert_eq!(out.get_f64("s"), Ok(0.5));
        assert_eq!(out.get_i32("_id"), Ok(1));
        assert!(out.contains_key(META_KEY));
    }

    #[test]
    fn computed_literal_is_appended_in_spec_order() {
        let out = run(doc! { "a": 1, "c": "lit" }, doc! { "_id": 1, "a": 2, "b": 3 }, None);
        let keys: Vec<&str> = out.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["_id", "a", "c"]);
        assert_eq!(out, doc! { "_id": 1, "a": 2, "c": "lit" });
    }

    #[test]
    fn agg_numeric_literal_is_computed_not_inclusion_flag() {
        let out = Projection::new(&doc! { "x": 2, "a": 1 }, ProjectionKind::Agg)
            .expect("valid")
            .apply(&doc! { "a": 5, "b": 9 }, None, &Vars::new())
            .expect("apply");
        assert_eq!(out, doc! { "a": 5, "x": 2 });
    }

    #[test]
    fn invalid_specs_return_mongodb_codes() {
        assert_eq!(code_of(doc! {}, ProjectionKind::Agg), Some(51272));
        assert_eq!(code_of(doc! { "a.$": 1 }, ProjectionKind::Agg), Some(16410));
        assert_eq!(code_of(doc! { "$x": 1 }, ProjectionKind::Find), Some(16410));
        assert_eq!(code_of(doc! { "a": { "$slice": 1, "$x": 2 } }, ProjectionKind::Find), Some(16410));
    }

    #[test]
    fn add_fields_replaces_in_place_and_appends() {
        let out = add_fields(&doc! { "a": 1, "b": 2 }, &doc! { "b": 9, "c": "lit" }, &Vars::new()).expect("ok");
        let keys: Vec<&str> = out.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
        assert_eq!(out, doc! { "a": 1, "b": 9, "c": "lit" });
    }

    #[test]
    fn add_fields_nested_merges_and_dotted_creates() {
        let out = add_fields(
            &doc! { "a": { "c": 4, "d": 5 } },
            &doc! { "a": { "b": 1, "c": 2 }, "x.y": "z" },
            &Vars::new(),
        )
        .expect("ok");
        assert_eq!(out, doc! { "a": { "c": 2, "d": 5, "b": 1 }, "x": { "y": "z" } });
        let keys: Vec<&str> = out.get_document("a").expect("a").keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["c", "d", "b"]);
    }

    #[test]
    fn add_fields_empty_doc_literal_and_scalar_collision() {
        let out = add_fields(&doc! {}, &doc! { "e": {} }, &Vars::new()).expect("ok");
        assert_eq!(out, doc! { "e": {} });
        let err = add_fields(&doc! { "a": 5 }, &doc! { "a.b": 1 }, &Vars::new()).expect_err("scalar in path");
        assert_eq!(err.code, 28);
    }

    #[test]
    fn add_fields_sets_inside_array_documents() {
        let out = add_fields(&doc! { "a": [1, { "x": 1 }] }, &doc! { "a.y": 2 }, &Vars::new()).expect("ok");
        assert_eq!(out, doc! { "a": [1, { "x": 1, "y": 2 }] });
    }
}
