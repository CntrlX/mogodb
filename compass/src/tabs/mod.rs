//! Documents, query bars, aggregation pipelines, schema, indexes, explain and performance tabs.

use crate::model::{QueryRequest, ServerInfo};
use crate::palette::{self, *};
use bson::Bson;
use bson::Document;
use eframe::egui;
use std::collections::BTreeMap;

pub mod agg;
pub mod documents;
pub mod explain;
pub mod json;
pub mod indexes;
pub mod performance;
pub mod schema;

pub use agg::AggregationTab;
pub use explain::ExplainTab;
pub use indexes::IndexesTab;
pub use performance::PerformanceTab;
pub use schema::SchemaTab;

/// What each tab needs from the app shell.
pub trait TabHost {
    fn server(&self) -> Option<usize>;
    fn server_info(&self) -> Option<&ServerInfo>;
    fn db(&self) -> &str;
    fn coll(&self) -> &str;
    fn send(&self, cmd: crate::model::Cmd) -> Option<u64>;
    fn status(&self, text: String);
    fn error(&self, text: String);
    fn request(&self) -> QueryRequest;
    fn set_request(&mut self, q: QueryRequest);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TabKind {
    Documents,
    Aggregation,
    Schema,
    Indexes,
    Explain,
    Performance,
}

impl TabKind {
    pub const ALL: [TabKind; 6] = [
        TabKind::Documents,
        TabKind::Aggregation,
        TabKind::Schema,
        TabKind::Indexes,
        TabKind::Explain,
        TabKind::Performance,
    ];

    pub fn label(self) -> &'static str {
        match self {
            TabKind::Documents => "Documents",
            TabKind::Aggregation => "Aggregation",
            TabKind::Schema => "Schema",
            TabKind::Indexes => "Indexes",
            TabKind::Explain => "Explain",
            TabKind::Performance => "Performance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataView {
    Json,
    Table,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Missing,
    Null,
    Bool(bool),
    Int(i64),
    Double(f64),
    Text(String),
    Date(i64),
    Oid(String),
    Binary(usize),
    Doc(usize),
    Array(usize),
    Other(&'static str),
}

pub fn cell_of(v: Option<&Bson>) -> Cell {
    match v {
        None | Some(Bson::Undefined) => Cell::Missing,
        Some(Bson::Null) => Cell::Null,
        Some(Bson::Boolean(b)) => Cell::Bool(*b),
        Some(Bson::Int32(i)) => Cell::Int(*i as i64),
        Some(Bson::Int64(i)) => Cell::Int(*i),
        Some(Bson::Double(d)) => Cell::Double(*d),
        Some(Bson::String(s)) => Cell::Text(s.clone()),
        Some(Bson::Symbol(s)) => Cell::Text(s.clone()),
        Some(Bson::DateTime(d)) => Cell::Date(d.timestamp_millis()),
        Some(Bson::ObjectId(o)) => Cell::Oid(o.to_hex()),
        Some(Bson::Binary(b)) => Cell::Binary(b.bytes.len()),
        Some(Bson::Document(d)) => Cell::Doc(d.len()),
        Some(Bson::Array(a)) => Cell::Array(a.len()),
        Some(_) => Cell::Other("value"),
    }
}

/// Every field path seen in a sample of documents, with its type.
pub fn collect_fields(docs: &[bson::Document]) -> BTreeMap<String, Cell> {
    let mut out = BTreeMap::new();
    for d in docs {
        collect_into("", d, &mut out);
    }
    out
}

fn collect_into(prefix: &str, d: &bson::Document, out: &mut BTreeMap<String, Cell>) {
    for (k, v) in d {
        let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        let cell = cell_of(Some(v));
        if let Bson::Document(sub) = v {
            if !sub.is_empty() {
                collect_into(&path, sub, out);
            }
        }
        out.entry(path).or_insert(cell);
    }
}

/// Renders a BSON value as an editable JSON string, and parses edits back.
pub fn json_editor(ui: &mut egui::Ui, value: &mut Bson, id: &str) -> bool {
    let mut text = serde_json::to_string_pretty(value).unwrap_or_else(|_| "null".into());
    let before = text.clone();
    let response = ui.add(egui::TextEdit::multiline(&mut text).id(egui::Id::new(id)).desired_rows(10).code_editor().desired_width(f32::INFINITY));
    if response.changed() {
        match serde_json::from_str::<serde_json::Value>(&text) {
            Ok(v) => match crate::tabs::json_to_bson(&v) {
                Some(b) => {
                    *value = b;
                    return true;
                }
                None => {
                    crate::tabs::status_set("cannot convert JSON to BSON");
                }
            },
            Err(e) => {
                crate::tabs::status_set(format!("invalid JSON: {e}"));
            }
        }
        text = before;
    }
    false
}

/// JSON → BSON with MongoDB type hints (`{"$oid": ...}`, `{"$date": ...}`, ...).
pub fn json_to_bson(v: &serde_json::Value) -> Option<Bson> {
    use serde_json::Value;
    Some(match v {
        Value::Null => Bson::Null,
        Value::Bool(b) => Bson::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= i32::MIN as i64 && i <= i32::MAX as i64 && !n.to_string().contains('.') && !n.to_string().contains('e') {
                    Bson::Int32(i as i32)
                } else {
                    Bson::Int64(i)
                }
            } else {
                Bson::Double(n.as_f64()?)
            }
        }
        Value::String(s) => Bson::String(s.clone()),
        Value::Array(a) => Bson::Array(a.iter().filter_map(json_to_bson).collect()),
        Value::Object(o) => {
            if o.len() == 1 {
                let (k, v) = o.iter().next()?;
                match (k.as_str(), v) {
                    ("$oid", Value::String(s)) => return Bson::ObjectId(s.parse().ok()?).into(),
                    ("$date", Value::String(s)) => return chrono::DateTime::parse_from_rfc3339(s).ok().map(|d| Bson::DateTime(bson::DateTime::from_millis(d.timestamp_millis()))).or(Some(Bson::Null)),
                    ("$numberInt", Value::String(s)) => return s.parse::<i32>().ok().map(Bson::Int32),
                    ("$numberLong", Value::String(s)) => return s.parse::<i64>().ok().map(Bson::Int64),
                    ("$numberDouble", Value::String(s)) => return s.parse::<f64>().ok().map(Bson::Double),
                    ("$numberDecimal", Value::String(s)) => return s.parse::<bson::Decimal128>().ok().map(Bson::Decimal128),
                    _ => {}
                }
            }
            let mut d = bson::Document::new();
            for (k, v) in o {
                d.insert(k.clone(), json_to_bson(v)?);
            }
            Bson::Document(d)
        }
    })
}

pub fn status_set(text: impl Into<String>) {
    STATUS.with(|s| *s.borrow_mut() = Some(text.into()));
}

pub fn status_take() -> Option<String> {
    STATUS.with(|s| s.borrow_mut().take())
}

thread_local! {
    static STATUS: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

pub fn fmt_date(ms: i64) -> String {
    match chrono::DateTime::from_timestamp_millis(ms) {
        Some(d) => d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        None => format!("{ms}"),
    }
}

pub fn fmt_cell(c: &Cell) -> String {
    match c {
        Cell::Missing => "—".into(),
        Cell::Null => "null".into(),
        Cell::Bool(b) => b.to_string(),
        Cell::Int(i) => i.to_string(),
        Cell::Double(d) => crate::tabs::fmt_double(*d),
        Cell::Text(s) => s.replace('\n', "⏎"),
        Cell::Date(ms) => fmt_date(*ms),
        Cell::Oid(o) => o.clone(),
        Cell::Binary(n) => format!("BinData({n})"),
        Cell::Doc(n) => format!("{{…}} {n}"),
        Cell::Array(n) => format!("[…] {n}"),
        Cell::Other(t) => t.to_string(),
    }
}

/// Path lookup for table rendering (`a.b.0`).
pub fn get_path(d: &Document, path: &str) -> Option<Bson> {
    let mut cur = Bson::Document(d.clone());
    for seg in path.split('.') {
        match cur {
            Bson::Document(ref doc) => cur = doc.get(seg).cloned()?,
            Bson::Array(ref a) => cur = a.get(seg.parse::<usize>().ok()?)?.clone(),
            _ => return None,
        }
    }
    Some(cur)
}

pub fn fmt_double(d: f64) -> String {
    if d == d.trunc() && d.abs() < 1e15 {
        format!("{:.1}", d)
    } else {
        format!("{d}")
    }
}