//! Syntax-highlighted JSON rendering for the documents view.
//!
//! Compass shows every document as a pretty-printed JSON card with coloured tokens
//! (keys, strings, numbers, booleans, null). `highlight` turns a JSON string into an
//! egui `LayoutJob` so a plain `Label` can render it with those colours.

use crate::palette::{ACCENT, FG, INFO, MUTED, WARN};
use eframe::egui;
use eframe::egui::text::{LayoutJob, TextFormat};

/// Token colors, roughly Compass' dark theme.
const KEY: egui::Color32 = ACCENT;
const STRING: egui::Color32 = FG;
const NUMBER: egui::Color32 = INFO;
const LITERAL: egui::Color32 = WARN;
const PUNCT: egui::Color32 = MUTED;

fn push(job: &mut LayoutJob, text: &str, color: egui::Color32, mono: bool) {
    let mut fmt = TextFormat {
        color,
        ..Default::default()
    };
    if mono {
        fmt.font_id = egui::FontId::monospace(12.0);
    }
    job.append(text, 0.0, fmt);
}

/// Colourise `text` as JSON. Unparseable input is rendered plain, so the view never
/// panics on a document with an unusual value.
pub fn highlight(text: &str, size: f32) -> LayoutJob {
    let _ = size;
    let mut job = LayoutJob::default();
    let bytes: Vec<char> = text.chars().collect();
    let mut i = 0;
    let n = bytes.len();

    while i < n {
        let c = bytes[i];
        match c {
            '"' => {
                // Read the whole string literal, then decide key vs value by looking
                // ahead for a ':' (ignoring whitespace).
                let start = i;
                i += 1;
                while i < n {
                    if bytes[i] == '\\' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == '"' {
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                let mut j = i;
                while j < n && bytes[j].is_whitespace() {
                    j += 1;
                }
                let is_key = j < n && bytes[j] == ':';
                let lit: String = bytes[start..i].iter().collect();
                push(&mut job, &lit, if is_key { KEY } else { STRING }, true);
            }
            '-' | '0'..='9' => {
                let start = i;
                i += 1;
                while i < n && (bytes[i].is_ascii_digit() || matches!(bytes[i], '.' | 'e' | 'E' | '+' | '-')) {
                    i += 1;
                }
                let lit: String = bytes[start..i].iter().collect();
                push(&mut job, &lit, NUMBER, true);
            }
            't' | 'f' | 'n' => {
                // true / false / null
                let rest: String = bytes[i..n.min(i + 5)].iter().collect();
                let word = if rest.starts_with("true") {
                    "true"
                } else if rest.starts_with("false") {
                    "false"
                } else if rest.starts_with("null") {
                    "null"
                } else {
                    ""
                };
                if word.is_empty() {
                    push(&mut job, &c.to_string(), PUNCT, true);
                    i += 1;
                } else {
                    push(&mut job, word, LITERAL, true);
                    i += word.len();
                }
            }
            _ => {
                push(&mut job, &c.to_string(), PUNCT, true);
                i += 1;
            }
        }
    }
    job
}

/// Convenience: highlight a BSON document's pretty JSON.
pub fn highlight_doc(doc: &bson::Document, size: f32) -> LayoutJob {
    let text = serde_json::to_string_pretty(doc).unwrap_or_default();
    let mut job = highlight(&text, size);
    for sec in &mut job.sections {
        sec.format.font_id = egui::FontId::monospace(size);
    }
    job
}

/// Render a document as editable JSON text.
///
/// ObjectIds become bare 24-hex strings (rather than `{"$oid": "..."}`) and dates become
/// ISO-8601 strings, so the editor shows what a human expects. `coerce_document` puts the
/// real BSON types back on save using the collection's observed field types.
pub fn to_editor_text(doc: &bson::Document) -> String {
    let v = to_plain(doc);
    serde_json::to_string_pretty(&v).unwrap_or_default()
}

/// BSON → plain JSON, flattening ObjectIds to hex and dates to ISO strings.
fn to_plain(doc: &bson::Document) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    for (k, v) in doc {
        m.insert(k.clone(), value_to_plain(v));
    }
    serde_json::Value::Object(m)
}

fn value_to_plain(v: &bson::Bson) -> serde_json::Value {
    use bson::Bson;
    match v {
        Bson::ObjectId(o) => serde_json::Value::String(o.to_hex()),
        Bson::DateTime(d) => serde_json::Value::String(crate::tabs::fmt_date(d.timestamp_millis())),
        Bson::Document(d) => to_plain(d),
        Bson::Array(a) => serde_json::Value::Array(a.iter().map(value_to_plain).collect()),
        Bson::String(s) => serde_json::Value::String(s.clone()),
        Bson::Int32(i) => serde_json::json!(i),
        Bson::Int64(i) => serde_json::json!(i),
        Bson::Double(f) => serde_json::json!(f),
        Bson::Boolean(b) => serde_json::json!(b),
        Bson::Null | Bson::Undefined => serde_json::Value::Null,
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

/// Parse edited JSON back into a document, restoring ObjectIds and dates for the paths
/// the collection is known to use. Unknown paths keep whatever the JSON said.
pub fn coerce_document(text: &str, field_types: &std::collections::BTreeMap<String, crate::tabs::Cell>) -> Option<bson::Document> {
    use crate::tabs::Cell;
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    let obj = v.as_object()?;
    let mut out = bson::Document::new();
    coerce_into("", obj, field_types, &mut out);
    Some(out)
}

fn coerce_into(
    prefix: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
    types: &std::collections::BTreeMap<String, crate::tabs::Cell>,
    out: &mut bson::Document,
) {
    use crate::tabs::Cell;
    for (k, v) in obj {
        let path = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
        let ty = types.get(&path);
        let b = match (ty, v) {
            // A 24-hex string in an ObjectId field is the id the user typed.
            (Some(Cell::Oid(_)), serde_json::Value::String(s)) => match bson::oid::ObjectId::parse_str(s) {
                Ok(oid) => bson::Bson::ObjectId(oid),
                Err(_) => crate::tabs::json_to_bson(v).unwrap_or(bson::Bson::Null),
            },
            // ISO date strings in a date field go back to BSON dates.
            (Some(Cell::Date(_)), serde_json::Value::String(s)) => match chrono::DateTime::parse_from_rfc3339(s) {
                Ok(d) => bson::Bson::DateTime(bson::DateTime::from_millis(d.timestamp_millis())),
                Err(_) => crate::tabs::json_to_bson(v).unwrap_or(bson::Bson::Null),
            },
            (_, serde_json::Value::Object(map)) => {
                // Nested object: recurse when the schema says it is a sub-document, but
                // keep `$date`/`$oid` wrappers intact for fields with no recorded type.
                if (map.len() == 1 && map.contains_key("$date")) || (map.len() == 1 && map.contains_key("$oid")) {
                    crate::tabs::json_to_bson(v).unwrap_or(bson::Bson::Null)
                } else {
                    let mut sub = bson::Document::new();
                    coerce_into(&path, map, types, &mut sub);
                    bson::Bson::Document(sub)
                }
            }
            (_, other) => crate::tabs::json_to_bson(other).unwrap_or(bson::Bson::Null),
        };
        out.insert(k.clone(), b);
    }
}