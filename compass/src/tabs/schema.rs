//! Schema tab: samples documents and shows field coverage, types and value ranges.

use crate::model::QueryRequest;
use crate::palette::{self, *};
use crate::tabs::{cell_of, collect_fields, fmt_cell, Cell, TabHost};
use bson::Bson;
use eframe::egui;
use std::collections::BTreeMap;

pub struct SchemaTab {
    db: String,
    coll: String,
    sample_size: i64,
    docs: Vec<bson::Document>,
    fields: BTreeMap<String, FieldStats>,
    pub loading: bool,
    error: Option<String>,
}

#[derive(Clone)]
pub struct FieldStats {
    pub cell: Cell,
    pub count: u64,
    pub examples: Vec<String>,
    pub values: Vec<Bson>,
}

impl Default for FieldStats {
    fn default() -> Self {
        FieldStats { cell: crate::tabs::Cell::Missing, count: 0, examples: Vec::new(), values: Vec::new() }
    }
}

impl Default for SchemaTab {
    fn default() -> Self {
        SchemaTab { db: String::new(), coll: String::new(), sample_size: 200, docs: Vec::new(), fields: BTreeMap::new(), loading: false, error: None }
    }
}

impl SchemaTab {
    pub fn open(&mut self, db: &str, coll: &str) {
        if self.db == db && self.coll == coll && !self.docs.is_empty() {
            return;
        }
        self.db = db.into();
        self.coll = coll.into();
        self.docs.clear();
        self.fields.clear();
        self.error = None;
    }

    pub fn analyze(&mut self, host: &mut dyn TabHost) {
        self.open(&self.db.clone(), &self.coll.clone());
        let Some(server) = host.server() else { return };
        let q = QueryRequest { db: self.db.clone(), coll: self.coll.clone(), filter: Default::default(), sort: None, projection: None, skip: 0, limit: self.sample_size, collation: None };
        self.loading = true;
        let _ = host.send(crate::model::Cmd::RunQuery(server, q));
    }

    pub fn apply(&mut self, reply: &crate::model::Reply) {
        if let crate::model::Reply::Docs(docs) = reply {
            self.docs = docs.clone();
            self.loading = false;
            self.recompute();
        }
    }

    fn recompute(&mut self) {
        let mut fields: BTreeMap<String, FieldStats> = BTreeMap::new();
        for d in &self.docs {
            for (path, cell) in collect_fields(std::slice::from_ref(d)) {
                let e = fields.entry(path).or_default();
                e.count += 1;
                e.cell = cell;
            }
            for (path, _) in collect_fields(std::slice::from_ref(d)) {
                if let Some(v) = crate::tabs::get_path(d, &path) {
                    if let Some(e) = fields.get_mut(&path) {
                        let s = short(&v);
                        if e.examples.len() < 3 && !e.examples.contains(&s) {
                            e.examples.push(s);
                        }
                        if e.values.len() < 50 && !e.values.contains(&v) {
                            e.values.push(v);
                        }
                    }
                }
            }
        }
        self.fields = fields;
    }

    pub fn ui(&mut self, ui: &mut egui::Ui, host: &mut dyn TabHost) {
        if host.server().is_none() {
            ui.label(muted("No server connected"));
            return;
        }
        ui.horizontal(|ui| {
            ui.label("sample");
            let before = self.sample_size;
            ui.add(egui::DragValue::new(&mut self.sample_size).range(1..=10_000));
            ui.label("documents");
            if ui.button("Analyze").clicked() || before != self.sample_size {
                self.analyze(host);
            }
            if self.loading {
                ui.spinner();
            }
            ui.label(muted(&format!("{} sampled", self.docs.len())));
        });
        if let Some(e) = &self.error {
            ui.label(egui::RichText::new(e).color(ERROR));
        }
        ui.separator();
        let total = self.docs.len().max(1) as f64;
        egui_extras::TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::remainder())
            .header(20.0, |mut h| {
                for t in ["field", "type", "coverage", "documents", "examples"] {
                    h.col(|ui| {
                        ui.strong(t);
                    });
                }
            })
            .body(|mut body| {
                for (path, f) in &self.fields {
                    body.row(18.0, |mut row| {
                        row.col(|ui| {
                            ui.label(mono(path));
                        });
                        row.col(|ui| {
                            let c = fmt_cell(&f.cell);
                            ui.label(muted(&c));
                        });
                        row.col(|ui| {
                            let pct = f.count as f64 / total * 100.0;
                            ui.add(egui::ProgressBar::new((pct / 100.0) as f32).desired_width(90.0).text(format!("{pct:.0}%")));
                        });
                        row.col(|ui| {
                            let cs = f.count.to_string();
                            ui.label(muted(&cs));
                        });
                        row.col(|ui| {
                            ui.label(muted(&f.examples.join(", ")));
                        });
                    });
                }
            });
        let _ = cell_of;
    }
}

fn e_name(_f: &BTreeMap<String, FieldStats>, e: &FieldStats) -> String {
    let _ = e;
    String::new()
}

fn short(v: &Bson) -> String {
    let s = serde_json::to_string(v).unwrap_or_default();
    if s.len() > 60 {
        format!("{}…", &s[..57])
    } else {
        s
    }
}