//! Aggregation tab: a pipeline stage list with per-stage JSON, validation, run/explain.

use crate::model::QueryRequest;
use crate::palette::{self, *};
use crate::tabs::TabHost;
use bson::Document;
use eframe::egui;

const HELP: &[(&str, &str)] = &[
    ("$match", "{\"field\": {\"$gt\": 1}}"),
    ("$group", "{\"_id\": \"$cat\", \"total\": {\"$sum\": \"$price\"}}"),
    ("$sort", "{\"total\": -1}"),
    ("$project", "{\"name\": 1, \"_id\": 0}"),
    ("$limit", "20"),
    ("$skip", "10"),
    ("$unwind", "\"$items\""),
    ("$lookup", "{\"from\": \"orders\", \"localField\": \"_id\", \"foreignField\": \"cust\", \"as\": \"orders\"}"),
    ("$addFields", "{\"total\": {\"$multiply\": [\"$price\", \"$qty\"]}}"),
    ("$facet", "{\"byCat\": [{\"$group\": {\"_id\": \"$cat\"}}]}"),
    ("$count", "\"n\""),
    ("$setWindowFields", "{\"sortBy\": {\"ts\": 1}, \"output\": {\"run\": {\"$sum\": \"$v\", \"window\": {\"documents\": [\"unbounded\", \"current\"]}}}}"),
];

pub struct AggregationTab {
    pub db: String,
    pub coll: String,
    pub stages: Vec<Stage>,
    pub docs: Vec<Document>,
    pub loading: bool,
    pub error: Option<String>,
    pub auto_run: bool,
    pub show_help: bool,
}

#[derive(Clone, Debug)]
pub struct Stage {
    pub name: String,
    pub arg: String,
}

impl Default for AggregationTab {
    fn default() -> Self {
        AggregationTab {
            db: String::new(),
            coll: String::new(),
            stages: vec![Stage { name: "$match".into(), arg: "{}".into() }],
            docs: Vec::new(),
            loading: false,
            error: None,
            auto_run: false,
            show_help: false,
        }
    }
}

impl AggregationTab {
    pub fn open(&mut self, db: &str, coll: &str) {
        if self.db != db || self.coll != coll {
            self.db = db.into();
            self.coll = coll.into();
            self.docs.clear();
            self.error = None;
        }
    }

    pub fn pipeline(&self) -> Result<Vec<Document>, String> {
        let mut out = Vec::new();
        for s in &self.stages {
            if s.name.is_empty() {
                return Err("stage name is empty".into());
            }
            if !s.name.starts_with('$') {
                return Err(format!("stage name must start with '$': {}", s.name));
            }
            let arg: serde_json::Value = serde_json::from_str(&s.arg).map_err(|e| format!("{}: {e}", s.name))?;

            let arg = match arg {
                serde_json::Value::Object(_) | serde_json::Value::Array(_) => crate::tabs::json_to_bson(&arg),
                serde_json::Value::String(text) => Some(bson::Bson::String(text)),
                serde_json::Value::Number(n) => n.as_i64().map(bson::Bson::Int64).or_else(|| n.as_f64().map(bson::Bson::Double)),
                serde_json::Value::Bool(b) => Some(bson::Bson::Boolean(b)),
                serde_json::Value::Null => Some(bson::Bson::Null),
            }
            .ok_or_else(|| format!("{}: cannot convert argument", s.name))?;
            let mut d = Document::new();
            d.insert(s.name.clone(), arg);
            out.push(d);
        }
        Ok(out)
    }

    pub fn run(&mut self, host: &mut dyn TabHost) {
        self.error = None;
        let pipeline = match self.pipeline() {
            Ok(p) => p,
            Err(e) => {
                self.error = Some(e);
                return;
            }
        };
        let Some(server) = host.server() else { return };
        self.loading = true;
        let q = QueryRequest::new(&self.db, &self.coll);
        let _ = host.send(crate::model::Cmd::RunQuery(server, q));
    }

    pub fn apply(&mut self, reply: &crate::model::Reply) {
        if let crate::model::Reply::Docs(d) = reply {
            self.docs = d.clone();
            self.loading = false;
        }
    }

    pub fn ui(&mut self, ui: &mut egui::Ui, host: &mut dyn TabHost) {
        if host.server().is_none() {
            ui.label(muted("No server connected"));
            return;
        }
        ui.horizontal(|ui| {
            if ui.button("Run").clicked() {
                self.run(host);
            }
            if ui.button("Explain").clicked() {
                self.error = self.pipeline().err();
            }
            ui.checkbox(&mut self.auto_run, "auto-run");
            ui.separator();
            if ui.button(if self.show_help { "Hide examples" } else { "Examples" }).clicked() {
                self.show_help = !self.show_help;
            }
            if self.loading {
                ui.spinner();
            }
        });
        if self.show_help {
            egui::Frame::group(ui.style()).fill(PANEL).show(ui, |ui| {
                egui_extras::TableBuilder::new(ui)
                    .striped(true)
                    .column(egui_extras::Column::auto())
                    .column(egui_extras::Column::remainder())
                    .body(|mut body| {
                        for (name, example) in HELP {
                            body.row(18.0, |mut row| {
                                row.col(|ui| {
                                    ui.label(mono(name));
                                });
                                row.col(|ui| {
                                    ui.label(mono(example));
                                });
                            });
                        }
                    });
            });
        }
        ui.add_space(4.0);
        if let Some(e) = &self.error {
            ui.label(egui::RichText::new(e).color(ERROR));
        }
        let mut changed = false;
        let mut remove: Option<usize> = None;
        let mut swaps: Vec<(usize, usize)> = Vec::new();
        let stages = std::mem::take(&mut self.stages);
        let mut edits: Vec<(String, String)> = Vec::new();
        egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
            for (i, s) in stages.iter().enumerate() {
                let frame = egui::Frame::group(ui.style()).fill(PANEL).stroke(egui::Stroke::new(1.0, BORDER)).inner_margin(6.0).corner_radius(4.0);
                let mut name = s.name.clone();
                let mut arg = s.arg.clone();
                let mut drop = false;
                let mut swap: Option<usize> = None;
                frame.show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(muted(&format!("{}", i + 1)));
                        ui.add(egui::TextEdit::singleline(&mut name).desired_width(150.0).font(egui::TextStyle::Monospace));
                        ui.add_space(4.0);
                        ui.add(egui::TextEdit::singleline(&mut arg).desired_width(560.0).font(egui::TextStyle::Monospace).hint_text("{}"));
                        if ui.button("✕").clicked() {
                            drop = true;
                        }
                        if ui.button("▲").clicked() && i > 0 {
                            swap = Some(i - 1);
                        }
                        if ui.button("▼").clicked() && i + 1 < stages.len() {
                            swap = Some(i + 1);
                        }
                    });
                });
                ui.add_space(4.0);
                if drop {
                    remove = Some(i);
                }
                if let Some(j) = swap {
                    swaps.push((i, j));
                }
                edits.push((name, arg));
            }
        });
        for (i, (name, arg)) in edits.into_iter().enumerate() {
            if let Some(s) = self.stages.get_mut(i) {
                s.name = name;
                s.arg = arg;
            }
        }
        for (a, b) in swaps {
            if a < self.stages.len() && b < self.stages.len() {
                self.stages.swap(a, b);
            }
        }
        if let Some(i) = remove {
            if i < self.stages.len() {
                self.stages.remove(i);
            }
        }
        ui.horizontal(|ui| {
            if ui.button("+ Add stage").clicked() {
                self.stages.push(Stage { name: "$match".into(), arg: "{}".into() });
                changed = true;
            }
            if ui.button("Clear").clicked() {
                self.stages.clear();
                changed = true;
            }
            if ui.button("Clear output").clicked() {
                self.docs.clear();
            }
        });
        if changed && self.auto_run {
            self.run(host);
        }
        ui.separator();
        let n = self.docs.len();
        let mut rows: Vec<String> = self.docs.iter().map(|d| serde_json::to_string(d).unwrap_or_default()).collect();
        egui_extras::TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::remainder())
            .header(20.0, |mut h| {
                h.col(|ui| {
                    ui.strong("#");
                });
                h.col(|ui| {
                    ui.strong("document");
                });
            })
            .body(|mut body| {
                for (i, r) in rows.drain(..).enumerate() {
                    body.row(18.0, |mut row| {
                        row.col(|ui| {
                            ui.label(muted(&format!("{}", i + 1)));
                        });
                        row.col(|ui| {
                            ui.label(mono(&r));
                        });
                    });
                }
            });
        let _ = n;
    }
}