//! Explain tab: runs explain and renders the plan tree with winning/rejected plans.

use crate::model::QueryRequest;
use crate::palette::{self, *};
use crate::tabs::TabHost;
use bson::Bson;
use eframe::egui;

pub struct ExplainTab {
    pub db: String,
    pub coll: String,
    pub filter_text: String,
    pub verbosity: String,
    pub plan: Option<bson::Document>,
    pub error: Option<String>,
    pub loading: bool,
}

impl Default for ExplainTab {
    fn default() -> Self {
        ExplainTab {
            db: String::new(),
            coll: String::new(),
            filter_text: "{}".into(),
            verbosity: "allPlansExecution".into(),
            plan: None,
            error: None,
            loading: false,
        }
    }
}

impl ExplainTab {
    pub fn open(&mut self, db: &str, coll: &str) {
        if self.db != db || self.coll != coll {
            self.db = db.into();
            self.coll = coll.into();
            self.plan = None;
        }
    }

    pub fn run(&mut self, host: &mut dyn TabHost) {
        self.error = None;
        let filter: serde_json::Value = match serde_json::from_str(&self.filter_text) {
            Ok(v) => v,
            Err(e) => {
                self.error = Some(format!("filter: {e}"));
                return;
            }
        };
        let Some(server) = host.server() else { return };
        let q = QueryRequest { db: self.db.clone(), coll: self.coll.clone(), filter: crate::tabs::json_to_bson(&filter).and_then(|b| b.as_document().cloned()).unwrap_or_default(), sort: None, projection: None, skip: 0, limit: 0, collation: None };
        self.loading = true;
        let _ = host.send(crate::model::Cmd::Explain(server, q));
        self.verbosity = self.verbosity.clone();
    }

    pub fn apply(&mut self, reply: &crate::model::Reply) {
        if let crate::model::Reply::Explain(d) = reply {
            self.plan = Some(d.clone());
            self.loading = false;
        }
    }

    pub fn ui(&mut self, ui: &mut egui::Ui, host: &mut dyn TabHost) {
        if host.server().is_none() {
            ui.label(muted("No server connected"));
            return;
        }
        ui.horizontal(|ui| {
            ui.label("Filter");
            ui.add(egui::TextEdit::singleline(&mut self.filter_text).desired_width(320.0).code_editor());
            if ui.button("Explain").clicked() {
                self.run(host);
            }
            if self.loading {
                ui.spinner();
            }
        });
        if let Some(e) = &self.error {
            ui.label(egui::RichText::new(e).color(ERROR));
        }
        ui.separator();
        let Some(plan) = &self.plan else {
            ui.label(muted("Run explain to see the winning plan"));
            return;
        };
        egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
            let qp = plan.get_document("queryPlanner").ok();
            if let Some(qp) = qp {
                if let Some(win) = qp.get_document("winningPlan").ok() {
                    palette::section(ui, "Winning plan");
                    tree(ui, win, 0);
                }
                if let Some(rejected) = qp.get_array("rejectedPlans").ok() {
                    if !rejected.is_empty() {
                        palette::section(ui, "Rejected plans");
                        for r in rejected {
                            if let Bson::Document(d) = r {
                                tree(ui, d, 0);
                            }
                        }
                    }
                }
            }
            if let Some(stats) = plan.get_document("executionStats").ok() {
                palette::section(ui, "Execution");
                kv(ui, "nReturned", stats.get("nReturned"));
                kv(ui, "totalKeysExamined", stats.get("totalKeysExamined"));
                kv(ui, "totalDocsExamined", stats.get("totalDocsExamined"));
                kv(ui, "executionTimeMillis", stats.get("executionTimeMillis"));
                if let Some(stage) = stats.get_document("executionStages").ok() {
                    tree(ui, stage, 0);
                }
            }
        });
    }
}

fn kv(ui: &mut egui::Ui, k: &str, v: Option<&Bson>) {
    ui.horizontal(|ui| {
        ui.label(muted(k));
        ui.label(mono(&v.map(|v| serde_json::to_string(v).unwrap_or_default()).unwrap_or_default()));
    });
}

fn tree(ui: &mut egui::Ui, d: &bson::Document, depth: usize) {
    let stage = d.get_str("stage").or_else(|_| d.get_str("type")).unwrap_or("");
    let mut bits: Vec<String> = Vec::new();
    for k in ["indexName", "nss", "direction", "limitAmount", "skipAmount", "filter", "sortPattern", "indexBounds", "transformBy", "docsExamined", "keysExamined", "seeks", "nReturned"] {
        if let Some(v) = d.get(k) {
            bits.push(format!("{k}: {}", serde_json::to_string(v).unwrap_or_default()));
        }
    }
    ui.horizontal(|ui| {
        ui.add_space(depth as f32 * 14.0);
        ui.label(accent(if stage.is_empty() { "?" } else { stage }));
        ui.label(muted(&bits.join("  ")));
    });
    if let Some(inner) = d.get_document("inputStage").ok() {
        tree(ui, inner, depth + 1);
    }
}