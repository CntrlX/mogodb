//! Indexes tab: list, create (key/type/unique/sparse/partial/collation/TTL), drop.

use crate::model::{CollectionInfo, QueryRequest};
use crate::palette::{self, *};
use crate::tabs::TabHost;
use bson::{Bson, Document};
use eframe::egui;

pub struct IndexesTab {
    pub db: String,
    pub coll: String,
    pub indexes: Vec<crate::model::IndexInfo>,
    pub error: Option<String>,
    pub create_open: bool,
    pub key_text: String,
    pub name_text: String,
    pub unique: bool,
    pub sparse: bool,
    pub expire_after: String,
    pub partial_text: String,
    pub collation_text: String,
}

impl Default for IndexesTab {
    fn default() -> Self {
        IndexesTab {
            db: String::new(),
            coll: String::new(),
            indexes: Vec::new(),
            error: None,
            create_open: false,
            key_text: "{\"field\": 1}".into(),
            name_text: String::new(),
            unique: false,
            sparse: false,
            expire_after: String::new(),
            partial_text: String::new(),
            collation_text: String::new(),
        }
    }
}

impl IndexesTab {
    pub fn open(&mut self, db: &str, coll: &str) {
        if self.db == db && self.coll == coll {
            return;
        }
        self.db = db.into();
        self.coll = coll.into();
        self.indexes.clear();
        self.error = None;
    }

    pub fn reload(&mut self, host: &mut dyn TabHost) {
        let Some(server) = host.server() else { return };
        self.open(&self.db.clone(), &self.coll.clone());
        let q = QueryRequest::new(&self.db, &self.coll);
        let _ = host.send(crate::model::Cmd::CollectionMeta(server, self.db.clone(), self.coll.clone()));
    }

    pub fn apply(&mut self, reply: &crate::model::Reply) {
        if let crate::model::Reply::Collection(c) = reply {
            self.indexes = c.indexes.clone();
        }
    }

    pub fn ui(&mut self, ui: &mut egui::Ui, host: &mut dyn TabHost) {
        if host.server().is_none() {
            ui.label(muted("No server connected"));
            return;
        }
        ui.horizontal(|ui| {
            if ui.button("Refresh").clicked() {
                self.reload(host);
            }
            if ui.button("Create index").clicked() {
                self.create_open = true;
            }
        });
        if let Some(e) = &self.error {
            ui.label(egui::RichText::new(e).color(ERROR));
        }
        ui.separator();
        egui_extras::TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::remainder())
            .column(egui_extras::Column::auto())
            .header(20.0, |mut h| {
                for t in ["name", "key", "options", ""] {
                    h.col(|ui| {
                        ui.strong(t);
                    });
                }
            })
            .body(|mut body| {
                let mut drop: Option<String> = None;
                for idx in self.indexes.clone() {
                    body.row(20.0, |mut row| {
                        row.col(|ui| {
                            ui.label(mono(&idx.name));
                        });
                        row.col(|ui| {
                            ui.label(mono(&serde_json::to_string(&idx.key).unwrap_or_default()));
                        });
                        row.col(|ui| {
                            let mut bits = Vec::new();
                            if idx.unique {
                                bits.push("unique".to_string());
                            }
                            if idx.sparse {
                                bits.push("sparse".to_string());
                            }
                            if let Some(c) = &idx.collation {
                                let s = serde_json::to_string(c).unwrap_or_default();
                                bits.push(s);
                            }
                            let b = bits.join(", ");
                            ui.label(muted(&b));
                        });
                        row.col(|ui| {
                            if idx.name != "_id_" && ui.button("Drop").clicked() {
                                drop = Some(idx.name.clone());
                            }
                        });
                    });
                }
                if let Some(name) = drop {
                    let Some(server) = host.server() else { return };
                    let q = QueryRequest::new(&self.db, &self.coll);
                    let _ = host.send(crate::model::Cmd::DropIndex(server, q, name));
                    self.reload(host);
                }
            });
        self.create(ui, host);
    }

    fn create(&mut self, ui: &mut egui::Ui, host: &mut dyn TabHost) {
        if !self.create_open {
            return;
        }
        let mut submit = false;
        let mut close = false;
        egui::Window::new("Create index")
            .id(egui::Id::new("create_index"))
            .collapsible(false)
            .resizable(false)
            .show(ui.ctx(), |ui| {
                ui.label("Key");
                ui.add(egui::TextEdit::singleline(&mut self.key_text).desired_width(360.0).code_editor());
                ui.add_space(4.0);
                ui.label("Name (blank = generated)");
                ui.add(egui::TextEdit::singleline(&mut self.name_text).desired_width(360.0));
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    ui.checkbox(&mut self.unique, "unique");
                    ui.checkbox(&mut self.sparse, "sparse");
                });
                ui.add_space(4.0);
                ui.label("expireAfterSeconds");
                ui.add(egui::TextEdit::singleline(&mut self.expire_after).desired_width(120.0));
                ui.add_space(4.0);
                ui.label("partialFilterExpression");
                ui.add(egui::TextEdit::singleline(&mut self.partial_text).desired_width(360.0).code_editor().hint_text("{}"));
                ui.add_space(4.0);
                ui.label("collation");
                ui.add(egui::TextEdit::singleline(&mut self.collation_text).desired_width(360.0).code_editor().hint_text("{}"));
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Create").clicked() {
                        submit = true;
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                });
            });
        if submit {
            match self.build() {
                Ok((spec, opts)) => {
                    let Some(server) = host.server() else { return };
                    let q = QueryRequest::new(&self.db, &self.coll);
                    let _ = host.send(crate::model::Cmd::CreateIndex(server, q, spec, opts));
                    self.create_open = false;
                    self.reload(host);
                }
                Err(e) => self.error = Some(e),
            }
        } else if close {
            self.create_open = false;
        }
    }

    fn build(&self) -> Result<(Document, Document), String> {
        let key: serde_json::Value = serde_json::from_str(&self.key_text).map_err(|e| format!("key: {e}"))?;
        let key = crate::tabs::json_to_bson(&key).ok_or("key: cannot convert")?;
        let Bson::Document(mut spec) = key else { return Err("key must be an object".into()) };
        let mut spec = spec;
        let name = if self.name_text.is_empty() {
            let mut n = String::new();
            for (i, (k, v)) in spec.iter().enumerate() {
                if i > 0 {
                    n.push('_');
                }
                let dir = match v {
                    Bson::Int32(-1) | Bson::Int64(-1) => "-1",
                    _ => "1",
                };
                n.push_str(&format!("{k}_{dir}"));
            }
            n
        } else {
            self.name_text.clone()
        };
        let mut index = bson::doc! {"key": Bson::Document(std::mem::take(&mut spec)), "name": name};
        if self.unique {
            index.insert("unique", true);
        }
        if self.sparse {
            index.insert("sparse", true);
        }
        if !self.expire_after.is_empty() {
            let n: i64 = self.expire_after.trim().parse().map_err(|_| "expireAfterSeconds: not a number")?;
            index.insert("expireAfterSeconds", n);
        }
        if !self.partial_text.is_empty() {
            let v: serde_json::Value = serde_json::from_str(&self.partial_text).map_err(|e| format!("partialFilterExpression: {e}"))?;
            index.insert("partialFilterExpression", crate::tabs::json_to_bson(&v).ok_or("partialFilterExpression: cannot convert")?);
        }
        if !self.collation_text.is_empty() {
            let v: serde_json::Value = serde_json::from_str(&self.collation_text).map_err(|e| format!("collation: {e}"))?;
            index.insert("collation", crate::tabs::json_to_bson(&v).ok_or("collation: cannot convert")?);
        }
        Ok((index, Document::new()))
    }
}