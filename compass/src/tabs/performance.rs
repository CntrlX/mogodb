//! Performance tab: server status, current operations (with kill), and a metrics strip.

use crate::palette::{self, *};
use crate::tabs::TabHost;
use bson::Bson;
use eframe::egui;

pub struct PerformanceTab {
    pub server: usize,
    pub status: Option<bson::Document>,
    pub ops: Vec<bson::Document>,
    pub filter: String,
    pub error: Option<String>,
    pub auto: bool,
    pub refresh: std::time::Instant,
}

impl Default for PerformanceTab {
    fn default() -> Self {
        PerformanceTab { server: usize::MAX, status: None, ops: Vec::new(), filter: String::new(), error: None, auto: false, refresh: std::time::Instant::now() }
    }
}

impl PerformanceTab {
    pub fn open(&mut self, server: usize) {
        if self.server != server {
            self.server = server;
            self.status = None;
            self.ops.clear();
        }
    }

    pub fn reload(&mut self, host: &mut dyn TabHost) {
        let Some(server) = host.server() else { return };
        self.open(server);
        self.error = None;
        let _ = host.send(crate::model::Cmd::ServerStatus(server));
        let _ = host.send(crate::model::Cmd::CurrentOps(server));
    }

    pub fn apply(&mut self, reply: &crate::model::Reply) {
        match reply {
            crate::model::Reply::Status(d) => self.status = Some(d.clone()),
            crate::model::Reply::Ops(o) => self.ops = o.clone(),
            _ => {}
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
            ui.checkbox(&mut self.auto, "auto (2s)");
            if self.auto && self.refresh.elapsed().as_secs() >= 2 {
                self.refresh = std::time::Instant::now();
                self.reload(host);
            }
            ui.separator();
            ui.label("filter");
            ui.add(egui::TextEdit::singleline(&mut self.filter).desired_width(200.0));
        });
        if let Some(e) = &self.error {
            ui.label(egui::RichText::new(e).color(ERROR));
        }
        ui.add_space(6.0);
        if let Some(s) = &self.status {
            palette::section(ui, "Server");
            let mut rows: Vec<(&str, String)> = Vec::new();
            if let Some(host) = s.get_str("host").ok() {
                rows.push(("host", host.to_string()));
            }
            if let Some(v) = s.get_str("version").ok() {
                rows.push(("version", v.to_string()));
            }
            if let Some(p) = s.get("process").and_then(Bson::as_str) {
                rows.push(("process", p.to_string()));
            }
            if let Some(u) = s.get("uptime").and_then(Bson::as_i64) {
                rows.push(("uptime", format!("{}s", u)));
            }
            if let Some(c) = s.get("connections").and_then(Bson::as_document) {
                let cur = c.get_i64("current").unwrap_or(0);
                let avail = c.get_i64("available").unwrap_or(0);
                rows.push(("connections", format!("{cur} current, {avail} available")));
            }
            if let Some(m) = s.get("mem").and_then(Bson::as_document) {
                rows.push(("resident", format!("{} MiB", m.get_i64("resident").unwrap_or(0) / 1024)));
                rows.push(("virtual", format!("{} MiB", m.get_i64("virtual").unwrap_or(0) / 1024)));
            }
            if let Some(o) = s.get("opcounters").and_then(Bson::as_document) {
                rows.push(("inserts", o.get_i64("insert").unwrap_or(0).to_string()));
                rows.push(("queries", o.get_i64("query").unwrap_or(0).to_string()));
                rows.push(("updates", o.get_i64("update").unwrap_or(0).to_string()));
                rows.push(("deletes", o.get_i64("delete").unwrap_or(0).to_string()));
                rows.push(("commands", o.get_i64("command").unwrap_or(0).to_string()));
            }
            if let Some(t) = s.get("metrics").and_then(Bson::as_document) {
                if let Some(q) = t.get("query").and_then(Bson::as_document) {
                    if let Some(e) = q.get("executor").and_then(Bson::as_document) {
                        rows.push(("queries executed", e.get_i64("scanned").unwrap_or(0).to_string()));
                    }
                }
            }
            egui_extras::TableBuilder::new(ui)
                .striped(true)
                .column(egui_extras::Column::auto())
                .column(egui_extras::Column::remainder())
                .body(|mut body| {
                    for (k, v) in rows {
                        body.row(18.0, |mut row| {
                            row.col(|ui| {
                                ui.label(muted(k));
                            });
                            row.col(|ui| {
                                ui.label(mono(&v));
                            });
                        });
                    }
                });
        }
        ui.add_space(8.0);
        palette::section(ui, "Current operations");
        let mut kill: Option<i64> = None;
        egui_extras::TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::auto())
            .column(egui_extras::Column::remainder())
            .header(20.0, |mut h| {
                for t in ["op", "ns", "secs", "client", "command"] {
                    h.col(|ui| {
                        ui.strong(t);
                    });
                }
            })
            .body(|mut body| {
                for op in &self.ops {
                    let name = op.get_str("op").unwrap_or("?").to_string();
                    if !self.filter.is_empty() && !name.to_lowercase().contains(&self.filter.to_lowercase()) {
                        continue;
                    }
                    let ns = op.get_str("ns").unwrap_or("").to_string();
                    let secs = op.get("secs_running").and_then(Bson::as_i64).or_else(|| op.get("microsecs_running").and_then(Bson::as_i64).map(|v| v / 1_000_000)).unwrap_or(0);
                    let client = op.get_str("client").unwrap_or("").to_string();
                    let cmd = match op.get_document("command") {
                        Ok(d) => {
                            let v = d.get("find").cloned().or_else(|| d.get("aggregate").cloned()).unwrap_or(Bson::Null);
                            serde_json::to_string(&v).unwrap_or_default()
                        }
                        Err(_) => String::new(),
                    };
                    let opid = op.get_i64("opid").unwrap_or(0);
                    body.row(18.0, |mut row| {
                        row.col(|ui| {
                            ui.label(mono(&name));
                        });
                        row.col(|ui| {
                            ui.label(mono(&ns));
                        });
                        row.col(|ui| {
                            ui.label(mono(&secs.to_string()));
                        });
                        row.col(|ui| {
                            ui.label(mono(&client));
                        });
                        row.col(|ui| {
                            if ui.button("kill").clicked() {
                                kill = Some(opid);
                            }
                        });
                    });
                }
            });
        if let Some(op) = kill {
            let Some(server) = host.server() else { return };
            let _ = host.send(crate::model::Cmd::KillOp(server, op));
            self.reload(host);
        }
    }
}