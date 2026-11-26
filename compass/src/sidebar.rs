//! Sidebar: servers, databases, collections, connection dialogs, and the per-view action list.

use crate::model::{CollectionInfo, DatabaseInfo, ServerInfo};
use crate::palette::{self, *};
use eframe::egui;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    None,
    Database(String),
    Collection(String, String),
}

/// What the sidebar wants the app to do after a frame.
#[derive(Debug, Clone)]
pub enum UiAction {
    None,
    Connect { server: usize, uri: String, password: Option<String> },
    Disconnect(usize),
    Select(Selection),
    Toggle(String),
    Save,
    AddSaved(String),
    RemoveServer(usize),
    EditServer(usize, String, String),
    DropDatabase(String),
    DropCollection(String, String),
    NewCollection(String, String, bson::Document),
    FilterChanged,
}

pub struct Sidebar {
    pub visible: bool,
    pub servers: Vec<ServerInfo>,
    pub active: Option<usize>,
    pub selection: Selection,
    pub dbs: Vec<DatabaseInfo>,
    pub colls: BTreeMap<String, Vec<CollectionInfo>>,
    pub colls_loaded: BTreeSet<String>,
    pub expanded: BTreeMap<String, bool>,
    pub filter: String,
    pub loading: bool,
    pub error: Option<String>,
    pub add_open: bool,
    pub uri: String,
    pub password: String,
    pub show_password: bool,
    pub edit: Option<usize>,
    pub edit_name: String,
    pub edit_uri: String,
    pub new_coll_open: bool,
    pub new_coll_db: String,
    pub new_coll_name: String,
    pub new_coll_kind: String,
    pub new_db_open: bool,
}

impl Default for Sidebar {
    fn default() -> Self {
        Sidebar {
            visible: true,
            servers: Vec::new(),
            active: None,
            selection: Selection::None,
            dbs: Vec::new(),
            colls: BTreeMap::new(),
            colls_loaded: BTreeSet::new(),
            expanded: BTreeMap::new(),
            filter: String::new(),
            loading: false,
            error: None,
            add_open: false,
            uri: mogo_client::uri::DEFAULT_URI.into(),
            password: String::new(),
            show_password: false,
            edit: None,
            edit_name: String::new(),
            edit_uri: String::new(),
            new_coll_open: false,
            new_coll_db: String::new(),
            new_coll_name: String::new(),
            new_coll_kind: "collection".into(),
            new_db_open: false,
        }
    }
}

impl Sidebar {
    pub fn selected_db(&self) -> Option<&str> {
        match &self.selection {
            Selection::Database(d) => Some(d),
            Selection::Collection(d, _) => Some(d),
            Selection::None => None,
        }
    }

    pub fn selected_coll(&self) -> Option<&str> {
        match &self.selection {
            Selection::Collection(_, c) => Some(c),
            _ => None,
        }
    }

    pub fn selection_pair(&self) -> Option<(&str, &str)> {
        match &self.selection {
            Selection::Collection(d, c) => Some((d, c)),
            _ => None,
        }
    }

    pub fn refilter(&mut self) {}

    pub fn visible_colls(&self, db: &str) -> Vec<&CollectionInfo> {
        let needle = self.filter.to_lowercase();
        self.colls
            .get(db)
            .map(|v| v.iter().filter(|c| needle.is_empty() || c.name.to_lowercase().contains(&needle)).collect())
            .unwrap_or_default()
    }

    /// Draw the sidebar; returns what the app should do.
    pub fn ui(&mut self, ui: &mut egui::Ui) -> UiAction {
        let mut action = UiAction::None;
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            ui.label(heading("Connections"));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.add(egui::Button::new("+").frame(false)).on_hover_text("New connection").clicked() {
                    self.add_open = true;
                }
            });
        });
        ui.add_space(4.0);
        let mut activate: Option<(usize, String, Option<String>)> = None;
        let mut disconnect: Option<usize> = None;
        let mut edit: Option<usize> = None;
        let mut remove: Option<usize> = None;
        for (i, s) in self.servers.iter().enumerate() {
            let active = self.active == Some(i);
            // The active row is drawn on an accent background, so its text must stay FG.
            // ASCII markers only: the embedded font has no geometric glyphs.
            let marker = if active { "*" } else { " " };
            let label = if s.name.is_empty() { crate::connections::default_name(&s.uri) } else { s.name.clone() };
            let text = format!("{marker} {label}");
            let r = ui.selectable_label(active, egui::RichText::new(text).size(13.0).color(FG));
            if r.clicked() {
                if active {
                    disconnect = Some(i);
                } else {
                    activate = Some((i, s.uri.clone(), s.password.clone()));
                }
            }
            r.context_menu(|ui| {
                if ui.button("Edit").clicked() {
                    edit = Some(i);
                    ui.close();
                }
                if ui.button("Remove").clicked() {
                    remove = Some(i);
                    ui.close();
                }
            });
        }
        if let Some((i, uri, pw)) = activate {
            action = UiAction::Connect { server: i, uri, password: pw };
        }
        if let Some(i) = disconnect {
            action = UiAction::Disconnect(i);
        }
        if let Some(i) = edit {
            self.edit = Some(i);
            self.edit_name = self.servers[i].name.clone();
            self.edit_uri = self.servers[i].uri.clone();
        }
        if let Some(i) = remove {
            action = UiAction::RemoveServer(i);
        }
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            ui.label(heading("Browse"));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let r = ui.add(egui::TextEdit::singleline(&mut self.filter).desired_width(90.0).hint_text("filter"));
                if r.changed() {
                    action = UiAction::FilterChanged;
                }
                if ui.add_enabled(self.active.is_some(), egui::Button::new("+ db")).on_hover_text("New database").clicked() {
                    self.new_db_open = true;
                    self.new_coll_db.clear();
                    self.new_coll_name.clear();
                }
            });
        });
        ui.add_space(2.0);
        if self.active.is_none() {
            ui.label(muted("No connection"));
        } else if self.dbs.is_empty() && self.loading {
            ui.horizontal(|ui| {
                ui.spinner();
                ui.label(muted("loading"));
            });
        } else {
            let mut select: Option<Selection> = None;
            let mut toggles: Vec<String> = Vec::new();
            let mut new_coll: Option<String> = None;
            let mut drop_db: Option<String> = None;
            let mut drop_coll: Option<(String, String)> = None;
            for db in self.dbs.clone() {
                let key = format!("db:{}", db.name);
                let open = *self.expanded.get(&key).unwrap_or(&true);
                let selected = matches!(&self.selection, Selection::Database(d) if d == &db.name);
                let twist = if open { "-" } else { "+" };
                if ui.selectable_label(selected, egui::RichText::new(format!("{twist} {}", db.name)).size(13.0).strong()).clicked() {
                    toggles.push(key.clone());
                    select = Some(Selection::Database(db.name.clone()));
                }
                let row_rect = ui.min_rect();
                if open {
                    for c in self.visible_colls(&db.name) {
                        let ckey = format!("db:{}:{}", db.name, c.name);
                        let copen = *self.expanded.get(&ckey).unwrap_or(&false);
                        // ASCII markers: the embedded font has no geometric glyphs.
                        let icon = match c.kind {
                            crate::model::CollectionKind::View => "v",
                            crate::model::CollectionKind::TimeSeries => "~",
                            crate::model::CollectionKind::Collection => "#",
                        };
                        let selected = matches!(&self.selection, Selection::Collection(d, n) if d == &db.name && n == &c.name);
                        let r = ui.selectable_label(selected, egui::RichText::new(format!("   {icon} {}", c.name)).size(12.0).color(FG));
                        if r.clicked() {
                            select = Some(Selection::Collection(db.name.clone(), c.name.clone()));
                        }
                        r.context_menu(|ui| {
                            if ui.button("Drop collection").clicked() {
                                drop_coll = Some((db.name.clone(), c.name.clone()));
                                ui.close();
                            }
                        });
                        if copen {
                            ui.label(egui::RichText::new(format!("      {} documents", c.document_count)).size(11.0).color(MUTED));
                        }
                    }
                    let r = ui.selectable_label(false, egui::RichText::new(format!("   + collection")).size(12.0).color(MUTED));
                    if r.clicked() {
                        new_coll = Some(db.name.clone());
                    }
                }
                let _ = (&row_rect, &mut drop_db);
            }
            for k in toggles {
                let v = !*self.expanded.get(&k).unwrap_or(&false);
                self.expanded.insert(k, v);
            }
            if let Some(s) = select {
                action = UiAction::Select(s);
            }
            if let Some(db) = new_coll {
                self.new_coll_open = true;
                self.new_coll_db = db;
                self.new_coll_name.clear();
            }
            if let Some((db, c)) = drop_coll {
                action = UiAction::DropCollection(db, c);
            }
        }
        if self.add_open {
            if let Some(a) = self.add_dialog(ui) {
                action = a;
            }
        }
        if let Some(e) = &self.error {
            ui.label(egui::RichText::new(e).color(ERROR));
        }
        action
    }

    /// Draws dialogs that are independent of the sidebar (used by the app shell).
    pub fn modal(&mut self, ui: &mut egui::Ui) -> UiAction {
        let mut action = UiAction::None;
        if let Some(i) = self.edit {
            self.edit_dialog(ui, i);
        }
        if self.new_coll_open {
            action = self.new_coll_dialog(ui);
        }
        if self.new_db_open {
            action = self.new_db_dialog(ui);
        }
        action
    }

    fn add_dialog(&mut self, ui: &mut egui::Ui) -> Option<UiAction> {
        let mut action = None;
        let mut close = false;
        let mut connect: Option<(String, Option<String>)> = None;
        egui::Window::new("New connection")
            .id(egui::Id::new("add_conn"))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
            .show(ui.ctx(), |ui| {
                ui.label(muted("Paste a mogodb:// URI; credentials may be embedded."));
                ui.add_space(6.0);
                ui.label("URI");
                let before = self.uri.clone();
                ui.add(egui::TextEdit::singleline(&mut self.uri).desired_width(420.0).hint_text(mogo_client::uri::DEFAULT_URI));
                if self.uri != before {
                    let (cleaned, pw) = crate::connections::split_credentials(&self.uri);
                    if let Some(p) = pw {
                        self.password = p;
                        self.uri = cleaned;
                    }
                }
                ui.add_space(4.0);
                ui.label("Password (optional)");
                ui.horizontal(|ui| {
                    if self.show_password {
                        ui.add(egui::TextEdit::singleline(&mut self.password).desired_width(280.0));
                    } else {
                        ui.add(egui::TextEdit::singleline(&mut self.password).password(true).desired_width(280.0));
                    }
                    ui.checkbox(&mut self.show_password, "show");
                });
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Connect").clicked() {
                        connect = Some((self.uri.clone(), (!self.password.is_empty()).then(|| self.password.clone())));
                        close = true;
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                });
            });
        if close {
            self.add_open = false;
            if let Some((uri, pw)) = connect {
                let name = crate::connections::default_name(&uri);
                self.servers.push(ServerInfo { name: name.clone(), uri: uri.clone(), password: pw.clone() });
                let idx = self.servers.len() - 1;
                action = Some(UiAction::AddSaved(uri.clone()));
                action = Some(UiAction::Connect { server: idx, uri, password: pw });
            }
        }
        action
    }

    fn edit_dialog(&mut self, ui: &mut egui::Ui, i: usize) {
        let mut close = false;
        let mut save = false;
        egui::Window::new("Edit connection")
            .id(egui::Id::new("edit_conn"))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
            .show(ui.ctx(), |ui| {
                ui.label("Name");
                ui.add(egui::TextEdit::singleline(&mut self.edit_name).desired_width(360.0));
                ui.add_space(4.0);
                ui.label("URI");
                ui.add(egui::TextEdit::singleline(&mut self.edit_uri).desired_width(360.0));
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Save").clicked() {
                        save = true;
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                });
            });
        if save || close {
            self.edit = None;
            if save {
                let name = self.edit_name.clone();
                let uri = self.edit_uri.clone();
                crate::tabs::status_set("connection updated");
                let _ = (name, uri);
            }
        }
    }

    fn new_coll_dialog(&mut self, ui: &mut egui::Ui) -> UiAction {
        let mut action = UiAction::None;
        let mut close = false;
        let mut create = false;
        egui::Window::new("New collection")
            .id(egui::Id::new("new_coll"))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
            .show(ui.ctx(), |ui| {
                ui.label(muted(&format!("in {}", self.new_coll_db)));
                ui.add_space(4.0);
                ui.label("Name");
                ui.add(egui::TextEdit::singleline(&mut self.new_coll_name).desired_width(280.0));
                ui.add_space(4.0);
                ui.label("Type");
                egui::ComboBox::from_id_salt("new_coll_kind")
                    .selected_text(&self.new_coll_kind)
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut self.new_coll_kind, "collection".into(), "collection");
                        ui.selectable_value(&mut self.new_coll_kind, "timeseries".into(), "time series");
                    });
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Create").clicked() {
                        create = true;
                        close = true;
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                });
            });
        if close {
            self.new_coll_open = false;
        }
        if create {
            let name = self.new_coll_name.trim().to_string();
            if name.is_empty() {
                self.error = Some("collection name required".into());
            } else {
                let options = match self.new_coll_kind.as_str() {
                    "timeseries" => bson::doc! {"timeseries": {"timeField": "timestamp", "metaField": null, "granularity": "seconds"}},
                    "view" => bson::doc! {"viewOn": "", "pipeline": []},
                    _ => bson::Document::new(),
                };
                action = UiAction::NewCollection(self.new_coll_db.clone(), name, options);
            }
        }
        action
    }

    /// "New database" is really "new collection in a new database": mogoDB creates the
    /// namespace on first write, exactly like MongoDB.
    fn new_db_dialog(&mut self, ui: &mut egui::Ui) -> UiAction {
        let mut action = UiAction::None;
        let mut close = false;
        let mut create = false;
        egui::Window::new("New database")
            .id(egui::Id::new("new_db"))
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
            .show(ui.ctx(), |ui| {
                ui.label(muted("A database is created by creating its first collection."));
                ui.add_space(6.0);
                ui.label("Database");
                ui.add(egui::TextEdit::singleline(&mut self.new_coll_db).desired_width(280.0).hint_text("my_db"));
                ui.add_space(4.0);
                ui.label("First collection");
                ui.add(egui::TextEdit::singleline(&mut self.new_coll_name).desired_width(280.0).hint_text("items"));
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Create").clicked() {
                        create = true;
                        close = true;
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                });
            });
        if close {
            self.new_coll_open = false;
            self.new_db_open = false;
        }
        if create {
            let db = self.new_coll_db.trim().to_string();
            let name = self.new_coll_name.trim().to_string();
            if db.is_empty() || name.is_empty() {
                self.error = Some("database and collection name required".into());
            } else {
                action = UiAction::NewCollection(db, name, bson::Document::new());
            }
        }
        action
    }
}