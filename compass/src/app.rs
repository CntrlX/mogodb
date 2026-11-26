//! App shell: sidebar, tab strip, status bar, and the bridge to the worker thread.
//!
//! Rendering is split from state transitions (`update_state` / `render_sidebar`) so the UI
//! can be driven headlessly in tests.

use crate::model::{Cmd, QueryRequest, Reply, Response};
use crate::palette;
use crate::sidebar::{Sidebar, UiAction};
use crate::tabs::{
    documents::DocumentsTab, TabHost, AggregationTab, ExplainTab, IndexesTab, PerformanceTab, SchemaTab, TabKind,
};
use eframe::egui;
use std::sync::mpsc::Receiver;

pub struct CompassApp {
    pub backend: crate::model::Backend,
    rx: Receiver<Response>,
    pub sidebar: Sidebar,
    pub tab: TabKind,
    pub documents: DocumentsTab,
    pub agg: AggregationTab,
    pub schema: SchemaTab,
    pub indexes: IndexesTab,
    pub explain: ExplainTab,
    pub performance: PerformanceTab,
    pub status: Option<String>,
    pub last_error: Option<String>,
    pub show_settings: bool,
    pub show_about: bool,
    pub connect_target: Option<usize>,
    pub db_order: Vec<String>,
    /// Collection list to refetch once the in-flight DDL reply lands.
    pub pending_refresh: Option<String>,
}

impl CompassApp {
    pub fn new(backend: crate::model::Backend, rx: Receiver<Response>) -> Self {
        Self::with_saved(backend, rx, true)
    }

    /// Build the app shell. `load_saved` is false in tests so they never depend on the
    /// developer's real `~/.config/mogocompass/connections.json`.
    pub fn with_saved(backend: crate::model::Backend, rx: Receiver<Response>, load_saved: bool) -> Self {
        let mut sidebar = Sidebar::default();
        let saved = if load_saved { crate::connections::load() } else { crate::connections::Store::default() };
        for s in &saved.servers {
            sidebar.servers.push(s.to_info(None));
        }
        if let Some(active) = saved.active.and_then(|n| sidebar.servers.iter().position(|s| s.name == n)) {
            sidebar.active = Some(active);
        }
        sidebar.refilter();
        CompassApp {
            backend,
            rx,
            sidebar,
            tab: TabKind::Documents,
            documents: DocumentsTab::default(),
            agg: AggregationTab::default(),
            schema: SchemaTab::default(),
            indexes: IndexesTab::default(),
            explain: ExplainTab::default(),
            performance: PerformanceTab::default(),
            status: None,
            last_error: None,
            show_settings: false,
            show_about: false,
            connect_target: None,
            db_order: Vec::new(),
            pending_refresh: None,
        }
    }

    /// Drain worker responses; returns true when state changed.
    pub fn pump(&mut self) -> bool {
        let mut changed = false;
        while let Ok(resp) = self.rx.try_recv() {
            changed = true;
            match resp.result {
                Err(e) => {
                    self.last_error = Some(e);
                    self.sidebar.loading = false;
                    self.documents.loading = false;
                    self.agg.loading = false;
                    self.schema.loading = false;
                    self.connect_target = None;
                }
                Ok(reply) => self.apply(reply, resp.started.elapsed()),
            }
        }
        changed
    }

    fn apply(&mut self, reply: Reply, elapsed: std::time::Duration) {
        match reply {
            Reply::Unit => {
                self.sidebar.loading = false;
                self.connect_target = None;
                self.status = Some(format!("done in {} ms", elapsed.as_millis()));
                if self.sidebar.active.is_some() && self.sidebar.dbs.is_empty() {
                    self.request_databases();
                }
                // A DDL reply (create/drop) invalidates the cached collection list.
                if let Some(db) = self.pending_refresh.take() {
                    self.sidebar.colls_loaded.remove(&db);
                    self.sidebar.loading = true;
                    self.request_collections();
                }
            }
            Reply::Docs(docs) => {
                self.documents.apply(&Reply::Docs(docs.clone()));
                self.agg.apply(&Reply::Docs(docs.clone()));
                self.schema.apply(&Reply::Docs(docs));
            }
            Reply::Databases(dbs) => {
                for db in dbs {
                    if let Some(existing) = self.sidebar.dbs.iter_mut().find(|d| d.name == db.name) {
                        existing.empty = db.empty;
                    } else {
                        self.sidebar.dbs.push(db);
                    }
                }
                self.sidebar.dbs.sort_by(|a, b| a.name.cmp(&b.name));
                self.db_order = self.sidebar.dbs.iter().map(|d| d.name.clone()).collect();
                self.sidebar.loading = false;
                self.request_collections();
            }
            // The reply carries the database it was requested for: filing by the current
            // selection would attach the list to whatever the user clicked meanwhile.
            Reply::Collections(db, colls) => {
                self.sidebar.colls.insert(db.clone(), colls);
                self.sidebar.colls_loaded.insert(db);
                self.sidebar.loading = false;
            }
            Reply::Collection(c) => self.indexes.apply(&Reply::Collection(c)),
            Reply::Count(n) => {
                self.documents.total = Some(n);
                self.status = Some(format!("{n} documents"));
            }
            Reply::Values(v) => self.status = Some(format!("{} distinct values", v.len())),
            Reply::Explain(d) => self.explain.apply(&Reply::Explain(d)),
            Reply::Status(d) => self.performance.apply(&Reply::Status(d)),
            Reply::Ops(o) => self.performance.apply(&Reply::Ops(o)),
            Reply::ServerInfo(info) => {
                self.status = Some(format!("connected to {}", info.name));
            }
        }
    }

    pub fn request_databases(&mut self) {
        let Some(server) = self.sidebar.active else { return };
        self.sidebar.loading = true;
        let _ = self.backend.send(Cmd::ListDatabases(server));
    }

    pub fn request_collections(&mut self) {
        let Some(server) = self.sidebar.active else { return };
        let dbs: Vec<String> = self.sidebar.dbs.iter().map(|d| d.name.clone()).collect();
        for db in dbs {
            if !self.sidebar.colls_loaded.contains(&db) {
                let _ = self.backend.send(Cmd::ListCollections(server, db));
            }
        }
    }

    pub fn request_meta(&mut self) {
        let (Some(server), Some(db), Some(coll)) = (self.sidebar.active, self.sidebar.selected_db().map(str::to_string), self.sidebar.selected_coll().map(str::to_string)) else {
            return;
        };
        let _ = self.backend.send(Cmd::CollectionMeta(server, db, coll));
    }

    pub fn sidebar_action(&mut self, action: UiAction) {
        match action {
            UiAction::None => {}
            UiAction::Connect { server, uri, password } => self.connect(server, uri, password),
            UiAction::Disconnect(server) => {
                let _ = self.backend.send(Cmd::Disconnect(server));
                self.sidebar.active = None;
                self.sidebar.dbs.clear();
                self.sidebar.colls.clear();
                self.sidebar.colls_loaded.clear();
                self.sidebar.selection = crate::sidebar::Selection::None;
            }
            UiAction::Select(sel) => {
                self.sidebar.selection = sel;
                self.sidebar.colls_loaded.clear();
                if self.sidebar.selected_db().is_some() {
                    self.request_collections();
                }
                if self.sidebar.selected_coll().is_some() {
                    self.request_meta();
                }
            }
            UiAction::Toggle(key) => {
                let open = !*self.sidebar.expanded.get(&key).unwrap_or(&false);
                self.sidebar.expanded.insert(key, open);
            }
            UiAction::Save => self.save_connections(),
            UiAction::AddSaved(uri) => {
                let name = crate::connections::default_name(&uri);
                self.sidebar.servers.push(crate::model::ServerInfo { name, uri, password: None });
                self.save_connections();
            }
            UiAction::RemoveServer(i) => {
                self.sidebar.servers.remove(i);
                if self.sidebar.active == Some(i) {
                    self.sidebar.active = None;
                }
                self.save_connections();
            }
            UiAction::EditServer(i, name, uri) => {
                if let Some(s) = self.sidebar.servers.get_mut(i) {
                    s.name = name;
                    s.uri = uri;
                }
                self.save_connections();
            }
            UiAction::DropDatabase(db) => {
                let Some(server) = self.sidebar.active else { return };
                let _ = self.backend.send(Cmd::DropDatabase(server, db.clone()));
                self.sidebar.dbs.retain(|d| d.name != db);
                self.sidebar.colls.remove(&db);
            }
            UiAction::DropCollection(db, coll) => {
                let Some(server) = self.sidebar.active else { return };
                let q = QueryRequest::new(&db, &coll);
                let _ = self.backend.send(Cmd::DropCollection(server, q));
                if let Some(list) = self.sidebar.colls.get_mut(&db) {
                    list.retain(|c| c.name != coll);
                }
            }
            UiAction::NewCollection(db, name, options) => {
                let Some(server) = self.sidebar.active else { return };
                if self.backend.send(Cmd::CreateCollection(server, db.clone(), name.clone(), options)).is_some() {
                    // The collection only exists once mogoDB has created the namespace, so
                    // the list is refetched when the DDL reply arrives.
                    self.pending_refresh = Some(db);
                    self.sidebar.loading = true;
                    self.status = Some(format!("created {name}"));
                }
            }
            UiAction::FilterChanged => self.sidebar.refilter(),
        }
    }

    fn connect(&mut self, server: usize, uri: String, password: Option<String>) {
        self.sidebar.active = Some(server);
        self.sidebar.dbs.clear();
        self.sidebar.colls.clear();
        self.sidebar.colls_loaded.clear();
        self.sidebar.selection = crate::sidebar::Selection::None;
        self.sidebar.error = None;
        self.connect_target = Some(server);
        self.sidebar.loading = true;
        let _ = self.backend.send(Cmd::Connect { server, uri, password });
    }

    fn save_connections(&self) {
        let store = crate::connections::Store {
            servers: self.sidebar.servers.iter().map(|s| crate::connections::SavedServer { name: s.name.clone(), uri: s.uri.clone() }).collect(),
            active: self.sidebar.active.and_then(|i| self.sidebar.servers.get(i)).map(|s| s.name.clone()),
        };
        crate::connections::save(&store);
    }
}

impl CompassApp {
    pub fn set_server(&mut self, i: usize) {
        self.sidebar.active = Some(i);
    }

    pub fn select(&mut self, db: &str, coll: &str) {
        self.sidebar.selection = crate::sidebar::Selection::Collection(db.into(), coll.into());
        self.sidebar.colls_loaded.clear();
        self.request_meta();
    }

    pub fn sidebar_view(&mut self, ui: &mut egui::Ui) {
        let action = self.sidebar.ui(ui);
        self.sidebar_action(action);
    }

    pub fn status_view(&mut self, ui: &mut egui::Ui) {
        ui.add_space(2.0);
        ui.horizontal(|ui| {
            if let Some(s) = &self.status {
                ui.label(palette::muted(s));
            }
            if self.sidebar.loading {
                ui.spinner();
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if let Some(info) = self.sidebar.active.and_then(|i| self.sidebar.servers.get(i)) {
                    ui.label(palette::muted(&format!("* {}", info.name)));
                }
            });
        });
        ui.add_space(2.0);
    }

    pub fn tab_view(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            for t in TabKind::ALL {
                if ui.selectable_label(self.tab == t, t.label()).clicked() {
                    self.tab = t;
                }
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if let Some((db, coll)) = self.sidebar.selection_pair() {
                    // Right-aligned context label; truncate so it never clips the tab strip.
                    let text = format!("{db}.{coll}");
                    let text = if text.len() > 28 { format!("{}...", &text[..27]) } else { text };
                    ui.label(palette::muted(&text));
                }
            });
        });
        ui.separator();
    }

    pub fn content_view(&mut self, ui: &mut egui::Ui) {
        let (db, coll) = match self.sidebar.selection_pair() {
            Some((d, c)) => (d.to_string(), c.to_string()),
            None => {
                ui.centered_and_justified(|ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(50.0);
                        ui.label(palette::heading("Mogo Compass"));
                        ui.add_space(6.0);
                        ui.label(palette::muted("Connect to a server and pick a collection to begin"));
                        if ui.button("New connection").clicked() {
                            self.sidebar.add_open = true;
                        }
                    });
                });
                return;
            }
        };
        let server = self.sidebar.active;
        let backend = self.backend.clone();
        let mut host = HostRef { server, backend: &backend, db: &db, coll: &coll };
        // Switching collection resets the tab state, so the first query must be issued here:
        // otherwise the table stays empty until the user presses Fetch.
        match self.tab {
            TabKind::Documents => {
                let changed = self.documents.open(&db, &coll);
                if changed && server.is_some() {
                    self.documents.reload(&mut host);
                }
            }
            TabKind::Aggregation => {
                self.agg.open(&db, &coll);
            }
            TabKind::Schema => {
                self.schema.open(&db, &coll);
            }
            TabKind::Indexes => {
                self.indexes.open(&db, &coll);
            }
            TabKind::Explain => {
                self.explain.open(&db, &coll);
            }
            TabKind::Performance => {}
        }
        match self.tab {
            TabKind::Documents => self.documents.ui(ui, &mut host, true),
            TabKind::Aggregation => self.agg.ui(ui, &mut host),
            TabKind::Schema => self.schema.ui(ui, &mut host),
            TabKind::Indexes => self.indexes.ui(ui, &mut host),
            TabKind::Explain => self.explain.ui(ui, &mut host),
            TabKind::Performance => {
                if server.is_none() {
                    ui.centered_and_justified(|ui| ui.label(palette::muted("No server connected")));
                } else {
                    self.performance.open(server.unwrap_or(0));
                    self.performance.ui(ui, &mut host);
                }
            }
        }
    }

    pub fn modal_view(&mut self, ui: &mut egui::Ui) {
        let action = self.sidebar.modal(ui);
        self.sidebar_action(action);
        if self.show_about {
            let mut open = true;
            egui::Window::new("About Mogo Compass").id(egui::Id::new("about")).open(&mut open).collapsible(false).resizable(false).show(ui.ctx(), |ui| {
                ui.label(palette::heading("Mogo Compass"));
                ui.label(palette::muted(&format!("MongoDB Compass-style GUI for mogoDB {}", env!("CARGO_PKG_VERSION"))));
                ui.add_space(4.0);
                ui.label(palette::muted("Pure-Rust client (OP_MSG + SCRAM) over mogoDB/mongod 9.0."));
            });
            if !open {
                self.show_about = false;
            }
        }
        if self.show_settings {
            let mut open = true;
            egui::Window::new("Settings").id(egui::Id::new("settings")).open(&mut open).collapsible(false).resizable(false).show(ui.ctx(), |ui| {
                ui.label(palette::muted("Theme is dark; fonts: Inter (embedded)."));
                ui.label(palette::muted(&format!("Connection store: {}", crate::connections::config_path().display())));
            });
            if !open {
                self.show_settings = false;
            }
        }
    }
}

pub struct HostRef<'a> {
    pub server: Option<usize>,
    pub backend: &'a crate::model::Backend,
    pub db: &'a str,
    pub coll: &'a str,
}

impl<'a> TabHost for HostRef<'a> {
    fn server(&self) -> Option<usize> {
        self.server
    }
    fn server_info(&self) -> Option<&'a crate::model::ServerInfo> {
        None
    }
    fn db(&self) -> &str {
        self.db
    }
    fn coll(&self) -> &str {
        self.coll
    }
    fn send(&self, cmd: Cmd) -> Option<u64> {
        self.backend.send(cmd)
    }
    fn status(&self, text: String) {
        let _ = text;
    }
    fn error(&self, text: String) {
        let _ = text;
    }
    fn request(&self) -> QueryRequest {
        QueryRequest::new(self.db, self.coll)
    }
    fn set_request(&mut self, q: QueryRequest) {
        let _ = q;
    }
}

impl eframe::App for CompassApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let _ = self.pump();
        if self.sidebar.visible {
            egui::Panel::left("sidebar")
                .resizable(true)
                .default_size(260.0)
                .show_inside(ui, |ui| self.sidebar_view(ui));
        }
        egui::Panel::bottom("status").show_inside(ui, |ui| self.status_view(ui));
        egui::CentralPanel::default().show_inside(ui, |ui| {
            self.tab_view(ui);
            self.content_view(ui);
            self.modal_view(ui);
        });
    }
}
