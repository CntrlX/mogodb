//! Documents tab: filter/sort/projection bars, paginated results (JSON or table), insert/edit/delete.

use crate::model::QueryRequest;
use crate::palette::{self, *};
use eframe::egui::Color32;
use crate::tabs::{cell_of, collect_fields, fmt_cell, DataView, TabHost};
use bson::{Bson, Document};
use eframe::egui;

pub struct DocumentsTab {
    pub request: QueryRequest,
    pub filter_text: String,
    pub sort_text: String,
    pub projection_text: String,
    pub view: DataView,
    pub docs: Vec<Document>,
    pub total: Option<u64>,
    pub loading: bool,
    pub error: Option<String>,
    pub page: usize,
    pub page_size: usize,
    pub fields: Vec<String>,
    /// Field path → observed BSON type, used to prefill the Insert editor.
    pub field_types: std::collections::BTreeMap<String, crate::tabs::Cell>,
    pub selected: Option<usize>,
    pub editor_open: bool,
    pub editor_doc: Document,
    pub editor_is_new: bool,
    pub editor_error: Option<String>,
}

impl Default for DocumentsTab {
    fn default() -> Self {
        DocumentsTab {
            request: QueryRequest::new("", ""),
            filter_text: "{}".into(),
            sort_text: "{}".into(),
            projection_text: "{}".into(),
            view: DataView::Json,
            docs: Vec::new(),
            total: None,
            loading: false,
            error: None,
            page: 0,
            page_size: 50,
            fields: Vec::new(),
            field_types: std::collections::BTreeMap::new(),
            selected: None,
            editor_open: false,
            editor_doc: Document::new(),
            editor_is_new: false,
            editor_error: None,
        }
    }
}

impl DocumentsTab {
    /// Point the tab at `db.coll`. Returns true when the target changed, so the caller
    /// knows the cached rows are stale and a fresh query is needed.
    pub fn open(&mut self, db: &str, coll: &str) -> bool {
        if self.request.db != db || self.request.coll != coll {
            self.request = QueryRequest::new(db, coll);
            self.docs.clear();
            self.total = None;
            self.page = 0;
            self.selected = None;
            self.fields.clear();
            self.filter_text = "{}".into();
            self.sort_text = "{}".into();
            self.projection_text = "{}".into();
            self.request.filter = Document::new();
            self.request.sort = None;
            self.request.projection = None;
            return true;
        }
        false
    }

    pub fn reload(&mut self, host: &mut dyn TabHost) {
        self.error = None;
        self.filter_text = serde_json::to_string(&self.request.filter).unwrap_or_else(|_| "{}".into());
        match crate::tabs::json_to_bson(&serde_json::from_str(&self.filter_text).unwrap_or(serde_json::Value::Object(Default::default()))) {
            Some(Bson::Document(d)) => self.request.filter = d,
            _ => {
                self.error = Some("filter must be a JSON object".into());
                return;
            }
        }
        self.request.skip = (self.page * self.page_size) as i64;
        self.request.limit = self.page_size as i64;
        if let Some(id) = host.send(crate::model::Cmd::RunQuery(host.server().unwrap_or(0), self.request.clone())) {
            self.loading = true;
            let _ = id;
        }
    }

    pub fn apply(&mut self, reply: &crate::model::Reply) {
        match reply {
            crate::model::Reply::Docs(docs) => {
                self.docs = docs.clone();
                self.loading = false;
                let fields = collect_fields(&docs);
                // Keep the observed types so Insert can prefill a typed skeleton.
                self.field_types = fields.clone();
                self.fields = fields.into_keys().collect();
            }
            _ => {}
        }
    }

    pub fn ui(&mut self, ui: &mut egui::Ui, host: &mut dyn TabHost, can_edit: bool) {
        let Some(server) = host.server() else {
            ui.label(muted("No server connected"));
            return;
        };
        ui.horizontal(|ui| {
            if ui.add_enabled(self.page > 0, egui::Button::new("<")).on_hover_text("Previous page").clicked() && self.page > 0 {
                self.page -= 1;
                self.reload(host);
            }
            ui.label(muted(&format!("page {} ", self.page + 1)));
            if ui.button(">").on_hover_text("Next page").clicked() {
                if self.docs.len() as i64 >= self.request.limit {
                    self.page += 1;
                    self.reload(host);
                }
            }
            ui.separator();
            ui.label("page size");
            ui.add(egui::DragValue::new(&mut self.page_size).range(1..=500));
            ui.separator();
            ui.label("skip");
            if ui.add(egui::DragValue::new(&mut self.request.skip).range(0..=1_000_000_000)).changed() {
                self.page = (self.request.skip as usize).saturating_div(self.page_size);
                self.reload(host);
            }
            ui.separator();
            if ui.button("Fetch").clicked() {
                self.reload(host);
            }
            if ui.button("Count").clicked() {
                let _ = host.send(crate::model::Cmd::CountDocuments(server, self.request.clone()));
            }
            if can_edit && ui.button("Insert").clicked() {
                // Prefill the collection's schema: every observed field gets a
                // type-correct placeholder, so nothing has to be retyped by hand.
                self.editor_doc = self.insert_template();
                self.editor_is_new = true;
                self.editor_open = true;
                self.editor_error = None;
            }
        });

        ui.add_space(4.0);
        ui.horizontal(|ui| {
            ui.label("Filter");
            let r = ui.add(egui::TextEdit::singleline(&mut self.filter_text).desired_width(360.0).code_editor().hint_text("{\"a\": 1}"));
            if r.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                self.page = 0;
                self.reload(host);
            }
            ui.label("Sort");
            let s = ui.add(egui::TextEdit::singleline(&mut self.sort_text).desired_width(140.0).code_editor().hint_text("{\"a\": 1}"));
            if s.changed() {
                self.request.sort = crate::tabs::json_to_bson(&serde_json::from_str(&self.sort_text).unwrap_or_default()).and_then(|b| b.as_document().cloned()).filter(|d| !d.is_empty());
            }
            ui.label("Project");
            let p = ui.add(egui::TextEdit::singleline(&mut self.projection_text).desired_width(140.0).code_editor().hint_text("{\"a\": 1}"));
            if p.changed() {
                self.request.projection = crate::tabs::json_to_bson(&serde_json::from_str(&self.projection_text).unwrap_or_default()).and_then(|b| b.as_document().cloned()).filter(|d| !d.is_empty());
            }
        });

        ui.horizontal(|ui| {
            ui.selectable_value(&mut self.view, DataView::Json, "JSON");
            ui.selectable_value(&mut self.view, DataView::Table, "Table");
            if let Some(total) = self.total {
                ui.label(muted(&format!("{total} documents")));
            }
            if self.loading {
                ui.spinner();
            }
        });

        if let Some(e) = &self.error {
            ui.label(egui::RichText::new(e).color(ERROR));
        }
        if let Some(e) = &self.editor_error {
            ui.label(egui::RichText::new(e).color(ERROR));
        }

        ui.separator();
        match self.view {
            DataView::Json => self.view_json(ui, can_edit),
            DataView::Table => self.view_table(ui, can_edit),
        }
        self.editor(ui, host);
    }

    /// Build a new-document skeleton from the fields seen in this collection.
    ///
    /// Nested paths (`a.b`) become nested objects; unobserved collections fall back to
    /// `{ "_id": null }` so the editor is never empty.
    pub fn insert_template(&self) -> Document {
        if self.field_types.is_empty() {
            return bson::doc! {"_id": Bson::Null};
        }
        let mut root = Document::new();
        for (path, cell) in &self.field_types {
            let mut cursor = &mut root;
            let parts: Vec<&str> = path.split('.').collect();
            for (n, part) in parts.iter().enumerate() {
                let last = n + 1 == parts.len();
                if last {
                    cursor.insert(*part, placeholder(cell));
                } else {
                    let entry = cursor.entry(part.to_string()).or_insert_with(|| Bson::Document(Document::new()));
                    if !matches!(entry, Bson::Document(_)) {
                        *entry = Bson::Document(Document::new());
                    }
                    cursor = match entry {
                        Bson::Document(d) => d,
                        _ => unreachable!("just normalised to a document"),
                    };
                }
            }
        }
        // `_id` is the primary key: always null in a new document so mogoDB generates
        // one. Inserted last so the observed string type can't overwrite it.
        root.insert("_id", Bson::Null);
        root
    }

    fn view_json(&mut self, ui: &mut egui::Ui, can_edit: bool) {
        if self.docs.is_empty() {
            ui.centered_and_justified(|ui| ui.label(muted("No documents")));
            return;
        }
        let mut action: Option<(usize, u8)> = None;
        egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
            for (i, d) in self.docs.iter().enumerate() {
                let selected = self.selected == Some(i);
                // Compass card: index + row actions on top, pretty JSON below.
                let fill = if selected { PANEL_ALT } else { PANEL };
                let frame = egui::Frame::group(ui.style())
                    .fill(fill)
                    .stroke(egui::Stroke::new(1.0, if selected { ACCENT } else { BORDER }))
                    .inner_margin(egui::Margin::symmetric(8, 6))
                    .corner_radius(4.0);
                frame.show(ui, |ui| {
                    ui.horizontal(|ui| {
                        let num = egui::RichText::new(format!("{}", self.request.skip + i as i64)).monospace().size(11.0).color(MUTED);
                        ui.label(num);
                        ui.add_space(4.0);
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.add_enabled(can_edit, egui::Button::new("Delete").frame(false)).clicked() {
                                action = Some((i, 2));
                            }
                            if ui.add_enabled(can_edit, egui::Button::new("Clone").frame(false)).clicked() {
                                action = Some((i, 1));
                            }
                            if ui.add_enabled(can_edit, egui::Button::new("Edit").frame(false)).clicked() {
                                action = Some((i, 0));
                            }
                        });
                    });
                    let text = crate::tabs::json::to_editor_text(d);
                    let job = crate::tabs::json::highlight(&text, 12.0);
                    let resp = ui.add(egui::Label::new(job));
                    let _ = &text;
                    if resp.clicked() {
                        self.selected = Some(i);
                    }
                    if resp.double_clicked() && can_edit {
                        self.editor_doc = d.clone();
                        self.editor_is_new = false;
                        self.editor_open = true;
                    }
                });
                ui.add_space(5.0);
            }
        });
        if let Some((i, what)) = action {
            self.selected = Some(i);
            if let Some(d) = self.docs.get(i) {
                match what {
                    0 => {
                        self.editor_doc = d.clone();
                        self.editor_is_new = false;
                        self.editor_open = true;
                    }
                    1 => {
                        self.editor_doc = d.clone();
                        let _ = self.editor_doc.remove("_id");
                        self.editor_is_new = true;
                        self.editor_open = true;
                    }
                    _ => {}
                }
            }
        }
    }

    fn view_table(&mut self, ui: &mut egui::Ui, can_edit: bool) {
        if self.docs.is_empty() {
            ui.centered_and_justified(|ui| ui.label(muted("No documents")));
            return;
        }
        // Clicking a cell selects the row; Enter opens it in the editor. The toolbar keeps
        // Insert for new documents, so this view only needs read + select.
        let mut clicked: Option<usize> = None;
        let fields = self.fields.clone();
        egui_extras::TableBuilder::new(ui)
            .striped(true)
            .resizable(true)
            .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
            .column(egui_extras::Column::auto().at_least(48.0))
            // All but the last column size to content; the last absorbs the rest so the
            // grid fills the pane instead of stopping short.
            .columns(
                egui_extras::Column::auto().at_least(90.0).resizable(true),
                fields.len().saturating_sub(1),
            )
            .column(egui_extras::Column::remainder().at_least(90.0).resizable(true))
            .header(24.0, |mut header| {
                header.col(|ui| {
                    ui.strong(muted("#"));
                });
                for f in &fields {
                    header.col(|ui| {
                        ui.strong(egui::RichText::new(f).size(12.0).color(FG));
                    });
                }
            })
            .body(|mut body| {
                for (i, d) in self.docs.iter().enumerate() {
                    let selected = self.selected == Some(i);
                    body.row(20.0, |mut row| {
                        if selected {
                            row.set_selected(true);
                        }
                        let num = egui::RichText::new(format!("{}", self.request.skip + i as i64)).monospace().size(12.0).color(MUTED);
                        let r = row.col(|ui| {
                            ui.label(num);
                        });
                        if r.1.clicked() {
                            clicked = Some(i);
                        }
                        for f in &fields {
                            let v = crate::tabs::get_path(d, f);
                            let text = fmt_cell(&cell_of(v.as_ref()));
                            let rt = egui::RichText::new(text).monospace().size(12.0);
                            let r = row.col(|ui| {
                                ui.label(rt);
                            });
                            if r.1.clicked() {
                                clicked = Some(i);
                            }
                        }
                    });
                }
            });
        if let Some(i) = clicked {
            self.selected = Some(i);
        }
        if let Some(i) = self.selected {
            if ui.input(|i| i.key_pressed(egui::Key::Enter)) {
                if let Some(d) = self.docs.get(i) {
                    self.editor_doc = d.clone();
                    self.editor_is_new = false;
                    self.editor_open = true;
                }
            }
        }
        let _ = can_edit;
    }

    fn editor(&mut self, ui: &mut egui::Ui, host: &mut dyn TabHost) {
        if !self.editor_open {
            return;
        }
        let mut save = false;
        let mut close = false;
        egui::Window::new(if self.editor_is_new { "Insert document" } else { "Edit document" })
            .id(egui::Id::new("doc_editor"))
            .collapsible(false)
            .resizable(true)
            .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
            .default_size(egui::vec2(600.0, 540.0))
            .show(ui.ctx(), |ui| {
                if self.editor_is_new {
                    ui.label(muted("Set _id to null for MongoDB to generate one."));
                }
                // Editable text: ObjectIds as bare hex, dates as ISO strings.
                let mut doc_value = self.editor_doc.clone();
                let mut edit = crate::tabs::json::to_editor_text(&doc_value);
                let resp = ui.add(egui::TextEdit::multiline(&mut edit).desired_rows(16).code_editor().desired_width(f32::INFINITY).hint_text("{\"_id\": \"6af981b2ea5391ad38e3900f\", \"name\": \"x\"}"));
                if resp.changed() {
                    // Restore real BSON types for the fields this collection is known to use.
                    match crate::tabs::json::coerce_document(&edit, &self.field_types) {
                        Some(d) => {
                            self.editor_doc = d;
                            self.editor_error = None;
                        }
                        None => self.editor_error = Some("document must be a JSON object".into()),
                    }
                }
                ui.add_space(6.0);
                ui.add_space(4.0);
                ui.horizontal(|ui| {
                    let save_btn = egui::Button::new(
                        egui::RichText::new("Save").color(if self.editor_error.is_some() { FG } else { Color32::from_rgb(20, 22, 26) }),
                    )
                    .fill(if self.editor_error.is_some() { ERROR } else { ACCENT });
                    if ui.add(save_btn).clicked() {
                        save = true;
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                });
                if let Some(e) = &self.editor_error {
                    ui.label(egui::RichText::new(e).color(ERROR));
                }
            });
        if save {
            let mut doc = self.editor_doc.clone();
            if doc.get("_id").is_some_and(|v| matches!(v, Bson::Null)) {
                doc.remove("_id");
            }
            if self.editor_is_new {
                let _ = host.send(crate::model::Cmd::Insert(host.server().unwrap_or(0), self.request.clone(), vec![doc]));
            } else {
                let id = doc.get("_id").cloned().unwrap_or(Bson::Null);
                let _ = host.send(crate::model::Cmd::Insert(
                    host.server().unwrap_or(0),
                    self.request.clone(),
                    vec![doc.clone()],
                ));
                let _ = id;
            }
            self.editor_open = false;
            self.editor_error = None;
            self.reload(host);
        } else if close {
            self.editor_open = false;
        }
    }
}

/// Type-correct placeholder for a new document field.
fn placeholder(cell: &crate::tabs::Cell) -> Bson {
    use crate::tabs::Cell;
    match cell {
        Cell::Bool(_) => Bson::Boolean(false),
        Cell::Int(_) => Bson::Int32(0),
        Cell::Double(_) => Bson::Double(0.0),
        Cell::Text(_) | Cell::Oid(_) => Bson::String(String::new()),
        Cell::Date(_) => Bson::DateTime(bson::DateTime::from_millis(chrono::Utc::now().timestamp_millis())),
        Cell::Binary(_) => Bson::Binary(bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: Vec::new() }),
        Cell::Doc(_) => Bson::Document(Document::new()),
        Cell::Array(_) => Bson::Array(Vec::new()),
        Cell::Null | Cell::Missing | Cell::Other(_) => Bson::Null,
    }
}
