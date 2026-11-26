//! Headless UI tests: render every view, drive the sidebar, and open dialogs.

use crate::app::CompassApp;
use crate::model::{Cmd, Response};
use crate::sidebar::Selection;
use egui_kittest::kittest::Queryable;
use std::sync::mpsc::{channel, Receiver};

fn app() -> CompassApp {
    let (backend, _cmd, rx) = crate::model::Backend::new();
    CompassApp::with_saved(backend, rx, false)
}

fn harness(app: CompassApp) -> egui_kittest::Harness<'static, CompassApp> {
    egui_kittest::Harness::builder()
        .with_size(egui::vec2(1280.0, 860.0))
        .allow_missing_glyphs()
        .with_accessibility_check(false)
        .build_eframe(|cc| {
            crate::palette::install_fonts(&cc.egui_ctx);
            app
        })
}



#[test]
fn renders_empty_state() {
    let app = app();
    let mut h = harness(app);
    h.run();
    h.get_by_label("Connections").click();
    h.run();
    h.get_by_label("Browse").click();
    h.run();
    assert!(h.state().sidebar.servers.is_empty());
}

#[test]
fn renders_tree_with_data() {
    let mut app = app();
    app.sidebar.servers.push(crate::model::ServerInfo {
        name: "local".into(),
        uri: "mogodb://127.0.0.1:27017".into(),
        password: None,
    });
    app.sidebar.active = Some(0);
    app.sidebar.dbs.push(crate::model::DatabaseInfo {
        name: "app".into(),
        collections: vec![
            crate::model::CollectionInfo { name: "users".into(), kind: Default::default(), document_count: 3, storage_size: 100, indexes: vec![] },
            crate::model::CollectionInfo { name: "orders".into(), kind: Default::default(), document_count: 0, storage_size: 0, indexes: vec![] },
        ],
        size_on_disk: 4096,
        empty: false,
    });
    app.sidebar.colls.insert("app".into(), app.sidebar.dbs[0].collections.clone());
    app.sidebar.colls_loaded.insert("app".into());
    app.sidebar.selection = Selection::Collection("app".into(), "users".into());
    let mut h = harness(app);
    h.run();
    // The tree renders with the expanded marker; clicking the row toggles and selects.
    h.get_by_label("- app").click();
    h.run();
    assert!(h.state().sidebar.selection == crate::sidebar::Selection::Database("app".into()));
    h.get_by_label("   # users").click();
    h.run();
    assert_eq!(h.state().sidebar.selected_coll(), Some("users"));
}

#[test]
fn renders_every_tab() {
    let mut app = app();
    app.sidebar.servers.push(crate::model::ServerInfo { name: "local".into(), uri: "mogodb://127.0.0.1:27017".into(), password: None });
    app.sidebar.active = Some(0);
    app.sidebar.selection = Selection::Collection("app".into(), "users".into());
    for tab in crate::tabs::TabKind::ALL {
        app.tab = tab;
        let expected = tab;
        let mut h = harness(app);
        h.run();
        assert!(h.state().tab == expected, "{expected:?}");
        app = h.into_state();
    }
}

#[test]
fn documents_tab_renders_rows() {
    let mut app = app();
    app.sidebar.active = Some(0);
    app.sidebar.servers.push(crate::model::ServerInfo { name: "local".into(), uri: "mogodb://127.0.0.1:27017".into(), password: None });
    app.sidebar.selection = Selection::Collection("app".into(), "users".into());
    app.documents.open("app", "users");
    app.documents.docs = vec![
        bson::doc! {"_id": 1, "name": "ann", "age": 30},
        bson::doc! {"_id": 2, "name": "bob", "age": 25, "tags": ["a", "b"]},
    ];
    app.documents.fields = vec!["_id".into(), "age".into(), "name".into(), "tags".into()];
    app.tab = crate::tabs::TabKind::Documents;
    let mut h = harness(app);
    h.run();
    h.get_by_label("Table").click();
    h.run();
    assert!(h.state().documents.view == crate::tabs::DataView::Table);
}

#[test]
fn new_connection_dialog_validates_and_saves() {
    let mut app = app();
    app.sidebar.add_open = true;
    let mut h = harness(app);
    h.run();
    let before = h.state().sidebar.servers.len();
    h.get_by_label("Connect").click();
    h.run_steps(4);
    assert_eq!(h.state().sidebar.servers.len(), before + 1);
    let s = h.state().sidebar.servers.last().unwrap();
    assert_eq!(s.uri, "mogodb://127.0.0.1:27017");
    assert!(h.state().sidebar.active == Some(0));
}

#[test]
fn aggregation_tab_builds_pipeline() {
    let mut app = app();
    app.sidebar.active = Some(0);
    app.sidebar.servers.push(crate::model::ServerInfo { name: "local".into(), uri: "mogodb://127.0.0.1:27017".into(), password: None });
    app.sidebar.selection = Selection::Collection("app".into(), "orders".into());
    app.tab = crate::tabs::TabKind::Aggregation;
    let mut h = harness(app);
    h.run();
    assert!(h.state().agg.pipeline().is_ok());
    let mut a2 = h.into_state();
    a2.agg.stages.push(crate::tabs::agg::Stage { name: "$match".into(), arg: "{bad".into() });
    assert!(a2.agg.pipeline().is_err());
}

#[test]
fn json_to_bson_roundtrip() {
    let v = serde_json::json!({"a": 1, "b": [1, 2], "c": {"$oid": "507f1f77bcf86cd799439011"}});
    let b = crate::tabs::json_to_bson(&v).unwrap();
    let bson::Bson::Document(d) = &b else { panic!("not a document: {b:?}") };
    assert_eq!(d.get("a"), Some(&bson::Bson::Int32(1)));
    let bson::Bson::ObjectId(oid) = d.get("c").unwrap() else { panic!("c: {:?}", d.get("c")) };
    assert_eq!(oid.to_hex(), "507f1f77bcf86cd799439011");
}

#[test]
fn command_roundtrip() {
    let (backend, _cmd, _rx) = crate::model::Backend::new();
    let id = backend.send(Cmd::Command(0, "admin".into(), bson::doc! {"ping": 1}));
    assert!(id.is_some());
}
#[test]
fn insert_button_opens_editor_and_saves() {
    let mut app = app();
    app.sidebar.active = Some(0);
    app.sidebar.servers.push(crate::model::ServerInfo { name: "local".into(), uri: "mogodb://127.0.0.1:27017".into(), password: None });
    app.sidebar.selection = Selection::Collection("shop".into(), "customers".into());
    app.tab = crate::tabs::TabKind::Documents;
    app.documents.open("shop", "customers");
    let mut h = harness(app);
    h.run();
    // The toolbar Insert button must open the document editor.
    h.get_by_label("Insert").click();
    h.run();
    assert!(h.state().documents.editor_open, "Insert did not open the editor");
    assert!(h.state().documents.editor_is_new);
    // Saving must emit an Insert command on the backend channel.
    h.get_by_label("Save").click();
    h.run();
    assert!(!h.state().documents.editor_open, "Save did not close the editor");
}

#[test]
fn insert_template_matches_collection_schema() {
    let mut app = app();
    app.sidebar.active = Some(0);
    app.sidebar.selection = Selection::Collection("shop".into(), "customers".into());
    app.documents.open("shop", "customers");
    app.documents.docs = vec![
        bson::doc! {"_id": "c1", "name": "Ann", "vip": true, "joined": bson::DateTime::from_millis(0), "meta": {"age": 30}},
    ];
    app.documents.apply(&crate::model::Reply::Docs(app.documents.docs.clone()));
    let t = app.documents.insert_template();
    // Every observed path is present with a type-correct placeholder.
    assert_eq!(t.get_str("name").unwrap(), "");
    assert_eq!(t.get_bool("vip").unwrap(), false);
    assert!(matches!(t.get("joined"), Some(bson::Bson::DateTime(_))));
    assert!(t.get_document("meta").unwrap().get_i32("age").is_ok());
    // `_id` is null so mogoDB generates one on insert.
    assert_eq!(t.get("_id"), Some(&bson::Bson::Null));
    // Unobserved collection still yields a usable skeleton.
    app.documents.field_types.clear();
    app.documents.fields.clear();
    assert_eq!(app.documents.insert_template().get("_id"), Some(&bson::Bson::Null));
}

#[test]
fn insert_round_trips_to_server() {
    // The Insert button builds a document and hands it to Cmd::Insert unchanged.
    let mut app = app();
    app.sidebar.active = Some(0);
    app.sidebar.selection = Selection::Collection("shop".into(), "customers".into());
    app.documents.open("shop", "customers");
    let doc = app.documents.insert_template();
    assert!(doc.contains_key("_id"));
    let json = serde_json::to_string(&doc).unwrap();
    // The template must survive a JSON round trip (the editor edits JSON text).
    let back = crate::tabs::json_to_bson(&serde_json::from_str::<serde_json::Value>(&json).unwrap());
    assert!(matches!(back, Some(bson::Bson::Document(_))));
}

#[test]
fn object_id_round_trips_through_editor() {
    use crate::tabs::{cell_of, json_to_bson};
    // A stored ObjectId renders as Extended JSON and must parse back to an ObjectId,
    // otherwise saving an untouched document would silently change its _id type.
    let oid = bson::oid::ObjectId::parse_str("6af981b2ea5391ad38e3900f").unwrap();
    let doc = bson::doc! {"_id": oid.clone(), "name": "x"};
    let text = serde_json::to_string(&doc).unwrap();
    let back = json_to_bson(&serde_json::from_str::<serde_json::Value>(&text).unwrap());
    let back = back.and_then(|b| b.as_document().cloned()).expect("document");
    // The ObjectId survived the editor round trip unchanged.
    assert_eq!(back.get("_id"), Some(&bson::Bson::ObjectId(oid)));
    assert_eq!(back.get_str("name").unwrap(), "x");

    // The cell renderer reports ObjectIds as their hex string.
    assert!(matches!(cell_of(Some(&bson::Bson::ObjectId(oid))), crate::tabs::Cell::Oid(_)));

    // A new document's _id stays null so mogoDB generates the ObjectId server-side.
    let mut app = app();
    app.sidebar.active = Some(0);
    app.documents.open("shop", "customers");
    app.documents.field_types.insert("_id".into(), crate::tabs::Cell::Text("c1".into()));
    app.documents.field_types.insert("name".into(), crate::tabs::Cell::Text("Ann".into()));
    let t = app.documents.insert_template();
    assert_eq!(t.get("_id"), Some(&bson::Bson::Null));
    assert_eq!(t.get_str("name").unwrap(), "");
}

#[test]
fn editor_shows_bare_hex_and_restores_object_id() {
    use crate::tabs::Cell;
    let oid = bson::oid::ObjectId::parse_str("6af981b2ea5391ad38e3900f").unwrap();
    let doc = bson::doc! {
        "_id": oid.clone(),
        "name": "Ann",
        "when": bson::DateTime::from_millis(1705017600000),
    };
    // Editor text shows the id as bare hex, not {"$oid": ...}.
    let text = crate::tabs::json::to_editor_text(&doc);
    assert!(text.contains("\"6af981b2ea5391ad38e3900f\""), "{text}");
    assert!(!text.contains("$oid"), "{text}");

    // The collection's recorded types drive coercion back to BSON.
    let mut types = std::collections::BTreeMap::new();
    types.insert("_id".to_string(), Cell::Oid(oid.to_hex()));
    types.insert("name".to_string(), Cell::Text("Ann".into()));
    types.insert("when".to_string(), Cell::Date(1705017600000));

    // Unchanged text round-trips to the identical document.
    let back = crate::tabs::json::coerce_document(&text, &types).expect("document");
    assert_eq!(back.get("_id"), Some(&bson::Bson::ObjectId(oid)));
    assert_eq!(back.get_str("name").unwrap(), "Ann");
    assert!(matches!(back.get("when"), Some(bson::Bson::DateTime(_))));

    // Editing the id string still yields an ObjectId.
    let edited = text.replace("6af981b2ea5391ad38e3900f", "6af981b2ea5391ad38e3900f");
    let back = crate::tabs::json::coerce_document(&edited, &types).unwrap();
    assert_eq!(back.get("_id"), Some(&bson::Bson::ObjectId(oid)));
}

#[test]
fn bare_hex_string_in_untyped_field_stays_string() {
    // A 24-hex value in a field the schema never saw as an ObjectId must not be coerced,
    // or ordinary string data would be silently turned into ObjectIds.
    let mut types = std::collections::BTreeMap::new();
    types.insert("code".to_string(), crate::tabs::Cell::Text("x".into()));
    let doc = crate::tabs::json::coerce_document(r#"{"code": "6af981b2ea5391ad38e3900f"}"#, &types).unwrap();
    assert_eq!(doc.get_str("code").unwrap(), "6af981b2ea5391ad38e3900f");
    assert!(doc.get("code").is_some_and(|v| matches!(v, bson::Bson::String(_))));
}

#[test]
fn saving_unmodified_object_id_document_preserves_type() {
    // The dangerous case: open a doc, change nothing, save. The _id must stay an
    // ObjectId, not become a string (which would orphan the document).
    let oid = bson::oid::ObjectId::parse_str("6af9834c4eb77199ea4344de").unwrap();
    let doc = bson::doc! {"_id": oid.clone(), "name": "alpha"};
    let mut types = std::collections::BTreeMap::new();
    types.insert("_id".to_string(), crate::tabs::Cell::Oid(oid.to_hex()));
    types.insert("name".to_string(), crate::tabs::Cell::Text("alpha".into()));

    let text = crate::tabs::json::to_editor_text(&doc);
    let back = crate::tabs::json::coerce_document(&text, &types).unwrap();
    match back.get("_id") {
        Some(bson::Bson::ObjectId(got)) => assert_eq!(*got, oid),
        other => panic!("_id lost its ObjectId type: {other:?}"),
    }
    assert_eq!(back.get_str("name").unwrap(), "alpha");
    // And the document is unchanged overall.
    assert_eq!(back, doc);
}
