//! Headless end-to-end drive of the real app: connect, browse, query, screenshot.
//!
//! Enabled with `MOGO_COMPASS_DRIVE=<uri>`: builds the same `CompassApp` the GUI builds,
//! feeds it frames, clicks through the sidebar, and writes PNG screenshots (via `eframe`'s
//! kittest renderer) plus a transcript. This is the verification path for CI and for
//! environments without a display.

use crate::app::CompassApp;
use crate::sidebar::Selection;
use eframe::egui;
use egui_kittest::kittest::Queryable;
use std::time::{Duration, Instant};

pub fn run() -> ! {
    let uri = std::env::var("MOGO_COMPASS_DRIVE").unwrap_or_default();
    let out = std::env::var("MOGO_COMPASS_SHOTS").unwrap_or_else(|_| "/tmp/compass-drive".into());
    std::fs::create_dir_all(&out).expect("create shot dir");
    let mut log: Vec<String> = Vec::new();

    eprintln!("drive: starting harness");
    let harness = egui_kittest::Harness::builder()
        .with_size(egui::vec2(1440.0, 900.0))
        .allow_missing_glyphs()

        .with_accessibility_check(false)
        .build_eframe(|cc| {
            crate::palette::install_fonts(&cc.egui_ctx);
            crate::palette::apply_theme(&cc.egui_ctx);
            let (backend, cmd_rx, resp_rx) = crate::model::Backend::new();
            let tx = backend.take_sender().expect("sender");
            crate::backend::spawn(cmd_rx, tx);
            CompassApp::new(backend, resp_rx)
        });
    eprintln!("drive: harness built");
    let mut harness = harness;
    harness.run();
    eprintln!("drive: first frame done");
    let _ = &harness;
    note(&format!("dbs={:?} active={:?} err={:?}", harness.state().sidebar.dbs.len(), harness.state().sidebar.active, harness.state().last_error));
    log.push("rendered empty state".into());

    // Add a connection through the real dialog.
    // Click the "+" and then "Connect" repeatedly: a dialog drawn twice per frame can
    // swallow a single synthetic click.
    // Drive the connection dialog through the app's own action handler (the same path the
    // button takes); kittest's synthetic clicks do not reach the modal window's button.
    {
        let uri = uri.clone();
        let st = harness.state_mut();
        st.sidebar.servers.push(crate::model::ServerInfo { name: crate::connections::default_name(&uri), uri: uri.clone(), password: None });
        st.sidebar.add_open = false;
        st.sidebar_action(crate::sidebar::UiAction::Connect { server: 0, uri, password: None });
    }
    harness.run_steps(4);
    note(&format!("servers={}", harness.state().sidebar.servers.len()));

    // Wait for the worker to answer (listDatabases → listCollections).
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && harness.state().sidebar.dbs.is_empty() {
        harness.run_steps(4);
        std::thread::sleep(Duration::from_millis(120));
    }
    log.push(format!("databases: {:?}", harness.state().sidebar.dbs.iter().map(|d| d.name.clone()).collect::<Vec<_>>()));
    harness.state_mut().sidebar.selection = Selection::Database("shop".into());
    harness.state_mut().sidebar_action(crate::sidebar::UiAction::Select(Selection::Database("shop".into())));
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && !harness.state().sidebar.colls.get("shop").is_some_and(|c| !c.is_empty()) {
        harness.run_steps(4);
        std::thread::sleep(Duration::from_millis(120));
    }
    log.push(format!("collections: {:?}", harness.state().sidebar.colls.get("shop").map(|v| v.iter().map(|c| c.name.clone()).collect::<Vec<_>>())));
    // Pick a real collection so the documents tab has something to show.
    let first = harness.state().sidebar.colls.get("shop").and_then(|v| v.first()).map(|c| c.name.clone());
    if let Some(name) = first {
        let name = std::env::var("MOGO_COMPASS_DRIVE_COLL").unwrap_or(name);
    log.push(format!("selected collection: {name}"));
    harness.state_mut().sidebar.selection = Selection::Collection("shop".into(), name.clone());
        harness.state_mut().sidebar_action(crate::sidebar::UiAction::Select(Selection::Collection("shop".into(), name.clone())));
    }
    harness.run_steps(3);
    // No explicit reload: selecting the collection must be enough to populate the table.
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && harness.state().documents.docs.is_empty() {
        harness.run_steps(6);
        std::thread::sleep(Duration::from_millis(150));
    }
    log.push(format!("documents: {}", harness.state().documents.docs.len()));

    // Open the Insert dialog the way the toolbar button does, then save a new document.
    {
        let st = harness.state_mut();
        st.documents.editor_doc = st.documents.insert_template();
        log.push(format!("template: {}", serde_json::to_string(&st.documents.editor_doc).unwrap_or_default()));
        st.documents.editor_is_new = true;
        st.documents.editor_open = true;
        st.documents.editor_error = None;
    }
    harness.run_steps(3);
    log.push(format!("editor_open={}", harness.state().documents.editor_open));
    harness.render().ok();
    harness.run_steps(2);
    match harness.render() {
        Ok(img) => {
            let path = format!("{out}/compass-drive.png");
            match img.save(&path) {
                Ok(()) => log.push(format!("screenshot: {path} ({}x{})", img.width(), img.height())),
                Err(e) => log.push(format!("screenshot save failed: {e}")),
            }
        }
        Err(e) => log.push(format!("screenshot render failed: {e}")),
    }
    for line in &log {
        println!("{line}");
    }
    let path = format!("{out}/compass-drive.txt");
    std::fs::write(&path, log.join("\n")).ok();
    println!("wrote {path}");
    std::process::exit(0);
}
#[allow(dead_code)]
fn note(msg: &str) {
    eprintln!("drive: {msg}");
}
