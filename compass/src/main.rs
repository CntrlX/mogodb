//! Compass-style GUI for mogoDB: browse, query, aggregate, inspect and manage collections.

mod app;
mod backend;
mod connections;
#[cfg(feature = "drive")]
mod drive;
mod model;
mod palette;
mod sidebar;
mod tabs;

use eframe::egui;

#[cfg(test)]
mod tests;

fn main() -> eframe::Result {
    #[cfg(feature = "drive")]
    if std::env::var_os("MOGO_COMPASS_DRIVE").is_some() {
        drive::run();
    }
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1440.0, 900.0])
            .with_min_inner_size([1100.0, 700.0])
            .with_title("Mogo Compass"),
        ..Default::default()
    };
    if std::env::var_os("MOGO_COMPASS_PROBE").is_some() {
        eprintln!("probe: display={:?} wayland={:?}", std::env::var("DISPLAY"), std::env::var("WAYLAND_DISPLAY"));
        eprintln!("probe: compiled ok");
    }
    let (backend, cmd_rx, resp_rx) = model::Backend::new();
    let resp_tx = backend.take_sender().expect("response sender");
    backend::spawn(cmd_rx, resp_tx);
    eframe::run_native(
        "Mogo Compass",
        options,
        Box::new(move |cc| {
            palette::install_fonts(&cc.egui_ctx);
            palette::apply_theme(&cc.egui_ctx);
            Ok(Box::new(app::CompassApp::new(backend, resp_rx)))
        }),
    )
}