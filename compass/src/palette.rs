//! Colour tokens and helpers shared by every tab.

use eframe::egui::{Color32, RichText};

pub const BG: Color32 = Color32::from_rgb(20, 22, 26);
pub const PANEL: Color32 = Color32::from_rgb(27, 30, 35);
pub const PANEL_ALT: Color32 = Color32::from_rgb(33, 37, 43);
pub const BORDER: Color32 = Color32::from_rgb(52, 58, 66);
pub const FG: Color32 = Color32::from_rgb(226, 230, 236);
pub const MUTED: Color32 = Color32::from_rgb(138, 148, 160);
pub const ACCENT: Color32 = Color32::from_rgb(76, 175, 120);
pub const ACCENT_HOVER: Color32 = Color32::from_rgb(94, 199, 140);
pub const WARN: Color32 = Color32::from_rgb(224, 168, 80);
pub const ERROR: Color32 = Color32::from_rgb(232, 104, 104);
pub const INFO: Color32 = Color32::from_rgb(96, 160, 232);

pub fn heading(text: &str) -> RichText {
    RichText::new(text).size(15.0).strong().color(FG)
}

pub fn muted(text: &str) -> RichText {
    RichText::new(text).color(MUTED)
}

pub fn mono(text: &str) -> RichText {
    RichText::new(text).family(egui::FontFamily::Monospace).size(12.0)
}

pub fn accent(text: &str) -> RichText {
    RichText::new(text).color(ACCENT)
}

pub fn apply_theme(ctx: &egui::Context) {
    let mut style = (*ctx.style_of(egui::Theme::Dark)).clone();
    style.visuals.dark_mode = true;
    style.visuals.override_text_color = Some(FG);
    style.visuals.panel_fill = BG;
    style.visuals.window_fill = PANEL;
    style.visuals.extreme_bg_color = PANEL_ALT;
    style.visuals.selection.bg_fill = ACCENT;
    style.visuals.selection.stroke = egui::Stroke::new(1.0, ACCENT_HOVER);
    style.visuals.widgets.noninteractive.bg_fill = PANEL_ALT;
    style.visuals.widgets.inactive.bg_fill = PANEL_ALT;
    style.visuals.widgets.hovered.bg_fill = BORDER;
    style.visuals.widgets.active.bg_fill = ACCENT;
    style.visuals.widgets.noninteractive.fg_stroke = egui::Stroke::new(1.0, FG);
    style.visuals.widgets.inactive.fg_stroke = egui::Stroke::new(1.0, FG);
    style.visuals.widgets.hovered.fg_stroke = egui::Stroke::new(1.0, FG);
    style.visuals.widgets.active.fg_stroke = egui::Stroke::new(1.0, FG);
    style.visuals.window_stroke = egui::Stroke::new(1.0, BORDER);
    style.visuals.selection.bg_fill = ACCENT;
    style.spacing.button_padding = egui::vec2(8.0, 5.0);
    style.spacing.item_spacing = egui::vec2(8.0, 6.0);
    ctx.set_style_of(egui::Theme::Dark, style);
}

/// Section title with a hairline underneath.
pub fn section(ui: &mut egui::Ui, title: &str) {
    ui.add_space(4.0);
    ui.label(heading(title));
    let rect = ui.available_rect_before_wrap();
    let y = ui.min_rect().bottom() + 2.0;
    let x = rect.left();
    let w = rect.width();
    ui.painter().line_segment([egui::pos2(x, y), egui::pos2(x + w, y)], egui::Stroke::new(1.0, BORDER));
    ui.add_space(6.0);
}
/// egui's built-in fonts lack geometric shapes (●, ▾, ▤), so the UI uses ASCII markers
/// and any stray glyph renders as tofu rather than panicking.
pub fn install_fonts(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    if let Some(family) = fonts.families.get_mut(&egui::FontFamily::Proportional) {
        if !family.iter().any(|f| f == "egui-icons") {
            family.push("egui-icons".to_owned());
        }
    }
    ctx.set_fonts(fonts);
    ctx.set_missing_glyph_policy(egui::MissingGlyphPolicy::Tofu);
}
