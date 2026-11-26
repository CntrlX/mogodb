use cfg_aliases::cfg_aliases;

fn main() {
    // The script doesn't depend on our code.
    println!("cargo:rerun-if-changed=build.rs");

    // Setup cfg aliases.
    cfg_aliases! {
        // Systems.
        android_platform: { target_os = "android" },
        web_platform: { all(target_family = "wasm", target_os = "unknown") },
        macos_platform: { target_os = "macos" },
        ios_platform: { target_os = "ios" },
        windows_platform: { target_os = "windows" },
        apple: { any(target_os = "ios", target_os = "macos") },
        free_unix: { all(unix, not(apple), not(android_platform), not(target_os = "emscripten")) },
        redox: { target_os = "redox" },

        // Native displays.
        x11_platform: { all(feature = "x11", free_unix, not(redox)) },
        wayland_platform: { all(feature = "wayland", free_unix, not(redox)) },
        orbital_platform: { redox },
    }

    // Winit defined cfgs.
    println!("cargo:rustc-check-cfg=cfg(unreleased_changelogs)");

    force_platforms();
    for v in std::env::var("CARGO_CFG_TARGET_OS").ok().map(|_| ()).into_iter().chain(std::env::var("MOGO_FORCE_X11").ok().map(|_| ())) {
        let _ = v;
    }
    if std::env::var_os("MOGO_FORCE_X11").is_some() {
        println!("cargo:rustc-cfg=x11_platform");
        println!("cargo:rustc-cfg=wayland_platform");
        println!("cargo:rustc-check-cfg=cfg(x11_platform)");
        println!("cargo:rustc-check-cfg=cfg(wayland_platform)");
    }
}

// mogo: the x11/wayland features are additive, but other crates in the graph can enable
// `rwh_06` without them, leaving eframe without a Linux backend. Re-emit the cfg aliases
// with the platform backends forced on for this build.
fn force_platforms() {
    println!("cargo:rustc-cfg=x11_platform");
    println!("cargo:rustc-cfg=wayland_platform");
    println!("cargo:rustc-check-cfg=cfg(x11_platform)");
    println!("cargo:rustc-check-cfg=cfg(wayland_platform)");
}
