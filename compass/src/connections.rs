//! Saved connections (no passwords on disk) and URI helpers.

use crate::model::ServerInfo;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Store {
    pub servers: Vec<SavedServer>,
    pub active: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedServer {
    pub name: String,
    pub uri: String,
}

impl SavedServer {
    pub fn to_info(&self, password: Option<String>) -> ServerInfo {
        ServerInfo { name: self.name.clone(), uri: self.uri.clone(), password }
    }
}

pub fn config_path() -> PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from).unwrap_or_else(|| {
        let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
        home.join(".config")
    });
    base.join("mogocompass").join("connections.json")
}

pub fn load() -> Store {
    fs::read_to_string(config_path()).ok().and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default()
}

pub fn save(store: &Store) {
    let path = config_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let _ = fs::write(path, serde_json::to_string_pretty(store).unwrap_or_default());
}

/// Credentials embedded in the URI, so the password can be removed from the visible string.
pub fn split_credentials(uri: &str) -> (String, Option<String>) {
    let Ok(rest) = mogo_client::uri::strip_scheme(uri) else { return (uri.to_string(), None) };
    let Some(at) = rest.find('@') else { return (uri.to_string(), None) };
    let userinfo = &rest[..at];
    let Some(colon) = userinfo.find(':') else { return (uri.to_string(), None) };
    let password = percent_decode(&userinfo[colon + 1..]);
    let cleaned = format!("{}{}", mogo_client::uri::SCHEME, &rest[at + 1..]);
    (cleaned, Some(password))
}

/// Default label: host list without credentials (a bare host keeps no port).
pub fn default_name(uri: &str) -> String {
    let host = mogo_client::uri::strip_scheme(uri).unwrap_or(uri);
    let host = host.split('@').next_back().unwrap_or(host);
    let host = host.split('/').next().unwrap_or(host);
    let host = host.split('?').next().unwrap_or(host);
    if host.is_empty() {
        return "localhost:27017".into();
    }
    if host.contains(':') {
        return host.to_string();
    }
    format!("{host}:27017")
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(b) = hex {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_credentials() {
        assert_eq!(default_name("mogodb://user:pw@db1:27018,db2/mydb?replicaSet=rs0"), "db1:27018,db2");
        assert_eq!(default_name("mogodb://localhost"), "localhost:27017");
        assert_eq!(default_name("mogodb://u@h1:27018,db2/x"), "h1:27018,db2");
        let (cleaned, pw) = split_credentials("mogodb://root:se%3Dcret@127.0.0.1:27031/?authSource=admin");
        assert_eq!(cleaned, "mogodb://127.0.0.1:27031/?authSource=admin");
        assert_eq!(pw.as_deref(), Some("se=cret"));
    }
}