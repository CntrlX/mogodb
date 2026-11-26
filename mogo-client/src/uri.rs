//! `mogodb://` URI parsing (host list, credentials, options).
//!
//! `mongodb://` is accepted as an alias so existing connection strings keep working.

use crate::error::{Error, Result};
use percent_encoding::percent_decode_str;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uri {
    pub hosts: Vec<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub default_db: Option<String>,
    pub options: HashMap<String, String>,
}

impl Uri {
    pub fn auth_source(&self) -> String {
        self.options.get("authsource").cloned().unwrap_or_else(|| "admin".to_string())
    }
    pub fn replica_set(&self) -> Option<String> {
        self.options.get("replicaset").cloned()
    }
    pub fn direct_connection(&self) -> bool {
        self.options.get("directconnection").is_some_and(|v| v != "false")
    }
    pub fn app_name(&self) -> String {
        self.options.get("appname").cloned().unwrap_or_else(|| "mogo-client".to_string())
    }
}

/// Accept `mogodb://` (canonical) and `mongodb://` (alias for existing tooling).
pub fn strip_scheme(uri: &str) -> Result<&str> {
    for scheme in ["mogodb://", "mongodb://", "mogodb+srv://", "mongodb+srv://"] {
        if let Some(rest) = uri.strip_prefix(scheme) {
            return Ok(rest);
        }
    }
    Err(Error::config("connection string must start with mogodb://"))
}

/// The canonical scheme, used when rendering URIs back to the user.
pub const SCHEME: &str = "mogodb://";

/// Default connection string shown in the UI and used when none is configured.
pub const DEFAULT_URI: &str = "mogodb://127.0.0.1:27017";

fn decode(s: &str) -> Result<String> {
    percent_decode_str(s).decode_utf8().map(|c| c.into_owned()).map_err(|_| Error::config(format!("invalid percent-encoding in {s:?}")))
}

pub fn parse(uri: &str) -> Result<Uri> {
    let rest = strip_scheme(uri)?;
    let (authority, rest) = match rest.find('/') {
        Some(i) => (&rest[..i], Some(&rest[i + 1..])),
        None => (rest, None),
    };
    let (userinfo, hosts) = match authority.rfind('@') {
        Some(i) => (Some(&authority[..i]), &authority[i + 1..]),
        None => (None, authority),
    };
    let (username, password) = match userinfo {
        Some(ui) => match ui.split_once(':') {
            Some((u, p)) => (Some(decode(u)?), Some(decode(p)?)),
            None => (Some(decode(ui)?), None),
        },
        None => (None, None),
    };
    let hosts: Vec<String> = hosts.split(',').map(|h| if h.contains(':') { h.to_string() } else { format!("{h}:27017") }).collect();
    if hosts.is_empty() || hosts.iter().any(|h| h.is_empty()) {
        return Err(Error::config("connection string has no hosts"));
    }
    let mut default_db = None;
    let mut options = HashMap::new();
    if let Some(path) = rest {
        let (path, query) = match path.find('?') {
            Some(i) => (&path[..i], Some(&path[i + 1..])),
            None => (path, None),
        };
        if !path.is_empty() {
            default_db = Some(decode(path)?);
        }
        if let Some(q) = query {
            for pair in q.split('&').filter(|p| !p.is_empty()) {
                let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
                options.insert(k.to_ascii_lowercase(), decode(v)?);
            }
        }
    }
    Ok(Uri { hosts, username, password, default_db, options })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full() {
        let u = parse("mogodb://us%40er:p%3Ass@h1:27018,h2/mydb?authSource=admin&replicaSet=rs0&appName=compass").unwrap();
        assert_eq!(u.hosts, vec!["h1:27018".to_string(), "h2:27017".to_string()]);
        assert_eq!(u.username.as_deref(), Some("us@er"));
        assert_eq!(u.password.as_deref(), Some("p:ss"));
        assert_eq!(u.default_db.as_deref(), Some("mydb"));
        assert_eq!(u.auth_source(), "admin");
        assert_eq!(u.replica_set().as_deref(), Some("rs0"));
        assert_eq!(u.app_name(), "compass");
    }

    #[test]
    fn defaults_and_errors() {
        let u = parse("mogodb://localhost").unwrap();
        assert_eq!(u.hosts, vec!["localhost:27017".to_string()]);
        assert_eq!(u.username, None);
        assert_eq!(u.auth_source(), "admin");
        assert!(parse("http://x").is_err());
    }
}