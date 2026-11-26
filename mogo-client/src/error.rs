//! Errors returned by the client: transport, protocol, server command failures.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorKind {
    /// Server replied `{ok: 0}` (or a write error).
    Command,
    /// SCRAM / authorization failure.
    Auth,
    /// Wire framing or unexpected message.
    Protocol,
    /// I/O, DNS, connect, or timeout.
    Io,
    /// Invalid URI or option.
    Config,
    /// BSON conversion.
    Bson,
}

#[derive(Debug, Clone)]
pub struct Error {
    pub kind: ErrorKind,
    /// Server error code when the failure came from the server.
    pub code: Option<i32>,
    /// Server error name (`codeName`).
    pub code_name: Option<String>,
    /// MongoDB error labels (e.g. `TransientTransactionError`).
    pub labels: Vec<String>,
    pub message: String,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Error { kind, code: None, code_name: None, labels: Vec::new(), message: message.into() }
    }
    pub fn command(code: i32, message: impl Into<String>) -> Self {
        Error { kind: ErrorKind::Command, code: Some(code), code_name: None, labels: Vec::new(), message: message.into() }
    }
    pub fn protocol(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::Protocol, message)
    }
    pub fn config(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::Config, message)
    }
    pub fn io(message: impl Into<String>) -> Self {
        Error::new(ErrorKind::Io, message)
    }

    pub fn is_transient(&self) -> bool {
        self.labels.iter().any(|l| l == "TransientTransactionError" || l == "RetryableWriteError")
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(c) = self.code {
            write!(f, " (code {c}{})", self.code_name.as_deref().unwrap_or(""))?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::io(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;