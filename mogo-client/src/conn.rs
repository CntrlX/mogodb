//! One connection to a mongod: handshake, SCRAM authentication, OP_MSG commands.

use crate::error::{Error, ErrorKind, Result};
use crate::scram::{Mechanism, ScramClient};
use crate::wire::{self, Message};
use bson::{Bson, Document};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, Default)]
pub struct ServerInfo {
    pub hello: Document,
    pub max_wire_version: i32,
    pub min_wire_version: i32,
    pub max_bson_object_size: i64,
    pub max_message_size_bytes: i64,
    pub is_replica_set: bool,
    pub set_name: Option<String>,
}

impl ServerInfo {
    pub fn version(&self) -> String {
        self.hello.get_str("version").unwrap_or("?").to_string()
    }
    pub fn from_hello(d: &Document) -> ServerInfo {
        ServerInfo {
            max_wire_version: d.get_i32("maxWireVersion").unwrap_or(0),
            min_wire_version: d.get_i32("minWireVersion").unwrap_or(0),
            max_bson_object_size: d.get_i64("maxBsonObjectSize").unwrap_or(16_777_216),
            max_message_size_bytes: d.get_i64("maxMessageSizeBytes").unwrap_or(48_000_000),
            is_replica_set: d.get_array("hosts").is_ok(),
            set_name: d.get_str("setName").ok().map(str::to_string),
            hello: d.clone(),
        }
    }
}

pub struct Connection {
    stream: TcpStream,
    next_id: i32,
    pub info: ServerInfo,
    pub auth_source: String,
    pub timeout: Duration,
    pub app_name: String,
    authenticated: bool,
    session_ok: bool,
    user: Option<String>,
    password: String,
}

impl Connection {
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Connection> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| Error::io(format!("connect to {addr} timed out")))?
            .map_err(|e| Error::io(format!("connect to {addr}: {e}")))?;
        stream.set_nodelay(true).ok();

        Ok(Connection { stream, next_id: 1, info: ServerInfo::default(), auth_source: "admin".into(), timeout, app_name: String::new(), authenticated: false, session_ok: false, user: None, password: String::new() })
    }

    /// `tokio::time::timeout` wraps every read/write in `command`, so this only affects
    /// how long a `getMore` may wait server-side (the caller sets `maxTimeMS` for that).
    pub fn set_timeout(&mut self, t: Duration) {
        self.timeout = t;
    }

    /// Legacy OP_QUERY handshake (what every driver sends first).
    pub async fn handshake(&mut self, app_name: &str, user: Option<&str>) -> Result<()> {
        let mut client = bson::doc! {
            "driver": {"name": "mogo-client", "version": env!("CARGO_PKG_VERSION")},
            "os": {"type": std::env::consts::OS, "architecture": std::env::consts::ARCH},
            "platform": format!("rust {}", std::env::consts::ARCH),
            "application": {"name": app_name},
        };
        if let Some(u) = user {
            client.insert("saslSupportedMechs", format!("{}.{}", self.auth_source, u));
        }
        let query = bson::doc! {"hello": 1, "helloOk": true, "client": client, "compression": Bson::Array(Vec::new())};
        let id = self.next_id();
        let buf = wire::encode_op_query(id, &format!("{}.$cmd", self.auth_source), &query);
        self.write(&buf).await?;
        let reply = self.read_message().await?;
        let doc = self.expect_reply(reply)?;
        if ok_value(&doc).unwrap_or(0.0) != 1.0 {
            return Err(self.command_error(&doc));
        }
        self.info = ServerInfo::from_hello(&doc);

        Ok(())
    }

    /// SCRAM-SHA-256 (preferred) or SCRAM-SHA-1 authentication.
    pub async fn authenticate(&mut self, user: &str, password: &str) -> Result<()> {
        let mech = self
            .info
            .hello
            .get_array("saslSupportedMechs")
            .ok()
            .and_then(|a| a.iter().filter_map(Bson::as_str).find_map(Mechanism::parse))
            .unwrap_or(Mechanism::Sha256);
        let mut scram = ScramClient::new(mech, user, password);
        self.password = password.to_string();
        let start = self
            .command_inner(
                &self.auth_source.clone(),
                bson::doc! {
                    "saslStart": 1,
                    "mechanism": mech.name(),
                    "payload": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: scram.client_first() },
                    "autoAuthorize": 1,
                    "options": {"skipEmptyExchange": false},
                },
            )
            .await?;
        let conversation_id = start.get_i32("conversationId").unwrap_or(0);
        let payload = binary_bytes(start.get("payload")).unwrap_or_default();
        let final_msg = scram.step(&payload)?;
        let cont = self
            .command_inner(
                &self.auth_source.clone(),
                bson::doc! {
                    "saslContinue": 1,
                    "conversationId": conversation_id,
                    "payload": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: final_msg },
                },
            )
            .await?;
        if cont.get_i32("code").is_ok() {
            return Err(self.command_error(&cont));
        }
        let payload = binary_bytes(cont.get("payload")).unwrap_or_default();
        if !payload.is_empty() {
            scram.verify_server_final(&payload)?;
        } else if !cont.get_bool("done").unwrap_or(false) {
            // Empty payload and `done: false`: the server refused (e.g. wrong password).
            return Err(Error::new(ErrorKind::Auth, "authentication failed"));
        }
        // The verified server signature proves authentication; mongod authorizes the session
        // on the next command that runs on this connection.
        self.user = Some(user.to_string());
        self.authenticated = true;
        self.session_ok = true;
        Ok(())
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    /// Authenticate if credentials are configured and this connection has not yet done so.
    pub async fn ensure_authenticated(&mut self, user: Option<&str>, pass: Option<&str>) -> Result<()> {
        if self.authenticated {
            return Ok(());
        }
        match (user, pass) {
            (Some(u), Some(p)) => self.authenticate(u, p).await,
            (None, _) | (_, None) => Ok(()),
        }
    }

    /// Connections that have completed SCRAM, keyed by `lsid` (mongod tracks auth per session).
    /// Our pool authenticates every connection it creates, so this is only a safety net.
    pub fn authenticated(&self) -> bool {
        self.authenticated
    }

    /// Run one command. mongod authorizes each connection separately, so the connection is
    /// always authenticated by the pool before use (see `Pool::checkout`).
    pub async fn command(&mut self, db: &str, cmd: Document) -> Result<Document> {
        self.command_inner(db, cmd).await
    }

    async fn command_inner(&mut self, db: &str, mut cmd: Document) -> Result<Document> {
        if !cmd.contains_key("$db") {
            cmd.insert("$db", db);
        }
        let id = self.next_id();
        let buf = wire::encode_op_msg(id, &cmd, &[]);
        self.write(&buf).await?;
        let reply = self.read_message().await?;
        let doc = self.expect_reply(reply)?;
        match ok_value(&doc) {
            Some(v) if v == 1.0 => Ok(doc),
            Some(_) => Err(self.command_error(&doc)),
            None if doc.contains_key("errmsg") => Err(self.command_error(&doc)),
            None => Ok(doc),
        }
    }

    /// Run a command whose reply may be `ok: 1` with `writeErrors`; returns the raw reply.
    pub async fn command_write(&mut self, db: &str, cmd: Document) -> Result<Document> {
        self.command(db, cmd).await
    }

    /// The connection's authenticated user, if any.
    pub fn authenticated_user(&self) -> Option<&str> {
        self.user.as_deref().filter(|_| self.authenticated)
    }

    fn next_id(&mut self) -> i32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        id
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        tokio::time::timeout(self.timeout, async {
            self.stream.write_all(buf).await?;
            self.stream.flush().await
        })
        .await
        .map_err(|_| Error::io("write timed out"))??;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<Vec<u8>> {
        tokio::time::timeout(self.timeout, async {
            let mut head = [0u8; 4];
            self.stream.read_exact(&mut head).await?;
            let len = i32::from_le_bytes(head) as usize;
            if !(16..=wire::MAX_MESSAGE_SIZE).contains(&len) {
                return Err(Error::protocol(format!("bad message length {len}")));
            }
            let mut buf = vec![0u8; len];
            buf[..4].copy_from_slice(&head);
            self.stream.read_exact(&mut buf[4..]).await?;
            Ok(buf)
        })
        .await
        .map_err(|_| Error::io("read timed out"))?
    }

    fn expect_reply(&self, buf: Vec<u8>) -> Result<Document> {
        match wire::parse(&buf)? {
            Message::Msg { doc, .. } => Ok(doc),
            Message::Reply(d) => Ok(d),
        }
    }

    pub fn command_error(&self, doc: &Document) -> Error {
        let code = doc.get_i32("code").ok().or_else(|| doc.get_i32("codeName").ok().map(|_| 0));
        let mut e = Error::command(
            code.unwrap_or(0),
            doc.get_str("errmsg").unwrap_or("command failed").to_string(),
        );
        if matches!(code, Some(18) | Some(13)) {
            e.kind = ErrorKind::Auth;
        }
        e.code_name = doc.get_str("codeName").ok().map(str::to_string);
        if let Some(Bson::Array(labels)) = doc.get("errorLabels") {
            e.labels = labels.iter().filter_map(Bson::as_str).map(str::to_string).collect();
        }
        if let Some(Bson::Array(writes)) = doc.get("writeErrors") {
            if let Some(first) = writes.first().and_then(Bson::as_document) {
                e.code = first.get_i32("code").ok().or(e.code);
                e.message = first.get_str("errmsg").unwrap_or(&e.message).to_string();
                if let Some(extra) = first.get_document("errInfo").ok() {
                    let _ = extra;
                }
            }
        }
        e
    }
}

pub fn ok_value(doc: &Document) -> Option<f64> {
    match doc.get("ok") {
        Some(Bson::Double(d)) => Some(*d),
        Some(Bson::Int32(i)) => Some(*i as f64),
        Some(Bson::Int64(i)) => Some(*i as f64),
        Some(Bson::Boolean(b)) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

pub fn binary_bytes(v: Option<&Bson>) -> Option<Vec<u8>> {
    match v {
        Some(Bson::Binary(b)) => Some(b.bytes.clone()),
        _ => None,
    }
}

/// The cursor reply of `find`/`aggregate`: cursor id, namespace, first batch.
#[derive(Debug, Clone)]
pub struct CursorReply {
    pub id: i64,
    pub ns: String,
    pub docs: Vec<Document>,
}

pub fn parse_cursor(doc: &Document) -> Result<CursorReply> {
    let c = doc.get_document("cursor").map_err(|_| Error::protocol("reply has no cursor"))?;
    Ok(CursorReply {
        id: c.get_i64("id").unwrap_or(0),
        ns: c.get_str("ns").unwrap_or("").to_string(),
        docs: c.get_array("firstBatch").or_else(|_| c.get_array("nextBatch")).map(|a| a.iter().filter_map(Bson::as_document).cloned().collect()).unwrap_or_default(),
    })
}

pub fn next_batch(doc: &Document) -> Result<(i64, Vec<Document>)> {
    let c = doc.get_document("cursor").map_err(|_| Error::protocol("getMore reply has no cursor"))?;
    Ok((
        c.get_i64("id").unwrap_or(0),
        c.get_array("nextBatch").map(|a| a.iter().filter_map(Bson::as_document).cloned().collect()).unwrap_or_default(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_forms() {
        assert_eq!(ok_value(&bson::doc! {"ok": 1.0}), Some(1.0));
        assert_eq!(ok_value(&bson::doc! {"ok": 1}), Some(1.0));
        assert_eq!(ok_value(&bson::doc! {"ok": true}), Some(1.0));
        assert_eq!(ok_value(&bson::doc! {"ok": 0}), Some(0.0));
        assert_eq!(ok_value(&bson::doc! {}), None);
    }
}