//! `saslStart` / `saslContinue` / speculative authentication / `logout`.

use crate::auth::ScramConversation;
use crate::bsonutil;
use crate::error::{Error, Result};
use crate::server::{AuthUser, CmdCtx, Server};
use bson::{Bson, Document, doc, spec::BinarySubtype};
use std::sync::Arc;

pub const USERS_NS: &str = "admin.system.users";

/// The `admin.system.users` document for `db.user`.
pub fn find_user(srv: &Server, db: &str, user: &str) -> Option<Document> {
    let c = srv.store.get_coll(USERS_NS)?;
    let snap = srv.store.snapshot();
    let id = format!("{db}.{user}");
    let filter = doc! {"_id": &id};
    let plan = srv.store.plan(&c, &filter, None).ok()?;
    let found = srv.store.execute(&snap, &c, &plan).ok()?.filter_map(|r| r.ok()).map(|x| x.1).find(|d| d.get_str("_id") == Ok(id.as_str()));
    found
}

pub fn supported_mechs(srv: &Server, db: &str, user: &str) -> Option<Vec<Bson>> {
    let u = find_user(srv, db, user)?;
    let creds = u.get_document("credentials").ok()?;
    Some(creds.keys().filter(|k| k.starts_with("SCRAM")).map(|k| Bson::String(k.clone())).collect())
}

fn payload_bytes(v: Option<&Bson>) -> Result<Vec<u8>> {
    match v {
        Some(Bson::Binary(b)) => Ok(b.bytes.clone()),
        Some(Bson::String(s)) => Ok(s.as_bytes().to_vec()),
        None => Ok(Vec::new()),
        Some(o) => Err(Error::type_mismatch(format!("payload must be BinData, not {}", bsonutil::type_name(o)))),
    }
}

fn bin(b: Vec<u8>) -> Bson {
    bsonutil::binary(BinarySubtype::Generic, b)
}

fn sasl_start(ctx: &mut CmdCtx, db: &str, spec: &Document) -> Result<Document> {
    let mech = spec.get_str("mechanism").map_err(|_| Error::new(40414, "BSON field 'saslStart.mechanism' is missing but a required field"))?.to_string();
    if !mech.starts_with("SCRAM-SHA-") {
        return Err(Error::new(2, format!("Received authentication for mechanism {mech} which is not enabled")));
    }
    let payload = payload_bytes(spec.get("payload"))?;
    let srv: Arc<Server> = ctx.srv.clone();
    let dbname = db.to_string();
    let lookup = move |user: &str| -> Option<Document> { find_user(&srv, &dbname, user).and_then(|u| u.get_document("credentials").ok().cloned()) };
    let (conv, out) = ScramConversation::start(&mech, &payload, &lookup)?;
    let skip_empty = spec.get_document("options").ok().map(|o| bsonutil::as_bool(o.get("skipEmptyExchange"))).unwrap_or(false);
    let id = ctx.conn.next_conv;
    ctx.conn.next_conv += 1;
    ctx.conn.sasl.insert(id, conv);
    if skip_empty {
        ctx.conn.skip_empty.insert(id);
    }
    Ok(doc! {"conversationId": id, "done": false, "payload": bin(out), "ok": 1.0})
}

fn sasl_continue(ctx: &mut CmdCtx) -> Result<Document> {
    let id = ctx.cmd.get("conversationId").and_then(bsonutil::num_i64).unwrap_or(1) as i32;
    let payload = payload_bytes(ctx.cmd.get("payload"))?;
    let db = ctx.db.clone();
    let Some(conv) = ctx.conn.sasl.get_mut(&id) else {
        return Err(Error::new(17, "No SASL session state found"));
    };
    let r = conv.step(&payload);
    let (out, mut done) = match r {
        Ok(x) => x,
        Err(e) => {
            ctx.conn.sasl.remove(&id);
            ctx.srv.log(format!("Authentication failed on conn{}: {}", ctx.conn.id, e.msg));
            return Err(Error::new(18, "Authentication failed."));
        }
    };
    let verified = out.starts_with(b"v=") || done;
    if verified && ctx.conn.skip_empty.contains(&id) {
        done = true;
    }
    if done {
        let conv = ctx.conn.sasl.remove(&id).expect("present");
        ctx.conn.skip_empty.remove(&id);
        let user = conv.username().to_string();
        let roles = crate::cmd::users::roles_for(ctx.srv, &db, &user);
        ctx.conn.users.retain(|u| !(u.user == user && u.db == db));
        ctx.conn.users.push(AuthUser { user, db, roles });
    }
    Ok(doc! {"conversationId": id, "done": done, "payload": bin(out), "ok": 1.0})
}

/// `hello.speculativeAuthenticate`: run saslStart inline.
pub fn speculative(ctx: &mut CmdCtx, spec: Document) -> Result<Document> {
    let db = spec.get_str("db").unwrap_or("admin").to_string();
    let mut r = sasl_start(ctx, &db, &spec)?;
    r.remove("ok");
    Ok(r)
}

pub fn route(ctx: &mut CmdCtx) -> Result<Document> {
    match ctx.name.as_str() {
        "saslStart" => {
            let db = ctx.db.clone();
            let spec = ctx.cmd.clone();
            sasl_start(ctx, &db, &spec)
        }
        "saslContinue" => sasl_continue(ctx),
        "logout" => {
            let db = ctx.db.clone();
            ctx.conn.users.retain(|u| u.db != db);
            Ok(doc! {"ok": 1.0})
        }
        "getnonce" => Err(Error::new(59, "no such command: 'getnonce'")),
        _ => Err(Error::new(2, "Unsupported authentication mechanism; use SCRAM-SHA-256 or SCRAM-SHA-1")),
    }
}
