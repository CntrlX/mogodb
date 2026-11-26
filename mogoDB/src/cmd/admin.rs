//! Admin, DDL, statistics, diagnostics and replication-stub commands.

use crate::bsonutil::{self, as_bool, as_i64_opt};
use crate::cursor::cursor_reply;
use crate::error::{Error, Result};
use crate::query::matcher::Matcher;
use crate::server::{CmdCtx, Server, VERSION};
use crate::storage::{Coll, Store};
use bson::{Bson, Document, doc};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

pub fn route(ctx: &mut CmdCtx) -> Result<Document> {
    match ctx.name.as_str() {
        "create" => create(ctx),
        "drop" => drop_coll(ctx),
        "dropDatabase" => drop_database(ctx),
        "renameCollection" => rename_collection(ctx),
        "collMod" => coll_mod(ctx),
        "createIndexes" => create_indexes(ctx),
        "dropIndexes" | "deleteIndexes" => drop_indexes(ctx),
        "listIndexes" => list_indexes(ctx),
        "listCollections" => list_collections(ctx),
        "listDatabases" => list_databases(ctx),
        "cloneCollectionAsCapped" => clone_as_capped(ctx),
        "convertToCapped" => convert_to_capped(ctx),
        "validate" => validate(ctx),
        "compact" => compact(ctx),
        "autoCompact" => auto_compact(ctx),
        "dbStats" => db_stats(ctx),
        "collStats" => coll_stats(ctx),
        "dataSize" => data_size(ctx),
        "serverStatus" => Ok(ok(server_status(ctx.srv))),
        "hostInfo" => Ok(ok(host_info())),
        "getCmdLineOpts" => cmd_line_opts(ctx),
        "getLog" => get_log(ctx),
        "getParameter" => get_parameter(ctx),
        "setParameter" => set_parameter(ctx),
        "setFeatureCompatibilityVersion" => set_fcv(ctx),
        "listCommands" => Ok(ok(doc! {"commands": doc! {}})),
        "currentOp" => current_op(ctx),
        "killOp" => kill_op(ctx),
        "fsync" => fsync(ctx),
        "fsyncUnlock" => fsync_unlock(ctx),
        "shutdown" => shutdown(ctx),
        "logRotate" => Ok(ok(doc! {})),
        "profile" => profile(ctx),
        "top" => Ok(ok(doc! {"totals": doc! {}})),
        "getDefaultRWConcern" => Ok(ok(doc! {
            "defaultReadConcern": doc! {"level": "local"},
            "defaultWriteConcern": doc! {"w": 1i32},
            "defaultReadConcernSource": "implicit",
            "defaultWriteConcernSource": "implicit",
        })),
        "setDefaultRWConcern" => set_default_rw_concern(ctx),
        "lockInfo" => Ok(ok(doc! {"lockInfo": Vec::<Bson>::new()})),
        "connPoolStats" => Ok(ok(conn_pool_stats(ctx.srv))),
        "getLastError" | "getlasterror" => Ok(get_last_error(ctx)),
        "resetError" => {
            ctx.conn.last_error = None;
            Ok(ok(doc! {}))
        }
        "features" => Ok(ok(doc! {})),
        "getFreeMonitoringStatus" | "setFreeMonitoring" => Ok(ok(doc! {"state": "disabled"})),
        "getDiagnosticData" => Ok(ok(doc! {"data": doc! {"serverStatus": server_status(ctx.srv)}})),
        "replSetGetStatus" => repl_set_get_status(ctx),
        "replSetGetConfig" => repl_set_get_config(ctx),
        "replSetInitiate" => repl_set_initiate(ctx),
        "sleep" => sleep_cmd(ctx),
        "dbHash" => db_hash(ctx),
        "atlasVersion" | "isdbgrid" | "getShardMap" | "listShards" | "filemd5" => Err(Error::command_not_found(ctx.name.as_str())),
        n => Err(Error::command_not_found(n)),
    }
}

/// Operations in progress, rendered as `currentOp` `inprog` entries and filtered by `filter`.
/// A filter that fails to compile matches nothing.
pub fn current_op_docs(srv: &Server, filter: &Document) -> Vec<Document> {
    let matcher = match Matcher::new(filter) {
        Ok(m) => m,
        Err(_) => return Vec::new(),
    };
    let host = srv.host();
    let now = Instant::now();
    let ops = srv.ops.lock().expect("ops");
    ops.iter()
        .filter_map(|op| {
            let micros = now.saturating_duration_since(op.started).as_micros() as i64;
            let d = doc! {
                "type": "op",
                "host": host.clone(),
                "desc": format!("conn{}", op.conn_id),
                "connectionId": op.conn_id,
                "client": op.client.clone(),
                "active": true,
                "opid": op.opid,
                "secs_running": micros / 1_000_000,
                "microsecs_running": micros,
                "op": "command",
                "ns": op.ns.clone(),
                "command": op.command.clone(),
                "numYields": 0i32,
                "locks": doc! {},
                "waitingForLock": false,
            };
            match matcher.matches(&d) {
                Ok(true) => Some(d),
                _ => None,
            }
        })
        .collect()
}

// ------------------------------------------------------------ helpers

/// Command-envelope fields that are not command arguments.
const ENVELOPE: &[&str] = &[
    "$db",
    "lsid",
    "$clusterTime",
    "writeConcern",
    "txnNumber",
    "autocommit",
    "startTransaction",
    "$readPreference",
    "apiVersion",
    "apiStrict",
    "apiDeprecationErrors",
    "comment",
    "maxTimeMS",
    "readConcern",
    "$client",
    "$audit",
    "$configServerState",
    "$oplogQueryData",
    "allowDiskUse",
];

fn is_envelope(k: &str) -> bool {
    ENVELOPE.contains(&k)
}

fn ok(mut d: Document) -> Document {
    d.insert("ok", 1.0f64);
    d
}

fn str_field(cmd: &Document, key: &str) -> Result<String> {
    match cmd.get(key) {
        Some(Bson::String(s)) => Ok(s.clone()),
        Some(v) => Err(Error::type_mismatch(format!("BSON field '{key}' is the wrong type '{}', expected type 'string'", bsonutil::type_name(v)))),
        None => Err(Error::new(40414, format!("BSON field '{key}' is missing but a required field"))),
    }
}

fn opt_doc(cmd: &Document, key: &str) -> Result<Option<Document>> {
    match cmd.get(key) {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::Document(d)) => Ok(Some(d.clone())),
        Some(v) => Err(Error::type_mismatch(format!("BSON field '{key}' is the wrong type '{}', expected type 'object'", bsonutil::type_name(v)))),
    }
}

fn docs_array(cmd: &Document, key: &str) -> Result<Vec<Document>> {
    match cmd.get(key) {
        Some(Bson::Array(a)) => a
            .iter()
            .map(|v| match v {
                Bson::Document(d) => Ok(d.clone()),
                o => Err(Error::type_mismatch(format!("BSON field '{key}' element is the wrong type '{}', expected type 'object'", bsonutil::type_name(o)))),
            })
            .collect(),
        None => Err(Error::new(40414, format!("BSON field '{key}' is missing but a required field"))),
        Some(v) => Err(Error::type_mismatch(format!("BSON field '{key}' is the wrong type '{}', expected type 'array'", bsonutil::type_name(v)))),
    }
}

/// Collection name carried as the value of the command key itself.
fn coll_arg(ctx: &CmdCtx) -> Result<String> {
    str_field(&ctx.cmd, &ctx.name)
}

fn ns_of(ctx: &CmdCtx, coll: &str) -> String {
    format!("{}.{coll}", ctx.db)
}

fn existing(ctx: &CmdCtx, ns: &str) -> Result<Arc<Coll>> {
    ctx.srv.store.get_coll(ns).ok_or_else(|| Error::new(26, format!("ns does not exist: {ns}")))
}

fn view_error(ns: &str) -> Error {
    Error::new(166, format!("Namespace {ns} is a view, not a collection"))
}

fn admin_only(ctx: &CmdCtx) -> Result<()> {
    if ctx.db != "admin" {
        return Err(Error::new(13, format!("{} may only be run against the admin database.", ctx.name)));
    }
    Ok(())
}

/// Command arguments: the cmd document without the command key and envelope fields.
fn args(ctx: &CmdCtx) -> Document {
    ctx.cmd
        .iter()
        .filter(|(k, _)| k.as_str() != ctx.name.as_str() && !is_envelope(k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn scale_arg(cmd: &Document) -> Result<i64> {
    match as_i64_opt(cmd.get("scale")) {
        None => Ok(1),
        Some(s) if s >= 1 => Ok(s),
        Some(_) => Err(Error::new(2, "scale has to be >= 1")),
    }
}

/// All documents of a collection in natural order.
fn read_docs(store: &Store, c: &Coll) -> Result<Vec<Document>> {
    let snap = store.snapshot();
    store.scan(&snap, c, false)?.map(|r| r.map(|(_, d)| d)).collect()
}

fn id_of(d: &Document) -> Bson {
    d.get("_id").cloned().unwrap_or(Bson::Null)
}

fn id_index_spec() -> Document {
    doc! {"v": 2i32, "key": doc! {"_id": 1i32}, "name": "_id_"}
}

// ------------------------------------------------------------ DDL

fn create(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    let mut opts = args(ctx);
    check_validation_enums("create", &opts)?;
    crate::timeseries::normalize_create(&ns, &mut opts)?;
    if opts.contains_key("viewOn") && opts.get_array("pipeline").is_ok_and(|p| p.iter().any(|s| s.as_document().is_some_and(|d| d.contains_key("$rankFusion") || d.contains_key("$scoreFusion")))) {
        return Err(Error::new(167, "$rankFusion and $scoreFusion is currently unsupported in a view definition"));
    }
    if opts.get_str("validationLevel") == Ok("constraint") && opts.get_str("validationAction") == Ok("warn") {
        return Err(Error::new(2, "Validation action of 'warn' is not allowed when Validation level is 'constraint'"));
    }
    if as_bool(opts.get("capped")) && !opts.contains_key("size") {
        return Err(Error::new(72, "the 'size' field is required when 'capped' is true"));
    }
    if let Some(c) = ctx.srv.store.get_coll(&ns) {
        // MongoDB 7.0 treats re-creating with identical options as success.
        if c.options == opts {
            return Ok(ok(doc! {}));
        }
        let mut existing = Document::new();
        if !c.options.contains_key("timeseries") {
            existing.insert("uuid", Bson::Binary(c.uuid.clone()));
        }
        existing.extend(c.options.clone());
        return Err(Error::new(48, format!("namespace '{ns}' already exists, but with different options: {}, requested: {}", shell_doc(&existing), shell_doc(&opts))));
    }
    ctx.srv.store.create_coll(&ns, opts)?;
    Ok(ok(doc! {}))
}

fn drop_coll(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    // MongoDB 7.0: dropping a missing collection succeeds.
    let Some(c) = ctx.srv.store.get_coll(&ns) else { return Ok(ok(doc! {})) };
    let n = c.indexes().len() as i32;
    ctx.srv.store.drop_coll(&ns)?;
    Ok(ok(doc! {"ns": ns.clone(), "nIndexesWas": n}))
}

fn drop_database(ctx: &CmdCtx) -> Result<Document> {
    ctx.srv.store.drop_database(&ctx.db)?;
    Ok(ok(doc! {"dropped": ctx.db.clone()}))
}

fn rename_collection(ctx: &CmdCtx) -> Result<Document> {
    admin_only(ctx)?;
    let from = coll_arg(ctx)?;
    let to = str_field(&ctx.cmd, "to")?;
    let (fdb, _) = from.split_once('.').ok_or_else(|| Error::new(73, format!("Invalid namespace specified '{from}'")))?;
    let (tdb, _) = to.split_once('.').ok_or_else(|| Error::new(73, format!("Invalid namespace specified '{to}'")))?;
    if fdb != tdb {
        return Err(Error::new(20, "Can't rename a collection to a different database"));
    }
    ctx.srv.store.rename_coll(&from, &to, as_bool(ctx.cmd.get("dropTarget")))?;
    Ok(ok(doc! {}))
}

const COLL_MOD_OPTS: &[&str] = &["validator", "validationLevel", "validationAction", "viewOn", "pipeline", "changeStreamPreAndPostImages"];

fn coll_mod(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    let c = existing(ctx, &ns)?;
    let mut reply = Document::new();
    let mut opts = c.options.clone();
    let mut changed = false;
    let args = args(ctx);
    check_validation_enums("collMod", &args)?;
    let validation_err = |msg: &str| Error::new(2, format!("Failed to set validation options :: caused by :: {msg}"));
    let cur_level = c.options.get_str("validationLevel").ok().map(str::to_string);
    let prepared = bsonutil::as_bool(c.options.get("prepareConstraintValidationLevel"));
    let touches_validation = args.keys().any(|k| matches!(k.as_str(), "validator" | "validationLevel" | "validationAction"));
    if args.contains_key("validator") {
        if prepared {
            return Err(validation_err("Validator cannot be changed while prepareConstraintValidationLevel is set. To make validator changes, first run: db.runCommand({collMod: \"<collection>\", prepareConstraintValidationLevel: false})"));
        }
        if cur_level.as_deref() == Some("constraint") && args.get_str("validationLevel").unwrap_or("constraint") == "constraint" {
            return Err(validation_err("Validator cannot be changed while validationLevel is 'constraint'. To change the validator, first downgrade validationLevel to 'strict'"));
        }
    }
    for (k, v) in &args {
        match k.as_str() {
            "index" => {
                let mut idx = match v {
                    Bson::Document(d) => d.clone(),
                    o => return Err(Error::type_mismatch(format!("BSON field 'index' is the wrong type '{}', expected type 'object'", bsonutil::type_name(o)))),
                };
                let target = if let Some(n) = idx.remove("name") {
                    n
                } else if let Some(kp) = idx.remove("keyPattern") {
                    kp
                } else {
                    return Err(Error::new(9, "Need either index name or key pattern in index"));
                };
                for (rk, rv) in ctx.srv.store.modify_index(&ns, &target, &idx)? {
                    reply.insert(rk, rv);
                }
            }
            "timeseries" | "expireAfterSeconds" if c.options.contains_key("timeseries") => {
                let (tsa, exp) = if k == "timeseries" { (Some(v), None) } else { (None, Some(v)) };
                crate::timeseries::coll_mod(&mut opts, tsa, exp)?;
                changed = true;
            }
            "prepareConstraintValidationLevel" => {
                if bsonutil::as_bool(Some(v)) {
                    opts.insert("prepareConstraintValidationLevel", true);
                } else {
                    opts.remove("prepareConstraintValidationLevel");
                }
                changed = true;
            }
            key if COLL_MOD_OPTS.contains(&key) => {
                if key == "validator" && matches!(v, Bson::Document(d) if d.is_empty()) {
                    opts.remove(key);
                } else {
                    opts.insert(key, v.clone());
                }
                changed = true;
            }
            other => return Err(Error::new(40415, format!("BSON field 'collMod.{other}' is an unknown field"))),
        }
    }
    if touches_validation {
        // collMod materializes the defaults once validation options are touched.
        if !opts.contains_key("validationLevel") {
            opts.insert("validationLevel", "strict");
        }
        if !opts.contains_key("validationAction") {
            opts.insert("validationAction", "error");
        }
    }
    let new_level = opts.get_str("validationLevel").unwrap_or("strict").to_string();
    if new_level == "constraint" {
        if cur_level.as_deref() != Some("constraint") {
            // Upgrade protocol: explicit strict -> scan -> prepared.
            if cur_level.as_deref() != Some("strict") {
                return Err(validation_err("validationLevel can only be changed to 'constraint' from 'strict'"));
            }
            if let (Some(m), Some(_)) = (&c.validator, c.docs.as_ref()) {
                let snap = ctx.srv.store.snapshot();
                for r in ctx.srv.store.scan(&snap, &c, false)? {
                    let (_, d) = r?;
                    if !m.matches(&d)? {
                        let id = d.get("_id").map(shell_repr).unwrap_or_default();
                        let v = c.options.get_document("validator").cloned().unwrap_or_default();
                        return Err(Error::new(
                            12370902,
                            format!("Cannot upgrade validationLevel to 'constraint': the collection contains documents that do not satisfy the validator. First offending document _id: {id} Run db.{}.find({{\"$nor\": [{}]}}) to find non-compliant documents.", c.name, shell_doc(&v)),
                        ));
                    }
                }
            }
            if !prepared || args.contains_key("validator") {
                return Err(validation_err("Cannot upgrade validationLevel to 'constraint' without first setting prepareConstraintValidationLevel to true"));
            }
            opts.remove("prepareConstraintValidationLevel");
        }
        if opts.get_str("validationAction").unwrap_or("error") == "warn" {
            return Err(validation_err("Validation action of 'warn' is not allowed when Validation level is 'constraint'"));
        }
    }
    if changed {
        ctx.srv.store.set_coll_options(&ns, opts)?;
    }
    Ok(ok(reply))
}

/// mongo shell rendering of a value (`ObjectId('..')`, quoted strings).
pub(crate) fn shell_repr(v: &Bson) -> String {
    match v {
        Bson::ObjectId(o) => format!("ObjectId('{o}')"),
        Bson::String(s) => format!("\"{s}\""),
        Bson::Document(d) => shell_doc(d),
        Bson::Binary(b) if b.subtype == bson::spec::BinarySubtype::Uuid && b.bytes.len() == 16 => {
            let h: String = b.bytes.iter().map(|x| format!("{x:02x}")).collect();
            format!("UUID(\"{}-{}-{}-{}-{}\")", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
        }
        Bson::Array(a) => format!("[ {} ]", a.iter().map(shell_repr).collect::<Vec<_>>().join(", ")),
        Bson::Int64(i) => i.to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn shell_doc(d: &Document) -> String {
    if d.is_empty() {
        return "{}".into();
    }
    let parts: Vec<String> = d.iter().map(|(k, v)| format!("{k}: {}", shell_repr(v))).collect();
    format!("{{ {} }}", parts.join(", "))
}

/// Normalize one user index spec into the stored shape `{v, key, name, ...options}`.
fn index_spec(spec: &Document) -> Result<Document> {
    let key = match spec.get("key") {
        Some(Bson::Document(k)) if !k.is_empty() => k.clone(),
        _ => return Err(Error::new(9, "The 'key' field for an index specification must be a non-empty object")),
    };
    let name = match spec.get("name") {
        Some(Bson::String(s)) => s.clone(),
        Some(_) => return Err(Error::new(9, "The index name must be a string")),
        None => default_index_name(&key),
    };
    let mut out_key = Document::new();
    let mut text_weights = Document::new();
    let mut is_text = false;
    for (k, v) in &key {
        if matches!(v, Bson::String(s) if s == "text") {
            is_text = true;
            text_weights.insert(k.clone(), 1i32);
        } else {
            out_key.insert(k.clone(), v.clone());
        }
    }
    if is_text {
        out_key.insert("_fts", "text");
        out_key.insert("_ftsx", 1i32);
    }
    let v = spec.get("v").cloned().unwrap_or(Bson::Int32(2));
    let mut out = doc! {"v": v, "key": out_key, "name": name};
    for (k, val) in spec {
        if matches!(k.as_str(), "v" | "key" | "name" | "ns" | "background") {
            continue;
        }
        out.insert(k.clone(), val.clone());
    }
    if is_text {
        if !out.contains_key("weights") {
            out.insert("weights", text_weights);
        }
        if !out.contains_key("default_language") {
            out.insert("default_language", "english");
        }
        if !out.contains_key("language_override") {
            out.insert("language_override", "language");
        }
        if !out.contains_key("textIndexVersion") {
            out.insert("textIndexVersion", 3i32);
        }
    }
    Ok(out)
}

/// `<field>_<direction-or-type>` parts joined by `_`, e.g. `a_1_b_-1`, `title_text`.
fn default_index_name(key: &Document) -> String {
    key.iter()
        .map(|(k, v)| {
            let part = match v {
                Bson::String(s) => s.clone(),
                other => bsonutil::num_i64(other).map(|n| n.to_string()).unwrap_or_default(),
            };
            format!("{k}_{part}")
        })
        .collect::<Vec<_>>()
        .join("_")
}

fn create_indexes(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    let specs = docs_array(&ctx.cmd, "indexes")?;
    if specs.is_empty() {
        return Err(Error::new(9, "Must specify at least one index to create"));
    }
    let existed = ctx.srv.store.get_coll(&ns);
    // A nonexistent collection reports the implicit `_id` index as its single existing index.
    let before = existed.as_ref().map(|c| c.indexes().len()).unwrap_or(1);
    let auto = existed.is_none();
    let mut created = 0usize;
    for spec in &specs {
        let spec_doc = index_spec(spec)?;
        if let Some(ts) = ctx.srv.store.get_coll(&ns).and_then(|c| crate::timeseries::spec(&c.options)) {
            crate::timeseries::check_index(&ts, &spec_doc)?;
        }
        if ctx.srv.store.create_index(&ns, spec_doc)? {
            created += 1;
        }
    }
    let after = ctx.srv.store.get_coll(&ns).map(|c| c.indexes().len()).unwrap_or(0);
    let mut d = doc! {
        "createdCollectionAutomatically": auto,
        "numIndexesBefore": before as i32,
        "numIndexesAfter": after as i32,
    };
    if created == 0 {
        d.insert("note", "all indexes already exist");
    }
    Ok(ok(d))
}

fn drop_indexes(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    let c = ctx.srv.store.get_coll(&ns).ok_or_else(|| Error::new(26, format!("ns not found {ns}")))?;
    let n_before = c.indexes().len() as i32;
    let target = ctx.cmd.get("index").cloned().ok_or_else(|| Error::new(40414, "BSON field 'index' is missing but a required field"))?;
    match &target {
        Bson::String(s) if s == "*" => {
            for idx in c.indexes() {
                if idx.name != "_id_" {
                    ctx.srv.store.drop_index(&ns, &idx.name)?;
                }
            }
        }
        // MongoDB 8.0+: dropping an index that does not exist succeeds.
        Bson::String(s) => drop_if_exists(ctx, &ns, s)?,
        Bson::Document(k) => {
            if let Some(idx) = c.indexes().into_iter().find(|i| bsonutil::cmp_docs(&i.key, k) == std::cmp::Ordering::Equal) {
                ctx.srv.store.drop_index(&ns, &idx.name)?;
            }
        }
        Bson::Array(items) => {
            for v in items {
                match v {
                    Bson::String(s) => drop_if_exists(ctx, &ns, s)?,
                    o => {
                        return Err(Error::type_mismatch(format!("index name must be a string, got '{}'", bsonutil::type_name(o))));
                    }
                }
            }
        }
        o => {
            return Err(Error::type_mismatch(format!("BSON field 'index' is the wrong type '{}', expected types '[string, object]'", bsonutil::type_name(o))));
        }
    }
    let mut out = doc! {"nIndexesWas": n_before};
    if matches!(&target, Bson::String(s) if s == "*") {
        out.insert("msg", "non-_id indexes dropped for collection");
    }
    Ok(ok(out))
}

fn drop_if_exists(ctx: &CmdCtx, ns: &str, name: &str) -> Result<()> {
    match ctx.srv.store.drop_index(ns, name) {
        Err(e) if e.code == 27 => Ok(()),
        r => r,
    }
}

/// Validate `validationLevel` / `validationAction` enumerations (MongoDB 9.0 values).
fn check_validation_enums(cmd_name: &str, opts: &Document) -> Result<()> {
    for (field, allowed) in [("validationLevel", &["off", "strict", "moderate", "constraint"][..]), ("validationAction", &["error", "warn", "errorAndLog"][..])] {
        if let Some(v) = opts.get(field) {
            let ok = v.as_str().is_some_and(|s| allowed.contains(&s));
            if !ok {
                let shown = v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string());
                return Err(Error::new(2, format!("Enumeration value '{shown}' for field '{cmd_name}.{field}' is not a valid value.")));
            }
        }
    }
    Ok(())
}

fn list_indexes(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    let c = existing(ctx, &ns)?;
    let batch: Vec<Bson> = c.indexes().iter().map(|i| Bson::Document(i.spec.clone())).collect();
    Ok(cursor_reply(&ns, 0, batch, true))
}

fn list_collections(ctx: &CmdCtx) -> Result<Document> {
    let name_only = as_bool(ctx.cmd.get("nameOnly"));
    let matcher = match opt_doc(&ctx.cmd, "filter")? {
        Some(f) => Some(Matcher::new(&f)?),
        None => None,
    };
    let mut batch = Vec::new();
    let colls = ctx.srv.store.list_colls(Some(ctx.db.as_str()));
    // MongoDB keeps view definitions in a real `system.views` collection.
    if ctx.srv.store.has_system_views(&ctx.db) {
        let d = if name_only {
            doc! {"name": "system.views", "type": "collection"}
        } else {
            doc! {"name": "system.views", "type": "collection", "options": {}, "info": {"readOnly": false}, "idIndex": id_index_spec()}
        };
        if matcher.as_ref().map(|m| m.matches(&d)).transpose()?.unwrap_or(true) {
            batch.push(Bson::Document(d));
        }
    }
    for c in colls {
        let is_ts = c.options.contains_key("timeseries");
        let ty = if c.is_view() { "view" } else if is_ts { "timeseries" } else { "collection" };
        let d = if name_only {
            doc! {"name": c.name.clone(), "type": ty}
        } else {
            let mut d = doc! {"name": c.name.clone(), "type": ty, "options": c.options.clone()};
            if c.is_view() {
                d.insert("info", doc! {"readOnly": true});
            } else {
                d.insert("info", doc! {"readOnly": false, "uuid": c.uuid.clone()});
                if !is_ts {
                    d.insert("idIndex", id_index_spec());
                }
            }
            d
        };
        if let Some(m) = &matcher {
            if !m.matches(&d)? {
                continue;
            }
        }
        batch.push(Bson::Document(d));
    }
    Ok(cursor_reply(&format!("{}.$cmd.listCollections", ctx.db), 0, batch, true))
}

fn list_databases(ctx: &CmdCtx) -> Result<Document> {
    let name_only = as_bool(ctx.cmd.get("nameOnly"));
    let matcher = match opt_doc(&ctx.cmd, "filter")? {
        Some(f) => Some(Matcher::new(&f)?),
        None => None,
    };
    let mut names = ctx.srv.store.list_dbs();
    for virt in ["admin", "config", "local"] {
        if !names.iter().any(|n| n == virt) {
            names.push(virt.to_string());
        }
    }
    names.sort();
    let mut dbs = Vec::new();
    let mut total = 0i64;
    for name in names {
        let colls = ctx.srv.store.list_colls(Some(name.as_str()));
        let size: i64 = colls.iter().map(|c| c.size.load(Ordering::Relaxed)).sum();
        let count: i64 = colls.iter().map(|c| c.count.load(Ordering::Relaxed)).sum();
        let full = doc! {"name": name.clone(), "sizeOnDisk": size, "empty": count == 0};
        if let Some(m) = &matcher {
            if !m.matches(&full)? {
                continue;
            }
        }
        total += size;
        dbs.push(Bson::Document(if name_only { doc! {"name": name} } else { full }));
    }
    if name_only {
        return Ok(ok(doc! {"databases": dbs}));
    }
    Ok(ok(doc! {"databases": dbs, "totalSize": total, "totalSizeMb": total / 1_048_576}))
}

fn clone_as_capped(ctx: &mut CmdCtx) -> Result<Document> {
    let src = coll_arg(ctx)?;
    let src_ns = ns_of(ctx, &src);
    let target = str_field(&ctx.cmd, "toCollection")?;
    let size = as_i64_opt(ctx.cmd.get("size")).ok_or_else(|| Error::new(40414, "BSON field 'size' is missing but a required field"))?;
    let c = existing(ctx, &src_ns)?;
    let dst_ns = ns_of(ctx, &target);
    let docs = read_docs(&ctx.srv.store, &c)?;
    let dst = ctx.srv.store.create_coll(&dst_ns, doc! {"capped": true, "size": size})?;
    ctx.write(|store, txn| {
        for d in &docs {
            store.insert(txn, &dst, d.clone())?;
        }
        Ok(())
    })?;
    Ok(ok(doc! {}))
}

fn convert_to_capped(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    let size = as_i64_opt(ctx.cmd.get("size")).ok_or_else(|| Error::new(40414, "BSON field 'size' is missing but a required field"))?;
    let c = existing(ctx, &ns)?;
    if c.is_view() {
        return Err(view_error(&ns));
    }
    let mut opts = c.options.clone();
    opts.insert("capped", true);
    opts.insert("size", size);
    ctx.srv.store.set_coll_options(&ns, opts)?;
    Ok(ok(doc! {}))
}

fn validate(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    let c = existing(ctx, &ns)?;
    if c.is_view() {
        return Err(view_error(&ns));
    }
    let found = read_docs(&ctx.srv.store, &c)?.len() as i64;
    let stored = c.count.load(Ordering::Relaxed);
    let mut errors = Vec::<Bson>::new();
    if found != stored {
        errors.push(Bson::String(format!("collection record count mismatch: stored {stored}, found {found}")));
    }
    use fjall::Readable as _;
    let snap = ctx.srv.store.snapshot();
    let mut keys_per = Document::new();
    let mut details = Document::new();
    for idx in c.indexes() {
        let n = if idx.has_entries() { snap.iter(&idx.ks).count() as i32 } else { found as i32 };
        keys_per.insert(idx.name.clone(), n);
        details.insert(idx.name.clone(), doc! {"valid": true, "spec": idx.spec.clone(), "isMultikey": idx.multikey.load(Ordering::Relaxed)});
    }
    let mut d = doc! {
        "valid": errors.is_empty(),
        "repaired": false,
        "repairMode": "None",
        "ns": ns.clone(),
        "uuid": c.uuid.clone(),
        "fastCountType": "legacySizeStorer",
        "warnings": Vec::<Bson>::new(),
        "errors": errors,
        "extraIndexEntries": Vec::<Bson>::new(),
        "missingIndexEntries": Vec::<Bson>::new(),
        "nInvalidDocuments": 0i32,
        "nNonCompliantDocuments": 0i32,
        "nrecords": small(found),
        "corruptRecords": Vec::<Bson>::new(),
        "nIndexes": c.indexes().len() as i32,
        "keysPerIndex": keys_per,
        "indexDetails": details,
    };
    d.insert("ok", 1.0);
    Ok(d)
}

fn small(n: i64) -> Bson {
    i32::try_from(n).map(Bson::Int32).unwrap_or(Bson::Int64(n))
}

fn compact(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    let c = existing(ctx, &ns)?;
    if c.is_view() {
        return Err(view_error(&ns));
    }
    if ctx.srv.cfg.repl_set.is_some() && !as_bool(ctx.cmd.get("force")) {
        return Err(Error::new(20, "will not run compact on an active replica set primary as this will slow down other running operations. use force:true to force"));
    }
    if as_bool(ctx.cmd.get("dryRun")) {
        return Ok(ok(doc! {"estimatedBytesFreed": 0}));
    }
    Ok(ok(doc! {"bytesFreed": 0}))
}

fn auto_compact(ctx: &CmdCtx) -> Result<Document> {
    admin_only(ctx)?;
    match ctx.cmd.get("autoCompact") {
        Some(Bson::Boolean(on)) => {
            ctx.srv.parameters.lock().expect("parameters").insert("enableAutoCompaction", *on);
            Ok(ok(doc! {}))
        }
        Some(v) => Err(Error::type_mismatch(format!("BSON field 'autoCompact.autoCompact' is the wrong type '{}', expected type 'bool'", bsonutil::type_name(v)))),
        None => Err(Error::new(40414, "BSON field 'autoCompact.autoCompact' is missing but a required field")),
    }
}

// ------------------------------------------------------------ stats

fn db_stats(ctx: &CmdCtx) -> Result<Document> {
    let scale = scale_arg(&ctx.cmd)?;
    let (mut n, mut views, mut objects, mut data, mut indexes) = (0i64, 0i64, 0i64, 0i64, 0i64);
    for c in ctx.srv.store.list_colls(Some(ctx.db.as_str())) {
        if c.is_view() {
            views += 1;
            continue;
        }
        n += 1;
        objects += c.count.load(Ordering::Relaxed);
        data += c.size.load(Ordering::Relaxed);
        indexes += c.indexes().len() as i64;
    }
    let avg = if objects > 0 { data / objects } else { 0 };
    Ok(ok(doc! {
        "db": ctx.db.clone(),
        "collections": n,
        "views": views,
        "objects": objects,
        "avgObjSize": avg as f64,
        "dataSize": data / scale,
        "storageSize": data / scale,
        "indexes": indexes,
        "indexSize": 0i64,
        "totalSize": data / scale,
        "scaleFactor": scale,
    }))
}

/// MongoDB's `appendNumber`: int32 when it fits, else int64.
fn small_num(n: i64) -> Bson {
    i32::try_from(n).map(Bson::Int32).unwrap_or(Bson::Int64(n))
}

fn coll_stats(ctx: &CmdCtx) -> Result<Document> {
    let coll = coll_arg(ctx)?;
    let ns = ns_of(ctx, &coll);
    let scale = scale_arg(&ctx.cmd)?;
    let c = ctx
        .srv
        .store
        .get_coll(&ns)
        .ok_or_else(|| Error::new(26, format!("Collection [{ns}] not found.")))?;
    if c.is_view() {
        return Err(view_error(&ns));
    }
    let count = c.count.load(Ordering::Relaxed);
    let size = c.size.load(Ordering::Relaxed);
    let avg = if count > 0 { size / count } else { 0 };
    let idx = c.indexes();
    let mut sizes = Document::new();
    for i in &idx {
        sizes.insert(i.name.clone(), 0i32);
    }
    let mut d = doc! {
        "ns": ns.clone(),
        "count": small_num(count),
        "size": small_num(size / scale),
        "avgObjSize": small_num(avg),
        "storageSize": small_num(size / scale),
        "nindexes": idx.len() as i32,
        "totalIndexSize": 0i32,
        "indexSizes": sizes,
        "scaleFactor": scale,
    };
    if let Some((max_size, max)) = c.capped() {
        d.insert("capped", true);
        d.insert("max", max.unwrap_or(-1));
        d.insert("maxSize", max_size);
    }
    if c.options.contains_key("timeseries") {
        d.remove("count");
        d.insert("timeseries", crate::timeseries::coll_stats(&ns, count));
    }
    Ok(ok(d))
}

fn data_size(ctx: &CmdCtx) -> Result<Document> {
    let ns = coll_arg(ctx)?;
    let (size, count) = match ctx.srv.store.get_coll(&ns) {
        Some(c) => (c.size.load(Ordering::Relaxed), c.count.load(Ordering::Relaxed)),
        None => (0, 0),
    };
    Ok(ok(doc! {"estimate": false, "size": size, "numObjects": count, "millis": 0i32}))
}

fn server_status(srv: &Server) -> Document {
    let c = &srv.counters;
    let get = |a: &std::sync::atomic::AtomicI64| a.load(Ordering::Relaxed);
    let cur = get(&c.conns_current);
    let uptime = srv.started.elapsed();
    doc! {
        "host": srv.host(),
        "version": VERSION,
        "process": "mogodb",
        "pid": std::process::id() as i64,
        "uptime": uptime.as_secs_f64(),
        "uptimeMillis": uptime.as_millis() as i64,
        "uptimeEstimate": uptime.as_secs() as i64,
        "localTime": bson::DateTime::now(),
        "connections": {
            "current": cur,
            "available": (65536 - cur).max(0),
            "totalCreated": get(&c.conns_total),
        },
        "opcounters": {
            "insert": get(&c.insert),
            "query": get(&c.query),
            "update": get(&c.update),
            "delete": get(&c.delete),
            "getmore": get(&c.getmore),
            "command": get(&c.command),
        },
        "network": {
            "bytesIn": get(&c.bytes_in),
            "bytesOut": get(&c.bytes_out),
            "numRequests": get(&c.requests),
        },
    }
}

fn host_info() -> Document {
    let read = |p: &str| std::fs::read_to_string(p).ok().map(|s| s.trim().to_string());
    let hostname = read("/proc/sys/kernel/hostname").unwrap_or_else(|| "localhost".to_string());
    let release = read("/proc/sys/kernel/osrelease").unwrap_or_default();
    let mem_mb = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<i64>().ok()))
        .map(|kb| kb / 1024)
        .unwrap_or(0);
    let cores = std::thread::available_parallelism().map(|n| n.get() as i64).unwrap_or(1);
    doc! {
        "system": {
            "currentTime": bson::DateTime::now(),
            "hostname": hostname,
            "cpuAddrSize": 64i32,
            "memSizeMB": mem_mb,
            "numCores": cores,
            "cpuArch": std::env::consts::ARCH,
            "numaEnabled": false,
        },
        "os": {"type": "Linux", "version": release},
        "extra": {},
    }
}

fn cmd_line_opts(ctx: &CmdCtx) -> Result<Document> {
    let cfg = &ctx.srv.cfg;
    let mut parsed = doc! {
        "net": {"port": cfg.port as i32, "bindIp": cfg.bind_ip.join(",")},
        "storage": {"dbPath": cfg.dbpath.display().to_string()},
    };
    if let Some(rs) = &cfg.repl_set {
        parsed.insert("replication", doc! {"replSet": rs.clone()});
    }
    if cfg.auth {
        parsed.insert("security", doc! {"authorization": "enabled"});
    }
    Ok(ok(doc! {"argv": cfg.argv.clone(), "parsed": parsed}))
}

fn get_log(ctx: &CmdCtx) -> Result<Document> {
    match str_field(&ctx.cmd, &ctx.name)?.as_str() {
        "*" => Ok(ok(doc! {"names": vec!["global", "startupWarnings"]})),
        "global" => {
            let lines: Vec<Bson> = ctx.srv.log.lock().expect("log").iter().map(|l| Bson::String(l.clone())).collect();
            Ok(ok(doc! {"totalLinesWritten": lines.len() as i64, "log": lines}))
        }
        "startupWarnings" => Ok(ok(doc! {"totalLinesWritten": 0i64, "log": Vec::<Bson>::new()})),
        other => Err(Error::new(2, format!("No log named {other}"))),
    }
}

fn get_parameter(ctx: &CmdCtx) -> Result<Document> {
    let params = ctx.srv.parameters.lock().expect("parameters").clone();
    let all = matches!(ctx.cmd.get(&ctx.name), Some(Bson::String(s)) if s == "*") || as_bool(ctx.cmd.get("allParameters"));
    let mut out = Document::new();
    if all {
        for (k, v) in &params {
            out.insert(k.clone(), v.clone());
        }
    } else {
        for (k, v) in args(ctx) {
            if bsonutil::truthy(&v) {
                if let Some(pv) = params.get(&k) {
                    out.insert(k, pv.clone());
                }
            }
        }
        if out.is_empty() {
            return Err(Error::new(72, "no option found to get"));
        }
    }
    Ok(ok(out))
}

fn set_parameter(ctx: &CmdCtx) -> Result<Document> {
    admin_only(ctx)?;
    let updates = args(ctx);
    let mut params = ctx.srv.parameters.lock().expect("parameters");
    for k in updates.keys() {
        if !params.contains_key(k) {
            return Err(Error::new(72, format!("no option found to set, unknown option {k}")));
        }
    }
    let mut was = Bson::Null;
    for (k, v) in updates {
        if k == "maxConcurrentMultiDocumentTransactions" {
            let n = bsonutil::num_i64(&v).filter(|n| *n >= 0).ok_or_else(|| Error::bad_value("maxConcurrentMultiDocumentTransactions must be a non-negative number"))?;
            crate::session::MAX_OPEN_TXNS.store(n, std::sync::atomic::Ordering::SeqCst);
        }
        was = params.get(&k).cloned().unwrap_or(Bson::Null);
        params.insert(k, v);
    }
    Ok(ok(doc! {"was": was}))
}

fn set_fcv(ctx: &CmdCtx) -> Result<Document> {
    admin_only(ctx)?;
    let v = str_field(&ctx.cmd, &ctx.name)?;
    if v != "8.0" && v != "9.0" {
        return Err(Error::new(2, format!("Invalid featureCompatibilityVersion value {v}; expected 8.0 or 9.0")));
    }
    ctx.srv.parameters.lock().expect("parameters").insert("featureCompatibilityVersion", doc! {"version": v});
    Ok(ok(doc! {}))
}

fn current_op(ctx: &CmdCtx) -> Result<Document> {
    let mut filter = args(ctx);
    filter.remove("$all");
    Matcher::new(&filter)?;
    let inprog: Vec<Bson> = current_op_docs(ctx.srv, &filter).into_iter().map(Bson::Document).collect();
    Ok(ok(doc! {"inprog": inprog}))
}

fn kill_op(ctx: &CmdCtx) -> Result<Document> {
    let opid = match ctx.cmd.get("op") {
        Some(Bson::String(s)) => s
            .rsplit(':')
            .next()
            .and_then(|t| t.parse::<i64>().ok())
            .ok_or_else(|| Error::new(2, format!("Invalid opid '{s}'")))?,
        Some(v) => bsonutil::num_i64(v)
            .ok_or_else(|| Error::type_mismatch(format!("BSON field 'op' is the wrong type '{}', expected type 'long'", bsonutil::type_name(v))))?,
        None => return Err(Error::new(40414, "BSON field 'op' is missing but a required field")),
    };
    let ops = ctx.srv.ops.lock().expect("ops");
    let info = match ops.iter().find(|o| o.opid == opid) {
        Some(op) => {
            op.killed.store(true, Ordering::Relaxed);
            "attempting to kill op"
        }
        None => "op not found",
    };
    Ok(ok(doc! {"info": info}))
}

fn fsync(ctx: &CmdCtx) -> Result<Document> {
    admin_only(ctx)?;
    ctx.srv.store.flush()?;
    if as_bool(ctx.cmd.get("lock")) {
        let n = ctx.srv.fsync_locked.fetch_add(1, Ordering::SeqCst) + 1;
        return Ok(ok(doc! {
            "info": "now locked against writes, use fsyncUnlock to unlock",
            "lockCount": n as i64,
        }));
    }
    Ok(ok(doc! {}))
}

fn fsync_unlock(ctx: &CmdCtx) -> Result<Document> {
    admin_only(ctx)?;
    match ctx.srv.fsync_locked.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)) {
        Ok(prev) => Ok(ok(doc! {"info": "fsyncUnlock completed", "lockCount": (prev - 1) as i64})),
        Err(_) => Err(Error::new(125, "fsyncUnlock called when fsync is not locked.")),
    }
}

fn shutdown(ctx: &CmdCtx) -> Result<Document> {
    admin_only(ctx)?;
    ctx.srv.store.flush()?;
    ctx.srv.shutdown.notify_waiters();
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(100));
        std::process::exit(0);
    });
    Ok(ok(doc! {}))
}

fn profile(ctx: &CmdCtx) -> Result<Document> {
    if let Some(level) = as_i64_opt(ctx.cmd.get(&ctx.name)) {
        if !(0..=2).contains(&level) {
            return Err(Error::new(2, "Invalid profiling level"));
        }
    }
    Ok(ok(doc! {"was": 0i32, "slowms": 100i32, "sampleRate": 1.0f64}))
}

fn set_default_rw_concern(ctx: &CmdCtx) -> Result<Document> {
    admin_only(ctx)?;
    let rc = opt_doc(&ctx.cmd, "defaultReadConcern")?;
    let wc = opt_doc(&ctx.cmd, "defaultWriteConcern")?;
    if rc.is_none() && wc.is_none() {
        return Err(Error::new(9, "At least one of either defaultReadConcern or defaultWriteConcern must be provided"));
    }
    let rc = rc.unwrap_or_else(|| doc! {"level": "local"});
    let wc = wc.unwrap_or_else(|| doc! {"w": 1i32});
    Ok(ok(doc! {"defaultReadConcern": rc, "defaultWriteConcern": wc}))
}

fn conn_pool_stats(srv: &Server) -> Document {
    let cur = srv.counters.conns_current.load(Ordering::Relaxed);
    doc! {
        "numClientConnections": cur,
        "numAScopedConnections": 0i32,
        "totalInUse": cur,
        "totalAvailable": 0i32,
        "totalCreated": srv.counters.conns_total.load(Ordering::Relaxed),
        "hosts": {},
        "replicaSets": {},
    }
}

fn get_last_error(ctx: &CmdCtx) -> Document {
    let mut d = ctx.conn.last_error.clone().unwrap_or_else(|| doc! {"n": 0i32, "err": Bson::Null});
    d.insert("connectionId", ctx.conn.id);
    ok(d)
}

// ------------------------------------------------------------ replication stubs

fn repl_name(ctx: &CmdCtx) -> Result<String> {
    ctx.srv.cfg.repl_set.clone().ok_or_else(|| Error::new(76, "not running with --replSet"))
}

fn repl_set_get_status(ctx: &CmdCtx) -> Result<Document> {
    let set = repl_name(ctx)?;
    let ts = ctx.srv.store.cluster_time();
    let uptime = ctx.srv.started.elapsed().as_secs() as i64;
    let member = doc! {
        "_id": 0i32,
        "name": ctx.srv.host(),
        "health": 1.0f64,
        "state": 1i32,
        "stateStr": "PRIMARY",
        "uptime": uptime,
        "optime": doc! {"ts": ts, "t": 1i64},
        "optimeDate": bson::DateTime::now(),
        "syncSourceHost": "",
        "syncSourceId": -1i32,
        "infoMessage": "",
        "configVersion": 1i32,
        "self": true,
        "lastHeartbeatMessage": "",
    };
    Ok(ok(doc! {
        "set": set,
        "date": bson::DateTime::now(),
        "myState": 1i32,
        "term": 1i64,
        "syncSourceHost": "",
        "syncSourceId": -1i32,
        "heartbeatIntervalMillis": 2000i32,
        "majorityVoteCount": 1i32,
        "writeMajorityCount": 1i32,
        "votingMembersCount": 1i32,
        "writableVotingMembersCount": 1i32,
        "optimes": {
            "lastCommittedOpTime": doc! {"ts": ts, "t": 1i64},
            "appliedOpTime": doc! {"ts": ts, "t": 1i64},
            "durableOpTime": doc! {"ts": ts, "t": 1i64},
        },
        "members": vec![Bson::Document(member)],
    }))
}

fn repl_set_get_config(ctx: &CmdCtx) -> Result<Document> {
    let set = repl_name(ctx)?;
    let member = doc! {
        "_id": 0i32,
        "host": ctx.srv.host(),
        "arbiterOnly": false,
        "buildIndexes": true,
        "hidden": false,
        "priority": 1.0f64,
        "tags": doc! {},
        "votes": 1i32,
    };
    Ok(ok(doc! {
        "config": {
            "_id": set,
            "version": 1i32,
            "term": 1i64,
            "protocolVersion": 1i64,
            "writeConcernMajorityJournalDefault": true,
            "members": vec![Bson::Document(member)],
            "settings": {
                "chainingAllowed": true,
                "heartbeatIntervalMillis": 2000i32,
                "electionTimeoutMillis": 10000i32,
            },
        },
    }))
}

fn repl_set_initiate(ctx: &CmdCtx) -> Result<Document> {
    if ctx.srv.cfg.repl_set.is_some() {
        Err(Error::new(23, "already initialized"))
    } else {
        Err(Error::new(76, "not running with --replSet"))
    }
}

// ------------------------------------------------------------ misc

/// `sleep`: milliseconds from `ms`, or seconds from `sec`/`secs`, on the value document or the command itself.
/// Capped at one hour; checks for `killOp` every 10 ms.
fn sleep_cmd(ctx: &CmdCtx) -> Result<Document> {
    let src = match ctx.cmd.get(&ctx.name) {
        Some(Bson::Document(d)) => d.clone(),
        _ => ctx.cmd.clone(),
    };
    let ms = match (as_i64_opt(src.get("ms")), as_i64_opt(src.get("sec")).or(as_i64_opt(src.get("secs")))) {
        (Some(ms), _) => ms,
        (None, Some(s)) => s.saturating_mul(1000),
        (None, None) => 0,
    };
    if ms < 0 {
        return Err(Error::new(2, "sleep duration must be non-negative"));
    }
    let end = Instant::now() + Duration::from_millis(ms.min(3_600_000) as u64);
    loop {
        ctx.check_killed()?;
        let now = Instant::now();
        if now >= end {
            break;
        }
        std::thread::sleep((end - now).min(Duration::from_millis(10)));
    }
    Ok(ok(doc! {}))
}

fn id_sorted(mut docs: Vec<Document>) -> Vec<Document> {
    docs.sort_by(|a, b| bsonutil::cmp_bson(&id_of(a), &id_of(b)));
    docs
}

/// `dbHash`: MD5 over each collection's documents in `_id` order (BSON bytes), then over the
/// collection names and their hashes. Not byte-compatible with mongod's own dbHash.
fn db_hash(ctx: &CmdCtx) -> Result<Document> {
    let mut per = Document::new();
    let mut outer = Vec::new();
    for c in ctx.srv.store.list_colls(Some(ctx.db.as_str())) {
        if c.is_view() {
            continue;
        }
        let docs = id_sorted(read_docs(&ctx.srv.store, &c)?);
        let mut buf = Vec::new();
        for d in &docs {
            buf.extend(bsonutil::to_bytes(d));
        }
        let h = md5_hex(&buf);
        outer.extend_from_slice(c.name.as_bytes());
        outer.extend_from_slice(h.as_bytes());
        per.insert(c.name.clone(), h);
    }
    Ok(ok(doc! {"collections": per, "md5": md5_hex(&outer), "timeMillis": 0i32}))
}

/// RFC 1321 MD5, lowercase hex.
fn md5_hex(data: &[u8]) -> String {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, //
        5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, //
        4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, //
        6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    let k: [u32; 64] = core::array::from_fn(|i| ((i as f64 + 1.0).sin().abs() * 4_294_967_296.0) as u32);
    let (mut a0, mut b0, mut c0, mut d0): (u32, u32, u32, u32) = (0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476);
    let mut msg = data.to_vec();
    let bit_len = (data.len() as u64).wrapping_mul(8);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_le_bytes());
    for chunk in msg.chunks(64) {
        let m: [u32; 16] = core::array::from_fn(|i| u32::from_le_bytes([chunk[4 * i], chunk[4 * i + 1], chunk[4 * i + 2], chunk[4 * i + 3]]));
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i / 16 {
                0 => ((b & c) | (!b & d), i),
                1 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                2 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let f = f.wrapping_add(a).wrapping_add(k[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(f.rotate_left(S[i]));
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }
    let mut out = String::with_capacity(32);
    for w in [a0, b0, c0, d0] {
        for byte in w.to_le_bytes() {
            out.push_str(&format!("{byte:02x}"));
        }
    }
    out
}
