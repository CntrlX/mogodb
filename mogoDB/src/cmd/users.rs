//! User and role management, `connectionStatus`, and the authorization check.
use crate::auth::make_credentials;
use crate::bsonutil::truthy;
use crate::cmd::authcmd::{find_user, USERS_NS};
use crate::error::{Error, Result};
use crate::query::matcher::Matcher;
use crate::server::{CmdCtx, Conn, Server};
use crate::storage::{new_uuid, Store, Txn};
use bson::{doc, Bson, Document};
use std::collections::HashMap;
use std::sync::Arc;

const ROLES_NS: &str = "admin.system.roles";
const EXTERNAL_DB: &str = "$external";
const SCRAM_MECHS: [&str; 2] = ["SCRAM-SHA-256", "SCRAM-SHA-1"];
const BUILTIN_ROLES: &[&str] = &[
    "read",
    "readWrite",
    "dbAdmin",
    "dbOwner",
    "userAdmin",
    "readAnyDatabase",
    "readWriteAnyDatabase",
    "userAdminAnyDatabase",
    "dbAdminAnyDatabase",
    "clusterMonitor",
    "clusterManager",
    "hostManager",
    "clusterAdmin",
    "backup",
    "restore",
    "enableSharding",
    "root",
    "__system",
];

type RoleRef = (String, String);
type RoleMap = HashMap<String, Document>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cap {
    Read,
    Write,
    DbAdmin,
    UserAdmin,
    Monitor,
    Cluster,
}

#[derive(Clone, PartialEq, Eq)]
enum Scope {
    Db(String),
    Any,
    Cluster,
}

#[derive(Clone)]
struct Grant {
    cap: Cap,
    scope: Scope,
}

type Need = (Cap, Scope);

enum Sel {
    AllInDb,
    AllDbs,
    Refs(Vec<RoleRef>),
}

/// Run `$name` over the rows of `ns` matching `filter`, reading through `src`
/// (a snapshot or a write transaction). Evaluates to `Result<Vec<(u64, Document)>>`.
macro_rules! rows {
    ($store:expr, $src:expr, $ns:expr, $filter:expr) => {{
        let ns: &str = $ns;
        let filter: &Document = $filter;
        match $store.get_coll(ns) {
            None => Ok(Vec::new()),
            Some(c) => {
                let plan = $store.plan(&c, filter, None)?;
                let it = $store.execute($src, &c, &plan)?;
                filter_rows(it, filter)
            }
        }
    }};
}

/// User/role management commands and `connectionStatus`.
pub fn route(ctx: &mut CmdCtx) -> Result<Document> {
    let name = ctx.name.clone();
    match name.as_str() {
        "createUser" => create_user(ctx),
        "updateUser" => update_user(ctx),
        "dropUser" => drop_user(ctx),
        "dropAllUsersFromDatabase" => drop_all_users(ctx),
        "usersInfo" => users_info(ctx),
        "grantRolesToUser" => change_refs(ctx, USERS_NS, true),
        "revokeRolesFromUser" => change_refs(ctx, USERS_NS, false),
        "createRole" => create_role(ctx),
        "updateRole" => update_role(ctx),
        "dropRole" => drop_role(ctx),
        "dropAllRolesFromDatabase" => drop_all_roles(ctx),
        "rolesInfo" => roles_info(ctx),
        "grantPrivilegesToRole" => change_role_privs(ctx, true),
        "revokePrivilegesFromRole" => change_role_privs(ctx, false),
        "grantRolesToRole" => change_refs(ctx, ROLES_NS, true),
        "revokeRolesFromRole" => change_refs(ctx, ROLES_NS, false),
        "connectionStatus" => connection_status(ctx),
        other => Err(Error::command_not_found(other)),
    }
}

/// True when at least one user exists (disables the localhost exception).
pub fn any_users(srv: &Arc<Server>) -> bool {
    query(&srv.store, USERS_NS, &Document::new())
        .map(|r| !r.is_empty())
        .unwrap_or(true)
}

/// Effective (role, db) pairs for `db.user`, including roles inherited through custom roles.
pub fn roles_for(srv: &Arc<Server>, db: &str, user: &str) -> Vec<(String, String)> {
    match load_roles(&srv.store) {
        Ok(roles) => user_roles(srv, &roles, db, user),
        Err(_) => Vec::new(),
    }
}

/// Check the authenticated users on `conn` may run command `name` (body `cmd`) on `db`.
/// Error 13 Unauthorized: "not authorized on <db> to execute command { <name>: ... }".
pub fn authorize(srv: &Arc<Server>, conn: &Conn, db: &str, name: &str, cmd: &Document) -> Result<()> {
    let Some(need) = requirement(name, db, cmd) else {
        return Ok(());
    };
    if name == "usersInfo" && !flag(cmd, "showCredentials") && self_lookup(conn, db, cmd) {
        return Ok(());
    }
    let roles = load_roles(&srv.store)?;
    let mut grants = Vec::new();
    for u in &conn.users {
        let refs = user_roles(srv, &roles, &u.db, &u.user);
        grants.extend(grants_of(&roles, &refs));
    }
    if need.iter().any(|n| grants.iter().any(|g| covers(g, n))) {
        Ok(())
    } else {
        Err(denied(db, name, cmd))
    }
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

/// Capabilities a command needs, as an OR-list. `None` means any connection may run it.
fn requirement(name: &str, db: &str, cmd: &Document) -> Option<Vec<Need>> {
    if name == "explain" {
        let clu = vec![(Cap::Cluster, Scope::Cluster)];
        let Ok(inner) = cmd.get_document("explain") else {
            return Some(clu);
        };
        let Some(inner_name) = inner.keys().next() else {
            return Some(vec![(Cap::Cluster, Scope::Cluster)]);
        };
        return requirement(inner_name, db, inner);
    }
    let d = Scope::Db(db.to_string());
    let rd = (Cap::Read, d.clone());
    let wr = (Cap::Write, d.clone());
    let da = (Cap::DbAdmin, d.clone());
    let ua = (Cap::UserAdmin, d);
    let mon = (Cap::Monitor, Scope::Cluster);
    let clu = (Cap::Cluster, Scope::Cluster);
    let caps = match name {
        "hello" | "isMaster" | "ismaster" | "ping" | "buildInfo" | "buildinfo" | "whatsmyuri"
        | "connectionStatus" | "getLastError" | "getlasterror" | "saslStart" | "saslContinue"
        | "saslSupportedMechs" | "authenticate" | "getnonce" | "logout" | "endSessions"
        | "killSessions" | "refreshSessions" | "killAllSessions" | "killAllSessionsByPattern"
        | "startSession" | "commitTransaction" | "abortTransaction" | "getMore" | "killCursors"
        | "listDatabases" | "listCommands" | "getFreeMonitoringStatus" | "atlasVersion"
        | "getLog" | "getParameter" | "getCmdLineOpts" => return None,
        "find" | "count" | "distinct" => vec![rd],
        "aggregate" => vec![if pipeline_writes(cmd) { wr } else { rd }],
        "insert" | "update" | "delete" | "bulkWrite" => vec![wr],
        "findAndModify" | "findandmodify" => vec![if writes_modify(cmd) { wr } else { rd }],
        "create" | "createIndexes" | "dropIndexes" | "drop" | "renameCollection"
        | "convertToCapped" => vec![wr, da],
        "dbStats" | "collStats" | "listIndexes" | "listCollections" | "dbHash" => vec![rd, da],
        "dropDatabase" | "collMod" | "compact" | "reIndex" | "validate" | "createView"
        | "enableSharding" => vec![da],
        "usersInfo" | "rolesInfo" => {
            vec![if for_all_dbs(cmd, name) { (Cap::UserAdmin, Scope::Any) } else { ua }]
        }
        "createUser" | "updateUser" | "dropUser" | "dropAllUsersFromDatabase"
        | "grantRolesToUser" | "revokeRolesFromUser" | "createRole" | "updateRole" | "dropRole"
        | "dropAllRolesFromDatabase" | "grantPrivilegesToRole" | "revokePrivilegesFromRole"
        | "grantRolesToRole" | "revokeRolesFromRole" => vec![ua],
        "serverStatus" | "hostInfo" | "replSetGetStatus" | "currentOp" | "top" | "connPoolStats"
        | "getShardMap" => vec![mon],
        _ => vec![clu],
    };
    Some(caps)
}

fn pipeline_writes(cmd: &Document) -> bool {
    cmd.get_array("pipeline")
        .map(|p| {
            p.iter().any(|s| {
                s.as_document()
                    .is_some_and(|d| d.contains_key("$out") || d.contains_key("$merge"))
            })
        })
        .unwrap_or(false)
}

fn writes_modify(cmd: &Document) -> bool {
    cmd.contains_key("update") || cmd.get("remove").is_some_and(truthy)
}

fn for_all_dbs(cmd: &Document, name: &str) -> bool {
    cmd.get(name)
        .and_then(Bson::as_document)
        .and_then(|d| d.get("forAllDBs"))
        .is_some_and(truthy)
}

/// `usersInfo` of exactly the connection's own users, without credentials, needs no privilege.
fn self_lookup(conn: &Conn, db: &str, cmd: &Document) -> bool {
    let Some(v) = cmd.get("usersInfo") else {
        return false;
    };
    match parse_sel(v, "user", db) {
        Ok(Sel::Refs(refs)) => {
            !refs.is_empty()
                && refs
                    .iter()
                    .all(|(u, d)| conn.users.iter().any(|a| &a.user == u && &a.db == d))
        }
        _ => false,
    }
}

fn covers(g: &Grant, need: &Need) -> bool {
    let cap_ok = g.cap == need.0 || (g.cap == Cap::Cluster && need.0 == Cap::Monitor);
    cap_ok && scope_covers(&g.scope, &need.1)
}

fn scope_covers(g: &Scope, need: &Scope) -> bool {
    match (g, need) {
        (Scope::Cluster, Scope::Cluster) => true,
        (Scope::Any, Scope::Db(_) | Scope::Any) => true,
        (Scope::Db(a), Scope::Db(b)) => a == b,
        _ => false,
    }
}

fn grants_of(roles: &RoleMap, refs: &[RoleRef]) -> Vec<Grant> {
    let mut out = Vec::new();
    for (role, rdb) in refs {
        if is_builtin(role) {
            out.extend(builtin_grants(role, rdb));
        } else if let Some(d) = roles.get(&format!("{rdb}.{role}")) {
            if let Ok(privs) = d.get_array("privileges") {
                out.extend(privilege_grants(privs));
            }
        }
    }
    out
}

fn builtin_grants(role: &str, db: &str) -> Vec<Grant> {
    let d = Scope::Db(db.to_string());
    let any = Scope::Any;
    let cluster = Scope::Cluster;
    let caps: Vec<(Cap, Scope)> = match role {
        "read" => vec![(Cap::Read, d)],
        "readWrite" => vec![(Cap::Read, d.clone()), (Cap::Write, d)],
        "dbAdmin" | "enableSharding" => vec![(Cap::DbAdmin, d)],
        "dbOwner" => vec![
            (Cap::DbAdmin, d.clone()),
            (Cap::Read, d.clone()),
            (Cap::Write, d.clone()),
            (Cap::UserAdmin, d),
        ],
        "userAdmin" => vec![(Cap::UserAdmin, d)],
        "readAnyDatabase" => vec![(Cap::Read, any)],
        "readWriteAnyDatabase" => vec![(Cap::Read, any.clone()), (Cap::Write, any)],
        "userAdminAnyDatabase" => vec![(Cap::UserAdmin, any)],
        "dbAdminAnyDatabase" => vec![(Cap::DbAdmin, any)],
        "clusterMonitor" => vec![(Cap::Monitor, cluster)],
        "clusterManager" => vec![(Cap::Monitor, cluster.clone()), (Cap::Cluster, cluster)],
        "hostManager" => vec![(Cap::Cluster, cluster)],
        "clusterAdmin" => vec![(Cap::Cluster, cluster), (Cap::DbAdmin, any)],
        "backup" => vec![(Cap::Read, any)],
        "restore" => vec![(Cap::Write, any)],
        "root" | "__system" => vec![
            (Cap::Read, any.clone()),
            (Cap::Write, any.clone()),
            (Cap::DbAdmin, any.clone()),
            (Cap::UserAdmin, any),
            (Cap::Cluster, cluster),
        ],
        _ => Vec::new(),
    };
    caps.into_iter()
        .map(|(cap, scope)| Grant { cap, scope })
        .collect()
}

fn privilege_grants(privs: &[Bson]) -> Vec<Grant> {
    let mut out = Vec::new();
    for p in privs {
        let Some(pd) = p.as_document() else {
            continue;
        };
        let scope = match pd.get_document("resource") {
            Ok(res) if res.get_bool("cluster").unwrap_or(false) => Scope::Cluster,
            Ok(res) if res.get_bool("anyResource").unwrap_or(false) => Scope::Any,
            Ok(res) => match res.get_str("db") {
                Ok(db) if !db.is_empty() => Scope::Db(db.to_string()),
                _ => Scope::Any,
            },
            Err(_) => continue,
        };
        let actions: Vec<&str> = pd
            .get_array("actions")
            .map(|a| a.iter().filter_map(|b| b.as_str()).collect())
            .unwrap_or_default();
        for action in actions {
            let caps: Vec<Cap> = if action == "anyAction" {
                if scope == Scope::Cluster {
                    vec![Cap::Monitor, Cap::Cluster]
                } else {
                    vec![Cap::Read, Cap::Write, Cap::DbAdmin, Cap::UserAdmin]
                }
            } else {
                action_caps(action).to_vec()
            };
            for cap in caps {
                let s = if matches!(cap, Cap::Monitor | Cap::Cluster) {
                    Scope::Cluster
                } else {
                    scope.clone()
                };
                out.push(Grant { cap, scope: s });
            }
        }
    }
    out
}

fn action_caps(action: &str) -> &'static [Cap] {
    match action {
        "find" | "changeStream" | "collStats" | "dbStats" | "listIndexes" | "listCollections"
        | "indexStats" | "dbHash" => &[Cap::Read],
        "insert" | "update" | "remove" | "createCollection" | "createIndex" | "dropCollection"
        | "dropIndex" | "renameCollectionSameDB" | "convertToCapped" => &[Cap::Write],
        "dropDatabase" | "collMod" | "compact" | "reIndex" | "validate" | "createView"
        | "enableProfiler" => &[Cap::DbAdmin],
        "createUser" | "dropUser" | "grantRole" | "revokeRole" | "viewUser" | "changePassword"
        | "changeCustomData" | "changeOwnPassword" | "changeOwnCustomData" | "createRole"
        | "dropRole" | "viewRole" | "setAuthenticationRestriction" => &[Cap::UserAdmin],
        "serverStatus" | "inprog" | "top" | "connPoolStats" | "replSetGetStatus"
        | "listDatabases" | "getShardMap" | "listSessions" | "hostInfo" => &[Cap::Monitor],
        "shutdown" | "fsync" | "setParameter" | "killop" | "replSetReconfig" | "killAnyCursor"
        | "cpuProfiler" => &[Cap::Cluster],
        _ => &[],
    }
}

// ---------------------------------------------------------------------------
// Storage helpers
// ---------------------------------------------------------------------------

fn filter_rows(
    rows: impl Iterator<Item = Result<(u64, Document)>>,
    filter: &Document,
) -> Result<Vec<(u64, Document)>> {
    let m = Matcher::new(filter)?;
    let mut out = Vec::new();
    for row in rows {
        let (rid, doc) = row?;
        if m.matches(&doc)? {
            out.push((rid, doc));
        }
    }
    Ok(out)
}

/// Read documents from `ns` matching `filter` (committed state).
fn query(store: &Store, ns: &str, filter: &Document) -> Result<Vec<Document>> {
    let snap = store.snapshot();
    let rows: Vec<(u64, Document)> = rows!(store, &snap, ns, filter)?;
    Ok(rows.into_iter().map(|(_, d)| d).collect())
}

fn load_roles(store: &Store) -> Result<RoleMap> {
    Ok(query(store, ROLES_NS, &Document::new())?
        .into_iter()
        .filter_map(|d| {
            let key = d.get_str("_id").ok()?.to_string();
            Some((key, d))
        })
        .collect())
}

fn not_found(ns: &str, name: &str, db: &str) -> Error {
    if ns == USERS_NS {
        Error::new(11, format!("Could not find user \"{name}\" for db \"{db}\""))
    } else {
        Error::new(31, format!("Could not find role: {name}@{db}"))
    }
}

fn denied(db: &str, name: &str, cmd: &Document) -> Error {
    let value = cmd.get(name).map(|v| v.to_string()).unwrap_or_default();
    Error::new(
        13,
        format!("not authorized on {db} to execute command {{ {name}: {value}, $db: \"{db}\" }}"),
    )
}

// ---------------------------------------------------------------------------
// Role resolution
// ---------------------------------------------------------------------------

fn is_builtin(role: &str) -> bool {
    BUILTIN_ROLES.contains(&role)
}

/// Direct role references of a user or role document.
fn user_roles(srv: &Server, roles: &RoleMap, db: &str, user: &str) -> Vec<RoleRef> {
    let direct = find_user(srv, db, user)
        .map(|u| refs_from(u.get("roles")))
        .unwrap_or_default();
    expand(roles, &direct)
}

/// Transitive closure of `direct` through custom roles, including `direct`. Cycle-safe.
fn expand(roles: &RoleMap, direct: &[RoleRef]) -> Vec<RoleRef> {
    let mut out: Vec<RoleRef> = Vec::new();
    let mut stack: Vec<RoleRef> = direct.to_vec();
    while let Some(r) = stack.pop() {
        if out.contains(&r) {
            continue;
        }
        if !is_builtin(&r.0) {
            if let Some(d) = roles.get(&format!("{}.{}", r.1, r.0)) {
                stack.extend(refs_from(d.get("roles")));
            }
        }
        out.push(r);
    }
    out
}

fn refs_from(v: Option<&Bson>) -> Vec<RoleRef> {
    let Some(Bson::Array(items)) = v else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|it| {
            let d = it.as_document()?;
            Some((
                d.get_str("role").ok()?.to_string(),
                d.get_str("db").ok()?.to_string(),
            ))
        })
        .collect()
}

fn refs_bson(refs: &[RoleRef]) -> Bson {
    Bson::Array(
        refs.iter()
            .map(|(r, d)| Bson::Document(doc! {"role": r.as_str(), "db": d.as_str()}))
            .collect(),
    )
}

fn parse_ref(v: &Bson, db: &str) -> Result<RoleRef> {
    match v {
        Bson::String(r) => Ok((r.clone(), db.to_string())),
        Bson::Document(d) => {
            let r = d
                .get_str("role")
                .map_err(|_| Error::bad_value("role reference needs a string 'role'"))?;
            let rdb = d
                .get_str("db")
                .map_err(|_| Error::bad_value("role reference needs a string 'db'"))?;
            Ok((r.to_string(), rdb.to_string()))
        }
        _ => Err(Error::bad_value("role must be a string or a {role, db} document")),
    }
}

fn parse_role_list(v: Option<&Bson>, db: &str) -> Result<Vec<RoleRef>> {
    let Some(v) = v else {
        return Ok(Vec::new());
    };
    let Bson::Array(items) = v else {
        return Err(Error::type_mismatch("'roles' must be an array"));
    };
    items.iter().map(|it| parse_ref(it, db)).collect()
}

fn optional_roles(cmd: &Document, db: &str) -> Result<Option<Vec<RoleRef>>> {
    if cmd.contains_key("roles") {
        Ok(Some(parse_role_list(cmd.get("roles"), db)?))
    } else {
        Ok(None)
    }
}

fn validate_roles(roles: &RoleMap, refs: &[RoleRef]) -> Result<()> {
    for (r, d) in refs {
        if !is_builtin(r) && !roles.contains_key(&format!("{d}.{r}")) {
            return Err(Error::new(31, format!("Could not find role: {r}@{d}")));
        }
    }
    Ok(())
}

/// Parse a `usersInfo`/`rolesInfo` argument. `key` is "user" or "role".
fn parse_sel(v: &Bson, key: &str, db: &str) -> Result<Sel> {
    match v {
        Bson::String(n) => Ok(Sel::Refs(vec![(n.clone(), db.to_string())])),
        Bson::Document(d) => {
            if let Some(n) = d.get(key) {
                let n = n
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch(format!("'{key}' must be a string")))?;
                let rdb = d.get_str("db").unwrap_or(db);
                Ok(Sel::Refs(vec![(n.to_string(), rdb.to_string())]))
            } else if d.get("forAllDBs").is_some_and(truthy) {
                Ok(Sel::AllDbs)
            } else {
                Err(Error::bad_value(format!(
                    "'{key}' or 'forAllDBs' must be specified"
                )))
            }
        }
        Bson::Array(items) => {
            let mut refs = Vec::new();
            for it in items {
                match parse_sel(it, key, db)? {
                    Sel::Refs(r) => refs.extend(r),
                    _ => {
                        return Err(Error::bad_value(
                            "array selectors must name users or roles",
                        ))
                    }
                }
            }
            Ok(Sel::Refs(refs))
        }
        other if truthy(other) => Ok(Sel::AllInDb),
        _ => Err(Error::type_mismatch(
            "usersInfo/rolesInfo argument must be a string, document, array, or truthy value",
        )),
    }
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

fn arg<'a>(cmd: &'a Document, key: &str) -> Result<&'a Bson> {
    cmd.get(key)
        .ok_or_else(|| Error::bad_value(format!("Missing required field '{key}'")))
}

fn str_arg(cmd: &Document, key: &str) -> Result<String> {
    match cmd.get(key) {
        Some(Bson::String(s)) => Ok(s.clone()),
        Some(_) => Err(Error::type_mismatch(format!("'{key}' must be a string"))),
        None => Err(Error::bad_value(format!("Missing required field '{key}'"))),
    }
}

fn flag(cmd: &Document, key: &str) -> bool {
    cmd.get(key).is_some_and(truthy)
}

fn ok_doc() -> Document {
    doc! {"ok": 1.0}
}

fn custom_data(cmd: &Document) -> Result<Option<Bson>> {
    match cmd.get("customData") {
        None => Ok(None),
        Some(v @ Bson::Document(_)) => Ok(Some(v.clone())),
        Some(_) => Err(Error::type_mismatch("'customData' must be an object")),
    }
}

fn new_credentials(p: &Bson, user: &str) -> Result<Document> {
    let Bson::String(pwd) = p else {
        return Err(Error::type_mismatch("'pwd' must be a string"));
    };
    Ok(make_credentials(user, pwd, &SCRAM_MECHS))
}

/// Credentials for `user` on `db` from `pwd`. `create` makes `pwd` mandatory outside `$external`.
fn credentials_for(cmd: &Document, db: &str, user: &str, create: bool) -> Result<Option<Document>> {
    let pwd = cmd.get("pwd");
    if db == EXTERNAL_DB {
        if pwd.is_some() {
            return Err(Error::bad_value(
                "Cannot set a password for users in the $external database",
            ));
        }
        return Ok(None);
    }
    match pwd {
        Some(p) => Ok(Some(new_credentials(p, user)?)),
        None if create => Err(Error::bad_value(
            "Must provide a 'pwd' field for all user documents, except those with '$external' as the user's source db",
        )),
        None => Ok(None),
    }
}

fn check_privileges(v: Option<&Bson>) -> Result<Vec<Bson>> {
    let Some(v) = v else {
        return Ok(Vec::new());
    };
    let Bson::Array(items) = v else {
        return Err(Error::bad_value("'privileges' must be an array"));
    };
    for it in items {
        let shaped = it.as_document().is_some_and(|d| {
            d.get_document("resource").is_ok()
                && d.get_array("actions")
                    .map(|a| a.iter().all(|x| x.as_str().is_some()))
                    .unwrap_or(false)
        });
        if !shaped {
            return Err(Error::bad_value(
                "each privilege needs a 'resource' document and an 'actions' string array",
            ));
        }
    }
    Ok(items.clone())
}

fn privilege_actions(d: &Document) -> Vec<Bson> {
    d.get_array("actions").map(|a| a.clone()).unwrap_or_default()
}

fn priv_bson(resource: &Document, actions: Vec<Bson>) -> Bson {
    Bson::Document(doc! {"resource": resource.clone(), "actions": actions})
}

/// Grant or revoke `add` on `cur`, merging actions per resource.
fn merge_privs(cur: &mut Vec<Bson>, add: &[Bson], grant: bool) {
    for p in add {
        let Some(pd) = p.as_document() else {
            continue;
        };
        let Ok(res) = pd.get_document("resource") else {
            continue;
        };
        let acts = privilege_actions(pd);
        let idx = cur.iter().position(|c| {
            c.as_document().and_then(|d| d.get_document("resource").ok()) == Some(res)
        });
        match idx {
            Some(i) => {
                let mut have = cur[i].as_document().map(privilege_actions).unwrap_or_default();
                if grant {
                    for a in &acts {
                        if !have.contains(a) {
                            have.push(a.clone());
                        }
                    }
                } else {
                    have.retain(|a| !acts.contains(a));
                }
                if have.is_empty() {
                    cur.remove(i);
                } else {
                    cur[i] = priv_bson(res, have);
                }
            }
            None if grant => cur.push(Bson::Document(pd.clone())),
            None => {}
        }
    }
}

fn user_doc(
    id: &str,
    user: &str,
    db: &str,
    cred: Option<Document>,
    roles: &[RoleRef],
    custom: Bson,
) -> Document {
    let mut d = doc! {
        "_id": id,
        "userId": new_uuid(),
        "user": user,
        "db": db,
        "roles": refs_bson(roles),
        "customData": custom,
    };
    if let Some(c) = cred {
        d.insert("credentials", c);
    }
    d
}

fn mechanisms(u: &Document) -> Bson {
    Bson::Array(
        u.get_document("credentials")
            .map(|c| {
                c.keys()
                    .map(|k| Bson::String(k.clone()))
                    .collect::<Vec<Bson>>()
            })
            .unwrap_or_default(),
    )
}

fn user_info(u: &Document, roles: &RoleMap, show_creds: bool, show_custom: bool) -> Document {
    let field = |k: &str| u.get(k).cloned().unwrap_or(Bson::Null);
    let direct = refs_from(u.get("roles"));
    let mut d = doc! {
        "_id": field("_id"),
        "userId": field("userId"),
        "user": field("user"),
        "db": field("db"),
        "roles": refs_bson(&direct),
        "inheritedRoles": refs_bson(&expand(roles, &direct)),
        "mechanisms": mechanisms(u),
    };
    if show_custom {
        d.insert(
            "customData",
            u.get("customData")
                .cloned()
                .unwrap_or_else(|| Bson::Document(Document::new())),
        );
    }
    if show_creds {
        if let Some(c) = u.get("credentials") {
            d.insert("credentials", c.clone());
        }
    }
    d
}

fn role_info(roles: &RoleMap, role: &str, rdb: &str, show_privs: bool) -> Option<Document> {
    if is_builtin(role) {
        let mut d = doc! {
            "role": role,
            "db": rdb,
            "isBuiltin": true,
            "roles": Bson::Array(Vec::new()),
            "inheritedRoles": Bson::Array(Vec::new()),
        };
        if show_privs {
            d.insert("privileges", Bson::Array(Vec::new()));
        }
        return Some(d);
    }
    let stored = roles.get(&format!("{rdb}.{role}"))?;
    let direct = refs_from(stored.get("roles"));
    let mut d = doc! {
        "role": role,
        "db": rdb,
        "isBuiltin": false,
        "roles": refs_bson(&direct),
        "inheritedRoles": refs_bson(&expand(roles, &direct)),
    };
    if show_privs {
        d.insert(
            "privileges",
            stored
                .get("privileges")
                .cloned()
                .unwrap_or_else(|| Bson::Array(Vec::new())),
        );
    }
    Some(d)
}

/// Remove references to `role@rdb` from all users and roles.
fn strip_refs(s: &Store, t: &mut Txn, role: &str, rdb: &str) -> Result<()> {
    for ns in [USERS_NS, ROLES_NS] {
        let Some(c) = s.get_coll(ns) else {
            continue;
        };
        let rows: Vec<(u64, Document)> = rows!(s, &t.tx, ns, &Document::new())?;
        for (rid, old) in rows {
            let cur = refs_from(old.get("roles"));
            let kept: Vec<RoleRef> = cur
                .iter()
                .filter(|r| !(r.0 == role && r.1 == rdb))
                .cloned()
                .collect();
            if kept.len() != cur.len() {
                let mut new = old.clone();
                new.insert("roles", refs_bson(&kept));
                s.update(t, &c, rid, &old, new, Document::new())?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// User commands
// ---------------------------------------------------------------------------

fn create_user(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let user = str_arg(&ctx.cmd, &ctx.name)?;
    let cred = credentials_for(&ctx.cmd, &db, &user, true)?;
    let roles = parse_role_list(ctx.cmd.get("roles"), &db)?;
    let custom = custom_data(&ctx.cmd)?.unwrap_or_else(|| Bson::Document(Document::new()));
    let store = ctx.store().clone();
    validate_roles(&load_roles(&store)?, &roles)?;
    store.coll_for_write(USERS_NS)?;
    let id = format!("{db}.{user}");
    let doc = user_doc(&id, &user, &db, cred, &roles, custom);
    ctx.write(|s, t| {
        if !rows!(s, &t.tx, USERS_NS, &doc!{"_id": id.clone()})?.is_empty() {
            return Err(Error::new(
                51003,
                format!("User \"{user}@{db}\" already exists"),
            ));
        }
        let c = s.coll_for_write(USERS_NS)?;
        s.insert(t, &c, doc.clone())?;
        Ok(())
    })?;
    Ok(ok_doc())
}

fn update_user(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let user = str_arg(&ctx.cmd, &ctx.name)?;
    let cred = credentials_for(&ctx.cmd, &db, &user, false)?;
    let custom = custom_data(&ctx.cmd)?;
    let roles = optional_roles(&ctx.cmd, &db)?;
    let store = ctx.store().clone();
    if let Some(r) = &roles {
        validate_roles(&load_roles(&store)?, r)?;
    }
    let id = format!("{db}.{user}");
    ctx.write(|s, t| {
        let c = s
            .get_coll(USERS_NS)
            .ok_or_else(|| not_found(USERS_NS, &user, &db))?;
        let (rid, old) = rows!(s, &t.tx, USERS_NS, &doc!{"_id": id.clone()})?
            .into_iter()
            .next()
            .ok_or_else(|| not_found(USERS_NS, &user, &db))?;
        let mut new = old.clone();
        if let Some(cr) = &cred {
            new.insert("credentials", cr.clone());
        }
        if let Some(cd) = &custom {
            new.insert("customData", cd.clone());
        }
        if let Some(r) = &roles {
            new.insert("roles", refs_bson(r));
        }
        s.update(t, &c, rid, &old, new, Document::new())
    })?;
    Ok(ok_doc())
}

fn drop_user(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let user = str_arg(&ctx.cmd, &ctx.name)?;
    let id = format!("{db}.{user}");
    ctx.write(|s, t| {
        let c = s
            .get_coll(USERS_NS)
            .ok_or_else(|| not_found(USERS_NS, &user, &db))?;
        let (rid, old) = rows!(s, &t.tx, USERS_NS, &doc!{"_id": id.clone()})?
            .into_iter()
            .next()
            .ok_or_else(|| not_found(USERS_NS, &user, &db))?;
        s.delete(t, &c, rid, &old)
    })?;
    Ok(ok_doc())
}

fn drop_all_users(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let n = ctx.write(|s, t| {
        let Some(c) = s.get_coll(USERS_NS) else {
            return Ok(0usize);
        };
        let rows: Vec<(u64, Document)> = rows!(s, &t.tx, USERS_NS, &doc!{"db": db.clone()})?;
        let n = rows.len();
        for (rid, old) in rows {
            s.delete(t, &c, rid, &old)?;
        }
        Ok(n)
    })?;
    Ok(doc! {"n": i32::try_from(n).unwrap_or(i32::MAX), "ok": 1.0})
}

fn users_info(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let value = arg(&ctx.cmd, &ctx.name)?;
    let sel = parse_sel(value, "user", &db)?;
    let show_creds = flag(&ctx.cmd, "showCredentials");
    let show_custom = ctx.cmd.get("showCustomData").map_or(true, truthy);
    let store = ctx.store().clone();
    let roles = load_roles(&store)?;
    let users: Vec<Document> = match &sel {
        Sel::AllInDb => query(&store, USERS_NS, &doc! {"db": db.as_str()})?,
        Sel::AllDbs => query(&store, USERS_NS, &Document::new())?,
        Sel::Refs(refs) => {
            let mut out = Vec::new();
            for (u, d) in refs {
                out.extend(query(&store, USERS_NS, &doc! {"_id": format!("{d}.{u}")})?);
            }
            out
        }
    };
    let out: Vec<Bson> = users
        .iter()
        .map(|u| Bson::Document(user_info(u, &roles, show_creds, show_custom)))
        .collect();
    Ok(doc! {"users": out, "ok": 1.0})
}

/// `grantRolesToUser`/`revokeRolesFromUser` (ns = USERS_NS) and
/// `grantRolesToRole`/`revokeRolesFromRole` (ns = ROLES_NS).
fn change_refs(ctx: &mut CmdCtx, ns: &'static str, grant: bool) -> Result<Document> {
    let db = ctx.db.clone();
    let name = str_arg(&ctx.cmd, &ctx.name)?;
    let refs = parse_role_list(ctx.cmd.get("roles"), &db)?;
    let store = ctx.store().clone();
    if grant {
        validate_roles(&load_roles(&store)?, &refs)?;
    }
    let id = format!("{db}.{name}");
    ctx.write(|s, t| {
        let c = s.get_coll(ns).ok_or_else(|| not_found(ns, &name, &db))?;
        let (rid, old) = rows!(s, &t.tx, ns, &doc! {"_id": id.clone()})?
            .into_iter()
            .next()
            .ok_or_else(|| not_found(ns, &name, &db))?;
        let mut cur = refs_from(old.get("roles"));
        if grant {
            for r in &refs {
                if !cur.contains(r) {
                    cur.push(r.clone());
                }
            }
        } else {
            cur.retain(|r| !refs.contains(r));
        }
        let mut new = old.clone();
        new.insert("roles", refs_bson(&cur));
        s.update(t, &c, rid, &old, new, Document::new())
    })?;
    Ok(ok_doc())
}

// ---------------------------------------------------------------------------
// Role commands
// ---------------------------------------------------------------------------

fn create_role(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let role = str_arg(&ctx.cmd, &ctx.name)?;
    if is_builtin(&role) {
        return Err(Error::new(
            51002,
            format!("Role \"{role}@{db}\" already exists"),
        ));
    }
    let privs = check_privileges(ctx.cmd.get("privileges"))?;
    let roles = parse_role_list(ctx.cmd.get("roles"), &db)?;
    let store = ctx.store().clone();
    validate_roles(&load_roles(&store)?, &roles)?;
    store.coll_for_write(ROLES_NS)?;
    let id = format!("{db}.{role}");
    let doc = doc! {
        "_id": id.clone(),
        "role": role.clone(),
        "db": db.clone(),
        "privileges": Bson::Array(privs),
        "roles": refs_bson(&roles),
    };
    ctx.write(|s, t| {
        if !rows!(s, &t.tx, ROLES_NS, &doc! {"_id": id.clone()})?.is_empty() {
            return Err(Error::new(
                51002,
                format!("Role \"{role}@{db}\" already exists"),
            ));
        }
        let c = s.coll_for_write(ROLES_NS)?;
        s.insert(t, &c, doc.clone())?;
        Ok(())
    })?;
    Ok(ok_doc())
}

fn update_role(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let role = str_arg(&ctx.cmd, &ctx.name)?;
    if is_builtin(&role) {
        return Err(not_found(ROLES_NS, &role, &db));
    }
    let privs = if ctx.cmd.contains_key("privileges") {
        Some(check_privileges(ctx.cmd.get("privileges"))?)
    } else {
        None
    };
    let roles = optional_roles(&ctx.cmd, &db)?;
    let store = ctx.store().clone();
    if let Some(r) = &roles {
        validate_roles(&load_roles(&store)?, r)?;
    }
    let id = format!("{db}.{role}");
    ctx.write(|s, t| {
        let c = s
            .get_coll(ROLES_NS)
            .ok_or_else(|| not_found(ROLES_NS, &role, &db))?;
        let (rid, old) = rows!(s, &t.tx, ROLES_NS, &doc! {"_id": id.clone()})?
            .into_iter()
            .next()
            .ok_or_else(|| not_found(ROLES_NS, &role, &db))?;
        let mut new = old.clone();
        if let Some(p) = &privs {
            new.insert("privileges", Bson::Array(p.clone()));
        }
        if let Some(r) = &roles {
            new.insert("roles", refs_bson(r));
        }
        s.update(t, &c, rid, &old, new, Document::new())
    })?;
    Ok(ok_doc())
}

fn drop_role(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let role = str_arg(&ctx.cmd, &ctx.name)?;
    let id = format!("{db}.{role}");
    ctx.write(|s, t| {
        let c = s
            .get_coll(ROLES_NS)
            .ok_or_else(|| not_found(ROLES_NS, &role, &db))?;
        let (rid, old) = rows!(s, &t.tx, ROLES_NS, &doc! {"_id": id.clone()})?
            .into_iter()
            .next()
            .ok_or_else(|| not_found(ROLES_NS, &role, &db))?;
        s.delete(t, &c, rid, &old)?;
        strip_refs(s, t, &role, &db)
    })?;
    Ok(ok_doc())
}

fn drop_all_roles(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let n = ctx.write(|s, t| {
        let Some(c) = s.get_coll(ROLES_NS) else {
            return Ok(0usize);
        };
        let rows: Vec<(u64, Document)> = rows!(s, &t.tx, ROLES_NS, &doc! {"db": db.clone()})?;
        let mut names = Vec::new();
        for (rid, old) in rows {
            if let Ok(r) = old.get_str("role") {
                names.push(r.to_string());
            }
            s.delete(t, &c, rid, &old)?;
        }
        for r in &names {
            strip_refs(s, t, r, &db)?;
        }
        Ok(names.len())
    })?;
    Ok(doc! {"n": i32::try_from(n).unwrap_or(i32::MAX), "ok": 1.0})
}

fn roles_info(ctx: &mut CmdCtx) -> Result<Document> {
    let db = ctx.db.clone();
    let value = arg(&ctx.cmd, &ctx.name)?;
    let sel = parse_sel(value, "role", &db)?;
    let show_builtin = flag(&ctx.cmd, "showBuiltinRoles");
    let show_privs = flag(&ctx.cmd, "showPrivileges");
    let store = ctx.store().clone();
    let roles = load_roles(&store)?;
    let mut refs: Vec<RoleRef> = Vec::new();
    match &sel {
        Sel::Refs(r) => refs.extend(r.iter().cloned()),
        Sel::AllInDb | Sel::AllDbs => {
            let only: Option<&str> = if matches!(sel, Sel::AllInDb) {
                Some(db.as_str())
            } else {
                None
            };
            refs.extend(roles.values().filter_map(|d| {
                let rdb = d.get_str("db").ok()?;
                if only.is_some_and(|o| o != rdb) {
                    return None;
                }
                Some((d.get_str("role").ok()?.to_string(), rdb.to_string()))
            }));
            if show_builtin {
                let bdb = only.unwrap_or(db.as_str());
                for b in BUILTIN_ROLES {
                    refs.push(((*b).to_string(), bdb.to_string()));
                }
            }
        }
    }
    let out: Vec<Bson> = refs
        .iter()
        .filter_map(|(r, d)| role_info(&roles, r, d, show_privs))
        .map(Bson::Document)
        .collect();
    Ok(doc! {"roles": out, "ok": 1.0})
}

fn change_role_privs(ctx: &mut CmdCtx, grant: bool) -> Result<Document> {
    let db = ctx.db.clone();
    let role = str_arg(&ctx.cmd, &ctx.name)?;
    let privs = check_privileges(ctx.cmd.get("privileges"))?;
    let id = format!("{db}.{role}");
    ctx.write(|s, t| {
        let c = s
            .get_coll(ROLES_NS)
            .ok_or_else(|| not_found(ROLES_NS, &role, &db))?;
        let (rid, old) = rows!(s, &t.tx, ROLES_NS, &doc! {"_id": id.clone()})?
            .into_iter()
            .next()
            .ok_or_else(|| not_found(ROLES_NS, &role, &db))?;
        let mut cur: Vec<Bson> = old
            .get_array("privileges")
            .map(|a| a.clone())
            .unwrap_or_default();
        merge_privs(&mut cur, &privs, grant);
        let mut new = old.clone();
        new.insert("privileges", Bson::Array(cur));
        s.update(t, &c, rid, &old, new, Document::new())
    })?;
    Ok(ok_doc())
}

fn connection_status(ctx: &mut CmdCtx) -> Result<Document> {
    let show_privs = flag(&ctx.cmd, "showPrivileges");
    let store = ctx.store().clone();
    let roles = load_roles(&store)?;
    let mut users: Vec<Bson> = Vec::new();
    let mut all_roles: Vec<RoleRef> = Vec::new();
    let mut privs: Vec<Bson> = Vec::new();
    for u in &ctx.conn.users {
        users.push(Bson::Document(doc! {"user": u.user.clone(), "db": u.db.clone()}));
        for r in user_roles(ctx.srv, &roles, &u.db, &u.user) {
            if all_roles.contains(&r) {
                continue;
            }
            if !is_builtin(&r.0) {
                if let Some(d) = roles.get(&format!("{}.{}", r.1, r.0)) {
                    if let Ok(p) = d.get_array("privileges") {
                        merge_privs(&mut privs, p, true);
                    }
                }
            }
            all_roles.push(r);
        }
    }
    let mut info = doc! {
        "authenticatedUsers": users,
        "authenticatedUserRoles": refs_bson(&all_roles),
    };
    if show_privs {
        info.insert("authenticatedUserPrivileges", Bson::Array(privs));
    }
    Ok(doc! {"authInfo": info, "ok": 1.0})
}
