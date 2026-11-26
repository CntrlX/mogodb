//! Command router.

pub mod admin;
pub mod authcmd;
pub mod bulk;
pub mod crud;
pub mod qsettings;
pub mod users;

use crate::error::{Error, Result};
use crate::server::{self, CmdCtx};
use bson::{Bson, Document, doc};

/// Commands permitted inside multi-document transactions.
pub fn txn_allowed(name: &str) -> bool {
    matches!(
        name,
        "find" | "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "aggregate" | "distinct" | "getMore"
            | "killCursors" | "count" | "create" | "createIndexes" | "hello" | "isMaster" | "ismaster" | "ping"
            | "commitTransaction" | "abortTransaction" | "bulkWrite" | "dbStats" | "buildInfo" | "listCollections"
    )
}

pub fn route(ctx: &mut CmdCtx) -> Result<Document> {
    let name = ctx.name.clone();
    match name.as_str() {
        "hello" | "isMaster" | "ismaster" => server::hello(ctx),
        "ping" => Ok(doc! {"ok": 1.0}),
        "buildInfo" | "buildinfo" => Ok(server::build_info()),
        "whatsmyuri" => Ok(doc! {"you": ctx.conn.peer.clone(), "ok": 1.0}),
        "endSessions" | "killSessions" => {
            if let Ok(a) = ctx.cmd.get_array(&name) {
                for l in a.iter().filter_map(Bson::as_document) {
                    ctx.srv.sessions.end(l);
                    ctx.srv.cursors.kill_for_session(&Bson::Document(l.clone()));
                }
            }
            Ok(doc! {"ok": 1.0})
        }
        "refreshSessions" | "killAllSessions" | "killAllSessionsByPattern" => Ok(doc! {"ok": 1.0}),
        "startSession" => {
            let id = crate::storage::new_uuid();
            Ok(doc! {"id": {"id": Bson::Binary(id)}, "timeoutMinutes": 30, "ok": 1.0})
        }
        "commitTransaction" | "abortTransaction" => txn_end(ctx),
        "saslStart" | "saslContinue" | "logout" | "authenticate" | "getnonce" => authcmd::route(ctx),
        "find" | "insert" | "update" | "delete" | "findAndModify" | "findandmodify" | "count" | "distinct" | "aggregate"
        | "getMore" | "killCursors" | "explain" => crud::route(ctx),
        "bulkWrite" => bulk::bulk_write(ctx),
        "mapReduce" | "mapreduce" => crate::js::map_reduce(ctx),
        "createSearchIndexes" | "updateSearchIndex" | "dropSearchIndex" => crate::search::route_cmd(ctx),
        "setQuerySettings" | "removeQuerySettings" => qsettings::route_cmd(ctx),
        "createUser" | "updateUser" | "dropUser" | "dropAllUsersFromDatabase" | "usersInfo" | "grantRolesToUser"
        | "revokeRolesFromUser" | "createRole" | "updateRole" | "dropRole" | "dropAllRolesFromDatabase" | "rolesInfo"
        | "grantPrivilegesToRole" | "revokePrivilegesFromRole" | "grantRolesToRole" | "revokeRolesFromRole"
        | "connectionStatus" => users::route(ctx),
        _ => admin::route(ctx),
    }
}

fn txn_end(ctx: &mut CmdCtx) -> Result<Document> {
    if ctx.db != "admin" {
        return Err(Error::new(13, format!("{} may only be run against the admin database.", ctx.name)));
    }
    let n = ctx.txn.txn_number.ok_or_else(|| Error::bad_value("txnNumber is required"))?;
    let store = ctx.srv.store.clone();
    let commit = ctx.name == "commitTransaction";
    ctx.with_session(|s| if commit { crate::session::commit(&store, s, n) } else { crate::session::abort(s, n) })
}
