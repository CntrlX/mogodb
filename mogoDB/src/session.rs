//! Logical sessions, multi-document transactions and retryable-write de-duplication.

use crate::error::{Error, Result};
use crate::storage::{Store, Txn};
use bson::{Bson, Document, doc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const TXN_LIFETIME: Duration = Duration::from_secs(60);
pub const SESSION_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnState {
    InProgress,
    Committed,
    Aborted,
}

pub struct ActiveTxn {
    pub number: i64,
    pub state: TxnState,
    /// Present while in progress.
    pub txn: Option<Txn>,
    pub started: Instant,
}

#[derive(Default)]
pub struct Session {
    pub last_used: Option<Instant>,
    pub txn: Option<ActiveTxn>,
    /// Retryable writes: last txnNumber and its reply.
    pub retry: Option<(i64, Document)>,
}

#[derive(Default)]
pub struct SessionMgr {
    map: Mutex<HashMap<Vec<u8>, Arc<Mutex<Session>>>>,
}

/// Session key: raw bytes of `lsid.id` (UUID).
pub fn lsid_key(lsid: &Document) -> Vec<u8> {
    match lsid.get("id") {
        Some(Bson::Binary(b)) => b.bytes.clone(),
        Some(other) => other.to_string().into_bytes(),
        None => Vec::new(),
    }
}

pub fn no_such_txn(n: i64) -> Error {
    Error::new(251, format!("Transaction with {{ txnNumber: {n} }} has been aborted.")).with_label("TransientTransactionError")
}

impl SessionMgr {
    pub fn get(&self, lsid: &Document) -> Arc<Mutex<Session>> {
        let k = lsid_key(lsid);
        let s = self.map.lock().expect("sessions").entry(k).or_default().clone();
        s.lock().expect("session").last_used = Some(Instant::now());
        s
    }

    pub fn end(&self, lsid: &Document) {
        if let Some(s) = self.map.lock().expect("sessions").remove(&lsid_key(lsid)) {
            if let Some(t) = s.lock().expect("session").txn.as_mut() {
                if let Some(x) = t.txn.take() {
                    x.tx.rollback();
                }
            }
        }
    }

    pub fn count(&self) -> usize {
        self.map.lock().expect("sessions").len()
    }

    /// Abort expired transactions and drop idle sessions.
    pub fn reap(&self) {
        let mut m = self.map.lock().expect("sessions");
        m.retain(|_, s| {
            let Ok(mut s) = s.try_lock() else { return true };
            if let Some(t) = s.txn.as_mut() {
                if t.state == TxnState::InProgress && t.started.elapsed() > TXN_LIFETIME {
                    if let Some(x) = t.txn.take() {
                        x.tx.rollback();
                    }
                    t.state = TxnState::Aborted;
                }
            }
            s.last_used.is_some_and(|l| l.elapsed() < SESSION_TIMEOUT)
        });
    }

    pub fn active_txns(&self) -> usize {
        self.map
            .lock()
            .expect("sessions")
            .values()
            .filter(|s| s.try_lock().is_ok_and(|s| s.txn.as_ref().is_some_and(|t| t.state == TxnState::InProgress)))
            .count()
    }
}

/// Transaction fields extracted from a command body.
#[derive(Clone, Debug, Default)]
pub struct TxnArgs {
    pub lsid: Option<Document>,
    pub txn_number: Option<i64>,
    pub autocommit_false: bool,
    pub start: bool,
}

impl TxnArgs {
    pub fn from_cmd(cmd: &Document) -> TxnArgs {
        TxnArgs {
            lsid: cmd.get_document("lsid").ok().cloned(),
            txn_number: cmd.get("txnNumber").and_then(crate::bsonutil::num_i64),
            autocommit_false: matches!(cmd.get("autocommit"), Some(Bson::Boolean(false))),
            start: crate::bsonutil::as_bool(cmd.get("startTransaction")),
        }
    }
    pub fn in_txn(&self) -> bool {
        self.autocommit_false && self.lsid.is_some() && self.txn_number.is_some()
    }
}

/// Prepare the session transaction for a statement: start it, or validate continuation.
pub fn prepare_txn(store: &Store, sess: &mut Session, args: &TxnArgs) -> Result<()> {
    let n = args.txn_number.unwrap_or(0);
    if args.start {
        if let Some(t) = &sess.txn {
            if t.number > n {
                return Err(Error::new(225, format!("Cannot start transaction {n} on session because a newer transaction {} has already started", t.number)));
            }
            if t.number == n && t.state != TxnState::Aborted {
                return Err(Error::new(251, format!("Given transaction number {n} does not match any in-progress transactions. The active transaction number is {}", t.number)));
            }
        }
        if let Some(old) = sess.txn.as_mut().and_then(|t| t.txn.take()) {
            old.tx.rollback();
        }
        let slot = OpenTxnSlot::acquire()?;
        let mut txn = store.begin();
        txn.slot = Some(slot);
        txn.lsid = args.lsid.clone();
        txn.txn_number = Some(n);
        sess.txn = Some(ActiveTxn { number: n, state: TxnState::InProgress, txn: Some(txn), started: std::time::Instant::now() });
        return Ok(());
    }
    match &sess.txn {
        Some(t) if t.number == n && t.state == TxnState::InProgress => Ok(()),
        Some(t) if t.number == n && t.state == TxnState::Committed => {
            Err(Error::new(256, "Transaction has been committed; cannot run more operations"))
        }
        _ => Err(no_such_txn(n)),
    }
}

/// Abort after a failed statement (MongoDB aborts the transaction on statement errors).
pub fn abort_on_error(sess: &mut Session) {
    if let Some(t) = sess.txn.as_mut() {
        if let Some(x) = t.txn.take() {
            x.tx.rollback();
        }
        t.state = TxnState::Aborted;
    }
}

pub fn commit(store: &Store, sess: &mut Session, n: i64) -> Result<Document> {
    let Some(t) = sess.txn.as_mut() else { return Err(no_such_txn(n)) };
    if t.number != n {
        return Err(no_such_txn(n));
    }
    match t.state {
        TxnState::Committed => Ok(doc! {"ok": 1.0}),
        TxnState::Aborted => Err(no_such_txn(n)),
        TxnState::InProgress => {
            let txn = t.txn.take().ok_or_else(|| no_such_txn(n))?;
            match store.commit(txn) {
                Ok(_) => {
                    t.state = TxnState::Committed;
                    Ok(doc! {"ok": 1.0})
                }
                Err(e) => {
                    t.state = TxnState::Aborted;
                    Err(e)
                }
            }
        }
    }
}

pub fn abort(sess: &mut Session, n: i64) -> Result<Document> {
    let Some(t) = sess.txn.as_mut() else { return Err(no_such_txn(n)) };
    if t.number != n {
        return Err(no_such_txn(n));
    }
    match t.state {
        TxnState::Committed => Err(Error::new(256, "Cannot abort a committed transaction")),
        TxnState::Aborted => Err(no_such_txn(n)),
        TxnState::InProgress => {
            if let Some(x) = t.txn.take() {
                x.tx.rollback();
            }
            t.state = TxnState::Aborted;
            Ok(doc! {"ok": 1.0})
        }
    }
}

/// Open multi-document transactions and the `maxConcurrentMultiDocumentTransactions` limit.
static OPEN_TXNS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
pub static MAX_OPEN_TXNS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(10000);

/// One open transaction; released on drop (commit, abort, or session reap).
pub struct OpenTxnSlot(());

impl OpenTxnSlot {
    pub fn acquire() -> Result<OpenTxnSlot> {
        use std::sync::atomic::Ordering::SeqCst;
        let limit = MAX_OPEN_TXNS.load(SeqCst);
        let open = OPEN_TXNS.fetch_add(1, SeqCst);
        if open >= limit {
            OPEN_TXNS.fetch_sub(1, SeqCst);
            return Err(Error::new(509, format!("cannot start a new multi-document transaction; there are already {open} open transactions, which meets or exceeds the limit of {limit}")));
        }
        Ok(OpenTxnSlot(()))
    }
}

impl Drop for OpenTxnSlot {
    fn drop(&mut self) {
        OPEN_TXNS.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
