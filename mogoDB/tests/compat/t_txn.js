const d = fresh('compat_txn');
d.createCollection('acct');
d.acct.insertMany([{ _id: 1, bal: 100 }, { _id: 2, bal: 0 }]);

// commit
t('txn commit', () => {
  const s = db.getMongo().startSession();
  const c = s.getDatabase('compat_txn').acct;
  s.startTransaction();
  c.updateOne({ _id: 1 }, { $inc: { bal: -30 } });
  c.updateOne({ _id: 2 }, { $inc: { bal: 30 } });
  const inside = c.find().sort({ _id: 1 }).toArray();
  const outside = d.acct.find().sort({ _id: 1 }).toArray();
  s.commitTransaction();
  const after = d.acct.find().sort({ _id: 1 }).toArray();
  s.endSession();
  return { inside, outside, after };
});

// abort
t('txn abort', () => {
  const s = db.getMongo().startSession();
  const c = s.getDatabase('compat_txn').acct;
  s.startTransaction();
  c.insertOne({ _id: 3, bal: 5 });
  c.deleteOne({ _id: 1 });
  s.abortTransaction();
  s.endSession();
  return d.acct.find().sort({ _id: 1 }).toArray();
});

// error aborts transaction
t('txn dup error', () => {
  const s = db.getMongo().startSession();
  const c = s.getDatabase('compat_txn').acct;
  s.startTransaction();
  c.insertOne({ _id: 4 });
  let err = null;
  try { c.insertOne({ _id: 1 }); } catch (e) { err = e.code; }
  let commitErr = null;
  try { s.commitTransaction(); } catch (e) { commitErr = e.codeName; }
  s.endSession();
  return { err, commitErr, has4: d.acct.countDocuments({ _id: 4 }) };
});

// write conflict between two transactions
t('txn write conflict', () => {
  const s1 = db.getMongo().startSession();
  const s2 = db.getMongo().startSession();
  const c1 = s1.getDatabase('compat_txn').acct;
  const c2 = s2.getDatabase('compat_txn').acct;
  s1.startTransaction();
  s2.startTransaction();
  c1.updateOne({ _id: 2 }, { $inc: { bal: 1 } });
  let e2 = null;
  try { c2.updateOne({ _id: 2 }, { $inc: { bal: 1 } }); s2.commitTransaction(); } catch (e) { e2 = e.errorLabels ? e.errorLabels.includes('TransientTransactionError') : e.code; }
  s1.commitTransaction();
  try { s2.abortTransaction(); } catch (e) {}
  s1.endSession(); s2.endSession();
  return { e2, bal: d.acct.findOne({ _id: 2 }).bal };
});

t('commit without txn', () => {
  const s = db.getMongo().startSession();
  let r;
  try { s.getDatabase('admin').runCommand({ commitTransaction: 1, txnNumber: NumberLong(42), autocommit: false }); r = 'nothrow'; } catch (e) { r = e.codeName; }
  const raw = s.getDatabase('admin').runCommand({ commitTransaction: 1, txnNumber: NumberLong(43), autocommit: false });
  s.endSession();
  return [raw.ok, raw.codeName];
});

// change streams
t('change stream', () => {
  const cs = d.acct.watch([], { fullDocument: 'updateLookup' });
  d.acct.insertOne({ _id: 10, bal: 1 });
  d.acct.updateOne({ _id: 10 }, { $set: { bal: 2 }, $unset: { x: 1 } });
  d.acct.replaceOne({ _id: 10 }, { bal: 3 });
  d.acct.deleteOne({ _id: 10 });
  const ev = [];
  for (let i = 0; i < 20 && ev.length < 4; i++) {
    const e = cs.tryNext();
    if (e) ev.push({ op: e.operationType, key: e.documentKey, full: e.fullDocument, ud: e.updateDescription, ns: e.ns });
  }
  cs.close();
  return ev;
});

t('change stream match', () => {
  const cs = d.acct.watch([{ $match: { operationType: 'insert' } }, { $project: { 'fullDocument._id': 1, operationType: 1 } }]);
  d.acct.insertOne({ _id: 20 });
  d.acct.deleteOne({ _id: 20 });
  d.acct.insertOne({ _id: 21 });
  const ev = [];
  for (let i = 0; i < 20 && ev.length < 2; i++) {
    const e = cs.tryNext();
    if (e) ev.push([e.operationType, e.fullDocument]);
  }
  cs.close();
  return ev;
});

t('change stream resume', () => {
  const cs = d.acct.watch();
  d.acct.insertOne({ _id: 30 });
  let first = null;
  for (let i = 0; i < 20 && !first; i++) first = cs.tryNext();
  cs.close();
  d.acct.insertOne({ _id: 31 });
  const cs2 = d.acct.watch([], { resumeAfter: first._id });
  let nxt = null;
  for (let i = 0; i < 20 && !nxt; i++) nxt = cs2.tryNext();
  cs2.close();
  return nxt.documentKey;
});

t('change stream drop', () => {
  d.tmp.insertOne({ _id: 1 });
  const cs = d.tmp.watch();
  d.tmp.drop();
  const ev = [];
  for (let i = 0; i < 20 && ev.length < 2; i++) {
    const e = cs.tryNext();
    if (e) ev.push(e.operationType);
  }
  return ev;
});

t('db change stream', () => {
  const cs = d.watch();
  d.other.insertOne({ _id: 1 });
  let e = null;
  for (let i = 0; i < 20 && !e; i++) e = cs.tryNext();
  cs.close();
  return [e.operationType, e.ns];
});
