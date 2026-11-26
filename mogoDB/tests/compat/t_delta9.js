// MongoDB 8.0-9.0 behaviour changes not covered by the feature-specific files.
const d = fresh('compat_delta9');
const em = (label, fn) => {
  try { show(label, fn()); } catch (e) { print(label + ' => ERR ' + e.code + ' ' + (e.errmsg || e.message)); }
};

// [8.0] {field: null} no longer matches undefined
d.u.insertMany([{ _id: 1, a: null }, { _id: 2 }, { _id: 3, a: [null] }, { _id: 4, a: 1 }]);
d.runCommand({ insert: 'u', documents: [EJSON.deserialize({ _id: 5, a: { $undefined: true } }), EJSON.deserialize({ _id: 6, a: [{ $undefined: true }] })] });
t('undefined stored', () => d.u.find({ _id: { $in: [5, 6] } }).sort({ _id: 1 }).toArray().map((x) => typeof x.a));
t('null eq', () => d.u.find({ a: null }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('null in', () => d.u.find({ a: { $in: [null] } }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('null ne', () => d.u.find({ a: { $ne: null } }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('null nin', () => d.u.find({ a: { $nin: [null] } }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('null expr eq', () => d.u.find({ $expr: { $eq: ['$a', null] } }, { _id: 1 }).sort({ _id: 1 }).toArray());
d.u.createIndex({ a: 1 });
t('null eq indexed', () => d.u.find({ a: null }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('null in indexed', () => d.u.find({ a: { $in: [null, 1] } }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('lookup null', () => d.u.aggregate([{ $match: { _id: 2 } }, { $lookup: { from: 'u', localField: 'a', foreignField: 'a', as: 'j' } }, { $project: { j: '$j._id' } }]).toArray());

// [9.0] $group empty field name
d.g.insertMany([{ _id: 1, k: 'a', v: 1 }, { _id: 2, k: 'b', v: 2 }]);
em('group empty name', () => d.g.aggregate([{ $group: { _id: '$k', '': { $sum: '$v' } } }]).toArray());

// [8.0] $rank/$denseRank null == missing
d.r.insertMany([{ _id: 1, s: null }, { _id: 2 }, { _id: 3, s: 1 }, { _id: 4, s: null }, { _id: 5, s: 2 }]);
t('rank nulls', () => d.r.aggregate([{ $setWindowFields: { sortBy: { s: 1 }, output: { r: { $rank: {} }, dr: { $denseRank: {} } } } }, { $sort: { _id: 1 } }]).toArray());

// [8.3] $setWindowFields $mergeObjects
d.w.insertMany([{ _id: 1, g: 1, o: { a: 1 } }, { _id: 2, g: 1, o: { b: 2 } }, { _id: 3, g: 2, o: { a: 3 } }]);
t('window mergeObjects', () => d.w.aggregate([{ $setWindowFields: { partitionBy: '$g', sortBy: { _id: 1 }, output: { m: { $mergeObjects: '$o', window: { documents: ['unbounded', 'current'] } } } } }, { $sort: { _id: 1 } }]).toArray());

// [8.1] $densify field prefix of partitionByFields
d.ds.insertMany([{ t: { h: 1 }, v: 1 }]);
em('densify prefix', () => d.ds.aggregate([{ $densify: { field: 't', partitionByFields: ['t.h'], range: { step: 1, bounds: 'full' } } }]).toArray());

// [8.3] BSONObjectTooLarge on upsert
const big = 'x'.repeat(16 * 1024 * 1024 - 100);
em('upsert too large', () => d.big.updateOne({ _id: 1 }, { $set: { a: big, b: big } }, { upsert: true }));

// [9.0] 2dsphere key errors
d.geo.createIndex({ loc: '2dsphere' });
em('2dsphere bad', () => d.geo.insertOne({ _id: 1, loc: { type: 'Point', coordinates: [500, 0] } }));
t('2dsphere bad raw', () => {
  const r = d.runCommand({ insert: 'geo', documents: [{ _id: 2, loc: { type: 'Point', coordinates: [500, 0] } }] });
  return r.writeErrors.map((e) => ({ code: e.code, codeName: e.codeName, errInfo: e.errInfo }));
});

// [9.0] parameters
t('param maxTxn', () => d.adminCommand({ getParameter: 1, maxConcurrentMultiDocumentTransactions: 1 }).maxConcurrentMultiDocumentTransactions);
t('param mem op', () => d.adminCommand({ getParameter: 1, internalQueryMaxMemoryUsageBytesPerOperation: 1 }).ok);
t('param single expr', () => d.adminCommand({ getParameter: 1, internalQueryMaxSingleExpressionMemoryUsageBytes: 1 }).internalQueryMaxSingleExpressionMemoryUsageBytes);
t('param qs rate', () => d.adminCommand({ getParameter: 1, internalQueryStatsSampleRate: 1 }).internalQueryStatsSampleRate);
t('param qs write rate', () => d.adminCommand({ getParameter: 1, internalQueryStatsWriteCmdSampleRate: 1 }).internalQueryStatsWriteCmdSampleRate);
t('param removed', () => d.adminCommand({ getParameter: 1, storeFindAndModifyImagesInSideCollection: 1 }).ok);
t('param upsert retries', () => d.adminCommand({ getParameter: 1, upsertMaxRetryAttemptsOnDuplicateKeyError: 1 }).upsertMaxRetryAttemptsOnDuplicateKeyError);
t('param autoCompaction', () => d.adminCommand({ getParameter: 1, enableAutoCompaction: 1 }).ok);

// [8.2] $currentDate-like: $$NOW in aggregate, $currentDate in pipeline updates
t('now type', () => d.g.aggregate([{ $limit: 1 }, { $project: { _id: 0, t: { $type: '$$NOW' } } }]).toArray());

// [8.0] update with sort
d.s.insertMany([{ _id: 1, k: 1, v: 3 }, { _id: 2, k: 1, v: 1 }, { _id: 3, k: 1, v: 2 }]);
t('updateOne sort', () => { d.s.updateOne({ k: 1 }, { $set: { hit: true } }, { sort: { v: 1 } }); return d.s.find({ hit: true }, { _id: 1 }).toArray(); });
t('replaceOne sort', () => { d.s.replaceOne({ k: 1 }, { k: 1, v: 9, rep: true }, { sort: { v: -1 } }); return d.s.find({ rep: true }, { _id: 1 }).toArray(); });
em('updateMany sort', () => d.runCommand({ update: 's', updates: [{ q: {}, u: { $set: { z: 1 } }, multi: true, sort: { v: 1 } }] }));

// [8.0] validate / compact
t('validate keys', () => Object.keys(d.runCommand({ validate: 's' })).filter((k) => !['$clusterTime', 'operationTime', 'uuid'].includes(k)).sort());
t('validate indexDetails', () => d.runCommand({ validate: 's' }).indexDetails);
em('compact dryRun', () => { const r = d.runCommand({ compact: 's', dryRun: true }); return [r.ok, typeof r.estimatedBytesFreed]; });
em('autoCompact', () => d.adminCommand({ autoCompact: false }).ok);

// [8.2] change streams: expanded fields only with showExpandedEvents
t('cs expanded', () => {
  const cs = d.cs.watch([], {});
  d.cs.insertOne({ _id: 1, a: { b: 1 } });
  d.cs.updateOne({ _id: 1 }, { $set: { 'a.b': 2 } });
  const out = [];
  while (out.length < 2) { if (cs.hasNext()) { const e = cs.next(); out.push([e.operationType, 'collectionUUID' in e, e.updateDescription ? Object.keys(e.updateDescription).sort() : null]); } }
  cs.close();
  const cs2 = d.cs.watch([], { showExpandedEvents: true });
  d.cs.updateOne({ _id: 1 }, { $set: { 'a.b': 3 } });
  while (out.length < 3) { if (cs2.hasNext()) { const e = cs2.next(); out.push([e.operationType, 'collectionUUID' in e, Object.keys(e.updateDescription).sort()]); } }
  cs2.close();
  return out;
});

// [8.1] connectionStatus authInfo
t('connectionStatus keys', () => Object.keys(d.runCommand({ connectionStatus: 1 }).authInfo).sort());

// [9.0] maxConcurrentMultiDocumentTransactions
em('txn cap', () => {
  d.adminCommand({ setParameter: 1, maxConcurrentMultiDocumentTransactions: 1 });
  const s1 = db.getMongo().startSession(); const s2 = db.getMongo().startSession();
  try {
    s1.startTransaction(); s1.getDatabase(d.getName()).tx.insertOne({ _id: 'a' });
    s2.startTransaction(); s2.getDatabase(d.getName()).tx.insertOne({ _id: 'b' });
    return 'no error';
  } finally {
    try { s2.abortTransaction(); } catch (e) {}
    try { s1.abortTransaction(); } catch (e) {}
    d.adminCommand({ setParameter: 1, maxConcurrentMultiDocumentTransactions: 10000 });
  }
});
em('txn after cap reset', () => {
  const s = db.getMongo().startSession(); s.startTransaction(); s.getDatabase(d.getName()).tx.insertOne({ _id: 'c' }); s.commitTransaction(); return d.tx.countDocuments({});
});

// [9.0] per-operation memory limit
d.pm.insertMany(Array.from({ length: 2000 }, (_, i) => ({ _id: i, s: 'x'.repeat(200) + i })));
em('per-op group', () => {
  d.adminCommand({ setParameter: 1, internalQueryMaxMemoryUsageBytesPerOperation: 100000 });
  try { return d.pm.aggregate([{ $group: { _id: '$s', n: { $sum: 1 } } }], { allowDiskUse: false }).toArray().length; }
  finally { d.adminCommand({ setParameter: 1, internalQueryMaxMemoryUsageBytesPerOperation: 1073741824 }); }
});
em('per-op group disk', () => {
  d.adminCommand({ setParameter: 1, internalQueryMaxMemoryUsageBytesPerOperation: 100000 });
  try { return d.pm.aggregate([{ $group: { _id: '$s', n: { $sum: 1 } } }], { allowDiskUse: true }).toArray().length; }
  finally { d.adminCommand({ setParameter: 1, internalQueryMaxMemoryUsageBytesPerOperation: 1073741824 }); }
});
