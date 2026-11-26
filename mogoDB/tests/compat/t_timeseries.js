// Time series collections (MongoDB 9.0). Document field order of read-backs is hash-based
// in mongod (it changes across restarts), so documents are compared with sorted keys.
const d = fresh('compat_ts');
const sk = (o) => (o && typeof o === 'object' && !Array.isArray(o) && !(o instanceof Date) && o.constructor && o.constructor.name === 'Object'
  ? Object.keys(o).sort().reduce((a, k) => ((a[k] = sk(o[k])), a), {}) : Array.isArray(o) ? o.map(sk) : o);
const em = (label, fn) => {
  try { show(label, fn()); } catch (e) { print(label + ' => ERR ' + e.code + ' ' + (e.errmsg || e.message)); }
};
const T = (h) => ISODate('2024-01-01T' + String(h).padStart(2, '0') + ':00:00Z');

// creation and options
em('create', () => d.createCollection('w', { timeseries: { timeField: 'ts', metaField: 'm', granularity: 'hours' }, expireAfterSeconds: 3600 }));
em('create min', () => d.createCollection('w2', { timeseries: { timeField: 't' } }));
em('create spans', () => d.createCollection('w3', { timeseries: { timeField: 't', bucketMaxSpanSeconds: 100, bucketRoundingSeconds: 100 } }));
t('info', () => d.getCollectionInfos({ name: 'w' }).map((c) => ({ name: c.name, type: c.type, options: c.options, ro: c.info.readOnly, idIndex: c.idIndex })));
t('info min', () => d.getCollectionInfos({ name: 'w2' })[0].options);
t('info spans', () => d.getCollectionInfos({ name: 'w3' })[0].options);
t('nameOnly', () => d.runCommand({ listCollections: 1, nameOnly: true, filter: { type: 'timeseries' } }).cursor.firstBatch.sort((a, b) => (a.name < b.name ? -1 : 1)));
t('indexes meta', () => d.w.getIndexes());
t('indexes none', () => d.w2.getIndexes());
em('exists same', () => d.createCollection('w', { timeseries: { timeField: 'ts', metaField: 'm', granularity: 'hours' }, expireAfterSeconds: 3600 }));
em('exists diff', () => d.createCollection('w2', { timeseries: { timeField: 'tt' } }));
em('gran days', () => d.createCollection('x1', { timeseries: { timeField: 't', granularity: 'days' } }));
em('meta eq time', () => d.createCollection('x2', { timeseries: { timeField: 't', metaField: 't' } }));
em('dollar time', () => d.createCollection('x3', { timeseries: { timeField: '$t' } }));
em('missing timeField', () => d.createCollection('x4', { timeseries: { metaField: 'm' } }));
em('spans unequal', () => d.createCollection('x5', { timeseries: { timeField: 't', bucketMaxSpanSeconds: 100, bucketRoundingSeconds: 50 } }));
em('only rounding', () => d.createCollection('x5b', { timeseries: { timeField: 't', bucketRoundingSeconds: 60 } }));
em('gran plus span', () => d.createCollection('x6', { timeseries: { timeField: 't', granularity: 'hours', bucketMaxSpanSeconds: 100, bucketRoundingSeconds: 100 } }));
em('capped', () => d.createCollection('x7', { timeseries: { timeField: 't' }, capped: true, size: 1000 }));
em('dotted time', () => d.createCollection('x8', { timeseries: { timeField: 'a.b' } }));
em('dotted meta', () => d.createCollection('x8b', { timeseries: { timeField: 't', metaField: 'a.b' } }));
em('unknown field', () => d.createCollection('x9', { timeseries: { timeField: 't', foo: 1 } }));
em('view on', () => d.createCollection('xa', { timeseries: { timeField: 't' }, viewOn: 'w' }));
em('expire string', () => d.createCollection('xb', { timeseries: { timeField: 't' }, expireAfterSeconds: 'x' }));
em('expire negative', () => d.createCollection('xc', { timeseries: { timeField: 't' }, expireAfterSeconds: -5 }));

// writes
em('insert', () => d.w.insertMany([
  { _id: 1, ts: T(0), m: { s: 'a' }, v: 1 },
  { _id: 2, ts: T(1), m: { s: 'b' }, v: 2 },
  { _id: 3, ts: T(2), m: { s: 'a' }, v: 3, extra: 'x' },
]));
em('dup _id allowed', () => d.w.insertOne({ _id: 1, ts: T(3), m: { s: 'z' }, v: 9 }));
em('missing time', () => d.w.insertOne({ v: 9 }));
em('bad time', () => d.w.insertOne({ ts: 'nope' }));
em('null time', () => d.w.insertOne({ ts: null }));
t('find', () => sk(d.w.find().sort({ ts: 1 }).toArray()));
t('find meta', () => sk(d.w.find({ 'm.s': 'a' }, { _id: 1, v: 1 }).sort({ ts: 1 }).toArray()));
t('no id', () => { d.w2.insertOne({ t: T(5), x: 1 }); return sk(d.w2.find({}, { _id: 0 }).toArray()); });
t('count', () => d.w.countDocuments({ v: { $gte: 2 } }));
t('agg', () => d.w.aggregate([{ $group: { _id: '$m.s', s: { $sum: '$v' } } }, { $sort: { _id: 1 } }]).toArray());
t('cursor ns', () => d.runCommand({ find: 'w', filter: {} }).cursor.ns);
t('agg cursor ns', () => d.runCommand({ aggregate: 'w', pipeline: [], cursor: {} }).cursor.ns);
t('listIndexes ns', () => d.runCommand({ listIndexes: 'w' }).cursor.ns);

// update / delete rules
em('updateOne measurement', () => d.w.updateOne({ _id: 2 }, { $set: { v: 20 } }));
em('updateMany measurement filter', () => d.w.updateMany({ _id: 2 }, { $set: { v: 20 } }));
em('updateMany meta sets measurement', () => d.w.updateMany({ 'm.s': 'a' }, { $set: { v: 5 } }));
em('updateMany pipeline', () => d.w.updateMany({ 'm.s': 'a' }, [{ $set: { 'm.q': 1 } }]));
em('updateMany meta', () => d.w.updateMany({ 'm.s': 'a' }, { $set: { 'm.s': 'c' } }));
em('updateMany rename meta', () => d.w.updateMany({ 'm.s': 'b' }, { $rename: { 'm.s': 'm.r' } }));
em('updateMany empty filter', () => d.w.updateMany({}, { $set: { 'm.k': 1 } }));
em('replaceOne', () => d.w.replaceOne({ _id: 3 }, { ts: T(2), m: 3 }));
em('upsert one', () => d.w.updateOne({ _id: 99 }, { $set: { m: 5 } }, { upsert: true }));
em('upsert many', () => d.w.updateMany({ m: 5 }, { $set: { m: 6 } }, { upsert: true }));
em('no meta update', () => d.w2.updateMany({}, { $set: { x: 3 } }));
em('fam', () => d.runCommand({ findAndModify: 'w', query: {}, update: { $set: { 'm.k': 2 } } }));
t('after updates', () => sk(d.w.find().sort({ ts: 1 }).toArray()));
em('deleteOne measurement', () => d.w.deleteOne({ v: 3 }));
em('deleteMany meta', () => d.w.deleteMany({ 'm.s': 'z' }));
t('after delete', () => sk(d.w.find().sort({ ts: 1 }).toArray()));

// indexes
em('index measurement', () => d.w.createIndex({ v: 1 }));
em('index partial', () => d.w.createIndex({ v: -1 }, { partialFilterExpression: { v: { $gt: 1 } } }));
em('index unique', () => d.w.createIndex({ v: 1 }, { unique: true, name: 'u' }));
em('index sparse', () => d.w.createIndex({ v: 1 }, { sparse: true, name: 'sp' }));
em('index ttl', () => d.w.createIndex({ ts: 1 }, { expireAfterSeconds: 10 }));
em('index text', () => d.w.createIndex({ s: 'text' }));
em('index _id_ name', () => d.w.createIndex({ a: 1 }, { name: '_id_' }));
em('hint _id_', () => d.w.find().hint('_id_').toArray().length);
t('index list', () => d.w.getIndexes().map((i) => [i.name, i.key]));

// collMod
em('collMod gran up', () => d.runCommand({ collMod: 'w2', timeseries: { granularity: 'minutes' } }).ok);
t('collMod gran info', () => d.getCollectionInfos({ name: 'w2' })[0].options);
em('collMod gran down', () => d.runCommand({ collMod: 'w2', timeseries: { granularity: 'seconds' } }));
em('collMod gran same', () => d.runCommand({ collMod: 'w2', timeseries: { granularity: 'minutes' } }).ok);
em('collMod spans', () => d.runCommand({ collMod: 'w2', timeseries: { bucketMaxSpanSeconds: 200000, bucketRoundingSeconds: 200000 } }).ok);
t('collMod spans info', () => d.getCollectionInfos({ name: 'w2' })[0].options);
em('collMod expire', () => d.runCommand({ collMod: 'w2', expireAfterSeconds: 60 }).ok);
t('collMod expire info', () => d.getCollectionInfos({ name: 'w2' })[0].options);
em('collMod expire off', () => d.runCommand({ collMod: 'w2', expireAfterSeconds: 'off' }).ok);
t('collMod expire off info', () => d.getCollectionInfos({ name: 'w2' })[0].options);

// misc
t('stats', () => { const s = d.runCommand({ collStats: 'w' }); return [s.ok, s.ns, s.timeseries.bucketsNs, s.count === undefined]; });
em('rename', () => d.w3.renameCollection('w3r').ok);
t('names', () => d.getCollectionNames().sort());
em('drop', () => d.w3r.drop());
t('names after drop', () => d.getCollectionNames().sort());
