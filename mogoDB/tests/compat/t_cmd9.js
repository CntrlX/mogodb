// Differential tests: setQuerySettings / removeQuerySettings / $querySettings / $listClusterCatalog.
// Query-shape hashes are normalised to 'H' because they are server-computed; the hash format is checked separately.

const d = fresh('cmd92_qs');
const adm = db.getSiblingDB('admin');
// Query settings are cluster-global: start from none so leftovers don't leak into results.
for (const e of adm.aggregate([{ $querySettings: {} }]).toArray()) adm.runCommand({ removeQuerySettings: e.queryShapeHash });

// Replace queryShapeHash values with 'H' so output is comparable across servers.
function hs(v) {
  if (Array.isArray(v)) return v.map(hs);
  if (v && typeof v === 'object' && v.constructor && v.constructor.name === 'Object') {
    const o = {};
    for (const k of Object.keys(v)) o[k] = k === 'queryShapeHash' ? 'H' : hs(v[k]);
    return o;
  }
  return v;
}

// Drop per-server collection UUIDs from listing output.
function cc(v) {
  return v.map((doc) => {
    const o = Object.assign({}, doc);
    if (o.info && typeof o.info === 'object') o.info = { readOnly: o.info.readOnly };
    return o;
  });
}

function clean(rq) {
  try {
    adm.runCommand({ removeQuerySettings: rq });
  } catch (e) {
    // ignore: cleanup only
  }
}

const RQ_FIND = { find: 'c1', filter: { a: 1 }, $db: 'cmd92_qs' };
const RQ_FIND2 = { find: 'c2', filter: {}, $db: 'cmd92_qs' };
const RQ_AGG = { aggregate: 'c1', pipeline: [{ $match: { a: 1 } }], cursor: {}, $db: 'cmd92_qs' };
const RQ_DIST = { distinct: 'c1', key: 'a', query: { a: 1 }, $db: 'cmd92_qs' };
const RQ_EMPTY = { find: 'c3', filter: {}, $db: 'cmd92_qs' };
const ALL_RQ = [RQ_FIND, RQ_FIND2, RQ_AGG, RQ_DIST, RQ_EMPTY];
ALL_RQ.forEach(clean);

// Collections used by $listClusterCatalog and debug shapes.
d.c1.insertMany([{ _id: 1, a: 1 }, { _id: 2, a: 2 }]);
d.c2.insertOne({ _id: 1 });
d.createView('v1', 'c1', [{ $match: {} }]);

// --- creation and merge ---
const r1 = adm.runCommand({ setQuerySettings: RQ_FIND, settings: { reject: true, maxTimeMS: 500, comment: 'c' } });
t('set creation', () => hs(r1));
t('set creation keys', () => Object.keys(r1).sort());
t('hash format', () => ({ len: r1.queryShapeHash.length, hex: /^[0-9A-F]+$/.test(r1.queryShapeHash) }));
t('set merge by rq', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND, settings: { queryFramework: 'sbe' } })));
t('set merge keys', () => Object.keys(adm.runCommand({ setQuerySettings: RQ_FIND, settings: { reject: false } })).sort());
t('set by hash default removes', () => hs(adm.runCommand({ setQuerySettings: r1.queryShapeHash, settings: { maxTimeMS: 0 } })));
t('set by hash keys', () => Object.keys(adm.runCommand({ setQuerySettings: r1.queryShapeHash, settings: { comment: 'd' } })).sort());
t('set by hash lowercase', () => hs(adm.runCommand({ setQuerySettings: r1.queryShapeHash.toLowerCase(), settings: { comment: 'e' } })));

// --- settings validation ---
t('empty settings', () => hs(adm.runCommand({ setQuerySettings: RQ_EMPTY, settings: {} })));
t('defaults only', () => hs(adm.runCommand({ setQuerySettings: RQ_EMPTY, settings: { reject: false, maxTimeMS: 0 } })));
t('unknown field only', () => hs(adm.runCommand({ setQuerySettings: RQ_EMPTY, settings: { zz: 1 } })));
t('unknown field ignored', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { zz: 1, reject: true } })));
t('framework classic', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { queryFramework: 'classic' } })));
t('framework bogus', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { queryFramework: 'bogus' } })));
t('maxTimeMS negative', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { maxTimeMS: -1 } })));
t('maxTimeMS string', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { maxTimeMS: 'x' } })));
t('reject string', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { reject: 'string' } })));
t('comment object', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { comment: { a: [1, 'x'] } } })));
t('comment null', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { comment: null } })));
t('queryKnob not settable', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { queryKnobs: { planCacheShapeHashStringifiedFormat: true } } })));

// --- indexHints ---
t('indexHints object', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { indexHints: { ns: { db: 'cmd92_qs', coll: 'c2' }, allowedIndexes: ['a_1'] } } })));
t('indexHints array', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { indexHints: [{ ns: { db: 'cmd92_qs', coll: 'c2' }, allowedIndexes: [{ _id: 1 }, 'x_1'] }] } })));
t('indexHints string', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { indexHints: 'x' } })));
t('indexHints elem non-doc', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { indexHints: [5] } })));
t('indexHints no ns', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { indexHints: { allowedIndexes: ['a_1'] } } })));
t('indexHints ns no coll', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { indexHints: { ns: { db: 'cmd92_qs' }, allowedIndexes: ['a_1'] } } })));
t('indexHints no allowed', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { indexHints: { ns: { db: 'cmd92_qs', coll: 'c2' } } } })));
t('indexHints allowed string', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { indexHints: { ns: { db: 'cmd92_qs', coll: 'c2' }, allowedIndexes: 'a_1' } } })));
t('indexHints allowed int', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND2, settings: { indexHints: { ns: { db: 'cmd92_qs', coll: 'c2' }, allowedIndexes: [1] } } })));

// --- hash inputs ---
t('hash not hex', () => hs(adm.runCommand({ setQuerySettings: 'zz', settings: { reject: true } })));
t('hash short', () => hs(adm.runCommand({ setQuerySettings: 'ABCD', settings: { reject: true } })));
t('hash unknown comment only', () => hs(adm.runCommand({ setQuerySettings: '0'.repeat(64), settings: { comment: 'x' } })));
t('remove hash not hex', () => hs(adm.runCommand({ removeQuerySettings: 'zz' })));
t('set int arg', () => hs(adm.runCommand({ setQuerySettings: 5, settings: { reject: true } })));
t('remove int arg', () => hs(adm.runCommand({ removeQuerySettings: 5 })));

// --- representativeQuery errors ---
t('rq missing body', () => hs(adm.runCommand({ setQuerySettings: {}, settings: { reject: true } })));
t('rq unknown field', () => hs(adm.runCommand({ setQuerySettings: { find: 'c1', filter: {}, zz: 1, $db: 'cmd92_qs' }, settings: { reject: true } })));
t('rq query int', () => hs(adm.runCommand({ setQuerySettings: 5, settings: { reject: true } })));
t('rq insert', () => hs(adm.runCommand({ setQuerySettings: { insert: 'c1', documents: [{ a: 1 }], $db: 'cmd92_qs' }, settings: { reject: true } })));
t('rq no db', () => hs(adm.runCommand({ setQuerySettings: { find: 'c1', filter: {} }, settings: { reject: true } })));
t('rq non-admin db', () => hs(d.runCommand({ setQuerySettings: RQ_FIND, settings: { reject: true } })));
t('rq no settings', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND })));
t('rq settings non-doc', () => hs(adm.runCommand({ setQuerySettings: RQ_FIND, settings: 5 })));

// --- other query shapes ---
t('set aggregate', () => hs(adm.runCommand({ setQuerySettings: RQ_AGG, settings: { reject: true } })));
t('set distinct', () => hs(adm.runCommand({ setQuerySettings: RQ_DIST, settings: { comment: 'dist' } })));

// --- removeQuerySettings ---
t('remove non-admin', () => hs(d.runCommand({ removeQuerySettings: RQ_FIND })));
t('remove by rq', () => hs(adm.runCommand({ removeQuerySettings: RQ_FIND2 })));
t('remove by rq missing', () => hs(adm.runCommand({ removeQuerySettings: RQ_EMPTY })));
t('remove by hash', () => hs(adm.runCommand({ removeQuerySettings: r1.queryShapeHash })));
t('remove by hash again', () => hs(adm.runCommand({ removeQuerySettings: r1.queryShapeHash })));
t('remove keys', () => Object.keys(adm.runCommand({ removeQuerySettings: RQ_AGG })).sort());

// --- $querySettings stage ---
const qsRows = (stage) => sorted(hs(adm.aggregate([stage, { $match: { 'debugQueryShape.cmdNs.db': 'cmd92_qs' } }]).toArray()));
t('qs stage debug', () => qsRows({ $querySettings: { showDebugQueryShape: true } }));
t('qs stage no debug keys', () => sorted(adm.aggregate([{ $querySettings: {} }, { $match: { 'representativeQuery.$db': 'cmd92_qs' } }]).toArray().map((x) => Object.keys(x).sort())));
t('qs stage non-doc', () => adm.aggregate([{ $querySettings: 'x' }]).toArray());
t('qs stage debug non-bool', () => adm.aggregate([{ $querySettings: { showDebugQueryShape: 'yes' } }]).toArray());
t('qs stage unknown', () => adm.aggregate([{ $querySettings: { zz: 1 } }]).toArray());
t('qs stage not first', () => adm.aggregate([{ $match: {} }, { $querySettings: {} }]).toArray());

// --- $listClusterCatalog stage ---
t('catalog db', () => cc(sorted(hs(d.aggregate([{ $listClusterCatalog: {} }]).toArray()))));
t('catalog shards', () => cc(sorted(hs(d.aggregate([{ $listClusterCatalog: { shards: true } }]).toArray()))));
t('catalog non-doc', () => d.aggregate([{ $listClusterCatalog: 'x' }]).toArray());
t('catalog unknown', () => d.aggregate([{ $listClusterCatalog: { zz: 1 } }]).toArray());
t('catalog shards non-bool', () => d.aggregate([{ $listClusterCatalog: { shards: 'x' } }]).toArray());
t('catalog not first', () => d.aggregate([{ $match: {} }, { $listClusterCatalog: {} }]).toArray());

// --- cleanup ---
ALL_RQ.forEach(clean);
clean(r1.queryShapeHash);
d.dropDatabase();
