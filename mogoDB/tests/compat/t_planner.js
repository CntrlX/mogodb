// Planner / explain parity: winning plan shapes, index bounds, sort-providing and covered
// plans, and execution counters. Volatile fields (timings, work units, hashes) are stripped.
const d = fresh('compat_plan');
const c = d.c;
c.insertMany(Array.from({ length: 60 }, (_, i) => ({ _id: i, a: i % 10, b: i, s: 'x' + (i % 7), n: i % 3 === 0 ? null : i, f: i + 0.5, l: NumberLong(i) })));
c.createIndex({ a: 1 });
c.createIndex({ a: 1, b: -1 });
c.createIndex({ s: 1 }, { unique: false });
c.createIndex({ l: -1 });

const VOLATILE = ['executionTimeMillis', 'executionTimeMillisEstimate', 'executionTimeMicros', 'works', 'advanced', 'needTime', 'needYield',
  'saveState', 'restoreState', 'isEOF', 'queryHash', 'planCacheShapeHash', 'planCacheKey', 'queryShapeHash', 'optimizationTimeMillis',
  'optimizationTimeMicros', 'totalDataSizeSorted', 'peakTrackedMemBytes', 'serverInfo', 'serverParameters', 'command', '$clusterTime', 'operationTime', 'rejectedPlans', 'allPlansExecution'];
const strip = (o) => JSON.parse(JSON.stringify(o, (k, v) => (VOLATILE.includes(k) ? undefined : v)));
const W = (q) => strip(q.explain().queryPlanner.winningPlan);
const E = (q) => {
  const e = strip(q.explain('executionStats').executionStats);
  return { n: e.nReturned, keys: e.totalKeysExamined, docs: e.totalDocsExamined, stages: e.executionStages };
};

// access paths
t('eq', () => W(c.find({ a: 3 })));
t('express id', () => W(c.find({ _id: 5 })));
t('id range', () => W(c.find({ _id: { $gt: 40 } })));
t('in', () => W(c.find({ a: { $in: [3, 1, 3] } })));
t('range both', () => W(c.find({ b: { $gte: 10, $lt: 20 } })));
t('compound eq+range', () => W(c.find({ a: 3, b: { $gt: 10 } })));
t('compound eq only prefix', () => W(c.find({ a: 3, s: 'x1' })));
t('desc index range', () => W(c.find({ l: { $gt: NumberLong(50) } })));
t('string eq', () => W(c.find({ s: 'x3' })));
t('regex prefix', () => W(c.find({ s: /^x3/ })));
t('empty interval', () => W(c.find({ a: { $gt: 5, $lt: 2 } })));
t('collscan', () => W(c.find({ f: 3.5 })));
t('collscan empty filter', () => W(c.find({})));
t('ne residual', () => W(c.find({ a: { $gte: 2 }, s: { $ne: 'q' } })));
t('no collection', () => strip(d.nope.find({ a: 1 }).explain().queryPlanner));
t('parsedQuery order', () => strip(c.find({ s: 'x1', a: { $in: [1, 3] }, b: { $lt: 5, $gt: 1 } }).explain().queryPlanner.parsedQuery));
t('hint', () => W(c.find({ a: 3 }).hint({ s: 1 })));
t('hint natural', () => W(c.find({ a: 3 }).hint({ $natural: -1 })));

// sort / skip / limit / projection wrappers
t('sort via index', () => W(c.find({}).sort({ a: 1 }).limit(3)));
t('sort via index reverse', () => W(c.find({ a: { $gt: 5 } }).sort({ a: -1 })));
t('blocking sort', () => W(c.find({ b: { $gt: 40 } }).sort({ s: 1 })));
t('blocking sort skip limit', () => W(c.find({ b: { $gt: 3 } }).sort({ f: 1 }).skip(2).limit(3)));
t('sort limit proj', () => W(c.find({ b: { $gt: 3 } }, { f: 1, _id: 0 }).sort({ f: 1 }).limit(3)));
t('skip below fetch', () => W(c.find({ a: 3 }).sort({ b: -1 }).skip(1).limit(2)));
t('skip above fetch', () => W(c.find({ a: { $gte: 2 }, f: { $ne: 1 } }).sort({ a: 1 }).skip(1).limit(2)));
t('collscan limit', () => W(c.find({ f: 1.5 }).limit(1)));
t('collscan skip', () => W(c.find({}).skip(5)));
t('proj simple', () => W(c.find({ a: 3 }, { s: 0 })));
t('proj default computed', () => W(c.find({ a: 3 }, { x: { $add: ['$b', 1] } })));
t('proj default dotted', () => W(c.find({ a: 3 }, { 'm.x': 1 })));
t('or subplan', () => W(c.find({ $or: [{ f: 1.5 }, { f: 2.5 }] })));

// covered plans: shape and results (types must survive the index round trip)
t('covered', () => W(c.find({ a: 3 }, { _id: 0, a: 1 })));
t('covered compound sort', () => W(c.find({ a: { $gt: 1 } }, { _id: 0, a: 1, b: 1 }).sort({ a: 1, b: -1 })));
t('covered results', () => c.find({ a: 3 }, { _id: 0, a: 1, b: 1 }).toArray());
t('covered long desc', () => c.find({ l: { $gte: NumberLong(55) } }, { _id: 0, l: 1 }).sort({ l: -1 }).toArray());
t('covered string', () => c.find({ s: 'x2' }, { _id: 0, s: 1 }).limit(2).toArray());
t('not covered with _id', () => W(c.find({ a: 3 }, { a: 1 })));

// sort-providing results match blocking-sort results
t('index sort results', () => c.find({ a: { $in: [2, 4] } }, { _id: 1, a: 1, b: 1 }).sort({ a: 1, b: -1 }).toArray());
t('index sort desc results', () => c.find({ a: { $lte: 1 } }, { _id: 1 }).sort({ a: -1, b: 1 }).toArray());
t('limit early stop', () => c.find({}).sort({ a: 1, b: -1 }).limit(4).toArray().map((x) => x._id));

// execution counters
t('stats eq', () => E(c.find({ a: 3 })));
t('stats collscan', () => E(c.find({ f: 3.5 })));
t('stats covered', () => E(c.find({ a: 3 }, { _id: 0, a: 1 })));
t('stats limit via index', () => E(c.find({ a: 3 }).sort({ b: -1 }).skip(1).limit(2)));
t('stats express', () => E(c.find({ _id: 5 })));
t('stats blocking sort', () => E(c.find({ b: { $lt: 10 } }).sort({ f: -1 }).limit(3)));
t('stats residual', () => E(c.find({ a: 3, f: { $gt: 20 } })));

// multikey disables covering and changes bounds
const m = d.m;
m.insertMany([{ _id: 1, t: [1, 2] }, { _id: 2, t: [2, 3] }, { _id: 3, t: 4 }]);
m.createIndex({ t: 1 });
t('multikey eq', () => W(m.find({ t: 2 })));
t('multikey not covered', () => W(m.find({ t: 2 }, { _id: 0, t: 1 })));
t('multikey results', () => m.find({ t: { $gte: 2 } }).sort({ _id: 1 }).toArray());
t('multikey stats', () => E(m.find({ t: { $gte: 2 } })));
