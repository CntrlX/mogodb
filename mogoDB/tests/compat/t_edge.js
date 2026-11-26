const d = fresh('compat_edge');
const c = d.e;
c.insertMany([
  { _id: 1, a: [{ b: [1, 2] }, { b: [3] }], n: NumberInt(1), s: 'A', m: { x: { y: 1 } } },
  { _id: 2, a: [{ b: 5 }, { c: 1 }], n: NumberLong(1), s: 'b', m: { x: [{ y: 2 }, { y: 3 }] } },
  { _id: 3, a: { b: [7, 8] }, n: 1.0, s: 'B', m: [] },
  { _id: 4, a: [], n: NumberDecimal('1.0'), s: 'a', m: null },
  { _id: 5, a: [[{ b: 9 }]], n: -0.0, s: '', m: { x: null } },
  { _id: 6, n: NaN, s: 'aa', t: true },
  { _id: 7, n: Infinity, s: 'Z', t: false, d: ISODate('2020-01-01') },
  { _id: 8, n: MinKey(), s: null },
  { _id: 9, n: MaxKey(), s: ObjectId('507f1f77bcf86cd799439011') },
]);
const ids = (q, p) => c.find(q, { _id: 1 }).sort({ _id: 1 }).toArray().map((x) => x._id);
t('nested array path', () => ids({ 'a.b': 3 }));
t('nested array path gt', () => ids({ 'a.b': { $gt: 4 } }));
t('nested array of arrays', () => ids({ 'a.b': 9 }));
t('path null missing', () => ids({ 'a.b': null }));
t('path exists', () => ids({ 'a.b': { $exists: true } }));
t('elemMatch nested', () => ids({ a: { $elemMatch: { b: { $gte: 2 } } } }));
t('all elemMatch', () => ids({ a: { $all: [{ $elemMatch: { b: 5 } }, { $elemMatch: { c: 1 } }] } }));
t('numeric eq types', () => ids({ n: 1 }));
t('nan eq', () => ids({ n: NaN }));
t('nan gt', () => ids({ n: { $gt: 0 } }));
t('neg zero', () => ids({ n: 0 }));
t('minkey', () => ids({ n: MinKey() }));
t('lt maxkey', () => ids({ n: { $lt: MaxKey() } }));
t('type bool', () => ids({ t: { $type: 'bool' } }));
t('type multi', () => ids({ s: { $type: ['null', 'objectId'] } }));
t('type num 2', () => ids({ s: { $type: 2 } }));
t('regex empty', () => ids({ s: /^$/ }));
t('regex on non-string', () => ids({ n: /1/ }));
t('m.x.y', () => ids({ 'm.x.y': { $gte: 2 } }));
t('m.x null', () => ids({ 'm.x': null }));
t('m.x.y null', () => ids({ 'm.x.y': null }));
t('size 0', () => ids({ a: { $size: 0 } }));
t('not gt', () => ids({ n: { $not: { $gt: 0 } } }));
t('in empty', () => ids({ n: { $in: [] } }));
t('nin empty', () => ids({ n: { $nin: [] } }));
t('eq doc', () => ids({ m: { x: { y: 1 } } }));
t('eq doc order', () => ids({ m: { x: { y: 1, z: 1 } } }));
t('bitsAllSet', () => { d.bits.insertMany([{ _id: 1, v: 54 }, { _id: 2, v: 20 }, { _id: 3, v: 4 }]); return d.bits.find({ v: { $bitsAllSet: [1, 5] } }).toArray(); });
t('bitsAnyClear', () => d.bits.find({ v: { $bitsAnyClear: 35 } }, { _id: 1 }).toArray());
t('sort mixed types', () => c.find({}, { _id: 1 }).sort({ n: 1, _id: 1 }).toArray().map((x) => x._id));
t('sort mixed desc', () => c.find({}, { _id: 1 }).sort({ s: -1, _id: 1 }).toArray().map((x) => x._id));
t('sort nested arr', () => c.find({}, { _id: 1 }).sort({ 'a.b': 1, _id: 1 }).toArray().map((x) => x._id));
t('sort nested arr desc', () => c.find({}, { _id: 1 }).sort({ 'a.b': -1, _id: 1 }).toArray().map((x) => x._id));
t('distinct nested', () => c.distinct('a.b'));
t('distinct mixed', () => c.distinct('n').length);

// update edge cases
const u = d.u;
u.insertOne({ _id: 1, a: { b: 1 }, arr: [1, 2, 3, 4, 5], objs: [{ k: 1, v: [1, 2] }, { k: 2, v: [3] }], num: NumberInt(5) });
t('set array index pad', () => { u.updateOne({ _id: 1 }, { $set: { 'arr.7': 8 } }); return u.findOne({ _id: 1 }).arr; });
t('set in array elem doc', () => { u.updateOne({ _id: 1 }, { $set: { 'objs.1.z': 'zz' } }); return u.findOne({ _id: 1 }).objs; });
t('nested positional all', () => { u.updateOne({ _id: 1 }, { $inc: { 'objs.$[].v.$[]': 10 } }); return u.findOne({ _id: 1 }).objs; });
t('arrayFilters nested', () => { u.updateOne({ _id: 1 }, { $set: { 'objs.$[o].v.$[x]': 0 } }, { arrayFilters: [{ 'o.k': 1 }, { x: { $gt: 11 } }] }); return u.findOne({ _id: 1 }).objs; });
t('inc int overflow', () => { u.updateOne({ _id: 1 }, { $inc: { num: 2147483647 } }); return u.findOne({ _id: 1 }).num; });
t('mul types', () => { u.updateOne({ _id: 1 }, { $mul: { 'a.b': 2.5 } }); return u.findOne({ _id: 1 }).a; });
t('set dotted on scalar err', () => u.updateOne({ _id: 1 }, { $set: { 'a.b.c': 1 } }));
t('unset nested', () => { u.updateOne({ _id: 1 }, { $unset: { 'a.b': 1 } }); return u.findOne({ _id: 1 }).a; });
t('new field order', () => { u.updateOne({ _id: 1 }, { $set: { zeta: 1, alpha: 2, 'nested.y': 1, 'nested.b': 2 } }); return Object.keys(u.findOne({ _id: 1 })); });
t('push to missing', () => { u.updateOne({ _id: 1 }, { $push: { fresh: 1 } }); return u.findOne({ _id: 1 }).fresh; });
t('push non-array err', () => u.updateOne({ _id: 1 }, { $push: { num: 1 } }));
t('addToSet doc', () => { u.updateOne({ _id: 1 }, { $addToSet: { objs: { k: 2, v: [13], z: 'zz' } } }); return u.findOne({ _id: 1 }).objs.length; });
t('pull nested cond', () => { u.updateOne({ _id: 1 }, { $pull: { objs: { v: 0 } } }); return u.findOne({ _id: 1 }).objs; });
t('rename nested', () => { u.updateOne({ _id: 1 }, { $rename: { 'nested.y': 'nested.yy' } }); return u.findOne({ _id: 1 }).nested; });
t('positional no match err', () => u.updateOne({ _id: 1 }, { $set: { 'arr.$': 1 } }));
t('max date', () => { u.updateOne({ _id: 1 }, { $max: { when: ISODate('2020-01-01') } }); u.updateOne({ _id: 1 }, { $max: { when: ISODate('2019-01-01') } }); return u.findOne({ _id: 1 }).when; });
t('bit', () => { u.updateOne({ _id: 1 }, { $bit: { flags: { or: NumberInt(5) } } }); u.updateOne({ _id: 1 }, { $bit: { flags: { and: NumberInt(4) } } }); return u.findOne({ _id: 1 }).flags; });
t('upsert $in', () => { u.updateOne({ _id: { $in: [50] }, x: 1 }, { $set: { y: 1 } }, { upsert: true }); return u.findOne({ x: 1 }); });
t('upsert replacement', () => { u.replaceOne({ q: 7 }, { r: 1 }, { upsert: true }); return u.findOne({ r: 1 }, { _id: 0 }); });
t('updateMany multi count', () => u.updateMany({}, { $set: { all: 1 } }));
t('empty update doc', () => u.updateOne({ _id: 1 }, {}));
t('replaceOne keeps id', () => { u.replaceOne({ _id: 1 }, { only: 1 }); return u.findOne({ _id: 1 }); });

// aggregation edge cases
const A = (p) => c.aggregate(p).toArray();
t('group doc key', () => A([{ $group: { _id: { t: { $type: '$n' } }, c: { $sum: 1 } } }, { $sort: { '_id.t': 1 } }]));
t('group numeric key merge', () => A([{ $match: { _id: { $lte: 4 } } }, { $group: { _id: '$n', ids: { $push: '$_id' } } }]));
t('group null missing', () => A([{ $group: { _id: '$t', c: { $sum: 1 } } }, { $sort: { _id: 1 } }]));
t('sum types', () => A([{ $match: { _id: { $lte: 5 } } }, { $group: { _id: null, s: { $sum: '$n' }, a: { $avg: '$n' } } }]));
t('unwind nested array', () => A([{ $match: { _id: 1 } }, { $unwind: '$a' }, { $unwind: '$a.b' }, { $project: { b: '$a.b' } }]));
t('project dotted array', () => A([{ $match: { _id: { $in: [1, 2] } } }, { $project: { 'a.b': 1 } }]));
t('project field path arrays', () => A([{ $match: { _id: { $in: [1, 2] } } }, { $project: { v: '$a.b' } }]));
t('addFields array of docs', () => A([{ $match: { _id: 2 } }, { $addFields: { 'a.z': 1 } }, { $project: { a: 1 } }]));
t('lookup array local', () => {
  d.f.insertMany([{ _id: 1, k: 1 }, { _id: 2, k: 2 }, { _id: 3, k: [1, 3] }]);
  d.l.insertOne({ _id: 1, ks: [1, 3] });
  return d.l.aggregate([{ $lookup: { from: 'f', localField: 'ks', foreignField: 'k', as: 'm' } }, { $project: { ids: '$m._id' } }]).toArray();
});
t('sort then group first', () => A([{ $sort: { _id: -1 } }, { $group: { _id: null, f: { $first: '$_id' } } }]));
t('limit 0 err', () => A([{ $limit: 0 }]));
t('skip neg err', () => A([{ $skip: -1 }]));
t('sort bad', () => A([{ $sort: { a: 2 } }]));
t('group no acc', () => A([{ $group: { _id: null, x: 1 } }]));
t('project empty err', () => A([{ $project: {} }]));
t('expr type errs', () => A([{ $project: { x: { $add: ['$s', 1] } } }]));
t('size err', () => A([{ $project: { x: { $size: '$s' } } }]));
t('concat type err', () => A([{ $match: { _id: 1 } }, { $project: { x: { $concat: ['$s', 1] } } }]));
t('cond missing', () => A([{ $match: { _id: 1 } }, { $project: { x: { $cond: [true, '$nope', 1] } } }]));
t('ifNull multi', () => A([{ $match: { _id: 1 } }, { $project: { x: { $ifNull: ['$nope', null, 3] } } }]));
t('arrayElemAt oob', () => A([{ $match: { _id: 1 } }, { $project: { x: { $arrayElemAt: [[1, 2], 5] } } }]));
t('toString types', () => A([{ $match: { _id: { $in: [2, 7, 9] } } }, { $project: { x: { $toString: '$n' }, y: { $toString: '$d' }, z: { $toString: '$s' } } }]));
t('round neg place', () => A([{ $match: { _id: 1 } }, { $project: { x: { $round: [1234.5678, -2] }, y: { $round: [NumberDecimal('1.25'), 1] }, z: { $trunc: [1234.5678, 1] } } }]));
t('decimal math', () => A([{ $match: { _id: 1 } }, { $project: { x: { $add: [NumberDecimal('0.1'), NumberDecimal('0.2')] }, y: { $multiply: [NumberDecimal('1.10'), 3] }, z: { $subtract: [NumberDecimal('10'), 0.5] } } }]));
t('date math', () => A([{ $match: { _id: 7 } }, { $project: { x: { $dateDiff: { startDate: '$d', endDate: ISODate('2021-03-01'), unit: 'week' } }, y: { $dateTrunc: { date: ISODate('2021-03-03T10:00:00Z'), unit: 'week' } }, z: { $dateAdd: { startDate: '$d', unit: 'quarter', amount: 3 } } } }]));
t('setWindow range', () => {
  d.w.insertMany([{ _id: 1, t: 1, v: 1 }, { _id: 2, t: 2, v: 2 }, { _id: 3, t: 5, v: 3 }, { _id: 4, t: 6, v: 4 }]);
  return d.w.aggregate([{ $setWindowFields: { sortBy: { t: 1 }, output: { s: { $sum: '$v', window: { range: [-1, 0] } }, avg: { $avg: '$v', window: { documents: [-1, 1] } }, dn: { $documentNumber: {} } } } }]).toArray();
});
t('bucketAuto granularity', () => d.w.aggregate([{ $bucketAuto: { groupBy: '$t', buckets: 2, granularity: 'POWERSOF2' } }]).toArray());
t('facet empty', () => A([{ $match: { nope: 1 } }, { $facet: { a: [{ $count: 'n' }], b: [{ $limit: 1 }] } }]));
t('sample size', () => A([{ $sample: { size: 3 } }]).length);
t('count large', () => d.big2.insertMany(Array.from({ length: 500 }, (_, i) => ({ i }))) && d.big2.aggregate([{ $match: { i: { $gte: 100 } } }, { $count: 'c' }]).toArray());
t('group many', () => d.big2.aggregate([{ $group: { _id: { $mod: ['$i', 7] }, n: { $sum: 1 } } }, { $sort: { _id: 1 } }]).toArray());
t('sort spill', () => d.big2.aggregate([{ $sort: { i: -1 } }, { $limit: 3 }, { $project: { _id: 0, i: 1 } }], { allowDiskUse: true }).toArray());
