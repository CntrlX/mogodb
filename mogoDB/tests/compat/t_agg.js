const d = fresh('compat_agg');
const s = d.sales;
s.insertMany([
  { _id: 1, item: 'abc', price: NumberDecimal('10'), qty: 2, date: ISODate('2014-03-01T08:00:00Z'), tags: ['a', 'b'], cust: 'x' },
  { _id: 2, item: 'jkl', price: 20, qty: 1, date: ISODate('2014-03-01T09:00:00Z'), tags: ['b'], cust: 'y' },
  { _id: 3, item: 'xyz', price: 5, qty: 10, date: ISODate('2014-03-15T09:00:00Z'), tags: [], cust: 'x' },
  { _id: 4, item: 'xyz', price: 5, qty: 20, date: ISODate('2014-04-04T11:21:39.736Z'), cust: 'z' },
  { _id: 5, item: 'abc', price: 10, qty: 10, date: ISODate('2014-04-04T21:23:13.331Z'), tags: ['c'], cust: 'y' },
  { _id: 6, item: 'def', price: 7.5, qty: 5, date: ISODate('2015-06-04T05:08:13Z'), tags: ['a'], cust: 'x' },
  { _id: 7, item: 'def', price: 7.5, qty: NumberLong(10), date: ISODate('2015-09-10T08:43:00Z'), cust: null },
  { _id: 8, item: 'abc', price: 10, qty: 5, date: ISODate('2016-02-06T20:20:13Z') },
]);
d.custs.insertMany([{ _id: 'x', name: 'Xavier', tier: 1 }, { _id: 'y', name: 'Yolanda', tier: 2 }, { _id: 'z', name: 'Zed', tier: 1 }]);

const A = (p, o) => s.aggregate(p, o).toArray();
t('group sum', () => A([{ $group: { _id: '$item', total: { $sum: { $multiply: ['$price', '$qty'] } }, n: { $sum: 1 } } }, { $sort: { _id: 1 } }]));
t('group avg/min/max', () => A([{ $group: { _id: null, avg: { $avg: '$qty' }, min: { $min: '$price' }, max: { $max: '$date' }, first: { $first: '$item' }, last: { $last: '$item' } } }]));
t('group push set', () => A([{ $sort: { _id: 1 } }, { $group: { _id: '$cust', items: { $push: '$item' }, uniq: { $addToSet: '$item' } } }, { $sort: { _id: 1 } }]).map((g) => ({ _id: g._id, items: g.items, uniq: g.uniq.sort() })));
t('group date', () => A([{ $group: { _id: { y: { $year: '$date' }, m: { $month: '$date' } }, c: { $count: {} } } }, { $sort: { '_id.y': 1, '_id.m': 1 } }]));
t('group stddev', () => A([{ $group: { _id: null, p: { $stdDevPop: '$qty' }, s: { $stdDevSamp: '$qty' } } }]));
t('group topN', () => A([{ $group: { _id: '$item', top: { $topN: { n: 2, sortBy: { qty: -1 }, output: '$qty' } }, bottom: { $bottom: { sortBy: { qty: -1 }, output: '$_id' } } } }, { $sort: { _id: 1 } }]));
t('group firstN', () => A([{ $sort: { _id: 1 } }, { $group: { _id: null, f: { $firstN: { input: '$_id', n: 3 } }, l: { $lastN: { input: '$_id', n: 2 } }, mx: { $maxN: { input: '$qty', n: 2 } } } }]));
t('group no id', () => A([{ $group: { total: { $sum: 1 } } }]));
t('match project', () => A([{ $match: { qty: { $gte: 10 } } }, { $project: { _id: 0, item: 1, big: { $gt: ['$qty', 10] } } }]));
t('addFields nested', () => A([{ $match: { _id: 1 } }, { $addFields: { 'meta.x': 1, total: { $add: ['$qty', 1] } } }]));
t('unset', () => A([{ $match: { _id: 2 } }, { $unset: ['date', 'tags'] }]));
t('unwind', () => A([{ $unwind: '$tags' }, { $project: { tags: 1 } }]));
t('unwind opts', () => A([{ $unwind: { path: '$tags', includeArrayIndex: 'i', preserveNullAndEmptyArrays: true } }, { $project: { tags: 1, i: 1 } }]));
t('sort limit skip', () => A([{ $sort: { qty: -1, _id: 1 } }, { $skip: 1 }, { $limit: 3 }, { $project: { qty: 1 } }]));
t('count', () => A([{ $match: { item: 'abc' } }, { $count: 'n' }]));
t('count empty', () => A([{ $match: { item: 'nope' } }, { $count: 'n' }]));
t('sortByCount', () => A([{ $sortByCount: '$item' }, { $sort: { count: -1, _id: 1 } }]));
t('lookup', () => A([{ $match: { _id: { $lte: 3 } } }, { $lookup: { from: 'custs', localField: 'cust', foreignField: '_id', as: 'c' } }, { $project: { 'c.name': 1 } }]));
t('lookup pipeline', () => A([{ $match: { _id: 2 } }, { $lookup: { from: 'custs', let: { c: '$cust' }, pipeline: [{ $match: { $expr: { $ne: ['$_id', '$$c'] } } }, { $project: { _id: 1 } }], as: 'others' } }, { $project: { others: 1 } }]));
t('lookup null', () => A([{ $match: { _id: { $in: [7, 8] } } }, { $lookup: { from: 'custs', localField: 'cust', foreignField: 'missing', as: 'c' } }, { $project: { n: { $size: '$c' } } }]));
t('facet', () => A([{ $facet: { byItem: [{ $group: { _id: '$item' } }, { $sort: { _id: 1 } }], cnt: [{ $count: 'n' }] } }]));
t('bucket', () => A([{ $bucket: { groupBy: '$qty', boundaries: [0, 5, 10, 50], default: 'other', output: { n: { $sum: 1 }, ids: { $push: '$_id' } } } }]));
t('bucketAuto', () => A([{ $bucketAuto: { groupBy: '$qty', buckets: 3 } }]));
t('replaceRoot', () => A([{ $match: { _id: 1 } }, { $replaceRoot: { newRoot: { i: '$item', q: '$qty' } } }]));
t('replaceWith merge', () => A([{ $match: { _id: 1 } }, { $replaceWith: { $mergeObjects: [{ a: 1 }, '$$ROOT'] } }, { $project: { a: 1, item: 1 } }]));
t('redact', () => A([{ $match: { _id: 1 } }, { $redact: { $cond: [{ $eq: ['$item', 'abc'] }, '$$KEEP', '$$PRUNE'] } }, { $project: { item: 1 } }]));
t('unionWith', () => A([{ $match: { _id: 1 } }, { $project: { _id: 1 } }, { $unionWith: { coll: 'custs', pipeline: [{ $project: { _id: 1 } }] } }]));
t('graphLookup', () => {
  d.emp.insertMany([{ _id: 1, name: 'a' }, { _id: 2, name: 'b', boss: 1 }, { _id: 3, name: 'c', boss: 2 }, { _id: 4, name: 'd', boss: 2 }]);
  return d.emp.aggregate([{ $match: { _id: 4 } }, { $graphLookup: { from: 'emp', startWith: '$boss', connectFromField: 'boss', connectToField: '_id', as: 'chain', depthField: 'depth' } }, { $project: { chain: { $sortArray: { input: '$chain', sortBy: { _id: 1 } } } } }]).toArray();
});
t('setWindowFields', () => A([{ $setWindowFields: { partitionBy: '$item', sortBy: { _id: 1 }, output: { run: { $sum: '$qty', window: { documents: ['unbounded', 'current'] } }, rank: { $rank: {} } } } }, { $project: { item: 1, run: 1, rank: 1 } }, { $sort: { _id: 1 } }]));
t('densify', () => d.dn.insertMany([{ x: 1 }, { x: 4 }]) && d.dn.aggregate([{ $densify: { field: 'x', range: { step: 1, bounds: 'full' } } }, { $project: { _id: 0, x: 1 } }]).toArray());
t('out', () => { s.aggregate([{ $match: { item: 'abc' } }, { $project: { qty: 1 } }, { $out: 'abc_out' }]).toArray(); return d.abc_out.find().sort({ _id: 1 }).toArray(); });
t('merge', () => { s.aggregate([{ $group: { _id: '$item', q: { $sum: '$qty' } } }, { $merge: { into: 'totals', whenMatched: 'merge', whenNotMatched: 'insert' } }]).toArray(); return d.totals.find().sort({ _id: 1 }).toArray(); });
t('documents', () => d.aggregate([{ $documents: [{ a: 1 }, { a: 2 }] }, { $match: { a: 2 } }]).toArray());
t('unknown stage', () => A([{ $foo: {} }]));
t('two keys stage', () => A([{ $match: {}, $sort: { a: 1 } }]));

// expressions
const E = (expr) => d.aggregate([{ $documents: [{ a: 7, b: 2, s: 'Hello World', arr: [3, 1, 2], n: null, dt: ISODate('2024-02-29T13:45:30.123Z'), o: { x: 1, y: 2 } }] }, { $project: { _id: 0, r: expr } }]).toArray()[0];
t('add', () => E({ $add: ['$a', '$b', 1.5] }));
t('add int overflow', () => E({ $add: [2147483647, 1] }));
t('add date', () => E({ $add: ['$dt', 1000] }));
t('subtract dates', () => E({ $subtract: ['$dt', ISODate('2024-02-29T00:00:00Z')] }));
t('divide', () => E({ $divide: ['$a', '$b'] }));
t('divide zero', () => E({ $divide: ['$a', 0] }));
t('mod', () => E({ $mod: ['$a', '$b'] }));
t('round', () => E([{ $round: [2.5, 0] }, { $round: [3.5, 0] }, { $round: [1.2345, 2] }, { $trunc: [-1.7] }]));
t('math', () => E([{ $abs: -3 }, { $ceil: 1.2 }, { $floor: -1.2 }, { $sqrt: 16 }, { $pow: [2, 10] }, { $exp: 0 }, { $ln: 1 }, { $log10: 100 }]));
t('null arith', () => E({ $add: ['$a', '$n'] }));
t('missing arith', () => E({ $multiply: ['$a', '$nope'] }));
t('cmp', () => E([{ $cmp: ['$a', '$b'] }, { $eq: ['$a', 7] }, { $lt: ['$n', 0] }, { $gt: ['$s', 1] }]));
t('cond', () => E([{ $cond: [{ $gt: ['$a', 5] }, 'big', 'small'] }, { $cond: { if: false, then: 1, else: 2 } }, { $ifNull: ['$nope', '$n', 'dflt'] }]));
t('switch', () => E({ $switch: { branches: [{ case: { $lt: ['$a', 0] }, then: 'neg' }, { case: { $gt: ['$a', 5] }, then: 'gt5' }], default: 'other' } }));
t('switch nodefault', () => E({ $switch: { branches: [{ case: false, then: 1 }] } }));
t('concat', () => E([{ $concat: ['$s', '!', ''] }, { $concat: ['a', '$n'] }]));
t('string ops', () => E([{ $toLower: '$s' }, { $toUpper: '$s' }, { $strLenCP: 'héllo' }, { $strLenBytes: 'héllo' }, { $substrCP: ['héllo', 1, 3] }, { $split: ['$s', ' '] }, { $indexOfCP: ['$s', 'o'] }, { $strcasecmp: ['abc', 'ABD'] }]));
t('trim', () => E([{ $trim: { input: '  x  ' } }, { $ltrim: { input: 'xxaxx', chars: 'x' } }, { $rtrim: { input: 'xxaxx', chars: 'x' } }]));
t('replace', () => E([{ $replaceOne: { input: 'aaa', find: 'a', replacement: 'b' } }, { $replaceAll: { input: 'aaa', find: 'a', replacement: 'b' } }]));
t('regex', () => E([{ $regexMatch: { input: '$s', regex: /wor/i } }, { $regexFind: { input: '$s', regex: '(o)(r)?' } }, { $size: { $regexFindAll: { input: '$s', regex: 'o' } } }]));
t('date parts', () => E([{ $year: '$dt' }, { $month: '$dt' }, { $dayOfMonth: '$dt' }, { $hour: '$dt' }, { $minute: '$dt' }, { $second: '$dt' }, { $millisecond: '$dt' }, { $dayOfYear: '$dt' }, { $dayOfWeek: '$dt' }, { $week: '$dt' }, { $isoWeek: '$dt' }, { $isoDayOfWeek: '$dt' }, { $isoWeekYear: '$dt' }]));
t('dateToString', () => E([{ $dateToString: { date: '$dt' } }, { $dateToString: { date: '$dt', format: '%Y/%m/%d %H:%M:%S.%L %j %U %u %w' } }, { $dateToString: { date: '$dt', format: '%H:%M', timezone: '+05:30' } }]));
t('dateFromString', () => E([{ $dateFromString: { dateString: '2024-01-02T03:04:05.006Z' } }, { $dateFromString: { dateString: '2024-01-02', format: '%Y-%m-%d' } }, { $dateFromString: { dateString: 'garbage', onError: 'bad' } }]));
t('dateAdd diff trunc', () => E([{ $dateAdd: { startDate: ISODate('2024-01-31T00:00:00Z'), unit: 'month', amount: 1 } }, { $dateDiff: { startDate: ISODate('2024-01-31T00:00:00Z'), endDate: ISODate('2024-02-01T00:00:00Z'), unit: 'month' } }, { $dateTrunc: { date: '$dt', unit: 'hour' } }, { $dateSubtract: { startDate: '$dt', unit: 'day', amount: 30 } }]));
t('dateToParts', () => E({ $dateToParts: { date: '$dt' } }));
t('dateFromParts', () => E({ $dateFromParts: { year: 2024, month: 14, day: 1 } }));
t('array ops', () => E([{ $arrayElemAt: ['$arr', -1] }, { $first: '$arr' }, { $last: '$arr' }, { $concatArrays: ['$arr', [9]] }, { $in: [2, '$arr'] }, { $indexOfArray: ['$arr', 2] }, { $isArray: '$arr' }, { $range: [0, 10, 3] }, { $reverseArray: '$arr' }, { $size: '$arr' }, { $slice: ['$arr', 1, 1] }, { $slice: ['$arr', -2] }]));
t('filter map reduce', () => E([{ $filter: { input: '$arr', as: 'x', cond: { $gte: ['$$x', 2] } } }, { $map: { input: '$arr', in: { $multiply: ['$$this', 10] } } }, { $reduce: { input: '$arr', initialValue: 0, in: { $add: ['$$value', '$$this'] } } }]));
t('sortArray', () => E([{ $sortArray: { input: '$arr', sortBy: 1 } }, { $sortArray: { input: [{ a: 2 }, { a: 1 }], sortBy: { a: 1 } } }]));
t('zip', () => E({ $zip: { inputs: [[1, 2], ['a']], useLongestLength: true, defaults: [0, 'z'] } }));
t('sets', () => E([{ $setUnion: [[1, 2], [2, 3]] }, { $setIntersection: [[1, 2], [2, 3]] }, { $setDifference: [[1, 2], [2]] }, { $setEquals: [[1, 2], [2, 1, 1]] }, { $setIsSubset: [[1], [1, 2]] }, { $anyElementTrue: [[0, 1]] }, { $allElementsTrue: [[1, 0]] }]));
t('objects', () => E([{ $mergeObjects: ['$o', { y: 3, z: 4 }] }, { $objectToArray: '$o' }, { $arrayToObject: [[['k', 1], ['j', 2]]] }, { $getField: 'a' }, { $setField: { field: 'q', input: '$o', value: 9 } }]));
t('let literal', () => E([{ $let: { vars: { x: 3 }, in: { $multiply: ['$$x', '$a'] } } }, { $literal: '$a' }]));
t('types', () => E([{ $type: '$a' }, { $type: '$nope' }, { $type: '$dt' }, { $isNumber: '$s' }, { $toString: '$a' }, { $toInt: '12' }, { $toDouble: '1.5' }, { $toLong: 5 }, { $toBool: 0 }, { $toDecimal: 1 }, { $toDate: 0 }]));
t('convert onError', () => E({ $convert: { input: 'abc', to: 'int', onError: -1 } }));
t('convert err', () => E({ $toInt: 'abc' }));
t('sum avg expr', () => E([{ $sum: '$arr' }, { $avg: '$arr' }, { $max: '$arr' }, { $min: [5, 2, 9] }]));
t('remove', () => E({ x: '$$REMOVE', y: 1 }));
t('unknown expr', () => E({ $nope: 1 }));
t('tz olson parts', () => E([{ $hour: { date: ISODate('2024-01-15T12:00:00Z'), timezone: 'America/New_York' } }, { $hour: { date: ISODate('2024-07-15T12:00:00Z'), timezone: 'America/New_York' } }, { $minute: { date: '$dt', timezone: 'Asia/Kolkata' } }, { $dayOfMonth: { date: ISODate('2024-03-01T02:00:00Z'), timezone: 'America/Los_Angeles' } }]));
t('tz dateToString', () => E([{ $dateToString: { date: ISODate('2024-07-04T16:00:00Z'), format: '%Y-%m-%d %H:%M %z %Z', timezone: 'America/New_York' } }, { $dateToString: { date: ISODate('2024-12-04T16:00:00Z'), format: '%H:%M %z', timezone: 'Europe/London' } }]));
t('tz dateFromParts', () => E([{ $dateFromParts: { year: 2024, month: 3, day: 10, hour: 12, timezone: 'America/New_York' } }, { $dateFromParts: { year: 2024, month: 11, day: 3, hour: 12, timezone: 'America/New_York' } }]));
t('tz dateFromString', () => E({ $dateFromString: { dateString: '2024-07-01T10:00:00', timezone: 'Europe/Berlin' } }));
t('tz trunc add', () => E([{ $dateTrunc: { date: ISODate('2024-03-10T15:00:00Z'), unit: 'day', timezone: 'America/New_York' } }, { $dateAdd: { startDate: ISODate('2024-03-09T17:00:00Z'), unit: 'day', amount: 1, timezone: 'America/New_York' } }]));
t('tz bad', () => E({ $hour: { date: '$dt', timezone: 'Nowhere/City' } }));
