const d = fresh('compat_crud');
const c = d.items;

t('insertMany', () => c.insertMany([
  { _id: 1, name: 'apple', qty: 5, price: 1.5, tags: ['red', 'fruit'], dims: { w: 2, h: 3 }, created: ISODate('2024-01-01T00:00:00Z') },
  { _id: 2, name: 'banana', qty: 0, price: 0.25, tags: ['yellow', 'fruit'], dims: { w: 1, h: 7 } },
  { _id: 3, name: 'carrot', qty: 12, price: NumberDecimal('0.10'), tags: ['orange', 'veg'], arr: [1, 2, 3] },
  { _id: 4, name: 'date', qty: NumberLong(7), price: 3, tags: [], nested: [{ k: 'a', v: 1 }, { k: 'b', v: 2 }] },
  { _id: 5, name: 'Eggplant', qty: null, tags: ['purple', 'veg'], arr: [[1, 2], [3]] },
  { _id: 6, name: 'fig', price: 2.5, nested: [{ k: 'a', v: 5 }, { k: 'c', v: 1 }], s: 'abc' },
]));
t('insert dup', () => c.insertOne({ _id: 1 }));
t('insertMany unordered dup', () => c.insertMany([{ _id: 10 }, { _id: 1 }, { _id: 11 }], { ordered: false }));
t('count after dup', () => c.countDocuments());
c.deleteMany({ _id: { $in: [10, 11] } });

t('find all', () => c.find());
t('eq', () => c.find({ name: 'apple' }));
t('eq array elem', () => c.find({ tags: 'veg' }, { _id: 1 }));
t('eq whole array', () => c.find({ tags: ['red', 'fruit'] }, { _id: 1 }));
t('eq null', () => c.find({ qty: null }, { _id: 1 }));
t('ne', () => c.find({ qty: { $ne: 5 } }, { _id: 1 }));
t('gt numbers mixed', () => c.find({ qty: { $gt: 4 } }, { _id: 1 }));
t('lte decimal', () => c.find({ price: { $lte: 1.5 } }, { _id: 1 }));
t('type bracket', () => c.find({ name: { $gt: 5 } }, { _id: 1 }));
t('string range', () => c.find({ name: { $gte: 'b', $lt: 'd' } }, { _id: 1 }));
t('in', () => c.find({ qty: { $in: [0, 7, null] } }, { _id: 1 }));
t('in regex', () => c.find({ name: { $in: [/^a/, /^c/] } }, { _id: 1 }));
t('nin', () => c.find({ tags: { $nin: ['fruit'] } }, { _id: 1 }));
t('exists', () => c.find({ price: { $exists: false } }, { _id: 1 }));
t('type string', () => c.find({ price: { $type: 'number' } }, { _id: 1 }));
t('type decimal', () => c.find({ price: { $type: 'decimal' } }, { _id: 1 }));
t('type array', () => c.find({ arr: { $type: 'array' } }, { _id: 1 }));
t('size', () => c.find({ tags: { $size: 2 } }, { _id: 1 }));
t('all', () => c.find({ tags: { $all: ['fruit', 'red'] } }, { _id: 1 }));
t('elemMatch doc', () => c.find({ nested: { $elemMatch: { k: 'a', v: { $gt: 2 } } } }, { _id: 1 }));
t('elemMatch value', () => c.find({ arr: { $elemMatch: { $gt: 2 } } }, { _id: 1 }));
t('dotted array', () => c.find({ 'nested.k': 'c' }, { _id: 1 }));
t('dotted index', () => c.find({ 'arr.1': 2 }, { _id: 1 }));
t('regex i', () => c.find({ name: /^e/i }, { _id: 1 }));
t('regex op', () => c.find({ name: { $regex: 'an', $options: '' } }, { _id: 1 }));
t('not regex', () => c.find({ name: { $not: /a/ } }, { _id: 1 }));
t('mod', () => c.find({ qty: { $mod: [5, 0] } }, { _id: 1 }));
t('or', () => c.find({ $or: [{ qty: 0 }, { name: 'fig' }] }, { _id: 1 }));
t('nor', () => c.find({ $nor: [{ tags: 'fruit' }, { tags: 'veg' }] }, { _id: 1 }));
t('and', () => c.find({ $and: [{ price: { $gt: 0 } }, { price: { $lt: 2 } }] }, { _id: 1 }));
t('expr', () => c.find({ $expr: { $gt: ['$qty', 6] } }, { _id: 1 }));
t('dims.w', () => c.find({ 'dims.w': { $gte: 2 } }, { _id: 1 }));
t('bad op', () => c.find({ qty: { $foo: 1 } }).toArray());
t('bad top op', () => c.find({ $foo: 1 }).toArray());

t('sort asc', () => c.find({}, { _id: 1, qty: 1 }).sort({ qty: 1 }));
t('sort desc', () => c.find({}, { _id: 1, qty: 1 }).sort({ qty: -1, _id: 1 }));
t('sort array', () => c.find({}, { _id: 1 }).sort({ tags: 1, _id: 1 }));
t('sort desc array', () => c.find({}, { _id: 1 }).sort({ tags: -1, _id: 1 }));
t('skip limit', () => c.find({}, { _id: 1 }).sort({ _id: 1 }).skip(2).limit(2));
t('neg limit', () => c.find({}, { _id: 1 }).sort({ _id: -1 }).limit(-2));
t('proj incl', () => c.find({ _id: 1 }, { name: 1, 'dims.w': 1 }));
t('proj excl', () => c.find({ _id: 1 }, { tags: 0, dims: { h: 0 } }));
t('proj no id', () => c.find({ _id: 1 }, { _id: 0, name: 1 }));
t('proj mixed err', () => c.find({}, { name: 1, qty: 0 }).toArray());
t('proj slice', () => c.find({ _id: 3 }, { arr: { $slice: -2 } }));
t('proj slice skip', () => c.find({ _id: 3 }, { arr: { $slice: [1, 1] }, name: 1 }));
t('proj elemMatch', () => c.find({ _id: 6 }, { nested: { $elemMatch: { v: { $lt: 3 } } } }));
t('proj positional', () => c.find({ 'nested.k': 'b' }, { 'nested.$': 1 }));
t('proj expr', () => c.find({ _id: 2 }, { total: { $multiply: ['$qty', '$price'] }, up: { $toUpper: '$name' } }));
t('proj nested array', () => c.find({ _id: 4 }, { 'nested.v': 1 }));

t('countDocuments', () => c.countDocuments({ tags: 'fruit' }));
t('estimated', () => c.estimatedDocumentCount());
t('count cmd', () => d.runCommand({ count: 'items', query: { qty: { $gte: 5 } } }).n);
t('distinct', () => c.distinct('tags'));
t('distinct filter', () => c.distinct('nested.k', { _id: { $gt: 3 } }));
t('distinct num', () => c.distinct('qty'));

// updates
t('set', () => c.updateOne({ _id: 1 }, { $set: { qty: 6, 'dims.d': 1, newb: 1, newa: 2 } }));
t('after set', () => c.findOne({ _id: 1 }));
t('inc', () => c.updateOne({ _id: 2 }, { $inc: { qty: 2, missing: NumberLong(3) } }));
t('inc string err', () => c.updateOne({ _id: 1 }, { $inc: { name: 1 } }));
t('mul', () => c.updateOne({ _id: 2 }, { $mul: { price: 4, nope: 2 } }));
t('after inc mul', () => c.findOne({ _id: 2 }));
t('unset', () => c.updateOne({ _id: 1 }, { $unset: { newa: '', 'tags.0': 1 } }));
t('after unset', () => c.findOne({ _id: 1 }));
t('min max', () => c.updateOne({ _id: 3 }, { $min: { qty: 10 }, $max: { hi: 5 } }));
t('rename', () => c.updateOne({ _id: 3 }, { $rename: { hi: 'high', name: 'title' } }));
t('after rename', () => c.findOne({ _id: 3 }));
t('push each', () => c.updateOne({ _id: 3 }, { $push: { arr: { $each: [9, 0, 5], $sort: -1, $slice: 4 } } }));
t('push position', () => c.updateOne({ _id: 4 }, { $push: { tags: { $each: ['x', 'y'], $position: 0 } } }));
t('addToSet', () => c.updateOne({ _id: 4 }, { $addToSet: { tags: { $each: ['y', 'z'] } } }));
t('pop', () => c.updateOne({ _id: 2 }, { $pop: { tags: -1 } }));
t('pull cond', () => c.updateOne({ _id: 3 }, { $pull: { arr: { $gte: 5 } } }));
t('pull doc', () => c.updateOne({ _id: 6 }, { $pull: { nested: { k: 'c' } } }));
t('pullAll', () => c.updateOne({ _id: 4 }, { $pullAll: { tags: ['x', 'z'] } }));
t('after array ops', () => c.find({ _id: { $in: [2, 3, 4, 6] } }).sort({ _id: 1 }));
t('positional update', () => c.updateOne({ 'nested.k': 'b' }, { $set: { 'nested.$.v': 20 } }));
t('all positional', () => c.updateOne({ _id: 4 }, { $inc: { 'nested.$[].v': 1 } }));
t('arrayFilters', () => c.updateOne({ _id: 4 }, { $set: { 'nested.$[e].flag': true } }, { arrayFilters: [{ 'e.v': { $gt: 5 } }] }));
t('after positional', () => c.findOne({ _id: 4 }));
t('conflict', () => c.updateOne({ _id: 1 }, { $set: { a: 1 }, $inc: { 'a.b': 1 } }));
t('unknown modifier', () => c.updateOne({ _id: 1 }, { $foo: { a: 1 } }));
t('set _id err', () => c.updateOne({ _id: 1 }, { $set: { _id: 99 } }));
t('replace', () => c.replaceOne({ _id: 5 }, { name: 'eggplant', qty: 1 }));
t('after replace', () => c.findOne({ _id: 5 }));
t('replace dollar', () => c.replaceOne({ _id: 5 }, { $set: { a: 1 } }));
t('upsert', () => c.updateOne({ _id: 7, kind: 'x' }, { $set: { qty: 1 }, $setOnInsert: { born: true } }, { upsert: true }));
t('upsert dotted', () => c.updateOne({ 'meta.code': 'm1' }, { $inc: { hits: 1 } }, { upsert: true }).matchedCount);
t('after upsert', () => c.find({ $or: [{ _id: 7 }, { 'meta.code': 'm1' }] }, { _id: 0 }).sort({ hits: 1 }));
t('updateMany', () => c.updateMany({ tags: 'veg' }, { $set: { veg: true } }));
t('updateMany nomatch', () => c.updateMany({ none: 1 }, { $set: { veg: true } }));
t('noop update', () => c.updateOne({ _id: 1 }, { $set: { qty: 6 } }));
t('pipeline update', () => c.updateOne({ _id: 2 }, [{ $set: { total: { $add: ['$qty', 1] }, name: { $concat: ['$name', '!'] } } }, { $unset: 'tags' }]));
t('after pipeline update', () => c.findOne({ _id: 2 }));
t('currentDate type', () => c.updateOne({ _id: 2 }, { $currentDate: { ts: { $type: 'timestamp' } } }) && typeof c.findOne({ _id: 2 }).ts);

// findAndModify
t('fam new', () => c.findOneAndUpdate({ _id: 1 }, { $inc: { qty: 1 } }, { returnDocument: 'after', projection: { qty: 1 } }));
t('fam old', () => c.findOneAndUpdate({ _id: 1 }, { $inc: { qty: 1 } }, { projection: { qty: 1 } }));
t('fam sort', () => c.findOneAndUpdate({ qty: { $gt: 0 } }, { $set: { picked: 1 } }, { sort: { qty: -1 }, projection: { _id: 1 } }));
t('fam upsert', () => c.findOneAndUpdate({ _id: 50 }, { $set: { v: 1 } }, { upsert: true, returnDocument: 'after' }));
t('fam replace', () => c.findOneAndReplace({ _id: 50 }, { v: 2 }, { returnDocument: 'after' }));
t('fam delete', () => c.findOneAndDelete({ _id: 50 }));
t('fam none', () => c.findOneAndDelete({ _id: 999 }));
t('fam raw', () => { const r = d.runCommand({ findAndModify: 'items', query: { _id: 7 }, update: { $set: { z: 1 } } }); return [r.lastErrorObject, r.value]; });

// deletes
t('deleteOne', () => c.deleteOne({ tags: 'fruit' }));
t('deleteMany', () => c.deleteMany({ veg: true }));
t('remaining', () => c.find({ _id: { $type: 'number' } }, { _id: 1 }).sort({ _id: 1 }));

// bulkWrite
t('bulkWrite', () => c.bulkWrite([
  { insertOne: { document: { _id: 100, a: 1 } } },
  { updateOne: { filter: { _id: 100 }, update: { $set: { a: 2 } } } },
  { updateMany: { filter: { a: 2 }, update: { $inc: { a: 1 } } } },
  { replaceOne: { filter: { _id: 101 }, replacement: { a: 9 }, upsert: true } },
  { deleteOne: { filter: { _id: 100 } } },
]));
t('after bulk', () => c.find({ _id: { $gte: 100 } }));

// cursors / batches
const big = d.big;
const docs = []; for (let i = 0; i < 250; i++) docs.push({ _id: i, v: i % 7, s: 'x'.repeat(i % 5) });
big.insertMany(docs);
t('batch iterate', () => { let n = 0; const cur = big.find({}).batchSize(10); while (cur.hasNext()) { cur.next(); n++; } return n; });
t('sum v', () => big.aggregate([{ $group: { _id: null, s: { $sum: '$v' } } }]));
t('getMore raw', () => { const r = d.runCommand({ find: 'big', filter: {}, batchSize: 5, sort: { _id: 1 } }); const m = d.runCommand({ getMore: r.cursor.id, collection: 'big', batchSize: 3 }); return [r.cursor.firstBatch.length, m.cursor.nextBatch.map((x) => x._id)]; });
t('killCursors', () => { const r = d.runCommand({ find: 'big', batchSize: 1 }); const k = d.runCommand({ killCursors: 'big', cursors: [r.cursor.id] }); return k.cursorsKilled.length; });
t('getMore dead', () => { const r = d.runCommand({ find: 'big', batchSize: 1 }); d.runCommand({ killCursors: 'big', cursors: [r.cursor.id] }); const m = d.runCommand({ getMore: r.cursor.id, collection: 'big' }); return [m.ok, m.code]; });
t('natural order', () => big.find({ _id: { $lt: 5 } }, { _id: 1 }));
t('natural reverse', () => big.find({ _id: { $lt: 5 } }, { _id: 1 }).sort({ $natural: -1 }));
t('custom id order', () => { const o = d.order; o.insertMany([{ _id: 'z' }, { _id: 'a' }, { _id: 5 }, { _id: 'm' }]); return o.find(); });
