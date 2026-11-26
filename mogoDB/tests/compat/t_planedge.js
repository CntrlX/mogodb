// Result-correctness edge cases for index plans: covered reads, sort-providing scans,
// $or canonicalization, regex bounds, unique enforcement after updates.
const d = fresh('compat_planedge');
const c = d.c;
c.insertMany([
  { _id: 1, a: 0, b: 'x' }, { _id: 2, a: -0.0, b: 'y' }, { _id: 3, a: NaN }, { _id: 4, a: NumberLong('9223372036854775807') },
  { _id: 5, a: NumberLong('-9223372036854775808') }, { _id: 6, a: 9007199254740993 }, { _id: 7, a: NumberLong('9007199254740993') },
  { _id: 8, a: 1.5, b: null }, { _id: 9, a: NumberDecimal('2.50') }, { _id: 10, a: 'str' }, { _id: 11, a: '' }, { _id: 12 },
  { _id: 13, a: null }, { _id: 14, a: true }, { _id: 15, a: ISODate('1960-01-01') }, { _id: 16, a: ObjectId('507f1f77bcf86cd799439011') },
  { _id: 17, a: { x: 1 } }, { _id: 18, a: MinKey() }, { _id: 19, a: MaxKey() }, { _id: 20, a: Timestamp(5, 6) }, { _id: 21, a: BinData(0, 'AQID') },
  { _id: 22, a: /re/i }, { _id: 23, a: 'a\u0000b' }, { _id: 24, a: Infinity }, { _id: 25, a: -Infinity }, { _id: 26, a: NumberInt(7) },
]);
c.createIndex({ a: 1 });
c.createIndex({ a: -1, b: 1 }, { name: 'a_desc_b' });
const cov = (f, p, s) => c.find(f, p).sort(s || { _id: 1 }).toArray();
const plan = (q) => q.explain().queryPlanner.winningPlan;
const stage = (q) => JSON.stringify(plan(q)).includes('PROJECTION_COVERED');

t('covered all types asc', () => c.find({ a: { $gte: MinKey() } }, { _id: 0, a: 1 }).sort({ a: 1 }).hint({ a: 1 }).toArray());
t('covered all types desc', () => c.find({ a: { $gte: MinKey() } }, { _id: 0, a: 1 }).sort({ a: -1 }).hint({ a: 1 }).toArray());
t('covered compound desc', () => c.find({ a: { $gt: -100 } }, { _id: 0, a: 1, b: 1 }).sort({ a: -1, b: 1 }).hint('a_desc_b').toArray());
t('covered is covered', () => stage(c.find({ a: { $gt: 5 } }, { _id: 0, a: 1 })));
t('covered missing vs null', () => c.find({ a: null }, { _id: 0, a: 1 }).hint({ a: 1 }).toArray());
t('covered long equality', () => cov({ a: NumberLong('9007199254740993') }, { _id: 0, a: 1 }, { a: 1 }));
t('eq numeric cross type', () => cov({ a: 9007199254740993 }, { _id: 1 }));
t('eq -0', () => cov({ a: -0.0 }, { _id: 1 }));
t('eq NaN', () => cov({ a: NaN }, { _id: 1 }));
t('range strings', () => cov({ a: { $gte: '', $lt: 'z' } }, { _id: 1 }));
t('range dates', () => cov({ a: { $lt: new Date() } }, { _id: 1 }));
t('in mixed reverse sort', () => c.find({ a: { $in: [7, 'str', null, 1.5] } }, { _id: 1, a: 1 }).sort({ a: -1 }).toArray());
t('in mixed fwd sort', () => c.find({ a: { $in: [7, 'str', null, 1.5] } }, { _id: 1, a: 1 }).sort({ a: 1 }).toArray());
t('sort limit early', () => c.find({}, { _id: 1 }).sort({ a: -1 }).limit(5).toArray());
t('sort skip limit', () => c.find({ a: { $type: 'number' } }, { _id: 1 }).sort({ a: 1 }).skip(3).limit(4).toArray());

// $or canonicalization must keep semantics
t('or eq to in', () => cov({ $or: [{ a: 7 }, { a: 'str' }] }, { _id: 1 }));
t('or null', () => cov({ $or: [{ a: null }, { a: 7 }] }, { _id: 1 }));
t('or doc eq', () => cov({ $or: [{ a: { x: 1 } }, { a: 7 }] }, { _id: 1 }));
t('or regex', () => cov({ $or: [{ a: /^st/ }, { a: 7 }] }, { _id: 1 }));
t('or mixed paths', () => cov({ $or: [{ a: 7 }, { b: 'x' }] }, { _id: 1 }));
t('or with and', () => cov({ a: { $gt: 1 }, $or: [{ a: 7 }, { a: 1.5 }] }, { _id: 1 }));
t('or explicit eq', () => cov({ $or: [{ a: { $eq: 7 } }, { a: { $eq: 1.5 } }] }, { _id: 1 }));

// regex bounds
d.r.insertMany([{ _id: 1, s: 'abc' }, { _id: 2, s: 'ab.c' }, { _id: 3, s: 'abd' }, { _id: 4, s: 'ABC' }, { _id: 5, s: /^ab/ }, { _id: 6, s: 'ab' }, { _id: 7, s: 'a b' }]);
d.r.createIndex({ s: 1 });
t('regex prefix', () => d.r.find({ s: /^ab/ }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('regex dot', () => d.r.find({ s: /^ab.c/ }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('regex escaped dot', () => d.r.find({ s: /^ab\.c/ }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('regex i', () => d.r.find({ s: /^ab/i }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('regex space', () => d.r.find({ s: /^a b/ }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('regex anchored end', () => d.r.find({ s: /^ab$/ }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('regex covered', () => d.r.find({ s: /^ab/ }, { _id: 0, s: 1 }).sort({ s: 1 }).toArray());

// multikey: no covering, correct dedupe and sort
d.m.insertMany([{ _id: 1, t: [3, 1] }, { _id: 2, t: [2] }, { _id: 3, t: [] }, { _id: 4, t: [[1, 2]] }, { _id: 5, t: 5 }]);
d.m.createIndex({ t: 1 });
t('multikey sort asc', () => d.m.find({}, { _id: 1 }).sort({ t: 1 }).toArray());
t('multikey sort desc', () => d.m.find({}, { _id: 1 }).sort({ t: -1 }).toArray());
t('multikey range dedupe', () => d.m.find({ t: { $gte: 1 } }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('multikey projection', () => d.m.find({ t: { $gte: 1 } }, { _id: 0, t: 1 }).sort({ t: 1 }).toArray());

// compound with missing fields, sparse and partial indexes
d.p.insertMany([{ _id: 1, x: 1, y: 2 }, { _id: 2, x: 1 }, { _id: 3, y: 5 }, { _id: 4, x: 2, y: null }, { _id: 5 }]);
d.p.createIndex({ x: 1, y: 1 });
d.p.createIndex({ y: 1 }, { sparse: true });
d.p.createIndex({ x: -1 }, { partialFilterExpression: { y: { $exists: true } } });
t('compound covered missing', () => d.p.find({ x: { $gte: 1 } }, { _id: 0, x: 1, y: 1 }).sort({ x: 1, y: 1 }).toArray());
t('sparse not used for null', () => d.p.find({ y: null }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('sparse sort', () => d.p.find({}, { _id: 1 }).sort({ y: 1, _id: 1 }).toArray());
t('partial eligible', () => d.p.find({ x: 1, y: { $exists: true } }, { _id: 1 }).toArray());

// updates that change type bits keep covered results and uniqueness correct
d.u.insertMany([{ _id: 1, k: 1 }, { _id: 2, k: 2 }]);
d.u.createIndex({ k: 1 }, { unique: true });
d.u.updateOne({ _id: 1 }, { $set: { k: 1.0 } });
t('typebits after update', () => d.u.find({ k: 1 }, { _id: 0, k: 1 }).toArray());
d.u.updateOne({ _id: 1 }, { $set: { k: NumberLong(1) } });
t('typebits long after update', () => d.u.find({ k: 1 }, { _id: 0, k: 1 }).toArray());
t('unique after update', () => { try { d.u.updateOne({ _id: 2 }, { $set: { k: 1 } }); return 'no error'; } catch (e) { return e.code; } });
t('unique dup double', () => { try { d.u.insertOne({ _id: 3, k: 2.0 }); return 'no error'; } catch (e) { return e.code; } });
d.u.deleteOne({ _id: 1 });
t('after delete', () => d.u.find({ k: { $gte: 0 } }, { _id: 0, k: 1 }).toArray());
t('reinsert freed key', () => { d.u.insertOne({ _id: 4, k: 1 }); return d.u.find({}, { _id: 1, k: 1 }).sort({ k: 1 }).toArray(); });

// collated index: covered strings must not come from collation keys
const cl = d.getCollection('cl');
cl.insertMany([{ _id: 1, s: 'B' }, { _id: 2, s: 'a' }, { _id: 3, s: 'C' }]);
cl.createIndex({ s: 1 }, { collation: { locale: 'en', strength: 2 } });
t('collated covered', () => cl.find({ s: { $gte: 'A' } }, { _id: 0, s: 1 }).collation({ locale: 'en', strength: 2 }).sort({ s: 1 }).toArray());
t('collated eq', () => cl.find({ s: 'b' }, { _id: 1 }).collation({ locale: 'en', strength: 2 }).toArray());
