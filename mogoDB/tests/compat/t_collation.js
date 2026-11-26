// Differential collation tests. Run via tests/compat/run.sh.

const d = fresh('coll_t');
const words = ['banana', 'Apple', 'apple', 'cherry', 'Banana', 'éclair', 'eclair', 'Zebra', 'zebra', 'a-b', 'ab', 'a b', '10', '9', 'Ünïcode', 'unicode', 'résumé', 'resume', 'Résumé'];
d.c.insertMany(words.map((s, i) => ({ _id: i, s: s })));
const vals = (cur) => cur.toArray().map((x) => x.s);
const ids = (cur) => cur.toArray().map((x) => x._id);
const en = (strength) => ({ locale: 'en', strength: strength });

// --- sorting ---
t('sort en s3 asc', () => vals(d.c.find({}).collation(en(3)).sort({ s: 1 })));
t('sort en s3 desc', () => vals(d.c.find({}).collation(en(3)).sort({ s: -1 })));
t('sort en s1', () => vals(d.c.find({}).collation(en(1)).sort({ s: 1, _id: 1 })));
t('sort en s2', () => vals(d.c.find({}).collation(en(2)).sort({ s: 1, _id: 1 })));
t('sort simple', () => vals(d.c.find({}).sort({ s: 1 })));
t('sort fr s2', () => vals(d.c.find({}).collation({ locale: 'fr', strength: 2 }).sort({ s: 1, _id: 1 })));
t('sort fr_CA s2', () => vals(d.c.find({}).collation({ locale: 'fr_CA', strength: 2 }).sort({ s: 1, _id: 1 })));
t('sort en caseFirst upper', () => vals(d.c.find({}).collation({ locale: 'en', caseFirst: 'upper' }).sort({ s: 1 })));
t('sort en caseFirst lower', () => vals(d.c.find({}).collation({ locale: 'en', caseFirst: 'lower' }).sort({ s: 1 })));
t('sort en caseLevel s1', () => vals(d.c.find({}).collation({ locale: 'en', strength: 1, caseLevel: true }).sort({ s: 1, _id: 1 })));
t('sort en numeric', () => vals(d.c.find({ s: { $in: ['10', '9', 'a b', 'ab', 'a-b'] } }).collation({ locale: 'en', numericOrdering: true }).sort({ s: 1 })));
t('sort en non-numeric', () => vals(d.c.find({ s: { $in: ['10', '9', 'a b', 'ab', 'a-b'] } }).collation(en(3)).sort({ s: 1 })));
t('sort en shifted', () => vals(d.c.find({ s: { $in: ['a b', 'ab', 'a-b', 'banana', 'Banana'] } }).collation({ locale: 'en', alternate: 'shifted' }).sort({ s: 1 })));
t('sort en maxVariable space', () => vals(d.c.find({ s: { $in: ['a b', 'ab', 'a-b'] } }).collation({ locale: 'en', alternate: 'shifted', maxVariable: 'space' }).sort({ s: 1 })));

// --- equality / range / in ---
t('eq s2 APPLE', () => ids(d.c.find({ s: 'APPLE' }).collation(en(2))).sort((a, b) => a - b));
t('eq s3 APPLE', () => ids(d.c.find({ s: 'APPLE' }).collation(en(3))));
t('eq s1 resume accent', () => ids(d.c.find({ s: 'RESUME' }).collation(en(1))).sort((a, b) => a - b));
t('eq s2 resume accent', () => ids(d.c.find({ s: 'RESUME' }).collation(en(2))).sort((a, b) => a - b));
t('eq s2 eclair accent', () => ids(d.c.find({ s: 'éclair' }).collation(en(2))).sort((a, b) => a - b));
t('eq s3 eclair accent', () => ids(d.c.find({ s: 'éclair' }).collation(en(3))).sort((a, b) => a - b));
t('in s2', () => ids(d.c.find({ s: { $in: ['BANANA', 'zebra'] } }).collation(en(2))).sort((a, b) => a - b));
t('gt b s3', () => vals(d.c.find({ s: { $gt: 'b' } }).collation(en(3)).sort({ s: 1 })));
t('gt B s2', () => vals(d.c.find({ s: { $gt: 'B' } }).collation(en(2)).sort({ s: 1 })));
t('lte apple s2', () => vals(d.c.find({ s: { $lte: 'APPLE' } }).collation(en(2)).sort({ s: 1 })));
t('ne apple s2', () => ids(d.c.find({ s: { $ne: 'apple' } }).collation(en(2))).length);

// --- distinct / aggregate ---
t('distinct s2', () => d.c.distinct('s', {}, { collation: en(2) }).sort());
t('group s2 count', () => d.c.aggregate([{ $group: { _id: '$s', n: { $sum: 1 } } }], { collation: en(2) }).toArray().map((x) => x.n).sort((a, b) => a - b));
t('group s3 count', () => d.c.aggregate([{ $group: { _id: '$s', n: { $sum: 1 } } }], { collation: en(3) }).toArray().map((x) => x.n).sort((a, b) => a - b));
t('group s1 count', () => d.c.aggregate([{ $group: { _id: '$s', n: { $sum: 1 } } }], { collation: en(1) }).toArray().map((x) => x.n).sort((a, b) => a - b));
t('agg sort s2', () => vals(d.c.aggregate([{ $sort: { s: 1, _id: 1 } }], { collation: en(2) })));
t('agg match s2', () => ids(d.c.aggregate([{ $match: { s: 'EcLaIr' } }], { collation: en(2) })).sort((a, b) => a - b));

// --- writes ---
const w = fresh('coll_w');
w.c.insertMany([{ _id: 1, s: 'APPLE' }, { _id: 2, s: 'apple' }, { _id: 3, s: 'pear' }]);
t('updateMany s2', () => {
  const r = w.c.updateMany({ s: 'Apple' }, { $set: { u: 1 } }, { collation: en(2) });
  return { m: r.matchedCount, n: r.modifiedCount };
});
t('updateMany s3 no match', () => {
  const r = w.c.updateMany({ s: 'Apple' }, { $set: { u: 2 } }, { collation: en(3) });
  return { m: r.matchedCount, n: r.modifiedCount };
});
t('deleteMany s2', () => {
  const r = w.c.deleteMany({ s: 'APPLE' }, { collation: en(2) });
  return r.deletedCount;
});
t('after delete', () => w.c.find().sort({ _id: 1 }).toArray());

// --- collection options ---
t('createCollection en s2', () => {
  w.createCollection('cx', { collation: en(2) });
  return w.getCollectionInfos({ name: 'cx' })[0].options;
});
t('createCollection fr_CA', () => {
  w.createCollection('cfr', { collation: { locale: 'fr_CA' } });
  return w.getCollectionInfos({ name: 'cfr' })[0].options;
});
t('createCollection simple', () => {
  w.createCollection('csimple', { collation: { locale: 'simple' } });
  return w.getCollectionInfos({ name: 'csimple' })[0].options;
});
t('createCollection en defaults', () => {
  w.createCollection('cdef', { collation: { locale: 'en' } });
  return w.getCollectionInfos({ name: 'cdef' })[0].options;
});
t('listCollections collation', () => w.getCollectionInfos({ name: 'cx' })[0].options.collation);

// --- index with collation ---
const x = fresh('coll_i');
x.c.insertMany([{ _id: 1, s: 'Apple' }, { _id: 2, s: 'pear' }, { _id: 3, s: 'Plum' }]);
t('createIndex collation s2', () => x.c.createIndex({ s: 1 }, { collation: en(2) }));
t('index eq s2', () => ids(x.c.find({ s: 'APPLE' }).collation(en(2))));
t('index range s2', () => vals(x.c.find({ s: { $gte: 'pl' } }).collation(en(2)).sort({ s: 1 })));
const u = fresh('coll_u');
u.c.insertMany([{ _id: 1, s: 'x' }, { _id: 2, s: 'X' }]);
t('unique collation dup', () => u.c.createIndex({ s: 1 }, { unique: true, collation: en(2) }));
u.c.deleteOne({ _id: 2 });
t('unique collation insert dup', () => u.c.insertOne({ _id: 3, s: 'X' }));

// --- invalid specs ---
t('bad locale xx', () => d.c.find({}).collation({ locale: 'xx' }).toArray());
t('bad locale en_US', () => d.c.find({}).collation({ locale: 'en_US' }).toArray());
t('bad locale empty', () => d.c.find({}).collation({ locale: '' }).toArray());
t('missing locale', () => d.c.find({}).collation({ strength: 2 }).toArray());
t('locale not string', () => d.c.find({}).collation({ locale: 1 }).toArray());
t('unknown field', () => d.c.find({}).collation({ locale: 'en', foo: 1 }).toArray());
t('strength 0', () => d.c.find({}).collation({ locale: 'en', strength: 0 }).toArray());
t('strength 6', () => d.c.find({}).collation({ locale: 'en', strength: 6 }).toArray());
t('strength string', () => d.c.find({}).collation({ locale: 'en', strength: '3' }).toArray());
t('caseLevel non-bool', () => d.c.find({}).collation({ locale: 'en', caseLevel: 1 }).toArray());
t('caseFirst bad', () => d.c.find({}).collation({ locale: 'en', caseFirst: 'middle' }).toArray());
t('alternate bad', () => d.c.find({}).collation({ locale: 'en', alternate: 'x' }).toArray());
t('version bad', () => d.c.find({}).collation({ locale: 'en', version: 'x' }).toArray());
t('collation not doc', () => d.c.find({}).collation('en').toArray());
t('createCollection bad locale', () => d.createCollection('cbad', { collation: { locale: 'xx' } }));
t('createCollection bad strength', () => d.createCollection('cbad2', { collation: { locale: 'en', strength: 9 } }));
