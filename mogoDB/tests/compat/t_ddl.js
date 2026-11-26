const d = fresh('compat_ddl');

// collections
t('create', () => d.createCollection('a'));
t('create exists', () => d.createCollection('a'));
t('create capped', () => d.createCollection('cap', { capped: true, size: 100000, max: 3 }));
t('capped trim', () => { for (let i = 0; i < 5; i++) d.cap.insertOne({ _id: i }); return d.cap.find().toArray(); });
t('create validator', () => d.createCollection('v', { validator: { $jsonSchema: { bsonType: 'object', required: ['name'], properties: { name: { bsonType: 'string' }, age: { bsonType: 'int', minimum: 0 } } } } }));
t('validator ok', () => d.v.insertOne({ _id: 1, name: 'x', age: 3 }));
t('validator fail', () => d.v.insertOne({ _id: 2, age: -1 }));
t('validator update fail', () => d.v.updateOne({ _id: 1 }, { $set: { name: 5 } }));
t('view', () => d.createView('av', 'a', [{ $match: { k: { $gt: 1 } } }, { $project: { k: 1 } }]));
d.a.insertMany([{ _id: 1, k: 1 }, { _id: 2, k: 2 }, { _id: 3, k: 3 }]);
t('view find', () => d.av.find({}).sort({ _id: -1 }));
t('view count', () => d.av.countDocuments({ k: 3 }));
t('view insert err', () => d.av.insertOne({ k: 9 }));
t('listCollections', () => d.getCollectionInfos().map((c) => ({ name: c.name, type: c.type, options: c.options, ro: c.info.readOnly })).sort((x, y) => (x.name < y.name ? -1 : 1)));
t('listCollections nameOnly', () => d.runCommand({ listCollections: 1, nameOnly: true, filter: { name: 'a' } }).cursor.firstBatch);
t('collection names', () => d.getCollectionNames().sort());
t('rename', () => d.a.renameCollection('b'));
t('after rename', () => [d.getCollectionNames().includes('a'), d.b.countDocuments()]);
t('rename missing', () => d.nope.renameCollection('x'));
t('drop', () => d.b.drop());
t('drop missing', () => d.runCommand({ drop: 'nonexistent' }));
t('collMod', () => d.runCommand({ collMod: 'v', validationLevel: 'off' }).ok);
t('after collMod insert', () => d.v.insertOne({ _id: 3 }));

// indexes
const i = d.idx;
i.insertMany([{ _id: 1, a: 1, b: 'x', tags: [1, 2] }, { _id: 2, a: 2, b: 'y', tags: [2, 3] }, { _id: 3, a: 3 }]);
t('createIndex', () => i.createIndex({ a: 1 }));
t('createIndex compound', () => i.createIndex({ a: 1, b: -1 }, { name: 'ab' }));
t('createIndex unique', () => i.createIndex({ b: 1 }, { unique: true, sparse: true }));
t('createIndex unique fail', () => i.createIndex({ tags: 1 }, { unique: true }));
t('createIndex multikey', () => i.createIndex({ tags: 1 }));
t('createIndex same', () => i.createIndex({ a: 1 }));
t('createIndex conflict name', () => i.createIndex({ b: -1 }, { name: 'ab' }));
t('createIndex ttl', () => i.createIndex({ created: 1 }, { expireAfterSeconds: 3600 }));
t('createIndex partial', () => i.createIndex({ a: -1 }, { partialFilterExpression: { a: { $gt: 1 } } }));
t('createIndex text', () => i.createIndex({ b: 'text' }));
t('listIndexes', () => i.getIndexes());
t('unique dup', () => i.insertOne({ _id: 9, b: 'x' }));
t('unique sparse missing ok', () => i.insertMany([{ _id: 10 }, { _id: 11 }]));
t('index query', () => i.find({ a: { $gte: 2 } }, { _id: 1 }).sort({ _id: 1 }));
t('index in', () => i.find({ a: { $in: [1, 3] } }, { _id: 1 }).sort({ _id: 1 }));
t('multikey query', () => i.find({ tags: 2 }, { _id: 1 }).sort({ _id: 1 }));
t('hint', () => i.find({ a: 1 }, { _id: 1 }).hint({ a: 1 }));
t('hint bad', () => i.find({}).hint('nope').toArray());
t('explain ixscan', () => i.find({ a: 2 }).explain().queryPlanner.winningPlan.inputStage.stage);
t('explain collscan', () => i.find({ zz: 2 }).explain().queryPlanner.winningPlan.stage);
t('text search', () => i.find({ $text: { $search: 'x' } }, { _id: 1 }));
t('dropIndex', () => i.dropIndex('ab'));
t('dropIndex missing', () => i.dropIndex('ab'));
t('dropIndex id', () => i.dropIndex('_id_'));
t('dropIndexes all', () => i.dropIndexes());
t('listIndexes after', () => i.getIndexes().map((x) => x.name));

// unique compound + updates maintaining index
const u = d.uq;
u.createIndex({ x: 1, y: 1 }, { unique: true });
u.insertMany([{ _id: 1, x: 1, y: 1 }, { _id: 2, x: 1, y: 2 }]);
t('uq update dup', () => u.updateOne({ _id: 2 }, { $set: { y: 1 } }));
t('uq update ok', () => u.updateOne({ _id: 2 }, { $set: { y: 3 } }));
t('uq reuse freed key', () => u.insertOne({ _id: 3, x: 1, y: 2 }));
t('uq find', () => u.find({ x: 1, y: { $gt: 1 } }).sort({ _id: 1 }));

// geo
const g = d.places;
g.insertMany([
  { _id: 1, loc: { type: 'Point', coordinates: [-73.97, 40.77] } },
  { _id: 2, loc: { type: 'Point', coordinates: [-73.88, 40.78] } },
  { _id: 3, loc: { type: 'Point', coordinates: [-118.24, 34.05] } },
]);
g.createIndex({ loc: '2dsphere' });
t('near', () => g.find({ loc: { $near: { $geometry: { type: 'Point', coordinates: [-73.9667, 40.78] }, $maxDistance: 20000 } } }, { _id: 1 }));
t('geoWithin', () => g.find({ loc: { $geoWithin: { $centerSphere: [[-73.97, 40.77], 0.01] } } }, { _id: 1 }));
t('geoNear', () => g.aggregate([{ $geoNear: { near: { type: 'Point', coordinates: [-73.9667, 40.78] }, distanceField: 'dist', spherical: true } }, { $project: { _id: 1 } }]).toArray());

// dbs
t('listDatabases has', () => d.adminCommand({ listDatabases: 1, nameOnly: true }).databases.map((x) => x.name).includes('compat_ddl'));
t('dbStats', () => { const s = d.stats(); return [s.db, s.collections > 0, typeof s.objects]; });
t('collStats', () => { const s = d.runCommand({ collStats: 'uq' }); return [s.ns, s.count, s.nindexes]; });
