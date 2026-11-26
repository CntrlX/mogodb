// Mongoose smoke test: NODE_PATH=<dir with node_modules> node tests/drivers/mongoose_smoke.js [port]
const mongoose = require('mongoose');
const assert = require('assert');

const port = process.argv[2] || '27017';

async function main() {
  await mongoose.connect(`mongodb://127.0.0.1:${port}/mongoose_smoke`);
  await mongoose.connection.dropDatabase();

  const Author = mongoose.model('Author', new mongoose.Schema({ name: { type: String, required: true, unique: true }, born: Date }));
  const Post = mongoose.model('Post', new mongoose.Schema({
    title: { type: String, index: true },
    author: { type: mongoose.Schema.Types.ObjectId, ref: 'Author' },
    tags: [String],
    stats: { views: { type: Number, default: 0 }, likes: Number },
    price: mongoose.Schema.Types.Decimal128,
  }, { timestamps: true }));
  await Author.init();
  await Post.init();

  const ann = await Author.create({ name: 'Ann', born: new Date('1990-01-01') });
  await assert.rejects(Author.create({ name: 'Ann' }), (e) => e.code === 11000);
  await assert.rejects(Author.create({}), (e) => e.name === 'ValidationError');

  await Post.insertMany([
    { title: 'a', author: ann._id, tags: ['x', 'y'], stats: { likes: 3 }, price: '9.99' },
    { title: 'b', author: ann._id, tags: ['y'], stats: { likes: 10 } },
    { title: 'c', tags: [], stats: { likes: 1 } },
  ]);
  const withAuthor = await Post.find({ author: ann._id }).populate('author').sort({ title: 1 }).lean();
  assert.deepStrictEqual(withAuthor.map((p) => [p.title, p.author.name]), [['a', 'Ann'], ['b', 'Ann']]);
  assert.ok(withAuthor[0].createdAt instanceof Date, 'timestamps');
  assert.strictEqual((await Post.findOne({ title: 'a' })).price.toString(), '9.99');

  const updated = await Post.findOneAndUpdate({ title: 'b' }, { $inc: { 'stats.views': 5 }, $push: { tags: 'z' } }, { new: true });
  assert.deepStrictEqual([updated.stats.views, updated.tags], [5, ['y', 'z']]);
  assert.strictEqual(await Post.countDocuments({ tags: 'y' }), 2);
  assert.deepStrictEqual((await Post.distinct('tags')).sort(), ['x', 'y', 'z']);

  const agg = await Post.aggregate([
    { $group: { _id: '$author', likes: { $sum: '$stats.likes' }, n: { $sum: 1 } } },
    { $lookup: { from: 'authors', localField: '_id', foreignField: '_id', as: 'a' } },
    { $project: { likes: 1, n: 1, name: { $first: '$a.name' } } },
    { $sort: { likes: -1 } },
  ]);
  assert.deepStrictEqual(agg.map((g) => [g.name, g.likes, g.n]), [['Ann', 13, 2], [undefined, 1, 1]]);

  const session = await mongoose.startSession();
  await session.withTransaction(async () => {
    await Post.updateMany({}, { $set: { 'stats.views': 100 } }, { session });
    await Author.create([{ name: 'Bob' }], { session });
  });
  await session.endSession();
  assert.strictEqual(await Post.countDocuments({ 'stats.views': 100 }), 3);
  assert.strictEqual(await Author.countDocuments(), 2);

  const idx = await Post.collection.indexes();
  assert.ok(idx.some((i) => i.name === 'title_1'), 'schema index created');
  assert.strictEqual((await Post.deleteMany({ 'stats.likes': { $lt: 5 } })).deletedCount, 2);

  await mongoose.connection.dropDatabase();
  await mongoose.disconnect();
  console.log(`mongoose ${mongoose.version}: all checks passed`);
}

main().catch((e) => {
  console.error('FAIL', e);
  process.exit(1);
});
