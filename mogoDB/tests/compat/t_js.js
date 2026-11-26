// Server-side JavaScript: $where, $function, $accumulator, mapReduce.
// Requires lib.js (run via run.sh). Output is deterministic: results are sorted client-side.

const d = fresh('js2t');
d.items.insertMany([
  { _id: 1, a: 1, b: 'x', v: [1, 2] },
  { _id: 2, a: 2, b: 'y', v: [3] },
  { _id: 3, a: 3, b: 'x', v: [] },
  { _id: 4, a: 0, b: null },
]);
d.upd.insertMany([
  { _id: 1, a: 1 },
  { _id: 2, a: 5 },
  { _id: 3, a: 9 },
]);

function byId(arr) {
  return arr.slice().sort((x, y) => {
    const a = String(x._id);
    const b = String(y._id);
    return a < b ? -1 : a > b ? 1 : 0;
  });
}
function mr(r) {
  return { ok: r.ok, results: byId(r.results) };
}

// ---- $where ----
t('where basic', () => d.items.find({ $where: 'this.a > 1' }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('where obj arg', () => d.items.find({ $where: 'obj.b === "x"' }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('where function form', () => d.items.find({ $where: 'function(){ return this.a === 2; }' }, { _id: 1 }).toArray());
t('where truthy', () => d.items.find({ $where: 'this.b' }, { _id: 1 }).sort({ _id: 1 }).toArray());
t('where number', () => d.items.find({ $where: 5 }).toArray());
t('where empty', () => d.items.find({ $where: '' }).toArray());
t('where syntax', () => d.items.find({ $where: 'this.a >>>' }).toArray());
t('where codewscope', () => d.items.find({ $where: new Code('this.a > 0', { x: 1 }) }).toArray());
t('where loop', () => d.items.find({ $where: 'while(true){}' }).toArray());
t('where update', () => {
  const r = d.upd.updateMany({ $where: 'this.a > 4' }, { $set: { big: true } });
  return { m: r.matchedCount, n: r.modifiedCount };
});
t('where update read', () => sorted(d.upd.find({}, { _id: 1, big: 1 }).toArray()));
t('where delete', () => {
  const r = d.upd.deleteMany({ $where: 'this.a === 1' });
  return { n: r.deletedCount };
});

// ---- $function ----
const fnStage = (spec) => [{ $match: { _id: 1 } }, { $project: { r: { $function: spec } } }];
t('fn basic', () => d.items.aggregate([{ $project: { r: { $function: { body: 'function(a,b){ return a + b; }', args: ['$a', 10], lang: 'js' } } } }, { $sort: { _id: 1 } }]).toArray());
t('fn undefined arg', () => d.items.aggregate(fnStage({ body: 'function(x){ return x === null; }', args: ['$nope'], lang: 'js' })).toArray());
t('fn array return', () => d.items.aggregate(fnStage({ body: 'function(){ return [Array.sum([1,2,3.5]), Array.avg([])]; }', args: [], lang: 'js' })).toArray());
t('fn int64 return', () => d.items.aggregate(fnStage({ body: 'function(){ return 2**40; }', args: [], lang: 'js' })).toArray());
t('fn double return', () => d.items.aggregate(fnStage({ body: 'function(){ return 1.5; }', args: [], lang: 'js' })).toArray());
t('fn objectid bad', () => d.items.aggregate(fnStage({ body: 'function(){ return ObjectId("bad"); }', args: [], lang: 'js' })).toArray());
t('fn long bad', () => d.items.aggregate(fnStage({ body: 'function(){ return NumberLong("9999999999999999999"); }', args: [], lang: 'js' })).toArray());
t('fn decimal bad', () => d.items.aggregate(fnStage({ body: 'function(){ return NumberDecimal("bad"); }', args: [], lang: 'js' })).toArray());
t('fn bindata bad', () => d.items.aggregate(fnStage({ body: 'function(){ return BinData(0,"!!"); }', args: [], lang: 'js' })).toArray());
t('fn throw', () => d.items.aggregate(fnStage({ body: 'function(){ throw new Error("boom"); }', args: [], lang: 'js' })).toArray());
t('fn syntax', () => d.items.aggregate(fnStage({ body: 'function( {', args: [], lang: 'js' })).toArray());
t('fn loop', () => d.items.aggregate(fnStage({ body: 'function(){ while(true){} }', args: [], lang: 'js' })).toArray());
t('fn body missing', () => d.items.aggregate(fnStage({ args: [], lang: 'js' })).toArray());
t('fn body wrong type', () => d.items.aggregate(fnStage({ body: 5, args: [], lang: 'js' })).toArray());
t('fn lang missing', () => d.items.aggregate(fnStage({ body: 'function(){ return 1; }', args: [] })).toArray());
t('fn lang python', () => d.items.aggregate(fnStage({ body: 'function(){ return 1; }', args: [], lang: 'python' })).toArray());
t('fn args missing', () => d.items.aggregate(fnStage({ body: 'function(){ return 1; }', lang: 'js' })).toArray());
t('fn args wrong', () => d.items.aggregate(fnStage({ body: 'function(){ return 1; }', args: 'x', lang: 'js' })).toArray());
t('fn spec string', () => d.items.aggregate(fnStage('bad')).toArray());

// ---- $accumulator ----
const accFull = {
  init: function () { return 0; },
  accumulate: function (s, v) { return s + v; },
  accumulateArgs: ['$a'],
  merge: function (x, y) { return x + y; },
  lang: 'js',
};
function accWithout(key) {
  const o = Object.assign({}, accFull);
  delete o[key];
  return o;
}
function accWith(extra) {
  return Object.assign({}, accFull, extra);
}
const grp = (acc) => [{ $group: { _id: null, s: { $accumulator: acc } } }];

t('acc sum', () => d.items.aggregate(grp(accFull)).toArray());
t('acc initargs', () => d.items.aggregate(grp(accWith({ init: function (x) { return x; }, initArgs: [100] }))).toArray());
t('acc finalize', () => d.items.aggregate(grp(accWith({ finalize: function (s) { return s * 2; } }))).toArray());
t('acc group by b', () => d.items.aggregate([
  { $group: { _id: '$b', s: { $accumulator: accFull } } },
  { $sort: { _id: 1 } },
]).toArray());
t('acc object state', () => d.items.aggregate(grp({
  init: function () { return { n: 0 }; },
  accumulate: function (s, v) { s.n = s.n + 1; return s; },
  accumulateArgs: ['$a'],
  merge: function (x, y) { return { n: x.n + y.n }; },
  finalize: function (s) { return s.n; },
  lang: 'js',
})).toArray());
t('acc spec string', () => d.items.aggregate(grp('bad')).toArray());
t('acc spec number', () => d.items.aggregate(grp(5)).toArray());
t('acc unknown field', () => d.items.aggregate(grp(accWith({ foo: 1 }))).toArray());
t('acc lang python', () => d.items.aggregate(grp(accWith({ lang: 'python' }))).toArray());
t('acc missing init', () => d.items.aggregate(grp(accWithout('init'))).toArray());
t('acc missing accumulate', () => d.items.aggregate(grp(accWithout('accumulate'))).toArray());
t('acc missing merge', () => d.items.aggregate(grp(accWithout('merge'))).toArray());
t('acc missing accumulateArgs', () => d.items.aggregate(grp(accWithout('accumulateArgs'))).toArray());
t('acc init non-code', () => d.items.aggregate(grp(accWith({ init: 5 }))).toArray());
t('acc initargs non-array', () => d.items.aggregate(grp(accWith({ initArgs: 'x' }))).toArray());
t('acc accumulateargs non-array', () => d.items.aggregate(grp(accWith({ accumulateArgs: '$a' }))).toArray());
t('acc in project', () => d.items.aggregate([{ $project: { s: { $accumulator: accFull } } }]).toArray());

// ---- mapReduce ----
const mapXA = function () { emit(this.b, this.a); };
const redSum = function (k, vs) { return Array.sum(vs); };

t('mr inline', () => mr(d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, out: { inline: 1 } })));
t('mr singleton reduce', () => mr(d.runCommand({
  mapReduce: 'items', map: mapXA,
  reduce: function (k, vs) { return { n: vs.length, tag: 'R' }; },
  out: { inline: 1 },
})));
t('mr finalize', () => mr(d.runCommand({
  mapReduce: 'items', map: mapXA, reduce: redSum,
  finalize: function (k, v) { return v * 10; },
  out: { inline: 1 },
})));
t('mr finalize string', () => mr(d.runCommand({
  mapReduce: 'items', map: mapXA,
  reduce: function () { return 'R'; },
  finalize: function (k, v) { return 'F' + v; },
  out: { inline: 1 },
})));
t('mr scope', () => mr(d.runCommand({
  mapReduce: 'items',
  map: function () { emit(this._id, this.a * mult); },
  reduce: function (k, vs) { return vs[0]; },
  scope: { mult: 3 },
  out: { inline: 1 },
})));
t('mr query sort limit', () => mr(d.runCommand({
  mapReduce: 'items',
  map: function () { emit(this._id, 1); },
  reduce: function (k, vs) { return vs[0]; },
  query: { a: { $gt: 0 } },
  sort: { _id: -1 },
  limit: 2,
  out: { inline: 1 },
})));
t('mr query where', () => mr(d.runCommand({
  mapReduce: 'items',
  map: function () { emit(this._id, 1); },
  reduce: function (k, vs) { return vs[0]; },
  query: { $where: 'this.a > 1' },
  out: { inline: 1 },
})));
t('mr int64 emit', () => mr(d.runCommand({
  mapReduce: 'items',
  map: function () { emit(this.b, NumberLong(this.a)); },
  reduce: function (k, vs) { return vs.length; },
  out: { inline: 1 },
})));
t('mr reduce int64 return', () => mr(d.runCommand({
  mapReduce: 'items', map: mapXA,
  reduce: function () { return NumberLong(7); },
  out: { inline: 1 },
})));
t('mr missing collection', () => mr(d.runCommand({ mapReduce: 'nosuch', map: mapXA, reduce: redSum, out: { inline: 1 } })));

t('mr out replace', () => {
  const r = d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, out: 'outrep' });
  return { result: r.result, ok: r.ok };
});
t('mr out replace read', () => sorted(d.outrep.find({}, { _id: 1, value: 1 }).toArray()));
t('mr out db', () => {
  const r = d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, out: { replace: 'outdb', db: 'js2t_o' } });
  return { result: r.result, ok: r.ok };
});
t('mr out db read', () => sorted(d.getSiblingDB('js2t_o').outdb.find({}, { _id: 1, value: 1 }).toArray()));

d.outm.drop();
d.outm.insertMany([{ _id: 'x', value: 'OLD' }, { _id: 'z', value: 'Z' }]);
t('mr out merge', () => {
  const r = d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, out: { merge: 'outm' } });
  return { result: r.result, ok: r.ok };
});
t('mr out merge read', () => sorted(d.outm.find({}, { _id: 1, value: 1 }).toArray()));

d.outr.drop();
d.outr.insertOne({ _id: 'x', value: 100 });
t('mr out reduce', () => {
  const r = d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, out: { reduce: 'outr' } });
  return { result: r.result, ok: r.ok };
});
t('mr out reduce read', () => sorted(d.outr.find({}, { _id: 1, value: 1 }).toArray()));

t('mr out missing', () => d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum }));
t('mr out empty', () => d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, out: {} }));
t('mr out unknown mode', () => d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, out: { bogus: 1 } }));
t('mr no map', () => d.runCommand({ mapReduce: 'items', reduce: redSum, out: { inline: 1 } }));
t('mr map not function', () => d.runCommand({ mapReduce: 'items', map: 5, reduce: redSum, out: { inline: 1 } }));
t('mr map throws', () => d.runCommand({ mapReduce: 'items', map: function () { throw new Error('m'); }, reduce: redSum, out: { inline: 1 } }));
t('mr reduce throws', () => d.runCommand({ mapReduce: 'items', map: mapXA, reduce: function () { throw new Error('r'); }, out: { inline: 1 } }));
t('mr unknown field', () => d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, keeptemp: true, out: { inline: 1 } }));
t('mr namespace', () => d.runCommand({ mapReduce: 5, map: mapXA, reduce: redSum, out: { inline: 1 } }));
t('mr query wrong type', () => d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, query: 'x', out: { inline: 1 } }));
t('mr sort wrong type', () => d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, sort: 'x', out: { inline: 1 } }));
t('mr limit zero', () => d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, limit: 0, out: { inline: 1 } }));
t('mr limit negative', () => d.runCommand({ mapReduce: 'items', map: mapXA, reduce: redSum, limit: -1, out: { inline: 1 } }));
