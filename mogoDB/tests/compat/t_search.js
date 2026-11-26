// $rankFusion differential tests (run by tests/compat/run.sh).
// tm() prints the error message as well as the code, so message text is compared too.
function tm(label, fn) {
  try {
    let r = fn();
    if (r && typeof r.toArray === 'function') r = r.toArray();
    show(label, r);
  } catch (e) {
    const code = e.code !== undefined ? e.code : '?';
    const name = e.codeName || e.name;
    print(label + ' => ERR ' + code + ' ' + name + ' :: ' + e.message);
  }
}

const d = fresh('searchfix_rf');
const docs = d.docs;
docs.insertMany([
  { _id: 1, a: 1, b: 'x', g: 'p' },
  { _id: 2, a: 2, b: 'y', g: 'p' },
  { _id: 3, a: 3, b: 'x', g: 'q' },
  { _id: 4, a: 4, b: 'z', g: 'q' },
  { _id: 5, a: 5, b: 'x', g: 'q' },
]);
d.other.insertMany([{ _id: 9, a: 9 }, { _id: 10, a: 10 }]);

const A = (p) => docs.aggregate(p).toArray();
const rf = (spec) => ({ $rankFusion: spec });
const asc = { $sort: { a: 1 } };
const desc = { $sort: { a: -1 } };
const two = { input: { pipelines: { p1: [asc], p2: [desc] } } };
const SD = { $project: { _id: 1, s: { $meta: 'score' }, sd: { $meta: 'scoreDetails' } } };
const S = { $project: { _id: 1, s: { $meta: 'score' } } };

// ---- successful results ----
t('rf basic', () => A([rf(two)]));
t('rf score', () => A([rf(two), S]));
t('rf scoreDetails', () => A([rf({ ...two, scoreDetails: true }), SD]));
t('rf weights', () => A([rf({ ...two, combination: { weights: { p1: 3 } }, scoreDetails: true }), SD]));
t('rf weight double', () => A([rf({ input: two.input, combination: { weights: { p1: 2.5, p2: 0.5 } } }), S]));
t('rf weight zero', () => A([rf({ input: two.input, combination: { weights: { p1: 0, p2: 0 } } }), S]));
t('rf weight -0', () => A([rf({ input: two.input, combination: { weights: { p1: -0, p2: 0 } } }), S]));
t('rf weight infinity', () => A([rf({ input: { pipelines: { p1: [asc] } }, combination: { weights: { p1: Infinity } } }), S]));
t('rf weight 1e300', () => A([rf({ input: { pipelines: { p1: [asc] } }, combination: { weights: { p1: 1e300 } } }), S]));
t('rf weight empty', () => A([rf({ input: two.input, combination: { weights: {} } }), S]));
t('rf dedupe', () => A([rf({ input: { pipelines: { p1: [asc], p2: [{ $sort: { a: 1 } }] } } }), SD]));
t('rf pipeline name with space', () => A([rf({ input: { pipelines: { 'a b': [asc] } }, scoreDetails: true }), SD]));
t('rf sort later', () => A([rf({ input: { pipelines: { p1: [{ $match: { a: { $gt: 1 } } }, asc, { $skip: 1 }] } } }), S]));
t('rf match sort', () => A([rf({ input: { pipelines: { p1: [{ $match: { g: 'p' } }, asc] } } }), S]));
t('rf sample', () => A([rf({ input: { pipelines: { p1: [{ $sort: { a: 1 } }, { $sample: { size: 3 } }] } } }), { $count: 'n' }]));
t('rf limit after', () => A([rf(two), { $limit: 2 }, S]));
t('rf skip input', () => A([rf({ input: { pipelines: { p1: [asc, { $skip: 2 }] } } }), S]));
t('rf limit input', () => A([rf({ input: { pipelines: { p1: [asc, { $limit: 2 }], p2: [desc] } } }), S]));
t('rf count after', () => A([rf(two), { $count: 'n' }]));
t('rf group after', () => A([rf(two), { $group: { _id: null, n: { $sum: 1 } } }]));
t('rf facet after', () => A([rf(two), { $facet: { x: [{ $count: 'n' }] } }]));
t('rf in facet', () => A([{ $facet: { x: [rf(two), { $sort: { _id: 1 } }] } }]));
t('rf in lookup', () => A([{ $match: { _id: 1 } }, { $lookup: { from: 'other', pipeline: [rf({ input: { pipelines: { p1: [{ $sort: { a: 1 } }] } } })], as: 'x' } }]));
t('rf in unionWith', () => A([{ $unionWith: { coll: 'other', pipeline: [rf({ input: { pipelines: { p1: [{ $sort: { a: 1 } }] } } })] } }, { $sort: { _id: 1 } }]));
t('rf other coll', () => d.other.aggregate([rf({ input: { pipelines: { p1: [{ $sort: { a: 1 } }] } } }), S]).toArray());
t('rf empty coll', () => d.nosuch.aggregate([rf(two)]).toArray());
t('rf meta only', () => A([rf(two)]));

// view with ranked pipelines
d.createView('v_rank', 'docs', [{ $match: { a: { $gte: 2 } } }]);
tm('rf view create with rf', () => d.createView('v_bad', 'docs', [rf(two)]));
t('rf on view', () => d.v_rank.aggregate([rf({ input: { pipelines: { p1: [asc] } } }), S]).toArray());

// ---- errors (message included) ----
tm('rf empty pipelines', () => A([rf({ input: { pipelines: {} } })]));
tm('rf empty pipeline arr', () => A([rf({ input: { pipelines: { p1: [] } } })]));
tm('rf nonranked', () => A([rf({ input: { pipelines: { p1: [{ $match: { a: 1 } }] } } })]));
tm('rf disallowed project', () => A([rf({ input: { pipelines: { p1: [asc, { $project: { a: 1 } }] } } })]));
tm('rf disallowed group', () => A([rf({ input: { pipelines: { p1: [asc, { $group: { _id: null } }] } } })]));
tm('rf disallowed unwind', () => A([rf({ input: { pipelines: { p1: [asc, { $unwind: '$b' }] } } })]));
tm('rf disallowed lookup', () => A([rf({ input: { pipelines: { p1: [asc, { $lookup: { from: 'other', localField: 'a', foreignField: 'a', as: 'x' } }] } } })]));
tm('rf disallowed sortByCount', () => A([rf({ input: { pipelines: { p1: [asc, { $sortByCount: '$b' }] } } })]));
tm('rf disallowed geoNear first', () => A([rf({ input: { pipelines: { p1: [{ $geoNear: { near: [0, 0], distanceField: 'd' } }] } } })]));
tm('rf weight string', () => A([rf({ input: two.input, combination: { weights: { p1: 'a' } } })]));
tm('rf weight null', () => A([rf({ input: two.input, combination: { weights: { p1: null } } })]));
tm('rf weight bool', () => A([rf({ input: two.input, combination: { weights: { p1: true } } })]));
tm('rf weight array', () => A([rf({ input: two.input, combination: { weights: { p1: [1] } } })]));
tm('rf weight negative', () => A([rf({ input: two.input, combination: { weights: { p1: -1 } } })]));
tm('rf weight NaN', () => A([rf({ input: two.input, combination: { weights: { p1: NaN } } })]));
tm('rf weight unknown name', () => A([rf({ input: two.input, combination: { weights: { p3: 1 } } })]));
tm('rf weight typo', () => A([rf({ input: { pipelines: { alpha: [asc], beta: [desc] } }, combination: { weights: { alpah: 1, bta: 2 } } })]));
tm('rf weight extra one', () => A([rf({ input: { pipelines: { p1: [asc] } }, combination: { weights: { p1: 1, p2: 2 } } })]));
tm('rf weight extra two', () => A([rf({ input: { pipelines: { p1: [asc] } }, combination: { weights: { q: 1, r: 1 } } })]));
tm('rf weight extra typo', () => A([rf({ input: { pipelines: { p1: [asc] } }, combination: { weights: { p2: 1 } } })]));
tm('rf unknown field', () => A([rf({ ...two, x: 1 })]));
tm('rf unknown input field', () => A([rf({ input: { pipelines: { p1: [asc] }, x: 1 } })]));
tm('rf unknown comb field', () => A([rf({ ...two, combination: { weights: {}, x: 1 } })]));
tm('rf missing input', () => A([rf({ scoreDetails: true })]));
tm('rf missing pipelines', () => A([rf({ input: {} })]));
tm('rf missing comb weights', () => A([rf({ ...two, combination: {} })]));
tm('rf scalar', () => A([rf(1)]));
tm('rf array arg', () => A([rf([])]));
tm('rf null arg', () => A([rf(null)]));
tm('rf empty spec', () => A([rf({})]));
tm('rf bad scoreDetails', () => A([rf({ ...two, scoreDetails: null })]));
tm('rf input not object', () => A([rf({ input: 1 })]));
tm('rf pipelines not object', () => A([rf({ input: { pipelines: [] } })]));
tm('rf combination not object', () => A([rf({ ...two, combination: 1 })]));
tm('rf weights not object', () => A([rf({ ...two, combination: { weights: [] } })]));
tm('rf pipeline not array', () => A([rf({ input: { pipelines: { p1: { $sort: { a: 1 } } } } })]));
tm('rf pipeline bad elem', () => A([rf({ input: { pipelines: { p1: [1] } } })]));
tm('rf dollar name', () => A([rf({ input: { pipelines: { $p1: [asc] } } })]));
tm('rf dot name', () => A([rf({ input: { pipelines: { 'p.1': [asc] } } })]));
tm('rf empty name', () => A([rf({ input: { pipelines: { '': [asc] } } })]));
tm('rf nested rf', () => A([rf({ input: { pipelines: { p1: [asc, rf(two)] } } })]));
tm('rf nested rf numeric', () => A([rf({ input: { pipelines: { p1: [asc, rf(1)] } } })]));
tm('rf nested rf unknown', () => A([rf({ input: { pipelines: { p1: [asc, rf({ x: 1 })] } } })]));
tm('rf nested rf empty', () => A([rf({ input: { pipelines: { p1: [asc, rf({})] } } })]));
tm('rf nested rf null', () => A([rf({ input: { pipelines: { p1: [rf(null)] } } })]));
tm('rf not first', () => A([{ $match: {} }, rf(two)]));
tm('rf two stages', () => A([rf(two), rf(two)]));
tm('rf stage two keys', () => A([rf({ input: { pipelines: { p1: [{ $sort: { a: 1 }, $limit: 1 }] } } })]));
tm('rf stage empty', () => A([rf({ input: { pipelines: { p1: [{}] } } })]));
tm('rf bogus stage', () => A([rf({ input: { pipelines: { p1: [asc, { $bogus: 1 }] } } })]));
tm('rf sort empty', () => A([rf({ input: { pipelines: { p1: [{ $sort: {} }] } } })]));
tm('rf sort bad', () => A([rf({ input: { pipelines: { p1: [{ $sort: 1 }] } } })]));
tm('rf sort meta', () => A([rf({ input: { pipelines: { p1: [{ $sort: { x: { $meta: 'textScore' } } }] } } })]));
tm('rf sort order', () => A([rf({ input: { pipelines: { p1: [{ $sort: { a: 2 } }] } } })]));
tm('rf limit 0', () => A([rf({ input: { pipelines: { p1: [asc, { $limit: 0 }] } } })]));
tm('rf limit neg', () => A([rf({ input: { pipelines: { p1: [asc, { $limit: -1 }] } } })]));
tm('rf limit str', () => A([rf({ input: { pipelines: { p1: [asc, { $limit: 'a' }] } } })]));
tm('rf skip neg', () => A([rf({ input: { pipelines: { p1: [asc, { $skip: -1 }] } } })]));
tm('rf match bad op', () => A([rf({ input: { pipelines: { p1: [asc, { $match: { a: { $bogus: 1 } } }] } } })]));
tm('rf search first', () => A([rf({ input: { pipelines: { p1: [{ $search: { index: 'default', text: { query: 'x', path: 'b' } } }] } } })]));
tm('rf vectorSearch first', () => A([rf({ input: { pipelines: { p1: [{ $vectorSearch: { index: 'v', path: 'e', queryVector: [1], limit: 1 } }] } } })]));
tm('rf search after sort', () => A([rf({ input: { pipelines: { p1: [asc, { $search: { index: 'default', text: { query: 'x', path: 'b' } } }] } } })]));
tm('rf meta score no rf', () => A([{ $project: { s: { $meta: 'score' } } }]));
tm('rf meta sd no details', () => A([rf(two), { $project: { sd: { $meta: 'scoreDetails' } } }]));
tm('rf meta bogus', () => A([rf(two), { $project: { s: { $meta: 'bogus' } } }]));
tm('rf nested pipeline rf valid', () => A([rf({ input: { pipelines: { p1: [rf(two)] } } })]));
tm('rf $sort then $rankFusion', () => A([rf({ input: { pipelines: { p1: [asc, rf(null)] } } })]));
tm('rf $sort then $bogus', () => A([rf({ input: { pipelines: { p1: [asc, { $bogus: 1 }] } } })]));
tm('rf $sort then $project', () => A([rf({ input: { pipelines: { p1: [asc, { $project: { a: 1 } }] } } })]));
tm('rf $sort then $setWindowFields', () => A([rf({ input: { pipelines: { p1: [asc, { $setWindowFields: { sortBy: { a: 1 }, output: {} } }] } } })]));
tm('rf $sort then $densify', () => A([rf({ input: { pipelines: { p1: [asc, { $densify: { field: 'a', range: { step: 1, bounds: 'full' } } }] } } })]));
tm('rf $sort then $out', () => A([rf({ input: { pipelines: { p1: [asc, { $out: 'x' }] } } })]));
tm('rf $sort then $merge', () => A([rf({ input: { pipelines: { p1: [asc, { $merge: { into: 'x' } }] } } })]));
tm('rf $sort then $graphLookup', () => A([rf({ input: { pipelines: { p1: [asc, { $graphLookup: { from: 'other', startWith: '$a', connectFromField: 'a', connectToField: 'a', as: 'g' } }] } } })]));
tm('rf $sort then $bucket', () => A([rf({ input: { pipelines: { p1: [asc, { $bucket: { groupBy: '$a', boundaries: [0, 10] } }] } } })]));
tm('rf $sort then $bucketAuto', () => A([rf({ input: { pipelines: { p1: [asc, { $bucketAuto: { groupBy: '$a', buckets: 2 } }] } } })]));
tm('rf $sort then $redact', () => A([rf({ input: { pipelines: { p1: [asc, { $redact: '$$KEEP' }] } } })]));
tm('rf $sort then $replaceRoot', () => A([rf({ input: { pipelines: { p1: [asc, { $replaceRoot: { newRoot: '$b' } }] } } })]));
tm('rf $sort then $sortByCount', () => A([rf({ input: { pipelines: { p1: [asc, { $sortByCount: '$b' }] } } })]));
tm('rf $sort then $count', () => A([rf({ input: { pipelines: { p1: [asc, { $count: 'n' }] } } })]));
tm('rf $sort then $facet', () => A([rf({ input: { pipelines: { p1: [asc, { $facet: { x: [] } }] } } })]));
tm('rf $sort then $unionWith', () => A([rf({ input: { pipelines: { p1: [asc, { $unionWith: 'other' }] } } })]));
tm('rf $sort then $addFields', () => A([rf({ input: { pipelines: { p1: [asc, { $addFields: { z: 1 } }] } } })]));
tm('rf $sort then $set', () => A([rf({ input: { pipelines: { p1: [asc, { $set: { z: 1 } }] } } })]));
tm('rf $sort then $unset', () => A([rf({ input: { pipelines: { p1: [asc, { $unset: 'b' }] } } })]));
tm('rf $sort then $group', () => A([rf({ input: { pipelines: { p1: [asc, { $group: { _id: null } }] } } })]));
tm('rf $sort then $geoNear', () => A([rf({ input: { pipelines: { p1: [asc, { $geoNear: { near: [0, 0], distanceField: 'd' } }] } } })]));
tm('rf $sort then $match', () => A([rf({ input: { pipelines: { p1: [asc, { $match: { a: 1 } }] } } })]));
tm('rf view of rank pipeline', () => d.v_rank.aggregate([rf({ input: { pipelines: { p1: [{ $match: { a: 2 } }] } } })]).toArray());
tm('rf view with $rankFusion inside view', () => d.v_bad.aggregate([{ $count: 'n' }]).toArray());

print('PASS-CHECK-DONE');
