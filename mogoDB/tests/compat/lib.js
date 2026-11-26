// Shared helpers for differential compatibility tests (run by tests/compat/run.sh).
// Each case prints one line: "<label> => <canonical EJSON>" or "<label> => ERR <code> <codeName>".

function norm(v) {
  if (v === null || v === undefined) return v;
  if (Array.isArray(v)) return v.map(norm);
  if (typeof v === 'object' && v.constructor && v.constructor.name === 'Object') {
    const o = {};
    for (const k of Object.keys(v)) {
      if (['$clusterTime', 'operationTime', 'electionId', 'localTime', 'wallTime', 'clusterTime', 'collectionUUID', 'lsid', 'txnNumber', 'postBatchResumeToken'].includes(k)) continue;
      o[k] = norm(v[k]);
    }
    return o;
  }
  return v;
}

function show(label, v) {
  print(label + ' => ' + EJSON.stringify(norm(v), { relaxed: false }));
}

function t(label, fn) {
  try {
    let r = fn();
    if (r && typeof r.toArray === 'function') r = r.toArray();
    show(label, r);
  } catch (e) {
    const code = e.code !== undefined ? e.code : (e.writeErrors && e.writeErrors[0] ? e.writeErrors[0].code : '?');
    const name = e.codeName || (e.writeErrors && e.writeErrors[0] ? e.writeErrors[0].codeName || '' : e.name);
    print(label + ' => ERR ' + code + ' ' + name);
  }
}

// Order-insensitive comparison helper: sort documents by canonical string.
function sorted(arr) {
  return arr.map((d) => EJSON.stringify(d, { relaxed: false })).sort().map((s) => EJSON.parse(s, { relaxed: false }));
}

function fresh(name) {
  const d = db.getSiblingDB(name);
  d.dropDatabase();
  return d;
}
