// Differential tests for $convert / $toX conversion expressions in src/agg/expr.rs.
const d = fresh('compat_convert');
const c = d.c;
c.insertMany([
  { _id: 1, s: '101', n: 5, dbl: 2.5, big: NumberLong('4294967296'), dec: NumberDecimal('12.5'), b: true, nul: null, dt: new Date(1000) },
  { _id: 2, s: '  7 ', n: -3, dbl: -0.0, big: NumberLong(7), dec: NumberDecimal('-1'), b: false },
]);

// Evaluate one expression against doc _id:1 (or the given _id) and return its value.
const P = (expr, id = 1) => c.aggregate([{ $match: { _id: id } }, { $project: { _id: 0, r: expr } }]).toArray();
const E = (label, expr, id) => t(label, () => P(expr, id));

// ---- $convert: basic targets
E('cv int from str', { $convert: { input: '42', to: 'int' } });
E('cv int from str bad', { $convert: { input: 'x', to: 'int' } });
E('cv int from str ws', { $convert: { input: '$s', to: 'int' } });
E('cv int ws ws', { $convert: { input: '  7 ', to: 'int' } });
E('cv int from dbl', { $convert: { input: 2.9, to: 'int' } });
E('cv int from neg dbl', { $convert: { input: -2.9, to: 'int' } });
E('cv int overflow', { $convert: { input: 2147483648, to: 'int' } });
E('cv int overflow onError', { $convert: { input: 2147483648, to: 'int', onError: 'bad' } });
E('cv int str overflow', { $convert: { input: '2147483648', to: 'int' } });
E('cv int dbl 2^63', { $convert: { input: 9223372036854775808, to: 'int' } });
E('cv int dec overflow', { $convert: { input: NumberDecimal('2147483648'), to: 'int' } });
E('cv long from str', { $convert: { input: '9223372036854775807', to: 'long' } });
E('cv long from dbl', { $convert: { input: 1e10, to: 'long' } });
E('cv long from big', { $convert: { input: '$big', to: 'long' } });
E('cv long bool', { $convert: { input: true, to: 'long' } });
E('cv long date', { $convert: { input: '$dt', to: 'long' } });
E('cv double from str', { $convert: { input: '1.5e3', to: 'double' } });
E('cv double from str bad', { $convert: { input: 'abc', to: 'double' } });
E('cv double from str inf', { $convert: { input: 'Infinity', to: 'double' } });
E('cv double from str nan', { $convert: { input: 'NaN', to: 'double' } });
E('cv double from str 1e400', { $convert: { input: '1e400', to: 'double' } });
E('cv double from bool', { $convert: { input: true, to: 'double' } });
E('cv double from date', { $convert: { input: '$dt', to: 'double' } });
E('cv dec from str', { $convert: { input: '1.25', to: 'decimal' } });
E('cv dec from str bad', { $convert: { input: 'x', to: 'decimal' } });
E('cv dec from str ws', { $convert: { input: ' 1', to: 'decimal' } });
E('cv dec from dbl', { $convert: { input: 0.5, to: 'decimal' } });
E('cv dec from int', { $convert: { input: 5, to: 'decimal' } });
E('cv dec from long', { $convert: { input: NumberLong(5), to: 'decimal' } });
E('cv dec from dec', { $convert: { input: '$dec', to: 'decimal' } });
E('cv str from int', { $convert: { input: 5, to: 'string' } });
E('cv str from dbl', { $convert: { input: 2.5, to: 'string' } });
E('cv str from bool', { $convert: { input: true, to: 'string' } });
E('cv str from dec', { $convert: { input: '$dec', to: 'string' } });
E('cv str from date', { $convert: { input: '$dt', to: 'string' } });
E('cv str from oid', { $convert: { input: ObjectId('0102030405060708090a0b0c'), to: 'string' } });
E('cv bool from int', { $convert: { input: 0, to: 'bool' } });
E('cv bool from str', { $convert: { input: '', to: 'bool' } });
E('cv bool from null', { $convert: { input: null, to: 'bool' } });
E('cv bool from dbl nan', { $convert: { input: NaN, to: 'bool' } });
E('cv bool from dec zero', { $convert: { input: NumberDecimal('0'), to: 'bool' } });
E('cv bool from arr', { $convert: { input: [1], to: 'bool' } });
E('cv bool from bindata', { $convert: { input: BinData(0, 'AQID'), to: 'bool' } });
E('cv date from str', { $convert: { input: '2020-01-02T03:04:05Z', to: 'date' } });
E('cv date from str bad', { $convert: { input: 'nope', to: 'date' } });
E('cv date from long', { $convert: { input: NumberLong(1000), to: 'date' } });
E('cv date from dbl', { $convert: { input: 1000.0, to: 'date' } });
E('cv date from int', { $convert: { input: 1000, to: 'date' } });
E('cv date from bool', { $convert: { input: true, to: 'date' } });
E('cv oid from str', { $convert: { input: '0102030405060708090a0b0c', to: 'objectId' } });
E('cv oid from str bad', { $convert: { input: 'zz', to: 'objectId' } });
E('cv oid from int', { $convert: { input: 5, to: 'objectId' } });

// ---- $convert: numeric type codes
E('cv code 1', { $convert: { input: '2.5', to: 1 } });
E('cv code 2', { $convert: { input: 5, to: 2 } });
E('cv code 5', { $convert: { input: '$dt', to: 5 } });
E('cv code 7', { $convert: { input: '0102030405060708090a0b0c', to: 7 } });
E('cv code 8', { $convert: { input: 0, to: 8 } });
E('cv code 9', { $convert: { input: 1000, to: 9 } });
E('cv code 16', { $convert: { input: '5', to: 16 } });
E('cv code 18', { $convert: { input: '5', to: 18 } });
E('cv code 19', { $convert: { input: '5', to: 19 } });
E('cv code 20', { $convert: { input: 5, to: 20 } });
E('cv code 22', { $convert: { input: 5, to: 22 } });
E('cv code 23', { $convert: { input: 5, to: 23 } });
E('cv code 255', { $convert: { input: 5, to: 255 } });
E('cv code 3', { $convert: { input: 5, to: 3 } });
E('cv code 4', { $convert: { input: 5, to: 4 } });
E('cv code 6', { $convert: { input: 5, to: 6 } });
E('cv code 10', { $convert: { input: 5, to: 10 } });
E('cv code 0', { $convert: { input: 5, to: 0 } });
E('cv code -1', { $convert: { input: 5, to: -1 } });
E('cv code 127', { $convert: { input: 5, to: 127 } });
E('cv code 7 int', { $convert: { input: 5, to: 7 } });
E('cv code 9 int', { $convert: { input: 5, to: 9 } });
E('cv code 17', { $convert: { input: 5, to: 17 } });

// ---- $convert: to names
E('cv to null', { $convert: { input: 5, to: null } });
E('cv to missing', { $convert: { input: 5, to: '$nope' } });
E('cv to bad name', { $convert: { input: 5, to: 'bogus' } });
E('cv to object name', { $convert: { input: 5, to: 'object' } });
E('cv to array name', { $convert: { input: 5, to: 'array' } });
E('cv to regex name', { $convert: { input: 5, to: 'regex' } });
E('cv to arr', { $convert: { input: 5, to: [1] } });
E('cv to bool value', { $convert: { input: 5, to: true } });

// ---- $convert: nullish input
E('cv null input', { $convert: { input: null, to: 'int' } });
E('cv missing input', { $convert: { input: '$nope', to: 'int' } });
E('cv null onNull', { $convert: { input: null, to: 'int', onNull: 'dflt' } });
E('cv missing onNull', { $convert: { input: '$nope', to: 'int', onNull: 'dflt' } });
E('cv onNull with value', { $convert: { input: 3, to: 'int', onNull: 'dflt' } });
E('cv onError with value', { $convert: { input: 'x', to: 'int', onError: 'dflt' } });
E('cv onError null', { $convert: { input: 'x', to: 'int', onError: null } });
E('cv unknown field', { $convert: { input: 3, to: 'int', q: 1 } });
E('cv missing to', { $convert: { input: 3 } });
E('cv missing input key', { $convert: { to: 'int' } });
E('cv scalar arg', { $convert: 5 });
E('cv array arg', { $convert: [5, 'int'] });

// ---- $convert: base (string <-> int)
E('cv base 2 str', { $convert: { input: '101', to: 'int', base: 2 } });
E('cv base 8 str', { $convert: { input: '101', to: 'int', base: 8 } });
E('cv base 16 str', { $convert: { input: '101', to: 'int', base: 16 } });
E('cv base 10 str', { $convert: { input: '101', to: 'int', base: 10 } });
E('cv base 2 long', { $convert: { input: '101', to: 'long', base: 2 } });
E('cv base 16 bad', { $convert: { input: 'zz', to: 'int', base: 16 } });
E('cv base 2 double', { $convert: { input: 2.5, to: 'int', base: 2 } });
E('cv base 1', { $convert: { input: '101', to: 'int', base: 1 } });
E('cv base 3', { $convert: { input: '101', to: 'int', base: 3 } });
E('cv base 36', { $convert: { input: '101', to: 'int', base: 36 } });
E('cv base 0', { $convert: { input: '101', to: 'int', base: 0 } });
E('cv base -1', { $convert: { input: '101', to: 'int', base: -1 } });
E('cv base 16.5', { $convert: { input: '101', to: 'int', base: 16.5 } });
E('cv base null', { $convert: { input: '101', to: 'int', base: null } });
E('cv base str', { $convert: { input: '101', to: 'int', base: 'x' } });
E('cv int to str base 2', { $convert: { input: 5, to: 'string', base: 2 } });
E('cv int to str base 8', { $convert: { input: 5, to: 'string', base: 8 } });
E('cv int to str base 16', { $convert: { input: 5, to: 'string', base: 16 } });
E('cv int to str base 16 neg', { $convert: { input: -255, to: 'string', base: 16 } });
E('cv int to str base 10', { $convert: { input: 5, to: 'string', base: 10 } });
E('cv long to str base 2', { $convert: { input: NumberLong(5), to: 'string', base: 2 } });
E('cv base 2 bits', { $convert: { input: '1111111111111111111111111111111111111111111', to: 'double', base: 2 } });
E('cv base 2 41', { $convert: { input: '10000000000000000000000000000000000000000', to: 'double', base: 2 } });

// ---- $convert: whitespace / strict numeric parsing
E('cv ws int', { $convert: { input: ' 5', to: 'int' } });
E('cv ws dbl', { $convert: { input: '5 ', to: 'double' } });
E('cv ws long', { $convert: { input: ' 5', to: 'long' } });
E('cv ws dec', { $convert: { input: ' 5', to: 'decimal' } });
E('cv plus int', { $convert: { input: '+5', to: 'int' } });
E('cv hex str int', { $convert: { input: '0x10', to: 'int' } });
E('cv exp str int', { $convert: { input: '1e3', to: 'int' } });
E('cv exp str long', { $convert: { input: '1e3', to: 'long' } });
E('cv empty str int', { $convert: { input: '', to: 'int' } });
E('cv empty str dbl', { $convert: { input: '', to: 'double' } });

// ---- $convert: binData
E('cv bin from str', { $convert: { input: 'AQID', to: 'binData', format: 'base64' } });
E('cv bin from str hex', { $convert: { input: '010203', to: 'binData', format: 'hex' } });
E('cv bin from str base64 bad', { $convert: { input: 'x!', to: 'binData', format: 'base64' } });
E('cv bin from str no format', { $convert: { input: 'AQID', to: 'binData' } });
E('cv bin from str auto', { $convert: { input: 'AQID', to: 'binData', format: 'auto' } });
E('cv bin from str bad format', { $convert: { input: 'AQID', to: 'binData', format: 'nope' } });
E('cv bin from str format int', { $convert: { input: 'AQID', to: 'binData', format: 5 } });
E('cv bin from int', { $convert: { input: 5, to: 'binData' } });
E('cv bin from int big', { $convert: { input: 5, to: 'binData', byteOrder: 'big' } });
E('cv bin from int format', { $convert: { input: 5, to: 'binData', format: 'base64' } });
E('cv bin from dbl', { $convert: { input: -0.0, to: 'binData' } });
E('cv bin from nan', { $convert: { input: NaN, to: 'binData' } });
E('cv bin from long', { $convert: { input: NumberLong(5), to: 'binData' } });
E('cv bin from bool', { $convert: { input: true, to: 'binData' } });
E('cv bin byteOrder bad', { $convert: { input: 5, to: 'binData', byteOrder: 'middle' } });
E('cv bin byteOrder int', { $convert: { input: 5, to: 'binData', byteOrder: 1 } });
E('cv bin byteOrder null', { $convert: { input: 5, to: 'binData', byteOrder: null } });
E('cv bin byteOrder little', { $convert: { input: 5, to: 'binData', byteOrder: 'little' } });
E('cv bin subtype 0', { $convert: { input: 'AQID', to: 'binData', format: 'base64', subtype: 0 } });
E('cv bin subtype 4', { $convert: { input: 'AQID', to: 'binData', format: 'base64', subtype: 4 } });
E('cv bin subtype bad', { $convert: { input: 'AQID', to: 'binData', format: 'base64', subtype: 300 } });
E('cv bin subtype str', { $convert: { input: 'AQID', to: 'binData', format: 'base64', subtype: 'x' } });
E('cv bin uuid', { $convert: { input: '00112233-4455-6677-8899-aabbccddeeff', to: 'binData', format: 'uuid' } });
E('cv bin uuid bad', { $convert: { input: 'zz', to: 'binData', format: 'uuid' } });
E('cv bin to object', { $convert: { input: 'AQID', to: { type: 'binData', format: 'base64' } } });
E('cv bin to object no type', { $convert: { input: 'AQID', to: { format: 'base64' } } });
E('cv bin to object bad type', { $convert: { input: 'AQID', to: { type: 'nope' } } });
E('cv bin to object subtype', { $convert: { input: 'AQID', to: { type: 'binData', format: 'base64', subtype: 5 } } });
E('cv bin to array', { $convert: { input: 'AQID', to: ['binData'] } });
E('cv bin to bin input', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'binData' } });
E('cv bin string out base64', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'string', format: 'base64' } });
E('cv bin string out hex', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'string', format: 'hex' } });
E('cv bin string out base64url', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'string', format: 'base64url' } });
E('cv bin string out utf8', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'string', format: 'utf8' } });
E('cv bin string out auto', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'string', format: 'auto' } });
E('cv bin string out no format', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'string' } });
E('cv bin string out bad format', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'string', format: 'nope' } });
E('cv bin string out uuid', { $convert: { input: { $literal: BinData(4, 'ABEiM0RVZneImaq7zN3u/w==') }, to: 'string', format: 'uuid' } });
E('cv bin string out base64 with format', { $convert: { input: { $literal: BinData(0, 'v7+/') }, to: 'string', format: 'base64' } });
E('cv bin to bool', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'bool' } });
E('cv bin to int 1', { $convert: { input: { $literal: BinData(0, 'BQ==') }, to: 'int' } });
E('cv bin to int 2 little', { $convert: { input: { $literal: BinData(0, 'AQI=') }, to: 'int' } });
E('cv bin to int 2 big', { $convert: { input: { $literal: BinData(0, 'AQI=') }, to: 'int', byteOrder: 'big' } });
E('cv bin to int 4', { $convert: { input: { $literal: BinData(0, 'BQAAAA==') }, to: 'int' } });
E('cv bin to int 3', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'int' } });
E('cv bin to long 8', { $convert: { input: { $literal: BinData(0, 'AQAAAAAAAAA=') }, to: 'long' } });
E('cv bin to long 2', { $convert: { input: { $literal: BinData(0, 'AQI=') }, to: 'long' } });
E('cv bin to long 3', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'long' } });
E('cv bin to dbl 8', { $convert: { input: { $literal: BinData(0, 'AAAAAAAA8D8=') }, to: 'double' } });
E('cv bin to dbl 4', { $convert: { input: { $literal: BinData(0, 'AACAPw==') }, to: 'double' } });
E('cv bin to dbl 3', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'double' } });
E('cv bin to dec', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'decimal' } });
E('cv bin to date', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'date' } });
E('cv bin to oid', { $convert: { input: { $literal: BinData(0, 'AQID') }, to: 'objectId' } });
E('cv bin to int with subtype', { $convert: { input: { $literal: BinData(0, 'BQAAAA==') }, to: 'int', subtype: 0 } });

// ---- $convert: binData round trip and uuid
E('cv int bin round', { $convert: { input: { $convert: { input: 513, to: 'binData', byteOrder: 'big' } }, to: 'int', byteOrder: 'big' } });
E('cv bin from neg int', { $convert: { input: -5, to: 'binData' } });
E('cv bin from long big', { $convert: { input: NumberLong('-2'), to: 'binData', byteOrder: 'big' } });

// ---- $toX shorthands
E('toInt str', { $toInt: '42' });
E('toInt bad', { $toInt: 'x' });
E('toInt null', { $toInt: null });
E('toInt missing', { $toInt: '$nope' });
E('toInt overflow', { $toInt: 2147483648 });
E('toInt dbl', { $toInt: 3.7 });
E('toInt bool', { $toInt: true });
E('toInt arr', { $toInt: [1] });
E('toInt date', { $toInt: '$dt' });
E('toInt two args', { $toInt: [1, 2] });
E('toLong str', { $toLong: '9223372036854775807' });
E('toLong overflow', { $toLong: '9223372036854775808' });
E('toLong dbl', { $toLong: 1e10 });
E('toLong date', { $toLong: '$dt' });
E('toLong null', { $toLong: null });
E('toDouble str', { $toDouble: '1.5' });
E('toDouble bad', { $toDouble: 'x' });
E('toDouble bool', { $toDouble: false });
E('toDouble null', { $toDouble: null });
E('toDecimal str', { $toDecimal: '1.25' });
E('toDecimal bad', { $toDecimal: 'x' });
E('toDecimal dbl', { $toDecimal: 0.5 });
E('toDecimal null', { $toDecimal: null });
E('toString int', { $toString: 5 });
E('toString dbl', { $toString: 2.5 });
E('toString bool', { $toString: true });
E('toString null', { $toString: null });
E('toString date', { $toString: '$dt' });
E('toBool int', { $toBool: 1 });
E('toBool str', { $toBool: 'x' });
E('toBool null', { $toBool: null });
E('toBool zero', { $toBool: 0 });
E('toDate str', { $toDate: '2020-01-02T03:04:05Z' });
E('toDate bad', { $toDate: 'nope' });
E('toDate int', { $toDate: 1000 });
E('toDate long', { $toDate: NumberLong(1000) });
E('toDate null', { $toDate: null });
E('toObjectId str', { $toObjectId: '0102030405060708090a0b0c' });
E('toObjectId bad', { $toObjectId: 'zz' });
E('toObjectId null', { $toObjectId: null });
E('toObjectId int', { $toObjectId: 5 });
E('toInt-dbl-neg', { $toInt: -2.9 });
E('toString missing', { $toString: '$nope' });
E('toLong ws', { $toLong: ' 5' });
E('toInt str neg', { $toInt: '-7' });
E('toInt str plus', { $toInt: '+7' });
E('toDouble nan str', { $toDouble: 'NaN' });
E('toDouble inf str', { $toDouble: '-Infinity' });
E('toDecimal nan', { $toDecimal: NaN });
E('toDecimal inf', { $toDecimal: Infinity });
E('toInt nan', { $toInt: NaN });
E('toLong inf', { $toLong: Infinity });
E('toInt dec', { $toInt: NumberDecimal('3.9') });
E('toLong dec overflow', { $toLong: NumberDecimal('1e30') });
E('toInt dec nan', { $toInt: NumberDecimal('NaN') });
E('toString oid', { $toString: ObjectId('0102030405060708090a0b0c') });
E('toBool dec nan', { $toBool: NumberDecimal('NaN') });
E('toBool date', { $toBool: '$dt' });
E('toString neg zero', { $toString: -0.0 });
E('toBinData int', { $convert: { input: 0, to: 'binData', format: 'base64' } });

// ---- $toBinData-like via $convert with int and format interplay
E('cv int bin base64 result', { $convert: { input: 5, to: 'binData', format: 'base64', byteOrder: 'big' } });
E('cv str bin uuid subtype override', { $convert: { input: '00112233-4455-6677-8899-aabbccddeeff', to: 'binData', format: 'uuid', subtype: 3 } });

// ---- exact decimal bounds for long/int
E('dec long hi', { $toLong: NumberDecimal('9223372036854775807.99999') });
E('dec long lo', { $toLong: NumberDecimal('-9223372036854775808.9') });
E('dec long over lo', { $toLong: NumberDecimal('-9223372036854775809.9') });
E('dec long over hi', { $toLong: NumberDecimal('9223372036854775808.0') });
E('dec long sci over', { $toLong: NumberDecimal('9.223372036854775808e18') });
E('dec int hi', { $toInt: NumberDecimal('2147483647.99999') });
E('dec int over', { $toInt: NumberDecimal('2147483648.0') });
E('dec int neg over', { $toInt: NumberDecimal('-2147483649.9') });
E('dec int neg ok', { $toInt: NumberDecimal('-2147483648.0') });
E('dec int tiny neg', { $toInt: NumberDecimal('-0.9') });

// ---- vector BinData from arrays and back
E('vec bits', { $convert: { input: [1, 0, 1, 0, 1, 0, 1, 0, 1], to: 'binData' } });
E('vec int8', { $convert: { input: [1, 2], to: 'binData' } });
E('vec float', { $convert: { input: [1.5], to: 'binData' } });
E('vec float be', { $convert: { input: [1.5], to: 'binData', byteOrder: 'big' } });
E('vec int8 128', { $convert: { input: [128], to: 'binData' } });
E('vec long elem', { $convert: { input: [NumberLong(3)], to: 'binData' } });
E('vec bool', { $convert: { input: [true], to: 'binData' } });
E('vec back int8', { $convert: { input: { $convert: { input: [1, 2], to: 'binData' } }, to: 'array' } });
E('vec back float', { $convert: { input: { $convert: { input: [1.5], to: 'binData' } }, to: 'array' } });
E('vec back bits', { $convert: { input: { $convert: { input: [1, 0, 1], to: 'binData' } }, to: 'array' } });
E('vec back bad dtype', { $convert: { input: BinData(9, 'AQI='), to: 'array' } });
E('vec back pad', { $convert: { input: BinData(9, 'EAg='), to: 'array' } });
E('vec back one byte', { $convert: { input: BinData(9, 'AQ=='), to: 'array' } });
E('vec back float len', { $convert: { input: BinData(9, 'JwAA'), to: 'array' } });
E('vec back subtype 0', { $convert: { input: BinData(0, 'AQI='), to: 'array' } });
E('vec binary subtype mismatch', { $convert: { input: BinData(9, 'AQI='), to: 'binData' } });

// ---- $map / $filter / $reduce arrayIndexAs and $$IDX
E('map idx default', { $map: { input: [5, 6], as: 'v', in: '$$IDX' } });
E('map arrayIndexAs', { $map: { input: [10, 20], as: 'v', in: { $add: ['$$v', '$$IDX'] }, arrayIndexAs: 'IDX' } });
E('map arrayIndexAs missing', { $map: { input: [10], as: 'v', in: '$$IDX', arrayIndexAs: 'J' } });
E('map unknown arg', { $map: { input: [1], as: 'v', in: '$$v', foo: 1 } });
E('map as eq arrayIndexAs', { $map: { input: [1], as: 'v', in: '$$v', arrayIndexAs: 'v' } });
E('map bad index name', { $map: { input: [1], as: 'v', in: '$$v', arrayIndexAs: 'a-b' } });
E('filter arrayIndexAs', { $filter: { input: [5, 6, 7], as: 'v', cond: { $gt: ['$$IDX', 0] }, arrayIndexAs: 'IDX' } });
E('filter as eq arrayIndexAs', { $filter: { input: [1], as: 'v', cond: true, arrayIndexAs: 'v' } });
E('filter unknown arg', { $filter: { input: [1], as: 'v', cond: true, foo: 1 } });
E('reduce sum', { $reduce: { input: [1, 2, 3], initialValue: 0, in: { $add: ['$$value', '$$this'] } } });
E('reduce no init', { $reduce: { input: [1], in: '$$value' } });
E('reduce non array', { $reduce: { input: 5, initialValue: 0, in: '$$value' } });
E('reduce null input', { $reduce: { input: null, initialValue: 0, in: '$$value' } });
E('reduce unknown arg', { $reduce: { input: [1], initialValue: 0, in: '$$value', foo: 1 } });

// ---- $subtype
E('subtype binary', { $subtype: BinData(4, 'AAAA') });
E('subtype binary 0', { $subtype: BinData(0, 'AAAA') });
E('subtype null', { $subtype: null });
E('subtype non binary', { $subtype: { a: 1 } });
E('subtype no args', { $subtype: [] });
E('subtype two args', { $subtype: [1, 2] });

// ---- $serializeEJSON
E('ser canonical', { $serializeEJSON: { input: { $literal: { a: NumberInt(5), b: NumberLong(7), c: 2.5, d: NumberDecimal('1.5') } } } });
E('ser relaxed', { $serializeEJSON: { input: { $literal: { a: NumberInt(5), b: NumberLong(7), c: 2.5, d: NumberDecimal('1.5') } }, relaxed: true } });
E('ser canonical date', { $serializeEJSON: { input: { $literal: { dt: new Date(1000) } } } });
E('ser relaxed date', { $serializeEJSON: { input: { $literal: { dt: new Date(1000) } }, relaxed: true } });
E('ser relaxed date neg', { $serializeEJSON: { input: { $literal: { dt: new Date(-1000) } }, relaxed: true } });
E('ser nan inf', { $serializeEJSON: { input: { $literal: { n: NaN, i: Infinity, ni: -Infinity } } } });
E('ser oid regex', { $serializeEJSON: { input: { $literal: { o: ObjectId('65a1b2c3d4e5f60718293a4b'), r: /ab/i } } } });
E('ser binary uuid', { $serializeEJSON: { input: { $literal: { b: BinData(0, 'AQID'), u: UUID('00010203-0405-0607-0809-0a0b0c0d0e0f') } } } });
E('ser minmax', { $serializeEJSON: { input: { $literal: { lo: MinKey(), hi: MaxKey() } } } });
E('ser timestamp', { $serializeEJSON: { input: { $literal: { ts: Timestamp(5, 6) } } } });
E('ser relaxed bad', { $serializeEJSON: { input: 1, relaxed: 'x' } });
E('ser missing input', { $serializeEJSON: { relaxed: true } });
E('ser unknown arg', { $serializeEJSON: { input: 1, foo: 1 } });
E('ser onError success', { $serializeEJSON: { input: 1, onError: 'fb' } });

// ---- $deserializeEJSON
E('de numberLong', { $deserializeEJSON: { input: { $literal: { $numberLong: '7' } } } });
E('de numberLong bad', { $deserializeEJSON: { input: { $literal: { $numberLong: 'x' } } } });
E('de numberLong num', { $deserializeEJSON: { input: { $literal: { $numberLong: 7 } } } });
E('de numberInt', { $deserializeEJSON: { input: { $literal: { $numberInt: '5' } } } });
E('de numberDouble nan', { $deserializeEJSON: { input: { $literal: { $numberDouble: 'NaN' } } } });
E('de numberDouble bad', { $deserializeEJSON: { input: { $literal: { $numberDouble: 'abc' } } } });
E('de oid', { $deserializeEJSON: { input: { $literal: { $oid: '65a1b2c3d4e5f60718293a4b' } } } });
E('de oid bad', { $deserializeEJSON: { input: { $literal: { $oid: 'zz' } } } });
E('de uuid bad', { $deserializeEJSON: { input: { $literal: { $uuid: 'nope' } } } });
E('de binary', { $deserializeEJSON: { input: { $literal: { $binary: { base64: 'AQID', subType: '00' } } } } });
E('de binary non object', { $deserializeEJSON: { input: { $literal: { $binary: 'x' } } } });
E('de binary base64 type', { $deserializeEJSON: { input: { $literal: { $binary: { base64: 1, subType: '00' } } } } });
E('de date iso', { $deserializeEJSON: { input: { $literal: { $date: '1970-01-01T00:00:01.000Z' } } } });
E('de date bad', { $deserializeEJSON: { input: { $literal: { $date: 'nope' } } } });
E('de date numberLong', { $deserializeEJSON: { input: { $literal: { $date: { $numberLong: '1000' } } } } });
E('de timestamp', { $deserializeEJSON: { input: { $literal: { $timestamp: { t: 5, i: 6 } } } } });
E('de timestamp i bad', { $deserializeEJSON: { input: { $literal: { $timestamp: { t: 5, i: 'x' } } } } });
E('de regex options bad', { $deserializeEJSON: { input: { $literal: { $regularExpression: { pattern: 'a', options: 1 } } } } });
E('de minKey', { $deserializeEJSON: { input: { $literal: { $minKey: 1 } } } });
E('de nested', { $deserializeEJSON: { input: { $literal: { a: [{ $numberLong: '3' }], b: 'x' } } } });
E('de scalar', { $deserializeEJSON: { input: 5 } });
E('de unknown arg', { $deserializeEJSON: { input: 5, foo: 1 } });
E('de onError', { $deserializeEJSON: { input: { $literal: { $oid: 'zz' } }, onError: 'fb' } });
