// Differential tests for string, regex and date operators implemented in src/agg/expr_ext.rs.
const d = fresh('compat_exprext');
const c = d.c;
c.insertMany([
  { _id: 1, s: 'Hello World', u: 'ÄÖÜ straße Ωμέγα', n: 12, dt: new Date(Date.UTC(2021, 0, 31, 13, 45, 30, 123)), tz: 'America/New_York', off: '+05:30', sp: '  a b  ', ch: 'xxhixx', csv: 'a,b,,c', re: 'ab12cd345', empty: '' },
  { _id: 2, s: null, u: null, n: null, dt: null },
  { _id: 3, s: 'éa', dt: new Date(Date.UTC(2020, 1, 29, 0, 0, 0, 0)), tz: 'Asia/Kolkata', re: 'AAA' },
]);

const P = (expr, id = 1) => c.aggregate([{ $match: { _id: id } }, { $project: { _id: 0, r: expr } }]).toArray();
const E = (label, expr, id) => t(label, () => P(expr, id));

// ---- $concat
E('concat basic', { $concat: ['a', 'b', 'c'] });
E('concat field', { $concat: ['$s', '!'] });
E('concat null', { $concat: ['a', null] });
E('concat null doc2', { $concat: ['$s', 'x'] }, 2);
E('concat empty', { $concat: [] });
E('concat num', { $concat: ['a', 1] });
E('concat scalar', { $concat: 'abc' });
E('concat array arg', { $concat: [['a']] });
E('concat missing', { $concat: ['$nope', 'x'] });
E('concat unicode', { $concat: ['ä', 'ö'] });

// ---- $toLower / $toUpper
E('lower ascii', { $toLower: '$s' });
E('upper ascii', { $toUpper: '$s' });
E('lower unicode', { $toLower: '$u' });
E('upper unicode', { $toUpper: '$u' });
E('upper sharp s', { $toUpper: 'straße' });
E('lower null', { $toLower: '$s' }, 2);
E('lower num', { $toLower: 12 });
E('lower missing', { $toLower: '$nope' });
E('lower arr', { $toLower: ['A'] });
E('lower bool', { $toLower: true });
E('upper date', { $toUpper: '$dt' });
E('lower sigma', { $toLower: 'ΟΔΟΣ' });
E('lower istanbul', { $toLower: 'İ' });

// ---- $strLenBytes / $strLenCP
E('strlenbytes ascii', { $strLenBytes: 'abc' });
E('strlenbytes utf8', { $strLenBytes: '$u' });
E('strlencp utf8', { $strLenCP: '$u' });
E('strlencp empty', { $strLenCP: '' });
E('strlenbytes null', { $strLenBytes: '$s' }, 2);
E('strlencp null', { $strLenCP: '$s' }, 2);
E('strlenbytes num', { $strLenBytes: 1 });
E('strlencp num', { $strLenCP: 1 });
E('strlencp missing', { $strLenCP: '$nope' });
E('strlencp arr', { $strLenCP: ['a'] });
E('strlenbytes emoji', { $strLenBytes: '😀' });
E('strlencp emoji', { $strLenCP: '😀' });

// ---- $substr / $substrBytes / $substrCP
E('substr basic', { $substr: ['$s', 0, 5] });
E('substr neg len', { $substr: ['$s', 0, -1] });
E('substr neg start', { $substr: ['$s', -3, 2] });
E('substr zero len', { $substr: ['$s', 3, 0] });
E('substr past end', { $substr: ['$s', 50, 3] });
E('substr null', { $substr: ['$s', 0, 1] }, 2);
E('substr num', { $substr: [12345, 1, 2] });
E('substr frac', { $substr: ['$s', 1.7, 2.2] });
E('substr bad start', { $substr: ['$s', 'x', 2] });
E('substr arity', { $substr: ['$s', 1] });
E('substr unicode bytes', { $substr: ['$u', 0, 3] });
E('substrbytes basic', { $substrBytes: ['$u', 0, 3] });
E('substrbytes mid char', { $substrBytes: ['$u', 1, 2] });
E('substrbytes into ö', { $substrBytes: ['$u', 0, 4] });
E('substrbytes neg len', { $substrBytes: ['$s', 0, -1] });
E('substrbytes null', { $substrBytes: ['$s', 0, 1] }, 2);
E('substrbytes oob', { $substrBytes: ['$s', 99, 1] });
E('substrbytes bad', { $substrBytes: ['$s', 0.5, 1] });
E('substrbytes neg start', { $substrBytes: ['$s', -1, 1] });
E('substrcp basic', { $substrCP: ['$u', 1, 4] });
E('substrcp null', { $substrCP: ['$s', 0, 1] }, 2);
E('substrcp neg len', { $substrCP: ['$s', 0, -1] });
E('substrcp emoji', { $substrCP: ['a😀b', 1, 1] });
E('substrcp oob', { $substrCP: ['$s', 100, 1] });
E('substrcp arity', { $substrCP: ['$s'] });
E('substrcp num start', { $substrCP: ['$s', '1', 1] });
E('substrcp long', { $substrCP: ['$s', 0, NumberLong(3)] });

// ---- $strcasecmp
E('strcasecmp eq', { $strcasecmp: ['abc', 'ABC'] });
E('strcasecmp lt', { $strcasecmp: ['a', 'B'] });
E('strcasecmp gt', { $strcasecmp: ['b', 'A'] });
E('strcasecmp null', { $strcasecmp: ['$s', 'x'] }, 2);
E('strcasecmp unicode', { $strcasecmp: ['ä', 'Ä'] });
E('strcasecmp num', { $strcasecmp: [1, '1'] });
E('strcasecmp arity', { $strcasecmp: ['a'] });
E('strcasecmp missing', { $strcasecmp: ['$nope', ''] });

// ---- $split
E('split basic', { $split: ['a,b,,c', ','] });
E('split empty sep', { $split: ['abc', ''] });
E('split no match', { $split: ['abc', 'z'] });
E('split null', { $split: ['$s', ','] }, 2);
E('split null sep', { $split: ['a', null] });
E('split num sep', { $split: ['a1b', 1] });
E('split sep num str', { $split: [12, ','] });
E('split unicode', { $split: ['ä-ö', '-'] });
E('split arity', { $split: ['a'] });
E('split missing', { $split: ['$nope', ','] });

// ---- $trim / $ltrim / $rtrim
E('trim default', { $trim: { input: '$sp' } });
E('trim default lit', { $trim: { input: '\t\n x \r\u000b\u000c' } });
E('trim nul', { $trim: { input: '\u0000 x \u0000' } });
E('ltrim default', { $ltrim: { input: '$sp' } });
E('rtrim default', { $rtrim: { input: '$sp' } });
E('trim chars', { $trim: { input: '$ch', chars: 'x' } });
E('trim chars set', { $trim: { input: '$ch', chars: 'xh' } });
E('ltrim chars', { $ltrim: { input: '$ch', chars: 'x' } });
E('rtrim chars', { $rtrim: { input: '$ch', chars: 'x' } });
E('trim null input', { $trim: { input: '$s' } }, 2);
E('trim null chars', { $trim: { input: 'a', chars: null } });
E('trim num input', { $trim: { input: 12 } });
E('trim missing input', { $trim: { input: '$nope' } });
E('trim no input', { $trim: { chars: 'x' } });
E('trim bad chars', { $trim: { input: 'a', chars: 1 } });
E('trim extra field', { $trim: { input: 'a', foo: 1 } });
E('trim unicode chars', { $trim: { input: 'éaé', chars: 'é' } });
E('trim empty chars', { $trim: { input: ' a ', chars: '' } });
E('trim arity', { $trim: 'abc' });
E('trim long chars', { $trim: { input: 'a', chars: 'x'.repeat(4097) } });
E('trim 4096 chars', { $trim: { input: 'x', chars: 'x'.repeat(4096) } });
E('trim bad input type', { $trim: { input: [1] } });

// ---- $indexOfBytes / $indexOfCP
E('ioB basic', { $indexOfBytes: ['$s', 'o'] });
E('ioB start', { $indexOfBytes: ['$s', 'o', 5] });
E('ioB range', { $indexOfBytes: ['$s', 'o', 5, 7] });
E('ioB notfound', { $indexOfBytes: ['$s', 'zz'] });
E('ioB empty sub', { $indexOfBytes: ['$s', ''] });
E('ioB empty sub start', { $indexOfBytes: ['abc', '', 2] });
E('ioB null str', { $indexOfBytes: ['$s', 'o'] }, 2);
E('ioB null sub', { $indexOfBytes: ['abc', null] });
E('ioB bytes utf8', { $indexOfBytes: ['$u', 'straße'] });
E('ioB mid char', { $indexOfBytes: ['$u', 'ß', 1] });
E('ioB neg start', { $indexOfBytes: ['$s', 'o', -1] });
E('ioB start oob', { $indexOfBytes: ['$s', 'o', 100] });
E('ioB end lt start', { $indexOfBytes: ['$s', 'o', 5, 2] });
E('ioB bad start', { $indexOfBytes: ['$s', 'o', 'x'] });
E('ioB arity', { $indexOfBytes: ['$s'] });
E('ioB num', { $indexOfBytes: [123, '2'] });
E('ioCP basic', { $indexOfCP: ['$u', 'ö'] });
E('ioCP start', { $indexOfCP: ['$u', 'a', 3] });
E('ioCP range', { $indexOfCP: ['$u', 'a', 0, 10] });
E('ioCP emoji', { $indexOfCP: ['x😀y', 'y'] });
E('ioCP null', { $indexOfCP: ['$s', 'o'] }, 2);
E('ioCP empty', { $indexOfCP: ['', ''] });
E('ioCP start float', { $indexOfCP: ['abc', 'c', 1.5] });
E('ioCP end neg', { $indexOfCP: ['abc', 'c', 0, -1] });

// ---- $replaceOne / $replaceAll
E('replaceOne basic', { $replaceOne: { input: '$s', find: 'o', replacement: '0' } });
E('replaceAll basic', { $replaceAll: { input: '$s', find: 'o', replacement: '0' } });
E('replaceOne none', { $replaceOne: { input: 'abc', find: 'z', replacement: 'y' } });
E('replaceAll empty find', { $replaceAll: { input: 'abc', find: '', replacement: '-' } });
E('replaceOne empty find', { $replaceOne: { input: 'abc', find: '', replacement: '-' } });
E('replaceOne null input', { $replaceOne: { input: '$s', find: 'o', replacement: '0' } }, 2);
E('replaceAll null find', { $replaceAll: { input: 'abc', find: null, replacement: '0' } });
E('replaceAll null repl', { $replaceAll: { input: 'abc', find: 'b', replacement: null } });
E('replaceOne null input 2', { $replaceOne: { input: null, find: 'a', replacement: 'b' } });
E('replaceOne missing repl', { $replaceOne: { input: 'abc', find: 'b' } });
E('replaceOne missing input', { $replaceOne: { find: 'b', replacement: 'x' } });
E('replaceAll num input', { $replaceAll: { input: 12, find: '1', replacement: 'x' } });
E('replaceAll unknown', { $replaceAll: { input: 'a', find: 'a', replacement: 'b', x: 1 } });
E('replaceAll bad find', { $replaceAll: { input: 'a', find: 1, replacement: 'b' } });
E('replaceAll unicode', { $replaceAll: { input: '$u', find: 'ö', replacement: 'oe' } });
E('replaceAll overlap', { $replaceAll: { input: 'aaaa', find: 'aa', replacement: 'b' } });
E('replaceOne arity', { $replaceOne: 'abc' });
E('replaceAll dollar repl', { $replaceAll: { input: 'a', find: 'a', replacement: '$1' } });

// ---- $regexMatch / $regexFind / $regexFindAll
E('rm basic', { $regexMatch: { input: '$re', regex: '[0-9]+' } });
E('rm nomatch', { $regexMatch: { input: 'abc', regex: '[0-9]' } });
E('rm null input', { $regexMatch: { input: '$s', regex: 'a' } }, 2);
E('rm missing input', { $regexMatch: { input: '$nope', regex: 'a' } });
E('rm i', { $regexMatch: { input: 'ABC', regex: 'abc', options: 'i' } });
E('rm m', { $regexMatch: { input: 'a\nb', regex: '^b', options: 'm' } });
E('rm s', { $regexMatch: { input: 'a\nb', regex: 'a.b', options: 's' } });
E('rm x', { $regexMatch: { input: 'ab', regex: 'a b', options: 'x' } });
E('rm bad opt', { $regexMatch: { input: 'ab', regex: 'a', options: 'q' } });
E('rm bad regex', { $regexMatch: { input: 'ab', regex: '(' } });
E('rm regex num', { $regexMatch: { input: 'ab', regex: 1 } });
E('rm input num', { $regexMatch: { input: 12, regex: '1' } });
E('rm bson regex', { $regexMatch: { input: 'ABC', regex: /abc/i } });
E('rm bson regex opts conflict', { $regexMatch: { input: 'ABC', regex: /abc/i, options: 'm' } });
E('rm bson regex opts same', { $regexMatch: { input: 'ABC', regex: /abc/i, options: 'i' } });
E('rm arity', { $regexMatch: { input: 'a' } });
E('rm unknown field', { $regexMatch: { input: 'a', regex: 'a', foo: 1 } });
E('rm options null', { $regexMatch: { input: 'a', regex: 'a', options: null } });
E('rm unicode', { $regexMatch: { input: 'straße', regex: 'ß' } });
E('rm ascii class', { $regexMatch: { input: 'Ä', regex: '\\w' } });

E('rf basic', { $regexFind: { input: '$re', regex: '[0-9]+' } });
E('rf nomatch', { $regexFind: { input: 'abc', regex: '[0-9]' } });
E('rf null', { $regexFind: { input: '$s', regex: 'a' } }, 2);
E('rf captures', { $regexFind: { input: 'ab12', regex: '([a-z]+)([0-9]+)' } });
E('rf named', { $regexFind: { input: 'ab12', regex: '(?<l>[a-z]+)(?<d>[0-9]+)' } });
E('rf unmatched group', { $regexFind: { input: 'ab', regex: '(x)?(a)' } });
E('rf idx cp', { $regexFind: { input: 'éé12', regex: '[0-9]+' } });
E('rf unicode idx', { $regexFind: { input: 'ÄÖ ö', regex: 'ö' } });
E('rf empty match', { $regexFind: { input: 'abc', regex: 'x*' } });
E('rf i opt', { $regexFind: { input: 'XyZ', regex: 'y', options: 'i' } });
E('rf bson', { $regexFind: { input: 'XyZ', regex: /y/i } });
E('rf arity', { $regexFind: { regex: 'a' } });
E('rf bad opts', { $regexFind: { input: 'a', regex: 'a', options: 'z' } });
E('rf num input', { $regexFind: { input: 5, regex: '5' } });

E('rfa basic', { $regexFindAll: { input: 'a1b22c333', regex: '[0-9]+' } });
E('rfa nomatch', { $regexFindAll: { input: 'abc', regex: '[0-9]' } });
E('rfa null', { $regexFindAll: { input: '$s', regex: 'a' } }, 2);
E('rfa empty matches', { $regexFindAll: { input: 'ab', regex: 'x*' } });
E('rfa captures', { $regexFindAll: { input: 'a1b2', regex: '([a-z])([0-9])' } });
E('rfa unicode', { $regexFindAll: { input: 'ö1ö2', regex: 'ö' } });
E('rfa i', { $regexFindAll: { input: 'aAa', regex: 'a', options: 'i' } });
E('rfa missing', { $regexFindAll: { input: '$nope', regex: 'a' } });
E('rfa arity', { $regexFindAll: { input: 'a' } });
E('rfa bad regex', { $regexFindAll: { input: 'a', regex: '[' } });
E('rfa adjacent empty', { $regexFindAll: { input: 'ab', regex: '(?:)' } });
E('rfa x opt', { $regexFindAll: { input: 'a1', regex: 'a # c\n 1', options: 'x' } });
E('rfa m s', { $regexFindAll: { input: 'x\ny', regex: '.y', options: 'ms' } });

// ---- date parts, Olson and offsets
const DT = '$dt';
for (const [nm, op] of [['year', '$year'], ['month', '$month'], ['dayOfMonth', '$dayOfMonth'], ['dayOfWeek', '$dayOfWeek'], ['dayOfYear', '$dayOfYear'], ['hour', '$hour'], ['minute', '$minute'], ['second', '$second'], ['millisecond', '$millisecond'], ['week', '$week'], ['isoWeek', '$isoWeek'], ['isoWeekYear', '$isoWeekYear'], ['isoDayOfWeek', '$isoDayOfWeek']]) {
  E(nm + ' utc', { [op]: DT });
  E(nm + ' ny', { [op]: { date: DT, timezone: 'America/New_York' } });
  E(nm + ' kolkata', { [op]: { date: DT, timezone: '+05:30' } });
  E(nm + ' offset neg', { [op]: { date: DT, timezone: '-03:00' } });
  E(nm + ' null', { [op]: '$dt' }, 2);
}
E('dateparts tz bad', { $year: { date: DT, timezone: 'Mars/Base' } });
E('dateparts tz num', { $year: { date: DT, timezone: 5 } });
E('dateparts tz offset bad', { $year: { date: DT, timezone: '+25:00' } });
E('dateparts tz utc', { $year: { date: DT, timezone: 'UTC' } });
E('dateparts tz Z', { $hour: { date: DT, timezone: 'Z' } });
E('dateparts tz GMT', { $hour: { date: DT, timezone: 'GMT' } });
E('dateparts tz fraction off', { $hour: { date: DT, timezone: '+05:30:15' } });
E('dateparts tz compact', { $hour: { date: DT, timezone: '+0530' } });
E('dateparts tz compact hour', { $hour: { date: DT, timezone: '+05' } });
E('dateparts tz unknown field', { $hour: { date: DT, timezone: 'UTC', foo: 1 } });
E('dateparts tz empty str', { $hour: { date: DT, timezone: '' } });
E('dateparts bad date', { $year: 'abc' });
E('dateparts num date', { $year: 5 });
E('dateparts timestamp', { $year: NumberLong(0) });
E('dateparts ObjectId missing date', { $year: { date: '$nope' } });
E('dateparts arity', { $year: [DT, DT] });
E('dateparts no date', { $year: { timezone: 'UTC' } });
E('dateparts from string', { $year: '2020-01-01T00:00:00Z' });
E('dateparts pre epoch', { $year: new Date(-1000000000000) });
E('dateparts pre epoch hour', { $hour: { date: new Date(-1), timezone: 'Asia/Kolkata' } });
E('dateparts ny dst', { $hour: { date: new Date(Date.UTC(2021, 2, 14, 7, 0)), timezone: 'America/New_York' } });
E('dateparts ny dst2', { $hour: { date: new Date(Date.UTC(2021, 2, 14, 6, 59)), timezone: 'America/New_York' } });
E('dateparts ny dst fall', { $hour: { date: new Date(Date.UTC(2021, 10, 7, 6, 0)), timezone: 'America/New_York' } });
E('dateparts ny dst fall2', { $hour: { date: new Date(Date.UTC(2021, 10, 7, 5, 59)), timezone: 'America/New_York' } });
E('dateparts isoWeek 2021 jan1', { $isoWeek: { date: new Date(Date.UTC(2021, 0, 1)) } });
E('dateparts week 2021 jan1', { $week: { date: new Date(Date.UTC(2021, 0, 1)) } });
E('dateparts week 2021 jan3', { $week: { date: new Date(Date.UTC(2021, 0, 3)) } });
E('dateparts isoWeekYear 2020 dec31', { $isoWeekYear: { date: new Date(Date.UTC(2020, 11, 31)) } });
E('dateparts year 9999', { $year: new Date(Date.UTC(9999, 11, 31, 23, 59)) });
E('dateparts year 0001', { $year: new Date(Date.UTC(1, 0, 1)) });

// ---- $dateToString
E('dts default', { $dateToString: { date: DT } });
E('dts format', { $dateToString: { date: DT, format: '%Y-%m-%d %H:%M:%S.%L' } });
E('dts all specs', { $dateToString: { date: DT, format: '%a %A %b %B %c %d %e %G %g %H %I %j %m %M %p %S %u %U %V %w %W %x %X %y %Y %z %Z %%' } });
E('dts tz', { $dateToString: { date: DT, format: '%Y-%m-%dT%H:%M:%S%z %Z', timezone: 'America/New_York' } });
E('dts tz offset', { $dateToString: { date: DT, format: '%H:%M %z', timezone: '+05:30' } });
E('dts onNull', { $dateToString: { date: '$dt', format: '%Y', onNull: 'none' } }, 2);
E('dts onNull missing', { $dateToString: { date: '$nope', onNull: 'x' } });
E('dts null no onNull', { $dateToString: { date: '$dt', format: '%Y' } }, 2);
E('dts null onNull obj', { $dateToString: { date: '$dt', onNull: { a: 1 } } }, 2);
E('dts bad spec', { $dateToString: { date: DT, format: '%Q' } });
E('dts trailing pct', { $dateToString: { date: DT, format: 'abc%' } });
E('dts bad format type', { $dateToString: { date: DT, format: 5 } });
E('dts bad date', { $dateToString: { date: 'x', format: '%Y' } });
E('dts unknown field', { $dateToString: { date: DT, foo: 1 } });
E('dts missing date', { $dateToString: { format: '%Y' } });
E('dts format null', { $dateToString: { date: DT, format: null } });
E('dts %L', { $dateToString: { date: DT, format: '%L %f' } });
E('dts %%', { $dateToString: { date: DT, format: '100%%' } });
E('dts tz bad', { $dateToString: { date: DT, timezone: 'Nope/Nope' } });
E('dts pre epoch', { $dateToString: { date: new Date(-1), format: '%Y-%m-%d %H:%M:%S.%L' } });
E('dts year lt 1000', { $dateToString: { date: new Date(Date.UTC(999, 0, 1)), format: '%Y-%m-%d' } });
E('dts %j %U %W', { $dateToString: { date: new Date(Date.UTC(2021, 0, 3)), format: '%j %U %W %V %G' } });
E('dts arity', { $dateToString: DT });

// ---- $dateFromString
E('dfs iso', { $dateFromString: { dateString: '2021-01-31T13:45:30.123Z' } });
E('dfs iso offset', { $dateFromString: { dateString: '2021-01-31T13:45:30+05:30' } });
E('dfs iso offset tz arg', { $dateFromString: { dateString: '2021-01-31T13:45:30+05:30', timezone: 'America/New_York' } });
E('dfs date only', { $dateFromString: { dateString: '2021-01-31' } });
E('dfs ym', { $dateFromString: { dateString: '2021-01' } });
E('dfs year only', { $dateFromString: { dateString: '2021' } });
E('dfs format', { $dateFromString: { dateString: '31/01/2021 13:45', format: '%d/%m/%Y %H:%M' } });
E('dfs format tz', { $dateFromString: { dateString: '31/01/2021', format: '%d/%m/%Y', timezone: 'Asia/Kolkata' } });
E('dfs format offset', { $dateFromString: { dateString: '31/01/2021 +0100', format: '%d/%m/%Y %z' } });
E('dfs format conflicting', { $dateFromString: { dateString: '31/01/2021 +0100', format: '%d/%m/%Y %z', timezone: 'Asia/Kolkata' } });
E('dfs format month name', { $dateFromString: { dateString: 'Jan 31 2021', format: '%b %d %Y' } });
E('dfs format full month', { $dateFromString: { dateString: 'January 31 2021', format: '%B %d %Y' } });
E('dfs format pm', { $dateFromString: { dateString: '01:05 PM', format: '%I:%M %p', timezone: 'UTC' } });
E('dfs format j', { $dateFromString: { dateString: '2021 031', format: '%Y %j' } });
E('dfs format %%', { $dateFromString: { dateString: '5%', format: '%d%%' } });
E('dfs format literal mismatch', { $dateFromString: { dateString: '31-01-2021', format: '%d/%m/%Y' } });
E('dfs format trailing', { $dateFromString: { dateString: '31/01/2021x', format: '%d/%m/%Y' } });
E('dfs bad string', { $dateFromString: { dateString: 'hello' } });
E('dfs onError', { $dateFromString: { dateString: 'hello', onError: 'bad' } });
E('dfs onError null', { $dateFromString: { dateString: 'hello', onError: null } });
E('dfs onError obj', { $dateFromString: { dateString: 'hello', onError: { a: 1 } } });
E('dfs onNull', { $dateFromString: { dateString: '$nope', onNull: 'nil' } });
E('dfs null', { $dateFromString: { dateString: '$s' } }, 2);
E('dfs null onNull', { $dateFromString: { dateString: '$s', onNull: 0 } }, 2);
E('dfs num', { $dateFromString: { dateString: 5 } });
E('dfs bad tz', { $dateFromString: { dateString: '2021-01-01', timezone: 'Bad/Zone' } });
E('dfs bad format', { $dateFromString: { dateString: '2021', format: '%Q' } });
E('dfs format num', { $dateFromString: { dateString: '2021', format: 5 } });
E('dfs format null', { $dateFromString: { dateString: '2021-01-01', format: null } });
E('dfs unknown field', { $dateFromString: { dateString: '2021-01-01', x: 1 } });
E('dfs missing', { $dateFromString: { format: '%Y' } });
E('dfs leap', { $dateFromString: { dateString: '2021-02-29' } });
E('dfs leap ok', { $dateFromString: { dateString: '2020-02-29' } });
E('dfs T space', { $dateFromString: { dateString: '2021-01-31 13:45:30' } });
E('dfs T no sec', { $dateFromString: { dateString: '2021-01-31T13:45Z' } });
E('dfs fraction 1', { $dateFromString: { dateString: '2021-01-31T13:45:30.1Z' } });
E('dfs fraction 6', { $dateFromString: { dateString: '2021-01-31T13:45:30.123456Z' } });
E('dfs negative year', { $dateFromString: { dateString: '-0001-01-01T00:00:00Z' } });
E('dfs iso week', { $dateFromString: { dateString: '2021-W05' } });
E('dfs compact offset', { $dateFromString: { dateString: '2021-01-31T13:45:30+0530' } });
E('dfs format %G', { $dateFromString: { dateString: '2021', format: '%G' } });
E('dfs format %y', { $dateFromString: { dateString: '21-01-31', format: '%y-%m-%d' } });
E('dfs format %L', { $dateFromString: { dateString: '12:00:00.250', format: '%H:%M:%S.%L' } });
E('dfs format %Z', { $dateFromString: { dateString: '2021 EST', format: '%Y %Z' } });
E('dfs format %z neg', { $dateFromString: { dateString: '2021 -0500', format: '%Y %z' } });

// ---- $dateToParts / $dateFromParts
E('dtp basic', { $dateToParts: { date: DT } });
E('dtp tz', { $dateToParts: { date: DT, timezone: 'America/New_York' } });
E('dtp iso', { $dateToParts: { date: DT, iso8601: true } });
E('dtp iso tz', { $dateToParts: { date: DT, timezone: 'Asia/Kolkata', iso8601: true } });
E('dtp null', { $dateToParts: { date: '$dt' } }, 2);
E('dtp bad iso', { $dateToParts: { date: DT, iso8601: 'yes' } });
E('dtp unknown', { $dateToParts: { date: DT, x: 1 } });
E('dtp bad date', { $dateToParts: { date: 'x' } });
E('dtp missing', { $dateToParts: { timezone: 'UTC' } });
E('dtp arity', { $dateToParts: DT });
E('dtp pre epoch', { $dateToParts: { date: new Date(-86400001) } });

E('dfp basic', { $dateFromParts: { year: 2021, month: 1, day: 31, hour: 13, minute: 45, second: 30, millisecond: 123 } });
E('dfp defaults', { $dateFromParts: { year: 2021 } });
E('dfp month overflow', { $dateFromParts: { year: 2021, month: 13, day: 1 } });
E('dfp month zero', { $dateFromParts: { year: 2021, month: 0, day: 1 } });
E('dfp day overflow', { $dateFromParts: { year: 2021, month: 2, day: 31 } });
E('dfp day zero', { $dateFromParts: { year: 2021, month: 1, day: 0 } });
E('dfp day neg', { $dateFromParts: { year: 2021, month: 1, day: -1 } });
E('dfp hour overflow', { $dateFromParts: { year: 2021, month: 1, day: 1, hour: 25, minute: 61, second: 61, millisecond: 1001 } });
E('dfp neg minute', { $dateFromParts: { year: 2021, month: 1, day: 1, minute: -30 } });
E('dfp tz', { $dateFromParts: { year: 2021, month: 1, day: 31, hour: 12, timezone: 'America/New_York' } });
E('dfp tz offset', { $dateFromParts: { year: 2021, month: 1, day: 31, hour: 12, timezone: '+05:30' } });
E('dfp tz bad', { $dateFromParts: { year: 2021, timezone: 'Bad/X' } });
E('dfp null year', { $dateFromParts: { year: null, month: 1 } });
E('dfp missing year', { $dateFromParts: { month: 1 } });
E('dfp iso', { $dateFromParts: { isoWeekYear: 2021, isoWeek: 1, isoDayOfWeek: 1 } });
E('dfp iso week 53', { $dateFromParts: { isoWeekYear: 2020, isoWeek: 53, isoDayOfWeek: 7, hour: 1 } });
E('dfp iso week 0', { $dateFromParts: { isoWeekYear: 2021, isoWeek: 0, isoDayOfWeek: 1 } });
E('dfp iso mixed', { $dateFromParts: { isoWeekYear: 2021, year: 2021, isoWeek: 1 } });
E('dfp iso mixed2', { $dateFromParts: { isoWeekYear: 2021, month: 1, isoDayOfWeek: 1 } });
E('dfp iso missing week', { $dateFromParts: { isoWeekYear: 2021 } });
E('dfp float', { $dateFromParts: { year: 2021.5, month: 1 } });
E('dfp string', { $dateFromParts: { year: '2021', month: 1 } });
E('dfp unknown', { $dateFromParts: { year: 2021, foo: 1 } });
E('dfp arity', { $dateFromParts: 2021 });
E('dfp year 1', { $dateFromParts: { year: 1, month: 1, day: 1 } });
E('dfp year 0', { $dateFromParts: { year: 0, month: 1, day: 1 } });
E('dfp year 10000', { $dateFromParts: { year: 10000, month: 1, day: 1 } });
E('dfp year 9999 overflow', { $dateFromParts: { year: 9999, month: 12, day: 31, hour: 24 } });
E('dfp pre-epoch', { $dateFromParts: { year: 1969, month: 12, day: 31, hour: 23, minute: 59, second: 59, millisecond: 999 } });
E('dfp leap', { $dateFromParts: { year: 2020, month: 2, day: 29 } });
E('dfp nonleap feb 29', { $dateFromParts: { year: 2021, month: 2, day: 29 } });

// ---- $dateAdd / $dateSubtract
const A = (unit, amt, date = DT, tz) => ({ startDate: date, unit, amount: amt, ...(tz ? { timezone: tz } : {}) });
for (const u of ['millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year']) {
  E('dateAdd ' + u, { $dateAdd: A(u, 3) });
  E('dateAdd neg ' + u, { $dateAdd: A(u, -3) });
  E('dateSubtract ' + u + ' is add-neg', { $dateAdd: A(u, -2) });
}
E('dateAdd month clamp', { $dateAdd: { startDate: new Date(Date.UTC(2021, 0, 31)), unit: 'month', amount: 1 } });
E('dateAdd month clamp feb', { $dateAdd: { startDate: new Date(Date.UTC(2021, 0, 31)), unit: 'month', amount: 13 } });
E('dateAdd leap year', { $dateAdd: { startDate: new Date(Date.UTC(2020, 1, 29)), unit: 'year', amount: 1 } });
E('dateAdd ny dst spring', { $dateAdd: { startDate: new Date(Date.UTC(2021, 2, 13, 12)), unit: 'day', amount: 1, timezone: 'America/New_York' } });
E('dateAdd ny dst hour', { $dateAdd: { startDate: new Date(Date.UTC(2021, 2, 14, 6, 30)), unit: 'hour', amount: 1, timezone: 'America/New_York' } });
E('dateAdd ny dst hour2', { $dateAdd: { startDate: new Date(Date.UTC(2021, 2, 14, 6, 30)), unit: 'hour', amount: 2, timezone: 'America/New_York' } });
E('dateAdd ny month across dst', { $dateAdd: { startDate: new Date(Date.UTC(2021, 2, 1, 12)), unit: 'month', amount: 1, timezone: 'America/New_York' } });
E('dateAdd tz offset', { $dateAdd: { startDate: DT, unit: 'day', amount: 1, timezone: '+05:30' } });
E('dateAdd pre epoch', { $dateAdd: { startDate: new Date(-1000), unit: 'second', amount: 2 } });
E('dateAdd pre epoch month', { $dateAdd: { startDate: new Date(-1000), unit: 'month', amount: -1 } });
E('dateAdd null', { $dateAdd: { startDate: '$dt', unit: 'day', amount: 1 } }, 2);
E('dateAdd missing', { $dateAdd: { startDate: '$nope', unit: 'day', amount: 1 } });
E('dateAdd bad unit', { $dateAdd: { startDate: DT, unit: 'fortnight', amount: 1 } });
E('dateAdd unit num', { $dateAdd: { startDate: DT, unit: 5, amount: 1 } });
E('dateAdd amount str', { $dateAdd: { startDate: DT, unit: 'day', amount: '1' } });
E('dateAdd amount null', { $dateAdd: { startDate: DT, unit: 'day', amount: null } });
E('dateAdd amount frac', { $dateAdd: { startDate: DT, unit: 'day', amount: 1.5 } });
E('dateAdd amount long', { $dateAdd: { startDate: DT, unit: 'day', amount: NumberLong(2) } });
E('dateAdd amount huge', { $dateAdd: { startDate: DT, unit: 'millisecond', amount: 9e15 } });
E('dateAdd missing unit', { $dateAdd: { startDate: DT, amount: 1 } });
E('dateAdd missing amount', { $dateAdd: { startDate: DT, unit: 'day' } });
E('dateAdd bad start', { $dateAdd: { startDate: 'x', unit: 'day', amount: 1 } });
E('dateAdd unknown field', { $dateAdd: { startDate: DT, unit: 'day', amount: 1, foo: 1 } });
E('dateAdd bad tz', { $dateAdd: { startDate: DT, unit: 'day', amount: 1, timezone: 'Bad/Y' } });
E('dateAdd arity', { $dateAdd: [DT] });
E('dateAdd out of range', { $dateAdd: { startDate: new Date(8640000000000000), unit: 'day', amount: 1 } });
E('dateAdd long ms', { $dateAdd: { startDate: new Date(8640000000000000 - 1000), unit: 'millisecond', amount: 5000 } });
E('dateSubtract basic', { $dateSubtract: { startDate: DT, unit: 'day', amount: 2 } });
E('dateSubtract month', { $dateSubtract: { startDate: DT, unit: 'month', amount: 2, timezone: 'America/New_York' } });
E('dateSubtract null', { $dateSubtract: { startDate: null, unit: 'day', amount: 2 } });
E('dateSubtract bad unit', { $dateSubtract: { startDate: DT, unit: 'x', amount: 2 } });
E('dateSubtract pre epoch', { $dateSubtract: { startDate: new Date(0), unit: 'millisecond', amount: 1 } });
E('dateSubtract year', { $dateSubtract: { startDate: new Date(Date.UTC(2020, 1, 29)), unit: 'year', amount: 1 } });

// ---- $dateDiff
const D = (s, e, unit, extra = {}) => ({ startDate: s, endDate: e, unit, ...extra });
const s1 = new Date(Date.UTC(2021, 0, 31, 10, 0, 0, 0));
const e1 = new Date(Date.UTC(2021, 2, 1, 9, 30, 15, 500));
for (const u of ['millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year']) {
  E('dateDiff ' + u, { $dateDiff: D(s1, e1, u) });
  E('dateDiff rev ' + u, { $dateDiff: D(e1, s1, u) });
}
E('dateDiff same', { $dateDiff: D(s1, s1, 'day') });
E('dateDiff week sun', { $dateDiff: D(new Date(Date.UTC(2021, 0, 2)), new Date(Date.UTC(2021, 0, 10)), 'week', { startOfWeek: 'sunday' }) });
E('dateDiff week mon', { $dateDiff: D(new Date(Date.UTC(2021, 0, 2)), new Date(Date.UTC(2021, 0, 10)), 'week', { startOfWeek: 'monday' }) });
E('dateDiff week fri', { $dateDiff: D(new Date(Date.UTC(2021, 0, 2)), new Date(Date.UTC(2021, 0, 10)), 'week', { startOfWeek: 'fri' }) });
E('dateDiff week bad', { $dateDiff: D(s1, e1, 'week', { startOfWeek: 'funday' }) });
E('dateDiff week null sow', { $dateDiff: D(s1, e1, 'week', { startOfWeek: null }) });
E('dateDiff startOfWeek non-week', { $dateDiff: D(s1, e1, 'day', { startOfWeek: 'monday' }) });
E('dateDiff tz', { $dateDiff: D(s1, e1, 'day', { timezone: 'America/New_York' }) });
E('dateDiff tz month', { $dateDiff: D(s1, e1, 'month', { timezone: 'Asia/Kolkata' }) });
E('dateDiff dst', { $dateDiff: D(new Date(Date.UTC(2021, 2, 13, 12)), new Date(Date.UTC(2021, 2, 14, 12)), 'hour', { timezone: 'America/New_York' }) });
E('dateDiff dst day', { $dateDiff: D(new Date(Date.UTC(2021, 2, 13, 12)), new Date(Date.UTC(2021, 2, 14, 12)), 'day', { timezone: 'America/New_York' }) });
E('dateDiff null', { $dateDiff: D(null, e1, 'day') });
E('dateDiff missing', { $dateDiff: D('$nope', e1, 'day') });
E('dateDiff bad unit', { $dateDiff: D(s1, e1, 'decade') });
E('dateDiff bad start', { $dateDiff: D('x', e1, 'day') });
E('dateDiff unknown', { $dateDiff: D(s1, e1, 'day', { foo: 1 }) });
E('dateDiff arity', { $dateDiff: [s1, e1] });
E('dateDiff num dates', { $dateDiff: D(1, 2, 'day') });
E('dateDiff month end', { $dateDiff: D(new Date(Date.UTC(2021, 0, 31)), new Date(Date.UTC(2021, 1, 28)), 'month') });
E('dateDiff month same day', { $dateDiff: D(new Date(Date.UTC(2021, 0, 15)), new Date(Date.UTC(2021, 1, 15)), 'month') });
E('dateDiff year leap', { $dateDiff: D(new Date(Date.UTC(2020, 1, 29)), new Date(Date.UTC(2021, 1, 28)), 'year') });
E('dateDiff quarter', { $dateDiff: D(new Date(Date.UTC(2020, 0, 1)), new Date(Date.UTC(2020, 6, 1)), 'quarter') });
E('dateDiff pre epoch', { $dateDiff: D(new Date(-5000), new Date(5000), 'second') });
E('dateDiff pre epoch day', { $dateDiff: D(new Date(-86400000), new Date(0), 'day') });

// ---- $dateTrunc
const T = (date, unit, bin, extra = {}) => ({ date, unit, binSize: bin, ...extra });
const tdate = new Date(Date.UTC(2021, 4, 17, 13, 47, 33, 789));
for (const u of ['millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year']) {
  E('dateTrunc ' + u, { $dateTrunc: T(tdate, u, 1) });
  E('dateTrunc bin3 ' + u, { $dateTrunc: T(tdate, u, 3) });
}
E('dateTrunc week sun', { $dateTrunc: T(tdate, 'week', 1, { startOfWeek: 'sunday' }) });
E('dateTrunc week mon', { $dateTrunc: T(tdate, 'week', 2, { startOfWeek: 'monday' }) });
E('dateTrunc week bad', { $dateTrunc: T(tdate, 'week', 1, { startOfWeek: 'x' }) });
E('dateTrunc bin 0', { $dateTrunc: T(tdate, 'day', 0) });
E('dateTrunc bin neg', { $dateTrunc: T(tdate, 'day', -1) });
E('dateTrunc bin frac', { $dateTrunc: T(tdate, 'day', 1.5) });
E('dateTrunc bin str', { $dateTrunc: T(tdate, 'day', '1') });
E('dateTrunc bin null', { $dateTrunc: T(tdate, 'day', null) });
E('dateTrunc bin missing', { $dateTrunc: { date: tdate, unit: 'day' } });
E('dateTrunc tz', { $dateTrunc: T(tdate, 'day', 1, { timezone: 'America/New_York' }) });
E('dateTrunc tz hour', { $dateTrunc: T(tdate, 'hour', 1, { timezone: '+05:30' }) });
E('dateTrunc tz month', { $dateTrunc: T(tdate, 'month', 1, { timezone: 'Asia/Kolkata' }) });
E('dateTrunc tz quarter', { $dateTrunc: T(tdate, 'quarter', 1, { timezone: 'Australia/Sydney' }) });
E('dateTrunc null', { $dateTrunc: T(null, 'day', 1) }, 2);
E('dateTrunc missing date', { $dateTrunc: { unit: 'day', binSize: 1 } });
E('dateTrunc bad unit', { $dateTrunc: T(tdate, 'fortnight', 1) });
E('dateTrunc unit num', { $dateTrunc: T(tdate, 3, 1) });
E('dateTrunc unknown', { $dateTrunc: T(tdate, 'day', 1, { q: 1 }) });
E('dateTrunc arity', { $dateTrunc: [tdate] });
E('dateTrunc bin day 2', { $dateTrunc: T(new Date(Date.UTC(2021, 0, 10)), 'day', 4) });
E('dateTrunc pre epoch', { $dateTrunc: T(new Date(-1), 'second', 1) });
E('dateTrunc pre epoch year', { $dateTrunc: T(new Date(Date.UTC(1960, 5, 1)), 'year', 5) });
E('dateTrunc bin year 2', { $dateTrunc: T(new Date(Date.UTC(2021, 5, 1)), 'year', 2) });
E('dateTrunc bin month 5', { $dateTrunc: T(new Date(Date.UTC(2021, 5, 1)), 'month', 5) });
E('dateTrunc bin hour 7', { $dateTrunc: T(new Date(Date.UTC(2021, 5, 1, 20)), 'hour', 7) });
E('dateTrunc bin ms big', { $dateTrunc: T(tdate, 'millisecond', 500) });
E('dateTrunc bin second 90', { $dateTrunc: T(tdate, 'second', 90) });

// ---- misc date parts via $dateToString on pre-1970 / timezones
E('dts ny', { $dateToString: { date: DT, format: '%Y-%m-%d %H:%M %Z %z', timezone: 'America/New_York' } });
E('dts sydney', { $dateToString: { date: DT, format: '%Y-%m-%d %H:%M %z', timezone: 'Australia/Sydney' } });
E('dts kolkata', { $dateToString: { date: DT, format: '%H:%M %z', timezone: 'Asia/Kolkata' } });
E('dts field tz', { $dateToString: { date: DT, format: '%H:%M', timezone: '$tz' } });
E('dts field tz rec 3', { $dateToString: { date: '$dt', format: '%H:%M', timezone: '$tz' } }, 3);
E('dts %G %V ISO boundaries', { $dateToString: { date: new Date(Date.UTC(2019, 11, 30)), format: '%G-W%V-%u' } });
E('dts %U %W first week', { $dateToString: { date: new Date(Date.UTC(2021, 0, 1)), format: '%U %W %w %u' } });
E('dts %g year', { $dateToString: { date: new Date(Date.UTC(2021, 0, 1)), format: '%g %G' } });
E('dts %e', { $dateToString: { date: new Date(Date.UTC(2021, 0, 5)), format: '[%e] [%d]' } });
E('dts %x %X', { $dateToString: { date: DT, format: '%x|%X|%c' } });
E('dts %p am', { $dateToString: { date: new Date(Date.UTC(2021, 0, 1, 0, 5)), format: '%I %p' } });
E('dts %p noon', { $dateToString: { date: new Date(Date.UTC(2021, 0, 1, 12, 5)), format: '%I %p' } });
E('dts %Z utc', { $dateToString: { date: DT, format: '%Z' } });
E('dts %Z offset tz', { $dateToString: { date: DT, format: '%Z|%z', timezone: '+05:30' } });

// ---- sanity: regex on fields
E('regex field match', { $regexMatch: { input: '$re', regex: '^ab' } });
E('regex field find all', { $regexFindAll: { input: '$re', regex: '[0-9]' } });
E('regex field replace', { $replaceAll: { input: '$re', find: 'a', replacement: 'A' } });
E('split field', { $split: ['$csv', ','] });
E('strlen mixed', { $map: { input: ['ab', 'ö'], as: 's', in: { $strLenBytes: '$$s' } } });
E('concat mixed arr', { $reduce: { input: ['a', 'b'], initialValue: '', in: { $concat: ['$$value', '$$this'] } } });
E('substr map', { $map: { input: [0, 1, 2], as: 'i', in: { $substrCP: ['abc', '$$i', 1] } } });
E('lower map', { $map: { input: ['AB', 'Ö'], as: 'x', in: { $toLower: '$$x' } } });
E('dateTrunc array', { $map: { input: [tdate], as: 'x', in: { $dateTrunc: { date: '$$x', unit: 'hour', binSize: 1 } } } });
E('dateToString unwind', { $dateToString: { date: { $arrayElemAt: [[tdate], 0] }, format: '%Y' } });
E('dateAdd mixed', { $dateAdd: { startDate: { $dateFromString: { dateString: '2020-01-01' } }, unit: 'day', amount: { $size: [1, 2] } } });
