//! Server-side JavaScript: `$where`, `$function`, `$accumulator` and `mapReduce`.
//!
//! Runs on the Boa engine (`js` feature). Each thread owns one interpreter context whose local
//! time zone is UTC, matching the reference server. BSON values cross into JS as plain objects,
//! arrays, native `Date`/`RegExp`, or tagged objects (ObjectId, NumberLong, ...) that carry their
//! BSON type in a non-enumerable `__mogo` property.

use crate::agg::accum::Accumulator;
use crate::agg::expr::Vars;
use crate::error::Result;
#[cfg(not(feature = "js"))]
use crate::error::Error;
use crate::server::CmdCtx;
use bson::{Bson, Document};

/// `$where`: evaluate `code` (String / Code / CodeWithScope; a function or a bare expression)
/// with `this` and `obj` bound to `doc`; truthy result matches.
#[cfg(feature = "js")]
pub fn where_matches(code: &Bson, doc: &Document) -> Result<bool> {
    imp::where_matches(code, doc)
}

#[cfg(not(feature = "js"))]
pub fn where_matches(code: &Bson, doc: &Document) -> Result<bool> {
    let _ = (code, doc);
    Err(disabled())
}

/// `$function: {body, args, lang: "js"}` expression; `args` are aggregation expressions
/// evaluated against `doc`.
#[cfg(feature = "js")]
pub fn function_expr(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    imp::function_expr(args, doc, vars)
}

#[cfg(not(feature = "js"))]
pub fn function_expr(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
    let _ = (args, doc, vars);
    Err(disabled())
}

/// `$accumulator: {init, initArgs, accumulate, accumulateArgs, merge, finalize, lang: "js"}`.
#[cfg(feature = "js")]
pub fn accumulator(spec: &Bson) -> Result<Box<dyn Accumulator>> {
    imp::accumulator(spec)
}

#[cfg(not(feature = "js"))]
pub fn accumulator(spec: &Bson) -> Result<Box<dyn Accumulator>> {
    let _ = spec;
    Err(disabled())
}

/// `mapReduce` command.
#[cfg(feature = "js")]
pub fn map_reduce(ctx: &mut CmdCtx) -> Result<Document> {
    imp::map_reduce(ctx)
}

#[cfg(not(feature = "js"))]
pub fn map_reduce(ctx: &mut CmdCtx) -> Result<Document> {
    let _ = ctx;
    Err(disabled())
}

#[cfg(not(feature = "js"))]
fn disabled() -> Error {
    Error::new(31264, "server-side JavaScript is not supported in this build")
}

#[cfg(feature = "js")]
mod imp {
    use crate::agg::accum::Accumulator;
    use crate::agg::expr::{Vars, eval};
    use crate::agg::pipeline::AggContext;
    use crate::bsonutil::cmp_bson;
    use crate::cmd::crud::{ServerAggCtx, run_agg_on};
    use crate::error::{Error, Result};
    use crate::server::CmdCtx;
    use base64::Engine as _;
    use bson::oid::ObjectId;
    use bson::spec::BinarySubtype;
    use bson::{Binary, Bson, DateTime, Decimal128, Document, doc};
    use boa_engine::builtins::date::Date;
    use boa_engine::builtins::regexp::RegExp;
    use boa_engine::context::HostHooks;
    use boa_engine::object::ObjectInitializer;
    use boa_engine::object::builtins::JsArray;
    use boa_engine::{Context, JsError, JsObject, JsResult, JsString, JsValue, Source};
    use std::cell::RefCell;
    use std::cmp::Ordering;
    use std::collections::HashMap;
    use std::rc::Rc;

    /// Prefix of errors thrown by the prelude (`__MOGO__:<code>:<message>`).
    const MARK: &str = "__MOGO__:";
    /// Loop iterations allowed per evaluation before the run is interrupted (11601).
    const LOOP_LIMIT: u64 = 20_000_000;
    /// Compiled-function cache entries per thread before the cache is cleared.
    const CACHE_MAX: usize = 256;
    const MR_FIELDS: &[&str] = &[
        "map", "reduce", "finalize", "out", "query", "sort", "limit", "scope", "verbose", "collation",
        "readConcern", "writeConcern", "bypassDocumentValidation", "comment", "maxTimeMS", "lsid",
        "txnNumber", "jsMode",
    ];
    const ACC_FIELDS: &[&str] = &["init", "initArgs", "accumulate", "accumulateArgs", "merge", "finalize", "lang"];

    /// JS source evaluated once per context. Returns the helper object used from Rust.
    const PRELUDE: &str = r##"(function () {
  var def = function (o, k, v) {
    Object.defineProperty(o, k, { value: v, enumerable: false, writable: false, configurable: false });
    return o;
  };
  var mk = function (tag, payload) { return def({}, "__mogo", [tag, payload]); };
  var fail = function (code, msg) { return new Error("__MOGO__:" + code + ":" + msg); };
  var HEX = "0123456789abcdef";
  var rnd = function (n, alphabet) {
    var s = "";
    for (var i = 0; i < n; i++) s += alphabet[Math.floor(Math.random() * alphabet.length)];
    return s;
  };
  function ObjectId(s) {
    if (s === undefined) s = rnd(24, HEX);
    if (typeof s !== "string" || s.length !== 24) throw fail(10448, "invalid object id: length");
    if (!/^[0-9a-fA-F]{24}$/.test(s)) throw fail(10448, "invalid object id: characters");
    s = s.toLowerCase();
    return def(mk("ObjectId", s), "str", s);
  }
  var LMAX = BigInt("9223372036854775807");
  var LMIN = BigInt("-9223372036854775808");
  function NumberLong(v) {
    if (v === undefined) v = 0;
    var b;
    if (typeof v === "number") {
      if (!isFinite(v) || v >= 9223372036854775808 || v < -9223372036854775808)
        throw fail(2, "number passed to NumberLong must be representable as an int64_t");
      b = BigInt(Math.trunc(v));
    } else {
      try { b = BigInt(String(v).trim()); } catch (e) { throw fail(2, "could not convert string to long long"); }
    }
    if (b > LMAX || b < LMIN) throw fail(2, "could not convert string to long long");
    return mk("NumberLong", b.toString());
  }
  function NumberInt(v) { return mk("NumberInt", (v === undefined ? 0 : Number(v)) | 0); }
  var DEC = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
  function NumberDecimal(v) {
    var s = String(v === undefined ? "" : v);
    if (!DEC.test(s) && s !== "Infinity" && s !== "-Infinity" && s !== "NaN")
      throw fail(2, "Input is not a valid Decimal128 value.");
    return mk("NumberDecimal", s);
  }
  function Timestamp(t, i) {
    return mk("Timestamp", [t === undefined ? 0 : t >>> 0, i === undefined ? 0 : i >>> 0]);
  }
  function BinData(sub, b64) {
    if (typeof b64 !== "string" || b64.length % 4 !== 0 || !/^[A-Za-z0-9+\/]*={0,2}$/.test(b64))
      throw fail(10270, "invalid base64");
    return mk("BinData", [sub | 0, b64]);
  }
  function UUID(s) {
    if (s === undefined) s = rnd(32, HEX);
    if (typeof s !== "string" || !/^[0-9a-fA-F]{32}$/.test(s)) throw fail(10270, "invalid uuid");
    return mk("UUID", s.toLowerCase());
  }
  function MinKey() { return mk("MinKey", null); }
  function MaxKey() { return mk("MaxKey", null); }
  function Code(src, scope) { return mk("Code", [String(src), scope === undefined ? null : scope]); }
  function DBRef(ns, id) { return mk("DBRef", [String(ns), id]); }
  function ISODate(s) { return s === undefined ? new Date() : new Date(s); }

  var emits = [];
  var emit = function (k, v) { emits.push([k, v]); };
  var takeEmits = function () { var out = emits.slice(); emits.length = 0; return out; };

  def(Array, "sum", function (a) {
    var s = 0;
    for (var i = 0; i < a.length; i++) if (typeof a[i] === "number") s += a[i];
    return s;
  });
  def(Array, "avg", function (a) {
    var s = 0, n = 0;
    for (var i = 0; i < a.length; i++) if (typeof a[i] === "number") { s += a[i]; n++; }
    return n === 0 ? null : s / n;
  });

  var names = {
    ObjectId: ObjectId, NumberLong: NumberLong, NumberInt: NumberInt, NumberDecimal: NumberDecimal,
    Timestamp: Timestamp, BinData: BinData, UUID: UUID, MinKey: MinKey, MaxKey: MaxKey,
    Code: Code, DBRef: DBRef, ISODate: ISODate
  };
  for (var n in names) globalThis[n] = names[n];
  var noop = function () {};
  globalThis.print = noop;
  globalThis.printjson = noop;
  globalThis.gc = noop;
  globalThis.sleep = noop;
  // Globals the server lacks are removed; shell helpers are added.
  delete globalThis.Reflect;
  delete globalThis.Intl;
  delete globalThis.Atomics;
  delete globalThis.SharedArrayBuffer;
  var tojson = function (v) {
    if (v === undefined) return "undefined";
    if (v === null) return "null";
    if (typeof v === "string") return JSON.stringify(v);
    if (typeof v !== "object") return String(v);
    if (Array.isArray(v)) return v.length === 0 ? "[ ]" : "[ " + v.map(function (x) { return tojson(x); }).join(", ") + " ]";
    var ks = Object.keys(v);
    if (ks.length === 0) return "{ }";
    return "{ " + ks.map(function (k) { return JSON.stringify(k) + " : " + tojson(v[k]); }).join(", ") + " }";
  };
  var md5Hex = function (str) {
    var enc = encodeURIComponent(str), bytes = [];
    for (var i = 0; i < enc.length; i++) {
      if (enc.charAt(i) === "%") { bytes.push(parseInt(enc.substr(i + 1, 2), 16)); i += 2; }
      else bytes.push(enc.charCodeAt(i));
    }
    var bitLen = bytes.length * 8;
    bytes.push(0x80);
    while (bytes.length % 64 !== 56) bytes.push(0);
    for (var b = 0; b < 4; b++) bytes.push((bitLen >>> (8 * b)) & 255);
    var hi = Math.floor(bitLen / 4294967296);
    for (var b2 = 0; b2 < 4; b2++) bytes.push((hi >>> (8 * b2)) & 255);
    var S = [7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21];
    var K = [];
    for (var k = 0; k < 64; k++) K.push(Math.floor(Math.abs(Math.sin(k + 1)) * 4294967296) | 0);
    var a0 = 0x67452301 | 0, b0 = 0xefcdab89 | 0, c0 = 0x98badcfe | 0, d0 = 0x10325476 | 0;
    for (var off = 0; off < bytes.length; off += 64) {
      var M = [];
      for (var j = 0; j < 16; j++) {
        var p = off + 4 * j;
        M.push(bytes[p] | (bytes[p + 1] << 8) | (bytes[p + 2] << 16) | (bytes[p + 3] << 24));
      }
      var A = a0, B = b0, C = c0, D = d0;
      for (var r = 0; r < 64; r++) {
        var F, g;
        if (r < 16) { F = (B & C) | (~B & D); g = r; }
        else if (r < 32) { F = (D & B) | (~D & C); g = (5 * r + 1) % 16; }
        else if (r < 48) { F = B ^ C ^ D; g = (3 * r + 5) % 16; }
        else { F = C ^ (B | ~D); g = (7 * r) % 16; }
        F = (F + A + K[r] + M[g]) | 0;
        A = D; D = C; C = B;
        B = (B + ((F << S[r]) | (F >>> (32 - S[r])))) | 0;
      }
      a0 = (a0 + A) | 0; b0 = (b0 + B) | 0; c0 = (c0 + C) | 0; d0 = (d0 + D) | 0;
    }
    var out = "", words = [a0, b0, c0, d0];
    for (var w = 0; w < 4; w++) {
      for (var q = 0; q < 4; q++) {
        var byte = (words[w] >>> (8 * q)) & 255;
        out += HEX.charAt(byte >>> 4) + HEX.charAt(byte & 15);
      }
    }
    return out;
  };
  globalThis.tojson = tojson;
  globalThis.tojsononeline = tojson;
  globalThis.hex_md5 = function (s) { return md5Hex(String(s)); };
  globalThis.isObject = function (v) { return typeof v === "object"; };
  globalThis.isNumber = function (v) { return typeof v === "number"; };
  globalThis.isString = function (v) { return typeof v === "string"; };

  return {
    mkTag: mk,
    mkDate: function (ms) { return new Date(ms); },
    mkRegex: function (p, f) { return new RegExp(p, f); },
    keys: function (o) { return Object.keys(o); },
    takeEmits: takeEmits,
    emit: emit
  };
})()"##;

    /// Boa host hooks that pin local time to UTC.
    struct UtcHooks;

    impl HostHooks for UtcHooks {
        fn local_timezone_offset_seconds(&self, _unix_time_seconds: i64) -> i32 {
            0
        }
    }

    /// Prelude helper functions, fetched once per context.
    struct Helpers {
        mk_tag: JsObject,
        mk_date: JsObject,
        mk_regex: JsObject,
        keys: JsObject,
        take_emits: JsObject,
        emit: JsObject,
    }

    struct Engine {
        ctx: Context,
        h: Helpers,
        fns: HashMap<String, JsObject>,
        wheres: HashMap<String, JsObject>,
    }

    thread_local! {
        static ENGINE: RefCell<Option<Engine>> = const { RefCell::new(None) };
    }

    /// Run `f` against this thread's engine. `f` must not re-enter `with_engine`.
    fn with_engine<R>(f: impl FnOnce(&mut Engine) -> Result<R>) -> Result<R> {
        ENGINE.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = Some(new_engine()?);
            }
            match slot.as_mut() {
                Some(e) => f(e),
                None => Err(Error::internal("js engine unavailable")),
            }
        })
    }

    fn new_engine() -> Result<Engine> {
        let mut ctx = Context::builder().host_hooks(Rc::new(UtcHooks)).build().je()?;
        ctx.runtime_limits_mut().set_loop_iteration_limit(LOOP_LIMIT);
        let prelude = ctx.eval(Source::from_bytes(PRELUDE)).je()?;
        let p = prelude
            .as_object()
            .ok_or_else(|| Error::internal("js prelude did not produce an object"))?;
        let h = Helpers {
            mk_tag: method(&p, "mkTag", &mut ctx)?,
            mk_date: method(&p, "mkDate", &mut ctx)?,
            mk_regex: method(&p, "mkRegex", &mut ctx)?,
            keys: method(&p, "keys", &mut ctx)?,
            take_emits: method(&p, "takeEmits", &mut ctx)?,
            emit: method(&p, "emit", &mut ctx)?,
        };
        Ok(Engine { ctx, h, fns: HashMap::new(), wheres: HashMap::new() })
    }

    fn method(p: &JsObject, name: &str, ctx: &mut Context) -> Result<JsObject> {
        let v = p.get(JsString::from(name), ctx).je()?;
        v.as_object().ok_or_else(|| Error::internal(format!("js prelude is missing {name}")))
    }

    /// Converts a boa result into this crate's error type.
    trait Je<T> {
        fn je(self) -> Result<T>;
    }

    impl<T> Je<T> for JsResult<T> {
        fn je(self) -> Result<T> {
            self.map_err(js_err)
        }
    }

    /// Maps a JS exception to a MongoDB error: prelude errors carry their code, runtime limits
    /// map to Interrupted, and everything else is a JSInterpreterFailure.
    fn js_err(e: JsError) -> Error {
        let s = e.to_string();
        if let Some(i) = s.find(MARK) {
            if let Some((code, msg)) = s[i + MARK.len()..].split_once(':') {
                if let Ok(code) = code.trim().parse::<i32>() {
                    return Error::new(code, msg.lines().next().unwrap_or_default().trim().to_string());
                }
            }
        }
        if s.contains("maximum number of iteration loops") {
            return Error::new(11601, "operation exceeded time limit");
        }
        Error::new(139, format!("JSInterpreterFailure: {s}"))
    }

    /// Compiles `(src)` into a callable, cached by source text.
    fn compile_fn(e: &mut Engine, src: &str) -> Result<JsObject> {
        if let Some(f) = e.fns.get(src) {
            return Ok(f.clone());
        }
        let v = e
            .ctx
            .eval(Source::from_bytes(&format!("({src}\n)")))
            .map_err(|x| Error::new(139, format!("Failed to create JS function :: {x}")))?;
        let f = v
            .as_object()
            .filter(|o| o.is_callable())
            .ok_or_else(|| Error::new(139, "Failed to create JS function :: not a function"))?;
        if e.fns.len() >= CACHE_MAX {
            e.fns.clear();
        }
        e.fns.insert(src.to_string(), f.clone());
        Ok(f)
    }

    /// Compiles a `$where` source: an expression (`return (SRC)`), or a function body if that
    /// does not parse. The result is a function taking `obj`, called with `this` = document.
    fn compile_where(e: &mut Engine, src: &str) -> Result<JsObject> {
        if let Some(f) = e.wheres.get(src) {
            return Ok(f.clone());
        }
        let expr = format!("(function (obj) {{ return ({src}\n); }})");
        let v = match e.ctx.eval(Source::from_bytes(&expr)) {
            Ok(v) => v,
            Err(_) => e
                .ctx
                .eval(Source::from_bytes(&format!("(function (obj) {{ {src}\n }})")))
                .je()?,
        };
        let f = v
            .as_object()
            .filter(|o| o.is_callable())
            .ok_or_else(|| Error::new(139, "Failed to create JS function :: not a function"))?;
        if e.wheres.len() >= CACHE_MAX {
            e.wheres.clear();
        }
        e.wheres.insert(src.to_string(), f.clone());
        Ok(f)
    }

    fn call_obj(ctx: &mut Context, f: &JsObject, this: &JsValue, args: &[JsValue]) -> Result<JsValue> {
        f.call(this, args, ctx).je()
    }

    fn call_method(ctx: &mut Context, o: &JsObject, name: &str, args: &[JsValue]) -> Result<JsValue> {
        let f = o.get(JsString::from(name), ctx).je()?;
        let f = f
            .as_object()
            .ok_or_else(|| Error::bad_value(format!("{name} is not a function")))?;
        call_obj(ctx, &f, &JsValue::from(o.clone()), args)
    }

    fn str_val(s: &str) -> JsValue {
        JsValue::from(JsString::from(s))
    }

    fn str_of(ctx: &mut Context, v: &JsValue) -> Result<String> {
        Ok(v.to_string(ctx).je()?.to_std_string_lossy())
    }

    fn num_of(v: &JsValue) -> f64 {
        v.as_number().unwrap_or(0.0)
    }

    fn at(v: &[JsValue], i: usize) -> JsValue {
        v.get(i).cloned().unwrap_or_else(JsValue::undefined)
    }

    /// Elements of a JS array (empty for non-objects).
    fn items(ctx: &mut Context, v: &JsValue) -> Result<Vec<JsValue>> {
        let Some(o) = v.as_object() else {
            return Ok(Vec::new());
        };
        let len_v = o.get(JsString::from("length"), ctx).je()?;
        let len = len_v.to_number(ctx).je()? as u32;
        let mut out = Vec::with_capacity(len as usize);
        for i in 0..len {
            out.push(o.get(i, ctx).je()?);
        }
        Ok(out)
    }

    fn js_array(ctx: &mut Context, vals: Vec<JsValue>) -> JsValue {
        JsValue::from(JsObject::from(JsArray::from_iter(vals, ctx)))
    }

    /// Builds a tagged BSON-typed JS object through the prelude.
    fn tag(ctx: &mut Context, h: &Helpers, name: &str, payload: JsValue) -> Result<JsValue> {
        call_obj(ctx, &h.mk_tag, &JsValue::undefined(), &[str_val(name), payload])
    }

    fn num_bson(n: f64) -> Bson {
        if n.is_finite() && n.fract() == 0.0 && !(n == 0.0 && n.is_sign_negative()) {
            if n >= f64::from(i32::MIN) && n <= f64::from(i32::MAX) {
                return Bson::Int32(n as i32);
            }
            // 2^63 saturates to i64::MAX; the reference server stores it the same way.
            if n >= -9_223_372_036_854_775_808.0 && n <= 9_223_372_036_854_775_808.0 {
                return Bson::Int64(n as i64);
            }
        }
        Bson::Double(n)
    }

    fn to_js(ctx: &mut Context, h: &Helpers, v: &Bson) -> Result<JsValue> {
        Ok(match v {
            Bson::Double(f) => JsValue::from(*f),
            Bson::String(s) | Bson::Symbol(s) => str_val(s),
            Bson::Boolean(b) => JsValue::from(*b),
            Bson::Null | Bson::Undefined => JsValue::null(),
            Bson::Int32(i) => JsValue::from(f64::from(*i)),
            Bson::Int64(i) => tag(ctx, h, "NumberLong", str_val(&i.to_string()))?,
            Bson::ObjectId(o) => tag(ctx, h, "ObjectId", str_val(&o.to_hex()))?,
            Bson::DateTime(d) => call_obj(
                ctx,
                &h.mk_date,
                &JsValue::undefined(),
                &[JsValue::from(d.timestamp_millis() as f64)],
            )?,
            Bson::Timestamp(t) => {
                let p = js_array(
                    ctx,
                    vec![JsValue::from(f64::from(t.time)), JsValue::from(f64::from(t.increment))],
                );
                tag(ctx, h, "Timestamp", p)?
            }
            Bson::Binary(b) => {
                let sub = u8::from(b.subtype);
                let b64 = base64::engine::general_purpose::STANDARD.encode(&b.bytes);
                let p = js_array(ctx, vec![JsValue::from(f64::from(sub)), str_val(&b64)]);
                tag(ctx, h, "BinData", p)?
            }
            Bson::Decimal128(d) => tag(ctx, h, "NumberDecimal", str_val(&d.to_string()))?,
            Bson::RegularExpression(r) => call_obj(
                ctx,
                &h.mk_regex,
                &JsValue::undefined(),
                &[str_val(&r.pattern), str_val(&r.options)],
            )?,
            Bson::JavaScriptCode(c) => {
                let p = js_array(ctx, vec![str_val(c), JsValue::null()]);
                tag(ctx, h, "Code", p)?
            }
            Bson::JavaScriptCodeWithScope(c) => {
                let scope = doc_to_js(ctx, h, &c.scope)?;
                let p = js_array(ctx, vec![str_val(&c.code), scope]);
                tag(ctx, h, "Code", p)?
            }
            Bson::MinKey => tag(ctx, h, "MinKey", JsValue::null())?,
            Bson::MaxKey => tag(ctx, h, "MaxKey", JsValue::null())?,
            Bson::Array(a) => {
                let mut out = Vec::with_capacity(a.len());
                for x in a {
                    out.push(to_js(ctx, h, x)?);
                }
                js_array(ctx, out)
            }
            Bson::Document(d) => doc_to_js(ctx, h, d)?,
            _ => JsValue::null(),
        })
    }

    fn doc_to_js(ctx: &mut Context, h: &Helpers, d: &Document) -> Result<JsValue> {
        let o = ObjectInitializer::new(ctx).build();
        for (k, v) in d {
            let jv = to_js(ctx, h, v)?;
            o.set(JsString::from(k.as_str()), jv, false, ctx).je()?;
        }
        Ok(JsValue::from(o))
    }

    fn from_js(ctx: &mut Context, h: &Helpers, v: &JsValue) -> Result<Bson> {
        if v.is_undefined() || v.is_null() {
            return Ok(Bson::Null);
        }
        if let Some(b) = v.as_boolean() {
            return Ok(Bson::Boolean(b));
        }
        if let Some(n) = v.as_number() {
            return Ok(num_bson(n));
        }
        if let Some(s) = v.as_string() {
            return Ok(Bson::String(s.to_std_string_lossy()));
        }
        if v.is_symbol() || v.is_bigint() {
            return Err(Error::new(16662, "unable to convert JavaScript property to mongo element"));
        }
        let Some(o) = v.as_object() else {
            return Ok(Bson::Null);
        };
        if o.downcast_ref::<Date>().is_some() {
            let t = call_method(ctx, &o, "getTime", &[])?;
            let ms = t.as_number().unwrap_or(f64::NAN);
            let ms = if ms.is_finite() { ms as i64 } else { 0 };
            return Ok(Bson::DateTime(DateTime::from_millis(ms)));
        }
        if o.downcast_ref::<RegExp>().is_some() {
            let src_v = o.get(JsString::from("source"), ctx).je()?;
            let pattern = str_of(ctx, &src_v)?;
            let flags_v = o.get(JsString::from("flags"), ctx).je()?;
            let options = str_of(ctx, &flags_v)?;
            return Ok(Bson::RegularExpression(bson::Regex { pattern, options }));
        }
        let tag_v = o.get(JsString::from("__mogo"), ctx).je()?;
        if let Some(t) = tag_v.as_object() {
            if t.is_array() {
                let parts = items(ctx, &tag_v)?;
                if parts.len() == 2 {
                    let name = str_of(ctx, &parts[0])?;
                    return from_tag(ctx, h, &name, &parts[1]);
                }
            }
        }
        if o.is_callable() {
            let s = v.to_string(ctx).je()?.to_std_string_lossy();
            return Ok(Bson::JavaScriptCode(s));
        }
        if o.is_array() {
            let elems = items(ctx, v)?;
            let mut out = Vec::with_capacity(elems.len());
            for x in &elems {
                out.push(from_js(ctx, h, x)?);
            }
            return Ok(Bson::Array(out));
        }
        let keys = call_obj(ctx, &h.keys, &JsValue::undefined(), std::slice::from_ref(v))?;
        let mut d = Document::new();
        for kv in items(ctx, &keys)? {
            let k = str_of(ctx, &kv)?;
            let pv = o.get(JsString::from(k.as_str()), ctx).je()?;
            let bv = from_js(ctx, h, &pv)?;
            d.insert(k, bv);
        }
        Ok(Bson::Document(d))
    }

    fn from_tag(ctx: &mut Context, h: &Helpers, name: &str, p: &JsValue) -> Result<Bson> {
        Ok(match name {
            "ObjectId" => {
                let s = str_of(ctx, p)?;
                let oid = ObjectId::parse_str(&s).map_err(|_| Error::new(10448, "invalid object id"))?;
                Bson::ObjectId(oid)
            }
            "NumberLong" => {
                let s = str_of(ctx, p)?;
                let n = s
                    .parse::<i64>()
                    .map_err(|_| Error::bad_value("could not convert string to long long"))?;
                Bson::Int64(n)
            }
            "NumberInt" => Bson::Int32(num_of(p) as i32),
            "NumberDecimal" => {
                let s = str_of(ctx, p)?;
                let d = s
                    .parse::<Decimal128>()
                    .map_err(|_| Error::bad_value("Input is not a valid Decimal128 value."))?;
                Bson::Decimal128(d)
            }
            "Timestamp" => {
                let v = items(ctx, p)?;
                Bson::Timestamp(bson::Timestamp {
                    time: num_of(&at(&v, 0)) as u32,
                    increment: num_of(&at(&v, 1)) as u32,
                })
            }
            "BinData" => {
                let v = items(ctx, p)?;
                let sub = num_of(&at(&v, 0)) as u8;
                let b64 = str_of(ctx, &at(&v, 1))?;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|_| Error::new(10270, "invalid base64"))?;
                Bson::Binary(Binary { subtype: BinarySubtype::from(sub), bytes })
            }
            "UUID" => {
                let s = str_of(ctx, p)?;
                let bytes = hex_bytes(&s).ok_or_else(|| Error::new(10270, "invalid uuid"))?;
                Bson::Binary(Binary { subtype: BinarySubtype::Uuid, bytes })
            }
            "MinKey" => Bson::MinKey,
            "MaxKey" => Bson::MaxKey,
            "Code" => {
                let v = items(ctx, p)?;
                let code = str_of(ctx, &at(&v, 0))?;
                let scope = at(&v, 1);
                if scope.is_null() || scope.is_undefined() {
                    Bson::JavaScriptCode(code)
                } else {
                    match from_js(ctx, h, &scope)? {
                        Bson::Document(scope) => {
                            Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope { code, scope })
                        }
                        _ => Bson::JavaScriptCode(code),
                    }
                }
            }
            "DBRef" => {
                let v = items(ctx, p)?;
                let r = str_of(ctx, &at(&v, 0))?;
                let id = from_js(ctx, h, &at(&v, 1))?;
                Bson::Document(doc! {"$ref": r, "$id": id})
            }
            _ => Bson::Null,
        })
    }

    fn hex_bytes(s: &str) -> Option<Vec<u8>> {
        if s.len() % 2 != 0 {
            return None;
        }
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
            .collect()
    }

    fn eval_args(args: &[Bson], doc: &Document, vars: &Vars) -> Result<Vec<Bson>> {
        args.iter()
            .map(|a| {
                Ok(match eval(a, doc, vars)? {
                    Bson::Undefined => Bson::Null,
                    v => v,
                })
            })
            .collect::<Result<Vec<Bson>>>()
    }

    fn bson_type_name(b: &Bson) -> &'static str {
        match b {
            Bson::String(_) => "string",
            Bson::Int32(_) => "int",
            Bson::Int64(_) => "long",
            Bson::Double(_) => "double",
            Bson::Boolean(_) => "bool",
            Bson::Null => "null",
            Bson::Array(_) => "array",
            Bson::Document(_) => "object",
            _ => "other",
        }
    }

    // ----------------------------------------------------------------- $where

    pub fn where_matches(code: &Bson, doc: &Document) -> Result<bool> {
        let src = match code {
            Bson::String(s) | Bson::JavaScriptCode(s) => s.as_str(),
            Bson::JavaScriptCodeWithScope(_) => {
                return Err(Error::new(4649201, "$where no longer supports deprecated BSON type CodeWScope"));
            }
            _ => return Err(Error::bad_value("$where got bad type")),
        };
        if src.is_empty() {
            return Err(Error::bad_value("$where got bad type"));
        }
        with_engine(|e| {
            let w = compile_where(e, src)?;
            let this = doc_to_js(&mut e.ctx, &e.h, doc)?;
            let v = call_obj(&mut e.ctx, &w, &this, std::slice::from_ref(&this))?;
            let v = match v.as_object() {
                Some(f) if f.is_callable() => call_obj(&mut e.ctx, &f, &this, std::slice::from_ref(&this))?,
                _ => v,
            };
            Ok(v.to_boolean())
        })
    }

    // -------------------------------------------------------------- $function

    pub fn function_expr(args: &Bson, doc: &Document, vars: &Vars) -> Result<Bson> {
        let Bson::Document(spec) = args else {
            return Err(Error::new(
                31260,
                format!("$function requires an object as an argument, found: {}", bson_type_name(args)),
            ));
        };
        let body = match spec.get("body") {
            None => return Err(Error::new(31261, "The body function must be specified.")),
            Some(Bson::String(s)) | Some(Bson::JavaScriptCode(s)) => s.clone(),
            Some(_) => {
                return Err(Error::new(
                    31262,
                    "Invalid $project :: caused by :: The body function must evaluate to type string or code",
                ));
            }
        };
        match spec.get("lang") {
            None => return Err(Error::new(31418, "The lang field must be specified.")),
            Some(Bson::String(l)) if l == "js" => {}
            Some(_) => {
                return Err(Error::new(
                    31419,
                    "Currently the only supported language specifier is 'js'.",
                ));
            }
        }
        let argv = match spec.get("args") {
            None => return Err(Error::new(31263, "The args field must be specified.")),
            Some(Bson::Array(a)) => a,
            Some(_) => return Err(Error::new(31266, "The args field must be of type array")),
        };
        let vals = eval_args(argv, doc, vars)?;
        with_engine(|e| {
            let f = compile_fn(e, &body)?;
            let mut js_args = Vec::with_capacity(vals.len());
            for v in &vals {
                js_args.push(to_js(&mut e.ctx, &e.h, v)?);
            }
            let r = call_obj(&mut e.ctx, &f, &JsValue::undefined(), &js_args)?;
            from_js(&mut e.ctx, &e.h, &r)
        })
    }

    // ----------------------------------------------------------- $accumulator

    struct JsAccum {
        init: String,
        init_args: Vec<Bson>,
        accumulate: String,
        acc_args: Vec<Bson>,
        finalize: Option<String>,
        state: Option<Bson>,
    }

    fn required_code(d: &Document, key: &str, missing: i32) -> Result<String> {
        match d.get(key) {
            None => Err(Error::new(missing, format!("$accumulator requires '{key}'"))),
            Some(Bson::String(s)) | Some(Bson::JavaScriptCode(s)) => Ok(s.clone()),
            Some(_) => Err(Error::new(4544702, format!("'{key}' must be a function"))),
        }
    }

    fn optional_code(d: &Document, key: &str) -> Result<Option<String>> {
        match d.get(key) {
            None => Ok(None),
            Some(Bson::String(s)) | Some(Bson::JavaScriptCode(s)) => Ok(Some(s.clone())),
            Some(_) => Err(Error::new(4544702, format!("'{key}' must be a function"))),
        }
    }

    fn required_array(d: &Document, key: &str, missing: i32, bad: i32) -> Result<Vec<Bson>> {
        match d.get(key) {
            None => Err(Error::new(missing, format!("$accumulator requires '{key}'"))),
            Some(Bson::Array(a)) => Ok(a.clone()),
            Some(_) => Err(Error::new(bad, format!("'{key}' must be an array"))),
        }
    }

    fn optional_array(d: &Document, key: &str, bad: i32) -> Result<Vec<Bson>> {
        match d.get(key) {
            None => Ok(Vec::new()),
            Some(Bson::Array(a)) => Ok(a.clone()),
            Some(_) => Err(Error::new(bad, format!("'{key}' must be an array"))),
        }
    }

    fn type_name(v: &Bson) -> &'static str {
        match v {
            Bson::String(_) => "string",
            Bson::Int32(_) => "int",
            Bson::Int64(_) => "long",
            Bson::Double(_) => "double",
            Bson::Boolean(_) => "bool",
            Bson::Array(_) => "array",
            Bson::Null => "null",
            _ => "object",
        }
    }

    pub fn accumulator(spec: &Bson) -> Result<Box<dyn Accumulator>> {
        let Bson::Document(d) = spec else {
            return Err(Error::new(4544703, format!("$accumulator expects an object as an argument; found: {}", type_name(spec))));
        };
        if let Some(k) = d.keys().find(|k| !ACC_FIELDS.contains(&k.as_str())) {
            return Err(Error::new(4544706, format!("unrecognized field '{k}' in $accumulator")));
        }
        match d.get("lang") {
            None => {}
            Some(Bson::String(l)) if l == "js" => {}
            Some(_) => return Err(Error::new(4544705, "$accumulator 'lang' must be 'js'")),
        }
        let init = required_code(d, "init", 4544707)?;
        let accumulate = required_code(d, "accumulate", 4544708)?;
        let acc_args = required_array(d, "accumulateArgs", 4544710, 4544712)?;
        required_code(d, "merge", 4544709)?;
        let init_args = optional_array(d, "initArgs", 4544711)?;
        let finalize = optional_code(d, "finalize")?;
        Ok(Box::new(JsAccum { init, init_args, accumulate, acc_args, finalize, state: None }))
    }

    impl JsAccum {
        fn ensure_init(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
            if self.state.is_some() {
                return Ok(());
            }
            let args = eval_args(&self.init_args, doc, vars)?;
            let init = self.init.clone();
            let st = with_engine(|e| {
                let f = compile_fn(e, &init)?;
                let mut js_args = Vec::with_capacity(args.len());
                for v in &args {
                    js_args.push(to_js(&mut e.ctx, &e.h, v)?);
                }
                let r = call_obj(&mut e.ctx, &f, &JsValue::undefined(), &js_args)?;
                from_js(&mut e.ctx, &e.h, &r)
            })?;
            self.state = Some(st);
            Ok(())
        }
    }

    impl Accumulator for JsAccum {
        fn process(&mut self, doc: &Document, vars: &Vars) -> Result<()> {
            let args = eval_args(&self.acc_args, doc, vars)?;
            self.ensure_init(doc, vars)?;
            let state = self.state.take().unwrap_or(Bson::Null);
            let src = self.accumulate.clone();
            let next = with_engine(|e| {
                let f = compile_fn(e, &src)?;
                let mut js_args = Vec::with_capacity(args.len() + 1);
                js_args.push(to_js(&mut e.ctx, &e.h, &state)?);
                for v in &args {
                    js_args.push(to_js(&mut e.ctx, &e.h, v)?);
                }
                let r = call_obj(&mut e.ctx, &f, &JsValue::undefined(), &js_args)?;
                from_js(&mut e.ctx, &e.h, &r)
            })?;
            self.state = Some(next);
            Ok(())
        }

        fn finish(&mut self) -> Result<Bson> {
            self.ensure_init(&Document::new(), &Vars::new())?;
            let state = self.state.clone().unwrap_or(Bson::Null);
            match self.finalize.clone() {
                None => Ok(state),
                Some(src) => with_engine(|e| {
                    let f = compile_fn(e, &src)?;
                    let sv = to_js(&mut e.ctx, &e.h, &state)?;
                    let r = call_obj(&mut e.ctx, &f, &JsValue::undefined(), &[sv])?;
                    from_js(&mut e.ctx, &e.h, &r)
                }),
            }
        }
    }

    // ---------------------------------------------------------------- mapReduce

    #[derive(PartialEq)]
    enum OutKind {
        Inline,
        Replace,
        Merge,
        Reduce,
    }

    struct Out {
        kind: OutKind,
        db: String,
        coll: String,
        explicit_db: bool,
    }

    fn code_field(cmd: &Document, key: &str) -> Result<Option<String>> {
        match cmd.get(key) {
            None => Ok(None),
            Some(Bson::String(s)) | Some(Bson::JavaScriptCode(s)) => Ok(Some(s.clone())),
            Some(Bson::Null) => Ok(None),
            Some(_) => Err(Error::bad_value(format!("{key} must be a function"))),
        }
    }

    fn has_where(d: &Document) -> bool {
        d.iter().any(|(k, v)| {
            k == "$where"
                || match v {
                    Bson::Document(sub) => has_where(sub),
                    Bson::Array(a) => a.iter().any(|x| matches!(x, Bson::Document(s) if has_where(s))),
                    _ => false,
                }
        })
    }

    fn opt_doc(cmd: &Document, key: &str) -> Result<Option<Document>> {
        match cmd.get(key) {
            None | Some(Bson::Null) => Ok(None),
            Some(Bson::Document(d)) => Ok(Some(d.clone())),
            Some(_) => Err(Error::type_mismatch(format!("{key} must be an object"))),
        }
    }

    fn opt_limit(cmd: &Document) -> Result<Option<i64>> {
        let n = match cmd.get("limit") {
            None | Some(Bson::Null) => return Ok(None),
            Some(Bson::Int32(n)) => i64::from(*n),
            Some(Bson::Int64(n)) => *n,
            Some(Bson::Double(f)) if f.fract() == 0.0 => *f as i64,
            Some(_) => return Err(Error::type_mismatch("limit must be a number")),
        };
        if n <= 0 {
            return Err(Error::new(15958, "The limit specified to mapReduce must be positive"));
        }
        Ok(Some(n))
    }

    fn parse_out(cmd: &Document, default_db: &str) -> Result<Out> {
        let default = || default_db.to_string();
        match cmd.get("out") {
            None => Err(Error::new(40414, "BSON field 'mapReduce.out' is missing but a required field")),
            Some(Bson::String(s)) => Ok(Out { kind: OutKind::Replace, db: default(), coll: s.clone(), explicit_db: false }),
            Some(Bson::Document(d)) => {
                if d.is_empty() {
                    return Err(Error::bad_value("'out' must specify inline, replace, merge or reduce"));
                }
                let mut found: Option<(OutKind, String)> = None;
                for (k, v) in d {
                    let kind = match k.as_str() {
                        "inline" => OutKind::Inline,
                        "replace" => OutKind::Replace,
                        "merge" => OutKind::Merge,
                        "reduce" => OutKind::Reduce,
                        "db" | "sharded" | "nonAtomic" => continue,
                        _ => return Err(Error::bad_value(format!("unknown out mode '{k}'"))),
                    };
                    if found.is_some() {
                        return Err(Error::bad_value("'out' must specify only one mode"));
                    }
                    let coll = if kind == OutKind::Inline {
                        String::new()
                    } else {
                        match v {
                            Bson::String(s) => s.clone(),
                            _ => return Err(Error::bad_value("out collection name must be a string")),
                        }
                    };
                    found = Some((kind, coll));
                }
                let (kind, coll) = found
                    .ok_or_else(|| Error::bad_value("'out' must specify inline, replace, merge or reduce"))?;
                let (db, explicit_db) = match d.get("db") {
                    None => (default(), false),
                    Some(Bson::String(s)) => (s.clone(), true),
                    Some(_) => return Err(Error::bad_value("'db' must be a string")),
                };
                Ok(Out { kind, db, coll, explicit_db })
            }
            Some(_) => Err(Error::bad_value("'out' must be a string or an object")),
        }
    }

    fn install_scope(e: &mut Engine, scope: &Document) -> Result<()> {
        let g = e.ctx.global_object();
        for (k, v) in scope {
            let jv = to_js(&mut e.ctx, &e.h, v)?;
            g.set(JsString::from(k.as_str()), jv, false, &mut e.ctx).je()?;
        }
        Ok(())
    }

    fn remove_scope(e: &mut Engine, scope: &Document) {
        let g = e.ctx.global_object();
        for k in scope.keys() {
            let _ = g.delete_property_or_throw(JsString::from(k.as_str()), &mut e.ctx);
        }
    }

    /// Runs `map` on every document and collects the emitted (key, value) pairs.
    fn map_pairs(e: &mut Engine, cx: &CmdCtx, docs: &[Document], map_src: &str) -> Result<Vec<(Bson, Bson)>> {
        let map_fn = compile_fn(e, map_src)?;
        let undef = JsValue::undefined();
        // Drop anything left behind by an earlier failed run.
        call_obj(&mut e.ctx, &e.h.take_emits, &undef, &[])?;
        // `emit` exists only while `map` runs.
        let emit = JsValue::from(e.h.emit.clone());
        let g = e.ctx.global_object();
        g.set(JsString::from("emit"), emit, false, &mut e.ctx).je()?;
        let r = run_map(e, cx, docs, &map_fn);
        let _ = g.delete_property_or_throw(JsString::from("emit"), &mut e.ctx);
        r
    }

    fn run_map(e: &mut Engine, cx: &CmdCtx, docs: &[Document], map_fn: &JsObject) -> Result<Vec<(Bson, Bson)>> {
        let undef = JsValue::undefined();
        let mut pairs = Vec::new();
        for d in docs {
            cx.check_killed()?;
            let this = doc_to_js(&mut e.ctx, &e.h, d)?;
            call_obj(&mut e.ctx, map_fn, &this, &[])?;
            let emitted = call_obj(&mut e.ctx, &e.h.take_emits, &undef, &[])?;
            for pair in items(&mut e.ctx, &emitted)? {
                let kv = items(&mut e.ctx, &pair)?;
                let (Some(k), Some(v)) = (kv.first(), kv.get(1)) else {
                    continue;
                };
                let key = from_js(&mut e.ctx, &e.h, k)?;
                let val = from_js(&mut e.ctx, &e.h, v)?;
                pairs.push((key, val));
            }
        }
        Ok(pairs)
    }

    fn reduce_call(e: &mut Engine, f: &JsObject, key: &Bson, vals: Vec<Bson>) -> Result<Bson> {
        let k = to_js(&mut e.ctx, &e.h, key)?;
        let v = to_js(&mut e.ctx, &e.h, &Bson::Array(vals))?;
        let r = call_obj(&mut e.ctx, f, &JsValue::undefined(), &[k, v])?;
        from_js(&mut e.ctx, &e.h, &r)
    }

    fn finalize_call(e: &mut Engine, f: &JsObject, key: &Bson, value: &Bson) -> Result<Bson> {
        let k = to_js(&mut e.ctx, &e.h, key)?;
        let v = to_js(&mut e.ctx, &e.h, value)?;
        let r = call_obj(&mut e.ctx, f, &JsValue::undefined(), &[k, v])?;
        from_js(&mut e.ctx, &e.h, &r)
    }

    /// Groups pairs by key (key order), reduces every group, then finalizes.
    fn reduce_groups(
        e: &mut Engine,
        mut pairs: Vec<(Bson, Bson)>,
        reduce_src: &str,
        finalize_src: Option<&str>,
    ) -> Result<Vec<(Bson, Bson)>> {
        let reduce_fn = compile_fn(e, reduce_src)?;
        let fin = match finalize_src {
            Some(s) => Some(compile_fn(e, s)?),
            None => None,
        };
        pairs.sort_by(|a, b| cmp_bson(&a.0, &b.0));
        let mut out = Vec::new();
        let mut i = 0;
        while i < pairs.len() {
            let mut j = i + 1;
            while j < pairs.len() && cmp_bson(&pairs[i].0, &pairs[j].0) == Ordering::Equal {
                j += 1;
            }
            let key = pairs[i].0.clone();
            let vals: Vec<Bson> = pairs[i..j].iter().rev().map(|p| p.1.clone()).collect();
            let mut value = reduce_call(e, &reduce_fn, &key, vals)?;
            if let Some(f) = &fin {
                value = finalize_call(e, f, &key, &value)?;
            }
            out.push((key, value));
            i = j;
        }
        Ok(out)
    }

    pub fn map_reduce(cmd_ctx: &mut CmdCtx) -> Result<Document> {
        let cmd = cmd_ctx.cmd.clone();
        let name = cmd_ctx.name.clone();
        let src_coll = match cmd.get(&name) {
            Some(Bson::String(s)) => s.clone(),
            _ => return Err(Error::new(73, "Invalid namespace specified for mapReduce")),
        };
        for k in cmd.keys() {
            if k == &name || k.starts_with('$') || MR_FIELDS.contains(&k.as_str()) {
                continue;
            }
            return Err(Error::new(40415, format!("BSON field 'mapReduce.{k}' is an unknown field")));
        }

        let map_src = code_field(&cmd, "map")?.ok_or_else(|| Error::new(40414, "BSON field 'mapReduce.map' is missing but a required field"))?;
        let reduce_src = code_field(&cmd, "reduce")?.ok_or_else(|| Error::new(40414, "BSON field 'mapReduce.reduce' is missing but a required field"))?;
        let fin_src = code_field(&cmd, "finalize")?;
        let query = opt_doc(&cmd, "query")?;
        if query.as_ref().is_some_and(has_where) {
            return Err(Error::bad_value("$where is not allowed in a mapReduce query"));
        }
        let sort = opt_doc(&cmd, "sort")?;
        let limit = opt_limit(&cmd)?;
        let scope = opt_doc(&cmd, "scope")?.unwrap_or_default();
        let out = parse_out(&cmd, &cmd_ctx.db)?;

        let mut stages: Vec<Document> = Vec::new();
        if let Some(q) = query {
            stages.push(doc! {"$match": q});
        }
        if let Some(s) = sort {
            stages.push(doc! {"$sort": s});
        }
        if let Some(l) = limit {
            stages.push(doc! {"$limit": l});
        }
        // Materialize the input first: a `$where` in the query must not run inside `with_engine`.
        let docs = run_agg_on(cmd_ctx, &src_coll, stages, &cmd, Vars::new())?.collect::<Result<Vec<Document>>>()?;

        let results = with_engine(|e| {
            install_scope(e, &scope)?;
            let r = map_pairs(e, &*cmd_ctx, &docs, &map_src)
                .and_then(|pairs| reduce_groups(e, pairs, &reduce_src, fin_src.as_deref()));
            remove_scope(e, &scope);
            r
        })?;

        let agg = ServerAggCtx { store: cmd_ctx.srv.store.clone() };
        let docs_out: Vec<Document> = results
            .into_iter()
            .map(|(k, v)| doc! {"_id": k, "value": v})
            .collect();
        match out.kind {
            OutKind::Inline => {
                let arr: Vec<Bson> = docs_out.into_iter().map(Bson::Document).collect();
                Ok(doc! {"results": arr, "ok": 1.0})
            }
            OutKind::Replace => {
                agg.replace_collection(&out.db, &out.coll, docs_out)?;
                Ok(out_result(&out))
            }
            OutKind::Merge => {
                for d in docs_out {
                    let id = d.get("_id").cloned().unwrap_or(Bson::Null);
                    let existing = agg.find(&out.db, &out.coll, &doc! {"_id": id.clone()})?;
                    if existing.is_empty() {
                        agg.insert(&out.db, &out.coll, d)?;
                    } else {
                        agg.replace_by_id(&out.db, &out.coll, &id, d)?;
                    }
                }
                Ok(out_result(&out))
            }
            OutKind::Reduce => {
                let reduce_src = reduce_src.clone();
                for d in docs_out {
                    let id = d.get("_id").cloned().unwrap_or(Bson::Null);
                    let existing = agg.find(&out.db, &out.coll, &doc! {"_id": id.clone()})?;
                    match existing.into_iter().next() {
                        None => {
                            agg.insert(&out.db, &out.coll, d)?;
                        }
                        Some(old) => {
                            let old_v = old.get("value").cloned().unwrap_or(Bson::Null);
                            let new_v = d.get("value").cloned().unwrap_or(Bson::Null);
                            let key = id.clone();
                            let combined = with_engine(|e| {
                                let f = compile_fn(e, &reduce_src)?;
                                let reduced = reduce_call(e, &f, &key, vec![old_v, new_v])?;
                                match &fin_src {
                                    Some(s) => {
                                        let fin = compile_fn(e, s)?;
                                        finalize_call(e, &fin, &key, &reduced)
                                    }
                                    None => Ok(reduced),
                                }
                            })?;
                            agg.replace_by_id(&out.db, &out.coll, &id, doc! {"_id": id.clone(), "value": combined})?;
                        }
                    }
                }
                Ok(out_result(&out))
            }
        }
    }

    fn out_result(out: &Out) -> Document {
        let result = if out.explicit_db {
            Bson::Document(doc! {"db": out.db.clone(), "collection": out.coll.clone()})
        } else {
            Bson::String(out.coll.clone())
        };
        doc! {"result": result, "ok": 1.0}
    }
}
