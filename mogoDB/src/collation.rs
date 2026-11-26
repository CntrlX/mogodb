//! Collation: locale-aware string comparison (pure-Rust Unicode Collation Algorithm subset).
//!
//! The active collation is thread-local: commands install it with [`enter`] for the
//! duration of their execution (and cursors for each `getMore`), and every string
//! comparison in `bsonutil::cmp_bson` consults [`current`].

use crate::error::{Error, Result};
use bson::{Bson, Document, doc};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::sync::Arc;
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::is_combining_mark;

/// The single collator version MongoDB 9.0 ships with.
const VERSION: &str = "57.1";

/// Locales MongoDB 9.0 accepts (ICU collator locales, matched exactly).
const LOCALES: &[&str] = &[
    "af", "am", "ar", "as", "az", "be", "bg", "bn", "bo", "bs", "ca", "chr", "cs", "cy", "da",
    "de", "de_AT", "de@collation=phonebook", "dsb", "dz", "ee", "el", "en", "en_US", "en_US_POSIX",
    "eo", "es", "et", "fa", "fi", "fil", "fo", "fr", "fr_CA", "ga", "gl", "gu", "ha", "haw", "he",
    "hi", "hr", "hsb", "hu", "hy", "id", "ig", "is", "it", "ja", "ka", "kk", "kl", "km", "kn", "ko",
    "kok", "ky", "lb", "lkt", "ln", "lo", "lt", "lv", "mk", "ml", "mn", "mr", "ms", "mt", "my",
    "nb", "ne", "nl", "nn", "om", "or", "pa", "pl", "ps", "pt", "ro", "ru", "se", "si", "sk", "sl",
    "smn", "sq", "sr", "sr_Latn", "sv", "sw", "ta", "te", "th", "to", "tr", "uk", "ur", "vi", "wae",
    "yo", "zh", "zh_Hant", "zu",
];

const FIELDS: &[&str] = &[
    "locale",
    "caseLevel",
    "caseFirst",
    "strength",
    "numericOrdering",
    "alternate",
    "maxVariable",
    "normalization",
    "backwards",
    "version",
];

// Primary weight classes (high byte), in MongoDB/ICU order:
// whitespace < punctuation < symbols < currency < digits < Latin letters < other letters.
const P_WS: u32 = 0x0100_0000;
const P_PUNCT: u32 = 0x0200_0000;
const P_SYM: u32 = 0x0300_0000;
const P_CUR: u32 = 0x0400_0000;
const P_DIGIT: u32 = 0x0500_0000;
const P_LATIN: u32 = 0x0600_0000;
const P_OTHER: u32 = 0x0700_0000;

/// Secondary weight of an unaccented base character (combining marks sort above it).
const SEC_BASE: u8 = 0x10;
const STROKE: u8 = 0x1B;

const WS: &str = "\t\n\u{b}\u{c}\r \u{a0}";
const PUNCT: &str = "_-–—―,;:!¡?¿.…·'‘’‚‹›\"“”„«»()[]{}§¶@*/\\&#%‰†‡•′″";
const SYMBOLS: &str = "`΄´˜^¯˘˙¨˚˝¸˛°℃©®+±÷×<=>¬|¦~ːˑ";
const CURRENCY: &str = "¤¢$£¥€";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CaseFirst {
    Off,
    Upper,
    Lower,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Collation {
    pub locale: String,
    /// 1..=5 (primary .. identical). Default 3.
    pub strength: u8,
    pub case_level: bool,
    pub case_first: CaseFirst,
    pub numeric_ordering: bool,
    /// `alternate: "shifted"` (ignore whitespace/punctuation below quaternary level).
    pub alternate_shifted: bool,
    /// `maxVariable: "space"` (only whitespace is ignorable) vs `"punct"` (default).
    pub max_variable_space: bool,
    /// French secondary ordering (accents compared from the end).
    pub backwards: bool,
    pub normalization: bool,
}

/// One collation element.
#[derive(Clone, Copy)]
struct Ce {
    /// Primary weight (0 = no primary, e.g. combining marks).
    p: u32,
    /// Secondary weight.
    s: u8,
    /// Tertiary weight.
    t: u8,
    /// Case-level byte (1 = sorts earlier, 2 = sorts later).
    k: u8,
    /// Variable (whitespace / punctuation): ignorable under `alternate: shifted`.
    var: bool,
}

enum Special {
    /// Separate primary sorting right after the given base letter.
    Sep(char),
    /// Base letter plus one mark with the given secondary weight.
    Attach(char, u8),
    /// Expansion into several letters, all tertiary variants.
    Expand(&'static str),
}

impl Collation {
    /// Parse a collation spec. `Ok(None)` for `{locale: "simple"}`.
    /// Errors: 40414 missing locale, 40415 unknown field, 14 wrong type, 2 BadValue, 161 bad version.
    pub fn parse(spec: &Document) -> Result<Option<Arc<Collation>>> {
        let mut locale: Option<String> = None;
        let mut case_level = false;
        let mut case_first = CaseFirst::Off;
        let mut strength: u8 = 3;
        let mut numeric = false;
        let mut shifted = false;
        let mut max_var_space = false;
        let mut normalization = false;
        let mut backwards: Option<bool> = None;

        for (k, v) in spec.iter() {
            if !FIELDS.contains(&k.as_str()) {
                return Err(Error::new(
                    40415,
                    format!("BSON field 'create.collation.{k}' is an unknown field."),
                ));
            }
            if matches!(v, Bson::Null) {
                continue;
            }
            match k.as_str() {
                "locale" => locale = Some(string_field("locale", v)?),
                "caseLevel" => case_level = bool_field("caseLevel", v)?,
                "caseFirst" => {
                    let s = string_field("caseFirst", v)?;
                    case_first = match s.as_str() {
                        "off" => CaseFirst::Off,
                        "upper" => CaseFirst::Upper,
                        "lower" => CaseFirst::Lower,
                        _ => return Err(enum_error("caseFirst", &s)),
                    };
                }
                "strength" => strength = strength_field(v)?,
                "numericOrdering" => numeric = bool_field("numericOrdering", v)?,
                "alternate" => {
                    let s = string_field("alternate", v)?;
                    shifted = match s.as_str() {
                        "non-ignorable" => false,
                        "shifted" => true,
                        _ => return Err(enum_error("alternate", &s)),
                    };
                }
                "maxVariable" => {
                    let s = string_field("maxVariable", v)?;
                    max_var_space = match s.as_str() {
                        "space" => true,
                        "punct" => false,
                        _ => return Err(enum_error("maxVariable", &s)),
                    };
                }
                "normalization" => normalization = bool_field("normalization", v)?,
                "backwards" => {
                    backwards = Some(match v {
                        Bson::Boolean(b) => *b,
                        _ => {
                            return Err(Error::type_mismatch(format!(
                                "Field 'backwards' should be a boolean value, but found: {}",
                                type_name(v)
                            )));
                        }
                    })
                }
                "version" => {
                    let s = string_field("version", v)?;
                    if s != VERSION {
                        return Err(Error::new(
                            161,
                            format!(
                                "Requested collation version {s} but the only available collator version was {VERSION}."
                            ),
                        ));
                    }
                }
                _ => {}
            }
        }

        let locale = locale.ok_or_else(|| {
            Error::new(
                40414,
                "BSON field 'create.collation.locale' is missing but a required field",
            )
        })?;
        if locale.is_empty() {
            return Err(Error::bad_value("Field 'locale' cannot be the empty string"));
        }
        if locale == "simple" {
            return Ok(None);
        }
        if !LOCALES.contains(&locale.as_str()) {
            return Err(Error::bad_value(format!(
                "Field 'locale' is invalid in: {{ locale: \"{locale}\" }}"
            )));
        }
        let backwards = backwards.unwrap_or(locale == "fr_CA");
        Ok(Some(Arc::new(Collation {
            locale,
            strength,
            case_level,
            case_first,
            numeric_ordering: numeric,
            alternate_shifted: shifted,
            max_variable_space: max_var_space,
            backwards,
            normalization,
        })))
    }

    /// Fully-specified spec as MongoDB reports it (`locale`, `caseLevel`, `caseFirst`, `strength`,
    /// `numericOrdering`, `alternate`, `maxVariable`, `normalization`, `backwards`, `version`).
    pub fn to_doc(&self) -> Document {
        doc! {
            "locale": self.locale.as_str(),
            "caseLevel": self.case_level,
            "caseFirst": match self.case_first {
                CaseFirst::Off => "off",
                CaseFirst::Upper => "upper",
                CaseFirst::Lower => "lower",
            },
            "strength": Bson::Int32(i32::from(self.strength)),
            "numericOrdering": self.numeric_ordering,
            "alternate": if self.alternate_shifted { "shifted" } else { "non-ignorable" },
            "maxVariable": if self.max_variable_space { "space" } else { "punct" },
            "normalization": self.normalization,
            "backwards": self.backwards,
            "version": VERSION,
        }
    }

    pub fn cmp_str(&self, a: &str, b: &str) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        self.sort_key(a).cmp(&self.sort_key(b))
    }

    /// Byte-comparable key: `sort_key(a).cmp(&sort_key(b)) == cmp_str(a, b)`.
    ///
    /// Levels are concatenated, each terminated by a 0 byte. Every weight byte is non-zero, and
    /// 4-byte primaries start with a non-zero byte, so level boundaries never collide.
    pub fn sort_key(&self, s: &str) -> Vec<u8> {
        let ces = self.elements(s);
        let shifted = self.alternate_shifted;
        let kept = |ce: &&Ce| !(shifted && ce.var);
        let mut key = Vec::with_capacity(ces.len() * 6 + 8);

        // Primary.
        for ce in ces.iter().filter(kept).filter(|ce| ce.p != 0) {
            key.extend_from_slice(&ce.p.to_be_bytes());
        }
        key.push(0);

        // Secondary.
        if self.strength >= 2 {
            let mut sec: Vec<u8> = ces.iter().filter(kept).map(|ce| ce.s).collect();
            if self.backwards {
                sec.reverse();
            }
            key.extend_from_slice(&sec);
            key.push(0);
        }

        // Case level (between secondary and tertiary).
        if self.case_level {
            key.extend(
                ces.iter()
                    .filter(kept)
                    .filter(|ce| ce.p != 0)
                    .map(|ce| ce.k),
            );
            key.push(0);
        }

        // Tertiary.
        if self.strength >= 3 {
            key.extend(ces.iter().filter(kept).map(|ce| ce.t));
            key.push(0);
        }

        // Quaternary: only meaningful under shifted alternates.
        if self.strength >= 4 {
            for ce in &ces {
                let q = if shifted && ce.var { ce.p } else { u32::MAX };
                key.extend_from_slice(&q.to_be_bytes());
            }
            key.push(0);
        }

        // Identical: code points of the canonical decomposition.
        if self.strength >= 5 {
            for ch in s.nfd() {
                key.extend_from_slice(&(ch as u32).to_be_bytes());
            }
        }
        key
    }

    fn flips(&self, upper: bool) -> bool {
        upper != (self.case_first == CaseFirst::Upper)
    }

    fn tert(&self, upper: bool, variant: bool) -> u8 {
        1 + if self.flips(upper) { 5 } else { 0 } + if variant { 0x10 } else { 0 }
    }

    fn ce(&self, p: u32, upper: bool, variant: bool, var: bool) -> Ce {
        Ce {
            p,
            s: SEC_BASE,
            t: self.tert(upper, variant),
            k: if self.flips(upper) { 2 } else { 1 },
            var,
        }
    }

    fn mark(&self, weight: u8, variant: bool) -> Ce {
        Ce {
            p: 0,
            s: weight,
            t: 1 + if variant { 0x10 } else { 0 },
            k: 0,
            var: false,
        }
    }

    /// Collation elements of a whole string (numeric runs collapsed when `numericOrdering`).
    fn elements(&self, s: &str) -> Vec<Ce> {
        let chars: Vec<char> = s.chars().collect();
        let mut out = Vec::with_capacity(chars.len() + 4);
        let mut i = 0;
        while i < chars.len() {
            if self.numeric_ordering && chars[i].is_ascii_digit() {
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                let run = &chars[start..i];
                let first = run.iter().position(|&c| c != '0').unwrap_or(run.len() - 1);
                let digits = &run[first..];
                out.push(self.ce(P_DIGIT + 0x100 + digits.len() as u32, false, false, false));
                for d in digits {
                    out.push(self.ce(P_DIGIT + 0x10 + (*d as u32 - '0' as u32), false, false, false));
                }
            } else {
                self.push_char(chars[i], false, &mut out);
                i += 1;
            }
        }
        out
    }

    fn push_char(&self, c: char, variant: bool, out: &mut Vec<Ce>) {
        let upper = c.is_uppercase();
        if let Some(i) = index_in(WS, c) {
            out.push(self.ce(P_WS + 1 + i, false, variant, true));
            return;
        }
        if is_ignorable(c) {
            return;
        }
        if is_combining_mark(c) {
            out.push(self.mark(mark_weight(c as u32), variant));
            return;
        }
        if let Some(i) = index_in(PUNCT, c) {
            out.push(self.ce(P_PUNCT + i, false, variant, !self.max_variable_space));
            return;
        }
        if let Some(i) = index_in(SYMBOLS, c) {
            out.push(self.ce(P_SYM + i, false, variant, false));
            return;
        }
        if let Some(i) = index_in(CURRENCY, c) {
            out.push(self.ce(P_CUR + i, false, variant, false));
            return;
        }
        if c.is_ascii_digit() {
            out.push(self.ce(P_DIGIT + 0x10 + (c as u32 - '0' as u32), false, variant, false));
            return;
        }
        if c.is_ascii_alphabetic() {
            out.push(self.ce(letter_primary(c), upper, variant, false));
            return;
        }
        if let Some(sp) = special(c) {
            match sp {
                Special::Sep(base) => {
                    out.push(self.ce(letter_primary(base) + 0x10, upper, variant, false));
                }
                Special::Attach(base, w) => {
                    out.push(self.ce(letter_primary(base), upper, variant, false));
                    out.push(self.mark(w, variant));
                }
                Special::Expand(seq) => {
                    for ch in seq.chars() {
                        out.push(self.ce(letter_primary(ch), upper, true, false));
                    }
                }
            }
            return;
        }
        let nfd: Vec<char> = c.nfd().collect();
        if nfd != [c] {
            for ch in nfd {
                self.push_char(ch, variant, out);
            }
            return;
        }
        let nfkd: Vec<char> = c.nfkd().collect();
        if nfkd != [c] {
            for ch in nfkd {
                self.push_char(ch, true, out);
            }
            return;
        }
        if c.is_alphabetic() || c.is_numeric() {
            out.push(self.ce(P_OTHER + c as u32, upper, variant, false));
        } else {
            out.push(self.ce(P_SYM + 0x1000 + c as u32, false, variant, false));
        }
    }
}

fn index_in(table: &str, c: char) -> Option<u32> {
    table.chars().position(|x| x == c).map(|i| i as u32)
}

fn letter_primary(c: char) -> u32 {
    P_LATIN + (c.to_ascii_lowercase() as u32 - 'a' as u32) * 0x100
}

fn is_ignorable(c: char) -> bool {
    c.is_control()
        || matches!(c, '\u{ad}' | '\u{200b}'..='\u{200f}' | '\u{2060}'..='\u{2064}' | '\u{feff}')
}

/// Secondary weight of a combining mark, in MongoDB's accent order.
fn mark_weight(cp: u32) -> u8 {
    match cp {
        0x301 => 0x11,
        0x300 => 0x12,
        0x306 => 0x13,
        0x302 => 0x14,
        0x30C => 0x15,
        0x30A => 0x16,
        0x308 => 0x17,
        0x30B => 0x18,
        0x303 => 0x19,
        0x307 => 0x1A,
        0x327 => 0x1C,
        0x328 => 0x1D,
        0x304 => 0x1E,
        0x30F => 0x1F,
        0x311 => 0x20,
        _ => 0x21 + (cp % 0xDE) as u8,
    }
}

fn special(c: char) -> Option<Special> {
    let lower = c.to_lowercase().next().unwrap_or(c);
    Some(match lower {
        'æ' => Special::Sep('a'),
        'œ' => Special::Sep('o'),
        'ø' => Special::Attach('o', STROKE),
        'đ' | 'ð' => Special::Attach('d', STROKE),
        'ł' => Special::Attach('l', STROKE),
        'ħ' => Special::Attach('h', STROKE),
        'ŧ' => Special::Attach('t', STROKE),
        'ƀ' => Special::Attach('b', STROKE),
        'ƒ' => Special::Attach('f', 0x21),
        'ı' => Special::Sep('i'),
        'ŋ' => Special::Sep('n'),
        'þ' => Special::Sep('z'),
        'ĸ' => Special::Sep('k'),
        'ß' => Special::Expand("ss"),
        _ => return None,
    })
}

fn type_name(v: &Bson) -> &'static str {
    match v {
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Double(_) => "double",
        Bson::Decimal128(_) => "decimal",
        Bson::Boolean(_) => "bool",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Null => "null",
        _ => "other",
    }
}

fn string_field(name: &str, v: &Bson) -> Result<String> {
    match v {
        Bson::String(s) => Ok(s.clone()),
        _ => Err(Error::type_mismatch(format!(
            "BSON field 'create.collation.{name}' is the wrong type '{}', expected type 'string'",
            type_name(v)
        ))),
    }
}

fn bool_field(name: &str, v: &Bson) -> Result<bool> {
    match v {
        Bson::Boolean(b) => Ok(*b),
        _ => Err(Error::type_mismatch(format!(
            "BSON field 'create.collation.{name}' is the wrong type '{}', expected type 'bool'",
            type_name(v)
        ))),
    }
}

fn enum_error(name: &str, value: &str) -> Error {
    Error::bad_value(format!(
        "Enumeration value '{value}' for field 'create.collation.{name}' is not a valid value."
    ))
}

fn strength_field(v: &Bson) -> Result<u8> {
    let n = match v {
        Bson::Int32(n) => f64::from(*n),
        Bson::Int64(n) => *n as f64,
        Bson::Double(d) => *d,
        Bson::Decimal128(d) => d.to_string().parse::<f64>().unwrap_or(f64::NAN),
        _ => {
            return Err(Error::type_mismatch(format!(
                "BSON field 'create.collation.strength' is the wrong type '{}', expected types '[long, int, decimal, double]'",
                type_name(v)
            )));
        }
    };
    let t = n.trunc();
    if t.is_nan() || t < 1.0 {
        return Err(Error::bad_value(format!(
            "Enumeration value '{}' for field 'collation.strength' is not a valid value.",
            t as i64
        )));
    }
    if t > 5.0 {
        return Err(Error::bad_value(format!(
            "BSON field 'strength' value must be <= 5, actual value '{}'",
            t as i64
        )));
    }
    Ok(t as u8)
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<Collation>>> = const { RefCell::new(None) };
}

/// The collation active on this thread (`None` = simple binary comparison).
pub fn current() -> Option<Arc<Collation>> {
    CURRENT.with(|c| c.borrow().clone())
}

/// Restores the previous collation when dropped.
pub struct Guard(Option<Arc<Collation>>);

impl Drop for Guard {
    fn drop(&mut self) {
        let prev = self.0.take();
        CURRENT.with(|c| *c.borrow_mut() = prev);
    }
}

/// Install `c` as the active collation until the returned guard is dropped.
pub fn enter(c: Option<Arc<Collation>>) -> Guard {
    Guard(CURRENT.with(|cur| std::mem::replace(&mut *cur.borrow_mut(), c)))
}

/// Compare two strings under the active collation.
pub fn cmp_current(a: &str, b: &str) -> Ordering {
    CURRENT.with(|c| match &*c.borrow() {
        Some(col) => col.cmp_str(a, b),
        None => a.as_bytes().cmp(b.as_bytes()),
    })
}
