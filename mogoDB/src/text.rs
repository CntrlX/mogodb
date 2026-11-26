//! Lightweight full-text search for `$text` (tokenize, stem, score).
//!
//! Tokens are produced by splitting on non-alphanumeric characters, lowercasing (unless
//! case-sensitive), dropping English stop words, and applying the Porter stemmer to ASCII
//! lowercase words. Query and documents go through the same [`tokenize`].
//!
//! Scoring (per matched field): for each distinct query term `t` present in the field with
//! frequency `f` among `n` tokens of that field, add `weight * (0.5 + 0.5 * f / n)`. For each
//! phrase found in the field's strings, add `weight * 0.5`. Scores are summed over fields.
use crate::bsonutil::META_KEY;
use bson::{Bson, Document};
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct TextQuery {
    pub terms: Vec<String>,
    pub phrases: Vec<String>,
    pub negated: Vec<String>,
    pub case_sensitive: bool,
}

const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they",
    "this", "to", "was", "will", "with", "i", "me", "my", "we", "our", "you", "your", "he", "she",
    "his", "her", "its", "them", "what", "which", "who", "whom", "so", "than", "too", "very", "can",
    "just", "do", "does", "did", "have", "has", "had", "from", "up", "down", "out", "over", "about",
    "all", "any", "both", "each", "few", "more", "most", "other", "some", "own", "same", "only",
    "again", "further", "once", "here", "when", "where", "why", "how", "been", "being", "were", "am",
    "would", "should", "could",
];

fn is_stop(w: &str) -> bool {
    STOP_WORDS.contains(&w)
}

/// Lowercase (unless case-sensitive), split on non-alphanumerics, drop stop words, stem.
pub fn tokenize(s: &str, case_sensitive: bool) -> Vec<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .filter_map(|w| {
            let lower = w.to_lowercase();
            if is_stop(&lower) {
                return None;
            }
            let t = if case_sensitive { w.to_string() } else { lower };
            Some(stem(&t))
        })
        .collect()
}

/// Parse a `$search` string: bare terms, `"quoted phrases"`, `-negated` terms.
///
/// Words inside a quoted phrase are also added to `terms` (or `negated` for `-"..."`), so the
/// phrase contributes to scoring. A phrase is stored normalized (trimmed, lowercased unless
/// case-sensitive) for substring matching.
pub fn parse_search(search: &str, case_sensitive: bool) -> TextQuery {
    let mut q = TextQuery { case_sensitive, ..Default::default() };
    let mut rest = search;
    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            break;
        }
        let negated = if let Some(r) = rest.strip_prefix('-') {
            rest = r;
            if rest.is_empty() || rest.starts_with(char::is_whitespace) {
                // Lone '-' is ignored.
                continue;
            }
            true
        } else {
            false
        };
        let (body, is_phrase, after) = if let Some(r) = rest.strip_prefix('"') {
            match r.find('"') {
                Some(end) => (&r[..end], true, &r[end + 1..]),
                None => (r, true, ""),
            }
        } else {
            let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
            (&rest[..end], false, &rest[end..])
        };
        rest = after;
        let toks = tokenize(body, case_sensitive);
        if is_phrase {
            let norm = body.trim();
            if !norm.is_empty() {
                let norm = if case_sensitive { norm.to_string() } else { norm.to_lowercase() };
                if !negated {
                    q.phrases.push(norm);
                }
            }
        }
        if negated {
            q.negated.extend(toks);
        } else {
            q.terms.extend(toks);
        }
    }
    q
}

/// Porter-style stemmer for ASCII lowercase words; other words are returned unchanged.
fn stem(word: &str) -> String {
    if word.len() <= 2 || !word.bytes().all(|b| b.is_ascii_lowercase()) {
        return word.to_string();
    }
    let mut w = word.as_bytes().to_vec();
    step1a(&mut w);
    step1b(&mut w);
    step1c(&mut w);
    step2(&mut w);
    step3(&mut w);
    step4(&mut w);
    step5(&mut w);
    String::from_utf8(w).unwrap_or_else(|_| word.to_string())
}

fn is_cons(w: &[u8], i: usize) -> bool {
    match w[i] {
        b'a' | b'e' | b'i' | b'o' | b'u' => false,
        b'y' => i == 0 || !is_cons(w, i - 1),
        _ => true,
    }
}

/// Porter measure m: number of VC sequences in `w`.
fn measure(w: &[u8]) -> usize {
    let n = w.len();
    let mut i = 0;
    while i < n && is_cons(w, i) {
        i += 1;
    }
    let mut m = 0;
    loop {
        while i < n && !is_cons(w, i) {
            i += 1;
        }
        if i >= n {
            break;
        }
        while i < n && is_cons(w, i) {
            i += 1;
        }
        m += 1;
    }
    m
}

fn has_vowel(w: &[u8]) -> bool {
    (0..w.len()).any(|i| !is_cons(w, i))
}

fn double_cons(w: &[u8]) -> bool {
    let n = w.len();
    n >= 2 && w[n - 1] == w[n - 2] && is_cons(w, n - 1)
}

/// consonant-vowel-consonant, final consonant not w, x or y.
fn cvc(w: &[u8]) -> bool {
    let n = w.len();
    n >= 3
        && is_cons(w, n - 3)
        && !is_cons(w, n - 2)
        && is_cons(w, n - 1)
        && !matches!(w[n - 1], b'w' | b'x' | b'y')
}

fn replace_suffix(w: &mut Vec<u8>, suffix: &str, repl: &str) {
    let n = w.len() - suffix.len();
    w.truncate(n);
    w.extend_from_slice(repl.as_bytes());
}

fn step1a(w: &mut Vec<u8>) {
    if w.ends_with(b"sses") || w.ends_with(b"ies") {
        w.truncate(w.len() - 2);
    } else if w.ends_with(b"ss") {
    } else if w.ends_with(b"s") {
        w.pop();
    }
}

fn step1b(w: &mut Vec<u8>) {
    if w.ends_with(b"eed") {
        if measure(&w[..w.len() - 3]) > 0 {
            w.pop();
        }
        return;
    }
    let suffix = if w.ends_with(b"ed") {
        2
    } else if w.ends_with(b"ing") {
        3
    } else {
        return;
    };
    let stem_len = w.len() - suffix;
    if !has_vowel(&w[..stem_len]) {
        return;
    }
    w.truncate(stem_len);
    if w.ends_with(b"at") || w.ends_with(b"bl") || w.ends_with(b"iz") {
        w.push(b'e');
    } else if double_cons(w) && !matches!(w[w.len() - 1], b'l' | b's' | b'z') {
        w.pop();
    } else if measure(w) == 1 && cvc(w) {
        w.push(b'e');
    }
}

fn step1c(w: &mut Vec<u8>) {
    if w.ends_with(b"y") && has_vowel(&w[..w.len() - 1]) {
        let n = w.len();
        w[n - 1] = b'i';
    }
}

/// Apply the first matching suffix rule (list order matters) when the stem measure is > 0.
fn apply_rules(w: &mut Vec<u8>, rules: &[(&str, &str)]) {
    if let Some((suf, repl)) = rules.iter().find(|(s, _)| w.ends_with(s.as_bytes())) {
        if measure(&w[..w.len() - suf.len()]) > 0 {
            replace_suffix(w, suf, repl);
        }
    }
}

fn step2(w: &mut Vec<u8>) {
    apply_rules(
        w,
        &[
            ("ational", "ate"),
            ("tional", "tion"),
            ("enci", "ence"),
            ("anci", "ance"),
            ("izer", "ize"),
            ("bli", "ble"),
            ("alli", "al"),
            ("entli", "ent"),
            ("eli", "e"),
            ("ousli", "ous"),
            ("ization", "ize"),
            ("ation", "ate"),
            ("ator", "ate"),
            ("alism", "al"),
            ("iveness", "ive"),
            ("fulness", "ful"),
            ("ousness", "ous"),
            ("aliti", "al"),
            ("iviti", "ive"),
            ("biliti", "ble"),
        ],
    );
}

fn step3(w: &mut Vec<u8>) {
    apply_rules(
        w,
        &[
            ("icate", "ic"),
            ("ative", ""),
            ("alize", "al"),
            ("iciti", "ic"),
            ("ical", "ic"),
            ("ful", ""),
            ("ness", ""),
        ],
    );
}

fn step4(w: &mut Vec<u8>) {
    const SUFFIXES: &[&str] = &[
        "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent", "ion",
        "ou", "ism", "ate", "iti", "ous", "ive", "ize",
    ];
    let Some(suf) = SUFFIXES.iter().find(|s| w.ends_with(s.as_bytes())) else { return };
    let stem_len = w.len() - suf.len();
    let st = &w[..stem_len];
    let ok_ion = *suf != "ion" || matches!(st.last(), Some(b's' | b't'));
    if measure(st) > 1 && ok_ion {
        w.truncate(stem_len);
    }
}

fn step5(w: &mut Vec<u8>) {
    if w.ends_with(b"e") {
        let st = &w[..w.len() - 1];
        let m = measure(st);
        if m > 1 || (m == 1 && !cvc(st)) {
            w.pop();
        }
    }
    if measure(w) > 1 && double_cons(w) && w.ends_with(b"l") {
        w.pop();
    }
}

/// Collect string values at dot-path `segs` (array fan-out, no index support).
fn collect_val(v: &Bson, segs: &[&str], out: &mut Vec<String>) {
    match v {
        Bson::Array(items) => {
            for it in items {
                collect_val(it, segs, out);
            }
        }
        Bson::Document(d) if !segs.is_empty() => collect_doc(d, segs, out),
        Bson::String(s) if segs.is_empty() => out.push(s.clone()),
        _ => {}
    }
}

fn collect_doc(d: &Document, segs: &[&str], out: &mut Vec<String>) {
    if let Some((first, rest)) = segs.split_first() {
        if let Some(v) = d.get(*first) {
            collect_val(v, rest, out);
        }
    }
}

/// Every string anywhere in `v`, skipping the reserved metadata key.
fn collect_all(v: &Bson, out: &mut Vec<String>) {
    match v {
        Bson::String(s) => out.push(s.clone()),
        Bson::Array(items) => items.iter().for_each(|it| collect_all(it, out)),
        Bson::Document(d) => collect_all_doc(d, out),
        _ => {}
    }
}

fn collect_all_doc(d: &Document, out: &mut Vec<String>) {
    for (k, v) in d {
        if k != META_KEY {
            collect_all(v, out);
        }
    }
}

/// Score `doc` against `q` over `fields` `(path, weight)`; path `"$**"` = every string field.
/// `None` when the document does not match.
pub fn score(doc: &Document, fields: &[(String, f64)], q: &TextQuery) -> Option<f64> {
    if q.terms.is_empty() && q.phrases.is_empty() {
        return None;
    }
    // Per configured field: its strings, weight.
    let mut field_strings: Vec<(f64, Vec<String>)> = Vec::with_capacity(fields.len());
    for (path, weight) in fields {
        let mut strings = Vec::new();
        if path == "$**" {
            collect_all_doc(doc, &mut strings);
        } else {
            let segs: Vec<&str> = path.split('.').collect();
            collect_doc(doc, &segs, &mut strings);
        }
        field_strings.push((*weight, strings));
    }

    let norm = |s: &str| if q.case_sensitive { s.to_string() } else { s.to_lowercase() };
    let phrase_in = |strings: &[String], phrase: &str| {
        strings.iter().any(|s| norm(s).contains(phrase))
    };

    // Every phrase must appear as a substring somewhere in the weighted fields.
    let all_strings: Vec<&String> = field_strings.iter().flat_map(|(_, s)| s.iter()).collect();
    for p in &q.phrases {
        if !all_strings.iter().any(|s| norm(s).contains(p.as_str())) {
            return None;
        }
    }

    // Token statistics per field.
    let mut field_tokens: Vec<(f64, HashMap<String, usize>, usize)> = Vec::new();
    for (w, strings) in &field_strings {
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut n = 0usize;
        for s in strings {
            for t in tokenize(s, q.case_sensitive) {
                *counts.entry(t).or_insert(0) += 1;
                n += 1;
            }
        }
        field_tokens.push((*w, counts, n));
    }

    let present = |t: &str| field_tokens.iter().any(|(_, c, _)| c.contains_key(t));
    if q.negated.iter().any(|t| present(t)) {
        return None;
    }
    let mut uniq_terms: Vec<&str> = q.terms.iter().map(String::as_str).collect();
    uniq_terms.sort_unstable();
    uniq_terms.dedup();
    let any_term = uniq_terms.iter().any(|t| present(t));
    let phrase_only = q.terms.is_empty();
    if !(any_term || phrase_only) {
        return None;
    }

    let mut score = 0.0;
    for ((w, counts, n), (_, strings)) in field_tokens.iter().zip(field_strings.iter()) {
        if *n > 0 {
            for t in &uniq_terms {
                if let Some(&f) = counts.get(*t) {
                    score += w * (0.5 + 0.5 * f as f64 / *n as f64);
                }
            }
        }
        for p in &q.phrases {
            if phrase_in(strings, p) {
                score += w * 0.5;
            }
        }
    }
    Some(score)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn stop_words_dropped() {
        assert_eq!(tokenize("the cat is on a mat", false), vec!["cat", "mat"]);
        assert!(tokenize("and or the", false).is_empty());
    }

    #[test]
    fn tokenize_splits_and_lowercases() {
        assert_eq!(tokenize("Hello, WORLD!", false), vec!["hello", "world"]);
        assert_eq!(tokenize("Hello WORLD", true), vec!["Hello", "WORLD"]);
        assert_eq!(tokenize("café naïve", false), vec!["café", "naïve"]);
    }

    #[test]
    fn stemming_examples() {
        assert_eq!(stem("running"), "run");
        assert_eq!(stem("cats"), "cat");
        assert_eq!(stem("caresses"), "caress");
        assert_eq!(stem("ponies"), "poni");
        assert_eq!(stem("happily"), "happili");
        assert_eq!(stem("relational"), "relat");
        assert_eq!(stem("generalization"), "gener");
        assert_eq!(stem("agreed"), "agre");
        assert_eq!(stem("hopping"), "hop");
        assert_eq!(stem("hoping"), "hope");
        assert_eq!(stem("sky"), "sky");
        assert_eq!(stem("Running"), "Running");
        // query and document stems agree
        assert_eq!(tokenize("running cats", false), tokenize("RUNNING CAT", false));
    }

    #[test]
    fn parse_search_forms() {
        let q = parse_search(r#"coffee "fresh beans" -tea -"green tea""#, false);
        assert_eq!(q.phrases, vec!["fresh beans".to_string()]);
        assert!(q.terms.contains(&"coffe".to_string()) || q.terms.contains(&"coffee".to_string()));
        assert!(q.terms.contains(&"fresh".to_string()));
        assert!(q.terms.contains(&"bean".to_string()));
        assert!(q.negated.contains(&"tea".to_string()));
        assert!(q.negated.contains(&"green".to_string()));
        assert!(!q.negated.contains(&"coffee".to_string()));
        assert!(q.case_sensitive == false);
    }

    fn fields() -> Vec<(String, f64)> {
        vec![("title".to_string(), 10.0), ("body".to_string(), 1.0)]
    }

    #[test]
    fn match_terms_phrases_negation() {
        let d1 = doc! {"title": "Coffee brewing", "body": "Fresh beans make great coffee"};
        let d2 = doc! {"title": "Tea time", "body": "Green tea and coffee"};
        let d3 = doc! {"title": "Coffee", "body": "beans are fresh"};

        let q = parse_search("fresh beans", false);
        assert!(score(&d1, &fields(), &q).is_some());
        assert!(score(&d3, &fields(), &q).is_some());
        assert!(score(&d2, &fields(), &q).is_none());

        // phrase must appear contiguously
        let pq = parse_search("\"fresh beans\"", false);
        assert!(score(&d1, &fields(), &pq).is_some());
        assert!(score(&d3, &fields(), &pq).is_none());

        // negation excludes docs containing the term
        let nq = parse_search("coffee -tea", false);
        assert!(score(&d1, &fields(), &nq).is_some());
        assert!(score(&d2, &fields(), &nq).is_none());

        // phrase-only query (no bare terms)
        let only = parse_search("\"beans make\"", false);
        assert!(score(&d1, &fields(), &only).is_some());
        assert!(score(&d3, &fields(), &only).is_none());

        // nothing matches
        assert!(score(&d1, &fields(), &parse_search("zebra", false)).is_none());
        // stop-word-only query matches nothing
        assert!(score(&d1, &fields(), &parse_search("the", false)).is_none());
    }

    #[test]
    fn case_sensitive_phrase() {
        let d = doc! {"body": "Fresh Beans"};
        let f = vec![("body".to_string(), 1.0)];
        assert!(score(&d, &f, &parse_search("\"fresh beans\"", false)).is_some());
        assert!(score(&d, &f, &parse_search("\"fresh beans\"", true)).is_none());
        assert!(score(&d, &f, &parse_search("\"Fresh Beans\"", true)).is_some());
        // case-sensitive terms do not match different case
        assert!(score(&d, &f, &parse_search("beans", true)).is_none());
    }

    #[test]
    fn wildcard_and_arrays_and_nested() {
        let d = doc! {
            "meta": {"tags": ["Rust", "database"]},
            "deep": {"x": [{"y": "mongo compatible"}]},
            "n": 5,
            "$__meta": {"textScore": "ignored zebra"}
        };
        let wild = vec![("$**".to_string(), 1.0)];
        assert!(score(&d, &wild, &parse_search("rust", false)).is_some());
        assert!(score(&d, &wild, &parse_search("mongo", false)).is_some());
        // metadata subdocument is not searched
        assert!(score(&d, &wild, &parse_search("zebra", false)).is_none());
        // a specific path with array fan-out
        let tags = vec![("meta.tags".to_string(), 1.0)];
        assert!(score(&d, &tags, &parse_search("database", false)).is_some());
        assert!(score(&d, &tags, &parse_search("mongo", false)).is_none());
        // nested array of docs
        let deep = vec![("deep.x.y".to_string(), 1.0)];
        assert!(score(&d, &deep, &parse_search("compatible", false)).is_some());
        // non-string values are ignored
        assert!(score(&d, &vec![("n".to_string(), 1.0)], &parse_search("5", false)).is_none());
    }

    #[test]
    fn weights_increase_score() {
        let d = doc! {"title": "coffee", "body": "coffee"};
        let q = parse_search("coffee", false);
        let heavy_title = score(&d, &[("title".to_string(), 10.0), ("body".to_string(), 1.0)], &q).unwrap();
        let light_title = score(&d, &[("title".to_string(), 1.0), ("body".to_string(), 1.0)], &q).unwrap();
        assert!(heavy_title > light_title);

        // more occurrences score higher within a field
        let one = doc! {"body": "coffee shop"};
        let two = doc! {"body": "coffee coffee shop"};
        let f = vec![("body".to_string(), 1.0)];
        assert!(score(&two, &f, &q).unwrap() > score(&one, &f, &q).unwrap());
    }

    #[test]
    fn empty_query_matches_nothing() {
        let d = doc! {"body": "anything"};
        let f = vec![("body".to_string(), 1.0)];
        assert!(score(&d, &f, &TextQuery::default()).is_none());
    }
}
