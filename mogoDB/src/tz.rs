//! Olson time zones from the system zoneinfo database (TZif v1–v3, RFC 8536),
//! including the POSIX TZ footer rule for instants after the last transition.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

/// A parsed zone: transition instants (UTC seconds) with their UTC offsets (seconds).
#[derive(Debug)]
pub struct Zone {
    transitions: Vec<i64>,
    offsets: Vec<i64>,
    /// Offset before the first transition.
    initial: i64,
    rule: Option<PosixRule>,
}

#[derive(Debug, Clone, Copy)]
enum DayRule {
    /// Mm.w.d — month, week 1..=5 (5 = last), weekday 0 = Sunday.
    MonthWeekDay(u32, u32, u32),
    /// Jn — day 1..=365, February 29 never counted.
    Julian1(u32),
    /// n — zero-based day 0..=365, counting February 29.
    Julian0(u32),
}

#[derive(Debug, Clone, Copy)]
struct Transition {
    day: DayRule,
    /// Local wall-clock seconds after midnight (may be negative or > 24h).
    time: i64,
}

#[derive(Debug, Clone, Copy)]
struct PosixRule {
    std_off: i64,
    dst: Option<(i64, Transition, Transition)>,
}

static CACHE: LazyLock<Mutex<HashMap<String, Option<Arc<Zone>>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Load a zone by Olson name (e.g. `"America/New_York"`). `None` if unknown.
pub fn load(name: &str) -> Option<Arc<Zone>> {
    if name.is_empty() || name.starts_with('/') || name.contains("..") || name.contains('\0') {
        return None;
    }
    if let Some(z) = CACHE.lock().ok()?.get(name) {
        return z.clone();
    }
    let mut dirs: Vec<String> = Vec::new();
    if let Ok(d) = std::env::var("TZDIR") {
        dirs.push(d);
    }
    dirs.extend(["/usr/share/zoneinfo", "/usr/lib/zoneinfo", "/usr/share/lib/zoneinfo"].map(String::from));
    let zone = dirs
        .iter()
        .find_map(|d| std::fs::read(std::path::Path::new(d).join(name)).ok())
        .and_then(|b| parse_tzif(&b))
        .map(Arc::new);
    if let Ok(mut c) = CACHE.lock() {
        c.insert(name.to_string(), zone.clone());
    }
    zone
}

fn be32(b: &[u8], at: usize) -> Option<u32> {
    b.get(at..at + 4).map(|s| u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

fn parse_tzif(b: &[u8]) -> Option<Zone> {
    if b.get(0..4)? != b"TZif" {
        return None;
    }
    let version = *b.get(4)?;
    let counts = |at: usize| -> Option<[usize; 6]> {
        let mut c = [0usize; 6];
        for (i, v) in c.iter_mut().enumerate() {
            *v = be32(b, at + 20 + i * 4)? as usize;
        }
        Some(c)
    };
    // [isutcnt, isstdcnt, leapcnt, timecnt, typecnt, charcnt]
    let c1 = counts(0)?;
    let v1_len = c1[3] * 5 + c1[4] * 6 + c1[5] + c1[2] * 8 + c1[1] + c1[0];
    let (hdr, tsize, c) = if version >= b'2' {
        let h2 = 44 + v1_len;
        if b.get(h2..h2 + 4)? != b"TZif" {
            return None;
        }
        (h2, 8usize, counts(h2)?)
    } else {
        (0, 4usize, c1)
    };
    let (timecnt, typecnt, charcnt, leapcnt) = (c[3], c[4], c[5], c[2]);
    let mut p = hdr + 44;
    let mut transitions = Vec::with_capacity(timecnt);
    for i in 0..timecnt {
        let at = p + i * tsize;
        let t = if tsize == 8 {
            i64::from_be_bytes(b.get(at..at + 8)?.try_into().ok()?)
        } else {
            be32(b, at)? as i32 as i64
        };
        transitions.push(t);
    }
    p += timecnt * tsize;
    let idx: Vec<usize> = b.get(p..p + timecnt)?.iter().map(|&x| x as usize).collect();
    p += timecnt;
    let mut types = Vec::with_capacity(typecnt);
    for i in 0..typecnt {
        let at = p + i * 6;
        types.push(be32(b, at)? as i32 as i64);
    }
    p += typecnt * 6 + charcnt + leapcnt * (tsize + 4) + c[1] + c[0];
    let offsets = idx.iter().map(|&i| types.get(i).copied()).collect::<Option<Vec<i64>>>()?;
    let initial = *types.first()?;
    let rule = if version >= b'2' {
        let rest = b.get(p..)?;
        let s = std::str::from_utf8(rest).ok()?;
        let footer = s.strip_prefix('\n')?.split('\n').next()?;
        parse_posix(footer)
    } else {
        None
    };
    Some(Zone { transitions, offsets, initial, rule })
}

// ---------------------------------------------------------------- POSIX TZ strings

struct P<'a> {
    s: &'a [u8],
    i: usize,
}

impl P<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }
    fn eat(&mut self, c: u8) -> bool {
        if self.peek() == Some(c) {
            self.i += 1;
            true
        } else {
            false
        }
    }
    fn name(&mut self) -> Option<()> {
        if self.eat(b'<') {
            while self.peek()? != b'>' {
                self.i += 1;
            }
            self.i += 1;
            return Some(());
        }
        let start = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_alphabetic()) {
            self.i += 1;
        }
        (self.i - start >= 3).then_some(())
    }
    fn num(&mut self) -> Option<i64> {
        let start = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
        std::str::from_utf8(&self.s[start..self.i]).ok()?.parse().ok()
    }
    /// `[+-]hh[:mm[:ss]]` in seconds.
    fn hms(&mut self) -> Option<i64> {
        let sign = if self.eat(b'-') {
            -1
        } else {
            self.eat(b'+');
            1
        };
        let mut v = self.num()? * 3600;
        if self.eat(b':') {
            v += self.num()? * 60;
            if self.eat(b':') {
                v += self.num()?;
            }
        }
        Some(sign * v)
    }
    fn transition(&mut self) -> Option<Transition> {
        let day = if self.eat(b'M') {
            let m = self.num()? as u32;
            self.eat(b'.').then_some(())?;
            let w = self.num()? as u32;
            self.eat(b'.').then_some(())?;
            let d = self.num()? as u32;
            DayRule::MonthWeekDay(m, w, d)
        } else if self.eat(b'J') {
            DayRule::Julian1(self.num()? as u32)
        } else {
            DayRule::Julian0(self.num()? as u32)
        };
        let time = if self.eat(b'/') { self.hms()? } else { 7200 };
        Some(Transition { day, time })
    }
}

fn parse_posix(s: &str) -> Option<PosixRule> {
    if s.is_empty() {
        return None;
    }
    let mut p = P { s: s.as_bytes(), i: 0 };
    p.name()?;
    // POSIX offsets are west-positive; UTC offsets are east-positive.
    let std_off = -p.hms()?;
    if p.peek().is_none() {
        return Some(PosixRule { std_off, dst: None });
    }
    p.name()?;
    let dst_off = if p.peek().is_some_and(|c| c != b',') { -p.hms()? } else { std_off + 3600 };
    p.eat(b',').then_some(())?;
    let start = p.transition()?;
    p.eat(b',').then_some(())?;
    let end = p.transition()?;
    Some(PosixRule { std_off, dst: Some((dst_off, start, end)) })
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn year_of_days(days: i64) -> i64 {
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    yoe + era * 400 + i64::from(m <= 2)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
    }
}

/// Local midnight (days since epoch) of a rule's transition day in `year`.
fn rule_day(year: i64, r: DayRule) -> i64 {
    match r {
        DayRule::MonthWeekDay(m, w, d) => {
            let first = days_from_civil(year, m, 1);
            let wd_first = (first + 4).rem_euclid(7) as u32; // 1970-01-01 was a Thursday
            let mut day = 1 + (d + 7 - wd_first) % 7 + (w.saturating_sub(1)) * 7;
            while day > days_in_month(year, m) {
                day -= 7;
            }
            days_from_civil(year, m, day)
        }
        DayRule::Julian1(n) => {
            let n = n as i64;
            let leap_skip = i64::from(is_leap(year) && n >= 60);
            days_from_civil(year, 1, 1) + n - 1 + leap_skip
        }
        DayRule::Julian0(n) => days_from_civil(year, 1, 1) + n as i64,
    }
}

impl PosixRule {
    fn offset_at(&self, t: i64) -> i64 {
        let Some((dst_off, start, end)) = self.dst else { return self.std_off };
        let year = year_of_days((t + self.std_off).div_euclid(86400));
        // DST starts at local standard time and ends at local daylight time.
        let s = rule_day(year, start.day) * 86400 + start.time - self.std_off;
        let e = rule_day(year, end.day) * 86400 + end.time - dst_off;
        let in_dst = if s < e { t >= s && t < e } else { !(t >= e && t < s) };
        if in_dst { dst_off } else { self.std_off }
    }
}

impl Zone {
    fn offset_secs(&self, t: i64) -> i64 {
        match self.transitions.partition_point(|&x| x <= t) {
            0 => {
                if self.transitions.is_empty() {
                    self.rule.map(|r| r.offset_at(t)).unwrap_or(self.initial)
                } else {
                    self.initial
                }
            }
            n if n == self.transitions.len() => match &self.rule {
                Some(r) => r.offset_at(t),
                None => self.offsets[n - 1],
            },
            n => self.offsets[n - 1],
        }
    }

    /// UTC offset in seconds in effect at the UTC instant `utc_ms`.
    pub fn offset_at_utc(&self, utc_ms: i64) -> i64 {
        self.offset_secs(utc_ms.div_euclid(1000))
    }

    /// UTC offset in seconds for a local wall-clock time (`local_ms` = millis as if local were UTC).
    /// Ambiguous (repeated) times use the earlier offset; skipped times use the offset in effect
    /// before the transition.
    pub fn offset_for_local(&self, local_ms: i64) -> i64 {
        let l = local_ms.div_euclid(1000);
        let before = self.offset_secs(l - 86400);
        let after = self.offset_secs(l + 86400);
        for o in [before, after] {
            if self.offset_secs(l - o) == o {
                return o;
            }
        }
        before
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(y: i64, m: u32, d: u32, h: i64) -> i64 {
        (days_from_civil(y, m, d) * 86400 + h * 3600) * 1000
    }

    fn zone(name: &str) -> Option<Arc<Zone>> {
        if !std::path::Path::new("/usr/share/zoneinfo").join(name).exists() {
            return None;
        }
        load(name)
    }

    #[test]
    fn new_york_dst_and_footer_rule() {
        let Some(z) = zone("America/New_York") else { return };
        assert_eq!(z.offset_at_utc(ms(2024, 1, 15, 12)), -5 * 3600);
        assert_eq!(z.offset_at_utc(ms(2024, 7, 15, 12)), -4 * 3600);
        assert_eq!(z.offset_at_utc(ms(2100, 7, 1, 12)), -4 * 3600);
        assert_eq!(z.offset_at_utc(ms(2100, 12, 1, 12)), -5 * 3600);
        // 2024-03-10 12:00 local is after the 02:00 spring-forward.
        assert_eq!(z.offset_for_local(ms(2024, 3, 10, 12)), -4 * 3600);
        assert_eq!(z.offset_for_local(ms(2024, 3, 9, 12)), -5 * 3600);
    }

    #[test]
    fn other_zones() {
        if let Some(z) = zone("Europe/London") {
            assert_eq!(z.offset_at_utc(ms(2024, 7, 1, 0)), 3600);
            assert_eq!(z.offset_at_utc(ms(2024, 12, 1, 0)), 0);
        }
        if let Some(z) = zone("Asia/Kolkata") {
            assert_eq!(z.offset_at_utc(ms(2024, 7, 1, 0)), 19800);
        }
        if let Some(z) = zone("Australia/Sydney") {
            assert_eq!(z.offset_at_utc(ms(2099, 1, 1, 0)), 11 * 3600);
            assert_eq!(z.offset_at_utc(ms(2099, 7, 1, 0)), 10 * 3600);
        }
        assert!(load("../etc/passwd").is_none());
        assert!(load("Nowhere/City").is_none());
    }

    #[test]
    fn posix_rules() {
        let r = parse_posix("CET-1CEST,M3.5.0,M10.5.0/3").unwrap();
        assert_eq!(r.std_off, 3600);
        assert_eq!(r.offset_at(ms(2030, 7, 1, 0) / 1000), 7200);
        assert_eq!(r.offset_at(ms(2030, 1, 1, 0) / 1000), 3600);
        assert_eq!(parse_posix("<+0530>-5:30").unwrap().std_off, 19800);
    }
}
