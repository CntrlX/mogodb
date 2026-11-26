//! SCRAM-SHA-256 / SCRAM-SHA-1 server-side SASL (RFC 5802 / 7677) as used by MongoDB.
use crate::error::{Error, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use bson::{Bson, Document, doc};
use hmac::{Hmac, Mac};
use md5::Md5;
use rand::RngCore;
use sha1::Sha1;
use sha2::{Digest, Sha256};

const SHA1_ITERATIONS: u32 = 10_000;
const SHA1_SALT_LEN: usize = 16;
const SHA256_ITERATIONS: u32 = 15_000;
const SHA256_SALT_LEN: usize = 28;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mech {
    Sha1,
    Sha256,
}

impl Mech {
    fn parse(s: &str) -> Option<Mech> {
        match s {
            "SCRAM-SHA-1" => Some(Mech::Sha1),
            "SCRAM-SHA-256" => Some(Mech::Sha256),
            _ => None,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Mech::Sha1 => "SCRAM-SHA-1",
            Mech::Sha256 => "SCRAM-SHA-256",
        }
    }
    fn hash_len(self) -> usize {
        match self {
            Mech::Sha1 => 20,
            Mech::Sha256 => 32,
        }
    }
    fn iterations(self) -> u32 {
        match self {
            Mech::Sha1 => SHA1_ITERATIONS,
            Mech::Sha256 => SHA256_ITERATIONS,
        }
    }
    fn salt_len(self) -> usize {
        match self {
            Mech::Sha1 => SHA1_SALT_LEN,
            Mech::Sha256 => SHA256_SALT_LEN,
        }
    }
}

fn hash(mech: Mech, data: &[u8]) -> Vec<u8> {
    match mech {
        Mech::Sha1 => Sha1::digest(data).to_vec(),
        Mech::Sha256 => Sha256::digest(data).to_vec(),
    }
}

fn hmac(mech: Mech, key: &[u8], data: &[u8]) -> Vec<u8> {
    // HMAC accepts keys of any length, so construction cannot fail.
    match mech {
        Mech::Sha1 => {
            let mut m = <Hmac<Sha1> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
            m.update(data);
            m.finalize().into_bytes().to_vec()
        }
        Mech::Sha256 => {
            let mut m = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
            m.update(data);
            m.finalize().into_bytes().to_vec()
        }
    }
}

fn salted_password(mech: Mech, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    match mech {
        Mech::Sha1 => {
            let mut out = [0u8; 20];
            pbkdf2::pbkdf2_hmac::<Sha1>(password, salt, iterations, &mut out);
            out.to_vec()
        }
        Mech::Sha256 => {
            let mut out = [0u8; 32];
            pbkdf2::pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out);
            out.to_vec()
        }
    }
}

fn xor(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b).map(|(x, y)| x ^ y).collect()
}

/// Length-checked comparison without early exit on the first differing byte.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Minimal SASLprep (RFC 4013): drops "mapped to nothing" code points and maps
/// non-ASCII spaces to U+0020. ASCII passes through unchanged. NFKC is not applied.
fn sasl_prep(s: &str) -> String {
    s.chars()
        .filter_map(|c| {
            let u = c as u32;
            if matches!(u, 0x00AD | 0x034F | 0x1806 | 0x180B..=0x180D | 0x200B..=0x200D | 0x2060 | 0xFE00..=0xFE0F | 0xFEFF) {
                None
            } else if matches!(u, 0x00A0 | 0x1680 | 0x2000..=0x200A | 0x202F | 0x205F | 0x3000) {
                Some(' ')
            } else {
                Some(c)
            }
        })
        .collect()
}

fn md5_hex(s: &str) -> String {
    Md5::digest(s.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

/// Build one mechanism's credential subdocument from explicit salt bytes.
fn build_creds(username: &str, password: &str, mech: Mech, salt: &[u8], iterations: u32) -> Document {
    // SCRAM-SHA-1 uses MongoDB's legacy digest of the password (not SASLprep'd).
    let kdf_password = match mech {
        Mech::Sha1 => md5_hex(&format!("{username}:mongo:{password}")),
        Mech::Sha256 => sasl_prep(password),
    };
    let salted = salted_password(mech, kdf_password.as_bytes(), salt, iterations);
    let client_key = hmac(mech, &salted, b"Client Key");
    let stored_key = hash(mech, &client_key);
    let server_key = hmac(mech, &salted, b"Server Key");
    doc! {
        "iterationCount": iterations as i32,
        "salt": B64.encode(salt),
        "storedKey": B64.encode(&stored_key),
        "serverKey": B64.encode(&server_key),
    }
}

/// Build the `credentials` subdocument stored in `admin.system.users`:
/// `{ "SCRAM-SHA-1": {iterationCount, salt, storedKey, serverKey}, "SCRAM-SHA-256": {...} }`.
pub fn make_credentials(username: &str, password: &str, mechanisms: &[&str]) -> Document {
    let mut out = Document::new();
    let mut rng = rand::thread_rng();
    for name in mechanisms {
        let Some(mech) = Mech::parse(name) else { continue };
        let mut salt = vec![0u8; mech.salt_len()];
        rng.fill_bytes(&mut salt);
        out.insert(mech.name(), build_creds(username, password, mech, &salt, mech.iterations()));
    }
    out
}

struct StoredCreds {
    iterations: u32,
    salt: String,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

fn parse_creds(doc: &Document, mech: Mech) -> Option<StoredCreds> {
    let sub = doc.get_document(mech.name()).ok()?;
    let iterations = match sub.get("iterationCount")? {
        Bson::Int32(i) => i64::from(*i),
        Bson::Int64(i) => *i,
        Bson::Double(d) => *d as i64,
        _ => return None,
    };
    let iterations = u32::try_from(iterations).ok().filter(|i| *i > 0)?;
    let salt = sub.get_str("salt").ok()?.to_string();
    let stored_key = B64.decode(sub.get_str("storedKey").ok()?).ok()?;
    let server_key = B64.decode(sub.get_str("serverKey").ok()?).ok()?;
    if stored_key.len() != mech.hash_len() || server_key.len() != mech.hash_len() {
        return None;
    }
    Some(StoredCreds { iterations, salt, stored_key, server_key })
}

fn auth_failed() -> Error {
    Error::new(18, "Authentication failed.")
}

fn decode_username(s: &str) -> String {
    s.replace("=2C", ",").replace("=3D", "=")
}

/// Returns (gs2 header such as `n,,`, client-first-bare).
fn parse_client_first(payload: &[u8]) -> Result<(String, String)> {
    let msg = std::str::from_utf8(payload)
        .map_err(|_| Error::bad_value("SCRAM client-first message is not valid UTF-8"))?;
    let mut parts = msg.splitn(3, ',');
    let flag = parts.next().unwrap_or("");
    let authzid = parts.next().unwrap_or("");
    let bare = parts
        .next()
        .ok_or_else(|| Error::failed_to_parse("Invalid SCRAM client-first message"))?;
    if flag.starts_with("p=") {
        return Err(Error::bad_value("SCRAM channel binding is not supported"));
    }
    if flag != "n" && flag != "y" {
        return Err(Error::failed_to_parse("Invalid SCRAM gs2 header"));
    }
    if !authzid.is_empty() {
        return Err(Error::bad_value("SCRAM authzid is not supported"));
    }
    Ok((format!("{flag},,"), bare.to_string()))
}

/// Parses `n=<user>,r=<cnonce>[,...]` from client-first-bare.
fn parse_bare(bare: &str) -> Result<(&str, &str)> {
    let mut attrs = bare.split(',');
    let n = attrs
        .next()
        .and_then(|a| a.strip_prefix("n="))
        .ok_or_else(|| Error::failed_to_parse("Invalid SCRAM username attribute"))?;
    let r = attrs
        .next()
        .and_then(|a| a.strip_prefix("r="))
        .filter(|r| !r.is_empty())
        .ok_or_else(|| Error::failed_to_parse("Invalid SCRAM nonce attribute"))?;
    Ok((n, r))
}

pub struct ScramConversation {
    mech: Mech,
    mechanism: String,
    username: String,
    gs2: String,
    client_first_bare: String,
    server_first: String,
    nonce: String,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
    state: State,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    AwaitProof,
    AwaitDone,
    Finished,
}

impl ScramConversation {
    /// Handle `saslStart`: parse client-first message, return (state, server-first message).
    /// `lookup(username)` returns the user's `credentials` document.
    pub fn start(mechanism: &str, payload: &[u8], lookup: &dyn Fn(&str) -> Option<Document>) -> Result<(ScramConversation, Vec<u8>)> {
        let mut snonce_bytes = [0u8; 24];
        rand::thread_rng().fill_bytes(&mut snonce_bytes);
        Self::start_internal(mechanism, payload, lookup, B64.encode(snonce_bytes))
    }

    /// `start` with a caller-supplied server nonce suffix (used by tests for fixed vectors).
    fn start_internal(mechanism: &str, payload: &[u8], lookup: &dyn Fn(&str) -> Option<Document>, snonce: String) -> Result<(ScramConversation, Vec<u8>)> {
        let mech = Mech::parse(mechanism).ok_or_else(|| {
            Error::bad_value(format!("Received authentication for mechanism {mechanism} which is not enabled"))
        })?;
        let (gs2, bare) = parse_client_first(payload)?;
        let (raw_user, cnonce) = parse_bare(&bare)?;
        let username = decode_username(raw_user);
        let creds = lookup(&username)
            .and_then(|d| parse_creds(&d, mech))
            .ok_or_else(auth_failed)?;
        let nonce = format!("{cnonce}{snonce}");
        let server_first = format!("r={nonce},s={},i={}", creds.salt, creds.iterations);
        let conv = ScramConversation {
            mech,
            mechanism: mech.name().to_string(),
            username,
            gs2,
            client_first_bare: bare,
            server_first: server_first.clone(),
            nonce,
            stored_key: creds.stored_key,
            server_key: creds.server_key,
            state: State::AwaitProof,
        };
        Ok((conv, server_first.into_bytes()))
    }

    /// Handle `saslContinue`. Returns (server payload, done).
    pub fn step(&mut self, payload: &[u8]) -> Result<(Vec<u8>, bool)> {
        let state = std::mem::replace(&mut self.state, State::Finished);
        match state {
            State::Finished => Err(conversation_finished()),
            State::AwaitDone => {
                if payload.is_empty() {
                    Ok((Vec::new(), true))
                } else {
                    Err(conversation_finished())
                }
            }
            State::AwaitProof => {
                let server_final = self.verify_client_final(payload)?;
                self.state = State::AwaitDone;
                Ok((server_final, false))
            }
        }
    }

    fn verify_client_final(&self, payload: &[u8]) -> Result<Vec<u8>> {
        let msg = std::str::from_utf8(payload).map_err(|_| auth_failed())?;
        let idx = msg.rfind(",p=").ok_or_else(|| Error::failed_to_parse("Invalid SCRAM client-final message"))?;
        let without_proof = &msg[..idx];
        let proof = B64.decode(&msg[idx + 3..]).map_err(|_| auth_failed())?;

        let mut attrs = without_proof.split(',');
        let c = attrs.next().and_then(|a| a.strip_prefix("c=")).ok_or_else(|| Error::failed_to_parse("Invalid SCRAM channel binding"))?;
        let r = attrs.next().and_then(|a| a.strip_prefix("r=")).ok_or_else(|| Error::failed_to_parse("Invalid SCRAM nonce"))?;
        if c != B64.encode(self.gs2.as_bytes()) || r != self.nonce {
            return Err(auth_failed());
        }
        if proof.len() != self.mech.hash_len() {
            return Err(auth_failed());
        }

        let auth_message = format!("{},{},{}", self.client_first_bare, self.server_first, without_proof);
        let client_signature = hmac(self.mech, &self.stored_key, auth_message.as_bytes());
        let client_key = xor(&proof, &client_signature);
        if !ct_eq(&hash(self.mech, &client_key), &self.stored_key) {
            return Err(auth_failed());
        }
        let server_signature = hmac(self.mech, &self.server_key, auth_message.as_bytes());
        Ok(format!("v={}", B64.encode(&server_signature)).into_bytes())
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn mechanism(&self) -> &str {
        &self.mechanism
    }
}

fn conversation_finished() -> Error {
    Error::new(17, "Attempted to continue a SASL conversation after it finished.")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Client-side SCRAM exchange, computing the proof from `kdf_password`.
    fn client_exchange(mech: &str, user: &str, kdf_password: &str, creds: &Document, cnonce: &str) -> Result<()> {
        let lookup = |u: &str| if u == "alice" { Some(creds.clone()) } else { None };
        let m = Mech::parse(mech).expect("mech");
        let bare = format!("n={user},r={cnonce}");
        let client_first = format!("n,,{bare}");
        let (mut conv, sf) = ScramConversation::start(mech, client_first.as_bytes(), &lookup)?;
        let sf = String::from_utf8(sf).expect("utf8");
        let nonce = sf.split(',').next().and_then(|a| a.strip_prefix("r=")).expect("r=").to_string();
        let salt = B64.decode(sf.split(',').nth(1).and_then(|a| a.strip_prefix("s=")).expect("s=")).expect("b64");
        let iters: u32 = sf.split(',').nth(2).and_then(|a| a.strip_prefix("i=")).expect("i=").parse().expect("iters");

        let salted = salted_password(m, kdf_password.as_bytes(), &salt, iters);
        let client_key = hmac(m, &salted, b"Client Key");
        let stored_key = hash(m, &client_key);
        let server_key = hmac(m, &salted, b"Server Key");
        let without_proof = format!("c=biws,r={nonce}");
        let auth = format!("{bare},{sf},{without_proof}");
        let client_sig = hmac(m, &stored_key, auth.as_bytes());
        let proof = xor(&client_key, &client_sig);
        let client_final = format!("{without_proof},p={}", B64.encode(&proof));

        let (server_final, done) = conv.step(client_final.as_bytes())?;
        assert!(!done);
        let expected_sig = format!("v={}", B64.encode(hmac(m, &server_key, auth.as_bytes())));
        assert_eq!(String::from_utf8(server_final).expect("utf8"), expected_sig);
        let (empty, done) = conv.step(b"")?;
        assert!(done);
        assert!(empty.is_empty());
        assert_eq!(conv.username(), user);
        assert_eq!(conv.mechanism(), mech);
        Ok(())
    }

    #[test]
    fn full_exchange_sha256_and_sha1() {
        let creds = make_credentials("alice", "s3cret", &["SCRAM-SHA-256", "SCRAM-SHA-1"]);
        let sha256_pw = "s3cret";
        let sha1_pw = md5_hex("alice:mongo:s3cret");
        client_exchange("SCRAM-SHA-256", "alice", sha256_pw, &creds, "clientnonce1").expect("sha256");
        client_exchange("SCRAM-SHA-1", "alice", &sha1_pw, &creds, "clientnonce2").expect("sha1");
    }

    #[test]
    fn wrong_password_is_rejected_with_18() {
        let creds = make_credentials("alice", "s3cret", &["SCRAM-SHA-256", "SCRAM-SHA-1"]);
        let err = client_exchange("SCRAM-SHA-256", "alice", "wrong", &creds, "n1").unwrap_err();
        assert_eq!(err.code, 18);
        let err = client_exchange("SCRAM-SHA-1", "alice", &md5_hex("alice:mongo:wrong"), &creds, "n2").unwrap_err();
        assert_eq!(err.code, 18);
    }

    #[test]
    fn unknown_user_and_mechanism_return_18() {
        let creds = make_credentials("alice", "s3cret", &["SCRAM-SHA-256"]);
        let err = client_exchange("SCRAM-SHA-256", "bob", "s3cret", &creds, "n").unwrap_err();
        assert_eq!(err.code, 18);
        // SHA-1 credentials absent -> 18
        let err = client_exchange("SCRAM-SHA-1", "alice", "x", &creds, "n").unwrap_err();
        assert_eq!(err.code, 18);
    }

    #[test]
    fn unknown_mechanism_is_code_2() {
        let lookup = |_: &str| None;
        let err = ScramConversation::start("PLAIN", b"n,,n=a,r=b", &lookup).err().expect("err");
        assert_eq!(err.code, 2);
    }

    #[test]
    fn channel_binding_and_escaped_username() {
        let creds = make_credentials("a,b=c", "pw", &["SCRAM-SHA-256"]);
        let lookup = |u: &str| if u == "a,b=c" { Some(creds.clone()) } else { None };
        assert!(ScramConversation::start("SCRAM-SHA-256", b"p=tls-unique,,n=a,r=b", &lookup).is_err());
        let (conv, _) = ScramConversation::start("SCRAM-SHA-256", b"n,,n=a=2Cb=3Dc,r=xyz", &lookup).expect("start");
        assert_eq!(conv.username(), "a,b=c");
    }

    #[test]
    fn sasl_prep_ascii_passthrough_and_mapping() {
        assert_eq!(sasl_prep("pencil"), "pencil");
        assert_eq!(sasl_prep("a\u{00A0}b"), "a b");
        assert_eq!(sasl_prep("a\u{00AD}b"), "ab");
    }

    /// RFC 7677 SCRAM-SHA-256 example: user "user", password "pencil".
    #[test]
    fn rfc7677_vector() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let creds = doc! {
            "SCRAM-SHA-256": build_creds("user", "pencil", Mech::Sha256, &salt, 4096)
        };
        let lookup = |u: &str| if u == "user" { Some(creds.clone()) } else { None };
        let snonce = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let (mut conv, sf) = ScramConversation::start_internal(
            "SCRAM-SHA-256",
            b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO",
            &lookup,
            snonce.to_string(),
        )
        .expect("start");
        assert_eq!(
            String::from_utf8(sf).expect("utf8"),
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        );
        let client_final = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
        let (sv, done) = conv.step(client_final.as_bytes()).expect("step");
        assert!(!done);
        assert_eq!(String::from_utf8(sv).expect("utf8"), "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
        let (_, done) = conv.step(b"").expect("done");
        assert!(done);
        assert_eq!(conv.step(b"").unwrap_err().code, 17);
    }
}
