//! Client-side SCRAM (RFC 5802) for SCRAM-SHA-1 and SCRAM-SHA-256.

use crate::error::{Error, Result};
use base64::Engine;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::Sha256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mechanism {
    Sha1,
    Sha256,
}

impl Mechanism {
    pub fn name(self) -> &'static str {
        match self {
            Mechanism::Sha1 => "SCRAM-SHA-1",
            Mechanism::Sha256 => "SCRAM-SHA-256",
        }
    }
    pub fn parse(s: &str) -> Option<Mechanism> {
        match s {
            "SCRAM-SHA-1" => Some(Mechanism::Sha1),
            "SCRAM-SHA-256" => Some(Mechanism::Sha256),
            _ => None,
        }
    }
    fn hmac(self, key: &[u8], data: &[u8]) -> Vec<u8> {
        match self {
            Mechanism::Sha1 => {
                let mut m = <Hmac<Sha1> as Mac>::new_from_slice(key).expect("any key length");
                m.update(data);
                m.finalize().into_bytes().to_vec()
            }
            Mechanism::Sha256 => {
                let mut m = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("any key length");
                m.update(data);
                m.finalize().into_bytes().to_vec()
            }
        }
    }
    fn hash(self, data: &[u8]) -> Vec<u8> {
        match self {
            Mechanism::Sha1 => <Sha1 as sha1::Digest>::digest(data).to_vec(),
            Mechanism::Sha256 => <Sha256 as sha2::Digest>::digest(data).to_vec(),
        }
    }
    fn hi(self, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
        let mut out = vec![0u8; self.hash(&[]).len()];
        match self {
            Mechanism::Sha1 => pbkdf2::pbkdf2_hmac::<Sha1>(password, salt, iterations, &mut out),
            Mechanism::Sha256 => pbkdf2::pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out),
        }
        out
    }
}

fn escape_username(user: &str) -> String {
    user.replace('=', "=3D").replace(',', "=2C")
}

/// One SCRAM conversation. `client_first()` returns the payload for `saslStart`; each
/// `step()` consumes a server payload and returns the next client payload.
pub struct ScramClient {
    mech: Mechanism,
    client_nonce: String,
    client_first_bare: String,
    auth_message: String,
    server_signature: Option<Vec<u8>>,
    password: Vec<u8>,
    finished: bool,
}

impl ScramClient {
    pub fn new(mech: Mechanism, user: &str, password: &str) -> Self {
        Self::with_nonce(mech, user, password, None)
    }

    pub fn with_nonce(mech: Mechanism, user: &str, password: &str, nonce: Option<String>) -> Self {
        let nonce = nonce.unwrap_or_else(|| base64::engine::general_purpose::STANDARD.encode(rand::random::<[u8; 18]>()));
        let client_first_bare = format!("n={},r={}", escape_username(user), nonce);
        ScramClient {
            mech,
            client_nonce: nonce,
            client_first_bare,
            auth_message: String::new(),
            server_signature: None,
            password: password.as_bytes().to_vec(),
            finished: false,
        }
    }

    /// `gs2-header,,client-first-bare`, as sent to `saslStart`.
    pub fn client_first(&self) -> Vec<u8> {
        format!("n,,{}", self.client_first_bare).into_bytes()
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Consume the server-first message and produce the client-final message.
    pub fn step(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        if self.finished {
            return Err(Error::new(crate::error::ErrorKind::Auth, "SCRAM conversation already finished"));
        }
        let text = std::str::from_utf8(payload).map_err(|_| Error::new(crate::error::ErrorKind::Auth, "SCRAM payload is not UTF-8"))?;
        if let Some(err) = text.strip_prefix("e=") {
            return Err(Error::new(crate::error::ErrorKind::Auth, format!("SCRAM error: {err}")));
        }
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for part in text.split(',') {
            if let Some(v) = part.strip_prefix("r=") {
                nonce = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("s=") {
                salt = Some(base64::engine::general_purpose::STANDARD.decode(v).map_err(|_| Error::new(crate::error::ErrorKind::Auth, "SCRAM salt is not base64"))?);
            } else if let Some(v) = part.strip_prefix("i=") {
                iterations = v.parse::<u32>().ok();
            }
        }
        let (nonce, salt, iterations) = match (nonce, salt, iterations) {
            (Some(n), Some(s), Some(i)) if i > 0 => (n, s, i),
            _ => return Err(Error::new(crate::error::ErrorKind::Auth, "malformed SCRAM server-first message")),
        };
        if !nonce.starts_with(&self.client_nonce) {
            return Err(Error::new(crate::error::ErrorKind::Auth, "SCRAM server nonce does not extend the client nonce"));
        }
        let salted = self.mech.hi(&self.password, &salt, iterations);
        let client_key = self.mech.hmac(&salted, b"Client Key");
        let stored_key = self.mech.hash(&client_key);
        let client_final_wo_proof = format!("c=biws,r={nonce}");
        self.auth_message = format!("{},{text},{client_final_wo_proof}", self.client_first_bare);
        let client_signature = self.mech.hmac(&stored_key, self.auth_message.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(&client_signature).map(|(a, b)| a ^ b).collect();
        let server_key = self.mech.hmac(&salted, b"Server Key");
        self.server_signature = Some(self.mech.hmac(&server_key, self.auth_message.as_bytes()));
        Ok(format!("{},p={}", client_final_wo_proof, base64::engine::general_purpose::STANDARD.encode(proof)).into_bytes())
    }

    /// Verify the server-final signature (`v=`), completing authentication.
    pub fn verify_server_final(&mut self, payload: &[u8]) -> Result<()> {
        let text = std::str::from_utf8(payload).map_err(|_| Error::new(crate::error::ErrorKind::Auth, "SCRAM payload is not UTF-8"))?;
        let Some(v) = text.strip_prefix("v=") else {
            if text.starts_with("e=") {
                return Err(Error::new(crate::error::ErrorKind::Auth, format!("SCRAM error: {}", &text[2..])));
            }
            return Err(Error::new(crate::error::ErrorKind::Auth, "missing server signature"));
        };
        let got = base64::engine::general_purpose::STANDARD.decode(v).map_err(|_| Error::new(crate::error::ErrorKind::Auth, "server signature is not base64"))?;
        let want = self.server_signature.clone().unwrap_or_default();
        if got != want {
            return Err(Error::new(crate::error::ErrorKind::Auth, "SCRAM server signature mismatch"));
        }
        self.finished = true;
        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc5802_sha1_vector() {
        // RFC 5802 §5 example (SCRAM-SHA-1, user "user", password "pencil").
        let mut c = ScramClient::with_nonce(Mechanism::Sha1, "user", "pencil", Some("fyko+d2lbbFgONRv9qkxdawL".into()));
        assert_eq!(String::from_utf8(c.client_first()).unwrap(), "n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL");
        let server_first = b"r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
        let final_msg = c.step(server_first).unwrap();
        assert_eq!(
            String::from_utf8(final_msg).unwrap(),
            "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
        );
        c.verify_server_final(b"v=rmF9pqV8S7suAoZWja4dJRkFsKQ=").unwrap();
    }

    #[test]
    fn rejects_bad_signature() {
        let mut c = ScramClient::with_nonce(Mechanism::Sha256, "u", "p", Some("abcdefghijklmnopqrstuvwx".into()));
        let f = c.step(b"r=abcdefghijklmnopqrstuvwx0123456789,s=QSXCR+Q6sek8bf92,i=4096").unwrap();
        assert!(String::from_utf8(f).unwrap().contains(",p="));
        assert!(c.verify_server_final(b"v=AAAA").is_err());
    }

    #[test]
    fn escapes_usernames() {
        let c = ScramClient::new(Mechanism::Sha256, "a=b,c", "p");
        assert!(String::from_utf8(c.client_first()).unwrap().contains("n=a=3Db=2Cc,"));
    }
}