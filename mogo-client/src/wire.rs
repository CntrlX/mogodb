//! Wire framing: OP_MSG, legacy OP_QUERY/OP_REPLY, message reading.

use crate::error::{Error, Result};
use bson::Document;

pub const OP_REPLY: i32 = 1;
pub const OP_QUERY: i32 = 2004;
pub const OP_MSG: i32 = 2013;

pub const MAX_MESSAGE_SIZE: usize = 48_000_000;

const FLAG_CHECKSUM: u32 = 1;
const FLAG_MORE_TO_COME: u32 = 1 << 1;

/// A complete wire message: opcode plus its payload document.
#[derive(Debug, Clone)]
pub enum Message {
    Reply(Document),
    Msg { flags: u32, doc: Document, sequences: Vec<(String, Vec<Document>)> },
}

/// Kind-1 payload: int32 identifier length (including the NUL), identifier, NUL, documents.
fn read_identifier(buf: &[u8]) -> Result<(&str, &[u8])> {
    if buf.len() < 5 {
        return Err(Error::protocol("short OP_MSG sequence header"));
    }
    let n = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if n == 0 || n > buf.len() - 4 {
        return Err(Error::protocol(format!("bad OP_MSG identifier length {n}")));
    }
    if buf[4 + n] != 0 {
        return Err(Error::protocol("OP_MSG identifier is not NUL-terminated"));
    }
    Ok((std::str::from_utf8(&buf[4..4 + n]).map_err(|_| Error::protocol("OP_MSG identifier is not UTF-8"))?, &buf[4 + n + 1..]))
}

/// Read one leading document; returns it and the unconsumed remainder.
fn decode_one<'a>(buf: &'a [u8], what: &str) -> Result<(Document, &'a [u8])> {
    if buf.len() < 5 {
        return Err(Error::protocol(format!("{what}: truncated document")));
    }
    let len = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len < 5 || len > buf.len() {
        return Err(Error::protocol(format!("{what}: bad document length {len}")));
    }
    let doc = bson::from_slice::<Document>(&buf[..len]).map_err(|e| Error::protocol(format!("{what} bson: {e}")))?;
    Ok((doc, &buf[len..]))
}

pub fn encode_op_msg(request_id: i32, doc: &Document, sequences: &[(String, Vec<Document>)]) -> Vec<u8> {
    let mut sections = Vec::new();
    sections.push(0); // section kind 0
    sections.extend_from_slice(&bson::to_vec(doc).expect("document encodes"));
    for (identifier, docs) in sequences {
        sections.push(1); // section kind 1
        let mut payload = Vec::new();
        payload.extend_from_slice(&(identifier.len() as i32).to_le_bytes());
        payload.extend_from_slice(identifier.as_bytes());
        payload.push(0);
        for d in docs {
            payload.extend_from_slice(&bson::to_vec(d).expect("document encodes"));
        }
        sections.extend_from_slice(&((payload.len() + 4) as i32).to_le_bytes());
        sections.extend_from_slice(&payload);
    }
    let mut body = Vec::with_capacity(4 + sections.len());
    body.extend_from_slice(&0u32.to_le_bytes()); // flagBits
    body.extend_from_slice(&sections);
    frame(request_id, 0, OP_MSG, body)
}

pub fn encode_op_query(request_id: i32, full_collection: &str, query: &Document) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0i32.to_le_bytes()); // flags
    body.extend_from_slice(full_collection.as_bytes());
    body.push(0);
    body.extend_from_slice(&0i32.to_le_bytes()); // numberToSkip
    body.extend_from_slice(&(-1i32).to_le_bytes()); // numberToReturn
    body.extend_from_slice(&bson::to_vec(query).expect("document encodes"));
    frame(request_id, 0, OP_QUERY, body)
}

fn frame(request_id: i32, response_to: i32, op_code: i32, body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + body.len());
    out.extend_from_slice(&((16 + body.len()) as i32).to_le_bytes());
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&response_to.to_le_bytes());
    out.extend_from_slice(&op_code.to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Parse a full message from `buf` (header + body).
pub fn parse(buf: &[u8]) -> Result<Message> {
    if buf.len() < 16 {
        return Err(Error::protocol("short message header"));
    }
    let len = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let op = i32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
    if len > buf.len() || len < 16 {
        return Err(Error::protocol(format!("bad message length {len}")));
    }
    let mut body = &buf[16..len];
    match op {
        OP_REPLY => {
            // flags(4) cursorId(8) startingFrom(4) numberReturned(4) documents...
            if body.len() < 20 {
                return Err(Error::protocol("short OP_REPLY"));
            }
            let n = i32::from_le_bytes([body[16], body[17], body[18], body[19]]) as usize;
            body = &body[20..];
            let mut docs = Vec::with_capacity(n);
            for _ in 0..n {
                let (doc, rest) = decode_one(body, "OP_REPLY")?;
                docs.push(doc);
                body = rest;
            }
            Ok(Message::Reply(docs.into_iter().next().unwrap_or_default()))
        }
        OP_MSG => {
            let flags = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
            let checksum = flags & FLAG_CHECKSUM != 0;
            let mut payload = &body[4..];
            if checksum {
                if payload.len() < 4 {
                    return Err(Error::protocol("OP_MSG checksum missing"));
                }
                payload = &payload[..payload.len() - 4];
            }
            let mut doc = None;
            let mut sequences = Vec::new();
            while !payload.is_empty() {
                let kind = payload[0];
                payload = &payload[1..];
                match kind {
                    0 => {
                        let (d, rest) = decode_one(payload, "OP_MSG")?;
                        doc = Some(d);
                        payload = rest;
                    }
                    1 => {
                        if payload.len() < 4 {
                            return Err(Error::protocol("short OP_MSG section"));
                        }
                        let size = i32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
                        // `size` includes the 4-byte size prefix; the payload follows it.
                        if size < 4 || size > payload.len() {
                            return Err(Error::protocol(format!("bad OP_MSG section size {size} (payload {})", payload.len())));
                        }
                        let sec = &payload[4..size];
                        payload = &payload[size..];
                        let (identifier, body) = read_identifier(sec)?;
                        let identifier = identifier.to_string();
                        let mut docs = Vec::new();
                        let mut rest = body;
                        while !rest.is_empty() {
                            let (d, r) = decode_one(rest, "OP_MSG sequence")?;
                            docs.push(d);
                            rest = r;
                        }
                        sequences.push((identifier, docs));
                    }
                    other => return Err(Error::protocol(format!("unknown OP_MSG section kind {other}"))),
                }
            }
            Ok(Message::Msg { flags: flags & !FLAG_CHECKSUM, doc: doc.unwrap_or_default(), sequences })
        }
        other => Err(Error::protocol(format!("unexpected opcode {other}"))),
    }
}

/// `moreToCome` is set on exhaust-allow replies we never issue.
pub fn wants_more(flags: u32) -> bool {
    flags & FLAG_MORE_TO_COME != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_op_msg() {
        let doc = bson::doc! {"find": "c", "filter": {"a": 1}, "$db": "test"};
        let buf = encode_op_msg(7, &doc, &[]);
        assert_eq!(i32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]), OP_MSG);
        match parse(&buf).unwrap() {
            Message::Msg { doc: d, .. } => assert_eq!(d, doc),
            _ => panic!("expected msg"),
        }
    }

    #[test]
    fn roundtrip_sequence() {
        let doc = bson::doc! {"insert": "c", "$db": "test"};
        let docs = vec![bson::doc! {"_id": 1}, bson::doc! {"_id": 2}];
        let buf = encode_op_msg(9, &doc, &[("documents".to_string(), docs.clone())]);
        match parse(&buf) {
            Ok(Message::Msg { doc: d, sequences, .. }) => {
                assert_eq!(d.get_str("insert").unwrap(), "c");
                assert_eq!(sequences.len(), 1);
                assert_eq!(sequences[0].0, "documents");
                assert_eq!(sequences[0].1.len(), 2);
                assert_eq!(sequences[0].1[0].get_i32("_id").unwrap(), 1);
                assert_eq!(sequences[0].1[1].get_i32("_id").unwrap(), 2);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_op_reply() {
        let mut body = Vec::new();
        body.extend_from_slice(&0i32.to_le_bytes());
        body.extend_from_slice(&0i64.to_le_bytes());
        body.extend_from_slice(&0i32.to_le_bytes());
        body.extend_from_slice(&1i32.to_le_bytes());
        body.extend_from_slice(&bson::to_vec(&bson::doc! {"ok": 1.0}).unwrap());
        let buf = frame(1, 0, OP_REPLY, body);
        match parse(&buf).unwrap() {
            Message::Reply(d) => assert_eq!(d.get_f64("ok").unwrap(), 1.0),
            _ => panic!("expected reply"),
        }
    }
}