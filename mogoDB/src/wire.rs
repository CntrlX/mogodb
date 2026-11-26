//! MongoDB wire protocol: OP_MSG (2013), OP_COMPRESSED (2012), legacy OP_QUERY (2004) / OP_REPLY (1).

use crate::error::{Error, Result};
use bson::{Document, RawDocument};
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt};

pub const OP_REPLY: i32 = 1;
pub const OP_QUERY: i32 = 2004;
pub const OP_COMPRESSED: i32 = 2012;
pub const OP_MSG: i32 = 2013;

pub const FLAG_CHECKSUM: u32 = 1;
pub const FLAG_MORE_TO_COME: u32 = 1 << 1;
pub const FLAG_EXHAUST_ALLOWED: u32 = 1 << 16;

pub const MAX_MESSAGE_SIZE: usize = 48_000_000;

/// Compressor ids as negotiated in `hello.compression`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compressor {
    Snappy = 1,
    Zlib = 2,
    #[cfg(feature = "zstd")]
    Zstd = 3,
}

impl Compressor {
    pub fn from_name(n: &str) -> Option<Self> {
        match n {
            "snappy" => Some(Self::Snappy),
            "zlib" => Some(Self::Zlib),
            #[cfg(feature = "zstd")]
            "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Snappy => "snappy",
            Self::Zlib => "zlib",
            #[cfg(feature = "zstd")]
            Self::Zstd => "zstd",
        }
    }
    fn from_id(id: u8) -> Option<Self> {
        match id {
            1 => Some(Self::Snappy),
            2 => Some(Self::Zlib),
            #[cfg(feature = "zstd")]
            3 => Some(Self::Zstd),
            _ => None,
        }
    }
}

pub const SUPPORTED_COMPRESSORS: &[&str] = &[
    #[cfg(feature = "zstd")]
    "zstd",
    "snappy",
    "zlib",
];

#[derive(Debug)]
pub struct MsgOp {
    pub flags: u32,
    pub body: Document,
    /// Kind-1 document sequences: (identifier, documents).
    pub sequences: Vec<(String, Vec<Document>)>,
}

#[derive(Debug)]
pub struct QueryOp {
    pub flags: u32,
    pub collection: String,
    pub skip: i32,
    pub n_return: i32,
    pub query: Document,
}

#[derive(Debug)]
pub enum Op {
    Msg(MsgOp),
    Query(QueryOp),
}

#[derive(Debug)]
pub struct Request {
    pub request_id: i32,
    pub op: Op,
    /// Set when the request arrived in OP_COMPRESSED; replies reuse the compressor.
    pub compressor: Option<Compressor>,
}

/// Read one complete message (header included). `Ok(None)` on clean EOF.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = i32::from_le_bytes(len_buf);
    if len < 16 || len as usize > MAX_MESSAGE_SIZE {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("invalid message length {len}")));
    }
    let mut buf = vec![0u8; len as usize];
    buf[..4].copy_from_slice(&len_buf);
    r.read_exact(&mut buf[4..]).await?;
    Ok(Some(buf))
}

fn rd_i32(b: &[u8], at: usize) -> Result<i32> {
    b.get(at..at + 4)
        .map(|s| i32::from_le_bytes([s[0], s[1], s[2], s[3]]))
        .ok_or_else(|| Error::new(22, "truncated message"))
}

fn rd_cstr(b: &[u8], at: usize) -> Result<(&str, usize)> {
    let rest = b.get(at..).ok_or_else(|| Error::new(22, "truncated message"))?;
    let end = rest.iter().position(|&c| c == 0).ok_or_else(|| Error::new(22, "unterminated cstring"))?;
    let s = std::str::from_utf8(&rest[..end]).map_err(|_| Error::new(22, "invalid utf-8 cstring"))?;
    Ok((s, at + end + 1))
}

/// Read a BSON document at `at`, validating its length prefix in O(1) before parsing.
fn rd_doc(b: &[u8], at: usize) -> Result<(Document, usize)> {
    let len = rd_i32(b, at)?;
    if len < 5 {
        return Err(Error::new(22, "invalid BSON length"));
    }
    let end = at + len as usize;
    let slice = b.get(at..end).ok_or_else(|| Error::new(22, "BSON document exceeds message"))?;
    let raw = RawDocument::from_bytes(slice).map_err(|e| Error::new(22, format!("invalid BSON: {e}")))?;
    let doc = Document::try_from(raw).map_err(|e| Error::new(22, format!("invalid BSON: {e}")))?;
    Ok((doc, end))
}

pub fn parse(frame: &[u8]) -> Result<Request> {
    let request_id = rd_i32(frame, 4)?;
    let op_code = rd_i32(frame, 12)?;
    match op_code {
        OP_MSG => Ok(Request { request_id, op: Op::Msg(parse_msg(&frame[16..])?), compressor: None }),
        OP_QUERY => Ok(Request { request_id, op: Op::Query(parse_query(&frame[16..])?), compressor: None }),
        OP_COMPRESSED => {
            let body = &frame[16..];
            let original = rd_i32(body, 0)?;
            let size = rd_i32(body, 4)?;
            if size < 0 || size as usize > MAX_MESSAGE_SIZE {
                return Err(Error::new(22, "invalid uncompressed size"));
            }
            let id = *body.get(8).ok_or_else(|| Error::new(22, "truncated OP_COMPRESSED"))?;
            let data = &body[9..];
            let (inner, comp) = if id == 0 {
                (data.to_vec(), None)
            } else {
                let c = Compressor::from_id(id).ok_or_else(|| Error::new(22, format!("unsupported compressor {id}")))?;
                (decompress(c, data, size as usize)?, Some(c))
            };
            let op = match original {
                OP_MSG => Op::Msg(parse_msg(&inner)?),
                OP_QUERY => Op::Query(parse_query(&inner)?),
                o => return Err(Error::new(22, format!("unsupported compressed opcode {o}"))),
            };
            Ok(Request { request_id, op, compressor: comp })
        }
        o => Err(Error::new(22, format!("unsupported opcode {o}"))),
    }
}

fn parse_msg(b: &[u8]) -> Result<MsgOp> {
    let flags = rd_i32(b, 0)? as u32;
    // Unknown required bits (0-15 except 0,1) must be rejected.
    if flags & 0xFFFC != 0 {
        return Err(Error::new(22, format!("unknown required OP_MSG flag bits {flags:#x}")));
    }
    let end = if flags & FLAG_CHECKSUM != 0 {
        let n = b.len().checked_sub(4).ok_or_else(|| Error::new(22, "truncated checksum"))?;
        n
    } else {
        b.len()
    };
    let mut at = 4;
    let mut body = None;
    let mut sequences = Vec::new();
    while at < end {
        let kind = b[at];
        at += 1;
        match kind {
            0 => {
                let (d, next) = rd_doc(b, at)?;
                if body.replace(d).is_some() {
                    return Err(Error::new(22, "multiple OP_MSG body sections"));
                }
                at = next;
            }
            1 => {
                let size = rd_i32(b, at)? as usize;
                let sec_end = at + size;
                if sec_end > end {
                    return Err(Error::new(22, "document sequence exceeds message"));
                }
                let (id, mut p) = rd_cstr(b, at + 4)?;
                let mut docs = Vec::new();
                while p < sec_end {
                    let (d, next) = rd_doc(b, p)?;
                    docs.push(d);
                    p = next;
                }
                sequences.push((id.to_string(), docs));
                at = sec_end;
            }
            k => return Err(Error::new(22, format!("unknown OP_MSG section kind {k}"))),
        }
    }
    let body = body.ok_or_else(|| Error::new(22, "OP_MSG without body section"))?;
    Ok(MsgOp { flags, body, sequences })
}

fn parse_query(b: &[u8]) -> Result<QueryOp> {
    let flags = rd_i32(b, 0)? as u32;
    let (coll, at) = rd_cstr(b, 4)?;
    let skip = rd_i32(b, at)?;
    let n_return = rd_i32(b, at + 4)?;
    let (query, _) = rd_doc(b, at + 8)?;
    Ok(QueryOp { flags, collection: coll.to_string(), skip, n_return, query })
}

fn header(buf: &mut Vec<u8>, request_id: i32, response_to: i32, op: i32) {
    buf.extend_from_slice(&[0; 4]);
    buf.extend_from_slice(&request_id.to_le_bytes());
    buf.extend_from_slice(&response_to.to_le_bytes());
    buf.extend_from_slice(&op.to_le_bytes());
}

fn finish_len(buf: &mut [u8]) {
    let n = buf.len() as i32;
    buf[..4].copy_from_slice(&n.to_le_bytes());
}

/// Encode an OP_MSG reply (optionally wrapped in OP_COMPRESSED).
pub fn encode_msg(request_id: i32, response_to: i32, flags: u32, doc: &Document, comp: Option<Compressor>) -> Vec<u8> {
    let mut payload = Vec::with_capacity(128);
    payload.extend_from_slice(&flags.to_le_bytes());
    payload.push(0);
    doc.to_writer(&mut payload).expect("serialize reply");
    wrap(request_id, response_to, OP_MSG, payload, comp)
}

/// Encode a legacy OP_REPLY (response to OP_QUERY handshakes).
pub fn encode_reply(request_id: i32, response_to: i32, doc: &Document, comp: Option<Compressor>) -> Vec<u8> {
    let mut payload = Vec::with_capacity(128);
    payload.extend_from_slice(&8i32.to_le_bytes()); // responseFlags: AwaitCapable
    payload.extend_from_slice(&0i64.to_le_bytes()); // cursorID
    payload.extend_from_slice(&0i32.to_le_bytes()); // startingFrom
    payload.extend_from_slice(&1i32.to_le_bytes()); // numberReturned
    doc.to_writer(&mut payload).expect("serialize reply");
    wrap(request_id, response_to, OP_REPLY, payload, comp)
}

fn wrap(request_id: i32, response_to: i32, op: i32, payload: Vec<u8>, comp: Option<Compressor>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(payload.len() + 32);
    match comp {
        None => {
            header(&mut buf, request_id, response_to, op);
            buf.extend_from_slice(&payload);
        }
        Some(c) => {
            header(&mut buf, request_id, response_to, OP_COMPRESSED);
            buf.extend_from_slice(&op.to_le_bytes());
            buf.extend_from_slice(&(payload.len() as i32).to_le_bytes());
            buf.push(c as u8);
            buf.extend_from_slice(&compress(c, &payload));
        }
    }
    finish_len(&mut buf);
    buf
}

fn compress(c: Compressor, data: &[u8]) -> Vec<u8> {
    match c {
        Compressor::Snappy => snap::raw::Encoder::new().compress_vec(data).expect("snappy compress"),
        Compressor::Zlib => {
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
            e.write_all(data).expect("zlib compress");
            e.finish().expect("zlib finish")
        }
        #[cfg(feature = "zstd")]
        Compressor::Zstd => zstd::bulk::compress(data, 1).expect("zstd compress"),
    }
}

fn decompress(c: Compressor, data: &[u8], size: usize) -> Result<Vec<u8>> {
    let bad = |e: String| Error::new(22, format!("decompression failed: {e}"));
    let out = match c {
        Compressor::Snappy => snap::raw::Decoder::new().decompress_vec(data).map_err(|e| bad(e.to_string()))?,
        Compressor::Zlib => {
            let mut out = Vec::with_capacity(size);
            flate2::read::ZlibDecoder::new(data).take(size as u64 + 1).read_to_end(&mut out).map_err(|e| bad(e.to_string()))?;
            out
        }
        #[cfg(feature = "zstd")]
        Compressor::Zstd => zstd::bulk::decompress(data, size).map_err(|e| bad(e.to_string()))?,
    };
    if out.len() != size {
        return Err(bad(format!("size mismatch {} != {size}", out.len())));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn msg_frame(body: &Document, seq: Option<(&str, Vec<Document>)>) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&0u32.to_le_bytes());
        p.push(0);
        body.to_writer(&mut p).unwrap();
        if let Some((id, docs)) = seq {
            let mut s = Vec::new();
            s.extend_from_slice(id.as_bytes());
            s.push(0);
            for d in docs {
                d.to_writer(&mut s).unwrap();
            }
            p.push(1);
            p.extend_from_slice(&((s.len() + 4) as i32).to_le_bytes());
            p.extend_from_slice(&s);
        }
        wrap(7, 0, OP_MSG, p, None)
    }

    #[test]
    fn op_msg_with_sequence() {
        let f = msg_frame(&doc! {"insert": "c", "$db": "t"}, Some(("documents", vec![doc! {"a": 1}, doc! {"a": 2}])));
        let r = parse(&f).unwrap();
        assert_eq!(r.request_id, 7);
        let Op::Msg(m) = r.op else { panic!() };
        assert_eq!(m.body.get_str("insert").unwrap(), "c");
        assert_eq!(m.sequences[0].0, "documents");
        assert_eq!(m.sequences[0].1.len(), 2);
    }

    #[test]
    fn compressed_roundtrip() {
        for c in [Compressor::Snappy, Compressor::Zlib] {
            let enc = encode_msg(1, 2, 0, &doc! {"ok": 1.0, "x": "y".repeat(100)}, Some(c));
            let r = parse(&enc).unwrap();
            assert_eq!(r.compressor, Some(c));
            let Op::Msg(m) = r.op else { panic!() };
            assert_eq!(m.body.get_f64("ok").unwrap(), 1.0);
        }
    }
}
