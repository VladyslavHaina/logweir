//! KBAK v1 segment decoder.
//! SOURCE: U/kafka-backup/crates/kafka-backup-core/src/segment/format.rs @ v0.21.0
//!
//! [VERIFIED format.rs:18-25] Header (32 bytes):
//!   magic: [u8;4] = "KBAK" · version: u8 · compression: u8 (0=none,1=zstd,2=lz4)
//!   reserved: [u8;2] · record_count: u64 LE · start_offset: i64 LE · end_offset: i64 LE
//! [VERIFIED format.rs:27-42] Record:
//!   total_len: u32 LE (length of the REMAINING record data) · timestamp: i64 LE
//!   offset: i64 LE · key_len: i32 LE (-1 null) · key · value_len: i32 LE (-1 null) · value
//!   header_count: u16 LE · headers[ key_len: u16 LE, key, value_len: i32 LE (-1 null), value ]
//! [VERIFIED format.rs:44-46] Footer (8 bytes): crc32: u32 LE over all preceding bytes,
//!   magic_end: [u8;4] = "BKAE"
//! [VERIFIED format.rs:53,56,60,63,66] MAGIC_BYTES=b"KBAK", MAGIC_END=b"BKAE",
//!   VERSION=1, HEADER_SIZE=32, FOOTER_SIZE=8
use logweir_core::engine::EngineError;

const MAGIC: &[u8; 4] = b"KBAK";
const MAGIC_END: &[u8; 4] = b"BKAE";
const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedRecord {
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

fn err(m: impl Into<String>) -> EngineError {
    EngineError::Unsupported(m.into())
}

pub fn decode_segment(bytes: &[u8]) -> Result<Vec<ArchivedRecord>, EngineError> {
    // The legacy JSON fallback. Pre-declared degradation, never a panic.
    // `first()` rather than `bytes.len() >= 1 && bytes[0] == ...`: the latter is
    // clippy::len_zero, and Task 1's justfile and ci.yml both run
    // `cargo clippy --workspace --all-targets -- -D warnings`.
    if bytes.first() == Some(&b'[') || bytes.starts_with(b"{") {
        return Err(err("legacy JSON segment: byte fingerprints unavailable"));
    }
    if bytes.len() < HEADER_SIZE + FOOTER_SIZE {
        return Err(err("segment shorter than header+footer"));
    }
    if &bytes[0..4] != MAGIC {
        return Err(err("bad magic: not a KBAK segment"));
    }
    let version = bytes[4];
    if version != 1 {
        return Err(err(format!("KBAK version {version} is not v1")));
    }
    let compression = bytes[5];
    let record_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());

    let tail = bytes.len() - FOOTER_SIZE;
    if &bytes[tail + 4..] != MAGIC_END {
        return Err(err("bad end magic: truncated segment"));
    }
    let want_crc = u32::from_le_bytes(bytes[tail..tail + 4].try_into().unwrap());
    let got_crc = crc32fast::hash(&bytes[..tail]);
    if want_crc != got_crc {
        return Err(err(format!(
            "CRC32 mismatch: {got_crc:#x} != {want_crc:#x}"
        )));
    }

    let body = &bytes[HEADER_SIZE..tail];
    let body = match compression {
        0 => body.to_vec(),
        1 => zstd::stream::decode_all(body).map_err(|e| err(format!("zstd: {e}")))?,
        2 => lz4_flex::block::decompress_size_prepended(body)
            .map_err(|e| err(format!("lz4: {e}")))?,
        c => return Err(err(format!("unknown compression byte {c}"))),
    };

    let mut out = Vec::with_capacity(record_count as usize);
    let mut c = std::io::Cursor::new(&body[..]);
    for _ in 0..record_count {
        out.push(read_record(&mut c)?);
    }
    Ok(out)
}

fn read_record(c: &mut std::io::Cursor<&[u8]>) -> Result<ArchivedRecord, EngineError> {
    use byteorder::{ReadBytesExt, LE};
    use std::io::Read;
    let _total_len = c
        .read_u32::<LE>()
        .map_err(|e| err(format!("total_len: {e}")))?;
    let timestamp = c
        .read_i64::<LE>()
        .map_err(|e| err(format!("timestamp: {e}")))?;
    let offset = c
        .read_i64::<LE>()
        .map_err(|e| err(format!("offset: {e}")))?;

    let key_len = c
        .read_i32::<LE>()
        .map_err(|e| err(format!("key_len: {e}")))?;
    let key = opt_bytes(c, key_len)?;
    let value_len = c
        .read_i32::<LE>()
        .map_err(|e| err(format!("value_len: {e}")))?;
    let value = opt_bytes(c, value_len)?;

    let header_count = c
        .read_u16::<LE>()
        .map_err(|e| err(format!("header_count: {e}")))?;
    let mut headers = Vec::with_capacity(header_count as usize);
    for _ in 0..header_count {
        let hk_len = c
            .read_u16::<LE>()
            .map_err(|e| err(format!("hkey_len: {e}")))? as usize;
        let mut hk = vec![0u8; hk_len];
        c.read_exact(&mut hk)
            .map_err(|e| err(format!("hkey: {e}")))?;
        let hv_len = c
            .read_i32::<LE>()
            .map_err(|e| err(format!("hval_len: {e}")))?;
        let hv = opt_bytes(c, hv_len)?;
        headers.push((String::from_utf8_lossy(&hk).into_owned(), hv));
    }
    Ok(ArchivedRecord {
        offset,
        timestamp,
        key,
        value,
        headers,
    })
}

/// A FREE function taking the cursor explicitly. As a closure capturing `c` by
/// mutable reference it is `E0502` — the very next statement uses `c` while the
/// closure is still live — and the file does not compile.
fn opt_bytes(c: &mut std::io::Cursor<&[u8]>, len: i32) -> Result<Option<Vec<u8>>, EngineError> {
    use std::io::Read;
    if len < 0 {
        return Ok(None);
    }
    let mut b = vec![0u8; len as usize];
    c.read_exact(&mut b)
        .map_err(|e| err(format!("payload: {e}")))?;
    Ok(Some(b))
}
