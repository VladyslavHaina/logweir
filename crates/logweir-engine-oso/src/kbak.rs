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
//!
//! ## Threat model
//!
//! The footer CRC32 is an integrity check against accidental corruption, not
//! an authentication tag — CRC32 is unkeyed, so anyone able to write an
//! object into the archive bucket can produce a CRC-valid header carrying any
//! `record_count`, `key_len`, or compressed-size prefix they like. Every
//! length read from this format is therefore treated as hostile: bounds are
//! checked against bytes ACTUALLY available before any allocation is sized on
//! them, matching upstream's own `format.rs:238-243, 253-258, 282-287,
//! 299-305`, which check `data.len() < len` before every `split_to`.
use logweir_core::engine::EngineError;

const MAGIC: &[u8; 4] = b"KBAK";
const MAGIC_END: &[u8; 4] = b"BKAE";
const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 8;

/// Zstandard frame magic (`0xFD2FB528`, stored little-endian), unambiguous
/// regardless of context. A whole-object blob starting with this is never a
/// KBAK segment (which always starts with the literal "KBAK" envelope, even
/// when its *body* is zstd-compressed) — it can only be a legacy JSON segment
/// that upstream stored zstd-compressed.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// The smallest a record can legally be on the wire: the 4-byte `total_len`
/// prefix, plus a minimal frame (timestamp 8 bytes, offset 8 bytes, key_len 4
/// bytes, value_len 4 bytes, header_count 2 bytes) declaring a null key, a
/// null value and zero headers. Bounds `record_count` against the
/// decompressed body size before it is ever used to size an allocation.
const MIN_RECORD_SIZE: u64 = 4 + 8 + 8 + 4 + 4 + 2;

/// `lz4_flex::block::decompress_size_prepended` reads its 4-byte declared
/// output size and allocates a buffer of exactly that size BEFORE attempting
/// to decompress into it — an attacker-controlled `u32` there is a
/// multi-gigabyte allocation paid for by four bytes of input. 1 GiB is a
/// generous bound for a single segment; revisit if a legitimate segment ever
/// needs more.
const MAX_LZ4_DECOMPRESSED_BYTES: usize = 1 << 30;

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
    // A whole-object zstd-compressed legacy JSON segment. Upstream picks the
    // decompression codec from the object KEY's extension, not from these
    // bytes [VERIFIED restore/helpers.rs:22-38] — decode_segment receives no
    // key, so it cannot decompress this. But the zstd frame magic is
    // unambiguous, so this is at least reported as its own outcome instead of
    // being misdiagnosed as a corrupt KBAK segment.
    if bytes.starts_with(&ZSTD_MAGIC) {
        return Err(err(
            "possible zstd-compressed legacy JSON segment (zstd frame magic 28 b5 2f fd): \
             decode_segment cannot decompress this without the object key's compression extension",
        ));
    }
    if bytes.len() < HEADER_SIZE + FOOTER_SIZE {
        return Err(err("segment shorter than header+footer"));
    }
    if &bytes[0..4] != MAGIC {
        // An lz4-compressed legacy JSON segment lands here too: lz4_flex's
        // block format is a bare length prefix with no magic of its own, so
        // it is genuinely indistinguishable from corrupt bytes without the
        // object key's `.lz4` extension [VERIFIED restore/helpers.rs:22-38],
        // which this function does not receive. Say so rather than asserting
        // corruption with unearned confidence.
        return Err(err(
            "bad magic: not a KBAK segment (if this is an lz4-compressed legacy JSON segment, \
             its codec cannot be told from bytes alone — upstream selects it from the object \
             key's .lz4 extension, which decode_segment does not receive)",
        ));
    }
    let version = bytes[4];
    if version != 1 {
        return Err(err(format!("KBAK version {version} is not v1")));
    }
    let compression = bytes[5];
    let raw_record_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());

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
        2 => checked_lz4_decompress(body)?,
        c => return Err(err(format!("unknown compression byte {c}"))),
    };

    // `record_count` comes from a CRC-covered header, but CRC32 is unkeyed
    // (see module doc): anyone who can write into the archive bucket can
    // produce a CRC-valid header naming any `record_count` — `u64::MAX`
    // panics `Vec::with_capacity` with "capacity overflow" today, and a large
    // but plausible value (~10^8) aborts the process on a ~9 GiB allocation.
    // Bound it against what the decompressed body could actually hold before
    // it ever sizes an allocation.
    let max_records = body.len() as u64 / MIN_RECORD_SIZE;
    if raw_record_count > max_records {
        return Err(err(format!(
            "record_count {raw_record_count} exceeds what {} decompressed bytes could contain (max {max_records})",
            body.len()
        )));
    }
    let record_count = raw_record_count as usize;

    let mut out = Vec::with_capacity(record_count);
    let mut pos: usize = 0;
    for _ in 0..record_count {
        if body.len() - pos < 4 {
            return Err(err(
                "total_len: not enough bytes remaining for the length prefix",
            ));
        }
        let total_len = u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if body.len() - pos < total_len {
            return Err(err(format!(
                "total_len {total_len} exceeds the {} bytes remaining in the segment",
                body.len() - pos
            )));
        }
        let frame = &body[pos..pos + total_len];
        out.push(read_record(frame, total_len)?);
        pos += total_len;
    }
    Ok(out)
}

fn checked_lz4_decompress(body: &[u8]) -> Result<Vec<u8>, EngineError> {
    if body.len() < 4 {
        return Err(err("lz4: missing the 4-byte prepended size"));
    }
    let declared = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    if declared > MAX_LZ4_DECOMPRESSED_BYTES {
        return Err(err(format!(
            "lz4: declared decompressed size {declared} exceeds the {MAX_LZ4_DECOMPRESSED_BYTES}-byte cap"
        )));
    }
    lz4_flex::block::decompress_size_prepended(body).map_err(|e| err(format!("lz4: {e}")))
}

/// Parses exactly one record from `frame`, which is `total_len` bytes taken
/// verbatim from the body stream (the caller has already bounds-checked that
/// `total_len` bytes were available — `frame.len() == total_len` always).
/// `total_len` is upstream's own frame boundary [VERIFIED format.rs:27-42];
/// treating it as authoritative (parse strictly within it, then verify every
/// byte was consumed) means a single corrupted record fails by itself instead
/// of desynchronizing every record after it — upstream's reader does the same
/// (`reader.rs:110-127`: slice exactly `total_len` bytes, parse inside that
/// bound, advance by it).
fn read_record(frame: &[u8], total_len: usize) -> Result<ArchivedRecord, EngineError> {
    use byteorder::{ReadBytesExt, LE};
    let mut c = std::io::Cursor::new(frame);
    let timestamp = c
        .read_i64::<LE>()
        .map_err(|e| err(format!("timestamp: {e}")))?;
    let offset = c
        .read_i64::<LE>()
        .map_err(|e| err(format!("offset: {e}")))?;

    let key_len = c
        .read_i32::<LE>()
        .map_err(|e| err(format!("key_len: {e}")))?;
    let key = opt_bytes(&mut c, key_len, "key")?;
    let value_len = c
        .read_i32::<LE>()
        .map_err(|e| err(format!("value_len: {e}")))?;
    let value = opt_bytes(&mut c, value_len, "value")?;

    let header_count = c
        .read_u16::<LE>()
        .map_err(|e| err(format!("header_count: {e}")))?;
    let mut headers = Vec::with_capacity(header_count as usize);
    for _ in 0..header_count {
        let hk_len = c
            .read_u16::<LE>()
            .map_err(|e| err(format!("hkey_len: {e}")))?;
        let hk = take(&mut c, hk_len as usize, "hkey")?;
        let hv_len = c
            .read_i32::<LE>()
            .map_err(|e| err(format!("hval_len: {e}")))?;
        let hv = opt_bytes(&mut c, hv_len, "hval")?;
        headers.push((String::from_utf8_lossy(&hk).into_owned(), hv));
    }

    let consumed = c.position() as usize;
    if consumed != total_len {
        return Err(err(format!(
            "total_len mismatch: record declared {total_len} bytes but its fields used {consumed}"
        )));
    }

    Ok(ArchivedRecord {
        offset,
        timestamp,
        key,
        value,
        headers,
    })
}

/// Reads exactly `len` bytes, bounds-checked against what actually remains in
/// `c` BEFORE allocating — a hostile length can therefore never force a large
/// allocation; it fails immediately with a clean, distinguishable error
/// instead. Used for definite (non-nullable) byte fields; see `opt_bytes` for
/// the nullable ones.
fn take(c: &mut std::io::Cursor<&[u8]>, len: usize, what: &str) -> Result<Vec<u8>, EngineError> {
    let remaining = c.get_ref().len() as u64 - c.position();
    if len as u64 > remaining {
        return Err(err(format!(
            "{what}: length {len} exceeds the {remaining} bytes remaining in the record"
        )));
    }
    let mut b = vec![0u8; len];
    use std::io::Read;
    c.read_exact(&mut b)
        .map_err(|e| err(format!("{what}: {e}")))?;
    Ok(b)
}

/// A nullable length-prefixed field: `-1` is `None`; otherwise `take`s the
/// (already bounds-checked, by `take`) byte count. Kept as a free function
/// rather than a closure capturing `c` by mutable reference: as a closure it
/// is `E0502` — the very next statement uses `c` while the closure is still
/// live — and the file does not compile.
fn opt_bytes(
    c: &mut std::io::Cursor<&[u8]>,
    len: i32,
    what: &str,
) -> Result<Option<Vec<u8>>, EngineError> {
    if len < 0 {
        return Ok(None);
    }
    Ok(Some(take(c, len as usize, what)?))
}
