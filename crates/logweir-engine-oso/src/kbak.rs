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

// ---------------------------------------------------------------------------
// PROD-03.0: a STREAMING scan that keeps only each record's prefix bytes.
// ---------------------------------------------------------------------------

/// What [`scan_segment`] keeps of one record: its offset and at most the
/// first `prefix` bytes of its key and of its value (`None` is null). Never a
/// whole payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordPrefix {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

/// Why [`scan_segment`] stopped without reading the segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanError {
    /// The body decompresses past the caller's cap, or a zstd frame declares
    /// a window larger than [`ZSTD_WINDOW_LOG_MAX`] allows (a large segment,
    /// or a decompression bomb). Nothing past the cap was held.
    TooLarge(String),
    /// The caller's deadline passed while the scan was reading.
    TimedOut(String),
    /// Not a KBAK v1 segment this scan can read: bad magic or version, a CRC
    /// mismatch, an unknown codec, a corrupt body, or a record that does not
    /// fit its frame.
    Unreadable(String),
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooLarge(m) => write!(f, "too large: {m}"),
            Self::TimedOut(m) => write!(f, "timed out: {m}"),
            Self::Unreadable(m) => write!(f, "unreadable: {m}"),
        }
    }
}

/// The bounds one [`scan_segment`] runs within.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanLimits {
    /// The most bytes the body may decompress to; past it, `TooLarge`.
    pub max_decompressed: u64,
    /// When the scan must stop reading, checked on every read of the body;
    /// past it, `TimedOut`. `None` is no deadline.
    pub deadline: Option<std::time::Instant>,
}

/// **The zstd window cap: 2^23 bytes (8 MiB).** A zstd frame names the window
/// its decoder must hold — up to 2^31 bytes — and the decoder allocates and
/// touches it whatever the output cap says. Above this the scan refuses the
/// frame as `TooLarge` (the frame's header is read first; the decoder is also
/// told, for any later frame). The engine Logweir runs writes zstd at its
/// default level 3 (window 2^21) and never pledges a size; levels 20-22 and
/// single-segment frames over 8 MiB are not judged.
pub const ZSTD_WINDOW_LOG_MAX: u32 = 23;

/// How far back an LZ4 match may reach: the format's 16-bit offset.
const LZ4_WINDOW: usize = 1 << 16;

/// The window the FIRST zstd frame of `body` declares, when its header can be
/// read: the window descriptor's size, or a single-segment frame's content
/// size (RFC 8878 section 3.1.1.1). `None` defers to the decoder.
fn zstd_first_window(body: &[u8]) -> Option<u64> {
    if body.len() < 6 || body[0..4] != ZSTD_MAGIC {
        return None;
    }
    let descriptor = body[4];
    if descriptor & 0x20 == 0 {
        let w = body[5];
        let base = 1u64 << (10 + u64::from(w >> 3));
        return Some(base + (base / 8) * u64::from(w & 7));
    }
    let dictionary = [0usize, 1, 2, 4][usize::from(descriptor & 3)];
    let size = [1usize, 2, 4, 8][usize::from(descriptor >> 6)];
    let field = body.get(5 + dictionary..5 + dictionary + size)?;
    let mut v = 0u64;
    for (i, b) in field.iter().enumerate() {
        v |= u64::from(*b) << (8 * i);
    }
    Some(if size == 2 { v + 256 } else { v })
}

/// An LZ4 block (lz4_flex's size-prepended format: four bytes of declared
/// size, then the block), decoded as a STREAM: it keeps only the last 64 KiB
/// of output a match can reach (at most about 192 KiB at once), never the
/// whole decompressed body. Every length and offset is bounds-checked; output
/// past the declared size, a zero offset or one before the start, and a block
/// that ends short of its declared size are `InvalidData`.
struct Lz4Stream<'a> {
    src: &'a [u8],
    pos: usize,
    hist: Vec<u8>,
    handed: usize,
    state: Lz4State,
    produced: u64,
    declared: u64,
}

#[derive(Clone, Copy)]
enum Lz4State {
    Token,
    Literals {
        left: usize,
        match_base: usize,
    },
    Match {
        offset: usize,
        left: usize,
        done: usize,
    },
    Done,
}

fn lz4_bad(m: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, format!("lz4: {m}"))
}

impl<'a> Lz4Stream<'a> {
    /// `body` is the size-prepended block.
    fn new(body: &'a [u8]) -> std::io::Result<Self> {
        if body.len() < 4 {
            return Err(lz4_bad("missing the 4-byte prepended size"));
        }
        Ok(Self {
            src: &body[4..],
            pos: 0,
            hist: Vec::with_capacity(2 * LZ4_WINDOW),
            handed: 0,
            state: Lz4State::Token,
            produced: 0,
            declared: u64::from(u32::from_le_bytes(body[0..4].try_into().unwrap())),
        })
    }

    fn byte(&mut self) -> std::io::Result<u8> {
        let b = *self
            .src
            .get(self.pos)
            .ok_or_else(|| lz4_bad("the block ends inside a sequence"))?;
        self.pos += 1;
        Ok(b)
    }

    /// A length's extension bytes: added while each is 255.
    fn extension(&mut self) -> std::io::Result<usize> {
        let mut total = 0usize;
        loop {
            let b = self.byte()?;
            total = total.saturating_add(usize::from(b));
            if b != 255 {
                return Ok(total);
            }
        }
    }

    fn produce(&mut self, n: usize) -> std::io::Result<()> {
        self.produced += n as u64;
        if self.produced > self.declared {
            return Err(lz4_bad("the block decodes past its declared size"));
        }
        Ok(())
    }
}

impl std::io::Read for Lz4Stream<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        const CHUNK: usize = 1 << 16;
        loop {
            if self.handed < self.hist.len() {
                let n = buf.len().min(self.hist.len() - self.handed);
                buf[..n].copy_from_slice(&self.hist[self.handed..self.handed + n]);
                self.handed += n;
                return Ok(n);
            }
            if buf.is_empty() {
                return Ok(0);
            }
            // Everything produced has been handed over: keep only the window.
            if self.hist.len() > 2 * LZ4_WINDOW {
                let cut = self.hist.len() - LZ4_WINDOW;
                self.hist.drain(..cut);
                self.handed -= cut;
            }
            match self.state {
                Lz4State::Done => return Ok(0),
                Lz4State::Token => {
                    if self.pos == self.src.len() {
                        if self.produced != self.declared {
                            return Err(lz4_bad("the block ends short of its declared size"));
                        }
                        self.state = Lz4State::Done;
                        continue;
                    }
                    let token = self.byte()?;
                    let mut left = usize::from(token >> 4);
                    if left == 15 {
                        left = left.saturating_add(self.extension()?);
                    }
                    self.state = Lz4State::Literals {
                        left,
                        match_base: usize::from(token & 15),
                    };
                }
                Lz4State::Literals { left, match_base } if left > 0 => {
                    let n = left.min(CHUNK);
                    let lit = self
                        .src
                        .get(self.pos..self.pos + n)
                        .ok_or_else(|| lz4_bad("literals run past the block"))?;
                    self.produce(n)?;
                    self.hist.extend_from_slice(lit);
                    self.pos += n;
                    self.state = Lz4State::Literals {
                        left: left - n,
                        match_base,
                    };
                }
                Lz4State::Literals { match_base, .. } => {
                    if self.pos == self.src.len() {
                        // The last sequence is literals only.
                        self.state = Lz4State::Token;
                        continue;
                    }
                    let offset = usize::from(u16::from_le_bytes([self.byte()?, self.byte()?]));
                    if offset == 0 || offset as u64 > self.produced {
                        return Err(lz4_bad("a match offset outside the output"));
                    }
                    let mut left = match_base + 4;
                    if match_base == 15 {
                        left = left.saturating_add(self.extension()?);
                    }
                    self.state = Lz4State::Match {
                        offset,
                        left,
                        done: 0,
                    };
                }
                Lz4State::Match { offset, left, done } => {
                    if left == 0 {
                        self.state = Lz4State::Token;
                        continue;
                    }
                    // Each copy reads only bytes already in the history. An
                    // overlapping match repeats its `offset`-byte pattern, so
                    // once this match has written some of it, a copy may reach
                    // back any whole number of periods inside the run — which
                    // lets a long match of a short pattern grow its copies
                    // instead of moving `offset` bytes at a time.
                    let run = (offset + done).min(CHUNK);
                    let reach = (run / offset).max(1) * offset;
                    let n = left.min(reach);
                    self.produce(n)?;
                    let start = self.hist.len() - reach;
                    self.hist.extend_from_within(start..start + n);
                    self.state = Lz4State::Match {
                        offset,
                        left: left - n,
                        done: done + n,
                    };
                }
            }
        }
    }
}

/// A reader that yields at most `left` bytes and then reports — as an
/// `io::Error` the scan maps to [`ScanError::TooLarge`] — any byte past them,
/// and that stops — mapped to [`ScanError::TimedOut`] — once `deadline` has
/// passed.
struct Capped<R> {
    inner: R,
    left: u64,
    exceeded: bool,
    deadline: Option<std::time::Instant>,
    timed_out: bool,
}

impl<R: std::io::Read> std::io::Read for Capped<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self
            .deadline
            .is_some_and(|d| std::time::Instant::now() >= d)
        {
            self.timed_out = true;
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "the scan's deadline passed",
            ));
        }
        if self.left == 0 {
            let mut probe = [0u8; 1];
            return match self.inner.read(&mut probe)? {
                0 => Ok(0),
                _ => {
                    self.exceeded = true;
                    Err(std::io::Error::other("decompressed past the cap"))
                }
            };
        }
        let want = buf
            .len()
            .min(usize::try_from(self.left).unwrap_or(usize::MAX));
        let n = self.inner.read(&mut buf[..want])?;
        self.left -= n as u64;
        Ok(n)
    }
}

/// **Streams** a KBAK v1 segment's records in order and hands each one's
/// [`RecordPrefix`] to `visit`, keeping at most `prefix` bytes of every key
/// and value; `visit` returns `false` to stop early. Returns how many records
/// were handed over.
///
/// Bounded in memory whatever the segment says, for a reader that runs inside
/// a backup that already succeeded (PROD-03.0's detector). Beyond the `bytes`
/// the caller holds, it allocates a few hundred KiB of stream buffers and,
/// for zstd, a window of at most 2^[`ZSTD_WINDOW_LOG_MAX`] bytes:
/// - no record is materialised, and no allocation is sized on a length the
///   segment declares;
/// - zstd is decoded as a stream, and a frame declaring a window above the cap
///   is `TooLarge` before it is decoded;
/// - lz4 is decoded as a stream that keeps the 64 KiB a match can reach, never
///   the whole body;
/// - every codec's output stops at `limits.max_decompressed` (`TooLarge`), and
///   every read stops once `limits.deadline` passes (`TimedOut`).
///
/// The envelope checks are [`decode_segment`]'s: magic, version, end magic and
/// the CRC32 over the bytes held.
pub fn scan_segment(
    bytes: &[u8],
    limits: &ScanLimits,
    prefix: usize,
    visit: &mut dyn FnMut(RecordPrefix) -> bool,
) -> Result<u64, ScanError> {
    use std::io::Read as _;
    let unreadable = |m: String| ScanError::Unreadable(m);
    if bytes.first() == Some(&b'[') || bytes.starts_with(b"{") {
        return Err(unreadable("legacy JSON segment".into()));
    }
    if bytes.starts_with(&ZSTD_MAGIC) {
        return Err(unreadable("whole-object zstd legacy JSON segment".into()));
    }
    if bytes.len() < HEADER_SIZE + FOOTER_SIZE {
        return Err(unreadable("segment shorter than header+footer".into()));
    }
    if &bytes[0..4] != MAGIC {
        return Err(unreadable("bad magic: not a KBAK segment".into()));
    }
    if bytes[4] != 1 {
        return Err(unreadable(format!("KBAK version {} is not v1", bytes[4])));
    }
    let compression = bytes[5];
    let record_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let tail = bytes.len() - FOOTER_SIZE;
    if &bytes[tail + 4..] != MAGIC_END {
        return Err(unreadable("bad end magic: truncated segment".into()));
    }
    let want_crc = u32::from_le_bytes(bytes[tail..tail + 4].try_into().unwrap());
    let got_crc = crc32fast::hash(&bytes[..tail]);
    if want_crc != got_crc {
        return Err(unreadable(format!(
            "CRC32 mismatch: {got_crc:#x} != {want_crc:#x}"
        )));
    }
    let body = &bytes[HEADER_SIZE..tail];
    let max_decompressed = limits.max_decompressed;
    let too_large = || {
        ScanError::TooLarge(format!(
            "the body decompresses past {max_decompressed} bytes"
        ))
    };
    let stream: Box<dyn std::io::Read + '_> = match compression {
        0 => Box::new(body),
        1 => {
            let window_cap = 1u64 << ZSTD_WINDOW_LOG_MAX;
            if let Some(window) = zstd_first_window(body) {
                if window > window_cap {
                    return Err(ScanError::TooLarge(format!(
                        "the zstd frame declares a {window}-byte window, above the \
                         {window_cap}-byte cap"
                    )));
                }
            }
            let mut decoder = zstd::stream::read::Decoder::with_buffer(body)
                .map_err(|e| unreadable(format!("zstd: {e}")))?;
            decoder
                .window_log_max(ZSTD_WINDOW_LOG_MAX)
                .map_err(|e| unreadable(format!("zstd: {e}")))?;
            Box::new(decoder)
        }
        2 => {
            let stream = Lz4Stream::new(body).map_err(|e| unreadable(e.to_string()))?;
            if stream.declared > max_decompressed {
                return Err(too_large());
            }
            Box::new(stream)
        }
        c => return Err(unreadable(format!("unknown compression byte {c}"))),
    };
    let mut reader = std::io::BufReader::new(Capped {
        inner: stream,
        left: max_decompressed,
        exceeded: false,
        deadline: limits.deadline,
        timed_out: false,
    });
    let mut visited: u64 = 0;
    while visited < record_count {
        let mut len = [0u8; 4];
        let step = reader.read_exact(&mut len).and_then(|()| {
            let total_len = u64::from(u32::from_le_bytes(len));
            let mut frame = (&mut reader).take(total_len);
            let r = read_prefix_record(&mut frame, prefix)?;
            if frame.limit() != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "total_len mismatch: record declared {total_len} bytes and its fields \
                         used {}",
                        total_len - frame.limit()
                    ),
                ));
            }
            Ok(r)
        });
        let record = match step {
            Ok(r) => r,
            Err(_) if reader.get_ref().exceeded => return Err(too_large()),
            Err(_) if reader.get_ref().timed_out => {
                return Err(ScanError::TimedOut(format!(
                    "the deadline passed after {visited} records"
                )))
            }
            // A frame the decoder refuses for its window (a later frame than
            // the one whose header was read above).
            Err(e) if compression == 1 && e.to_string().contains("too much memory") => {
                return Err(ScanError::TooLarge(format!("zstd: {e}")))
            }
            Err(e) => return Err(unreadable(format!("record {visited}: {e}"))),
        };
        visited += 1;
        if !visit(record) {
            break;
        }
    }
    Ok(visited)
}

/// One record from its frame, keeping `prefix` bytes of the key and value and
/// skipping everything else without holding it.
fn read_prefix_record(
    frame: &mut impl std::io::Read,
    prefix: usize,
) -> std::io::Result<RecordPrefix> {
    use byteorder::{ReadBytesExt, LE};
    let _timestamp = frame.read_i64::<LE>()?;
    let offset = frame.read_i64::<LE>()?;
    let key_len = frame.read_i32::<LE>()?;
    let key = prefix_of(frame, key_len, prefix)?;
    let value_len = frame.read_i32::<LE>()?;
    let value = prefix_of(frame, value_len, prefix)?;
    let header_count = frame.read_u16::<LE>()?;
    for _ in 0..header_count {
        let hk_len = frame.read_u16::<LE>()?;
        skip(frame, u64::from(hk_len))?;
        let hv_len = frame.read_i32::<LE>()?;
        if hv_len >= 0 {
            skip(frame, hv_len as u64)?;
        }
    }
    Ok(RecordPrefix { offset, key, value })
}

/// A nullable field's first `prefix` bytes (`-1` is `None`), the rest skipped.
fn prefix_of(
    frame: &mut impl std::io::Read,
    len: i32,
    prefix: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    if len < 0 {
        return Ok(None);
    }
    let len = len as u64;
    let keep = len.min(prefix as u64) as usize;
    let mut kept = vec![0u8; keep];
    frame.read_exact(&mut kept)?;
    skip(frame, len - keep as u64)?;
    Ok(Some(kept))
}

/// Reads and discards exactly `n` bytes; a short read is an error.
fn skip(frame: &mut impl std::io::Read, n: u64) -> std::io::Result<()> {
    let mut bounded = <&mut _ as std::io::Read>::take(&mut *frame, n);
    let copied = std::io::copy(&mut bounded, &mut std::io::sink())?;
    if copied != n {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!("a field declared {n} bytes and the frame held {copied}"),
        ));
    }
    Ok(())
}
