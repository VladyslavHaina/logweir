//! **PROD-03.0 — `kbak::scan_segment`, the streaming prefix scan the backup's
//! schema dependency detector reads segments through.** It must agree with
//! `decode_segment` on every record it hands over (offsets, null-ness and the
//! first bytes of each key and value), stop when told, and refuse — as
//! `TooLarge`, before holding anything past its cap — a body that decompresses
//! past the cap, whatever the codec.
use logweir_engine_oso::kbak::{
    decode_segment, scan_segment, RecordPrefix, ScanError, ScanLimits, ZSTD_WINDOW_LOG_MAX,
};

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("../../e2e/fixtures/segments/{name}")).unwrap()
}

fn limits(cap: u64) -> ScanLimits {
    ScanLimits {
        max_decompressed: cap,
        deadline: None,
    }
}

fn scan_all(bytes: &[u8], cap: u64, prefix: usize) -> Result<Vec<RecordPrefix>, ScanError> {
    let mut out = Vec::new();
    scan_segment(bytes, &limits(cap), prefix, &mut |r| {
        out.push(r);
        true
    })?;
    Ok(out)
}

fn cut(v: &Option<Vec<u8>>, n: usize) -> Option<Vec<u8>> {
    v.as_ref().map(|b| b[..b.len().min(n)].to_vec())
}

/// Every codec, and the real upstream segment: the scan hands over exactly
/// the records the full decoder decodes, cut to the prefix.
#[test]
fn the_scan_agrees_with_the_full_decoder_on_every_fixture() {
    for name in ["none.kbak", "zstd.kbak", "lz4.kbak", "upstream-0.21.0.kbak"] {
        let bytes = fixture(name);
        let full = decode_segment(&bytes).unwrap();
        for prefix in [0, 1, 6, 1 << 20] {
            let scanned = scan_all(&bytes, 1 << 30, prefix).unwrap();
            assert_eq!(scanned.len(), full.len(), "{name}");
            for (s, f) in scanned.iter().zip(&full) {
                assert_eq!(s.offset, f.offset, "{name}");
                assert_eq!(s.key, cut(&f.key, prefix), "{name} prefix {prefix}");
                assert_eq!(s.value, cut(&f.value, prefix), "{name} prefix {prefix}");
            }
        }
    }
}

#[test]
fn the_scan_stops_when_told() {
    let bytes = fixture("none.kbak");
    let mut seen = 0;
    let read = scan_segment(&bytes, &limits(1 << 30), 6, &mut |_| {
        seen += 1;
        seen < 2
    })
    .unwrap();
    assert_eq!((read, seen), (2, 2));
}

/// A zstd body of 4 MiB of zeros, stored in a few hundred bytes: past a 1 MiB
/// cap the scan stops with `TooLarge`; under an 8 MiB cap it reads.
#[test]
fn a_zstd_bomb_is_too_large_at_the_cap() {
    let value = 4u64 << 20;
    let mut body = Vec::new();
    body.extend_from_slice(&((8 + 8 + 4 + 4 + value + 2) as u32).to_le_bytes());
    body.extend_from_slice(&0i64.to_le_bytes());
    body.extend_from_slice(&0i64.to_le_bytes());
    body.extend_from_slice(&(-1i32).to_le_bytes());
    body.extend_from_slice(&(value as i32).to_le_bytes());
    body.resize(body.len() + value as usize, 0);
    body.extend_from_slice(&0u16.to_le_bytes());
    let bytes = envelope(1, 1, &zstd::encode_all(&body[..], 19).unwrap());
    assert!(bytes.len() < 4096, "{}", bytes.len());
    match scan_all(&bytes, 1 << 20, 6) {
        Err(ScanError::TooLarge(_)) => {}
        other => panic!("a bomb past the cap is TooLarge: {other:?}"),
    }
    let read = scan_all(&bytes, 8 << 20, 6).unwrap();
    assert_eq!(read.len(), 1, "NEGATIVE CONTROL: under the cap it reads");
    assert_eq!(read[0].value, Some(vec![0; 6]));
}

/// An lz4 body (block format, decompressed whole) DECLARING more than the cap
/// is refused before anything is allocated for it.
#[test]
fn an_lz4_body_declaring_more_than_the_cap_is_too_large() {
    let bytes = fixture("lz4.kbak");
    let declared = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as u64;
    match scan_all(&bytes, declared - 1, 6) {
        Err(ScanError::TooLarge(_)) => {}
        other => panic!("{other:?}"),
    }
    assert!(
        scan_all(&bytes, declared, 6).is_ok(),
        "NEGATIVE CONTROL: at the cap it reads"
    );
    // A body DECLARING 512 MiB in four bytes, followed by bytes that are no lz4
    // block: refused as `TooLarge` from the declaration alone. A scan that
    // allocated first would reach the codec and call it `Unreadable`.
    let mut body = (512u32 << 20).to_le_bytes().to_vec();
    body.extend_from_slice(&[0xFF; 16]);
    match scan_all(&envelope(1, 2, &body), 1 << 20, 6) {
        Err(ScanError::TooLarge(_)) => {}
        other => panic!("a declaration past the cap is refused before decoding: {other:?}"),
    }
}

#[test]
fn a_corrupt_segment_is_unreadable_never_a_partial_answer() {
    let mut bytes = fixture("none.kbak");
    let n = bytes.len();
    bytes[n / 2] ^= 0xFF;
    assert!(matches!(
        scan_all(&bytes, 1 << 30, 6),
        Err(ScanError::Unreadable(_))
    ));
    assert!(matches!(
        scan_all(b"[{\"legacy\":1}]", 1 << 30, 6),
        Err(ScanError::Unreadable(_))
    ));
}

fn envelope(count: u64, codec: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"KBAK");
    out.extend_from_slice(&[1, codec, 0, 0]);
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes());
    out.extend_from_slice(body);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(b"BKAE");
    out
}

/// One record frame: a null key and `value`.
fn frame(value: &[u8]) -> Vec<u8> {
    let mut rec = Vec::new();
    rec.extend_from_slice(&0i64.to_le_bytes());
    rec.extend_from_slice(&7i64.to_le_bytes());
    rec.extend_from_slice(&(-1i32).to_le_bytes());
    rec.extend_from_slice(&(value.len() as i32).to_le_bytes());
    rec.extend_from_slice(value);
    rec.extend_from_slice(&0u16.to_le_bytes());
    let mut out = (rec.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&rec);
    out
}

/// M1 (review): a zstd frame that DECLARES a large window is refused before
/// the decoder allocates it — windowLog 27 is 128 MiB of resident memory
/// whatever the output cap. The same records at the default window read.
#[test]
fn a_zstd_frame_declaring_a_window_above_the_cap_is_too_large() {
    use std::io::Write as _;
    let body = frame(&[0u8; 4096]);
    let encode = |window_log: Option<u32>| {
        let mut enc = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
        if let Some(w) = window_log {
            enc.window_log(w).unwrap();
        }
        enc.write_all(&body).unwrap();
        envelope(1, 1, &enc.finish().unwrap())
    };
    match scan_all(&encode(Some(27)), 1 << 30, 6) {
        Err(ScanError::TooLarge(m)) => assert!(m.contains("window"), "{m}"),
        other => panic!("a windowLog-27 frame is TooLarge: {other:?}"),
    }
    match scan_all(&encode(Some(ZSTD_WINDOW_LOG_MAX + 1)), 1 << 30, 6) {
        Err(ScanError::TooLarge(_)) => {}
        other => panic!("one above the cap is TooLarge: {other:?}"),
    }
    assert_eq!(
        scan_all(&encode(Some(ZSTD_WINDOW_LOG_MAX)), 1 << 30, 6)
            .unwrap()
            .len(),
        1,
        "NEGATIVE CONTROL: at the cap it reads"
    );
    assert_eq!(scan_all(&encode(None), 1 << 30, 6).unwrap().len(), 1);
}

/// The streaming lz4 decoder is lz4_flex's block decoder, byte for byte, over
/// literals only, long overlapping matches, mixed data, and bodies past the
/// 64 KiB match window — read in small and odd-sized pieces.
#[test]
fn the_streaming_lz4_decoder_agrees_with_the_block_decoder() {
    let mut x: u64 = 0x1234_5678_9ABC_DEF0;
    let mut rnd = || {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (x >> 33) as u8
    };
    let random: Vec<u8> = (0..200_000).map(|_| rnd()).collect();
    let mut mixed = Vec::new();
    for i in 0..3000u32 {
        mixed.extend_from_slice(format!("{{\"id\":{i},\"note\":\"aaaaaaaaaaaa\"}}").as_bytes());
        mixed.extend((0..(i % 50)).map(|_| rnd()));
    }
    let inputs: Vec<Vec<u8>> = vec![
        Vec::new(),
        vec![7],
        vec![0; 300_000],
        b"abc".repeat(100_000),
        random,
        mixed,
    ];
    for (n, input) in inputs.iter().enumerate() {
        let body = frame(input);
        let block = lz4_flex::block::compress_prepend_size(&body);
        let bytes = envelope(1, 2, &block);
        let got = scan_all(&bytes, 1 << 30, 1 << 30).unwrap_or_else(|e| panic!("input {n}: {e}"));
        assert_eq!(got.len(), 1, "input {n}");
        assert_eq!(got[0].value.as_deref(), Some(input.as_slice()), "input {n}");
    }
}

/// A corrupt lz4 block is Unreadable, never a panic and never a partial
/// answer: a truncated block, an offset before the start, and a block
/// decoding past its declared size.
#[test]
fn a_corrupt_lz4_block_is_unreadable() {
    let body = frame(&b"hello world ".repeat(1000));
    let good = lz4_flex::block::compress_prepend_size(&body);
    let mut truncated = good.clone();
    truncated.truncate(good.len() / 2);
    let mut understated = good.clone();
    understated[0..4].copy_from_slice(&((body.len() - 1) as u32).to_le_bytes());
    // A literal of one byte, then a match whose offset reaches before it.
    let before_start = {
        let mut b = (100u32).to_le_bytes().to_vec();
        b.extend_from_slice(&[0x10, b'x', 0x05, 0x00]);
        b
    };
    for (what, block) in [
        ("truncated", truncated),
        ("understated size", understated),
        ("offset before the start", before_start),
    ] {
        match scan_all(&envelope(1, 2, &block), 1 << 30, 6) {
            Err(ScanError::Unreadable(_)) => {}
            other => panic!("{what}: {other:?}"),
        }
    }
    assert_eq!(
        scan_all(&envelope(1, 2, &good), 1 << 30, 6).unwrap().len(),
        1
    );
}

/// The deadline stops the scan on its next read: a past deadline is
/// `TimedOut`, and no deadline reads.
#[test]
fn a_passed_deadline_stops_the_scan() {
    let bytes = fixture("zstd.kbak");
    let past = ScanLimits {
        max_decompressed: 1 << 30,
        deadline: Some(std::time::Instant::now()),
    };
    match scan_segment(&bytes, &past, 6, &mut |_| true) {
        Err(ScanError::TimedOut(_)) => {}
        other => panic!("{other:?}"),
    }
    assert!(scan_segment(&bytes, &limits(1 << 30), 6, &mut |_| true).is_ok());
}
