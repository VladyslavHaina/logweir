//! **PROD-03.0 — `kbak::scan_segment`, the streaming prefix scan the backup's
//! schema dependency detector reads segments through.** It must agree with
//! `decode_segment` on every record it hands over (offsets, null-ness and the
//! first bytes of each key and value), stop when told, and refuse — as
//! `TooLarge`, before holding anything past its cap — a body that decompresses
//! past the cap, whatever the codec.
use logweir_engine_oso::kbak::{decode_segment, scan_segment, RecordPrefix, ScanError};

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(format!("../../e2e/fixtures/segments/{name}")).unwrap()
}

fn scan_all(bytes: &[u8], cap: u64, prefix: usize) -> Result<Vec<RecordPrefix>, ScanError> {
    let mut out = Vec::new();
    scan_segment(bytes, cap, prefix, &mut |r| {
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
    let read = scan_segment(&bytes, 1 << 30, 6, &mut |_| {
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
