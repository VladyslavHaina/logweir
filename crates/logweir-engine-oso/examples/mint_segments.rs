//! Mints e2e/fixtures/segments/{none,zstd,lz4}.kbak and legacy.json by ENCODING
//! the layout kbak.rs decodes. Given in full because every later golden compares
//! against these bytes, so "generate it once" is only reproducible if the
//! generator is in the plan.
use byteorder::{WriteBytesExt, LE};
use std::io::Write;

fn put_opt(buf: &mut Vec<u8>, v: Option<&[u8]>) {
    match v {
        None => buf.write_i32::<LE>(-1).unwrap(),
        Some(b) => {
            buf.write_i32::<LE>(b.len() as i32).unwrap();
            buf.write_all(b).unwrap();
        }
    }
}

/// timestamp, offset, key, value, headers — the record layout of format.rs:27-42.
/// `key`/`value` are `Option`, not bare slices: a fixture generator that can
/// only ever emit `Some` structurally cannot mint the null-key/null-value
/// case the decoder's `opt_bytes` `len < 0 -> None` branch exists to handle
/// (upstream issue #155).
fn record(
    offset: i64,
    timestamp: i64,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
    headers: &[(&str, Option<&[u8]>)],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.write_i64::<LE>(timestamp).unwrap();
    body.write_i64::<LE>(offset).unwrap();
    put_opt(&mut body, key);
    put_opt(&mut body, value);
    body.write_u16::<LE>(headers.len() as u16).unwrap();
    for (k, v) in headers {
        body.write_u16::<LE>(k.len() as u16).unwrap();
        body.write_all(k.as_bytes()).unwrap();
        put_opt(&mut body, *v);
    }
    let mut out = Vec::new();
    // total_len counts the REMAINING record data (format.rs:27-42).
    out.write_u32::<LE>(body.len() as u32).unwrap();
    out.extend_from_slice(&body);
    out
}

fn segment(compression: u8, records: &[Vec<u8>], start_offset: i64, end_offset: i64) -> Vec<u8> {
    let plain: Vec<u8> = records.concat();
    let body = match compression {
        0 => plain.clone(),
        1 => zstd::stream::encode_all(&plain[..], 3).unwrap(),
        2 => lz4_flex::block::compress_prepend_size(&plain),
        c => panic!("unknown compression {c}"),
    };
    let mut out = Vec::with_capacity(32 + body.len() + 8);
    out.extend_from_slice(b"KBAK"); // magic
    out.push(1); // version
    out.push(compression);
    out.extend_from_slice(&[0u8; 2]); // reserved
    out.write_u64::<LE>(records.len() as u64).unwrap(); // record_count
    out.write_i64::<LE>(start_offset).unwrap();
    out.write_i64::<LE>(end_offset).unwrap();
    assert_eq!(out.len(), 32, "HEADER_SIZE is 32 (format.rs:63)");
    out.extend_from_slice(&body);
    let crc = crc32fast::hash(&out); // over all preceding bytes
    out.write_u32::<LE>(crc).unwrap();
    out.extend_from_slice(b"BKAE"); // magic_end
    out
}

fn main() {
    std::fs::create_dir_all("e2e/fixtures/segments").unwrap();
    let mut recs: Vec<Vec<u8>> = (0..3)
        .map(|i| {
            let off = 100 + i as i64;
            let ts = 1_756_425_600_000 + i as i64;
            record(
                off,
                ts,
                Some(format!("k{i}").as_bytes()),
                Some(format!("v{i}").as_bytes()),
                &[
                    // Both are little-endian i64 [VERIFIED offset_headers.rs:19-22,
                    // config.rs:447-448, backup/engine.rs:2140-2147], each record's
                    // OWN offset/timestamp — not a shared constant.
                    ("x-original-offset", Some(&off.to_le_bytes()[..])),
                    ("x-original-timestamp", Some(&ts.to_le_bytes()[..])),
                ],
            )
        })
        .collect();
    // A fourth record dedicated to the null/empty distinction end to end: a
    // null key, a null value, and a header whose VALUE is null (the header's
    // own key is never optional in this format — only its value is). Absent
    // this record, `opt_bytes`'s `len < 0 -> None` branch has no fixture
    // coverage at all: all of records 0-2 carry a real key, a real value and
    // two non-null header values.
    recs.push(record(
        103,
        1_756_425_600_003,
        None,
        None,
        &[("x-null-header", None)],
    ));
    for (name, c) in [("none", 0u8), ("zstd", 1), ("lz4", 2)] {
        std::fs::write(
            format!("e2e/fixtures/segments/{name}.kbak"),
            segment(c, &recs, 100, 103),
        )
        .unwrap();
    }
    std::fs::write(
        "e2e/fixtures/segments/legacy.json",
        br#"[{"offset":100,"timestamp":1,"key":"azA=","value":"djA="}]"#, // harness-only: JSON fixture field name, not a kafka-backup CLI subcommand
    )
    .unwrap();
    println!("minted 3 .kbak fixtures (4 records each) and legacy.json");
}
