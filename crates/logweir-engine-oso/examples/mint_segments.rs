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
fn record(
    offset: i64,
    timestamp: i64,
    key: &[u8],
    value: &[u8],
    headers: &[(&str, Option<&[u8]>)],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.write_i64::<LE>(timestamp).unwrap();
    body.write_i64::<LE>(offset).unwrap();
    put_opt(&mut body, Some(key));
    put_opt(&mut body, Some(value));
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

fn segment(compression: u8, records: &[Vec<u8>]) -> Vec<u8> {
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
    out.write_i64::<LE>(100).unwrap(); // start_offset
    out.write_i64::<LE>(102).unwrap(); // end_offset
    assert_eq!(out.len(), 32, "HEADER_SIZE is 32 (format.rs:63)");
    out.extend_from_slice(&body);
    let crc = crc32fast::hash(&out); // over all preceding bytes
    out.write_u32::<LE>(crc).unwrap();
    out.extend_from_slice(b"BKAE"); // magic_end
    out
}

fn main() {
    std::fs::create_dir_all("e2e/fixtures/segments").unwrap();
    let recs: Vec<Vec<u8>> = (0..3)
        .map(|i| {
            let off = 100 + i as i64;
            record(
                off,
                1_756_425_600_000 + i as i64,
                format!("k{i}").as_bytes(),
                format!("v{i}").as_bytes(),
                &[
                    ("x-original-offset", Some(off.to_string().as_bytes())),
                    ("x-original-timestamp", Some(b"1756425600000")),
                ],
            )
        })
        .collect();
    for (name, c) in [("none", 0u8), ("zstd", 1), ("lz4", 2)] {
        std::fs::write(
            format!("e2e/fixtures/segments/{name}.kbak"),
            segment(c, &recs),
        )
        .unwrap();
    }
    std::fs::write(
        "e2e/fixtures/segments/legacy.json",
        br#"[{"offset":100,"timestamp":1,"key":"azA=","value":"djA="}]"#, // harness-only: JSON fixture field name, not a kafka-backup CLI subcommand
    )
    .unwrap();
    println!("minted 3 .kbak fixtures and legacy.json");
}
