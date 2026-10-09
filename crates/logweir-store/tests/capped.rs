//! **FX-31 — every object-store read is bounded in size.**
//!
//! `Store::get` used to read a whole object, so the memory one read could take
//! was whatever a bucket held. These rows hold the two fences of
//! `Store::get_capped` (the reported size first, then a running cap over the
//! stream), the `head` an existence test uses instead of a read, the version
//! read, the cap table, and the streaming manifest parse that replaced a
//! `serde_json::Value` walk — against the walk itself, kept here as the
//! oracle.
//!
//! No socket: the in-memory backend, the misreporting double
//! (`Store::in_memory_misreporting_size`) and a scratch filesystem tree.

use logweir_store::{caps, OverCap, Store, StoreError, StoreErrorClass};

const KEY: &str = "logweir/drills/run-1.json";

fn put(s: &Store, key: &str, bytes: &[u8]) {
    s.put_create_only(key, bytes)
        .expect("the fixture is written");
}

// ---------------------------------------------------------------- fence 1

/// The cap is inclusive, and one byte over it is refused, naming the cap and
/// the size the store reported.
///
/// KILLS: "no head check" only together with the next row; on its own it kills
/// an off-by-one (`>=` for `>`) and a message that does not name the cap.
#[test]
fn an_object_within_the_cap_is_read_and_one_byte_over_is_refused_naming_the_cap() {
    let s = Store::in_memory("logweir/");
    put(&s, KEY, &[7u8; 100]);

    let (bytes, _) = s
        .get_capped(KEY, 100)
        .expect("exactly at the cap is within it");
    assert_eq!(bytes.len(), 100);

    match s.get_capped(KEY, 99) {
        Err(StoreError::TooLarge { key, cap, observed }) => {
            assert_eq!(key, KEY);
            assert_eq!(cap, 99);
            assert_eq!(observed, OverCap::Reported(100));
        }
        other => panic!("one byte over the cap must be TooLarge, got {other:?}"),
    }
    let message = s.get_capped(KEY, 99).unwrap_err().to_string();
    assert!(
        message.contains("is larger than the 99-byte read cap")
            && message.contains("the store reports 100 bytes")
            && message.contains(KEY),
        "the refusal names the key, the cap and the reported size: {message}"
    );
}

/// **Fence 1 is checked BEFORE the body.** The double reports a gigabyte and
/// holds ten bytes: a reader that streamed first would get ten bytes back and
/// call it a success.
///
/// KILLS: "no head check" (the size is never consulted, the ten bytes come
/// back as `Ok`).
#[test]
fn an_object_whose_reported_size_is_over_the_cap_is_refused_before_its_body_is_read() {
    let (s, meter) = Store::in_memory_misreporting_size("logweir/", 1 << 30);
    put(&s, KEY, b"0123456789");
    match s.get_capped(KEY, 1 << 20) {
        Err(StoreError::TooLarge {
            observed: OverCap::Reported(size),
            cap,
            ..
        }) => {
            assert_eq!(size, 1 << 30, "the store's own claim is what refused it");
            assert_eq!(cap, 1 << 20);
        }
        other => panic!(
            "a reported size over the cap must be refused on that size alone, without the body; \
             got {other:?}"
        ),
    }
    assert_eq!(
        meter.streamed(),
        0,
        "not one body byte was taken: the refusal is on the reported size alone"
    );
}

/// The same fence on a real backend: a filesystem object's metadata. The file
/// is SPARSE, so the row costs no disk.
#[test]
fn the_reported_size_fence_holds_on_a_filesystem_store() {
    let tree = Tree::new("fs-size");
    let path = tree.root.join(KEY);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let f = std::fs::File::create(&path).unwrap();
    f.set_len(2 << 20).unwrap();
    drop(f);
    let s = tree.handle();
    match s.get_capped(KEY, 1 << 20) {
        Err(StoreError::TooLarge {
            observed: OverCap::Reported(size),
            ..
        }) => assert_eq!(size, 2 << 20),
        other => panic!("a 2 MiB file over a 1 MiB cap must be refused, got {other:?}"),
    }
    let (bytes, _) = s.get_capped(KEY, 2 << 20).expect("at the cap it reads");
    assert_eq!(bytes.len(), 2 << 20);
}

// ---------------------------------------------------------------- fence 2

/// **A store that reports a small size and streams more is cut off at the
/// cap.** The double says ten bytes and streams a mebibyte.
///
/// The control is the same object under a cap that holds it: the lie alone is
/// not a refusal, so the row's refusal is the running cap's and nothing else's.
///
/// KILLS: "no running cap" (the whole mebibyte comes back as `Ok`).
#[test]
fn a_store_that_reports_a_small_size_and_streams_more_is_cut_off_at_the_cap() {
    let (s, _meter) = Store::in_memory_misreporting_size("logweir/", 10);
    put(&s, KEY, &vec![1u8; 1 << 20]);
    match s.get_capped(KEY, 64 << 10) {
        Err(StoreError::TooLarge {
            observed: OverCap::Streamed { reported, read },
            cap,
            ..
        }) => {
            assert_eq!(
                reported, 10,
                "the store's claim is recorded beside what it did"
            );
            assert!(
                read > cap,
                "reading stopped at the chunk that crossed the cap: {read}"
            );
            assert_eq!(cap, 64 << 10);
        }
        other => panic!(
            "a stream past the cap must be cut off whatever size the store reported; got {other:?}"
        ),
    }
    let message = s.get_capped(KEY, 64 << 10).unwrap_err().to_string();
    assert!(
        message.contains("the store reported 10 bytes and streamed at least"),
        "the refusal says what the store did: {message}"
    );

    // CONTROL: under a cap that holds the object, the misreporting store's
    // bytes come back whole.
    let (bytes, _) = s.get_capped(KEY, 1 << 20).expect("within the cap");
    assert_eq!(bytes.len(), 1 << 20);
}

// ---------------------------------------------------------------- versions

/// The version read has both fences too: over the cap is `TooLarge` naming
/// the version it was asked for.
#[test]
fn a_version_read_is_capped_like_a_current_read() {
    let (s, bucket) = Store::in_memory_versioned("logweir/");
    put(&s, KEY, &[3u8; 64]);
    let first = bucket.versions(KEY).pop().expect("one version");
    bucket.overwrite(KEY, b"later");

    let (old, answered) = s
        .get_version_capped(KEY, &first, 64)
        .expect("at the cap it reads");
    assert_eq!(old.len(), 64);
    assert_eq!(answered.as_deref(), Some(first.as_str()));

    match s.get_version_capped(KEY, &first, 63) {
        Err(StoreError::TooLarge { key, cap, observed }) => {
            assert_eq!(key, format!("{KEY}?versionId={first}"));
            assert_eq!(cap, 63);
            assert_eq!(observed, OverCap::Reported(64));
        }
        other => panic!("a pinned version over the cap must be refused, got {other:?}"),
    }
}

// ---------------------------------------------------------------- head

/// **The existence test reads no body.** `head` answers with the size the
/// store reports and takes no byte of the body, while a capped GET of the same
/// object — present, small, and REPORTED over the cap — is refused, so a GET
/// cannot stand in for `head` as an existence test.
///
/// KILLS: "existence via a full read": a `head` written as a GET takes body
/// bytes (the meter moves) or is refused (the size never comes back).
#[test]
fn head_answers_with_the_reported_size_and_takes_no_body_byte() {
    let (s, meter) = Store::in_memory_misreporting_size("logweir/", 5 << 30);
    put(&s, KEY, b"a small object reported as five gibibytes");
    let head = s.head(KEY).expect("a HEAD of a present object answers");
    assert_eq!(head.size, 5 << 30, "the store's own size, unread");
    assert_eq!(meter.streamed(), 0, "a HEAD takes no body byte");
    assert!(
        matches!(
            s.get_capped(KEY, caps::SIDECAR),
            Err(StoreError::TooLarge { .. })
        ),
        "a capped GET of the same object refuses it, so it is no existence test"
    );
    assert_eq!(meter.streamed(), 0, "and the refusal took none either");

    // CONTROL: the meter sees a read that does take the body.
    let (_, meter_too) = {
        let (s2, m2) = Store::in_memory_misreporting_size("logweir/", 1);
        put(&s2, KEY, b"0123456789");
        let _ = s2.get_capped(KEY, 1 << 20);
        (s2, m2)
    };
    assert!(
        meter_too.streamed() > 0,
        "the meter counts what a GET streams"
    );

    assert!(matches!(
        s.head("logweir/drills/absent.json"),
        Err(StoreError::NotFound(_))
    ));
}

/// `head` on a real backend: a filesystem object's metadata, absent is
/// `NotFound`.
#[test]
fn head_reports_a_filesystem_objects_size() {
    let tree = Tree::new("head");
    let path = tree.root.join(KEY);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let f = std::fs::File::create(&path).unwrap();
    f.set_len(3 << 20).unwrap();
    drop(f);
    let s = tree.handle();
    assert_eq!(s.head(KEY).unwrap().size, 3 << 20);
    assert!(matches!(
        s.head("logweir/drills/absent.json"),
        Err(StoreError::NotFound(_))
    ));
}

// ---------------------------------------------------------------- the error

/// `TooLarge` is structural: never token-scanned into a code it does not mean
/// (its text carries an adopter's key), and a plain operational error for every
/// `?` caller, whose message still names the cap.
#[test]
fn too_large_is_unclassified_and_names_the_cap_through_engine_error() {
    let e = StoreError::TooLarge {
        key: "logweir/timeout/AccessDenied.json".to_string(),
        cap: 1024,
        observed: OverCap::Reported(4096),
    };
    assert_eq!(
        StoreErrorClass::classify(&e),
        StoreErrorClass::StoreErrorUnclassified,
        "a key that spells a code must not become that code"
    );
    let engine: logweir_core::engine::EngineError = e.into();
    assert!(
        engine.to_string().contains("1024-byte read cap"),
        "{engine}"
    );
}

// ---------------------------------------------------------------- the table

/// The caps the controller reads with are the evidence relay's, so a document
/// is verifiable by the controller's own handle exactly when it is verifiable
/// through a relay; every controller cap is at most its runner twin; the probe
/// reads no body.
#[test]
fn the_cap_table_holds_its_own_relations() {
    use logweir_core::check_contract::{MAX_EVIDENCE_PAYLOAD_BYTES, MAX_EVIDENCE_SIDECAR_BYTES};
    assert_eq!(caps::CONTROLLER_DOCUMENT, MAX_EVIDENCE_PAYLOAD_BYTES);
    assert_eq!(caps::SIDECAR, MAX_EVIDENCE_SIDECAR_BYTES);
    const _: () = assert!(caps::CONTROLLER_DOCUMENT <= caps::SIGNED_DOCUMENT);
    const _: () = assert!(caps::CONTROLLER_MANIFEST <= caps::MANIFEST);
    assert_eq!(caps::PROBE, 0);
    // A probe GET of a present, non-empty object is answered and refused
    // unread; an empty one is read (nothing to read).
    let s = Store::in_memory("logweir/");
    put(&s, KEY, b"{}");
    assert!(matches!(
        s.get_capped(KEY, caps::PROBE),
        Err(StoreError::TooLarge { .. })
    ));
    put(&s, "logweir/drills/empty.json", b"");
    assert!(s
        .get_capped("logweir/drills/empty.json", caps::PROBE)
        .is_ok());
}

// ---------------------------------------------------- the streaming manifest

/// The `serde_json::Value` walk `manifest_facts` used before FX-31, VERBATIM
/// in its decisions, as the oracle: `(oldest, newest)` or the error's variant
/// and message.
fn value_walk(key: &str, bytes: &[u8]) -> Result<(i64, i64), (String, String)> {
    fn kind_of(v: &serde_json::Value) -> &'static str {
        match v {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "a boolean",
            serde_json::Value::Number(_) => "a number",
            serde_json::Value::String(_) => "a string",
            serde_json::Value::Array(_) => "an array",
            serde_json::Value::Object(_) => "an object",
        }
    }
    let io = |m: String| ("Io".to_string(), m);
    let v: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| io(format!("storage: {key}: {e}")))?;
    let mut newest: Option<i64> = None;
    let mut oldest: Option<i64> = None;
    let Some(topics) = v.get("topics").map(|t| {
        t.as_array()
            .map(|a| a.as_slice())
            .ok_or_else(|| format!("`topics` is {}, not an array", kind_of(t)))
    }) else {
        return Err((
            "NotAManifest".to_string(),
            format!("{key} is not a backup manifest: it declares no `topics` key"),
        ));
    };
    let topics = topics.map_err(|why| {
        (
            "NotAManifest".to_string(),
            format!("{key} is not a backup manifest: {why}"),
        )
    })?;
    for t in topics {
        let parts = t
            .get("partitions")
            .and_then(|p| p.as_array())
            .map(|a| a.as_slice())
            .unwrap_or(&[]);
        for p in parts {
            let ss = p
                .get("segments")
                .and_then(|s| s.as_array())
                .map(|a| a.as_slice())
                .unwrap_or(&[]);
            for s in ss {
                let (Some(t0), Some(t1)) = (
                    s.get("start_timestamp").and_then(|x| x.as_i64()),
                    s.get("end_timestamp").and_then(|x| x.as_i64()),
                ) else {
                    return Err((
                        "Backend".to_string(),
                        format!(
                            "unsupported storage backend `{key}: segment entry missing \
                             start_timestamp/end_timestamp`"
                        ),
                    ));
                };
                oldest = Some(oldest.map_or(t0, |o: i64| o.min(t0)));
                newest = Some(newest.map_or(t1, |n: i64| n.max(t1)));
            }
        }
    }
    match (oldest, newest) {
        (Some(o), Some(n)) => Ok((o, n)),
        _ => Err((
            "Backend".to_string(),
            format!(
                "unsupported storage backend `{key}: manifest declares no segment, so it bounds \
                 no window`"
            ),
        )),
    }
}

fn streamed(s: &Store, key: &str) -> Result<(i64, i64), (String, String)> {
    match s.manifest_facts(key, caps::CONTROLLER_MANIFEST) {
        Ok(f) => Ok((f.oldest_record_ms, f.newest_record_ms)),
        Err(e) => {
            let variant = match &e {
                StoreError::Io(_) => "Io",
                StoreError::NotAManifest(..) => "NotAManifest",
                StoreError::Backend(_) => "Backend",
                other => panic!("manifest_facts answered a variant the walk never did: {other:?}"),
            };
            Err((variant.to_string(), e.to_string()))
        }
    }
}

fn seg(t0: &str, t1: &str) -> String {
    format!(r#"{{"key":"k","start_timestamp":{t0},"end_timestamp":{t1}}}"#)
}

/// **The streaming fold answers exactly what the `Value` walk answered**, body
/// by body, variant AND message — including which duplicate key wins, and
/// what `Value`'s own parse refuses (invalid UTF-8 and a lone surrogate inside
/// a string nobody reads, trailing bytes, depth).
///
/// KILLS: "the first duplicate wins", "merge every duplicate", "skip with
/// `IgnoredAny`" (the UTF-8 and surrogate bodies fold to a window), "a
/// non-object segment is ignored", "a float or a too-big integer is a
/// timestamp".
#[test]
fn the_streaming_window_answers_exactly_what_the_value_walk_answered() {
    let ok = seg("100", "200");
    let late = seg("50", "900");
    let deep = format!("{}{}", "[".repeat(200), "]".repeat(200));
    let mut bodies: Vec<(&str, Vec<u8>)> = vec![
        ("one segment", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{ok}]}}]}}]}}"#).into_bytes()),
        ("two topics", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{ok}]}}]}},{{"partitions":[{{"segments":[{late}]}}]}}]}}"#).into_bytes()),
        ("no topics key", br#"{"hello":"world"}"#.to_vec()),
        ("top-level array", br#"[{"topics":[]}]"#.to_vec()),
        ("top-level string", br#""topics""#.to_vec()),
        ("top-level null", b"null".to_vec()),
        ("topics a number", br#"{"topics":5}"#.to_vec()),
        ("topics an object", br#"{"topics":{"a":1}}"#.to_vec()),
        ("topics a string", br#"{"topics":"x"}"#.to_vec()),
        ("topics a boolean", br#"{"topics":true}"#.to_vec()),
        ("topics null", br#"{"topics":null}"#.to_vec()),
        ("empty topics", br#"{"topics":[]}"#.to_vec()),
        ("topic not an object", br#"{"topics":[5,[1],"x",null]}"#.to_vec()),
        ("partitions not an array", br#"{"topics":[{"partitions":{"segments":[]}}]}"#.to_vec()),
        ("segments not an array", br#"{"topics":[{"partitions":[{"segments":7}]}]}"#.to_vec()),
        ("segment not an object", br#"{"topics":[{"partitions":[{"segments":[5]}]}]}"#.to_vec()),
        ("segment an array", br#"{"topics":[{"partitions":[{"segments":[[1,2]]}]}]}"#.to_vec()),
        ("segment missing end", br#"{"topics":[{"partitions":[{"segments":[{"start_timestamp":1}]}]}]}"#.to_vec()),
        ("float timestamp", br#"{"topics":[{"partitions":[{"segments":[{"start_timestamp":1.0,"end_timestamp":2}]}]}]}"#.to_vec()),
        ("timestamp above i64", br#"{"topics":[{"partitions":[{"segments":[{"start_timestamp":9223372036854775808,"end_timestamp":2}]}]}]}"#.to_vec()),
        ("negative timestamps", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{}]}}]}}]}}"#, seg("-5", "-1")).into_bytes()),
        ("string timestamp", br#"{"topics":[{"partitions":[{"segments":[{"start_timestamp":"1","end_timestamp":2}]}]}]}"#.to_vec()),
        ("bad segment, later valid one", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{{"start_timestamp":1}},{ok}]}}]}}]}}"#).into_bytes()),
        ("duplicate topics, last bad", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{ok}]}}]}}],"topics":5}}"#).into_bytes()),
        ("duplicate topics, last good", format!(r#"{{"topics":5,"topics":[{{"partitions":[{{"segments":[{ok}]}}]}}]}}"#).into_bytes()),
        ("duplicate topics, last empty", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{ok}]}}]}}],"topics":[]}}"#).into_bytes()),
        ("duplicate partitions", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{{"start_timestamp":1}}]}}],"partitions":[{{"segments":[{late}]}}]}}]}}"#).into_bytes()),
        ("duplicate segments", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{late}],"segments":[{ok}]}}]}}]}}"#).into_bytes()),
        ("duplicate timestamp, last bad", br#"{"topics":[{"partitions":[{"segments":[{"start_timestamp":1,"end_timestamp":2,"start_timestamp":"x"}]}]}]}"#.to_vec()),
        ("duplicate timestamp, last good", br#"{"topics":[{"partitions":[{"segments":[{"start_timestamp":"x","end_timestamp":2,"start_timestamp":1}]}]}]}"#.to_vec()),
        ("escaped key", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{ok}]}}]}}]}}"#).into_bytes()),
        ("lone surrogate in an ignored string", format!(r#"{{"note":"\ud800","topics":[{{"partitions":[{{"segments":[{ok}]}}]}}]}}"#).into_bytes()),
        ("unknown fields everywhere", format!(r#"{{"backup_id":"b","x":[1,{{"y":null}}],"topics":[{{"name":"t","partitions":[{{"partition_id":0,"gaps":[],"segments":[{ok}]}}]}}]}}"#).into_bytes()),
        ("trailing bytes", format!(r#"{{"topics":[{{"partitions":[{{"segments":[{ok}]}}]}}]}} x"#).into_bytes()),
        ("truncated", br#"{"topics":[{"partitions":[{"segm"#.to_vec()),
        ("not JSON", b"not json".to_vec()),
        ("too deep", format!(r#"{{"note":{deep},"topics":[]}}"#).into_bytes()),
        ("number out of range", br#"{"note":1e400,"topics":[]}"#.to_vec()),
    ];
    // Invalid UTF-8 inside a string nothing reads.
    let mut bad_utf8 = br#"{"note":""#.to_vec();
    bad_utf8.extend_from_slice(&[0xff, 0xfe]);
    bad_utf8.extend_from_slice(
        format!(r#"","topics":[{{"partitions":[{{"segments":[{ok}]}}]}}]}}"#).as_bytes(),
    );
    bodies.push(("invalid UTF-8 in an ignored string", bad_utf8));

    let s = Store::in_memory("logweir/");
    let mut checked = 0;
    for (i, (label, body)) in bodies.iter().enumerate() {
        let key = format!("logweir/m{i}/manifest.json");
        put(&s, &key, body);
        let want = value_walk(&key, body);
        let got = streamed(&s, &key);
        assert_eq!(
            got, want,
            "{label}: the streaming fold and the Value walk disagree"
        );
        checked += 1;
    }
    assert_eq!(checked, bodies.len());
}

/// A manifest over the reader's cap is `TooLarge`, unread — the retention
/// report's `skipped` reason.
#[test]
fn a_manifest_over_the_cap_is_too_large() {
    let s = Store::in_memory("logweir/");
    let body = format!(
        r#"{{"topics":[{{"partitions":[{{"segments":[{}]}}]}}]}}"#,
        seg("1", "2")
    );
    put(&s, "logweir/b/manifest.json", body.as_bytes());
    let cap = u64::try_from(body.len()).unwrap() - 1;
    assert!(matches!(
        s.manifest_facts("logweir/b/manifest.json", cap),
        Err(StoreError::TooLarge { .. })
    ));
    assert!(s.manifest_facts("logweir/b/manifest.json", cap + 1).is_ok());
}

// ---------------------------------------------------------------- scaffolding

/// A scratch filesystem tree, removed when the row ends.
struct Tree {
    root: std::path::PathBuf,
}

impl Tree {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "logweir-fx31-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self { root }
    }

    fn handle(&self) -> Store {
        Store::read_only_from_url(&logweir_core::engine::StorageUrl::Filesystem {
            path: self.root.clone(),
        })
        .expect("a filesystem handle over an existing directory builds")
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

// ---------------------------------------------------------------- the guard

/// Every read call in production source, with the text of its arguments.
fn production_reads() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/logweir-store sits two levels under the workspace root")
        .to_path_buf();
    let patterns = [
        ".get_capped(",
        ".get_version_capped(",
        ".manifest_facts(",
        "access.get(",
        "access.get_with_version(",
        "access.get_version(",
        "archive.get(",
    ];
    let mut out = Vec::new();
    let mut stack = vec![root.join("crates")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("the tree lists") {
            let path = entry.expect("an entry").path();
            if path.is_dir() {
                // Production source only: `src/` trees, never `tests/`.
                if path
                    .file_name()
                    .is_some_and(|n| n != "tests" && n != "target")
                {
                    stack.push(path);
                }
                continue;
            }
            let rel = path
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .to_string();
            if !rel.contains("/src/") || path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("a source file reads");
            for pat in patterns {
                let mut from = 0;
                while let Some(at) = text[from..].find(pat) {
                    let open = from + at + pat.len();
                    let mut depth = 1usize;
                    let mut end = open;
                    for (i, c) in text[open..].char_indices() {
                        match c {
                            '(' => depth += 1,
                            ')' => {
                                depth -= 1;
                                if depth == 0 {
                                    end = open + i;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                    let line = text[..open].matches('\n').count() + 1;
                    out.push((format!("{rel}:{line}"), text[open..end].to_string()));
                    from = open;
                }
            }
        }
    }
    out
}

/// **Every production read names its cap from the table**, and none is
/// "far too large": each call's arguments name `caps::…`, the catalog walk's
/// own `CATALOG_DOCUMENT_READ_CAP`, or a `max_bytes` its own caller chose
/// (the evidence fetch's plan cap, the store's own pass-through), and none
/// names `u64::MAX`.
///
/// KILLS: "a cap far too large" at any production read site (a `u64::MAX`
/// or a literal in place of a named cap), including one a later change adds.
#[test]
fn every_production_read_names_a_cap_from_the_table() {
    let reads = production_reads();
    assert!(
        reads.len() >= 25,
        "the scan must see the production reads, found {}: {reads:?}",
        reads.len()
    );
    let named = ["caps::", "CATALOG_DOCUMENT_READ_CAP", "max_bytes"];
    let bad: Vec<&(String, String)> = reads
        .iter()
        .filter(|(_, args)| args.contains("u64::MAX") || !named.iter().any(|n| args.contains(n)))
        .collect();
    assert!(
        bad.is_empty(),
        "every production read names a cap from `logweir_store::caps` (or its caller's \
         `max_bytes`) and never `u64::MAX`: {bad:#?}"
    );
}
