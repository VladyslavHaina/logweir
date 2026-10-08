#!/usr/bin/env python3
"""Check a kafka-backup manifest against the broker and one copied segment.

`scripts/e2e-seed.sh` calls this after its backup, with the manifest and one
segment it has just downloaded from the archive. It asserts COUNTS and HASHES,
never an exit status: an archive of empty topics, a truncated download and a
manifest with placeholder hashes all fail here.

    seed-manifest-check.py [--segment-sha256 required|optional] \
        MANIFEST BROKER_RECORDS SEGMENT_KEY SEGMENT_SHA256

--segment-sha256 required (the default) is the pinned engine's contract: every
segment carries a sha256 (written since kafka-backup 0.21), and the copied
segment's bytes match the digest the manifest records for it.

--segment-sha256 optional exists for engine-matrix rows BELOW the 0.21.0
full-drill floor (docs/support-matrix.md). Those engines never write a segment
digest, so demanding one refused every such row before a single test ran
(engine-matrix runs 34830737064, 35586616823 and 36413265594). It relaxes only
the ABSENCE of a digest: the record count must still equal the broker's, the
copied segment must still be listed, and a digest the manifest does carry must
still match the bytes. e2e-seed.sh refuses to refresh the tracked fixtures in
this mode, because those must come from a >=0.21 archive.

Extracted from an inline heredoc in e2e-seed.sh so that both modes have tests
that can fail (crates/logweir/tests/engine_matrix.rs).
"""
import argparse
import json
import sys


def check(manifest, broker_records, segment_key, segment_sha, mode):
    """Return the lines to print on success; raise SystemExit on a refusal."""
    segs = [s for t in manifest["topics"] for p in t["partitions"] for s in p["segments"]]
    if not segs:
        raise SystemExit("manifest lists no segments: the archive is empty")
    total = sum(s["record_count"] for s in segs)
    if total != broker_records:
        raise SystemExit(f"manifest holds {total} records but the broker holds {broker_records}")
    missing = [s["key"] for s in segs if not s.get("sha256")]
    if missing and mode == "required":
        raise SystemExit(f"segments with an empty sha256 (not a v0.21.0 manifest): {missing}")
    recorded = {s["key"]: s.get("sha256", "") for s in segs}
    if segment_key not in recorded:
        raise SystemExit(f"copied segment {segment_key} is not listed in the manifest")
    want = recorded[segment_key]
    if want and want != segment_sha:
        raise SystemExit(f"copied segment sha256 {segment_sha} != manifest's {want}")
    lines = []
    if missing:
        lines.append(
            f"    manifest: {len(manifest['topics'])} topics, {len(segs)} segments, "
            f"{total} records; {len(missing)} of {len(segs)} segments carry no sha256 "
            "(an engine below 0.21 writes none; --segment-sha256 optional)"
        )
    else:
        lines.append(
            f"    manifest: {len(manifest['topics'])} topics, {len(segs)} segments, "
            f"{total} records, every sha256 present"
        )
    lines.append(f"    segment:  {segment_key}")
    if want:
        lines.append(f"              sha256 {segment_sha} matches the manifest byte for byte")
    else:
        lines.append(f"              sha256 {segment_sha} (the manifest records none to compare)")
    return lines


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--segment-sha256", dest="mode", choices=["required", "optional"], default="required"
    )
    parser.add_argument("manifest")
    parser.add_argument("broker_records", type=int)
    parser.add_argument("segment_key")
    parser.add_argument("segment_digest")
    args = parser.parse_args(argv)
    with open(args.manifest, encoding="utf-8") as f:
        manifest = json.load(f)
    for line in check(manifest, args.broker_records, args.segment_key, args.segment_digest, args.mode):
        print(line)


if __name__ == "__main__":
    main(sys.argv[1:])
