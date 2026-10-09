#!/usr/bin/env python3
"""PROD-04.1: regenerate the consumer position corpus cases.

Two kinds of case, both under `e2e/fixtures/invariants/`:

* **Receipt cases** (`backup-receipt-index.json`, ids `receipt_1_5_*` and
  `consumer_positions_*`): a 1.5.0 receipt whose `consumer_positions` block
  breaks one of arms 22-27, walked like every other receipt case.
* **Positions document cases** (`consumer-positions-index.json`): a receipt
  and the positions document handed to both readers with
  `--consumer-positions`, breaking one of arms CP-1 to CP-14.

Why generated rather than hand-written: a document case needs a receipt that
binds THAT document's exact SHA-256 and length (arm CP-2), so a hand edit of a
document without its receipt would test CP-2 instead of the arm it names.
Every receipt here is the accepted base with one change; every document the
accepted document with one change.

The recorded `reason` is what `docs/verify_scorecard.py` returns. That is not
the authority — `crates/logweir/tests/two_reader_parity_receipt.rs` and
`two_reader_parity_positions.rs` hold the RUST reader to the same text, and
`crates/logweir-core/tests/backup_receipt.rs` asserts each Rust message in
full — so a reason this script records wrongly fails those, never passes.

Run: python3 scripts/fixtures/consumer_positions_corpus.py   (idempotent)
"""
import copy
import hashlib
import importlib.util
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
CORPUS = ROOT / "e2e" / "fixtures" / "invariants"


def verifier():
    spec = importlib.util.spec_from_file_location("verify_scorecard", ROOT / "docs/verify_scorecard.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


V = verifier()


def facts(p, marks=None, after=None, archived=None):
    v = {"partition": p, "observed": marks is not None}
    if marks:
        v["log_start"], v["high_watermark"] = marks
    if after:
        v["log_start_after"], v["high_watermark_after"] = after
    if archived:
        v["archived_first"], v["archived_last"] = archived
    return v


BASE = json.loads((CORPUS / "receipt_1_3_with_topic_configuration.json").read_text())
# The minor that defines the block, read from the verifier (which
# `test_the_consumer_positions_minor_is_the_rust_readers` holds to the Rust
# constant): a renumber at integration moves it, and this script is rerun.
SINCE = V.RECEIPT_CONSUMER_POSITIONS_SINCE_MINOR
BASE["format_version"] = f"1.{SINCE}.0"


def document():
    """The accepted positions document: `orders` has three partitions, the
    third added during the capture; `payments` one. `billing` and `audit` are
    captured."""
    return {
        "format_version": "1.0.0",
        "backup_id": BASE["backup_id"],
        "run_id": BASE["run_id"],
        "topics": {
            "orders": {
                "partitions": [
                    facts(0, (0, 20), (0, 24), (0, 23)),
                    facts(1, (5, 9), (5, 9), (5, 8)),
                    facts(2, None, (0, 3), (0, 2)),
                ],
                "changed_during_capture": False,
            },
            "payments": {
                "partitions": [facts(0, (0, 7), None, (0, 6))],
                "changed_during_capture": False,
            },
        },
        "groups": {
            "audit": {
                "positions": [
                    {"topic": "orders", "partition": 0, "status": "excluded", "position": 21,
                     "reason": "PositionBeyondEnd"},
                    {"topic": "orders", "partition": 1, "status": "captured", "position": 2,
                     "coverage": "beforeLogStart"},
                    {"topic": "orders", "partition": 2, "status": "notObserved",
                     "reason": "PartitionAddedDuringCapture"},
                    {"topic": "payments", "partition": 0, "status": "failed", "reason": "Unstable"},
                ],
                "no_committed_position": 0,
            },
            "billing": {
                "positions": [
                    {"topic": "orders", "partition": 0, "status": "captured", "position": 12,
                     "coverage": "withinArchive"},
                    {"topic": "orders", "partition": 2, "status": "notObserved",
                     "reason": "PartitionAddedDuringCapture"},
                    {"topic": "payments", "partition": 0, "status": "captured", "position": 7,
                     "coverage": "atArchiveEnd"},
                ],
                "no_committed_position": 1,
            },
        },
    }


def counts(related, not_related, never, beyond, failed, not_observed):
    return {"related": related, "not_related": not_related, "never_committed": never,
            "beyond_end": beyond, "failed": failed, "not_observed": not_observed}


def receipt():
    """The accepted receipt, NOT yet bound to a document."""
    r = copy.deepcopy(BASE)
    r["consumer_positions"] = {
        "observed_from": "2026-09-09T11:02:15Z",
        "observed_to": "2026-09-09T11:02:16Z",
        "listing": "complete",
        "document": {"key": "", "sha256": "", "bytes": 0},
        "groups": {
            "audit": {"outcome": "captured", "group_type": "consumer", "state": "Empty",
                      "listed_state": "Empty", "members": 0, "active": False,
                      "counts": counts(0, 1, 0, 1, 1, 1)},
            "billing": {"outcome": "captured", "group_type": "classic", "state": "Stable",
                        "listed_state": "Stable", "members": 2, "active": True,
                        "counts": counts(2, 0, 1, 0, 0, 1)},
            "gone": {"outcome": "excluded", "reason": "GroupNotFound"},
            "hidden": {"outcome": "failed", "reason": "NotVisibleToPrincipal"},
            "share-1": {"outcome": "excluded", "reason": "GroupTypeNotCaptured",
                        "group_type": "other"},
        },
    }
    return r


def dump(value) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()


def bind(r, doc_bytes):
    r["consumer_positions"]["document"] = {
        "key": f"logweir/backups/{r['backup_id']}/{r['run_id']}.consumer-positions.json",
        "sha256": "sha256:" + hashlib.sha256(doc_bytes).hexdigest(),
        "bytes": len(doc_bytes),
    }
    return r


def receipt_reason(r):
    return V._receipt_shape(r) or V.check_backup_receipt_invariants(r)


def document_reason(r, doc_bytes):
    problem = V._receipt_shape(r) or V.check_backup_receipt_invariants(r)
    assert problem == "", f"a document case's receipt must hold: {problem}"
    pd = json.loads(doc_bytes)
    return V._positions_document_shape(pd) or V.check_consumer_positions_document(r, doc_bytes, pd)


written = set()


def write(name, data: bytes):
    (CORPUS / name).write_bytes(data)
    written.add(name)


# --------------------------------------------------------------- receipt cases
ACCEPTED_DOC = "consumer_positions_document_accepted.json"
accepted_doc_bytes = dump(document())
write(ACCEPTED_DOC, accepted_doc_bytes)


def accepted():
    return bind(receipt(), accepted_doc_bytes)


def g(r, gid):
    return r["consumer_positions"]["groups"][gid]


def strip(r, gid, *names):
    for n in names:
        g(r, gid).pop(n, None)
    return r


RECEIPT_CASES = [
    ("receipt_1_5_with_consumer_positions", lambda r: r),
    ("consumer_positions_under_the_minor_before_it",
     lambda r: r.update(format_version=f"1.{SINCE - 1}.0") or r),
    ("consumer_positions_no_group",
     lambda r: r["consumer_positions"].update(groups={}) or r),
    ("consumer_positions_capture_ends_before_it_starts",
     lambda r: r["consumer_positions"].update(observed_to="2026-09-09T11:02:14.999Z") or r),
    ("consumer_positions_document_of_another_run",
     lambda r: r["consumer_positions"]["document"].update(
         key=f"logweir/backups/{r['backup_id']}/another-run.consumer-positions.json") or r),
    ("consumer_positions_document_digest_not_lowercase_hex",
     lambda r: r["consumer_positions"]["document"].update(
         sha256=r["consumer_positions"]["document"]["sha256"].upper().replace("SHA256:", "sha256:"))
     or r),
    ("consumer_positions_exclusion_with_a_failure_reason",
     lambda r: g(r, "gone").update(reason="NotVisibleToPrincipal") or r),
    ("consumer_positions_counts_on_a_failed_group",
     lambda r: g(r, "hidden").update(counts=counts(0, 0, 0, 0, 0, 0)) or r),
    ("consumer_positions_captured_without_active", lambda r: strip(r, "billing", "active")),
    ("consumer_positions_captured_without_members", lambda r: strip(r, "billing", "members")),
    ("consumer_positions_captured_without_listed_state",
     lambda r: strip(r, "billing", "listed_state")),
    ("consumer_positions_captured_without_counts", lambda r: strip(r, "billing", "counts")),
    ("consumer_positions_vanished_group_recorded_as_captured",
     lambda r: g(r, "audit").update(state="Dead", listed_state="Stable", active=True) or r),
    ("consumer_positions_rebalancing_group_called_inactive",
     lambda r: g(r, "audit").update(listed_state="PreparingRebalance") or r),
]

receipt_index = []
for case_id, mutate in RECEIPT_CASES:
    r = mutate(accepted())
    reason = receipt_reason(r)
    accept = reason == ""
    assert accept == (case_id == "receipt_1_5_with_consumer_positions"), (case_id, reason)
    write(f"{case_id}.json", dump(r))
    receipt_index.append({
        "id": case_id,
        "file": f"{case_id}.json",
        "rust_exit": 0 if accept else 4,
        "python_exit": 0 if accept else 1,
        "reason": reason,
        "arm": reason,
    })


# ------------------------------------------------------------ document cases
def doc_g(d, gid):
    return d["groups"][gid]


def regress(d):
    d["topics"]["orders"]["partitions"][1]["high_watermark_after"] = 8
    return d


def changed_payments(d):
    p = d["topics"]["payments"]["partitions"][0]
    p["log_start_after"], p["high_watermark_after"] = 0, 3
    d["topics"]["payments"]["changed_during_capture"] = True
    return d


def entry(d, gid, i, **changes):
    e = d["groups"][gid]["positions"][i]
    for k, v in changes.items():
        if v is None:
            e.pop(k, None)
        else:
            e[k] = v
    return d


def facts_of(d, topic, i, **changes):
    f = d["topics"][topic]["partitions"][i]
    for k, v in changes.items():
        if v is None:
            f.pop(k, None)
        else:
            f[k] = v
    return d


def unread_payments(d):
    d["topics"]["payments"]["partitions"] = []
    return d


def flag_payments(d):
    d["topics"]["payments"]["changed_during_capture"] = True
    return d


def hidden_reason(why):
    def apply(r):
        g(r, "hidden")["reason"] = why
        return r
    return apply


def billing_counts(r):
    g(r, "billing")["counts"] = counts(1, 1, 1, 0, 0, 1)
    return r


def no_block(_r):
    return json.loads((CORPUS / "receipt_1_3_with_topic_configuration.json").read_text())


def tamper(d):
    d["groups"]["billing"]["positions"][0]["position"] = 13
    return d


def same(x):
    return x


def drop_unobserved(d):
    b = d["groups"]["billing"]
    b["positions"].pop(1)
    b["no_committed_position"] = 2
    return d


# (id, document change, receipt change, bind the receipt to the changed document)
DOCUMENT_CASES = [
    ("positions_document_accepted", same, same, True),
    ("positions_document_for_a_receipt_that_selected_no_group", same, no_block, False),
    ("positions_document_tampered_after_signing", tamper, same, False),
    ("positions_document_of_another_run",
     lambda d: d.update(run_id="another-run") or d, same, True),
    ("positions_document_missing_a_named_topic",
     lambda d: d["topics"].pop("payments") and d, same, True),
    ("positions_document_partition_out_of_order",
     lambda d: facts_of(d, "orders", 1, partition=2), same, True),
    ("positions_document_mark_pair_half_recorded",
     lambda d: facts_of(d, "orders", 0, high_watermark=None), same, True),
    ("positions_document_unobserved_partition_with_marks",
     lambda d: facts_of(d, "orders", 2, log_start=0, high_watermark=3), same, True),
    ("positions_document_changed_flag_hides_a_regression", regress, same, True),
    ("positions_document_changed_flag_without_a_regression", flag_payments, same, True),
    ("positions_document_positions_for_a_failed_group",
     lambda d: d["groups"].update(hidden=copy.deepcopy(d["groups"]["billing"])) or d, same, True),
    ("positions_document_position_on_a_changed_topic", changed_payments, same, True),
    ("positions_document_blame_without_a_change", same,
     hidden_reason("GenerationChangedDuringCapture"), True),
    ("positions_document_capture_over_an_unread_topic", unread_payments, same, True),
    ("positions_document_partitions_not_read_with_every_topic_read", same,
     hidden_reason("PartitionsNotRead"), True),
    ("positions_document_never_committed_partition_dropped",
     lambda d: doc_g(d, "billing").update(no_committed_position=0) or d, same, True),
    ("positions_document_unobserved_partition_left_out", drop_unobserved, same, True),
    ("positions_document_no_committed_position_read_as_zero",
     lambda d: entry(d, "billing", 0, status="noCommittedPosition", position=0, coverage=None),
     same, True),
    ("positions_document_negative_position",
     lambda d: entry(d, "billing", 2, position=-1), same, True),
    ("positions_document_beyond_end_recorded_as_captured",
     lambda d: entry(d, "audit", 0, status="captured", reason=None, coverage="beyondArchive"),
     same, True),
    ("positions_document_coverage_the_facts_do_not_derive",
     lambda d: entry(d, "billing", 0, coverage="atArchiveEnd"), same, True),
    ("positions_document_counts_the_positions_do_not_give", same, billing_counts, True),
]

document_index = []
for case_id, change_doc, change_receipt, rebind in DOCUMENT_CASES:
    doc_bytes = dump(change_doc(document()))
    r = change_receipt(receipt())
    if "consumer_positions" in r:
        bind(r, doc_bytes if rebind else accepted_doc_bytes)
    reason = document_reason(r, doc_bytes)
    accept = reason == ""
    assert accept == (case_id == "positions_document_accepted"), (case_id, reason)
    doc_name = f"{case_id}.positions.json"
    receipt_name = f"{case_id}.receipt.json"
    write(doc_name, doc_bytes)
    write(receipt_name, dump(r))
    document_index.append({
        "id": case_id,
        "receipt": receipt_name,
        "document": doc_name,
        "rust_exit": 0 if accept else 4,
        "python_exit": 0 if accept else 1,
        "reason": reason,
        "arm": reason,
    })

# ------------------------------------------------------------------- indexes
index_path = CORPUS / "backup-receipt-index.json"
MINE = {case_id for case_id, _ in RECEIPT_CASES}
index = [e for e in json.loads(index_path.read_text())
         if not (e["id"].startswith("consumer_positions_") or e["id"] in MINE)]
index += receipt_index
index_path.write_text(json.dumps(index, indent=2, ensure_ascii=False) + "\n")
(CORPUS / "consumer-positions-index.json").write_text(
    json.dumps(document_index, indent=2, ensure_ascii=False) + "\n")

# Old PROD-04.1 case files nothing names any more.
for path in sorted(CORPUS.glob("consumer_positions_*.json")) + sorted(
        CORPUS.glob("positions_document_*.json")):
    if path.name not in written:
        path.unlink()
        print(f"removed {path.name}", file=sys.stderr)

print(f"{len(receipt_index)} receipt case(s), {len(document_index)} positions document case(s)")
