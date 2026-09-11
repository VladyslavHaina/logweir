# Quickstart

Three paths. The first two prove the tool works on your laptop with no cloud
resources — one for the **backup-and-recover** path, one for the **drill**
path. The third runs a drill against a scratch cluster you already have.

If you only want to know what a scorecard means once someone hands you one, read
[verify-a-scorecard.md](verify-a-scorecard.md) instead.

---

## Path 1: `just mvp-demo` — backup, point-in-time restore, signed receipt

```bash
just e2e-up      # Kafka (KRaft) + MinIO, in docker compose
just mvp-demo
just e2e-down
```

This is the one command that exercises the whole CLI path, in order:

| step | what runs | what it proves |
|---|---|---|
| 1 | preflight | the stack is up and healthy, and `orders` and `payments` hold **zero** records |
| 2 | produce | 1000 records into each topic, end offsets read back off the broker |
| 3 | `logweir backup run` | **the product** takes the backup, behind phase −1's admission guard, and signs a backup receipt |
| 4 | two readers | `logweir drill verify --payload-type backup-receipt` **and** `python3 docs/verify_scorecard.py --payload-type backup-receipt` |
| 5 | `logweir drill approve` | the plan is approved by hash, with a **different key** from the one that signs the result |
| 6 | `logweir restore run` | `target.mode: newTopic` at a `restore.point_in_time`: the records land in topics that did not exist, and nothing is torn down |
| 7 | two readers again | over the scorecard, plus the evidence object keys an auditor would fetch |
| 8 | summary | the new topics, their record count off the broker, the measured RTO and RPO, the receipt key and the scorecard key |

**It wants a FRESH stack and says so.** Run it on a stack whose `orders` or
`payments` already hold records and it exits **1** at step 1, before producing
anything and before any archive exists, and tells you to run
`just e2e-down && just e2e-up`. That is not fussiness: a second backup into a
colliding `backup_id` does not accumulate — measured on this stack, a re-run
left the manifest describing 2048 records while the broker held 6000 — and a
partial archive that a restore reads from happily is exactly the false pass
this project keeps designing against.

It needs `docker`, `cargo`, `openssl`, `awk`, and `python3` with the
`cryptography` package. All of them are checked before anything starts.
Override the interpreter with `LOGWEIR_PYTHON=/path/to/python3`.

Everything it writes goes to `.demo/mvp/`, which is gitignored, and it sweeps
its own archive out of the shared bucket at both ends — including from a
`trap … EXIT`, so a run that dies mid-flight sweeps too.

**The two keys it mints prove integrity, not provenance.** `logweir`'s signer
mints silently against an empty key path, so a signature can be perfectly valid
over a key that nothing attests, that no roster names and that no auditor has
ever seen. The demo prints that warning rather than letting a green "verified"
imply more than it means. See [keys.md](keys.md).

**Every exit code is read directly.** No line in `scripts/mvp-demo.sh` whose
first word is `logweir`, `docker`, `just`, `kubectl` or `curl` contains an
unquoted `|`, and every one of them is followed by a line reading `$?` —
because `cmd | grep` reports *grep's* status, and the exit code is the
contract. That is checked on every commit by
`crates/logweir/tests/mvp_demo_lint.rs`, not by eye.

---

## Path 2: the drill demo

```bash
./scripts/demo.sh
```

**How it differs from Path 1.** This one takes its archive **from the
harness** — `scripts/e2e-seed.sh`, whose backup step is the pinned
`kafka-backup` engine invoked directly — and restores it into a **scratch**
cluster behind a marker topic, at no point in time. Everything it proves is
true, and none of it is the product taking a backup. Path 1 is the product's
own `backup run` and its `newTopic` point-in-time restore; this is the drill
path, which is what v0.1 shipped.

Needs `docker`, `cargo`, `openssl`, `shasum`, and `python3` with the
`cryptography` package. All five are checked before anything starts, so a missing one costs you
a second rather than four minutes. Override the interpreter with
`LOGWEIR_PYTHON=/path/to/python3`.

Tear down with `just e2e-down`.

**It leaves your working tree clean.** Everything the demo writes goes to
`.demo/` and `.engine/`, both gitignored, and the script checks `git status`
itself at the end and tells you the result.

The one thing worth knowing: `scripts/e2e-seed.sh` has a second job besides
seeding — by default it also refreshes two **checked-in** fixtures
(`e2e/fixtures/manifests/0.21.json` and
`e2e/fixtures/segments/upstream-0.21.0.kbak`) from the archive it just made.
Those bytes are not reproducible between runs, so refreshing them shows up as
two modified tracked files. That is a **maintainer** action — `just e2e-seed` —
and the demo opts out of it with `LOGWEIR_SEED_REFRESH_FIXTURES=0`. If you run
`scripts/e2e-seed.sh` directly and see two modified fixtures, that is why; set
the same variable to avoid it.

What it proves, in order: the engine is digest-pinned and extractable; a real
backup exists; the plan was approved by a **different key** than the one that
signs the result; the drill ran every phase against a real broker and a real
archive; and the scorecard verifies under **two independent verifiers**, one of
which shares no code with Logweir.

---

## Path 3: a scratch cluster you already have

### 0. What you need before you start

- An **existing** `kafka-backup` archive in an S3-compatible bucket. Logweir
  does not back up (`--from-cluster` is in v0.1's scope but its code lands in a
  follow-up — [ADR 0007](adr/0007-from-cluster-in-v0.1.md)).
- A **scratch** Kafka cluster you are willing to have topics created in. Not
  your production cluster, and not a cluster anything else depends on.
- A **marker topic** on that scratch cluster. This is v0.1's segregation proof:
  if it is absent, phase 0 refuses the drill with exit 3 before anything runs.
  Create it with any name you like and put that name in the spec.
- The `kafka-backup` binary of the pinned digest on `$PATH`, or the container
  image, which carries it.

### 1. Write the drill spec

Start from [`examples/drill.yaml`](../examples/drill.yaml). Three blocks need
your attention:

**`source`** — where the archive is. `prefix` is the backup id's prefix in the
bucket, not the bucket root; getting this wrong produces "the archive holds no
backup set at the configured source storage location", which is the correct
refusal and a confusing first experience.

**`sample`** — **the window is deployment-specific and the example's dates are
illustrative.**

```yaml
sample:
  window_start: "2026-09-04T00:00:00Z"   # a range your archive actually covers
  window_end:   "2026-09-05T00:00:00Z"
  records_per_partition: 25
  anchor: head
```

A window that overlaps no segment is **refused** — "a drill over an empty window
would report a pass that means nothing" — rather than reported as a pass over
nothing. `anchor: head` is the only value v0.1 implements; `tail` and `random`
are refused at phase 0, for reasons [stability.md](stability.md) sets out in
full (they are unsound here, not merely unimplemented).

**`objectives`** — what you are actually testing.

```yaml
objectives:
  rto_seconds: 900
  rpo_seconds: 300
  pass_rate: 1.0
```

`rto_seconds` is compared against `measured.rto_excluding_preflight_seconds`,
not against the wall clock — see
[formats/drill-scorecard.md](formats/drill-scorecard.md) for why.

### 2. Check before you run

```bash
logweir doctor \
  --spec drill.yaml \
  --allowed-clusters allowed-clusters.json \
  --approver-key approver.pub.pem
```

`doctor` checks credentials, the engine **version** and glibc floor, target
reachability, the marker topic and the approver key — before a drill is
attempted. It compares the engine's own `--version` output against the pinned
`0.21.0`; it does **not** compute or compare an image digest
(`third_party/kafka-backup-binary.digest` is quoted in the failure message and
nowhere else), so a green `ok engine version` line says the right version ran,
not that the right binary did. Add `--strict` to treat a check it could not perform (for example
`storage`, with no live bucket to list against) as a failure rather than a skip.

`allowed-clusters.json` must name the target cluster's own id:

```json
{ "allowed_cluster_ids": ["<the scratch cluster's id>"], "source_cluster_id": null }
```

**Derive it from the running broker rather than typing it.** An allowlist that
does not name this cluster is refused at phase 0 with exit 3, which is the guard
doing its job and looks like a bug the first time.

### 3. The two-key approval flow

The approval is a separate signed document. In a real deployment it is produced
on the **approver's** machine, with the **approver's** key, and only
`approval.json` and `approval.sig` cross the boundary.

```bash
# On the approver's machine, with the approver's PRIVATE key:
logweir drill approve \
  --spec drill.yaml \
  --key approver.pem \
  --approver sre-oncall@example.com \
  --ticket CHG-40881 \
  --out approval.json
```

That writes `approval.json` **and** `approval.sig` beside it — the only path
`drill run` looks for the sidecar. It needs nothing but the `logweir` binary:
no clone, no Rust toolchain, no `jq`, no `shasum`. In the container image it is
`docker run --rm -v "$PWD:/w" -w /w logweir:v0.1.0 drill approve …`.

`plan_hash` binds the approval to the **exact bytes** of the spec that will run.
Edit the spec after approving — including moving the sample window — and phase 1
refuses with exit 3, which is the point: **re-run `drill approve` on every spec
edit.** `openssl dgst` cannot produce this sidecar: the signature covers
PAE(payloadType, payload), never the bare bytes.

**If the approver key equals the signing key**, Logweir does not refuse; it
labels the scorecard `approval.self_attested: true`, and both verifiers print
`SELF-ATTESTED` — **because they derived it**, by comparing `approval.key_id`
against the key that verified the signature, not because the document said so.
A document whose claim disagrees with that derivation is refused (`drill
verify` exits 4, `verify_scorecard.py` exits 1). A self-attested run is not a
forgery, but it is a materially weaker governance signal, and an auditor is
entitled to treat it as a reason to seek corroboration.

### 4. Run the drill

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1
# Optional. `logweir doctor` and `logweir drill run` resolve the engine through
# ONE chain — $LOGWEIR_ENGINE_BIN, then ./.engine/kafka-backup, then
# /usr/local/bin/kafka-backup, then $PATH — so an engine on $PATH is enough for
# both and this line only pins a non-standard location. (They used to disagree:
# `doctor` searched $PATH and `drill run` did not, so a $PATH install passed
# `doctor` and then died mid-drill.)
export LOGWEIR_ENGINE_BIN=/usr/local/bin/kafka-backup
export LOGWEIR_ENGINE_VERSION=0.21.0
export LOGWEIR_ENGINE_DIGEST=sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317

logweir drill run \
  --spec drill.yaml \
  --approval approval.json --approver-key approver.pub.pem \
  --allowed-clusters allowed-clusters.json \
  --signing-key signer.pem \
  --out scorecard.json \
  --metrics-file /var/lib/node_exporter/textfile/logweir.prom \
  --triggered-by "quarterly DR drill"
```

Credentials come from `object_store`'s **own** chain (static keys, then web
identity / IRSA, ECS, EKS Pod Identity, IMDS). That is **not** the AWS SDK
chain: `~/.aws/credentials`, `AWS_PROFILE` and SSO are unsupported.

`LOGWEIR_ENGINE_VERSION` and `LOGWEIR_ENGINE_DIGEST` are mandatory. An empty
value is refused with exit 1: a signed scorecard must name the engine image that
produced the restore.

`--out` writes `scorecard.json` and its DSSE sidecar beside it as
`scorecard.sig`.

### 5. Read the exit code — it is the result

| Code | Meaning | What you do |
|---|---|---|
| 0 | Pass. | Archive the scorecard. |
| 1 | Operational — the drill could not be attempted or continued. **No artifact.** | Fix the environment and re-run. |
| **2** | **A drill ran, was measured, and did not pass. A scorecard WAS written and signed.** | **Read the scorecard.** This is the finding you scheduled the drill for. |
| 3 | Refused by a guard, before anything ran. | Fix the plan. Nothing happened. |
| 4 | Signing or lock proof failed — and nothing was uploaded. | Fix keys or bucket permissions. |

**1 and 2 are completely different things** and are easy to confuse under a
scheduler. Under Kubernetes they are actively hard to tell apart unless the Job
is shaped correctly — read [kubernetes.md](kubernetes.md) before scheduling one.

### 6. Read the scorecard

```bash
logweir drill show scorecard.json
```

The table is a fixed-width summary of a signed document. Below the fourteen rows
it prints the objectives, whether they were met, `integrity.partial_reason` and
the engine sub-report's own caveat — the qualifiers that most change how much a
result is worth and that the frozen layout does not carry.

Verify it, twice:

```bash
logweir drill verify --scorecard scorecard.json \
  --signature scorecard.sig --public-key signer.pub.pem

python3 docs/verify_scorecard.py scorecard.json scorecard.sig signer.pub.pem
```

The second shares no code with Logweir. If they ever disagree, the format is
broken, not merely one of the tools.

### 7. What landed in the bucket

Under your evidence prefix — Logweir writes under `logweir/` and nowhere else,
and refuses a prefix that is not exactly that:

```
logweir/drills/<run_id>.json           the signed scorecard
logweir/drills/<run_id>.sig            its DSSE sidecar
logweir/drills/<run_id>.receipt.json   what the store answered AFTER the put
logweir/drills/<run_id>.receipt.sig
logweir/drills/<run_id>.teardown.json  what was torn down
logweir/drills/<run_id>.teardown.sig
```

The receipt exists because the scorecard's four `evidence` fields describe facts
that only exist after the upload, and the scorecard is signed before it. Verify
it with the same tool:

```bash
python3 docs/verify_scorecard.py --payload-type receipt \
    <run_id>.receipt.json <run_id>.receipt.sig signer.pub.pem
```

Then check the binding by hand — `scorecard_sha256` in the receipt is the sha256
of the scorecard's exact stored bytes:

```bash
shasum -a 256 scorecard.json
```

A **missing** receipt means "no storage evidence was published for this run" —
never "the upload was not create-only".

---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
