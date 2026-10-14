# Quickstart

Two ways to see Logweir work in minutes, on a laptop, with no cluster: the local
demo, with or without a Rust toolchain, and a drill against an archive you
already have. Neither is an installation. To install, back up, restore and
recover on Kubernetes, follow [install.md](install.md). To read evidence
someone gives you, use [verify-a-scorecard.md](verify-a-scorecard.md).

---

## The supported path: install to disaster restore

The supported path is the Helm chart with its managed identity, the
`weirkeeper` controller and the console, and it lives in
[install.md](install.md):

1. [Install](install.md#install), with the console on, so restores are
   confirmed in one click and nobody handles a key.
2. [The first backup](install.md#the-first-backup): a connection, a
   destination, a schedule, *Run first backup now*.
3. [The first restore, and verifying it](install.md#the-first-restore-and-verifying-it).
4. [Approval modes](install.md#5f-approval-modes-confirm-two-person-strict-approvalpolicy):
   confirm, two-person or strict, per namespace.
5. [Recover onto a clean installation with the kit](install.md#recover-onto-a-clean-installation-with-the-kit).
6. [Upgrade and roll back](install.md#upgrade-crds-before-upgrading-the-controller).

[deploy/poc/](../deploy/poc/README.md) installs the same path end to end with
Traefik, a local CA, Dex and the shared console.

---

## Demos and the standalone CLI

The paths below run the CLI and the local compose stack. They are demos and
drills, not an installation. Paths 2 and 4, the v0.1 drill demo and the
`kubectl apply` laptop walk, were withdrawn in v1
([release notes](release-notes.md), item 76).

## Path 1: `just mvp-demo` — backup, point-in-time restore, signed receipt

```bash
just e2e-up      # Kafka (KRaft) + MinIO, in docker compose
just mvp-demo
just e2e-down
```

The one command that runs the whole CLI path, in order:

| step | what runs | what it proves |
|---|---|---|
| 1 | preflight | the stack is up and healthy, and `orders` and `payments` hold **zero** records |
| 2 | produce | 1000 records into each topic, end offsets read back off the broker |
| 3 | `logweir backup run` | **the product** takes the backup, behind phase −1's admission guard, and signs a backup receipt |
| 4 | two readers | `logweir drill verify --payload-type backup-receipt` **and** `python3 docs/verify_scorecard.py --payload-type backup-receipt` |
| 5 | `logweir drill approve` | the plan is approved by hash, with a **different key** from the one that signs the result |
| 6 | `logweir restore run` | `target.mode: newTopic` at a `restore.point_in_time`: records land in topics that did not exist, and nothing is torn down |
| 7 | two readers again | over the scorecard, plus the evidence object keys an auditor would fetch |
| 8 | summary | the new topics, their record count off the broker, the measured RTO and RPO, the receipt key and the scorecard key |

It refuses a stack whose `orders` or `payments` already hold records; restart
with `just e2e-down && just e2e-up`. It needs `docker`, `cargo`, `openssl`,
`awk` and `python3` with `cryptography` (`LOGWEIR_PYTHON` overrides the
interpreter), writes to the ignored `.demo/mvp/`, and removes its own archive
prefix on exit. Its approval and signing keys are freshly minted: they show
integrity, not provenance ([keys.md](keys.md)).

### No Rust toolchain: `just sandbox`

The same demo, with every `logweir` command and its engine run from the
**published runner image** instead of a local build:

```bash
just e2e-up
just sandbox docker.io/vladyslavhaina/logweir:<tag>   # default: …/logweir:latest
just e2e-down
```

It needs `docker`, `openssl`, `awk` and `python3` with `cryptography`, and no
`cargo`. `scripts/logweir-image.sh` runs the image's own `logweir` with the
current directory mounted at the same path, and points `localhost` inside the
container at the Docker host, where compose publishes the broker and MinIO. The
demo names the engine from the image's `/etc/logweir/engine-identity`. Pin the
image of your checkout's release, so the examples and the binary agree.

---

## Path 3: a scratch cluster you already have

A drill restores an **existing** archive into a scratch cluster, verifies it and
signs a scorecard.

### 0. What you need

- An archive in an S3-compatible bucket, written by `logweir backup run` or
  another compatible producer (integrated `--from-cluster` capture is deferred,
  [architecture](architecture.md#adr-0007-source-capture-scope)). An archive
  from an engine before 0.21, which includes anything `kafka-backup-operator`
  1.3.0 writes, has no segment digests and drills as `outcome: fail-integrity`,
  `integrity.result: partial`, exit 2 ([support-matrix.md](support-matrix.md)).
- A **scratch** Kafka cluster that nothing else depends on, with a **marker
  topic** of any name, named in the spec. Without it phase 0 refuses with exit 3.
- Logweir's build of the engine on `$PATH`: `docker create` the runner image and
  `docker cp <container>:/usr/local/bin/kafka-backup .`, or
  `scripts/engine-source.sh build`. `kafka-backup --version` prints
  `kafka-backup 0.23.3+logweir.4`.

### 1. Write the drill spec

Start from [`examples/drill.yaml`](../examples/drill.yaml). **`source.prefix`**
is the backup id's prefix in the bucket, not the bucket root; a wrong one is
refused with "the archive holds no backup set at the configured source storage
location". **`sample`**'s window is yours; the example's dates are illustrative:

```yaml
sample:
  window_start: "2026-09-04T00:00:00Z"   # a range your archive actually covers
  window_end:   "2026-09-05T00:00:00Z"
  records_per_partition: 25
  anchor: head
objectives:
  rto_seconds: 900
  rpo_seconds: 300
  pass_rate: 1.0
```

A window that overlaps no segment is refused ("a drill over an empty window
would report a pass that means nothing"). `anchor: head` is the only value;
`tail` and `random` are refused at phase 0 ([stability.md](stability.md) says
why). `rto_seconds` is compared with `measured.rto_excluding_preflight_seconds`
([formats/drill-scorecard.md](formats/drill-scorecard.md)).

### 2. Check before you run

```bash
logweir doctor --spec drill.yaml --allowed-clusters allowed-clusters.json \
  --approver-key approver.pub.pem
```

`doctor` checks credentials, the engine **version** and glibc floor, target
reachability, the marker topic and the approver key. It compares the engine's
`--version` with `0.23.3+logweir.4` as a whole token (`0.23.3+logweir.3` or
`0.23.3` is a mismatch; OSO's `0.23.3` passes only as the declared rollback) and
refuses an engine that is not the one the run would sign. It computes no digest:
a green `ok engine version` says the right version ran, not the right binary.
`--strict` turns a check it could not perform into a failure.
`allowed-clusters.json` names the scratch cluster's own id, read from the
running broker; an allowlist without it is refused at phase 0 with exit 3:

```json
{ "allowed_cluster_ids": ["<the scratch cluster's id>"], "source_cluster_id": null }
```

### 3. The two-key approval

On the **approver's** machine, with the approver's private key; only
`approval.json` and `approval.sig` cross the boundary:

```bash
logweir drill approve --spec drill.yaml --key approver.pem \
  --approver sre-oncall@example.com --ticket CHG-40881 --out approval.json
```

It needs only the `logweir` binary (in the image: `docker run --rm -v
"$PWD:/w" -w /w logweir:v0.1.0 drill approve …`). `plan_hash` binds the
approval to the spec's exact bytes, so **re-run `drill approve` on every spec
edit**; phase 1 refuses an edited spec with exit 3. `openssl dgst` cannot make
this sidecar: the signature covers PAE(payloadType, payload). If the approver
key is the signing key, the scorecard says `approval.self_attested: true` and
both verifiers print `SELF-ATTESTED`, because they derive it; a document whose
claim disagrees is refused (`drill verify` exit 4, `verify_scorecard.py` exit 1).

### 4. Run the drill

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1
# Optional engine override. Both doctor and drill run search LOGWEIR_ENGINE_BIN,
# ./.engine/kafka-backup, /usr/local/bin/kafka-backup, then PATH.
export LOGWEIR_ENGINE_BIN=/usr/local/bin/kafka-backup
export LOGWEIR_ENGINE_VERSION=0.23.3+logweir.4
export LOGWEIR_ENGINE_DIGEST=sha256:c8b6fee435621f63672e8ec25f893a9774775e6099dec95e09e7aae5474f9662

logweir drill run --spec drill.yaml \
  --approval approval.json --approver-key approver.pub.pem \
  --allowed-clusters allowed-clusters.json --signing-key signer.pem \
  --out scorecard.json --metrics-file /var/lib/node_exporter/textfile/logweir.prom \
  --triggered-by "quarterly DR drill"
```

Credentials come from `object_store`'s own chain (static keys, web identity /
IRSA, ECS, EKS Pod Identity, IMDS), not the AWS SDK's: `~/.aws/credentials`,
`AWS_PROFILE` and SSO are unsupported. `LOGWEIR_ENGINE_VERSION` and
`LOGWEIR_ENGINE_DIGEST` are mandatory outside the runner image (empty is exit
1): they are Logweir's build (`third_party/kafka-backup-build.env`), and inside
the image `/etc/logweir/engine-identity` is what is signed. `--out` writes
`scorecard.json` and `scorecard.sig`.

### 5. The exit code is the result

| Code | Meaning | What you do |
|---|---|---|
| 0 | Pass. | Archive the scorecard. |
| 1 | Operational: the drill could not be attempted or continued. **No artifact.** | Fix the environment and re-run. |
| **2** | **A drill ran, was measured, and did not pass. A scorecard WAS written and signed.** | **Read the scorecard.** This is the finding you scheduled the drill for. |
| 3 | Refused by a guard, before anything ran. | Fix the plan. Nothing happened. |
| 4 | Signing or lock proof failed, and nothing was uploaded. | Fix keys or bucket permissions. |

1 and 2 are different things; under Kubernetes they are hard to tell apart
unless the Job is shaped correctly ([kubernetes.md](kubernetes.md)).

### 6. Read and verify the scorecard

```bash
logweir drill show scorecard.json
logweir drill verify --scorecard scorecard.json \
  --signature scorecard.sig --public-key signer.pub.pem
python3 docs/verify_scorecard.py scorecard.json scorecard.sig signer.pub.pem
```

`drill show` prints the fourteen rows, then the objectives, whether they were
met, `integrity.partial_reason` and the engine sub-report's caveat. The Python
reader shares no code with Logweir: if the two disagree, the format is broken.
Logweir writes under the evidence prefix `logweir/` and refuses any other:

```
logweir/drills/<run_id>.json           the signed scorecard
logweir/drills/<run_id>.sig            its DSSE sidecar
logweir/drills/<run_id>.receipt.json   what the store answered AFTER the put
logweir/drills/<run_id>.receipt.sig
logweir/drills/<run_id>.teardown.json  what was torn down
logweir/drills/<run_id>.teardown.sig
```

The receipt exists because the scorecard's `evidence` fields describe the
upload, which happens after signing. Verify it and its binding:

```bash
python3 docs/verify_scorecard.py --payload-type receipt \
    <run_id>.receipt.json <run_id>.receipt.sig signer.pub.pem
shasum -a 256 scorecard.json   # equals the receipt's scorecard_sha256
```

A **missing** receipt means "no storage evidence was published for this run",
never "the upload was not create-only".

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
