# The drill spec: `name`, `source.point` and `notifications`

**This is not yet a complete drill-spec reference.** It documents exactly three
things — the top-level `name` key, `source.point`, and the `notifications`
block — because those are what Task 14 and decision D3 created and changed. Every other key of a drill spec is
described today only by the commented example at
[`examples/drill.yaml`](../../examples/drill.yaml) and by
[`crates/logweir-core/src/spec.rs`](../../crates/logweir-core/src/spec.rs). A
full reference is a separate piece of work; a partial document that says so is
more use than none, and less use than one that pretends to be complete.

Every key on this page is **snake_case**, matching the rest of the spec
(`pagerduty_routing_key`, `slack_webhook`, `engine_overrides`).

---

## `name`

```yaml
name: nightly-orders-drill
```

Optional. This drill's own stable identity.

It exists so that two drill specs pointed at **one** scratch cluster do not
share a PagerDuty incident. The alert dedup key is
`logweir-drill-{name}-{cluster_id}`; when `name` is absent it falls back to the
first 12 hex characters of the approval's `plan_hash` — a sha256 over the
approved plan bytes, distinct per spec and stable across re-runs of that spec.
Should that hash ever be too short to supply 12 characters, the key uses
`unnamed` rather than an empty identity, so it can never collapse back to
`logweir-drill-{cluster_id}` — one incident per cluster is the defect this key
exists to fix.

Set it. The fallback is correct but opaque, and the operational-failure route
(below) has no cluster id to fall back on at all: a spec with no `name` reports
every "logweir could not run this drill" under the single key
`logweir-drill-unnamed-preflight`, so every unnamed spec on the install shares
one incident.

`name` is **spec-side only**. It is never written to a scorecard. An
artifact-side drill identity is backlog **T1-8**, assigned to decision **O16**
with default *not funded*; see
[`docs/stability.md`](../stability.md#known-limitations-of-v01) for the
residual.

---

## `source.point` (execution contract v2)

```yaml
source:
  storage: {backend: s3, bucket: recovery, prefix: archive/}
  backup: nightly-7
  topics: [orders]
  point:
    point_id: lwp1-3f2a91c74b8e05d6a1f0c2b3948e7d15
    receipt_key: logweir/backups/nightly-7/01J….receipt.json
    receipt_sha256: sha256:…
    manifest_sha256: sha256:…
```

Optional. **The recovery point this plan is bound to** (decision D3 §5.5).
Absent means exactly what it meant before this block existed: the archive set
is chosen by `source.backup` (`latestCompleted` or a pinned backup id) and no
binding is checked. Every plan written before this block existed therefore
still loads, still verifies byte for byte, and still runs unchanged.

**Why it is in the plan and not in the Job's environment.** The environment is
the controller's word for it; the plan is what the approver signed. Binding the
point into plan bytes means the approval covers *which archive object this
restore recovers from* — and the disaster path (PLAT-15.2: a fresh
installation, no `Backup` CR anywhere, only a bucket) has nothing else to bind
to.

**What the runner does with it, before any data-plane work.** Before a broker
client is constructed and before anything is written, the runner:

1. reads `receipt_key` from `source.storage` through a **read-only** handle;
2. checks `sha256(receipt bytes) == receipt_sha256`;
3. re-derives the point identity from those bytes — `lwp1-` plus the first 32
   lowercase hex characters of the same digest — and checks it equals
   `point_id`. The identity is content-derived, so it is never *believed*: a
   point id that had to be taken on trust would be a label anyone could
   relabel;
4. checks the receipt's own `archive.manifest_sha256` equals `manifest_sha256`;
5. verifies the receipt's **signature** (its `.sig` sidecar, DSSE, payload
   type `application/vnd.logweir.backup-receipt+json;version=1.0.0`) against the
   evidence-signing keyring passed as `--evidence-keys`, and judges the key
   that verified with `logweir_core::trust::decide` for `EvidenceSigning` at
   the receipt's own `finished_at` (D3 §5.5 step 6);
6. reads the manifest the receipt names and checks its bytes hash to the same
   value. Steps 2–4 prove the plan and the receipt agree; step 5 proves an
   installation this one trusts wrote the receipt; this one proves the
   *archive* does.

A digest or identity mismatch is **exit 3**, with `PointBindingMismatch` at the
start of the refusal message — the tampered-bundle case, moved to the archive.
A signature fault is **exit 3** with `PointUntrusted`: no keyring, a keyring
holding no key, a receipt with no sidecar, a sidecar that does not parse, a
signature no key in the keyring verifies, or a key the keyring's lifecycle
refuses for this receipt. A receipt or manifest that is **missing or
unreadable** is **exit 1**: the archive did not answer, and that may be a
rotated credential or a briefly unavailable bucket, so telling an operator to
change an approved document would be the wrong repair. See
[`docs/stability.md`](../stability.md) for the whole stdout and exit contract.

**The evidence keyring (`--evidence-keys`).** Inside a cluster the controller
renders it into the Job's approval bundle as `evidence-keys.json` and pins its
digest (`LOGWEIR_EXECUTION_EVIDENCE_KEYS_SHA256`); it carries every key of the
namespace's resolved trust whose public half parses, WITH its lifecycle:

```json
{
  "formatVersion": "1.0.0",
  "keys": [{
    "publicKeyPem": "-----BEGIN PUBLIC KEY-----\n…\n-----END PUBLIC KEY-----\n",
    "trust": {
      "key_id": "f27c7f51…", "principal_id": "install:f27c7f51…",
      "usages": ["EvidenceSigning"],
      "not_before": "2026-01-01T00:00:00Z", "not_after": "2027-01-01T00:00:00Z",
      "state": "Active", "retired_at": null, "revoked_at": null,
      "revocation_reason": null, "revocation_effective_from": null
    }
  }]
}
```

The lifecycle is in the file because the runner, not the controller, is the one
that has read the receipt's claimed signing time. So a key **retired** after
the receipt was written still verifies it (D3 §7.4: a retired key keeps what it
signed before `retired_at`), a key **revoked for compromise** verifies nothing
(a restore Job holds no earlier independent observation of the receipt), a key
revoked as `Superseded`/`Unspecified` is a retirement at
`revocation_effective_from`, and a key without `EvidenceSigning` refuses as
`KeyUsageMismatch`. A standalone `logweir restore run` of a point-bound plan
(the disaster path, with no cluster) needs the same file, written by hand from
the installation's public evidence-signing keys.

A plan carrying this block requires **execution contract v2**, and that is
enforced: a v1 invocation carrying `source.point` is a post-rollout Restore
wearing an old version number, and it is refused by name with exit 3 before any
data-plane work. Let the in-flight legacy Restore finish (or delete it) and
create the new one; a legacy object is not upgraded in place.

---

## `notifications`

```yaml
notifications:
  webhooks:
    - https://example.internal/logweir-hook
  slack_webhook: https://hooks.slack.com/services/T0.../B0.../XXXXXXXX
  pagerduty_routing_key: R0...
  pagerduty_endpoint: https://events.eu.pagerduty.com/v2/enqueue
```

The whole block is optional, and so is every key in it. Absent means "notify
nobody"; it is never an error.

### `webhooks` (list of URLs, default empty) and `slack_webhook` (URL)

The **scorecard-summary** route. Each receives one JSON summary of a completed
drill — outcome, measured RTO and RPO, integrity level and result, whether the
approval was self-attested, and the list of redacted paths.

These fire **only when a scorecard exists** — a drill that ran to completion,
whether it passed or not. They are not used for the operational-failure route
below, because that route has no scorecard, and a scorecard-shaped body with no
scorecard behind it is how a dashboard starts reporting drills that never ran.

Every transport failure is logged and swallowed. A webhook being down never
changes a drill's exit code (Global Constraint 11).

### `pagerduty_routing_key` (string)

The PagerDuty **Events v2 integration routing key**. Present means the
PagerDuty route is on; absent means it is off. There is no other switch.

Two families of event are sent, under **two different dedup keys**:

| When | `event_action` | `dedup_key` | `severity` |
|---|---|---|---|
| The drill passed (exit 0) | `resolve` | `logweir-drill-{name}-{cluster_id}` | `warning` |
| The drill ran and did not pass (exit 2) | `trigger` | `logweir-drill-{name}-{cluster_id}` | `warning` |
| Operational failure, no artifact (exit 1) | `trigger` | `logweir-drill-{name}-preflight` | `critical` |
| A guard refused the plan (exit 3) | `trigger` | `logweir-drill-{name}-preflight` | `warning` |
| Result unattested, nothing uploaded (exit 4) | `trigger` | `logweir-drill-{name}-preflight` | `critical` |

The two keys are separate deliberately. "This drill ran and did not pass" and
"logweir could not run this drill at all" are different facts, and a later
passing run's `resolve` must not silently close an operational-failure incident
nobody has looked at.

### `pagerduty_endpoint` (URL, default US region)

Which PagerDuty **service region** the events go to.

Absent means `https://events.pagerduty.com/v2/enqueue` — the **US** region, the
behaviour of every earlier version. An account on the **EU** service region
must set:

```yaml
  pagerduty_endpoint: https://events.eu.pagerduty.com/v2/enqueue
```

Only `https://` is accepted. Anything else is **refused before a request is
made**, because the routing key travels in the request body and plaintext HTTP
would put a bearer credential on the wire. A refusal is not silent: it logs at
WARN with the message `pagerduty alert NOT delivered`, the `dedup_key` of the
incident that did **not** open, the run id, and the reason. The same line is
emitted when a request is attempted and fails, so "no page arrived" is always
greppable and never has to be inferred from the absence of anything.

A refused or failed enqueue never changes the exit code (Global Constraint 11),
and there is no retry.

---

## Credentials in this block

**A Slack incoming-webhook URL and a PagerDuty routing key are bearer
credentials, and today they live in the plaintext drill spec.** Whoever holds
one can post as that integration. When the spec is delivered to Kubernetes it
is delivered as a **ConfigMap**, which means every subject with `get
configmaps` in the drill namespace can read them; a ConfigMap is not a Secret
and is not encrypted at rest by default.

This is recorded, not fixed. Moving these three keys to a Secret reference is
decision **O17**, default *not funded*. Until it is funded:

- Treat the drill spec itself as sensitive, and scope RBAC on the drill
  namespace as you would for a Secret.
- Prefer a webhook or routing key scoped narrowly enough that its disclosure is
  a rotation and not an incident.

`pagerduty_endpoint` is **not** a credential — but it is free-form input, and
that is a different thing. Logweir reduces it to `scheme://host/…` wherever it
reaches a display surface: the WARN line above and `Debug` output both. The
host is what says *which region*, which is the whole reason the key exists, so
nothing diagnostic is lost; what is dropped is the userinfo, path and query,
which is where a token lives in a URL somebody pasted. The other three keys are
redacted more strongly still — presence is reported, values never are.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
