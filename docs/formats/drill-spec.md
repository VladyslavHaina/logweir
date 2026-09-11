# The drill spec: `name` and `notifications`

**This is not yet a complete drill-spec reference.** It documents exactly two
things — the top-level `name` key and the `notifications` block — because those
are what Task 14 created and changed. Every other key of a drill spec is
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
