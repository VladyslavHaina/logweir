# The protection event format, field by field

`application/vnd.logweir.protection-event+json;version=1.0.0`

A protection event is what PLAT-14.2's protection controller writes when a policy's alert ledger
changes state, and what `logweir notify deliver` posts to the configured sinks. The authority for
its shape is the Rust type `logweir::notify::ProtectionEvent`
([`crates/logweir/src/notify.rs`](../../crates/logweir/src/notify.rs)), driven over this document's
own worked example by `crates/logweir/tests/notify_deliver.rs`. The subcommand's environment, stdout
and exit-code contract lives in [stability.md](../stability.md).

## It is UNSIGNED, and that is stated rather than implied

A notification is not evidence and is never rendered as one. The signed documents in this product
are the drill scorecard, the backup receipt and the teardown attestation, each with a detached DSSE
sidecar and a documented verification path; this one has none of that. It is a control message
between two components of one release — the controller writes it into an immutable ConfigMap, the
delivery Job reads it from a projected volume — and nothing downstream may treat its contents as
verified facts about an archive. What it says about verification, it says in one field, and that
field is the point of the next section.

## `verification_scope` is `sampled`, `degraded` or `none` — never `complete`

This is the load-bearing field. Its value travels into a PagerDuty incident and a Slack channel,
where it is read by someone deciding, during an incident, whether an archive can be trusted.
**Logweir verifies a sample.** It compares a selected window of records byte-for-byte; it does not
read an archive end to end and it never has.

- `sampled` — a sampled per-record comparison ran.
- `degraded` — `integrityLevel: consume-only`: records were read back but not compared
  byte-for-byte.
- `none` — no record check ran at all.

There is no fourth value. `"verification_scope": "complete"` is a **parse failure**, not a value a
reviewer has to notice, because the Rust type is a three-variant enum and the document is parsed
with `deny_unknown_fields`. Beyond that, every body this product is about to POST is scanned for a
fixed list of exhaustive-verification claims and is **not sent** if one is found — including a claim
arriving through the controller's free-text `summary`, which reaches a PagerDuty incident title
verbatim.

The word `exhaustive` is forbidden in a notification body even inside a denial. A denial and a claim
differ by one word, and these channels truncate: PagerDuty clips an incident summary, Slack collapses
a long message behind "show more", and a body gets pasted into a ticket by hand. A sentence that
survives clipping as "…an exhaustive comparison" would be the product's one unrecoverable lie,
delivered by its own disclaimer. So the disclaimers say what Logweir *did* — a sample was checked,
never the whole archive — and do not reach for the word they are denying.

## Reading rules a consumer must honour

1. **`format_version` is semver and its major must be `1`.** A document of another major is refused
   naming both versions, *before* its shape is examined, so a reader is sent to the upgrade they
   need rather than to a field name they will not find.
2. **Unknown fields are REFUSED, not ignored.** This is a deliberate departure from
   [stability.md](../stability.md)'s general format policy, which has a reader ignore unknown fields
   on a matching major. That policy is about signed, archival documents read years later by
   something that was not there when they were written. This is neither: it is a message passed
   between a controller and a Job whose image the same chart pins, and what a lenient reader would
   silently drop is an alert detail an on-call responder then never learns. The cost is stated
   rather than hidden — a minor bump that adds a field needs the runner image upgraded alongside the
   controller, which the chart already does, because it pins both.
3. **`last_available_point` is optional and its absence is the fact.** A `health: Unprotected`
   policy has no available recovery point. Requiring the field would make the one event that matters
   most unpublishable; inventing one would print a recovery point that does not exist into an
   incident.
4. **The document is at most 1 MiB.** It arrives as a ConfigMap key and the API server caps a
   ConfigMap there, so a larger file is not a protection event by construction.

## The fields

| field | type | meaning |
|---|---|---|
| `format_version` | string | semver; major `1` |
| `event_id` | string | `sha256` of `policyUID\|alertKey\|transition`, as the controller computed it. Opaque to the deliverer — it is carried so a sink can correlate a POST with the ConfigMap |
| `policy.namespace` / `policy.name` / `policy.uid` | string | which `ProtectionPolicy` this is about. The UID is the half of the dedup key that survives a rename |
| `alert.key` | string | the dedup key — see below |
| `alert.kind` | enum | `BackupFailure`, `Staleness`, `ArchiveUnavailable`, `RehearsalFailure`, `RecoveryCompleted` |
| `alert.action` | enum | `trigger` or `resolve` |
| `alert.transition` | integer | how many times this key has changed state |
| `health` | enum | `Healthy`, `AtRisk`, `Stale`, `Unprotected`, `Unknown` |
| `summary` | string | one controller-authored sentence for a human; reaches an incident title verbatim |
| `last_available_point` | object or absent | `point_id`, `recovery_point_at`, `age_seconds`, `evidence` |
| `consecutive_failed_runs` | integer | consecutive failed **slots**; a retry chain counts once |
| `missed_slots` | integer | missed schedule slots |
| `verification_scope` | enum | `sampled`, `degraded`, `none` |
| `details_route` | string | a UI fragment route, not an absolute URL: the controller does not know the installation's external hostname, and inventing one would put a dead link in an incident |
| `generated_at` | RFC 3339 | when the controller generated the event |

`last_available_point.recovery_point_at` is the **capture start** of the newest available point, not
its finish and not the newest archived record instant — the freshness definition an idle topic does
not defeat. `age_seconds` is carried rather than recomputed at delivery: a Job that recomputed it
would report the age at *delivery* time, which drifts from the age the health decision was made on
by however long the Job waited to be scheduled.

## The dedup key

One open alert per `(policy, kind)`, and the key is stable:

```
logweir-protection-<policyUID>-<kind>
```

PagerDuty gets `trigger` on Open and `resolve` on Resolved under that exact string, so a key that
changed between the two would leave an incident open forever. `RecoveryCompleted` is the exception:
it keys on the **Restore UID** rather than on the policy, because a policy's points can be restored
many times and each of those is its own completed recovery with its own topic names and counts.

The prefix keeps the family apart from the drill families (`logweir-drill-…`), so a protection
`resolve` can never close a drill's open page, and a UID that identifies nothing collapses to
`unknown` rather than to `logweir-protection--Staleness` — a key that would be one incident per kind
across every policy in the cluster, where one team's resolve closes another team's page.

**The controller is the authority on this value.** `logweir notify deliver` posts the key it is
given and never recomputes it; a key that does not match the `(policy, kind)` rule is reported on a
named log line and delivered anyway, because a mis-keyed page still reaches a human who can act on
the incident and a refused one reaches nobody at all.

## `RecoveryCompleted` is webhook and Slack only

It is informational and auto-resolves immediately. A PagerDuty incident under it would be opened and
closed in the same breath — a page for something that went *right*, at 03:00, with nothing for the
responder to do. A configured routing key is therefore not a configured sink for this kind and
prints no `notify-result=pagerduty:` line.

## A worked example

```json
{"format_version":"1.0.0","event_id":"sha256:0123456789abcdef",
 "policy":{"namespace":"team-a","name":"orders-prod","uid":"11111111-2222-3333-4444-555555555555"},
 "alert":{"key":"logweir-protection-11111111-2222-3333-4444-555555555555-Staleness",
          "kind":"Staleness","action":"trigger","transition":3},
 "health":"Stale",
 "summary":"orders-prod: newest available recovery point is 31h old (objective 26h)",
 "last_available_point":{"point_id":"lwp1-aaa","recovery_point_at":"2026-09-15T01:00:00Z",
                         "age_seconds":111600,"evidence":"Valid"},
 "consecutive_failed_runs":2,"missed_slots":1,
 "verification_scope":"sampled",
 "details_route":"#/protection?ns=team-a&name=orders-prod",
 "generated_at":"2026-09-16T08:00:00Z"}
```

## What each sink receives

- **`webhook`** — `{"media_type": …, "event": <the document, re-serialized>}`. Re-serialized from
  the parsed type and never passed through as the bytes that arrived, so nothing unrecognised can
  reach a sink that might render it.
- **`slack`** — `{"text": …}` and nothing else. A Slack incoming webhook answers `invalid_payload`
  to a JSON body with no `text`, `blocks` or `attachments`, so posting the document verbatim would
  make every Slack delivery a failure for a channel that is working perfectly.
- **`pagerduty`** — an Events v2 enqueue whose `event_action` is `alert.action`, whose `dedup_key` is
  `alert.key`, whose `payload.source` is `<namespace>/<name>`, and whose `payload.custom_details` is
  the whole document. `payload.severity` is `critical` for `Stale` and `Unprotected` and `warning`
  otherwise — coarse on purpose, because a severity scale nobody can predict is a severity scale
  nobody routes on.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
