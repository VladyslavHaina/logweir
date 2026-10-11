# The protection event format, field by field

`application/vnd.logweir.protection-event+json;version=1.0.0`

**No longer written (release notes, item 74).** Alert delivery is withdrawn, and with it the
controller that wrote this document and `logweir notify deliver`, which read it. The fields below
are kept as the format's reading rules. A protection event was what PLAT-14.2's protection
controller wrote, into an immutable ConfigMap, when a policy's alert ledger changed state.

## It is UNSIGNED

A notification is not evidence and is never rendered as one. The signed documents in this product
each carry a detached DSSE sidecar and a documented verification path; this one has none of that.
It was a control message between two components of one release, and nothing downstream may treat
its contents as verified facts about an archive.

## `verification_scope` is `sampled`, `degraded` or `none` — never `complete`

**It describes the policy's newest available point, and that is a sample.** The controller
published `sampled` for a point whose evidence verified, by the sampled check every point gets; it
did not read an archive end to end to raise a protection event. A RESTORE can ask for complete
coverage (PROD-08.1a), and reports what it covered on the `Restore` and in the product API's
`verificationScope.coverage` — never in this field.

- `sampled` — a sampled per-record comparison ran.
- `degraded` — `integrityLevel: consume-only`: records were read back but not compared
  byte-for-byte.
- `none` — no record check ran at all.

There is no fourth value. `"verification_scope": "complete"` is a **parse failure**: the Rust type
is a three-variant enum and the document is parsed with `deny_unknown_fields`.

## Reading rules a consumer must honour

1. **`format_version` is semver and its major must be `1`.** A document of another major is refused
   naming both versions, *before* its shape is examined.
2. **Unknown fields are REFUSED, not ignored.** This is a deliberate departure from
   [stability.md](../stability.md#format-policy)'s general format policy, which has a reader ignore
   unknown fields on a matching major. That policy is about signed, archival documents read years
   later; this was a message between a controller and a Job whose image the same chart pinned, so a
   minor bump that adds a field needs the runner image upgraded alongside the controller.
3. **`last_available_point` is optional and its absence is the fact.** A `health: Unprotected`
   policy has no available recovery point; an invented one would print a recovery point that does
   not exist.
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
| `summary` | string | one controller-authored sentence for a human |
| `last_available_point` | object or absent | `point_id`, `recovery_point_at`, `age_seconds`, `evidence` |
| `consecutive_failed_runs` | integer | consecutive failed **slots**; a retry chain counts once |
| `missed_slots` | integer | missed schedule slots |
| `verification_scope` | enum | `sampled`, `degraded`, `none` |
| `details_route` | string | a UI fragment route, not an absolute URL: the controller does not know the installation's external hostname |
| `generated_at` | RFC 3339 | when the controller generated the event |

`last_available_point.recovery_point_at` is the **capture start** of the newest available point, not
its finish and not the newest archived record instant — the freshness definition an idle topic does
not defeat. `age_seconds` is the age the health decision was made on, carried rather than
recomputed.

## The dedup key

One open alert per `(policy, kind)`, and the key is stable:

```
logweir-protection-<policyUID>-<kind>
```

`trigger` and `resolve` of one alert carry that exact string. `RecoveryCompleted` is the exception:
it keys on the **Restore UID** rather than on the policy, because each restore of a policy's points
is its own completed recovery. The prefix keeps the family apart from the drill families
(`logweir-drill-…`), and a UID that identifies nothing collapses to `unknown` rather than to
`logweir-protection--Staleness`. The controller was the authority on this value; a reader never
recomputes it.

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

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
