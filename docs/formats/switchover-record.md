# The switchover plan and record

PROD-04.2. Two documents of a [`Switchover`](../kubernetes.md#switching-consumer-groups-over):
the plan an approver signs, and the signed record of one run. The positions
mapping they apply is the scorecard's
[`positions_mapping`](drill-scorecard.md#integrityverificationpositions_mapping-format-1120-and-240).

## The plan (format 1.0.0)

YAML or JSON, the exact bytes `Switchover.spec.planBytes` carries and the
approval's `plan_hash` covers. `logweir switchover plan` writes a forward plan;
`logweir switchover rollback-plan` writes the plan that reverses a record.

```yaml
format_version: "1.0.0"
restore: {name: incident-4471, uid: 6f1c…, plan_hash: "sha256:…",
          scorecard_sha256: "sha256:…", mapping_sha256: "sha256:…"}
rollback_of: {key: logweir/switchovers/<run_id>.record.json, sha256: "sha256:…"}  # rollback plans only
groups:
  - source: orders-app            # the captured group
    target: orders-app            # the target group id
    topics:
      restore-orders:             # the restored topic
        offsets: {0: 1200, 1: 50} # partition -> offset to commit
        approximate: [1]
        expected: none            # or {0: 1180, 1: 50}: the positions the group must hold
```

Bounds: 1 to 100 groups, 1 to 8,192 positions, 256 KiB, distinct target ids,
offsets not negative. A forward plan's every offset is the mapping's, mapped
(`PlanDisagreesWithMapping` otherwise); its `expected` is a precondition only.
A rollback plan's every offset is the record's prior and its `expected` the
record's readback, for a group the record applied.

## The record (format 1.0.0)

`logweir/switchovers/<run_id>.record.json` and `.record.sig` in the restore's
evidence store, payload type
`application/vnd.logweir.switchover-record+json;version=1.0.0`, schema
`schemas/logweir-switchover-record-1.0.0.json` (`logweir schema
switchover-record`). It names the switchover object, the plan hash, the
restore it applied (scorecard and mapping digests), the approval's mode and
approver, the target cluster id and each restored topic's ID at creation and
now, the start and end, the outcome (`Succeeded`, `PartiallyApplied`, `Failed`,
`Refused` with the refusal's sentence), the counts, and per group its
classification, outcome, reason, `created`, whether this run `committed`, and
per partition `prior`, `planned`, `applied` (`true`, `false` or `"unknown"`)
and `readback`. Both readers verify it signature-only:
`logweir drill verify --payload-type switchover-record` and
`verify_scorecard.py --payload-type switchover-record`.

## Versions

| Document | Version | Adds |
|---|---|---|
| Plan | 1.0.0 | the grammar above (PROD-04.2) |
| Positions mapping | 1.0.0 | the document (PROD-04.2) |
| Record | 1.0.0 | the record (PROD-04.2) |

An older verifier refuses the record's payload type and ignores the
scorecard's block; an older controller ignores `Switchover` objects.

---

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
