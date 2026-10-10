# PROD-01.2 — The compatibility contract

- Row: PROD-01.2 (research and implementation, Tier B, lab `compose`), [product-expansion tracker](../product-expansion.md).
- Status: **in progress** (this record is written first and kept current; the sections marked *pending* are filled as their rows run).
- Date: 2026-10-10 (UTC). Branch `claude/prod-01-2`, from `8436f941`.
- Every row is local compose (slot 2). No cloud resource was created and no remote endpoint was dialled (OD-4).

## 0. Decision summary (so far)

1. **"Supported" is a row in this repository, never a reading.** §2 defines supported, limited, untested and unsupported by the evidence each needs.
2. **Redpanda v26.2.4: backup works, restore does not.** Redpanda serves Produce v0–v7; the engine sends Produce v8 and never negotiates, so Redpanda closes the connection and the restore fails (exit 1, nothing signed). Measured.
3. **Confluent Platform 8.3.2 (`cp-kafka`): no difference from Apache Kafka 4.3.1** in what Logweir uses. Measured.
4. **Two shipped readers recorded an unreported value as a fact** (the target's record-timestamp bound and the broker's timestamp type, on a broker whose configuration answer does not carry the keys). §6.
5. **Capability detection** is four new CheckIds (§5), asked for by the plan so an older controller never receives a row it cannot read.

## 1. What Logweir needs from a Kafka-compatible endpoint

*Pending: the per-operation table (API key, version range, permission, behaviour when missing with `file:line`).*

Sources already on record, not re-measured here: the engine's fixed request versions ([PROD-00.1](PROD-00-engine-route.md) §3.8, [PROD-01.5](PROD-01.5-fixture-profiles.md) §2.3, byte-identical at `crates/kafka-backup-core/src/kafka/client.rs:625-648` of the vendored 0.23.3 tarball), and librdkafka's group and ACL calls ([PROD-04.0](PROD-04.0-admin-path.md) §1.2).

## 2. The classification

*Pending.*

## 3. The minimum-permission profile

*Pending: measured on the `acl` profile.*

## 4. Measured rows

### 4.1 API ranges (2026-10-10, `kafka-broker-api-versions.sh` from the 4.3.1 image, in-network)

| API | Engine sends | Apache Kafka 4.3.1 | Redpanda v26.2.4 | Confluent Platform 8.3.2 |
|---|---|---|---|---|
| Produce | v8 | v0–v13 | **v0–v7** | v0–v13 |
| Fetch | v11 | v4–v18 | v4–v13 | v4–v18 |
| ListOffsets | v5 | v1–v11 | v0–v6 | v1–v11 |
| Metadata | v9 | v0–v13 | v0–v12 | v0–v13 |
| DescribeConfigs | v1 | v1–v4 | v0–v4 | v1–v4 |
| ListGroups | (Logweir: v5 for types) | v0–v5 | **v0–v4** | v0–v5 |

*Pending: the rest of the rows and the runs.*

## 5. Capability detection

*Pending.*

## 6. Capture paths swept for "unsupported metadata never appears captured"

*Pending.*

## 7. Archive backends

*Pending.*

## 8. Managed providers

*Pending.*

## 9. Limits, and proposed rows

*Pending.*

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
