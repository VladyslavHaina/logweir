# Metrics

The CLI writes a Prometheus textfile at `--metrics-file`; it has no HTTP
metrics endpoint. node_exporter's textfile collector scrapes the file. For
persistent mounts, see [kubernetes.md §5](kubernetes.md).

Every metric is written by [`crates/logweir/src/metrics.rs`](../crates/logweir/src/metrics.rs)
and rendered by [`../dashboards/logweir.json`](../dashboards/logweir.json).

## The terminal-path contract

**With `--metrics-file` configured, every handled terminal path attempts to
write the file.** A killed process or failed write cannot report its own result:

| Exit | What happened | Written by | Metric families in the file |
|---|---|---|---|
| `0` | the drill passed | `write_textfile` | all of them |
| `1` | operational error; no artifact | `write_minimal_textfile` | exit code + timestamp only |
| `2` | a drill result that is not a pass; the scorecard **is** signed | `write_textfile` | all of them |
| `3` | the plan was refused by a guard, before anything ran | `write_minimal_textfile` | exit code + timestamp only |
| `4` | signing or lock proof failed; nothing was uploaded | `write_minimal_textfile` | exit code + timestamp only |

An existing file may contain an old run. Check freshness as well as its values.
Scorecard-derived families are absent on exits 1, 3 and 4.

A metrics write that fails is logged at `warn` and swallowed. It never changes
the exit code the drill already decided: the textfile is a local operational
side-channel on a node-local volume, not an artifact and not an upload.

## Labels

`cluster` is on every series: the scorecard's `target.cluster_id` on exits
0 and 2, and `unknown` on exits 1, 3 and 4, where the terminal writer has no
scorecard-derived id. The terminal writer does not reparse the spec.

`run_id` is a comment, never a label, to avoid unbounded cardinality:

```
# logweir run_id=01JBQ7Q6X2K3V4M5N6P7R8S9T0
```

Use it to correlate the file with logs, including failures without scorecards.

## The metrics

| Metric | Type | Labels | What its ABSENCE means |
|---|---|---|---|
| `logweir_drill_last_run_timestamp_seconds` | gauge | `cluster` | No current sample was collected. Check whether the run finished, the write succeeded, the file still exists and the exporter scraped it. |
| `logweir_drill_exit_code` | gauge | `cluster` | same. `0` pass · `1` operational, no artifact · `2` a signed drill result that is not a pass · `3` refused by a guard · `4` signing or lock proof failed. |
| `logweir_drill_runs_total` | counter | `cluster`, `outcome` | **no scorecard was produced** — the run ended on exit 1, 3 or 4. This is the single most informative absence in the file. |
| `logweir_drill_rto_seconds` | gauge | `cluster` | `measured.rto_excluding_preflight_seconds` was null, or no scorecard exists. |
| `logweir_drill_rpo_seconds` | gauge | `cluster` | `measured.rpo_seconds` was null, or no scorecard exists. |
| `logweir_drill_objective_met` | gauge | `cluster`, `objective` | that objective was **not measurable**, which is not the same as met. Read no value, never a zero. |
| `logweir_drill_fingerprint_mismatches` | gauge | `cluster` | no scorecard exists. Emitted unconditionally otherwise, `0` included. |
| `logweir_drill_integrity_level` | gauge | `cluster`, `level` | no scorecard exists. `level` is `byte-fingerprint`, `consume-only` or `not-attempted`. |
| `logweir_drill_integrity_result` | gauge | `cluster`, `result` | no scorecard exists. `result` is `pass`, `partial` or `fail`. |
| `logweir_drill_redactions` | gauge | `cluster` | no scorecard exists. Emitted unconditionally otherwise, `0` included, so `> 0` is a valid alert — an absent series and a whole document are otherwise indistinguishable to PromQL. |
| `logweir_drill_teardown_topics_failed` | gauge | `cluster` | no scorecard exists — the run ended on exit 1, 3 or 4, before phase 9. Emitted unconditionally otherwise: `0` means phase 9 ran and cleaned up, and a non-zero value is the count of scratch topics the broker refused to delete. |
| `logweir_evidence_lock_verified` | gauge | `cluster` | no scorecard exists. `0` means no proof was obtainable, **not** that the bucket is unprotected. |

The removed redaction **paths** are deliberately not labels (document-controlled
text, unbounded cardinality); read them from `drill show`'s qualifiers footer or
the notification body. The **names** of the scratch topics teardown could not
delete are left off `logweir_drill_teardown_topics_failed` for the same reason —
topic names are cluster-controlled text — and are carried instead by the run's
`WARN` line, by the `drill run` summary line and by the signed teardown
attestation, all three of which have a reader who can hold them.

`logweir_drill_teardown_topics_failed > 0` does **not** mean the drill failed.
Phase 9 runs after phase 8 has signed and uploaded the drill result, so a run
that verified correctly and could not clean up still exits `0` and still counts
as a pass in `logweir_drill_runs_total`. What it means is that scratch topics
are still on the target cluster and someone has to remove them by hand. Whether
it *should* also change the exit code is an open question, recorded under
"Known limitations" in [stability.md](stability.md).

Although declared a counter, `logweir_drill_runs_total` is written as `1` for
the current scorecard and replaces the previous file. It is not a cumulative
run count; `rate()` or `increase()` over this textfile does not count drills.

## Is the drill still running at all?

This is the query, and it is deliberately **not** a Logweir series:

```promql
time() - node_textfile_mtime_seconds{file=~".*logweir.*"} > 691200
```

A metric Logweir writes cannot report that Logweir did not run. node_exporter
publishes the textfile's own mtime as `node_textfile_mtime_seconds`, which
carries **no Logweir label at all** — so stale-file detection works identically on
the terminal paths where the cluster id was never learned and every Logweir
series is labelled `cluster="unknown"`.

`691200` seconds is eight days: one day of slack over the weekly CronJob in
[examples/cronjob-drill.yaml](../examples/cronjob-drill.yaml). The dashboard
uses the same threshold. This query detects a stale **existing** file. It
cannot detect a file that was never created, a removed file, or an exporter
that stopped reporting; use a separate absence/exporter-health alert scoped to
the hosts expected to run drills. Neither freshness timestamp can distinguish
"pod did not start" from "metrics writing failed" without that context.

**v0.1 ships no alert rules.** The query above is documented, not deployed;
`.rules.yaml` files are a later piece of work.

## Where there is no textfile

The `restricted` Pod Security variant of the CronJob manifest drops the
`hostPath` volume, its mount and `--metrics-file` altogether, because a
`hostPath` volume is forbidden by both `baseline` and `restricted`. That variant
therefore emits **no textfile on any exit path**, including the ones described
above, and there is no PVC or sidecar fallback in v0.1. No metrics is an honest
state; a dashboard fed by a deleted file is not. See
[kubernetes.md §5](kubernetes.md).

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
