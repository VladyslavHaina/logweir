# Metrics

v0.1 has **no HTTP surface** — no `/metrics`, no `/healthz`, no `/readyz`. The
Prometheus **textfile** written at `--metrics-file` is not one of two routes; it
is the route, and node_exporter's textfile collector scrapes it. Where that file
has to live is [kubernetes.md §5](kubernetes.md); what it contains is here.

Every metric is written by [`crates/logweir/src/metrics.rs`](../crates/logweir/src/metrics.rs)
and rendered by [`../dashboards/logweir.json`](../dashboards/logweir.json).

## The terminal-path contract

**Every terminal path writes the file.** There are five, one per exit code:

| Exit | What happened | Written by | Metric families in the file |
|---|---|---|---|
| `0` | the drill passed | `write_textfile` | all of them |
| `1` | operational error; no artifact | `write_minimal_textfile` | exit code + timestamp only |
| `2` | a drill result that is not a pass; the scorecard **is** signed | `write_textfile` | all of them |
| `3` | the plan was refused by a guard, before anything ran | `write_minimal_textfile` | exit code + timestamp only |
| `4` | signing or lock proof failed; nothing was uploaded | `write_minimal_textfile` | exit code + timestamp only |

Before this contract existed, exits 1, 3 and 4 wrote nothing at all, so a
CronJob whose pod died and a CronJob that was never scheduled produced the same
observation — none — and the dashboard's `1`/`3`/`4` value mappings were
unreachable by any code path.

Two consequences follow, and they are the whole point:

- **The file's existence carries no information any more.** Staleness does.
  See [Is the drill still running at all?](#is-the-drill-still-running-at-all)
  below.
- **The absence of `logweir_drill_runs_total` inside a PRESENT file** means the
  run ended before a scorecard existed. Every scorecard-derived family is
  written only on exits 0 and 2.

A metrics write that fails is logged at `warn` and swallowed. It never changes
the exit code the drill already decided: the textfile is a local operational
side-channel on a node-local volume, not an artifact and not an upload.

## Labels

`cluster` is on every series. On exits 0 and 2 it is the scorecard's
`target.cluster_id`. On exits 1, 3 and 4 it is the literal `unknown`: the id is
learned from the live broker at phase 2, a guard refusal (exit 3) happens at
phase 0 and most operational failures happen earlier still, and the drill spec
does not carry a cluster id to fall back on. The spec is deliberately **not**
parsed on a terminal path — a new failure surface at the exact moment the
process is already failing — and the label is never omitted, because a series
that sometimes has a label and sometimes does not is a Prometheus modelling
error.

`run_id` is **not a label**, on any series, ever. It is a ULID — unbounded
cardinality, strictly worse than `triggered_by`, which lives in the scorecard
for the same reason. It rides both shapes of the file as a leading comment line
that the textfile collector passes over and `cat` shows:

```
# logweir run_id=01JBQ7Q6X2K3V4M5N6P7R8S9T0
```

That is the only correlation handle an operator has on the failure paths, where
there is no scorecard to read.

## The metrics

| Metric | Type | Labels | What its ABSENCE means |
|---|---|---|---|
| `logweir_drill_last_run_timestamp_seconds` | gauge | `cluster` | nothing has ever written this file. Present on every terminal path, so an absent series means no run reached the end of `drill run` at all. |
| `logweir_drill_exit_code` | gauge | `cluster` | same. `0` pass · `1` operational, no artifact · `2` a signed drill result that is not a pass · `3` refused by a guard · `4` signing or lock proof failed. |
| `logweir_drill_runs_total` | counter | `cluster`, `outcome` | **no scorecard was produced** — the run ended on exit 1, 3 or 4. This is the single most informative absence in the file. |
| `logweir_drill_rto_seconds` | gauge | `cluster` | `measured.rto_excluding_preflight_seconds` was null, or no scorecard exists. |
| `logweir_drill_rpo_seconds` | gauge | `cluster` | `measured.rpo_seconds` was null, or no scorecard exists. |
| `logweir_drill_objective_met` | gauge | `cluster`, `objective` | that objective was **not measurable**, which is not the same as met. Read no value, never a zero. |
| `logweir_drill_fingerprint_mismatches` | gauge | `cluster` | no scorecard exists. Emitted unconditionally otherwise, `0` included. |
| `logweir_drill_integrity_level` | gauge | `cluster`, `level` | no scorecard exists. `level` is `byte-fingerprint`, `consume-only` or `not-attempted`. |
| `logweir_drill_integrity_result` | gauge | `cluster`, `result` | no scorecard exists. `result` is `pass`, `partial` or `fail`. |
| `logweir_drill_redactions` | gauge | `cluster` | no scorecard exists. Emitted unconditionally otherwise, `0` included, so `> 0` is a valid alert — an absent series and a whole document are otherwise indistinguishable to PromQL. |
| `logweir_evidence_lock_verified` | gauge | `cluster` | no scorecard exists. `0` means no proof was obtainable, **not** that the bucket is unprotected. |

The removed redaction **paths** are deliberately not labels (document-controlled
text, unbounded cardinality); read them from `drill show`'s qualifiers footer or
the notification body.

## Is the drill still running at all?

This is the query, and it is deliberately **not** a Logweir series:

```promql
time() - node_textfile_mtime_seconds{file=~".*logweir.*"} > 8d
```

A metric Logweir writes cannot report that Logweir did not run. node_exporter
publishes the textfile's own mtime as `node_textfile_mtime_seconds`, which
carries **no Logweir label at all** — so absence detection works identically on
the terminal paths where the cluster id was never learned and every Logweir
series is labelled `cluster="unknown"`.

`8d` is one day of slack over the weekly CronJob schedule in
[`../examples/cronjob-drill.yaml`](../examples/cronjob-drill.yaml). The
dashboard panel "Time since the last drill reported" thresholds at the same
`691200` seconds so the panel and the alert cannot disagree.

`time() - logweir_drill_last_run_timestamp_seconds{cluster=~".+"}` answers a *different*
question — when the last run that got as far as writing a file ended — and it
cannot see a pod that never started. Use the timestamp series to read the last
run's wall clock; use the mtime query to alert.

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

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
