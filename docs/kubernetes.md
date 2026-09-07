# Running Logweir on Kubernetes

v0.1 **does not require Kubernetes** — `logweir drill run` is a CLI that spawns
a local subprocess, and the demo runs on a laptop with `docker compose`. But a
scheduled drill is the point of the tool, and a `CronJob` is how most people
schedule one, so the facts below are part of the release rather than folklore.

Every fact on this page marked **verified** was checked on a live
`docker-desktop` cluster. Facts that were *not* checked live are marked
**unverified** and say so — please do not promote one to the other without
running it.

There is a worked manifest at [../examples/cronjob-drill.yaml](../examples/cronjob-drill.yaml).

---

## 1. The exit-code contract is nearly invisible in Kubernetes

This is the most important thing on the page, because it silently destroys the
signal Logweir exists to produce.

Logweir's exit codes are a published interface
([docs/stability.md](stability.md)):

| Code | Meaning |
|---|---|
| 0 | Pass. |
| 1 | Operational — the drill could not be attempted or continued. **No artifact was written.** |
| **2** | **A drill result that is not a pass. A scorecard WAS written and signed.** |
| 3 | Refused by a guard, before anything ran. |
| 4 | Signing or lock proof failed — and nothing was uploaded. |

**Exit 2 is the most valuable result the tool produces**: the drill ran, it was
measured, and your backup did not meet its objective. That is the finding you
scheduled the drill to get.

**Verified — and re-verified independently on 2026-09-05** with a job whose
container simply `exit 2`s: `kubectl get pods` showed `Error` with no code, the
Job's `.status` carried only `BackoffLimitExceeded`, and the exit code `2` was
readable at exactly one path. In Kubernetes the code appears only at —

```
pod.status.containerStatuses[].state.terminated.exitCode
```

(or `.lastState.terminated.exitCode` after a restart). It is **not** in Job
status, and `kubectl get pods` renders every non-zero exit as a generic
`Error`. So to an operator glancing at the namespace, **exit 2 is
indistinguishable from exit 1** — "your backup failed its drill" looks exactly
like "the drill could not run".

**The mitigation, verified working:**

```yaml
spec:
  backoffLimit: 0          # do not retry
  template:
    spec:
      restartPolicy: Never # exactly one pod, one attempt
```

That yields **exactly one pod** (re-verified: `kubectl get pods -l job-name=... |
wc -l` returned 1), an immediate `Failed` Job condition, and a cleanly readable
exit code at the path above:

```bash
kubectl get pod -l job-name=<job> \
  -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}'
```

**The alternative is actively harmful, and worse than "buries the code".**
`restartPolicy: OnFailure` with `backoffLimit > 0` **retries a drill that
legitimately did not pass** — it runs your restore again, against the same
archive, expecting a different answer.

Re-verified on this machine's `docker-desktop` cluster on 2026-09-05, with a
job whose container simply `exit 2`s (`backoffLimit: 2`, `restartPolicy:
OnFailure`). What actually happened, in order:

```
Warning  BackOff               Back-off restarting failed container probe in pod ...
Normal   SuccessfulDelete      Deleted pod: logweir-onfailure-probe-79czs
Warning  BackoffLimitExceeded  Job has reached the specified backoff limit
```

The container was restarted **in place**, and then the job controller
**DELETED THE POD**. `kubectl get pods` afterwards: `No resources found`. Since
the exit code lives only on the pod object, **it is not buried in `lastState` —
it is gone**, and the Job records only `BackoffLimitExceeded`. A drill that
found a real problem leaves behind no evidence of which problem it was.

Do not use `OnFailure` for a drill.

**Unverified:** `podFailurePolicy` (GA) supports exit-code-specific Job actions
and would express "code 2 is a real result, do not retry; code 1 may be
retried" declaratively. It was **not** tested live and is recorded here as an
option to evaluate, not as a recommendation.

### 1b. The log line is how you correlate a drill, and it works at the shipped default

The mitigation above gets you the code off one pod. Tying that pod to the
archive it read, to the metrics textfile it wrote (§5) and to the scorecard in
the bucket is the log's job.

`drill run` writes structured JSON to **stdout**, one object per line. The
default level is **`info` even with `RUST_LOG` unset**, so a pod nobody
configured still emits a correlatable log. Three facts follow:

- Every line carries the run id — on the event as `fields.run_id`, or on the
  entered span as `span.run_id`. It is the same id as the scorecard's `run_id`
  and the same id the `--metrics-file` textfile carries as a leading
  `# logweir run_id=…` comment, so one grep joins all three.
- Every terminal path emits `drill finished` with `fields.exit_code` and a
  `fields.meaning` string. That line is the one place the §1 distinction —
  "could not run" versus "ran and did not pass" — survives into a log
  aggregator at all.
- `RUST_LOG` overrides the default whenever it is set to anything non-blank.
  `RUST_LOG=warn` keeps the error line and its run id and drops everything
  else. A blank `value:` is treated as unset, not as "log nothing".

`examples/cronjob-drill.yaml` pins `RUST_LOG: info` in the container's `env:`
anyway. That is belt-and-braces rather than the mechanism: it holds the level
where a cluster-wide policy or a base image might otherwise inject a quieter
one, and it makes the level visible in `kubectl get cronjob -o yaml` without
reading Logweir's source.

Pulling the run id out of a failed pod, from your laptop:

```bash
kubectl logs job/<job> > drill.log
jq -r 'select(.level=="ERROR") | .fields.run_id' drill.log
```

`jq` is a laptop-side tool here — it is **not** in the runtime image (§2), and
the redirect is deliberate: `kubectl logs … | jq` would work, but piping
`logweir` itself into `jq` replaces the drill's exit code with `jq`'s, and §1
is about not losing that code.

## 2. The engine image is linux/amd64 only

**Verified.** Upstream publishes `osodevops/kafka-backup` for **linux/amd64
only**; no arm64 manifest exists for any tag. On an arm64 node it runs under
emulation, but **only after a host-side pre-pull**:

```bash
docker pull --platform linux/amd64 osodevops/kafka-backup@sha256:8ff5be…
```

Kubelet-side pulling fails with `no matching manifest`. Two consequences:

- **Use `imagePullPolicy: Never`.** A missing pre-pull then fails legibly as
  `ErrImageNeverPull` rather than as an opaque pull error. This is the honest
  trade: the pod cannot fetch what it needs, so make it say so.
- **Never set `nodeSelector: kubernetes.io/arch: amd64`** on a single-arm64-node
  cluster. The pod stays `Pending` **forever**, with no event that names the
  real problem.

**Verified, and it will bite you later:** `docker image prune` evicts the
pre-pulled image, and drills silently start failing with **no code change and no
manifest change**. If drills stop working after a routine Docker cleanup, this
is why.

## 3. Registry, permissions and mounts

All **verified** on `docker-desktop`:

- **No registry is needed.** Docker Desktop's kubelet shares the local Docker
  image store, so a locally built `logweir:v0.1.0` is directly runnable.
- **The default ServiceAccount has zero access.** A drill pod needs its own
  ServiceAccount, Role and RoleBinding. `automountServiceAccountToken: false` is
  appropriate for v0.1, which makes no Kubernetes API calls at all.
- **The signing key needs `fsGroup`. Without it a non-root pod cannot read its
  own key, and this page used to claim the Secret "mounts cleanly".** It does
  not: kubelet writes Secret files owned by `root:root`, so a container running
  as `runAsUser: 65532` — which the image sets and the manifest repeats — reads
  nothing through the owner bits. `examples/cronjob-drill.yaml` shipped
  `runAsUser: 65532`, `defaultMode: 0400` and **no `fsGroup` anywhere**, so
  every scheduled drill would have died at phase 1 opening
  `/etc/logweir-keys/signing.pem`.

  **All four combinations run live on `docker-desktop`, 2026-09-05**, in one
  pod shape with the manifest's own uid/gid, `busybox` doing `ls -lL` then
  `cat`:

  | `defaultMode` | `fsGroup` | file as mounted | `cat` |
  |---|---|---|---|
  | `0400` | none | `-r-------- root root` | **Permission denied** |
  | `0440` | none | `-r--r----- root root` | **Permission denied** |
  | `0400` | `65532` | `-r--r----- root 65532` | reads |
  | `0440` | `65532` | `-r--r----- root 65532` | reads |

  **`fsGroup` is the load-bearing half, and it is sufficient on its own.**
  Two things happen when it is set: kubelet chowns the volume's group to that
  GID, *and* it ORs group-read into the file mode — which is why row 3 lands on
  disk as `0440` even though the manifest asked for `0400`. Widening the mode
  without `fsGroup` (row 2) fixes nothing, because the group is still root's.

  `examples/cronjob-drill.yaml` now sets `fsGroup: 65532` and writes
  `defaultMode: 0440`, so the manifest states the permission that actually
  lands rather than one kubelet silently widens.
- **The ConfigMap was never the failing half, and that was checked, not
  assumed.** The drill spec, the approval and the allowed-clusters list mount as
  a ConfigMap; the manifest sets no `defaultMode` there, so kubelet's `0644`
  applies. Verified live in the same pod shape, with **no `fsGroup`**:
  `-rw-r--r-- root root drill.yaml`, `cat` exit 0. The files are `root:root` for
  the same reason the Secret's are — the group bits simply do not matter when
  the world bits are set. That is also why the key is a Secret and not a
  ConfigMap.
- **`emptyDir` is verified** for the engine's scratch directory. **No PVC was
  exercised.** The only StorageClass on the test cluster was `hostpath`.
- **The metrics file must NOT be an `emptyDir`** — see §5.

## 4. What the pod still needs from you

The container image carries the engine; the environment does not carry itself:

| Variable | Why |
|---|---|
| `LOGWEIR_ENGINE_BIN` | Set by the image to `/usr/local/bin/kafka-backup`. Override only if you mount an engine elsewhere. |
| `LOGWEIR_ENGINE_VERSION`, `LOGWEIR_ENGINE_DIGEST` | **Mandatory.** An empty value is refused with exit 1: a signed scorecard must name the engine image that produced the restore. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`, or an IRSA / Pod Identity setup | `object_store`'s own credential chain — **not** the AWS SDK's. `~/.aws/credentials`, `AWS_PROFILE` and SSO are unsupported. See [stability.md](stability.md). |
| `TMPDIR` | Where the rendered `restore.yaml` and the restore checkpoint land. Point it at a writable volume; the checkpoint is pod-local and is never uploaded, so a crashed restore is not resumable in v0.1. |

## 5. The metrics file is the only metrics surface, so where you mount it decides whether you get any

v0.1 has **no HTTP surface** — no `/metrics`, no `/healthz`, no `/readyz`. The
Prometheus **textfile** written at `--metrics-file` is not one of two routes; it
is the route. So the volume behind it is load-bearing.

`examples/cronjob-drill.yaml` mounted it on an **`emptyDir`**, directly beneath
its own comment telling the reader to mount node_exporter's textfile directory.
An emptyDir is deleted with the pod. Every drill therefore wrote its metrics
into a filesystem nothing would ever scrape and then destroyed it, and
[../dashboards/logweir.json](../dashboards/logweir.json) stayed empty forever —
which looks exactly like Logweir never running.

**Verified live on `docker-desktop`, 2026-09-05.** One pod shape at
`runAsUser: 65532` wrote `logweir.prom`; the pod was then **deleted**; a second
pod mounted the same volume and read it back:

```
hostPath /var/lib/node_exporter/textfile   ->  logweir_drill_last_run_timestamp_seconds{cluster="unknown"} 1757000000
emptyDir                                   ->  total 0
                                               cat: can't open '/metrics/logweir.prom'
```

The sample above is emitted by `crates/logweir/src/metrics.rs` — `write_textfile`
on a drill result, `write_minimal_textfile` on exits 1, 3 and 4. The `cluster`
label is always present: it is the scorecard's `target.cluster_id`, or the
literal `unknown` on a terminal path that never learned it (see
[metrics.md](metrics.md)). The value is that run's own `Utc::now().timestamp()`
at emit time, never a constant.

Three facts decide the shape, each of them run:

- **`type: Directory`, not `DirectoryOrCreate`.** `DirectoryOrCreate` makes
  kubelet create the path and it lands `drwxr-xr-x root root`, which uid 65532
  cannot write: `can't create /metrics/logweir.prom: Permission denied`, at
  phase 8, after the whole drill has already run. `Directory` fails at **mount**
  time instead, before the container starts, with an event that names the path:

  ```
  Warning  FailedMount  MountVolume.SetUp failed for volume "metrics":
                        hostPath type check failed: /var/lib/... is not a directory
  ```

  Loud beats silent.
- **`fsGroup` does not apply to `hostPath`.** Ownership is host-side setup, not
  manifest setup. Verified: the directory at `root:root 0755` refuses the write;
  at `0775` with group 65532 it succeeds.

  ```bash
  sudo install -d -m 0775 -g 65532 /var/lib/node_exporter/textfile
  ```

- **A `hostPath` volume is forbidden by Pod Security `baseline` AND
  `restricted`.** Verified by labelling the namespace and re-applying:

  ```
  Error from server (Forbidden): violates PodSecurity "baseline:latest":
    hostPath volumes (volume "metrics")
  ```

  Everything else in the worked manifest satisfies `restricted`; the metrics
  volume is the one thing that does not. In a namespace that enforces either
  profile, **delete both the `--metrics-file` argument and the `metrics`
  volume** rather than pointing the flag at an emptyDir. No metrics is an honest
  state; a dashboard fed by a deleted file is not.

  That is what the **`restricted` manifest variant** does: it ships without the
  `hostPath` volume, without its mount and without `--metrics-file`. It
  therefore emits **no textfile on any exit path** — not on a pass, not on a
  drill result, and not on the exits 1, 3 and 4 that now write a minimal record
  everywhere else. Nothing below rescues it: there is no PVC, no sidecar, no
  push route and no HTTP surface in v0.1, so a `restricted` namespace gets its
  drill results from the signed scorecard and the pod's exit code alone. This
  gap is a known regression, recorded rather than fixed, and it is written up
  with the hardened manifests; do not close it by adding a volume back.

### Is the drill still running at all?

Every terminal path now writes the textfile, so the file's *existence* says
nothing. Its **mtime** does:

```promql
time() - node_textfile_mtime_seconds{file=~".*logweir.*"} > 8d
```

This is mtime-based and not `time() - logweir_drill_last_run_timestamp_seconds{cluster=~".+"}`
on purpose. A metric Logweir writes cannot report that Logweir did not run, and
node_exporter's `node_textfile_mtime_seconds` carries **no Logweir label at
all** — so it still matches on the terminal paths where the cluster id was never
learned and every Logweir series is labelled `cluster="unknown"`. `8d` is one
day of slack over the weekly schedule in `examples/cronjob-drill.yaml`; the
dashboard's "Time since the last drill reported" panel thresholds at the same
`691200` seconds. Full metric reference: [metrics.md](metrics.md).

**Unverified:** a shared PVC read by a node_exporter sidecar, and any push-based
route (Pushgateway, OTLP). Neither was tested, and v0.1 emits nothing but the
textfile.

## 6. What is NOT here in v0.1

Stated so nobody goes looking:

- **No operator, no CRDs.** `weirkeeper`, `logweir.dev/v1alpha1`, `RestoreDrill`
  and `MetadataSnapshot` are SP5.
- **No Kubernetes Job execution of the engine.** v0.1 spawns a local
  subprocess inside the drill pod; it does not create a Job of its own.
- **No namespace-label segregation proof.** v0.1 proves the target is a scratch
  cluster with a **marker topic**, because a guard that needs a kubeconfig
  cannot run in the unit suite.
- **No `SubjectAccessReview`-verified approval.** Approval is a DSSE-signed
  document checked against the approver's public key.
- **No HTTP surface.** No `/metrics`, `/healthz` or `/readyz`. Metrics are a
  Prometheus **textfile** written at `--metrics-file` and scraped through
  node_exporter's textfile collector — see §5 for where that file has to live,
  and [../dashboards/logweir.json](../dashboards/logweir.json) for the
  dashboard it feeds.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
