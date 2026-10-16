# PROD-19.1 — One namespace per install, workspaces inside it

- Row: PROD-19.1 (impl, Tier A), [product-expansion tracker](../product-expansion.md). Owner decisions: OD-23 (2026-10-16: one namespace per install, workspaces inside it, "do not over engineer it, needs to work"), OD-11 (one team per install, less code), OD-8 (approval modes), OD-7 (signed formats: MINOR only to the safer side).
- Date: 2026-10-16. Branch `claude/od23-ws-design`, at `56f4436d`. Design record only; it amends [D0](D0-product-api-and-identity.md) (stage 5 is withdrawn, §5) and [PROD-16.2](PROD-16.2-console-approval.md) (the approval names the workspace, §4).
- Written beside [OD-23-core](OD-23-core.md) (Schema Registry, engine rows, PROD-07.2). §10 says what each record needs from the other.

## 1. The model

An install is one Helm release in one namespace. Everything Logweir creates lives there: its objects, credential Secrets, signing and console keys, runner Jobs, the controller and the console. The controller watches that namespace only, and nobody is asked for a namespace. Inside the install, a **workspace** (for example `dev`, `tst`, `stg`, `prd`) is a name in the chart's `workspaces` list and a label on every object. A workspace groups connections, destinations, schedules, backups, restores, catalogs and checks. It has its own role bindings and its own approval mode. A reference from one workspace to another is refused. The console shows one workspace at a time and shows the picker only when the signed-in person holds roles in more than one. Archives in the bucket are untouched, so every existing archive can be imported into any workspace through a catalog. Kubernetes RBAC cannot see workspaces. The console enforces them, so `kubectl` write access to the namespace is an installation administrator's (§4.4).

### The operator's view

The default, one workspace (`localAdmin`, or `shared` with bindings to `default`):

```yaml
workspaces: [default]          # the chart default; the console shows no picker
```

Four environments in one install:

```yaml
workspaces: [dev, tst, stg, prd]
approvalPolicy:
  allowOrdinaryConfirmation: true
  policies:
    - { name: one-person, mode: confirm, maxAgeSeconds: 900 }
    - { name: two-person, mode: two-person, maxAgeSeconds: 900 }
  workspaces: { prd: two-person }   # unbound workspaces use approvalPolicy.default (confirm on a fresh install)
api:
  enabled: true
  console:
    enabled: true
    mode: shared
    roles:
      bindings:
        - { role: operator,      workspace: dev, groups: [kafka-devs] }
        - { role: operator,      workspace: prd, groups: [kafka-sre] }
        - { role: approver,      workspace: prd, groups: [kafka-leads] }
        - { role: administrator, workspace: dev, groups: [kafka-admins] }   # one row per workspace, as today per namespace
```

Install: `helm install logweir <chart> -n test -f values.yaml`. Adding a workspace later is a `helm upgrade` with a longer list. Removing one is allowed only when it holds no objects: objects whose workspace leaves the list stop with `WorkspaceUnknown`, and their archives stay in the bucket. A workspace cannot be renamed; make a new one and import its points through a catalog (§6).

## 2. What a workspace is on an object

**The label `logweir.dev/workspace: <name>`.** It is required, has no default, and its value must be in the install's `workspaces` list, which the controller gets as `LOGWEIR_WORKSPACES` and the console as `workspaces:` in its config.

Why a label and not a spec field:
- **Listing.** The API lists one workspace with a label selector on the API server, so `limit`, continue tokens and the signed cursor (`crates/logweir-api/src/kube.rs:800-889`) work unchanged. A spec field could only be filtered server-side with CRD `selectableFields`, which needs Kubernetes 1.31; the minimum is 1.29 (`docs/kubernetes.md`, *Concepts*). Filtering in memory would break pages.
- **No CRD change.** The fourteen CRDs and their CEL rules stay as they are. Any kind od23-core adds gets workspaces for free.
- **Mutability is acceptable.** CEL cannot see labels, so Kubernetes does not stop a `kubectl` writer from changing one. Such writers are installation administrators anyway (§4.4). The console never patches a label. An approval signs the workspace (§4.2), so relabelling an approved Restore refuses the run.

Why required and not defaulted to the first workspace: a label selector cannot match "absent or `dev`". An unlabelled object would be invisible in the console while the controller ran it.

| Who | Writes the label | Reads it |
| --- | --- | --- |
| API | on every create, from the path's `{ws}`; a body never carries one, and a label sent anyway is overwritten | `KubeAdapter` is the single choke point. `list` adds the selector, `get` and every patch treat another workspace's object as `NotFound`, and `create` sets the label. No handler names a namespace |
| Controller | on objects it creates (Backups from a schedule, Jobs), copied from the parent | at the top of every reconcile (`WorkspaceUnknown`), after every spec reference it resolves (`WorkspaceMismatch`), and when it keys the caps (§3) |
| Chart | on its own objects: `kafka.enabled`'s KafkaClusters and MinIO's `logweir-s3`, labelled with `workspaces[0]` | — |
| Runner | never reads labels. It gets the workspace as `LOGWEIR_EXECUTION_SUBJECT_WORKSPACE` (restore) or `--workspace` (switchover) and compares it with the signed document | — |
| `kubectl` users | must set it. Every sample in `docs/` and `examples/` carries it | `kubectl get backups -l logweir.dev/workspace=prd`, `-L logweir.dev/workspace` |

**Not doing:**
- A spec field: it needs a CRD change for 14 kinds and either Kubernetes 1.31 or broken pagination.
- A ValidatingAdmissionPolicy that freezes the label: it guards against installation administrators only.
- A `WORKSPACE` printer column: `-L` already shows the label.

## 3. What is per workspace, and what stays per install

| Per workspace | How |
| --- | --- |
| Roles | `api.console.roles.bindings[].workspace` replaces `.namespace` (`crates/logweir-api/src/config.rs:1347-1410`). The viewer/operator/approver/administrator matrix of D0 is unchanged, read per workspace. `localAdmin` is administrator of every workspace |
| Approval mode | `approvalPolicy.workspaces: {<ws>: <policy>}` replaces `.namespaces` (`crates/logweir-core/src/approval_policy.rs:1056-1065`, the binding map and its digest). An unbound workspace resolves exactly as an unbound namespace does today. Policies themselves, their snapshot bytes and digests are unchanged |
| Manual-run and check caps | Keyed by workspace instead of namespace: `run_pool.rs:391-421` compares the label, and `check/limits.rs:111-155` reads the Job's label. The values are renamed `…PerWorkspace` with the same defaults. `checks.maxActiveTotal` stays per install and protects the cluster. Per install was the smaller change (no code), but 2 restores running in `dev` would then queue a `prd` restore during an incident. Changing the key is a few lines and keeps `prd`'s slots |
| Rate limits | `api.console.rateLimits.*` count per person, per workspace, per minute (the limiter key changes; the values keep their names) |
| Console view | Lists, the Overview, Approvals and the wizard show one workspace. Addresses become `#/<ws>/…` |

**Per install:** trust (the `TrustPolicy` that governs the release namespace governs every workspace, including strict mode's approver keys), the signing identity, the console keys, `approvalPolicy.policies`, `allowOrdinaryConfirmation` and `default`, the archive settings, `checks.maxActiveTotal`, discovery and preflight retention, and the images.

**Not doing:**
- Per-workspace trust: one team per install, and strict mode is opt-in.
- Per-workspace values for the caps: one number for every workspace is enough.
- Workspace quotas.

## 4. Isolation

### 4.1 References across workspaces are refused

| Where | What | Reason |
| --- | --- | --- |
| API, create | A body naming a connection, destination, schedule, catalog, approval or point that another workspace holds | `422 validation_failed` with the field, "no `<kind>` named `<x>` in workspace `<ws>`". A referent that exists nowhere is admitted, as before, and refused by the controller (as built, PR #73) |
| API, read | `GET /workspaces/dev/backups/<a prd backup>` | `404 not_found` (the adapter's label check) |
| API, path | A workspace the person holds no role in | `403 workspace_forbidden` (renamed from `namespace_forbidden`), or `404` where unbound workspaces are hidden today |
| Controller | Every spec reference: Backup (`sourceRef`, `destinationRef`, `retryOf`), BackupSchedule (`sourceRef`, `destinationRef`), Restore (`clusterRef`, `approvalRef`, `sourceDestinationRef`, `evidenceDestinationRef`), RecoveryCatalog, RetentionPolicy, ProtectionPolicy, Preflight, Switchover and TopicDiscovery refs (`crds/*.rs`, every `LocalRef` in a spec) | condition `WorkspaceMismatch`, terminal for that object, no Job: "connection `x` is in workspace `dev`; this Restore is in `prd`" |
| Controller | Label absent, or not in `LOGWEIR_WORKSPACES` | condition `WorkspaceUnknown`, nothing reconciled further |
| Controller, runner | An approval whose signed `subject.workspace` is not the Restore's or Switchover's label | `AuthorizationSubjectMismatch` (existing reason, `approval_policy.rs:1863`) |

**A Restore of another workspace's point** is refused by the reference rule, because a point is reached only through a destination of the Restore's own workspace and the wizard lists only that workspace's catalogs. Two workspaces whose destinations name the same bucket and prefix share its points. That is the administrator's choice: whoever holds a bucket's credentials holds its points, and the console asks for those credentials per destination. This is also how `prd` data reaches `stg` for a drill: a `stg` destination with a read-only grant on `prd`'s bucket (open point 2).

**Not doing:**
- Overlap detection between destination locations, or a workspace field in receipts: both would block that drill and add a signed field, with no protection the bucket credential does not already give.

### 4.2 The workspace in signed bytes

The namespace separated authority in exactly one signed document: the **authorization document v2** (`subject {apiVersion, kind, namespace, name, uid}`, `approval_policy.rs:1408-1420`). It now carries `subject.workspace`.
- The console writes it on every request and approval (`approval_scope` shows it to the approver).
- Every reader of this build requires it and compares it with the subject's label, beside name and UID: the Approval controller, Restore and Switchover admission, the bundle writer and the runner (`ExpectedSubject`, `approval_policy.rs:1689`; runner `drill/phase1_approval.rs:399`).
- A `dev` approval can never authorise a `prd` run, even when both workspaces share a policy and someone relabels the object.
- `subject.namespace` stays and always holds the release namespace.

This is one MINOR step of the authorization document. An older reader refuses the unknown field (`deny_unknown_fields`), which is the safer verdict under OD-7. This build refuses a document without `subject.workspace` (an older minor), which only touches requests in flight, and a fresh install has none. Scorecards, receipts, catalog points and switchover records do not change (§6).

### 4.3 Object names are unique per install

Kubernetes enforces this, and it costs nothing. API-derived names (Backups, Restores, checks) hash `(issuer, subject, workspace, route, key)` (`idempotency.rs:153-162`; the cursor binds the workspace the same way, `cursor.rs:58-61`), so they never collide. A name a person types that is already taken anywhere in the install is refused `409 state_conflict`: "the name `source` is in use in this installation; names are shared by every workspace".

**Not doing:** per-workspace names through a mangled `<ws>-<name>`, which every reference, `kubectl` user and log would then have to translate.

### 4.4 What Kubernetes RBAC can and cannot do here

RBAC stops at the namespace. Inside it, `kubectl` users see and change every workspace. The docs (`docs/kubernetes.md`, a new *Workspaces* section, and `SECURITY.md`) must say:
1. Reading a Logweir kind in the namespace reads it in every workspace. `logweir-viewer` is the one Role to hand out for `kubectl`. It reads every workspace and no Secret.
2. Creating any Logweir object is creating it in any workspace: the label is free text, and the console's role bindings do not apply to `kubectl`.
3. **Whoever can read Secrets, create Pods or Jobs, or create a Logweir object that names a Secret (a KafkaCluster, BackupDestination or RetentionPolicy) in the release namespace holds Logweir's keys.** A runner Job mounts the Secret the object names. In the release namespace that can be `logweir-signing-key`, the console's session, cursor and confirmation keys, or the OIDC client secret. With them a person can sign evidence and forge any approval mode, `prd`'s two-person rule included. The console never lets a person name an existing Secret: connection and destination creates refuse `credentialRef` and `secret.existing` (release-notes items 33, 38).
4. So: give people the console. Give `kubectl` write access to the release namespace only to the people you would make administrators of every workspace.

## 5. What gets removed, and what stays cluster-scoped

**Removed (refused at render where it is a value, as in the chart README's *Withdrawn values*):**

| Item | Today |
| --- | --- |
| `controller.watchNamespaces`, `LOGWEIR_WATCH_NAMESPACES`, the `WatchScope` fan-out | `values.yaml:51-52`, `crates/weirkeeper/src/scope.rs:16-160`, `main.rs:262-288`. The controller runs one watch per kind in its own namespace (`LOGWEIR_INSTALLATION_NAMESPACE`, already set) |
| `templates/clusterrolebinding.yaml` and `controller-scope.yaml`'s per-namespace loop | The controller gets a `Role` plus `RoleBinding` in the release namespace and keeps `weirkeeper-cluster-scope` |
| The shared-mode rule "watchNamespaces required, release namespace excluded" | `_validate.tpl:86-95, 156-157`. D0 stage 5 (D0 line 212) is withdrawn by OD-23 (open point 1) |
| `kubernetes.namespace`, `kubernetes.connectionsNamespace`, `api.namespaces`, the `logweir.api.namespaces` and `logweir.connectionsNamespace` helpers | `values.yaml:9-10, 170`; `_helpers.tpl:104-114, 170-178`; `_validate.tpl:148-151, 211-218` |
| `identity.authorizedRunnerNamespaces` and the whole distribution: the source Role, the per-namespace Secret copies, the runner and distributor ServiceAccounts, the Role, RoleBinding and Job; `logweir identity distribute` | `values.yaml:33`; `identity.yaml:357-520`; `retention.yaml:22`, `networkpolicy.yaml:11`; `crates/logweir/src/identity.rs:216, 692-…`, `cli.rs:330-340` |
| `approvalPolicy.namespaces`, `roles.bindings[].namespace`, the four `…PerNamespace` caps | Renamed (§7). `checks.discovery.visibilityAttestations[].namespace` stays as it is and names the release namespace: the field is part of the check contract (`logweir-core/src/check_contract.rs:4015`), and an attestation is still matched by connection |
| The console's *namespace* picker and wording | `console/src/app/Shell.tsx:112-140, 374-403`, `router.tsx:37` |
| Docs | `docs/install.md` §4 *Runner namespace prerequisites*; `charts/logweir/README.md` §`controller.watchNamespaces`; `docs/kubernetes.md` *The signing-oracle residual*, rewritten to §4.4 item 3 |

**Made namespaced** (a `kind:` change; a ClusterRole bound by RoleBinding granted the same):
- the controller's namespaced rules (`clusterrole.yaml`, now `Role/weirkeeper`);
- the API's namespaced rules (`ui/api-rbac.yaml:49`);
- the human roles `logweir-viewer`, `-operator`, `-approver` and `-retention-admin` (`human-roles.yaml`; open point 3).

**Stays cluster-scoped, and why:**
- the CRDs, because Kubernetes has no namespaced CRDs. Their objects are namespaced and live in the release namespace;
- the `TrustPolicy`/`TrustRoster` reads (`weirkeeper-cluster-scope`, `<release>-api-trustpolicies`, `-trustroster`) and `logweir-trust-admin`, because those kinds are cluster-scoped;
- the identity singleton ClusterRole, which marks one install per cluster (`_validate.tpl:283`, unchanged), and the post-install trust-grant hook;
- the opt-in ValidatingAdmissionPolicy.

**Outside the release namespace, both opt-in:**
- the Strimzi `KafkaTopic` read Role in Strimzi's own namespaces (`strimzi.namespaces`). It reads someone else's objects, and Logweir creates nothing there;
- the trusted-proxy EndpointSlice Role in the ingress namespace (`trustedProxyService`, shared mode).

## 6. Archives and evidence

What the namespace reaches today:

| Signed document or layout | Namespace in it | Change |
| --- | --- | --- |
| Backup receipt | none (`owner.reference` is a Strimzi `KafkaTopic`'s `namespace/name`, someone else's namespace) | none |
| Catalog point | `execution.namespace`, optional and never written by this build (`catalog/record.rs:583-601`) | none |
| Scorecard (1.x, 2.x) | none. The approval block names principals and the key, not the subject | none |
| Switchover record | `switchover`/`restore` `{namespace, name, uid}` (`switchover.rs:896-901`) | none: it records, it does not authorise; the namespace is the release namespace |
| Authorization document v2 | `subject.namespace` | `+ subject.workspace` (§4.2) |
| Bucket keys | none: `logweir/drills/<run>…`, `logweir/backups/<id>/…`, `logweir/switchovers/<run>…`, the engine's `<backupId>/…` | none |

So an archive made under the old layout (any namespace, any version) stays readable and verifiable, and can be imported into any workspace: create a destination in that workspace over its bucket and prefix, then a `RecoveryCatalog` over the destination. The old signer's public key reaches the new install's `TrustPolicy` through the trust export of [install.md](../../install.md#recover-onto-a-clean-installation-with-the-kit), as historical, so old receipts verify.

**The upgrade sections become** (`docs/kubernetes.md` *Upgrade*, `docs/install.md` *Upgrade CRDs before upgrading the controller*, the chart README): "0.4.x to 0.5.0 has no in-place upgrade. Export trust as in *Recover onto a clean installation with the kit*, keep the bucket, `helm uninstall` the old release (one install per cluster), install 0.5.0 into one namespace with `workspaces`, import trust, and then in each workspace add connections, destinations and schedules and connect a catalog to see the old points. Delete the old namespaces when done."

## 7. Interfaces, decided once

| Surface | Before | After | Step |
| --- | --- | --- | --- |
| Chart value | — | `workspaces: [default]`: 1–256 unique DNS labels | new |
| Chart values | `kubernetes.namespace`, `kubernetes.connectionsNamespace`, `controller.watchNamespaces`, `identity.authorizedRunnerNamespaces`, `api.namespaces` | removed, refused at render by name | breaking (pre-1.0, fresh install) |
| Chart value | `approvalPolicy.namespaces` | `approvalPolicy.workspaces` (keys must be in `workspaces`) | renamed |
| Chart value | `api.console.roles.bindings[].namespace` | `.workspace` (must be in `workspaces`) | renamed |
| Chart values | `checks.maxActivePerNamespace`, `checks.maxEvidenceFetchActivePerNamespace`, `runs.maxManualBackupsActivePerNamespace`, `runs.maxManualRestoresActivePerNamespace` | `…PerWorkspace`, same defaults; also in the `weirkeeper-policy` document | renamed |
| Chart value | `api.console.rateLimits.*` | per person, per workspace | meaning |
| Label | — | `logweir.dev/workspace` on every Logweir object, runner Job and console-written Secret; required | new |
| CRDs | — | unchanged | none |
| Controller env | `LOGWEIR_WATCH_NAMESPACES` | removed; `LOGWEIR_WORKSPACES` (comma list, required, malformed refuses to start) | new |
| Controller reasons | — | `WorkspaceUnknown`, `WorkspaceMismatch` | new |
| API config | `namespaces:`, `roles.bindings[].namespace` | `namespace:` (the release namespace), `workspaces:`, `roles.bindings[].workspace` | renamed |
| API routes | `GET /api/v1/namespaces`, `/api/v1/namespaces/{ns}/…` (every route, `app.rs:284-448`) | `GET /api/v1/workspaces`, `/api/v1/workspaces/{ws}/…`, the same suffixes | OpenAPI next `-alpha.N`, breaking (docs/stability.md, pre-release) |
| API session | `namespaces[]` grants | `workspaces[]` grants (name, roles, capabilities) | same |
| API problem code | `namespace_forbidden` | `workspace_forbidden` | same |
| API access | `Access::Namespaced`, `AnyNamespace` (`access.rs:53-80`) | `Workspaced`, `AnyWorkspace`; the trust-policy reads go to an administrator of any workspace | same |
| Audit record | `namespace` | `workspace` | `docs/audit-log.md` |
| Runner env and CLI | `LOGWEIR_EXECUTION_SUBJECT_NAMESPACE` (`execution_contract.rs:44`); `switchover run --namespace` | `+ LOGWEIR_EXECUTION_SUBJECT_WORKSPACE`; `+ --workspace`; `logweir identity distribute` removed | CLI: removal is breaking (pre-1.0) |
| Signed: authorization document v2 | `subject {apiVersion, kind, namespace, name, uid}` | `+ subject.workspace`, required by this build | **MINOR** (OD-7: older readers refuse) |
| Signed: approval scope view | `namespace` | `+ workspace` | follows the API |
| Installation approval document | `namespaces: {}` | `workspaces: {}` (older binaries refuse the key at start) | not signed |
| Scorecard, receipt, catalog point, switchover record, bucket layout | — | unchanged | none |
| Console | `#/<ns>/…`, namespace picker | `#/<ws>/…`; workspace picker only when the session lists more than one | — |

## 8. Work split

| Run | Owns (files) | Scope |
| --- | --- | --- |
| **W0, first** | `crates/weirkeeper/src/workspace.rs` (new; replaces `scope.rs`), `crates/logweir-api/src/kube.rs`, `app.rs`, `access.rs` | The label constant; a `Workspace` (DNS label) and the `LOGWEIR_WORKSPACES` parse; `of(meta)` and `same(referrer, referent)` returning the two reasons; `KubeAdapter` holding the namespace with `ws` signatures; the `{ws}` route rename with handlers compiling. Mechanical, one run |
| W1 chart and RBAC | `charts/logweir/**`, `deploy/poc/**`, `crates/logweir/tests/{chart_lint,manifest_lint}.rs`, `scripts/check-chart.sh` | §5 removals and the Role conversions; `workspaces`; renamed values; env and config rendering; the new PoC values (one namespace, four workspaces, §9) |
| W2 controller, runner and core | `crates/weirkeeper/**` (after W0), `crates/logweir-core/src/{approval_policy,approval_scope,execution_contract}.rs`, `crates/logweir/src/**`, `docs/formats/approval-documents.md` | One watch; `of`/`same` at every reconcile and spec reference; labels on created Backups and Jobs; caps per workspace; `subject.workspace` written and required; the policy binding map; the runner env and flag; `identity distribute` deleted |
| W3 API | `crates/logweir-api/**` (after W0), `schemas/logweir-api-v1.openapi.json`, `docs/api.md`, `docs/audit-log.md` | Config, grants, authz, session, idempotency and cursor scope, rate-limit key, audit field, create-time reference checks, the name-in-use message, approval minting with `subject.workspace` (W2's core type) |
| W4 console | `console/**` | Regenerated client, `$ws` routes, the picker rule, wording, fixtures, vitest rows |
| W5 docs | `docs/{kubernetes,install,stability}.md`, `SECURITY.md`, a D0 amendment line, release-note text in the report | §4.4, §5, §6; samples carry the label |

**Order.** W0 lands on the integration branch first. Then W1, W2 and W3 run in parallel; W3 rebases on W2's first commit, which holds the `logweir-core` types. W4 starts once W3's OpenAPI document lands. W5 runs alongside, from the names in §7. **od23-core's builders start after W0**: they name workspaces through `weirkeeper::workspace` and `KubeAdapter`, and add routes under `/workspaces/{ws}`.

## 9. The combined acceptance pass

**C rows: crate tests, chart lint and vitest, run once on the integration branch.**

| # | Pass predicate | Negative control |
| --- | --- | --- |
| C1 | Default values render `workspaces: [default]`, `LOGWEIR_WORKSPACES=default`, no `ClusterRoleBinding/weirkeeper`, and no object outside the release namespace except the two opt-in Roles | Each removed value in §7 set alone: the render fails naming it |
| C2 | Four workspaces with bindings and approvals naming them render | A binding or approval naming `qa` (not listed), an empty list, or a duplicate: the render fails by name |
| C3 | A labelled `dev` Backup with `dev` refs proceeds to its Job | Unlabelled, and labelled `qa`: `WorkspaceUnknown`, no Job |
| C4 | One table row per spec reference field in §4.1: same workspace admitted | The referent in another workspace: `WorkspaceMismatch`, no Job, the message names both workspaces |
| C5 | An approval with `subject.workspace: prd` for a `prd` Restore is admitted by the controller and the runner | The same document against a Restore relabelled `dev`; a 2.2.0 document with no workspace: `AuthorizationSubjectMismatch` at both |
| C6 | With `prd` bound two-person and `dev` unbound (confirm), a confirmed `dev` request is admitted | A confirm document for a `prd` Restore: `ApprovalPolicyMismatch` |
| C7 | Two `dev` restores active: a third `dev` restore is `Queued` while a `prd` restore runs; rate limits count per (person, workspace) | Both workspaces counted as one pool: the `prd` restore would queue (the test fails if it does) |
| C8 | API with a fake Kubernetes: `dev` lists only `dev` objects, across pages; a create sets the label and overwrites one sent in the body | `GET /workspaces/dev/backups/<prd backup>` returns 404; a `dev` schedule naming a `prd` connection returns 422 |
| C9 | Role matrix per workspace: an operator bound only in `dev` gets `/workspaces` → `[dev]` and can mutate in `dev` | Every `prd` mutation, and `console-approval` in `prd` by an approver bound only in `dev`: 403 `workspace_forbidden` or `forbidden` |
| C10 | The same Idempotency-Key in two workspaces makes two objects | A cursor minted in `dev` used in `prd`: `cursor_invalid` |
| C11 | Console vitest: a session with one workspace renders no picker; with two, the picker lists them and switching changes the address | A session with two workspaces and the picker hidden fails the row |
| C12 | `catalog sync` over the checked-in pre-0.5 signed fixtures lists their points; `verify_scorecard.py` and `logweir verify` pass on old scorecards unchanged | A tampered fixture still fails verification |
| C13 | `rg 'WATCH_NAMESPACES\|authorizedRunnerNamespaces\|connectionsNamespace\|identity distribute'` over `crates/ charts/ console/src docs/` finds nothing but release notes | — |

**P rows: one fresh PoC install, `helm install logweir -n logweir`, with workspaces `dev`, `tst`, `stg`, `prd`.** Visible clicks only, as in OD-19. Bindings: `operator` is operator in `dev` only; `viewer` is viewer in `tst`; `approver` is approver in `stg`; `bob` is operator in `stg` and approver in `prd`; `alice` is operator and approver in `prd`; `admin` is administrator in all four; `norole` has none. `prd` is two-person; the rest are one-person. Demo Kafka and MinIO run in `logweir`.

| # | Pass predicate | Negative control |
| --- | --- | --- |
| P1 | `kubectl get all,secrets,jobs,roles,rolebindings,serviceaccounts -A -l app.kubernetes.io/instance=logweir` shows the `logweir` namespace only, plus the trusted-proxy Role in `traefik`. The controller logs one watched namespace, `logweir` | `kubectl auth can-i create jobs -n default --as system:serviceaccount:logweir:weirkeeper` returns `no` |
| P2 | `admin` sees a picker with four workspaces. In each one, by clicks: add the source and target connections and a destination, check, discover, create a schedule, Back up now → Verified | In `prd`, adding a destination with the name of an existing `dev` destination: the "in use in this installation" message, and nothing created (connections cannot be used here: the API names them) |
| P3 | `operator` sees no picker, only `dev`; a `dev` restore runs after the one-person confirmation and ends Restored and verified | Typing `#/prd/backups` into the address bar shows the no-access page; `norole` sees the no-role page |
| P4 | In `prd`, `alice` requests a restore and sees no Approve button on it; `bob` sees every topic and the workspace `prd`, approves, and the restore ends verified; both verifiers print `console approval:` naming `alice` and `bob` | `approver` (approver in `stg` only) sees no `prd` workspace and no `prd` request |
| P5 | A `stg` restore by `bob` runs after one person (stg is one-person) | — |
| P6 | Cross-workspace: `kubectl apply` a Restore labelled `prd` whose `clusterRef` is the generated name of a `dev` target connection (`kubectl get kafkaclusters -l logweir.dev/workspace=dev`): condition `WorkspaceMismatch`, no Job. The `prd` wizard lists only `prd` targets and points | The same Restore naming a `prd` target connection: Pending, waiting for approval |
| P7 | Approval reuse: the approved `dev` Approval's document, copied into an Approval labelled `prd` and named by a `prd` Restore, is refused `AuthorizationSubjectMismatch` | — |
| P8 | Import: a `stg` destination over the previous install's archive (copied to the new MinIO under `legacy/` with `mc mirror` before the old release is removed), with the old signer imported as historical, lists the old points in a catalog; a restore of one ends verified | A catalog in `dev` over a `stg`-only destination name: 422 |
| P9 | `logweir-viewer` bound to a test user through a RoleBinding lists Backups of all four workspaces with `-L logweir.dev/workspace` | `kubectl get secrets` as that user is forbidden |

## 10. Interfaces with the other record (`OD-23-core.md`)

What PROD-19.1 needs from od23-core:
- Every new object it adds, for example a Schema Registry connection or its credential Secret, carries `logweir.dev/workspace` and is created through `KubeAdapter`.
- Its references go through `workspace::same`, and its routes go under `/api/v1/workspaces/{ws}/`.
- A new run cap is keyed per workspace.
- If od23-core also adds a field to the authorization document (for example a schema scope), its field and `subject.workspace` ship as **one** MINOR step.
- PROD-03.2's schema-aware restore is a Restore, so it inherits §4.2 with no work.

What od23-core can assume: every object lives in the release namespace and belongs to exactly one workspace, and nothing in the bucket layout changes.

## 11. Open points for the owner

**Decided 2026-10-16 by the owner: all three as recommended, (a), (a) and (b).**

| # | Question | Options | Recommendation |
| --- | --- | --- | --- |
| 1 | The console's keys now share the namespace where the controller creates Jobs (D0 stage 5 kept them apart) | (a) accept: whoever can create Pods, Jobs or Secret-naming Logweir objects in the install's namespace holds every key (§4.4), documented as residual O1; (b) a second "control" namespace for the console's keys | **(a)**: it is what OD-23 asks ("everything in test"), and under OD-11 the people with that access are the install's administrators |
| 2 | Restoring `prd` data into `stg` | (a) through a `stg` destination with a read-only grant on `prd`'s bucket, set up by an administrator; no cross-workspace Restore; (b) a cross-workspace restore feature | **(a)**: no new code, and the bucket credential is the real boundary |
| 3 | `kubectl` write roles that no longer narrow anything (`logweir-operator`, `-approver`, `-retention-admin` can each mount any Secret in the namespace) | (a) keep them as namespaced Roles, documented as installation-wide; (b) delete them and ship `logweir-viewer` and `logweir-trust-admin` only; everyone else uses the console, and administrators use Kubernetes' `admin` | **(b)**: three fewer roles, and no role whose name promises less than it grants |

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
