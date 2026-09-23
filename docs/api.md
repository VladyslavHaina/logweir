# The product API (`logweir-api`)

`logweir-api` is a small HTTP service that serves the static UI at `/ui/` and a
typed JSON API at `/api/v1` on one origin, and that reads and creates the
existing `logweir.dev/v1alpha1` resources through one Kubernetes adapter.

**It is not a Kubernetes proxy.** There is no generic
`{group}/{version}/{resource}` route, no request-supplied Kubernetes path, no
Secret, Pod, log, exec or Job access, no delete, and no arbitrary patch. It
creates custom resources; `weirkeeper` remains the only execution authority and
isolated runner Jobs remain the data-plane boundary. The service dials no Kafka
broker and no object store.

## What ships today, and what does not

**Two modes, and the configuration file must name one.** There is no default:
a file that forgets to say which mode it wants is refused rather than read as
the more permissive one.

* `mode: localAdmin` — a loopback-only listener, the configured administrator
  as the actor, and namespaces from configuration alone. Not SSO, not a shared
  console. Run in the cluster it is the **in-cluster administrator mode**: the
  chart renders no Service, no Ingress and no ingress NetworkPolicy rule beside
  it, the identity is the `<release>-api` ServiceAccount, readiness is not gated
  on OIDC, and `kubectl port-forward deploy/<release>-api` is the only way in.
  It is for isolated labs and break-glass administration, and it is not a
  shared console. Its one actor is still a requester: with an approval policy
  configured it signs ordinary confirmations for its own requests and is
  refused a governed approval of them (*Approval policy* below).
* `mode: shared` — the SSO console: OpenID Connect identity, a short-lived
  encrypted session cookie, a synchronizer CSRF token on every unsafe method,
  exact role and namespace bindings, and one audit record per request. It
  refuses a non-HTTPS `publicBaseUrl` before it binds anything.

`logweir-api` **is packaged and deployable** as of D0 stage 7, and the two
sentences that used to stand here — "no image builds it", "the API runs with
whatever Kubernetes identity its kubeconfig carries, which may be cluster-admin"
— are no longer true. `Dockerfile.console` builds the `logweir-console` image
(this binary plus the static page files); `charts/logweir` runs it under
`api.console.enabled` as the `<release>-api` ServiceAccount that `api.enabled`
renders, with per-namespace RoleBindings, an optional TLS Ingress and an
optional NetworkPolicy. See *Deployment* below and `charts/logweir/README.md`.
`publish = false` still keeps the binary out of the release **archives**: it
ships as an image, not as a tarball.

**Nothing about an existing installation changes when it is not enabled.**
`api.console.enabled` defaults to false, `api.enabled` keeps meaning exactly
what it meant (an account and its grants, no pod), and the chart's default and
demo renders are unchanged. Turning the console off again removes a ConfigMap,
a Deployment and a Service and leaves every custom resource, every grant and
every piece of evidence untouched — this service creates and reads objects and
executes nothing.

**Shared mode is a supported deployment shape, and since PLAT-17.2's
completion the chart renders it only outside the controller's Job-create
authority.** The image, the chart templates, the ingress, the NetworkPolicy,
the ServiceAccount and its RoleBindings exist and refuse the configurations that
would publish a console over plain HTTP; D0 stage 5 is met by
`controller.watchNamespaces` (the controller holds Job-create authority only in
the execution namespaces, never in the release namespace that holds the
console's keys — *Deployment* below), and the chart refuses shared mode without
it. What D0 still lists beyond this service's own boundary is the browser
journey against a real provider and TLS ingress (stage 8), the production-CNI
evidence for the NetworkPolicy (Docker Desktop's acceptance of a policy proves
nothing about deny behaviour). The trust-read narrowing that
`charts/logweir/templates/ui/api-rbac.yaml`'s comment still calls open (D3 W11
F2) has landed: `GET /api/v1/trust-policies` is Administrator-only and filters
namespace lists to the actor's (*Protection, rehearsals, catalogs, retention
and trust* below). Read those before calling an installation a production
shared console.

A domain whose routes do not exist yet has **no route at all** — no stub and no
`501`. `GET /api/v1/session` reports each one as `false` under `capabilities`,
so a client learns what is unavailable instead of discovering it from an error.
Today that is: connection tests. (Manual backup creation, operation event
streams and — since PLAT-19.2 — governed approval submission have routes.) Saved destinations, topic discovery, preflight
checks and write-only credential input **do** have routes now (D2 W12); their
three capability flags are the domain's READ floor, and whether the actor may
also start or cancel is the role table each grant publishes in `roles`.

## Running it

```yaml
# config.yaml
mode: localAdmin                       # the only accepted value
listen: "127.0.0.1:8484"               # a loopback IP literal; a hostname is refused
publicOrigin: "http://127.0.0.1:8484"  # the exact Origin unsafe requests must carry
uiDirectory: ./ui
localAdmin:
  subject: admin
  displayName: Local administrator     # never used for authorization
namespaces: [team-a]                   # explicit grants; core Namespaces are never listed
kubernetes:
  source: kubeconfig                   # or `inCluster`
  context: docker-desktop              # required; `current-context` is never used
cursorKeyFile: ./cursor.key            # at least 32 bytes
```

```console
$ head -c 32 /dev/urandom > cursor.key && chmod 600 cursor.key
$ cargo run -p logweir-api -- --config config.yaml
```

**Paths in this file are taken literally.** A relative one — `./ui`,
`./cursor.key` above — resolves against the **configuration file's own
directory**, not the working directory, so moving the file moves what it names.
A leading `~` is **not** expanded: `kubeconfig: ~/.kube/config` would look for a
directory actually named `~`, and the service would exit 2 saying it cannot read
that file. Write an absolute path, or omit `kubernetes.kubeconfig` entirely as
the example does — with the key absent the usual `KUBECONFIG`-then-`~/.kube/config`
lookup applies, performed by the Kubernetes client library and by the shell
conventions it implements, which is the only place that expansion belongs.
`kubernetes.context` is required either way.

Exit codes: `0` after a clean `SIGTERM`/`SIGINT`, `2` when the configuration is
refused — before any Kubernetes client or socket exists — and `1` for any other
startup or runtime failure.

The configuration is validated first, and that order is the security property.
`listen` must parse as a loopback IP literal: `localhost` is refused even though
it usually resolves to loopback, because resolution is an input this process
does not control. `0.0.0.0`, `[::]` and any routable address are refused by
name. So is a kubeconfig context whose user impersonates another identity, a
`mode` this release does not implement, and a cursor key shorter than 32 bytes.

`/healthz` answers whenever the process is alive and consults nothing.
`/readyz` additionally requires that Kubernetes answers for this service's
identity; its body never says which endpoint or why.

## Routes

All product routes are under `/api/v1`. The route table is the boundary:
anything not listed is `404`.

| Route | What it does |
|---|---|
| `GET /api/v1/session` | The actor, the explicit namespace grants and the capability flags. |
| `GET /api/v1/namespaces` | The configured grants. It never lists core `Namespace` objects. |
| `GET /api/v1/namespaces/{ns}/connections[/{name}]` | `KafkaCluster` projections: role, bootstrap addresses, auth mode, username, TLS, the credential Secret's **name**, and the controller's reachability observation. |
| `POST /api/v1/namespaces/{ns}/connections` | Create a `KafkaCluster` that references an existing credential Secret by name. |
| `GET /api/v1/cadence-previews` | What a cron expression — or a preset — will actually do in a time zone, before anything is saved. No namespace, no Kubernetes call. |
| `GET /api/v1/namespaces/{ns}/schedules[/{name}]` | `BackupSchedule` projections, with the cadence policy, the revision and the controller's own next runs. |
| `POST /api/v1/namespaces/{ns}/schedules` | Create a `BackupSchedule` from the whole policy: cadence and zone, a named or dynamic selection, an inline archive **or** a saved destination, deadlines, catch-up, retries and retention. |
| `PUT /api/v1/namespaces/{ns}/schedules/{name}` | Replace the schedule's **future** policy under `expectedGeneration`. It cannot name `spec.sourceRef` and it never reaches a run that already exists. |
| `POST /api/v1/namespaces/{ns}/schedules/{name}:set-suspension` | Suspend or resume, under `expectedResourceVersion`. |
| `GET /api/v1/namespaces/{ns}/backups[/{name}]` | `Backup` projections, with the trigger and the schedule revision the run copied. |
| `POST /api/v1/namespaces/{ns}/backups` | "Back up now" from a schedule, or "Run first backup now" from a cluster. |
| `GET /api/v1/namespaces/{ns}/restores[/{name}]` | `Restore` projections. Saved-destination restores carry the stored optional `sourceDestinationRef` and `evidenceDestinationRef`; legacy inline-archive restores omit both. |
| `POST /api/v1/namespaces/{ns}/restores` | Create a `Restore`, preserving the plan bytes exactly. An optional `topicMapping` declares the mapping the caller previewed and is checked against the prefix this request stores — see below. |
| `GET /api/v1/namespaces/{ns}/approvals[/{name}]` | Approval metadata and status, including — for a verified authorization document v2 — `authorization {mode, policyName, policyDigest, requester, confirmationKeyId}`. |
| `POST /api/v1/namespaces/{ns}/restores/{name}/approval` | PLAT-19.2: a governed approver submits the sidecar `logweir drill countersign` wrote over the console's confirmation (Approver role; never the requester); in an unbound namespace an approver records the two `logweir drill approve` files. See *Approval policy*. |
| `GET /api/v1/namespaces/{ns}/approval-policy` | PLAT-19.2: the namespace's effective approval policy, the installation document's digest and the console confirmation key id. |
| `GET /api/v1/namespaces/{ns}/approvals/{name}/packet` | The raw approval document, only through this explicit route. |
| `GET /api/v1/namespaces/{ns}/destinations` | `BackupDestination` rows: the canonical URL, the endpoint, the transport, the addressing and the controller's `Valid` verdict. |
| `POST /api/v1/namespaces/{ns}/destinations` | Create a destination **under the name in the body**, because every schedule, backup and restore references it by that name. |
| `GET /api/v1/namespaces/{ns}/destinations/{name}` | One destination, with the four grants as **references** and the last explicit access test. `lastTest.truncated` says the search for it hit its page bound. |
| `POST /api/v1/namespaces/{ns}/destinations/{name}:update-access` | Rotate the four grants and the CA reference under `expectedGeneration`. It cannot name the location or the transport. |
| `POST /api/v1/namespaces/{ns}/destinations/{name}:test` | Start a `DestinationAccess` `Preflight` for every configured grant; `202` with it. On a destination with `writeProbe: createOnlyMarker` the test creates the one readiness marker `logweir/readiness/<uid>.json` **as the `evidenceWrite` grant**, and `destination.evidenceWritable` is that principal's answer (its fact `grant` says `evidenceWrite` or `destination`); with `disabled` nothing is written and the row is `WriteNotProbed`. |
| `POST /api/v1/namespaces/{ns}/destinations:from-legacy` | Adopt a legacy `BackupSchedule` or `Backup`'s location. **This build always refuses** — see below. |
| `GET /api/v1/namespaces/{ns}/destinations/{name}/usage` | Schedules and backups labelled for this destination, at most 100 each, with the `basis` stated. |
| `GET /api/v1/namespaces/{ns}/connections/{name}/topic-discoveries` | One page of discoveries for a connection; `?latest=true` answers `{latestAttempt, lastSuccessful}` instead. |
| `POST /api/v1/namespaces/{ns}/connections/{name}/topic-discoveries` | Start a bounded inventory: `202`, or `200` with `reused: true` for a fresh identical result. |
| `GET /api/v1/namespaces/{ns}/topic-discoveries/{id}` | One discovery, with `visibility`, `counts`, `truncated` and a `stale` recomputed per read. |
| `GET /api/v1/namespaces/{ns}/topic-discoveries/{id}/topics` | One page of the stored inventory: `limit`, `cursor`, `q`, `prefix`, `internal`, `errored`. |
| `POST /api/v1/namespaces/{ns}/topic-discoveries/{id}:cancel` | Ask an unfinished discovery **of one's own** to stop. |
| `POST /api/v1/namespaces/{ns}/preflights` | Start a readiness check for a `Backup`, a `Restore` or a destination's grants. |
| `GET /api/v1/namespaces/{ns}/preflights/{id}` | One result; `?planHash=` is the plan the caller is looking at now. |
| `GET /api/v1/namespaces/{ns}/preflights/{id}/details` | One page of the check's detail document. |
| `POST /api/v1/namespaces/{ns}/preflights/{id}:cancel` | Ask an unfinished preflight **of one's own** to stop. |
| `GET /api/v1/namespaces/{ns}/operations/{kind}/{name}` | The normalized status; `kind` is the closed set `backup\|restore\|discovery\|preflight`. |
| `GET /api/v1/namespaces/{ns}/operations/{kind}/{name}/events` | The same thing as a bounded `text/event-stream`; `kind` is `backup\|restore`. |
| `GET /api/v1/namespaces/{ns}/protection-policies[/{name}]` | Protection health: the newest point that can actually be recovered from, how availability was decided, the objective, the failed and missed runs, each schedule's own readiness, and the alert ledger with its delivery state. |
| `GET /api/v1/namespaces/{ns}/rehearsal-schedules[/{name}]` | Recurring recovery rehearsals: the last pass, the last failure, the last **skip** with its reason, and the leftover topics that block the next slot. |
| `GET /api/v1/namespaces/{ns}/catalogs` | Recovery catalogs: ten verdict counts, the signer list, and whether the Kubernetes view is a window over a larger archive. |
| `POST /api/v1/namespaces/{ns}/catalogs` | Connect an existing archive: create a `RecoveryCatalog` **under the name in the body**, because every protection, rehearsal and retention policy references it by that name. |
| `GET /api/v1/namespaces/{ns}/catalogs/{name}` | One catalog. |
| `GET /api/v1/namespaces/{ns}/catalogs/{name}/points` | One page of the materialised point view, with availability and verification as separate columns. |
| `GET /api/v1/namespaces/{ns}/catalogs/{name}/signers` | The untrusted-signer panel: key ids, point counts, whether the bound policy accepts each one, and the out-of-band fingerprint command. |
| `GET /api/v1/namespaces/{ns}/retention-policies[/{name}]` | Retention: what the last evaluation would remove, what is **actually** enforcing it, which guarantees are in force and by whom, where the approved-plan gate stands, and whether enforcement has degraded. |
| `GET /api/v1/trust-policies[/{name}]` | The installation's trust policies. **Cluster-scoped** and administrator-only; `unknown` is not `valid`. |

### Approval policy: routing a submission, and the governed submission (PLAT-19.2)

The console reads the installation's approval-policy document —
`approvalPolicyFile`, **the same file the controller mounts** — and its own
`ConsoleConfirmation` private key — `confirmationKeyFile`, from a Secret. Both
are optional; a served namespace bound to a policy without the key is a startup
refusal (exit 2), because both modes carry the console's signature. The full
contract, the four enforcement points and upgrade/rollback are in
`docs/kubernetes.md` §8, *Approval policy*.

**`POST .../restores` answers where the submission goes next.** After the
Restore exists (so it has a UID) the response carries `authorization`:

| namespace | what the console does | `authorization.state` | the console routes to |
|---|---|---|---|
| unbound (`legacy-governed-v1`) | signs nothing | `awaitingApproval`, `legacy: true` | the approval page (today's flow) |
| bound `Ordinary` | signs authorization document v2 for this Restore's UID, plan hash, the authenticated requester and the policy digest, and stores it as the `Approval` `spec.approvalRef` names | `confirmed` | the operation view: weirkeeper admits the run once it verifies the confirmation |
| bound `Governed` | signs the same document and stores it as `<approvalRef>-confirmation`, which authorises nothing | `awaitingApproval`, `confirmationName` | the approval page, where an approver countersigns |

`authorization` also names `mode`, `policy`, `policyDigest`, `requester` and
`expiresAt`. A replay of the same request completes an interrupted sequence by
reading what exists, never signs twice, and never adopts an `Approval` this
Restore did not produce (`409 state_conflict`). In a Governed-bound namespace
`approvalRef.name` is at most 240 characters, so its confirmation name is still
an object name, and may not end in `-confirmation` (`422`, `reserved_suffix`),
which is where another Restore's confirmation lives.

Refused **before anything is created** (no Restore, no Approval):

| request | answer |
|---|---|
| an `Ordinary`-bound namespace in a **`localAdmin`** console — D0: that mode "does not expose Ordinary"; its one identity is the port-forward administrator, not a person a confirmation could attest | `409 policy_mismatch`; use the shared console. `GET .../approval-policy` says `ordinaryConfirmationAvailable: false` |
| a `Governed`-bound namespace without `ticket`, or a blank / padded / over-128-character one (D0: the ticket is "required in Governed") | `422`, field `ticket` (`required` / `invalid`) |
| `ticket` in an unbound namespace, which signs nothing (`logweir drill approve --ticket` carries it there) | `422`, field `ticket`, `not_accepted` |

`ticket` is optional under `Ordinary`; when given it is signed into the
document. `GET .../approval-policy` also answers `ticketRequired`.

**`POST .../restores/{name}/approval`** takes `{"sidecarBytes": "…"}` — the file
`logweir drill countersign --document <approvalBytes> --confirmation
<sidecarBytes> --key <privkey> --out approval.sig` wrote over the confirmation's
packet (`GET .../approvals/<approvalRef>-confirmation/packet`). No
`Idempotency-Key`: the Approval it creates is named by the Restore's own
immutable `approvalRef`, so a replay returns `200` with the same object.

**In an unbound namespace** (`legacy-governed-v1`, today's flow) the same route
takes `{"approvalBytes": "…", "sidecarBytes": "…"}` — `approval.json` and
`approval.sig` exactly as `logweir drill approve` wrote them over the Restore's
plan — and stores them byte-for-byte as the Approval `spec.approvalRef` names.
It verifies no signature (the Approval controller does, against the
namespace's `GovernedApproval` keys, and the runner again), but refuses early
what could never verify for this Restore: a sidecar whose payload type is not
the v1 approval's (`409 policy_mismatch`), a document whose `plan_hash` is not
the Restore's (`422 validation_failed`, `plan_mismatch`), or no document
(`422`, `required`). A v1 document names no requester, so the route's authority
is the Approver role plus the approver key's custody — exactly what `kubectl
create` required before. Under an explicit Governed policy `approvalBytes` is
refused (`422`, `not_accepted`): the confirmation's bytes are the ones signed.
An Ordinary namespace has nothing to approve (`409 policy_mismatch`).

| refusal | when |
|---|---|
| `403 forbidden` | not an Approver in this namespace (the role table), **or the submitting actor is the requester the console attested** — whatever other roles it holds; an administrator is not a bypass |
| `409 policy_mismatch` | the namespace is bound to an Ordinary policy, or the confirmation names another UID, plan or policy digest (the policy changed since: submit the Restore again), or — unbound — the sidecar is not a v1 approval's |
| `404 not_found` | no such Restore, or no console confirmation for it |
| `409 state_conflict` | the request has expired, or an Approval with other contents holds the name |
| `422 validation_failed` | not a sidecar, or it adds no signature beside the console's; or either field carries private-key text (`private_key` — refused on the server as well as by the page, and never echoed; D0 forbids a private key in any custom resource) |

The Approval it creates carries the confirmation's **exact** document bytes and
the console's signatures plus the approver's. The controller then verifies both
signatures, the approver key's `GovernedApproval` usage and that its
`principal.id` is not the requester's before any Job exists.

`backup` and `restore` answer an `OperationViewResponse` — a result, evidence
references and a verification verdict. `discovery` and `preflight` answer a
`CheckOperationResponse`, which carries **none** of those fields: a transient
check has no archive result and no signed evidence, and publishing an empty
`verification` for one would invite a console to render a verdict that can never
arrive.

`schemas/logweir-api-v1.openapi.json` is the generated contract. `just schema`
rewrites it and `just schema-check` fails on drift, as does
`crates/logweir-api/tests/contract.rs`, which compares the checked-in bytes with
the generator in-process.

`Backup` projections identify a saved archive with
`destinationRef: {name, uid?}` and publish the run's frozen
`locationDigest` copied from `Backup.status.destination`. The `name` is the
destination the immutable spec requested; the optional `uid` and digest are
facts recorded when the controller froze that destination, and are never
recomputed from the live `BackupDestination`. `destinationRef` is absent only
for a legacy inline-archive run. `locationDigest` is also absent when an older
controller (or a run not frozen yet) has not recorded the frozen destination;
that absence means "not recorded", not a location match.

### The normalized operation, and what is absent

`OperationView` is PLAT-17.1's `Operation` plus D3 §2.5's additions, flattened
into one object: the sixteen frozen fields keep their spellings and the new keys
sit beside them, so a client written against either shape reads the one it
knows.

**An absent field means "not observed", never a default that flatters the
object.** The rules, exactly:

- `progress` is absent on anything an older controller reconciled, and that
  absence is why `queued` and `preparing` are **never inferred**. They are
  distinctions only the progress channel can draw — a Job with no pod, against
  a pod whose container has not started — and guessing them from
  `phase: Pending` would publish an observation nobody made.
- `stage` is absent when no stage was written and the phase is one this build
  does not recognise. A `Resolving` backup (D1's dynamic discovery) is
  `preparing`; anything unrecognised is `unknown/UnrecognizedPhase`.
- A stage **never overrides a terminal phase.** `phase`, `exitCode` and
  `outcome` own the outcome; the progress channel answers "what is happening
  and why is it taking so long".
- `trust.basis` is `None` when the status carries no `trust` block: nothing has
  been compared with anything. The spelling is the CRD's own
  (`Current`, `Historical`, `RecordedBeforeRevocation`, `Unverified`, `None`),
  which is D3 §12's sentence word for word, and an unrecognised value is passed
  through rather than rounded to the nearest word this build knows. A basis
  refines an already-safe result and never rescues an unsafe one — no basis
  string turns a result that is not `Valid` into a verified state, and `Valid`
  beside `basis: Unverified` is `notAttempted`.
- `trust.state` is `verified`/`verifiedHistorical` exactly where the
  controller's badge may be green: `Valid` with no `trust` block (the
  pre-existing rule), `Valid` on `basis: Current` (`verified`) or
  `basis: Historical` (`verifiedHistorical`), and nothing else. `Valid` on
  `basis: RecordedBeforeRevocation` is **`untrusted`** — D3 §7.4: a verdict
  recorded before its key's compromise revocation is "never green" — and so is
  `Valid` beside a `trust` block whose basis is `None`, absent, or a word this
  build does not know. A recorded `result: Untrusted` is `untrusted` whatever
  the key state beside it; `NotAttempted` beside `basis: Unverified` is
  `notAttempted` (no verdict has been reached). Builds before
  `TRUST-STATE-RBR-VERIFIED` published `verified` for
  `RecordedBeforeRevocation`; a client that must read older servers checks
  `trust.basis` as well.
- `verificationScope.level` is `sampled` (`byte-fingerprint`), `degraded`
  (`consume-only`) or `none`. **`complete` does not exist in v1.** A Backup is
  always `none` with the three counts **absent** rather than zero: a receipt
  attests counts and a window, not a restore, and `0 of 0 sampled records
  matched` reads as a failed comparison.
- `stale` is `true`, and the state is `unknown/StatusStale`, when an active run
  has not been observed for 300 s or an object has carried no status for 120 s
  after creation. A **terminal** object never goes stale: nothing is going to
  observe it again.
- `readiness` is always `{state: "unknown", basis: "notImplemented"}` on an
  operation in this build: readiness is served by the `Preflight` routes
  ([Readiness that is a result about something](#readiness-that-is-a-result-about-something)),
  and the operation projection does not join one. It is a separate object and
  never overwrites `state`.
- `targetMode` (`scratch` or `newTopic`) is on the operation and not on the
  completion panel: D3 §3.5 keys its two fixed guidance blocks on
  `spec.target.mode`, which is a fact about the run from the moment it is
  created. It is absent on a Backup.
- `progress.runnerPhase` is the CRD's own name for the runner's
  `progress-phase=` lines. `teardown.deleted` and
  `lastEnforcement.deleted` are **lists of names**, not counts: the incident
  question is which topics or points went, and a count cannot be reconciled
  against the plan or against the names the run created.
- **A property the projection can omit is never `required` in the schema.** A
  list that is absent when empty must not be declared required, or a client
  validating against the document refuses a body the server considers correct;
  `a_required_property_is_never_omitted_by_its_own_projection` round-trips each
  view through its own schema to keep the two honest.

The Job name, the pod name and the container state are **not** published.
D0's "remains visible in bounded form" list is reason, message, exit code, last
phase, timestamps and evidence references, and a Job name is on none of them; a
diagnostic's `object {kind, name}` **is** published, because PLAT-14.1 asks for
pod mount and scheduling failures as resource-scoped errors. The server authors
no prose: `verificationScope` carries the level and the counts, and the fixed
sentences are the console's.

### The event stream

`GET .../operations/{kind}/{name}/events` is `text/event-stream` on the same
origin. Event types are `operation` (the whole view), `reset` (the whole view,
after a resume this service cannot replay), `heartbeat` and `end`.

**Every bound is the server's.**

- **No query parameter is accepted.** `EventSource` cannot set headers and the
  standard workaround is `?access_token=`; a URL is the one place a credential
  survives in a proxy log, a browser history and a `Referer`, so this route
  answers `malformed_request` to any parameter at all. The stream is
  authenticated by the same session cookie as every other route.
- Authorization is decided **once, at subscribe**, before the first Kubernetes
  call: the namespace grant, `operation.read` and `operation.stream`. The audit
  record is written then, which is when the decision was made.
- The first read is **synchronous**: a missing object is `404`, not a stream
  that opens and says nothing.
- The event id is the object's `resourceVersion` — the same number the read
  route publishes. A `Last-Event-ID` that is not a bounded decimal is
  `malformed_request`.
- **Resuming is either silence or one `reset`.** This service keeps no history
  of resourceVersions, so it cannot replay what happened between two of them. A
  client already at the current version is sent nothing; any other value gets
  one `reset` carrying the whole current state.
- The connection closes after 300 s (`end`, `reason: maxDuration`); reconnect.
  A silent stream sends a `heartbeat` every 15 s. A terminal operation **whose
  verification has settled** ends immediately (`reason: settled`) — a run that
  exited 0 with its verdict still pending keeps the connection, because the
  verdict is what the console is waiting for. A deleted object ends the stream
  with `reason: vanished`.
- Concurrent streams are capped per principal per namespace; past the ceiling
  the answer is `rate_limited` with `Retry-After`, and the advice is to fall
  back to polling the read route. A slot is released when the connection drops.
- The object is **polled**, not watched: this service has no `watch` verb and
  its RBAC does not gain one.

### Protection, rehearsals, catalogs, retention and trust

Five bounded read families, the same cursor as every other list, and one write
among them.

**No response carries a credential, and that shapes the projections.** A
`ProtectionPolicy`'s notification routes carry `secretKeyRef`s; the projection
publishes each route's **name** and which channels it has as booleans, because
a webhook URL is a bearer token with a hostname on the front and the name of
the Secret holding one is what a reader needs to decide what to read next. A
`RetentionPolicy`'s enforcement block names the one delete-capable credential in
the installation; the projection publishes `credentialConfigured: true` and not
the Secret's name.

**Two rules, and which applies where.** An *archive's* credential Secret is
published by NAME — `legacyArchive.credentialRef` on a catalog and on a
protection policy, exactly as `destinations` has published `credentialRef` since
PLAT-08 — because the name is part of the adoption contract: an operator types
it into the connect form and the console echoes back the reference it stored. An
*alert sink's* and a *deletion credential's* names are not published at all,
because nothing echoes them back and naming them only tells a reader which
Secret to try next. Every archive URL is published with its userinfo redacted,
wherever it appears. A `TrustPolicy`'s keys carry `spkiPem`; the projection
publishes the **key id** — the SHA-256 of the DER SPKI, the number `openssl`
prints — so an operator compares fingerprints out of band, which is the
supported path.

**Retention says what is actually enforcing.** `enforcement` is
`RecommendationOnly`, `LogweirWorker` or `ExternalLifecycleDeclared`, and
`guarantees` says per guarantee whether Logweir enforces it, a provider is
*declared* to (unverified), or nothing does. `approvedPlanState` is the
two-step gate in one value — `notApplicable`, `notRequired`, `noPlan`,
`awaitingApproval`, `approved`, `expired` or `unknown` — and it is **`unknown`
whenever an input is absent**, never `approved`. Nothing in `lastEvaluation`
was deleted. The enforcement record is create-only and **unsigned**, verified by
the digest beside it; no surface calls it signed.

**A catalog's view is a window, and the response says so.** The durable truth is
in object storage; Kubernetes holds the newest `sync.viewLimit` points in
immutable `ConfigMap` pages owned by the sync Job, and when that Job's TTL
collects it the pages go with it. So `/points` publishes `truncated`,
`viewExpired` and `viewExpiresAt`, and an empty list with `viewExpired: true`
means the window aged out and **not** that the archive is empty. If a named
page has already disappeared before the status is refreshed, `/points` returns
`incomplete: true` (and may retain rows read before that page); callers must
not report an absent row as "not in the catalog". Page
`ConfigMap`s are read by the names the catalog's own status records — never from
a caller — at most eight per request, and each one is refused with
`result_integrity_failed` unless it is immutable and its bytes match the
recorded digest. The point cursor binds the **view generation**: a re-sync under
a paging client is `cursor_invalid` with "restart the list", never half of one
view and half of another. `availability` and `verification` are separate
columns — one is an outage and one is a stranger's signature — with the
controller's own `selectable` conjunction beside them.

**A verdict the controller reached outranks the row.** Each `/points` request
also lists the namespace's `Backup` objects (at most 2 000) and joins their
`status.evidence.verification.result` to the rows — by the full
`receiptSha256`, or by `backupId` for a `Backup` that carries no digest. A point
whose `Backup` the controller refused (`Invalid`, `Untrusted`, a result this
build does not recognise, or a `Valid` on a trust basis the badge refuses) is
published `selectable: false` with the refusal in the additive `backupVerdict`
field, and `?selectable=true` does not list it — however `Available`/`Verified`
its row reads, because a view is served until `viewExpiresAt` and the row may
predate the refusal. A `Valid` counts as a pass only beside no `trust` block (or
`trust: null`) or a `Current`/`Historical` basis, the controller badge's own
rule (D3 §7.4, §12); a `Valid` on any other basis — `RecordedBeforeRevocation`,
`None`, a block with no basis, a word this build does not know — is a refusal
and is published as `backupVerdict: "Untrusted"`, even though the `Backup`
itself still reads `result: Valid`. `NotAttempted`, `Pending` (the evidence
fetch is still running), an absent result, a passing `Valid`, and a `Valid` on
`trust.basis: Unverified` (nothing has been compared yet) leave the row in
charge; `backupVerdict` is absent then, and absent never means "verified".

**The join degrades per object, never per page.** `Backup` objects are read
through a lenient projection of the three fields the rule needs, so one object
this build cannot type (a newer trigger kind, an older stored schema) does not
fail the list. A `Backup` whose verdict field is present but unreadable refuses
its own point with `backupVerdict: "Unreadable"`. When the verdicts could not
all be read the page is still served and carries the additive
`backupVerdictsIncomplete`: `Truncated` when the namespace holds more `Backup`s
than the bound, `Unavailable` when the `Backup` list failed (transport, RBAC, a
missing CRD) or a refusal named neither a digest nor a set id and so could be
tied to no row. Rows then reflect only the verdicts that were read. The page is
served either way because the point list is what an operator reads to choose a
point; the Restore reconciler and the runner re-verify the point before any
data-plane work. Both fields are optional additions: a client that ignores
them still reads the corrected `selectable`. The `?selectable=true` cursor is
bound to the view generation and not to the refusal set, so a refusal recorded
between two page requests can shift that filtered list by one point; restart
the list to see it exactly.

**`/signers` offers no button.** It publishes the key id, the point count and
whether the bound policy accepts it, with the fingerprint command. There is no
"trust this key" route in v1: a key found beside an archive is never trusted by
proximity, and one-click trust is proximity with a confirmation dialog on it.
An administrator adds the key with `kubectl apply`.

**Trust: `unknown` is not `valid`.** A key's `effectiveState` is the
controller's verdict only when the evaluation is **fresh** — the object has a
status, that status was computed from the current `metadata.generation`, and
`evaluatedAt` is younger than fifteen minutes. Otherwise it is `unknown` with no
usability verdict. Freshness is measured against **this server's** clock, and
the response publishes `evaluation.serverTime` beside `evaluatedAt` so a reader
can check the arithmetic instead of redoing it against a clock the cluster never
saw. **The cluster-scoped read is narrowed, and the narrowing is a deviation that is
written down.** D0's matrix row for installation trust is "installation-admin
only, cluster scope", and this authorization model has no installation-scoped
role: every binding is `(role, namespace)`. So `GET /api/v1/trust-policies`
requires the administrator role in at least one bound namespace **and** serves
only policies that govern a namespace the reader administers or are the
installation `default` — anything else is `404`, the same answer a policy that
does not exist gives, because "it exists and you may not see it" is the
enumeration being closed. Inside a policy that is served, `namespaces`,
`boundNamespaces` and `conflicts[]` are filtered to the namespaces the reader
administers, and `namespacesFiltered` says when a list is partial. Without that
filter an administrator of one namespace reads the name of every other
namespace in the installation off the one object that has no `404` to hide
behind. The keys view §7.7 needs is unaffected.

`capabilities.trustPoliciesRead` is computed per namespace like every other
flag, but the route it names is cluster-scoped: **a console must read it from
the union of the grants, not from the namespace it happens to be showing.** An
actor who administers `team-b` and only views `team-a` would otherwise be shown
`false` for a page the server serves. Trust **writes** stay off the API in v1
(`capabilities.trustAdministration` is `false` for everybody, including the
local administrator); the supported path is `kubectl apply` plus the
`logweir trust` helpers. `capabilities.catalogWindowQuery` is `false` for the
same reason: an absent capability, never a fake stub.

There is no cancel and no delete for `Backup` or `Restore`: their external side
effects and cleanup semantics are not defined yet. Aborting a read cancels only
that HTTP and Kubernetes read; it never retracts an accepted mutation.

**A transient check is the one thing that can be cancelled.** A discovery and a
preflight own nothing but a Job, so `:cancel` raises `spec.cancelRequested` from
`false` to `true` — the single transition the CRD permits — under a
`resourceVersion` precondition, and the controller verifies the exact owned Job
and UID before stopping anything. Cancellation deletes no archive byte, no Kafka
topic, no durable run and no signed evidence. Repeating it is `200` with nothing
written; cancelling a finished check is `200` with `alreadyTerminal: true`. An
operator may cancel only the checks **it started**: the comparison is the
`issuer#subject` recorded on the object when it was created, so two operators
holding the same role in the same namespace still cannot stop each other's work.
**An administrator of the namespace may cancel any check in it.** That is the
one place a role overrides ownership, and it is deliberately narrow: D0's
"cancel own checks" is written in the operator row, cancelling destroys nothing,
and an administrator who could not stop a twenty-thousand-topic discovery an
operator started before going home would have to wait out `timeoutSeconds` or
reach for `kubectl`. The audit line records `cancelledAnotherActorsCheck` when
that path is taken, so the two events are never confused.

### Cadence, time zones and previews

`spec.schedule` — five cron fields, the controller's own parser — stays the
single source of truth. `spec.timeZone` is an IANA name and **absent means UTC
and reproduces, instant for instant, the slots a controller without the field
computed**. A slot's identity is always the UTC instant, so names stay unique,
monotonic and DNS-1123 whatever the zone.

`GET /api/v1/cadence-previews` answers what an expression will do, and the
browser never evaluates cron: a second implementation is a second answer, and
the one that matters is the controller's. This route calls the same
`weirkeeper::cadence` module the scheduler calls.

* `?schedule=<cron>` **or** `?preset=<kind>` with that preset's parameters —
  exactly one. With a preset the answer carries the canonical expression it
  compiled to, which is the string a form saves; with an expression it carries
  the preset the expression **is**, or nothing at all for "Advanced cron".
* `?timeZone=`, `?count=` (1 to 20, default 10) and `?after=` (RFC 3339,
  default the server's now).
* `200 {schedule, preset?, timeZone, tzdb, after, runs:[{at, localTime,
  adjustment?}]}`. `timeZone` is the **effective** zone, so `UTC` comes back
  when nothing was sent. `tzdb` names the compiled-in database, because two
  releases can disagree about a slot after a tzdata update.
* `422 validation_failed` with `schedule: schedule_invalid` or `timeZone:
  timezone_unknown` — the field the person typed, not a single code for both.

**`adjustment` is the part to render.** `NonexistentLocalTimeShifted` means the
matched local time does not exist and the run was moved to the end of the gap;
`RepeatedLocalTimeFirst` / `RepeatedLocalTimeSecond` mean the matched local time
happens twice and **both instants fire**. A fixed-time schedule inside a
repeated hour therefore runs twice that night — deliberately, because the rule
never loses a real interval and never skips a fixed-time day, and the preview is
where that becomes predictable. `localTime` carries its offset for exactly this
reason: `02:30:00+02:00` and `02:30:00+01:00` are one clock face and two
instants.

A **shorter list than `count` is a real answer**, not a failure: `0 0 29 2 *`
runs out of firings inside the engine's walk, and an empty list means "it does
not fire again".

A saved schedule's own previews are `status.nextRuns`, written by the controller
from the same function and published in the same shape by
`GET .../schedules/{name}`. **`status.nextRuns[0].at` in the past is the
staleness signal**; `status.policy.evaluatedAt` is *when the status last moved*
and not a liveness probe — the controller re-examines every schedule every 30 s
and deliberately writes nothing when nothing changed, so comparing that instant
with the requeue interval would report a healthy schedule as stale. An absent
`status.activeRuns` means "not yet computed", never "none are running".

### Creating a schedule

`POST .../schedules` takes the whole policy, and has since PLAT-10.1: the same
field set `PUT .../schedules/{name}` takes, minus `expectedGeneration` and plus
`sourceRef`, which is settable exactly once. Before that it took seven fields —
`schedule`, `sourceRef`, `topics`, an inline `archive`, `concurrencyPolicy`,
`retention` and `suspended` — so a
console could *edit* a schedule into a shape it could not *create*, and the only
way to a saved destination, a dynamic selection, a zone, a catch-up policy or
retries was a second request against an object that was already admitting slots
under a policy nobody had asked for.

* **`archive` xor `destinationRef`.** Exactly one, as on the edit route, and the
  sentinel `archive.url` (`logweir-destination://<name>`, no `secretRef`) is
  built by the route and never accepted from a body.
* **`topics` xor `allUserTopics`**, spelled flat rather than under a
  `topicSelection` object: this route has carried a top-level `topics` since
  PLAT-17.1 and moving it would break every existing caller to gain a nesting.
  Both routes validate through one function, so an empty selection, a glob, a
  duplicate and an exclusion pattern are refused here with the codes and
  messages the edit route uses, on `topics`, `topics[i]` and
  `allUserTopics.exclude.prefixes[i]`.
* **`timeZone`, `startingDeadlineSeconds`, `catchUpPolicy`, `retry`,
  `activeDeadlineSeconds`, `retention` and `concurrencyPolicy`** are all
  optional, and absent each means exactly what the edit route's documentation
  says it means: UTC, 3600, `None`, no retries, 3600, no evaluation, `Forbid`.
* **The cadence is parsed in the zone it will be stored with**, by the
  controller's own parser and tz database, so an unknown zone is `timeZone:
  timezone_unknown` here rather than `Ready=False`/`UnknownTimeZone` on an
  object this route said was fine.
* **A pre-PLAT-10.1 body creates exactly what it created before.** Every added
  field is optional and an absent one is written absent — never a default this
  route invented — so an existing caller's stored spec, and therefore its run
  policy digest, is byte for byte what it was. **Its idempotency request hash
  is unchanged too:** the added members are omitted from the canonical
  request when absent, so an old-shape create retried under its old
  `Idempotency-Key` after an upgrade replays onto its object rather than
  answering `409 idempotency_conflict`.
* **One refusal code changed for old callers.** An empty `topics` (with no
  `allUserTopics`) was `topics: count_out_of_range` ("between 1 and 256 named
  topics are required") and is now `topics: selection_invalid` ("a run needs
  either named topics or allUserTopics"), the edit route's code for the same
  condition. More than 256 named topics is still `count_out_of_range`.
* The object's name is minted from the idempotency scope (`sch-<26 base32>`), as
  it always was, and `Idempotency-Key` is what makes a double click, a lost
  response and a reload one schedule rather than three.

### Editing a schedule's future policy

Every `BackupSchedule.spec` field is editable **except `sourceRef`**: a
schedule's identity is the cluster it protects, and one schedule's history must
not mix two clusters.

**`generation` is always emitted and declared optional.** `GET
.../schedules[/{name}]` returns `generation` on every schedule this build
serves — the API server sets `metadata.generation` on every object — but the
published schema marks it optional, because `ui/contract.js`'s decoder drift
test compares the schema's `required` set with a frozen console-side list, and
moving a field into that set is a change that must land in the same commit as
`ui/contract.js` and the console fixtures. **That commit is D1 W7's**, and it
should make the field required. Until then: **a console that somehow sees
`generation` absent must ask the person to reload, never default it.** Sending
`expectedGeneration: 0` is a `412` nobody can explain.

`PUT .../schedules/{name}` takes the **whole** policy — `expectedGeneration`,
`schedule`, `timeZone?`, `topicSelection`, `archive` xor `destinationRef`,
`concurrencyPolicy?`, `startingDeadlineSeconds?`, `catchUpPolicy?`, `retry?`,
`activeDeadlineSeconds?`, `retention?`, `suspended` — and a field omitted is
**removed**. That is what makes "absent means the documented default" reachable
from a form: a schedule edited back to no retries really has no `spec.retry`,
rather than a stale one nobody can see and the scheduler still obeys.

* **It changes the future and nothing else.** A `Backup` that already exists
  keeps its copied policy, its frozen execution inputs and its Job. The edit's
  entire footprint is one merge patch on one `BackupSchedule`.
* **"Omitted is removed" covers the spec fields THIS BUILD knows.** The patch
  names every mutable key of `BackupScheduleSpec` as this binary declares it,
  so a key a *newer* CRD added is not in the patch and a merge patch leaves it
  in place. That window is real and documented: the upgrade order is CRD →
  controller → API (D1 §5.7), so between the CRD rollout and the API rollout an
  operator editing through the old console **preserves** a newer CRD's fields
  rather than removing them. This is deliberate — removing keys a build cannot
  name would be far worse than keeping them — and the response reads the
  patched object back, so a preserved field is *visible* rather than silently
  retained. Finish the API rollout before relying on replace semantics for a
  field the CRD has just gained.
* **`expectedGeneration`, not `expectedResourceVersion`.** `metadata.generation`
  is what a person can see and reason about ("revision g7"), it moves only when
  the spec changes, and it is what a manual run records. A different current
  generation is `412 precondition_failed`. The write itself still carries the
  `resourceVersion` of the read it was decided on, so an edit that lands *between*
  the read and the write is refused by the API server and answered `412` as well
  — the two preconditions are one promise: nobody's edit is silently overwritten.
  *(D1 §5.6 specified `Api::replace` with `expectedResourceVersion`; a typed
  merge patch was chosen instead. It keeps the Kubernetes verb at `patch`, which
  the console ServiceAccount already holds for `:set-suspension`, so the edit
  needs no new RBAC at all.)*
* **`sourceRef` is refused before anything is read**, as `422
  validation_failed` with `sourceRef: field_immutable` and the CRD's own
  sentence. It is on the request DTO *only* so that the refusal is a field error:
  a console that writes back everything it read sends `sourceRef`, and "unknown
  field" would not tell the person that the answer is "create a new schedule".
* **The CRD's own rules are not copied here.** `allUserTopics` together with a
  non-empty `topics`, and retries on a schedule name longer than 29 characters,
  are the API server's to refuse — a second copy of a CEL rule drifts from the
  schema, and a stored object can break a rule this build has never heard of. The
  refusal comes back as `422 validation_failed` carrying the CRD's published
  message — never the API server's text — on one of three field codes:
  `topicSelection: selection_invalid`, `retry.maxRetries: schedule_invalid`, or
  `destinationRef: destination_sentinel_mismatch` when `archive.url` and
  `destinationRef` disagree. **The sentinel code is deliberately not
  `field_immutable`**: it is a shape rule, and telling a person their
  destination cannot be changed would contradict the line above. A refusal
  whose message this build does not recognise stays a generic `422` whose
  reason is in the service log — naming a rule it cannot identify would be a
  guess.
* **What *is* checked before the write** is what no rule can catch: an
  unparseable expression, an unknown zone, an empty selection, a glob in a topic
  name, a range. The cron parser and the zone table are the controller's own, so
  an expression this route accepts cannot leave `Ready=False` on an object the
  console said was fine.
* **`Idempotency-Key` is refused.** An edit is not a durable create and cannot
  replay: the same body sent twice under the same `expectedGeneration` is `412`
  the second time, because the first one moved the generation. That *is* the
  idempotence.
* `destinationRef` is mutable. The sentinel `archive.url` the CRD requires
  (`logweir-destination://<name>`, with no `secretRef`) is **built here and never
  accepted from a body**: a client that could type that URL could also type it
  without a `destinationRef`, which is the reserved-scheme case the rule exists
  to refuse.

**Who may edit.** D0's role matrix gives an operator "create and set suspension"
on schedules and says nothing about editing, because PLAT-05.1 had not made a
schedule editable when it was written. **This build's decision: editing a
schedule's future policy is the same authority as creating one** — an operator
(and an administrator) may do it in a bound namespace, a viewer and an approver
may not. It reaches exactly the fields `POST .../schedules` already sets, it
cannot reach `spec.sourceRef`, and it cannot touch a run that already exists, so
it grants nothing a create did not. The audit record says `schedule.editPolicy`
rather than `schedule.create`, and the capability flag a console reads for both
buttons is `scheduleCreate`; `tests/role_matrix.rs` pins that the two actions
have the same role row, so splitting the flag and splitting the authority have
to happen together.

### Back up now

`POST .../backups` creates the canonical manual `Backup` — the same object
`kubectl create -f config/samples/backup-manual.yaml` creates. `trigger.kind:
Manual`, `triggeredBy: manual`, no slot, attempt 0, and an execution id that is
the object's own UID, so it can never collide with a scheduled run's
`<scheduleUID>-<slot>` and can never append into a partial archive.

**Two bodies.**

* **From a schedule** — `{scheduleRef: {name, expectedGeneration?},
  readinessAcknowledgement?}`. The API reads the schedule and copies its current
  revision with the *scheduler's own* policy builder, recording `scheduleRef
  {name, uid, generation, runPolicySha256}`. A policy field sent beside
  `scheduleRef` is `422`: "Back up now on this schedule" promises the run the
  schedule describes, and a run whose receipt named the schedule and whose
  contents were something else would be the wrong thing written down.
* **Ad hoc** — `{sourceRef, topicSelection, legacyArchive xor destinationRef,
  deadlineSeconds?, readinessAcknowledgement?}`, for a cluster with no schedule
  yet. It reads nothing and records no `scheduleRef`.

**Idempotence is the name.** `Idempotency-Key` is required; the object is named
`logweir-manual-` plus 26 base32 characters of `sha256(issuer, subject,
namespace, route, key)`. A double click, a lost `201` and an API restart all
target the same name, so the API server's own `AlreadyExists` is what makes a
second run impossible. Same key and same body is `200` with `replayed: true` and
the same UID — **including after the schedule has been edited in between**,
because the request hash covers the body as sent and the replay returns the run
that was created rather than a fresh copy of today's policy. Same key and a
different body is `409 idempotency_conflict`. A new key is a new, deliberate run.

**The answer names the run.** `201` (or `200` with `replayed: true`) carries
`{requestId, replayed, item, schedule?}` — the same `{requestId, replayed,
item}` envelope every create route answers with, plus `schedule` for a
from-schedule run. `item` is the created `Backup`'s projection, so the run's
identity is `item.name`, `item.namespace`, `item.uid` and
`item.resourceVersion`, read from the object the API server stored (on a
replay, the stored object — never a rebuild). A console names and links the run
from `item`; there is no second, top-level copy of the name. The idempotency
record hashes the request as sent, never the response, so the answer's shape is
not part of what makes a replay a replay.

**The schedule's state never blocks the run** (and the response says so instead
of hiding it). A suspended schedule stops future *slots*, not people: the run is
created, `spec.suspend` is untouched, and `schedule.suspended` comes back `true`.
A scheduled run already going under `concurrencyPolicy: Forbid` neither blocks
the manual run nor counts it — `concurrencyPolicy` is about slots, and this is
the CronJob "run now" precedent — and the active runs come back as a
non-blocking notice. A schedule deleted between the request and the freeze still
produces a run, because the controller does not read a `BackupSchedule` for a
manual run at all.

**`409 policy_changed`** is the one refusal that is about the schedule:
`expectedGeneration` named a revision that has been superseded. The problem
document carries a `policy` extension member — `{currentGeneration,
currentRunPolicySha256?}` — so the console can show what changed; confirming
starts a new idempotency intent, because running a policy the person did not see
is not what the button promised.

**Readiness is recorded and never obeyed.** This API does not call a preflight,
does not wait for one and does not refuse on one; execution-time guards stay the
authority and the direct `kubectl` path exists regardless. A
`readinessAcknowledgement {preflight, state}` becomes the annotation
`logweir.dev/readiness-ack` and nothing more. **A console must not render this
route's acceptance as a readiness verdict**: if it wants a "Run anyway"
confirmation it implements one against `POST .../preflights`, whose result is a
real check — and a `ready` verdict still does not mean the execution-only checks
passed. There is no preflight this route consults and none it can fake.

### The restore's declared topic mapping

The create response and both restore reads preserve the optional saved
destination references stored on the `Restore`. They are metadata, not access
material: the response publishes destination names only, never Secret contents.
Their absence remains the legacy inline-archive shape.

`POST .../restores` takes an optional `topicMapping`: the exact source/target
rows the caller previewed.

```json
{"topicMapping": [{"source": "orders",   "target": "restore-20260907T140500Z-orders"},
                  {"source": "payments", "target": "restore-20260907T140500Z-payments"}]}
```

**It is a declaration and it is never stored.** `Restore.spec` has no topic
list — the subset lives in the opaque plan bytes
(`logweir_core::spec::SourceSpec::topics`) — so nothing here is persisted and
the created object is byte-for-byte what it was before the field existed.

**It is defined for `target.mode: newTopic` only.** `logweir_core::spec::target_topic_prefix`
reads `target.topic_naming.prefix` for `newTopic` and the **plan's own**
`topic_mapping_prefix` for `scratch` — and this route never parses the plan, so
in `scratch` mode it does not hold the string the run would map through. A check
against `topicNaming.prefix` there would be a verdict about a value the runner
does not read: it would pass a declaration that disagrees with the run and fail
one that agrees with it. This service does not check a value it does not have,
so a declaration with `mode: scratch` is refused `topicMapping` /
`unsupported_for_mode`. In that mode the rails are the console's own preview and
the runner's phase 0.

**For `newTopic`, what it buys is a rail this service can check without parsing
the plan**, and the service still parses nothing. The mapping rule is prefix
concatenation and nothing else — there is no per-topic rename in the grammar —
and `target.topicNaming.prefix` **is** a stored field. So `prefix + source` is a
pure function of the object about to be created, and a request whose preview and
whose submission disagree is refused here rather than discovered in phase 0,
after an approver has signed.

| `errors[].field` | `errors[].code` | when |
|---|---|---|
| `topicMapping` | `unsupported_for_mode` | `target.mode` is `scratch`. The declaration is defined for `newTopic` only — see above. |
| `topicMapping` | `empty` | the field is present with no rows. Omit it to declare none. |
| `topicMapping` | `too_many` | more than 1000 rows. |
| `topicMapping[i].source` | `invalid_topic` | the source is not a name a broker accepts (`^[a-zA-Z0-9._-]{1,249}$`). |
| `topicMapping[i].target` | `mapped_name_illegal` | the mapped name is not one a broker accepts. The message names the source. |
| `topicMapping[i].target` | `mapping_identity` | the target equals its source — a restore writing over the topic it came from. |
| `topicMapping[i].target` | `mapping_mismatch` | the target is not `prefix + source`. The message names the target that prefix produces. |
| `topicMapping[i].target` | `duplicate_mapping` | two rows map to one target name. With an injective prefix map that is a repeated SOURCE, so the message names **both** rows and the target they share. |

The mapping is checked only when `target.topicNaming.prefix` is itself legal:
a mismatch computed from a refused prefix would name an expected target nobody
could produce, and would send the operator to the wrong field.

**Absent is exactly the behaviour this route had before the field existed**, and
an absent declaration is left out of the idempotency request hash, so a client
that predates it replays onto the same object it always did.

### Saved destinations

`spec.storage` and `spec.transport.security` are immutable — a different
location or transport is a different destination — so the create route
evaluates D2 §3.2's rules *before* the object exists and answers `422
destination_invalid` with one field error per rule broken (`bucket_invalid`,
`endpoint_not_origin`, `transport_scheme_mismatch`,
`insecure_http_requires_http_endpoint`, `ca_bundle_requires_tls`,
`prefix_reserved`, `addressing_unsupported_by_engine`, `grant_mode_fields`).
**Addressing is never a transport choice** in either direction: path-style and
virtual-hosted say how a request names the bucket, and only the endpoint's
scheme has to agree with `transport.security`.

The namespace's **default** destination is marked with both the annotation D2
§3.1 names and a label, and the at-most-one check is a single bounded label
read; a second `default: true` is `409 state_conflict`. A default set by hand
with only the annotation is still honoured on read, but is not seen by that
check.

`:update-access` carries `expectedGeneration`; a stale value is `412
precondition_failed`, and a rotation the API server rejects as invalid is `409
destination_location_immutable`. A CA bundle on a plaintext destination is `409
transport_downgrade_forbidden`, because the transport can never be changed and a
CA means nothing without TLS.

**Adopting a legacy location needs a source that recorded it, and this build has
none.** A legacy `archive.url` carries the bucket and the prefix. A destination
also needs the endpoint, the region, the addressing and the transport — and an
installation whose runs used MinIO over a custom endpoint with path-style
addressing writes *exactly the same URL* as one that used AWS S3. The two places
that record the difference are the frozen `execution-inputs.json` of a succeeded
`Backup` (PLAT-06.1) and the installation's legacy addressing in the policy
`ConfigMap` (D2 W11); the adapter reads a `ConfigMap` only when a **check** owns
it, and neither of those is a check result. `locationDigest` — which restore
selection and catalog indexing compare — is computed over exactly the fields
that would have to be guessed, so a guess does not produce an approximate
destination, it produces a different one that looks derived from facts.

So `:from-legacy` takes D2 §3.12's third branch: it validates the request, reads
the legacy object, and answers `404 legacy_location_unknown` naming the bucket
and prefix it *did* recover so you can paste them into an explicit `POST
.../destinations` with the endpoint your runs actually used. **Adoption from the
installation config arrives with W11**, and `addressingSource` — the field that
will say which source a derived location came from — is deliberately absent from
the response until a route can fill it honestly.

**Write-only credential entry.** A grant may carry a value once, in
`access.<role>.secret.new`. It becomes a Secret named
`lwd-<destination>-<role>`, of type `logweir.dev/object-store-credential`,
labelled `logweir.dev/credential-for`, owned by the destination — and it is
never read back. This service has no Secret read verb at all; the create
response's `data` is dropped by the parser before anything can see it, and every
response, log line and stored projection carries the Secret's **name** and the
**key names** inside it, both of which are public references.

**Entering a value twice for the same role is `409 state_conflict`, and
nothing at all is written.** The Secret's name is a function of the destination
and the role, so a second `secret.new` names an object that already exists —
and this service holds `create` and nothing else: it cannot read the existing
value to compare it, and it cannot overwrite it. Answering `200` there would
let an operator responding to a leaked key record a rotation that did not
happen while the leaked value stayed live.

So **every credential name a request would write is checked before anything is
written** — before the Secrets, and before the `BackupDestination` itself. The
check needs no read verb: a `dryRun: All` create is still the `create` verb,
and it answers `AlreadyExists` for a taken name while telling the caller
nothing about the object holding it. The probe carries an empty `data`, so the
value you typed does not travel to the API server before the decision to write
it has been made. The refusal names **every** taken name, not the first, and
nothing is created: not the destination, and not the credential of some other
role that happened to come earlier. To rotate, change the Secret's content out
of band with `kubectl` or your secret manager, or point the grant at a
different existing Secret with `secret.existing`.

That ordering is what makes the retry below safe. A first attempt that refuses
leaves no destination behind, so the retry cannot look like a replay of one —
and a Secret sitting under a deterministic name is never, in any path, taken as
evidence that *this* request wrote it. Only this service's own idempotency
record (the scope and request hashes it stamps on the object it creates) marks
a request as its own earlier attempt.

**The retry contract.** Once the names are known to be free, a create writes the
`BackupDestination` first and its credential Secrets second, because the Secrets
are owned by the destination and an owner reference needs a UID. If a Secret
write then fails (a timeout, a refusal, Kubernetes unavailable) the destination
exists and names a Secret that does not. **Repeat the request with the same
`Idempotency-Key`**: the replay is recognised from that record, returns the same
destination and finishes the credential writes. Repeating with a *new* key does
not — it is a different request against an existing name, and is refused. The
audit line reports `credentialSecretsCreated` and `credentialSecretsReplayed`
separately, so "written now" and "already there from my earlier attempt" are
never the same entry.

There is one window this cannot close: a name taken by someone else *between*
the check and the write. The write then refuses, and because an earlier role may
already be live the message names the Secrets that **were** created and says
they must be deleted before a retry, rather than claiming nothing changed.

**The request hash on the object is salted.** Every durable create records
`api.logweir.dev/request-sha256`. For a destination that request contained an
entered credential, so the hash is taken over the idempotency scope as well as
the body: the scope is derived from your `Idempotency-Key`, which is never
stored or published, and without it the annotation cannot be recomputed from the
object's public projection. (An object created by an API build older than this
one carries the unsalted hash and will replay as `409 idempotency_conflict`;
nothing is deployed yet, so no such object exists outside a test.)

### Bounded, honest topic inventory

The API reads the immutable `ConfigMap` chunks the controller committed; it
never opens a broker connection and it never lists `ConfigMap`s — chunk names
come from the discovery's own status, and the adapter has no list verb for them.
Every chunk is verified before a row is served: owned by this discovery,
`immutable: true`, and its annotated digest equal both to the digest the status
indexes and to the SHA-256 of the bytes. Any mismatch is `409
result_integrity_failed`, and the page is refused rather than served from bytes
whose provenance did not hold.

At most eight chunks are read per request, so a sparse `q` returns a short page
with `scan.complete: false` and a cursor instead of reading the whole result.
`page.snapshot` is `<uid>@<topicsSha256>`, so a continued page cannot land on a
different inventory. **A successful list is never called complete**:
`visibility.state` is `unknown` unless an authorization omission was observed
(`limited`) or an administrator-governed attestation says otherwise
(`attestedComplete`).

`stale` is recomputed on every read — freshness expiry, a replaced or edited
connection, a changed principal — and a failed attempt never hides the last
successful inventory: `?latest=true` returns both slots separately.

A claim of completeness is checked before it is published: a controller that
writes `visibility.state: attestedComplete` **without** an `attestation` is
answered `unknown`, with `attestationMissing` recorded in `basis`. `limited` is
passed through as the controller wrote it, because it is a claim about an
authorization failure observed inside the check Job and the API has nothing to
verify it against.

### Readiness that is a result about something

Every preflight carries a `binding`: the plan hash and the digest over the
objects it resolved. `applicable` and `stale` are recomputed on **every** read
against the `?planHash=` the caller sends, so editing the plan, choosing another
target or destination, or letting the result expire all make it inapplicable
rather than quietly current. The aggregate is advisory: `ready` authorizes
nothing, execution-time guards remain the authority, and `executionOnly` names
the checks that can only be answered while the run executes. A blocking check
that was skipped keeps the aggregate `unknown` — skipping a question is not
answering it — and a `Completed` check with no recorded aggregate reads
`unknown`, never `ready`.

**Four operations, and exactly one block.** `operation` is `backup`,
`restore`, `destinationAccess` or `sourceConnection`, and the body carries the
matching block and no other; anything else is `422 validation_failed` naming
both fields. The fourth is the narrowest:

```http
POST /api/v1/namespaces/team-a/preflights
Idempotency-Key: <one per deliberate test>

{"operation": "sourceConnection", "sourceConnection": {"connectionRef": "source"}}
```

It asks "does this connection answer, as this principal, right now" and takes
nothing else: no destination, no plan, no topic list. `connectionRef` is
REQUIRED — an omitted block is `422 sourceConnection required` and an omitted
reference is `422 connectionRef required`, because a connectivity check with no
connection is not a smaller check. Every request body on this API is
`deny_unknown_fields`, so a destination or a topic list added to that block is
`422 unknown_field` rather than a wider check nobody asked for.

**A restore check names ONE recovery point** (PLAT-15.2). `restore.recoveryPoint
{backupName, backupUid}` names a `Backup`; `restore.catalogPoint {catalog,
pointId}` names a point in a `RecoveryCatalog`'s view instead — a point with no
`Backup` object behind it, or a run the controller could not verify itself.
Both at once is `422 exactly_one` on `restore.catalogPoint`; a `pointId` that is
not `lwp1-` plus 32 lowercase hex is `422 invalid_point_id`. The controller
re-reads that row when the check runs — availability, verification, any
reached `Backup` verdict on the same receipt, and whether the plan's
`source.point` is the row's binding — and reports `recoveryPoint.state` from it
(`docs/kubernetes.md` §21.8):

```http
POST /api/v1/namespaces/team-a/preflights
Idempotency-Key: <one per deliberate check>

{"operation": "restore", "restore": {
  "planBytes": "<the exact plan bytes>", "planHash": "sha256:…", "target": "target",
  "sourceDestination": "archive", "evidenceDestination": "archive",
  "catalogPoint": {"catalog": "archive", "pointId": "lwp1-0123456789abcdef0123456789abcdef"}}}
```

**The idempotency key of a connectivity test is per deliberate test, not per
subject.** For a readiness check over an unchanged plan the subject IS the
question, so a retry after a lost response replays. A connectivity test's
subject is a broker that may answer differently a minute later, so a client that
composed its key from the connection name alone would replay its first verdict
for ever — and a control that did that would be a re-read wearing a dial's
label. Mint a fresh key per click and let the in-flight guard, not the key,
collapse a double click.

**`staleReasons` is typed and closed, and it is RECOMPUTED, not reported.**
On every read of a preflight this service reads back each object
`status.binding.referents[]` names — `kind`, `name`, `uid`, `generation` are
all structured there — and compares them with the recorded revisions using
`logweir-core`'s own `stale_reasons`, so the API and the controller cannot each
implement half of the rule. Nothing parses a message. `staleBasis` lists what
the comparison covered, in order, so an empty `staleReasons` cannot be confused
with a check that was skipped.

Each entry is `{reason, kind?, name?, basis?}`, and `reason` is one of seven:

| reason | what moved |
|---|---|
| `expired` | the verdict is past `expiresAt`, or recorded no expiry at all |
| `planHashChanged` | the plan you are looking at is not the plan the check was bound to |
| `referentChanged` | a named object's UID or generation moved, or it appeared or vanished — a recreated destination, an edited access block, a re-created recovery point, a re-created `RecoveryCatalog`, an edited or re-created `TrustRoster/default` or governing `TrustPolicy`, or a re-created `Approval`. `kind` and `name` say **which**; `name` is the name the binding recorded (`default` for the cluster's `TrustRoster`). On a re-read this service compares a referent the binding records **without** a generation (the recovery-point `Backup`, the `Approval` and a catalog point's `RecoveryCatalog`, bound by identity) by UID alone -- the binding records no resourceVersion to compare -- so an unchanged recovery point or catalog is not `referentChanged`; before PLAT-08.2 a recovery point was reported on every re-read, and before the catalog-referent fix a catalog point's catalog was `unverifiable` on every re-read. Any other kind recorded without a generation is `unverifiable`, never compared by UID alone. An `Approval` whose resourceVersion moved when verification landed is **not** seen by this GET: the controller's `inputsDigest` revalidation catches it and downgrades the stored verdict (`ready` → `unknown`) on its own requeue cadence |
| `caBundleChanged` | a destination's CA bundle now digests differently. **RESERVED — nothing emits it.** `status.binding` does not record the CA bundle list, so neither the controller nor this service can compare it; CA drift surfaces through the destination's own `referentChanged`, because editing its `caBundle` reference bumps its generation. Do not branch on it |
| `policyChanged` | the installation policy `ConfigMap` digests differently, which can change the concurrency ceilings, the engine CA rule, the `ControllerIdentity` allowlist and the visibility attestations the verdict was computed under |
| `inputsDigestChanged` | the recomputed digest differs and none of the named reasons explains it. **RESERVED on the same grounds:** the recorded digest was taken over a wider document than this service can rebuild, so comparing the two would report an artefact of the narrower recomputation rather than a change in the world |
| `unverifiable` | **this service could not compare something, so it will not call the verdict applicable.** `basis` says what: a referent whose read failed (a refused `get` on `trustrosters/default` or `trustpolicies/<name>` included), a `TrustRoster` referent named anything but `default` (the one roster this service may read), a referent of a kind this build does not know, a referent of a generation-bearing kind recorded without a generation, or a result that recorded no binding at all |

**Who compares what.** This service compares the expiry, the `?planHash=` you
sent, and every referent the binding records — the seven namespaced kinds
(`KafkaCluster`, `BackupDestination`, `Backup`, `Restore`, `Approval`,
`BackupSchedule`, `RecoveryCatalog`) and the two cluster-scoped trust referents,
`TrustRoster/default` and the namespace's governing `TrustPolicy`, each by UID and
generation (by UID alone for the three kinds bound by identity, above). The
`RecoveryCatalog` read is the `get` on `recoverycatalogs` the API's namespaced
role already carries for `catalog.read`; no new verb. Before
PREFLIGHT-TRUSTROSTER-STALE the two trust kinds were `unverifiable`, so every
verdict on a cluster with a roster was served stale and the restore wizard could
not submit after any readiness check; the chart's `<release>-api-trustroster`
(`get`, `resourceNames: ["default"]`) and the existing `<release>-api-trustpolicies`
`get` are what let it compare them. The
installation policy it cannot read: that digest is `CheckPolicy::digest()` over
the parsed `LOGWEIR_POLICY_CONFIGMAP` document (default `weirkeeper-policy`,
key `policy.json`) in the installation namespace, and this service reads a
`ConfigMap` only when a check owns it. The **controller** compares it on every
reconcile and downgrades `result.state` to `unknown` when it moves, so
`staleBasis` records `policyDigest:byController` rather than pretending either
that it was checked here or that nobody checked it. Reaching that document from
the API is D2 W11's to grant.

**It never fails open.** Anything that could not be compared is `unverifiable`
with a cause, and `applicable` is false — because "I did not check" and "I
checked and it matches" are different answers and only one of them may look
like a green badge. An earlier build recovered the reasons from the
controller's prose instead; that prose is redacted and capped at 512
characters, so a long referent list lost its closing bracket, the parse
returned nothing, and a downgraded verdict was reported as applicable.

There is no `cancelRequested` reason. A cancelled check ends with no result at
all, so its verdict is not out of date — it is **absent**, and `state:
cancelled` with `terminal: true` is what says so.

**One asymmetry to know about.** The controller only rewrites `result.state`
for verdicts that were `ready`: a `notReady` or `unknown` result that later
stops applying keeps its recorded state. The API's recomputation has no such
limit — `stale`, `staleReasons` and `applicable` are computed the same way for
every completed verdict — so the two can disagree about a non-green result, and
the API's is the current one.

An actor bound **only** as Approver reads `Restore` readiness, because that is
what an approval packet needs, and gets the nonexistent-resource answer for
anything else.

## The Kubernetes boundary

Every Kubernetes call is a method on one adapter, typed over a **sealed** set of
eight custom resources — `KafkaCluster`, `BackupSchedule`, `Backup`, `Restore`,
`Approval`, `BackupDestination`, `TopicDiscovery` and `Preflight`. No method
takes a group, a version, a plural or a path, so no request can name a ninth
kind, and no method can reach a Pod, a log, an exec stream, a Job or a core
`Namespace` at all. There is no delete, anywhere.

**Two core objects are reached, each through exactly one verb and one
hand-written type.** `ConfigMap` is a `get` and nothing else, used only for the
chunk and detail documents a check owns, and every read is verified by owner
UID, immutability and digest before a row is served. `Secret` is a `create` and
nothing else — **there is no Secret read verb in this service**, so a stored
credential cannot be read back by any route, any projection or any future
refactor of one. Asking whether a name is free is the same `create` verb with
`dryRun: All`, which is why establishing that costs no extra permission and
still reveals nothing about the object occupying the name. `k8s-openapi`'s own `Secret` and `ConfigMap` types are
deliberately not imported: a type that can hold a Secret's data is a type that
can leak one. The credential type's `data` field is `skip_deserializing`, so the
API server's create response — which echoes `data` — is parsed into a value
whose `data` is empty.

**`create` on Secrets is the widest grant this service asks for, and it is not
yet fenced.** In a namespace it could in principle mint a
`kubernetes.io/service-account-token` Secret for any ServiceAccount there. The
credentials this API creates carry the distinct, immutable-after-create type
`logweir.dev/object-store-credential` (and PLAT-07.1's carry
`logweir.dev/kafka-sasl-password`) precisely so a ValidatingAdmissionPolicy
scoped to the console ServiceAccount can require one of those two values.
**That policy does not ship yet.** Until it does, the grant is wider than the
route that uses it, and that is the single most important thing the RBAC stage
owes.

**There are four updates, and each is a merge patch built here from typed
arguments**: `BackupSchedule.spec.suspend`, a `BackupSchedule`'s editable policy
(`PUT .../schedules/{name}`), a destination's four grants and CA reference
(`:update-access`), and a check's `spec.cancelRequested` (`:cancel`). Each
carries `metadata.resourceVersion`, so each is a conditional write the API
server refuses on a stale read, and none of them accepts a caller-supplied path
or patch document. A destination's location and transport have no key in any of
them, and **`spec.sourceRef` has no key in the schedule edit**: the policy patch
is built from a struct whose fields *are* the mutable set, so the immutable one
is unreachable rather than merely refused.

**A merge patch merges nested objects key by key**, so every optional key inside
one is spelled out — `null` included. A policy patch that sent `archive: {url}`
alone would leave a `secretRef` from the previous archive in place: a credential
reference nothing reads, on an object that then breaks the CRD's own sentinel
rule. Absent is written as `null`, which the API server removes.

**The two routes D1 W6 added ask for no new Kubernetes permission.** `create
backups` and `patch backupschedules` are already in the console
ServiceAccount's Role; the policy edit uses the same `patch` verb
`:set-suspension` uses, and the manual run uses the same `create` the RBAC stage
already owes for `backups`. `tests/linkage.rs` pins the whole `(verb, resource)`
set from the adapter's source, so this claim fails a test rather than a review
if it stops being true.

Every call carries a **10-second deadline**, and the client's own connect, read
and write timeouts are set to the same bound. A timeout is `504
upstream_timeout`. No Kubernetes message is returned verbatim: the status code
and reason class decide the problem, and the message is redacted — bearer
tokens, URL userinfo and anything past 512 bytes removed — before it reaches the
log. Nothing from an inbound request reaches an outbound Kubernetes request but
validated names.

## Conventions

**Errors are `application/problem+json`**, always, with a stable `code` and the
`requestId`:

```json
{
  "type": "https://logweir.dev/problems/validation-failed",
  "title": "Request validation failed",
  "status": 422,
  "code": "validation_failed",
  "detail": "One or more fields are invalid.",
  "requestId": "01M2N5SHWR0N2E7SDCW2H22Z8W",
  "retryable": false,
  "errors": [
    {"field": "bootstrapServers[0]", "code": "invalid_port",
     "message": "must be host:port with no scheme or userinfo"}
  ]
}
```

Mutation bodies are strict: an unknown field is `422 validation_failed` naming
the field, not a silently ignored key. So is an unknown or repeated query
parameter (`400 malformed_request`). Bodies are capped at 1 MiB.

**`errors[].field` is a path and nothing else** — `topics[2]`,
`scheduleRef.name`, `access.archiveWrite.mode`, or a header or query-parameter
name. It never carries a parenthetical or any other note a client would have to
parse; what went wrong is `message`, which is a sentence. A console may match
on it exactly.

**One condition has one `errors[].code`, on every route that can produce it.**
An expression the cadence engine cannot read is `schedule: schedule_invalid`
from `POST .../schedules`, `PUT .../schedules/{name}` and
`GET /api/v1/cadence-previews` alike; a zone it cannot resolve is `timeZone:
timezone_unknown` — from all three of them, since PLAT-10.1 gave the create
route a `timeZone` as well. (`POST .../schedules` answered `invalid_cron` before
D1 W6, which meant a console highlighting the cadence input worked on the edit
form and the preview and silently did not on the create form, for the same typo.)

**Every response** carries `X-Request-ID` — freshly minted; a client-supplied ID
is ignored — a Content-Security-Policy, `X-Content-Type-Options: nosniff`,
`Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`, a restrictive
Permissions-Policy, both Cross-Origin-*-Policy headers, and `Cache-Control:
no-store` unless a static asset set its own. One log line per request records
the method, path, status and latency — never a query string, a header value or a
body.

**Three guards run before any handler.** Any `Impersonate-*` header is refused
with `400 header_not_allowed` naming it; this service never impersonates, under
any configuration. A `Host` this listener does not serve is `421`, which is what
stops DNS rebinding against a loopback listener. And an unsafe method must carry
an `Origin` exactly equal to `publicOrigin` (`403`) and `Content-Type:
application/json` (`415`). The identity stage adds its CSRF check at the same
seam.

**Durable `POST`s require `Idempotency-Key`**, 8–128 visible ASCII characters,
and Kubernetes is the only store. The object's name is a per-route prefix plus
130 bits of `SHA-256(issuer, subject, namespace, route, key)`, so a lost
response, a double click, a restart or a second replica all target the same
name and the API server's `AlreadyExists` is what makes a second object
impossible. The object records the scope hash and the canonical request hash:
the same key with the same request replays as `200` with the same UID, the same
key with a different request is `409 idempotency_conflict`, and an object that
this scope did not create is `409 state_conflict` and is never adopted. The raw
key is never stored or logged — only hashes derived from it. A deliberately new
operation uses a new key.

`:set-suspension` is the exception: it takes no `Idempotency-Key` and instead
requires `expectedResourceVersion`, which is the value last read. That
precondition is on the **whole object**, so a controller status write between
your read and your write makes it `412 precondition_failed`: read the schedule
again and resend. That is the contract working, not a fault.

**Lists** default to `limit=50`, maximum 200, and answer `{items, page:{limit,
nextCursor, snapshot}}`. A cursor is opaque and authenticated with the
persistent key from `cursorKeyFile`; it binds the actor, route, namespace,
filters, a fifteen-minute expiry and the Kubernetes continue token. The MAC is
checked first and in constant time, so a tampered cursor is `400 cursor_invalid`
whatever else it claims, and a valid cursor replayed on another route or
namespace is `cursor_invalid` too. Expiry, and a Kubernetes `410`, are `410
cursor_expired` with an instruction to restart the list. Filters are exact name
and label selectors; there is no substring search, because implementing one
would mean collecting an unbounded list.

**Nothing credential-shaped is in a response.** No Secret data, no
service-account token, no kubeconfig, no raw pod log and no unredacted upstream
error. A connection projection carries the credential Secret's *name* and
nothing from inside it.

## The static UI

`/ui/` serves an allowlist built once at startup: the top-level `*.html`,
`*.js` and `*.css` files of the configured directory and the same three
extensions directly inside `pages/` — the selection `Dockerfile.ui` makes, and
nothing else. `README.md`, the `tests/` directory and its throwaway keypair, any
other subdirectory, any other extension, dotfiles and symbolic links are never
in the map. A request path is looked up **exactly**, with no decoding,
normalisation or directory index, so `/ui/../Cargo.toml` is a `404` rather than
a traversal to defeat. The bytes a browser receives are the bytes the process
read at startup; no request performs file-system I/O.

## Shared mode

### Configuration

```yaml
# console.yaml
mode: shared
listen: "0.0.0.0:8484"                       # any address; TLS terminates in front
publicBaseUrl: "https://console.example.com" # EXACT, HTTPS, no path, no trailing slash
allowedHosts: ["console.internal.example"]   # EXTRA Host values; see below
uiDirectory: /srv/ui
oidc:
  issuer: https://idp.example.com/realms/logweir   # EXACT, as the discovery document states it
  clientId: logweir-console                        # EXACT; also the expected audience
  clientSecretFile: /var/run/secrets/oidc/clientSecret
  allowedAlgorithms: [RS256, ES256]                # the two this service verifies
  scopes: [openid, profile, groups]                # must contain `openid`
  groupsClaim: groups                              # the EXACT claim name
  displayNameClaim: name                           # presentation only
  tokenAuthMethod: clientSecretBasic               # or clientSecretPost
  caBundleFile: /var/run/logweir/oidc-ca/ca.crt    # OPTIONAL: a private issuer CA, ADDED to the system roots
  systemRoots: true                                # false: trust caBundleFile alone
roles:
  revision: "2026-09-16.1"                         # recorded in every audit line
  bindings:
    - role: viewer                                 # viewer|operator|approver|administrator
      namespace: team-a                            # must appear in `namespaces` below
      groups: ["logweir-team-a-viewers"]           # EXACT strings; no wildcard, no regex
    - role: operator
      namespace: team-a
      groups: ["logweir-team-a-operators"]
    - role: approver
      namespace: team-a
      subjects: ["https://idp.example.com/realms/logweir#3f0c…"]
sessionKey:
  file: /var/run/secrets/session/key               # versioned key file
  expectedVersion: 1
cursorKey:
  file: /var/run/secrets/cursor/key
  expectedVersion: 1
sessionMaxAgeSeconds: 900                          # 60…900
trustedProxyCidrs: ["192.0.2.0/24"]                # PLACEHOLDER: your ingress pods' range; never an identity
requireTrustedProxy: true                          # refuse any request not from it over HTTPS
namespaces: [team-a, team-b]
kubernetes:
  source: inCluster
  principal: system:serviceaccount:logweir-system:logweir-api  # recorded on every object
```

**`allowedHosts` widens a security guard, so it is not a convenience.** The
`Host` allowlist is the DNS-rebinding defence: a request arriving under a name
this listener does not serve is `421`, which is what stops a page on an
attacker's domain that resolves to the console's address from driving it. The
allowlist is `publicBaseUrl`'s own authority **plus** whatever `allowedHosts`
adds, so each entry is one more name a rebinding page may use. Add one only for
a second name that genuinely reaches this service — a Service DNS name a
sidecar uses, say — and never a wildcard, which the field does not support.
`/healthz` and `/readyz` are exempt from the allowlist regardless, because a
kubelet addresses the Pod by IP; neither reads a header, a cookie or a body.

**The ingress controller by its Service (`trustedProxyService`).** A
`trustedProxyCidrs` entry for the ingress pod is a `/32` that is wrong the
moment the pod is recreated, and a range wide enough to survive that trusts
every other pod the node schedules. Instead of (or beside) the list, name the
ingress controller's Service:

```yaml
trustedProxyService: {namespace: traefik, name: traefik}   # its SERVING endpoints are the trusted peers
```

The console reads that Service's `EndpointSlice`s every five seconds and trusts
each **serving** endpoint address as a single host — the ingress pods of the
moment, narrower than any range the width floors admit. A refresh that fails
keeps the last complete set for at most thirty seconds; after that the Service
source trusts nobody, browsers get `421` and `/readyz` reports not ready, so a
stale grant is never silent. Before the first read `/readyz` is not ready
either. The read is one `list` of `endpointslices` in that one namespace (the
chart grants exactly that). Whoever can edit that Service, write an
`EndpointSlice` there, or create or relabel a Pod matching the Service's
selector there could add an address — the ingress namespace's own
administrators, who already terminate the console's TLS. An ingress controller
on `hostNetwork` publishes the node's address, so the console then trusts every
hostNetwork pod and node process on that node and anything masqueraded to its
address; accept that knowingly, or decide a `trustedProxyCidrs` `/32` instead.

**A private CA for the issuer (`caBundleFile`, `systemRoots`).** An issuer
whose certificate a private CA issued — a Dex behind an ingress with an
internal certificate, an IdP on a corporate PKI — is trusted by naming a PEM
bundle of that CA's **public** certificates. Its certificates are trust anchors
*in addition to* the operating system's; `systemRoots: false` drops the system
roots and is refused without a bundle. The bundle widens who may issue the
provider's certificate and nothing else: the chain, its validity and the host
name in the URL are verified exactly as for a public CA, and `iss` is still
compared for exact equality with `issuer`. The bundle is read before the socket
binds; a file that cannot be read, holds no certificate, holds a malformed
block or holds anything but `CERTIFICATE` blocks (a private key mounted by
mistake) is exit 2. The chart mounts it from a ConfigMap or Secret
(`api.console.oidc.caBundle`, [charts/logweir/README.md](../charts/logweir/README.md)).
While the provider has not initialised, the console logs why at `warn` with the
transport cause — `invalid peer certificate: UnknownIssuer` is a missing
bundle, a name that does not resolve is a missing `hostAliases` entry. A
`hostAliases` entry for the issuer must point at an endpoint only the IdP's
owner can route — its own Service, with TLS terminated by the IdP — never at a
shared ingress, where an Ingress from any namespace could answer for the
issuer behind its real certificate ([charts/logweir/README.md](../charts/logweir/README.md)).

A key file is two lines:

```console
$ printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > session.key.yaml
```

### What startup refuses, before it binds a socket

Every one of these is exit 2 with the field named. A console that comes up on
plain HTTP, or with a key someone rewrote underneath it, is worse than one that
does not come up at all, because the first two look like they are working.

| refusal | why |
|---|---|
| `publicBaseUrl` is not `https://` | TLS at the shared entry point is required, not recommended |
| `publicBaseUrl` has a path, a trailing slash, userinfo or no host | the redirect URI is this value plus `/auth/callback`; a mismatch is a login that cannot complete |
| `publicOrigin` is present | shared mode derives the origin from `publicBaseUrl`, so the origin checked and the redirect URI registered cannot disagree |
| a key file is missing, malformed, or under 32 bytes | there is no default key and no generated one |
| a key file's `version` is not `expectedVersion` | an **unexpected rotation**. Bumping both is a deliberate act that ends every live session; bumping neither means the file changed behind the service's back |
| the client-secret file is missing or empty | |
| an `allowedAlgorithms` entry is not `RS256` or `ES256` | `none` cannot be on the list, so an `alg: none` token has no matching entry |
| `oidc.issuer` ends in `/`, or carries a query, fragment or userinfo | the issuer is compared for exact equality with the token's `iss` |
| a plain-HTTP issuer without `oidc.insecureLoopbackIssuer` | and that flag is accepted only for a loopback host, for a local mock provider in development |
| `oidc.systemRoots: false` without `oidc.caBundleFile` | a client with no trust anchor trusts no provider at all |
| `oidc.caBundleFile` cannot be read, holds no `CERTIFICATE` block, a malformed one, or any other section | a bundle that adds nothing is never what naming one means, and a private key there is a secret in the wrong place |
| a role binding names a role that is not one of the four | |
| a role binding names a namespace outside `namespaces` | |
| a role binding has neither `groups` nor `subjects` | |
| a binding string contains `*` or `?` | bindings are EXACT: a `*` would match nothing, so it is refused by name rather than silently granting nothing |
| `roles.revision` is empty | it is the provenance of every decision in the audit log |
| `sessionMaxAgeSeconds` outside 60…900 | a stateless session cannot be revoked before it expires |
| `requireTrustedProxy: true` with neither `trustedProxyCidrs` nor `trustedProxyService` | the entry point would refuse every request and look like an outage |
| `trustedProxyService` whose namespace or name is not a DNS-1123 label, or in `localAdmin` mode | |
| `requireTrustedProxy` in `localAdmin` mode | there is no proxy in front of a loopback listener |
| an in-cluster `kubernetes.principal` that is not `system:serviceaccount:<namespace>:<name>` | the value goes onto every created object; a pod's token can only be a ServiceAccount |

### Every route declares who may reach it

`crates/logweir-api/src/access.rs` holds one declaration per `(method, path)`
the router serves — `Public` (the probes, the page, the two sign-in steps),
`Authenticated` (`/session`, `/namespaces`, logout), `Namespaced(actions)`,
`Command{floor, verbs}` for the `:verb` command routes, or `AnyNamespace(action)`
for the two routes with no `{ns}` — and one route layer on every route group
enforces it **before any handler runs**: it authenticates (the CSRF token
included on unsafe methods), decides the declared actions against the role table
below, namespace first and before any Kubernetes call, and records the decision.
The event stream is declared as both `operation.read` and `operation.stream`, so
it is refused without an identity before any slot is taken or any watch opens.

**A route with no declaration is not served.** A path the router matches but the
table does not name answers `500 internal_error` (audit code
`route_access_undeclared`) and its handler never runs; a method registered on a
declared path without its own declaration is answered `405` by the layer.
`tests/route_access.rs` holds the table to `src/app.rs` in both directions and
sweeps every declared route for all four roles, so a route added by any later
stage inherits the enforcement or fails the build. Handlers keep their own
checks as defence in depth and for the object-level rules (an operator cancels
only its own checks; an approver reads only restore preflights).

### The trusted entry point

Identity is the OIDC session and nothing else; see *What can never be an
identity* below. What the proxy in front of the console may contribute is
transport facts, and `requireTrustedProxy: true` (with `trustedProxyCidrs`)
turns that into a refusal:

* a request whose **socket peer** is outside every `trustedProxyCidrs` range is
  `421 misdirected_request`, audit code `untrusted_entry_point`, note
  `peerNotTrusted`, whatever headers it carries;
* a request from a trusted peer that does not carry exactly one
  `X-Forwarded-Proto: https` — the proxy did not vouch for TLS — is refused the
  same way, note `forwardedProtoNotHttps`;
* `/healthz` and `/readyz` are exempt, because the kubelet dials the Pod IP.

The header is read only from a peer the administrator named, and it can only
refuse: nothing derives an identity, a callback URL or a grant from it, which is
D0's rule. Without the flag the entry point is exactly what it was.

**What it distinguishes depends on the range, so the range must be the ingress
controller's own.** kube-proxy keeps the source pod IP through a ClusterIP, so a
pod that dials the console Service directly arrives with its own address. The
gate refuses it only if that address is outside `trustedProxyCidrs` — that is,
only if the range covers the ingress controller's pods and no other pod. A range
like `10.0.0.0/8` contains the whole pod network of a typical cluster and would
let any pod through with a forged `X-Forwarded-Proto`, so with
`requireTrustedProxy` the service (and the chart) refuses any range wider than
`/16` (IPv4) or `/48` (IPv6) by name; the example ships the documentation range
`192.0.2.0/24` as a placeholder. Even with a tight range this is defence in
depth: on a cluster whose CNI enforces NetworkPolicy, the console's ingress
policy remains the network boundary, and on one that does not (Docker Desktop
among them), this gate is the only thing that tells the ingress from a pod.

### Readiness

In shared mode `/readyz` **starts** not ready: it becomes ready once Kubernetes
answers for the service's own identity **and** the provider has initialised —
its discovery document (naming the configured issuer, with https endpoints) and
a non-empty key set have been read. After that the provider half stays ready for
the life of the process. D0 says the API *starts* NotReady if the provider cannot
initialise, and that a valid session continues to its signed expiry: an IdP
outage later must not take every replica out of rotation and cut sessions that
need nothing from the provider. During such an outage sign-in fails closed on
its own path, and existing sessions keep working until they expire. The body
still names no endpoint and no reason.

### Sign-in

`GET /auth/login` starts an Authorization Code flow with PKCE S256, `state` and
`nonce`, and answers `303` to the provider's authorization endpoint. The three
per-login secrets travel in a sealed, `HttpOnly`, ten-minute
`__Host-logweir_login` cookie rather than in process memory, so a login begun on
one replica finishes on another.

`GET /auth/callback` compares `state` in constant time, exchanges the code with
the verifier, and validates the ID token: allowed `alg`, JWKS key by exact
`kid`, signature, exact `iss`, exact audience (`azp` required when there is more
than one), `exp`, `iat` (bounded skew, bounded age) and this login's `nonce`.
**The browser never receives a provider token**: the token response is
deserialised into a struct with one field, `id_token`, so no access or refresh
token exists in the process to leak.

JWKS are cached. An unknown `kid` provokes at most one refetch per minute —
that is what makes a provider's key rotation work without a restart, and what
stops an attacker-chosen `kid` from becoming a request amplifier. While the
provider is unreachable the cached keys keep working for a day, then validation
fails closed.

### The session and the CSRF token

| | |
|---|---|
| cookie | `__Host-logweir_session`, `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`, **no `Domain`** |
| contents | session id, issuer, subject, display claim, the group claims that appear in some role binding (unbound groups are not carried), issued/expiry/auth times, key version — **never** a provider token |
| size | the sealed `Set-Cookie` is measured at sign-in; a session that would exceed the browser cookie limit is refused by name (`session_too_large`) rather than truncated, because dropping a group silently drops a grant |
| protection | ChaCha20-Poly1305, with the cookie's own name and the key version as associated data |
| lifetime | at most 15 minutes; there is no refresh token and no server-side session table |
| CSRF token | `HMAC-SHA-256(session key, session id)`, returned by `GET /api/v1/session`, required in `X-CSRF-Token` on every unsafe method |
| logout | `POST /api/v1/session/logout` — an unsafe method, so it needs the exact `Origin`, `application/json` and the token like any other mutation |

Because the session is stateless, a restart or a second replica does not log
anyone out and nothing has to be replicated. What that costs is that revocation
before expiry is bounded by the expiry: removing an identity at the provider
takes effect within fifteen minutes. **Roles are not in the cookie** — they are
re-derived per request from the carried claims plus the current binding table —
so removing a role binding takes effect on the *next request*. Because only the
groups that were bindable at sign-in are carried, a binding *added* for a group
the session does not carry takes effect at the next sign-in, not the next
request.

### What can never be an identity

`X-Remote-User`, `X-Remote-Group(s)`, `X-Remote-Extra-*`, `X-Forwarded-User`,
`X-Forwarded-Email`, `X-Forwarded-Groups`, `X-Forwarded-Preferred-Username` and
`X-Auth-Request-*` are **stripped from the request before routing** and recorded
in the audit line by name only. Stripping rather than ignoring is deliberate:
ignoring is a property of every reader and a future route can lose it, removing
is a property of the request. `Impersonate-*` is not ignored — it is refused
outright with `400 header_not_allowed` naming the header.

No callback URL and no authorization decision derives from `Host`, `Forwarded`
or `X-Forwarded-*`. A forwarded client address reaches exactly one audit field,
`forwardedFor`, and only when the immediate socket peer falls inside a
`trustedProxyCidrs` range; it is the rightmost hop none of those proxies
added, because everything to its left was sent by the client.

There is no CORS layer: no response carries `Access-Control-Allow-Origin` or
`Access-Control-Allow-Credentials`.

### Roles

Actor identity is exactly `(issuer, sub)`. Group claims and subjects map by
**exact string** to bindings; multiple bindings union.

| action | viewer | operator | approver | administrator |
|---|:--:|:--:|:--:|:--:|
| read connections / schedules | ✓ | ✓ | | ✓ |
| create connection, test, credentials | | ✓ | | ✓ |
| read destinations / topic discoveries | ✓ | ✓ | | ✓ |
| create destination, rotate access, test, adopt a legacy location | | ✓ | | ✓ |
| start / cancel a discovery or a preflight | | ✓ | | ✓ |
| read preflights | ✓ | ✓ | restore only | ✓ |
| create schedule, set suspension | | ✓ | | ✓ |
| read backups / restores / approvals / operations | ✓ | ✓ | ✓ | ✓ |
| create manual backup, create restore | | ✓ | | ✓ |
| read the approval packet | | ✓ | ✓ | ✓ |
| submit a governed approval | | | ✓ | |

**Administrator is deliberately absent from the last row.** An administrator who
must approve is bound as an Approver as well, and the separation-of-duties check
then still compares `(issuer, sub)` — not display names, not email claims, not
key ids. Administrator is not a self-approval bypass.

The governed-approval route is `POST .../restores/{name}/approval`
(PLAT-19.2); `capabilities.approvalSubmit` follows this row. The route adds the
separation-of-duties refusal the table cannot express: the console-attested
requester is refused its own request.

### Enumeration resistance

In shared mode an ungranted namespace answers exactly what a nonexistent object
answers — `404 not_found`, byte for byte apart from the request id — and the
namespace is checked **before** any Kubernetes call, so the answer never depends
on cluster state the actor may not see. In localAdmin mode the more informative
`403 namespace_forbidden` is kept: there is one actor, it is the administrator,
and there is nothing to enumerate.

The audit record carries the real reason either way. Hiding a namespace from a
caller must not also hide an authorization problem from the operator reading the
log.

### The audit record

One JSON object per request on the `logweir_api::audit` tracing target, emitted
by the middleware so no handler can forget one:

`auditId` (= `X-Request-ID`), `method`, `path`, `authenticationMode`,
`actorId`, `displayClaim` (kept separate because it is never an authorization
input), `sessionIdHash`, `bindingRevision`, `roles`, `namespace`, `action`,
`resource`, `decision`, `policyDigest`, `idempotencyKeyHash`, `requestHash`,
`planHash`, `recoveryPoint`, `kubernetesPrincipal`, `objectName`, `objectUid`,
`objectResourceVersion`, `httpStatus`, `latencyMs`, `failureCode`, `peer`,
`forwardedFor`, `ignoredIdentityHeaders`.

**Every durable object the API creates carries its attribution** — CRs and the
write-only credential Secrets alike — in reserved annotations, stamped by the
Kubernetes adapter from the request's own audit record so no route passes them
and none can forget them:

| annotation | value |
|---|---|
| `api.logweir.dev/actor` | `<issuer>#<subject>` — never a display name |
| `api.logweir.dev/authentication-mode` | `oidc` or `localAdmin` |
| `api.logweir.dev/action` | the product action that authorized the write, e.g. `restore.create` |
| `api.logweir.dev/binding-revision` | the role-binding revision that decided it (shared mode) |
| `api.logweir.dev/kubernetes-principal` | the identity the write was made as — what Kubernetes audit records for the same call (`kubernetes.principal`; the chart sets `system:serviceaccount:<namespace>:<release>-api`) |
| `api.logweir.dev/recovery-point` | for a restore-shaped create: `backupSet=… pointInTime=… source=destination/<name>` (or the archive location with userinfo, query and fragment removed) |
| `api.logweir.dev/request-id` | the audit ID, `X-Request-ID` |
| `api.logweir.dev/request-sha256`, `api.logweir.dev/idempotency-scope-sha256` | D0's correlation hashes, unchanged |

A create with no authenticated actor or no decided action behind it is refused
before anything is sent. Objects created before this change keep their older
annotation set and still replay: replay compares only the two hashes. None of
these values is a credential; the recovery point names a destination, never its
access.

A record that reaches no decision point defaults to `deny`. Never logged:
cookies, bearer/authorization-code/refresh tokens, CSRF tokens, the raw
`Idempotency-Key`, Secret values, kubeconfig, approval or sidecar bytes, plan
bytes, raw pod logs, and object-store URLs carrying userinfo.

This is **attribution, not proof.** Annotations on a created object and lines on
stdout are correlation; the tamper-evident requester/approver record is the DSSE
document PLAT-19 owns, and Kubernetes audit supplies the complementary fact that
the `logweir-api` ServiceAccount made the API call. Retaining these lines is a
deployment responsibility: stdout alone is not durable evidence.

Dependency log targets (`kube_client`, `kube`, `hyper`, `hyper_util`, `rustls`,
`tower`, `h2`) are pinned at `warn` **regardless of `RUST_LOG`**, because
`kube_client` logs the upstream error body verbatim at `debug` — the exact text
this service redacts before logging it.

### Rate limits

`/auth/login` and `/auth/callback` are the only routes an unauthenticated caller
can reach that do work, so they carry a per-peer limit of 20 requests a minute
(`429` with `Retry-After`). The key is the **immediate socket peer**, never a
forwarded header: behind one ingress that makes it a global limit, which is the
correct conservative behaviour for a service whose per-user limits live behind
authentication.

Starting a transient check is bounded per actor and namespace: six discoveries
and twenty preflights a minute, `429` with `Retry-After`. The window is **per
process**, so two console replicas each permit the configured rate; it is a
politeness bound on how fast one operator can queue check Jobs, not a security
control. The real ceiling on concurrent checks is the controller's
`checks.maxActivePerNamespace`, which no API can talk past.

The operation event stream (`…/operations/{kind}/{name}/events`) is declared
`operation.read` + `operation.stream`, so it answers `401` without a session
before anything else happens, and each principal holds a bounded number of
concurrent streams per namespace (`429` beyond it); a stream closes at its
maximum duration with a terminal `end` event.

## Deployment

The service ships as one image and one optional Helm component.

**The image is `logweir-console`, built by `Dockerfile.console`.** It carries
`/usr/local/bin/logweir-api` and, at `/ui`, the same twenty-six static files the
`logweir-ui` image carries — the same two globs, from the same one `ui/`
directory in the source tree, so there is one copy of the page in the repository
and two images that copy from it. `scripts/check-image-api.sh` hashes every file
the image will serve against that directory (as
`scripts/check-image-ui.sh` does for the other), requires all three licence
files, requires the CA bundle, and refuses an image carrying any key-shaped path
anywhere outside the system trust store: this is the one image in the tree that
*mounts* key material at runtime, and none of it is ever built into a layer.
The image declares `USER 65532:65532`, no `CMD`, and
`ENTRYPOINT ["/usr/local/bin/logweir-api"]` — every argument is the chart's, so
the MODE cannot come from a layer no chart test can see.

**The chart runs it under two switches.** `api.enabled` renders the principal —
a ServiceAccount, three ClusterRoles and one RoleBinding per configured namespace
— and starts nothing, which is a state you can interrogate with
`kubectl auth can-i` before anything runs as it. `api.console.enabled` runs the
workload as that principal and requires the first; the render refuses the pair
by name rather than producing nothing.

**`api.console.mode` has no chart default either**, for the reason this file
gives above: a mode read by fall-through is the more permissive one nobody
chose. Enabling the console without naming a mode is a render-time refusal
naming the field.

| object | rendered when |
|---|---|
| immutable, content-addressed `ConfigMap` holding `config.yaml` | `api.console.enabled` |
| `Deployment` `<release>-api` | `api.console.enabled` |
| ClusterIP `Service` `<release>-api` | `api.console.mode: shared` — **only**. The in-cluster administrator mode binds loopback, so a Service there would advertise a ready endpoint and refuse every connection |
| `PodDisruptionBudget` | `api.console.replicas` > 1 |
| `Ingress` | `api.console.ingress.enabled` — shared mode only, TLS required, host must be `publicBaseUrl`'s authority |
| `NetworkPolicy` | `api.console.networkPolicy.enabled` — an allow rule for the configured ingress controller in shared mode, `ingress: []` (deny) in the administrator mode |

**The configuration file is that ConfigMap and it holds no credential.** Every
one of the three — the OIDC client secret, the session key, the cursor MAC key —
appears as a *path* into a read-only Secret volume under `/var/run/logweir/`,
never as a value. The Secrets are the operator's: the chart generates no key
material, because a Helm-generated key changes on every render and is
unrecoverable on upgrade. **Those Secrets live in the release namespace, and
shared mode renders only when that namespace is outside `weirkeeper`'s
Job-create authority (D0 stage 5).** A Job can mount any Secret in its
namespace, so Job-create authority there would be holding the keys — residual
O1 (`docs/kubernetes.md` §15.4). `controller.watchNamespaces` replaces the
controller's cluster-wide binding with one RoleBinding per execution namespace
plus a ClusterRole for the two cluster-scoped trust kinds only, and the
controller watches exactly those namespaces; the chart refuses shared mode
without the list, with a list that includes the release namespace, beside the
legacy `ui.enabled` proxy, and with a product binding in a namespace no
controller reconciles. `charts/logweir/README.md` §`controller.watchNamespaces`
has the `kubectl auth can-i` matrix and the migration. `kubernetes.source` is always `inCluster` and there is
no chart value for the other source — a pod that read a kubeconfig would act
with whatever identity that file carried, and the RBAC argument above would be
about an account nothing runs as. The token is the projected, time-bound kind
the kubelet mounts.

**The pod is non-root (65532), read-only-root, drops every capability and
mounts one writable path (`/tmp`) that holds no state** — the page is read into
memory once at startup and every audit record goes to stdout.

**Probes exist only in `shared` mode**, and their absence in `localAdmin` mode
is a consequence of the loopback rule rather than an omission: a kubelet HTTP
probe is made against the Pod IP from the node's network namespace, and a
loopback listener refuses it, so a readiness probe would hold a working console
NotReady forever and a liveness probe would restart it. In that mode a refused
configuration surfaces as `CrashLoopBackOff` with exit code 2 and a log line
naming the field, which is what to look for.

**What the chart refuses at render time**, each with the field named, because a
console that comes up on plain HTTP or with a key someone rewrote underneath it
looks like it is working: a `publicBaseUrl` that is not `https://` or that
carries a path, trailing slash or userinfo; an Ingress with no TLS Secret; an
Ingress in front of `localAdmin` mode at all; a role binding naming a namespace
the ServiceAccount is not bound in; `*` or `?` in a binding; a NetworkPolicy
with no ingress-controller selector; a missing key Secret. `values.schema.json`
types the HTTPS rule as well, so `--set` is refused before a template runs, and
`config.rs` refuses the same things again at startup with exit 2.

`charts/logweir/README.md` §`api.console.enabled` and
[install.md](install.md) §5e carry the commands, including the key Secret you
must create first and the `kubectl port-forward deploy/<release>-api` the
in-cluster administrator mode is reached with.

## Local administrator mode is not a shared console

**Where it runs decides whose authority it carries, and the two are not the
same.** Run from a laptop, `mode: localAdmin` uses the selected kubeconfig
identity and **may be cluster-admin** — that is the manual administrator path
D0 describes, and the closed adapter is a source-level bound on what it reaches,
not an RBAC one. Run in the cluster under `api.console.enabled` it is the
**in-cluster administrator mode**: `kubernetes.source: inCluster`, with no chart
value for a kubeconfig at all, so it carries the `<release>-api` ServiceAccount
and nothing else — the narrow grant in
`charts/logweir/templates/ui/api-rbac.yaml`, which `kubectl auth can-i` can be
asked about before anything runs as it. In that shape the chart also renders no
Service, no Ingress and no ingress NetworkPolicy rule, and `create
pods/portforward` in the namespace is the whole authorization boundary.

**What is the same in both.** It is an explicit administrator mode, not SSO and
not per-user authorization: the namespace grants come from the configuration
file, and every actor of this process is the same actor. It must not bind a
routable address, must not get an Ingress, and adding a login in front of it
would not create per-user authorization. Shared operation uses `mode: shared`, which is a different
listener, a different authenticator and a different authorizer — never this one
with a login bolted in front. An approval policy applies the same way in both
modes (PLAT-19.2): with a namespace bound Ordinary the console signs the
configured administrator's confirmation, which is an authorization only
because the installation chose that policy; under Governed the administrator
is the requester of everything it submits, so it can never approve one of its
own requests. What stays unavailable is the legacy `kubectl proxy` page, which
cannot sign a confirmation at all (D0) and keeps today's governed flow.

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
