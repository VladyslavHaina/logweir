# The product API (`logweir-api`)

`logweir-api` serves the console page at `/ui/` and a typed JSON API at
`/api/v1` on one origin. It reads and creates the `logweir.dev/v1alpha1`
resources through one Kubernetes adapter.

**It is not a Kubernetes proxy.** There is no generic
`{group}/{version}/{resource}` route, no request-supplied Kubernetes path, no
Secret, Pod, log, exec or Job access, no delete, and no arbitrary patch.
`weirkeeper` stays the only execution authority, and runner Jobs stay the
data-plane boundary. The service dials no Kafka broker and no object store.

**The reference is this page's route table and the generated OpenAPI
document**, [`schemas/logweir-api-v1.openapi.json`](../schemas/logweir-api-v1.openapi.json).

- The OpenAPI document carries each route's **idempotency rule** (the
  `Idempotency-Key` parameter on a durable create, or a description saying the
  route refuses or does not use it) and its **problem codes**
  (`x-logweir-problem-codes`). The table below does not repeat them.
- Every schema member has a description there: what an absent member means,
  every enum value and every bound. This page states the rules that cross
  routes, the field refusal codes, and how to run the service.
- `just schema` rewrites the document. `just schema-check` and
  `crates/logweir-api/tests/contract.rs` fail on drift.

## Modes

The configuration file must name a mode. There is no default: a file that names
none is refused rather than read as the more permissive one.

- **`mode: localAdmin`.** A loopback-only listener, the configured
  administrator as the only actor, and namespaces from the file. Not SSO and not
  a shared console. In the cluster it is the **in-cluster administrator mode**:
  no Service, no Ingress and no ingress NetworkPolicy rule, the `<release>-api`
  ServiceAccount as identity, readiness not gated on OIDC, and
  `kubectl port-forward deploy/<release>-api` as the only way in. Use it for
  isolated labs and break-glass administration.
- **`mode: shared`.** The SSO console: OpenID Connect identity, a short-lived
  encrypted session cookie, a CSRF token on every unsafe method, exact role and
  namespace bindings, and one audit record per request. It refuses a non-HTTPS
  `publicBaseUrl` before it binds anything.

## Running it

```yaml
# config.yaml
mode: localAdmin                       # the only accepted value
listen: "127.0.0.1:8484"               # a loopback IP literal; a hostname is refused
publicOrigin: "http://127.0.0.1:8484"  # the exact Origin unsafe requests must carry
uiDirectory: console/dist              # `npx vite build` in console/ writes it
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

- **Paths are taken literally.** A relative path resolves against the
  configuration file's own directory. A leading `~` is **not** expanded:
  `kubeconfig: ~/.kube/config` looks for a directory named `~` and the service
  exits 2. Write an absolute path, or omit `kubernetes.kubeconfig` to get the
  client's `KUBECONFIG`-then-`~/.kube/config` lookup. `kubernetes.context` is
  required either way.
- **Exit codes:** `0` after a clean `SIGTERM` or `SIGINT` (open connections
  get ten seconds); `2` when the configuration is refused, before any
  Kubernetes client or socket exists; `1` for any other failure.
- **Refused at start** (exit 2, field named): a `listen` that is not a loopback
  IP literal (`localhost`, `0.0.0.0`, `[::]` and routable addresses by name), a
  kubeconfig context that impersonates another identity, an unknown `mode`, a
  cursor key under 32 bytes. Shared mode adds *What startup refuses*.
- `/healthz` answers whenever the process is alive. `/readyz` also needs
  Kubernetes to answer for this identity (and, in shared mode, the provider).
  Neither body names an endpoint or a reason.

## Routes

`…` is `/api/v1/namespaces/{ns}`. Anything not listed is `404`. A domain with
no route has no stub: `GET /api/v1/session` reports it `false` under
`capabilities` (today: connection tests; `trustAdministration` and
`catalogWindowQuery` are `false` for everybody).

| Route | Action | What it does |
|---|---|---|
| `GET /healthz`, `GET /readyz` | public | liveness, readiness |
| `GET /ui/…` | public | the console (*The console's files*) |
| `GET /auth/login`, `GET /auth/callback` | public | shared-mode sign-in (*Sign-in*) |
| `GET /api/v1/session` | signed in | the actor, roles, grants, CSRF token, capability flags |
| `POST /api/v1/session/logout` | signed in | clears the session cookie (shared mode) |
| `GET /api/v1/namespaces` | signed in | the configured grants, never core `Namespace` objects |
| `GET /api/v1/cadence-previews` | `schedule.read`, any namespace | the next runs of a cron line or preset |
| `GET …/connections[/{name}]` | `connection.read` | `KafkaCluster` projections; Secret names, never a credential |
| `POST …/connections` | `connection.create` and `credential.write` | creates a `KafkaCluster` named `conn-<26 base32>` (*Credentials*) |
| `GET …/connections/{name}/topic-discoveries` | `topicDiscovery.read` | a page of discoveries, or `?latest=true` |
| `POST …/connections/{name}/topic-discoveries` | `topicDiscovery.start` | starts a bounded inventory |
| `GET …/topic-discoveries/{id}[/topics]` | `topicDiscovery.read` | one discovery; a page of its inventory |
| `POST …/topic-discoveries/{id}:cancel` | `topicDiscovery.cancel` | stops an unfinished discovery |
| `GET …/destinations[/{name}]`, `…/destinations/{name}/usage` | `destination.read` | destinations, grants as references, the last access test, what uses one |
| `POST …/destinations` | `destination.manage` | creates a destination under the `name` in the body |
| `POST …/destinations/{name}:update-access` | `destination.manage` | rotates the grants and the CA reference |
| `POST …/destinations/{name}:test` | `destination.manage` | starts a `DestinationAccess` `Preflight` (*Destination access test*) |
| `POST …/preflights` | `preflight.start` | starts a readiness check (*Readiness checks*) |
| `GET …/preflights/{id}[/details]` | `preflight.read` | one result (`?planHash=`); its detail pages |
| `POST …/preflights/{id}:cancel` | `preflight.cancel` | stops an unfinished preflight |
| `GET …/schedules[/{name}]` | `schedule.read` | `BackupSchedule` projections |
| `POST …/schedules` | `schedule.create` | creates a schedule (*Schedules*) |
| `PUT …/schedules/{name}` | `schedule.editPolicy` | replaces the future policy |
| `POST …/schedules/{name}:set-suspension` | `schedule.setSuspension` | suspends or resumes |
| `GET …/backups[/{name}]` | `backup.read` | `Backup` projections |
| `POST …/backups` | `backup.create` | Back up now |
| `GET …/restores[/{name}]` | `restore.read` | `Restore` projections; `planBytes` on one restore only |
| `POST …/restores` | `restore.create` | creates a `Restore` (*Restores*) |
| `POST …/restores/{name}/approval` | `approval.submit` | a governed countersignature, or the two `drill approve` files |
| `GET …/restores/{name}/approval-request` | `approval.read` | a two-person request, as the approver is shown it |
| `POST …/restores/{name}/console-approval` | `approval.submit` | the second person's click |
| `GET …/switchovers[/{name}]` | `restore.read` | `Switchover` projections; `planBytes` on one switchover only (PROD-04.2) |
| `POST …/switchovers` | `restore.create` | creates a `Switchover` from a reviewed plan, under the restore create's gates |
| `POST …/switchovers/{name}/approval` | `approval.submit` | the restore route's submission, for subject kind `Switchover` |
| `GET …/switchovers/{name}/approval-request` | `approval.read` | a two-person switchover request: every group, topic and position |
| `POST …/switchovers/{name}/console-approval` | `approval.submit` | the second person's click on a switchover |
| `GET …/approval-policy` | `approval.read` | the namespace's effective approval policy |
| `GET …/approvals[/{name}]` | `approval.read` | Approval metadata, no documents |
| `GET …/approvals/{name}/packet` | `approval.readPacket` | the approval and sidecar documents, verbatim |
| `GET …/operations/{kind}/{name}` | `operation.read` | the normalized status of a backup, restore, discovery or preflight |
| `GET …/operations/{kind}/{name}/events` | `operation.read` and `operation.stream` | the same as a bounded event stream (backup or restore) |
| `GET …/protection-policies[/{name}]` | `protection.read` | protection health; alert delivery is withdrawn in v1 (release-notes item 74) |
| `GET …/catalogs[/{name}[/points\|/signers]]` | `catalog.read` | catalogs, their point view, their signers |
| `POST …/catalogs` | `catalog.connect` | connects an existing archive under the `name` in the body |
| `GET …/retention-policies[/{name}]` | `retention.read` | retention policies and their last evaluation |
| `GET /api/v1/trust-policies[/{name}]` | `trustPolicy.read`, any namespace | the installation's trust policies (*Trust*) |

**Who holds an action.** Roles are bound per namespace in shared mode. In
localAdmin mode the one actor holds every action in its namespaces, and the
approval routes still refuse it its own requests.

| action | viewer | operator | approver | administrator |
|---|:--:|:--:|:--:|:--:|
| `connection.read`, `destination.read`, `topicDiscovery.read`, `schedule.read`, `protection.read`, `catalog.read`, `retention.read` | ✓ | ✓ | | ✓ |
| `preflight.read` | ✓ | ✓ | restore checks only | ✓ |
| `backup.read`, `restore.read`, `approval.read`, `operation.read`, `operation.stream` | ✓ | ✓ | ✓ | ✓ |
| `connection.create`, `connection.test`, `credential.write`, `destination.manage`, `topicDiscovery.start`, `topicDiscovery.cancel`, `preflight.start`, `preflight.cancel`, `schedule.create`, `schedule.editPolicy`, `schedule.setSuspension`, `backup.create`, `restore.create`, `catalog.connect` | | ✓ | | ✓ |
| `approval.readPacket` | | ✓ | ✓ | ✓ |
| `approval.submit` | | | ✓ | |
| `trustPolicy.read` | | | | ✓ |

- **An administrator is not an approver.** One who must approve is bound as an
  Approver too, and separation of duties still compares `(issuer, sub)`.
- **Editing a schedule's policy is the authority to create one.** The console
  reads one flag, `scheduleCreate`, for both; `tests/role_matrix.rs` pins the
  two rows together. The audit record says `schedule.editPolicy`.
- **An approver reads only `Restore` readiness**; any other preflight answers
  as if it did not exist.
- **An operator cancels only the checks it started**, by the `issuer#subject`
  recorded at creation. An administrator of the namespace may cancel any; the
  audit line then records `cancelledAnotherActorsCheck`. Cancelling deletes no
  archive byte, topic, run or evidence.

## Conventions

**Errors are `application/problem+json`**, always, with a stable `code` and the
`requestId`:

```json
{"type": "https://logweir.dev/problems/validation-failed", "title": "Request validation failed",
 "status": 422, "code": "validation_failed", "detail": "One or more fields are invalid.",
 "requestId": "01M2N5SHWR0N2E7SDCW2H22Z8W", "retryable": false,
 "errors": [{"field": "bootstrapServers[0]", "code": "invalid_port",
             "message": "must be host:port with no scheme or userinfo"}]}
```

- **`errors[].field` is a path and nothing else** (`topics[2]`,
  `access.archiveWrite.mode`, a header or query name). `message` is a sentence.
  A console may match `field` exactly.
- **One condition has one `errors[].code` on every route**: an unreadable
  cadence is `schedule: schedule_invalid` and an unknown zone `timeZone:
  timezone_unknown` from the schedule create, the edit and the cadence preview.
- **Bodies are strict and capped at 1 MiB.** An unknown field is
  `422 validation_failed`; an unknown or repeated query parameter is
  `400 malformed_request`.
- **Three guards run before any handler.** `Impersonate-*` is
  `400 header_not_allowed`. A `Host` this listener does not serve is
  `421 misdirected_request` (the DNS-rebinding guard). An unsafe method needs an
  `Origin` exactly equal to the public origin (`403 origin_mismatch`) and
  `Content-Type: application/json` (`415`); shared mode adds the CSRF token.
- **Every response** carries a fresh `X-Request-ID` (a client's is ignored), a
  Content-Security-Policy, `X-Content-Type-Options: nosniff`,
  `Referrer-Policy: no-referrer`, `X-Frame-Options: DENY`, a Permissions-Policy,
  both Cross-Origin-*-Policy headers and `Cache-Control: no-store`. There is no
  CORS layer. One log line per request: method, path, status, latency.
- **Nothing credential-shaped is in a response**: no Secret data, token,
  kubeconfig, raw pod log or unredacted upstream error.

**Durable `POST`s require `Idempotency-Key`** (8 to 128 visible ASCII
characters). The object's name is a per-route prefix plus 130 bits of
`SHA-256(issuer, subject, namespace, route, key)`, so a lost response, a double
click, a restart or a second replica all target one name. The same key and
request replays as `200` with the same UID; the same key with a different
request is `409 idempotency_conflict`; an object this scope did not create is
`409 state_conflict` and is never adopted. The raw key is never stored or
logged. A new operation uses a new key. A member added to a create body later
is left out of the request hash when absent, so an old-shape retry replays.

**Writes to an existing object take a precondition instead**:
`expectedGeneration` (`PUT …/schedules/{name}`, `:update-access`) or
`expectedResourceVersion` (`:set-suspension`, on the whole object, so a
controller status write in between is `412 precondition_failed`: read and
resend). The precondition is checked before any field rule, so a stale write
is always `412` and nothing is written.

**Lists** default to `limit=50`, at most 200: `{items, page: {limit,
nextCursor, snapshot}}`. A cursor is MAC-authenticated with the cursor key and
binds the actor, route, namespace, filters, a fifteen-minute expiry and the
Kubernetes continue token. Tampered or replayed elsewhere is
`400 cursor_invalid`; expired is `410 cursor_expired` (restart the list).
Filters are exact names and label selectors.

**Connections.** HTTP/1.1 only; an HTTP/2 preface is closed at its first line.

| bound | value | past it |
|---|---|---|
| request head | 10 s from accept, and after each answer on keep-alive; 32 KiB | closed |
| a stalled answer | 30 s with no progress (about 35 s on macOS) | the connection ends; a steady reader and a read event stream are never cut |
| a request body | 32 KiB per 30-second window | `400 malformed_request` ("stopped arriving, or arrived too slowly") |
| a JSON body's total | 60 s | `400 malformed_request` ("not received within 60 seconds") |
| connections | 256 at once | wait in the listen queue |
| one peer | 32 at once, in shared mode with a trusted proxy configured | closed at accept; the log says `closed a connection at once` |
| event streams | 4 per actor per namespace; a stopped reader holds one up to 310 s | `429 rate_limited`, `Retry-After` |

**The per-peer cap.** The trusted proxy (`trustedProxyService` or
`trustedProxyCidrs`) is never capped. With none configured nobody is capped: the
start line `logweir-api started` shows `per_peer_cap` 0 and a warning. While a
`trustedProxyService` set is stale the console trusts nobody, so the ingress is
capped and `/readyz` is false. localAdmin mode has no cap. The chart refuses a
shared console behind its Ingress with no trusted proxy, unless
`api.console.trustedProxy: none` opts out
([chart README](../charts/logweir/README.md)).

- A **service-mesh sidecar** that re-originates connections (Istio `127.0.0.6`,
  Linkerd `127.0.0.1`) makes every client one peer: name it as a `/32`, or opt
  out.
- A **wide `trustedProxyCidrs` range** caps nothing inside it. Keep it to the
  ingress controller's pods. **Kubelet probes** share their node's address.
- **What the bounds do not stop:** a fast-reading client keeps a connection
  while its pipelined answers last. The per-peer cap bounds that to 32 per
  address; with no trusted proxy, or in localAdmin mode, one client can hold
  all 256. Only an enforcing NetworkPolicy keeps such peers away, and
  `api.console.networkPolicy.enabled` is off by default.

## Operations

A backup or restore answers an `OperationViewResponse`; a discovery or a
preflight a `CheckOperationResponse`, with no result, evidence or verification.
**An absent member means "not observed", never a flattering default**; each
member's rule is in its schema description. Beyond those:

- An unrecognised phase is `unknown/UnrecognizedPhase`. An active run not
  observed for 300 s, or an object with no status 120 s after creation, is
  `stale: true` and `unknown/StatusStale`; a terminal object never goes stale.
- A stage never overrides a terminal phase: `phase`, `exitCode` and `outcome`
  own the outcome. An `outcome` is the scorecard's claim until
  `verifiedSuccess`.
- `teardown.deleted` and `lastEnforcement.deleted` are lists of names.
- The Job name, pod name and container state are not published.

**The event stream** accepts no query parameter (`malformed_request`), so no
token rides in the URL. It is authorized once, at subscribe. A malformed
`Last-Event-ID` is `malformed_request`. It ends with `end` and a reason:
`maxDuration` after 300 s (reconnect), `settled` once a terminal operation's
verification settled, `vanished` when the object is deleted. Past the stream
ceiling, fall back to polling the read route.

## Schedules

`GET /api/v1/cadence-previews` runs the scheduler's own `weirkeeper::cadence`
module; the browser never evaluates cron. A repeated local time fires **both
instants**. A shorter list than `count` is a real answer. On a saved schedule,
**`status.nextRuns[0].at` in the past is the staleness signal**, and an absent
`status.activeRuns` means not computed yet.

**`POST …/schedules`** takes the whole policy: `schedule`, `timeZone?`,
`sourceRef` (set once), `topics` xor `allUserTopics`, `archive` xor
`destinationRef`, `concurrencyPolicy?`, `startingDeadlineSeconds?`,
`catchUpPolicy?`, `retry?`, `activeDeadlineSeconds?` and `suspended`. Absent
means UTC, 3600, `None`, no retries, 3600 and `Forbid`. The name is
`sch-<26 base32>`. An empty selection is `topics: selection_invalid`; more than
256 named topics is `count_out_of_range`. `retention` is withdrawn in v1
(release-notes item 78): it is stored as `spec.retention`, reports nothing, and
the schedule carries `RetentionReportWithdrawn`. Use a `RetentionPolicy`.

**`PUT …/schedules/{name}`** takes the same policy with `expectedGeneration`
and `topicSelection`, and **a field omitted is removed**. It changes the future
only: an existing `Backup` keeps its copied policy and its Job.

- `sourceRef` is refused before anything is written: `422`,
  `sourceRef: field_immutable`. A different cluster is a different schedule.
- A different current generation, or an edit landing between read and write, is
  `412 precondition_failed`. A console that sees `generation` absent asks for a
  reload and never sends `expectedGeneration: 0`.
- The CRD's own rules come back as `422` with the CRD's message:
  `topicSelection: selection_invalid`, `retry.maxRetries: schedule_invalid`
  (retries on a name over 29 characters), `destinationRef:
  destination_sentinel_mismatch` (a shape rule, not `field_immutable`). An
  unrecognised refusal is a generic `422`, its reason in the service log.
- The sentinel `archive.url` (`logweir-destination://<name>`, no `secretRef`)
  is built by the route and never accepted from a body.
- "Omitted is removed" covers the fields this build knows. Upgrade CRD, then
  controller, then API; an older API preserves a newer CRD's field and the
  response shows it.

**`POST …/backups`** (Back up now) creates the manual `Backup` that
`kubectl create -f config/samples/backup-manual.yaml` creates, named
`logweir-manual-<26 base32>`. From a schedule, `{scheduleRef: {name,
expectedGeneration?}}` copies its current revision; a policy field beside it is
`422`. Ad hoc, `{sourceRef, topicSelection, legacyArchive xor destinationRef,
deadlineSeconds?}` needs no schedule. Either may carry `readinessAcknowledgement
{preflight, state}`, which becomes the annotation `logweir.dev/readiness-ack`
and nothing more: readiness is recorded, never obeyed.

- A suspended schedule, an active `Forbid` run or a schedule deleted meanwhile
  never blocks the run.
- `409 policy_changed`: `expectedGeneration` names a superseded revision. The
  problem's `policy` member carries `currentGeneration` and
  `currentRunPolicySha256`; confirming again is a new intent.
- `201` means the `Backup` exists. At most
  `runs.maxManualBackupsActivePerNamespace` (default 4) manual runs hold a
  runner slot; the rest wait `queued` / `ConcurrencyLimited` with nothing
  created, in creation order.

## Restores

`POST …/restores` stores `planBytes` unchanged, checked against `planHash`.
There is no cancel and no delete for a `Backup` or a `Restore`. At most
`runs.maxManualRestoresActivePerNamespace` (default 2) admitted manual restores
run at once per namespace; the rest wait `queued`, and an authorization that
expires while queued is refused `AuthorizationExpired`.

### Approval policy

The console reads the installation's approval-policy document
(`approvalPolicyFile`, the file the controller mounts) and its
`ConsoleConfirmation` key (`confirmationKeyFile`). Exit 2: a served namespace
bound to a policy without the key; a key file that does not exist, unless
`confirmationKeyManaged: true` says the identity hook writes it (then a confirm
request before it exists is `409 policy_mismatch`, nothing created);
`confirmationKeyManaged` without `confirmationKeyFile`.
`installationIdentity: {namespace, publicConfigMap}` names where the
fresh-install marker is read. The modes are in [kubernetes.md](kubernetes.md).

**The create's `authorization` says where the submission goes next:**

| namespace | the console | `authorization.state` | next |
|---|---|---|---|
| unbound (`legacy-governed-v1`) | signs nothing | `awaitingApproval`, `legacy: true` | the approval page |
| `confirm` (`Ordinary`) | signs authorization document v2 (UID, plan hash, requester, policy digest) as the `Approval` `spec.approvalRef` names | `confirmed` | the operation view |
| `strict` (`Governed`) | signs the same as `<approvalRef>-confirmation`, which authorises nothing | `awaitingApproval`, `confirmationName` | the approval page, where an approver countersigns |
| `two-person` | as `strict` | `awaitingApproval`, `operatorMode: two-person` | the console approval below |

A replay completes an interrupted sequence, never signs twice and never adopts
an `Approval` this Restore did not produce (`409 state_conflict`). A localAdmin
console confirms as `urn:logweir:local-admin#admin`. Refused before anything is
created:

| request | answer |
|---|---|
| a `confirm` namespace before the console's key exists | `409 policy_mismatch` |
| the fresh-install marker cannot be read (not a `404`) | that read's error |
| `strict` or `two-person` without `ticket`, or a blank, padded or over-128-character one | `422`, `ticket` (`required` / `invalid`) |
| `ticket` in an unbound namespace | `422`, `ticket`, `not_accepted` |
| `strict` or `two-person` with `approvalRef.name` over 240 characters, or ending in `-confirmation` | `422`; the suffix is `reserved_suffix` |

**`POST …/restores/{name}/approval`** takes `{"sidecarBytes": "…"}`, the file
`logweir drill countersign --document <approvalBytes> --confirmation
<sidecarBytes> --key <privkey> --out approval.sig` wrote over
`GET …/approvals/<approvalRef>-confirmation/packet`. In an unbound namespace it
takes `{"approvalBytes", "sidecarBytes"}` from `logweir drill approve`, stored
byte for byte; it verifies no signature, but refuses a sidecar that is not a v1
approval's (`409 policy_mismatch`), a `plan_hash` that is not the Restore's
(`422`, `plan_mismatch`) and a missing document (`422`, `required`). The
controller verifies every signature, the key's usage and that the approver is
not the requester before any Job.

| refusal | when |
|---|---|
| `403 forbidden` | not an Approver here, or the submitter is the requester, whatever other roles it holds |
| `409 policy_mismatch` | a `confirm` namespace; a confirmation for another UID, plan or policy digest (submit the Restore again); a `two-person` namespace (`countersignature_not_accepted`) |
| `404 not_found` | no such Restore, or no console confirmation |
| `409 state_conflict` | expired, or another Approval holds the name |
| `422 validation_failed` | not a sidecar or no added signature; `approvalBytes` under `strict` (`not_accepted`); private-key text in either field (`private_key`, never echoed) |

#### Two-person approval in the console (PROD-16.2)

A `two-person` namespace takes its approval from a second signed-in person.
More refusals at `POST …/restores`:

| request | answer |
|---|---|
| through a localAdmin console | `409 policy_mismatch` |
| a requester identity that cannot be compared (empty, over 255 characters or non-ASCII; an issuer with `#`; a Kubernetes system identity) | `409 policy_mismatch` |
| a ticket that is not printable ASCII, or over 1024 characters | `422`, `ticket`, `not_showable` |
| a scope that cannot be shown whole: over 1024 topics or 8192 partitions, a non-Kafka name, an unshowable value, an unparseable plan, a notification sink | `422`, `planBytes`, `scope_incomplete` |

`GET …/approval-request` verifies the console's own signature on the stored
request before projecting anything, and `approve.offered` is advisory.
Beside the signed `requester` it serves `requesterEmail`, the requester's email
when the identity provider marked it verified (`email_verified: true`), and
`requesterName`, only when `oidc.displayNameClaim` is configured: self-asserted at
most identity providers, so shown as unverified and never in place of the
identifier. Neither is part of the signed request: both travel in a display
document the console signs with its confirmation key (its own payload type) and
binds to the request's approval name, `requester` and plan hash, stored in the
annotations `logweir.dev/requester-display` and `-display-signature`. They are
served only when that signature verifies for this request; otherwise neither is,
and `requesterDetailsNotVerified: true` says so. An email is shown only exactly
as received (printable ASCII, `local@domain`, at most 254 bytes), never
normalised or cut; the name is cleaned and capped at 128 characters. A request
made before the document existed has neither and no flag.
`POST …/console-approval` takes `{"confirmationSha256": "sha256:<64 hex>"}` and
nothing else: `201` with the `Approval`, or `200 replayed: true` for the same
approver's second click. Checked in order:

| check | refusal |
|---|---|
| session; Approver role here (an Administrator is not one unless bound) | `401`; `403`, or `404` with no grant |
| exact `Origin`, CSRF token, JSON | `403 origin_mismatch`, `403`, `415` |
| a shared console | `409 policy_mismatch` (`local_admin_cannot_approve`) |
| a two-person policy | `409 policy_mismatch` (`not_a_console_approval_policy`) |
| the console's signature, naming this Restore, UID, plan and policy | `409 policy_mismatch` (`confirmation_not_verified`, `confirmation_not_bound`); `404` (`confirmation_absent`) |
| not expired; hash matches; scope complete now | `409 state_conflict` (`request_expired`, `request_changed`, `scope_incomplete`) |
| not the requester (issuer lower-cased without a trailing `/`; subject lower-cased) | `403` (`self_approval_forbidden`) |
| inside the window; nothing holds the name | `409 state_conflict` (`approval_outside_window`, `already_approved`) |

The stored `Approval` is the request with `approver` and `approvedAt` added, as
`formatVersion` 2.2.0, signed by the console's key. The audit notes of a click
are `requester`, `approverPrincipal`, `requesterIssuer`, `requesterSubject`,
`requesterSha256`, `approverIssuer`, `approverSubject`, `approverSha256`
(a note is cut at 200 bytes, so the digest tells long identities apart),
`planHash`, `policyDigest`, `scope` (`complete` or `incomplete:<reason>`),
`scopeTopicsShown`, `scopeTopicsInPlan`, `separation` (`distinct` or
`refused`), `approvedAt`, `expiresAt`, `approval`, and on a refusal the
`failureCode` above.

#### Switchovers (PROD-04.2)

The switchover routes are held to every gate above, by the same functions
(`routes::subject`): `POST …/switchovers` runs the restore create's refusals
before the plan's own grammar, `POST …/switchovers/{name}/approval` is the
restore submission (bounds, the private-key refusal, `subject_kind_mismatch`
for a v1 document of another kind, `not_an_approval` for one that does not
parse), and `…/approval-request` and `…/console-approval` are the restore
read and click. A switchover's scope is the restore its plan applies and
every group, topic and position with its class (`approximate`) and the
position the group must hold first (`expected`, absent for none):
`tooManyGroups` over 100 groups, `tooManyPositions` over 8192 positions,
counted before the grammar. Its click's audit notes say `scopeGroupsShown`,
`scopePositionsShown` and `scopePositionsInPlan` in place of the topic counts.
The stored `Approval` has `subjectRef.kind: Switchover` and the document's
`subject.kind` is `Switchover`, which every verifier compares.

### Topic mapping and original names

`topicMapping` declares the previewed rows; it is never stored, is defined for
`newTopic` mode only, and is checked against `target.topicNaming.prefix` when
that prefix is legal. `target.topicNaming.originalName: true` restores under
the source names into topics that do not exist; it needs `coverage: complete`.
In a `confirm` namespace the request also carries
`originalNameConfirmation: {typedTopics: […]}`, the names re-typed, which the
console signs; the route reads only `source.topics` from the plan for this.
Such a document is `formatVersion` 2.1.0.

| `errors[].field` | `errors[].code` | when |
|---|---|---|
| `topicMapping` | `unsupported_for_mode`, `empty`, `too_many` | `scratch` mode; no rows; over 1000 rows |
| `topicMapping[i].source` | `invalid_topic` | not `^[a-zA-Z0-9._-]{1,249}$` |
| `topicMapping[i].target` | `mapped_name_illegal`, `mapping_identity`, `mapping_mismatch`, `duplicate_mapping` | illegal name; target equals source without `originalName`; not `prefix + source`; two rows, one target |
| `target.topicNaming.originalName` | `requires_new_topic` | `scratch` mode |
| `target.topicNaming.prefix` | `prefix_with_original_name`, `invalid_prefix` | a prefix with `originalName`; otherwise empty, over 128 characters or illegal |
| `coverage` | `original_name_requires_complete` | `originalName` without `coverage: complete` |
| `approvalBytes` | `approval_subject_mismatch` | the signed `approval_subject` is not the Restore's |
| `originalNameConfirmation.typedTopics` | `typed_topics_required`, `typed_topics_mismatch` | missing; not exactly the plan's `source.topics` |
| `originalNameConfirmation` | `not_accepted` | on any other request |
| `planBytes` | `invalid` | a plan that does not parse, where the route reads it |

### The restore's coverage (PROD-08.1a)

`coverage: complete` and `completeMaxRecords` declare what the plan says; the
controller ends a mismatch `Failed` / `ExecutionSpecInvalid`.
`completeMaxRecords` below 1 is `out_of_range`, and without complete coverage
`requires_complete_coverage`. Complete coverage costs about a minute per GiB of
one-KiB records, against about five seconds sampled. **`covered: false` is
never a pass** (`CompleteNotCovered`). `coverage`, `timeBasis`, `selection`,
`integrityResult`, `integrityPartialReason` and `targetTopicsAppeared` are claims
of the signed scorecard, described in the OpenAPI document; `topicPreflight` and
`oldTopics` are what phase 0 read and what the restore read from.

## Destinations and credentials

The create checks the location rules first: `422 destination_invalid`, one
field error per rule (`bucket_invalid`, `endpoint_not_origin`,
`transport_scheme_mismatch`, `insecure_http_requires_http_endpoint`,
`ca_bundle_requires_tls`, `prefix_reserved`, `addressing_unsupported_by_engine`,
`grant_mode_fields`). Location and transport are immutable. A second
`default: true` is `409 state_conflict`; a default set by hand with only the
annotation is honoured on read but not seen by that check. `:update-access`:
a stale generation is `412`, a rejected rotation
`409 destination_location_immutable`, a CA on plaintext
`409 transport_downgrade_forbidden`. `POST …/destinations:from-legacy` is
withdrawn in v1 (release-notes item 75).

### Credentials

**A credential is entered once and never named.**

- **Connections:** `auth.credential` carries `password` (`scramSha512`,
  `scramSha256`, or `plain`, which needs TLS: else `422 plain_requires_tls`) or
  unencrypted `certificatePem` and `privateKeyPem` (`mtls`). It becomes the
  Secret `<connection>-credential`, owned by the connection and carrying its
  `logweir-binding`. A private CA is a ConfigMap key.
- **Destinations:** `access.<role>.secret.new` becomes `lwd-<destination>-<role>`
  (type `logweir.dev/object-store-credential`), owned and bound the same way.
  `:update-access` writes a new value under `lwd-<destination>-<role>-<8 hex>`
  and leaves the superseded Secret for you to delete; it accepts
  `secret.existing` only for a Secret the destination already names.
- Naming any other existing Secret (`auth.credentialRef`, `secret.existing` on a
  create) is `422 existing_credential_refused`. Every runner enforces the
  binding however the object was written.
- **A taken name is `409 state_conflict` and nothing is written.** Every name is
  checked first with a `dryRun: All` create carrying no data, and the refusal
  names each one. To rotate, use `:update-access`, or change the Secret out of
  band keeping its `logweir-binding` key.
- **The retry contract.** The destination is written before its Secrets. If a
  Secret write fails, **repeat with the same `Idempotency-Key`**; a new key is
  refused. The audit line separates `credentialSecretsCreated` from
  `credentialSecretsReplayed`. A name taken by someone else in between is
  refused with the Secrets already created, which you delete before retrying.
- **No digest covers a value.** Write-only fields hash as `<write-only>`, so a
  value is not part of a create's identity. An object an older build created
  replays as `409 idempotency_conflict`; on such a destination remove the old
  hash (`kubectl annotate backupdestination <name>
  api.logweir.dev/request-sha256-`) or rotate the key.

### Destination access test

`:test` with no `roles` probes every role a run uses this destination's
credentials for: `archiveWrite`, `archiveRead` and `evidenceWrite` (unset, the
last two use the `archiveWrite` credential, and the `archiveRead` list dials
it), and `evidenceRead` when it is set. With `writeProbe: createOnlyMarker` it
creates `logweir/readiness/<uid>.json` as the `evidenceWrite` grant, which
answers `destination.evidenceWritable`; with `disabled` the row is
`WriteNotProbed`. `destination.evidenceReadable` is the `evidenceRead` grant's
answer, and `destination.archivePrefixWritable` is never probed.
`destination.credentialBound` (blocking) is `notReady` /
`CredentialBindingMismatch` for a grant whose Secret is unbound or bound to
another object, naming the field, destination and Secret.

## Checks

**Topic inventory.** The API reads the controller's immutable chunks by the
names the status records and verifies each (owner, immutability, digest), or
answers `409 result_integrity_failed`. A successful list is never called
complete: `attestedComplete` without an `attestation` is served as `unknown`,
with `attestationMissing` in `basis`.

**Readiness checks.** A body names one `operation` (`backup`, `restore`,
`destinationAccess`, `sourceConnection`, `switchover`) and that block only,
else `422` naming both. A `switchover` block names `restoreName` (and
`restoreUid` when known) and at most 100 `groups` (`source`, optional
`target`; a blank or control-character id is `invalid_group`, a target named
twice `duplicate_target`); an approver may read it, as a restore check. A restore check names `recoveryPoint` or `catalogPoint`, not both
(`422 exactly_one`); a `pointId` that is not `lwp1-` plus 32 lowercase hex is
`422 invalid_point_id`. `applicable`, `stale` and `staleReasons` are recomputed
on every read against the `?planHash=` you send; the verdict is advisory.
`caBundleChanged` and `inputsDigestChanged` are reserved: nothing emits them,
so do not branch on them. Anything the API could not compare is
`unverifiable`, never applicable. The installation policy digest is compared
by the controller (`staleBasis` `policyDigest:byController`). A readiness key
replays only while its check can still be the answer; send a new key once it
is expired, inapplicable, `failed` or `cancelled`, and one per click for a
connectivity test.

## Catalogs, retention, trust

No response carries a credential. An archive credential is published by name
(`legacyArchive.credentialRef`), a deletion credential as
`credentialConfigured: true`, and every archive URL without userinfo.
`/signers` offers no trust button: an administrator adds a key with
`kubectl apply`. A schedule's `spec.consumerGroups` is set by the schedule
create's `consumerGroups` (refused by field as the runner would refuse it) and
shown on the schedule and on each run; an edit leaves it alone, and `kubectl`
changes it. Retention and catalog members are described in the OpenAPI
document; nothing in a retention evaluation was deleted.

### Trust

A key's state is the controller's verdict only while its evaluation is fresh;
otherwise it is `unknown`, which is not `valid`. `GET /api/v1/trust-policies`
needs the administrator role in at least one namespace and serves only the
policies governing a namespace the reader administers, plus the installation
`default`; anything else is `404`. A console reads
`capabilities.trustPoliciesRead` from the union of its grants. Trust writes are
`kubectl apply` and the `logweir trust` helpers.

## The Kubernetes boundary

Every call is a method on one adapter typed over a sealed set: `KafkaCluster`,
`BackupSchedule`, `Backup`, `Restore`, `Approval`, `BackupDestination`,
`TopicDiscovery`, `Preflight`, `ProtectionPolicy`, `RecoveryCatalog`,
`RetentionPolicy`, the cluster-scoped `TrustPolicy` (read only) and a `get` of
`TrustRoster/default`. No method takes a group, version, plural or path, or
reaches a Pod, log, exec stream, Job or core `Namespace`. There is no delete.

- `ConfigMap` is `get` only, for documents a check owns. `Secret` is `create`
  only: no read verb exists, so a credential cannot be read back.
- `create` on Secrets is wider than the routes until the chart's
  `admissionPolicy.enabled` fences it to the Logweir credential types.
- Four updates, each a merge patch with `metadata.resourceVersion`:
  `spec.suspend`, a schedule's policy, a destination's grants, a check's
  `spec.cancelRequested`. `spec.sourceRef` and a destination's location have no
  key in any of them. `tests/linkage.rs` pins the `(verb, resource)` set.
- Every call has a 10-second deadline (`504 upstream_timeout`). Kubernetes
  messages are redacted before they are logged and never returned.

## The console's files

`/ui/` serves an allowlist built at startup from `uiDirectory`, the console's
build (`console/dist`): the top-level `*.html`, `*.js` and `*.css` files and,
in `assets/`, the hashed scripts, styles, `woff2` fonts and `svg` images.
Other files and directories, dotfiles and symbolic links are never served.
Paths are matched exactly, so `/ui/../Cargo.toml` is `404`. `/ui/` is
`index.html`, `no-cache`; a file in `assets/` is
`Cache-Control: public, max-age=31536000, immutable`. The service adds
`favicon.svg`. The page learns its state from `GET /api/v1/session`.

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
  scopes: [openid, profile, email, groups]         # must contain `openid`; `email` names a requester to an approver
  groupsClaim: groups                              # the EXACT claim name
  # displayNameClaim: name                         # OPT-IN: self-asserted at most identity providers; shown as unverified
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

A key file is two lines:
`printf 'version: 1\nkey: "%s"\n' "$(openssl rand -base64 32)" > session.key.yaml`.

- **`allowedHosts` widens the DNS-rebinding guard**: each entry is one more name
  a rebinding page may use. Add only a second name that reaches this service;
  there is no wildcard. The probes are exempt.
- **`trustedProxyService: {namespace, name}`** trusts the serving endpoints of
  the ingress controller's Service, re-read every five seconds, each as a
  single host. A failed refresh keeps the last set for thirty seconds, then
  trusts nobody (`421`, `/readyz` false). Whoever can edit that Service, its
  `EndpointSlice`s or a matching Pod can add an address. A `hostNetwork`
  ingress publishes its node's address.
- **`caBundleFile`** adds a private issuer CA to the system roots;
  `systemRoots: false` needs a bundle. Chain, host name and exact `iss` are
  still verified. The console logs why the provider is not usable:
  `invalid peer certificate: UnknownIssuer` is a missing bundle, an
  unresolvable name a missing `hostAliases` entry. Point a `hostAliases` entry
  only at an endpoint the IdP's owner alone can route, never a shared ingress.

### What startup refuses

Each is exit 2 with the field named.

| refusal | why |
|---|---|
| `publicBaseUrl` not `https://`, or with a path, trailing slash, userinfo or no host | the redirect URI is this value plus `/auth/callback` |
| `publicOrigin` present | it is derived from `publicBaseUrl` |
| missing `oidc`, `roles`, `sessionKey` or `cursorKey`; `localAdmin` or `cursorKeyFile` present | the field is named |
| a key file missing, malformed or under 32 bytes, or its `version` not `expectedVersion` | no default key; an unexpected rotation. Bumping both ends every session |
| the client-secret file missing or empty | |
| an `allowedAlgorithms` entry other than `RS256`, `ES256` | `none` cannot be listed |
| `oidc.issuer` ending in `/`, or with a query, fragment or userinfo | `iss` is compared exactly |
| a plain-HTTP issuer, unless `oidc.insecureLoopbackIssuer` with a loopback host | |
| `oidc.systemRoots: false` without a bundle; a bundle that is unreadable, empty, malformed or holds anything but `CERTIFICATE` blocks | |
| an unknown role; a binding namespace outside `namespaces`; a binding with neither `groups` nor `subjects`; `*` or `?` in a binding | bindings are exact |
| `roles.revision` empty | it is in every audit record |
| `sessionMaxAgeSeconds` outside 60…900 | |
| `requireTrustedProxy` with no proxy named, or a range wider than `/16` (IPv4) or `/48` (IPv6) | |
| `trustedProxyService` not two DNS-1123 labels; it or `requireTrustedProxy` in localAdmin mode | |
| `kubernetes.principal` not `system:serviceaccount:<namespace>:<name>` | it is stamped on every object |

### Access, identity and readiness

- **Every route declares who may reach it** in
  `crates/logweir-api/src/access.rs`: `Public`, `Authenticated`,
  `Namespaced(actions)`, `Command {verbs}` or `AnyNamespace(action)`. One layer
  enforces it before any handler, namespace first, and records the decision. An
  undeclared route is `500 internal_error` (`route_access_undeclared`), an
  undeclared method `405`; `tests/route_access.rs` holds the table to the
  router.
- **Actor identity is exactly `(issuer, sub)`.** Groups and subjects match
  bindings by exact string; bindings union (the action table is under
  *Routes*). `X-Remote-User`, `X-Remote-Group(s)`, `X-Remote-Extra-*`,
  `X-Forwarded-User`, `X-Forwarded-Email`, `X-Forwarded-Groups`,
  `X-Forwarded-Preferred-Username` and `X-Auth-Request-*` are stripped before
  routing and named in the audit line. Nothing derives from `Host`,
  `Forwarded` or `X-Forwarded-*` except the audit field `forwardedFor` and the
  sign-in rate-limit key, and only from a trusted proxy.
- **Enumeration resistance.** An ungranted namespace answers what a nonexistent
  object answers (`404 not_found`), decided before any Kubernetes call.
  localAdmin mode answers `403 namespace_forbidden`. The audit record has the
  real reason.
- **The trusted entry point.** With `requireTrustedProxy: true`, a socket peer
  that is not a trusted proxy is `421` (audit `untrusted_entry_point`, note
  `peerNotTrusted`), and so is a trusted peer without exactly one
  `X-Forwarded-Proto: https` (`forwardedProtoNotHttps`). The probes are exempt.
  The range must be the ingress controller's own pods: kube-proxy keeps a pod's
  source address through a ClusterIP.
- **Readiness** starts false and turns true once Kubernetes answers and the
  provider initialises (discovery naming the issuer with https endpoints, a
  non-empty key set). After that a provider outage fails sign-in closed and
  leaves sessions working until they expire.

### Sign-in

`GET /auth/login` starts Authorization Code with PKCE S256, `state` and `nonce`,
and answers `303`. The per-login secrets travel in a sealed, ten-minute
`__Host-logweir_login` cookie, so a login can finish on another replica. `next`
is accepted only as a path under `/ui/`.

`GET /auth/callback` validates the ID token: allowed `alg`, JWKS key by exact
`kid`, signature, exact `iss` and audience (`azp` with more than one), `exp`,
`iat` and `nonce`. **The browser never receives a provider token**; the process
holds no access or refresh token. Every `401` clears the login cookie. A
callback with no query, or from a cross-site navigation, is refused and clears
it.

| the callback | answer | audit `failureCode` |
|---|---|---|
| a state already redeemed on this replica (within 600 s) | `401`, "This sign-in was already used. Start again at /auth/login." | `login_state_replayed` |
| the first replay on another replica, or after a restart | `401`, "The sign-in could not be completed." (the provider refuses a used code) | `code_exchange_failed` |
| the token or key-set request stalled | `401`, "The sign-in could not be completed." | `provider_timeout` |
| discovery stalled, at `GET /auth/login` | `503 kubernetes_unavailable`, "The identity provider could not be reached. Try again shortly." | `provider_timeout` |

- A state is redeemed before the code is exchanged; a failed exchange still used
  it. Start again at `/auth/login`. The record holds 65,536 states per process
  and forgets the oldest when full (audit `usedSignInStates: full`; log `this
  console's record of redeemed sign-in states is full`).
- Every provider request has a ten-second deadline, body included, and a
  document over 512 KiB is refused. A refusal is `provider_unreachable`
  (discovery, key set) or `code_exchange_failed` (token); the warnings `the
  identity provider is not usable` and `a sign-in was refused` name the cause.
- JWKS are cached; an unknown `kid` refetches at most once a minute; cached keys
  outlive a provider outage by a day. Discovery is cached for an hour, failures
  not at all, so a stalled provider holds each stale-cache login for ten
  seconds.

### The session and the CSRF token

| | |
|---|---|
| cookie | `__Host-logweir_session`, `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`, no `Domain` |
| contents | session id, issuer, subject, display claim, the bindable groups, times, key version; never a provider token |
| size | too large is refused (`session_too_large`), never truncated |
| protection | ChaCha20-Poly1305, with the cookie name and key version as associated data |
| lifetime | at most 15 minutes; no refresh, no server-side table |
| CSRF token | `HMAC-SHA-256(session key, session id)`, from `GET /api/v1/session`, required in `X-CSRF-Token` on every unsafe method |
| logout | `POST /api/v1/session/logout`: it needs the exact `Origin`, `application/json` and the token |

A `401` from `GET /api/v1/session` is `unauthenticated` or `session_expired`.
Removing an identity at the provider takes effect within fifteen minutes.
Roles are re-derived per request, so removing a binding takes effect at once; a
binding added for a group the session does not carry takes effect at the next
sign-in.

### The audit record

One JSON object per request on the `logweir_api::audit` target: `auditId`
(`X-Request-ID`), `method`, `path`, `authenticationMode`, `actorId`,
`displayClaim`, `sessionIdHash`, `bindingRevision`, `roles`, `namespace`,
`action`, `resource`, `decision` (default `deny`), `policyDigest`,
`idempotencyKeyHash`, `requestHash`, `planHash`, `recoveryPoint`,
`kubernetesPrincipal`, `objectName`, `objectUid`, `objectResourceVersion`,
`httpStatus`, `latencyMs`, `failureCode`, `peer`, `forwardedFor`,
`ignoredIdentityHeaders`.

Every object the API creates carries `api.logweir.dev/actor`
(`<issuer>#<subject>`), `authentication-mode` (`oidc` or `localAdmin`),
`action`, `binding-revision`, `kubernetes-principal`, `recovery-point` (for a
restore), `request-id`, `request-sha256` and `idempotency-scope-sha256`.

Never logged: cookies, tokens, CSRF tokens, the raw `Idempotency-Key`, Secret
values, kubeconfig, approval, sidecar or plan bytes, raw pod logs, URLs with
userinfo. Dependency log targets (`kube_client`, `kube`, `hyper`, `hyper_util`,
`rustls`, `tower`, `h2`) stay at `warn` whatever `RUST_LOG` says. This is
attribution, not proof, and keeping stdout is the deployment's job.

### Rate limits

| what | limit per minute | key |
|---|---|---|
| `GET /auth/login`, `GET /auth/callback` | 20 | the client (below) |
| starting a topic discovery / a preflight | 6 / 20 | actor and namespace |
| `POST …/backups` | `rateLimits.manualBackupsPerMinute`, default 10 | actor, namespace, route |
| `POST …/restores` | `rateLimits.manualRestoresPerMinute`, default 5 | actor, namespace, route |

Past a limit the answer is `429 rate_limited` with `Retry-After` and nothing is
created. Windows are per console process, so two replicas allow twice the rate.
A run limit of `0` or above `600` is refused at start. A run counts after
authorization and validation, so a `422` spends nothing and a replay counts.

**The sign-in client** is, behind a trusted proxy, the rightmost
`X-Forwarded-For` hop that is not a trusted proxy, and otherwise the socket
peer (audit `loginRateKey`). `Forwarded` is never read. IPv6 clients count by
`/64`. Past 65,536 tracked clients a new one is served without a window
(`loginRateUntracked: tableFull`). There is no global budget.

- If kube-proxy or a load balancer replaces the client address before the
  ingress (`externalTrafficPolicy: Cluster`, an instance-mode load balancer),
  clients through one node share a budget. Keep the address with
  `externalTrafficPolicy: Local`, the PROXY protocol, or an L7 hop both Traefik
  (`forwardedHeaders.trustedIPs`) and the console trust.
- A range wider than the ingress pods, or an ingress that passes
  `X-Forwarded-For` through, lets a client spend another client's budget.
- **The residual:** a distributed attacker can spend this console's quota at
  the provider. Set the provider's per-client limits generously; alert on
  `loginRateUntracked`, `code_exchange_failed`, `provider_timeout`, bursts of
  `login_state_replayed` and `usedSignInStates: full`; keep the break-glass path
  `api.console.mode: localAdmin`, reached by `kubectl port-forward
  deploy/<release>-api`.

## Deployment

The image is `logweir-console` (`Dockerfile.console`): `logweir-api` and the
console's build at `/ui`, as `USER 65532:65532` with no `CMD`.
`scripts/check-image-api.sh` holds the served files to this tree's
`console/dist` and refuses a key-shaped path. The chart renders the principal under `api.enabled` (a
ServiceAccount, three ClusterRoles, a RoleBinding per namespace) and the
workload under `api.console.enabled`, which needs the first; its chart README
lists every object. A Service, an Ingress (TLS required, host
`publicBaseUrl`'s authority) and the ingress allow rule exist only in shared
mode. The configuration is an immutable, content-addressed ConfigMap that holds paths, never a credential, into read-only Secret volumes
under `/var/run/logweir/`. The chart generates no key. Shared mode renders only
with `controller.watchNamespaces`, so the controller cannot create a Job in the
release namespace that holds those keys. The pod runs as 65532, read-only root,
all capabilities dropped, one writable `/tmp`. Probes exist only in shared
mode; in localAdmin mode a refused configuration shows as `CrashLoopBackOff`,
exit code 2 and a log line naming the field. Values and commands are in
[charts/logweir/README.md](../charts/logweir/README.md) and
[install.md](install.md).

## Local administrator mode is not a shared console

From a laptop `mode: localAdmin` uses the kubeconfig identity, which **may be
cluster-admin**. In the cluster it carries only the `<release>-api`
ServiceAccount, and `create pods/portforward` in the namespace is its whole
authorization boundary. It is one actor either way. It must not bind a routable
address or get an Ingress, and a login in front would not add per-user
authorization. Under a `strict` policy it is the requester of everything it
submits, so it never approves its own request.

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
