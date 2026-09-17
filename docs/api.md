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
  console.
* `mode: shared` — the SSO console: OpenID Connect identity, a short-lived
  encrypted session cookie, a synchronizer CSRF token on every unsafe method,
  exact role and namespace bindings, and one audit record per request. It
  refuses a non-HTTPS `publicBaseUrl` before it binds anything.

`logweir-api` is **not packaged or deployed**. No image builds it, the Helm
chart has no `console` template or value, and `publish = false` keeps it out of
the release archives. It runs from a local build against a kubeconfig context.
Nothing about an existing installation changes when this crate is present, so
there is nothing to upgrade, migrate or roll back: removing the crate removes
the feature. The chart, image, ingress and NetworkPolicy work is a later stage.

**Shared mode is therefore not a supported deployment yet.** It is implemented
and tested, and it runs from a local build behind a TLS terminator, but the
console image, the chart's `console.*` templates, the ingress, the
NetworkPolicy, the API's own ServiceAccount and its per-namespace RoleBindings
do not exist. Until they do, the API runs with whatever Kubernetes identity its
kubeconfig carries, which may be cluster-admin: the closed adapter is a
**source-level** bound on what it can reach, not an RBAC one.

A domain whose routes do not exist yet has **no route at all** — no stub and no
`501`. `GET /api/v1/session` reports each one as `false` under `capabilities`,
so a client learns what is unavailable instead of discovering it from an error.
Today that is: connection tests, manual backup creation, approval submission
and operation event streams. Saved destinations, topic discovery, preflight
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
| `GET /api/v1/namespaces/{ns}/schedules[/{name}]` | `BackupSchedule` projections. |
| `POST /api/v1/namespaces/{ns}/schedules` | Create a `BackupSchedule`. |
| `POST /api/v1/namespaces/{ns}/schedules/{name}:set-suspension` | The one permitted update. |
| `GET /api/v1/namespaces/{ns}/backups[/{name}]` | `Backup` projections. |
| `GET /api/v1/namespaces/{ns}/restores[/{name}]` | `Restore` projections. |
| `POST /api/v1/namespaces/{ns}/restores` | Create a `Restore`, preserving the plan bytes exactly. |
| `GET /api/v1/namespaces/{ns}/approvals[/{name}]` | Approval metadata and status. |
| `GET /api/v1/namespaces/{ns}/approvals/{name}/packet` | The raw approval document, only through this explicit route. |
| `GET /api/v1/namespaces/{ns}/destinations` | `BackupDestination` rows: the canonical URL, the endpoint, the transport, the addressing and the controller's `Valid` verdict. |
| `POST /api/v1/namespaces/{ns}/destinations` | Create a destination **under the name in the body**, because every schedule, backup and restore references it by that name. |
| `GET /api/v1/namespaces/{ns}/destinations/{name}` | One destination, with the four grants as **references** and the last explicit access test. `lastTest.truncated` says the search for it hit its page bound. |
| `POST /api/v1/namespaces/{ns}/destinations/{name}:update-access` | Rotate the four grants and the CA reference under `expectedGeneration`. It cannot name the location or the transport. |
| `POST /api/v1/namespaces/{ns}/destinations/{name}:test` | Start a `DestinationAccess` `Preflight`; `202` with it. |
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

`backup` and `restore` answer an `OperationResponse` — a result, evidence
references and a verification verdict. `discovery` and `preflight` answer a
`CheckOperationResponse`, which carries **none** of those fields: a transient
check has no archive result and no signed evidence, and publishing an empty
`verification` for one would invite a console to render a verdict that can never
arrive.

`schemas/logweir-api-v1.openapi.json` is the generated contract. `just schema`
rewrites it and `just schema-check` fails on drift, as does
`crates/logweir-api/tests/contract.rs`, which compares the checked-in bytes with
the generator in-process.

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
| `referentChanged` | a named object's UID or generation moved, or it appeared or vanished — a recreated destination, an edited access block, a re-created recovery point, a `TrustRoster` edit, or an `Approval` whose resourceVersion moved when verification landed. `kind` and `name` say **which**; for the single `TrustRoster` and `Approval` a binding names, `name` is the UID, because that is what identifies them there |
| `caBundleChanged` | a destination's CA bundle now digests differently. **RESERVED — nothing emits it.** `status.binding` does not record the CA bundle list, so neither the controller nor this service can compare it; CA drift surfaces through the destination's own `referentChanged`, because editing its `caBundle` reference bumps its generation. Do not branch on it |
| `policyChanged` | the installation policy `ConfigMap` digests differently, which can change the concurrency ceilings, the engine CA rule, the `ControllerIdentity` allowlist and the visibility attestations the verdict was computed under |
| `inputsDigestChanged` | the recomputed digest differs and none of the named reasons explains it. **RESERVED on the same grounds:** the recorded digest was taken over a wider document than this service can rebuild, so comparing the two would report an artefact of the narrower recomputation rather than a change in the world |
| `unverifiable` | **this service could not compare something, so it will not call the verdict applicable.** `basis` says what: a referent whose read failed, a referent of a kind the console has no verb for (`TrustRoster` is cluster-scoped and outside the sealed set), or a result that recorded no binding at all |

**Who compares what.** This service compares the expiry, the `?planHash=` you
sent, and every referent it can read — the five namespaced product kinds. The
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

**There are three updates, and each is a merge patch built here from typed
arguments**: `BackupSchedule.spec.suspend`, a destination's four grants and CA
reference (`:update-access`), and a check's `spec.cancelRequested`
(`:cancel`). Each carries `metadata.resourceVersion`, so each is a conditional
write the API server refuses on a stale read, and none of them accepts a
caller-supplied path or patch document. A destination's location and transport
have no key in any of them.

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
trustedProxyCidrs: ["10.0.0.0/8"]                  # transport LOGGING only
namespaces: [team-a, team-b]
kubernetes:
  source: inCluster
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
| a role binding names a role that is not one of the four | |
| a role binding names a namespace outside `namespaces` | |
| a role binding has neither `groups` nor `subjects` | |
| a binding string contains `*` or `?` | bindings are EXACT: a `*` would match nothing, so it is refused by name rather than silently granting nothing |
| `roles.revision` is empty | it is the provenance of every decision in the audit log |
| `sessionMaxAgeSeconds` outside 60…900 | a stateless session cannot be revoked before it expires |

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
`trustedProxyCidrs` range.

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

The governed-approval **route** is PLAT-19.2 and does not exist yet;
`capabilities.approvalSubmit` is `false` and no path serves it. The
**decision** exists and is tested now.

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
`planHash`, `objectName`, `objectUid`, `objectResourceVersion`, `httpStatus`,
`latencyMs`, `failureCode`, `peer`, `forwardedFor`, `ignoredIdentityHeaders`.

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

There is no event stream yet: `capabilities.operationEvents` is `false` and no
path serves one, authenticated or not. The per-actor, per-namespace connection
slots it will need are implemented and tested.

## Local administrator mode is not a shared console

This mode uses the selected kubeconfig identity and may be cluster-admin. It is
an explicit administrator mode, not SSO and not per-user authorization: the
namespace grants come from the configuration file, and every actor of this
process is the same actor. It must not bind a routable address, must not get an
Ingress, and adding a login in front of it would not create per-user
authorization. Shared operation uses `mode: shared`, which is a different
listener, a different authenticator and a different authorizer — never this one
with a login bolted in front. Ordinary confirmation is unavailable through this
mode; it keeps the legacy governed approval behaviour.

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
