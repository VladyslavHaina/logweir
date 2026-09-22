# The Logweir UI

A Kubernetes API client built from static HTML, CSS and plain ES modules.
There is no frontend build step, bundler, package manager or database: the
shipped assets are the sources. API requests use the same origin as the page.

For local use, `kubectl proxy` serves the files using your kubeconfig. The
[Helm chart](../charts/logweir/README.md#the-uis-authority) optionally deploys
an in-cluster `kubectl proxy` using its own ServiceAccount.
[Dockerfile.ui](../Dockerfile.ui) copies the assets into `/ui` over a pinned
kubectl base; the chart supplies the proxy arguments and carries no duplicate
UI files or asset ConfigMap. The serving identity differs between the two
paths; neither requires a credential in the browser. `just smoke-ui` compares
every served file in the image with this directory.

## Serving it

Serve the directory and the Kubernetes API from one process:

```bash
kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1
```

Then open `http://127.0.0.1:8001/ui/`.

**One process, one origin, and that is the whole design.** `kubectl proxy` serves
the static files under `--www-prefix` **and** proxies the Kubernetes API on the
same origin, `127.0.0.1:8001`, attaching the viewer's own kubeconfig credential
to every request it forwards, server side. The page therefore addresses
`/apis/logweir.dev/v1alpha1/...` as a relative path on the origin that served
it: not a cross-origin request, no preflight, no granted header, and
**no bearer token, key or credential of any kind is ever placed in the page**.
It stores nothing either -- no browser storage, no cookie of its own.

A write is the same story. `POST /apis/logweir.dev/v1alpha1/namespaces/<ns>/restores`
goes to the proxy's own origin, so it is not cross-origin, triggers no preflight
and needs no granted header. `kubectl proxy`'s own defaults admit it: the shipped
v1.35.0 client's `--reject-methods='^$'` is a regular expression that matches only
the empty string, so POST, PUT and PATCH all pass.

Port-forwarding a pod alone does not supply API authentication or serve these
files. Port-forwarding the Helm chart's UI Service works because its backend
is already a same-origin `kubectl proxy`; follow the chart guide for that path.

### It must be a secure context, and the supported address already is

The restore wizard computes the plan's sha256 in the browser, with
`crypto.subtle`, and shows it before anything is submitted -- because the only
design in which the hash the page displays is the hash the controller recomputes
is one where the page produces the bytes itself. `globalThis.crypto.subtle` is
`undefined` on any origin that is not "potentially trustworthy".

**`http://127.0.0.1:8001/ui/` is trustworthy** -- loopback over plain transport
qualifies -- so the supported serving path above works with nothing extra.
Anything else must be a TLS origin. An OIDC-aware ingress on plain transport, or
a `--address=0.0.0.0` plus a browse to a LAN address, is not, and `ui/plan.js`
**refuses at module load** rather than rendering a plan with no hash beside it:

```
this page must be served from a secure context: loopback (127.0.0.1 or localhost)
over plain transport, or any TLS origin. SubtleCrypto is unavailable here and the
plan hash cannot be computed.
```

That message names no URL scheme, deliberately: `scripts/check-ui-offline.sh`
scans `ui/plan.js` like every other shipped byte and has no exemption inside
`ui/`.

### Taking the plan bytes back out of the cluster

The plan document an approver signs is `Restore.spec.planBytes`, verbatim. The
wizard offers a **download** beside the copy button for one reason: copying a
`<pre>` loses trailing whitespace in some browsers, and a plan whose last line
ends in spaces has a different sha256 from the one the page showed. If you would
rather take the bytes from the cluster than from the page:

```bash
kubectl --context docker-desktop get restore <name> -o jsonpath='{.spec.planBytes}' > <name>.yaml
```

Hash exactly what you downloaded, and approve that file.

### The archive credential, and where a Restore without it fails

Step 1 takes an **ARCHIVE CREDENTIAL (Secret name)**, prefilled from the `Backup`
object's own archive reference -- the same object this page read the archive URL
from. It is the name of a Secret in the namespace and nothing else: the page
shows the name, sends the name, and never reads what is in it.

It is not decoration. `spec.sourceArchive` is `ArchiveRef`, which is a URL **and**
a credential, and `weirkeeper` mounts the object-store credential into the runner
Job only when `spec.sourceArchive.secretRef` is set. The field is optional in the
CRD, so **a Restore created without it is admitted** -- the API server has no
objection to make -- and then **fails at the archive, not at admission**: the Job
starts, reaches for the first object, and cannot read it. Leave the field blank
only for an archive reached anonymously or by an instance role.

The credential is **not** in the plan bytes and naming it does not move the plan
hash. The runner takes it from its environment, which the controller fills from
the named Secret; `planBytes` carries only where the archive is, never what
reaches it.

### What that costs, said plainly

`kubectl proxy` forwards every API path except pod exec and attach, on the same
origin as the page, under the viewer's kubeconfig. So the page runs with the
**viewer's entire cluster authority**, not with the roles `logweir.yaml` ships:
`logweir-viewer`, `logweir-operator` and `logweir-approver` bind the **user**,
and under this serving path they bind nothing at all about the page. Anyone who
runs the UI from a cluster-admin kubeconfig gives the shipped bundle
cluster-admin. That residual is why there is no telemetry in this bundle, why
nothing in it is fetched from anywhere else, and why its contents are listed by
digest in the release notes.

**Two flags are the one-line escalation of exactly that residual, and neither may
change:**

- `--address=127.0.0.1` -- the proxy binds loopback only.
- `--disable-filter` -- **never pass it.** The default, `false`, keeps the
  `--accept-hosts` cross-site request filter on; the shipped client's default is
  `--accept-hosts='^localhost$,^127\.0\.0\.1$,^\[::1\]$'`.

**Changing either turns a local page holding your cluster authority into a network service holding it.**
On the LAN, unauthenticated, with your cluster credential attached to every
request it receives.

### The hardened alternative: a kubeconfig that holds less

The residual above is the viewer's own authority, so the way to narrow it is to
start the proxy under a kubeconfig that holds less. Bind a subject to
`logweir-viewer` (read) and, if the page should be able to write,
`logweir-operator` -- and nothing else:

```bash
kubectl --context docker-desktop create rolebinding logweir-ui-viewer \
  --clusterrole=logweir-viewer --user=logweir-ui -n <namespace>
kubectl --context docker-desktop create rolebinding logweir-ui-operator \
  --clusterrole=logweir-operator --user=logweir-ui -n <namespace>
```

Then build a throwaway kubeconfig that carries that subject and nothing else,
and serve from it. The context in it is **named `docker-desktop` on purpose**, so
the serving command above is unchanged and still names its context explicitly:

```bash
export KUBECONFIG="$PWD/logweir-ui.kubeconfig"
kubectl config set-cluster docker-desktop --server=https://127.0.0.1:6443 --certificate-authority=<ca.crt> --embed-certs=true
kubectl config set-credentials logweir-ui --client-certificate=<logweir-ui.crt> --client-key=<logweir-ui.key> --embed-certs=true
kubectl config set-context docker-desktop --cluster=docker-desktop --user=logweir-ui --namespace=<namespace>
kubectl config use-context docker-desktop
kubectl --context docker-desktop proxy --www=./ui --www-prefix=/ui/ --address=127.0.0.1
```

`kubectl config` writes the kubeconfig itself and takes no `--context`; every
other `kubectl` line in this document names `--context docker-desktop`.

Under that kubeconfig a 403 from the page is the API server refusing
`logweir-ui`, which is the story the page tells: every error it shows carries the
API server's own `reason` and `message`, verbatim, because the page's whole
authorisation story is "the API server evaluated the viewer's RBAC".


## What is in here

| file | what it is |
|---|---|
| `index.html` | the shell. Loads `./app.js` as a module; every reference relative. |
| `app.js` | the hash router and the frame. Eleven routes: `#/clusters`, `#/destinations`, `#/schedules`, `#/backups`, `#/history`, `#/operations`, `#/protection`, `#/catalog`, `#/restore`, `#/approvals`, `#/keys`. Three of them carry an identity in the hash -- see *The restore route, and the point it names* and *The operation route, and the run it names*. |
| `api.js` | the **only** module that issues a network request, in either mode. One `fetch`, on one line, and every identifier built by `path(...)`. |
| `client.js` | **which API is in front of this page**, decided once at boot, and the one object every page reads through. See *Two modes, one page*. |
| `contract.js` | the typed contract: JSDoc types and strict decoders for every DTO the page consumes, in both modes. A required field that is absent is a **contract failure the page renders**, never an empty cell. |
| `validate.js` | one set of checks and **one vocabulary of field paths**, so a disagreement from either server lands beside the field it is about. |
| `workflow.js` | the state machines: named transitions, and a transition error for a move a state does not accept. |
| `render.js` | DOM helpers. Sets text, never `innerHTML`. |
| `select.js` | **the saved-cluster selector and the words a connection probe may be described with**. Identity (`{uid, name}`) resolution, the freshness judgement, the searchable control. Shared by the clusters page, the schedule form and both wizard sides -- see *Choosing a saved connection*. |
| `plan.js` | the restore plan document, its sha256 and the two minted names. Refuses a non-secure context at module load. |
| `lifecycle.js` | what lives and dies with one route (reads, listeners) and what deliberately does not: the in-memory drafts, the mutation records and the idempotent create. |
| `pages/restore-wizard.js` | the recovery-point selector, the six wizard steps over the point somebody chose (step 4 carries the topic subset, the exact mapping preview and the recovery limits), the plan bytes, the readiness gate, the fresh-target retry, and the ONE guided submit that creates the Restore and opens what it needs next. |
| `pages/history.js` | Backups and Restores interleaved, the Restore detail view, and the "Restore this point" link a completed Backup row carries. |
| `pages/schedules.js` | the BackupSchedule list, the suspend toggle, the retention panel, and each schedule's recovery points with their own "Restore this point" links. |
| `pages/approvals.js` | the Approval list, the Restores waiting for one, and the create form for ONE chosen Restore. Refuses a private key by name and by the words that open its PEM, and never parses the two documents. |
| `pages/destinations.js` | **saved destinations** (PLAT-08): the list, the create form, the access rotation, the access test, `/usage`, and adopting a legacy inline archive. Console mode only -- see *Destinations, discovery and readiness*. |
| `pages/keys.js` | the cluster-scoped `TrustPolicy` (with `TrustRoster` as the named fallback), read-only, with the out-of-band fingerprint command and the evaluation column that reads `unknown` rather than `valid`. |
| `operation-watch.js` | the reconnecting watch over ONE operation -- backoff, terminal stop, disposal with the route -- and the bounded D3 reads the four D3 surfaces make, with every route they can address in one frozen table. |
| `pages/operation.js` | the durable operation view: state, reason, last update, the diagnoses, the result and the evidence rendered separately, and the completion panel. |
| `pages/protection.js` | protection health beside schedule health, the newest available recovery point with its two instants labelled apart, and the alert ledger with its delivery state. |
| `pages/catalog.js` | the recovery catalog: the connect-archive submission, the point list with availability and verification as two columns, and the untrusted-signer panel with no one-click trust. |
| `style.css` | the design system, in one file: tokens, light and dark, every component. System fonts; no font is fetched from anywhere. |
| `pages/index.html` | zero bytes, on purpose -- see below. |
| `tests/api.spec.js` | the behaviour arm of the two mechanical claims, under `node --test`. |
| `tests/pages.spec.js` | the behaviour suite over the page modules: the badge rules, the wizard, the approval form, the roster. |
| `tests/design.spec.js` | the design system's guarantees: the token layer, both schemes, reduced motion, the focus ring, badges with words, the stepper. |
| `tests/mutation.spec.js` | drafts, one mutation state, idempotent creates, the guided submit and the approval subject -- driven through the real mount halves over a fake node and an in-memory API. |
| `tests/contract.spec.js` | the decoders against `schemas/logweir-api-v1.openapi.json` itself: every console fixture is an instance of the published schema, and every decoder requires exactly what the schema requires. |
| `tests/client.spec.js` | the mode probe, the two modes' reads and writes, the idempotency key, the field-error translation and the plan round trip -- driven through the real transport with the one platform call stubbed. |
| `tests/workflow.spec.js` | the named transitions, the transition errors and the wizard's six steps as a machine. |
| `tests/selector.spec.js` | the saved-cluster selector: identity, rename, delete-and-recreate, freshness, the two contract v1 references, and the same rules in both client modes. |
| `tests/d2.spec.js` | **destinations, topic discovery and operation readiness**: every state the product API can put in front of those three surfaces, and the five sentences this product refuses to render. |
| `tests/d3.spec.js` | **the operation view, protection, the catalog, the keys view, the badge cases and the retention panel**: every state D3 declares, over the objects the D3 live runs recorded, and the five claims this product refuses to make. |
| `tests/restore-catalog.spec.js` | **PLAT-15.2**: the catalog-point route, the offer rule and every refusal it makes, the catalog-window offer for a run the controller could not verify, the bound plan and its golden, the readiness request, the restore body, drafts per point, and the selector, catalog-table and schedule-detail links -- each with its negative control. |
| `tests/preview-server.js` | a development tool, never a test: serves this directory over the fixtures under `tests/fixtures/preview/`. See *Previewing with fixtures*. |

**The design system** lives in `style.css` and nowhere else. It is written from
tokens: a type scale and a spacing scale, radii and two shadows, and one
colour system with semantic roles -- surface, surface-raised, border, text,
text-muted, accent, success, warning, danger, info -- defined once for the
light scheme and redefined once under `prefers-color-scheme: dark`, so every
component reads from the same ten names in both. Every text-on-surface pair
in both schemes measures 4.5:1 or better. Tables read as tables on a laptop
and **stack into cards below 720 px**, each cell captioned by its column: the
caption is copied from the header row into `data-label` by `app.js` when it
adopts the parsed nodes, so the page modules stay pure functions from a JSON
object to a string. Status badges carry their state in **words and colour**,
never colour alone. The restore wizard opens with a **stepper** -- the six
steps, which are done, which one you are on, which needs attention -- that
summarises the same state the six sections render and gates nothing: the
client-side checks are a convenience, and the controller and phase 0 are the
gate. Every interactive element has a visible focus ring, and
`prefers-reduced-motion` switches off every transition and animation at once.
`tests/design.spec.js` asserts each of those over the bytes of `style.css` and
the page modules' own output.

A **hash** router, because the static half of `kubectl proxy` is Go's
`http.StripPrefix(prefix, http.FileServer(http.Dir(base)))`: a plain file server
with no rewrite rule and no custom-404 hook. Every route lives after the `#`, so
no route ever asks the server for a path that is not a file, no rewrite rule is
needed, and no route can produce a path-level 404. That is also why there is no
`404.html` here: Go's `FileServer` would not use it, and nothing can reach it.

`pages/index.html` is **zero bytes** because Go's `FileServer` emits a directory
listing for any subdirectory that has no `index.html`. An empty index is the
whole guard. `tests/` is the one directory a local `just ui` will list, and the
release artefact excludes it, so no test harness and no fixture is ever
published over HTTP.

## Two modes, one page

The same twenty-six files are served two ways, and **they decide which one they are
looking at exactly once**.

**Legacy mode** is what ships today and what every section above describes:
`kubectl proxy` serves these files and proxies kube-apiserver on the same
origin, attaching the viewer's own kubeconfig credential to every request it
forwards. The page addresses `/apis/logweir.dev/v1alpha1/...` and holds no
credential of its own.

**Console mode** is `logweir-api` (`docs/api.md`) serving these files at `/ui/`
and a bounded product API at `/api/v1` on one origin. The page addresses
`/api/v1/namespaces/<ns>/...`, every error is `application/problem+json`, every
durable create carries an `Idempotency-Key`, lists are cursor-paged, and the
namespaces come from `GET /api/v1/session` rather than from `runtime.js`.

### The two principals, and why the console's is wider

The mode a viewer is in decides **which ServiceAccount their reads run as**, and
the two accounts are shaped by opposite arguments. They are separate objects in
the chart (`ui.enabled` and `api.enabled`) and neither one's reasoning may be
copied onto the other.

**`<release>-ui` acts for whoever reaches its Service.** `kubectl proxy`
attaches the pod's own token to every request it forwards, so reachability of
the Service *is* the authorisation boundary -- there is no Ingress, and the
supported way in is `kubectl port-forward`. Its ClusterRole is therefore
measured from this tree: `get`/`list` on the kinds the page renders, the four
`create`s the page issues, the one `patch` it issues (`spec.suspend` on a
schedule), and `list` on the cluster-scoped `TrustRoster` the keys view reads.
It holds **no verb on `configmaps`** -- the chunk documents a `TopicDiscovery`
owns are ConfigMaps, and paging them is the console's job with its own
owner-UID, immutability and digest checks -- and **no verb on `secrets`** at
all. A page that shows less than this role allows is a convenience, never a
control.

**The chart starts nothing.** `api.enabled` renders this identity and its roles
and nothing else -- no Deployment, no image, no Service. The console's
deployment is PLAT-17.1's own stage, so turning the flag on does not put the
page into console mode; it gives a console deployed some other way a reviewed
account to run as.

**`<release>-api` acts for a service.** `logweir-api` authenticates every
request, resolves the actor's roles from its own binding table, and refuses an
ungranted namespace *before* it makes any Kubernetes call. Its ClusterRole is
therefore the union of what every route may ever need, and the per-actor
narrowing happens above it in code that the page cannot reach and the viewer
cannot influence. That is why it is wider, and why "the console can read X"
never means "this viewer can read X".

Its grants come from one sealed adapter, not from a list somebody maintains:
`get`/`list` on the eight product kinds, `create` on seven of them, `patch` on
four, `get`/`list` on the four D3 kinds (`protectionpolicies`,
`recoverycatalogs`, `rehearsalschedules`, `retentionpolicies`) plus the
cluster-scoped `trustpolicies`, `create` on `recoverycatalogs` for "connect
existing archive", `get` on `configmaps`, and `create` on `secrets`.

And four absences that the page depends on:

* **no `watch`.** The operation event stream is server-sent events over the
  service's own reads, not a Kubernetes watch held open per browser tab.
* **no `delete`**, anywhere.
* **no read verb on `secrets`.** A credential this page submits is created and
  can never be read back -- by any route, by any role, or by anyone who
  compromises the service. That missing verb is the whole of the write-only
  property; the *shape* of the create is fenced separately by a
  ValidatingAdmissionPolicy, because RBAC cannot express "this Secret shape".
* **no write verb on `trustpolicies`.** `capabilities.trustAdministration` is
  `false` in this release, the keys view submits nothing, and even a defect in
  the service's own authorization could not produce a write, because the
  credential it would use does not hold the verb. Administering trust is
  `kubectl apply` under `logweir-trust-admin`.

The trust READ is the one grant whose narrowing RBAC cannot help with: the kind
is cluster-scoped, so the account sees every policy and the service is expected
to decide per actor what to serve. For `GET /api/v1/trust-policies` that
narrowing is not in place yet, so the keys view currently shows an actor every
policy's namespace list. That is a route defect, tracked against the console
API's wave, and it is why this account should not be run against until it
lands.

### How the choice is made

At boot, `client.js` asks `GET /api/v1/session` **once**. An answer that decodes
as a session document is console mode. Anything else -- the refusal a `kubectl
proxy` path filter gives, a body that is not JSON, a decode that found a
required field missing, or no answer within five seconds -- is legacy mode. The
answer is recorded for the life of the loaded page and never asked again: a
mode chosen per request is a page that can change APIs between a read and the
write that follows it.

That one probe is the only behavioural difference a legacy installation sees.
It is answered by the proxy's own path filter, it is not retried, and the page
renders before it returns.

### What does not change

* **One network call site.** Both modes go through the same `fetch` in
  `api.js`, and `crates/logweir/tests/ui_lint.rs::every_api_path_is_relative`
  still asserts there is exactly one in the whole tree.
* **No credential in the page, and no browser storage.** Console mode
  authenticates with a session cookie the browser attaches by itself; this page
  never reads or writes one. The synchroniser token the session document
  carries lives in `client.js`'s memory for the life of the loaded page and
  nowhere else -- the same place a draft lives, gone on reload.
* **Navigation cancels reads and never mutations.** A view's read carries the
  route's `AbortSignal` in both modes; a create carries none in either.
* **The plan bytes.** `plan.js` produces the document once and hands the same
  frozen object back for the same bytes, so the hash shown in the review step
  and the bytes submitted are one object. In console mode the `planHash` the
  product API takes beside `planBytes` comes from that object; bytes this page
  never prepared are refused before anything is sent.
* **A list holds the namespace.** The product API pages and `kubectl proxy`
  does not, so the console client asks for the maximum page size and **follows
  the cursor to the end**: the same table shows the same rows in both modes. A
  namespace larger than 25 pages (5 000 rows) is **refused by name** --
  `ListTooLarge`, naming how many were read and saying to use `kubectl` -- and
  never shown as a prefix pretending to be the whole. A "showing the first N,
  more exist" line beside the table would be better, and belongs to PLAT-18.2,
  which owns the table, its footer and its copy.

### What console mode cannot show, named

The product API's projections are not the custom resources byte for byte, and
the adapter records what it cannot supply on every object it projects, under
`__contract.absent`:

| kind | absent in console mode |
|---|---|
| `KafkaCluster` | `status.conditions` (the reachability observation is projected; the condition list is not exposed); `spec.auth.secretRef.passwordKey` and `spec.auth.tlsCa` (connection contract v1's two references, which `ConnectionAuthView` does not carry) |
| `BackupSchedule` | the per-manifest `status.retentionReport.skipped` entries (the API reports their **count**); `status.lastSlot`, `status.missedSlots`, `status.pendingRun` and `status.history` (D1 W7: `ScheduleStatusView` carries `policy`, `nextRuns` and `activeRuns` and stops there) |
| `Backup` | `status.manifestSha256`, `status.jobRef`, `status.selection` and `status.conditions` (D1 W7: the run's coverage label and its `TopicsResolved` condition) |
| `Restore` | `status.integrity`, `status.jobRef` |

Two further differences are worth stating outright, because they are not
absences:

* **The product API mints an object's name** from the idempotency scope. The
  name typed into a create form is therefore what makes that submission
  repeatable -- the same name composes the same `Idempotency-Key`, so a double
  click, a lost response and a reload all resolve to the object the first
  request made -- and the object's real name comes back in the response and is
  what the outcome line shows.
* **Ten normalized operation states, four phases.** `pending`, `running`,
  `succeeded` and `failed` are the resource's own phase words. `queued`,
  `preparing`, `verifying`, `refused`, `cancelled` and `unknown` are
  distinctions `logweir-api` draws that the resource does not record, and the
  page shows the API's own word for them rather than rounding it to a phase the
  controller never wrote.

The `TrustRoster` has **no product route at all**: it is cluster-scoped and
admin-only, and the `#/keys` page says so by name in console mode instead of
asking for a route that does not exist.

A create that carries either of connection contract v1's two references is
**refused by name** in console mode rather than sent without them:
`CreateConnectionRequest` declares `additionalProperties: false` and has no
field for either, so a request that dropped them would create a connection that
projects a different entry of the Secret, or that dials without the private CA
the form named. The refusal is `NoConsoleRoute` and it says which field and what
to do instead (create it with `kubectl`, or use the legacy direct mode).

### The wizard machine is a test-time invariant

`workflow.js` declares the six restore steps as a machine with named moves, and
the suite walks it over the page's own derived step states. **The page does not
enforce it.** `stepStates` (which wants a reachable target) and
`validateRestore` (which does not) legitimately disagree today, so enforcing the
walk at render time would change what the wizard accepts -- which PLAT-18.1 must
not. PLAT-18.2 owns the stepper and is where the two are reconciled. The
mutation machine, by contrast, **is** wired in: every write this page makes goes
through it.

### Where a contract failure comes from

**Including the second read a detail view makes.** A `Backup` or `Restore`
detail also reads its operation, and an approval also reads its packet; a 403 or
a 404 there means "this view does not get that extra" and the object is rendered
without it, but a contract failure or a 5xx is raised rather than turned into an
empty evidence block -- which is indistinguishable from "the controller recorded
nothing", and is the exact cell this whole module exists to prevent.

A response that is not what the contract says it is -- a required field absent,
a field of the wrong type, an enum member the contract does not declare -- is an
Error with `reason: ContractViolation` and `status: 0`, rendered by the same
error box every API refusal uses, naming the DTO and the JSON path. It is never
absorbed into an empty cell, because an empty cell is also what an absent
optional field looks like and a reader cannot tell the two apart. An **unknown**
field is the opposite case and is tolerated: decision D0 requires an older
client to accept a newer field, so it is recorded rather than refused.

## What a form keeps, and what one click can do

**Drafts live in this page's memory, and nowhere else.** What you type into a
form is kept by `lifecycle.js` in a per-namespace, per-form record, so a
validation refusal, the API server's own 422 or 403, a network failure, a
timeout and a trip to another route all leave every value where it was. Nothing
is written to browser storage: rule 2 of `scripts/check-ui-offline.sh` forbids
both storage writes and the cookie accessor, and `mutation.spec.js` asserts a
whole form journey while reading either one throws. So the persistence contract
is exactly this, and it is the same in every form:

* **survives** a refused field, an API refusal, a lost response, a timeout, and
  navigation between routes of the loaded page;
* **does not survive** a reload, a new tab or a closed tab -- a reload starts
  the form empty;
* **is never kept at all** for a value spelling the words that open a
  private-key PEM -- in any case, across any run of whitespace including a line
  break, and so for the PKCS#8, PKCS#1, SEC1, encrypted and OpenSSH labels
  alike -- whichever field it was pasted into, and for any field a form does
  not name in its own allowlist. The forms have no field a password goes in: a
  credential is always a **Secret's name**, which is also why `private-key`
  joined by a dash or an underscore counts only inside a PEM `BEGIN` line: a
  Secret may honestly be called `minio-private-key`. That test reads words, so
  a renamed, headerless blob spells none and is beyond it -- which is why the
  approvals form refuses a key by its **file name** as well (`.pem`, `.key`,
  `.p8`, `.p12`, `.pfx`, `.jks`, `.ppk`, or a name beginning `id_`) and why the
  controller, not the page, is the gate.

**One click makes one object.** Every create the page issues names its object:
a name you typed, or a name minted from the plan bytes. So a second click, a
retry after a timeout and a resubmission after a reload all send the SAME name,
and the API server answers the second one `409 AlreadyExists` instead of
creating a second object. The page then reads the stored object back and
compares its spec with the draft's:

* the same content is the same operation, reported as *already existed with
  exactly this content*, with the UID of the object that exists;
* different content is a **conflict**, named field by field, and nothing is
  overwritten -- this page has no update to overwrite with;
* a submission that is still pending disables its own button, so a double click
  cannot start a second request at all.

**A failure says what is known, about the request that was made.** A refusal
says nothing was created. An **unknown** outcome -- no answer, a timeout, a
5xx -- says exactly that: the object may or may not exist, the request was not
cancelled, and submitting again is safe for the reason above. The API server's
own status, reason and message are shown verbatim beside it, and a 422's
`causes[]` are put beside the fields they name.

Two requests are not creates and do not borrow a create's words. The **suspend
toggle** is a `PATCH` on a schedule that already exists: an unknown outcome
there says the change may or may not have landed, that nothing was created
either way, and that repeating it sets the same field to the same value. And an
outcome that is about a **plan the wizard is no longer showing** -- a create
that timed out, then a field edited while it was still outstanding -- says so:
it keeps the name it actually sent, says that submitting now would create a
different Restore instead of settling this one, and links to the Restore it
named. That record is not discarded by an edit, so a late answer to the
timed-out attempt still settles it and still names the object it made.

**The restore wizard has one action.** *Create the Restore* checks that the
plan about to be sent is the plan on screen -- a field changed after the bytes
were rendered is refused, not silently substituted -- creates the Restore, and
then opens what it needs next: its **approval page** while it waits for a
verified `Approval`, or its **operation view** once one authorises exactly that
Restore (this name, this namespace, this UID, this plan). There is no second
button that navigates without creating.

**The approvals page never assumes a subject.** A visit with no subject lists
the Restores waiting for an approval, or says there are none. A visit for one
Restore reads that Restore and derives everything it will submit from the
object itself -- the subject name, its UID, the `Approval` name its
`spec.approvalRef` names, and the sha256 of its own `spec.planBytes`, computed
on the page -- shows them read-only, and at submission checks that what is
SHOWN is what it would SEND and that the Restore is still the one it read. A
link whose `hash` or `name` disagrees with that Restore is refused and offers no
form; an `Approval` bound to another subject, another execution (another UID) or
another plan is shown as such and never offered for reuse. `pending`,
`refused` and `expired` are read from `Approval.status` and its `Verified`
condition, never derived here.

**A list this viewer may not read is a warning beside the page, not instead of
it.** An approver's role often grants `create` on `approvals` and no `list` or
`get`. Either list failing on the standalone visit is caught and rendered where
it happened, and the other one is still shown; a Restore's own approval page
whose `Approval` cannot be read says its state is **unknown** -- never "none
recorded" -- and still offers the form, because the subject comes from the
Restore, the create still names the one `Approval` that Restore's
`spec.approvalRef` names, and an `Approval` that already exists with different
content is a conflict that overwrites nothing.

## The restore route, and the point it names

**The wizard is bound to a recovery point somebody chose, and never picks one
itself** (PLAT-11.1). Until this landed, opening `#/restore` selected the newest
`Succeeded` `Backup` in the namespace -- so the page chose for the operator, and
changed its mind whenever a schedule completed. Task 28 measured three `Backup`
objects arriving in six minutes against a two-minute schedule, with the chosen
set, the covered window, the plan bytes, the plan hash and both minted names
moving between one render and the next.

The route is

```
#/restore?ns=<namespace>&backup=<backup name>&uid=<backup uid>
```

* **`uid` is the identity.** A `Backup`'s UID is the one identifier here that
  cannot be re-used: an object deleted and recreated under the same name is a
  different run over a different archive window, and a page resolving by name
  alone would follow the new one without saying so.
* **`backup` is for reading**, and for naming the point in a refusal when
  nothing answers to the UID. A link carrying only `backup` still works: it
  resolves by name and pins the UID it found, so an older or hand-typed link
  stays usable.
* **`ns` is explicit.** `default` is a real selected namespace and crosses the
  hand-off rather than being elided.

**Who links here.** A completed `Backup` row on `#/history` carries *Restore
this point*, and so does every recovery point listed on a schedule's card on
`#/schedules`. Both build the route with the same function, so what a click
sends is the identity the object actually has. A `Restore` row carries none: a
restore is not a point to restore *from*. A run still in flight carries none
either -- the wizard would only refuse it.

**What each visit renders.** With no identity, the **selector**: every recovery
point in the namespace, newest completion first, with its schedule, slot,
disclosed coverage, frozen topic list, record count, signed verdict and what is
known about its archive, filtered in place by a search box; and, when the
namespace holds none, the empty state naming the runs it does hold. With an
identity that resolves, the six steps. With an identity that does **not**
resolve -- the object is gone, or it is not `Succeeded` with a backup set and a
covered window -- a **refusal** naming the point that was asked for, with no
plan, no hash and no submit. Nothing is substituted, ever.

**The choice does not move.** Because the identity is in the address, every
re-read the page makes -- a reload, a return to the route, the re-read that
discarding a draft performs -- resolves the same UID. A backup completing
mid-wizard appears in the catalog table and changes nothing else: same set, same
bytes, same hash, same two minted names.

**Coverage, and what it is made of today.** "Coverage" is
`Backup.status.windowCovered`, two epoch-millisecond integers (interface I22),
shown as RFC 3339 and **closed at both ends**: a point equal to either bound is
inside it. A requested point-in-time outside it is a **field error that keeps
every value typed** -- nothing is sent, and the message sits beside the input.
The archive line is a statement about the *status*, not about the bucket: this
page holds no bucket credential and lists no object storage, so the nearest
thing to availability the cluster can tell it is whether the run recorded a
manifest key. **PLAT-15.1** is what turns that into a real answer -- a durable
catalog that records, per set, whether the objects are still there, and that
carries imported points this namespace's `Backup` objects do not.

**A swapped point is a different plan.** The plan document names the chosen
point's backup set and its covered window, so choosing another point changes the
bytes, the sha256 on screen and both minted names -- and the reviewed-plan check
(PLAT-13.2) refuses a submit whose prepared hash is not the hash that was
displayed. An approval covers the point it was signed over, and nothing else.

## A catalog point: the restore with no `Backup` behind it (PLAT-15.2)

**The second route** is a point read back from a connected archive:

```
#/restore?ns=<namespace>&catalog=<catalog name>&point=<lwp1-point-id>
```

plus `&backup=<name>&uid=<uid>` when the offer came from a run (below). The
point id is content-derived from the signed receipt (D3 section 5.1), so it is
the identity, and **everything the plan is built from is read again from the
product API** -- the catalog, the point (cursor-paged through at most 25 pages
of 200), and the catalog's saved destination. A receipt key or digest in the
address is never read: an address is something anyone can edit.

**When a point is offered** (`catalogPointOffer`, the one rule the selector, the
catalog table, the schedule detail and the wizard's own mount share): the API
row is `selectable` -- the controller's conjunction, joined server side with the
namespace's `Backup` verdicts; no `backupVerdict`; no `backupVerdictsIncomplete`
on the page (a join nobody finished cannot say that no `Backup` refused this
receipt); a current view; a point id, an unredacted receipt key and both
digests; and a covered window. Anything else is a refusal naming the reason,
with no plan, no hash and no submit, and never a substituted point.

**What the six steps do with it.** Step 1 reads the archive the catalog reads
(its saved destination, frozen at mount by UID and location digest and checked
again before the create, exactly as a Backup's destination is). Step 2 shows the
two verdicts, the signer key id, the binding and an input for the **topics to
restore** -- the view publishes no topic list, so the operator names them and
the readiness check reads the manifest for exactly those names. Step 3's
window is `[coveredFrom, coveredTo - 1 ms]`, because the catalog's end is
exclusive. The plan carries `source.backup` pinned to the point's set and
`source.point {point_id, receipt_key, receipt_sha256, manifest_sha256}`; the
runner re-reads that receipt and manifest before it contacts a broker and
refuses a mismatch (exit 3 `PointBindingMismatch`). A plan built from a
`Backup` carries no `point` block and is byte-identical to before
(`ui/tests/fixtures/plan-point.golden.yaml` beside `plan.golden.yaml`). Step 5
sends `restore.catalogPoint {catalog, pointId}` and nothing else, so the
controller re-reads the row when the check runs. A draft is kept per point: a
draft made for a Backup is never applied to a catalog point of the same set.

**Where the links come from.** The selector lists *Recovery points from
connected archives* beside the Backups, and a namespace with no completed
Backup shows them instead of the "wait for a run" advice (with a link to
connect an archive when there are none). `#/catalog`'s table links every row
the rule offers and says why a selectable row is not offered; it shows the
controller's `backupVerdict` beside the verification word and a banner when
`backupVerdictsIncomplete` is set.

**A run the controller wrote no window for (CONSOLE-RESTORE-IGNORES-CATALOG-WINDOW).**
A `Succeeded` destination-backed run whose own verdict is absent or
`NotAttempted` is offered on the schedule detail (the row's *Restore this point
(catalog window)*, and the latest-point action) from its catalog row -- only
when exactly one row answers its receipt digest (its set id when it reported
none), that row is offered by the rule above, and, in the wizard, the catalog
reads the destination the run froze (same name, UID and location digest). A
run whose verdict the controller REACHED -- `Invalid`, `Untrusted`, or a word
this build does not know -- is never made restorable by a row, and a run still
`Pending` (its evidence-fetch Job is reading the receipt) waits for that verdict
rather than being offered from the catalog meanwhile.

## The topic subset, the mapping, and what a recovery does not do

**Step 4 chooses which of the point's frozen topics to restore, and shows the
exact name each one becomes** (PLAT-11.2). Before it, the wizard took the whole
of `Backup.spec.topics` and showed no target name anywhere: an operator
restoring one topic out of forty had to edit the plan by hand, and nobody saw a
mapped name until the run created it.

**The mapping rule is the prefix and nothing else.**
`logweir_core::spec::target_topic_prefix` is the whole grammar -- `newTopic`
takes `target.topicNaming.prefix`, `scratch` takes `topic_mapping_prefix`, and
neither admits a per-topic rename -- so a mapped name is a concatenation.
`mappedTopicName` is that concatenation and the only place the page performs it;
`topicMapping(state)` builds the preview rows AND the rows the create request
declares, from one call, so a page that previewed one mapping cannot submit
another.

**Five refusals, made before anything is sent, and made again by the product
API in the same words** (`routes/restores.rs::validate_topic_mapping`):

| refused | the page says | the API answers |
|---|---|---|
| no topic selected | a restore of no topic is not a restore | `topicMapping` / `empty` |
| a topic the point did not freeze | names it, and names `PlanTopicsNotInRecoveryPoint` | `topicMapping[i].source` / `invalid_topic` |
| the same source twice | names both rows and the target they share | `topicMapping[i].target` / `duplicate_mapping` |
| a prefix that is not a Kafka name, or an empty one (the identity map) | names the value and the 249-character bound | `target.topicNaming.prefix` / `invalid_prefix`, or `topicMapping[i].target` / `mapping_identity` |
| a mapped name longer than a broker accepts | names the topic and the bound | `topicMapping[i].target` / `mapped_name_illegal` |

A duplicate target can only be a duplicate SOURCE, because a prefix map over
distinct sources is injective. The subset is canonicalised into the point's own
order before it reaches the plan, so the hash an approver signs does not move
with the order of the clicks -- but the DUPLICATE check reads the raw list, so a
repeated entry is refused rather than quietly deduplicated into a list that is
not the one the page was given.

**One prefix value, and the grammar has two keys for it.** The runner's document
carries `target.topic_naming.prefix` AND `target.topic_mapping_prefix`, and
`logweir_core::spec::target_topic_prefix` reads the first for `newTopic` and the
second for `scratch`. **Every writer writes both keys**: `setTopicPrefix` is the only thing that
mutates an existing state, and the two constructors that build a fields object
from scratch -- `initialState` and `draftFrom` -- set the pair together. So the
preview is what the run maps through in **both** modes. `effectivePrefix` is the
JavaScript half of that rule and is what `topicMapping` reads; the runner has a
third arm (a `newTopic` spec that states no `topic_naming` at all falls back to
`default_topic_prefix`) which this page cannot reach, because `ui/plan.js`
renders that key through `needed()` and throws on an absent one. Before this, only the first key moved on an edit, and
in `scratch` the preview, the declaration and the API rail all named a prefix the
run does not use.

**`topicMapping` rides on the product API's create REQUEST, is never stored, and
is sent for `newTopic` only.** `Restore.spec` has no topic list -- the subset
lives in the opaque plan bytes -- so the API recomputes every row from
`target.topicNaming.prefix`, which it DOES store, and answers 422 when the
preview and the submission disagree. It refuses a declaration for `mode:
scratch` (`unsupported_for_mode`), because the prefix the run reads there is in
the plan and the API never parses one; the wizard therefore does not send it in
that mode, and the rails there are its own preview and phase 0. In legacy mode
the field is stripped before the object is sent (`client.js`), because the
custom resource has nowhere to put it; there the plan bytes carry the same list,
from the same call, and phase 0 reads them. Both halves of that delivery are pinned by one test in
`ui/tests/client.spec.js` with three arms: console sends the rows verbatim,
an absent declaration stays absent, and legacy strips it without mutating the
caller's object.

**What a recovery changes, and what it does not**, beside the mapping, each from
a contract constant or a plan field and never from prose this page invented:

* the **target replication factor**, from the plan's own
  `target.default_replication_factor`;
* the **partition count**, which is *not shown and said not to be*: this build
  publishes a per-topic count only after a run
  (`Restore.status.completion.newTopics[].partitions`, from the target diff),
  and no field of a `Backup` or of its projection carries the archive
  manifest's. PLAT-15.1's catalog is where that would come from;
* the **sampled verification scope**, from the plan's `sample` block, closing
  with the clause D3 section 3.5 makes non-optional -- *a sampled check, not an
  exhaustive comparison*. No level in this version compares every record;
* the **consumer cutover limitation**, byte for byte from `render.js`'s
  `COMPLETION_GUIDANCE` and `TARGET_MODE_MEANING` -- the same fixed sentences
  the completion panel shows afterwards;
* **resume is not implemented**, said in the wizard before the run rather than
  discovered after one. A restore that fails part way cannot be continued from
  where it stopped; the way forward is a new restore to a fresh target.

`ui/tests/fixtures/restore-limits.json` pins the three numbers from both
languages: the node suite asserts the page renders them, and
`crates/logweir-api/tests/resources.rs` asserts they are `logweir_core`'s own.

## The readiness check holds the submit

**A readiness verdict that no longer describes the plan on screen refuses the
create** (PLAT-11.2, D2 section 6.3's invalidation rule). `readinessRefusal` is
checked on the button and again inside `submitRestore`, so a direct call cannot
walk past it, and it refuses four states:

* the result is about **another plan** -- the prefix, the subset or the point in
  time changed, so the hash moved. Both hashes are named;
* the **target** changed. This one is marked a step earlier, in `selectTarget`,
  and it has to be: two `KafkaCluster` objects with the same bootstrap servers
  and the same auth render identical plan bytes, so the hash arm is blind to the
  swap. D2 section 6.6 puts it under the same rule -- "choosing another target or
  destination, or a recreated one, changes a referent UID, so the result is
  stale" -- so a change of the selected UID marks the held verdict
  `applicable: false, stale: true` with `referentChanged` and the cluster named,
  and the stale arm below refuses the submit exactly as a hash change does. The
  mark is the console's own and is strictly more conservative than the server's
  recomputation, which answers `referentChanged` for this input;
* the server says it is **stale or inapplicable**. The judgement is the
  server's, recomputed on every GET against the caller's own plan hash, never a
  comparison this page makes against a browser clock. `target.mappedTopics`
  expires in five minutes, which is the shortest budget in D2 section 6.3's
  catalogue and exactly the check a slow review outlives;
* it has **not finished**;
* it is **not ready** -- which is where a target-topic collision lands, as
  `target.mappedTopics` / `MappedTopicExists`, named with its check id and code.

**An ABSENT check is a warning, not a refusal.** D2 section 6.6's rule is about
a verdict that has stopped applying; the runner's phase 0 refuses a mapped topic
that already exists whether or not a console asked first, and legacy mode has no
readiness route at all -- so its result is absent and it lands on this same arm,
which says in words that nothing has looked for an existing target topic. There
is no second arm beside it: an earlier draft had one keyed on a flag nothing in
this wizard sets, which is a refusal-bypass no reader could reach.

## Retrying a failed restore to a fresh target

**A failed `Restore` offers *Retry to a fresh target*** (PLAT-11.2,
PLAT-12.2's retry identity). The route is

```
#/restore?ns=<namespace>&retryOf=<failed restore name>
```

and it names **no recovery point**, deliberately: `Restore.spec` carries a
backup SET id and no reference to the `Backup` it came from, so the point is
chosen on the selector -- which carries `retryOf` forward -- rather than guessed
from a set id.

**The retry is a new execution, and the prefix is what makes it one.**
`defaultTopicPrefix` is a pure function of the recovery point, so retrying the
same point with the same prefix would render the same plan bytes, mint the same
`Restore` name and the same `Approval` name, and collide with the run being
retried -- and be authorised by the approval bound to it. `freshTargetPrefix`
derives the prefix from the failed run instead: deterministic (retrying twice is
the same retry, and the second submit is an idempotent replay), reading no
clock, and producing a target name the failed run never used.

**The old approval is never reused, structurally.** Both names are minted from
the plan bytes and `restoreBody` reads them from nowhere else, so a fresh prefix
mints a different `Approval` name; the failed run is not named anywhere in what
is sent. Where the namespace's policy is governed, the retry waits for an
approval of its own, exactly as a first restore does, and the controller's
`PlanHashMismatch` refusal is the backstop. **The failed run is not modified** --
the wizard writes to nothing that exists -- and its evidence and any topics it
had already created stay as they are.

**A draft cannot put the old prefix back.** A retry shares its recovery point,
and therefore its backup set, with the run it retries, so `applyWizardDraft`
compares `retryOf` as well as `backupSetRef`: an ordinary restore's kept prefix
does not apply to a retry, and a retry's does not apply to an ordinary restore.

## Choosing a saved connection

**A saved `KafkaCluster` is chosen by identity, not by name** (PLAT-07.2).
`ui/select.js` is the one module that resolves a selection, and the three
surfaces that need one -- the schedule form's source, the restore wizard's
target, and the wizard's own statement of which connection a recovery point came
from -- all read it. Before it, every one of those was a name in free text or a
name in a `<select>`, and a name is not an identity: delete a connection and
recreate it under the same name and each of those references silently follows a
different set of brokers reached with a different credential.

**The selection contract**, which PLAT-10.1 and PLAT-11.2 reuse:

```
selection = { uid, name }          // what a draft keeps, and what a form posts
resolveClusterSelection(clusters, selection) -> {
  state,          // "none" | "selected" | "recreated" | "missing"
  cluster,        // the object, for "selected"; null otherwise
  uid, name,      // the object's own identity for "selected"; what was asked
                  // for otherwise
  role,           // spec.role of the resolved object -- the capability label
  pinned,         // a name-only selection that has just been given an identity
  renamedFrom,    // the older name, when the object has since been renamed
  recreatedUid,   // the uid now holding that name, for "recreated"
}
```

* **`selected`** -- the UID answers. The `name` reported is the object's name
  **now**, so a request body that spells `sourceRef.name` or
  `target.clusterRef.name` sends the current one.
* **A rename is not a refusal.** Same UID, same brokers, same credential; only
  the label moved. The selection holds and the page says what it used to be
  called.
* **`recreated` is a refusal, and it names both UIDs.** The UID is gone and a
  different object answers to the name. Nothing is selected, the forms refuse
  before sending, and the wizard renders no plan -- the same rule PLAT-11.1
  applies to a recovery point, for the same reason.
* **`missing` is a refusal too**, with its own sentence: the connection is gone
  and nothing took its name.
* **A name-only selection still resolves**, and is **pinned** to the UID that
  answered it. That is what keeps an existing `BackupSchedule.spec.sourceRef`
  and a draft kept before PLAT-07.2 usable: it works, and from that moment it is
  an identity.
* **Every saved connection is offered, whatever its role.** `spec.role` is a
  label the adopter picks and the controller reports (the CRD says so, and the
  runner's own guard is the gate), so the selector **shows** the role on every
  option and filters nothing out. It decides only which option is preselected.
* **The search filters options and never removes them**, and never hides the
  selected one. What the form would submit is the same before and after a
  search.
* **A namespace change clears the selection.** Drafts are keyed by
  `{namespace, form}`, and the namespace picker rewrites the hash to
  `#/<route>?ns=<name>` with no identity in it (PLAT-13.1).

### `status.reachable` is a connection probe, never "ready"

D2 section 9 fixes the noun and this page keeps it: the badge says **connection
probe** and the word *ready* appears on no probe surface --
`tests/selector.spec.js` asserts that against every one of them at once, matched
as a bare word so `already` in a neighbouring sentence is not a false positive.

| what the object says | what the page says |
|---|---|
| `reachable: true` | `connection probe: reachable`, with the cluster id the broker gave |
| `reachable: false` | `connection probe: not reachable`, with `status.reason` |
| absent, `reason` one of `ConnectionConfigInvalid`, `ConnectionReferenceInvalid`, `ConnectionFieldUnsupported`, `CredentialNotRenderable` | `connection probe: refused`, the controller's own reason **verbatim**, and a one-sentence gloss |
| absent, `reason: ProbeRunning` | `connection probe: probing` -- and **not** stale: a probe in flight has observed nothing yet |
| absent, some other `reason` | `connection probe: unknown` |
| absent, no `reason` at all | `connection probe: never probed` |

A connection carrying no `status.observedAt` is **never stale** -- it is
`never observed`, its own badge. `stale` means "this reading describes the past"
and is a statement *about* an observation; a connection the controller refused
has none for it to be about, and labelling it stale said two contradictory
things at once and told an operator to wait for a refresh that a refused
connection never gets (the controller starts no probe Job for one). The "older
than the budget" clause is emitted only when there is an instant to measure.

**Freshness is its own badge**, because "reachable, and nobody has checked in
two hours" is two facts and a reader needs both. An observation older than
**630 seconds** is labelled `stale` beside whatever it says. That budget is
derived, not picked: `weirkeeper`'s `controllers::kafka_cluster::PROBE_TTL_SECONDS`
is 300 and `RE_PROBE_SECS` is that plus a 15 s margin, so a healthy controller
refreshes `status.observedAt` about every 315 s and twice that is the point at
which a missed refresh is no longer jitter. An observation more than a minute
in the **future** is stale too -- there the arithmetic itself cannot be trusted,
and an untrustworthy number presented as current is the same defect wearing a
different hat. (No `observedAt` at all is `never observed`, above, not stale.)

**"Test connection" dials, and "Re-read probe" reads.** Two things a console
can honestly do about a connection, and each control now wears the label of
the one it does.

*Re-read probe*, on the list and in the probe panel, **re-reads the object**
and renders the newest observation the controller has recorded since. It dials
nothing: `KafkaCluster.spec` is immutable and the re-probe cadence is the probe
Job's own `ttlSecondsAfterFinished`. Each row's control re-reads **that**
cluster and repaints **that** row's probe cell, after checking that the name
still answers to the same UID. Until D2-SOURCECHECK this control was labelled
"Test connection", which is the defect PLAT-07.2's row named: the only honest
reading of that label is "dial the broker now", and nothing in the console
could.

*Test connection*, on a cluster's own page, creates a `Preflight` with
`operation: SourceConnection`. The controller resolves the connection, renders
a check plan carrying it and nothing else, and runs one isolated Job that
projects this connection's own credential and dials the brokers. The panel
renders that object's own rows -- state, code, message, remedy, observed and
expires -- and computes none of them. It makes **no claim about topics**: the
check names none, so `connection.topicsDescribable` is not reported and the
panel says as much, pointing at *Discover topics* instead.

* **One click, one Preflight.** The guard is the form's mutation record, which
  every mount of the form in this namespace shares, so a double click is one
  check even across a re-render.
* **One idempotency key per deliberate test.** The key carries a token minted
  when a click is ACCEPTED. A key composed from the connection alone would
  replay the first verdict for ever, which is the re-read this control stopped
  being.
* **A refused connection disables the control** and prints the controller's
  own reason verbatim beside its gloss. PLAT-07.1's resolver refuses before any
  credential is renderable, so the check would be created, fail to render a
  plan and record no row at all.
* **The follow is bounded and says when it stops.** The panel re-reads the
  started check at most twelve times, about thirty seconds, and then says so:
  the check is still the controller's and was not cancelled. An unbounded timer
  would keep reading a namespace for as long as a tab is open.

A read that answers after the route has left paints nothing (PLAT-13.1), and
that holds for every read of the follow loop as well.

### Connection contract v1 in the cluster form

The create form takes contract v1's two references, and neither is a value:

* **`spec.auth.secretRef.passwordKey`** -- which entry of the credential Secret
  the controller projects. **Blank is left out of the request**, not sent as the
  default: an object that omits it is byte-for-byte what every release before
  contract v1 wrote, `spec` is immutable, and the frozen execution inputs record
  what is there.
* **`spec.auth.tlsCa`** -- exactly one key of a `Secret` or a `ConfigMap` in the
  same namespace holding PEM CA certificate(s). One control picks which kind, so
  the CRD's "exactly one of" rule cannot be broken from here.
* **The `tls` switch is independent of the auth mode.** Contract v1 supports
  `scramSha512` over TLS; `plaintext` with `tls: true` is **refused** by the
  resolver rather than dialled in the clear, and the form says so beside the
  box before a request is made.

**There is no field a password could be typed into**, `CLUSTER_DRAFT_FIELDS`
names none, and `FORBIDDEN_CLUSTER_FIELDS` holds the names that are forbidden as
data so the guard is an exact list rather than a regex that cannot tell
`passwordKey` -- the name of a data key -- from a credential.

## Addressing style never enables plaintext transport

`path_style` addressing and insecure HTTP are **two controls over two different
things**, and this page keeps them apart (defect `UI-HTTPDOWNGRADE`, decision
D-SEAMS S5). The wizard used to read the addressing checkbox into the plan's
insecure-transport flag, so an operator ticking `path_style` for a MinIO or Ceph
endpoint -- which is what every on-premises object store needs -- also told the
runner it could carry the object-store credential and every restored record over
an unencrypted connection, in a document an approver then signed.

Step 1 now renders a separate **"Allow insecure HTTP (explicit, local
development only)"** checkbox. It defaults **off**, it is the only thing that
sets the flag, and a warning beside it says what ticking it costs. Neither the
addressing style, nor the shape of an endpoint, nor any value from the
environment sets it -- in either storage block. A draft restored after a refusal
keeps the two apart as well: the flag is a field of the draft in its own right
and is never re-derived. The behaviour gate row is
`restore_wizard_path_style_does_not_enable_http`, which drives the real mount
half and compares the bytes it submits.

~~One endpoint, one region and one addressing value still apply to **both** the
source archive and the evidence store.~~ **Closed by PLAT-08.2** -- see
*Storage choices* below: the evidence store has its own four controls, and its
own insecure-transport box.

## Storage choices: inherited destinations, two stores, one control each (PLAT-08.2)

**A saved recovery point is restored with nothing re-entered.** The wizard reads
the point's `destinationRef`, pins the destination by uid and frozen
`locationDigest`, and signs the destination's public storage -- bucket, prefix,
region, endpoint, addressing, transport -- into both plan blocks. There is no
endpoint, region, addressing, transport, evidence-bucket or Secret input for such
a point, and a kept draft cannot override them.

**The evidence store is its own choice.** Step 1 offers an *evidence destination*
selector, by uid, starting on the point's own destination. Choosing another one
rebuilds only the plan's evidence block, from that destination's storage exactly
as `BackupDestination::evidence_storage_url` does (its bucket under `logweir/`,
its own endpoint, addressing and transport), and the create body and readiness
request name it as `evidenceDestinationRef` / `evidenceDestination`. The archive
does not move. A kept choice whose uid no longer answers is a refusal that signs
nothing, and the pre-submit read confirms the evidence destination's uid and
digest as it does the source's. The list comes from `GET .../destinations`; when
that read fails only the point's own destination is offered, and the page says
so.

**A legacy point's evidence store has its own settings.** A ticked box, *the
evidence store uses the archive store's endpoint, region, addressing and
transport*, keeps the old plan bytes; unticking it gives the evidence store its
own endpoint, region, `path_style` box and **its own "Allow insecure HTTP"** box,
which defaults off. No box on either store sets the other store's flags, and no
addressing box sets any transport flag. A draft kept before this change (one set
of values) still means one store.

**Readiness follows the check, and the submit asks again.** Step 5 re-reads a
started check until it is terminal (`?planHash=` of the plan on screen). The
submit re-reads the held check against the reviewed plan's hash before anything
is created, so a destination edited during the draft -- an access rotation, a CA
change: the generation moves, the plan bytes do not -- refuses the submit with
the product API's `referentChanged:BackupDestination/<name>` until the check is
run again. A staleness mark the page made itself (a changed target or evidence
destination, which the server's binding cannot see) survives a fresher read and
is cleared only by a new check.

**A schedule inherits its destination, and says what it inherits.** A create
form nobody has chosen a location on preselects the namespace default
destination (two defaults are none); a draft that chose the inline archive keeps
it. Beside the choice the form shows, as facts and never as inputs, what the
schedule inherits: location, endpoint, region, addressing, transport, CA, the
destination's verdict and the revision read. The choice is pinned by uid; at
submit the destinations are read again, and a destination deleted or recreated
under the same name while the form was open is refused, while an *edit* (same
uid) is not -- the next run simply resolves the edited destination.

**Converting an inline schedule keeps the location, or says it moves.** When the
Future policy panel changes where a schedule writes (inline to a destination, a
destination to inline, one destination to another), it compares the two
locations by bucket and prefix. The same location is a note -- recovery points
before and after share the prefix, runs already created keep what they froze,
and an inline archive's endpoint comes from the controller environment this page
cannot read. A different location, or one the page cannot compare, needs an
explicit *write new runs to the new location* box before the save is sent.

**The destination form refuses `virtualHosted` with a custom endpoint** beside
the addressing control, as the product API does (`addressing_unsupported_by_engine`):
the pinned engine addresses a custom endpoint path-style whatever it is told.
Transport and addressing stay two radios in two fieldsets; the only rule touching
both is the scheme/transport consistency check, and neither is changed to suit
the other.

Rows: `ui/tests/storage-choices.spec.js`, and in `ui/tests/mutation.spec.js`
`restore_wizard_evidence_store_has_its_own_transport_and_addressing`,
`a_saved_point_inherits_its_destination_and_can_write_evidence_to_another` and
`a_destination_edited_during_the_draft_refuses_the_submit_until_the_check_runs_again`.
The browser journey is `scripts/plat08-2-ui-e2e.mjs`.

## Destinations, discovery and readiness

Three domains landed with D2, and all three are served by the **product API and
nowhere else**. `kubectl proxy` would serve the custom resources -- they are in
the same API group -- but the legacy UI ServiceAccount has no binding for them
and `api.js`'s `WRITABLE_PLURALS` is deliberately unchanged, so in legacy mode
every call is refused *here*, by name, with a sentence saying which API serves
the flow. That is on purpose: a 403 from a route the page could not reach would
have to be narrated, and a page that narrates an authorisation decision it did
not make is the thing this whole tree is written to avoid.

The `#/destinations` tab is in the navigation in **both** modes. Hiding it in
legacy mode would be worse than showing it: the mode is decided once at boot,
after the first paint, so the tab would appear and vanish under a reader.

### What the three surfaces show, and the five sentences they will not write

A destination is one archive location written down once, with an identity, so
that schedules, backups and restores name it instead of each spelling a URL, an
endpoint, a region, an addressing flag and a Secret of their own.

| surface | where | what it does |
|---|---|---|
| destinations | `#/destinations` | list, create, rotate access (`:update-access`), test access (`:test`), `/usage`, adopt a legacy archive (`:from-legacy`) |
| topic discovery | `#/clusters?...&name=<connection>` | start, cancel, the two slots, the searchable paged inventory with its filters |
| backup readiness | `#/schedules` | a `Preflight` of operation `backup` over a chosen connection, destination and topic list |
| restore readiness | `#/restore` step 5 | a `Preflight` of operation `restore`, bound to the exact plan hash on screen |

Five sentences this product must never render, and where each refusal lives:

1. **"complete"**, for a topic inventory. An all-topics Kafka Metadata request
   silently omits every topic the principal cannot `DESCRIBE` -- no error, no
   count, nothing to notice -- so a listing that worked perfectly proves nothing
   about completeness. `visibility.state` is `unknown` for one, `unknown` is
   that field's **healthy default**, and the word appears on the page only
   inside `attestedComplete`.
2. **"verified by Logweir"**, for an attestation. `attestedComplete` is an
   administrator's claim recorded in a policy `ConfigMap`. Logweir checked that
   the claim matches this cluster, this principal and this moment; it did not
   check that the claim is true, and it cannot. `render.js`'s `attestationLine`
   appends "not verified by Logweir" and is the only path that renders one.
3. **"invalid"**, for a destination nothing has judged. `status.valid` is
   absent until a controller reaches a verdict -- which on a cluster whose
   controller predates the kind is always -- and the page reads "not judged
   yet". Absent is not `false`, and it is not `true` either.
4. **"ready"**, from an empty result. A readiness verdict comes from a
   `Preflight`'s own recorded aggregate. An empty `checks` array is not a pass;
   a state this build does not recognise is `unknown`, never `ready`; a
   `skipped` blocking check is labelled *never a pass*; and a `ready` aggregate
   is rendered beside its execution-only checks with the sentence saying it
   never meant those passed.
5. **"out of date"**, for `unverifiable`. That reason is the service saying it
   could not **compare** something, with a `basis` naming what. It is rendered
   "could not be checked", because a verdict nobody checked reported as merely
   stale is a verdict somebody will act on.

### A short page is not the last page

`GET .../topic-discoveries/{id}/topics` reads at most eight stored chunks per
request. `scan.complete: false` means that budget was spent before the end of
the result -- which is exactly what a sparse `q` over a large inventory looks
like -- so the table says so in words and offers `page.nextCursor`. Treating a
short page as the end would have shown an operator four topics out of five
thousand with nothing on screen saying so.

### The credential is never in a draft

`lifecycle.js`'s `keepDraft` takes an **allowlist** of field names, and
`DESTINATION_DRAFT_FIELDS` names no credential input. A credential typed into
the destination form therefore survives exactly as long as the form element
does: through nothing -- not a refusal, not a re-render after a 409, not a route
change. The form's credential inputs carry no `value` attribute in any render,
so there is nowhere for one to land even if a draft somehow held it.
`CREDENTIAL_INPUTS` is **derived from the four grant roles** (three field names
each, `sessionToken` included even though no input renders one yet, because
`grantBody` already reads it) and exported beside the allowlist, so the suite
asserts the two lists are disjoint mechanically rather than by reading them.

**And the failure banner says so.** "Your input is kept" is true of every form
in this tree except the two that take a credential, where it is the exact
opposite of what happened -- the re-render an operator is reading has just
emptied the boxes the sentence is about. Both credential forms declare
`clearsCredentials: true` on their mutation subject and get the true sentence
instead. The rotation also runs the grants' own checks **before** it sends, so
an operator who chose `new` and typed nothing is refused beside the field
rather than by a 422 on a JSON path that matches no input on screen.

### Who may start one

`destinations`, `topicDiscovery` and `preflight` are the three domains' **read
floors**: "this build serves the domain and you may read it". They are true for
a Viewer, who may not create a destination, start a discovery or cancel a check.
So every write goes through `client.js`'s `mayOperate(ns)`, which reads the
product roles `/session` publishes per grant. An **empty role list is a mode,
not a refusal**: legacy mode has no product roles at all and localAdmin has none
either, and in both the server is the gate. What is refused is the case that
really is one -- an authenticated actor who holds roles in this namespace and
none of them is Operator or Administrator.

Splitting the three flags into read/start pairs is the better answer and is
deliberately not taken here: `CAPABILITY_FLAGS` is frozen at nineteen entries
pinned against the schema's own `required` set, so it is one commit across
`authz.rs`, `contract.rs`, `openapi.rs`, the schema and `contract.js`.

### What this build cannot do yet, said rather than worked around

Each sentence below is also **on the page**, beside the control it is about,
with the task that owes the missing piece named in it. A "not available yet"
sentence with no owner is how a gap becomes a permanent feature.

* ~~**A NEW schedule cannot name a destination.**~~ **Closed by PLAT-10.1.**
  `CreateScheduleRequest` took an inline `archive` and had no `destinationRef`,
  so the create form kept two hand-typed inputs and said which task owed the
  field; the create route now takes `destinationRef` (and `allUserTopics`, a
  `timeZone`, deadlines, catch-up and retries), the guided form sends it, and
  the inline URL and Secret moved behind a disclosure for an installation with
  no saved destination yet. Neither form has ever derived an inline archive
  from a chosen destination, and neither does now: a destination carries an
  endpoint, a region, an addressing mode and a CA bundle that an inline archive
  does not, and dropping four of those silently would write to the wrong place.
  What this page also does is **read** it: the schedules table has a DESTINATION
  column resolving `spec.destinationRef` by name against the destinations it
  read, showing the location a live one writes to, refusing to say where a
  vanished one writes, and naming an inline archive as one.
* ~~**A recovery point publishes no frozen destination.**~~ **Closed.** The
  `Backup` projection now publishes `destinationRef` (with the frozen uid) and
  `locationDigest` (BACKUP-PROJECTION-NO-DESTINATION), the wizard takes the
  source destination from the point and matches the digest, and PLAT-08.2 adds
  the evidence destination -- see *Storage choices*.
* **A coverage label needs a field the console does not publish.** The three
  strings are `Coverage::label()`'s, verbatim, and D1 W7 built the surface:
  wherever an object carries `status.selection`, the page renders its
  `coverage`, its `mode` ("the policy asked for named topics" / "all user
  topics") and its counts, with the `limitedTopicCount` this block exists to
  make visible, and the `TopicsResolved=False` reason and message beside it.
  In **legacy mode that is every run**. In console mode it is none of them:
  `Backup`'s projection publishes `trigger` and `scheduleRef` and no
  `status.selection` at all, so there is nothing to read, and the page says so
  and names **PLAT-09.2** for the projection. It does not infer a coverage from
  the selection mode, the frozen topic list or a successful phase -- guessing
  "all user topics" from an empty `topics` would invent the very claim the
  labels bound. **A schedule never carries a coverage**: coverage is decided at
  a run's freeze, so the schedule card shows the policy and the run shows what
  it got.
* **There is no source-connectivity check kind.** D2 section 4.2's plan kinds
  are `topicInventory`, `operationReadiness`, `restorePreflight`,
  `destinationAccess` and `evidenceFetch`; none of them is "dial this connection
  and tell me if it answers" on its own, and a backup readiness check requires
  one to a thousand named topics. So the clusters page keeps its re-read
  control, still labelled *connection probe*, says that the real dial there is
  "Discover topics", and names **PLAT-03.1** for the check kind and
  **PLAT-07.2** for the control that would consume it.


## A schedule's future policy, its next runs, and Back up now

D1 W7. Three routes, four surfaces, and one rule underneath all of them: the
browser evaluates no cron and infers no state.

### The policy panel is a REPLACE, and the form is built around that

`PUT .../schedules/{name}` replaces a schedule's **whole future policy** under
an `expectedGeneration` precondition. A field omitted from the request is
**removed from the schedule**. A panel that showed three fields and sent three
fields would therefore silently clear the other nine, so every field of the
policy is on screen -- the cadence, the time zone, the selection in both its
shapes, the location, the deadlines, the catch-up and retry policies, the
concurrency policy, retention and suspension -- and every one of them is sent.
The sentence above the button says exactly that.

An input left **blank** sends nothing for that field, which removes it and
returns the schedule to the documented default. That is why the inputs are not
prefilled with the defaults: prefilling `3600` would turn every save into a
schedule that explicitly pins what it used to inherit. The default is printed
in the help text beside each input instead -- `timeZone` UTC,
`startingDeadlineSeconds` 3600, `catchUpPolicy` `None`, `retry` none,
`retry.delaySeconds` 300 when a retry policy is set without one,
`activeDeadlineSeconds` 3600.

`sourceRef` is never sent. The route carries it only to refuse it (`422
sourceRef: field_immutable`, before any read), and this page has no reason to
ask for that refusal.

**A successful save re-reads.** The panel is opened at the revision the card
was rendered from, and saving moves that revision -- so on a 2xx the page reads
the schedule again and renders what the API server now holds, exactly as the
suspend toggle has always done. Repainting from the copy captured at mount
would leave an operator looking at the policy they had just replaced, opened at
a revision that no longer exists, and a second save from that screen would
resend the pre-edit policy under a stale `expectedGeneration` and be refused
`412` for a reason nothing on screen explained.

**A schedule whose revision this build does not publish gets no form at all.**
The precondition IS the generation; a request without one would ask the API to
replace whatever revision happens to be current when it lands, which is the
lost update the precondition exists to prevent. The panel says so and points at
`kubectl`, which carries its own `resourceVersion` precondition.

### A preset is compiled by the server, and the browser computes nothing

The five presets are D1 section 4.2's catalogue -- `hourly`, `everyNHours`,
`daily`, `weekly`, `monthly` -- and everything else is **Advanced cron**. The
form holds the kinds and their parameter bounds and **no `cronTemplate`**:
filling one in would be a browser-side cron compiler, which is the second
implementation the decision forbids. `ui/tests/d1.spec.js` compares the form's
catalogue with `ui/tests/fixtures/cadence-presets.json`, which
`crates/weirkeeper/tests/cadence.rs` generates from
`weirkeeper::cadence::presets`, so the two cannot drift.

**Preview next runs** sends the preset's parameters (or the typed expression)
and the time zone to `GET /api/v1/cadence-previews`, which compiles the preset
against the controller's own tz database and returns the canonical expression
and the next five firings. **That expression is what gets saved.** A preset
whose parameters have changed since the last preview cannot be saved until it
is previewed again -- not as a nag, but because this page has no expression to
save until the server has produced one. The identity of a preview is its
QUERY, so changing retention or the archive does not invalidate it and changing
the zone does.

The panel renders `status.nextRuns` on a saved schedule and the preview's
`runs` on a draft through **one** renderer, because they are one shape.

* **A repeated local hour fires TWICE and both rows are shown**, each with its
  own offset (`+02:00` then `+01:00`) and the controller's own PascalCase
  marker, `RepeatedLocalTimeFirst` / `RepeatedLocalTimeSecond`. Hiding the
  second would make the page disagree with the cluster about how many backups
  happen that night.
* **A local time that does not exist** is `NonexistentLocalTimeShifted`, at the
  end of the gap.
* **Blank means UTC, and the page says so** rather than leaving it implied.
* **An empty list is an answer** ("no further firing"); an **absent** list is
  not the same thing and is rendered as "this build has not computed any".
* **Staleness is `nextRuns[0].at` in the past, and nothing else.**
  `status.policy.evaluatedAt` is when the status last MOVED -- the controller
  writes nothing when nothing changed -- so comparing it with the requeue
  interval would label every healthy schedule stale. With no clock supplied the
  page renders the table and makes no staleness claim at all.

### The revision, and what a running run keeps showing

The card prints the generation in force with the run-policy digest beside it. A
suspend flip moves the generation and leaves the digest unchanged, which is why
both are printed. When `status.observedGeneration` is behind
`metadata.generation` the card says the controller has not evaluated the saved
policy yet.

**A run already created keeps the revision it froze.** Saving a new policy does
not touch a `Backup` that exists, its frozen inputs or its Job, so the active-run
table reads the RUNS and not the schedule: the schedule at g9 beside a run that
froze g7 is the PLAT-05.1 invariant on screen, not a rendering mistake. A run
with no recorded revision says **"revision not recorded"** -- never revision 0,
and `g0` IS a revision, so it is the absence of the field that means this.

### Back up now, and the one intent behind it

`POST .../backups` requires an `Idempotency-Key`, and that key is what makes a
double click, a retry after a timeout and a "Check status" one run.

**The intent is a field of the draft, and its lifetime is the draft's.** The
manual-run form's draft (`formKey(ns, "schedule-run-now", <schedule>)`) holds
exactly one declared field, `intent`, whose value is `logweir-ui.manual.` plus
**32 random hex characters** -- `crypto.getRandomValues`, not a counter and not
a clock. Every attempt of that intent reads the same field and sends the same
key, and the API answers the second one `200` with `replayed: true` and the
first run's uid.

**A reload genuinely ends the intent.** The draft registry is module state and
there is no browser storage anywhere in this tree, so the draft and its key are
lost together; because the body is random rather than ordered, a new session
cannot reproduce an old key even by accident. A click after a reload is
therefore a deliberate NEW run and will create a second one -- which is what
this page, this file and `docs/kubernetes.md` section 16 all say. (An earlier cut
composed the key from a module-level counter that reset on every load, so the
first intent after a reload reproduced the first intent before it and every one
of those sentences was false; a live review caught it.) The panel also lists
the manual runs of this schedule that already exist, newest first, with their
triggers and frozen revisions, so the reader sees the run their click made
before deciding to make another.

**"Back up again" is the only thing that mints a new intent**, and it appears
once a run exists or once the API has refused with a conflict. It drops the
draft, which ends the intent, and clears the mutation record; the run that
exists stays on screen and keeps its own identity. That is PLAT-06.2's second
acceptance clause -- *a deliberate later backup creates another* -- and without
that control a console could take exactly one manual backup of a schedule,
ever.

**A `409` carries a fact and the page renders it.** `policy_changed` shows the
revision that is in force NOW, from the problem document's one extension
member, and says that running a revision you have not seen is what
`expectedGeneration` exists to prevent; `idempotency_conflict` says the intent
was spent on a different request. Both are followed by "Back up again", which
is the answer the problem document itself asks for. Every other refusal -- a
403, a 422 -- gets the generic message and no new intent: spending a fresh key
on the same bad request is not an answer.

**A save that could not be confirmed says the right thing.** For every other
create in this tree the object's NAME is its idempotence, so "submitting again
reuses the name" is true; for a manual run the server derives the name and the
KEY is the idempotence, so the unknown-outcome sentence names the key instead
and no sentence carries an empty name.

**Nothing blocks a manual run, and the page reflects that.** A suspended
schedule and an active run are notices, not refusals: D1 section 8.3 is explicit
that nothing about a schedule blocks one, and running a manual backup does not
resume a suspended schedule. What a suspended schedule or a `notReady` preflight
does is require a second, explicit confirmation, carrying **the object's own
recorded reason**; confirming a red verdict sends it as
`readinessAcknowledgement`, which the API stores as an annotation, so a backup
taken past a failed check says so on the object for ever. The API itself never
reads that check and gates on nothing.

The body is the schedule's name and the revision this card was rendered from,
and no policy field at all -- a policy field in that body is a `422` by design,
because the API copies the schedule's own revision. That is what makes "the
copied schedule revision" a fact about the run rather than a form's guess. A
`409 policy_changed` carries the revision that is in force now, and the page
shows it.

### The trigger column, and the ceiling it will not invent

The Backups table reads `spec.trigger.kind` and nothing else: `Scheduled`,
`CatchUp`, `Retry` and `Manual`. `spec.triggeredBy` -- the older
`manual | schedule` string -- is still rendered on a run's detail as the
separate fact it is, and is never read as a kind, because it cannot tell a
catch-up or a retry from an ordinary slot. A run frozen before PLAT-05.1
carries no trigger and says so.

A retry renders **"Retry, attempt 2"** in the Backups table and **"Retry 2 of
3"** on the schedule's own card. The ceiling is the schedule's CURRENT
`retry.maxRetries`, a run carries no copy of it, and the table does not read
schedules -- so where the number is not at hand it is not printed.

### What legacy mode can do, and what it cannot

The cadence preview and the policy replace are **console-only**. There is no
preview route in front of `kubectl proxy` -- it is a computation `logweir-api`
performs with the controller's own time-zone database, and the browser will not
evaluate cron to fill the gap, because a second implementation is a second
opinion about when a backup runs. A saved schedule's `status.nextRuns` IS
rendered in legacy mode: the controller computed it, and the page reads the
object. The whole-policy replace is console-only for a different reason: it is
built on the product API's `expectedGeneration` precondition and its pre-write
refusals, and a JSON-merge patch against kube-apiserver would have neither.

`Back up now` is **no longer console-only** (PLAT-06.2). It was, for two
reasons that were both about the mode and are both answered now.

**The name.** A manual run has no name until its idempotency scope is hashed:
D1 section 8.2 makes it `logweir-manual-` plus the first 26 characters of the
lowercase, unpadded RFC 4648 base32 of `sha256` over the length-prefixed tuple
`(issuer, subject, namespace, route, key)`. That rule now lives in one place
this page can call, `ui/client.js`'s `manualBackupName`, written to match
`crates/logweir-api/src/idempotency.rs::identity` byte for byte and PINNED
against it by `ui/tests/fixtures/manual-backup-names.json` -- ONE file, and each
side is held to it by its own unit test:
`ui/tests/d1.spec.js::the_manual_run_name_rule_is_one_rule_and_the_fixture_pins_both_sides`
drives this function over every recorded row, and
`crates/logweir-api/tests/manual_backups.rs::the_manual_run_name_fixture_is_this_routes_own_rule`
drives `idempotency::identity` over the same rows. Neither side pins itself, and
a drift in either fails a test with no cluster and no browser.
`scripts/live/d1/run.py`'s `L-06-2-cli` adds the live leg: it re-derives every
recorded row with this function on a real machine, and then requires the name
the real `logweir-api` gave an object it created to equal the name this function
derives for that same live scope. The scope's issuer and subject are the EMPTY string
in legacy mode, because a browser behind `kubectl proxy` genuinely holds
neither: the credential is attached by the proxy, out of the page's sight. The
consequence is stated rather than hidden -- in legacy mode a run's name is
decided by `(namespace, route, key)` alone, and the key is 128 random bits
minted per draft, so two proxies cannot collide by accident; if they ever did,
the second create would be an `AlreadyExists` whose stored request hash decides
replay versus conflict, which is the same answer the product API gives.

**The grant.** `charts/logweir/templates/ui/ui.yaml` now grants the page's
ServiceAccount `create` on `backups`, and nothing else on that resource: no
`patch`, no `update`, no `delete`. A run's inputs are frozen (PLAT-06.1), a
second run is a second object, and
`chart_lint_the_legacy_page_creates_a_backup_and_never_edits_one` pins the verb
set.

**What this mode does NOT do is compute a run policy digest.**
`spec.scheduleRef.runPolicySha256` is `weirkeeper::policy::run_policy_sha256`
over a canonical document; the controller recomputes it and refuses the run
terminally on a mismatch, so a browser canonicalising the policy itself would
be a second opinion about the one number whose job is to say that two parties
agree. The page COPIES `status.policy.runPolicySha256` and only when
`status.policy.generation` equals the `metadata.generation` it is about to
copy -- the controller's own statement that the two describe one revision. When
it does not, the page refuses by name, says which number is behind, and points
at `kubectl create -f config/samples/backup-manual.yaml`. An ad-hoc run with no
schedule (D1 section 8.2's body B) stays console-only for the same reason: there
is no published digest to carry and no schedule to copy.

**Idempotence in this mode is the object, not this page's memory.** The created
`Backup` carries `logweir.dev/request-sha256` over the request as the page
meant it -- route, namespace, schedule name, expected generation, readiness
acknowledgement, in that order, with an absent optional spelled `null`. A
second click derives the same name, gets kube-apiserver's own `AlreadyExists`,
reads the stored annotation, and answers "already started" when it matches and
a refusal when it does not. Nothing is patched, replaced or adopted either way.
Console mode's equivalent annotations are `api.logweir.dev/request-sha256`
beside a scope hash, a request id and an actor; the two are different
annotations because they are hashes of different documents, and the field-by-
field comparison that proves "one CR path" is over `spec` and the labels, which
ARE identical.

`Schedule.generation` is **optional in the published schema and always emitted
by this build**. D1 W6 left it out of the schema's `required` set so that
tightening it would be one cross-side commit; `ui/tests/contract.spec.js`
compares this client's required set with the schema's, so moving it here alone
turns that arm red. The tightening needs `crates/logweir-api/src/contract.rs` in
the same commit.

## Creating a schedule, and the page one schedule has

`#/schedules` is a list and, since PLAT-10.2, a **detail**:
`#/schedules?ns=<ns>&name=<name>` is one schedule, its actions and its history.
The shape is the one every other list/detail pair here uses -- `?name=` on the
list's own route -- which is also the whole of the migration story: `#/schedules?ns=<ns>`
was the only schedules link there had ever been, a hash with no `name` reaches
the list exactly as it always did, and there is nothing to redirect.
`ui/tests/schedules-detail.spec.js::the_deep_link_reaches_one_schedule_and_the_old_list_link_still_reaches_the_list`
asserts both halves through `parseHash`.

### The create form and the Future policy panel are one form

`renderScheduleForm` composes the same four renderers the policy panel
composes -- `renderCadenceFields`, `renderSelectionFields`,
`renderPolicyLocation` and `renderPolicyFields` -- under the same field names and
the same draft list. A person who creates a schedule here and edits it there is
looking at one form twice, and a rule that holds on one holds on both because
there is one implementation of it. What creation adds is the **identity**: the
object's name and the source connection, which the edit route cannot reach
(`sourceRef: field_immutable`).

The standard route is therefore: a cadence **preset** with the API's next-run
preview beside it, a **coverage** (a named allowlist, or all user topics with
exclusions and an explicit `incompleteDiscovery`), a **saved destination** by
name, and the saved cluster selector. No YAML, no endpoint to reconstruct, no
signing step. The deadlines, catch-up policy, retries and concurrency policy are
inside a collapsed `<details>`, and so is the inline archive URL: present for an
installation with no saved destination yet, and off the route a person is led
down.

**`POST .../schedules` takes the whole policy** (PLAT-10.1). It took seven fields
while the edit route took thirteen, which is why D2 W13's record listed
`CreateScheduleRequest.destinationRef` under *owed by the API before these pages
are complete* and why this form used to print that debt on screen. `archive` xor
`destinationRef` and `topics` xor `allUserTopics` are the route's rules, not a
copy kept here, and every added field is optional -- so a pre-PLAT-10.1 body
still creates byte for byte the object it created before.

### What the page stopped deciding, and what it still decides

**It no longer parses cron.** 10.1's acceptance is that an invalid field is
refused *in the API's own words* with the draft retained, and a page that
pre-empted the cadence engine answered in words the controller never said. What
survives is a **shape** check -- five whitespace-separated fields of cron
characters, or a macro -- so `every night` never leaves the browser and
`61 * * * *` does, and comes back as `schedule: schedule_invalid` with the API's
message beside the cadence input. The zone is the same: `timeZone:
timezone_unknown`, on its own field. `SCHEDULE_FIELD_PATHS` carries **both**
spellings, the stored object's (`spec.schedule`) and the request's (`schedule`),
because in legacy mode the API server refuses and in console mode the product
API does.

**It still refuses to invent an expression.** A preset cannot be created until
`GET /api/v1/cadence-previews` has compiled it: this page holds no cron
compiler, so there is literally nothing to put in `spec.schedule` until the
server has produced one. That is `submitPolicy`'s rule for `submitPolicy`'s
reason, and a preview of *different* values is not a preview of these.

**Readiness is inside the form** and starts the same `Preflight` of operation
`backup` the standalone panel starts, against **this form's** source,
destination and topics -- so what is checked is what is about to be created. The
verdict is the check's own recorded result (UI-FAKEPREFLIGHT), a verdict that
has not arrived is `pending` and never `ready`, and a dynamic selection sends no
topic list because what it will cover is decided per run by a discovery. It does
**not** gate the create: a readiness check is a statement about the minute it ran
in, and a credential can be rotated in the next one.

**A verdict belongs to the request it answered.** The form records the exact
readiness request it sent; once the source, destination or topics no longer
describe that request, the verdict is replaced by a stale note
(`data-readiness="stale"`) rather than shown beside inputs it was not about. A
refused or failed start is an answer too: the Check button stays, the refusal is
shown beside it (`#schedule-readiness-error`), and its field errors
(`backup.topics`, `backup.sourceConnection`, `backup.destination`) are placed on
the inputs they name.

**A successful create navigates to the new schedule**, carrying the name the
*server* minted -- in console mode that is `sch-<hash>` from the idempotency
scope and not the name typed into the form. That is 10.1's first-run redirect,
and the detail is where "Run first backup now" is.

### The history, and the two verdicts it will not collapse

The detail's history is every run the schedule made, newest first -- running,
failed and finished alike, because a table of only the good rows would agree
with itself and disagree with the cluster. Each row is joined to the **durable
recovery catalog** on the backup set id (`status.backupId` on the `Backup`
against `backupId` on the catalog's view entry) and carries two columns:

* **AVAILABILITY** -- can the archive still serve this point: `Available`,
  `Missing`, `Unreadable`, `Deleted`, `Conflict`, `UnsupportedFormat`,
  `Partial`.
* **VERIFICATION** -- does its evidence verify under a key this installation
  accepts: `Verified`, `VerifiedHistorical`, `UntrustedSigner`, `Revoked`,
  `Invalid`, `NoEvidence`, `NotAttempted`.

Both words are the catalog's, rendered verbatim. **Green comes from the
catalog's own `selectable`** -- D3 section 5.4's conjunction, materialised by the
controller and joined by the product API with the controller's verdict on the
point's own `Backup` (a refused one publishes `backupVerdict` and is never
selectable) -- and never from this page recomputing it; availability keeps a green
of its own, because "the bytes are readable but the signer is a stranger" is an
evidence problem and not an outage.

**A run writes a set and a set can hold several points.** The console's own
`ui/tests/fixtures/console/catalog-points-states.json` carries five entries
under one backup set id, so a join taking the first of them would call a set
holding a `Missing` and a `Deleted` point healthy. The verdicts are over the
**whole set**: every distinct word is shown (no severity order is invented
here -- deciding whether `Conflict` is worse than `Unreadable` would be an
opinion the catalog has never published), and a set with one unselectable point
is not green.

**A run the catalog has never seen is neither available nor unavailable.** Both
columns read `not in the catalog` and neither is green: PLAT-11.1's own
limitation was that availability "reflects what the run recorded, not the
bucket", and the honest answer to "is it still there" when nothing has looked is
that nobody has looked. A catalog that cannot be read at all -- legacy mode has
no route for the point list -- says so in its own words and still colours
nothing.

**Only a complete, current view may say `not in the catalog`.** The points
route answers with the view's own coverage (`docs/api.md`), and a run the view
does not list is named by it, never by a guess:

| The view says | An unlisted run reads | Why |
| --- | --- | --- |
| a page vanished mid-read (`incomplete`), a page failed, or the page budget ran out | `catalog incomplete` | the read is partial; nothing about the archive follows |
| `viewExpired: true` | `catalog view expired` | the sync Job's TTL collected the pages -- the window aged out, not the archive |
| `truncated: true` | `outside the catalog view` | the archive holds more points than the view's `sync.viewLimit`; an older run may be in the archive |
| none of the above | `not in the catalog` | a complete, current view that has looked and does not list it |

A failed or partial read dominates, then an expired view, then a truncated one;
each also puts a note above the table (`#schedule-catalog-unreadable`,
`#schedule-catalog-expired`, `#schedule-catalog-truncated`).

### Paused, and deleted

A **paused** schedule keeps its history and its restores: suspension stops
future slots and touches nothing that exists. The control is the list's own
suspend toggle, bound to this schedule.

A **deleted** schedule is a state and not a 404. PLAT-05.2 decoupled retained
history from schedule deletion -- deleting a schedule cascades to nothing, and
every run, plan and archive object outlives it -- so the detail renders the
history it left behind, says the schedule is gone, offers every per-point
restore, and offers none of the three controls that need a schedule to act on
(no toggle, no policy form, no "Back up now"). Runs carrying a **different**
`spec.scheduleRef.uid` under the same name are listed apart: a schedule deleted
and recreated under one name is a different schedule, and counting its
predecessor's runs would attribute one policy's protection to another.

### Restore, from here

Every restore on this page is PLAT-11.1's deep link, built by
`restorePointRoute` -- `#/restore?ns=&backup=&uid=` -- and the wizard is
untouched by this task. The page-level "Restore from this point" takes the
newest recovery point **the catalog does not rule out** and the per-row links
take their own, so choosing an older one is a different link and not the same
link with a row highlighted. A row that is not a recovery point
(`isRecoveryPoint`: `Succeeded`, with a backup set id and a covered window)
offers no link, because there is no plan to build.

**The catalog can rule a point out.** A set the catalog lists with any point
not `selectable` (a `Missing` manifest, an untrusted signer) offers no row link
-- the row says the catalog marks it not selectable -- and is never the
page-level Restore or the "Latest restorable point" whose age the facts report.
A newer set skipped that way is named beside the offered one. A run the view
does not list is not ruled out by it: the catalog has said nothing, and the
row's verdict words say which of the four cases above applies.

**Runs with no schedule UID are their own group.** A run naming this schedule
in `spec.scheduleRef` with no `uid` -- what runs created before the UID was
recorded look like -- could be this schedule's or an earlier same-name
schedule's, so the live detail lists it under "Runs naming this schedule
without a schedule UID" and never counts it as this schedule's latest point.

**Every action re-reads this page.** A successful pause, resume, policy save or
Back up now re-mounts the detail from the API server's current objects (the
run-now panel keeps its result across the re-read), and never the namespace
list.

## The operation route, and the run it names

`#/operations?ns=&kind=&name=&uid=` is where one durable run lives. It is
reached FROM a run -- a row in `#/backups` or `#/history`, or the outcome line
one "Back up now" click produced -- and it has no list of its own, because a
list of operations would be those two tables a second time.

**The route carries the whole identity, and that is what makes it durable.** A
refresh, a new tab and a link pasted into an incident channel all open the same
run, because the custom resource is the source of truth and nothing about this
view is state. The `uid` beside the name is a guard and not decoration: a name
is reused, a Backup deleted and recreated under one is a different run with
different evidence, and an object answering to a different uid gets **this name
now refers to a different run** rather than a silent switch under a reader who
came from a link about the old one. The guard is applied to every document that
arrives, not only the first.

**The watch is a read that repeats, and aborting it cancels nothing.** In
console mode it opens `GET .../operations/{kind}/{name}/events` with
`EventSource` -- the second request site in this tree, beside the one `fetch`,
built by the same `path(...)` validator, carrying no header and no token in its
identifier, because the browser attaches the session cookie itself on a
same-origin stream. A connection that fails is retried at 1s, 2s, 5s and 30s
with jitter, and after **three** failed connects the page polls instead: the
interesting cause of a failed stream is not a flaky network but a proxy that
buffers or refuses `text/event-stream`, and that one never resolves on its own.
Legacy mode polls the object every 5s, and every 30s after five consecutive
errors. Leaving the route closes the connection; the Job keeps running, and
there is no cancel route in v1.

**The read and the stream carry two different shapes.** `GET
.../operations/{kind}/{name}` answers `OperationViewResponse` -- `{item,
requestId}`, both required -- and the stream's `operation` and `reset` frames
are `serde_json::to_string(&OperationView)`: the flat view, no wrapper, no
request id. `contract.js` declares both (`decodeD3Operation` and
`decodeD3OperationFrame`) and the watch uses the right one at each site. An
envelope arriving on the stream is a contract failure naming its field, never
a blank render.

**`end` is a reason, not a document, and two of its three reasons are not an
end.** Its payload is `{"reason": "settled" | "maxDuration" | "vanished"}`.
`settled` stops the watch -- the run is terminal and its verdict is in, the
same pair `is_settled` uses on both sides. `vanished` stops it too and says
so: the last snapshot stays, because it is what was true, and an object
created later under the same name is a different run. `maxDuration` is the
CONNECTION's 300-second ceiling, which a run longer than five minutes hits
while it is still running, so the watch reconnects rather than freezing the
page on a mid-run snapshot; it falls back to polling after as many empty
closes as failed connects, and one document in between resets that count. An
`end` this build cannot read takes the `maxDuration` side, which costs a
connection rather than showing a running operation as a finished one.

**A rehearsal is labelled before its scorecard exists.** `targetMode` is on
the published view from the moment a `Restore` is created, and the operation
view prints it there -- `scratch` or `newTopic` -- for a run that is pending,
running or refused. It is not the completion guidance: that stays beside the
scorecard and says what the run produced, while this says what the run is, and
"these topics are deleted by teardown" is worth reading in advance. A
`Restore` that records no mode says so; no mode is guessed.

**It computes no state.** In console mode the normalized `state` is one of ten
words `logweir-api` derives; this page prints the API's word. In legacy mode
there IS no normalized state, and the page says which API computes it and shows
the controller's own `status.progress` rather than implementing that mapping a
second time in a browser. `Date.now()` is used for one thing in this whole
flow -- reconnect jitter -- and for no verdict at all.

**The result and the evidence are two sections.** "The run exited 0" and "the
document it signed verifies" are facts about two different things. A succeeded
run whose evidence is `NotAttempted` is never rendered as a verified success.

## What a badge claims after D3

The green rule gained one clause and the not-green caption gained a word.

**In console mode the verdict arrives already combined.** `OperationTrust.state`
is D3 section 2.5's own word -- `verified`, `verifiedHistorical`, `untrusted`,
`invalid`, `notAttempted`, `notApplicable`, `pending` -- computed by
`logweir-api` from the controller's `result` and its `trust.basis`. The page
reads that word; re-deriving it would be the normalizing API's own table
implemented a second time in a browser. Beside it, and never instead of it,
`OperationVerification.state` is the SIGNATURE result: an authentic signature
under a key this installation does not accept is `verification: valid` and
`trust: untrusted`, and only the second one decides the badge.

**In legacy mode there is no such word**, so the page keeps its own rule over
the custom resource. Two documents, two rules, each reading what its own
document carries.

**That rule: green** is `evidence.verification.result == "Valid"` **and** a
trust basis that leaves the verdict standing **and** the kind's own success
field (`exitCode == 0` for a Backup, `outcome == "pass"` for a Restore).

**Three bases leave it standing, and they are three different facts:**
`Current`, `Historical`, and *no basis at all* -- which arrives two ways. An
object an older controller wrote carries no `trust` block; a DTO that has to
spell something spells it `basis: "None"`, which is what D3 section 12 says it
carries for exactly that object. Both are ABSENCES: the trust layer has nothing
to say about the verdict, not that the verdict is worse. Reading either as a
downgrade would put `unverified: no verification was recorded` on every archive
an upgraded cluster holds, with `result: Valid`, `verifiedAt` and
`matchedKeyId` sitting beside it saying otherwise. A basis this build cannot
read is not green, because a word the page cannot understand is not one it may
treat as a pass.

A **`Historical`** basis carries `(signed before that key was retired)` and is a
**pass, not a warning**: the supported key rotation is meant to produce exactly
that state, and the public material can never be edited out of the policy.

**Not green still carries the word `unverified`**, which is what every older
surface looks for and what makes a new verdict fail closed on one. What is new
is the case beside it, because the three are three different claims with three
different repairs:

| case | a claim about | what to go and look at |
|---|---|---|
| `Invalid` | the DOCUMENT | the signature or a digest did not match |
| `NotAttempted` | the CONTROLLER | it had no credential, could not read the object, or had no trust material |
| `Untrusted` | the SIGNER | the bytes are authentic and this installation does not accept the key |

A fourth row is not a `result` but a basis: `RecordedBeforeRevocation` is a
compromise-revoked key whose evidence a controller had already observed. It is
**never green**, and it is not silent either: the observation is real and it is
not a substitute for a signature this installation still accepts.

**Every restore result carries its scope sentence.** A record check is a
SAMPLE: the counts are labelled exactly, the last clause says "this is a sampled
check, not an exhaustive comparison", and the word `complete` is not a level
this version has -- no function in `render.js` can spell it.

## What the four D3 surfaces refuse to say

* **`#/protection`.** Schedule health and protection health are two columns and
  are never collapsed: an enabled, healthy, never-failing schedule can have no
  recoverable backup. The capture-start instant and the newest archived record
  are labelled apart, because an idle topic makes the second look old for a
  reason that is not a gap in protection. `Protected` has three statuses, and
  `Unknown` is never rounded to `False`. A delivery failure changes nothing
  about a backup's own recorded result, and the page says so beside the ledger.
* **`#/catalog`.** Availability and verification are two axes with two repairs.
  Whether a point may be restored from is the catalog's own materialised
  `selectable` field as the product API publishes it -- joined with the
  controller's `Backup` verdicts, whose refusal is shown beside the row as
  `backupVerdict` -- read and never recomputed here; a page whose join is
  incomplete (`backupVerdictsIncomplete`) says so and offers no restore.
  Nothing is hidden --
  every state is listed with its remedy -- and the view is named as a bounded
  WINDOW over object storage, so an expired one reads as a missing VIEW and not
  as a missing archive. The point table says when it is ONE HTTP page of a
  larger view, which is a different truncation from the window and reads as
  one. **There is no one-click trust**: an unknown signer gets its key id, the
  out-of-band fingerprint command and a document this page renders and does not
  apply. Its one submission clears itself: a durable result empties the form
  and re-reads the list, so a second click cannot make a duplicate catalog, and
  a corrected body mints a new idempotency intent rather than spending the one
  the refused request used.
* **`#/keys`.** `unknown` is not `valid`. A key's evaluation reads `unknown`
  whenever the object carries no status, its `observedGeneration` is behind, or
  its `evaluatedAt` is outside the freshness window -- measured against the
  **server's** clock (the `Date` header of the answer that carried the object),
  never the browser's, and `unknown` again when there is no server instant at
  all, under its own reason. That instant is carried forward by the time
  elapsed since it was recorded and **dropped after five minutes**: a recorded
  instant that never expired is a stopped clock, and a clock in the past
  shrinks the measured age, which is the direction that reads fresh. The
  elapsed time is a difference of two monotonic readings and never an absolute
  one. Retirement and revocation are explained apart.
* **`#/schedules`' retention panel.** It reads `status.enforcement`, which is
  what is HAPPENING, and not `spec.mode`, which is what was asked for.
  `RETENTION_SENTENCE` is kept verbatim for a schedule report and for
  `RecommendationOnly` and is replaced by the mode's own sentence otherwise;
  printing "Logweir never deletes from your archive" beside a policy that
  deletes nightly would be the most consequential false sentence this console
  could render.

**The four D3 kinds are not in the legacy in-cluster UI's ClusterRole.** The
chart's role is unchanged by this change, so under the Helm UI the three D3 tabs
are console flows and the keys view falls back to the roster by name, with the
API server's own refusal rendered rather than an empty table. A local `kubectl
proxy` run from a kubeconfig that may read them shows them in legacy mode too.

## The three rules, each with a gate

1. **No external resource of any kind.** No content delivery network, no
   remotely hosted font, no analytics, no icon fetched from elsewhere, no source
   map, no bare module specifier. `scripts/check-ui-offline.sh` walks everything
   here except `*.md` and `tests/`, and fails the lint gate naming file and line.
   It has no exemption inside that scope, and it fails if it enumerated no file
   at all -- so renaming this directory without moving the gate's root turns the
   gate red instead of green.
2. **No credential in the page.** The page holds none and stores nothing. The
   same gate fails on a request header carrying authorisation, on its scheme
   keyword, on either browser-storage write and on the cookie accessor.
3. **`create` only, plus one update.** `api.js` exports no delete and no generic
   patch. `patchSuspend` is the single write beyond `create`, and it sends a
   JSON-merge patch touching only `spec.suspend`. `create` and `patchSuspend`
   check the plural against the frozen `WRITABLE_PLURALS` first and throw a
   `RangeError` otherwise -- `trustrosters` included, which is cluster-scoped,
   admin-only, and never writable from a page.

`crates/logweir/tests/ui_lint.rs` holds the Rust-side half: exactly one network
call site in the whole directory, on one line, inside one private function in
`api.js`, and every call to that function carrying an identifier `path(...)`
built; no absolute URL, no build artefact, no invisible codepoint, an
`index.html` in every subdirectory, and the serving command identical in this
file, in `../docs/kubernetes.md` and in the `ui` recipe of `../justfile`.

That test counts the call sites by name, so this file cannot spell the name
either -- which is why the sentence above says "network call site" and not the
token. A document that had to quote it would be the one file the count could
not survive.

## Running the tests

```bash
node --test 'ui/tests/*.spec.js'
```

from `logweir/`, with **node >= 20.0.0**. Node is a test runner here and nothing
else: it builds no asset and fetches no package, and with no `package.json` in
the tree it detects these files as ES modules from their syntax.

On the node shipped with this host (v25.6.1) a **directory** argument to
`--test` is treated as a file to execute and fails with `Cannot find module`;
the quoted glob above is the form that works, and node expands it itself.

Only `*.spec.js` is a test. `tests/` also holds **tools** -- `emit-plan.js`
prints the plan golden on stdout, and `scripts/check-ui-behaviour.sh` invokes it
by name for the `diff -u` arm. Under the wider glob `ui/tests/*.js` node
executed that tool as a test file and counted it as one passing test, which
disarmed the gate's zero-count refusal: with every `*.spec.js` deleted the run
still reported one test and exited 0. The suffix is what keeps that refusal able
to refuse.

## Previewing with fixtures

`node ui/tests/preview-server.js` serves this directory over the JSON under `tests/fixtures/preview/`, with no cluster and no credential anywhere.
Open `http://127.0.0.1:8011/ui/`; the namespace `default` is populated, `forbidden` answers every read with a 403, and any other name is empty.
It is a development tool and not a test, not a proxy and not part of the product: it binds loopback only, answers every write with a 405, and the release bundle and the gates never see it.

`scripts/plat13-ui-e2e.mjs` and `scripts/plat12-13-ui-e2e.mjs` are the live
browser harnesses: the first owns the navigation-lifetime journeys, the second
the draft, idempotency, guided-submit and approval-subject journeys. Both drive
Chromium against a real `kubectl proxy` over real objects; the second creates
its own `lw-ui-correct-*` namespace and deletes it afterwards:

```bash
NODE_PATH="$(npm root -g)" node scripts/plat12-13-ui-e2e.mjs
```

## What this page does not do

It does not mint an approval, hold a key, or submit the cluster-scoped
`TrustRoster`. There is no "Approve" button that produces a signature anywhere
in it: the approver runs `logweir drill approve` where their private key lives,
and the approvals page takes the two files that command wrote -- as UTF-8 text,
verbatim -- and refuses anything whose name ends `.pem` or `.key` or whose
content carries a private-key header, with the message **this page never accepts
a private key**. The roster is a cluster-admin step -- see the trust-roster step in
[the installation guide](../docs/install.md) -- and the page surfaces that
snippet rather than submitting it.

Apache Kafka(R) and Kafka(R) are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
