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
| `app.js` | the hash router and the frame. Eight routes: `#/clusters`, `#/destinations`, `#/schedules`, `#/backups`, `#/history`, `#/restore`, `#/approvals`, `#/keys`. Two of them carry an identity in the hash -- see *The restore route, and the point it names*. |
| `api.js` | the **only** module that issues a network request, in either mode. One `fetch`, on one line, and every identifier built by `path(...)`. |
| `client.js` | **which API is in front of this page**, decided once at boot, and the one object every page reads through. See *Two modes, one page*. |
| `contract.js` | the typed contract: JSDoc types and strict decoders for every DTO the page consumes, in both modes. A required field that is absent is a **contract failure the page renders**, never an empty cell. |
| `validate.js` | one set of checks and **one vocabulary of field paths**, so a disagreement from either server lands beside the field it is about. |
| `workflow.js` | the state machines: named transitions, and a transition error for a move a state does not accept. |
| `render.js` | DOM helpers. Sets text, never `innerHTML`. |
| `select.js` | **the saved-cluster selector and the words a connection probe may be described with**. Identity (`{uid, name}`) resolution, the freshness judgement, the searchable control. Shared by the clusters page, the schedule form and both wizard sides -- see *Choosing a saved connection*. |
| `plan.js` | the restore plan document, its sha256 and the two minted names. Refuses a non-secure context at module load. |
| `lifecycle.js` | what lives and dies with one route (reads, listeners) and what deliberately does not: the in-memory drafts, the mutation records and the idempotent create. |
| `pages/restore-wizard.js` | the recovery-point selector, the six wizard steps over the point somebody chose, the plan bytes, and the ONE guided submit that creates the Restore and opens what it needs next. |
| `pages/history.js` | Backups and Restores interleaved, the Restore detail view, and the "Restore this point" link a completed Backup row carries. |
| `pages/schedules.js` | the BackupSchedule list, the suspend toggle, the retention panel, and each schedule's recovery points with their own "Restore this point" links. |
| `pages/approvals.js` | the Approval list, the Restores waiting for one, and the create form for ONE chosen Restore. Refuses a private key by name and by the words that open its PEM, and never parses the two documents. |
| `pages/destinations.js` | **saved destinations** (PLAT-08): the list, the create form, the access rotation, the access test, `/usage`, and adopting a legacy inline archive. Console mode only -- see *Destinations, discovery and readiness*. |
| `pages/keys.js` | the cluster-scoped `TrustRoster`, read-only, with the out-of-band fingerprint command. |
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

The same twenty-two files are served two ways, and **they decide which one they are
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
| `BackupSchedule` | the per-manifest `status.retentionReport.skipped` entries (the API reports their **count**) |
| `Backup` | `status.manifestSha256`, `status.jobRef` |
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

**"Test connection" is a read, and the control says so.** The page has no
authority to make the controller dial anything: `KafkaCluster.spec` is
immutable, this page's whole write surface is five creates and one suspend
patch, and the re-probe cadence is the probe Job's own
`ttlSecondsAfterFinished`. So the control **re-reads the object** and renders
the newest observation the controller has recorded since -- which is what
"test the connection" can honestly mean from a browser holding no execution
authority. On the list each row's control re-reads **that** cluster and repaints
**that** row's probe cell, after checking that the name still answers to the
same UID; on the detail view it re-renders the panel. A read that answers after
the route has left paints nothing (PLAT-13.1).

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

One endpoint, one region and one addressing value still apply to **both** the
source archive and the evidence store. Separating those two is PLAT-08.2's UI
slice and is not done here.

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
`CREDENTIAL_INPUTS` is exported beside the allowlist so the suite can assert the
two lists are disjoint mechanically rather than by reading them.

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

* **A schedule cannot name a destination.** `CreateScheduleRequest` requires an
  inline `archive` and has no `destinationRef` until PLAT-06.2. The create form
  says so and keeps the inline fields; it does **not** derive an inline archive
  from a chosen destination, because a destination carries an endpoint, a
  region, an addressing mode and a CA bundle that an inline archive does not,
  and dropping four of those silently would write to the wrong place. The
  selector lives in the readiness panel instead, where
  `BackupPreflightRequest.destination` makes it real, and moves into the create
  form unchanged the day the field lands.
* **A recovery point publishes no frozen destination.** `Backup`'s projection
  carries `archive` and no `destination`/`locationDigest`, so the wizard cannot
  take a source destination from the point's frozen one. It sends the inline
  source archive -- which is what the `Restore` it creates carries anyway -- and
  step 5 says so.
* **A coverage label needs a field the console does not publish.** The three
  strings are `Coverage::label()`'s, verbatim, and the schedule card renders one
  when the object carries `status.selection.coverage` (legacy mode). The product
  API's `Schedule` DTO publishes neither that nor `allUserTopics`, so in console
  mode a schedule naming no topic gets a sentence saying which two shapes that
  could be and to read the object with `kubectl` -- guessing "all user topics"
  from an empty list would invent the very claim the labels exist to bound.
* **There is no source-connectivity check kind.** D2 section 4.2's plan kinds
  are `topicInventory`, `operationReadiness`, `restorePreflight`,
  `destinationAccess` and `evidenceFetch`; none of them is "dial this connection
  and tell me if it answers" on its own. So the clusters page keeps its re-read
  control, still labelled *connection probe*, and the real dial available there
  is "Discover topics".


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
