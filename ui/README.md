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
| `app.js` | the hash router and the frame. Seven routes: `#/clusters`, `#/schedules`, `#/backups`, `#/history`, `#/restore`, `#/approvals`, `#/keys`. |
| `api.js` | the **only** module that issues a network request. |
| `render.js` | DOM helpers. Sets text, never `innerHTML`. |
| `plan.js` | the restore plan document, its sha256 and the two minted names. Refuses a non-secure context at module load. |
| `lifecycle.js` | what lives and dies with one route (reads, listeners) and what deliberately does not: the in-memory drafts, the mutation records and the idempotent create. |
| `pages/restore-wizard.js` | the six wizard steps, the plan bytes, and the ONE guided submit that creates the Restore and opens what it needs next. |
| `pages/approvals.js` | the Approval list, the Restores waiting for one, and the create form for ONE chosen Restore. Refuses a private key by name and by the words that open its PEM, and never parses the two documents. |
| `pages/keys.js` | the cluster-scoped `TrustRoster`, read-only, with the out-of-band fingerprint command. |
| `style.css` | the design system, in one file: tokens, light and dark, every component. System fonts; no font is fetched from anywhere. |
| `pages/index.html` | zero bytes, on purpose -- see below. |
| `tests/api.spec.js` | the behaviour arm of the two mechanical claims, under `node --test`. |
| `tests/pages.spec.js` | the behaviour suite over the page modules: the badge rules, the wizard, the approval form, the roster. |
| `tests/design.spec.js` | the design system's guarantees: the token layer, both schemes, reduced motion, the focus ring, badges with words, the stepper. |
| `tests/mutation.spec.js` | drafts, one mutation state, idempotent creates, the guided submit and the approval subject -- driven through the real mount halves over a fake node and an in-memory API. |
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
