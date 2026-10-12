# The Logweir console page

Developer notes for `ui/`: how the page is built, served and tested, and where
its pages live. The operator's view of the console is in
[docs/kubernetes.md](../docs/kubernetes.md); its API is
[docs/api.md](../docs/api.md).

The page is static HTML, CSS and plain ES modules. There is no build step,
bundler, package manager or database: the shipped assets are the sources. Every
request goes to the same origin as the page. The legacy `kubectl proxy` mode
and its `logweir-ui` image are withdrawn in v1 (release-notes item 75); the
page is served only by the console.

## Serving it

The console, `logweir-api`, serves these files at `/ui/` and the product API at
`/api/v1` on one origin. `Dockerfile.console` copies the top-level `*.html`,
`*.js` and `*.css` files and the same directly inside `pages/` into the image
at `/ui`: twenty-six files. `just smoke-console`
(`scripts/check-image-api.sh`) compares every served file with this directory.
`README.md` and `tests/` are never copied or served.

- **In a cluster:** install the chart with `api.console.enabled`
  ([docs/install.md](../docs/install.md), section 5e). In localAdmin mode,
  reach it with `kubectl port-forward deploy/<release>-api 8484:8484` and open
  `http://127.0.0.1:8484/ui/`.
- **From a checkout:** run `cargo run -p logweir-api -- --config config.yaml`
  with `uiDirectory: ./ui` (docs/api.md, *Running it*). The service reads the
  files once at startup; restart it after an edit.
- The service serves its own `runtime.js` in place of the file here, setting
  `window.LOGWEIR_CONSOLE = {servedBy: "logweir-api"}`.

**It must be a secure context.** The restore wizard computes the plan's sha256
in the browser with `crypto.subtle`, which exists only on a trustworthy origin.
`http://127.0.0.1:8484/ui/` qualifies; anything else must be TLS. `ui/plan.js`
refuses at module load elsewhere:

```
this page must be served from a secure context: loopback (127.0.0.1 or localhost)
over plain transport, or any TLS origin. SubtleCrypto is unavailable here and the
plan hash cannot be computed.
```

**No bearer token, key or credential is ever placed in the page**, and it
stores nothing: no browser storage, no cookie of its own. The session cookie is
the browser's. The CSRF token from `GET /api/v1/session` lives in `client.js`'s
memory (`sessionToken`) for the life of the loaded page, and every write takes
it from there.

## Where things live

| file | what it is |
|---|---|
| `index.html` | the shell; loads `./app.js` as a module, every reference relative |
| `app.js` | the hash router and the frame: routes, tabs, header, error box, datagrid enhancement, focus |
| `api.js` | the only module that issues a network request: one `fetch` on one line, every identifier built by `path(...)`, plus the one `EventSource` for the operation stream |
| `client.js` | the session state, decided once at boot, and `apiClient()`, the one object every page reads and writes through; translates page objects into product-API requests |
| `contract.js` | JSDoc types and strict decoders for every product-API document the page reads |
| `validate.js` | one set of checks and one vocabulary of field paths, so a refusal lands beside its field |
| `workflow.js` | named state machines and their transition errors |
| `lifecycle.js` | per-route reads and listeners, drafts, mutation records, idempotent creates, check intents and check following |
| `render.js` | DOM helpers: text, never `innerHTML`; badges; `when` and `whenLocal`; `NOT_PUBLISHED` |
| `select.js` | the saved-connection selector and the words a connection probe may be described with |
| `plan.js` | the restore plan document, its sha256 and the two minted names |
| `operation-watch.js` | the reconnecting watch over one operation and the bounded catalog, protection, retention and trust reads |
| `style.css` | the whole design system |
| `pages/clusters.js` | connections: list, create (credential typed once), probe, discovery |
| `pages/destinations.js` | saved destinations: list, create, access rotation, access test, usage |
| `pages/schedules.js` | schedules: list, guided create, Future policy edit, suspend, Back up now, retention panel, a schedule's own page |
| `pages/backups.js`, `pages/history.js` | runs, the Restore detail, and "Restore this point" links |
| `pages/restore-wizard.js` | the recovery-point selector and the six-step wizard with its one guided submit |
| `pages/approvals.js` | Approval lists, the governed submission, and the two-person approval page |
| `pages/operation.js` | one run: state, reason, diagnoses, result, evidence and completion |
| `pages/protection.js`, `pages/catalog.js`, `pages/keys.js` | protection health, the recovery catalog and connect form, the read-only trust policies |
| `pages/index.html` | zero bytes on purpose; every directory has an `index.html` |

## Routes

A **hash** router: every route lives after `#`, so the server is asked only for
files. `app.js`'s `ROUTES` table names each route, its title, its mount and the
capability flags it needs. A route is a tab when the session holds one of its
flags in the chosen namespace (for `#/keys`, in any namespace); a hidden route
still refuses by name when an address reaches it. A visit with no route, and
the sign-in round trip, land on Clusters when the role can use it, else on
Approvals, else on the first tab it can use (`landingRoute`).

| route | page |
|---|---|
| `#/clusters`, `#/destinations`, `#/schedules`, `#/backups`, `#/history`, `#/protection`, `#/catalog`, `#/approvals`, `#/keys` | tabs; most accept `?ns=<namespace>&name=<object>` for one object's page |
| `#/restore?ns=&backup=<name>&uid=<uid>&step=1..6` | the wizard over one `Backup`; `uid` is the identity, `backup` is for reading and for naming a refusal, and a link with only `backup` resolves and pins the uid |
| `#/restore?ns=&catalog=<catalog>&point=<lwp1-id>` | the wizard over a catalog point; everything the plan needs is read again from the API, never from the address |
| `#/restore?ns=&retryOf=<failed restore>` | *Retry to a fresh target*: the selector, with a prefix derived from the failed run so the retry is a new execution and never reuses its approval |
| `#/approvals?ns=&subject=<restore>&hash=<plan hash>&name=<approval>` | one Restore's approval page |
| `#/operations?ns=&kind=&name=&uid=` | one durable run; not a tab. An object answering with another `uid` is "this name now refers to a different run" |

`step=` is written with `history.replaceState`, so a reload or a copied link
opens the same step. `ns` is always explicit, `default` included.
`scheduleDetailRoute` and the "Restore this point" builders are the only places
these addresses are composed.

## How the page decides its state

At boot `client.js` calls `GET /api/v1/session` **once**, bounded by five
seconds (`PROBE_TIMEOUT_MS`), and records the answer for the life of the page:

| answer | state | what renders |
|---|---|---|
| a session document | signed in | the tabs the capability flags allow, the header with the actor, its roles in the namespace, and **Sign out** (`POST /api/v1/session/logout` with the token, then a reload; a localAdmin console has no Sign out) |
| `401` with the product API's `unauthenticated` or `session_expired` | `SIGNED_OUT` | "Sign in to Logweir" and `/auth/login?next=/ui/#<the address asked for>`; no tabs, no namespace box, every client call refused before the network |
| anything else: no answer, a network failure, a 5xx or 429, a body that does not decode | `UNAVAILABLE` | "Can't reach the Logweir service", the probe's status and code, and Retry |

An error box leads with a headline for the status, then the server's message
verbatim, then its status, code and request id. A namespace with no role shows
the no-role landing.

## The product API contract

**A page is never handed a response.** It is handed what `contract.js` decoded
(declared members only; an absent optional as `null`; other members dropped
and recorded in `unknown`) and, for the custom-resource views, what a
projection copied into the resource's shape.

- A required member that is absent, a wrong type or an undeclared enum value is
  an Error with `reason: ContractViolation` and `status: 0`, rendered by the
  error box with the document and JSON path. It is never an empty cell. An
  unknown member is tolerated, because an older client must accept a newer
  field.
- A detail view's second read (the operation, an approval's packet) may answer
  `403` or `404` and the object renders without it; a contract failure or a
  5xx is raised.
- What a projection cannot supply is listed on the object under
  `__contract.absent`, and a cell for it reads **not published by the product
  API** (`NOT_PUBLISHED`, `notPublishedIn`, `notPublishedCell` in `render.js`),
  never `-`.
- `tests/contract-coverage.spec.js` holds the page to
  `schemas/logweir-api-v1.openapi.json` with five checks: every published member
  is declared in `DECLARED_SHAPES` or named in `IGNORED_MEMBERS`; every route's
  answer is decoded by the shape the document publishes (`observeDecodes`) and
  every body sent is an instance of the route's request schema; every console
  fixture is listed in `tests/console-fixture.js` with its schema and survives
  its decoder; every decoded member reaches the page or is named in
  `NOT_HANDED_ON`; no spec reads a member straight off an undecoded fixture.
- In a spec, `wire(name)` is the API's document, `decoded(name)` is what the
  page is handed, `wireItem(name)` composes another wire document, and
  `handedPoint`, `handedPoints` and `handedPage` decode a catalog point a row
  writes itself.

## Forms, drafts and writes

**Drafts live in this page's memory and nowhere else** (`lifecycle.js`,
per namespace and per form). A draft survives a refused field, an API refusal,
a lost response, a timeout and navigation between routes; it does not survive a
reload or a new tab. The store keeps strings and booleans only, so a form keeps
a list as text, one name per line. A value spelling the words that open a
private-key PEM, in any case and across line breaks, is never kept
(`carriesKeyMaterial`), nor is any field a form does not list. No form keeps a
password: a kept credential is always a Secret's name, and the console's
write-only credential inputs are on no draft list.

**One click makes one object.** Every create carries an `Idempotency-Key`
composed from an intent minted once per draft: `logweir-ui.connection.<32 hex>`
and `logweir-ui.schedule.<32 hex>` (the server names those objects, so the
forms have no name field), the plan bytes for a restore, and the typed name
for a destination or a catalog. A double click, a lost response and a reload
replay onto the first object. A pending submission disables its own button.

- A refusal says nothing was created. An **unknown** outcome (no answer, a
  timeout, a 5xx) says the object may or may not exist and that submitting again
  is safe. The server's message is shown verbatim and a `422`'s field errors go
  beside their fields.
- An outcome about a plan the wizard is no longer showing keeps the name it
  sent and links to it.
- **Navigation cancels reads and never mutations**: a read carries the route's
  `AbortSignal`, a create carries none.
- A list follows the cursor to the end (`LIST_PAGE_SIZE` 200); past 25 pages it
  is refused by name (`ListTooLarge`), never shown as a prefix.

**Checks are asked, not replayed** (`checkIntent`, `askCheck`): one intent token
per form goes into the check's key and is renewed only when the held check is
spent (expired, inapplicable, `failed` or `cancelled`), so a retry after a lost
response replays and asking again after that does not. A followed check is read
until its own deadline (`followCheck`: every 2 s for 20 s, then backing off to
10 s); past it the page says so and offers *Run the check again*.

**Where a submitted Restore goes** is decided by the `authorization` the API
returns (docs/api.md, *Approval policy*): `confirmed` opens the operation view,
`awaitingApproval` opens the approval page, and a two-person namespace's
approval page carries the one **Approve** button. The page never signs and never
holds a key. Under a strict or unbound namespace the approver runs `logweir
drill approve` or `drill countersign` elsewhere, and the approvals page takes
the files as text, refusing a file named `.pem`, `.key`, `.p8`, `.p12`, `.pfx`,
`.jks`, `.ppk` or `id_*`, or carrying a private-key header: **this page never
accepts a private key**.

**The restore wizard has one action.** *Create the Restore* checks that the
plan about to be sent is the plan on screen, creates it, and opens what it needs
next. `plan.js` produces the bytes once and returns the same frozen object for
the same bytes, so the hash shown and the bytes sent are one object; the API's
`planHash` comes from it, and bytes the page did not prepare are refused. A
download beside the copy button exists because copying a `<pre>` can lose
trailing whitespace. Every step is rendered and every one but the current step
is hidden, so every input is read, drafted and hashed whichever step is shown.
`workflow.js`'s step machine is a test-time invariant, not enforced at render.

Steps 4 and 6 carry `REPLICATION_DIFFERS_NOTE` wherever `replicationMayDiffer`
says the plan's factor may not be the source's; the quickstart carries the same
sentence, and one row in `tests/replication-factor.spec.js` holds all three:

> This factor can differ from the source's, and the target's storage follows
> it: a topic the source kept at replication factor 1, restored at 3, takes
> three times the storage it took there, with the replication traffic on top; a
> factor below the source's keeps fewer copies than the source had.

## Rendering rules

- **Text, never markup.** `render.js` sets text; `cell` and `messageText` show
  a bidi control as U+FFFD (`inert`), so a controller's message cannot reverse
  what follows it.
- **Instants** go through `when` (`2026-09-24 16:39:04 UTC`, in a `<time>`
  carrying the exact value); a schedule's local times through `whenLocal`, with
  their offset.
- **Badges carry words and colour, never colour alone.** A run's scorecard
  facts carry an "unverified scorecard claim" label unless the verdict is green
  by the controller's badge rule. A consumed Approval whose signer was later
  revoked for compromise is its own state, never green.
- **A queued run** reads "Queued (limit N active)" from the object's own
  `queue.limit`, plus the approval deadline for a restore (`runPhaseBadge`).
- A list row whose verification block is absent is green exactly when the API's
  `verifiedSuccess` says so.

## The design system

`style.css` is the design system and nothing else carries one. It follows VMware
Clarity as a design language, not a dependency: Clarity's token names and values
(`vmware-clarity/core` v6.17.0, MIT, noticed in `THIRD_PARTY_NOTICES.md`) are
defined once for the light theme and redefined, aliases only, under
`prefers-color-scheme: dark`. No rule outside those two `:root` blocks spells a
colour or a length; `crates/logweir/tests/ui_lint.rs` refuses one, and refuses a
`style` attribute, a `<style>` element or a CSSOM write in a shipped file. The
header of `style.css` lists where it departs from Clarity (no web font, a 16 px
base, the focus ring's colour, a theme that follows the system).

- Tables stack into cards below 768 px; `app.js` copies each column's caption
  into `data-label`, so page modules stay pure functions from JSON to a string.
- A page declares a **datagrid** by passing `{id, label}` as `table()`'s fifth
  argument. `app.js` adds the filter, sort buttons with `aria-sort`, a live count
  and pages of 10/20/50/100, kept in memory per grid until a reload.
- **Focus survives a re-render**: to the element with the same `id`, else the
  same position in the form, else the form's status region, else the view.
- Every interactive element has a visible focus ring, and
  `prefers-reduced-motion` turns off every transition.
- A chip wraps only at a space; `app.js` marks one holding a word over 24
  characters `long`, and only that one may break anywhere.
- A long identifier in a table (`ident` in `render.js`: a key id, a point id,
  a backup set) is one line, cut between 8 and 14 characters wide.
  The full value is its hover title and its text, and a click copies it. The
  Approvals table draws a principal as its issuer's host and a short form of
  its subject, each cut an ellipsis titled as one; the Approval card and the
  two-person panel show every principal in full.

## The rules and their gates

1. **No external resource.** No CDN, remote font, analytics, remote icon,
   source map or bare module specifier. `scripts/check-ui-offline.sh` walks
   everything here except `*.md` and `tests/`, with no exemption, and fails if it
   enumerated nothing.
2. **No credential in the page.** The same gate refuses a request header
   carrying authorisation, its scheme keyword, either browser-storage write and
   the cookie accessor.
3. **A bounded write surface.** `api.js` has no delete and exactly one replace,
   `consoleSchedulePolicy`; durable creates go only to
   `CONSOLE_WRITABLE_PLURALS`. `ui_lint.rs` pins the export list, the one
   network call site and the one stream construction, refuses an absolute URL,
   a build artefact and a non-ASCII byte (this file included), and requires an
   `index.html` in every subdirectory.
4. **The plan the page writes is the plan the runner reads.** `ui_lint.rs` parses
   the goldens (`tests/fixtures/plan-*.golden.yaml`) into the runner's
   `RestoreSpec`.

## Running the tests

```bash
node --test 'ui/tests/*.spec.js'
```

Run it from the repository root with **node 20 or later**. Node is only a test
runner here: no `package.json`, no packages. Quote the glob: a directory
argument is executed as a module by recent node, and only `*.spec.js` is a test.
`tests/` also holds tools (`emit-plan.js` prints the plan golden for
`scripts/check-ui-behaviour.sh`; `emit-restore-body.js` builds the chart walk's
Restore body), which the gate's narrow glob keeps from counting as tests.

`scripts/check-ui-behaviour.sh` is the gate `just lint` runs: it needs node 20
and a release `logweir` binary (`cargo build --release -p logweir`, or
`LOGWEIR_BIN`), and it fails on a run that reports zero tests. The suites are
pure: page modules are functions from a fixture to a string, `fake-view.js` and
`fake-dom.js` stand in for the browser, and no spec dials the network
(`the_ui_behaviour_suite_never_dials`). Fixtures under `tests/fixtures/` say in
`tests/fixtures/README.md` where each came from; `tests/fixtures/console/` are
instances of the published schema.

A new behaviour row comes with its negative control: the assertion that fails
when the behaviour is removed.

## Live browser journeys

The console's live journey is `scripts/prod16-1-ui-e2e.mjs` (approvals without a
personal key). It needs a cluster and is not part of `just lint`.

Apache Kafka(R) and Kafka(R) are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
