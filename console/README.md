# The Logweir console

Developer notes for `console/`: how the console is built, served and tested, and
where things live. It is React, TypeScript, Vite and PatternFly 6, with a client
typed from `schemas/logweir-api-v1.openapi.json`
([decision record](../docs/to-do/decisions/PROD-18.1-console.md)). The
operator's view of it is in [docs/kubernetes.md](../docs/kubernetes.md); its API
is [docs/api.md](../docs/api.md).

## Serving it

`logweir-api` serves the built console at **`/ui/`** and the product API at
`/api/v1` on one origin. `Dockerfile.console` builds `console/` and copies its
`dist/` (the page and its hashed files under `assets/`) into the image at
`/ui`, and nothing else; `scripts/check-image-api.sh` holds the image's `/ui` to
this tree's `dist/`, file by file. The page is `no-cache`; a hashed file is
`immutable`, since its name changes with its bytes.

- **In a cluster:** install the chart with `api.console.enabled`
  ([docs/install.md](../docs/install.md), section 5e). In localAdmin mode,
  reach it with `kubectl port-forward deploy/<release>-api 8484:8484` and open
  `http://127.0.0.1:8484/ui/`.
- **From a checkout:** `npx vite build`, then run
  `cargo run -p logweir-api -- --config config.yaml` with
  `uiDirectory: console/dist`, relative to the configuration file
  ([docs/api.md](../docs/api.md), *Running it*).
  The service reads the files once at startup; restart it after a build.

**It must be a secure context.** The restore wizard and the cutover compute
the plan's sha256 with `crypto.subtle`, which exists only on a trustworthy
origin: `http://127.0.0.1:8484/ui/` qualifies, anything else must be TLS.
Elsewhere the wizard says so and offers no Create.

**No token, key or credential is ever placed in the page**, and it stores
nothing in the browser. The session cookie is the browser's; the CSRF token from
`GET /api/v1/session` lives in `src/api/client.ts`'s memory for the life of the
page. ESLint refuses `dangerouslySetInnerHTML`, browser storage,
`document.cookie`, and `fetch` or `EventSource` outside `src/api/`.

## Routes

Hash routes: every address after `#` is `/<namespace>/<page>`, so the service is
asked only for files and the sign-in round trip (`next=/ui/#/...`) comes back to
the same page. A visit with no namespace lands on the session's first one.

| route | page |
|---|---|
| `#/<ns>` | Overview |
| `#/<ns>/backups`, `schedules`, `protection` | Protect; each has `/<name>` for one object |
| `#/<ns>/restores`, `restores/<name>` | Restores and one restore's page |
| `#/<ns>/restores/new?backup=&uid=`, `?catalog=&point=`, `?retryOf=` | the five-step wizard from a backup, a catalog point, or a failed restore (to a fresh target) |
| `#/<ns>/restores/<name>/cutover`, `switchovers`, `switchovers/<name>` | theme A's switchover review, and the Switchovers |
| `#/<ns>/approvals`, `approvals/<restore>?hash=&approval=`, `approvals/switchover/<name>` | Approvals, one restore's, one switchover's |
| `#/<ns>/catalog`, `trust` | the Recovery catalog and Trust |
| `#/<ns>/connections`, `destinations` | Settings; each has `/<name>` |

A page whose read the role lacks in the namespace says "Not available to you
here" and sends no request; an address with no page says "This page does not
exist".

## How the page decides its state

At boot `src/app/SessionGate.tsx` reads `GET /api/v1/session` once, bounded by
five seconds, and renders one of four pages:

| answer | what renders |
|---|---|
| a session document | the console, for the namespaces it grants |
| `401` `unauthenticated` or `session_expired` | "Sign in to Logweir", linking to `/auth/login?next=<this address>` |
| no answer, a 5xx or 429, a body that is not the API | "Can't reach the Logweir service", with what it got, and Retry |
| a session with no namespace | "No namespace is granted to you", and Sign out |

A namespace the session holds no role in says "You hold no role in `<ns>`".

Once the console is up it stays up (`src/app/SessionClock.tsx`). The session is
capped (`sessionMaxAgeSeconds`); about two minutes before the session's
`expiresAt` a warning offers **Sign in again in a new tab**, which renews the
cookie this tab shares. When the service later answers 401, a blocking dialog
("Your session ended: sign in again in a new tab, then continue here") keeps the
page and what was typed on it, with no browser storage, until a read of the
session succeeds again (Continue here, or coming back to the tab).

## Running it

Use Node 22 or 24. `npm ci` installs exactly `package-lock.json`, with install
scripts off (`.npmrc`).

- **On the mock:** `npm ci && npm run dev`. Every request goes to the in-page
  mock (`src/mock/`), and nothing leaves the page.
- **On a real API:** start `logweir-api` on `127.0.0.1:8484`, then run
  `VITE_LOGWEIR_API=live npm run dev`. Vite proxies `/api` and `/auth` to it,
  with the API's own `Host` and `Origin`.

| command | output | talks to |
|---|---|---|
| `npx vite build` (the image's build) | `dist/`: `index.html` and hashed files under `assets/` | the product API on the page's own origin |
| `npm run build:single` | `dist-single/index.html`, everything inlined, for demos | the in-page mock |

## Testing it

`just console` runs every gate, the same as CI's `console` job, which runs when
`console/` or the schema changes; `publish` waits for it:

1. `npm ci`;
2. `npm run check:api`: the committed `src/api/schema.d.ts` matches the schema
   (`npm run gen:api` regenerates it);
3. `tsc --noEmit`;
4. ESLint (`npm run lint`), with the rules above;
5. Vitest (`npm test`), on jsdom, over the fixtures in `fixtures/`;
6. `vite build`, then `scripts/check-dist.mjs`: the output has the shape `/ui/`
   serves, none of the mock, and no stylesheet that loads from another origin;
7. `npm audit --audit-level=high`;
8. `scripts/check-licences.mjs`: each package's licence from the lockfile is on
   the allowlist (shipped code, or build and test tools).

The plan goldens in `fixtures/` are read by both sides: `plan.test.ts` holds the
renderer to them byte for byte, and `crates/logweir/tests/console_plan.rs`
deserialises each into the runner's `RestoreSpec`.

**End to end** (`e2e/*.spec.ts`, Playwright with axe-core): run against a served
console, never the mock, and create nothing:

```
LOGWEIR_CONSOLE_URL=http://127.0.0.1:8484 npx playwright test
```

They check the page's headers, the landing, every page the navigation offers,
the wizard up to its target step and the refusals, each screen with 0 console
errors, 0 CSP violations, 0 requests off the origin and 0 axe violations. A
shared-mode console needs a signed-in browser state in
`LOGWEIR_CONSOLE_STORAGE_STATE`. A console whose certificate the host does not
trust (the PoC's local CA) needs `LOGWEIR_E2E_IGNORE_HTTPS_ERRORS=1`; it is off
by default, so a run against a real install checks the certificate. The journeys that need a cluster (sign-in
through the identity provider, Back up now, a restore to Verified, approvals)
are run on the PoC.

That storage state is a live session. Run a shared-mode console with
`npx playwright test --trace off`: a failed spec's `trace.zip` keeps the
session cookie, and a failed request can print its `cookie` header in the log.
Delete the state file and `e2e-results/` after the run, and redact the log
before it goes anywhere.

`scripts/emit-restore-body.mjs` and `scripts/emit-schema-dependency.mjs` run the
console's own code under `node --experimental-strip-types` for the Helm demo and
an e2e row.

Dependabot proposes npm updates weekly, grouped, once a release is 14 days old
(`.github/dependabot.yml`).

## Where things live

| path | what it is |
|---|---|
| `src/api/client.ts` | the one client: same origin, the cookie session, `X-CSRF-Token` on unsafe methods; `ApiError` (with the problem's field errors); `outcomeUnknown`; `mintKey`; `listAll`; the bounded session read; sign-in and sign-out |
| `src/api/queries.ts` | the lists the Overview, the navigation and the pages share, as TanStack Query hooks, and the query keys |
| `src/api/<area>.ts` | each page group's own reads and writes |
| `src/api/session.tsx` | the session document under the gate: `useSessionDoc`, `useGrant`, `useCan` |
| `src/api/stream.ts` | one run's event stream, written into the query cache |
| `src/api/schema.d.ts` | generated from the schema; never edited by hand |
| `src/app/` | the session gate, the shell (masthead, namespace selector, user menu, navigation with counts) and the Overview's numbers |
| `src/lib/shell.ts` | `shellArg`: a value reaches a command an operator copies only shaped and single-quoted, or as a placeholder |
| `src/router.tsx` | the hash routes; each page group adds its routes in one line |
| `src/pages/<area>/`, `src/components/` | pages; shared pieces (`States.tsx`: loading, empty and error; `Status.tsx`: the one status style) |
| `src/mock/` | the demo's data and fake `fetch`, used by the mock builds and the tests; a live build drops it |
| `fixtures/` | test data both the console's rows and the Rust rows read ([README](fixtures/README.md)) |
| `e2e/`, `playwright.config.ts` | the end-to-end rows |
| `scripts/` | `check-dist.mjs`, `check-licences.mjs`, the two emitters |

## Patterns

- **Query keys:** `[kind, ns]` for a list and `[kind, ns, name]` for one object
  (`keys` in `queries.ts`). After a write, invalidate `[kind, ns]`.
- **Capabilities:** a hook whose read the session does not grant in the
  namespace stays idle, and the page leaves that part out (`useCan`). The
  navigation hides an entry the same way.
- **States:** `LoadingRows` while loading; `ErrorState` for an `ApiError`
  (headline by status, the server's message, status, code and request id, and
  Try again); `Empty` with the one action that fills the page. A request with
  no answer is `status` 0; a write whose outcome is unknown (`outcomeUnknown`:
  no answer, 408 or 5xx) keeps its `Idempotency-Key` for the retry.

## Licences

The image carries the bundled npm packages' licences (`dist/.vite/license.md`,
never served) as `/usr/share/licenses/logweir/console-third-party.md`. The Red
Hat fonts it bundles are OFL-1.1: their text is
`licenses/RedHatFont-OFL-1.1.txt`, shipped as
`/usr/share/licenses/logweir/console-fonts-OFL-1.1.txt` and printed in
`THIRD_PARTY_NOTICES.md`.

Documentation is licensed [CC-BY-4.0](../docs/LICENSE-docs).

Apache Kafka(R) and Kafka(R) are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
