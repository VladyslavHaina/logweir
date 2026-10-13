# The rebuilt Logweir console

Developer notes for `console/`, the console being rebuilt under PROD-18.1
([decision record](../docs/to-do/decisions/PROD-18.1-console.md)). It is React,
TypeScript, Vite and PatternFly 6, with a client typed from
`schemas/logweir-api-v1.openapi.json`.

The image serves it at **`/ui/next/`**, beside the current console at `/ui/`
(`ui/`, unchanged). It is built page by page: the Overview runs on the product
API now. Every other page shows "Not in the new console yet" and links to the
same page in the current console. Step R8 moves it to `/ui/` and retires `ui/`.

## Running it

Use Node 22 or 24. `npm ci` installs exactly `package-lock.json`, with install
scripts off (`.npmrc`).

- **On the mock:** `npm ci && npm run dev`. Every request goes to the in-page
  mock (`src/mock/`), and nothing leaves the page.
- **On a real API:** start `logweir-api` on `127.0.0.1:8484` as
  [docs/api.md](../docs/api.md) *Running it* describes, then run
  `VITE_LOGWEIR_API=live npm run dev`. Vite proxies `/api` and `/auth` to it,
  with the API's own `Host` and `Origin`.
- **As the image serves it:** `npx vite build` writes `dist/`. Copy it to
  `<uiDirectory>/next/` and open `/ui/next/` on the API.

## Building it

| command | output | talks to |
|---|---|---|
| `npx vite build` (the image's build) | `dist/`: `index.html` and hashed files under `assets/` | the product API on the page's own origin |
| `npm run build:single` | `dist-single/index.html`, everything inlined, for demos | the in-page mock |

`Dockerfile.console`'s `console` stage runs `npm ci`, `npm run check:api`,
`npx vite build` and `scripts/check-dist.mjs`, then copies `dist/` to
`/ui/next` and the bundled packages' licences (`dist/.vite/license.md`, never
served) to `/usr/share/licenses/logweir/console-third-party.md`. The Red Hat
fonts it bundles are OFL-1.1: their text is `licenses/RedHatFont-OFL-1.1.txt`,
shipped as `/usr/share/licenses/logweir/console-fonts-OFL-1.1.txt` and printed in
`THIRD_PARTY_NOTICES.md`. `scripts/check-image-api.sh` holds the image's `/ui/next` to this
tree's `dist/`, sha256 for sha256, so run `just console` before
`just smoke-console`.

## Testing it

`just console` runs every gate, the same as CI's `console` job, which runs when
`console/`, `ui/tests/fixtures/console/` or the schema changes; `publish` waits
for it:

1. `npm ci`;
2. `npm run check:api`: the committed `src/api/schema.d.ts` matches the schema
   (`npm run gen:api` regenerates it);
3. `tsc --noEmit`;
4. ESLint (`npm run lint`), with three rules of our own: no
   `dangerouslySetInnerHTML`, `fetch` and `EventSource` only in `src/api/`, and
   no browser storage or `document.cookie`;
5. Vitest (`npm test`), on jsdom;
6. `vite build`, then `scripts/check-dist.mjs`: the output has the shape
   `/ui/next/` serves, and none of the mock;
7. `npm audit --audit-level=high`;
8. `scripts/check-licences.mjs`: each package's licence from the lockfile is on
   the allowlist (shipped code, or build tools).

Dependabot proposes npm updates weekly, grouped, once a release is 14 days old
(`.github/dependabot.yml`).

## Where things live

| path | what it is |
|---|---|
| `src/api/client.ts` | the one client: same origin, the cookie session, `X-CSRF-Token` on unsafe methods; `ApiError` (with the problem's field errors); `outcomeUnknown`; `mintKey`; `listAll` (every page of a list); the bounded session read; sign-in and sign-out |
| `src/api/queries.ts` | the lists the Overview, the navigation and the pages share, as TanStack Query hooks, and the query keys |
| `src/api/<area>.ts` | each page group's own reads and writes (connections, destinations, schedules, backups with the catalog and trust) |
| `src/api/session.tsx` | the session document for everything under the gate: `useSessionDoc`, `useGrant`, `useCan` |
| `src/api/stream.ts` | one run's event stream, written into the query cache |
| `src/api/schema.d.ts` | generated from the schema; never edited by hand |
| `src/app/SessionGate.tsx` | the boot read of `/api/v1/session` and its four pages: console, sign in, unavailable, no namespace |
| `src/app/Shell.tsx` | the masthead, the namespace selector, the user menu, and the navigation with counts |
| `src/app/attention.ts`, `src/pages/overviewData.ts` | the Overview's numbers, as pure functions of the API's documents |
| `src/router.tsx` | hash routes, `#/<namespace>/<page>`; each page group adds its routes in one line; which pages are still prototype-only |
| `src/pages/`, `src/components/` | pages; shared pieces (`States.tsx`: loading, empty and error) |
| `src/mock/` | the prototype's data and fake `fetch`, used by the mock builds and the tests; a live build drops it |
| `scripts/` | `check-dist.mjs`, `check-licences.mjs` |

## Patterns for the pages still to build

- **Query keys:** `[kind, ns]` for a list and `[kind, ns, name]` for one
  object (`keys` in `queries.ts`). After a write, invalidate `[kind, ns]`.
- **Capabilities:** a hook whose read the session does not grant in the
  namespace stays idle, and the page leaves that part out (`useCan`). The
  navigation hides an entry the same way, and the shell answers a page whose
  read the role lacks with "Not available to you here".
- **States:** `LoadingRows` while loading; `ErrorState` for an `ApiError`
  (headline by status, the server's message, status, code and request id, and
  Try again); `Empty` with the one action that fills the page. A request with
  no answer is `status` 0; a write whose outcome is unknown (`outcomeUnknown`:
  no answer, 408 or 5xx) keeps its `Idempotency-Key` for the retry.
- **Making a page live:** drop its `PROTOTYPE ?` in `src/router.tsx`, read
  through `queries.ts`, and replace anything it takes from `src/mock/`.

Documentation is licensed [CC-BY-4.0](../docs/LICENSE-docs).

Apache Kafka(R) and Kafka(R) are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
