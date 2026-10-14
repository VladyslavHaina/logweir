# PROD-18.1 — The rebuilt console: architecture, stack, design and migration

- Row: PROD-18.1 (impl, Tier B), [product-expansion tracker](../product-expansion.md). Owner decisions: OD-16 (rebuild the console after theme A), OD-11 (less code, one team per install), OD-8 and OD-10 (approval modes and typed confirmation, which the new screens keep).
- Date: 2026-10-13. Branch `claude/prod-18-1-design`. This record replaces the 2026-09-22 rule of a static console with no build step and no runtime dependency (PLAT-18.2). It does not change the API, the CSP or the shipped console: `ui/` still ships until step R8 of the [migration plan](#5-migration-plan).
- **Prototype:** `console/` in this branch. It is not served or shipped. It uses the recommended stack on mock data typed from `schemas/logweir-api-v1.openapi.json`. To run it: `cd console && npm ci && npm run dev`. To build it as one HTML page with no network requests: `npm run build:single`, which writes `console/dist-single/index.html`.

## 0. Summary

1. **Architecture: keep it.** One Rust service, `logweir-api`, serves the built page and the product API from one origin. The browser never talks to Kubernetes. This is the right shape for a production console. A Node server, direct calls from the browser to the Kubernetes API, and a desktop app are each worse on security, operations or both ([section 1](#1-architecture-verdict)).
2. **Stack: React 19 + TypeScript + Vite + PatternFly 6**, with TanStack Query and TanStack Router, a client typed from the OpenAPI document (openapi-typescript and openapi-fetch), react-hook-form with zod, Vitest and Playwright. PatternFly was the only candidate built for operations consoles that also ran under the existing CSP with no violations ([section 2](#2-the-stack)).
3. **Build:** a Node stage in `Dockerfile.console` runs `npm ci` and `vite build`. About 30 lines change in `assets.rs`, which also serves `assets/` (hashed files, fonts). Exact versions, a committed lockfile and `ignore-scripts`; CI runs audit and licence gates ([section 3](#3-build-and-supply-chain)).
4. **Design:** grouped navigation. An Overview a presenter can open first. A 5-step restore wizard instead of 6. A live restore page. Theme A's cutover review. One set of loading, empty and error states. Light and dark ([section 4](#4-information-architecture-and-design)).
5. **Migration:** build beside `ui/` flow by flow, then switch the image in one revertible PR. About 8 worker runs plus review rounds and one PoC pass ([section 5](#5-migration-plan)). Six open points for the owner are in [section 6](#6-open-points-for-the-owner).

## 1. Architecture verdict

**Question (the owner, 2026-10-13):** "im not sure if proxy is the right path". **Answer: yes, for this product.** The current shape is a backend-for-frontend: the console calls one bounded product API on its own origin. It is not a proxy to Kubernetes. That kind of proxy (`kubectl proxy`, `Dockerfile.ui`) was withdrawn in v1 and stays withdrawn.

| | **One origin, Rust BFF** (today) | Separate Node / Next.js server | Browser calls the Kubernetes API | Desktop app (Electron or Tauri) |
| --- | --- | --- | --- | --- |
| Who holds credentials | The service. The browser has a `__Host-` HttpOnly session cookie and an in-memory CSRF token (D0 §5) | The same, plus a second server that must forward or re-implement the session, CSRF and Origin checks | The browser holds a Kubernetes token, or a kubeconfig reaches every laptop | Every laptop holds a kubeconfig |
| What a caller can do | Only what the bounded API offers: no generic path, no Secret, Pod, log or exec access, no delete (OpenAPI `info`) | The same API behind one more hop | Whatever the user's RBAC allows. The UI is not the boundary, and per-user RBAC for Logweir's CRDs becomes the install's job | The same as the previous column |
| Approvals, audit, CSRF | Enforced once, in one place (`logweir-api`) | Split across two processes | Not enforced: the controller would see raw CR writes from anyone with RBAC | Not enforced |
| CSP | `default-src 'self'` with no nonce works for a static SPA (measured below) | SSR inline scripts need a nonce per response | `connect-src` must allow the API server | Not applicable |
| Operations | One image, one non-root binary, no Node at runtime | Two runtimes to patch (Node and Next.js have their own CVE streams), two Deployments | No server, but each user needs Kubernetes access and a reachable API server | Per-user installs and updates; no single URL to present or share |
| Fit | An operations console behind SSO; no SEO, no server rendering needed | Server rendering brings nothing to an authenticated SPA | The Kubernetes Dashboard was archived; the project now points users to Headlamp ([Kubernetes blog, 2026-06-01](https://kubernetes.io/blog/2026/06/01/dashboard-to-headlamp/)) | Fits a multi-cluster admin tool, not a team's recovery console |

**Recommendation:** keep the one-origin BFF. Only the asset serving changes: hashed files under `assets/` and a font type ([section 3.2](#32-the-rust-side-minimal)). The API, the CSP, the session and the CSRF and Origin checks stay as they are.

## 2. The stack

### 2.1 Comparison (versions and dates from the npm registry, read 2026-10-13)

Measured in this branch. A bench app with the same four elements was built with Vite 8.3.1 and React 19.3.0 (Vue 3.5.44 for Vuetify): a text input, a button, a modal dialog and a 5-row table. Each build was served with `logweir-api`'s exact CSP (`crates/logweir-api/src/http.rs`). Then the dialog was opened in Chromium.

| | **React + PatternFly 6** | React + Carbon | React + shadcn/ui (Radix + Tailwind) | Vue 3 + Vuetify 4 |
| --- | --- | --- | --- | --- |
| Built for operations consoles | Yes. The OpenShift console runs `@patternfly/react-core ~6.6.2` ([openshift/console `frontend/package.json`](https://github.com/openshift/console/blob/main/frontend/package.json)). It ships what Logweir needs: Table (sort, select, expand, row actions, stacked rows on phones), Toolbar and filters, **Wizard**, EmptyState, ProgressStepper, DescriptionList, Masthead and Nav, Alert, Label, ClipboardCopy | Strong (IBM Cloud): DataTable, Modal, ProgressIndicator. **No wizard in the core library**: the Tearsheet and create flows live in the separate "Carbon for IBM Products" package | Only primitives. The table (TanStack Table), wizard, empty and error states and the shell would be ours to build and keep | Material look: `v-data-table` and `v-stepper` work, but it reads like a consumer app |
| Accessibility | Targets **WCAG 2.2 AA**; checked with axe plus screen readers ([PatternFly accessibility](https://www.patternfly.org/accessibility/overview)). The prototype: **0 axe violations** (wcag2a/aa, 21aa, 22aa) over 45 screen checks | IBM Accessibility Checklist, WCAG AA and Section 508 ([Carbon accessibility](https://carbondesignsystem.com/guidelines/accessibility/overview/)) | Radix primitives follow WAI-ARIA; the composed screens are ours to test | Not assessed here |
| Release cadence | `react-core` 6.6.1 (2026-08-03, the `latest` tag; a 6.6.2 patch from 2026-08-26 is published untagged). A minor each quarter: 6.3 on 2025-07-22, 6.4 on 2025-10-16, 6.5 on 2026-05-20, 6.6 on 2026-07-01; 10 releases in 12 months. Red Hat | `@carbon/react` 1.118.0 (2026-10-07). A minor every two weeks (1.111 to 1.118, July to October 2026); 30 releases in 12 months. IBM | `radix-ui` 1.8.1 (2026-10-12), `tailwindcss` 4.3.3 (2026-07-16), `shadcn` CLI 4.21.4 (2026-10-07). The components are copied into our tree, so their updates are ours to merge | `vue` 3.5.44 (2026-10-13), `vuetify` 4.2.4 (2026-10-06); 65 Vuetify releases in 12 months. PrimeVue 5 moved to a commercial licence that needs a key ([`primevue@5.0.2` LICENSE.md](https://unpkg.com/primevue@5.0.2/LICENSE.md)) |
| Licence | MIT | Apache-2.0 | MIT | MIT |
| Bench JS + CSS, gzip | 121 KB + 56 KB (the base stylesheet carries every theme) | 119 KB + 18 KB (Sass, only the used components) | **82 KB + 2.4 KB** | 139 KB + 43 KB |
| Under the **existing CSP** (measured) | **0 violations**; the dialog locks page scroll | **0 violations** | **1 violation:** Radix Dialog's scroll lock injects a `<style>` (`react-remove-scroll` → `react-style-singleton`), which the CSP blocks, so the page scrolls behind the dialog. Sonner toasts and ScrollArea also inject styles | **2 violations:** Vuetify's theme stylesheet is a runtime `<style>` (`vuetify.esm.js` `upsertStyles`), so its theme colours do not apply |
| Fix for the CSP | None needed | None needed | A per-response nonce: `assets.rs` would rewrite `index.html` on each request | `theme.cspNonce` plus the same per-response nonce |
| Build | Plain CSS imports | Needs `sass-embedded`; prints Sass deprecation warnings today | Tailwind compiler plugin | Vue plugin |
| Code we write | Least: the console composes ready-made components | More: the wizard and flows are ours | Most: about 40 or more component files (shadcn's copy-in model) plus the patterns | Medium, and outside the React ecosystem (TanStack Query has a Vue port) |

**Recommendation: React + TypeScript + Vite + PatternFly 6.** It is the de facto design system for Kubernetes and OpenShift consoles, so operators already know its patterns. It has every component the flows need. It works under the CSP unchanged. It is MIT-licensed, ships on a predictable quarterly cadence and has a large maintainer. The prototype took about 4.5k lines of TypeScript for six screens, mocks included. shadcn/ui is smaller and looks distinctive, but it means owning the components (more code, against OD-11) and adding a CSP nonce. Carbon is a close second, but it lacks a wizard in its core library and needs a Sass build.

### 2.2 The rest of the stack (exact versions pinned in `console/package.json`)

| Concern | Choice | Why |
| --- | --- | --- |
| Language | TypeScript **5.9.3**, `strict`, `noUncheckedIndexedAccess` | `openapi-typescript` 7.13.0 declares `typescript ^5.x`. TypeScript 7.0.2 (native compiler, 2026-07-08) can follow once it is supported |
| Framework | React **19.3.0** | PatternFly 6.6 accepts React 17–19 |
| Build | Vite **8.3.1** (Rolldown) and `@vitejs/plugin-react` 6.1.1 | Hashed file names, static CSS files, no inline script in `index.html`, `base: "./"` so the page works under `/ui/` |
| Design system | `@patternfly/react-core`, `-table` and `-icons` **6.6.1** | Section 2.1. Icons are deep-imported per icon, so only the used ones ship |
| Routing | TanStack Router **1.170.39**, **hash history** | Typed path and search parameters. The namespace is in every address (`#/team-a/backups/<name>`). Hash routes need no server change, and the sign-in round trip already carries `next=/ui/#/...` (`crates/logweir-api/src/auth/login.rs`) |
| Server state | TanStack Query **5.104.0** | Caching, refetch on focus, re-reading only while a run is in progress, invalidation after a write. The operation stream (`EventSource`) writes into the same cache |
| API client | `openapi-typescript` **7.13.0** (types) and `openapi-fetch` **0.17.0** (client) | The generated `src/api/schema.d.ts` is committed. `npm run check:api` fails when it drifts from the OpenAPI document; it must run with `--default-non-nullable false`, or optional request fields become required. One middleware adds `X-CSRF-Token` on unsafe methods. The prototype's mock is a `fetch` handed to the same client, so pages, cache and client are the real ones |
| Forms | react-hook-form **7.89.0**, zod **4.6.5**, `@hookform/resolvers` **5.9.1** | Validation per wizard step (`trigger(fields)`). Under the CSP, zod needs `z.config({ jitless: true })` (`src/csp.ts`); otherwise its `new Function` probe is reported as a violation (measured) |
| Charts | None | The Overview's run strip is CSS. If trend charts are wanted later: `@patternfly/react-charts` 8.6.1 ([open point 6](#6-open-points-for-the-owner)) |
| Tests | Vitest and Testing Library (components, flows on mocks); Playwright (end-to-end, with axe-core and the CSP row) | [Section 5.2](#52-which-tests-carry-over) |

### 2.3 Measured on the prototype

- `npx tsc --noEmit`: rc 0.
- `vite build` output: one JS file of 952 KB (283 KB gzip), one CSS file of 1,401 KB (117 KB gzip), and the woff2 fonts the CSS names. A page view loads three fonts, about 111 KB.
- **The normal build under the real CSP, served at `/ui/`:** 0 CSP violations and 0 console errors across Overview, Backups, the wizard (the plan hash came from `crypto.subtle` on 127.0.0.1) and the cutover. Every request stayed on `/ui/` and `/ui/assets/`.
- **The single-file build** (`console/dist-single/index.html`, 2,709,505 bytes): walked by Playwright at 1440 px and 390 px, light and dark, 60 screens. 0 requests other than the page, 0 console errors or warnings, 0 screens wider than the viewport, 0 axe violations.
- **On Linux:** `npm ci && npm run check:api && tsc && npm run build` in `node:22-bookworm-slim` (linux/arm64, node 22.23.3, npm 10.9.9) passed. The hashed file names matched the macOS build (`index-BKIF1TAk.css`, `index-BG_C81Tb.js`), so the build is reproducible across hosts.

## 3. Build and supply chain

### 3.1 The image

`Dockerfile.console` gains one stage. The compiled Rust stage is unchanged.

```dockerfile
FROM --platform=$BUILDPLATFORM node:22-bookworm-slim@sha256:<pinned> AS console
WORKDIR /src/console
COPY console/package.json console/package-lock.json console/.npmrc ./
RUN npm ci
COPY schemas/logweir-api-v1.openapi.json /src/schemas/
COPY console/ ./
RUN npm run check:api && npm run build
# runtime stage: replaces `COPY ui/*.html ui/*.js ui/*.css /ui/` and `COPY ui/pages /ui/pages`
COPY --from=console /src/console/dist /ui
```

- The output is platform-independent JavaScript and CSS, so the stage runs on `$BUILDPLATFORM`. There is no emulation, so standing rule 10 holds.
- The Node image is pinned by digest. It moves when the Rust base images move.
- `.dockerignore` gains `/console/node_modules`, `/console/dist` and `/console/dist-single` (done in this branch), so the Rust builder's `COPY . .` never takes a local install of about 380 MB.

### 3.2 The Rust side, minimal

About 30 lines in `crates/logweir-api/src/assets.rs`, plus its tests:
- also collect `assets/`, one level deep, under the same exact-path lookup and `safe_name` rule (no directory index, no decoding);
- add the types `woff2` (`font/woff2`) and `svg` (`image/svg+xml`), for files under `assets/` only. The compiled-in `favicon.svg` stays at the top level, so nothing can shadow it;
- serve `assets/*` with `Cache-Control: public, max-age=31536000, immutable`, since the names carry a content hash. `index.html` keeps `no-cache`;
- drop the served `runtime.js`. The new console is always behind `logweir-api` and learns its state from `GET /api/v1/session`;
- keep `MAX_ASSET_BYTES` at 8 MiB. The largest file is 1.4 MB;
- leave the CSP, the Permissions-Policy and the routes untouched.

`scripts/check-image-api.sh` check 1 then compares the image's `/ui` with `console/dist` instead of `ui/`. The file count is no longer 26.

### 3.3 Rules

- **Pinned:** exact versions in `package.json` (`save-exact=true`). A committed `package-lock.json` (lockfile v3; every entry resolves to `registry.npmjs.org` and carries an integrity hash). `.npmrc` names the public registry. CI and the image use only `npm ci`.
- **No install scripts:** `ignore-scripts=true` in `console/.npmrc`. The build works without them (verified on macOS and Linux).
- **CI job `console`:** `npm ci`, `npm run check:api`, `tsc --noEmit`, `vitest run`, `vite build`, `npm audit --audit-level=high` over every dependency (0 found today), and a licence allowlist over `npm ls --all --json`:
  - shipped code: MIT, ISC, BSD-2-Clause, BSD-3-Clause, Apache-2.0, 0BSD, Unlicense. Today's runtime graph is 38 MIT, 1 0BSD (`tslib`) and 1 Unlicense (`isbot`, which the bundle tree-shakes away);
  - build-only packages may also be MPL-2.0 (`lightningcss`) or Python-2.0 (`argparse`). Neither reaches the image;
  - the fonts are SIL OFL 1.1 ([RedHatOfficial/RedHatFont](https://github.com/RedHatOfficial/RedHatFont)).
  - `THIRD_PARTY_NOTICES.md` gains a generated section for the runtime npm packages and the fonts, and `doc_lint`'s inventory row learns to read it.
- **Updates:** Dependabot for the `npm` ecosystem in `console/`, weekly and grouped (PatternFly, TanStack, build tools), with a 14-day cooldown except for security fixes. PatternFly minors are taken each quarter. A React major is taken only when PatternFly supports it.
- **Avoided on purpose:** `vite-plugin-singlefile`. Its `micromatch` → `braces` chain carries [GHSA-vfj7-8cjw-p6xm](https://github.com/advisories/GHSA-vfj7-8cjw-p6xm) (high, published 2026-09-18, no patched version), so the demo build inlines its own files with 30 lines in `console/vite.config.ts`.
- **What replaces `ui/`'s gates** (`ui_lint.rs`, `check-ui-offline.sh`) once `ui/` is retired:
  - ESLint `no-restricted-syntax` and `no-restricted-globals`: no `dangerouslySetInnerHTML`; `fetch` and `EventSource` only in `src/api/`; no `localStorage`, `sessionStorage` or `document.cookie`;
  - the Playwright CSP row from section 2.3: the built image with the real headers, failing on any CSP violation or any request off the origin;
  - the plan goldens (section 5.2).

## 4. Information architecture and design

### 4.1 Navigation and pages

The masthead holds the brand, the **namespace selector** and the user menu. The user menu shows the name, the roles in this namespace, **Appearance** (System, Light, Dark) and Sign out. A left navigation, grouped by what the operator is doing, gives every item an icon and shows counts where they help: needs attention on Overview, failed backups in the last 24 hours on Backups, and requests waiting for you on Approvals (only a two-person namespace has any). On a wide screen it collapses to an icon rail; below 1200 px it is a drawer with a backdrop that Escape closes. Capability flags from `/session` hide what the role cannot use, as today.

| Group | Page (route under `#/<ns>/`) | Primary action | Replaces |
| --- | --- | --- | --- |
| — | **Overview** (`""`) | Restore; Back up now | none (new) |
| Protect | Backups (`backups`, `backups/<name>`) | Restore this point | `#/backups`, `#/history` (backup half) |
| | Schedules (`schedules`, `schedules/<name>`) | Create a schedule; Back up now; Suspend | `#/schedules` |
| | Protection (`protection`, `protection/<name>`) | none | `#/protection` |
| Recover | Restores (`restores`, `restores/<name>`) | Restore a point | `#/history` (restore half), `#/operations` |
| | the wizard (`restores/new?backup=` or `?point=`) | Create restore | `#/restore` |
| | Cutover (`restores/<name>/cutover`) | Switch over | none (theme A) |
| | Approvals (`approvals`, `approvals/<name>`), with a badge for requests waiting on you | Approve | `#/approvals` |
| Evidence | Recovery catalog (`catalog`, `catalog/<name>`) | Restore this point; Connect a catalog | `#/catalog` |
| | Trust (`trust`) | none (read-only) | `#/keys` |
| Settings | Connections (`connections`, `connections/<name>`) | Add a connection; Check; Discover topics | `#/clusters` |
| | Destinations (`destinations`, `destinations/<name>`) | Add; Test access; Rotate access | `#/destinations` |

### 4.2 The main flows

1. **First connection to first backup.** An empty namespace's Overview shows **"Get `<ns>` protected"**: four steps, each with the button that does it (prototype screen 02). Connections → Add (the credential is typed once and write-only) → Check → Discover topics. Destinations → Add → Test access. Schedules → guided create (topics from the discovery, a cadence preview, retention) → Back up now. The run appears on Backups and turns **Verified** when the controller has verified its receipt.
2. **Restore, five steps** (screens 05–09). The old wizard had six; archive and recovery point are one choice here.
   1. Recovery point: a table of verified points, newest first, preselected from "Restore this point".
   2. Topics and time: the topics, and the end of the point or an earlier moment inside its window.
   3. Target: target-role clusters only, with reachability; new topics with a prefix, or the original names; optional automatic clean-up (a clone, theme A); and the **topic configuration preview** (theme A, PROD-05.2).
   4. Readiness check: a real Preflight. Next is disabled until it passes.
   5. Review and create: the summary, the plan hash, "show the plan", which approval mode applies, and OD-10's typed names for an original-name restore.

   **Create restore** opens the restore's page.
3. **Verify** (screens 10–11). The restore page shows a stepper (Admitted, Preparing, Restoring, Verifying, Done), a records bar, the activity log, and then "Restored and verified" with the sample counts and the controller-verified scorecard.
4. **Approvals** (OD-8). One-person namespace: creating the restore confirms it. Two-person: the restore page says it waits for a second person; the approver's Approvals page lists it and its request page shows the scope, plan and typed names, then **Approve**. Strict: the CLI commands and a text upload, which refuses anything that looks like a private key, as today.
5. **Theme A cutover** (screens 12–14), shown on a verified restore as **Switch consumers over**.
   1. **Review positions.** Each consumer group shows per-partition translations: **Exact**, **Approximate** (the partition end) or **Unavailable** (with the reason). It also shows whether consumers are still connected on the target, and a verdict: Ready, Stop consumers first, or Blocked. Blocked groups are left out, never guessed. "Check again" re-reads the target.
   2. **Confirm.** The groups, the positions committed (compare-and-set, `expected: none`), positions saved first, groups left out with their reason, the approval mode (a switchover follows the namespace's mode, OD-17), the plan hash and plan, and the undo limit.
   3. **Switched over.** Per-group commit progress and the switchover record.

   The topic configuration (PROD-05.2) is chosen in the wizard, with the restore preflight's per-topic preview, and its transition runs after the verdict; the restore page shows `Restore.status.topicConfiguration` before and after. A clone (scratch mode with a TTL) shows "deletes in 23 hours" on Restores and `status.clone.expiresAt` and its clean-up on its page. The prototype's `src/mock/themeA.ts` follows the shapes in `decisions/PROD-04.2-cutover.md`; the build replaces it with the theme A API.

### 4.3 The Overview (what a presenter opens first)

The Overview has four numbers, then four cards:
- **The numbers:** policies meeting their objective; the newest recovery point's age; verified and failed backups in the last 24 hours; items needing attention.
- **Protection policies:** each policy's age-against-objective bar.
- **Needs attention:** an unreachable connection, an unprotected policy, a failure fixed by a retry, each with one link.
- **Recent runs per schedule:** a 24-run strip; each cell is a link with a spoken label.
- **Recent activity:** the latest backups and restores.

A presenter's path through it: Overview → Backups → a backup → Restore this point → five steps → the live restore → Switch consumers over.

### 4.4 States, live updates, theme, widths, words

- **Loading:** skeleton rows in place of the content.
- **Empty:** what the page is for, and the one action that fills it.
- **Error:** a headline chosen by status (not found, no access, the service had a problem), then the server's message verbatim, then the status, the code and the request id, and Try again. A write with an unknown outcome keeps today's rule: "it may or may not exist; submitting again is safe" (same `Idempotency-Key`). Signed out, unavailable and no-role namespace each have their own full-page state, as in `ui/README.md`.
- **Live updates:** one `EventSource` per open operation (`/operations/{kind}/{name}/events`) writes into the query cache. Lists re-read every 1.5 s only while a run in them is in progress, and otherwise on focus. The prototype re-reads its mock.
- **Theme:** PatternFly's light and dark themes with Logweir's accent (`#0b5cad`) set through PatternFly's brand tokens, and four surface levels (masthead, sidebar, page, card) plus raised menus, in `src/styles.css`. In dark mode the accent stays strong (white on it is 4.8:1). One status style everywhere: an outline label with an icon and a light tint. It follows the system; the user menu's Appearance choice overrides it for the life of the page, with no browser storage ([open point 5](#6-open-points-for-the-owner)). PatternFly's high-contrast theme is available later at no cost.
- **Widths:** checked at 1440 px and 390 px with no horizontal scroll. On a phone, lists become compact rows (name, status, time; the rest on the item's page), and the wizard's step list becomes a drop-down. Run-strip cells are at least 24 px, the WCAG 2.2 target size (found by axe on the prototype and fixed).
- **Words:**
  - status is always a word plus an icon plus a colour;
  - "Verified" only when the controller verified signed evidence, never "verified in your browser";
  - times are relative, with the exact UTC instant on hover, and exact on detail pages;
  - a missing value is "Not recorded", never "-";
  - long identifiers are shortened, with copy;
  - internal reason codes are secondary text only;
  - one primary action per page; a restore under the original names asks for typed confirmation (OD-10);
  - the button says what happens: "Create restore", "Switch over 2 groups".

## 5. Migration plan

### 5.1 Order (one worker run each, unless noted)

New code lives in `console/` beside `ui/`. During development it runs with `vite dev`, proxying `/api` to a local `cargo run -p logweir-api`. The image keeps serving `ui/` until R8, and there is no switch in the chart (OD-11). The mock layer stays for component tests and offline development; shipped code never imports it.

| Run | Scope |
| --- | --- |
| R1 | Foundation: `assets.rs` (section 3.2); the `Dockerfile.console` stage; `.dockerignore`; the CI `console` job with its audit and licence gates; the session gate (signed out, unavailable, no role); the shell; the shared states; the client and the `EventSource` hook; the Overview on real data |
| R2 | Connections and Destinations: create, write-only credentials, check, topic discovery, access test and rotation |
| R3 | Schedules (guided create, cadence preview, policy edit, suspend, Back up now, retention) and Protection |
| R4 | Backups, the Recovery catalog (points, connect a catalog) and Trust |
| R5 | The restore wizard and restore page: plan bytes and hash equal the goldens; Preflight; original names; partition subsets and complete coverage; retry to a fresh target |
| R6 | Approvals: one-person, two-person Approve, strict upload |
| R7 | Theme A screens (cutover, configuration preview and transition, clones) against the theme A API |
| R8 | Switch: the image serves `console/dist`; retire `ui/` and its gates; port the end-to-end harness; update `ui/README.md` (it becomes `console/README.md`), `docs/kubernetes.md` and `docs/install.md`; PoC pass with visible clicks (PROD-18.1's acceptance) |

**Estimate:** 8 worker runs, about 4 review or fix rounds (Tier B, one checklist pass each; R1 and R6 touch serving and approvals, so they get the closer read), and 1 PoC pass. Expect the console at well under half of `ui/`'s 41k lines. The wizard went from 7,941 lines to 765 in the prototype, without the subset and catalog paths.

**Rollback:** until R8 nothing shipped changes. R8 is one PR that changes only what the image serves, so a revert restores `ui/`.

### 5.2 Which tests carry over

- **`ui/tests/` (57 spec files, about 980 rows)** test hand-written DOM helpers and decoders, and most go with that code. What carries over, rewritten as Vitest rows over the new components:
  - the plan goldens: `ui/tests/fixtures/plan-*.golden.yaml` move to `console/`, and `ui_lint.rs`'s check that each parses into the runner's `RestoreSpec` stays;
  - one click makes one object (idempotency keys);
  - the unknown-outcome wording;
  - the green-badge rules for Backup and Restore (`pages/backups.js`, `pages/history.js` headers);
  - the approvals upload's refusal of private keys and key-shaped file names;
  - the original-name typed confirmation (OD-10);
  - the replication-factor note;
  - the capability-flag routing;
  - the schema fixtures under `ui/tests/fixtures/console/`, which feed both the component tests and the mock.
- **`contract-coverage.spec.js` and `contract.js`'s decoders** are replaced by the generated types, the `check:api` drift gate, and a test that type-checks every fixture against the generated schema. The decoders' runtime refusal of a wrong enum value keeps one guard: a zod schema for the few documents whose states drive a badge (OperationSummary, ProtectionPolicy health).
- **End-to-end:** `scripts/prod16-1-ui-e2e.mjs` and `scripts/console-steps.mjs` drive the six-step wizard by element ids. Its offline guard is `scripts/console_steps.py`. They become `console/e2e/*.spec.ts` (`@playwright/test`), using role locators against compose or the PoC, plus axe on each screen and the CSP row. `console_steps.py` is retired, because role locators and TypeScript replace what it guarded.

## 6. Open points for the owner

**Decided 2026-10-13** (OD-16 amended): every point as recommended.

| # | Question | Options | Recommendation |
| --- | --- | --- | --- |
| 1 | The design system | (a) PatternFly 6; (b) shadcn/ui, more distinctive, with more code to own and a CSP nonce; (c) Carbon | **(a)** |
| 2 | The look | (a) PatternFly's default blue and Red Hat fonts (shipped as files, OFL 1.1); (b) Logweir's own accent colour (the favicon's `#0b5cad`) through PatternFly's brand tokens, with the same fonts | **(b)**: one token, no new design work |
| 3 | Addresses | (a) keep hash routes (`/ui/#/team-a/backups`); (b) path routes, with `assets.rs` returning `index.html` for unknown `/ui/*` paths | **(a)**: no server change, and sign-in already returns to `#` routes. In-page anchors move focus themselves, as the skip link does |
| 4 | When | (a) after theme A, as decided; (b) R1 (foundation, no theme A file) alongside theme A's single test pass | **(b)** if a worker slot is free; R2 onwards after theme A |
| 5 | Remember the theme choice | (a) follow the system, with the switch lasting for the page's life (no browser storage, as today); (b) one `localStorage` key | **(a)** |
| 6 | Trend charts on the Overview | (a) none, as in the prototype; (b) `@patternfly/react-charts` (a charting library in the bundle) | **(a)** for v1 of the console |

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
