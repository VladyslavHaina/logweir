// The PoC defect re-proof after the upgrade to the fixed publication (poc-upgrade-1): each row
// drives the deployed shared console in a real Chromium through Traefik and Dex and REQUIRES the
// outcome it records. Row groups (the fix reports' "Rows for the PoC re-proof"):
//
//   P1        masthead and colophon name the product API, no kubeconfig/kubectl proxy (console-shared-fix 3)
//   P4        Catalog -> Connect an existing archive: X-CSRF-Token + Idempotency-Key, 201; a double click
//             sends one POST; a lost answer resubmitted is `replayed`; the viewer is refused by capability (1)
//   P2        the wizard's point panel and namespace table: green + "manifest attested" exactly where the
//             API says verifiedSuccess, "unverified: ..." + "no manifest recorded" elsewhere (2)
//   SWEEP     a Succeeded restore's History scope line (5); schedule facts' creation-instant label (6);
//             the retention sentence with no RetentionPolicy (7)
//   P6        the catalog page's sync-mode help (legacy-point-restore L4)
//   COMPLETION <restore>  a finished-without-success restore has no completion section; a Succeeded one does (4)
//   P7        Clusters: no name input in console mode, the minted-name sentence, source+target created as
//             conn-..., each one's role under its name (P7LIST re-reads only that); a double click creates exactly one (poc-fixes-2 R7.1, R7.2)
//   SESSION   signed out = the one Sign in page, the Dex round trip back to the asked address; the header
//             (name, role in the namespace, Sign out) per role; tabs by role; Sign out (console-ux-1 MCP-1/5/33)
//   P8        Destinations -> primary -> Test access settles (R8.1), a second test is a new check (R8.2),
//             a reload shows the recorded test (R8.4)
//
//   NODE_PATH="$(npm root -g)" node scripts/live/poc/reproof.mjs <outdir> <GROUP>[,<GROUP>...] [args]
//
// Passwords come from the credentials file and are typed into Dex; nothing secret is printed.
import { writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import {
  chromium, newSession, gotoHash, textOf, waitForText, connectCatalog, createCluster, chooseCatalogDestination, BASE, HOST, credential,
  openWizard, wizardAt, wizardStep, readinessRows, showEveryPoint, listRow, revealInGrid, settledRows, checkVerdict, outcomeOf,
} from "./console.mjs";

const OUT = process.argv[2] || "/tmp/poc-reproof";
const GROUPS = (process.argv[3] || "P1").split(",");
const ARG = process.argv[4] || "";
const NS = process.env.POC_NAMESPACE || "logweir-poc";
mkdirSync(OUT, { recursive: true });
const ROWS = [];
const log = (m) => console.log(new Date().toISOString(), m);
function row(id, ok, evidence) {
  ROWS.push({ id, pass: !!ok, evidence });
  log(`${ok ? "PASS" : "FAIL"} ${id} ${JSON.stringify(evidence).slice(0, 600)}`);
  writeFileSync(`${OUT}/rows-${GROUPS.join("_")}.json`, JSON.stringify(ROWS, null, 1));
}
const kj = (...args) => JSON.parse(execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=30s", "-n", NS, ...args, "-o", "json"], { timeout: 45000, maxBuffer: 256 * 1024 * 1024 }).toString());
async function shot(page, name) { await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: true }); }
async function session(page) { return page.evaluate(async () => (await fetch("/api/v1/session", { credentials: "same-origin" })).json()); }
async function apiGet(page, path) {
  return page.evaluate(async (p) => { const r = await fetch(p, { credentials: "same-origin" }); return { status: r.status, body: await r.json().catch(() => null) }; }, path);
}
async function allBackups(page) {
  const out = [];
  let cursor = null;
  for (let i = 0; i < 20; i++) {
    const r = await apiGet(page, `/api/v1/namespaces/${NS}/backups?limit=200${cursor ? "&cursor=" + encodeURIComponent(cursor) : ""}`);
    out.push(...((r.body || {}).items || []));
    cursor = (((r.body || {}).page) || {}).nextCursor || null;
    if (!cursor) break;
  }
  return out;
}

const browser = await chromium.launch();
try {
  const { page, context } = await newSession(browser, "operator");
  // ------------------------------------------------------------------ P1
  if (GROUPS.includes("P1")) {
    await gotoHash(page, `#/schedules?ns=${NS}`);
    const t = await page.locator("#masthead-tagline").innerText();
    const c = await page.locator("#colophon-serving").innerText();
    row("P1 the shared console's masthead names the product API and no kubeconfig",
      /Every request goes to the Logweir product API, which authorises it for this session's identity and namespace grants/.test(t) && !/kubeconfig|kubectl proxy/i.test(t), { tagline: t });
    row("P1 the colophon reads 'Served by logweir-api', no 'kubectl proxy'", /^Served by logweir-api, which authorises every request/.test(c) && !/kubeconfig|kubectl proxy/i.test(c), { colophon: c });
    await shot(page, "P1-masthead");
  }
  // ------------------------------------------------------------------ SESSION (console-ux-1 MCP-1/4/5/33, its round-2 rows 1-2)
  // Signed out: every route is the one Sign in page (no tabs, no namespace box, no JSON), whose link
  // returns to the address asked for through Dex. Signed in: the header names the person, their role
  // in the namespace, and Sign out; the tabs follow the role. Sign out is a POST carrying the
  // session's own token (compared at capture, never kept) and leaves the page signed out.
  if (GROUPS.includes("SESSION")) {
    const want = `#/backups?ns=${NS}`;
    const anon = await browser.newContext({ ignoreHTTPSErrors: true, viewport: { width: 1440, height: 1000 } });
    const ap = await anon.newPage();
    await ap.goto(`${BASE}/ui/${want}`, { waitUntil: "domcontentloaded" });
    await ap.waitForSelector("#sign-in", { timeout: 30000 }).catch(() => {});
    const so = await ap.evaluate(() => ({
      card: !!document.querySelector("#sign-in"), h2: (document.querySelector("#sign-in h2") || {}).innerText || "",
      href: (document.querySelector("#sign-in-link") || { getAttribute: () => null }).getAttribute("href"),
      // textContent, not innerText: the button is styled uppercase, and innerText applies text-transform
      signInButtons: [...document.querySelectorAll("a, button")].filter((x) => /^\s*sign in\s*$/i.test(x.textContent)).length,
      tabs: document.querySelectorAll("#nav-slot .nav-link").length, text: document.body.innerText }));
    row("SESSION signed out: the Sign in page -- one Sign in, no tabs, no namespace prompt, no JSON; its link returns to the asked address",
      so.card && /^Sign in to Logweir$/.test(so.h2.trim()) && so.signInButtons === 1 && so.tabs === 0 && !/Choose a namespace/.test(so.text) && !/[{]\s*"/.test(so.text)
        && so.href === "/auth/login?next=" + encodeURIComponent("/ui/" + want),
      { h2: so.h2, href: so.href, signInButtons: so.signInButtons, tabs: so.tabs });
    await shot(ap, "SESSION-signed-out");
    const { user, pw } = credential("operator");
    await ap.click("#sign-in-link");
    await ap.waitForSelector('input[name="login"]', { timeout: 30000 });
    await ap.fill('input[name="login"]', user);
    await ap.fill('input[name="password"]', pw);
    await Promise.all([ap.waitForURL((u) => u.hostname === HOST && u.pathname.startsWith("/ui"), { timeout: 30000 }), ap.click('button[type="submit"]')]);
    await ap.waitForSelector("#session-identity", { timeout: 30000 }).catch(() => {});
    const back = new URL(ap.url());
    row("SESSION the Dex round trip returns to the same address", back.pathname === "/ui/" && back.hash === want, { url: back.pathname + back.hash });
    await anon.close();
    const TABS = {};
    for (const role of ["operator", "approver", "viewer", "admin"]) {
      const s = await newSession(browser, role);
      await gotoHash(s.page, `#/backups?ns=${NS}`);
      await s.page.waitForSelector("#session-identity", { timeout: 30000 }).catch(() => {});
      const h = await s.page.evaluate(() => ({
        name: (document.querySelector("#session-identity .session-name") || {}).innerText || "",
        role: (document.querySelector("#session-role") || {}).innerText || "",
        signOut: !!document.querySelector("#sign-out"),
        tabs: [...document.querySelectorAll("#nav-slot .nav-link")].map((a) => a.innerText.trim()) }));
      const sess = await session(s.page);
      TABS[role] = h.tabs;
      const shown = (sess.actor || {}).displayName || "";
      row(`SESSION header (${role}): the person's name, '${role} in ${NS}', and Sign out`,
        h.name.length > 0 && (!shown || h.name === shown) && new RegExp(`\\b${role === "admin" ? "admin(istrator)?" : role}\\b.* in ${NS}$`).test(h.role) && h.signOut,
        { name: h.name, apiDisplayName: shown, role: h.role, signOut: h.signOut, tabs: h.tabs });
      await shot(s.page, `SESSION-header-${role}`);
      await s.context.close();
    }
    row("SESSION tabs follow the role: only admin has Keys; approver has Approvals; operator has Restore; viewer has no Restore",
      TABS.admin.includes("Keys") && !TABS.operator.includes("Keys") && !TABS.approver.includes("Keys") && !TABS.viewer.includes("Keys")
        && TABS.approver.includes("Approvals") && TABS.operator.includes("Restore") && !TABS.viewer.includes("Restore"), TABS);
    // Sign out, as operator
    const s = await newSession(browser, "operator");
    await gotoHash(s.page, `#/backups?ns=${NS}`);
    await s.page.waitForSelector("#sign-out", { timeout: 30000 });
    const sess = await session(s.page);
    const outs = [];
    s.page.on("request", (r) => { if (r.method() === "POST" && /\/api\/v1\/session\/logout$/.test(new URL(r.url()).pathname)) outs.push({ sessionToken: r.headers()["x-csrf-token"] === sess.csrfToken }); });
    const answers = [];
    s.page.on("response", (r) => { if (r.request().method() === "POST" && /\/api\/v1\/session\/logout$/.test(new URL(r.url()).pathname)) answers.push(r.status()); });
    await s.page.click("#sign-out");
    await s.page.waitForSelector("#sign-in", { timeout: 30000 }).catch(() => {});
    const after = await s.page.evaluate(async () => (await fetch("/api/v1/session", { credentials: "same-origin" })).status);
    const card = await s.page.locator("#sign-in").count();
    row("SESSION Sign out: one POST /api/v1/session/logout with the session's own token, answered 2xx; the page is the Sign in page and the session is gone (401)",
      outs.length === 1 && outs[0].sessionToken === true && answers.length === 1 && answers[0] >= 200 && answers[0] < 300 && card === 1 && after === 401,
      { posts: outs, answers, signInCard: card, sessionAfter: after });
    await shot(s.page, "SESSION-signed-out-after");
    await s.context.close();
  }
  // ------------------------------------------------------------------ P6 / L4
  if (GROUPS.includes("P6")) {
    await gotoHash(page, `#/catalog?ns=${NS}`);
    await waitForText(page, /Connect an existing archive/, 60, "the catalog page");
    const help = await page.locator("#catalog-mode-help").innerText();
    row("P6/L4 the catalog sync-mode help says Full reads catalog records only and names logweir catalog sync",
      help.includes("Full rescans every catalog record under logweir/catalog/v1/points/") && help.includes("logweir catalog sync") && !/Full walks the receipts and manifests/.test(help), { help });
  }
  // ------------------------------------------------------------------ P4
  if (GROUPS.includes("P4")) {
    const posts = [];
    page.on("request", (r) => { if (r.method() === "POST" && /\/catalogs$/.test(new URL(r.url()).pathname)) posts.push({ headers: r.headers(), at: Date.now() }); });
    const answers = [];
    page.on("response", async (r) => { if (r.request().method() === "POST" && /\/catalogs$/.test(new URL(r.url()).pathname)) answers.push({ status: r.status(), body: await r.json().catch(() => null) }); });
    const sess = await session(page);
    const name = `poc-reproof-${Date.now().toString(36).slice(-5)}`;
    await gotoHash(page, `#/catalog?ns=${NS}`);
    await waitForText(page, /Connect an existing archive/, 60, "the catalog page");
    const form = page.locator("form[data-connect-archive]");
    await form.locator('input[name="name"]').fill(name);
    const destinationControl = await chooseCatalogDestination(form, "primary");
    await form.locator('select[name="syncMode"]').selectOption("full");
    // a DOUBLE click: the second lands while the first is pending
    await form.getByRole("button", { name: /connect archive/i }).dblclick();
    const t = await waitForText(page, /(connected|already connected|refused|403|409|422)/, 60, "the connect answer");
    await page.waitForTimeout(2000);
    const p0 = posts[0] || { headers: {} };
    row("P4 Connect an existing archive: POST carries X-CSRF-Token = the session's csrfToken and an Idempotency-Key, answered 201",
      posts.length >= 1 && p0.headers["x-csrf-token"] === sess.csrfToken && !!p0.headers["idempotency-key"] && (answers[0] || {}).status === 201 && !/does not match this session/.test(t),
      { posts: posts.length, csrfMatches: p0.headers["x-csrf-token"] === sess.csrfToken, idempotencyKey: !!p0.headers["idempotency-key"], answer: (answers[0] || {}).status, outcome: (t.match(/[^\n]*connected[^\n]*/) || [""])[0], destinationControl });
    row("P4 control: a double click sends one POST", posts.length === 1, { posts: posts.length });
    const cat = kj("get", "recoverycatalogs").items.find((c) => c.metadata.name === name);
    row("P4 the RecoveryCatalog exists", !!cat, { name, uid: cat && cat.metadata.uid });
    await shot(page, "P4-connected");
    // control: a LOST answer (the server created it, the browser never saw the 201) resubmitted is `replayed`
    const name2 = `${name}-lost`;
    let dropped = 0;
    await page.route(/\/api\/v1\/namespaces\/[^/]+\/catalogs$/, async (route) => {
      if (route.request().method() === "POST" && dropped === 0) {
        dropped++;
        await route.fetch();          // the request reaches the API and the catalog is created
        await route.abort("failed");  // ... and the page never sees the answer
        return;
      }
      await route.continue();
    });
    await gotoHash(page, `#/catalog?ns=${NS}`);
    await waitForText(page, /Connect an existing archive/, 60, "the catalog page");
    const f2 = page.locator("form[data-connect-archive]");
    await f2.locator('input[name="name"]').fill(name2);
    await chooseCatalogDestination(f2, "primary");
    await f2.locator('select[name="syncMode"]').selectOption("full");
    const n0 = posts.length;
    await f2.getByRole("button", { name: /connect archive/i }).click();
    await page.waitForTimeout(4000);
    await f2.getByRole("button", { name: /connect archive/i }).click();
    const t2 = await waitForText(page, /already connected/, 60, "the replayed answer");
    const k1 = (posts[n0] || { headers: {} }).headers["idempotency-key"], k2 = (posts[n0 + 1] || { headers: {} }).headers["idempotency-key"];
    const replayed = answers.find((a) => a.body && a.body.replayed === true);
    row("P4 control: a resubmit of the same draft after a lost answer reuses its key and is answered replayed (one catalog)",
      !!k1 && k1 === k2 && !!replayed && /already connected/.test(t2) && kj("get", "recoverycatalogs").items.filter((c) => c.metadata.name === name2).length === 1,
      { sameKey: !!k1 && k1 === k2, replayedStatus: replayed && replayed.status, outcome: (t2.match(/[^\n]*already connected[^\n]*/) || [""])[0] });
    await page.unroute(/\/api\/v1\/namespaces\/[^/]+\/catalogs$/);
    await shot(page, "P4-replayed");
    // control: the viewer is refused by capability, not by CSRF -- submitted THROUGH THE PAGE
    const v = await newSession(browser, "viewer");
    const vposts = [], vanswers = [];
    const vsess = await session(v.page);
    // THE TOKEN IS COMPARED AT CAPTURE AND NEVER KEPT: evidence carries the boolean only.
    v.page.on("request", (r) => { if (r.method() === "POST" && /\/catalogs$/.test(new URL(r.url()).pathname)) vposts.push({ sessionToken: r.headers()["x-csrf-token"] === vsess.csrfToken, key: !!r.headers()["idempotency-key"] }); });
    v.page.on("response", async (r) => { if (r.request().method() === "POST" && /\/catalogs$/.test(new URL(r.url()).pathname)) vanswers.push({ status: r.status(), body: await r.json().catch(() => null) }); });
    await gotoHash(v.page, `#/catalog?ns=${NS}`);
    await waitForText(v.page, /Connect an existing archive/, 60, "the viewer's catalog page");
    const vf = v.page.locator("form[data-connect-archive]");
    const vformShown = await vf.count();
    let vbtn = "absent", vt = "";
    if (vformShown) {
      await vf.locator('input[name="name"]').fill("viewer-must-not");
      await chooseCatalogDestination(vf, "primary");
      const b = vf.getByRole("button", { name: /connect archive/i });
      vbtn = (await b.isDisabled()) ? "disabled" : "enabled";
      if (vbtn === "enabled") { await b.click(); await v.page.waitForTimeout(4000); }
      vt = await v.page.locator("[data-connect-status]").innerText().catch(() => "");
    }
    const va = vanswers[0] || {};
    const vcode = ((va.body || {}).error || {}).code || (va.body || {}).code || null;
    row("P4 control: the viewer's connect is refused by capability (403 forbidden, its session token sent), never a CSRF refusal, and no catalog is made",
      (vbtn === "disabled" || (vposts.length === 1 && vposts[0].sessionToken === true && va.status === 403 && vcode === "forbidden")) &&
        !/synchronizer token|x-csrf-token/i.test(vt + JSON.stringify(va.body || {})) && !kj("get", "recoverycatalogs").items.some((c) => c.metadata.name === "viewer-must-not"),
      { viewerForm: vformShown, viewerButton: vbtn, posts: vposts, answer: { status: va.status, code: vcode }, pageSays: vt.slice(0, 300), grants: (vsess.namespaces || []).map((g) => [g.name, g.roles]) });
    await shot(v.page, "P4-viewer");
    await v.context.close();
    writeFileSync(`${OUT}/P4-facts.json`, JSON.stringify({ name, name2, posts: posts.map((p) => ({ at: p.at, key: !!p.headers["idempotency-key"], csrf: p.headers["x-csrf-token"] === sess.csrfToken })), answers: answers.map((a) => ({ status: a.status, replayed: a.body && a.body.replayed })) }, null, 1));
  }
  // ------------------------------------------------------------------ P2
  if (GROUPS.includes("P2")) {
    const api = await allBackups(page);
    const byName = Object.fromEntries(api.map((b) => [b.name, b]));
    const post = api.filter((b) => b.destinationRef && b.operation && b.operation.verifiedSuccess && b.createdAt > (ARG || "2026-09-25T00:19:00Z"))
      .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))[0];
    const pick = post || api.find((b) => b.operation && b.operation.verifiedSuccess);
    // ONE STEP AT A TIME: the point panel is step 2, reached with Next from step 1.
    await openWizard(page, `#/restore?ns=${NS}&backup=${encodeURIComponent(pick.name)}&uid=${encodeURIComponent(pick.uid)}`);
    await wizardStep(page, 2);
    const facts = await page.evaluate(() => {
      const out = {};
      for (const dt of document.querySelectorAll("#step-backup-set dt")) { const dd = dt.nextElementSibling; if (dd) out[dt.innerText.trim()] = dd.innerText.trim(); }
      return out;
    });
    row("P2 step 2 of a post-upgrade point: signed is the green 'verified by weirkeeper' badge",
      /verified by weirkeeper/.test(facts.signed || "") && !/unverified/.test(facts.signed || ""), { point: pick.name, createdAt: pick.createdAt, signed: facts.signed });
    row("P2 step 2 of that point: archive '... -- manifest attested by its verified receipt'",
      /-- manifest attested by its verified receipt$/.test(facts.archive || ""), { archive: facts.archive });
    await shot(page, "P2-point-panel");
    // THE SELECTOR ("Choose a recovery point") carries the signed and archive columns per point;
    // the wizard's "What this namespace holds" table does not.
    await gotoHash(page, `#/restore?ns=${NS}`);
    await waitForText(page, /manifest attested by its verified receipt|no manifest recorded/, 90, "the restore selector");
    await page.waitForTimeout(1500);
    // 20 points at a time since console-ux-1 (MCP-26): "every selector row" is read once every
    // point is shown. The action is column 1 and the run's name leads column 2 with its schedule
    // and slot beneath, so a row is named by its own "Restore this point" link (`backup=`).
    const selectorCount = await showEveryPoint(page);
    await shot(page, "P2-selector");
    const tableRows = await page.evaluate(() => [...document.querySelectorAll("#step-select-point tr[data-search]")].map((tr) => {
      const a = tr.querySelector('a[href*="backup="]');
      return { name: a ? new URLSearchParams(a.getAttribute("href").split("?")[1] || "").get("backup") : null,
        cells: [...tr.querySelectorAll("td")].map((c) => c.innerText.trim()) };
    }));
    const checked = [], wrong = [];
    for (const { name, cells } of tableRows) {
      if (!name || !byName[name]) continue;
      const b = byName[name];
      const text = cells.join(" | ");
      // only rows WITH a signed cell (the selector's); the holdings table beside it has none
      if (!/verified by weirkeeper|unverified/.test(text)) continue;
      const green = /verified by weirkeeper/.test(text) && /manifest attested by its verified receipt/.test(text);
      const refused = /unverified/.test(text) && /no manifest recorded/.test(text);
      const want = b.operation.verifiedSuccess === true && b.operation.verificationState === "valid";
      checked.push({ name, want, green, refused });
      if ((want && !green) || (!want && (!refused || green))) wrong.push({ name, want, text: text.slice(0, 300) });
    }
    const controls = checked.filter((c) => !c.want);
    const nonVerified = api.filter((b) => !(b.operation && b.operation.verifiedSuccess === true && b.operation.verificationState === "valid"));
    row("P2 every selector row agrees with the API: green + 'manifest attested' exactly where verifiedSuccess/valid; a point the API did not verify is never green there (refused 'unverified: ...' or not offered at all)",
      checked.length > 0 && wrong.length === 0 && nonVerified.length > 0 && nonVerified.every((b) => !checked.some((c) => c.name === b.name && c.green)),
      { rowsChecked: checked.length, selectorCount, green: checked.filter((c) => c.green).length, nonVerifiedInApi: nonVerified.map((b) => [b.name, b.operation && b.operation.verificationState]),
        shownRefused: controls.map((c) => c.name), notOffered: nonVerified.filter((b) => !checked.some((c) => c.name === b.name)).map((b) => b.name), wrong: wrong.slice(0, 5) });
    // the same points on the Backups page: the one badge rule (backupBadge), never green
    await gotoHash(page, `#/backups?ns=${NS}`);
    await page.waitForTimeout(3000);
    // Each point's own row, found by its NAME cell through the list's filter (console-ux-1: the
    // name cell carries "Follow this run" beneath it, and the list is paginated).
    const lines = [];
    for (const b of nonVerified) {
      const r = await listRow(page, "backups", b.name);
      lines.push([b.name, r ? r.text : "", r ? [r.cells.PHASE, r.cells.EXIT, r.cells.RECORDS, r.cells.SIGNED].join(" | ") : "no row"]);
    }
    row("P2 control: on the Backups page each point the API did not verify reads 'unverified: <case>' and never the green badge",
      lines.length > 0 && lines.every(([, l]) => /unverified: /.test(l) && !/verified by weirkeeper/.test(l)), { lines: lines.map(([n, , cells]) => [n, cells]) });
    writeFileSync(`${OUT}/P2-table.json`, JSON.stringify({ pick: pick.name, facts, checked }, null, 1));
  }
  // ------------------------------------------------------------------ SWEEP (5, 6, 7)
  if (GROUPS.includes("SWEEP")) {
    const rst = ARG || kj("get", "restores").items.filter((r) => (r.status || {}).phase === "Succeeded").map((r) => r.metadata.name).sort()[0];
    await gotoHash(page, `#/history?ns=${NS}&name=${encodeURIComponent(rst)}`);
    await page.waitForTimeout(2500);
    const h = await textOf(page);
    const op = await apiGet(page, `/api/v1/namespaces/${NS}/operations/restore/${rst}`);
    const scopeLine = (h.match(/[^\n]*sampled records matched byte-for-byte[^\n]*/) || [""])[0];
    row("SWEEP5 a Succeeded restore's History detail prints the API's verification scope, not 'No verification scope was recorded'",
      !!scopeLine && !/No verification scope was recorded/.test(h) && !!((op.body || {}).verificationScope || ((op.body || {}).item || {}).verificationScope),
      { restore: rst, scopeLine, apiHasScope: !!((op.body || {}).verificationScope || ((op.body || {}).item || {}).verificationScope) });
    await shot(page, "SWEEP5-history");
    const sch = kj("get", "backupschedules").items.find((s) => (s.spec.destinationRef || {}).name === "primary").metadata.name;
    await gotoHash(page, `#/schedules?ns=${NS}&name=${encodeURIComponent(sch)}`);
    await waitForText(page, /Latest point|Retention/, 60, "the schedule detail");
    const s = await textOf(page);
    const latest = (s.match(/Latest point completed[^\n]*(\n[^\n]*)?/) || [""])[0];
    row("SWEEP6 schedule facts: 'Latest point completed' is labelled as the creation instant", /\(its creation instant: this view does not publish when the run completed/.test(latest),
      { schedule: sch, latest });
    const policies = kj("get", "retentionpolicies").items.length;
    // THE PANEL'S OWN SENTENCE (`p.never-deletes`, render.js RETENTION_SENTENCE), not the form's help
    // "It reports; it never deletes.", on the destination schedule and on a legacy one.
    const panel = [];
    for (const name of [sch, ...kj("get", "backupschedules").items.filter((x) => !x.spec.destinationRef).map((x) => x.metadata.name).slice(0, 1)]) {
      await gotoHash(page, `#/schedules?ns=${NS}&name=${encodeURIComponent(name)}`);
      await page.waitForTimeout(2500);
      const n = page.locator("p.never-deletes");
      panel.push({ schedule: name, sentence: (await n.count()) ? await n.first().innerText() : null,
        enforcement: (await n.count()) ? await n.first().getAttribute("data-enforcement") : null,
        caveat: /does not record which RetentionPolicy covers/.test(await textOf(page)) });
    }
    row("SWEEP7 with no RetentionPolicy in the namespace the retention panel keeps 'Logweir never deletes from your archive' and no coverage caveat",
      policies === 0 && panel.length > 0 && panel.every((p) => p.sentence && p.sentence.startsWith("Logweir never deletes from your archive.") && !p.enforcement && !p.caveat),
      { retentionPolicies: policies, panel });
  }
  // ------------------------------------------------------------------ COMPLETION
  if (GROUPS.includes("COMPLETION")) {
    const [bad, good] = ARG.split(":");
    const rb = kj("get", "restore", bad);
    await gotoHash(page, `#/operations?ns=${NS}&kind=restore&name=${encodeURIComponent(bad)}&uid=${rb.metadata.uid}`);
    await page.waitForTimeout(2500);
    const t = await textOf(page);
    row("COMPLETION a restore that finished without success has no 'What this restore produced' and no 'No completion was recorded'",
      !/What this restore produced/.test(t) && !/No completion was recorded/.test(t) && ["Failed", "Refused", "Cancelled"].includes((rb.status || {}).phase),
      { restore: bad, phase: (rb.status || {}).phase, reason: (rb.status || {}).reason });
    await shot(page, "COMPLETION-failed");
    const rg = kj("get", "restore", good);
    await gotoHash(page, `#/clusters?ns=${NS}`);
    await gotoHash(page, `#/operations?ns=${NS}&kind=restore&name=${encodeURIComponent(good)}&uid=${rg.metadata.uid}`);
    await page.waitForTimeout(2500);
    const g = await textOf(page);
    row("COMPLETION control: a Succeeded restore's operation view shows the panel", /What this restore produced/.test(g) && /records verified in the sampled window|Completion not yet verified/.test(g),
      { restore: good, excerpt: (g.match(/[^\n]*records verified in the sampled window[^\n]*/) || [""])[0] });
    await shot(page, "COMPLETION-succeeded");
  }
  // ------------------------------------------------------------------ P7
  if (GROUPS.includes("P7")) {
    await gotoHash(page, `#/clusters?ns=${NS}`);
    await waitForText(page, /Create a KafkaCluster/, 60, "the clusters page");
    const form = page.locator("form", { has: page.locator('input[name="servers"]') }).first();
    const nameInputs = await form.locator('input[name="name"]').count();
    const t0 = await textOf(page);
    row("R7.1 the shared Clusters form has no name input and says the server names it conn- + 26 characters",
      nameInputs === 0 && /a connection is named by the server: conn- followed by 26 characters/.test(t0), { nameInputs });
    const made = {};
    for (const c of [{ role: "source", servers: "logweir-kafka-source.logweir-system.svc.cluster.local:9092" },
                     { role: "target", servers: "logweir-kafka-target.logweir-system.svc.cluster.local:9092" }]) {
      const before = new Set(kj("get", "kafkaclusters").items.map((k) => k.metadata.name));
      const t = await createCluster(page, NS, c, log);
      const after = kj("get", "kafkaclusters").items.filter((k) => !before.has(k.metadata.name));
      made[c.role] = { created: after.map((k) => [k.metadata.name, k.spec.role]), outcome: (t.match(/Created KafkaCluster conn-[a-z2-7]{26}/) || [""])[0] };
    }
    row("R7.1 source and target created: each answers 'Created KafkaCluster conn-...' and exactly one object of that role",
      ["source", "target"].every((r) => made[r].created.length === 1 && /^conn-[a-z2-7]{26}$/.test(made[r].created[0][0]) && made[r].created[0][1] === r && made[r].outcome.endsWith(made[r].created[0][0])), made);
    writeFileSync(`${OUT}/P7-made.json`, JSON.stringify({ made }, null, 1));
  }
  // THE ROLE UNDER EACH NAME (MCP round 2, R2-3 folded the ROLE column into the NAME cell): every
  // connection's row shows its spec.role on the name's second line -- how section 10 tells a
  // source from a target. Its own group, so a re-run reads the list without creating objects.
  if (GROUPS.includes("P7") || GROUPS.includes("P7LIST")) {
    await gotoHash(page, `#/clusters?ns=${NS}`);
    await waitForText(page, /conn-[a-z2-7]{26}/, 60, "the clusters list");
    const want = Object.fromEntries(kj("get", "kafkaclusters").items.map((k) => [k.metadata.name, k.spec.role]));
    const shown = await page.evaluate(() => [...document.querySelectorAll("table tbody tr")].map((tr) => {
      const td = tr.querySelector("td");
      const lines = td ? td.innerText.split("\n").map((x) => x.trim()).filter(Boolean) : [];
      return [lines[0] || "", lines[1] || ""];
    }));
    // EVERY connection's row, by its name -- minted conn-... ones and any made with kubectl (a test
    // fixture such as poc-upgrade-4's pu4-bh-source is listed by the same page)
    const listed = shown.filter((r) => r[0] in want);
    row("R7.1 the Clusters list shows each connection's role (source/target) under its name, as its spec says",
      listed.length === Object.keys(want).length && listed.every((r) => r[1] === want[r[0]]),
      { listed: listed.length, connections: Object.keys(want).length, mismatched: listed.filter((r) => r[1] !== want[r[0]]) });
    await shot(page, "R7-clusters");
  }
  if (GROUPS.includes("P7")) {
    // R7.2 double click
    await gotoHash(page, `#/clusters?ns=${NS}`);
    await waitForText(page, /Create a KafkaCluster/, 60, "the clusters page");
    const f = page.locator("form", { has: page.locator('input[name="servers"]') }).first();
    await f.locator('input[name="servers"]').fill("logweir-kafka-target.logweir-system.svc.cluster.local:9092");
    await f.locator('input[name="role"]').fill("target");
    await f.locator('select[name="mode"]').selectOption("plaintext");
    const before = new Set(kj("get", "kafkaclusters").items.map((k) => k.metadata.name));
    await f.getByRole("button", { name: /^create$/i }).dblclick();
    await page.waitForTimeout(5000);
    const extra = kj("get", "kafkaclusters").items.filter((k) => !before.has(k.metadata.name)).map((k) => k.metadata.name);
    row("R7.2 a double click on Create makes exactly one KafkaCluster", extra.length === 1, { created: extra });
    writeFileSync(`${OUT}/P7-double-click.json`, JSON.stringify({ doubleClick: extra }, null, 1));
  }
  // ------------------------------------------------------------------ P8
  if (GROUPS.includes("P8")) {
    const calls = [];
    page.on("request", (r) => { const p = new URL(r.url()).pathname; if (/destinations\/primary:test$|\/preflights\/pf-/.test(p)) calls.push({ m: r.method(), p, at: Date.now() }); });
    const answers = [];
    page.on("response", async (r) => { if (/destinations\/primary:test$/.test(new URL(r.url()).pathname)) answers.push(await r.json().catch(() => null)); });
    async function testOnce(label) {
      await gotoHash(page, `#/destinations?ns=${NS}&name=primary`);
      await waitForText(page, /Test access/, 60, "the destination page");
      const t0 = Date.now();
      const n0 = calls.length;
      await page.locator("#destination-test").getByRole("button", { name: /test access/i }).click();
      // SETTLED = rows read from the check table's cells, no "checking..." and no stop mark
      // (`checkVerdict`). The text alone cannot say it: since R2-11 a running check's note reads
      // "Whether its result applies to your current inputs is decided when it has one" (poc-upgrade-3,
      // H7). The wait ends on the verdict or on the page's own stop (P15), not on a 90 s budget.
      const read = await checkVerdict(page, "#destination-test", 240);
      const t = read.text;
      const settled = read.settled && read.applies ? Date.now() - t0 : null;
      const mine = calls.slice(n0);
      const post = mine.find((c) => c.m === "POST");
      const reads = mine.filter((c) => c.m === "GET" && post && c.at >= post.at);
      const pf = ((answers[answers.length - 1] || {}).item || (answers[answers.length - 1] || {}).preflight || {}).name || (t.match(/pf-[a-z2-7]{26}/) || [""])[0];
      const blocking = (read ? read.rows : []).filter((r) => r.gating === "blocking");
      await shot(page, `P8-${label}`);
      return { settledMs: settled, text: t, reads: reads.length, pf, blocking, outcome: read.outcome };
    }
    const r1 = await testOnce("R8.1");
    row("R8.1 Test access settles: 'applies to your current inputs', blocking rows ready, no pending / 'compared: nothing' / 'No access test has been recorded', a GET of the check after the POST",
      r1.settledMs !== null && r1.blocking.length > 0 && r1.blocking.every((r) => r.verdict === "ready") && !/compared: nothing/.test(r1.text) && !/No access test has been recorded/.test(r1.text) && r1.reads > 0,
      { outcome: r1.outcome, settledMs: r1.settledMs, pf: r1.pf, readsAfterPost: r1.reads, blocking: r1.blocking.map((r) => `${r.id}/${r.verdict}/${r.code}`) });
    const r2 = await testOnce("R8.2");
    row("R8.2 a second Test access is a new check (a new pf- id) and settles", r2.settledMs !== null && !!r2.pf && r2.pf !== r1.pf, { first: r1.pf, second: r2.pf, settledMs: r2.settledMs, outcome: r2.outcome });
    await page.reload({ waitUntil: "domcontentloaded" });
    await page.waitForTimeout(3000);
    const rt = await textOf(page);
    const rec = (rt.match(/Recorded by pf-[a-z2-7]{26}, observed[^\n]*/) || [""])[0];
    const api = await apiGet(page, `/api/v1/namespaces/${NS}/destinations/primary`);
    const last = (((api.body || {}).item || api.body || {}).lastTest) || {};
    row("R8.4 after a reload the page shows 'Recorded by pf-..., observed ...' with a ready badge, matching the API's lastTest",
      !!rec && /ready/.test(rt.slice(rt.indexOf(rec) - 200, rt.indexOf(rec) + 400)) && rec.includes(last.preflightId || "none"), { recorded: rec, apiLastTest: { preflightId: last.preflightId, state: last.state } });
    await shot(page, "P8-R8.4-reload");
  }
  // ------------------------------------------------------------------ LEGACY (P3, P5: legacy-point-restore L1, L1b, L2, L3)
  if (GROUPS.some((g) => g.startsWith("LEGACY"))) {
    // A recovery point with NO saved destination: an inline-archive run (every point v0.1.5 wrote).
    const legacy = kj("get", "backups").items.filter((b) => !b.spec.destinationRef && (b.spec.archive || {}).url && (b.status || {}).phase === "Succeeded")
      .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? -1 : 1));
    const point = ARG ? legacy.find((b) => b.metadata.name === ARG) : legacy[0];
    const sched = (point.spec.scheduleRef || {}).name;
    const target = kj("get", "kafkaclusters").items.find((i) => i.spec.role === "target" && (i.status || {}).reachable !== false).metadata.name;
    const stamp = Date.now().toString(36).slice(-4);
    // ONE STEP AT A TIME (console-ux-1, MCP-29): the inline archive's fields are step 1's, the
    // target and prefix step 4's; the evidence bucket and its note are read on step 1, where they
    // are shown. Walked 1 -> 4 with Next.
    const legacyFill = async (evidenceBucket, prefix) => {
      await wizardStep(page, 1);
      await page.fill("#store-endpoint", "http://logweir-minio.logweir-system.svc:9000");
      await page.fill("#store-region", "us-east-1");
      await page.check('input[name="pathStyle"]');
      await page.check('input[name="allowHttp"]');
      if (evidenceBucket) { await page.fill('input[name="evidenceBucket"]', evidenceBucket); await page.locator('input[name="evidenceBucket"]').blur(); }
      const evidence = await page.inputValue('input[name="evidenceBucket"]');
      const note = (await page.locator("#legacy-evidence-bucket").count()) ? await page.locator("#legacy-evidence-bucket").innerText() : "";
      await wizardStep(page, 4);
      const opts = await page.$$eval('select[name="targetCluster"] option', (os) => os.map((x) => [x.value, x.textContent]));
      await page.selectOption('select[name="targetCluster"]', opts.find((x) => x[1].startsWith(target + " "))[0]);
      await page.selectOption("#target-mode", "newTopic");
      await page.fill('input[name="topicPrefix"]', prefix); await page.locator('input[name="topicPrefix"]').blur();
      await page.waitForTimeout(1500);
      return { evidence, note };
    };
    // Step 5's check, then step 6, where Create's state is read.
    const readiness = async (label) => {
      await wizardStep(page, 5);
      const t0 = new Date(Date.now() - 2000).toISOString();
      await page.click("#restore-readiness-start");
      const read = await readinessRows(page, 240);
      const pf = kj("get", "preflights").items.filter((p) => p.metadata.creationTimestamp >= t0.slice(0, 19) + "Z" && ((p.spec.request || {}).operation === "Restore"))
        .sort((a, b) => (a.metadata.creationTimestamp < b.metadata.creationTimestamp ? 1 : -1))[0];
      const checks = (((pf || {}).status || {}).result || {}).checks || [];
      await shot(page, `${label}-readiness`);
      await wizardStep(page, 6);
      return { rows: read.rows, settled: read.settled, outcome: outcomeOf(read), s5: read.text || await page.locator("#step-preflight").innerText(), pf, checks, createDisabled: await page.isDisabled("#create-restore") };
    };
    const byId = (checks, id) => checks.find((c) => c.id === id) || {};
    const scopeOf = (c) => c.scope ? `${c.scope.kind}/${c.scope.name}` : "";
    // ---- L1
    // WHERE A PERSON FINDS THE POINT: its schedule's page while the schedule exists; once the
    // schedule is deleted (its runs stay, by design) the recovery-point selector, which lists every
    // point of the namespace. Either way the row REQUIRES the console's own "Restore this point".
    const schedAlive = !!sched && kj("get", "backupschedules").items.some((s) => s.metadata.name === sched);
    let link;
    if (schedAlive) {
      await gotoHash(page, `#/schedules?ns=${NS}&name=${encodeURIComponent(sched)}`);
      await waitForText(page, /Restore this point/, 60, "the legacy schedule page");
      link = page.locator(`a[href*="backup=${point.metadata.name}"]`, { hasText: /Restore this point/ }).first();
    } else {
      await gotoHash(page, `#/restore?ns=${NS}`);
      await waitForText(page, /manifest attested by its verified receipt|no manifest recorded/, 90, "the restore selector");
      await showEveryPoint(page);
      link = page.locator(`#step-select-point a[href*="backup=${point.metadata.name}"]`, { hasText: /Restore this point/ }).first();
    }
    row(`L1 the console offers 'Restore this point' on the pre-upgrade row (${schedAlive ? "its schedule's page" : "the recovery-point selector: its schedule is deleted"})`,
      (await link.count()) > 0, { schedule: sched, scheduleExists: schedAlive, point: point.metadata.name, created: point.metadata.creationTimestamp,
        verification: (((point.status || {}).evidence || {}).verification || {}).result });
    if (await link.count()) {
      if (schedAlive) await revealInGrid(page, link, point.metadata.name);
      await link.click();
      await wizardAt(page, 1, 60);
    } else await openWizard(page, `#/restore?ns=${NS}&backup=${encodeURIComponent(point.metadata.name)}&uid=${encodeURIComponent(point.metadata.uid)}`);
    // P12 (poc-fixes-3 P12-L1): the wizard no longer refuses the point as "not a recovery point".
    const opened = await textOf(page);
    const position = (await page.locator("#wizard-position").count()) ? (await page.locator("#wizard-position").textContent()).trim() : "";
    row("L1 (P12) the wizard opens the point: no 'is not a recovery point' refusal", !/is not a recovery point/.test(opened) && /^Step 1 of 6: /.test(position),
      { position, refusal: (opened.match(/[^\n]*is not a recovery point[^\n]*/) || [""])[0].slice(0, 200) });
    const { evidence: evb, note } = await legacyFill(null, `lg${stamp}-`);
    row("L1 the evidence bucket starts as the archive's own bucket (kafka-backups, not logweir-evidence), with the note naming LOGWEIR_ARCHIVE_URL",
      evb === "kafka-backups" && /LOGWEIR_ARCHIVE_URL/.test(note), { evidenceBucket: evb, note });
    const r1 = await readiness("L1");
    const c = r1.checks;
    const dr = byId(c, "destination.resolved");
    const want = {
      phase: ((r1.pf || {}).status || {}).phase,
      sentence: /with the Secret that Backup named/.test(r1.s5),
      destinationResolved: [dr.state, dr.code, scopeOf(dr), /logweir-s3/.test(dr.message || "")],
      archive: ["archive.backupSet", "archive.coverage", "archive.segments"].map((id) => [id, byId(c, id).state, scopeOf(byId(c, id))]),
      planBindings: [byId(c, "plan.bindings").state, byId(c, "plan.bindings").code],
      recoveryPoint: [byId(c, "recoveryPoint.state").state, byId(c, "recoveryPoint.state").code],
      approval: [byId(c, "approval.state").state, byId(c, "approval.state").code],
      evidenceReadableRow: !!c.find((x) => x.id === "destination.evidenceReadable"),
      createDisabled: r1.createDisabled,
    };
    row("L1 (P3) the legacy point's readiness: Completed; the Secret sentence; destination.resolved ready DestinationValid on InlineArchive/s3://kafka-backups/poc naming logweir-s3; archive.backupSet ready on InlineArchive/s3://kafka-backups/poc, coverage+segments ready on the check Job (as destination-backed); plan.bindings PlanMatchesReferences; recoveryPoint.state RecoveryPointSucceeded; approval.state skipped; no evidenceReadable row; Create enabled",
      want.phase === "Completed" && want.sentence && dr.state === "ready" && dr.code === "DestinationValid" && scopeOf(dr) === "InlineArchive/s3://kafka-backups/poc" && want.destinationResolved[3]
        && want.archive.every((a) => a[1] === "ready") && want.archive[0][2] === "InlineArchive/s3://kafka-backups/poc"
        // archive.coverage/segments are the check Job's own reads, scoped Job/<check job> exactly as on a
        // destination-backed restore check; only archive.backupSet names the archive location (both paths)
        && want.archive.slice(1).every((a) => a[2] === `Job/${(((r1.pf || {}).status || {}).jobRef || {}).name}`) && want.planBindings[1] === "PlanMatchesReferences"
        && want.recoveryPoint[1] === "RecoveryPointSucceeded" && want.approval[0] === "skipped" && !want.evidenceReadableRow && !want.createDisabled,
      { preflight: r1.pf && r1.pf.metadata.name, ...want });
    // the check Job reads with the restore Job's own principal: logweir-s3's keys
    let env = null;
    const jobName = (((r1.pf || {}).status || {}).jobRef || {}).name;
    if (jobName) {
      try {
        const job = kj("get", "job", jobName);
        env = (job.spec.template.spec.containers || []).flatMap((ct) => (ct.env || []).filter((e) => /^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY)$/.test(e.name)).map((e) => [e.name, JSON.stringify(e.valueFrom || "value")]));
      } catch (e) { env = [["error", String(e).slice(0, 120)]]; }
    }
    row("L1 the check Job's AWS_ACCESS_KEY_ID is a secretKeyRef to logweir-s3/access-key-id", !!env && env.some((e) => e[0] === "AWS_ACCESS_KEY_ID" && /"name":"logweir-s3"/.test(e[1]) && /"key":"access-key-id"/.test(e[1])), { job: jobName, env });
    writeFileSync(`${OUT}/L1-preflight.json`, JSON.stringify(r1.pf, null, 1));
    // ---- L1b: the verdict is bound to the archive Secret
    // The archive Secret is step 1's; the verdict it makes stale is read where the wizard shows
    // it, steps 5 and 6 (walked with Next), as the whole page carried both before.
    await wizardStep(page, 1);
    const secretField = page.locator("#archive-secret");
    const orig = await secretField.inputValue();
    await secretField.fill("logweir-s3-other"); await secretField.blur();
    await page.waitForTimeout(2000);
    await wizardStep(page, 5);
    const tb5 = await page.locator("#step-preflight").innerText();
    await wizardStep(page, 6);
    const tb = tb5 + "\n" + (await page.locator("#step-plan").innerText());
    const staleLine = (tb.match(/[^\n]*referentChanged[^\n]*/) || tb.match(/[^\n]*stale[^\n]*/i) || [""])[0];
    const disabledB = await page.isDisabled("#create-restore");
    let refusedB = disabledB;
    let createAnswer = null;
    if (!disabledB) {
      const before = new Set(kj("get", "restores").items.map((r) => r.metadata.name));
      await page.click("#create-restore");
      await page.waitForTimeout(3000);
      createAnswer = ((await textOf(page)).match(/[^\n]*(refus|stale|re-run|check again)[^\n]*/i) || [""])[0];
      refusedB = kj("get", "restores").items.every((r) => before.has(r.metadata.name));
    }
    row("L1b after a green check, another archive Secret marks the verdict stale (referentChanged Secret/...) and Create is refused",
      /referentChanged/.test(tb) && /Secret\//.test(staleLine + tb.slice(tb.indexOf("referentChanged"), tb.indexOf("referentChanged") + 200)) && refusedB,
      { orig, staleLine: staleLine.slice(0, 300), createDisabled: disabledB, createAnswer, restoreMade: !refusedB });
    await shot(page, "L1b-stale");
    await wizardStep(page, 1);
    await secretField.fill(orig); await secretField.blur();
    // ---- L2: re-check with the original Secret, then Create (Ordinary) -> Succeeded, Valid, completion
    if (!GROUPS.includes("LEGACY-NOCREATE")) {
      const r2 = await readiness("L2");
      const nr = r2.rows.filter((r) => r.gating === "blocking" && r.verdict !== "ready" && r.id !== "approval.state");
      row("L2 re-checked with the Backup's own Secret: ready again", r2.settled && r2.rows.some((r) => r.gating === "blocking") && nr.length === 0 && !r2.createDisabled, { outcome: r2.outcome, notReady: nr, preflight: r2.pf && r2.pf.metadata.name });
      await wizardStep(page, 6);
      await page.click("#create-restore");
      await page.waitForURL(/#\/(operations|approvals|history)/, { timeout: 60000 });
      const rname = decodeURIComponent((page.url().match(/[?&](?:name|subject)=([^&]+)/) || [])[1] || "");
      let st = {};
      const end = Date.now() + 600000;
      let term = 0;
      while (Date.now() < end) {
        await page.waitForTimeout(6000);
        st = kj("get", "restore", rname).status || {};
        if (["Succeeded", "Failed"].includes(st.phase)) { term = term || Date.now(); if (st.completion || st.phase === "Failed" || Date.now() - term > 120000) break; }
      }
      const v = ((st.evidence || {}).verification) || {};
      row("L2 (P5) the legacy point's restore Succeeds with verification Valid and status.completion",
        st.phase === "Succeeded" && v.result === "Valid" && !!st.completion, { restore: rname, phase: st.phase, verification: v.result, trust: v.trust, completion: st.completion });
      await gotoHash(page, `#/clusters?ns=${NS}`);
      await gotoHash(page, `#/history?ns=${NS}&name=${encodeURIComponent(rname)}`);
      await page.waitForTimeout(2500);
      const ht = await textOf(page);
      const check = ht.slice(ht.indexOf("Check it yourself"), ht.indexOf("Check it yourself") + 1500);
      row("L2 the restore's 'Check it yourself' commands fetch from s3://kafka-backups/logweir/drills/...", /s3:\/\/kafka-backups\/logweir\/drills\//.test(check) && !/logweir-evidence/.test(check),
        { excerpt: (check.match(/[^\n]*s3:\/\/[^\n]*/g) || []).slice(0, 3) });
      await shot(page, "L2-history");
      writeFileSync(`${OUT}/L2-restore.json`, JSON.stringify({ name: rname, status: st }, null, 1));
    }
    // ---- L3: the same point with evidence bucket logweir-evidence -> an advisory row, Create still enabled
    await gotoHash(page, `#/clusters?ns=${NS}`);
    await openWizard(page, `#/restore?ns=${NS}&backup=${encodeURIComponent(point.metadata.name)}&uid=${encodeURIComponent(point.metadata.uid)}`);
    await legacyFill("logweir-evidence", `le${stamp}-`);
    const r3 = await readiness("L3");
    const er = byId(r3.checks, "destination.evidenceReadable");
    row("L3 (P5 control) evidence bucket logweir-evidence: an advisory destination.evidenceReadable unknown/EvidenceReadNotConfigured naming s3://logweir-evidence and the handle by role only (no kafka-backups/logweir); Create still enabled",
      er.gating === "advisory" && er.state === "unknown" && er.code === "EvidenceReadNotConfigured" && /s3:\/\/logweir-evidence/.test(er.message || "") && /LOGWEIR_ARCHIVE_URL/.test(er.message || "") && !/kafka-backups\/logweir/.test(er.message || "") && !r3.createDisabled,
      { row: { gating: er.gating, state: er.state, code: er.code, message: er.message }, createDisabled: r3.createDisabled, preflight: r3.pf && r3.pf.metadata.name });
    if (GROUPS.includes("LEGACY-L3CREATE")) {
      await wizardStep(page, 6);
      await page.click("#create-restore");
      await page.waitForURL(/#\/(operations|approvals|history)/, { timeout: 60000 });
      const rname = decodeURIComponent((page.url().match(/[?&](?:name|subject)=([^&]+)/) || [])[1] || "");
      let st = {};
      const end = Date.now() + 600000;
      let term = 0;
      while (Date.now() < end) {
        await page.waitForTimeout(6000);
        st = kj("get", "restore", rname).status || {};
        const v = ((st.evidence || {}).verification) || {};
        if (["Succeeded", "Failed"].includes(st.phase)) { term = term || Date.now(); if (v.result && v.result !== "Pending") break; if (Date.now() - term > 180000) break; }
      }
      const v = ((st.evidence || {}).verification) || {};
      row("L3 optional create: verification NotAttempted naming s3://logweir-evidence and the handle by role (not its URL), and no completion",
        v.result === "NotAttempted" && /logweir-evidence/.test(v.detail || "") && !/kafka-backups\/logweir/.test(v.detail || "") && !st.completion,
        { restore: rname, phase: st.phase, exitReason: st.exitReason, verification: v.result, detail: v.detail, completion: !!st.completion });
      writeFileSync(`${OUT}/L3-restore.json`, JSON.stringify({ name: rname, status: st }, null, 1));
    }
  }
  // ------------------------------------------------------------------ README10 (deploy/poc/README.md section 10, as written)
  // Connections are the NEWEST source/target (made by P7 through the console, as section 10 says);
  // the destination `primary`; a NEW schedule from the form (R7.4, with R8.3 on its readiness);
  // Run first backup now; Restore this point with the section's values; R8.5 on the list panel.
  if (GROUPS.includes("README10")) {
    const PREFIX = ARG || "restored-";
    const newest = (role) => kj("get", "kafkaclusters").items.filter((k) => k.spec.role === role)
      .sort((a, b) => (a.metadata.creationTimestamp < b.metadata.creationTimestamp ? 1 : -1))[0].metadata.name;
    const SRC = newest("source"), TGT = newest("target");
    for (let i = 0; i < 60; i++) {   // the console's new connections are probed by the controller first
      const ks = kj("get", "kafkaclusters").items.filter((k) => [SRC, TGT].includes(k.metadata.name));
      if (ks.length === 2 && ks.every((k) => (k.status || {}).reachable === true)) break;
      await page.waitForTimeout(3000);
    }
    row("README10 the two connections made in the console (P7) are probed reachable", kj("get", "kafkaclusters").items.filter((k) => [SRC, TGT].includes(k.metadata.name) && (k.status || {}).reachable === true).length === 2, { source: SRC, target: TGT });
    // ---- schedule form (R7.4, R8.3)
    await gotoHash(page, `#/schedules?ns=${NS}`);
    await waitForText(page, /CHECK READINESS|Check readiness/, 60, "the schedule form");
    const form = page.locator("#schedule-form");
    const nameInputs = await form.locator('input[name="name"]').count();
    const ft = await textOf(page);
    row("R7.4 the shared schedule form has no name input and says the server names it sch- + 26 characters",
      nameInputs === 0 && /the server names the schedule: sch- followed by 26/.test(ft), { nameInputs });
    const opts = await form.locator('select[name="source"] option').evaluateAll((os) => os.map((o) => [o.value, o.textContent]));
    await form.locator('select[name="source"]').selectOption(opts.find((o) => o[1].includes(SRC))[0]);
    await form.locator('select[name="mode"]').selectOption("daily");
    await form.locator('input[name="hour"]').fill("2");
    await form.locator('input[name="minute"]').fill("0");
    await form.locator('select[name="selection"]').selectOption("named");
    await form.locator('input[name="topics"]').fill("orders, payments");
    const dopts = await form.locator('select[name="destination"] option').evaluateAll((os) => os.map((o) => [o.value, o.textContent]));
    await form.locator('select[name="destination"]').selectOption(dopts.find((o) => o[1].startsWith("primary"))[0]);
    await page.waitForTimeout(800);
    await form.getByRole("button", { name: /preview next runs/i }).click();
    // The AT (UTC) column in the one timestamp format (console-ux-1, MCP-7): was `…T02:00:00Z`.
    await waitForText(page, /NEXT RUNS[\s\S]*\d{4}-\d{2}-\d{2} 02:00:00 UTC/, 60, "the cadence preview");
    // The check's rows from the form's own fieldset (`checkVerdict`: four cells since MCP round 2,
    // poc-upgrade-3 H7; no rows unless settled, and a check the page stopped following is not, P15).
    // A replay is terminal on arrival and owes one read (P8), so once the rows settle the follow's
    // read gets up to 30 s to land before the verdict is taken.
    const verdictOf = async () => {
      const r = await checkVerdict(page, "#schedule-readiness", 240);
      return { t: r.text, rows: r.rows, applies: r.applies, outcome: r.outcome };
    };
    await page.locator("#schedule-check-readiness").click();
    const v1 = await verdictOf();
    await page.locator("#schedule-check-readiness").click();
    const v2 = await verdictOf();
    row("R8.3 the schedule form's Backup readiness clicked twice with unchanged inputs: the second (replayed) answer is read back — 'applies to your current inputs', no 'did not recompute staleness'",
      v2.rows.length > 0 && v2.applies && !/did not recompute staleness/.test(v2.t),
      { first: v1.outcome, second: v2.outcome, firstRows: v1.rows.map((x) => `${x.id}=${x.verdict}`).slice(0, 8), secondRows: v2.rows.map((x) => `${x.id}=${x.verdict}`).slice(0, 8), secondSays: (v2.t.match(/[^\n]*current inputs[^\n]*/) || [""])[0] });
    await shot(page, "README10-schedule-readiness");
    const before = new Set(kj("get", "backupschedules").items.map((s) => s.metadata.name));
    const create = form.getByRole("button", { name: /^create$/i });
    for (let i = 0; i < 20 && await create.isDisabled(); i++) await page.waitForTimeout(1500);
    await create.dblclick();
    await page.waitForTimeout(5000);
    const made = kj("get", "backupschedules").items.filter((s) => !before.has(s.metadata.name));
    const url = page.url();
    row("R7.4 a double click on Create makes exactly one BackupSchedule, named sch-..., and the page opens it by that name",
      made.length === 1 && /^sch-[a-z2-7]{26}$/.test(made[0].metadata.name) && url.includes(`name=${made[0].metadata.name}`),
      { created: made.map((s) => s.metadata.name), url: url.replace(/^.*#/, "#") });
    const SCH = made[0] && made[0].metadata.name;
    row("README10 the schedule is section 10's: daily 02:00, orders+payments, destination primary, source = the new source connection",
      !!SCH && made[0].spec.schedule === "0 2 * * *" && (made[0].spec.destinationRef || {}).name === "primary" && (made[0].spec.sourceRef || {}).name === SRC
        && JSON.stringify(made[0].spec.topics) === JSON.stringify(["orders", "payments"]), { schedule: SCH, spec: made[0] && { schedule: made[0].spec.schedule, topics: made[0].spec.topics, source: made[0].spec.sourceRef, destination: made[0].spec.destinationRef } });
    // ---- Run first backup now
    await gotoHash(page, `#/schedules?ns=${NS}&name=${SCH}`);
    await waitForText(page, /RUN FIRST BACKUP NOW|Run first backup now/, 60, "the new schedule detail");
    const b0 = new Set(kj("get", "backups").items.map((b) => b.metadata.name));
    await page.getByRole("button", { name: /run first backup now/i }).last().click();
    let b = null;
    for (let i = 0; i < 80; i++) {
      await page.waitForTimeout(5000);
      b = kj("get", "backups").items.find((x) => !b0.has(x.metadata.name) && ((x.spec.scheduleRef || {}).name === SCH));
      const v = b && (((b.status || {}).evidence || {}).verification || {}).result;
      if (b && (b.status || {}).phase === "Succeeded" && v && v !== "Pending") break;
    }
    const bv = ((((b || {}).status || {}).evidence || {}).verification) || {};
    row("README10 Run first backup now: the run Succeeds and verifies Valid", !!b && b.status.phase === "Succeeded" && bv.result === "Valid", { backup: b && b.metadata.name, verification: bv.result, key: bv.matchedKeyId });
    await gotoHash(page, `#/operations?ns=${NS}&kind=backup&name=${b.metadata.name}`);
    const bt = await textOf(page);
    const badge = (bt.match(/verified by weirkeeper[^\n]*/) || [""])[0];
    row("README10 the backup's operation page reads Succeeded with 'verified by weirkeeper ... against key ...'", /Succeeded/i.test(bt) && /verified by weirkeeper .* against key/.test(badge), { badge });
    await shot(page, "README10-backup-operation");
    // ---- Restore this point (section 10 row 'Restore')
    const { restoreFromBackup } = await import("./console.mjs");
    await gotoHash(page, `#/schedules?ns=${NS}&name=${SCH}`);
    const t0 = Date.now();
    await waitForText(page, /Restore this point/, 90, "the schedule's points");
    const link = page.locator(`a[href*="backup=${b.metadata.name}"]`, { hasText: /Restore this point/ }).first();
    // the schedule card's tables are paginated: a point past page 1 is absent until filtered for
    const revealed = await revealInGrid(page, link, b.metadata.name).then(() => null, (e) => String(e.message || e));
    row("README10 the schedule's page offers 'Restore this point'", (await link.count()) > 0 && (await link.isVisible()), { renderedAfterMs: Date.now() - t0, revealError: revealed });
    const r = await restoreFromBackup(page, NS, b.metadata.name, b.metadata.uid, { target: TGT, prefix: PREFIX, shots: `${OUT}/README10-restore` }, log);
    const st = r.status || {};
    const rv = ((st.evidence || {}).verification) || {};
    row("README10 readiness: every blocking row ready but approval.state", r.readiness.settled && r.readiness.rows.some((x) => x.gating === "blocking") && (r.readiness.blockingNotReady || ["x"]).length === 0,
      { outcome: outcomeOf(r.readiness), rows: r.readiness.rows.map((x) => `${x.id}=${x.verdict}`) });
    row("README10 the Restore (named rst-..., Ordinary) Succeeds with its completion panel", /^rst-/.test(r.name) && st.phase === "Succeeded" && !!st.completion && /records verified in the sampled window/.test(r.operationText || ""),
      { restore: r.name, verification: rv.result, completion: st.completion && { restored: st.completion.recordsRestored, sampledMatching: st.completion.recordsSampledMatching, topics: (st.completion.newTopics || []).map((t) => t.name) } });
    let topics = [];
    try {
      topics = execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=30s", "-n", "logweir-system", "exec", "logweir-kafka-target-0", "--",
        "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"], { timeout: 90000 }).toString().split("\n").filter((t) => t.startsWith(PREFIX));
    } catch (e) { topics = [`error: ${String(e).slice(0, 160)}`]; }
    row(`README10 the ${PREFIX} topics exist on the target broker`, topics.includes(`${PREFIX}orders`) && topics.includes(`${PREFIX}payments`), { topics });
    // ---- R8.5: the schedules list's readiness panel
    await gotoHash(page, `#/schedules?ns=${NS}`);
    await waitForText(page, /Backup readiness/, 60, "the schedules list");
    const panel = page.locator("#backup-readiness");
    const sopts = await panel.locator('select[name="source"] option').evaluateAll((os) => os.map((o) => [o.value, o.textContent]));
    await panel.locator('select[name="source"]').selectOption(sopts.find((o) => o[1].includes(SRC))[0]);
    const dsel = panel.locator('select[name="destination"]');
    if (await dsel.count()) { const d2 = await dsel.locator("option").evaluateAll((os) => os.map((o) => [o.value, o.textContent])); const hit = d2.find((o) => o[1].startsWith("primary")); if (hit) await dsel.selectOption(hit[0]); }
    await panel.locator("#readiness-topics").fill("orders, payments");
    await panel.getByRole("button", { name: /^check readiness$/i }).click();
    const read = await settledRows(page, "#backup-readiness", 240, 3000);
    const settled = read.settled;
    const pt = read.text || await panel.innerText();
    row("R8.5 the schedules list's Backup readiness panel settles to a verdict read back from the check", settled && !/did not recompute staleness/.test(pt),
      { outcome: outcomeOf(read), excerpt: read.rows.filter((x) => x.gating === "blocking").map((x) => `${x.id}=${x.verdict}`).slice(0, 10) });
    await shot(page, "README10-R8.5-panel");
    writeFileSync(`${OUT}/README10-facts.json`, JSON.stringify({ SRC, TGT, SCH, backup: b.metadata.name, restore: r.name, prefix: PREFIX }, null, 1));
  }
} catch (e) {
  row("reproof completed every step", false, { error: String((e && e.stack) || e).slice(0, 800) });
} finally {
  await browser.close();
  const failed = ROWS.filter((r) => !r.pass);
  log(`${ROWS.length - failed.length}/${ROWS.length} rows pass${failed.length ? "; FAILED: " + failed.map((r) => r.id).join(" | ") : ""}`);
  process.exitCode = failed.length ? 1 : 0;
}
