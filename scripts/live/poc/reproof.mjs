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
//             conn-..., ROLE column; a double click creates exactly one (poc-fixes-2 R7.1, R7.2)
//   P8        Destinations -> primary -> Test access settles (R8.1), a second test is a new check (R8.2),
//             a reload shows the recorded test (R8.4)
//
//   NODE_PATH="$(npm root -g)" node scripts/live/poc/reproof.mjs <outdir> <GROUP>[,<GROUP>...] [args]
//
// Passwords come from the credentials file and are typed into Dex; nothing secret is printed.
import { writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { chromium, newSession, gotoHash, textOf, waitForText, connectCatalog, createCluster } from "./console.mjs";

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
    await form.locator('input[name="destination"]').fill("primary");
    await form.locator('select[name="syncMode"]').selectOption("full");
    // a DOUBLE click: the second lands while the first is pending
    await form.getByRole("button", { name: /connect archive/i }).dblclick();
    const t = await waitForText(page, /(connected|already connected|refused|403|409|422)/, 60, "the connect answer");
    await page.waitForTimeout(2000);
    const p0 = posts[0] || { headers: {} };
    row("P4 Connect an existing archive: POST carries X-CSRF-Token = the session's csrfToken and an Idempotency-Key, answered 201",
      posts.length >= 1 && p0.headers["x-csrf-token"] === sess.csrfToken && !!p0.headers["idempotency-key"] && (answers[0] || {}).status === 201 && !/does not match this session/.test(t),
      { posts: posts.length, csrfMatches: p0.headers["x-csrf-token"] === sess.csrfToken, idempotencyKey: !!p0.headers["idempotency-key"], answer: (answers[0] || {}).status, outcome: (t.match(/[^\n]*connected[^\n]*/) || [""])[0] });
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
    await f2.locator('input[name="destination"]').fill("primary");
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
      await vf.locator('input[name="destination"]').fill("primary");
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
    await gotoHash(page, `#/restore?ns=${NS}&backup=${encodeURIComponent(pick.name)}&uid=${encodeURIComponent(pick.uid)}`);
    await waitForText(page, /6\. Plan, hash and names/, 60, "the wizard");
    const facts = await page.evaluate(() => {
      const out = {};
      for (const dt of document.querySelectorAll("dt")) { const dd = dt.nextElementSibling; if (dd) out[dt.innerText.trim()] = dd.innerText.trim(); }
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
    await shot(page, "P2-selector");
    const tableRows = await page.evaluate(() => [...document.querySelectorAll("tr")].map((tr) => [...tr.querySelectorAll("td,th")].map((c) => c.innerText.trim())));
    const checked = [], wrong = [];
    for (const cells of tableRows) {
      const name = cells.find((c) => byName[c]);
      if (!name) continue;
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
      { rowsChecked: checked.length, green: checked.filter((c) => c.green).length, nonVerifiedInApi: nonVerified.map((b) => [b.name, b.operation && b.operation.verificationState]),
        shownRefused: controls.map((c) => c.name), notOffered: nonVerified.filter((b) => !checked.some((c) => c.name === b.name)).map((b) => b.name), wrong: wrong.slice(0, 5) });
    // the same points on the Backups page: the one badge rule (backupBadge), never green
    await gotoHash(page, `#/backups?ns=${NS}`);
    await page.waitForTimeout(3000);
    const bl = await textOf(page);
    const lines = nonVerified.map((b) => [b.name, (bl.split("\n").find((l) => l.startsWith(b.name + "\t")) || "")]);
    row("P2 control: on the Backups page each point the API did not verify reads 'unverified: <case>' and never the green badge",
      lines.length > 0 && lines.every(([, l]) => /unverified: /.test(l) && !/verified by weirkeeper/.test(l)), { lines: lines.map(([n, l]) => [n, l.split("\t").slice(2, 6).join(" | ")]) });
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
    await gotoHash(page, `#/clusters?ns=${NS}`);
    const lt = await textOf(page);
    row("R7.1 the Clusters list shows the ROLE column", /ROLE|Role|role/.test(lt.slice(0, lt.indexOf("Create a KafkaCluster") > 0 ? lt.indexOf("Create a KafkaCluster") : lt.length)), {});
    await shot(page, "R7-clusters");
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
    writeFileSync(`${OUT}/P7-made.json`, JSON.stringify({ made, doubleClick: extra }, null, 1));
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
      let t = "", settled = null;
      for (let i = 0; i < 45; i++) {
        await page.waitForTimeout(2000);
        t = await page.locator("#destination-test").innerText();
        if (/applies to your current inputs/.test(t) && !/\tpending\t|\bpending\b/.test(t.split("\n").filter((l) => /\t(blocking|advisory)\t/.test(l)).join("\n"))) { settled = Date.now() - t0; break; }
      }
      const mine = calls.slice(n0);
      const post = mine.find((c) => c.m === "POST");
      const reads = mine.filter((c) => c.m === "GET" && post && c.at >= post.at);
      const pf = ((answers[answers.length - 1] || {}).item || (answers[answers.length - 1] || {}).preflight || {}).name || (t.match(/pf-[a-z2-7]{26}/) || [""])[0];
      const blocking = t.split("\n").filter((l) => /\tblocking\t/.test(l));
      await shot(page, `P8-${label}`);
      return { settledMs: settled, text: t, reads: reads.length, pf, blocking };
    }
    const r1 = await testOnce("R8.1");
    row("R8.1 Test access settles: 'applies to your current inputs', blocking rows ready, no pending / 'compared: nothing' / 'No access test has been recorded', a GET of the check after the POST",
      r1.settledMs !== null && r1.blocking.length > 0 && r1.blocking.every((l) => /\tready\t/.test(l)) && !/compared: nothing/.test(r1.text) && !/No access test has been recorded/.test(r1.text) && r1.reads > 0,
      { settledMs: r1.settledMs, pf: r1.pf, readsAfterPost: r1.reads, blocking: r1.blocking.map((l) => l.split("\t").slice(0, 3).join("/")) });
    const r2 = await testOnce("R8.2");
    row("R8.2 a second Test access is a new check (a new pf- id) and settles", r2.settledMs !== null && !!r2.pf && r2.pf !== r1.pf, { first: r1.pf, second: r2.pf, settledMs: r2.settledMs });
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
    const legacyFill = async (evidenceBucket) => {
      await page.fill('input[name="endpoint"]', "http://logweir-minio.logweir-system.svc:9000");
      await page.fill('input[name="region"]', "us-east-1");
      await page.check('input[name="pathStyle"]');
      await page.check('input[name="allowHttp"]');
      if (evidenceBucket) { await page.fill('input[name="evidenceBucket"]', evidenceBucket); await page.locator('input[name="evidenceBucket"]').blur(); }
      const opts = await page.$$eval('select[name="targetCluster"] option', (os) => os.map((x) => [x.value, x.textContent]));
      await page.selectOption('select[name="targetCluster"]', opts.find((x) => x[1].startsWith(target + " "))[0]);
      await page.selectOption('select[name="mode"]', "newTopic");
    };
    const readiness = async (label) => {
      const t0 = new Date(Date.now() - 2000).toISOString();
      await page.click("#restore-readiness-start");
      let rows = [], s5 = "";
      const end = Date.now() + 240000;
      while (Date.now() < end) {
        await page.waitForTimeout(4000);
        const t = await textOf(page);
        s5 = t.slice(t.indexOf("5. Operation readiness"), t.indexOf("6. Plan, hash and names"));
        rows = [...s5.matchAll(/^([a-zA-Z]+\.[a-zA-Z]+)\t([^\t]+)\t(blocking|advisory|executionOnly)\t([A-Za-z]+)/gm)].map((m) => ({ id: m[1], verdict: m[2], gating: m[3], code: m[4] }));
        if (rows.length > 0 && !rows.some((r) => /pending|running/i.test(r.verdict))) break;
      }
      const pf = kj("get", "preflights").items.filter((p) => p.metadata.creationTimestamp >= t0.slice(0, 19) + "Z" && ((p.spec.request || {}).operation === "Restore"))
        .sort((a, b) => (a.metadata.creationTimestamp < b.metadata.creationTimestamp ? 1 : -1))[0];
      const checks = (((pf || {}).status || {}).result || {}).checks || [];
      await shot(page, `${label}-readiness`);
      return { rows, s5, pf, checks, createDisabled: await page.isDisabled("#create-restore") };
    };
    const byId = (checks, id) => checks.find((c) => c.id === id) || {};
    const scopeOf = (c) => c.scope ? `${c.scope.kind}/${c.scope.name}` : "";
    // ---- L1
    await gotoHash(page, `#/schedules?ns=${NS}&name=${encodeURIComponent(sched)}`);
    await waitForText(page, /Restore this point/, 60, "the legacy schedule page");
    const link = page.locator(`a[href*="backup=${point.metadata.name}"]`, { hasText: /Restore this point/ }).first();
    row("L1 the legacy schedule's page offers 'Restore this point' on the pre-upgrade row", (await link.count()) > 0, { schedule: sched, point: point.metadata.name, created: point.metadata.creationTimestamp });
    if (await link.count()) await link.click();
    else await gotoHash(page, `#/restore?ns=${NS}&backup=${encodeURIComponent(point.metadata.name)}&uid=${encodeURIComponent(point.metadata.uid)}`);
    await waitForText(page, /6\. Plan, hash and names/, 60, "the wizard");
    await legacyFill(null);
    await page.fill('input[name="topicPrefix"]', `lg${stamp}-`); await page.locator('input[name="topicPrefix"]').blur();
    await page.waitForTimeout(1500);
    const evb = await page.inputValue('input[name="evidenceBucket"]');
    const note = (await page.locator("#legacy-evidence-bucket").count()) ? await page.locator("#legacy-evidence-bucket").innerText() : "";
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
    const secretField = page.locator('input[name="archiveSecret"]');
    const orig = await secretField.inputValue();
    await secretField.fill("logweir-s3-other"); await secretField.blur();
    await page.waitForTimeout(2000);
    const tb = await textOf(page);
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
    await secretField.fill(orig); await secretField.blur();
    // ---- L2: re-check with the original Secret, then Create (Ordinary) -> Succeeded, Valid, completion
    if (!GROUPS.includes("LEGACY-NOCREATE")) {
      const r2 = await readiness("L2");
      const nr = r2.rows.filter((r) => r.gating === "blocking" && r.verdict !== "ready" && r.id !== "approval.state");
      row("L2 re-checked with the Backup's own Secret: ready again", nr.length === 0 && !r2.createDisabled, { notReady: nr, preflight: r2.pf && r2.pf.metadata.name });
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
    await gotoHash(page, `#/restore?ns=${NS}&backup=${encodeURIComponent(point.metadata.name)}&uid=${encodeURIComponent(point.metadata.uid)}`);
    await waitForText(page, /6\. Plan, hash and names/, 60, "the wizard");
    await legacyFill("logweir-evidence");
    await page.fill('input[name="topicPrefix"]', `le${stamp}-`); await page.locator('input[name="topicPrefix"]').blur();
    await page.waitForTimeout(1500);
    const r3 = await readiness("L3");
    const er = byId(r3.checks, "destination.evidenceReadable");
    row("L3 (P5 control) evidence bucket logweir-evidence: an advisory destination.evidenceReadable unknown/EvidenceReadNotConfigured naming s3://logweir-evidence and the handle by role only (no kafka-backups/logweir); Create still enabled",
      er.gating === "advisory" && er.state === "unknown" && er.code === "EvidenceReadNotConfigured" && /s3:\/\/logweir-evidence/.test(er.message || "") && /LOGWEIR_ARCHIVE_URL/.test(er.message || "") && !/kafka-backups\/logweir/.test(er.message || "") && !r3.createDisabled,
      { row: { gating: er.gating, state: er.state, code: er.code, message: er.message }, createDisabled: r3.createDisabled, preflight: r3.pf && r3.pf.metadata.name });
    if (GROUPS.includes("LEGACY-L3CREATE")) {
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
    await waitForText(page, /NEXT RUNS[\s\S]*T02:00:00Z/, 60, "the cadence preview");
    const verdictOf = async () => {
      const end = Date.now() + 240000;
      let t = "";
      while (Date.now() < end) {
        await page.waitForTimeout(3000);
        t = await page.locator("#schedule-readiness").innerText();
        const rows = t.split("\n").filter((l) => /\t(blocking|advisory|executionOnly)\t/.test(l));
        if (rows.length && !rows.some((l) => /\t(pending|running)\t/i.test(l))) return { t, rows };
      }
      return { t, rows: [] };
    };
    await page.locator("#schedule-check-readiness").click();
    const v1 = await verdictOf();
    await page.locator("#schedule-check-readiness").click();
    const v2 = await verdictOf();
    row("R8.3 the schedule form's Backup readiness clicked twice with unchanged inputs: the second (replayed) answer is read back — 'applies to your current inputs', no 'did not recompute staleness'",
      v2.rows.length > 0 && /applies to your current inputs/.test(v2.t) && !/did not recompute staleness/.test(v2.t) && !/does not apply to your current inputs/.test(v2.t),
      { first: v1.rows.map((l) => l.split("\t").slice(0, 2).join("=")).slice(0, 8), second: v2.rows.map((l) => l.split("\t").slice(0, 2).join("=")).slice(0, 8), secondSays: (v2.t.match(/[^\n]*current inputs[^\n]*/) || [""])[0] });
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
    row("README10 the schedule's page offers 'Restore this point'", (await link.count()) > 0, { renderedAfterMs: Date.now() - t0 });
    const r = await restoreFromBackup(page, NS, b.metadata.name, b.metadata.uid, { target: TGT, prefix: PREFIX, shots: `${OUT}/README10-restore` }, log);
    const st = r.status || {};
    const rv = ((st.evidence || {}).verification) || {};
    row("README10 readiness: every blocking row ready but approval.state", (r.readiness.blockingNotReady || ["x"]).length === 0, { rows: r.readiness.rows.map((x) => `${x.id}=${x.verdict}`) });
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
    let pt = "", settled = false;
    for (let i = 0; i < 80 && !settled; i++) {
      await page.waitForTimeout(3000);
      pt = await panel.innerText();
      const rows = pt.split("\n").filter((l) => /\t(blocking|advisory|executionOnly)\t/.test(l));
      settled = rows.length > 0 && !rows.some((l) => /\t(pending|running)\t/i.test(l));
    }
    row("R8.5 the schedules list's Backup readiness panel settles to a verdict read back from the check", settled && !/did not recompute staleness/.test(pt),
      { excerpt: pt.split("\n").filter((l) => /\t(blocking)\t/.test(l)).map((l) => l.split("\t").slice(0, 2).join("=")).slice(0, 10) });
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
