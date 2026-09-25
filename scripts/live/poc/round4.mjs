// The fourth PoC round's console rows (poc-upgrade-4), driven in a real Chromium through Traefik and
// Dex against the deployed shared console. Each row REQUIRES the outcome it records. The rows are
// the ones claude/poc-fixes-5 named for the next PoC round ("Rows for the next PoC round"):
//
//   CONN    P15-1 Test connection on a blackhole connection: "checking..." at 30, 60 and 90 s, the
//           recorded verdict at about 120-135 s without a reload, and the product API's own request
//           log showing the follow's read cadence (2 s for 20 s, then 3, 4.5, 6.75, 10 s). P15-5 the
//           same check left for Schedules and come back to within 60 s: the panel follows it again
//           (no read while away) and shows the verdict.
//   DISC    P15-4 Discover topics on a live connection (pending -> succeeded without a reload, both
//           slots updated) and on the blackhole one (settles failed).
//   PANEL   P15-2 Schedules -> Backup readiness with the blackhole source, then the schedule form's
//           Check readiness: the verdict and its applicability land without a reload.
//   WIZ     P15-3 restore step 5 against the blackhole target: the verdict lands on the step after
//           the old 90 s, and Create is gated by it.
//   DEST    P15-7 Test access on a destination whose endpoint nothing answers.
//   R3      R3-1 at 390x844: after Check readiness and after Check this plan again, both the focused
//           status and the verdict's headline sit above the sticky Back/Next bar
//           (getBoundingClientRect). R2-10: step 5's source sentence has no backtick, its names are
//           code.
//   R3ROLES R3-2 viewer: Catalog, Backups, a schedule's points and its latest-point line say "an
//           operator or administrator can restore this point" and carry no link; operator: the
//           links are there and open the wizard. R3-3 norole: the header reads "no role yet".
//
//   NODE_PATH="$(npm root -g)" node scripts/live/poc/round4.mjs <outdir> <GROUP>[,<GROUP>...]
//
// The blackhole fixtures (KafkaClusters pu4-bh-source / pu4-bh-target, BackupDestination
// pu4-bh-dest, all labelled logweir.dev/test-owner=poc-upgrade-4) are the report's, made with
// kubectl before this runs. Every row runs in a fresh sign-in so it fits the 900 s session.
//
// HOW A SLOW CHECK IS STAGED. A blackhole BOOTSTRAP is not slow: the check's Kafka calls carry a
// 10 s metadata timeout (logweir-kafka ProbeTimeouts), and a blackhole connection check records
// notReady about 33 s after it is made (run 1 of this round). A blackhole ENDPOINT is: each store
// request is given the check's remaining budget, so Test access on pu4-bh-dest records notReady
// after about 100 s. So Test access, the readiness panel and the schedule form are slowed by
// pu4-bh-dest itself; Test connection and restore step 5, whose checks read no destination that
// can be blackholed, are slowed the way a busy installation slows them: four slow Test access
// checks hold the namespace's four check slots (policy checks.maxActivePerNamespace = 4), so the
// row's check is Queued (ConcurrencyLimited) until a slot frees -- about 100 s -- and then runs.
// Passwords come from the credentials file and are typed into Dex; nothing secret is printed.
// Idempotency keys are recorded as a digest only; the CSRF token is never read.
import { writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chromium, newSession, gotoHash, waitForText, openWizard, wizardStep, revealInGrid,
  checkRowsIn, settledRows, outcomeOf,
} from "./console.mjs";

const OUT = process.argv[2] || "/tmp/poc-round4";
const GROUPS = (process.argv[3] || "CONN").split(",");
const NS = process.env.POC_NAMESPACE || "logweir-poc";
const BH_SRC = "pu4-bh-source", BH_TGT = "pu4-bh-target", BH_DEST = "pu4-bh-dest";
mkdirSync(OUT, { recursive: true });
const ROWS = [];
const TAG = GROUPS.join("_");
const log = (m) => console.log(new Date().toISOString(), m);
function row(id, ok, evidence) {
  ROWS.push({ id, pass: !!ok, at: new Date().toISOString(), evidence });
  log(`${ok ? "PASS" : "FAIL"} ${id} ${JSON.stringify(evidence).slice(0, 900)}`);
  writeFileSync(`${OUT}/rows-${TAG}.json`, JSON.stringify(ROWS, null, 1));
}
const kj = (...args) => JSON.parse(execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=30s", "-n", NS, ...args, "-o", "json"], { timeout: 45000, maxBuffer: 256 * 1024 * 1024 }).toString());
async function shot(page, name) { await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: true }).catch(() => {}); }
const digest = (s) => (s ? createHash("sha256").update(s).digest("hex").slice(0, 12) : null);
const nowIso = () => new Date().toISOString();
const sleep = (page, ms) => page.waitForTimeout(ms);
const secs = (t0) => Math.round((Date.now() - t0) / 100) / 10;

// Every product-API POST answer (the check it names) and every GET of a check (pf-/td-), per page.
function capture(page) {
  const net = { posts: [], reads: [] };
  page.on("response", async (r) => {
    const q = r.request();
    const u = new URL(r.url());
    if (!u.pathname.startsWith("/api/v1/")) return;
    if (q.method() === "POST") {
      let body = null;
      try { body = await r.json(); } catch { body = null; }
      const item = (body && body.item) || {};
      net.posts.push({ at: nowIso(), path: u.pathname, status: r.status(), keyDigest: digest(q.headers()["idempotency-key"] || ""), replayed: body ? body.replayed === true : null, id: item.id || null, state: item.state || null });
    } else if (q.method() === "GET" && /\/(preflights|topic-discoveries)\/(pf|td)-/.test(u.pathname)) {
      let body = null;
      try { body = await r.json(); } catch { body = null; }
      const it = (body && body.item) || {};
      net.reads.push({ at: nowIso(), id: u.pathname.split("/").pop(), status: r.status(), state: it.state || null, terminal: it.terminal });
    }
  });
  return net;
}
async function postedId(page, net, since, re) {
  for (let i = 0; i < 60; i++) {
    const p = net.posts.find((n) => n.at >= since && re.test(n.path) && n.id);
    if (p) return p;
    await sleep(page, 500);
  }
  return null;
}

// A page load is the only thing that clears a window global: a sentinel set before the click and
// still there at the verdict proves the verdict landed WITHOUT a reload.
const plant = (page) => page.evaluate(() => { window.__pu4Sentinel = "kept"; });
const planted = (page) => page.evaluate(() => window.__pu4Sentinel === "kept");

// One read of the check under `selector` once `at` seconds have passed since `t0`: a sample of
// what the page shows while the follow runs (not a settle wait).
async function sampleAt(page, selector, t0, at) {
  const left = t0 + at * 1000 - Date.now();
  if (left > 0) await sleep(page, left);
  const r = await checkRowsIn(page, selector);
  return { at, checking: r.checking, stopped: r.stopped, settled: r.settled, rows: r.rows.length };
}

// The product API's own record of the reads of one check: every GET of `id` in both console pods'
// request logs since `sinceIso`, in time order, with the gaps between them.
function apiReads(id, sinceIso) {
  const text = execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=60s", "-n", "logweir-system", "logs",
    "-l", "app.kubernetes.io/component=api", "--since-time=" + sinceIso, "--tail=-1", "--max-log-requests=6"], { timeout: 90000, maxBuffer: 512 * 1024 * 1024 }).toString();
  const reads = [];
  for (const line of text.split("\n")) {
    if (!line.includes(id) || !line.includes('"message":"request"')) continue;
    let j = null;
    try { j = JSON.parse(line); } catch { continue; }
    const f = j.fields || {};
    if (f.method === "GET" && String(f.path || "").endsWith("/" + id)) reads.push({ at: j.timestamp, status: f.status });
  }
  reads.sort((a, b) => (a.at < b.at ? -1 : 1));
  const gaps = reads.slice(1).map((r, i) => Math.round((Date.parse(r.at) - Date.parse(reads[i].at)) / 10) / 100);
  return { reads, gaps };
}
// The follow's schedule (ui/lifecycle.js followGap): 2 s for the first twenty seconds, then 3, 4.5,
// 6.75 and 10 s. `gaps` must follow it within a second (a read's own latency moves each by less).
function cadenceOf(api, until) {
  const g = api.gaps.slice(0, until === undefined ? api.gaps.length : until);
  const want = g.map((_, i) => (i < 9 ? 2 : [3, 4.5, 6.75][i - 9] || 10));
  const off = g.map((x, i) => Math.round((x - want[i]) * 100) / 100);
  const first = api.reads.length ? Date.parse(api.reads[0].at) : 0;
  const span = api.reads.length ? (Date.parse(api.reads[api.reads.length - 1].at) - first) / 1000 : 0;
  return { reads: api.reads.length, spanSeconds: Math.round(span), gaps: g, want, within1s: off.every((x) => Math.abs(x) <= 1.0), worst: Math.max(0, ...off.map(Math.abs)) };
}
// The cluster's own record of a check: when it was made, when its result was observed, and what.
function preflightFacts(id) {
  const p = kj("get", "preflight", id);
  const st = p.status || {};
  const took = st.observedAt ? Math.round((Date.parse(st.observedAt) - Date.parse(p.metadata.creationTimestamp)) / 1000) : null;
  return { id, created: p.metadata.creationTimestamp, phase: st.phase, state: (st.result || {}).state, observedAt: st.observedAt, tookSeconds: took, timeoutSeconds: (p.spec || {}).timeoutSeconds };
}
const brief = (rows) => rows.filter((r) => r.gating === "blocking").map((r) => `${r.id}=${r.verdict}/${r.code}`).slice(0, 14);

// Test connection on a connection's detail: the click, the samples at 30/60/90 s, the settle.
async function testConnection(page, net, name, label) {
  await gotoHash(page, `#/clusters?ns=${NS}&name=${name}`);
  await waitForText(page, /TEST CONNECTION|Test connection/, 60, "the connection detail");
  await plant(page);
  const since = new Date(Date.now() - 5000).toISOString();
  const t0 = Date.now();
  await page.locator("#connection-check-form").getByRole("button", { name: /^test connection$/i }).click();
  const post = await postedId(page, net, new Date(t0 - 1000).toISOString(), /preflights$/);
  log(`${label}: Test connection POST ${post && post.status} ${post && post.id}`);
  return { since, t0, post };
}

// Four slow checks that hold the namespace's check slots: Test access on pu4-bh-dest in `n` tabs of
// the row's own session, one click each (a per-click token, so four checks).
async function occupySlots(context, n, label) {
  const pages = [];
  const ids = [];
  for (let i = 0; i < n; i++) {
    const p = await context.newPage();
    const net = capture(p);
    await gotoHash(p, `#/destinations?ns=${NS}&name=${BH_DEST}`);
    await waitForText(p, /Test access/, 60, "the blackhole destination");
    const t = new Date(Date.now() - 1000).toISOString();
    await p.locator("#destination-test").getByRole("button", { name: /test access/i }).click();
    const post = await postedId(p, net, t, /:test$|\/test$/);
    pages.push(p);
    ids.push(post && post.id);
  }
  // each holder has its Job (admitted, not queued) before the row's own check is made
  let phases = [];
  for (let i = 0; i < 40; i++) {
    phases = ids.map((id) => (id ? ((kj("get", "preflight", id).status || {}).phase || "none") : "no id"));
    if (phases.every((x) => x === "Pending" || x === "Running")) break;
    await sleep(pages[0], 1500);
  }
  log(`${label}: ${n} slow Test access checks hold the check slots: ${ids.join(", ")} (${phases.join(", ")})`);
  return { ids, phases, close: async () => { for (const p of pages) await p.close().catch(() => {}); } };
}
// The row's own check in the cluster ten seconds after it was made: Queued behind the ceiling.
async function queuedAt10(page, id, t0) {
  await sleep(page, Math.max(0, t0 + 10000 - Date.now()));
  const st = (kj("get", "preflight", id).status || {});
  return { phase: st.phase, reason: st.reason, message: String(st.message || "").slice(0, 160) };
}

const browser = await chromium.launch();
try {
  // ======================================================================== CONN
  if (GROUPS.includes("CONN")) {
    // ---------------------------------------------------------------- P15-1
    {
      const { page, context } = await newSession(browser, "operator");
      const net = capture(page);
      let slots = null;
      try {
        slots = await occupySlots(context, 4, "P15-1");
        const { since, t0, post } = await testConnection(page, net, BH_SRC, "P15-1");
        const queued = post ? await queuedAt10(page, post.id, t0) : null;
        const samples = [];
        for (const at of [30, 60, 90]) samples.push(await sampleAt(page, "#connection-check", t0, at));
        row("P15-1a Test connection on the blackhole connection, queued behind four slow checks: 'checking...' at 30, 60 and 90 s (no verdict, no stop mark)",
          !!post && post.status === 202 && !!queued && queued.phase === "Queued" && samples.every((s) => s.checking && !s.stopped && !s.settled),
          { post: post && [post.status, post.id, post.keyDigest], queuedAt10s: queued, slotHolders: slots.ids, samples });
        await shot(page, "P15-1-at-90s");
        const v = await settledRows(page, "#connection-check", 240, 1000);
        const landed = secs(t0);
        const kept = await planted(page);
        const pf = post ? preflightFacts(post.id) : {};
        row("P15-1b the recorded verdict lands at about 120-135 s WITHOUT a reload (the old follow stopped at 30 s)",
          v.settled && landed >= 95 && landed <= 240 && kept && pf.phase === "Completed" && pf.tookSeconds >= 90 && v.rows.some((r) => r.gating === "blocking" && r.verdict !== "ready"),
          { outcome: outcomeOf(v), landedSeconds: landed, sentinelKept: kept, cluster: pf, rows: brief(v.rows) });
        await shot(page, "P15-1-verdict");
        await sleep(page, 3000);
        const api = post ? apiReads(post.id, since) : { reads: [], gaps: [] };
        const c = cadenceOf(api);
        writeFileSync(`${OUT}/P15-1-api-reads.json`, JSON.stringify({ id: post && post.id, api, cadence: c, browserReads: net.reads.filter((x) => post && x.id === post.id) }, null, 1));
        row("P15-1c the product API's request log shows the follow's cadence: 2 s for 20 s, then 3, 4.5, 6.75, 10 s (each gap within 1 s), reads past the old 30 s budget until the verdict",
          c.reads >= 14 && c.within1s && c.spanSeconds >= 95 && api.reads.every((r) => r.status === 200),
          c);
      } catch (e) { row("P15-1 completed", false, { error: String(e.stack || e).slice(0, 700) }); }
      if (slots) await slots.close();
      await context.close();
    }
    // ---------------------------------------------------------------- P15-5
    {
      const { page, context } = await newSession(browser, "operator");
      const net = capture(page);
      let slots = null;
      try {
        const detail = `#/clusters?ns=${NS}&name=${BH_SRC}`;
        slots = await occupySlots(context, 4, "P15-5");
        const { since, t0, post } = await testConnection(page, net, BH_SRC, "P15-5");
        const queued = post ? await queuedAt10(page, post.id, t0) : null;
        const left = Date.now();
        // AWAY, inside the app (a route change, not a load): the Schedules page.
        await page.evaluate((h) => { location.hash = h; }, `#/schedules?ns=${NS}`);
        await waitForText(page, /Backup readiness/, 60, "the schedules page");
        await sleep(page, Math.max(0, t0 + 40000 - Date.now()));
        const back = Date.now();
        await page.evaluate((h) => { location.hash = h; }, detail);
        await waitForText(page, /TEST CONNECTION|Test connection/, 60, "the connection detail again");
        const onReturn = await checkRowsIn(page, "#connection-check");
        const v = await settledRows(page, "#connection-check", 240, 1000);
        const landed = secs(t0);
        const kept = await planted(page);
        const mine = net.reads.filter((x) => post && x.id === post.id);
        const away = mine.filter((x) => Date.parse(x.at) > left + 3000 && Date.parse(x.at) < back);
        const after = mine.filter((x) => Date.parse(x.at) >= back);
        row("P15-5 Test connection started, Schedules opened, come back within 60 s: the page follows the check again (no read while away) and shows its verdict without a reload",
          !!post && !!queued && queued.phase === "Queued" && onReturn.checking && after.length >= 2 && away.length === 0 && v.settled && kept && landed >= 95,
          { post: post && [post.status, post.id], queuedAt10s: queued, awaySeconds: Math.round((back - left) / 1000), readsWhileAway: away.length, readsAfterReturn: after.length, onReturn: { checking: onReturn.checking, stopped: onReturn.stopped },
            outcome: outcomeOf(v), landedSeconds: landed, sentinelKept: kept, rows: brief(v.rows), cluster: post ? preflightFacts(post.id) : null });
        await shot(page, "P15-5-verdict-after-remount");
        const api = post ? apiReads(post.id, since) : { reads: [], gaps: [] };
        writeFileSync(`${OUT}/P15-5-api-reads.json`, JSON.stringify({ id: post && post.id, leftAt: new Date(left).toISOString(), backAt: new Date(back).toISOString(), api, browserReads: mine }, null, 1));
      } catch (e) { row("P15-5 completed", false, { error: String(e.stack || e).slice(0, 700) }); }
      if (slots) await slots.close();
      await context.close();
    }
  }

  // ======================================================================== DISC
  if (GROUPS.includes("DISC")) {
    const { page, context } = await newSession(browser, "operator");
    const net = capture(page);
    // A reachable source whose inventory is NOT fresh, so Discover topics makes a new discovery
    // (a fresh inventory is reused, answered at once and not followed).
    const fresh = new Set(kj("get", "topicdiscoveries").items.filter((d) => Date.parse((d.status || {}).freshUntil || 0) > Date.now() - 60000)
      .map((d) => ((d.spec.request || {}).connectionRef || {}).name));
    const live = kj("get", "kafkaclusters").items.filter((k) => k.spec.role === "source" && (k.status || {}).reachable === true && !fresh.has(k.metadata.name))
      .sort((a, b) => (a.metadata.creationTimestamp < b.metadata.creationTimestamp ? 1 : -1))[0].metadata.name;
    // A slot's discovery id and state. The page prints the SECOND slot as a note when the latest
    // attempt is also the last successful inventory (`#same-discovery`), or none has ever succeeded
    // (`#no-successful`): the slot is then that note.
    const slot = (role) => page.evaluate((r) => {
      const s = document.querySelector("#discovery-" + r);
      if (!s && r === "successful" && document.querySelector("#same-discovery")) return { same: true };
      if (!s && r === "successful" && document.querySelector("#no-successful")) return { none: true };
      if (!s) return null;
      const f = {};
      for (const dt of s.querySelectorAll("dt")) { const dd = dt.nextElementSibling; if (dd) f[dt.innerText.trim()] = dd.innerText.trim(); }
      return { id: f.id || null, state: f.state || null, stopped: !!s.querySelector("[data-check-stopped]") };
    }, role);
    async function discover(name, label, want, budgetS) {
      await gotoHash(page, `#/clusters?ns=${NS}&name=${name}`);
      await waitForText(page, /Discover topics|DISCOVER TOPICS/, 60, "the connection detail");
      await plant(page);
      const before = { latest: await slot("latest"), successful: await slot("successful") };
      const since = new Date(Date.now() - 5000).toISOString();
      const t0 = Date.now();
      await page.locator("#discovery-start").click();
      const post = await postedId(page, net, new Date(t0 - 1000).toISOString(), /topic-discoveries$/);
      const seen = [];
      let now = await slot("latest");
      const end = t0 + budgetS * 1000;
      while (Date.now() < end) {
        now = await slot("latest");
        if (!seen.length || seen[seen.length - 1].state !== (now || {}).state || seen[seen.length - 1].id !== (now || {}).id) seen.push({ at: secs(t0), id: (now || {}).id, state: (now || {}).state });
        if (now && post && now.id === post.id && (want.includes(now.state) || now.stopped)) break;
        await sleep(page, 1500);
      }
      await sleep(page, 2500);
      const after = { latest: await slot("latest"), successful: await slot("successful"),
        topics: await page.evaluate(() => [...document.querySelectorAll("#topics-slot tbody tr")].map((tr) => (tr.querySelector("td") || {}).innerText || "").slice(0, 8)) };
      const td = post && post.id ? kj("get", "topicdiscovery", post.id) : null;
      const api = post && post.id ? apiReads(post.id, since) : { reads: [], gaps: [] };
      writeFileSync(`${OUT}/${label}-discovery.json`, JSON.stringify({ name, post, before, seen, after, cluster: td && td.status, api }, null, 1));
      return { post, before, seen, after, landed: secs(t0), kept: await planted(page), td: td && { phase: (td.status || {}).phase, created: td.metadata.creationTimestamp, observedAt: (td.status || {}).observedAt }, api: cadenceOf(api) };
    }
    try {
      const a = await discover(live, "P15-4a", ["succeeded"], 240);
      row("P15-4a Discover topics on a live connection: the latest attempt goes pending -> succeeded WITHOUT a reload, and the last-successful slot then names that discovery (or says the latest is also it; the stored topics are read on request, by the page's search form, so none is listed until asked)",
        !!a.post && a.post.status === 202 && a.seen.some((s) => s.id === a.post.id && s.state !== "succeeded") && (a.after.latest || {}).state === "succeeded" && (a.after.latest || {}).id === a.post.id
          && ((a.after.successful || {}).id === a.post.id || (a.after.successful || {}).same === true) && a.kept && a.api.reads >= 1,
        { connection: live, post: a.post && [a.post.status, a.post.id, a.post.keyDigest], before: a.before, seen: a.seen, after: a.after, landedSeconds: a.landed, sentinelKept: a.kept, cluster: a.td, apiReads: a.api.reads });
      await shot(page, "P15-4a-live-discovery");
      const b = await discover(BH_SRC, "P15-4b", ["failed"], 300);
      row("P15-4b Discover topics on the blackhole connection: it settles 'failed' on the page without a reload (followed past any old budget), the last successful slot untouched",
        !!b.post && b.post.status === 202 && (b.after.latest || {}).state === "failed" && (b.after.latest || {}).id === b.post.id && !(b.after.latest || {}).stopped && b.kept
          && JSON.stringify(b.after.successful) === JSON.stringify(b.before.successful) && b.api.reads >= 2,
        { post: b.post && [b.post.status, b.post.id], seen: b.seen, after: b.after, landedSeconds: b.landed, sentinelKept: b.kept, cluster: b.td, api: b.api });
      await shot(page, "P15-4b-blackhole-discovery");
    } catch (e) { row("DISC completed", false, { error: String(e.stack || e).slice(0, 700) }); }
    await context.close();
  }

  // ======================================================================== PANEL
  if (GROUPS.includes("PANEL")) {
    const { page, context } = await newSession(browser, "operator");
    const net = capture(page);
    try {
      await gotoHash(page, `#/schedules?ns=${NS}`);
      await waitForText(page, /Backup readiness/, 60, "the schedules list");
      const sopts = await page.locator("#readiness-source option").evaluateAll((os) => os.map((o) => [o.value, o.textContent.trim()]));
      const dopts = await page.locator("#readiness-destination option").evaluateAll((os) => os.map((o) => [o.value, o.textContent.trim()]));
      await page.locator("#readiness-source").selectOption(sopts.find((o) => o[1].includes(BH_SRC))[0]);
      await page.locator("#readiness-destination").selectOption(dopts.find((o) => o[1].startsWith(BH_DEST))[0]);
      await sleep(page, 2500);
      await page.locator("#readiness-topics").fill("orders");
      await plant(page);
      const since = new Date(Date.now() - 5000).toISOString();
      const t0 = Date.now();
      await page.locator("#backup-readiness").getByRole("button", { name: /^check readiness$/i }).click();
      const post = await postedId(page, net, new Date(t0 - 1000).toISOString(), /preflights$/);
      const samples = [];
      for (const at of [30, 60, 90]) samples.push(await sampleAt(page, "#backup-readiness", t0, at));
      const v = await settledRows(page, "#backup-readiness", 240, 1000);
      const landed = secs(t0);
      const text = v.text || "";
      const says = (text.match(/[^\n]*(applies to your current inputs|does not apply to your current inputs)[^\n]*/) || [""])[0];
      const kept = await planted(page);
      row("P15-2a Schedules -> Backup readiness with the blackhole source and destination: 'checking...' at 30/60/90 s, then the verdict and its applicability land without a reload (the old follow stopped at 40 s)",
        !!post && post.status === 202 && samples.every((s) => s.checking && !s.stopped) && v.settled && !!says && landed >= 95 && kept && text.includes(post.id),
        { post: post && [post.status, post.id, post.keyDigest], samples, outcome: outcomeOf(v), landedSeconds: landed, applicability: says, sentinelKept: kept, rows: brief(v.rows), cluster: post ? preflightFacts(post.id) : null });
      await shot(page, "P15-2a-panel");
      const api = post ? apiReads(post.id, since) : { reads: [], gaps: [] };
      writeFileSync(`${OUT}/P15-2a-api-reads.json`, JSON.stringify({ id: post && post.id, api, cadence: cadenceOf(api) }, null, 1));
      // ---- the schedule form's Check readiness, same source
      await gotoHash(page, `#/schedules?ns=${NS}`);
      await waitForText(page, /CHECK READINESS|Check readiness/, 60, "the schedule form");
      const form = page.locator("#schedule-form");
      const fo = await form.locator('select[name="source"] option').evaluateAll((os) => os.map((o) => [o.value, o.textContent]));
      await form.locator('select[name="source"]').selectOption(fo.find((o) => o[1].includes(BH_SRC))[0]);
      await form.locator('select[name="mode"]').selectOption("daily");
      await form.locator('input[name="hour"]').fill("2");
      await form.locator('input[name="minute"]').fill("0");
      await form.locator('select[name="selection"]').selectOption("named");
      await form.locator('input[name="topics"]').fill("orders");
      const fd = await form.locator('select[name="destination"] option').evaluateAll((os) => os.map((o) => [o.value, o.textContent]));
      await form.locator('select[name="destination"]').selectOption(fd.find((o) => o[1].startsWith(BH_DEST))[0]);
      await sleep(page, 800);
      await plant(page);
      const since2 = new Date(Date.now() - 5000).toISOString();
      const t1 = Date.now();
      await page.locator("#schedule-check-readiness").click();
      const post2 = await postedId(page, net, new Date(t1 - 1000).toISOString(), /preflights$/);
      const samples2 = [];
      for (const at of [30, 60, 90]) samples2.push(await sampleAt(page, "#schedule-readiness", t1, at));
      const v2 = await settledRows(page, "#schedule-readiness", 240, 1000);
      const landed2 = secs(t1);
      const says2 = ((v2.text || "").match(/[^\n]*(applies to your current inputs|does not apply to your current inputs)[^\n]*/) || [""])[0];
      const kept2 = await planted(page);
      row("P15-2b the schedule form's Check readiness with the blackhole source and destination: 'checking...' at 30/60/90 s, then the verdict and its applicability land without a reload",
        !!post2 && post2.status === 202 && samples2.every((s) => s.checking && !s.stopped) && v2.settled && !!says2 && landed2 >= 95 && kept2 && (v2.text || "").includes(post2.id),
        { post: post2 && [post2.status, post2.id, post2.keyDigest], samples: samples2, outcome: outcomeOf(v2), landedSeconds: landed2, applicability: says2, sentinelKept: kept2, rows: brief(v2.rows), cluster: post2 ? preflightFacts(post2.id) : null });
      await shot(page, "P15-2b-form");
      const api2 = post2 ? apiReads(post2.id, since2) : { reads: [], gaps: [] };
      writeFileSync(`${OUT}/P15-2b-api-reads.json`, JSON.stringify({ id: post2 && post2.id, api: api2, cadence: cadenceOf(api2) }, null, 1));
    } catch (e) { row("PANEL completed", false, { error: String(e.stack || e).slice(0, 700) }); }
    await context.close();
  }

  // ======================================================================== WIZ
  if (GROUPS.includes("WIZ")) {
    const { page, context } = await newSession(browser, "operator");
    const net = capture(page);
    let slots = null;
    try {
      const b = kj("get", "backups").items.filter((x) => (x.spec.scheduleRef || {}).name === "pu4-every5" && (((x.status || {}).evidence || {}).verification || {}).result === "Valid")
        .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? 1 : -1))[0];
      await openWizard(page, `#/restore?ns=${NS}&backup=${b.metadata.name}&uid=${b.metadata.uid}`);
      await wizardStep(page, 4);
      const options = await page.$$eval('select[name="targetCluster"] option', (os) => os.map((x) => [x.value, x.textContent]));
      await page.selectOption('select[name="targetCluster"]', options.find((x) => x[1].startsWith(BH_TGT + " "))[0]);
      await page.selectOption("#target-mode", "newTopic");
      await page.fill('input[name="topicPrefix"]', "pu4bh-");
      await page.locator('input[name="topicPrefix"]').blur();
      await wizardStep(page, 6);
      await sleep(page, 1500);
      slots = await occupySlots(context, 4, "P15-3");
      await wizardStep(page, 5);
      await plant(page);
      const since = new Date(Date.now() - 5000).toISOString();
      const t0 = Date.now();
      await page.click("#restore-readiness-start");
      const post = await postedId(page, net, new Date(t0 - 1000).toISOString(), /preflights$/);
      const queued = post ? await queuedAt10(page, post.id, t0) : null;
      const samples = [];
      for (const at of [30, 60, 90]) samples.push(await sampleAt(page, "#step-preflight", t0, at));
      const v = await settledRows(page, "#step-preflight", 240, 1000);
      const landed = secs(t0);
      const kept = await planted(page);
      const blocking = v.rows.filter((r) => r.gating === "blocking");
      const notReady = blocking.filter((r) => r.verdict !== "ready" && r.id !== "approval.state");
      row("P15-3a restore step 5 against the blackhole target, queued behind four slow checks: 'checking...' at 30/60/90 s, then the verdict lands ON THE STEP without a reload, after the old 90 s budget",
        !!post && post.status === 202 && !!queued && queued.phase === "Queued" && samples.every((s) => s.checking && !s.stopped) && v.settled && landed >= 95 && kept && notReady.length > 0,
        { point: b.metadata.name, post: post && [post.status, post.id, post.keyDigest], queuedAt10s: queued, slotHolders: slots.ids, samples, outcome: outcomeOf(v), landedSeconds: landed, sentinelKept: kept, notReady: notReady.map((r) => `${r.id}=${r.verdict}/${r.code}`), cluster: post ? preflightFacts(post.id) : null });
      await shot(page, "P15-3-step5-verdict");
      await wizardStep(page, 6);
      const createDisabled = await page.isDisabled("#create-restore");
      const before = new Set(kj("get", "restores").items.map((r) => r.metadata.name));
      row("P15-3b Create the Restore is gated by that verdict (disabled; no Restore made)", createDisabled && kj("get", "restores").items.every((r) => before.has(r.metadata.name)), { createDisabled });
      await shot(page, "P15-3-step6-gated");
      const api = post ? apiReads(post.id, since) : { reads: [], gaps: [] };
      writeFileSync(`${OUT}/P15-3-api-reads.json`, JSON.stringify({ id: post && post.id, api, cadence: cadenceOf(api) }, null, 1));
    } catch (e) { row("WIZ completed", false, { error: String(e.stack || e).slice(0, 700) }); }
    if (slots) await slots.close();
    await context.close();
  }

  // ======================================================================== DEST
  if (GROUPS.includes("DEST")) {
    const { page, context } = await newSession(browser, "operator");
    const net = capture(page);
    try {
      await gotoHash(page, `#/destinations?ns=${NS}&name=${BH_DEST}`);
      await waitForText(page, /Test access/, 60, "the destination detail");
      await plant(page);
      const since = new Date(Date.now() - 5000).toISOString();
      const t0 = Date.now();
      await page.locator("#destination-test").getByRole("button", { name: /test access/i }).click();
      const post = await postedId(page, net, new Date(t0 - 1000).toISOString(), /:test$|\/test$/);
      const samples = [];
      for (const at of [30, 55]) samples.push(await sampleAt(page, "#destination-test", t0, at));
      const v = await settledRows(page, "#destination-test", 240, 1000);
      const landed = secs(t0);
      const kept = await planted(page);
      const pf = post && post.id ? preflightFacts(post.id) : null;
      row("P15-7 Test access on a destination whose endpoint nothing answers: 'checking...' at 30 and 55 s, and the verdict lands after the old 60 s budget without a reload",
        !!post && post.status === 202 && samples.every((s) => s.checking && !s.stopped) && v.settled && landed > 60 && kept,
        { post: post && [post.status, post.id, post.keyDigest], samples, outcome: outcomeOf(v), landedSeconds: landed, sentinelKept: kept, rows: brief(v.rows), cluster: pf });
      await shot(page, "P15-7-test-access");
      const api = post && post.id ? apiReads(post.id, since) : { reads: [], gaps: [] };
      writeFileSync(`${OUT}/P15-7-api-reads.json`, JSON.stringify({ id: post && post.id, api, cadence: cadenceOf(api) }, null, 1));
    } catch (e) { row("DEST completed", false, { error: String(e.stack || e).slice(0, 700) }); }
    await context.close();
  }

  // ======================================================================== R3 (390 x 844)
  if (GROUPS.includes("R3")) {
    const { page, context } = await newSession(browser, "operator");
    const net = capture(page);
    await page.setViewportSize({ width: 390, height: 844 });
    // Where the focused status and the verdict headline sit against the sticky Back/Next bar, in
    // the viewport's own coordinates.
    const geometry = (id) => page.evaluate((pf) => {
      const r = (el) => { if (!el) return null; const b = el.getBoundingClientRect(); return { top: Math.round(b.top), bottom: Math.round(b.bottom), height: Math.round(b.height) }; };
      const nav = document.querySelector(".wizard-nav");
      const heads = [...document.querySelectorAll("#step-preflight .preflight-result")];
      const head = pf ? document.querySelector(`#preflight-${pf} .preflight-head`) : (heads.length ? heads[heads.length - 1].querySelector(".preflight-head") : null);
      const result = head ? head.closest(".preflight-result") : null;
      return { viewport: window.innerHeight, scrollY: Math.round(window.scrollY), docHeight: document.documentElement.scrollHeight,
        focused: (document.activeElement || {}).id || "", nav: r(nav), navPosition: nav ? getComputedStyle(nav).position : null,
        status: r(document.querySelector("#restore-readiness-status")), statusText: ((document.querySelector("#restore-readiness-status") || {}).innerText || "").slice(0, 80),
        head: r(head), headOf: result ? result.id : "", headText: head ? head.innerText.slice(0, 100) : "" };
    }, id);
    // on screen and clear of the bar; `clear` alone: not under the bar (it may have scrolled up)
    const above = (g, what) => !!g[what] && !!g.nav && g[what].top >= 0 && g[what].bottom <= g.nav.top;
    const clear = (g, what) => !!g[what] && !!g.nav && g[what].bottom <= g.nav.top;
    try {
      const b = kj("get", "backups").items.filter((x) => (x.spec.scheduleRef || {}).name === "pu4-every5" && (((x.status || {}).evidence || {}).verification || {}).result === "Valid")
        .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? 1 : -1))[0];
      const tgt = kj("get", "kafkaclusters").items.filter((k) => k.spec.role === "target" && (k.status || {}).reachable === true)
        .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? 1 : -1))[0].metadata.name;
      await openWizard(page, `#/restore?ns=${NS}&backup=${b.metadata.name}&uid=${b.metadata.uid}`);
      await wizardStep(page, 4);
      const options = await page.$$eval('select[name="targetCluster"] option', (os) => os.map((x) => [x.value, x.textContent]));
      await page.selectOption('select[name="targetCluster"]', options.find((x) => x[1].startsWith(tgt + " "))[0]);
      await page.selectOption("#target-mode", "newTopic");
      await page.fill('input[name="topicPrefix"]', "pu4r3-");
      await page.locator('input[name="topicPrefix"]').blur();
      await wizardStep(page, 6);
      await sleep(page, 1500);
      await wizardStep(page, 5);
      // R2-10 first: the source sentence, as text and as markup
      const src = await page.evaluate(() => { const p = document.querySelector("#readiness-source-destination"); return p ? { text: p.innerText, codes: [...p.querySelectorAll("code")].map((c) => c.textContent) } : null; });
      const dest = kj("get", "backupdestination", "primary");
      row("R2-10 step 5's 'This check reads the saved destination ...' has no backtick; the destination name and its location digest are code",
        !!src && !src.text.includes("`") && src.codes.includes("primary") && src.codes.some((c) => c === (b.status || {}).locationDigest || c === ((dest.status || {}).locationDigest)),
        { text: src && src.text.slice(0, 400), codes: src && src.codes, pointLocationDigest: (b.status || {}).locationDigest });
      // R3-1: Check readiness
      for (const [label, again] of [["Check readiness", false], ["Check this plan again", true]]) {
        const button = await page.locator("#restore-readiness-start").innerText();
        await page.evaluate(() => { window.__pu4Scrolls = []; if (!window.__pu4ScrollHooked) { window.__pu4ScrollHooked = true; window.addEventListener("scroll", () => window.__pu4Scrolls.push([Math.round(performance.now()), Math.round(window.scrollY)]), { passive: true }); } });
        const nReads = net.reads.length;
        const t0 = Date.now();
        const p0 = await page.evaluate(() => Math.round(performance.now()));
        await page.click("#restore-readiness-start");
        // where the focused status sits while the check runs: sampled from the click until 6 s
        const trace = [];
        for (const ms of [500, 1500, 2500, 4000, 6000]) {
          await sleep(page, Math.max(0, t0 + ms - Date.now()));
          trace.push(Object.assign({ ms }, await geometry(null)));
        }
        const post = await postedId(page, net, new Date(t0 - 1000).toISOString(), /preflights$/);
        const v = await settledRows(page, "#step-preflight", 240, 1000);
        await sleep(page, 1500);
        const atVerdict = await geometry(post && post.id);
        const painted = trace.filter((g) => g.ms >= 1500);
        const scrolls = (await page.evaluate(() => window.__pu4Scrolls || [])).map(([at, y]) => [at - p0, y]).filter(([ms]) => ms <= 8000);
        const readsMs = net.reads.slice(nReads).filter((x) => post && x.id === post.id).map((x) => [Date.parse(x.at) - t0, x.state]).filter(([ms]) => ms <= 8000);
        row(`R3-1 at 390x844, after '${label}': the focused status sits on screen above the sticky bar from the repaint until the verdict, and the verdict's headline lands on screen above the bar with the status not under it (getBoundingClientRect)`,
          (again ? /check this plan again/i.test(button) : /check readiness/i.test(button)) && trace.every((g) => g.navPosition === "sticky" && g.focused === "restore-readiness-status")
            && painted.every((g) => above(g, "status")) && v.settled && clear(atVerdict, "status") && above(atVerdict, "head") && !!post && atVerdict.headOf === `preflight-${post.id}`,
          { button, post: post && [post.status, post.id], outcome: outcomeOf(v), statusOnScreenAt: trace.map((g) => [g.ms, above(g, "status"), g.status, g.scrollY, g.docHeight]),
            scrollEventsMs: scrolls, followReadsMs: readsMs, trace, atVerdict });
        // THE BRIEF'S TWO MOMENTS, each on its own: the status right after the click's repaint, and
        // the verdict's headline (with the status not under the bar) when it lands.
        row(`R3-1 (the fixed moments) at 390x844, after '${label}': the focused status on screen above the bar after the click's repaint (0.5-1.5 s), and the verdict's headline on screen above the bar when it lands`,
          trace.filter((g) => g.ms <= 1500).every((g) => g.focused === "restore-readiness-status" && above(g, "status")) && v.settled && clear(atVerdict, "status") && above(atVerdict, "head") && !!post && atVerdict.headOf === `preflight-${post.id}`,
          { atClick: trace.filter((g) => g.ms <= 1500).map((g) => [g.ms, g.status, g.nav && g.nav.top, g.scrollY]), atVerdict: { status: atVerdict.status, head: atVerdict.head, navTop: atVerdict.nav && atVerdict.nav.top, headOf: atVerdict.headOf, scrollY: atVerdict.scrollY } });
        await page.screenshot({ path: `${OUT}/R3-1-${again ? "again" : "first"}-viewport.png` }).catch(() => {});
      }
      // R2-10's class elsewhere (an observation, not the row): lines of each wizard step with a backtick
      const ticks = {};
      for (const n of [1, 2, 3, 4, 5, 6]) {
        await wizardStep(page, n);
        ticks[n] = (await page.evaluate(() => document.querySelector("main") ? document.querySelector("main").innerText : document.body.innerText)).split("\n").filter((l) => l.includes("`")).slice(0, 5);
      }
      writeFileSync(`${OUT}/R2-10-wizard-backticks.json`, JSON.stringify(ticks, null, 1));
      log(`R2-10 observation: wizard lines with a backtick per step ${JSON.stringify(Object.fromEntries(Object.entries(ticks).map(([k, v]) => [k, v.length])))}`);
    } catch (e) { row("R3 completed", false, { error: String(e.stack || e).slice(0, 700) }); }
    await context.close();
  }

  // ======================================================================== R3ROLES
  if (GROUPS.includes("R3ROLES")) {
    const SCH = "pu4-every5";
    const offers = (page, scope) => page.evaluate((sel) => {
      const root = document.querySelector(sel) || document.body;
      return { links: [...root.querySelectorAll("a")].filter((a) => /^\s*Restore this point\s*$/i.test(a.textContent)).map((a) => a.getAttribute("href")).slice(0, 3),
        linkCount: [...root.querySelectorAll("a")].filter((a) => /^\s*Restore this point\s*$/i.test(a.textContent)).length,
        refused: [...root.querySelectorAll('[data-restore-refused="role"]')].map((x) => x.textContent.trim()).slice(0, 2),
        refusedCount: root.querySelectorAll('[data-restore-refused="role"]').length };
    }, scope);
    const latestLine = (page) => page.evaluate(() => { const p = document.querySelector("#schedule-latest-point"); return p ? { text: p.innerText, link: !!p.querySelector("a"), href: (p.querySelector("a") || { getAttribute: () => null }).getAttribute("href") } : null; });
    const SENT = "an operator or administrator can restore this point";
    const surfaces = [
      ["Catalog", `#/catalog?ns=${NS}&name=archive`, /restore this point/i, "main"],
      ["the History list (history.js restorePointCell, the runs list with a RESTORE column)", `#/history?ns=${NS}`, /restore this point/i, "main"],
      ["a schedule's points", `#/schedules?ns=${NS}&name=${SCH}`, /restore this point/i, "main"],
    ];
    const seen = {};
    for (const role of ["viewer", "operator"]) {
      const { page, context } = await newSession(browser, role);
      seen[role] = {};
      try {
        // #/backups prints no restore action for any role (backups.js): recorded, not a row
        await gotoHash(page, `#/backups?ns=${NS}`);
        await waitForText(page, /logweir-backup-|No backup/, 90, "the Backups list");
        await sleep(page, 1500);
        seen[role].backupsPage = await offers(page, "main");
        for (const [what, hash, ready, scope] of surfaces) {
          await gotoHash(page, hash);
          await waitForText(page, ready, 90, what);
          await sleep(page, 2500);
          seen[role][what] = await offers(page, scope);
          if (what === "a schedule's points") seen[role].latest = await latestLine(page);
          await shot(page, `R3-2-${role}-${what.replace(/[^a-z]+/gi, "-")}`);
        }
        if (role === "viewer") {
          for (const [what] of surfaces) {
            const o = seen.viewer[what];
            row(`R3-2 as viewer, ${what}: "an operator or administrator can restore this point" and no 'Restore this point' link`,
              o.linkCount === 0 && o.refusedCount > 0 && o.refused.every((t) => t.toLowerCase().startsWith(SENT)), o);
          }
          const l = seen.viewer.latest;
          row("R3-2 as viewer, the schedule's detail (its latest recovery point line): 'An operator or administrator can restore this point.' and no link",
            !!l && !l.link && l.text.includes("An operator or administrator can restore this point."), l);
        } else {
          for (const [what, hash, ready] of surfaces) {
            const o = seen.operator[what];
            let opened = null;
            if (o.linkCount > 0) {
              await gotoHash(page, hash);
              await waitForText(page, ready, 90, what);
              await sleep(page, 2000);
              const link = page.locator("main a", { hasText: /^\s*Restore this point\s*$/ }).first();
              await revealInGrid(page, link, "");
              await link.click();
              await page.waitForURL(/#\/restore\?/, { timeout: 30000 }).catch(() => {});
              await sleep(page, 2500);
              opened = await page.evaluate(() => ({ hash: location.hash.slice(0, 120), position: ((document.querySelector("#wizard-position") || {}).textContent || "").trim(), refusal: !!document.querySelector("#role-refusal") }));
            }
            row(`R3-2 as operator, ${what}: the 'Restore this point' links are there, and one opens the wizard`,
              o.linkCount > 0 && o.refusedCount === 0 && !!opened && /^#\/restore\?/.test(opened.hash) && /^Step \d of 6: /.test(opened.position) && !opened.refusal, { offers: o, opened });
          }
          const l = seen.operator.latest;
          let opened = null;
          if (l && l.link) {
            await gotoHash(page, `#/schedules?ns=${NS}&name=${SCH}`);
            await waitForText(page, /Latest recovery point/, 90, "the schedule detail");
            await page.locator("#schedule-latest-point a").first().click();
            await page.waitForURL(/#\/restore\?/, { timeout: 30000 }).catch(() => {});
            await sleep(page, 2500);
            opened = await page.evaluate(() => ({ hash: location.hash.slice(0, 120), position: ((document.querySelector("#wizard-position") || {}).textContent || "").trim() }));
          }
          row("R3-2 as operator, the schedule's latest recovery point line carries the link, and it opens the wizard",
            !!l && l.link && !l.text.includes("An operator or administrator can restore") && !!opened && /^Step \d of 6: /.test(opened.position), { line: l, opened });
        }
      } catch (e) { row(`R3-2 ${role} completed`, false, { error: String(e.stack || e).slice(0, 700) }); }
      await context.close();
    }
    writeFileSync(`${OUT}/R3-2-offers.json`, JSON.stringify(seen, null, 1));
    // R3-3
    const s = await newSession(browser, "norole");
    try {
      for (const hash of ["", `#/backups?ns=${NS}`]) {
        await gotoHash(s.page, hash);
        await s.page.waitForSelector("#session-identity", { timeout: 30000 }).catch(() => {});
        await sleep(s.page, 1500);
        const h = await s.page.evaluate(() => ({ role: ((document.querySelector("#session-role") || {}).innerText || "").trim(), header: ((document.querySelector("#session-identity") || {}).innerText || "").trim(), card: ((document.querySelector("#no-role h2") || {}).innerText || "").trim() }));
        row(`R3-3 norole${hash ? " on " + hash : " on the landing"}: the header's role line reads 'no role yet', never 'choose a namespace to see your role'`,
          h.role === "no role yet" && !/choose a namespace to see your role/i.test(h.header), h);
        await shot(s.page, `R3-3-norole${hash ? "-backups" : ""}`);
      }
    } catch (e) { row("R3-3 completed", false, { error: String(e.stack || e).slice(0, 700) }); }
    await s.context.close();
  }
} catch (e) {
  row("round4 completed every step", false, { error: String((e && e.stack) || e).slice(0, 800) });
} finally {
  await browser.close();
  const failed = ROWS.filter((r) => !r.pass);
  log(`${ROWS.length - failed.length}/${ROWS.length} rows pass${failed.length ? "; FAILED: " + failed.map((r) => r.id).join(" | ") : ""}`);
  process.exitCode = failed.length ? 1 : 0;
}
