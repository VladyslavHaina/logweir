// P10 row 6 on the PoC install (claude/manual-run-bound): three approved (Ordinary) restores
// created at once through the console wizard, in a real Chromium through Traefik and Dex.
//
//   NODE_PATH="$(npm root -g)" node scripts/live/poc/restore_burst.mjs <outdir> [n=3]
//
// Each of n pages runs the wizard for its own recovery point to a ready readiness verdict; then
// all n "Create the Restore" clicks fire together. Every 2 s until every restore is terminal the
// row samples the restore Jobs' pods and the Restores: at most 2 run at once, the rest read
// `phase: Queued`, `reason: ConcurrencyLimited`, `status.queue.limit: 2` and
// `status.queue.authorizationExpiresAt`; a console page shows "Queued (limit 2 active; approval
// expires T)" for a queued one. Then the per-person restore window: the first create's own
// request is REPLAYED from the page (same Idempotency-Key and body: a replay counts toward the
// window and never creates a restore) until the first 429, whose Retry-After is recorded. The
// window is per console process (docs/api.md, Rate limits). Nothing secret is printed.
import { writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { chromium, newSession, gotoHash, textOf, waitForText } from "./console.mjs";

const OUT = process.argv[2] || "/tmp/poc-restore-burst";
const N = Number(process.argv[3] || 3);
const NS = process.env.POC_NAMESPACE || "logweir-poc";
mkdirSync(OUT, { recursive: true });
const ROWS = [];
const log = (m) => console.log(new Date().toISOString(), m);
function row(id, ok, evidence) {
  ROWS.push({ id, pass: !!ok, evidence });
  log(`${ok ? "PASS" : "FAIL"} ${id} ${JSON.stringify(evidence).slice(0, 500)}`);
  writeFileSync(`${OUT}/rows.json`, JSON.stringify(ROWS, null, 1));
}
const kj = (...args) => JSON.parse(execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=30s", "-n", NS, ...args, "-o", "json"], { timeout: 45000, maxBuffer: 256 * 1024 * 1024 }).toString());
const cond = (o, t) => (((o.status || {}).conditions) || []).find((c) => c.type === t) || {};
const TERMINAL = new Set(["Succeeded", "Failed", "Cancelled", "Refused"]);

async function prepare(page, b, target, prefix) {
  await gotoHash(page, `#/restore?ns=${NS}&backup=${encodeURIComponent(b.metadata.name)}&uid=${encodeURIComponent(b.metadata.uid)}`);
  await waitForText(page, /6\. Plan, hash and names/, 60, "the wizard");
  const options = await page.$$eval('select[name="targetCluster"] option', (os) => os.map((x) => [x.value, x.textContent]));
  const hit = options.find((x) => x[1].startsWith(target + " "));
  await page.selectOption('select[name="targetCluster"]', hit[0]);
  await page.selectOption('select[name="mode"]', "newTopic");
  await page.fill('input[name="topicPrefix"]', prefix);
  await page.locator('input[name="topicPrefix"]').blur();
  await page.waitForTimeout(1500);
  await page.click("#restore-readiness-start");
  const end = Date.now() + 240000;
  while (Date.now() < end) {
    await page.waitForTimeout(4000);
    const t = await textOf(page);
    const s5 = t.slice(t.indexOf("5. Operation readiness"), t.indexOf("6. Plan, hash and names"));
    const rows = [...s5.matchAll(/^([a-zA-Z]+\.[a-zA-Z]+)\t([^\t]+)\t(blocking|advisory|executionOnly)\t([A-Za-z]+)/gm)].map((m) => ({ id: m[1], verdict: m[2], gating: m[3] }));
    if (rows.length > 0 && !rows.some((r) => /pending|running/i.test(r.verdict))) {
      return rows.filter((r) => r.gating === "blocking" && r.verdict !== "ready" && r.id !== "approval.state");
    }
  }
  throw new Error(`no readiness verdict for ${b.metadata.name}`);
}

const browser = await chromium.launch();
try {
  const { context, page: first } = await newSession(browser, "operator");
  const target = kj("get", "kafkaclusters").items.find((i) => i.spec.role === "target" && (i.status || {}).reachable !== false).metadata.name;
  const points = kj("get", "backups").items
    .filter((x) => (x.spec.destinationRef || {}).name === "primary" && (x.status || {}).phase === "Succeeded" && (((x.status || {}).evidence || {}).verification || {}).result === "Valid")
    .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? 1 : -1)).slice(1, 1 + N);
  const pages = [first];
  for (let i = 1; i < N; i++) pages.push(await context.newPage());
  const stamp = Date.now().toString(36).slice(-4);
  const posts = [];
  for (const p of pages) {
    p.on("request", (r) => {
      if (r.method() === "POST" && /\/api\/v1\/namespaces\/[^/]+\/restores$/.test(new URL(r.url()).pathname)) {
        posts.push({ url: r.url(), headers: r.headers(), body: r.postData(), at: new Date().toISOString() });
      }
    });
  }
  const notReady = [];
  for (let i = 0; i < N; i++) notReady.push(await prepare(pages[i], points[i], target, `q${i + 1}${stamp}-`));
  row(`P10.6 ${N} wizards reach a ready verdict (every blocking row but approval.state)`, notReady.every((x) => x.length === 0),
    { points: points.map((p) => p.metadata.name), notReady });
  for (const p of pages) if (await p.isDisabled("#create-restore")) throw new Error("Create the Restore is disabled");
  const before = new Set(kj("get", "restores").items.map((r) => r.metadata.name));
  const clickedAt = new Date().toISOString();
  await Promise.all(pages.map((p) => p.click("#create-restore")));
  await Promise.all(pages.map((p) => p.waitForURL(/#\/(operations|approvals|history)/, { timeout: 60000 }).catch(() => {})));
  let mine = [];
  for (let i = 0; i < 15 && mine.length < N; i++) {
    mine = kj("get", "restores").items.filter((r) => !before.has(r.metadata.name)).map((r) => r.metadata.name);
    if (mine.length < N) await first.waitForTimeout(1000);
  }
  row(`P10.6 ${N} Restores created together`, mine.length === N, { restores: mine, clickedAt, posts: posts.length });
  // ---- sample until terminal
  const samples = [];
  let consoleShot = null;
  const viewer = pages[N - 1];
  const end = Date.now() + 900000;
  while (Date.now() < end) {
    const rs = kj("get", "restores").items.filter((r) => mine.includes(r.metadata.name));
    const pods = kj("get", "pods").items.filter((p) => ["Pending", "Running"].includes((p.status || {}).phase) && mine.includes(((p.metadata.labels || {})["job-name"]) || ""));
    const s = {
      t: new Date().toISOString().slice(11, 19),
      restoreJobPods: [...new Set(pods.map((p) => p.metadata.labels["job-name"]))],
      restores: rs.map((r) => ({ name: r.metadata.name, phase: (r.status || {}).phase, reason: (r.status || {}).reason, queue: (r.status || {}).queue || null,
        admitted: [cond(r, "Admitted").status, cond(r, "Admitted").reason] })),
    };
    samples.push(s);
    writeFileSync(`${OUT}/samples.json`, JSON.stringify(samples, null, 1));
    const q = s.restores.find((r) => r.phase === "Queued");
    if (q && !consoleShot) {
      await gotoHash(viewer, `#/history?ns=${NS}&name=${encodeURIComponent(q.name)}`);
      await viewer.waitForTimeout(1500);
      const t = await textOf(viewer);
      consoleShot = { name: q.name, text: (t.match(/[^\n]*Queued \(limit[^\n]*/) || [""])[0], sentence: /waiting for a slot/.test(t) };
      await viewer.screenshot({ path: `${OUT}/queued-restore-history.png`, fullPage: true });
      await gotoHash(viewer, `#/operations?ns=${NS}&kind=restore&name=${encodeURIComponent(q.name)}`);
      await viewer.waitForTimeout(1500);
      const t2 = await textOf(viewer);
      consoleShot.operation = (t2.match(/[^\n]*[Qq]ueued[^\n]*/) || [""])[0];
      await viewer.screenshot({ path: `${OUT}/queued-restore-operation.png`, fullPage: true });
    }
    if (rs.length === N && rs.every((r) => TERMINAL.has((r.status || {}).phase))) break;
    await first.waitForTimeout(2000);
  }
  const peak = Math.max(...samples.map((s) => s.restoreJobPods.length));
  row("P10.6 at most 2 restore Jobs' pods run at once", peak <= 2, { peak, samples: samples.length });
  const queued = samples.flatMap((s) => s.restores.filter((r) => r.phase === "Queued"));
  const q0 = queued[0] || {};
  row("P10.6 the third restore is Queued, reason ConcurrencyLimited, queue.limit 2, with the approval deadline on the object",
    queued.length > 0 && q0.reason === "ConcurrencyLimited" && (q0.queue || {}).limit === 2 && !!(q0.queue || {}).authorizationExpiresAt
      && q0.admitted[0] === "False" && q0.admitted[1] === "ConcurrencyLimited",
    { first: q0, observations: queued.length, distinct: [...new Set(queued.map((r) => r.name))] });
  row("P10.6 the console shows 'Queued (limit 2 active; approval expires T)' for it", !!consoleShot && /Queued \(limit 2 active; approval expires /.test(consoleShot.text),
    consoleShot || { note: "no Queued restore observed" });
  const final = kj("get", "restores").items.filter((r) => mine.includes(r.metadata.name));
  row(`P10.6 all ${N} restores then run to Succeeded`, final.every((r) => (r.status || {}).phase === "Succeeded"),
    final.map((r) => ({ name: r.metadata.name, phase: (r.status || {}).phase, verification: ((((r.status || {}).evidence || {}).verification) || {}).result, completion: !!(r.status || {}).completion, approval: (r.spec.approvalRef || {}).name })));
  // ---- the per-person restore window (5/min), with replays of the first create
  const p0 = posts[0];
  const results = [];
  if (p0) {
    for (let i = 0; i < 14; i++) {
      const r = await first.evaluate(async ({ url, headers, body }) => {
        const h = {};
        for (const k of ["content-type", "idempotency-key", "x-csrf-token"]) if (headers[k]) h[k] = headers[k];
        const resp = await fetch(url, { method: "POST", headers: h, body, credentials: "same-origin" });
        let code = null;
        try { const j = await resp.json(); code = (j.error && j.error.code) || j.code || (j.replayed === true ? "replayed" : null); } catch (e) { code = null; }
        return { status: resp.status, retryAfter: resp.headers.get("retry-after"), code };
      }, p0);
      results.push({ request: N + i + 1, ...r });
      if (r.status === 429) break;
    }
  }
  const countAfter = kj("get", "restores").items.length;
  const hit = results.find((r) => r.status === 429);
  row("P10.6 a later restore create from the same person within the minute is 429 rate_limited with Retry-After (replays count), creating nothing",
    !!hit && hit.code === "rate_limited" && Number(hit.retryAfter) >= 1 && Number(hit.retryAfter) <= 60 && countAfter === before.size + N,
    { results, restoresBefore: before.size, restoresAfter: countAfter });
  writeFileSync(`${OUT}/facts.json`, JSON.stringify({ restores: mine, points: points.map((p) => p.metadata.name), clickedAt, consoleShot, replays: results }, null, 1));
} catch (e) {
  row("restore burst completed every step", false, { error: String((e && e.stack) || e).slice(0, 600) });
} finally {
  await browser.close();
  const failed = ROWS.filter((r) => !r.pass);
  log(`${ROWS.length - failed.length}/${ROWS.length} rows pass${failed.length ? "; FAILED: " + failed.map((r) => r.id).join(" | ") : ""}`);
  process.exitCode = failed.length ? 1 : 0;
}
