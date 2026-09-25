// The fifth PoC round's console rows (poc-upgrade-5), driven in a real Chromium through Traefik and
// Dex against the deployed shared console. Each row REQUIRES the outcome it records. The rows are
// the ones claude/poc-fixes-6 named for the next PoC round (its §9, "Rows for the next PoC round",
// and its fix round's "Remaining"), measured with round4.mjs's `geometry()`:
//
//   R31    R3-1 at 390x844, restore step 5: STRICT after Check readiness and after Check this plan
//          again (the focused status on screen above the sticky Back/Next bar at 0.5/1.5/2.5/4/6 s,
//          no scroll event after the click's, scrollY at 2.5-6 s equal to the 1.5 s value, and at
//          the verdict the head above the bar with the status not under it); UNDER-BAR (the status
//          scrolled 1-50 px under the bar before the click is above it right after); the reader
//          SCROLLS AWAY to 0 at 1.5 s and stays at 0; the reader scrolls the status BEHIND THE BAR
//          at 1.5 s and stays there. Each needs the check to be followed while it runs: a row
//          requires a non-terminal follow read between 1.5 and 6 s and the verdict after 6 s.
//   CLASS  P16's class at 390x844: Test connection and Discover topics on a connection detail
//          scrolled so the button is mid-screen (scrollY unchanged at 0.7-6 s, focus on
//          connection-check-status / discovery-start); Test access (focus on
//          destination-test-status at 0.7-6 s) and the schedule form's Check readiness (focus on
//          schedule-readiness-verdict at 0.7-6 s), both on the blackhole destination.
//   NAV    The fix round's rows: Next and Back from the bottom of steps 1, 2 and 5 land the new
//          step's heading at about 25 px, at 390x844 AND 1440x900; a `&step=5` deep link lands at
//          the heading at both; a page navigation from a scrolled long page (Backups -> History,
//          History -> Schedules, a backup's detail link from the bottom row) opens at 0; the
//          namespace picker's Go opens at 0.
//   O2     The Catalog: the list, one catalog's status and its points with a next cursor print no
//          backtick, and `logweir catalog list` is code.
//
//   NODE_PATH="$(npm root -g)" node scripts/live/poc/round5.mjs <outdir> <GROUP>[,<GROUP>...]
//
// The blackhole fixtures (KafkaCluster <P>-bh-source, BackupDestination <P>-bh-dest, P =
// POC_BH_PREFIX, default pu5, labelled logweir.dev/test-owner=poc-upgrade-5) and the five-minute
// schedule POC_TEST_SCHEDULE (default pu5-every5) are the report's, made with kubectl before this
// runs. Every group signs in fresh, so each fits the 900 s session. Passwords come from the
// credentials file and are typed into Dex; nothing secret is printed. Idempotency keys are recorded
// as a digest only; the CSRF token is never read.
//
// HOW A CLICK IS MADE. Where the row is about the page's own scroll, the click is a mouse click at
// the control's centre while it is on screen (`clickInPlace`), because Playwright's locator click
// scrolls a control into view first and would move the page the row measures. A navigation "from a
// scrolled page" through a link or form that is off screen (the masthead's nav and namespace
// picker are not sticky) is made with the element's own `click()` / `requestSubmit()` while the
// page stays scrolled, and the row says so.
import { writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chromium, newSession, gotoHash, waitForText, openWizard, wizardStep, wizardAt,
  settledRows, outcomeOf,
} from "./console.mjs";

const OUT = process.argv[2] || "/tmp/poc-round5";
const GROUPS = (process.argv[3] || "R31").split(",");
const NS = process.env.POC_NAMESPACE || "logweir-poc";
const P = process.env.POC_BH_PREFIX || "pu5";
const BH_SRC = `${P}-bh-source`, BH_DEST = `${P}-bh-dest`;
const SCHEDULE = process.env.POC_TEST_SCHEDULE || "pu5-every5";
mkdirSync(OUT, { recursive: true });
const ROWS = [];
const TAG = GROUPS.join("_");
const log = (m) => console.log(new Date().toISOString(), m);
function row(id, ok, evidence) {
  ROWS.push({ id, pass: !!ok, at: new Date().toISOString(), evidence });
  log(`${ok ? "PASS" : "FAIL"} ${id} ${JSON.stringify(evidence).slice(0, 1200)}`);
  writeFileSync(`${OUT}/rows-${TAG}.json`, JSON.stringify(ROWS, null, 1));
}
const kj = (...args) => JSON.parse(execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=30s", "-n", NS, ...args, "-o", "json"], { timeout: 45000, maxBuffer: 256 * 1024 * 1024 }).toString());
const digest = (s) => (s ? createHash("sha256").update(s).digest("hex").slice(0, 12) : null);
const nowIso = () => new Date().toISOString();
const sleep = (page, ms) => page.waitForTimeout(ms);
const until = (page, t0, ms) => sleep(page, Math.max(0, t0 + ms - Date.now()));

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
// The follow's reads of `id` since `t0`, as [ms after t0, state, terminal].
const readsOf = (net, id, t0) => net.reads.filter((x) => x.id === id).map((x) => [Date.parse(x.at) - t0, x.state, x.terminal === true]);

// Scroll events and repaints (a batch of the view's children swapped), in ms of the page's own
// clock; `mark()` answers that clock so the harness can express both relative to its click.
async function hook(page) {
  await page.evaluate(() => {
    window.__pu5Scrolls = [];
    window.__pu5Swaps = [];
    if (!window.__pu5Hooked) {
      window.__pu5Hooked = true;
      window.addEventListener("scroll", () => window.__pu5Scrolls.push([Math.round(performance.now()), Math.round(window.scrollY)]), { passive: true });
      new MutationObserver((ms) => {
        const removed = ms.reduce((n, m) => n + m.removedNodes.length, 0);
        if (removed > 0) window.__pu5Swaps.push([Math.round(performance.now()), removed]);
      }).observe(document.querySelector("main") || document.body, { childList: true, subtree: true });
    }
  });
  return page.evaluate(() => Math.round(performance.now()));
}
const events = async (page, p0) => page.evaluate((z) => ({
  scrolls: (window.__pu5Scrolls || []).map(([at, y]) => [at - z, y]),
  swaps: (window.__pu5Swaps || []).map(([at, n]) => [at - z, n]),
}), p0);

// A mouse click at the control's centre while it is on screen (and above the wizard bar when there
// is one), so the click itself scrolls nothing; the row's evidence says which way it was made.
async function clickInPlace(page, selector) {
  const at = await page.evaluate((sel) => {
    const el = document.querySelector(sel);
    if (!el) return null;
    const b = el.getBoundingClientRect();
    const nav = document.querySelector(".wizard-nav");
    const floor = nav ? nav.getBoundingClientRect().top : window.innerHeight;
    return { x: b.left + b.width / 2, y: b.top + b.height / 2, onScreen: b.top >= 0 && b.bottom <= floor };
  }, selector);
  if (at && at.onScreen) {
    await page.mouse.click(at.x, at.y);
    return "mouse, in place";
  }
  await page.locator(selector).click();
  return "locator (scrolled into view first)";
}

// ------------------------------------------------------------------ round4.mjs's geometry()
// Where the focused status and the verdict headline sit against the sticky Back/Next bar, in the
// viewport's own coordinates (copied from round4.mjs's R3 group, unchanged).
const geometry = (page, id) => page.evaluate((pf) => {
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

// The newest Valid point of the test schedule (else of any schedule), for the wizard.
function newestPoint() {
  const valid = kj("get", "backups").items.filter((x) => (((x.status || {}).evidence || {}).verification || {}).result === "Valid")
    .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? 1 : -1));
  return valid.find((x) => (x.spec.scheduleRef || {}).name === SCHEDULE) || valid[0];
}
const wizardHash = (b, extra) => `#/restore?ns=${NS}&backup=${b.metadata.name}&uid=${b.metadata.uid}${extra || ""}`;

const browser = await chromium.launch();
try {
  // ======================================================================== R31 (390 x 844)
  if (GROUPS.includes("R31")) {
    const { page, context } = await newSession(browser, "operator");
    const net = capture(page);
    await page.setViewportSize({ width: 390, height: 844 });
    try {
      const b = newestPoint();
      const tgt = kj("get", "kafkaclusters").items.filter((k) => k.spec.role === "target" && (k.status || {}).reachable === true)
        .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? 1 : -1))[0].metadata.name;
      await openWizard(page, wizardHash(b));
      await wizardStep(page, 4);
      const options = await page.$$eval('select[name="targetCluster"] option', (os) => os.map((x) => [x.value, x.textContent]));
      await page.selectOption('select[name="targetCluster"]', options.find((x) => x[1].startsWith(tgt + " "))[0]);
      await page.selectOption("#target-mode", "newTopic");
      await page.fill('input[name="topicPrefix"]', "pu5r3-");
      await page.locator('input[name="topicPrefix"]').blur();
      await wizardStep(page, 6);
      await sleep(page, 1500);
      await wizardStep(page, 5);
      await sleep(page, 1000);
      log(`R31: point ${b.metadata.name}, target ${tgt}`);

      // One readiness click on step 5 and what the page does until its verdict. `prepare` runs
      // before the click (and answers what it set up); `reader` runs at 1.5 s (the reader's own
      // scroll, answering the offset it scrolled to).
      async function readinessRun(label, prepare, reader) {
        const button = (await page.locator("#restore-readiness-start").innerText()).trim();
        const prepared = prepare ? await prepare() : null;
        await sleep(page, 400);
        const before = await geometry(page, null);
        const p0 = await hook(page);
        const nReads = net.reads.length;
        const t0 = Date.now();
        const how = await clickInPlace(page, "#restore-readiness-start");
        const trace = [];
        let readerAt = null;
        for (const ms of [500, 1500, 2500, 4000, 6000]) {
          await until(page, t0, ms);
          trace.push(Object.assign({ ms }, await geometry(page, null)));
          if (ms === 1500 && reader) readerAt = await reader(trace[trace.length - 1]);
        }
        const post = await postedId(page, net, new Date(t0 - 1000).toISOString(), /preflights$/);
        const v = await settledRows(page, "#step-preflight", 240, 1000);
        const verdictMs = Date.now() - t0;
        await sleep(page, 1500);
        const atVerdict = await geometry(page, post && post.id);
        const ev = await events(page, p0);
        const reads = post ? readsOf({ reads: net.reads.slice(nReads) }, post.id, t0) : [];
        const followed = reads.some(([ms, , term]) => ms > 1500 && ms <= 6000 && !term) && verdictMs > 6000;
        await page.screenshot({ path: `${OUT}/R31-${label.replace(/[^a-z0-9]+/gi, "-")}.png` }).catch(() => {});
        return { label, button, how, prepared, readerAt, before, trace, post: post && [post.status, post.id, post.keyDigest], postId: post && post.id,
          outcome: outcomeOf(v), settled: v.settled, verdictMs, atVerdict, scrollEventsMs: ev.scrolls.filter(([ms]) => ms >= -50 && ms <= verdictMs + 2000),
          swapsMs: ev.swaps.filter(([ms]) => ms >= -50 && ms <= verdictMs + 2000), followReadsMs: reads.filter(([ms]) => ms <= verdictMs + 2000), followedWhileRunning: followed };
      }
      const brief = (r) => ({ button: r.button, how: r.how, prepared: r.prepared, readerAt: r.readerAt, post: r.post, outcome: r.outcome, verdictMs: r.verdictMs, followedWhileRunning: r.followedWhileRunning,
        statusAt: r.trace.map((g) => [g.ms, g.focused, g.status && [g.status.top, g.status.bottom], g.nav && g.nav.top, g.scrollY, g.docHeight]),
        scrollEventsMs: r.scrollEventsMs, swapsMs: r.swapsMs.slice(0, 20), followReadsMs: r.followReadsMs.slice(0, 12),
        atVerdict: { status: r.atVerdict.status, head: r.atVerdict.head, navTop: r.atVerdict.nav && r.atVerdict.nav.top, headOf: r.atVerdict.headOf, scrollY: r.atVerdict.scrollY, focused: r.atVerdict.focused } });
      const verdictOk = (r) => r.settled && clear(r.atVerdict, "status") && above(r.atVerdict, "head") && !!r.postId && r.atVerdict.headOf === `preflight-${r.postId}`;
      const runs = [];

      // ---- R3-1 strict, twice
      for (const [label, want] of [["Check readiness", /check readiness/i], ["Check this plan again", /check this plan again/i]]) {
        const r = await readinessRun(`strict ${label}`, null, null);
        runs.push(r);
        const y15 = r.trace.find((g) => g.ms === 1500).scrollY;
        const lateMoves = r.scrollEventsMs.filter(([ms, y]) => ms > 1500 && ms <= 6000 && y !== y15);
        row(`R3-1 strict at 390x844, after '${label}': focus on restore-readiness-status and the status on screen above the bar (top >= 0, bottom <= nav.top) at 0.5/1.5/2.5/4/6 s; no scroll event to 0 after the click's; scrollY at 2.5-6 s equal to the 1.5 s value; at the verdict the head above the bar and the status not under it; the check followed while it ran`,
          want.test(r.button) && r.followedWhileRunning && r.trace.every((g) => g.navPosition === "sticky" && g.focused === "restore-readiness-status" && above(g, "status"))
            && !r.scrollEventsMs.some(([ms, y]) => ms > 1000 && y === 0 && y15 !== 0) && lateMoves.length === 0
            && r.trace.filter((g) => g.ms >= 2500).every((g) => g.scrollY === y15) && verdictOk(r),
          Object.assign(brief(r), { y15, lateMoves }));
      }

      // ---- R3-1 under-bar: before the click, the status 1-50 px under the bar (still in the viewport)
      {
        const r = await readinessRun("under-bar", async () => {
          const g = await geometry(page, null);
          const want = g.nav.top + 25; // the status's bottom, 25 px below the bar's top
          await page.evaluate((dy) => window.scrollBy(0, dy), g.status.bottom - want);
          await sleep(page, 400);
          const h = await geometry(page, null);
          return { scrollY: h.scrollY, status: h.status, navTop: h.nav.top, underBy: h.status.bottom - h.nav.top };
        }, null);
        runs.push(r);
        const staged = !!r.prepared && r.prepared.underBy >= 1 && r.prepared.underBy <= 50 && r.prepared.status.bottom <= r.before.viewport;
        const atClick = r.trace.filter((g) => g.ms <= 1500);
        row("R3-1 under-bar at 390x844: with the status's own position 1-50 px under the bar before 'Check this plan again', right after the click (0.5 and 1.5 s) the status's bottom is <= nav.top and its top >= 0, focus on it; it stays above the bar through 6 s; the check followed while it ran",
          staged && r.followedWhileRunning && atClick.every((g) => g.focused === "restore-readiness-status" && above(g, "status")) && r.trace.every((g) => above(g, "status")) && verdictOk(r),
          Object.assign(brief(r), { staged }));
      }

      // ---- R3-1 reader scrolls away to 0 at 1.5 s
      {
        const r = await readinessRun("reader-to-0", null, async () => { await page.evaluate(() => window.scrollTo(0, 0)); await sleep(page, 150); return Math.round(await page.evaluate(() => window.scrollY)); });
        runs.push(r);
        const late = r.trace.filter((g) => g.ms >= 2500);
        const pulled = r.scrollEventsMs.filter(([ms, y]) => ms > 1700 && ms <= 6000 && y !== 0);
        row("R3-1 reader-scrolls-away at 390x844: after 'Check this plan again' the reader scrolls to 0 at 1.5 s; scrollY stays 0 at 2.5/4/6 s (no pull-back while the check runs); when the verdict lands its head is brought above the bar (L7); the check followed while it ran",
          r.readerAt === 0 && r.followedWhileRunning && late.every((g) => g.scrollY === 0) && pulled.length === 0 && r.settled && above(r.atVerdict, "head"),
          Object.assign(brief(r), { pulled }));
      }

      // ---- R3-1 reader scrolls the status behind the bar at 1.5 s (poc-fixes-6 fix round, MEDIUM)
      {
        const r = await readinessRun("reader-behind-bar", null, async (g) => {
          // move the page so the status's top sits 20 px below the bar's top: wholly behind it
          await page.evaluate((dy) => window.scrollBy(0, dy), g.status.top - (g.nav.top + 20));
          await sleep(page, 150);
          const h = await geometry(page, null);
          return { scrollY: h.scrollY, status: h.status, navTop: h.nav.top };
        });
        runs.push(r);
        const y = r.readerAt && r.readerAt.scrollY;
        const behind = !!r.readerAt && r.readerAt.status.top >= r.readerAt.navTop && r.readerAt.status.bottom <= r.trace[0].viewport;
        const late = r.trace.filter((g) => g.ms >= 2500);
        const pulled = r.scrollEventsMs.filter(([ms, sy]) => ms > 1700 && ms <= 6000 && sy !== y);
        row("R3-1 reader-behind-the-bar at 390x844: after 'Check this plan again' the reader scrolls the status wholly behind the bar at 1.5 s; the page stays at that offset at 2.5/4/6 s (no pull-back); when the verdict lands its head is brought above the bar (L7); the check followed while it ran",
          behind && r.followedWhileRunning && late.every((g) => g.scrollY === y) && pulled.length === 0 && r.settled && above(r.atVerdict, "head"),
          Object.assign(brief(r), { behind, pulled }));
      }
      writeFileSync(`${OUT}/R31-runs.json`, JSON.stringify(runs, null, 1));
    } catch (e) { row("R31 completed", false, { error: String(e.stack || e).slice(0, 700) }); }
    await context.close();
  }

  // ======================================================================== CLASS (390 x 844)
  if (GROUPS.includes("CLASS")) {
    const { page, context } = await newSession(browser, "operator");
    const net = capture(page);
    await page.setViewportSize({ width: 390, height: 844 });
    const place = (sel) => page.evaluate((s) => {
      const el = document.querySelector(s);
      const b = el ? el.getBoundingClientRect() : null;
      return { scrollY: Math.round(window.scrollY), docHeight: document.documentElement.scrollHeight, focused: (document.activeElement || {}).id || (document.activeElement || {}).tagName || "",
        target: b ? [Math.round(b.top), Math.round(b.bottom)] : null };
    }, sel);
    // Scroll `button` to mid-screen, click it in place, and sample the page at 0.7-6 s.
    async function clickAndWatch(label, button, watchSel, postRe) {
      await page.evaluate((s) => document.querySelector(s).scrollIntoView({ block: "center" }), button);
      await sleep(page, 600);
      const before = await place(button);
      const p0 = await hook(page);
      const nReads = net.reads.length;
      const t0 = Date.now();
      const how = await clickInPlace(page, button);
      const samples = [];
      for (const ms of [700, 1500, 2500, 4000, 6000]) {
        await until(page, t0, ms);
        samples.push(Object.assign({ ms }, await place(watchSel)));
      }
      const post = await postedId(page, net, new Date(t0 - 1000).toISOString(), postRe);
      const ev = await events(page, p0);
      const reads = post && post.id ? readsOf({ reads: net.reads.slice(nReads) }, post.id, t0) : [];
      await page.screenshot({ path: `${OUT}/CLASS-${label}.png` }).catch(() => {});
      return { label, how, before, samples, post: post && [post.status, post.id, post.keyDigest, post.replayed], scrollEventsMs: ev.scrolls.filter(([ms]) => ms >= -50 && ms <= 6500),
        swapsMs: ev.swaps.filter(([ms]) => ms >= -50 && ms <= 6500).slice(0, 20), followReadsMs: reads.filter(([ms]) => ms <= 6500),
        nonTerminalReadIn: reads.some(([ms, , term]) => ms <= 6000 && !term) };
    }
    const runs = [];
    try {
      // A reachable source whose inventory is NOT fresh, so Discover topics makes a new discovery.
      const fresh = new Set(kj("get", "topicdiscoveries").items.filter((d) => Date.parse((d.status || {}).freshUntil || 0) > Date.now() - 60000)
        .map((d) => ((d.spec.request || {}).connectionRef || {}).name));
      const sources = kj("get", "kafkaclusters").items.filter((k) => k.spec.role === "source" && (k.status || {}).reachable === true)
        .sort((a, b) => (a.metadata.creationTimestamp < b.metadata.creationTimestamp ? 1 : -1)).map((k) => k.metadata.name);
      const connTest = sources[0];
      const connDisc = sources.find((n) => !fresh.has(n)) || sources[0];
      // ---- Test connection
      await gotoHash(page, `#/clusters?ns=${NS}&name=${connTest}`);
      await waitForText(page, /Test connection/i, 60, "the connection detail");
      await sleep(page, 1500);
      const a = await clickAndWatch("test-connection", "#connection-check-form button[type=submit]", "#connection-check-status", /preflights$/);
      runs.push(a);
      row(`P16 class, Test connection at 390x844 (${connTest}, the button mid-screen): scrollY unchanged at 0.7/1.5/2.5/4/6 s after the click, focus on connection-check-status`,
        !!a.post && a.post[0] === 202 && a.before.scrollY > 300 && a.samples.every((s) => s.scrollY === a.before.scrollY && s.focused === "connection-check-status"),
        { connection: connTest, how: a.how, before: a.before, samples: a.samples.map((s) => [s.ms, s.scrollY, s.focused, s.target]), post: a.post, scrollEventsMs: a.scrollEventsMs, swapsMs: a.swapsMs, followReadsMs: a.followReadsMs, nonTerminalReadIn: a.nonTerminalReadIn });
      // ---- Discover topics
      await gotoHash(page, `#/clusters?ns=${NS}&name=${connDisc}`);
      await waitForText(page, /Discover topics/i, 60, "the connection detail");
      await sleep(page, 1500);
      const d = await clickAndWatch("discover-topics", "#discovery-start", "#discovery-start", /topic-discoveries$/);
      runs.push(d);
      row(`P16 class, Discover topics at 390x844 (${connDisc}, the button mid-screen): scrollY unchanged at 0.7/1.5/2.5/4/6 s after the click, focus on discovery-start`,
        !!d.post && d.post[0] === 202 && d.before.scrollY > 300 && d.samples.every((s) => s.scrollY === d.before.scrollY && s.focused === "discovery-start"),
        { connection: connDisc, how: d.how, before: d.before, samples: d.samples.map((s) => [s.ms, s.scrollY, s.focused, s.target]), post: d.post, scrollEventsMs: d.scrollEventsMs, swapsMs: d.swapsMs, followReadsMs: d.followReadsMs, nonTerminalReadIn: d.nonTerminalReadIn });
      // ---- Test access on the blackhole destination (a check of about 100 s)
      await gotoHash(page, `#/destinations?ns=${NS}&name=${BH_DEST}`);
      await waitForText(page, /Test access/, 60, "the destination detail");
      await sleep(page, 1500);
      const t = await clickAndWatch("test-access", "#destination-test button[type=submit]", "#destination-test-status", /:test$|\/test$/);
      runs.push(t);
      row(`P16 class, Test access at 390x844 (${BH_DEST}): document.activeElement.id === "destination-test-status" at 0.7/1.5/2.5/4/6 s after the click, the check still running`,
        !!t.post && t.post[0] === 202 && t.samples.every((s) => s.focused === "destination-test-status") && t.nonTerminalReadIn,
        { how: t.how, before: t.before, samples: t.samples.map((s) => [s.ms, s.scrollY, s.focused, s.target]), post: t.post, scrollEventsMs: t.scrollEventsMs, swapsMs: t.swapsMs, followReadsMs: t.followReadsMs });
      // ---- the schedule form's Check readiness, blackhole source and destination (about 150 s)
      await gotoHash(page, `#/schedules?ns=${NS}`);
      await waitForText(page, /Check readiness/i, 60, "the schedule form");
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
      await sleep(page, 1000);
      const s = await clickAndWatch("schedule-form", "#schedule-check-readiness", "#schedule-readiness-verdict", /preflights$/);
      runs.push(s);
      row(`P16 class, the schedule form's Check readiness at 390x844 (${BH_SRC} -> ${BH_DEST}): document.activeElement.id === "schedule-readiness-verdict" at 0.7/1.5/2.5/4/6 s after the click, the check still running`,
        !!s.post && s.post[0] === 202 && s.samples.every((x) => x.focused === "schedule-readiness-verdict") && s.nonTerminalReadIn,
        { how: s.how, before: s.before, samples: s.samples.map((x) => [x.ms, x.scrollY, x.focused, x.target]), post: s.post, scrollEventsMs: s.scrollEventsMs, swapsMs: s.swapsMs, followReadsMs: s.followReadsMs });
    } catch (e) { row("CLASS completed", false, { error: String(e.stack || e).slice(0, 700) }); }
    writeFileSync(`${OUT}/CLASS-runs.json`, JSON.stringify(runs, null, 1));
    await context.close();
  }

  // ======================================================================== NAV (390 AND 1440)
  if (GROUPS.includes("NAV")) {
    const b = newestPoint();
    const all = {};
    const heading = (page, n) => page.evaluate((k) => {
      const shown = [...document.querySelectorAll("[data-wizard-step]")].find((p) => !p.hidden);
      const h = shown ? [...shown.querySelectorAll("h3")].find((x) => x.textContent.trim().startsWith(k + ". ")) : null;
      const top = h ? Math.round(h.getBoundingClientRect().top) : null;
      return { top, text: h ? h.textContent.trim().slice(0, 40) : null, scrollY: Math.round(window.scrollY),
        maxOffset: document.documentElement.scrollHeight - window.innerHeight, viewport: window.innerHeight };
    }, n);
    // "about 25 px": the step's section aligned to the top of the viewport; a step too short for
    // that leaves the page at its maximum offset with the heading lower and on screen (noted).
    const lands = (h) => h.top !== null && ((h.top >= 15 && h.top <= 35) || (h.scrollY >= h.maxOffset - 1 && h.top > 35 && h.top < h.viewport));
    const exact = (h) => h.top !== null && h.top >= 15 && h.top <= 35;
    for (const [w, hgt] of [[390, 844], [1440, 900]]) {
      const vp = `${w}x${hgt}`;
      const { page, context } = await newSession(browser, "operator");
      await page.setViewportSize({ width: w, height: hgt });
      all[vp] = {};
      try {
        // ---- Next / Back from the bottom of steps 1, 2 and 5
        await openWizard(page, wizardHash(b));
        for (const [from, dir] of [[1, "next"], [2, "next"], [2, "back"], [5, "next"], [5, "back"]]) {
          await wizardStep(page, from);
          await sleep(page, 800);
          await page.evaluate(() => window.scrollTo(0, document.documentElement.scrollHeight));
          await sleep(page, 600);
          const bottom = await page.evaluate(() => ({ scrollY: Math.round(window.scrollY), docHeight: document.documentElement.scrollHeight }));
          const to = dir === "next" ? from + 1 : from - 1;
          const t0 = Date.now();
          const how = await clickInPlace(page, dir === "next" ? "#wizard-next" : "#wizard-back");
          await wizardAt(page, to, 30);
          const samples = [];
          for (const ms of [60, 400, 1200, 2500]) { await until(page, t0, ms); samples.push(Object.assign({ ms }, await heading(page, to))); }
          const settled = samples.filter((x) => x.ms >= 1200);
          const ok = settled.every(lands);
          all[vp][`${from}-${dir}`] = { bottom, how, samples };
          row(`NAV ${dir === "next" ? "Next" : "Back"} from the bottom of step ${from} at ${vp}: step ${to}'s heading lands at about 25 px (settled at 1.2 and 2.5 s)`,
            ok, { bottom, how, samples: samples.map((x) => [x.ms, x.scrollY, x.top, x.maxOffset]), atMaxOffsetNotExact: ok && !settled.every(exact) });
        }
        // ---- a &step=5 deep link
        {
          const t0 = Date.now();
          await gotoHash(page, wizardHash(b, "&step=5"));
          await wizardAt(page, 5, 60);
          await sleep(page, 1200);
          const h = await heading(page, 5);
          all[vp].deepLink = { h, afterMs: Date.now() - t0 };
          row(`NAV a '&step=5' deep link at ${vp} opens at step 5's heading (about 25 px)`, exact(h), h);
        }
        // ---- page navigation from a scrolled long page opens at 0
        const openedAt = async (t0) => { const out = []; for (const ms of [60, 400, 1200, 2500]) { await until(page, t0, ms); out.push([ms, Math.round(await page.evaluate(() => window.scrollY))]); } return out; };
        for (const [from, fromHash, linkText, toRe, what] of [
          ["Backups", `#/backups?ns=${NS}`, "History", /#\/history\?/, "Backups -> History (the masthead's nav link)"],
          ["History", `#/history?ns=${NS}`, "Schedules", /#\/schedules\?/, "History -> Schedules (the masthead's nav link)"],
        ]) {
          await gotoHash(page, fromHash);
          await waitForText(page, /logweir-backup-|No backup|No run/, 90, from);
          await sleep(page, 1500);
          await page.evaluate(() => window.scrollTo(0, document.documentElement.scrollHeight));
          await sleep(page, 600);
          const was = await page.evaluate(() => Math.round(window.scrollY));
          const t0 = Date.now();
          await page.evaluate((txt) => { [...document.querySelectorAll("nav a, header a")].find((a) => a.textContent.trim() === txt).click(); }, linkText);
          const at = await openedAt(t0);
          const hash = await page.evaluate(() => location.hash);
          all[vp][what] = { was, at, hash };
          row(`NAV ${what} at ${vp}, from the bottom of ${from} (scrollY ${was}; the link's own click() while the page stays scrolled): the new page opens at 0`,
            was > 1000 && toRe.test(hash) && at.every(([, y]) => y === 0), { was, at, hash });
        }
        {
          // a backup's detail link from the bottom row, with the mouse
          await gotoHash(page, `#/backups?ns=${NS}`);
          await waitForText(page, /logweir-backup-/, 90, "the Backups list");
          await sleep(page, 1500);
          const link = page.locator('main a[href*="#/backups?"][href*="name="]').last();
          await link.evaluate((a) => a.scrollIntoView({ block: "center" }));
          await sleep(page, 600);
          const was = await page.evaluate(() => Math.round(window.scrollY));
          const box = await link.boundingBox();
          const t0 = Date.now();
          await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
          const at = await openedAt(t0);
          const hash = await page.evaluate(() => location.hash);
          all[vp].detailLink = { was, at, hash };
          row(`NAV a backup's detail link from the bottom row of Backups at ${vp} (scrollY ${was}, a mouse click): the detail opens at 0`,
            was > 1000 && /name=/.test(hash) && at.every(([, y]) => y === 0), { was, at, hash: hash.slice(0, 120) });
        }
        {
          // the namespace picker's Go from a scrolled page (every PoC user holds ONE namespace, so
          // the picker names the same namespace; from a detail its Go mounts the namespace's list)
          await gotoHash(page, `#/clusters?ns=${NS}&name=${BH_SRC}`);
          await waitForText(page, /Test connection/i, 60, "a connection detail");
          await sleep(page, 1500);
          await page.evaluate(() => window.scrollTo(0, document.documentElement.scrollHeight));
          await sleep(page, 600);
          const was = await page.evaluate(() => Math.round(window.scrollY));
          const opts = await page.evaluate(() => { const i = document.querySelector("#ns-input"); return i && i.tagName === "SELECT" ? [...i.options].map((o) => o.value) : null; });
          const t0 = Date.now();
          await page.evaluate((ns) => { const i = document.querySelector("#ns-input"); i.value = ns; i.form.requestSubmit(); }, NS);
          const at = await openedAt(t0);
          const hash = await page.evaluate(() => location.hash);
          all[vp].namespaceGo = { was, at, hash, options: opts };
          row(`NAV the namespace picker's Go at ${vp}, from the bottom of a connection detail (scrollY ${was}; the form's requestSubmit() while the page stays scrolled): the namespace's page opens at 0`,
            was > 1000 && hash === `#/clusters?ns=${NS}` && at.every(([, y]) => y === 0), { was, at, hash, pickerOptions: opts });
        }
      } catch (e) { row(`NAV ${vp} completed`, false, { error: String(e.stack || e).slice(0, 700) }); }
      await context.close();
    }
    writeFileSync(`${OUT}/NAV-all.json`, JSON.stringify(all, null, 1));
  }

  // ======================================================================== O2 (390 x 844)
  if (GROUPS.includes("O2")) {
    const { page, context } = await newSession(browser, "operator");
    await page.setViewportSize({ width: 390, height: 844 });
    const seen = {};
    // Backticks OUTSIDE code/pre/textarea in the main view, and the code elements' texts.
    const read = () => page.evaluate(() => {
      const main = document.querySelector("main") || document.body;
      const clone = main.cloneNode(true);
      const ticksIn = [...main.querySelectorAll("code, pre, textarea")].map((x) => x.textContent).filter((t) => t.includes("`")).length;
      clone.querySelectorAll("code, pre, textarea").forEach((x) => x.remove());
      return { backtickLines: main.innerText.split("\n").filter((l) => l.includes("`")).slice(0, 8), outsideCode: (clone.textContent.match(/`/g) || []).length,
        ticksInsideCode: ticksIn, codes: [...main.querySelectorAll("code")].map((c) => c.textContent).filter((t) => /logweir catalog list/.test(t)),
        moreNote: !!main.querySelector("[data-more-points]"), statusTruncatedNote: !!(main.querySelector(".catalog-status") && /bounded WINDOW/.test(main.querySelector(".catalog-status").innerText)) };
    });
    try {
      const cat = kj("get", "recoverycatalog", "archive");
      await gotoHash(page, `#/catalog?ns=${NS}`);
      await waitForText(page, /Recovery catalog/, 60, "the Catalog list");
      await sleep(page, 1500);
      seen.list = await read();
      await page.screenshot({ path: `${OUT}/O2-catalog-list.png`, fullPage: true }).catch(() => {});
      row("O2 the Catalog list (#/catalog) at 390x844: innerText has no backtick, and a code element reads 'logweir catalog list'",
        seen.list.backtickLines.length === 0 && seen.list.outsideCode === 0 && seen.list.codes.length >= 1, seen.list);
      await gotoHash(page, `#/catalog?ns=${NS}&name=archive`);
      await waitForText(page, /The view/, 90, "the catalog's status");
      await sleep(page, 2500);
      seen.detail = await read();
      seen.truncated = (cat.status || {}).truncated === true;
      await page.screenshot({ path: `${OUT}/O2-catalog-archive.png`, fullPage: true }).catch(() => {});
      row(`O2 one catalog (archive; its status ${seen.truncated ? "truncated" : "NOT truncated, so the window sentence is not in its status"}) and its points with a next cursor: innerText has no backtick, and a code element reads 'logweir catalog list'`,
        seen.detail.backtickLines.length === 0 && seen.detail.outsideCode === 0 && seen.detail.moreNote && seen.detail.codes.length >= 1 && (!seen.truncated || seen.detail.statusTruncatedNote), Object.assign({ truncated: seen.truncated }, seen.detail));
      // the O2 sweep's other pages, as an observation (not a row)
      const others = {};
      for (const [k, hash] of [["keys", "#/keys"], ["destinations", `#/destinations?ns=${NS}&name=primary`], ["protection", `#/protection?ns=${NS}`], ["approvals", `#/approvals?ns=${NS}`], ["clusters", `#/clusters?ns=${NS}`], ["schedules", `#/schedules?ns=${NS}`]]) {
        await gotoHash(page, hash);
        await sleep(page, 2500);
        const r = await read();
        others[k] = { backtickLines: r.backtickLines, outsideCode: r.outsideCode };
      }
      seen.others = others;
      log(`O2 observation, other pages' backticks outside code: ${JSON.stringify(Object.fromEntries(Object.entries(others).map(([k, v]) => [k, v.outsideCode])))}`);
    } catch (e) { row("O2 completed", false, { error: String(e.stack || e).slice(0, 700) }); }
    writeFileSync(`${OUT}/O2-seen.json`, JSON.stringify(seen, null, 1));
    await context.close();
  }
} catch (e) {
  row("round5 completed every step", false, { error: String((e && e.stack) || e).slice(0, 800) });
} finally {
  await browser.close();
  const failed = ROWS.filter((r) => !r.pass);
  log(`${ROWS.length - failed.length}/${ROWS.length} rows pass${failed.length ? "; FAILED: " + failed.map((r) => r.id).join(" | ") : ""}`);
  process.exitCode = failed.length ? 1 : 0;
}
