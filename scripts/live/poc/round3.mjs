// The third PoC round's console rows (poc-upgrade-3), driven in a real Chromium through Traefik and
// Dex against the deployed shared console. Each row REQUIRES the outcome it records. The rows are
// the ones claude/poc-fixes-4 named for the next PoC round ("Rows for the next PoC round"):
//
//   LONG    P13 on Schedules -> Backup readiness (topics typed FIRST, then a non-default source
//           found through the search box and a non-default destination; the discovery repaint
//           lands; everything typed is kept; the POST carries it, 202; an edit made while the
//           check is followed survives its reads). P14 on the schedule form AND on the list panel:
//           a double click is one Preflight; a retry inside the validity replays the same pf-
//           and reads back "applies"; a click after the check's expiresAt gives a FRESH check
//           (a replay naming `expired`, then a new key) that applies. P14's class: Discover
//           topics after the inventory's freshUntil is a new td-; Cancel then Check is a new pf-.
//           P13's class on a connection's detail: expected topics and the topic filter typed
//           while Test connection is followed are kept. One page per surface, held open across
//           the validity windows (the intent tokens live in the page's memory).
//   WIZARD  step 5 while pending reads "checking..." (R2-11); settled on a draft it reads "needs
//           approval" in the headline and the stepper, and Create is allowed (R2-12, review L1);
//           no tracker task id on step 5 (R2-13); Back hidden on step 1 (R2-8); P13's class: Back
//           to step 4 while step 5 is followed, a prefix typed without leaving the field is kept,
//           and leaving it paints the plan with the edit. Nothing is created.
//   DEST    P13's class on a destination's detail: a new key typed into Rotate access while Test
//           access is followed is kept (the rotation is never submitted).
//   ROLES   R2-14 viewer and norole on #/restore get the role refusal, the viewer's Catalog has no
//           Connect form; R2-14b sign out from a deep link, and the next user lands on the default
//           route; R2-16 norole's landing sentence.
//
//   NODE_PATH="$(npm root -g)" node scripts/live/poc/round3.mjs <outdir> <GROUP>[,<GROUP>...]
//
// Passwords come from the credentials file and are typed into Dex; nothing secret is printed.
// Idempotency keys are recorded as a digest only; the CSRF token is never read.
import { writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chromium, newSession, gotoHash, textOf, waitForText, openWizard, wizardAt, wizardStep, BASE, HOST, credential,
  settledRows, checkVerdict, outcomeOf,
} from "./console.mjs";

const OUT = process.argv[2] || "/tmp/poc-round3";
const GROUPS = (process.argv[3] || "LONG").split(",");
const NS = process.env.POC_NAMESPACE || "logweir-poc";
mkdirSync(OUT, { recursive: true });
const ROWS = [];
const NET = [];
const log = (m) => console.log(new Date().toISOString(), m);
function row(id, ok, evidence) {
  ROWS.push({ id, pass: !!ok, at: new Date().toISOString(), evidence });
  log(`${ok ? "PASS" : "FAIL"} ${id} ${JSON.stringify(evidence).slice(0, 700)}`);
  writeFileSync(`${OUT}/rows-${GROUPS.join("_")}.json`, JSON.stringify(ROWS, null, 1));
}
const kj = (...args) => JSON.parse(execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=30s", "-n", NS, ...args, "-o", "json"], { timeout: 45000, maxBuffer: 256 * 1024 * 1024 }).toString());
async function shot(page, name) { await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: true }).catch(() => {}); }
const digest = (s) => (s ? createHash("sha256").update(s).digest("hex").slice(0, 12) : null);
const sleep = (page, ms) => page.waitForTimeout(ms);

// Every POST the page makes to the product API, with the answer's check id, replay flag and
// staleness, and the request body of a check (no credential is ever in one). Tagged by surface.
function capture(page, tag) {
  page.on("response", async (r) => {
    const q = r.request();
    if (q.method() !== "POST") return;
    const u = new URL(r.url());
    if (!u.pathname.startsWith("/api/v1/")) return;
    let body = null;
    try { body = await r.json(); } catch { body = null; }
    let sent = null;
    if (/preflights|topic-discoveries|:test/.test(u.pathname)) { try { sent = q.postDataJSON(); } catch { sent = null; } }
    const item = (body && body.item) || {};
    NET.push({
      tag, at: new Date().toISOString(), path: u.pathname, status: r.status(), keyDigest: digest(q.headers()["idempotency-key"] || ""),
      replayed: body ? body.replayed === true : null, id: item.id || null, state: item.state || null, terminal: item.terminal,
      applicable: item.applicable, staleReasons: (item.staleReasons || []).map((x) => x.reason), staleBasis: item.staleBasis || null,
      expiresAt: item.expiresAt || null, freshUntil: item.freshUntil || null, stale: item.stale, sent,
    });
    writeFileSync(`${OUT}/network-${GROUPS.join("_")}.json`, JSON.stringify(NET, null, 1));
  });
}
const posts = (tag, since, re) => NET.filter((n) => n.tag === tag && n.at >= since && re.test(n.path));
const nowIso = () => new Date().toISOString();
const PF = /pf-[a-z2-7]{26}/;
const TD = /td-[a-z2-7]{26}/;

// A readiness verdict on screen (the schedule form's fieldset or the list panel's section): the
// check's rows read from the DOM (`checkVerdict`, four cells since MCP round 2), settled when the
// check is no longer running ("checking...") and the page has not stopped following it (P15,
// `[data-check-stopped]`: reported as `outcome`, never as a verdict); then up to 30 s for the
// follow's read to land (a replay is terminal on arrival and owes one read, P8), and the check id
// and applicability. The wait ends on the verdict or the stop, the budget being only a backstop.
async function settle(page, selector, seconds) {
  const r = await checkVerdict(page, selector, seconds);
  return { t: r.text || "", rows: r.rows, pf: r.pf, applies: r.applies, settled: r.settled, stopped: r.stopped, outcome: r.outcome };
}
// The check's own expiry, as the product API reports it on a read.
async function expiryOf(page, id) {
  return page.evaluate(async ([ns, pf]) => {
    const r = await fetch(`/api/v1/namespaces/${ns}/preflights/${pf}`, { credentials: "same-origin" });
    const b = await r.json().catch(() => null);
    return ((b && (b.item || b)) || {}).expiresAt || null;
  }, [NS, id]);
}
const brief = (v) => ({ outcome: v.outcome, pf: v.pf, applies: v.applies, rows: v.rows.map((x) => `${x.id}=${x.verdict}/${x.gating}`).slice(0, 12), says: (v.t.match(/[^\n]*current inputs[^\n]*/) || [""])[0] });

async function waitUntil(page, iso, extraMs, what) {
  const until = Date.parse(iso) + extraMs;
  const cap = Date.now() + 20 * 60 * 1000;   // never more than 20 minutes for one wait
  log(`waiting until ${new Date(Math.min(until, cap)).toISOString()} for ${what}`);
  while (Date.now() < Math.min(until, cap)) await sleep(page, Math.min(20000, Math.max(500, Math.min(until, cap) - Date.now())));
  return Date.now() >= until;
}

const browser = await chromium.launch();
try {
  // ======================================================================== LONG
  if (GROUPS.includes("LONG")) {
    const { context } = await newSession(browser, "operator");
    // The console's session ends sessionMaxAgeSeconds (900 s in this profile, the shared-mode
    // maximum) after sign-in, and a sign-in is a page load, which mints new intent tokens: a row
    // that needs one page past that age cannot be staged (poc-upgrade-3).
    const SESSION_ENDS = Date.now() + 900 * 1000;
    const clusters = kj("get", "kafkaclusters").items;
    const sources = clusters.filter((k) => k.spec.role === "source").sort((a, b) => (a.metadata.creationTimestamp < b.metadata.creationTimestamp ? -1 : 1));
    const FORM_SRC = sources[0].metadata.name;
    // ---------------------------------------------------------- the schedule form (pageF)
    const pageF = await context.newPage();
    capture(pageF, "form");
    let formA = null;
    try {
      await gotoHash(pageF, `#/schedules?ns=${NS}`);
      await waitForText(pageF, /CHECK READINESS|Check readiness/, 60, "the schedule form");
      const form = pageF.locator("#schedule-form");
      const opts = await form.locator('select[name="source"] option').evaluateAll((os) => os.map((o) => [o.value, o.textContent]));
      await form.locator('select[name="source"]').selectOption(opts.find((o) => o[1].includes(FORM_SRC))[0]);
      await form.locator('select[name="mode"]').selectOption("daily");
      await form.locator('input[name="hour"]').fill("2");
      await form.locator('input[name="minute"]').fill("0");
      await form.locator('select[name="selection"]').selectOption("named");
      await form.locator('input[name="topics"]').fill("orders, payments");
      const dopts = await form.locator('select[name="destination"] option').evaluateAll((os) => os.map((o) => [o.value, o.textContent]));
      await form.locator('select[name="destination"]').selectOption(dopts.find((o) => o[1].startsWith("primary"))[0]);
      await sleep(pageF, 800);
      const before = new Set(kj("get", "preflights").items.map((p) => p.metadata.name));
      const t0 = nowIso();
      await pageF.locator("#schedule-check-readiness").dblclick();
      await sleep(pageF, 6000);
      const p0 = posts("form", t0, /preflights$/);
      const made = kj("get", "preflights").items.filter((p) => !before.has(p.metadata.name)).map((p) => p.metadata.name);
      row("P14-F0 a double click on the schedule form's Check readiness makes ONE Preflight (one POST, 202)",
        p0.length === 1 && p0[0].status === 202 && made.length === 1, { posts: p0.map((n) => [n.status, n.id, n.keyDigest]), created: made });
      formA = await settle(pageF, "#schedule-readiness", 240);
      const expA = formA.pf ? await expiryOf(pageF, formA.pf) : null;
      row("P14-F1 the first check settles and applies to the current inputs", formA.applies && formA.pf === (p0[0] || {}).id, Object.assign(brief(formA), { expiresAt: expA }));
      await shot(pageF, "P14-F1-form-first");
      // a retry inside the validity replays the same check
      const t1 = nowIso();
      await pageF.locator("#schedule-check-readiness").click();
      await sleep(pageF, 4000);
      const v1 = await settle(pageF, "#schedule-readiness", 240);
      const p1 = posts("form", t1, /preflights$/);
      row("P14-F2 a retry inside the validity replays: one POST, 200 replayed:true, the SAME pf-, and the page reads back 'applies to your current inputs'",
        p1.length === 1 && p1[0].status === 200 && p1[0].replayed === true && p1[0].id === formA.pf && v1.applies && v1.pf === formA.pf,
        { posts: p1.map((n) => [n.status, n.replayed, n.id, n.keyDigest, n.staleReasons]), page: brief(v1) });
      formA.expiresAt = expA;
    } catch (e) { row("P14-F form phase completed", false, { error: String(e.stack || e).slice(0, 600) }); }

    // ---------------------------------------------------------- the list panel (pageP)
    const pageP = await context.newPage();
    capture(pageP, "panel");
    let panelA = null;
    const TOPICS = "orders, payments";
    try {
      await gotoHash(pageP, `#/schedules?ns=${NS}`);
      await waitForText(pageP, /Backup readiness/, 60, "the schedules list");
      const panel = pageP.locator("#backup-readiness");
      const pre = { source: await pageP.inputValue("#readiness-source"), destination: await pageP.inputValue("#readiness-destination") };
      const sopts = await pageP.locator("#readiness-source option").evaluateAll((os) => os.map((o) => [o.value, o.textContent.trim()]));
      const dopts = await pageP.locator("#readiness-destination option").evaluateAll((os) => os.map((o) => [o.value, o.textContent.trim()]));
      // a source that is NOT the preselected one, and the destination that is NOT the default
      const srcNames = sources.map((s) => s.metadata.name);
      const pick = sopts.find((o) => o[0] && o[0] !== pre.source && srcNames.some((n) => o[1].includes(n)));
      const PANEL_SRC = srcNames.find((n) => pick && pick[1].includes(n));
      const dpick = dopts.find((o) => o[1].startsWith("pu3-dest"));
      // TOPICS FIRST (the order that lost them before the fix), typed as a person types
      await pageP.locator("#readiness-topics").click();
      await pageP.locator("#readiness-topics").pressSequentially(TOPICS, { delay: 30 });
      const query = PANEL_SRC.slice(0, 9);
      await pageP.locator("#readiness-source-search").pressSequentially(query, { delay: 30 });
      const tSel = nowIso();
      const disc = [];
      pageP.on("response", (r) => { if (r.request().method() === "GET" && /topic-discoveries/.test(new URL(r.url()).pathname)) disc.push({ at: nowIso(), path: new URL(r.url()).pathname, status: r.status() }); });
      await pageP.locator("#readiness-source").selectOption(pick[0]);
      await pageP.locator("#readiness-destination").selectOption(dpick[0]);
      // the discovery read the source change started lands and repaints the panel
      for (let i = 0; i < 40 && disc.length === 0; i++) await sleep(pageP, 500);
      await sleep(pageP, 3000);
      const kept = {
        topics: await pageP.inputValue("#readiness-topics"), source: await pageP.inputValue("#readiness-source"),
        search: await pageP.inputValue("#readiness-source-search"), destination: await pageP.inputValue("#readiness-destination"),
      };
      row("P13-1 topics typed FIRST, then a non-default source found through the search box and a non-default destination: after the discovery repaint lands, topics, source, search and destination are all kept",
        disc.length > 0 && kept.topics === TOPICS && kept.source === pick[0] && kept.search === query && kept.destination === dpick[0] && pick[0] !== pre.source && dpick[0] !== pre.destination,
        { preselected: pre, chosen: { source: PANEL_SRC, destination: "pu3-dest" }, discoveryReadsLanded: disc.map((d) => [d.status, d.path.split("/").slice(-2).join("/")]), kept: Object.assign({}, kept, { sourceIsChosen: kept.source === pick[0], destinationIsChosen: kept.destination === dpick[0] }) });
      await shot(pageP, "P13-1-panel-kept");
      const t2 = nowIso();
      await panel.getByRole("button", { name: /^check readiness$/i }).click();
      await sleep(pageP, 2500);
      const p2 = posts("panel", t2, /preflights$/);
      const sent = ((p2[0] || {}).sent || {}).backup || {};
      row("P13-2 the POST carries the typed topics, the chosen source and the chosen (non-default) destination, and is accepted 202",
        p2.length === 1 && p2[0].status === 202 && JSON.stringify(sent.topics) === JSON.stringify(["orders", "payments"]) && sent.destination === "pu3-dest" && sent.sourceConnection === PANEL_SRC,
        { posts: p2.map((n) => [n.status, n.id, n.keyDigest]), sent });
      // an edit made while the check is followed survives its reads
      const EDIT = TOPICS + ", zz-typed-while-followed";
      await pageP.locator("#readiness-topics").click();
      await pageP.locator("#readiness-topics").press("End");
      await pageP.locator("#readiness-topics").pressSequentially(", zz-typed-while-followed", { delay: 25 });
      const gets = [];
      pageP.on("response", (r) => { if (r.request().method() === "GET" && /\/preflights\//.test(new URL(r.url()).pathname)) gets.push(nowIso()); });
      for (let i = 0; i < 30 && gets.length < 2; i++) await sleep(pageP, 1000);
      await sleep(pageP, 1500);
      const edited = await pageP.inputValue("#readiness-topics");
      row("P13-3 an edit typed while the check is followed survives the follow's reads",
        gets.length >= 2 && edited === EDIT, { followReadsDuringEdit: gets.length, value: edited });
      await pageP.locator("#readiness-topics").fill(TOPICS);
      panelA = await settle(pageP, "#backup-readiness", 240);
      panelA.expiresAt = panelA.pf ? await expiryOf(pageP, panelA.pf) : null;
      row("P13-4 (R8.5) the panel settles to a verdict read back from the check (the check the POST made), naming no 'did not recompute staleness'",
        panelA.settled && panelA.rows.length > 0 && panelA.pf === (p2[0] || {}).id && !/did not recompute staleness/.test(panelA.t), Object.assign(brief(panelA), { expiresAt: panelA.expiresAt }));
      await shot(pageP, "P13-4-panel-settled");
    } catch (e) { row("P13 panel phase completed", false, { error: String(e.stack || e).slice(0, 600) }); }

    // ---------------------------------------------------------- a connection's detail (pageD)
    const pageD = await context.newPage();
    capture(pageD, "conn");
    let tdA = null;
    try {
      await gotoHash(pageD, `#/clusters?ns=${NS}&name=${FORM_SRC}`);
      await waitForText(pageD, /Discover topics|DISCOVER TOPICS/, 60, "the connection detail");
      const t3 = nowIso();
      await pageD.locator("#discovery-start").click();
      await sleep(pageD, 3000);
      const p3 = posts("conn", t3, /topic-discoveries$/);
      tdA = (p3[0] || {}).id;
      let td = null;
      for (let i = 0; i < 60; i++) { td = kj("get", "topicdiscovery", tdA); if (["Succeeded", "Failed"].includes((td.status || {}).phase)) break; await sleep(pageD, 3000); }
      tdA = { id: tdA, freshUntil: (td.status || {}).freshUntil, phase: (td.status || {}).phase };
      row("P14-D0 Discover topics makes a discovery (202) that succeeds with a freshness bound", p3.length === 1 && p3[0].status === 202 && tdA.phase === "Succeeded" && !!tdA.freshUntil,
        { posts: p3.map((n) => [n.status, n.id, n.keyDigest]), discovery: tdA });
      // P13's class: typed discovery inputs and the topic filter survive Test connection's reads
      await gotoHash(pageD, `#/clusters?ns=${NS}&name=${FORM_SRC}`);
      await waitForText(pageD, /Discover topics|DISCOVER TOPICS/, 60, "the connection detail");
      await pageD.locator("#discovery-expected").click();
      await pageD.locator("#discovery-expected").pressSequentially("orders", { delay: 25 });
      const hasQ = (await pageD.locator("#topic-q").count()) > 0;
      if (hasQ) { await pageD.locator("#topic-q").click(); await pageD.locator("#topic-q").pressSequentially("ord", { delay: 25 }); }
      const t4 = nowIso();
      const cgets = [];
      pageD.on("response", (r) => { if (r.request().method() === "GET" && /\/preflights\//.test(new URL(r.url()).pathname)) cgets.push(nowIso()); });
      await pageD.locator("#connection-check-form").getByRole("button", { name: /^test connection$/i }).click();
      for (let i = 0; i < 40 && cgets.length < 2; i++) await sleep(pageD, 1000);
      await sleep(pageD, 1500);
      const p4 = posts("conn", t4, /preflights|:test|connection/);
      const keptD = { expected: await pageD.inputValue("#discovery-expected"), filter: hasQ ? await pageD.inputValue("#topic-q") : null };
      row("P13-C1 a connection's detail: expected topics and the topic filter typed before Test connection are kept while its check is followed",
        p4.length >= 1 && cgets.length >= 2 && keptD.expected === "orders" && (!hasQ || keptD.filter === "ord"),
        { testPosts: p4.map((n) => [n.status, n.id]), followReads: cgets.length, kept: keptD, filterPresent: hasQ });
      await shot(pageD, "P13-C1-connection-kept");
      // back to the question the discovery was asked with (no expected topics), for the renewal row
      await pageD.locator("#discovery-expected").fill("");
      if (hasQ) await pageD.locator("#topic-q").fill("");
    } catch (e) { row("P14-D / P13-C phase completed", false, { error: String(e.stack || e).slice(0, 600) }); }

    // ---------------------------------------------------------- after the checks' validity
    try {
      const exp = [formA && formA.expiresAt, panelA && panelA.expiresAt].filter(Boolean).sort().pop();
      await waitUntil(pageF, exp, 20000, "both readiness checks to expire");
      if (formA && formA.pf) {
        const t5 = nowIso();
        await pageF.locator("#schedule-check-readiness").click();
        await sleep(pageF, 5000);
        const v5 = await settle(pageF, "#schedule-readiness", 240);
        const p5 = posts("form", t5, /preflights$/);
        const fresh = p5.find((n) => n.status === 202);
        const replay = p5.find((n) => n.status === 200);
        row("P14-F3 the schedule form's Check readiness clicked after its check EXPIRED gives a FRESH check: a new pf- (202, a new key) that applies, not the expired replay",
          !!fresh && fresh.id !== formA.pf && (!replay || (replay.id === formA.pf && replay.staleReasons.includes("expired") && replay.keyDigest !== fresh.keyDigest)) && v5.pf === fresh.id && v5.applies,
          { expired: formA.pf, expiresAt: formA.expiresAt, clickedAt: t5, posts: p5.map((n) => [n.status, n.replayed, n.id, n.keyDigest, n.staleReasons, n.staleBasis]), page: brief(v5) });
        row("P14-F3b the click's first answer is the old key's replay naming its own expiry: staleReasons [expired, unverifiable], staleBasis [expiry] (the API half)",
          !!replay && replay.replayed === true && replay.staleReasons.includes("expired") && replay.staleReasons.includes("unverifiable") && JSON.stringify(replay.staleBasis) === JSON.stringify(["expiry"]),
          { replay: replay || null });
        await shot(pageF, "P14-F3-form-fresh");
      }
      if (panelA && panelA.pf) {
        const t6 = nowIso();
        await pageP.locator("#backup-readiness").getByRole("button", { name: /^check readiness$/i }).click();
        await sleep(pageP, 5000);
        const v6 = await settle(pageP, "#backup-readiness", 240);
        const p6 = posts("panel", t6, /preflights$/);
        const fresh = p6.find((n) => n.status === 202);
        const replay = p6.find((n) => n.status === 200);
        row("P14-P3 the list panel's Check readiness after its check EXPIRED gives a FRESH check that applies (same inputs, a new pf-)",
          !!fresh && fresh.id !== panelA.pf && (!replay || (replay.id === panelA.pf && replay.staleReasons.includes("expired"))) && v6.pf === fresh.id && v6.settled && v6.rows.length > 0 && !/did not recompute staleness/.test(v6.t)
            && JSON.stringify(((fresh.sent || {}).backup || {}).topics) === JSON.stringify(["orders", "payments"]) && ((fresh.sent || {}).backup || {}).destination === "pu3-dest",
          { expired: panelA.pf, expiresAt: panelA.expiresAt, posts: p6.map((n) => [n.status, n.replayed, n.id, n.keyDigest, n.staleReasons]), page: brief(v6) });
        await shot(pageP, "P14-P3-panel-fresh");
        // (P14-X1, Cancel then Check, is its own group CANCEL: it runs in a fresh session.)
      }
    } catch (e) { row("P14 after-expiry phase completed", false, { error: String(e.stack || e).slice(0, 600) }); }

    // ---------------------------------------------------------- after the inventory's freshness
    try {
      if (tdA && tdA.freshUntil && Date.parse(tdA.freshUntil) + 20000 >= SESSION_ENDS) {
        // An inventory is fresh for 15 minutes and the session lasts at most 15: the same page can
        // never ask again after the inventory went stale, so the stale replay P14-D1 guards against
        // is unreachable here. Not a row (neither a pass nor a failure); the report says so.
        log(`P14-D1 NOT STAGEABLE: freshUntil ${tdA.freshUntil} is after the session ends (${new Date(SESSION_ENDS).toISOString()})`);
      } else if (tdA && tdA.freshUntil) {
        await waitUntil(pageD, tdA.freshUntil, 20000, "the discovery inventory to go stale");
        const t9 = nowIso();
        await pageD.locator("#discovery-start").click();
        await sleep(pageD, 4000);
        const p9 = posts("conn", t9, /topic-discoveries$/);
        const fresh = p9.find((n) => n.status === 202);
        row("P14-D1 Discover topics again after the inventory's freshUntil makes a NEW td- (not the stale one replayed)",
          !!fresh && fresh.id !== tdA.id && TD.test(fresh.id || ""), { stale: tdA, clickedAt: t9, posts: p9.map((n) => [n.status, n.replayed, n.id, n.keyDigest, n.stale]) });
        await shot(pageD, "P14-D1-rediscover");
      }
    } catch (e) { row("P14-D1 phase completed", false, { error: String(e.stack || e).slice(0, 600) }); }
    await context.close();
  }

  // ======================================================================== PANEL14
  // P14 on the list panel alone, signed in just before so the whole row fits the console's
  // 900 s session (sessionMaxAgeSeconds): a check, a retry inside its validity (replayed), and a
  // click after its expiresAt -- with every read the panel makes and what it shows, sampled, so a
  // panel that does not follow its renewed check is visible in the trace.
  // PANEL14B mirrors LONG's panel sequence before the expiry (P13's steps: topics typed first, the
  // source found through the search box, the non-default destination, an edit while followed) and
  // makes no in-window retry -- to reproduce LONG run 2's P14-P3 failure with the reads traced.
  const MIRROR = GROUPS.includes("PANEL14B");
  if (GROUPS.includes("PANEL14") || MIRROR) {
    const { context } = await newSession(browser, "operator");
    const signedIn = Date.now();
    const pageP = await context.newPage();
    capture(pageP, "panel");
    const READS = [];
    pageP.on("response", async (r) => {
      if (r.request().method() !== "GET" || !/\/preflights\/pf-/.test(new URL(r.url()).pathname)) return;
      let b = null; try { b = await r.json(); } catch { b = null; }
      const it = (b && b.item) || {};
      READS.push({ at: nowIso(), status: r.status(), id: new URL(r.url()).pathname.split("/").pop(), state: it.state || null, terminal: it.terminal, applicable: it.applicable });
      writeFileSync(`${OUT}/reads-PANEL14.json`, JSON.stringify(READS, null, 1));
    });
    const shows = async () => pageP.evaluate(() => {
      const s = document.querySelector("#backup-readiness");
      const t = s ? s.innerText : "";
      return { pf: (t.match(/pf-[a-z2-7]{26}/) || [null])[0], checking: !!(s && s.querySelector('[data-applicability="checking"]')), stopped: (s && s.querySelector("[data-check-stopped]") || { getAttribute: () => "" }).getAttribute("data-check-stopped") || "", applies: /applies to your current inputs/.test(t) && !/does not apply/.test(t),
        busy: !!(s && s.querySelector('#readiness-form[aria-busy="true"]')), disabled: !!(s && s.querySelector("#readiness-form fieldset[disabled]")) };
    });
    const TRACE = [];
    const trace = async (label, seconds) => { for (let i = 0; i < seconds / 2; i++) { TRACE.push(Object.assign({ at: nowIso(), label }, await shows())); await sleep(pageP, 2000); } writeFileSync(`${OUT}/trace-PANEL14.json`, JSON.stringify(TRACE, null, 1)); };
    try {
      await gotoHash(pageP, `#/schedules?ns=${NS}`);
      await waitForText(pageP, /Backup readiness/, 60, "the schedules list");
      const sopts = await pageP.locator("#readiness-source option").evaluateAll((os) => os.map((o) => [o.value, o.textContent.trim()]));
      const dopts = await pageP.locator("#readiness-destination option").evaluateAll((os) => os.map((o) => [o.value, o.textContent.trim()]));
      if (MIRROR) {
        await pageP.locator("#readiness-topics").click();
        await pageP.locator("#readiness-topics").pressSequentially("orders, payments", { delay: 30 });
        await pageP.locator("#readiness-source-search").pressSequentially("conn-pu7p", { delay: 30 });
      }
      await pageP.locator("#readiness-source").selectOption(sopts.find((o) => o[1].includes("conn-pu7pwgan6rdxmaxflvg3rmchxk"))[0]);
      await pageP.locator("#readiness-destination").selectOption(dopts.find((o) => o[1].startsWith("pu3-dest"))[0]);
      await sleep(pageP, 2500);
      if (!MIRROR) await pageP.locator("#readiness-topics").fill("orders, payments");
      const t1 = nowIso();
      await pageP.locator("#backup-readiness").getByRole("button", { name: /^check readiness$/i }).click();
      if (MIRROR) {
        await sleep(pageP, 2500);
        await pageP.locator("#readiness-topics").click();
        await pageP.locator("#readiness-topics").press("End");
        await pageP.locator("#readiness-topics").pressSequentially(", zz-typed-while-followed", { delay: 25 });
        await sleep(pageP, 4000);
        await pageP.locator("#readiness-topics").fill("orders, payments");
      }
      const v1 = await settle(pageP, "#backup-readiness", 240);
      const p1 = posts("panel", t1, /preflights$/);
      const exp = v1.pf ? await expiryOf(pageP, v1.pf) : null;
      row("P14-P1 the list panel's check settles on the check its POST made (202) and applies", p1.length === 1 && p1[0].status === 202 && v1.pf === p1[0].id && v1.applies,
        Object.assign(brief(v1), { posts: p1.map((n) => [n.status, n.id, n.keyDigest]), expiresAt: exp }));
      if (!MIRROR) {
        const t2 = nowIso();
        await pageP.locator("#backup-readiness").getByRole("button", { name: /^check readiness$/i }).click();
        await trace("in-window retry", 20);
        const v2 = await settle(pageP, "#backup-readiness", 240);
        const p2 = posts("panel", t2, /preflights$/);
        row("P14-P2 the list panel's retry inside the validity replays (200, same pf-) and reads back 'applies'",
          p2.length === 1 && p2[0].status === 200 && p2[0].replayed === true && p2[0].id === v1.pf && v2.pf === v1.pf && v2.applies,
          { posts: p2.map((n) => [n.status, n.replayed, n.id, n.keyDigest, n.staleReasons]), page: brief(v2) });
      }
      await waitUntil(pageP, exp, 20000, "the panel's check to expire");
      const left = Math.round((signedIn + 900000 - Date.now()) / 1000);
      const t3 = nowIso();
      await pageP.locator("#backup-readiness").getByRole("button", { name: /^check readiness$/i }).click();
      await trace("after-expiry click", 60);
      const v3 = await settle(pageP, "#backup-readiness", 240);
      const p3 = posts("panel", t3, /preflights$/);
      const fresh = p3.find((n) => n.status === 202);
      const replay = p3.find((n) => n.status === 200);
      const readsOfFresh = READS.filter((x) => fresh && x.id === fresh.id);
      row("P14-P3 the list panel's Check readiness after its check EXPIRED gives a FRESH check that the panel follows to a verdict that applies (same inputs, a new pf-)",
        !!fresh && fresh.id !== v1.pf && (!replay || (replay.id === v1.pf && replay.staleReasons.includes("expired"))) && v3.pf === fresh.id && v3.applies,
        { expired: v1.pf, expiresAt: exp, sessionSecondsLeftAtClick: left, posts: p3.map((n) => [n.status, n.replayed, n.id, n.keyDigest, n.staleReasons]), readsOfFreshCheck: readsOfFresh.map((x) => [x.at, x.status, x.state, x.terminal]), readsTotal: READS.length, page: brief(v3), lastShown: TRACE[TRACE.length - 1] });
      if (fresh) {
        const cr = (kj("get", "preflight", fresh.id).status || {});
        row("P14-P3 control: the fresh check itself completed in the cluster", cr.phase === "Completed", { id: fresh.id, phase: cr.phase, state: (cr.result || {}).state });
      }
      await shot(pageP, "P14-P3-panel");
    } catch (e) { row("PANEL14 completed", false, { error: String(e.stack || e).slice(0, 600) }); }
    await context.close();
  }

  // ======================================================================== CANCEL
  // P14's class on the list panel: Cancel a running check, then Check again with the same inputs:
  // a new pf-, not the cancelled one. A fresh session, so it cannot outlive sessionMaxAgeSeconds.
  if (GROUPS.includes("CANCEL")) {
    const { context } = await newSession(browser, "operator");
    const pageP = await context.newPage();
    capture(pageP, "panel");
    try {
      await gotoHash(pageP, `#/schedules?ns=${NS}`);
      await waitForText(pageP, /Backup readiness/, 60, "the schedules list");
      await pageP.locator("#readiness-topics").fill("orders");
      const t7 = nowIso();
      await pageP.locator("#backup-readiness").getByRole("button", { name: /^check readiness$/i }).click();
      let pfC = null;
      for (let i = 0; i < 40 && !pfC; i++) { await sleep(pageP, 250); pfC = (posts("panel", t7, /preflights$/).find((n) => n.status === 202) || {}).id; }
      for (let i = 0; i < 40 && !(await pageP.locator("#readiness-cancel").count()); i++) await sleep(pageP, 250);
      const couldCancel = (await pageP.locator("#readiness-cancel").count()) > 0;
      if (couldCancel) await pageP.locator("#readiness-cancel").click();
      let st = null;
      for (let i = 0; i < 40 && pfC; i++) { await sleep(pageP, 1500); st = (kj("get", "preflight", pfC).status || {}).phase; if (["Cancelled", "Completed", "Failed"].includes(st)) break; }
      await sleep(pageP, 3000);
      const t8 = nowIso();
      await pageP.locator("#backup-readiness").getByRole("button", { name: /^check readiness$/i }).click();
      await sleep(pageP, 5000);
      const v8 = await settle(pageP, "#backup-readiness", 240);
      const p8 = posts("panel", t8, /preflights$/);
      const fresh8 = p8.find((n) => n.status === 202);
      row("P14-X1 Cancel a check, then Check again with the same inputs: a new pf-, not the cancelled one",
        couldCancel && st === "Cancelled" && !!fresh8 && fresh8.id !== pfC && v8.pf === fresh8.id && v8.settled,
        { cancelled: pfC, cancelledPhase: st, cancelButton: couldCancel, posts: p8.map((n) => [n.status, n.replayed, n.id, n.keyDigest, n.staleReasons, n.state]), page: brief(v8) });
      await shot(pageP, "P14-X1-after-cancel");
    } catch (e) { row("CANCEL completed", false, { error: String(e.stack || e).slice(0, 600) }); }
    await context.close();
  }

  // ======================================================================== WIZARD
  if (GROUPS.includes("WIZARD")) {
    const { page, context } = await newSession(browser, "operator");
    capture(page, "wizard");
    const b = kj("get", "backups").items.filter((x) => (x.spec.scheduleRef || {}).name === "pu3-every5" && (((x.status || {}).evidence || {}).verification || {}).result === "Valid")
      .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? 1 : -1))[0];
    const tgt = kj("get", "kafkaclusters").items.filter((k) => k.spec.role === "target").sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? -1 : 1))[0].metadata.name;
    await openWizard(page, `#/restore?ns=${NS}&backup=${b.metadata.name}&uid=${b.metadata.uid}`);
    const back1 = await page.evaluate(() => { const x = document.querySelector("#wizard-back"); return x ? { disabled: x.disabled, visibility: getComputedStyle(x).visibility } : null; });
    row("R2-8 Back on step 1 is hidden (visibility:hidden) and disabled", !!back1 && back1.disabled && back1.visibility === "hidden", { back: back1 });
    await wizardStep(page, 4);
    const options = await page.$$eval('select[name="targetCluster"] option', (os) => os.map((x) => [x.value, x.textContent]));
    await page.selectOption('select[name="targetCluster"]', options.find((x) => x[1].startsWith(tgt + " "))[0]);
    await page.selectOption("#target-mode", "newTopic");
    await page.fill('input[name="topicPrefix"]', "pu3w-");
    await page.locator('input[name="topicPrefix"]').blur();
    await wizardStep(page, 6);
    await sleep(page, 1500);
    await wizardStep(page, 5);
    const t0 = nowIso();
    await page.click("#restore-readiness-start");
    // R2-11: while the check is pending or running, the applicability line reads "checking..."
    let sawChecking = false, pendingText = "";
    for (let i = 0; i < 40 && !sawChecking; i++) {
      await sleep(page, 250);
      pendingText = await page.locator("#step-preflight").innerText().catch(() => "");
      sawChecking = /checking\.\.\./.test(pendingText);
    }
    const pendingSaysNotApply = /does not apply to your current inputs/.test(pendingText);
    row("R2-11 step 5 while the check is pending reads 'checking...' and not 'does not apply to your current inputs'",
      sawChecking && !pendingSaysNotApply, { excerpt: pendingText.split("\n").filter((l) => /checking|apply|compared/i.test(l)).slice(0, 4) });
    const settled5 = await settledRows(page, "#step-preflight", 240, 3000);
    const s5 = settled5.settled ? settled5.text : await page.locator("#step-preflight").innerText();
    const stepper = await page.evaluate(() => [...document.querySelectorAll("ol.stepper li")].map((li) => li.innerText.replace(/\s+/g, " ").trim()));
    const rows5 = settled5.rows;
    const approvalRow = rows5.find((r) => r.id === "approval.state") || null;
    const otherBlocking = rows5.filter((r) => r.gating === "blocking" && r.id !== "approval.state");
    row("R2-12 settled on a draft, step 5's headline reads 'needs approval' (every other blocking check ready; creating it requests the approval) and not 'ready'",
      /needs approval/.test(s5) && /every other blocking check is ready/.test(s5) && !!approvalRow && approvalRow.verdict !== "ready"
        && otherBlocking.length > 0 && otherBlocking.every((r) => r.verdict === "ready"),
      { outcome: outcomeOf(settled5), headline: (s5.match(/[^\n]*needs approval[^\n]*/) || [""])[0].slice(0, 300), approvalRow, otherBlocking: otherBlocking.map((r) => `${r.id}=${r.verdict}`) });
    row("R2-12 the stepper's step 5 says 'needs approval' (not 'done')", stepper.some((s) => /Operation readiness/i.test(s) && /needs approval/i.test(s)) && !stepper.some((s) => /Operation readiness/i.test(s) && /\bdone\b/i.test(s)), { stepper });
    row("R2-13 step 5 names no tracker task (no PLAT-nn / PROD-nn in its text)", s5.length > 0 && !/\b(PLAT|PROD)-\d/.test(s5), { length: s5.length, hits: s5.match(/\b(PLAT|PROD)-\d[^\s]*/g) || [] });
    await shot(page, "R2-12-step5-needs-approval");
    await wizardStep(page, 6);
    const createDisabled = await page.isDisabled("#create-restore");
    row("R2-12 Create the Restore is allowed on that verdict (enabled; not clicked)", !createDisabled, { disabled: createDisabled });
    // P13's class in the wizard: ask again, go Back to step 4 while step 5 is followed, type a
    // prefix without leaving the field -- kept while the answer lands; leaving it paints the plan.
    await wizardStep(page, 5);
    const w1 = nowIso();
    await page.click("#restore-readiness-start");
    await sleep(page, 700);
    await wizardStep(page, 4);
    const gets = [];
    page.on("response", (r) => { if (r.request().method() === "GET" && /\/preflights\//.test(new URL(r.url()).pathname)) gets.push(nowIso()); });
    await page.locator("#topic-prefix").click();
    await page.locator("#topic-prefix").press("End");
    await page.locator("#topic-prefix").pressSequentially("x", { delay: 40 });
    for (let i = 0; i < 40 && gets.length < 2; i++) await sleep(page, 1000);
    await sleep(page, 2500);
    const typed = await page.inputValue("#topic-prefix");
    const focused = await page.evaluate(() => (document.activeElement || {}).id || "");
    row("P13-W1 the wizard: Back to step 4 while step 5's check is followed, a prefix typed without leaving the field is kept while its answers land",
      typed === "pu3w-x" && focused === "topic-prefix" && gets.length >= 1, { value: typed, focused, readsWhileTyping: gets.length, startPosts: posts("wizard", w1, /preflights$/).map((n) => [n.status, n.id]) });
    await page.locator("#topic-prefix").blur();
    await sleep(page, 1500);
    await wizardStep(page, 6);
    const plan = await page.locator("#step-plan").innerText();
    row("P13-W2 leaving the field paints the edit: step 6's plan carries the typed prefix", /pu3w-x/.test(plan), { planHasPrefix: /pu3w-x/.test(plan) });
    await shot(page, "P13-W2-plan");
    await context.close();
  }

  // ======================================================================== DEST
  if (GROUPS.includes("DEST")) {
    const { page, context } = await newSession(browser, "operator");
    capture(page, "dest");
    await gotoHash(page, `#/destinations?ns=${NS}&name=primary`);
    await waitForText(page, /Rotate access/, 60, "the destination detail");
    await page.selectOption("#rotate-archiveRead-source", "new");
    await sleep(page, 400);
    const typedId = "pu3-typed-access-key-id";
    const typedSecretHalf = "pu3-typed-" + "not-a-real-credential";
    await page.locator("#rotate-archiveRead-akid").pressSequentially(typedId, { delay: 15 });
    await page.locator("#rotate-archiveRead-sak").pressSequentially(typedSecretHalf, { delay: 15 });
    const t0 = nowIso();
    const gets = [];
    page.on("response", (r) => { if (r.request().method() === "GET" && /\/preflights\//.test(new URL(r.url()).pathname)) gets.push(nowIso()); });
    await page.locator("#destination-test-form").getByRole("button").last().click();
    for (let i = 0; i < 40 && gets.length < 2; i++) await sleep(page, 1000);
    await sleep(page, 2000);
    const kept = { id: (await page.inputValue("#rotate-archiveRead-akid")) === typedId, secretHalf: (await page.inputValue("#rotate-archiveRead-sak")) === typedSecretHalf, source: await page.inputValue("#rotate-archiveRead-source") };
    const tp = posts("dest", t0, /:test|test/);
    row("P13-DST a destination's detail: a new key typed into Rotate access is kept while Test access is followed (the rotation is never submitted)",
      tp.length >= 1 && tp[0].status === 202 && gets.length >= 2 && kept.id && kept.secretHalf && kept.source === "new",
      { testPosts: tp.map((n) => [n.status, n.id]), followReads: gets.length, kept });
    await page.locator("#rotate-archiveRead-akid").fill("");
    await page.locator("#rotate-archiveRead-sak").fill("");
    const rotated = posts("dest", t0, /rotate|destinations\/primary$/).filter((n) => !/test/.test(n.path));
    row("P13-DST control: no rotation was sent", rotated.length === 0, { rotationPosts: rotated.length });
    await context.close();
  }

  // ======================================================================== ROLES
  if (GROUPS.includes("ROLES")) {
    const b = kj("get", "backups").items.filter((x) => (((x.status || {}).evidence || {}).verification || {}).result === "Valid")
      .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? 1 : -1))[0];
    const deep = `#/restore?ns=${NS}&backup=${b.metadata.name}&uid=${b.metadata.uid}`;
    for (const role of ["viewer", "norole"]) {
      const s = await newSession(browser, role);
      await gotoHash(s.page, deep);
      await sleep(s.page, 1500);
      const v = await s.page.evaluate(() => ({
        refusal: (document.querySelector("#role-refusal-sentence") || {}).innerText || "", noRole: !!document.querySelector("#no-role"),
        wizard: !!document.querySelector("#wizard-position"), text: document.body.innerText.slice(0, 1500) }));
      const ok = role === "viewer"
        ? /Your role in logweir-poc can't start restores\./.test(v.refusal) && !v.wizard
        : (v.noRole || /can't start restores/.test(v.refusal)) && !v.wizard;
      row(`R2-14 ${role} opening a restore deep link gets the role refusal, not the wizard`, ok, { refusal: v.refusal, noRoleLanding: v.noRole, wizard: v.wizard });
      await shot(s.page, `R2-14-${role}-restore`);
      if (role === "viewer") {
        await gotoHash(s.page, `#/catalog?ns=${NS}`);
        await waitForText(s.page, /Catalog|catalog/, 60, "the catalog page");
        await sleep(s.page, 1500);
        const c = await s.page.evaluate(() => ({ connectForm: !!document.querySelector('form select[name="syncMode"]'), connectButton: [...document.querySelectorAll("button")].filter((x) => /connect archive/i.test(x.textContent)).length }));
        row("R2-14 the viewer's Catalog has no Connect form", !c.connectForm && c.connectButton === 0, c);
        await shot(s.page, "R2-14-viewer-catalog");
      } else {
        await gotoHash(s.page, "");
        await sleep(s.page, 1500);
        const l = await s.page.evaluate(() => ({ noRole: !!document.querySelector("#no-role"), h2: (document.querySelector("#no-role h2") || {}).innerText || "", prompt: /Choose a namespace/.test(document.body.innerText) }));
        row("R2-16 norole lands on 'You have no role in any namespace yet', before any namespace prompt", l.noRole && /^You have no role in any namespace yet$/.test(l.h2.trim()) && !l.prompt, l);
        await shot(s.page, "R2-16-norole-landing");
      }
      await s.context.close();
    }
    // R2-14b: sign out from a deep link; the next person signs in and lands on the default route
    const s = await newSession(browser, "operator");
    await gotoHash(s.page, deep);
    await s.page.waitForSelector("#sign-out", { timeout: 30000 });
    await s.page.click("#sign-out");
    await s.page.waitForSelector("#sign-in", { timeout: 30000 }).catch(() => {});
    await sleep(s.page, 1000);
    const out = await s.page.evaluate(() => ({ hash: location.hash, path: location.pathname, href: (document.querySelector("#sign-in-link") || { getAttribute: () => null }).getAttribute("href") }));
    // Sign out leaves the path with no hash (signedOutAddress); the app then shows its DEFAULT
    // route there (#/clusters, ROUTES[0]), so the Sign in link returns to /ui/ or /ui/#/clusters --
    // never to the previous user's deep link.
    const DEFAULT_ROUTE = "#/clusters";
    row("R2-14b after Sign out from a deep link the page is on no route or the default one, and its Sign in link returns there, not to the deep link",
      ["", DEFAULT_ROUTE].includes(out.hash) && ["/ui/", "/ui/" + DEFAULT_ROUTE].map((n) => "/auth/login?next=" + encodeURIComponent(n)).includes(out.href) && !/restore/.test(decodeURIComponent(out.href || "")), out);
    const { user, pw } = credential("viewer");
    await s.page.click("#sign-in-link");
    await s.page.waitForSelector('input[name="login"]', { timeout: 30000 });
    await s.page.fill('input[name="login"]', user);
    await s.page.fill('input[name="password"]', pw);
    await Promise.all([s.page.waitForURL((u) => u.hostname === HOST && u.pathname.startsWith("/ui"), { timeout: 30000 }), s.page.click('button[type="submit"]')]);
    await s.page.waitForSelector("#session-identity", { timeout: 30000 }).catch(() => {});
    await sleep(s.page, 2000);
    const landed = await s.page.evaluate(() => ({ hash: location.hash, refusal: !!document.querySelector("#role-refusal"), wizard: !!document.querySelector("#wizard-position"), who: (document.querySelector("#session-role") || {}).innerText || "" }));
    row("R2-14b the next user (viewer) lands on the default route, not the previous user's restore deep link",
      !/restore/.test(landed.hash) && !landed.refusal && !landed.wizard && /viewer/.test(landed.who), landed);
    await shot(s.page, "R2-14b-next-user");
    await s.context.close();
  }
} catch (e) {
  row("round3 completed every step", false, { error: String((e && e.stack) || e).slice(0, 800) });
} finally {
  await browser.close();
  const failed = ROWS.filter((r) => !r.pass);
  log(`${ROWS.length - failed.length}/${ROWS.length} rows pass${failed.length ? "; FAILED: " + failed.map((r) => r.id).join(" | ") : ""}`);
  process.exitCode = failed.length ? 1 : 0;
}
