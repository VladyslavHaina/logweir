// The PoC's console journey (deploy/poc/README.md steps 9-10, quickstart steps 4-8), driven in a
// real Chromium through Traefik and Dex against the deployed shared console, as `operator`:
//
//   J1 two connections       J2 Test connection on each      J3 the destination + Test access
//   J4 a schedule, with its readiness check                  J5 Run first backup now -> verified
//   J6 Restore this point (readiness, Create = Ordinary confirmation)  J7 completion + evidence
//
//   NODE_PATH="$(npm root -g)" node scripts/live/poc/journey.mjs <outdir> [--from J3]
//
// Every row REQUIRES the outcome it records (a verdict word, a phase, a written field); a row
// that does not see it fails the run. Object names come from the cluster, because the console
// mints connection and schedule names (see the report's P7). Passwords and MinIO keys are read
// from the credentials file and the cluster and typed into fields; nothing secret is printed.
import { writeFileSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import {
  chromium, newSession, gotoHash, textOf, waitForText, createCluster, createDestination,
  restoreFromBackup, secretValue, revealInGrid,
} from "./console.mjs";

const OUT = process.argv[2] || "/tmp/poc-journey";
const FROM = process.argv.includes("--from") ? process.argv[process.argv.indexOf("--from") + 1] : "J1";
const NS = process.env.POC_NAMESPACE || "logweir-poc";
mkdirSync(OUT, { recursive: true });
const ROWS = [];
const log = (m) => console.log(new Date().toISOString(), m);
function row(id, ok, evidence) {
  ROWS.push({ id, pass: !!ok, evidence });
  log(`${ok ? "PASS" : "FAIL"} ${id} ${JSON.stringify(evidence).slice(0, 400)}`);
  writeFileSync(`${OUT}/rows.json`, JSON.stringify(ROWS, null, 1));
}
const kj = (...args) => JSON.parse(execFileSync("kubectl", ["--context", "docker-desktop", "--request-timeout=30s", "-n", NS, ...args, "-o", "json"], { timeout: 45000, maxBuffer: 256 * 1024 * 1024 }).toString());
const byRole = (role) => (kj("get", "kafkaclusters").items.find((i) => i.spec.role === role) || { metadata: {} }).metadata.name;
// THE JOURNEY'S SCHEDULE IS THE FIRST ONE MADE ON THE DESTINATION: a namespace that later holds
// more schedules (a re-run, another round's rows) must not move J4-J6 onto one of them. The
// list is alphabetical, and minted sch-<26 base32> names sort in no useful order.
const firstSchedule = () => kj("get", "backupschedules").items.filter((x) => (x.spec.destinationRef || {}).name === "primary")
  .sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? -1 : 1))[0];
const step = (id) => ["J1", "J2", "J3", "J4", "J5", "J6", "J7"].indexOf(id) >= ["J1", "J2", "J3", "J4", "J5", "J6", "J7"].indexOf(FROM);
async function shot(page, name) { await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: true }); }

async function waitRows(page, sectionStart, seconds) {
  const end = Date.now() + seconds * 1000;
  let rows = [];
  while (Date.now() < end) {
    await page.waitForTimeout(4000);
    const t = await textOf(page);
    const s = t.slice(t.indexOf(sectionStart));
    rows = [...s.matchAll(/^([a-zA-Z]+\.[a-zA-Z]+)\t([^\t]+)\t(blocking|advisory|executionOnly)\t([A-Za-z]+)/gm)].map((m) => ({ id: m[1], verdict: m[2], gating: m[3], code: m[4] }));
    if (rows.length > 0 && !rows.some((r) => /pending|running/i.test(r.verdict))) return rows;
  }
  return rows;
}

const browser = await chromium.launch();
try {
  const { page } = await newSession(browser, "operator");
  // ---------------------------------------------------------------- J1 connections
  if (step("J1")) {
    for (const c of [{ name: "source", role: "source", servers: "logweir-kafka-source.logweir-system.svc.cluster.local:9092" },
                     { name: "target", role: "target", servers: "logweir-kafka-target.logweir-system.svc.cluster.local:9092" }]) {
      if (!byRole(c.role)) await createCluster(page, NS, c, log);
    }
    const src = byRole("source"), tgt = byRole("target");
    row("J1 two connections exist (source, target)", !!src && !!tgt, { source: src, target: tgt });
    await shot(page, "J1-clusters");
  }
  const SRC = byRole("source"), TGT = byRole("target");
  // ---------------------------------------------------------------- J2 test connection
  if (step("J2")) {
    for (const name of [SRC, TGT]) {
      await gotoHash(page, `#/clusters?ns=${NS}&name=${name}`);
      await waitForText(page, /TEST CONNECTION|Test connection/, 60, "the cluster page");
      await page.getByRole("button", { name: /^test connection$/i }).click();
      const rows = await waitRows(page, "Test connection", 180);
      const blocking = rows.filter((r) => r.gating === "blocking");
      row(`J2 Test connection ${name}: every blocking row ready`, blocking.length > 0 && blocking.every((r) => r.verdict === "ready"),
        { rows: rows.map((r) => `${r.id}=${r.verdict}/${r.code}`) });
      await shot(page, `J2-test-connection-${name}`);
    }
  }
  // ---------------------------------------------------------------- J3 destination + test access
  if (step("J3")) {
    const exists = kj("get", "backupdestinations").items.some((d) => d.metadata.name === "primary");
    if (!exists) {
      const users = (k) => secretValue("logweir-system", "logweir-poc-minio-users", k);
      await createDestination(page, NS, {
        name: "primary", description: "PoC demo MinIO (README step 10)", bucket: "kafka-backups", prefix: "poc",
        region: "us-east-1", endpoint: "http://logweir-minio.logweir-system.svc:9000", addressing: "pathStyle", security: "insecureHttp",
        grants: {
          archiveWrite: { source: "new", accessKeyId: "logweir-poc-writer", secretAccessKey: users("writer") },
          archiveRead: { source: "new", accessKeyId: "logweir-poc-reader", secretAccessKey: users("reader") },
          evidenceRead: { source: "new", accessKeyId: "logweir-poc-evidence", secretAccessKey: users("evidence") },
        },
        writeProbe: "createOnlyMarker", isDefault: true,
      }, log);
    }
    for (let i = 0; i < 20; i++) {
      const d = kj("get", "backupdestinations").items.find((x) => x.metadata.name === "primary");
      if (d && (d.status || {}).conditions) break;
      await page.waitForTimeout(3000);
    }
    const d = kj("get", "backupdestination", "primary");
    const valid = ((d.status || {}).conditions || []).find((c) => c.type === "Valid");
    row("J3 destination primary created by the console and Valid", valid && valid.status === "True",
      { grants: Object.fromEntries(Object.entries(d.spec.access).map(([k, v]) => [k, (v.secret || {}).name || v.mode])), valid: valid && valid.reason });
    await gotoHash(page, `#/destinations?ns=${NS}&name=primary`);
    await waitForText(page, /Test access/, 60, "the destination page");
    await page.locator("#destination-test").getByRole("button", { name: /test access/i }).click();
    const rows = await waitRows(page, "Test access", 240);
    const blocking = rows.filter((r) => r.gating === "blocking");
    row("J3 Test access: every blocking row ready", blocking.length > 0 && blocking.every((r) => r.verdict === "ready"),
      { rows: rows.map((r) => `${r.id}=${r.verdict}/${r.code}`) });
    await shot(page, "J3-test-access");
  }
  // ---------------------------------------------------------------- J4 schedule
  if (step("J4")) {
    if (kj("get", "backupschedules").items.length === 0) {
      await gotoHash(page, `#/schedules?ns=${NS}`);
      await waitForText(page, /CHECK READINESS|Check readiness/, 60, "the schedule form");
      const form = page.locator("#schedule-form");
      // THE SHARED CONSOLE HAS NO SCHEDULE NAME FIELD (poc-fixes-2 review L5):
      // the product API names it sch-<26 base32>. Fill it only where it exists.
      const scheduleName = form.locator('input[name="name"]');
      if (await scheduleName.count() > 0) await scheduleName.fill("orders-nightly");
      const opts = await form.locator('select[name="source"] option').evaluateAll((os) => os.map((o) => [o.value, o.textContent]));
      const pick = opts.find((o) => o[1].includes(SRC));
      await form.locator('select[name="source"]').selectOption(pick[0]);
      await form.locator('select[name="mode"]').selectOption("daily");
      await form.locator('input[name="hour"]').fill("2");
      await form.locator('input[name="minute"]').fill("0");
      await form.locator('select[name="selection"]').selectOption("named");
      await form.locator('input[name="topics"]').fill("orders, payments");
      const dopts = await form.locator('select[name="destination"] option').evaluateAll((os) => os.map((o) => [o.value, o.textContent]));
      const dpick = dopts.find((o) => o[1].startsWith("primary"));
      await form.locator('select[name="destination"]').selectOption(dpick[0]);
      await page.waitForTimeout(800);
      // Create is held until the cadence is previewed: the API compiles the preset and this
      // page shows exactly what will be saved ("Preview this cadence before saving it").
      await form.getByRole("button", { name: /preview next runs/i }).click();
      // The AT (UTC) column in the one timestamp format (console-ux-1, MCP-7): was `…T02:00:00Z`.
      await waitForText(page, /NEXT RUNS[\s\S]*\d{4}-\d{2}-\d{2} 02:00:00 UTC/, 60, "the cadence preview");
      await page.locator('button[name="schedule-check-readiness"], #schedule-check-readiness').first().click();
      const rows = await waitRows(page, "READINESS", 240);
      const blocking = rows.filter((r) => r.gating === "blocking");
      row("J4 schedule readiness: blocking rows", blocking.length > 0, { rows: rows.map((r) => `${r.id}=${r.verdict}/${r.code}`) });
      await shot(page, "J4-schedule-readiness");
      const create = form.getByRole("button", { name: /^create$/i });
      for (let i = 0; i < 20 && await create.isDisabled(); i++) await page.waitForTimeout(1500);
      await create.click();
      await page.waitForTimeout(4000);
    }
    const sch = firstSchedule();
    row("J4 schedule created by the console (daily 02:00, orders+payments, destination primary)",
      !!sch && sch.spec.destinationRef && sch.spec.destinationRef.name === "primary",
      { name: sch && sch.metadata.name, schedule: sch && sch.spec.schedule, topics: sch && sch.spec.topics, source: sch && sch.spec.sourceRef });
    await shot(page, "J4-schedule-created");
  }
  const SCH = (firstSchedule() || { metadata: {} }).metadata.name;
  // ---------------------------------------------------------------- J5 first backup
  let BACKUP = null;
  if (step("J5")) {
    await gotoHash(page, `#/schedules?ns=${NS}&name=${SCH}`);
    await waitForText(page, /RUN FIRST BACKUP NOW|Run first backup now|BACK UP NOW/, 60, "the schedule detail");
    const before = kj("get", "backups").items.length;
    if (before === 0) {
      await page.getByRole("button", { name: /run first backup now/i }).last().click();
      await page.waitForTimeout(3000);
    }
    let b = null;
    for (let i = 0; i < 60; i++) {
      b = kj("get", "backups").items.sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? -1 : 1))[0];
      const v = b && ((b.status || {}).evidence || {}).verification;
      if (b && b.status && b.status.phase === "Succeeded" && v && v.result && v.result !== "Pending") break;
      await page.waitForTimeout(6000);
    }
    BACKUP = b;
    const v = ((b.status || {}).evidence || {}).verification || {};
    row("J5 Run first backup now: Succeeded and Valid", b.status.phase === "Succeeded" && v.result === "Valid",
      { backup: b.metadata.name, trigger: b.spec.trigger, phase: b.status.phase, verification: v.result, key: v.matchedKeyId, trust: v.trust });
    await gotoHash(page, `#/operations?ns=${NS}&kind=backup&name=${b.metadata.name}`);
    const t = await textOf(page);
    const badge = (t.match(/verified by weirkeeper[^\n]*/) || [""])[0];
    row("J5 the operation page shows the green badge 'verified by weirkeeper ... against key ...'", /verified by weirkeeper .* against key/.test(badge), { badge });
    writeFileSync(`${OUT}/J5-operation.txt`, t);
    await shot(page, "J5-backup-operation");
  }
  // ---------------------------------------------------------------- J6 restore
  if (step("J6")) {
    const b = BACKUP || kj("get", "backups").items.sort((x, y) => (x.metadata.creationTimestamp < y.metadata.creationTimestamp ? -1 : 1))[0];
    await gotoHash(page, `#/schedules?ns=${NS}&name=${SCH}`);
    // A SCHEDULE WITH HUNDREDS OF RUNS RENDERS ITS POINTS AFTER "Reading <name>...": wait for them
    // before counting, or the row fails on a page that has not painted yet (poc-upgrade-1, J6).
    await waitForText(page, /Restore this point/, 90, "the schedule's points");
    const link = page.locator(`a[href*="backup=${b.metadata.name}"]`, { hasText: /Restore this point/ }).first();
    // THE SCHEDULE CARD'S TABLES ARE PAGINATED (console-ux-1, MCP-26): an older run is on a later
    // page and not in the DOM at all, so it is looked up through the grid's filter, as a person would.
    const revealed = await revealInGrid(page, link, b.metadata.name).then(() => null, (e) => String(e.message || e));
    row("J6 the schedule page offers 'Restore this point' for the run", (await link.count()) > 0 && (await link.isVisible()),
      { href: (await link.count()) ? await link.getAttribute("href") : null, revealError: revealed });
    const r = await restoreFromBackup(page, NS, b.metadata.name, b.metadata.uid, { target: TGT, prefix: process.env.POC_RESTORE_PREFIX || "restored-", shots: `${OUT}/J6` }, log);
    const notReady = (r.readiness && r.readiness.blockingNotReady) || ["no readiness"];
    row("J6 readiness: every blocking row ready except approval.state (skipped until the Restore exists)", notReady.length === 0,
      { rows: r.readiness && r.readiness.rows.map((x) => `${x.id}=${x.verdict}`) });
    const st = r.status || {};
    row("J6 the Restore (Ordinary confirmation) reaches Succeeded", st.phase === "Succeeded", { restore: r.name, phase: st.phase, exitReason: st.exitReason });
    writeFileSync(`${OUT}/J6-restore.json`, JSON.stringify({ name: r.name, planHash: r.planHash, status: st }, null, 1));
    // ------------------------------------------------------------------- J7 verify
    const ap = kj("get", "approvals").items.find((a) => (a.spec.subjectRef || {}).name === r.name);
    const apst = (ap && ap.status) || {};
    row("J7 the Approval is Verified=True, authorised by the operator's own console confirmation (Ordinary)",
      ((apst.conditions || []).find((c) => c.type === "Verified") || {}).status === "True",
      { approval: ap && ap.metadata.name, authorization: apst.authorization, approver: apst.approverSubject || apst.approver });
    const v = ((st.evidence || {}).verification) || {};
    row("J7 scorecard Valid and status.completion written", v.result === "Valid" && !!st.completion,
      { verification: v.result, trust: v.trust, completion: st.completion });
    const opText = r.operationText || "";
    row("J7 the operation page shows the completion panel with the sampled-window count",
      /records verified in the sampled window/i.test(opText), { excerpt: (opText.match(/[^\n]*records verified in the sampled window[^\n]*/i) || [""])[0] });
    writeFileSync(`${OUT}/J7-operation.txt`, opText);
  }
} catch (e) {
  row("journey completed every step", false, { error: String(e && e.stack || e).slice(0, 600) });
} finally {
  await browser.close();
  const failed = ROWS.filter((r) => !r.pass);
  log(`${ROWS.length - failed.length}/${ROWS.length} rows pass${failed.length ? "; FAILED: " + failed.map((r) => r.id).join(" | ") : ""}`);
  process.exitCode = failed.length ? 1 : 0;
}
