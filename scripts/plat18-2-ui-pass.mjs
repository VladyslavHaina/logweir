// PLAT-18.2 live pass: the half of `scripts/plat18-2-ui-e2e.mjs` that drives
// the browser. Run it through that file (UI_E2E_STAGE=pass or all); its
// header says what is proved and how. Kept in its own module so the
// provisioning half can be read on its own.

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  API_BIN,
  ARTIFACTS,
  AXE,
  JOURNEYS,
  LABEL,
  UI_DIR,
  WORK_DIR,
  chromium,
  check,
  commandText,
  freePort,
  kube,
  kubeJson,
  namespace,
  pause,
  randomBytes,
  readState,
  spawn,
  syncCatalog,
  writeState,
} from "./plat18-2-ui-e2e.mjs";

const ONLY = (process.env.UI_E2E_ONLY || "").split(",").filter((s) => s.length > 0);
const wants = (part) => ONLY.length === 0 || ONLY.indexOf(part) !== -1;

const result = {
  harness: "scripts/plat18-2-ui-e2e.mjs (pass: scripts/plat18-2-ui-pass.mjs)",
  task: "PLAT-18.2",
  label: LABEL,
  namespace: namespace,
  uiDirectory: UI_DIR,
  apiBinary: API_BIN,
  startedAt: new Date().toISOString(),
  journeys: [],
  controls: [],
  failed: [],
  routes: [],
  states: [],
  measurements: [],
  created: [],
  screenshots: [],
};

function record(name, detail) {
  result.journeys.push(Object.assign({ journey: name }, detail || {}));
  process.stderr.write("== passed: " + name + "\n");
}

function control(name, detail) {
  result.controls.push(Object.assign({ control: name }, detail || {}));
  process.stderr.write("   -- control: " + name + "\n");
}

function failed(name, error, detail) {
  const message = error instanceof Error ? error.message : String(error);
  result.failed.push(Object.assign({ journey: name, error: message.slice(0, 3000) }, detail || {}));
  process.stderr.write("== FAILED: " + name + ": " + message.slice(0, 600) + "\n");
}

// ------------------------------------------------------------- the service

let api = null;
const apiLog = [];

async function startApi(port) {
  mkdirSync(WORK_DIR, { recursive: true, mode: 0o700 });
  const cursorKey = join(WORK_DIR, "cursor-" + LABEL + ".key");
  writeFileSync(cursorKey, randomBytes(32), { mode: 0o600 });
  const configPath = join(WORK_DIR, "config-" + LABEL + ".yaml");
  const config = [
    "mode: localAdmin",
    "listen: \"127.0.0.1:" + port + "\"",
    "publicOrigin: \"http" + "://127.0.0.1:" + port + "\"",
    "uiDirectory: " + UI_DIR,
    "localAdmin:",
    "  subject: admin",
    "  displayName: Local administrator",
    "namespaces: [" + namespace + "]",
    "kubernetes:",
    "  source: kubeconfig",
    "  context: docker-desktop",
    "cursorKeyFile: " + cursorKey,
    "",
  ].join("\n");
  writeFileSync(configPath, config);
  writeFileSync(join(ARTIFACTS, "config.yaml"), config);
  api = spawn(API_BIN, ["--config", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  api.stdout.on("data", (b) => apiLog.push(String(b)));
  api.stderr.on("data", (b) => apiLog.push(String(b)));
  for (let i = 0; i < 60; i += 1) {
    try {
      const probe = await fetch("http" + "://127.0.0.1:" + port + "/healthz");
      if (probe.ok) {
        return;
      }
    } catch (notYet) {
      // still binding
    }
    await pause(500);
  }
  throw new Error("logweir-api never answered /healthz within 30 s. Log:\n" + apiLog.join(""));
}

function stopApi() {
  if (api !== null && api.exitCode === null) {
    api.kill("SIGTERM");
  }
}

// ------------------------------------------------------------ page helpers

async function text(page) {
  return (await page.evaluate(() => document.body.innerText)).toLowerCase();
}

async function waitFor(page, selector, label, timeout) {
  try {
    await page.waitForSelector(selector, { timeout: timeout || 30000 });
  } catch (never) {
    throw new Error(label + ": " + selector + " never appeared. Saw:\n" +
      (await text(page)).slice(0, 1500));
  }
}

/** The view has rendered: no loading line left in the view slot. */
async function settled(page, timeout) {
  await page.waitForFunction(() => {
    const main = document.getElementById("view-slot");
    return main !== null && main.children.length > 0 && main.querySelector(":scope > .pending") === null;
  }, null, { timeout: timeout || 30000 });
}

async function open(page, url) {
  await page.goto(url, { waitUntil: "load", timeout: 30000 });
  await page.reload({ waitUntil: "load", timeout: 30000 });
  await settled(page, 45000);
  await pause(400);
}

async function shot(page, name) {
  const at = join(ARTIFACTS, name + ".png");
  await page.screenshot({ path: at, fullPage: true });
  result.screenshots.push(at);
  return at;
}

/** Tab until `selector` holds focus; the trail is the proof of focus order. */
async function tabTo(page, selector, label, limit) {
  const seen = [];
  for (let i = 0; i < (limit || 200); i += 1) {
    await page.keyboard.press("Tab");
    const landed = await page.evaluate((target) => {
      const node = document.activeElement;
      const want = document.querySelector(target);
      return {
        at: node === null ? "(none)" : (node.id || node.getAttribute("name") ||
          node.tagName.toLowerCase()),
        hit: want !== null && node === want,
      };
    }, selector);
    seen.push(landed.at);
    if (landed.hit) {
      return seen;
    }
  }
  throw new Error(label + ": Tab did not reach " + selector + ". Focus trail: " +
    JSON.stringify(seen.slice(-40)));
}

/** Moves a focused <select> to `wanted` with the keyboard alone. */
async function keyboardSelect(page, selector, wanted, letter) {
  const read = () => page.evaluate((sel) => document.querySelector(sel).value, selector);
  const how = [];
  for (let i = 0; letter && i < 8 && (await read()) !== wanted; i += 1) {
    await page.keyboard.press(letter);
    how.push(letter);
  }
  if ((await read()) !== wanted) {
    await page.keyboard.press("Home");
    how.push("Home");
    for (let i = 0; i < 24 && (await read()) !== wanted; i += 1) {
      await page.keyboard.press("ArrowDown");
      how.push("ArrowDown");
    }
  }
  const landed = await read();
  check(landed === wanted, "the keyboard did not reach " + wanted + " on " + selector +
    "; landed on " + landed + " after " + JSON.stringify(how));
  return how;
}

async function armPointerCounter(page) {
  await page.evaluate(() => {
    window.__p182Pointer = 0;
    for (const type of ["pointerdown", "mousedown", "click"]) {
      document.addEventListener(type, (event) => {
        // A keyboard activation of a button dispatches a synthetic `click`
        // with detail 0 and no pointer; only a real pointer counts.
        if (event.isTrusted && (type !== "click" || event.detail > 0)) {
          window.__p182Pointer += 1;
        }
      }, { capture: true });
    }
  });
}

async function pointerCount(page) {
  return page.evaluate(() => window.__p182Pointer || 0);
}

async function activeId(page) {
  return page.evaluate(() => {
    const node = document.activeElement;
    return node === null ? null : (node === document.body ? "(body)" : (node.id ||
      node.tagName.toLowerCase()));
  });
}

function backups() {
  return kubeJson(["-n", namespace, "get", "backups"]).items;
}

function schedules() {
  return kubeJson(["-n", namespace, "get", "backupschedules"]).items;
}

async function waitForTerminalBackup(name) {
  for (let attempt = 0; attempt < 300; attempt += 1) {
    const object = kubeJson(["-n", namespace, "get", "backup", name]);
    const phase = String((object.status || {}).phase || "");
    if (["Succeeded", "Failed", "Cancelled"].includes(phase)) {
      return object;
    }
    await pause(2000);
  }
  throw new Error("Backup " + name + " never reached a terminal phase");
}

async function waitForVerdict(name) {
  const seen = [];
  let object = null;
  for (let attempt = 0; attempt < 180; attempt += 1) {
    object = kubeJson(["-n", namespace, "get", "backup", name]);
    const verdict = String(((((object.status || {}).evidence || {}).verification) || {}).result ||
      "absent");
    if (seen[seen.length - 1] !== verdict) {
      seen.push(verdict);
    }
    if (verdict !== "Pending" && verdict !== "absent" &&
      !(verdict === "NotAttempted" && (((object.status || {}).evidence || {}).observation || {})
        .retryAfter)) {
      break;
    }
    await pause(2000);
  }
  return { object: object, seen: seen };
}

// ---------------------------------------------------------------- the pass

export async function runPass() {
  const state = readState();
  mkdirSync(ARTIFACTS, { recursive: true });
  result.revision = commandText("git", ["-C", UI_DIR, "rev-parse", "HEAD"]);
  result.apiBinarySha256 = commandText("shasum", ["-a", "256", API_BIN]);
  result.axe = { path: AXE, version: JSON.parse(readFileSync(join(AXE, "..", "package.json"),
    "utf8")).version };
  result.uiStyleSha256 = commandText("shasum", ["-a", "256", join(UI_DIR, "style.css")]);
  const port = await freePort();
  await startApi(port);
  const base = "http" + "://127.0.0.1:" + port + "/ui/";
  const route = (hash) => base + "#/" + hash + (hash.indexOf("?") === -1 ? "?" : "&") + "ns=" +
    encodeURIComponent(namespace);
  result.base = base;

  const browser = await chromium.launch();
  try {
    if (JOURNEYS && wants("configure")) {
      await configureJourney(browser, state, route);
    }
    writeState(state);
    if (JOURNEYS && wants("restore")) {
      await restoreJourney(browser, state, route);
    }
    writeState(state);
    if (wants("probes")) {
      await focusProbes(browser, state, route);
    }
    if (wants("routes")) {
      await routePass(browser, state, route);
    }
    if (wants("states")) {
      await statePass(browser, state, route);
    }
    if (wants("large")) {
      await largePass(browser, state, route);
    }
  } finally {
    await browser.close();
    stopApi();
    writeFileSync(join(ARTIFACTS, "api.log"), apiLog.join(""));
    result.finishedAt = new Date().toISOString();
    const at = join(ARTIFACTS, "live" + (ONLY.length > 0 ? "-" + ONLY.join("-") : "") + ".json");
    writeFileSync(at, JSON.stringify(result, null, 2) + "\n");
    process.stderr.write("== " + result.journeys.length + " journey(s), " +
      result.controls.length + " control(s), " + result.failed.length + " failure(s); " + at + "\n");
  }
  if (result.failed.length > 0) {
    throw new Error(result.failed.length + " failure(s): " +
      result.failed.map((f) => f.journey).join("; "));
  }
}

// ------------------------------------------------------ journey: configure

async function configureJourney(browser, state, route) {
  const name = "keyboard-only configure: connection, schedule, Back up now";
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();
  try {
    // 1. A saved connection, from the clusters page, by keyboard. In console
    // mode the product API names it (`conn-<hash>`), so the typed name is a
    // request and the stored object is found as the one this click added.
    const typedSource = "orders-src";
    const clustersBefore = kubeJson(["-n", namespace, "get", "kafkaclusters"]).items
      .map((c) => c.metadata.uid);
    await open(page, route("clusters"));
    await armPointerCounter(page);
    const trail = [];
    trail.push(await tabTo(page, "#cluster-name", "connection name"));
    await page.keyboard.type(typedSource);
    await tabTo(page, "#cluster-servers", "bootstrap servers");
    await page.keyboard.type(state.lab.kafka);
    await tabTo(page, "#cluster-mode", "auth mode");
    const modeKeys = await keyboardSelect(page, "#cluster-mode", "scramSha512", "s");
    await tabTo(page, "#cluster-username", "auth username");
    await page.keyboard.type("scram-user");
    await tabTo(page, "#cluster-secret", "auth Secret name");
    await page.keyboard.type("orders-scram");
    const ring = await page.evaluate(() => {
      const node = document.activeElement;
      const style = window.getComputedStyle(node);
      return { id: node.id, focusVisible: node.matches(":focus-visible"),
        outline: style.outlineStyle, width: style.outlineWidth };
    });
    check(ring.focusVisible && ring.outline === "solid" && ring.width === "2px",
      "the keyboard-focused field shows no visible focus ring: " + JSON.stringify(ring));
    await tabTo(page, "#cluster-form button[type=submit]", "Create connection");
    await page.keyboard.press("Enter");
    let made = null;
    for (let i = 0; i < 40 && made === null; i += 1) {
      await pause(500);
      const added = kubeJson(["-n", namespace, "get", "kafkaclusters"]).items
        .filter((c) => clustersBefore.indexOf(c.metadata.uid) === -1);
      check(added.length <= 1, "one keyboard activation created " + added.length + " connections");
      made = added.length === 1 ? added[0] : null;
    }
    if (made === null) {
      // A repeat of the same request is recognised, not duplicated: the page
      // then says the object already existed, by name. That is this journey's
      // connection too.
      const said = await page.evaluate(() =>
        String((document.querySelector("#cluster-form-status") || {}).textContent || ""));
      const existing = kubeJson(["-n", namespace, "get", "kafkaclusters"]).items.find((c) =>
        said.indexOf("already existed") !== -1 && said.indexOf(c.metadata.name) !== -1);
      made = existing || null;
    }
    const source = made === null ? null : made.metadata.name;
    check(made !== null, "the keyboard-created connection never reached the cluster");
    check(made.spec.auth.secretRef.name === "orders-scram" && made.spec.role === "source",
      "the stored connection is not the one typed: " + JSON.stringify(made.spec));
    result.created.push({ kind: "KafkaCluster", name: source, uid: made.metadata.uid,
      createdBy: "the page (keyboard only)" });
    const statusFocus = await activeId(page);
    await shot(page, "k1-01-connection-created");

    // 2. A schedule, from the schedules page, by keyboard.
    await open(page, route("schedules"));
    await armPointerCounter(page);
    // The saved connection's probe must exist before the selector offers it.
    const sourceUid = made.metadata.uid;
    const scheduleName = "orders-" + state.suffix;
    await tabTo(page, "#schedule-name", "schedule name");
    await page.keyboard.type(scheduleName);
    await tabTo(page, "#schedule-source", "schedule source");
    const sourceKeys = await keyboardSelect(page, "#schedule-source", sourceUid, "c");
    await tabTo(page, "#policy-create-topics", "topics");
    await page.keyboard.type("orders");
    await tabTo(page, "#policy-create-destination", "destination");
    const destinationKeys = await keyboardSelect(page, "#policy-create-destination",
      state.destination, "p");
    // The form previews the cadence before it saves one (PLAT-10.1): the
    // Create button is disabled until the preview answered for these values.
    await page.keyboard.down("Shift");
    await tabTo(page, "#schedule-form button[type=button]:not(#schedule-check-readiness)",
      "Preview next runs");
    await page.keyboard.up("Shift");
    await page.keyboard.press("Enter");
    await page.waitForFunction(() => {
      const b = document.querySelector("#schedule-form button[type=submit]");
      return b !== null && !b.disabled;
    }, null, { timeout: 30000 });
    await shot(page, "k1-02-schedule-filled");
    await tabTo(page, "#schedule-form button[type=submit]", "Create schedule");
    await page.keyboard.press("Space");
    await waitFor(page, "#schedule-detail", "the redirect after a keyboard-only create", 45000);
    const scheduled = schedules().filter((s) => (s.spec.sourceRef || {}).name === source);
    check(scheduled.length === 1, "the keyboard journey did not create exactly one schedule: " +
      scheduled.length);
    state.schedule = scheduled[0].metadata.name;
    result.created.push({ kind: "BackupSchedule", name: state.schedule,
      uid: scheduled[0].metadata.uid, createdBy: "the page (keyboard only)" });
    await settled(page);
    await pause(500);
    await shot(page, "k1-03-schedule-detail");

    // 3. Back up now, by keyboard, on the detail -- and focus afterwards.
    const before = backups().map((b) => b.metadata.name);
    await tabTo(page, "form.run-now-form button[type=submit]", "Back up now");
    const usedId = await activeId(page);
    await page.keyboard.press("Enter");
    let run = [];
    for (let i = 0; i < 60 && run.length === 0; i += 1) {
      await pause(500);
      run = backups().filter((b) => before.indexOf(b.metadata.name) === -1);
    }
    check(run.length === 1, "one keyboard activation produced " + run.length + " run(s)");
    state.backup = run[0].metadata.name;
    state.backupUid = run[0].metadata.uid;
    result.created.push({ kind: "Backup", name: state.backup, uid: state.backupUid,
      createdBy: "the page (Back up now, keyboard)" });
    // The detail re-reads itself after a successful action (PLAT-10's remount).
    await page.waitForFunction(() =>
      document.querySelector(".run-now-result, [data-run-again]") !== null, null,
    { timeout: 30000 }).catch(() => null);
    await pause(1500);
    const after = await page.evaluate(() => {
      const node = document.activeElement;
      const main = document.getElementById("view-slot");
      return {
        id: node === null ? null : (node === document.body ? "(body)" : node.id || node.tagName),
        insideView: main !== null && node !== null && main.contains(node),
        isBody: node === document.body,
        text: node === null ? "" : String(node.textContent || "").trim().slice(0, 80),
      };
    });
    await shot(page, "k1-04-after-back-up-now");
    const pointers = await pointerCount(page);
    check(pointers === 0, "the configure journey sent " + pointers + " pointer event(s)");
    result.focusAfterRemount = { usedControl: usedId, after: after };
    check(!after.isBody && after.insideView,
      "after Back up now re-rendered the detail, focus was not returned into the view: " +
        JSON.stringify(after));
    record(name, {
      connection: source, typedName: typedSource, connectionUid: made.metadata.uid, modeKeys: modeKeys,
      focusAfterConnectionCreate: statusFocus, focusRing: ring,
      schedule: state.schedule, sourceKeys: sourceKeys, destinationKeys: destinationKeys,
      backup: state.backup, backupUid: state.backupUid, focusAfterRemount: after,
      pointerEvents: pointers,
    });
  } catch (error) {
    await shot(page, "k1-failure").catch(() => null);
    failed(name, error);
  } finally {
    await context.close();
  }
}

// -------------------------------------------------------- journey: restore

async function restoreJourney(browser, state, route) {
  const name = "keyboard-only restore: history, a real recovery point, the wizard, a Restore";
  check(typeof state.backup === "string", "no Backup to restore from; run the configure journey");
  const terminal = await waitForTerminalBackup(state.backup);
  const verdict = await waitForVerdict(state.backup);
  state.backupPhase = terminal.status.phase;
  state.backupVerdicts = verdict.seen;
  await syncCatalog("after-" + state.backup);
  result.backupReached = { phase: terminal.status.phase, verdicts: verdict.seen,
    windowCovered: (verdict.object.status || {}).windowCovered || null };
  check(terminal.status.phase === "Succeeded", "the Backup did not succeed: " +
    JSON.stringify(terminal.status).slice(0, 800));
  process.stderr.write("   backup " + state.backup + " " + terminal.status.phase + " " +
    verdict.seen.join(">") + "\n");

  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();
  try {
    // 1. History: Tab to THIS point's own "Restore this point" link, Enter.
    await open(page, route("history"));
    await armPointerCounter(page);
    const link = "a[href*=\"uid=" + state.backupUid + "\"][href^=\"#/restore\"]";
    const historyTrail = await tabTo(page, link, "Restore this point");
    await shot(page, "k2-01-history");
    await page.keyboard.press("Enter");
    await waitFor(page, "#target-cluster", "the wizard bound to the point", 45000);
    await settled(page);
    await pause(800);
    const hash = await page.evaluate(() => window.location.hash);
    check(hash.indexOf("uid=" + state.backupUid) !== -1, "the wizard is not bound to the point: " +
      hash);
    const focusOnArrival = await activeId(page);

    // 2. The stepper is a keyboard control: step 4's button moves focus to
    // its section.
    const stepButton = ".stepper-link[data-target]";
    const steps = await page.evaluate((sel) =>
      Array.from(document.querySelectorAll(sel)).map((b) => b.getAttribute("data-target")), stepButton);
    await tabTo(page, stepButton + "[data-target=\"" + steps[3] + "\"]", "stepper step 4");
    await page.keyboard.press("Enter");
    await pause(600);
    const afterStep = await activeId(page);
    check(afterStep === steps[3], "the stepper did not move focus to its section: " + afterStep);

    // 3. The target, by keyboard.
    const targetUid = kubeJson(["-n", namespace, "get", "kafkacluster", state.target]).metadata.uid;
    await tabTo(page, "#target-cluster", "target cluster");
    // Move OFF the target first when it is preselected, so the choice below is
    // a real change that re-renders the wizard (and a focus check that means
    // something): Home lands on the first connection, then back by keyboard.
    const preselected = await page.evaluate(() => document.querySelector("#target-cluster").value);
    let movedOff = null;
    if (preselected === targetUid) {
      // Type-ahead: "c" jumps to the page-created `conn-...` connection.
      // (Chromium on macOS opens the popup on an arrow key instead of moving
      // the selection, so the letter is what a person there would press.)
      await page.keyboard.press("c");
      await pause(1200);
      movedOff = await page.evaluate(() => ({
        value: document.querySelector("#target-cluster").value,
        active: document.activeElement === null ? null : (document.activeElement.id || "(body)"),
      }));
      check(movedOff.value !== targetUid, "type-ahead did not move the target selector off the target");
      check(movedOff.active === "target-cluster", "after a target change re-rendered the wizard " +
        "focus left the select: " + JSON.stringify(movedOff));
    }
    const targetKeys = await keyboardSelect(page, "#target-cluster", targetUid, "r");
    // The select's change re-renders the whole wizard: focus must still be
    // on the select (PLAT-18.2 focus restoration).
    await pause(1200);
    const afterTarget = await activeId(page);
    check(afterTarget === "target-cluster", "after choosing the target, focus left the select: " +
      afterTarget);

    // 4. A topic box, Space twice: each toggle re-renders the wizard, and
    // focus must stay on the box a keyboard reader is on.
    await tabTo(page, "#topic-0", "the first topic box");
    await page.keyboard.press("Space");
    await pause(1200);
    const afterUntick = await page.evaluate(() => ({
      active: document.activeElement === null ? null : (document.activeElement.id || "(body)"),
      checked: document.querySelector("#topic-0").checked,
      mapped: document.querySelectorAll("[data-datagrid=\"topic-mapping\"] tbody tr").length,
    }));
    await page.keyboard.press("Space");
    await pause(1200);
    const afterTick = await page.evaluate(() => ({
      active: document.activeElement === null ? null : (document.activeElement.id || "(body)"),
      checked: document.querySelector("#topic-0").checked,
    }));
    result.focusAfterToggle = { untick: afterUntick, tick: afterTick };
    check(afterUntick.active === "topic-0" && afterUntick.checked === false,
      "after unticking a topic the wizard re-rendered and focus left the box: " +
        JSON.stringify(afterUntick));
    check(afterTick.active === "topic-0" && afterTick.checked === true,
      "after ticking it again focus left the box: " + JSON.stringify(afterTick));
    await shot(page, "k2-02-wizard-filled");

    // 5. Create the Restore, by keyboard.
    const restoresBefore = kubeJson(["-n", namespace, "get", "restores"]).items
      .map((r) => r.metadata.uid);
    await tabTo(page, "#create-restore", "Create the Restore", 400);
    await page.keyboard.press("Enter");
    let made = [];
    for (let i = 0; i < 60 && made.length === 0; i += 1) {
      await pause(500);
      made = kubeJson(["-n", namespace, "get", "restores"]).items
        .filter((r) => restoresBefore.indexOf(r.metadata.uid) === -1);
    }
    check(made.length <= 1, "one keyboard activation created " + made.length + " Restore(s)");
    let recognised = false;
    if (made.length === 0) {
      // The same plan submitted again is recognised by its name, not
      // duplicated: the page goes on to that Restore's approval route.
      await page.waitForFunction(() => window.location.hash.indexOf("#/approvals?subject=") === 0,
        null, { timeout: 30000 });
      const subject = await page.evaluate(() =>
        decodeURIComponent(/subject=([^&]*)/.exec(window.location.hash)[1]));
      made = [kubeJson(["-n", namespace, "get", "restore", subject])];
      recognised = true;
    }
    const restore = made[0];
    state.restore = restore.metadata.name;
    result.created.push({ kind: "Restore", name: restore.metadata.name, uid: restore.metadata.uid,
      createdBy: recognised ? "the page (keyboard only; an identical earlier create recognised)"
        : "the page (keyboard only)" });
    await pause(2500);
    await settled(page).catch(() => null);
    const landed = await page.evaluate(() => ({ hash: window.location.hash,
      active: document.activeElement === null ? null :
        (document.activeElement === document.body ? "(body)" : document.activeElement.id ||
          document.activeElement.tagName) }));
    await shot(page, "k2-03-after-create");
    const pointers = await pointerCount(page);
    check(pointers === 0, "the restore journey sent " + pointers + " pointer event(s)");
    check(restore.spec.targetRef === undefined || JSON.stringify(restore.spec).indexOf(state.target) !== -1,
      "the Restore does not name the keyboard-chosen target: " + JSON.stringify(restore.spec).slice(0, 600));
    record(name, {
      point: state.backup, pointUid: state.backupUid, backupVerdicts: verdict.seen,
      historyTabs: historyTrail.length, focusOnArrival: focusOnArrival, stepperFocus: afterStep,
      targetPreselected: preselected === targetUid, movedOff: movedOff,
      targetKeys: targetKeys, focusAfterTarget: afterTarget, focusAfterToggle: result.focusAfterToggle,
      restore: restore.metadata.name, restoreUid: restore.metadata.uid, recognised: recognised,
      restorePhase: (restore.status || {}).phase || null, landedOn: landed, pointerEvents: pointers,
    });
  } catch (error) {
    await shot(page, "k2-failure").catch(() => null);
    failed(name, error, { focusAfterToggle: result.focusAfterToggle || null });
  } finally {
    await context.close();
  }
}

// ------------------------------------------------------------ focus probes

/** The two re-render focus probes, run against WHICHEVER `ui/` this pass
 *  serves. On the branch each must keep focus in the view on the control or
 *  its form's status region; on main's `ui/` (UI_E2E_LABEL=baseline) the same
 *  probes record where focus went -- the negative control for PLAT-18.2's
 *  focus restoration and for the PLAT-10 review's LOW. Neither probe creates
 *  anything new: Back up now repeats this page load's own intent, and a topic
 *  toggle is a draft edit. */
async function focusProbes(browser, state, route) {
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();
  const probes = {};
  try {
    // A. The wizard: one topic box toggled with Space re-renders everything.
    await open(page, route("restore?backup=" + encodeURIComponent(state.backup) + "&uid=" +
      encodeURIComponent(state.backupUid)));
    await waitFor(page, "#topic-0", "the wizard's first topic box");
    await tabTo(page, "#topic-0", "the first topic box", 400);
    await page.keyboard.press("Space");
    await pause(1500);
    probes.wizardToggle = await page.evaluate(() => ({
      active: document.activeElement === null ? null :
        (document.activeElement === document.body ? "(body)" : document.activeElement.id ||
          document.activeElement.tagName),
    }));
    await page.keyboard.press("Space").catch(() => null);
    // B. The schedule detail: Enter on the run-now form's submit re-reads the
    // detail after the answer (PLAT-10's remount).
    await open(page, route("schedules?name=" + encodeURIComponent(state.schedule)));
    const again = await page.$("button[data-run-again]");
    if (again !== null) {
      await tabTo(page, "button[data-run-again]", "Back up again");
      await page.keyboard.press("Enter");
      await pause(800);
    }
    await tabTo(page, "form.run-now-form button[type=submit]", "Back up now");
    await page.keyboard.press("Enter");
    await pause(4000);
    probes.scheduleRemount = await page.evaluate(() => {
      const node = document.activeElement;
      const main = document.getElementById("view-slot");
      return {
        active: node === null ? null : (node === document.body ? "(body)" : node.id ||
          node.className || node.tagName),
        insideView: main !== null && node !== null && main.contains(node),
      };
    });
    // C. The operation view announces its state in the page's one live
    // region, which no re-render replaces.
    await open(page, route("operations?kind=backup&name=" + encodeURIComponent(state.backup) +
      "&uid=" + encodeURIComponent(state.backupUid)));
    await pause(1500);
    probes.operationAnnouncement = await page.evaluate(() => {
      const region = document.getElementById("announcer");
      return region === null ? null : { text: region.textContent,
        role: region.getAttribute("role"), live: region.getAttribute("aria-live") };
    });
    result.focusProbes = probes;
    const announced = probes.operationAnnouncement !== null &&
      probes.operationAnnouncement.text.indexOf("Operation " + state.backup + ": ") === 0;
    const kept = probes.wizardToggle.active === "topic-0" && probes.scheduleRemount.insideView &&
      announced;
    if (LABEL === "baseline") {
      control("main's ui/ loses focus on a re-render (the PLAT-10 LOW, reproduced)", {
        probes: probes, lost: !kept });
    } else {
      check(kept, "focus did not survive a re-render: " + JSON.stringify(probes));
      record("focus survives the wizard's and the schedule detail's re-render", probes);
    }
  } catch (error) {
    failed("focus probes", error, { probes: probes });
  } finally {
    await context.close();
  }
}

// ------------------------------------------------------------ route pass

function routesOf(state) {
  const ns = "";
  void ns;
  return [
    ["clusters", "clusters"],
    ["cluster-detail", "clusters?name=" + state.target],
    ["destinations", "destinations"],
    ["destination-detail", "destinations?name=" + state.destination],
    ["schedules", "schedules"],
    ["schedule-detail", "schedules?name=" + encodeURIComponent(state.schedule || "")],
    ["backups", "backups"],
    ["backup-detail", "backups?name=" + encodeURIComponent(state.backup || "")],
    ["history", "history"],
    ["restore-detail", "history?name=" + encodeURIComponent(state.restore || "")],
    ["operation", "operations?kind=backup&name=" + encodeURIComponent(state.backup || "") +
      "&uid=" + encodeURIComponent(state.backupUid || "")],
    ["protection", "protection"],
    ["catalog", "catalog"],
    ["catalog-detail", "catalog?name=" + encodeURIComponent(state.catalog || "")],
    ["restore-wizard", "restore?backup=" + encodeURIComponent(state.backup || "") + "&uid=" +
      encodeURIComponent(state.backupUid || "")],
    ["approvals", "approvals"],
    ["approval-subject", "approvals?subject=" + encodeURIComponent(state.restore || "")],
    ["keys", "keys"],
  ];
}

const VARIANTS = [
  { id: "light-desktop", colorScheme: "light", width: 1280, height: 900 },
  { id: "dark-desktop", colorScheme: "dark", width: 1280, height: 900 },
  { id: "light-phone", colorScheme: "light", width: 390, height: 844 },
  { id: "dark-phone", colorScheme: "dark", width: 390, height: 844 },
];

const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"];

/** axe-core, injected from the host's offline copy. The console's CSP allows
 *  scripts from 'self' only, so this context bypasses CSP for the injection
 *  and nothing else; the page under test is the same bytes. */
async function runAxe(page) {
  await page.addScriptTag({ path: AXE });
  return page.evaluate(async (tags) => {
    const out = await window.axe.run(document, { runOnly: { type: "tag", values: tags },
      resultTypes: ["violations"] });
    return out.violations.map((v) => ({
      id: v.id, impact: v.impact, help: v.help, nodes: v.nodes.length,
      targets: v.nodes.slice(0, 4).map((n) => n.target.join(" ")),
      summary: v.nodes.slice(0, 2).map((n) => String(n.failureSummary || "").slice(0, 300)),
    }));
  }, AXE_TAGS);
}

/** The harness's own checks, for what axe does not measure. */
async function ownChecks(page) {
  return page.evaluate(() => {
    const doc = document.documentElement;
    const named = (el) => {
      if (el.getAttribute("aria-label") || el.getAttribute("aria-labelledby")) {
        return true;
      }
      if (el.tagName === "BUTTON" || el.tagName === "SUMMARY" || el.tagName === "A") {
        return el.textContent.trim().length > 0;
      }
      if (el.id && document.querySelector("label[for=\"" + CSS.escape(el.id) + "\"]")) {
        return true;
      }
      return el.closest("label") !== null;
    };
    const controls = Array.from(document.querySelectorAll(
      "main input:not([type=hidden]), main select, main textarea, main button, main summary"))
      .filter((el) => el.offsetParent !== null);
    const unnamed = controls.filter((el) => !named(el)).map((el) =>
      el.tagName.toLowerCase() + (el.id ? "#" + el.id : "") + (el.className ? "." + el.className : ""));
    // An EMPTY style attribute (Chromium leaves `style=""` on some inputs it
    // touched) carries no colour and no size; only a declaration counts.
    const inline = Array.from(document.querySelectorAll("body [style]"))
      .filter((el) => String(el.getAttribute("style")).trim().length > 0).map((el) =>
      el.tagName.toLowerCase() + "#" + el.id + " style=\"" + el.getAttribute("style") + "\"");
    const style = getComputedStyle(document.body);
    const root = getComputedStyle(doc);
    return {
      overflowX: doc.scrollWidth > window.innerWidth + 1,
      scrollWidth: doc.scrollWidth,
      innerWidth: window.innerWidth,
      controls: controls.length,
      unnamed: unnamed.slice(0, 10),
      unnamedCount: unnamed.length,
      inlineStyles: inline.slice(0, 10),
      bodyBackground: style.backgroundColor,
      appBackgroundToken: root.getPropertyValue("--cds-alias-object-app-background").trim(),
      grids: document.querySelectorAll("[data-datagrid]").length,
      pending: document.querySelectorAll("main .pending").length,
      alerts: document.querySelectorAll("main [role=alert]").length,
    };
  });
}

/** Tab from the view slot to the first control in it and read its ring. */
async function focusRing(page) {
  await page.evaluate(() => {
    const main = document.getElementById("view-slot");
    if (main !== null) {
      main.focus();
    }
  });
  for (let i = 0; i < 6; i += 1) {
    await page.keyboard.press("Tab");
    const got = await page.evaluate(() => {
      const node = document.activeElement;
      if (node === null || node === document.body) {
        return null;
      }
      const cs = getComputedStyle(node);
      return { at: node.tagName.toLowerCase() + (node.id ? "#" + node.id : ""),
        focusVisible: node.matches(":focus-visible"), outline: cs.outlineStyle,
        width: cs.outlineWidth, color: cs.outlineColor };
    });
    if (got !== null && got.at !== "main#view-slot") {
      return got;
    }
  }
  return null;
}

async function routePass(browser, state, route) {
  const onlyVariants = (process.env.UI_E2E_VARIANTS || "").split(",").filter((v) => v.length > 0);
  const onlyRoutes = (process.env.UI_E2E_ROUTES || "").split(",").filter((v) => v.length > 0);
  for (const variant of VARIANTS) {
    if (onlyVariants.length > 0 && onlyVariants.indexOf(variant.id) === -1) {
      continue;
    }
    const context = await browser.newContext({
      viewport: { width: variant.width, height: variant.height },
      colorScheme: variant.colorScheme,
      bypassCSP: true,
    });
    const page = await context.newPage();
    const consoleErrors = [];
    page.on("console", (m) => {
      if (m.type() === "error") {
        consoleErrors.push(m.text().slice(0, 300));
      }
    });
    for (const [id, hash] of routesOf(state)) {
      if (onlyRoutes.length > 0 && onlyRoutes.indexOf(id) === -1) {
        continue;
      }
      const entry = { route: id, hash: hash, variant: variant.id };
      try {
        await open(page, route(hash));
        await pause(600);
        entry.screenshot = await shot(page, "route-" + id + "-" + variant.id);
        entry.checks = await ownChecks(page);
        entry.ring = await focusRing(page);
        entry.axe = await runAxe(page);
        entry.axeViolations = entry.axe.length;
      } catch (error) {
        entry.error = error instanceof Error ? error.message.slice(0, 800) : String(error);
      }
      result.routes.push(entry);
    }
    result.consoleErrors = (result.consoleErrors || []).concat(
      Array.from(new Set(consoleErrors)).map((e) => variant.id + ": " + e));
    await context.close();
  }
  const bad = result.routes.filter((r) => r.error || r.axeViolations > 0 ||
    (r.checks && (r.checks.overflowX || r.checks.unnamedCount > 0 || r.checks.inlineStyles.length > 0)) ||
    (r.ring !== null && r.ring !== undefined && !(r.ring.outline === "solid" && r.ring.width === "2px")));
  result.routeSummary = {
    visits: result.routes.length,
    withFindings: bad.length,
    findings: bad.map((r) => ({ route: r.route, variant: r.variant, error: r.error,
      axe: (r.axe || []).map((v) => v.id + "(" + v.nodes + ")"),
      overflowX: r.checks && r.checks.overflowX, unnamed: r.checks && r.checks.unnamed,
      inline: r.checks && r.checks.inlineStyles, ring: r.ring })),
  };
  if (LABEL === "baseline") {
    control("main's ui/ route pass (recorded, not asserted)", { findings: bad.length });
  } else if (bad.length === 0) {
    record("every primary route, light/dark/phone: axe WCAG 2.1 AA clean, no overflow, named " +
      "controls, no inline style, 2px focus ring", { visits: result.routes.length });
  } else {
    failed("route pass", new Error(bad.length + " route visit(s) with findings"),
      { findings: result.routeSummary.findings });
  }
}

// ------------------------------------------------------------ state pass

function isList(url, plural) {
  const path = new URL(url).pathname;
  return new RegExp("/api/v1/namespaces/[^/]+/" + plural + "$").test(path);
}

async function statePass(browser, state, route) {
  const lists = [
    ["backups", "backups", ["backups"]],
    ["history", "history", ["restores", "backups"]],
    ["schedules", "schedules", ["schedules"]],
    ["clusters", "clusters", ["connections"]],
  ];
  for (const [id, hash, plurals] of lists) {
    for (const mode of ["slow", "error", "empty"]) {
      const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
      const page = await context.newPage();
      const entry = { route: id, state: mode };
      try {
        await page.route((url) => plurals.some((p) => isList(url.toString(), p)), async (r) => {
          if (mode === "slow") {
            await pause(4000);
            await r.continue();
          } else if (mode === "error") {
            await r.fulfill({ status: 500, contentType: "application/problem+json",
              body: JSON.stringify({ type: "about:blank", title: "Internal Server Error",
                status: 500, detail: "PLAT-18.2 injected failure", code: "internal",
                requestId: "p182-injected" }) });
          } else {
            const real = await r.fetch();
            const body = await real.json();
            body.items = [];
            await r.fulfill({ response: real, body: JSON.stringify(body) });
          }
        });
        await page.goto(route(hash), { waitUntil: "load", timeout: 30000 });
        if (mode === "slow") {
          await pause(1500);
          entry.during = await page.evaluate(() => {
            const p = document.querySelector("#view-slot > .pending");
            return p === null ? null : { role: p.getAttribute("role"), text: p.textContent.trim() };
          });
          entry.screenshot = await shot(page, "state-" + id + "-slow-loading");
          await settled(page, 30000);
          entry.after = await page.evaluate(() => document.querySelectorAll("main table").length);
          check(entry.during !== null && entry.during.role === "status",
            "no role=status loading line while the read was slow");
        } else if (mode === "error") {
          await settled(page, 30000);
          await pause(500);
          entry.alert = await page.evaluate(() => {
            const a = document.querySelector("main [role=alert]");
            return a === null ? null : a.textContent.trim().slice(0, 300);
          });
          entry.screenshot = await shot(page, "state-" + id + "-error");
          check(entry.alert !== null && entry.alert.indexOf("injected") !== -1,
            "the refused read was not rendered as a role=alert error naming it");
        } else {
          await settled(page, 30000);
          await pause(500);
          entry.empty = await page.evaluate(() => Array.from(document.querySelectorAll(
            "main td.empty, main .empty-state")).map((n) => n.textContent.trim().slice(0, 200)));
          entry.screenshot = await shot(page, "state-" + id + "-empty");
          check(entry.empty.length > 0, "an empty list rendered no empty-state sentence");
        }
        entry.ok = true;
      } catch (error) {
        entry.ok = false;
        entry.error = error instanceof Error ? error.message.slice(0, 600) : String(error);
      }
      result.states.push(entry);
      await context.close();
    }
  }
  const bad = result.states.filter((s) => !s.ok);
  if (LABEL === "baseline") {
    control("main's ui/ loading/error/empty states (recorded)", { failures: bad.length });
  } else if (bad.length === 0) {
    record("slow network, error and empty states on backups, history, schedules and clusters",
      { visits: result.states.length });
  } else {
    failed("state pass", new Error(bad.length + " state visit(s) failed"),
      { failures: bad.map((b) => b.route + "/" + b.state + ": " + b.error) });
  }
}

// ------------------------------------------------------------ large data

function inflate(items, count, key) {
  const out = [];
  for (let i = 0; i < count; i += 1) {
    const copy = JSON.parse(JSON.stringify(items[i % items.length]));
    const n = String(i).padStart(5, "0");
    copy.name = String(copy.name) + "-x" + n;
    copy.uid = "00000000-0000-4000-8" + key + "00-" + n.padStart(12, "0");
    if (typeof copy.createdAt === "string") {
      copy.createdAt = new Date(Date.parse(copy.createdAt) - i * 60000).toISOString()
        .replace(/\.\d+Z$/, "Z");
    }
    out.push(copy);
  }
  return out;
}

async function timeHash(page, hash, ready) {
  await page.evaluate((h) => {
    window.__p182t0 = performance.now();
    window.location.hash = h;
  }, hash);
  await page.waitForFunction(ready, null, { timeout: 120000, polling: "raf" });
  return page.evaluate(() => {
    // Force the layout the reader waits for before reading the clock.
    void document.body.offsetHeight;
    return Math.round(performance.now() - window.__p182t0);
  });
}

async function largePass(browser, state, route) {
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();
  const entry = { label: LABEL };
  try {
    await page.route((url) => isList(url.toString(), "backups") || isList(url.toString(), "restores"),
      async (r) => {
        const real = await r.fetch();
        const body = await real.json();
        const restores = isList(r.request().url(), "restores");
        if (Array.isArray(body.items) && body.items.length > 0) {
          body.items = inflate(body.items, 500, restores ? "2" : "1");
        }
        await r.fulfill({ response: real, body: JSON.stringify(body) });
      });
    await open(page, route("clusters"));
    const nsHash = "?ns=" + encodeURIComponent(namespace);
    const rowsReady = () => {
      const main = document.getElementById("view-slot");
      return main !== null && main.querySelector(":scope > .pending") === null &&
        main.querySelectorAll("table.grid tbody tr").length > 0;
    };
    entry.historyFirstPaintMs = await timeHash(page, "#/history" + nsHash, rowsReady);
    entry.historyDom = await page.evaluate(() => ({
      rowsInDocument: document.querySelectorAll("main table.grid tbody tr").length,
      rowsVisible: Array.from(document.querySelectorAll("main table.grid tbody tr"))
        .filter((r) => r.offsetParent !== null).length,
      count: (document.querySelector("#history-count") || {}).textContent || null,
      nodes: document.getElementsByTagName("*").length,
    }));
    await shot(page, "large-history-1000");
    if (await page.$("#history-filter") !== null) {
      // A filter keystroke and a page change, timed to the live count.
      entry.historyFilterMs = await page.evaluate(async () => {
        const input = document.getElementById("history-filter");
        const count = document.getElementById("history-count");
        const before = count.textContent;
        const t0 = performance.now();
        input.value = "x00042";
        input.dispatchEvent(new Event("input", { bubbles: true }));
        void document.body.offsetHeight;
        return { ms: Math.round(performance.now() - t0), before: before, after: count.textContent };
      });
      await page.evaluate(() => {
        const input = document.getElementById("history-filter");
        input.value = "";
        input.dispatchEvent(new Event("input", { bubbles: true }));
      });
      entry.historyNextPageMs = await page.evaluate(() => {
        const t0 = performance.now();
        document.getElementById("history-next").click();
        void document.body.offsetHeight;
        return { ms: Math.round(performance.now() - t0),
          count: document.getElementById("history-count").textContent };
      });
    }
    entry.backupsFirstPaintMs = await timeHash(page, "#/backups" + nsHash, rowsReady);
    entry.backupsDom = await page.evaluate(() => ({
      rowsInDocument: document.querySelectorAll("main table.grid tbody tr").length,
      nodes: document.getElementsByTagName("*").length,
    }));
    await page.unroute("**");
  } catch (error) {
    entry.listError = error instanceof Error ? error.message.slice(0, 600) : String(error);
  } finally {
    await context.close();
  }

  // The restore wizard over a point that froze 2,000 topics.
  const wizard = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page2 = await wizard.newPage();
  try {
    const topics = Array.from({ length: 2000 }, (_, i) => "orders.region-" + String(i % 40) +
      ".stream-" + String(i).padStart(5, "0"));
    await page2.route((url) => {
      const path = new URL(url.toString()).pathname;
      return /\/api\/v1\/namespaces\/[^/]+\/backups(\/[^/]+)?$/.test(path);
    }, async (r) => {
      const real = await r.fetch();
      const body = await real.json();
      const touch = (item) => {
        if (item && item.uid === state.backupUid && Array.isArray(item.topics)) {
          item.topics = topics.slice();
        }
      };
      (Array.isArray(body.items) ? body.items : []).forEach(touch);
      touch(body);
      touch(body.item);
      await r.fulfill({ response: real, body: JSON.stringify(body) });
    });
    await open(page2, route("clusters"));
    const hash = "#/restore?ns=" + encodeURIComponent(namespace) + "&backup=" +
      encodeURIComponent(state.backup) + "&uid=" + encodeURIComponent(state.backupUid);
    entry.wizardFirstPaintMs = await timeHash(page2, hash, () =>
      document.querySelectorAll(".topic-box").length >= 2000);
    entry.wizardDom = await page2.evaluate(() => ({
      boxes: document.querySelectorAll(".topic-box").length,
      boxesVisible: Array.from(document.querySelectorAll(".topic-box"))
        .filter((b) => b.offsetParent !== null).length,
      mappingRowsInDocument: document.querySelectorAll("[data-datagrid=\"topic-mapping\"] tbody tr, " +
        "#topic-subset ~ .table-wrap tbody tr").length,
      nodes: document.getElementsByTagName("*").length,
    }));
    await shot(page2, "large-wizard-2000-topics");
    // One checkbox toggled with the keyboard: time until the re-rendered box
    // exists (the whole wizard is replaced on a change).
    await tabTo(page2, "#topic-0", "the first of 2,000 topic boxes", 600);
    const toggles = [];
    for (let i = 0; i < 3; i += 1) {
      // A toggle is only a toggle while focus is on the box: where a
      // re-render dropped focus (main's `ui/`), the next Space would scroll
      // the page, and that loss is the finding recorded instead.
      if ((await activeId(page2)) !== "topic-0") {
        entry.wizardFocusLostAfterToggle = i;
        break;
      }
      await page2.evaluate(() => {
        document.getElementById("topic-0").__p182old = true;
        window.__p182t0 = performance.now();
      });
      await page2.keyboard.press("Space");
      await page2.waitForFunction(() => {
        const box = document.getElementById("topic-0");
        return box !== null && box.__p182old !== true;
      }, null, { timeout: 120000, polling: "raf" });
      toggles.push(await page2.evaluate(() => {
        void document.body.offsetHeight;
        return Math.round(performance.now() - window.__p182t0);
      }));
      await pause(300);
    }
    entry.wizardToggleMs = toggles;
    entry.wizardFocusAfterToggle = await activeId(page2);
  } catch (error) {
    entry.wizardError = error instanceof Error ? error.message.slice(0, 600) : String(error);
  } finally {
    await wizard.close();
  }
  result.measurements.push(entry);
  if (entry.listError || entry.wizardError) {
    failed("large datasets", new Error(String(entry.listError || entry.wizardError)));
  } else {
    record("large datasets measured (1,000 history rows, 500 backup rows, 2,000 frozen topics)", entry);
  }
}
