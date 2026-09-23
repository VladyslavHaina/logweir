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
}
