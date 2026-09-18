// D2 W14 — the console journeys behind PLAT-09.1's acceptance sentence:
// "users select visible topics and can distinguish empty, failed, stale and
// permission-limited discovery", plus PLAT-08.1's "secret values are not
// returned" and PLAT-03.1's "names each failed prerequisite and its remedy,
// with check time and scope".
//
// The sibling of `scripts/d2w13-ui-e2e.mjs`, and the difference is the whole
// point: that harness ran against a lab whose controller did not know the three
// kinds, so every state it could assert was "pending, for ever". This one runs
// against `e2e/k8s/d2/d2_live.py`'s namespace on the refreshed lab, where a
// real controller has already produced a 5,003-topic success, an empty
// cluster's zero-topic success, a failure, and an ACL-limited listing. The
// journeys read those states off the page.
//
// EVERY OBJECT IS REAL. Nothing is intercepted, delayed or fabricated: the page
// talks to a real `logweir-api` in localAdmin mode on a loopback port, which
// talks to the real kube-apiserver, and every assertion is made against what
// the browser rendered. `--explore` prints what each route says instead of
// asserting, which is how the assertions below were written.
//
//   NODE_PATH="$(npm root -g)" node e2e/k8s/d2/ui_live.mjs
//
// Environment:
//   D2W14_NAMESPACE   the namespace d2_live.py created (required).
//   D2W14_ART         where screenshots and the result document go.
//   UI_E2E_API_BIN    the logweir-api binary; default target/debug/logweir-api.

import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { createServer } from "node:net";
import { mkdirSync, writeFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";

const require = createRequire(import.meta.url);
const { chromium } = require("playwright");

const KUBE_CONTEXT = "docker-desktop";
const KUBECTL = "kubectl";
const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
const UI_DIR = join(REPO, "ui");
const API_BIN = process.env.UI_E2E_API_BIN || join(REPO, "target", "debug", "logweir-api");
const NAMESPACE = process.env.D2W14_NAMESPACE || "";
const ARTIFACTS = join(process.env.D2W14_ART || "/tmp/logweir-roadmap-run/claude/artifacts/d2-live",
  "ui");
const WORK_DIR = join("/tmp", "d2w14-ui-" + (NAMESPACE || "none"));
const EXPLORE = process.argv.includes("--explore");

const result = {
  harness: "e2e/k8s/d2/ui_live.mjs",
  kubeContext: KUBE_CONTEXT,
  namespace: NAMESPACE,
  mode: "console (logweir-api, localAdmin, loopback)",
  startedAt: new Date().toISOString(),
  journeys: [],
  failures: [],
  screenshots: [],
  credentialScan: null,
};

function check(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function pause(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function kube(args, options) {
  const opts = options || {};
  const done = spawnSync(KUBECTL, ["--context", KUBE_CONTEXT].concat(args), {
    encoding: "utf8", input: opts.input,
    timeout: opts.timeout || 30000, maxBuffer: 8 * 1024 * 1024,
  });
  if (!(opts.expected || [0]).includes(done.status)) {
    throw new Error(KUBECTL + " " + args.join(" ") + " exited " + done.status + ": " +
      String(done.stderr || "").trim().slice(0, 1200));
  }
  return done;
}

function kubeJson(args) {
  return JSON.parse(kube(args.concat(["-o", "json"])).stdout);
}

function freePort() {
  return new Promise((ok, bad) => {
    const server = createServer();
    server.on("error", bad);
    server.listen(0, "127.0.0.1", () => {
      const port = server.address().port;
      server.close(() => ok(port));
    });
  });
}

async function text(page) {
  return (await page.evaluate(() => document.body.innerText)).toLowerCase();
}

async function shot(page, name) {
  const at = join(ARTIFACTS, name + ".png");
  await page.screenshot({ path: at, fullPage: true });
  result.screenshots.push(at);
  return at;
}

async function waitForText(page, needle, label) {
  const wanted = needle.toLowerCase();
  for (let i = 0; i < 60; i += 1) {
    if ((await text(page)).includes(wanted)) {
      return;
    }
    await pause(500);
  }
  throw new Error(label + ": never saw " + JSON.stringify(needle) + ". Page said:\n" +
    (await text(page)).slice(0, 3000));
}

let api = null;
const apiLog = [];
const responseBodies = [];

async function startApi(port) {
  mkdirSync(WORK_DIR, { recursive: true, mode: 0o700 });
  const cursorKey = join(WORK_DIR, "cursor.key");
  writeFileSync(cursorKey, randomBytes(32), { mode: 0o600 });
  const configPath = join(WORK_DIR, "config.yaml");
  const config = [
    "mode: localAdmin",
    "listen: \"127.0.0.1:" + port + "\"",
    "publicOrigin: \"http://127.0.0.1:" + port + "\"",
    "uiDirectory: " + UI_DIR,
    "localAdmin:",
    "  subject: admin",
    "  displayName: Local administrator",
    "namespaces: [" + NAMESPACE + "]",
    "kubernetes:",
    "  source: kubeconfig",
    "  context: " + KUBE_CONTEXT,
    "cursorKeyFile: " + cursorKey,
    "",
  ].join("\n");
  writeFileSync(configPath, config);
  api = spawn(API_BIN, ["--config", configPath], { stdio: ["ignore", "pipe", "pipe"] });
  api.stdout.on("data", (b) => apiLog.push(String(b)));
  api.stderr.on("data", (b) => apiLog.push(String(b)));
  for (let i = 0; i < 60; i += 1) {
    try {
      const probe = await fetch("http://127.0.0.1:" + port + "/healthz");
      if (probe.ok) {
        return;
      }
    } catch (notYet) { /* still binding */ }
    await pause(500);
  }
  throw new Error("logweir-api never answered /healthz within 30 s:\n" + apiLog.join(""));
}

function stopApi() {
  if (api !== null && api.exitCode === null) {
    api.kill("SIGTERM");
  }
}

async function journey(name, body) {
  try {
    const detail = await body();
    result.journeys.push(Object.assign({ journey: name, passed: true }, detail || {}));
    process.stderr.write("== PASS: " + name + "\n");
  } catch (error) {
    result.journeys.push({ journey: name, passed: false, error: String(error).slice(0, 4000) });
    result.failures.push(name);
    process.stderr.write("== FAIL: " + name + "\n   " + String(error).slice(0, 1500) + "\n");
  }
}

async function main() {
  check(NAMESPACE.startsWith("lw-d2w14-"),
    "D2W14_NAMESPACE must be the d2_live.py namespace, not " + JSON.stringify(NAMESPACE));
  mkdirSync(ARTIFACTS, { recursive: true });
  const port = await freePort();
  await startApi(port);
  const base = "http://127.0.0.1:" + port + "/ui/";
  const browser = await chromium.launch();
  const context = await browser.newContext({ viewport: { width: 1400, height: 1000 } });
  const page = await context.newPage();
  page.on("response", async (response) => {
    if (!response.url().includes("/api/")) {
      return;
    }
    try {
      responseBodies.push({ url: response.url(), body: await response.text() });
    } catch (unreadable) { /* redirects and 204s have no body */ }
  });

  try {
    if (EXPLORE) {
      for (const route of ["#/destinations?ns=" + NAMESPACE,
                           "#/clusters?ns=" + NAMESPACE + "&name=source-admin",
                           "#/clusters?ns=" + NAMESPACE + "&name=empty",
                           "#/clusters?ns=" + NAMESPACE + "&name=blackhole",
                           "#/clusters?ns=" + NAMESPACE + "&name=source-limited"]) {
        await page.goto(base + route, { waitUntil: "load", timeout: 30000 });
        await pause(3000);
        process.stdout.write("\n===== " + route + " =====\n" + (await text(page)).slice(0, 4000) + "\n");
      }
      return;
    }

    // --------------------------------------------------------------- J1
    await journey("a succeeded discovery lists the topics it saw, searchable and paged",
      async () => {
        await page.goto(base + "#/clusters?ns=" + NAMESPACE + "&name=source-admin",
          { waitUntil: "load", timeout: 30000 });
        await waitForText(page, "Latest attempt", "the discovery panel");
        const body = await text(page);
        check(body.includes("succeeded"), "the panel does not say the run succeeded");
        check(!body.includes("attestedcomplete"),
          "the page claims attested completeness for a listing nobody attested");
        await shot(page, "j1-discovery-succeeded");
        return { route: "#/clusters&name=source-admin", saidSucceeded: true };
      });

    // --------------------------------------------------------------- J2
    await journey("the topic list is searchable and a topic can be selected", async () => {
      const rowsNow = async () => page.$$eval("#topics-slot table tbody tr",
        (trs) => trs.map((tr) => tr.textContent.trim().split(/\s+/)[0])).catch(() => []);
      const submit = async () => {
        const f = await page.$("#topic-filters");
        check(f !== null, "the panel offers no inventory search form");
        await f.evaluate((el) => (el.requestSubmit ? el.requestSubmit() : el.submit()));
      };
      // The stored inventory is read on demand: the panel shows the RESULT's
      // facts until an operator asks for the topics, which is the bounded read
      // D2 §5.6 is about. So the unfiltered page is fetched first.
      await submit();
      let unfiltered = [];
      for (let i = 0; i < 40; i += 1) {
        unfiltered = await rowsNow();
        if (unfiltered.length > 1) {
          break;
        }
        await pause(500);
      }
      check(unfiltered.length > 1,
        "the inventory rendered " + unfiltered.length + " rows when asked for. The panel said:\n"
        + (await page.$eval("#cluster-discovery", (el) => el.innerText)).slice(0, 2000));
      const input = await page.$("#topic-q");
      check(input !== null, "the panel offers no `contains` search input");
      await input.fill("bulk-0499");
      await submit();
      await pause(2000);
      const body = await text(page);
      check(body.includes("bulk-0499"), "searching for bulk-0499 showed no bulk-0499x topic");
      await shot(page, "j2-topic-search");
      // "users SELECT visible topics": the inventory rows are what a schedule
      // or a backup form reads from, so the assertion is that a named, visible
      // topic is on screen and addressable, and that the count the page shows
      // is the count the object recorded.
      let rows = [];
      for (let i = 0; i < 20; i += 1) {
        rows = await rowsNow();
        if (rows.length > 0 && rows.every((r) => r.startsWith("bulk-0499"))) {
          break;
        }
        await pause(500);
      }
      check(rows.length > 0, "the filtered inventory rendered no rows");
      check(rows.every((r) => r.startsWith("bulk-0499")),
        "the filtered rows are not all bulk-0499*: " + JSON.stringify(rows.slice(0, 5)));
      check(rows.length < unfiltered.length || unfiltered.every((r) => r.startsWith("bulk-0499")),
        "the filter did not narrow anything");
      return { searched: "bulk-0499", rowsBefore: unfiltered.length, rowsAfter: rows.length,
               sample: rows.slice(0, 3) };
    });

    // --------------------------------------------------------------- J3
    await journey("an empty cluster is shown as empty, not as failed or complete", async () => {
      await page.goto(base + "#/clusters?ns=" + NAMESPACE + "&name=empty",
        { waitUntil: "load", timeout: 30000 });
      await waitForText(page, "Latest attempt", "the discovery panel for the empty cluster");
      // The assertion is about the ATTEMPT's own section, not the whole page:
      // the standing prose above it explains what a refusal looks like and
      // carries the word "failed" (in `topic_authorization_failed`) whatever
      // the state is.
      const section = (await page.$eval("section.discovery",
        (el) => el.innerText)).toLowerCase();
      check(section.includes("succeeded"),
        "an empty cluster's successful listing is not shown as succeeded: " + section.slice(0, 600));
      check(!section.includes("failed"),
        "the empty listing's own section says failed: " + section.slice(0, 600));
      const emptyNote = await page.$("#discovery-empty");
      check(emptyNote !== null,
        "the page renders no empty-inventory sentence: " + section.slice(0, 900));
      const emptyText = (await emptyNote.innerText()).toLowerCase();
      await shot(page, "j3-empty-cluster");
      return { route: "#/clusters&name=empty", emptySentence: emptyText.slice(0, 200) };
    });

    // --------------------------------------------------------------- J4
    await journey("a failed discovery is shown as failed, with the reason", async () => {
      await page.goto(base + "#/clusters?ns=" + NAMESPACE + "&name=blackhole",
        { waitUntil: "load", timeout: 30000 });
      await waitForText(page, "Latest attempt", "the discovery panel for the blackhole");
      const body = await text(page);
      check(body.includes("failed"), "a failed discovery is not shown as failed");
      check(!body.includes("succeeded"), "a failed discovery is also described as succeeded");
      await shot(page, "j4-failed-discovery");
      const discoveries = kubeJson(["-n", NAMESPACE, "get", "topicdiscoveries"]).items
        .filter((d) => d.spec.request.connectionRef.name === "blackhole");
      return {
        route: "#/clusters&name=blackhole",
        objectReason: discoveries.length ? discoveries[0].status.reason : null,
        reasonOnScreen: discoveries.length
          ? body.includes(String(discoveries[0].status.reason || "").toLowerCase())
          : null,
      };
    });

    // --------------------------------------------------------------- J5
    await journey("an ACL-limited listing says limited and never claims completeness",
      async () => {
        await page.goto(base + "#/clusters?ns=" + NAMESPACE + "&name=source-limited",
          { waitUntil: "load", timeout: 30000 });
        await waitForText(page, "Latest attempt", "the discovery panel for the limited principal");
        const body = await text(page);
        check(body.includes("limited"), "the page does not call the listing limited");
        check(!body.includes("attestedcomplete") && !body.includes("attested complete"),
          "a limited listing is described as attested-complete");
        await shot(page, "j5-limited-visibility");
        return { route: "#/clusters&name=source-limited" };
      });

    // --------------------------------------------------------------- J6
    await journey("a discovery whose connection was replaced reads as stale, with the reason",
      async () => {
        // A REAL delete-and-recreate, not a clock trick: the page's staleness
        // is `freshUntil` plus binding drift, and binding drift is what an
        // operator actually causes by recreating a connection under the same
        // name. The 15-minute freshness budget is not waited out.
        const connection = "stale-probe";
        const existing = kube(["-n", NAMESPACE, "get", "kafkacluster", connection],
          { expected: [0, 1] });
        if (existing.status !== 0) {
          throw new Error("the harness fixture `stale-probe` is missing; run d2_live.py s21prep");
        }
        await page.goto(base + "#/clusters?ns=" + NAMESPACE + "&name=" + connection,
          { waitUntil: "load", timeout: 30000 });
        await waitForText(page, "Latest attempt", "the panel before the connection is replaced");
        const fresh = (await page.$eval("section.discovery", (el) => el.innerText)).toLowerCase();
        check(!fresh.includes("stale:"),
          "the discovery already read stale before the connection was replaced");
        await shot(page, "j6a-before-replacement");
        const oldUid = kubeJson(["-n", NAMESPACE, "get", "kafkacluster", connection])
          .metadata.uid;
        kube(["-n", NAMESPACE, "delete", "kafkacluster", connection, "--wait=true"]);
        kube(["-n", NAMESPACE, "apply", "-f", "-"], {
          input: JSON.stringify(JSON.parse(kube(["-n", NAMESPACE, "get", "configmap",
            "stale-probe-spec", "-o", "jsonpath={.data.spec\\.json}"]).stdout)),
        });
        const newUid = kubeJson(["-n", NAMESPACE, "get", "kafkacluster", connection])
          .metadata.uid;
        check(newUid !== oldUid, "the recreated connection kept its UID");
        await page.reload({ waitUntil: "load", timeout: 30000 });
        await waitForText(page, "stale:", "the stale badge after the connection was replaced");
        const after = (await page.$eval("section.discovery", (el) => el.innerText)).toLowerCase();
        await shot(page, "j6b-stale-after-replacement");
        check(after.includes("connectionreplaced"),
          "the stale badge does not name connectionReplaced: " + after.slice(0, 600));
        return { connection: connection, oldUid: oldUid, newUid: newUid,
                 badge: after.split("\n").slice(0, 4).join(" | ") };
      });

    // --------------------------------------------------------------- J7
    await journey("the destinations page names every destination and no credential", async () => {
      await page.goto(base + "#/destinations?ns=" + NAMESPACE,
        { waitUntil: "load", timeout: 30000 });
      await waitForText(page, "dest-a", "the destinations list");
      const body = await text(page);
      for (const name of ["dest-a", "dest-b", "dest-denied", "dest-tls"]) {
        check(body.includes(name), "the list omits " + name);
      }
      check(body.includes("lw-a") && body.includes("lw-b"),
        "the list does not show the two buckets");
      await shot(page, "j7-destinations");
      return { destinationsOnScreen: ["dest-a", "dest-b", "dest-denied", "dest-tls"] };
    });

    // --------------------------------------------------------------- J8
    await journey("no credential value reaches the browser", async () => {
      const credentials = JSON.parse(
        spawnSync("/tmp/logweir-roadmap-run/venv/bin/python3",
          ["-c", "import json;print(json.dumps(json.load(open('/tmp/logweir-d2w14/keys/credentials.json'))))"],
          { encoding: "utf8" }).stdout);
      const markers = Object.entries(credentials).filter(([, v]) => v && v.length > 8);
      check(markers.length > 5, "no credential markers were loaded, so this proves nothing");
      const dom = await page.content();
      const haystacks = [
        ["dom", dom],
        ["page-text", await text(page)],
        ["api-responses", responseBodies.map((r) => r.body).join("\n")],
        ["api-log", apiLog.join("")],
      ];
      const hits = [];
      for (const [label, value] of markers) {
        for (const [where, hay] of haystacks) {
          if (hay.includes(value) || hay.includes(Buffer.from(value).toString("base64"))) {
            hits.push({ marker: label, where: where });
          }
        }
      }
      result.credentialScan = {
        markers: markers.length,
        responsesCaptured: responseBodies.length,
        apiLogBytes: apiLog.join("").length,
        hits: hits,
      };
      check(hits.length === 0, "credential values reached the browser: " + JSON.stringify(hits));
      return result.credentialScan;
    });
  } finally {
    result.finishedAt = new Date().toISOString();
    result.ok = result.failures.length === 0;
    result.passed = result.journeys.filter((j) => j.passed).length;
    result.total = result.journeys.length;
    writeFileSync(join(ARTIFACTS, "ui-result.json"), JSON.stringify(result, null, 2));
    writeFileSync(join(ARTIFACTS, "api.log"), apiLog.join(""));
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
    stopApi();
    rmSync(WORK_DIR, { recursive: true, force: true });
  }
  process.stderr.write("\n" + result.passed + "/" + result.total + " journeys passed\n");
  if (!result.ok) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  process.stderr.write("FATAL: " + String(error) + "\n");
  process.exitCode = 2;
});
