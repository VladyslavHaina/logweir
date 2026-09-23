// PLAT-18.2: the render cost of the console's dense lists, measured in node.
//
//     node scripts/plat18-2-measure.mjs [ui-directory]
//
// WHAT THIS MEASURES, exactly and only: the time the page modules' own pure
// string functions take to render a large, synthetic but contract-shaped
// dataset -- 1,000 history rows (500 Backups and 500 Restores), 1,000 backup
// rows, and a recovery point freezing 2,000 topics rendered through the
// restore wizard's topic subset and through the whole wizard. It does not
// measure parsing, layout or paint; the browser half of the measurement is
// `scripts/plat18-2-ui-e2e.mjs`, which times the same datasets in Chromium.
//
// It exists so the framework and virtualization decision PLAT-18.2 asks for
// is taken against numbers, and so the SAME numbers can be taken against any
// tree: pass the directory of another `ui/` (for example main's, extracted
// with `git archive <rev> ui`) to measure the baseline.
//
// No network, no clock but `performance.now`, no file written: the result is
// one JSON document on stdout.

import { performance } from "node:perf_hooks";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const UI = resolve(process.argv[2] || join(REPO, "ui"));
const FIXTURES = join(REPO, "ui", "tests", "fixtures");
const RUNS = Number(process.env.MEASURE_RUNS || "7");

function fixture(name) {
  return JSON.parse(readFileSync(join(FIXTURES, name), "utf8"));
}

async function load(relative) {
  return import(pathToFileURL(join(UI, relative)).href);
}

function pad(n, width) {
  return String(n).padStart(width, "0");
}

/** `count` Backups cloned from a real fixture, each with its own name, uid and
 *  creation instant, newest first. */
function backups(count) {
  const template = fixture("backup-valid-exit0.json");
  const items = [];
  for (let i = 0; i < count; i += 1) {
    const copy = JSON.parse(JSON.stringify(template));
    copy.metadata.name = "orders-hourly-" + pad(i, 6);
    copy.metadata.uid = "00000000-0000-4000-8000-" + pad(i, 12);
    copy.metadata.creationTimestamp = new Date(Date.UTC(2026, 8, 1) + i * 60000).toISOString()
      .replace(/\.\d+Z$/, "Z");
    items.push(copy);
  }
  return { apiVersion: "logweir.dev/v1alpha1", kind: "BackupList", items: items };
}

function restores(count) {
  const template = fixture("restore-valid-pass.json");
  const items = [];
  for (let i = 0; i < count; i += 1) {
    const copy = JSON.parse(JSON.stringify(template));
    copy.metadata.name = "orders-drill-" + pad(i, 6);
    copy.metadata.uid = "00000000-0000-4000-9000-" + pad(i, 12);
    copy.metadata.creationTimestamp = new Date(Date.UTC(2026, 8, 1) + i * 60000 + 30000)
      .toISOString().replace(/\.\d+Z$/, "Z");
    items.push(copy);
  }
  return { apiVersion: "logweir.dev/v1alpha1", kind: "RestoreList", items: items };
}

function topics(count) {
  const out = [];
  for (let i = 0; i < count; i += 1) {
    out.push("orders.region-" + pad(i % 40, 2) + ".stream-" + pad(i, 5));
  }
  return out;
}

async function time(label, fn) {
  const samples = [];
  let size = 0;
  for (let i = 0; i < RUNS; i += 1) {
    const start = performance.now();
    const out = await fn();
    samples.push(performance.now() - start);
    size = typeof out === "string" ? out.length : size;
  }
  samples.sort((a, b) => a - b);
  return {
    label: label,
    runs: RUNS,
    medianMs: Number(samples[Math.floor(samples.length / 2)].toFixed(2)),
    maxMs: Number(samples[samples.length - 1].toFixed(2)),
    htmlBytes: size,
  };
}

async function main() {
  const history = await load("pages/history.js");
  const backupsPage = await load("pages/backups.js");
  const wizard = await load("pages/restore-wizard.js");

  const b500 = backups(500);
  const r500 = restores(500);
  const b1000 = backups(1000);

  const point = JSON.parse(JSON.stringify(fixture("wizard-backups.json")));
  point.items[0].spec.topics = topics(2000);
  const newest = wizard.recoveryPoints(point)[0];
  const selection = { uid: newest.metadata.uid, backup: newest.metadata.name };
  const state = wizard.initialState("logweir-t27", fixture("wizard-clusters.json"), point, selection);

  const results = [
    await time("history list, 1,000 rows (500 Backups + 500 Restores)",
      () => history.renderHistoryList(r500, b500, "ns")),
    await time("backups list, 1,000 rows", () => backupsPage.renderBackupList(b1000, "ns")),
    await time("restore wizard topic subset, 2,000 frozen topics",
      () => wizard.renderTopicSubset(state)),
    await time("whole restore wizard, 2,000 frozen topics (one keystroke re-renders this)",
      () => wizard.renderRestoreWizard(state)),
  ];
  process.stdout.write(JSON.stringify({ ui: UI, node: process.version, results: results }, null, 2) +
    "\n");
}

main().catch((error) => {
  process.stderr.write(String((error && error.stack) || error) + "\n");
  process.exit(1);
});
