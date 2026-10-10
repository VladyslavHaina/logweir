// backups-page-binding.spec.js -- FX-35: a restore started from a Backup (the
// Backups, History and Schedules pages open the wizard on
// `#/restore?ns=..&backup=..&uid=..`) is bound to that Backup's recovery point
// exactly as the catalog flow binds one, and says so when it cannot be.
//
// THROUGH THE PRODUCT'S OWN DECODERS. Every row mounts the wizard in console
// mode with the real client (`ui/client.js`) and the real D3 readers
// (`ui/operation-watch.js`): the transport answers with the product API's
// documents, so a field `ui/contract.js` does not declare is dropped before the
// page sees it, and the row fails (R2).
//
// THE POINT IS THE POC'S. `lwp1-ee285127...` is the 1.3.0 point PoC batch 5
// restored both ways (`poc-batch-5/fx21/catalog/api-0.json`): unbound from a
// Backup (time basis NOT RECORDED, `configuration (unknown)`) and bound from
// the catalog (both recorded).

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { resetMode, selectMode } from "../client.js";
import { dropDraft, formKey } from "../lifecycle.js";
import {
  WIZARD_FORM,
  mountRestoreWizard,
  restoreCatalogPointRoute,
  restorePointRoute,
  restoreRouteParams,
} from "../pages/restore-wizard.js";
import { fakeView, parse as viewParse } from "./fake-view.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/console/", import.meta.url));
const con = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

const NS = "team-a";
/** The destination's wire document; the Backups below are written against it. */
const DESTINATION_DOC = con("destination.json");
const DESTINATION = DESTINATION_DOC.item;

/** The 1.3.0 point, as the PoC's API served it (ids kept, namespace moved). */
const OLD = {
  name: "logweir-manual-xta3lxnm7kejxzuytlrpe45qnw",
  uid: "6c9b9b35-b9ce-4104-a8fd-7347bd40b49f",
  pointId: "lwp1-ee285127c4ce5123d9de1b7d2017b52b",
  receipt: "sha256:ee285127c4ce5123d9de1b7d2017b52b8cd9b4949fc317ef266d635f9424cd71",
  runId: "01M4H3RCHMW1KSPG151ES0KMGT",
  manifest: "sha256:cb7360913e2a99538b265c7091118fe8d0cc8cbe6a68681b8d7b0ea1da5e6f4e",
  from: "2026-10-07T23:51:10.296Z",
  to: "2026-10-07T23:51:13.206Z",
  created: "2026-10-09T19:55:25Z",
};
/** A NEWER run of the same topics: the point a wrong join would take. */
const NEW = {
  name: "logweir-manual-newer00000000000000000000",
  uid: "7d0c0c46-0000-4000-8000-000000000002",
  pointId: "lwp1-5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a",
  receipt: "sha256:" + "5a".repeat(32),
  runId: "01M4H9ZZZZZZZZZZZZZZZZZZZZ",
  manifest: "sha256:" + "6b".repeat(32),
  from: "2026-10-08T23:51:10.296Z",
  to: "2026-10-08T23:51:14.000Z",
  created: "2026-10-10T08:00:00Z",
};

/** One Backup, as `GET /backups` lists it. */
function backupItem(run) {
  return {
    name: run.name, namespace: NS, uid: run.uid, resourceVersion: "1",
    createdAt: run.created, deadlineSeconds: 3600,
    archive: { url: "logweir-destination://primary" },
    destinationRef: { name: "primary", uid: DESTINATION.uid },
    locationDigest: DESTINATION.locationDigest,
    backupId: run.uid,
    operation: { state: "succeeded", stateReason: "Ok", terminal: true,
      verificationState: "valid", verifiedSuccess: true },
    records: 197,
    sourceRef: { name: "orders-prod" },
    topics: ["orders", "payments"],
    trigger: { attempt: 0, kind: "Manual" },
    triggeredBy: "manual",
    windowCovered: { fromMs: Date.parse(run.from), toMs: Date.parse(run.to) },
  };
}

/** One point, as `GET /catalogs/{name}/points` lists it. */
function pointItem(run) {
  return {
    pointId: run.pointId, backupId: run.uid, runId: run.runId,
    recoveryPointAt: run.created, coveredFrom: run.from, coveredTo: run.to,
    availability: "Available", verification: "Verified", selectable: true,
    signerKeyId: "d1d189d668589cb352f3836931504c03b6dd5341849149fcfbe9db8bd117feaf",
    receiptKey: "logweir/backups/" + run.uid + "/" + run.runId + ".receipt.json",
    receiptSha256: run.receipt,
    manifestKey: "team-a/prod/" + run.uid + "/manifest.json",
    manifestSha256: run.manifest,
    locations: [{ locationId: "s3://kafka-backups/team-a/prod", availability: "Available" }],
    formatVersion: "1.3.0",
    topics: [
      { name: "orders", partitions: 3, replicationFactor: 1, configCoverage: "captured",
        applyRoute: "unknown" },
      { name: "payments", partitions: 3, replicationFactor: 1, configCoverage: "captured",
        applyRoute: "unknown" },
    ],
    ownerDetection: [],
  };
}

/** The run's operation, as `GET /operations/backup/{name}` answers it: the
 *  receipt digest is published here and not on the list. */
function operationOf(run) {
  const doc = con("operation-backup.json");
  Object.assign(doc.item, {
    name: run.name, namespace: NS, uid: run.uid,
    trust: { basis: "Current", state: "verified" },
    verificationScope: { level: "none" },
    readiness: { basis: "notImplemented", state: "unknown" },
    awaitingApproval: false, stale: false, stage: "finished",
  });
  doc.item.evidence = Object.assign({}, doc.item.evidence, { payloadSha256: run.receipt });
  return doc;
}

/** The catalog's wire document, moved over destination `primary`. */
function catalogItem() {
  const doc = con("catalog.json");
  return Object.assign(doc.item, { name: "archive", namespace: NS,
    destinationRef: { name: "primary" } });
}

const page = (items) => ({ requestId: "r", items: items,
  page: { limit: 200, nextCursor: null }, truncated: false, viewExpired: false });

/** Answers the product API's routes from `points` (the catalog's view). */
function transport(points) {
  const seen = [];
  const original = globalThis.fetch;
  const routes = [
    [/\/namespaces\/team-a\/connections(\?|$)/, () => con("connections-list.json")],
    [/\/namespaces\/team-a\/backups(\?|$)/, () => page([backupItem(NEW), backupItem(OLD)])],
    [/\/namespaces\/team-a\/catalogs(\?|$)/, () => page([catalogItem()])],
    [/\/namespaces\/team-a\/catalogs\/archive\/points(\?|$)/, () => page(points)],
    [/\/namespaces\/team-a\/catalogs\/archive(\?|$)/, () => ({ requestId: "r", item: catalogItem() })],
    [/\/operations\/backup\/logweir-manual-xta3/, () => operationOf(OLD)],
    [/\/operations\/backup\/logweir-manual-newer/, () => operationOf(NEW)],
    [/\/namespaces\/team-a\/destinations\/primary(\?|$)/, () => con("destination.json")],
    [/\/namespaces\/team-a\/destinations(\?|$)/, () => con("destinations-list.json")],
  ];
  globalThis.fetch = (u, init) => {
    seen.push(String(u));
    const route = routes.find(([pattern]) => pattern.test(String(u)));
    const body = route === undefined
      ? { type: "about:blank", title: "Not Found", status: 404, code: "not_found" }
      : route[1]();
    const status = route === undefined ? 404 : 200;
    return Promise.resolve({
      ok: status === 200, status: status, headers: { get: () => null },
      text: () => Promise.resolve(JSON.stringify(body)),
    });
  };
  return { seen: seen, restore: () => { globalThis.fetch = original; } };
}

const visible = (html) => html.replace(/<[^>]*>/g, "").replace(/&#39;/g, "'")
  .replace(/&quot;/g, "\"").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");

function byId(html, id) {
  const at = html.indexOf("id=\"" + id + "\"");
  if (at === -1) {
    return null;
  }
  const open = html.lastIndexOf("<", at);
  const name = /^<(\w+)/.exec(html.slice(open))[1];
  return html.slice(open, html.indexOf("</" + name + ">", at));
}

/** The plan's source binding: its set, its topics and its `point` block. */
function bindingOf(bytes) {
  const source = String(bytes).split("\ntarget:")[0] + "\n";
  return {
    backup: (/\n {2}backup: "([^"]*)"/.exec(source) || [])[1],
    topics: (/\n {2}topics:\n((?: {4}- .*\n)+)/.exec(source) || [])[1],
    point: (/\n {2}point:\n((?: {4}\w.*\n)+)/.exec(source) || [])[1],
  };
}

/** Mounts the wizard on `hash` in console mode over `points`, and returns the
 *  view's markup and the plan bytes on screen. */
async function mounted(hash, points, topics) {
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: con("session.json") }) });
  dropDraft(formKey(NS, WIZARD_FORM));
  const wire = transport(points);
  try {
    const view = fakeView();
    await mountRestoreWizard(view.root, NS, restoreRouteParams(hash), viewParse);
    if (topics !== undefined) {
      const input = view.find("#catalog-topics");
      assert.ok(input !== null, "the catalog flow asks for its topics: " +
        visible(view.html()).slice(0, 300));
      input.value = topics;
      await input.dispatch("change");
    }
    const html = view.html();
    const plan = byId(html, "plan-bytes");
    return { html: html, bytes: plan === null ? null : visible(plan), seen: wire.seen };
  } finally {
    wire.restore();
    dropDraft(formKey(NS, WIZARD_FORM));
    resetMode();
  }
}

const backupRoute = (run) => restorePointRoute(NS, { metadata: { name: run.name, uid: run.uid } });

test("fx35_r1_a_backups_page_restore_of_a_1_3_0_point_is_bound_to_it_as_the_catalog_flow_binds_it", async () => {
  // The catalog lists the NEWER point first, as its view does: a join that
  // took the first row, or the newest run, would bind the wrong point.
  const points = [pointItem(NEW), pointItem(OLD)];
  const fromBackup = await mounted(backupRoute(OLD), points);
  assert.ok(fromBackup.bytes !== null, "a plan is rendered: " +
    visible(fromBackup.html).slice(0, 400));
  const bound = bindingOf(fromBackup.bytes);
  assert.equal(bound.backup, OLD.uid, "the clicked run's set");
  assert.ok(bound.point !== undefined, "NEGATIVE CONTROL: a plan naming `backup:` only -- the " +
    "binding dropped -- fails this:\n" + fromBackup.bytes);
  assert.match(bound.point, new RegExp("point_id: \"" + OLD.pointId + "\""),
    "NEGATIVE CONTROL: the newer run's " + NEW.pointId + " fails this");
  assert.match(bound.point, new RegExp("receipt_sha256: \"" + OLD.receipt + "\""));
  assert.match(bound.point, new RegExp("manifest_sha256: \"" + OLD.manifest + "\""));
  assert.equal(byId(fromBackup.html, "point-unbound"), null, "a bound plan says nothing of " +
    "being unbound");
  // THE REVIEW: the recorded time basis and configuration, never NOT RECORDED.
  const recorded = visible(byId(fromBackup.html, "review-recorded") || "");
  assert.equal(recorded, "recorded: the run reads each topic's timestamp type and " +
    "configuration from the receipt of point " + OLD.pointId + ": orders timestamp type and " +
    "configuration captured; payments timestamp type and configuration captured");
  assert.doesNotMatch(recorded, /NOT RECORDED/);
  assert.match(visible(fromBackup.html), /the source's, as recovery catalog archive records point lwp1-ee285127/,
    "and the topic layout the point recorded sets the factor");
  // R2: the binding and the coverage arrived through the product's readers and
  // `ui/contract.js`'s decoders, from the routes' own documents.
  assert.ok(fromBackup.seen.some((u) => /\/operations\/backup\/logweir-manual-xta3/.test(u)),
    "the run's receipt digest was read from its operation: the list does not publish it");
  assert.ok(fromBackup.seen.some((u) => /\/catalogs\/archive\/points\?/.test(u)),
    "the point was read from the catalog's view");

  // CONTROL: the catalog flow's plan for the same point. The bindings are equal.
  const fromCatalog = await mounted(restoreCatalogPointRoute(NS, "archive", OLD.pointId), points,
    "orders, payments");
  assert.ok(fromCatalog.bytes !== null, visible(fromCatalog.html).slice(0, 400));
  assert.deepEqual(bound, bindingOf(fromCatalog.bytes),
    "the Backups-page binding is the catalog flow's binding, byte for byte");
  assert.equal(visible(byId(fromCatalog.html, "review-recorded") || ""), recorded,
    "and the review says the same of both");
});

test("fx35_r3_a_backup_with_no_catalog_point_says_so_and_offers_no_bound_restore", async () => {
  // The catalog has not synced the clicked run: only the newer point is listed.
  const unsynced = await mounted(backupRoute(OLD), [pointItem(NEW)]);
  const note = visible(byId(unsynced.html, "point-unbound") || "");
  assert.match(note, /This plan is NOT bound to a recovery point/,
    "NEGATIVE CONTROL: a page that says nothing fails this");
  assert.match(note, new RegExp("no recovery catalog over destination primary lists point " +
    OLD.pointId + " yet: the catalog has not synced this run; sync it, then reload this page"));
  assert.ok(unsynced.bytes !== null, "the unbound plan is still the Backup's plan");
  assert.equal(bindingOf(unsynced.bytes).point, undefined,
    "no binding, and nothing that looks like one -- never the newer run's point");
  assert.equal(bindingOf(unsynced.bytes).backup, OLD.uid);
  assert.match(visible(byId(unsynced.html, "review-recorded") || ""),
    /^NOT RECORDED: this plan is bound to no recovery point \(no recovery catalog over/);

  // CONTROL: the same Backup with its point listed offers the bound restore.
  const synced = await mounted(backupRoute(OLD), [pointItem(NEW), pointItem(OLD)]);
  assert.equal(byId(synced.html, "point-unbound"), null);
  assert.match(bindingOf(synced.bytes).point || "", new RegExp(OLD.pointId));
});
