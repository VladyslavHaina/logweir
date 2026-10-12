// visual-pass-a.spec.js -- the HIGH defects and M1/M9 PROD-14.1's visual pass
// found on the PoC console, each pinned where it originated.
//
// H1  the Backup detail's "Check it yourself" read the bucket off the
//     destination sentinel URL (`s3://primary/...`);
// H2  the destination's "Rotate access" form opened with every optional grant
//     at `absent`, so rotating one key removed the others;
// H3  the schedule's Retention panel never found the RetentionPolicy naming
//     its destination;
// M1  "What uses this" listed nothing: the API's usage answer and its basis;
// M9  a Restore detail showed rows the product API now publishes as "not
//     published by the product API".
//
// THROUGH THE PRODUCT'S OWN CLIENT AND DECODERS wherever a page reads: the
// transport answers with the product API's documents, so a member
// `ui/contract.js` does not declare is dropped before the page sees it.

import { test } from "node:test";
import assert from "node:assert/strict";

import { resetMode, selectMode } from "../client.js";
import {
  NO_EVIDENCE_KEYS_SENTENCE,
  mountBackupDetail,
} from "../pages/backups.js";
import { decoded, wire } from "./console-fixture.js";
import { LIFE, fakeView, parse } from "./fake-view.js";

const NS = "team-a";
const BACKUP = "orders-hourly-20260912-080000";

function decode(html) {
  return String(html)
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

/** Replaces the one platform call `ui/api.js` makes and records every request. */
function transport(answer) {
  const seen = [];
  const original = globalThis.fetch;
  globalThis.fetch = (u, init) => {
    seen.push(String(u));
    const reply = answer(String(u), init || {});
    if (reply === undefined || reply === null) {
      return Promise.reject(new Error("the suite has no answer for " + String(u)));
    }
    return Promise.resolve({
      ok: reply.status >= 200 && reply.status < 300,
      status: reply.status,
      headers: { get: () => null },
      text: () => Promise.resolve(JSON.stringify(reply.body)),
    });
  };
  return { seen: seen, restore: () => { globalThis.fetch = original; } };
}

/** Pins the shared console from `session.json`, with `change` applied. */
async function sharedConsole(change) {
  const session = wire("session.json");
  if (change) {
    change(session);
  }
  resetMode();
  return selectMode({ probe: async () => ({ ok: true, status: 200, body: session }) });
}

const problem = (status, code, detail) => ({ status: status, body: {
  type: "https://logweir.dev/problems/" + code, title: code, status: status, code: code,
  detail: detail, requestId: "01VISUALA", retryable: false } });

/** The Backup detail as a reader sees it, mounted through the real client:
 *  the Backup, its operation view, and the destination it names. */
async function backupDetail(backupDoc, destinationReply, operationDoc) {
  const view = fakeView();
  const wire_ = transport((u) => {
    if (u.indexOf("/api/v1/namespaces/" + NS + "/backups/" + BACKUP) === 0) {
      return { status: 200, body: backupDoc };
    }
    if (u.indexOf("/api/v1/namespaces/" + NS + "/operations/backup/" + BACKUP) === 0) {
      return { status: 200, body: operationDoc || wire("operation-backup.json") };
    }
    if (u.indexOf("/api/v1/namespaces/" + NS + "/destinations/primary") === 0) {
      return destinationReply;
    }
    return undefined;
  });
  try {
    await mountBackupDetail(view.root, NS, BACKUP, parse, LIFE());
  } finally {
    wire_.restore();
  }
  return { html: decode(view.html()), seen: wire_.seen };
}

/** The four lines of the copy block, or `[]`. */
function checkLines(html) {
  const at = html.indexOf("<section class=\"check\"");
  const open = html.indexOf("<pre class=\"copy\">", at);
  if (at === -1 || open === -1) {
    return [];
  }
  return html.slice(open + "<pre class=\"copy\">".length).split("</pre>")[0].split("\n");
}

// H1. THE BUCKET IS THE DESTINATION'S, NOT ITS NAME.

test("h1_the_backup_check_fetches_from_the_destinations_bucket_endpoint_and_region", async () => {
  // `primary` is on bucket `kafka-backups`, prefix `team-a/prod`, at a MinIO
  // endpoint: a destination named unlike its bucket. The run froze the
  // location the destination still names.
  await sharedConsole();
  const destination = wire("destination.json");
  const backup = wire("backup.json");
  backup.item.locationDigest = destination.item.locationDigest;
  assert.equal(backup.item.archive.url, "logweir-destination://primary",
    "CONTROL: the run's archive URL is the sentinel, whose 'bucket' is the destination's name");
  const { html, seen } = await backupDetail(backup, { status: 200, body: destination });
  const lines = checkLines(html);
  assert.equal(lines.length, 4, html);
  const evidence = decoded("operation-backup.json").item.evidence;
  const route = " --endpoint-url https://minio.storage.svc:9000 --region us-east-1";
  assert.equal(lines[0],
    "aws s3 cp s3://kafka-backups/" + evidence.payloadKey + " ./receipt.json" + route,
    "THE DEFECT: s3://primary/... -- the destination's name used as the bucket");
  assert.equal(lines[1],
    "aws s3 cp s3://kafka-backups/" + evidence.sidecarKey + " ./receipt.sig" + route);
  assert.equal(html.indexOf("s3://primary"), -1, "the destination's name is never a bucket");
  // THE KEYS ARE BUCKET-ABSOLUTE UNDER logweir/: the destination's prefix is
  // where the ARCHIVE is, not the evidence, so it is not in the fetch path.
  assert.equal(lines[0].indexOf("team-a/prod"), -1);
  assert.match(lines[2], /^logweir drill verify --payload-type backup-receipt/);
  assert.ok(seen.some((u) => u.indexOf("/destinations/primary") !== -1),
    "the page read the destination the run names");
});

test("h1_a_destination_that_moved_or_cannot_be_read_names_no_bucket_and_says_why", async () => {
  await sharedConsole();
  const destination = wire("destination.json");
  // The fixture run froze another location than the destination names now.
  const moved = await backupDetail(wire("backup.json"), { status: 200, body: destination });
  assert.match(moved.html, /destination primary now names location sha256:9f86d0/);
  assert.match(moved.html, /so its bucket is not where this run wrote/);
  assert.match(checkLines(moved.html)[0], /^aws s3 cp 's3:\/\/<bucket of destination primary>\//);
  assert.equal(moved.html.indexOf("s3://primary"), -1);
  assert.equal(moved.html.indexOf("s3://kafka-backups"), -1,
    "a location the run did not freeze is not offered as where it wrote");

  // A 403 on the destination read is the check's caveat, not the page's error.
  const backup = wire("backup.json");
  backup.item.locationDigest = destination.item.locationDigest;
  const refused = await backupDetail(backup,
    problem(403, "forbidden", "no grant on destinations"));
  assert.equal(refused.html.indexOf("class=\"error\""), -1, "the Backup detail still renders");
  assert.match(refused.html, /destination primary could not be read: no grant on destinations/);
  assert.equal(refused.html.indexOf("s3://primary"), -1);

  // A role that may read backups and not destinations (an approver) is never
  // sent to the route: the client refuses it, and the caveat says so.
  await sharedConsole((s) => { s.namespaces[0].capabilities.destinations = false; });
  const approver = await backupDetail(backup, { status: 200, body: destination });
  assert.equal(approver.seen.filter((u) => u.indexOf("/destinations/") !== -1).length, 0);
  assert.match(approver.html, /destination primary could not be read/);
  assert.equal(approver.html.indexOf("s3://primary"), -1);
  resetMode();
});

test("h1_a_run_with_no_receipt_key_yet_prints_no_fetch_of_the_bucket_root", async () => {
  await sharedConsole();
  const destination = wire("destination.json");
  const backup = wire("backup.json");
  backup.item.locationDigest = destination.item.locationDigest;
  const operation = wire("operation-backup.json");
  delete operation.item.evidence.payloadKey;
  delete operation.item.evidence.payloadSha256;
  delete operation.item.evidence.sidecarKey;
  const { html } = await backupDetail(backup, { status: 200, body: destination }, operation);
  assert.ok(html.indexOf(NO_EVIDENCE_KEYS_SENTENCE) !== -1, html);
  assert.equal(checkLines(html).length, 0,
    "THE DEFECT: `aws s3 cp s3://primary/ ./receipt.json` while the run was pending");
  resetMode();
});
