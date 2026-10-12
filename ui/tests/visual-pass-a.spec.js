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
import {
  ABSENT_REMOVES,
  ROTATE_DEFAULTS,
  keptGrant,
  renderRotateForm,
  rotationBody,
  submitRotation,
  validateGrants,
} from "../pages/destinations.js";
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

// H2. A ROTATION STARTS FROM WHAT THE DESTINATION HAS.

/** `primary` as the API projects it, with all three optional grants held:
 *  the PoC destination's shape. */
function fullyGranted() {
  const item = decoded("destination.json").item;
  return Object.assign({}, item, {
    access: Object.assign({}, item.access, {
      evidenceWrite: { mode: "secretKeys", secretName: "lwd-primary-evidence-write",
        keys: ["access-key-id", "secret-access-key"], serviceAccountName: null },
      evidenceRead: { mode: "secretKeys", secretName: "lwd-primary-evidence-read",
        keys: ["access-key-id", "secret-access-key"], serviceAccountName: null },
    }),
  });
}

/** The option a role's selector starts on. */
function selected(html, role) {
  const start = html.indexOf("id=\"rotate-" + role + "-source\"");
  const end = html.indexOf("</select>", start);
  const option = /<option value="([^"]+)" selected>/.exec(html.slice(start, end));
  return option === null ? null : option[1];
}

test("h2_the_rotation_form_opens_with_every_grant_on_keep", () => {
  const item = fullyGranted();
  const html = decode(renderRotateForm(item, { mayOperate: true }));
  for (const role of ["archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead"]) {
    assert.equal(selected(html, role), "keep",
      role + ": THE DEFECT: the form opened on `absent` (or `new` for archiveWrite)");
  }
  assert.match(html, /keep \(as it is: secretKeys lwd-primary-evidence-read\)/);
  assert.equal(ROTATE_DEFAULTS.evidenceReadSource, "keep");
});

/** The four sources the rendered form starts on, as its selectors carry them. */
function startingSources(html) {
  const out = {};
  for (const role of ["archiveWrite", "archiveRead", "evidenceWrite", "evidenceRead"]) {
    out[role + "Source"] = selected(html, role);
  }
  return out;
}

test("h2_rotating_one_key_keeps_the_others_exactly_as_the_destination_has_them", async () => {
  const item = fullyGranted();
  // The reader opens the form, rotates archiveWrite to a new key and touches
  // nothing else: every other grant is what the form started on.
  const html = renderRotateForm(item, { mayOperate: true });
  const values = Object.assign(startingSources(html), {
    archiveWriteSource: "new", archiveWriteAccessKeyId: "id-2", archiveWriteSecretAccessKey: "k-2",
  });
  let sent = null;
  await submitRotation(NS, "primary", values, item.generation, {
    updateDestinationAccess: (ns, name, body) => {
      sent = body;
      return Promise.resolve({ item: item });
    },
  }, item.access);
  assert.equal(sent.expectedGeneration, item.generation);
  assert.deepEqual(sent.access.archiveWrite,
    { mode: "secretKeys", secret: { new: { accessKeyId: "id-2", secretAccessKey: "k-2" } } });
  assert.deepEqual(sent.access.archiveRead,
    { mode: "secretKeys", secret: { existing: { name: "lwd-primary-archive-read" } } },
    "THE DEFECT: archiveRead was omitted, which the API reads as REMOVE");
  assert.deepEqual(sent.access.evidenceWrite,
    { mode: "secretKeys", secret: { existing: { name: "lwd-primary-evidence-write" } } });
  assert.deepEqual(sent.access.evidenceRead,
    { mode: "secretKeys", secret: { existing: { name: "lwd-primary-evidence-read" } } },
    "THE DEFECT: evidenceRead was omitted, so verification would stop");
  // Every other held mode is kept as itself, and an absent grant stays absent.
  assert.deepEqual(keptGrant({ mode: "archiveReadGrant", keys: [] }), { mode: "archiveReadGrant" });
  assert.deepEqual(keptGrant({ mode: "workloadIdentity", keys: [], serviceAccountName: "sa-1" }),
    { mode: "workloadIdentity", workloadIdentity: { serviceAccountName: "sa-1" } });
  assert.equal(keptGrant({ mode: "inheritsArchiveWrite", keys: [] }), null);
  assert.equal(keptGrant({ mode: "notConfigured", keys: [] }), null);
  assert.deepEqual(keptGrant({ mode: "secretKeys", secretName: "s",
    keys: ["access-key-id", "secret-access-key", "session-token"] }),
  { mode: "secretKeys", secret: { existing: { name: "s", sessionTokenKey: "session-token" } } });
});

test("h2_keep_is_refused_rather_than_guessed_when_the_key_names_are_not_the_defaults", async () => {
  const item = fullyGranted();
  item.access = Object.assign({}, item.access, {
    archiveRead: { mode: "secretKeys", secretName: "hand-made", keys: ["AWS_ID", "AWS_KEY"],
      serviceAccountName: null },
  });
  const problems = validateGrants(ROTATE_DEFAULTS, item.access);
  assert.match(problems.archiveReadSource, /keep cannot be sent for this grant/);
  assert.match(problems.archiveReadSource, /\(AWS_ID, AWS_KEY\)/);
  let sent = false;
  await assert.rejects(submitRotation(NS, "primary", ROTATE_DEFAULTS, 2, {
    updateDestinationAccess: () => { sent = true; return Promise.resolve({}); },
  }, item.access), (error) => error.kind === "invalid");
  assert.equal(sent, false, "nothing is sent that would point the grant at the wrong keys");
});

test("h2_choosing_absent_warns_what_turns_off_and_sends_exactly_that", () => {
  const item = fullyGranted();
  const html = decode(renderRotateForm(item, {
    mayOperate: true,
    rotate: { draft: Object.assign({}, ROTATE_DEFAULTS, { evidenceReadSource: "absent" }) },
  }));
  const warning = /<p class="caveat" data-grant-inputs="removes" id="rotate-evidenceRead-removes">([^<]*)<\/p>/
    .exec(html);
  assert.ok(warning !== null, "the warning is shown while absent is chosen");
  assert.equal(warning[1], ABSENT_REMOVES.evidenceRead);
  assert.match(warning[1], /evidence verification stops/);
  // A grant still on keep carries its warning hidden.
  assert.match(html, /id="rotate-archiveRead-removes" hidden>/);
  const body = rotationBody(Object.assign({}, ROTATE_DEFAULTS, { evidenceReadSource: "absent" }),
    item.generation, item.access);
  assert.equal(Object.prototype.hasOwnProperty.call(body.access, "evidenceRead"), false,
    "absent is sent as absent: the one grant the reader removed");
  assert.deepEqual(Object.keys(body.access).sort(),
    ["archiveRead", "archiveWrite", "evidenceWrite"]);
  // A grant the destination does not hold carries no warning: absent removes nothing.
  const unheld = decode(renderRotateForm(decoded("destination.json").item, { mayOperate: true }));
  assert.equal(unheld.indexOf("id=\"rotate-evidenceWrite-removes\""), -1);
});
