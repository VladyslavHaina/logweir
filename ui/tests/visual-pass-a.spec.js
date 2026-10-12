// visual-pass-a.spec.js -- the HIGH defects and M1/M9 PROD-14.1's visual pass
// found on the PoC console, each pinned where it originated.
//
// H1  the Backup detail's "Check it yourself" read the bucket off the
//     destination sentinel URL (`s3://primary/...`); and, the class swept,
//     every "Check it yourself" prints commands that work as printed: the
//     Restore detail's store, a path-style store, a run with no keys, and
//     the Python verifier's own grammar;
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
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { apiClient, resetMode, selectMode } from "../client.js";
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
import {
  RETENTION_ELSEWHERE_SENTENCE,
  policyForSchedule,
  readRetentionPolicies,
  renderRetentionPanel,
} from "../pages/schedules.js";
import { NOT_PUBLISHED, renderRestoreDetail } from "../pages/history.js";
import { ENFORCEMENT_SENTENCES, PATH_STYLE_SENTENCE, RETENTION_SENTENCE } from "../render.js";
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

// H1, THE CLASS: EVERY "CHECK IT YOURSELF" WORKS AS PRINTED. Both pages
// that print one read `primary`: named unlike its bucket (`kafka-backups`),
// path-style at a MinIO endpoint, in a region.

const REPO = fileURLToPath(new URL("../../", import.meta.url));
const RESTORE = "orders-drill-20260911";

/** `primary`'s storage, with the premises every row below stands on. */
function primaryStorage() {
  const item = decoded("destination.json").item;
  const storage = item.storage;
  assert.notEqual(item.name, storage.bucket, "PREMISE: the destination is named unlike its bucket");
  assert.equal(storage.addressing, "pathStyle", "PREMISE: the store is path-style");
  assert.ok(storage.endpoint.length > 0 && storage.region.length > 0,
    "PREMISE: a custom endpoint and a region");
  return storage;
}

/** The page's "Check it yourself" section, found by its heading. */
function section(html) {
  const heading = html.indexOf("<h3>Check it yourself</h3>");
  assert.notEqual(heading, -1, "the page has its check section");
  const at = html.lastIndexOf("<section class=\"check\"", heading);
  return html.slice(at, html.indexOf("</section>", heading));
}

/** The two fetches go to the store's bucket, at its endpoint and region, and
 *  the path-style note stands above them. */
function assertFetchesAsPrinted(check, storage, keys, files) {
  const lines = checkLines(check);
  assert.equal(lines.length, 4, check);
  const route = " --endpoint-url " + storage.endpoint + " --region " + storage.region;
  assert.equal(lines[0], "aws s3 cp s3://" + storage.bucket + "/" + keys[0] + " ./" + files[0] +
    route, "bucket, key, --endpoint-url and --region of the store the run wrote to");
  assert.equal(lines[1], "aws s3 cp s3://" + storage.bucket + "/" + keys[1] + " ./" + files[1] +
    route);
  assert.equal(lines[0].indexOf(storage.prefix), -1,
    "the evidence keys are bucket-absolute under logweir/, not under the archive prefix");
  const note = check.indexOf("data-addressing=\"path-style\"");
  assert.ok(note !== -1 && note < check.indexOf("<pre class=\"copy\">"),
    "THE DEFECT (F8): nothing said the store is path-style");
  assert.ok(check.includes(PATH_STYLE_SENTENCE.replace(/`([^`]*)`/g, "<code>$1</code>")),
    check);
  assert.match(PATH_STYLE_SENTENCE, /`aws configure set s3\.addressing_style path`/);
  return lines;
}

/** The restore plan's `evidence:` block replaced by `primary`'s evidence
 *  store, as the wizard signs it (`evidenceStoreOf`). */
function planOnPrimary(planBytes, storage) {
  const at = planBytes.indexOf("\nevidence:\n");
  const end = planBytes.indexOf("\nnotifications:", at);
  assert.ok(at !== -1 && end !== -1, "the fixture plan has an evidence block");
  return planBytes.slice(0, at + 1) + [
    "evidence:",
    "  backend: \"s3\"",
    "  bucket: \"" + storage.bucket + "\"",
    "  prefix: \"logweir/\"",
    "  region: \"" + storage.region + "\"",
    "  endpoint: \"" + storage.endpoint + "\"",
    "  path_style: true",
    "  allow_http: false",
  ].join("\n") + planBytes.slice(end);
}

/** The Restore detail as a reader sees it, read through the real client. */
async function restoreDetail(restoreDoc, operationDoc) {
  await sharedConsole();
  const wire_ = transport((u) => {
    const path = u.split("?")[0];
    if (path.indexOf("/api/v1/namespaces/" + NS + "/operations/restore/") === 0) {
      return { status: 200, body: operationDoc };
    }
    if (path.indexOf("/api/v1/namespaces/" + NS + "/restores/") === 0) {
      return { status: 200, body: restoreDoc };
    }
    return undefined;
  });
  try {
    return decode(renderRestoreDetail(await apiClient().get(NS, "restores", RESTORE)));
  } finally {
    wire_.restore();
    resetMode();
  }
}

test("h1_the_backup_check_prints_bucket_key_endpoint_region_and_the_path_style_note",
  async () => {
    const storage = primaryStorage();
    await sharedConsole();
    const destination = wire("destination.json");
    const backup = wire("backup.json");
    backup.item.locationDigest = destination.item.locationDigest;
    const { html } = await backupDetail(backup, { status: 200, body: destination });
    resetMode();
    const evidence = decoded("operation-backup.json").item.evidence;
    assertFetchesAsPrinted(section(html), storage,
      [evidence.payloadKey, evidence.sidecarKey], ["receipt.json", "receipt.sig"]);
  });

test("h1_the_restore_check_fetches_from_its_plans_evidence_store_as_printed", async () => {
  const storage = primaryStorage();
  const restore = wire("restore-results.json");
  assert.equal(restore.item.evidenceDestinationRef.name, "primary",
    "PREMISE: the restore writes its evidence to primary");
  restore.item.planBytes = planOnPrimary(restore.item.planBytes, storage);
  const html = await restoreDetail(restore, wire("operation-restore-completed.json"));
  const evidence = decoded("operation-restore-completed.json").item.evidence;
  const lines = assertFetchesAsPrinted(section(html), storage,
    [evidence.payloadKey, evidence.sidecarKey], ["scorecard.json", "scorecard.sig"]);
  assert.match(lines[2], /^logweir drill verify --payload-type scorecard /);
  assert.equal(html.indexOf("s3://primary"), -1, "the destination's name is never a bucket");
});

test("h1_a_pending_restore_prints_no_command_and_says_why", async () => {
  const storage = primaryStorage();
  const restore = wire("restore-results.json");
  restore.item.planBytes = planOnPrimary(restore.item.planBytes, storage);
  restore.item.operation = { state: "pending", stateReason: "Pending", terminal: false,
    verificationState: "pending", verifiedSuccess: false };
  const operation = wire("operation-restore-completed.json");
  Object.assign(operation.item, { state: "pending", stateReason: "Pending", terminal: false,
    verifiedSuccess: false, result: { status: "pending" }, verification: { state: "pending" },
    evidence: {} });
  const check = section(await restoreDetail(restore, operation));
  assert.equal(checkLines(check).length, 0,
    "THE DEFECT (I2): `aws s3 cp s3://<bucket>/ ./scorecard.json` -- a fetch of the bucket root");
  assert.equal(check.indexOf("aws s3 cp"), -1);
  assert.ok(check.includes("id=\"restore-check-no-keys\">" + NO_EVIDENCE_KEYS_SENTENCE), check);
});

test("h1_both_verify_lines_run_as_printed", async () => {
  // The Rust line, run by the release binary in a directory holding what the
  // two fetches write, with the reader's key in place of `<your key>`.
  const storage = primaryStorage();
  await sharedConsole();
  const destination = wire("destination.json");
  const backup = wire("backup.json");
  backup.item.locationDigest = destination.item.locationDigest;
  const { html } = await backupDetail(backup, { status: 200, body: destination });
  resetMode();
  const lines = checkLines(section(html));
  assert.ok(lines[0].includes(storage.bucket));
  const directory = mkdtempSync(join(tmpdir(), "logweir-ui-h1-"));
  const signed = join(REPO, "e2e/fixtures/signed/");
  copyFileSync(signed + "backup-receipt.json", join(directory, "receipt.json"));
  copyFileSync(signed + "backup-receipt.sig", join(directory, "receipt.sig"));
  const key = join(signed, "public.pem");
  const words = (line) => line.split(" ").map((w) => (w === "<your" ? key : w))
    .filter((w) => w !== "key>");
  const rust = words(lines[2]);
  assert.equal(rust[0], "logweir");
  // The gate hands a path relative to the repository; the line runs elsewhere.
  const binary = resolve(REPO, process.env.LOGWEIR_BIN || "target/release/logweir");
  const run = spawnSync(binary, rust.slice(1), { encoding: "utf8", cwd: directory });
  assert.equal(run.status, 0, "the printed Rust line verifies: " + String(run.error) +
    String(run.stdout) + String(run.stderr));
  assert.match(String(run.stdout), /signature: VALID/);

  // The Python line, read by the script's own grammar: `--payload-type` and
  // `--consumer-positions` take a value, and every other word is one of
  // EXACTLY three positionals, `<document.json> <document.sig> <public.pem>`.
  const script = readFileSync(join(REPO, "docs/verify_scorecard.py"), "utf8");
  assert.ok(script.includes("\"<document.json> <document.sig> <public.pem>\""),
    "the script's usage line is still three positionals");
  const python = words(lines[3]);
  assert.deepEqual(python.slice(0, 2), ["python3", "verify_scorecard.py"]);
  const positionals = [];
  let payloadType = null;
  for (let i = 2; i < python.length; i += 1) {
    if (python[i] === "--payload-type" || python[i] === "--consumer-positions") {
      payloadType = python[i] === "--payload-type" ? python[i + 1] : payloadType;
      i += 1;
    } else {
      positionals.push(python[i]);
    }
  }
  assert.equal(payloadType, "backup-receipt");
  assert.deepEqual(positionals, ["./receipt.json", "./receipt.sig", key],
    "THE DEFECT: `--scorecard ./receipt.json --signature ./receipt.sig --public-key <your " +
      "key>` is six positionals, and the script prints its usage and exits 1");
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
  assert.deepEqual(keptGrant({ mode: "secretKeys", secretName: "s",
    keys: ["access-key-id", "secret-access-key", "token"] }),
  { mode: "secretKeys", secret: { existing: { name: "s", sessionTokenKey: "token" } } },
    "beside the two defaults, the one key left is the session token's");
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

// H3. THE POLICY NAMING THE DESTINATION IS FOUND, AND SAYS WHAT IT DOES.

/** Schedule `tz`, which names destination `dest`, through the real client. */
async function scheduleTz() {
  const wire_ = transport((u) =>
    u.indexOf("/api/v1/namespaces/" + NS + "/schedules/tz") === 0
      ? { status: 200, body: wire("schedule-policy.json") }
      : undefined);
  try {
    return await apiClient().get(NS, "backupschedules", "tz");
  } finally {
    wire_.restore();
  }
}

/** The namespace's RetentionPolicies, as the panel's own reader reads them,
 *  from `retention-policies-list.json` with `change` applied to its items. */
async function policiesRead(change, reply) {
  const list = wire("retention-policies-list.json");
  if (change) {
    list.items = change(list.items);
  }
  const wire_ = transport((u) =>
    u.indexOf("/api/v1/namespaces/" + NS + "/retention-policies") === 0
      ? (reply || { status: 200, body: list })
      : undefined);
  try {
    return await readRetentionPolicies(NS);
  } finally {
    wire_.restore();
  }
}

/** The panel exactly as the schedule detail renders it. */
function retentionPanel(schedule, policies) {
  return decode(renderRetentionPanel(schedule, policyForSchedule(schedule, policies), policies));
}

const named = (item, destination, name) =>
  Object.assign({}, item, { name: name || item.name, destinationRef: { name: destination } });

test("h3_the_report_policy_naming_the_schedules_destination_is_shown_and_deletes_nothing",
  async () => {
    // The PoC's shape: RetentionPolicy `primary`, mode Report, naming the
    // schedule's destination. Here: `keep-a` renamed onto `dest`.
    await sharedConsole();
    const schedule = await scheduleTz();
    assert.equal(schedule.spec.destinationRef.name, "dest");
    const policies = await policiesRead((items) =>
      [named(items[0], "dest", "primary"), items[1], items[2]]);
    const panel = retentionPanel(schedule, policies);
    assert.match(panel, /<dt>policy<\/dt><dd>primary<\/dd>/,
      "THE DEFECT: 'No RetentionPolicy was read for this schedule's destination'");
    assert.match(panel, /<dt>mode asked for<\/dt><dd>Report<\/dd>/);
    assert.match(panel, /RetentionPolicy primary is in Report mode: it reports what its rules would remove and deletes nothing\./);
    assert.match(panel, /data-covering="reports"/);
    assert.equal(panel.indexOf(RETENTION_SENTENCE), -1,
      "the no-deletion sentence is for a destination NO policy names");
    assert.equal(panel.indexOf("No RetentionPolicy"), -1);
    resetMode();
  });

test("h3_an_enforce_policy_naming_the_destination_is_shown_as_deleting", async () => {
  await sharedConsole();
  const schedule = await scheduleTz();
  const enforcing = retentionPanel(schedule,
    await policiesRead((items) => [named(items[1], "dest")]));
  assert.match(enforcing, /<dt>policy<\/dt><dd>enforce-a<\/dd>/);
  assert.ok(enforcing.indexOf(ENFORCEMENT_SENTENCES.LogweirWorker) !== -1, enforcing);
  assert.match(enforcing, /data-covering="deletes"/);
  assert.equal(enforcing.indexOf(RETENTION_SENTENCE), -1);
  // Asked to enforce and not enforcing now: still shown as a policy that deletes.
  const paused = retentionPanel(schedule, await policiesRead((items) =>
    [Object.assign(named(items[1], "dest"), { enforcement: "RecommendationOnly" })]));
  assert.match(paused, /RetentionPolicy enforce-a is in Enforce mode: it deletes archive objects/);
  assert.match(paused, /data-covering="deletes"/);
  assert.equal(paused.indexOf(RETENTION_SENTENCE), -1);
  assert.equal(paused.indexOf("removes nothing"), -1,
    "an Enforce policy is never summarised as removing nothing");
  resetMode();
});

test("h3_a_successful_read_with_no_policy_naming_the_destination_keeps_the_sentence", async () => {
  await sharedConsole();
  const schedule = await scheduleTz();
  for (const [what, change] of [
    ["a Report policy on another destination", (items) => [items[0]]],
    ["no policy at all", () => []],
  ]) {
    const panel = retentionPanel(schedule, await policiesRead(change));
    assert.ok(panel.indexOf(RETENTION_SENTENCE) !== -1, what + ": " + panel);
    assert.match(panel, /No RetentionPolicy in this namespace names this schedule's destination, dest\./,
      what);
  }
  resetMode();
});

test("h3_a_policy_that_deletes_on_another_destination_is_named_beside_the_covering_one",
  async () => {
    // Review F1: `primary` (Report) names `dest`; `copy` (LogweirWorker) names
    // `dest-copy`, which may be at dest's bucket and prefix. Retention matches
    // points by location, so `copy` can delete what this schedule writes.
    await sharedConsole();
    const schedule = await scheduleTz();
    const panel = retentionPanel(schedule, await policiesRead((items) =>
      [named(items[0], "dest", "primary"), named(items[1], "dest-copy", "copy")]));
    assert.ok(panel.indexOf("RetentionPolicy primary is in Report mode") !== -1, panel);
    assert.ok(panel.indexOf(RETENTION_ELSEWHERE_SENTENCE) !== -1,
      "THE DEFECT: the Report policy's 'deletes nothing' with no word of `copy`");
    assert.match(panel, /<td>copy<\/td><td>dest-copy<\/td><td>LogweirWorker<\/td>/);
    assert.equal(panel.indexOf(RETENTION_SENTENCE), -1);
    // The covering policy is not listed as "another"; with no other policy
    // that deletes, there is no caveat.
    const enforcing = retentionPanel(schedule, await policiesRead((items) =>
      [named(items[1], "dest"), items[2]]));
    assert.match(enforcing, /<td>declared-c<\/td><td>dest-b<\/td>/);
    assert.equal(enforcing.indexOf("<td>enforce-a</td>"), -1);
    const alone = retentionPanel(schedule, await policiesRead((items) =>
      [named(items[0], "dest", "primary")]));
    assert.equal(alone.indexOf(RETENTION_ELSEWHERE_SENTENCE), -1);
    resetMode();
  });

test("h3_a_failed_read_or_a_conflict_never_claims_no_deletion", async () => {
  await sharedConsole();
  const schedule = await scheduleTz();
  const failed = retentionPanel(schedule, await policiesRead(null,
    problem(403, "forbidden", "no grant on retention-policies")));
  assert.equal(failed.indexOf(RETENTION_SENTENCE), -1);
  assert.match(failed, /The read failed: no grant on retention-policies/);
  // Two policies naming one destination are a Conflict: neither evaluates.
  const conflict = retentionPanel(schedule, await policiesRead((items) =>
    [named(items[0], "dest"), named(items[1], "dest")]));
  assert.equal(conflict.indexOf(RETENTION_SENTENCE), -1);
  assert.match(conflict, /More than one RetentionPolicy names this schedule's destination/);
  assert.match(conflict, /<td>keep-a<\/td><td>Report<\/td>/);
  assert.match(conflict, /<td>enforce-a<\/td><td>Enforce<\/td>/);
  resetMode();
});

// M9. WHAT THE API NOW PUBLISHES IS SHOWN; WHAT IT DOES NOT IS SAID ONCE.

/** The `<dd>` of `label` under the `<h3>` `heading`, tags stripped. */
function factUnder(html, heading, label) {
  const from = html.indexOf("<h3>" + heading + "</h3>");
  assert.notEqual(from, -1, heading + " is on the page");
  const section = html.slice(from, html.indexOf("</dl>", from));
  const open = "<dt>" + label + "</dt><dd>";
  const at = section.indexOf(open);
  assert.notEqual(at, -1, label + " is a row of " + heading);
  return section.slice(at + open.length, section.indexOf("</dd>", at)).replace(/<[^>]*>/g, "")
    .trim();
}

test("m9_a_verified_restore_shows_its_result_preflight_and_old_topics_and_no_unpublished_row",
  async () => {
    await sharedConsole();
    const wire_ = transport((u) => {
      const path = u.split("?")[0];
      if (path.indexOf("/api/v1/namespaces/" + NS + "/operations/restore/") === 0) {
        return { status: 200, body: wire("operation-restore-completed.json") };
      }
      if (path.indexOf("/api/v1/namespaces/" + NS + "/restores/") === 0) {
        return { status: 200, body: wire("restore-results.json") };
      }
      return undefined;
    });
    let object;
    try {
      object = await apiClient().get(NS, "restores", "orders-drill-20260911");
    } finally {
      wire_.restore();
      resetMode();
    }
    const html = decode(renderRestoreDetail(object));
    assert.equal(factUnder(html, "Integrity", "result"), "pass",
      "THE DEFECT: 'not published by the product API' on a verified restore");
    assert.equal(factUnder(html, "Integrity", "partial reason"), "-",
      "a pass records no partial reason: '-', never 'not published'");
    assert.equal(factUnder(html, "Target topic preflight", "timestampType"), "CreateTime");
    assert.equal(factUnder(html, "Target topic preflight", "retentionMs"), "604800000");
    assert.equal(factUnder(html, "Target topic preflight", "timestampBound"), "1756649100000");
    assert.equal(factUnder(html, "Topics", "old topics -- written to by nothing, in any tag"),
      "orders, orders-dlq");
    // The objectives and the measured values are on no route: one sentence,
    // no row.
    assert.equal(html.split(NOT_PUBLISHED).length - 1, 0,
      "BEFORE: eleven rows read '" + NOT_PUBLISHED + "'");
    assert.match(html, /id="restore-objectives-not-published"/);
    assert.equal(html.indexOf("<dt>objectives.rtoSeconds</dt>"), -1);
  });
