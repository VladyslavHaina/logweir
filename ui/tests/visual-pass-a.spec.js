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
import { ENFORCEMENT_SENTENCES, RETENTION_SENTENCE } from "../render.js";
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
