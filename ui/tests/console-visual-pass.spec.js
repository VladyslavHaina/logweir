// console-visual-pass.spec.js -- the console defects PROD-14.1's visual pass
// found as each role (claude/ui-pass.result.md: M2 to M8, M10, M11 and the
// cheap LOWs), one row each, with the behaviour it replaced as its NEGATIVE
// CONTROL. A layout row asserts what decides the visible property -- the
// markup a cell carries and the stylesheet rule that holds it -- because this
// suite has no browser; the measured widths are in the branch's report.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { decoded } from "./console-fixture.js";
import { LIFE, fakeView, parse } from "./fake-view.js";

import { resetMode, selectMode } from "../client.js";
import { mountClusters } from "../pages/clusters.js";
import { mountSchedules } from "../pages/schedules.js";

import { restoreDestination } from "../pages/restore-wizard.js";
import { restoreProgressSentence } from "../pages/approvals.js";
import { renderOperation } from "../pages/operation.js";

const PREPARED = Object.freeze({
  restoreName: "restore-x", approvalName: "approval-x", hash: "sha256:" + "e".repeat(64),
});

// M2. Step 6 promises "its operation view"; Create opened the History detail,
// which said "Pending: no Job exists, and none will until its approval is
// Verified=True" for a confirmation seconds old.

test("m2_create_opens_the_operation_view_step_6_promises", async () => {
  const restore = {
    metadata: { name: "restore-x", uid: "uid-x" },
    spec: { approvalRef: { name: "approval-x" } },
    __contract: { mode: "console", authorization: { state: "confirmed", mode: "ordinary" } },
  };
  const route = await restoreDestination({ get: async () => null }, { ns: "team-a" }, PREPARED,
    restore);
  assert.ok(route.startsWith("#/operations?"),
    "NEGATIVE CONTROL: Create opened " + route + ", the History detail");
  assert.equal(route, "#/operations?ns=team-a&kind=restore&name=restore-x&uid=uid-x",
    "the operation view of exactly this run");
});

test("m2_a_restore_waiting_for_its_approval_says_so_and_not_none_will", () => {
  const pending = { status: { phase: "Pending", reason: "ApprovalNotVerified" } };
  const sentence = restoreProgressSentence(pending);
  assert.match(sentence, /^Waiting for approval: /);
  assert.doesNotMatch(sentence, /none will/,
    "NEGATIVE CONTROL: a seconds-old Restore read as one that will never run");

  // And the operation view it now opens says the same, in its own row.
  const item = decoded("operation-restore-completed.json", (body) => {
    Object.assign(body.item, {
      state: "pending", stateReason: "ApprovalNotVerified", terminal: false,
      awaitingApproval: true, stage: "admission",
    });
  }).item;
  const html = renderOperation({
    ns: "team-a", kind: "restore", name: item.name, uid: "",
    document: item, console: true, meta: { transport: "stream", attempt: 0 },
  });
  assert.match(html, /<dt>awaiting approval<\/dt><dd>yes: waiting for approval/);
});

// M3 and M4. A viewer got the whole Create a KafkaCluster form with an enabled
// CREATE (password and key fields included), and the whole editable Create a
// BackupSchedule form. Destinations hides its form for such a role.

const FIXTURES = fileURLToPath(new URL("./fixtures/console/", import.meta.url));
const session = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

/** Runs `body` under the session document `doc`, with every network call
 *  refused: a mount's own reads go to the stub it is handed, and the one read
 *  through the default client (the retention list) fails, as its panel allows. */
async function signedInAs(doc, body) {
  const original = globalThis.fetch;
  globalThis.fetch = () => Promise.reject(new Error("this suite never dials"));
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: doc }) });
  try {
    return await body();
  } finally {
    resetMode();
    globalThis.fetch = original;
  }
}

const listsNothing = {
  list() { return Promise.resolve({ items: [] }); },
  destinations() { return Promise.resolve({ items: [] }); },
};

test("m3_a_role_that_cannot_create_a_connection_is_not_offered_the_form", async () => {
  const viewer = session("session-viewer.json");
  assert.equal(viewer.namespaces[0].capabilities.connectionCreate, false);
  const read = await signedInAs(viewer, async () => {
    const view = fakeView();
    await mountClusters(view.root, "team-a", parse, LIFE(), listsNothing);
    return view.html();
  });
  assert.doesNotMatch(read, /id="cluster-form"/,
    "NEGATIVE CONTROL: a viewer got the create form, CREATE enabled");
  assert.doesNotMatch(read, /type="password"|private key/);
  assert.match(read, /id="clusters-read-only"/);
  // A role that may create still gets it.
  const operator = session("session.json");
  assert.equal(operator.namespaces[0].capabilities.connectionCreate, true);
  const full = await signedInAs(operator, async () => {
    const view = fakeView();
    await mountClusters(view.root, "team-a", parse, LIFE(), listsNothing);
    return view.html();
  });
  assert.match(full, /id="cluster-form"/);
  assert.doesNotMatch(full, /id="clusters-read-only"/);
});

test("m4_a_role_that_cannot_create_a_schedule_is_not_offered_the_form", async () => {
  const viewer = session("session-viewer.json");
  assert.equal(viewer.namespaces[0].capabilities.scheduleCreate, false);
  const read = await signedInAs(viewer, async () => {
    const view = fakeView();
    await mountSchedules(view.root, "team-a", parse, LIFE(), listsNothing);
    return view.html();
  });
  assert.doesNotMatch(read, /id="schedule-form"/,
    "NEGATIVE CONTROL: a viewer got the whole editable create form");
  assert.match(read, /id="schedules-read-only"/);
  const full = await signedInAs(session("session.json"), async () => {
    const view = fakeView();
    await mountSchedules(view.root, "team-a", parse, LIFE(), listsNothing);
    return view.html();
  });
  assert.match(full, /id="schedule-form"/);
  assert.doesNotMatch(full, /id="schedules-read-only"/);
});
