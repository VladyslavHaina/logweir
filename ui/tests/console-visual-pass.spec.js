// console-visual-pass.spec.js -- the console defects PROD-14.1's visual pass
// found as each role (claude/ui-pass.result.md: M2 to M8, M10, M11 and the
// cheap LOWs), one row each, with the behaviour it replaced as its NEGATIVE
// CONTROL. A layout row asserts what decides the visible property -- the
// markup a cell carries and the stylesheet rule that holds it -- because this
// suite has no browser; the measured widths are in the branch's report.

import { test } from "node:test";
import assert from "node:assert/strict";

import { decoded } from "./console-fixture.js";

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
