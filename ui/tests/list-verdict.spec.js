// list-verdict.spec.js -- CONSOLE-HISTORY-VALID-SHOWN-UNVERIFIED, and the
// scorecard facts a Restore shows before its evidence verified
// (SCORECARD-FACTS-UNVERIFIED-SHOWN). Folded into PLAT-18.2's review round.
//
// THE DEFECT. In console mode a list item's `OperationSummary` carries
// `verificationState` and `verifiedSuccess` (the controller's own green-badge
// rule, computed by the API); `ui/client.js`'s projection dropped both and
// supplied no exit code, so every console list row -- a Valid backup included
// -- read "no verification was recorded for this run". These rows drive the
// real transport (the one platform call stubbed) in both modes.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { apiClient, resetMode, selectMode } from "../client.js";
import { LIST_VERIFIED_CAPTION, VERIFICATION_CASES, summaryBadge } from "../render.js";
import { backupBadge, renderBackupList } from "../pages/backups.js";
import { renderHistoryList, renderRestoreDetail, restoreBadge } from "../pages/history.js";
import { operationFacts, renderResult } from "../pages/operation.js";
import { decodeD3Operation as decodeOperation } from "../contract.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

function transport(answer) {
  const original = globalThis.fetch;
  globalThis.fetch = (u) => {
    const reply = answer(String(u));
    return Promise.resolve({
      ok: reply.status >= 200 && reply.status < 300,
      status: reply.status,
      text: () => Promise.resolve(JSON.stringify(reply.body)),
    });
  };
  return () => { globalThis.fetch = original; };
}

async function consoleMode() {
  resetMode();
  await selectMode({ probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }) });
}

async function legacyMode() {
  resetMode();
  await selectMode({ probe: async () => ({ ok: false, status: 403, body: null }) });
}

/** The console backups list with one item per summary given. */
function backupsList(summaries) {
  const list = fixture("console/backups-list.json");
  const template = list.items[0];
  list.items = summaries.map((operation, i) => Object.assign(
    JSON.parse(JSON.stringify(template)),
    { name: "b-" + String(i), uid: "00000000-0000-4000-8000-00000000000" + String(i),
      operation: Object.assign({ state: "succeeded", stateReason: "Ok", terminal: true }, operation) },
  ));
  return list;
}

test("a_console_list_row_whose_summary_is_verified_is_green_and_says_what_it_does_not_carry", async () => {
  await consoleMode();
  const restore = transport(() => ({ status: 200, body: backupsList([
    { verificationState: "valid", verifiedSuccess: true },
    { verificationState: "invalid", verifiedSuccess: false },
    { verificationState: "noEvidence", verifiedSuccess: false },
    { verificationState: "valid", verifiedSuccess: false },
  ]) }));
  try {
    const list = await apiClient().list("team-a", "backups");
    const [valid, invalid, none, validNotGreen] = list.items;
    assert.deepEqual(valid.status.__summary,
      { verificationState: "valid", verifiedSuccess: true, terminal: true },
      "the projection carries the summary's verdict instead of dropping it");
    assert.equal(backupBadge(valid.status),
      "<span class=\"badge badge-green\">" + LIST_VERIFIED_CAPTION.replace(/'/g, "&#39;") + "</span>");
    assert.match(backupBadge(invalid.status), /badge-unverified">unverified: invalid/);
    assert.match(backupBadge(none.status), /unverified: no evidence -- this run recorded no signed document/);
    // NEGATIVE CONTROL: a Valid document on a run that is not verifiedSuccess
    // (a signer no longer accepted, a run that failed) is NOT green.
    assert.match(backupBadge(validNotGreen.status), /badge-unverified">unverified: the document verified, and the run did not succeed/);
    // None of the four says "no verification was recorded": each has a verdict.
    for (const item of list.items) {
      assert.equal(backupBadge(item.status).indexOf(VERIFICATION_CASES.NotRecorded), -1,
        item.metadata.name + " is not called unrecorded");
    }
    // The list page and the history page render the same verdict.
    const page = renderBackupList(list, "team-a");
    assert.equal((page.match(/badge badge-green/g) || []).length, 1);
    const history = renderHistoryList({ items: [] }, list, "team-a");
    assert.equal((history.match(/badge badge-green/g) || []).length, 1);
    // What the list cannot supply is named, not defaulted.
    assert.ok(valid.__contract.absent.indexOf("status.exitCode") !== -1);
    assert.ok(valid.__contract.absent.indexOf("status.evidence") !== -1);
  } finally {
    restore();
  }
});

test("a_console_restore_list_row_reads_its_summary_too", async () => {
  await consoleMode();
  const restore = transport(() => ({ status: 200, body: fixture("console/restores-list.json") }));
  try {
    const list = await apiClient().list("team-a", "restores");
    const first = list.items[0];
    assert.equal(first.status.__summary.verifiedSuccess, true);
    assert.match(restoreBadge(first.status), /badge-green/);
    assert.ok(first.__contract.absent.indexOf("status.outcome") !== -1);
  } finally {
    restore();
  }
});

// THE EXIT CODE STAYS AUTHORITATIVE (rehearsal-fix review, LOW-4). Since
// interface I8's amendment an exit-2 restore publishes a signed failure that
// verifies Valid. The legacy Restore badge mirrors the controller's rule
// (`weirkeeper::verification::restore_badge`): a recorded non-zero exitCode is
// never green, even beside a planted `outcome: pass`; an absent exitCode is
// judged on the outcome. NEGATIVE CONTROL: the same object at exit 0 is green,
// so the refusal below is the exit code's and not the fixture's.
test("a_legacy_restore_badge_is_never_green_over_a_non_zero_exit_code", () => {
  const cr = fixture("d3/restore-completed-newtopic.json");
  assert.equal(cr.status.evidence.verification.result, "Valid");
  assert.equal(cr.status.outcome, "pass");
  const at = (exitCode) => {
    const s = JSON.parse(JSON.stringify(cr.status));
    if (exitCode === undefined) {
      delete s.exitCode;
    } else {
      s.exitCode = exitCode;
    }
    return restoreBadge(s);
  };
  assert.match(at(0), /badge-green/, "the control: exit 0, Valid, pass");
  assert.match(at(undefined), /badge-green/, "no recorded code is judged on the outcome");
  assert.doesNotMatch(at(2), /badge-green/, "exit 2 with a planted pass is not green");
  assert.match(at(2), /the run itself did not succeed/);
});

test("without_the_summary_the_old_projection_says_unrecorded_which_is_the_defect", () => {
  // NEGATIVE CONTROL for the rows above: the status the projection produced
  // before -- a phase and nothing else -- is exactly what rendered every
  // console row as unrecorded.
  const before = { phase: "Succeeded", reason: "Ok" };
  assert.match(backupBadge(before), /no verification was recorded for this run/);
  assert.match(summaryBadge({ verificationState: "valid", verifiedSuccess: true }), /badge-green/);
});

test("legacy_mode_keeps_the_recorded_block_rule_with_its_key_and_instant", async () => {
  await legacyMode();
  const cr = fixture("backup-valid-exit0.json");
  const badge = backupBadge(cr.status);
  assert.match(badge, /badge-green/);
  assert.match(badge, /against key /, "the custom resource's rule names the key");
  // A recorded block wins over a summary when an object carries both (a
  // console DETAIL, after the operation route merged the block in).
  const both = Object.assign({}, cr.status, { __summary: { verificationState: "invalid", verifiedSuccess: false } });
  assert.match(backupBadge(both), /against key /);
});

test("a_console_detail_takes_the_merged_fields_off_the_absent_list", async () => {
  await consoleMode();
  const restore = transport((url) => (url.indexOf("/operations/") !== -1
    ? { status: 200, body: fixture("console/operation-backup.json") }
    : { status: 200, body: fixture("console/backup.json") }));
  try {
    const object = await apiClient().get("team-a", "backups", "orders-hourly-20260912-080000");
    assert.equal(object.__contract.absent.indexOf("status.evidence"), -1,
      "the operation route supplied the evidence block");
    assert.ok(object.status.evidence !== undefined);
  } finally {
    restore();
  }
});

// ------------------------------------------ SCORECARD-FACTS-UNVERIFIED-SHOWN

test("a_restores_scorecard_facts_are_labelled_its_unverified_claim_unless_it_verified", () => {
  const valid = fixture("restore-valid-pass.json");
  const unverified = fixture("restore-notattempted-pass.json");
  const green = renderRestoreDetail(valid);
  const claim = renderRestoreDetail(unverified);
  // The verified one shows the facts plainly.
  assert.equal(green.indexOf("data-scorecard-claim"), -1, "a Valid scorecard's facts carry no caption");
  // The unverified one captions every scorecard fact.
  for (const label of ["outcome", "level", "result", "measured.rtoSeconds", "measured.rpoSeconds"]) {
    assert.match(claim, new RegExp("<dt>" + label.replace(".", "\\.") +
      "</dt><dd>[^<]*<span class=\"badge badge-unverified\" data-scorecard-claim=\"true\">"),
    label + " is shown as the scorecard's unverified claim");
  }
  assert.match(claim, /data-scorecard-claim="note"/, "and the section says why");
  // NEGATIVE CONTROL: the verdict rule is the badge rule. Make the same
  // unverified object Valid and the captions go.
  const made = JSON.parse(JSON.stringify(valid));
  made.status.outcome = unverified.status.outcome;
  assert.equal(renderRestoreDetail(made).indexOf("data-scorecard-claim"), -1);
});

test("the_operation_view_captions_a_restores_outcome_until_its_evidence_is_green", () => {
  const untrusted = operationFacts(decodeOperation(fixture("console/operation-restore-untrusted.json"))
    .value.item, true);
  assert.match(renderResult(untrusted), /<dt>outcome<\/dt><dd>[^<]*<span class="badge badge-unverified" data-scorecard-claim="true">/);
  const green = operationFacts(decodeOperation(fixture("console/operation-restore-completed.json"))
    .value.item, true);
  assert.equal(renderResult(green).indexOf("data-scorecard-claim"), -1,
    "a verified restore's outcome is shown plainly");
  const backup = operationFacts(decodeOperation(fixture("console/operation-backup-preparing.json")).value.item, true);
  assert.equal(renderResult(backup).indexOf("data-scorecard-claim"), -1, "a Backup has no scorecard outcome");
});
