// detail-trust.spec.js -- CONSOLE-DETAIL-TRUST-BASIS-DROPPED (PLAT-18.2
// re-check, LOW-R3).
//
// THE DEFECT. A console DETAIL view reads the run's operation route and
// `ui/client.js` `mergeOperation` folds it into the custom-resource-shaped
// object the pages render. It copied `result`, `matchedKeyId` and
// `verifiedAt` and dropped `trust.basis`, so the one badge rule
// (`validVerification`, `verificationCase`) saw an absent block -- the
// pre-existing rule -- and a `Valid` verdict on `RecordedBeforeRevocation`
// read green, as if the compromised key were still trusted. The scorecard
// caption ("unverified scorecard claim") uses the same rule and inherited the
// gap. D3 section 7.4: `RecordedBeforeRevocation` is "never green";
// `Historical` is a pass that carries the retired-key qualifier; `Current` is
// a plain pass.
//
// These rows drive the real transport (the one platform call stubbed) in BOTH
// modes and hold the two to one answer per basis. The operation view
// (`ui/pages/operation.js`), which reads the API's combined trust word, is
// held to the same three answers.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { apiClient, resetMode, selectMode } from "../client.js";
import { HISTORICAL_SUFFIX, VERIFICATION_CASES } from "../render.js";
import { renderBackupDetail } from "../pages/backups.js";
import { renderRestoreDetail } from "../pages/history.js";
import { evidenceGreen, operationFacts, renderEvidence, renderResult } from "../pages/operation.js";
import { decodeD3Operation, decodeOperationTrust } from "../contract.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));
const copy = (value) => JSON.parse(JSON.stringify(value));

const BASES = ["Current", "Historical", "RecordedBeforeRevocation"];

/** The API's combined word for a `Valid` verdict on each basis, as
 *  `crates/logweir-api/src/status.rs` `trust_of` computes it. A
 *  `RecordedBeforeRevocation` verdict arrives as `verified`: the word alone
 *  does not carry the veto, which is why the basis must travel. */
const STATE_OF = {
  Current: "verified",
  Historical: "verifiedHistorical",
  RecordedBeforeRevocation: "verified",
};

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

/** What a page must say for a `Valid` verdict on `basis`. Throws when it does
 *  not -- which is what the negative controls below rely on. */
function judge(html, basis, what) {
  const green = html.indexOf("badge badge-green") !== -1;
  const historical = html.indexOf(HISTORICAL_SUFFIX) !== -1;
  if (basis === "RecordedBeforeRevocation") {
    assert.equal(green, false, what + ": RecordedBeforeRevocation is never green");
    assert.ok(html.indexOf(VERIFICATION_CASES.RecordedBeforeRevocation) !== -1,
      what + ": the unverified badge names the case");
  } else if (basis === "Historical") {
    assert.equal(green, true, what + ": Historical is a pass");
    assert.equal(historical, true, what + ": and carries the retired-key qualifier");
  } else {
    assert.equal(green, true, what + ": Current is green");
    assert.equal(historical, false, what + ": with no retired-key qualifier");
  }
}

/** The scorecard facts are captioned exactly when the verdict is not green. */
function judgeClaim(html, basis, what) {
  const captioned = html.indexOf("data-scorecard-claim=\"true\"") !== -1;
  assert.equal(captioned, basis === "RecordedBeforeRevocation",
    what + ": the scorecard caption follows the badge rule");
}

/** The operation route's body for a `Valid` verdict on `basis`. */
function operationBody(name, basis) {
  const body = fixture(name);
  body.item.trust = Object.assign({}, body.item.trust || {}, {
    state: STATE_OF[basis], basis: basis,
    keyState: basis === "Current" ? "Active" : basis === "Historical" ? "Retired" : "Revoked",
  });
  body.item.verifiedSuccess = basis !== "RecordedBeforeRevocation";
  return body;
}

async function consoleDetail(plural, name, objectFixture, operation) {
  const restore = transport((url) => (url.indexOf("/operations/") !== -1
    ? { status: 200, body: operation }
    : { status: 200, body: fixture(objectFixture) }));
  try {
    return await apiClient().get("team-a", plural, name);
  } finally {
    restore();
  }
}

async function legacyDetail(plural, name, cr) {
  const restore = transport(() => ({ status: 200, body: cr }));
  try {
    return await apiClient().get("team-a", plural, name);
  } finally {
    restore();
  }
}

/** A custom resource whose recorded `Valid` verdict carries `basis`. */
function crWithBasis(name, basis) {
  const cr = fixture(name);
  cr.status.evidence.verification.trust = { basis: basis };
  return cr;
}

// ------------------------------------------------------------ console mode

test("a_console_backup_detail_judges_the_verdict_by_its_trust_basis", async () => {
  await consoleMode();
  for (const basis of BASES) {
    const object = await consoleDetail("backups", "orders-hourly-20260912-080000",
      "console/backup.json", operationBody("console/operation-backup.json", basis));
    assert.equal(object.status.evidence.verification.trust.basis, basis,
      "the basis travels to where the custom resource keeps it");
    judge(renderBackupDetail(object), basis, "console backup detail, " + basis);
  }
});

test("a_console_restore_detail_judges_the_verdict_and_the_scorecard_facts_by_the_basis", async () => {
  await consoleMode();
  for (const basis of BASES) {
    const object = await consoleDetail("restores", "orders-drill-20260911",
      "console/restore.json", operationBody("console/operation-restore-completed.json", basis));
    assert.equal(object.status.outcome, "pass");
    const html = renderRestoreDetail(object);
    judge(html, basis, "console restore detail, " + basis);
    judgeClaim(html, basis, "console restore detail, " + basis);
  }
});

test("a_console_detail_from_a_server_with_no_trust_block_keeps_the_pre_existing_rule", async () => {
  // D3 section 12: an absent block is NOT a downgrade. `operation-backup.json`
  // is the frozen sixteen with no `trust`; its Valid, exit-0 run stays green.
  await consoleMode();
  const body = fixture("console/operation-backup.json");
  assert.equal(decodeOperationTrust(body), null);
  const object = await consoleDetail("backups", "orders-hourly-20260912-080000",
    "console/backup.json", body);
  assert.equal(object.status.evidence.verification.trust, undefined);
  judge(renderBackupDetail(object), "Current", "console backup detail, no trust block");
});

test("the_basis_is_not_written_where_no_result_was_recorded", async () => {
  // A basis refines a recorded result and never stands in for one: a run the
  // controller has not judged keeps reading its list summary, not "no
  // verification was recorded".
  await consoleMode();
  const body = fixture("console/operation-backup-preparing.json");
  const object = await consoleDetail("backups", "orders-hourly-20260912-080000",
    "console/backup.json", body);
  assert.equal(((object.status.evidence || {}).verification || {}).trust, undefined);
});

// ------------------------------------------------------------- legacy mode

test("a_legacy_backup_detail_judges_the_verdict_by_its_trust_basis", async () => {
  await legacyMode();
  for (const basis of BASES) {
    const object = await legacyDetail("backups", "orders-hourly-20260911-124000",
      crWithBasis("backup-valid-exit0.json", basis));
    judge(renderBackupDetail(object), basis, "legacy backup detail, " + basis);
  }
});

test("a_legacy_restore_detail_judges_the_verdict_and_the_scorecard_facts_by_the_basis", async () => {
  await legacyMode();
  for (const basis of BASES) {
    const object = await legacyDetail("restores", "orders-drill",
      crWithBasis("restore-valid-pass.json", basis));
    const html = renderRestoreDetail(object);
    judge(html, basis, "legacy restore detail, " + basis);
    judgeClaim(html, basis, "legacy restore detail, " + basis);
  }
});

// ----------------------------------------------------------- operation view

test("the_operation_view_judges_the_verdict_by_the_basis_in_both_modes", () => {
  for (const basis of BASES) {
    const consoleView = operationFacts(decodeD3Operation(
      operationBody("console/operation-restore-completed.json", basis)).value.item, true);
    judge(renderEvidence(consoleView), basis, "console operation view, " + basis);
    judgeClaim(renderResult(consoleView), basis, "console operation view, " + basis);
    const legacyView = operationFacts(crWithBasis("restore-valid-pass.json", basis), false);
    judge(renderEvidence(legacyView), basis, "legacy operation view, " + basis);
    assert.equal(evidenceGreen(consoleView), evidenceGreen(legacyView),
      basis + ": the two modes give one answer");
  }
});

// -------------------------------------------------------- negative controls

test("the_old_projection_without_the_basis_fails_the_rows_above", async () => {
  // NEGATIVE CONTROL. The projection before the fix is this one with the
  // basis dropped. `judge` must REFUSE what it produced for
  // RecordedBeforeRevocation -- a green badge -- or the rows above prove
  // nothing.
  await consoleMode();
  const backup = await consoleDetail("backups", "orders-hourly-20260912-080000",
    "console/backup.json", operationBody("console/operation-backup.json", "RecordedBeforeRevocation"));
  delete backup.status.evidence.verification.trust;
  assert.throws(() => judge(renderBackupDetail(backup), "RecordedBeforeRevocation", "old backup"),
    /RecordedBeforeRevocation is never green/);

  const restore = await consoleDetail("restores", "orders-drill-20260911",
    "console/restore.json",
    operationBody("console/operation-restore-completed.json", "RecordedBeforeRevocation"));
  delete restore.status.evidence.verification.trust;
  const html = renderRestoreDetail(restore);
  assert.throws(() => judge(html, "RecordedBeforeRevocation", "old restore"),
    /RecordedBeforeRevocation is never green/);
  assert.throws(() => judgeClaim(html, "RecordedBeforeRevocation", "old restore"),
    /the scorecard caption follows the badge rule/);

  // And a Historical verdict lost its qualifier.
  const historical = await consoleDetail("backups", "orders-hourly-20260912-080000",
    "console/backup.json", operationBody("console/operation-backup.json", "Historical"));
  delete historical.status.evidence.verification.trust;
  assert.throws(() => judge(renderBackupDetail(historical), "Historical", "old historical"),
    /retired-key qualifier/);
});

test("the_old_operation_view_rule_on_the_trust_word_alone_fails_the_rows_above", () => {
  // NEGATIVE CONTROL for the operation view. The word alone is `verified` for
  // RecordedBeforeRevocation; with the basis unread (dropped here) the view
  // is green, and `judge` refuses it.
  const view = operationFacts(decodeD3Operation(
    operationBody("console/operation-restore-completed.json", "RecordedBeforeRevocation")).value.item, true);
  const unread = Object.assign({}, view, { trust: Object.assign({}, view.trust, { basis: undefined }) });
  assert.throws(() => judge(renderEvidence(unread), "RecordedBeforeRevocation", "old operation view"),
    /RecordedBeforeRevocation is never green/);
});
