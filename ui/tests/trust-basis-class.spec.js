// trust-basis-class.spec.js -- TRUST-VALID-BASIS-CLASS, the console half
// (review LOW-1 of `claude/api-trust-state`).
//
// THE DEFECT. Legacy mode read a custom resource's `trust.basis` with the rule
// written for the product API's DTO (`basisAllowsGreen`), where the string
// `None` spells an ABSENT block. A custom resource can say "no block" by having
// none, so there a PRESENT `{basis: "None"}`, `{}` or `{basis: null}` is a block
// the controller refuses: `weirkeeper::verification::valid_verification` says
// `VerificationUntrusted`, `logweir-api` says `untrusted`, and the console-mode
// detail folds it to `Untrusted`. Legacy mode alone painted it green.
//
// THE RULE NOW (render.js `trustBlockAllowsGreen`, the controller's
// `ValidBasis`): NO `trust` key keeps D3 section 12's pre-existing rule; a
// present block is green only on `Current` or `Historical`.
//
// These rows run every shape through the legacy badge on both pages, a legacy
// detail through the real transport, and the legacy operation view; the
// console detail is held to its own answer for the absent block the DTO spells
// `None`. The last row is the negative control: the old rule fails them.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { apiClient, resetMode, selectMode } from "../client.js";
import { basisAllowsGreen, trustBlockAllowsGreen } from "../render.js";
import { backupBadge, renderBackupDetail, validVerification } from "../pages/backups.js";
import { restoreBadge } from "../pages/history.js";
import { evidenceGreen, operationFacts, renderEvidence } from "../pages/operation.js";

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));
const fixture = (name) => JSON.parse(readFileSync(FIXTURES + name, "utf8"));

const ABSENT = Symbol("no trust key");

/** Every shape of `verification.trust` beside a `Valid`, and whether it is
 *  green. `ABSENT` is a resource with NO `trust` key at all (D3 section 12). */
const SHAPES = [
  ["Current", { basis: "Current", keyState: "Active" }, true],
  ["Historical", { basis: "Historical", keyState: "Retired" }, true],
  ["no trust key (D3 section 12)", ABSENT, true],
  ["Unverified", { basis: "Unverified", keyState: "Active" }, false],
  ["RecordedBeforeRevocation", { basis: "RecordedBeforeRevocation", keyState: "Revoked" }, false],
  ["a present basis None", { basis: "None", keyState: "Active" }, false],
  ["an empty block", {}, false],
  ["a null basis", { basis: null }, false],
  ["a null block", null, false],
  ["a word this build does not know", { basis: "SomeFutureBasis" }, false],
];

/** A `Valid`, exit-0 `Backup` custom resource carrying `trust`. */
function backupCr(trust) {
  const cr = fixture("backup-valid-exit0.json");
  const v = cr.status.evidence.verification;
  assert.equal(v.result, "Valid");
  delete v.trust;
  if (trust !== ABSENT) {
    v.trust = JSON.parse(JSON.stringify(trust));
  }
  return cr;
}

/** A `Valid`, `outcome: pass` `Restore` custom resource carrying `trust`. */
function restoreCr(trust) {
  const cr = fixture("d3/restore-completed-newtopic.json");
  const v = cr.status.evidence.verification;
  assert.equal(v.result, "Valid");
  assert.equal(cr.status.outcome, "pass");
  delete v.trust;
  if (trust !== ABSENT) {
    v.trust = JSON.parse(JSON.stringify(trust));
  }
  return cr;
}

const isGreen = (html) => html.indexOf("badge badge-green") !== -1;

/** Every legacy surface's answer for one shape, under `rule` -- the page's own
 *  functions by default. */
function legacyAnswers(trust) {
  const backup = backupCr(trust);
  const restore = restoreCr(trust);
  const facts = operationFacts(backup, false);
  assert.equal(facts.console, false, "a custom resource, read in legacy mode");
  return {
    validVerification: validVerification(backup.status) !== null,
    backupBadge: isGreen(backupBadge(backup.status)),
    restoreBadge: isGreen(restoreBadge(restore.status)),
    backupDetail: isGreen(renderBackupDetail(backup)),
    operationView: evidenceGreen(facts),
    operationEvidence: isGreen(renderEvidence(facts)),
  };
}

/** Throws unless every surface's answer is `green`. */
function judgeShapes(answersOf) {
  for (const [label, trust, green] of SHAPES) {
    const answers = answersOf(trust);
    for (const [surface, said] of Object.entries(answers)) {
      assert.equal(said, green, label + ": " + surface);
    }
  }
}

test("the_raw_rule_is_the_controllers_rule_for_every_shape", () => {
  for (const [label, trust, green] of SHAPES) {
    assert.equal(trustBlockAllowsGreen(trust === ABSENT ? undefined : trust), green, label);
  }
});

test("legacy_mode_is_green_on_a_valid_only_with_no_block_or_a_current_or_historical_basis", () => {
  judgeShapes(legacyAnswers);
});

test("a_legacy_detail_through_the_transport_gives_the_same_answer", async () => {
  resetMode();
  await selectMode({ probe: async () => ({ ok: false, status: 403, body: null }) });
  for (const [label, trust, green] of SHAPES) {
    const cr = backupCr(trust);
    const original = globalThis.fetch;
    globalThis.fetch = () => Promise.resolve({
      ok: true, status: 200, text: () => Promise.resolve(JSON.stringify(cr)),
    });
    let object;
    try {
      object = await apiClient().get("team-a", "backups", cr.metadata.name);
    } finally {
      globalThis.fetch = original;
    }
    assert.equal(isGreen(renderBackupDetail(object)), green, label + ": legacy detail");
  }
});

test("the_shared_valid_basis_none_fixture_is_not_green_in_legacy_mode", () => {
  // THE FIXTURE BOTH SIDES READ. `crates/weirkeeper/tests/trust_basis_class.rs`
  // `the_shared_valid_basis_none_fixture_is_refused_by_every_controller_reader`
  // reads the same file and gets `VerificationUntrusted` from the badge; the
  // fixture's own `Verified` condition already says so.
  const cr = fixture("d3/backup-valid-basis-none.json");
  const v = cr.status.evidence.verification;
  assert.equal(v.result, "Valid");
  assert.equal(v.trust.basis, "None");
  const verified = cr.status.conditions.find((c) => c.type === "Verified");
  assert.equal(verified.reason, "VerificationUntrusted");
  assert.equal(validVerification(cr.status), null);
  assert.equal(isGreen(backupBadge(cr.status)), false);
  assert.equal(evidenceGreen(operationFacts(cr, false)), false);
});

test("a_console_detail_keeps_an_absent_block_absent_and_green", async () => {
  // THE DTO SPELLS AN ABSENT BLOCK `basis: None`, and the API's word for it is
  // `verified`. `mergeOperation` must not write that as a PRESENT `None`
  // block, or the raw rule would turn every archive an older controller wrote
  // red in a console detail. A present `None` the API judged `untrusted` is
  // folded to `Untrusted` and stays not green.
  resetMode();
  await selectMode({
    probe: async () => ({ ok: true, status: 200, body: fixture("console/session.json") }),
  });
  for (const [state, green] of [["verified", true], ["untrusted", false]]) {
    const operation = fixture("console/operation-backup.json");
    operation.item.trust = { state: state, basis: "None", keyState: "Active" };
    operation.item.verifiedSuccess = green;
    const original = globalThis.fetch;
    globalThis.fetch = (u) => Promise.resolve({
      ok: true,
      status: 200,
      text: () => Promise.resolve(JSON.stringify(String(u).indexOf("/operations/") !== -1
        ? operation
        : fixture("console/backup.json"))),
    });
    let object;
    try {
      object = await apiClient().get("team-a", "backups", "orders-hourly-20260912-080000");
    } finally {
      globalThis.fetch = original;
    }
    const v = object.status.evidence.verification;
    assert.equal(v.trust, undefined, state + ": the DTO's `None` is not written as a block");
    assert.equal(v.result, green ? "Valid" : "Untrusted", state);
    assert.equal(isGreen(renderBackupDetail(object)), green, "console detail, " + state);
  }
});

test("the_old_legacy_rule_fails_the_rows_above", () => {
  // NEGATIVE CONTROL. The rule legacy mode used before this change read a raw
  // resource's basis with the DTO rule (`basisAllowsGreen`), so a present
  // `None`, `{}`, `{basis: null}` and `null` all read as absence and went
  // green. Under it the table above must fail.
  const oldRule = (trust) => {
    const green = basisAllowsGreen(((trust === ABSENT ? undefined : trust) || {}).basis);
    return { oldRule: green };
  };
  assert.throws(() => judgeShapes(oldRule), /a present basis None: oldRule/);
  for (const trust of [{ basis: "None" }, {}, { basis: null }, null]) {
    assert.equal(oldRule(trust).oldRule, true, JSON.stringify(trust) + " was green before");
  }
});
