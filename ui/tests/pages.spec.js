// pages.spec.js -- the behaviour arm of the four read-and-create pages.
//
// Run with `node --test 'ui/tests/*.js'` from `logweir/`, which is what
// `scripts/check-ui-behaviour.sh` runs on every `just lint`. The quoted glob is
// not decoration: `node --test ui/tests/` executes a DIRECTORY argument as a
// module on node 22 and later and fails with `Cannot find module`, and a bare
// `node --test` from the repository root reports `tests 0` and exits 0 -- a
// green run that asserted nothing. The gate uses the glob AND refuses a run
// whose reported test count is zero.
//
// WHY THESE ASSERTIONS ARE STRING CONTAINMENT. Every function under test is a
// pure function from a JSON object to an HTML string: no DOM, no network, no
// clock. There is no npm, no bundler, no browser and no DOM shim in this tree
// (Global Constraints 17 and 21), so a page rule is only checkable if a page
// produces a VALUE, and it does. The objects below are checked-in fixtures in
// the shape the API server returns, generated field-for-field from the CRD
// types in `crates/weirkeeper/src/crds/`.
//
// THERE IS NO TEXT-CHECK FALLBACK, AND THERE MUST NOT BE. A scan over this
// file could assert that an assertion was WRITTEN. It could not assert that it
// HOLDS -- so on a machine without node every page mutant would survive while
// the gate reported green, which is the failure mode STANDING RULE 21 names:
// a guard whose mutant passes is worse than no guard, because the ledger
// records it as closed. `check-ui-behaviour.sh` fails instead of degrading.
//
// NOTHING HERE REACHES THE NETWORK. No module under test issues a request; the
// two mount halves that do are not imported. `ui_lint.rs`'s
// `the_ui_behaviour_suite_never_dials` asserts the absence of every dial token
// from this whole directory.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  BUCKET_FOOTER,
  ENGINE_SUBREPORT_LINE,
  IMMUTABLE_LINE,
  RETENTION_SENTENCE,
} from "../render.js";
import { renderClusterList } from "../pages/clusters.js";
import { renderRetentionPanel, renderScheduleList } from "../pages/schedules.js";
import { renderBackupDetail, renderBackupList } from "../pages/backups.js";
import { renderHistoryList, renderRestoreDetail } from "../pages/history.js";

// ------------------------------------------------------------------ helpers

const FIXTURES = fileURLToPath(new URL("./fixtures/", import.meta.url));

function fixture(name) {
  return JSON.parse(readFileSync(FIXTURES + name, "utf8"));
}

/** The text a viewer sees: the five entities `render.js`'s `esc` produces,
 *  reversed. Assertions about a COPYABLE COMMAND are assertions about what a
 *  viewer's clipboard receives, not about the bytes that encode it. */
function decode(html) {
  return html
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, "&");
}

/** The first badge in a rendered output, class and caption together. The badge
 *  is where the verdict lives, so the assertions about what a verdict may and
 *  may not say are made on it and not on the surrounding page -- a `Restore`
 *  detail view RENDERS its `outcome`, so the whole-output form of the "never
 *  the word pass" rule is available on the `Backup` path only, and is asserted
 *  there in its strong form as well. */
function badgeOf(html) {
  const match = html.match(/<span class="badge[^"]*">[^<]*<\/span>/);
  assert.ok(match !== null, "the rendered output carries a badge");
  return match[0];
}

const TICK = "✓";
const ARROW = "→";

const BACKUP_FIXTURES = [
  ["backup-valid-exit0.json", true],
  ["backup-valid-exit2.json", false],
  ["backup-invalid-exit0.json", false],
  ["backup-notattempted-exit0.json", false],
];

const RESTORE_FIXTURES = [
  ["restore-valid-pass.json", true],
  ["restore-valid-failintegrity.json", false],
  ["restore-invalid-pass.json", false],
  ["restore-notattempted-pass.json", false],
];

// -------------------------------------------------------------------- rows

test("evidence_immutable_never_renders_as_a_tick", () => {
  for (const [name] of BACKUP_FIXTURES) {
    const out = renderBackupDetail(fixture(name));
    assert.ok(
      out.includes(IMMUTABLE_LINE),
      name + ": the exact immutable line is rendered",
    );
    assert.equal(
      IMMUTABLE_LINE,
      "immutable: false (this field carries no information in format_version 1.0.0)",
      "the line is the one spec section 12 fixes, verbatim",
    );
    assert.equal(
      out.indexOf(TICK),
      -1,
      name + ": the WHOLE output carries no tick anywhere. `immutable` is false in every " +
        "document this product writes and the field carries no information at all; a tick " +
        "beside it would tell a viewer the document is WORM-protected, which nothing " +
        "established.",
    );
  }
  // And on the Restore path, where the same block is rendered.
  for (const [name] of RESTORE_FIXTURES) {
    const out = renderRestoreDetail(fixture(name));
    assert.ok(out.includes(IMMUTABLE_LINE), name + ": the exact immutable line is rendered");
    assert.equal(out.indexOf(TICK), -1, name + ": no tick anywhere in the output");
  }
});

test("engine_subreport_renders_as_absent", () => {
  assert.equal(
    ENGINE_SUBREPORT_LINE,
    "engine sub-report: not produced by this engine version",
  );
  for (const [name] of RESTORE_FIXTURES) {
    const out = renderRestoreDetail(fixture(name));
    assert.ok(
      out.includes("engine sub-report: not produced by this engine version"),
      name + ": `engine_subreport` is null in every scorecard this engine version writes. " +
        "Omitting the row renders 'no caveat', which is a different and false claim.",
    );
  }
});

test("objectives_and_partial_reason_are_rendered", () => {
  const object = fixture("restore-valid-failintegrity.json");
  const out = renderRestoreDetail(object);
  const objectives = object.status.objectives;
  const integrity = object.status.integrity;

  assert.ok(String(objectives.rtoSeconds).length > 0);
  for (const value of [
    objectives.rtoSeconds,
    objectives.rpoSeconds,
    objectives.passRate,
    objectives.met,
  ]) {
    assert.ok(
      out.includes(String(value)),
      "the objectives value " + String(value) + " is rendered; `measured` alone says what " +
        "happened without saying whether it was enough",
    );
  }
  assert.ok(integrity.partialReason.length > 0);
  assert.ok(
    out.includes(integrity.partialReason),
    "integrity.partialReason is rendered; a `partial` with no reason is a verdict an " +
      "auditor cannot act on, and the frozen `drill show` table omits it",
  );
  assert.ok(out.includes(String(integrity.result)), "integrity.result is rendered");
  assert.ok(out.includes(String(integrity.level)), "integrity.level is rendered");
});

test("a_backup_green_badge_needs_a_valid_verification_and_exit_code_zero", () => {
  for (const [name, green] of BACKUP_FIXTURES) {
    const object = fixture(name);
    assert.equal(
      Object.prototype.hasOwnProperty.call(object.status, "outcome"),
      false,
      name + ": a Backup carries no `outcome` field at all",
    );
    const out = renderBackupDetail(object);
    const mark = badgeOf(out);
    const verification = object.status.evidence.verification;

    if (green) {
      assert.ok(mark.includes("badge-green"), name + ": Valid + exitCode 0 is the green case");
      assert.ok(
        mark.includes(
          "verified by weirkeeper at " +
            verification.verifiedAt +
            " against key " +
            verification.matchedKeyId,
        ),
        name + ": the green label names the instant and the key id, verbatim",
      );
    } else {
      assert.equal(
        mark.includes("badge-green"),
        false,
        name + ": only Valid AND exitCode 0 is green",
      );
      assert.ok(mark.includes("unverified"), name + ": every other case is the word `unverified`");
    }
    assert.equal(
      out.includes("verified in your browser"),
      false,
      name + ": this page holds no key and verified nothing",
    );
    assert.equal(
      mark.indexOf("pass"),
      -1,
      name + ": the BADGE never says `pass`. A Backup has no `outcome`, so a page that " +
        "labelled one would be reading a field that does not exist.",
    );

    // AND THE WHOLE-OUTPUT FORM, with the one exemption the wire format
    // forces. `status.exitReason` for a not-a-pass run is the literal string
    // `drill-not-pass` (Global Constraint 11's wire reasons), and this page
    // echoes the status the controller wrote, verbatim, because a page that
    // paraphrased a recorded reason would be inventing one. So the rule is:
    // remove the value the status itself carries, and the word must not
    // appear anywhere else in a Backup view -- not in a caption, not in a
    // column, not in a heading.
    const echoed =
      typeof object.status.exitReason === "string" ? object.status.exitReason : "";
    const withoutEchoedStatus = echoed === "" ? out : out.split(echoed).join("");
    assert.equal(
      withoutEchoedStatus.indexOf("pass"),
      -1,
      name + ": apart from the `exitReason` the controller recorded, the string `pass` " +
        "appears nowhere in a Backup view.",
    );
  }
});

test("a_restore_green_badge_needs_a_valid_verification_and_a_pass_outcome", () => {
  for (const [name, green] of RESTORE_FIXTURES) {
    const object = fixture(name);
    const out = renderRestoreDetail(object);
    const mark = badgeOf(out);
    const verification = object.status.evidence.verification;

    if (green) {
      assert.equal(object.status.outcome, "pass");
      assert.ok(mark.includes("badge-green"), name + ": Valid + outcome pass is the green case");
      assert.ok(
        mark.includes(
          "verified by weirkeeper at " +
            verification.verifiedAt +
            " against key " +
            verification.matchedKeyId,
        ),
        name + ": the green label names the instant and the key id, verbatim",
      );
    } else {
      assert.equal(
        mark.includes("badge-green"),
        false,
        name + ": only Valid AND outcome pass is green",
      );
      assert.ok(mark.includes("unverified"), name + ": every other case is the word `unverified`");
    }
    assert.equal(
      out.includes("verified in your browser"),
      false,
      name + ": this page holds no key and verified nothing",
    );
    assert.equal(
      mark.indexOf("pass"),
      -1,
      name + ": the BADGE never says `pass`. A `pass` is a statement about a run; the badge " +
        "is a statement about a signed document. (The whole-output form of this rule is " +
        "asserted on the Backup path: a Restore detail view renders its own `outcome`, and " +
        "`restore-valid-pass.json`'s outcome is the word itself.)",
    );
  }
});

test("the_restore_badge_reads_the_outcome_and_not_the_exit_code", () => {
  // Every checked-in Restore fixture has `outcome == "pass"` exactly when
  // `exitCode == 0`, so a rule that read the exit code would pass the rows
  // above unnoticed (Task 26's review, finding M-1). These two objects pull
  // the fields apart: a Valid scorecard SAYING the restore did not reconcile,
  // with exit 0, must not be green; a Valid `pass` with a non-zero exit is.
  const base = fixture("restore-valid-pass.json");
  assert.equal(base.status.evidence.verification.result, "Valid");

  const failedButExitZero = JSON.parse(JSON.stringify(base));
  failedButExitZero.status.outcome = "fail-integrity";
  failedButExitZero.status.exitCode = 0;
  const notGreen = badgeOf(renderRestoreDetail(failedButExitZero));
  assert.equal(
    notGreen.includes("badge-green"),
    false,
    "Valid + fail-integrity + exit 0 is NOT green: the Restore rule reads `outcome`",
  );
  assert.ok(notGreen.includes("unverified"), "and it says `unverified`");

  const passedButExitTwo = JSON.parse(JSON.stringify(base));
  passedButExitTwo.status.outcome = "pass";
  passedButExitTwo.status.exitCode = 2;
  const green = badgeOf(renderRestoreDetail(passedButExitTwo));
  assert.ok(
    green.includes("badge-green"),
    "Valid + pass + exit 2 IS green: the exit code is not part of the Restore rule",
  );
});

test("the_backup_detail_renders_the_covered_window_as_rfc3339", () => {
  const object = fixture("backup-valid-exit0.json");
  assert.deepEqual(
    object.status.windowCovered,
    { fromMs: 1757253900000, toMs: 1757254200000 },
    "the fixture carries the window as epoch milliseconds (interface I22)",
  );
  const out = renderBackupDetail(object);
  assert.ok(
    out.includes("covered: 2025-09-07T14:05:00Z " + ARROW + " 2025-09-07T14:10:00Z"),
    "both bounds are rendered as RFC 3339",
  );
  assert.equal(out.indexOf("1757253900000"), -1, "the raw fromMs is never printed");
  assert.equal(out.indexOf("1757254200000"), -1, "the raw toMs is never printed");
});

test("the_independent_check_command_fetches_before_it_verifies", () => {
  const object = fixture("backup-valid-exit0.json");
  const evidence = object.status.evidence;
  const out = decode(renderBackupDetail(object));

  const lines = out
    .slice(out.indexOf("<pre class=\"copy\">") + "<pre class=\"copy\">".length)
    .split("</pre>")[0]
    .split("\n");
  assert.equal(lines.length, 4, "four lines, not two: the verifiers take local files");

  assert.equal(
    lines[0],
    "aws s3 cp s3://logweir-evidence/" + evidence.receiptKey + " ./receipt.json",
  );
  assert.equal(
    lines[1],
    "aws s3 cp s3://logweir-evidence/" + evidence.sidecarKey + " ./receipt.sig",
  );
  assert.equal(
    lines[2],
    "logweir drill verify --payload-type backup-receipt --scorecard ./receipt.json " +
      "--signature ./receipt.sig --public-key <your key>",
  );
  assert.equal(
    lines[3],
    "python3 verify_scorecard.py --payload-type backup-receipt --scorecard ./receipt.json " +
      "--signature ./receipt.sig --public-key <your key>",
  );

  for (const key of [evidence.receiptKey, evidence.sidecarKey]) {
    assert.ok(
      lines[0].includes(key) || lines[1].includes(key),
      "each of the two object keys has its own fetch line",
    );
    assert.equal(lines[2].indexOf(key), -1, "no verify line carries a raw object key");
    assert.equal(lines[3].indexOf(key), -1, "no verify line carries a raw object key");
  }
  const firstVerify = out.indexOf("logweir drill verify");
  assert.ok(out.indexOf("aws s3 cp") < firstVerify, "both fetches precede either verify line");

  // The Restore path prints the same four lines with the other payload type.
  const restoreOut = decode(renderRestoreDetail(fixture("restore-valid-pass.json")));
  assert.ok(restoreOut.includes("--payload-type scorecard"));
  assert.equal(restoreOut.indexOf("--payload-type backup-receipt"), -1);
});

test("every_list_view_carries_the_bucket_footer", () => {
  assert.equal(
    BUCKET_FOOTER,
    "this list is the cluster's view; the authoritative index is the evidence bucket",
  );
  const views = [
    ["renderClusterList", renderClusterList(fixture("cluster-scram.json"))],
    ["renderScheduleList", renderScheduleList(fixture("schedule-retention.json"))],
    ["renderBackupList", renderBackupList(fixture("backup-valid-exit0.json"))],
    [
      "renderHistoryList",
      renderHistoryList(fixture("restore-valid-pass.json"), fixture("backup-valid-exit0.json")),
    ],
  ];
  for (const [label, out] of views) {
    assert.ok(
      out.includes(
        "this list is the cluster's view; the authoritative index is the evidence bucket",
      ),
      label + ": a deleted custom resource does not delete a signed document, so no list " +
        "view may present itself as the index",
    );
  }
  // And the list views really are lists: each one rendered its object.
  assert.ok(views[0][1].includes("orders-prod"));
  assert.ok(views[1][1].includes("orders-hourly"));
  assert.ok(views[2][1].includes("orders-hourly-20260911-124000"));
  assert.ok(views[3][1].includes("orders-drill-a") && views[3][1].includes("Backup"));
});

test("the_retention_panel_says_logweir_never_deletes", () => {
  assert.equal(
    RETENTION_SENTENCE,
    "Logweir never deletes from your archive. These are the commands you would run.",
  );
  const object = fixture("schedule-retention.json");
  const out = renderRetentionPanel(object);
  assert.ok(
    out.includes("Logweir never deletes from your archive. These are the commands you would run."),
  );
  assert.ok(out.includes("aws s3 rm"), "the scheme's own removal command is printed");
  assert.ok(out.includes("mc rm"), "and the mc spelling of the same removal");

  const text = decode(out);
  for (const command of object.status.retentionReport.awsCli) {
    assert.ok(text.includes(command), "the awsCli command is printed verbatim: " + command);
  }
  for (const command of object.status.retentionReport.mcCli) {
    assert.ok(text.includes(command), "the mcCli command is printed verbatim: " + command);
  }
  for (const set of object.status.retentionReport.setsThatWouldBeRemoved) {
    assert.ok(out.includes(set.backupId), "the set that would be removed is named");
    assert.ok(out.includes(set.reason), "and why");
  }
  for (const kept of object.status.retentionReport.setsKept) {
    assert.ok(out.includes(kept), "the kept set is named");
  }
});

test("no_secret_value_is_rendered", () => {
  const object = fixture("cluster-scram.json");
  assert.equal(object.spec.auth.secretRef.name, "my-scram");
  const out = renderClusterList(object);
  assert.ok(out.includes("my-scram"), "the Secret's NAME is rendered; a name is not a secret");
  assert.equal(
    out.toLowerCase().indexOf("password"),
    -1,
    "no credential word reaches the page. There is no password field on this object in any " +
      "mode, so there is nothing here for a page to leak -- and this asserts the page does " +
      "not invent one either.",
  );
  assert.ok(out.includes("scramSha512"), "the mode is rendered");
  assert.ok(out.includes("logweir-reader"), "and the username, which is what planBytes binds");
});
