// pages.spec.js -- the behaviour arm of the four read-and-create pages.
//
// Run with `node --test 'ui/tests/*.spec.js'` from `logweir/`, which is what
// `scripts/check-ui-behaviour.sh` runs on every `just lint`. The quoted glob is
// not decoration: `node --test ui/tests/` executes a DIRECTORY argument as a
// module on node 22 and later and fails with `Cannot find module`, and a bare
// `node --test` from the repository root reports `tests 0` and exits 0 -- a
// green run that asserted nothing. The gate uses the glob AND refuses a run
// whose reported test count is zero.
//
// THE `.spec` SUFFIX IS PART OF THAT REFUSAL. `ui/tests/` also holds the tool
// `emit-plan.js`, and under the wider glob node ran it as a test file and
// counted it as one passing test -- so with every `*.spec.js` removed the gate
// still saw `# tests 1` and exited 0. Only files named `*.spec.js` are tests
// here; anything else in this directory is a tool the gate invokes by name.
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

// -- Task 27: the wizard, the approval flow and the roster ------------------
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { mkdtempSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { mintNames, planHash, renderPlanBytes } from "../plan.js";
import {
  APPROVE_COMMAND,
  NO_COMPLETED_BACKUP_SENTENCE,
  NO_SUCCEEDED_SENTENCE,
  RELOAD_SENTENCE,
  SCRATCH_MARKER_WARNING,
  TARGET_ROLE_SENTENCE,
  approvalRoute,
  completedBackups,
  draftFrom,
  initialState,
  renderNoCompletedBackup,
  preparePlan,
  renderBackupSetStep,
  renderPointInTimeStep,
  renderRestoreWizard,
  renderTargetStep,
  submitRestore,
} from "../pages/restore-wizard.js";
import {
  approvalRouteParams,
  approvalState,
  refuseKeyMaterial,
  renderApprovalForm,
  renderApprovalSubject,
  renderApprovalsPage,
  routeMismatches,
  subjectOf,
  submitApproval,
} from "../pages/approvals.js";
import { mountKeys, renderKeysPage } from "../pages/keys.js";
import {
  CONVENIENCE_SENTENCE,
  COPY_CAVEAT,
  PRIVATE_KEY_REFUSAL,
  RESTORE_IMMUTABLE_SENTENCE,
  SELF_ATTESTED_FALSE,
  SELF_ATTESTED_TRUE,
} from "../render.js";

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

// ===========================================================================
// TASK 27 -- the restore wizard, the out-of-band approval flow, and the
// read-only roster.
//
// THE ONE ROW THAT LEAVES THIS PROCESS is the cross-language hash equality: it
// spawns the release `logweir` binary, because the whole value of that
// assertion is that the two numbers were computed by two languages and a stub
// would make it a test of one. It writes into `node:os.tmpdir()` and nothing
// into the worktree. `the_ui_behaviour_suite_never_dials` still holds -- a
// subprocess is not a socket, and this one reaches no network.
// ===========================================================================

const REPO_ROOT = fileURLToPath(new URL("../../", import.meta.url));

/** The api stub every write row uses: it RECORDS and it never reaches a
 *  network. The rows that assert a page writes NOTHING use `throwingApi`
 *  instead, whose writers throw on entry. */
function recordingApi(reads) {
  const calls = [];
  return {
    calls: calls,
    create: async (ns, plural, body) => {
      calls.push({ ns: ns, plural: plural, body: body });
      return body;
    },
    patchSuspend: async (ns, name, value) => {
      // Recorded, not thrown: a mutant that reached for the one update this UI
      // has must be caught by an assertion naming what it did, not by a
      // TypeError about a missing stub function.
      calls.push({ ns: ns, plural: "backupschedules", patch: { name: name, value: value } });
      return {};
    },
    list: async (ns, plural) => (reads || {})[plural] || { items: [] },
    listCluster: async (plural) => (reads || {})[plural] || { items: [] },
  };
}

function throwingApi(reads) {
  return {
    create: () => {
      throw new Error("this page issued a create; it must not");
    },
    patchSuspend: () => {
      throw new Error("this page issued a patch; it must not");
    },
    list: async (ns, plural) => (reads || {})[plural] || { items: [] },
    listCluster: async (plural) => (reads || {})[plural] || { items: [] },
  };
}

/** A node stand-in. `node --test` has no DOM and this tree has no shim, so the
 *  mount halves are exercised against the smallest object `render.js`'s
 *  `replace` actually touches, with the fragment parser injected. */
function fakeNode() {
  return {
    firstChild: null,
    adopted: [],
    appendChild(child) {
      this.adopted.push(child);
    },
    removeChild() {},
    querySelector() {
      return null;
    },
    querySelectorAll() {
      return [];
    },
  };
}

const fakeParse = (html) => [{ html: html }];

/** The text inside the plan `<pre>`, decoded back to what a viewer reads. */
function planPreOf(html) {
  const open = "<pre class=\"plan-bytes\" id=\"plan-bytes\">";
  const start = html.indexOf(open);
  assert.ok(start !== -1, "the plan step renders the bytes in a read-only pre");
  const end = html.indexOf("</pre>", start);
  assert.ok(end !== -1, "the pre is closed");
  return decode(html.slice(start + open.length, end));
}

function bytesOf(text) {
  return new TextEncoder().encode(text);
}

/** The wizard state the rows below drive, built the way the page builds it. */
function wizardState() {
  return initialState(
    "logweir-t27",
    fixture("wizard-clusters.json"),
    fixture("wizard-backups.json"),
  );
}

// -------------------------------------------------------------------- rows

test("render_plan_bytes_emits_a_document_the_runner_parses", () => {
  // ARM 2 (node). Arm 1 lives in `crates/logweir/tests/ui_lint.rs` and
  // deserialises this same golden into `logweir_core::spec::RestoreSpec` from
  // Rust -- because a hash-equality test cannot catch an invented shape
  // (sha256 of X in JavaScript equals sha256 of X in Rust whatever X is), and
  // a byte comparison between two JavaScript halves cannot either once both
  // have been regenerated together.
  const fields = fixture("plan-fields.json");
  const golden = readFileSync(FIXTURES + "plan.golden.yaml", "utf8");
  const rendered = renderPlanBytes(fields);

  const left = bytesOf(rendered);
  const right = bytesOf(golden);
  assert.equal(
    left.length,
    right.length,
    "the emitter and the committed golden are the same LENGTH. A field added to the " +
      "emitter without regenerating the golden fails here, and `check-ui-behaviour.sh`'s " +
      "`diff -u` arm fails beside it.",
  );
  assert.deepEqual(
    Array.from(left),
    Array.from(right),
    "byte for byte. These bytes are what Restore.spec.planBytes carries and what the " +
      "controller writes into the runner's plan ConfigMap verbatim.",
  );

  // And the three values arm 1 reads back out of the golden are in it, so the
  // two arms are reading the same document and not two coincidences.
  assert.ok(rendered.includes("mode: \"" + fields.target.mode + "\""));
  assert.ok(rendered.includes("prefix: \"" + fields.target.topicPrefix + "\""));
  assert.ok(rendered.includes("point_in_time: \"" + fields.pointInTime + "\""));
});

test("the_plan_hash_the_page_shows_is_the_hash_the_cli_computes", async () => {
  const bytes = renderPlanBytes(fixture("plan-fields.json"));
  const directory = mkdtempSync(join(tmpdir(), "logweir-ui-hash-"));
  const spec = join(directory, "plan.yml");
  writeFileSync(spec, bytes);

  const binary = process.env.LOGWEIR_BIN || join(REPO_ROOT, "target/release/logweir");
  const run = spawnSync(
    binary,
    [
      "drill",
      "approve",
      "--spec",
      spec,
      "--key",
      FIXTURES + "approver.pem",
      "--approver",
      "ui-test",
      "--ticket",
      "UI-1",
      "--subject-kind",
      "Restore",
      "--out",
      join(directory, "approval.json"),
    ],
    { encoding: "utf8", cwd: REPO_ROOT },
  );
  assert.equal(
    run.status,
    0,
    "the release binary ran. STANDING RULE 5's fourth prerequisite; the gate refuses " +
      "without it.\nstdout:\n" + String(run.stdout) + "\nstderr:\n" + String(run.stderr),
  );

  let printed = null;
  for (const line of String(run.stdout).split("\n")) {
    const at = line.indexOf("plan_hash");
    if (at !== -1) {
      printed = line.slice(at + "plan_hash".length).trim();
    }
  }
  assert.ok(printed !== null, "the CLI printed a plan_hash line; stdout was:\n" + run.stdout);

  const computed = await planHash(bytes);
  assert.equal(
    computed,
    printed,
    "ONE EXACT STRING, CROSS-LANGUAGE. `logweir_core::ids::sha256_prefixed` over the file's " +
      "bytes and `crypto.subtle` over the same string in this page must agree, or the hash " +
      "the operator reads off the page is not the hash the approval binds.",
  );
  assert.match(computed, /^sha256:[0-9a-f]{64}$/, "and it is the sha256:<lowercase hex> form");
});

test("the_wizard_never_reserialises_the_plan_bytes", async () => {
  // A plan whose last line ENDS IN TWO SPACES and whose `name` carries a
  // non-ASCII character: the two shapes a re-serialisation actually destroys.
  // (This file is under `ui/tests/`, which `the_ui_sources_are_ascii_only`
  // excludes; every shipped byte under `ui/` outside it is ASCII.)
  const fields = fixture("plan-fields.json");
  fields.name = "restauração-4471";
  const rendered = renderPlanBytes(fields);
  const crafted = rendered.slice(0, rendered.length - 1) + "  \n";
  assert.equal(crafted.slice(-3), "  \n", "the fixture ends in two spaces and a newline");
  assert.ok(crafted.indexOf("ç") !== -1, "and carries a non-ASCII character");
  assert.notDeepEqual(
    Array.from(bytesOf(crafted.trim())),
    Array.from(bytesOf(crafted)),
    "this is a live trim mutant: trimming before `create` loses signed bytes, so the final " +
      "submitted-versus-crafted equality below fails if the plan path ever does it",
  );

  const state = wizardState();
  state.planBytes = crafted;
  const api = recordingApi();
  const shown = planPreOf(await renderRestoreWizard(state));
  await submitRestore(state, api);

  assert.equal(api.calls.length, 1, "one create");
  const submitted = api.calls[0].body.spec.planBytes;
  const left = bytesOf(shown);
  const right = bytesOf(submitted);
  assert.equal(
    left.length,
    right.length,
    "THE LENGTHS MATCH. A trim, a normalise or a JSON round-trip changes this number, and " +
      "a hash over the shorter string is a hash of a document the approver never signed.",
  );
  assert.deepEqual(
    Array.from(left),
    Array.from(right),
    "byte for byte, the string in the pre is the string handed to create",
  );
  assert.deepEqual(
    Array.from(right),
    Array.from(bytesOf(crafted)),
    "and both are the bytes the wizard was given",
  );
});

test("the_wizard_mints_both_names_before_either_create", async () => {
  const state = wizardState();
  const api = recordingApi();
  const submitted = await submitRestore(state, api);
  // THE GUIDED SUBMIT RETURNS WHERE IT GOES NEXT. No Approval authorises the
  // new Restore, so that is its approval page -- Awaiting approval -- whose
  // route carries the reviewed identity for the page to check.
  const route = submitted.route;
  assert.equal(submitted.outcome, "created");
  assert.ok(route.startsWith("#/approvals?subject="), "approval required: " + route);

  const bytes = (await preparePlan(state)).bytes;
  // An INDEPENDENT digest: node's own hash implementation, not the one the
  // page used. A suffix taken from `mintNames` would only prove the page
  // agrees with itself.
  const suffix = createHash("sha256").update(bytes, "utf8").digest("hex").slice(0, 8);

  // THE ORDER ASSERTION COMES FIRST, DELIBERATELY. A mutant that creates the
  // Approval before the Restore also changes the COUNT, and a count assertion
  // placed first would report "2 !== 1" -- true, but not the defect. The
  // defect is that the first write went to the wrong kind.
  assert.equal(
    api.calls[0].plural,
    "restores",
    "THE FIRST CREATE IS THE RESTORE. Both references are immutable, so the Restore goes " +
      "first with a dangling approvalRef and the reconciler requeues at 30 s until the " +
      "Approval arrives; an Approval created first names a subject that does not exist.",
  );
  assert.equal(
    api.calls.length,
    1,
    "and it is the ONLY create the wizard issues. The second object is created by the " +
      "approvals page, from the two documents an approver's own machine produced.",
  );

  const body = api.calls[0].body;
  assert.equal(body.metadata.name, "restore-" + suffix);
  assert.equal(
    typeof (body.spec.approvalRef || {}).name,
    "string",
    "spec.approvalRef IS SET ON THE CREATE. Restore.spec is sealed by an object-level CEL " +
      "rule, so a reference left out here can never be added: there is no later patch that " +
      "would be accepted, and api.js exports none for a page to try.",
  );
  assert.equal(
    body.spec.approvalRef.name,
    "approval-" + suffix,
    "the Restore names an Approval that DOES NOT EXIST YET -- interface I19",
  );
  assert.equal(body.spec.planBytes, bytes, "and carries the bytes both names were minted from");

  assert.ok(
    route.indexOf("subject=" + encodeURIComponent("restore-" + suffix)) !== -1,
    "the approvals route carries the restore name as its subject; route was " + route,
  );
  assert.ok(
    route.indexOf("name=" + encodeURIComponent("approval-" + suffix)) !== -1,
    "and the minted approval name; route was " + route,
  );
  assert.ok(route.indexOf("hash=" + encodeURIComponent(await planHash(bytes))) !== -1);

  // The two names really are one function of the bytes, computed before
  // anything was sent.
  const minted = await mintNames(bytes);
  assert.deepEqual(minted, {
    restoreName: "restore-" + suffix,
    approvalName: "approval-" + suffix,
  });
});

/** A Restore as the API server returns it, over `bytes`, and the subject an
 *  approval for it names -- derived exactly the way the approvals page derives
 *  it: from the object, never from a route. */
async function approvalSubjectFor(bytes, ns) {
  const names = await mintNames(bytes);
  const restore = {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "Restore",
    metadata: { name: names.restoreName, namespace: ns, uid: "0f0e0d0c-0000-4000-8000-00000000a001" },
    spec: { planBytes: bytes, approvalRef: { name: names.approvalName } },
  };
  return { restore: restore, subject: subjectOf(restore, await planHash(bytes), ns) };
}

/** The recording api, answering a read of that one Restore and a 404 for
 *  anything else. */
function approvalApi(restore) {
  const api = recordingApi();
  api.get = async (ns, plural, name) => {
    if (plural === "restores" && name === restore.metadata.name) {
      return restore;
    }
    const missing = new Error(plural + " \"" + name + "\" not found");
    missing.status = 404;
    missing.reason = "NotFound";
    throw missing;
  };
  return api;
}

test("the_approval_form_builds_all_four_spec_fields", async () => {
  const bytes = renderPlanBytes(fixture("plan-fields.json"));
  const { restore, subject } = await approvalSubjectFor(bytes, "logweir-t27");
  assert.match(subject.planHash, /^sha256:[0-9a-f]{64}$/);
  const api = approvalApi(restore);
  const result = await submitApproval(
    subject,
    { approvalBytes: "{\"approver\": \"ui-test\"}\n", sidecarBytes: "{\"signatures\": []}\n" },
    api,
  );
  assert.equal(result.outcome, "created", "nothing about this submission is key material");
  assert.equal(api.calls.length, 1);
  assert.equal(api.calls[0].plural, "approvals");

  const body = api.calls[0].body;
  // THE FOUR KEYS FIRST. `Approval.spec` has four required fields, and a form
  // that posted only the two documents posts an object the CRD schema rejects
  // -- which is a 422 an operator sees and not a value this test can read, so
  // the shape is asserted before anything in it is dereferenced.
  assert.deepEqual(
    Object.keys(body.spec).sort(),
    ["approvalBytes", "planHash", "sidecarBytes", "subjectRef"],
    "FOUR spec fields. subjectRef and planHash cannot be derived from the documents: the page " +
      "is forbidden from parsing them. Both come from the Restore the page read.",
  );
  assert.equal(
    body.metadata.name,
    restore.spec.approvalRef.name,
    "metadata.name is the name the Restore's own spec.approvalRef names",
  );
  assert.equal(body.spec.subjectRef.kind, "Restore");
  assert.equal(body.spec.subjectRef.name, restore.metadata.name, "the subject is the Restore that was read");
  assert.equal(
    body.spec.planHash,
    "sha256:" + createHash("sha256").update(bytes, "utf8").digest("hex"),
    "planHash is the sha256 of the Restore's OWN planBytes -- computed here independently, " +
      "with node's hash -- and the controller recomputes it from the same bytes regardless",
  );
  assert.ok(body.spec.approvalBytes.length > 0);
  assert.ok(body.spec.sidecarBytes.length > 0);
});

test("the_approval_form_refuses_a_private_key", async () => {
  const { subject } = await approvalSubjectFor(renderPlanBytes(fixture("plan-fields.json")), "logweir-t27");
  const cases = [
    ["a file named approver.pem", { approvalFileName: "approver.pem", approvalBytes: "x", sidecarBytes: "y" }],
    ["a pasted PKCS#8 body", { approvalBytes: "-----BEGIN PRIVATE KEY-----\nMIG…\n", sidecarBytes: "y" }],
    ["a file named x.key", { sidecarFileName: "x.key", sidecarBytes: "y", approvalBytes: "x" }],
  ];
  for (const [label, documents] of cases) {
    // `throwingApi` has no reader and throws on any write: a refusal that ran
    // after the subject was re-read, or not at all, fails with another error.
    await assert.rejects(submitApproval(subject, documents, throwingApi()), (error) => {
      assert.equal(
        error.message,
        "this page never accepts a private key",
        label + ": refused with the exact message. v0.1 has no key lifecycle, and a page that " +
          "filled that gap would be inventing the most consequential missing subsystem in " +
          "this product inside a browser.",
      );
      assert.equal(error.message, PRIVATE_KEY_REFUSAL, label + ": and it is the shared constant");
      assert.equal(error.kind, "refused", label + ": a refusal this page made before sending anything");
      return true;
    });
  }
  // The refusal is a pure function, so the same three shapes are refusable
  // before a file is ever read.
  assert.equal(refuseKeyMaterial("approver.PEM", ""), PRIVATE_KEY_REFUSAL, "case-insensitive");
  assert.equal(refuseKeyMaterial("", "-----BEGIN EC PRIVATE KEY-----"), PRIVATE_KEY_REFUSAL);
  assert.equal(refuseKeyMaterial("approval.json", "{\"approver\": \"x\"}"), null, "and lets a real approval through");
});

test("the_approval_bytes_are_submitted_verbatim", async () => {
  // Trailing whitespace and a BOM-free UTF-8 non-ASCII approver name.
  const document =
    "{\n  \"approver\": \"Zoë Ramírez\",\n  \"ticket\": \"INC-4471\"\n}   \n";
  const { restore, subject } = await approvalSubjectFor(renderPlanBytes(fixture("plan-fields.json")), "logweir-t27");
  const api = approvalApi(restore);
  await submitApproval(subject, { approvalBytes: document, sidecarBytes: "{}\n" }, api);

  const submitted = api.calls[0].body.spec.approvalBytes;
  const left = bytesOf(submitted);
  const right = bytesOf(document);
  assert.equal(left.length, right.length, "the lengths match, trailing whitespace included");
  assert.deepEqual(Array.from(left), Array.from(right), "byte for byte");
  assert.ok(
    submitted.indexOf("\"approver\"") !== -1,
    "AND IT IS NOT BASE64. Interface I18: approvalBytes and sidecarBytes are the UTF-8 " +
      "document text, verbatim. An encoding step between the approver's file and the " +
      "hashed bytes is the class of transformation planBytes exists to forbid.",
  );
});

test("self_attested_risk_is_explained_not_asserted", () => {
  const route = { ns: "logweir-t27", subject: "", hash: "", name: "" };
  const sentences = [
    ["approvals-selfattested.json", true, SELF_ATTESTED_TRUE],
    ["approvals-not-selfattested.json", false, SELF_ATTESTED_FALSE],
  ];
  for (const [name, value, sentence] of sentences) {
    const object = fixture(name);
    assert.equal(object.items[0].status.selfAttestedRisk, value, name + ": the fixture's value");
    const out = renderApprovalsPage(object, route, Date.parse("2026-09-11T13:41:00Z"));
    assert.ok(out.includes(sentence), name + ": the exact sentence is rendered");

    // AND NO BARE BOOLEAN BESIDE IT. `false` here means only that the two
    // matched key ids differ -- one operator holding both keys satisfies it --
    // so a `false` next to the words reads as "two people signed off".
    const lower = out.toLowerCase();
    for (const spelling of ["selfattested", "self-attested", "self attested"]) {
      let at = lower.indexOf(spelling);
      while (at !== -1) {
        const window = lower.slice(at, at + spelling.length + 40);
        assert.equal(window.indexOf("true"), -1, name + ": no bare `true` beside " + spelling);
        assert.equal(window.indexOf("false"), -1, name + ": no bare `false` beside " + spelling);
        at = lower.indexOf(spelling, at + 1);
      }
    }
  }
  // The two sentences are one sentence and its second half, which is what
  // makes "for false, the second half alone" checkable.
  assert.ok(SELF_ATTESTED_TRUE.endsWith(SELF_ATTESTED_FALSE));
});

test("the_approvals_list_renders_the_five_columns", async () => {
  const route = { ns: "logweir-t27", subject: "restore-1a2b3c4d", hash: "sha256:x", name: "approval-1a2b3c4d" };
  const object = fixture("approvals-selfattested.json");
  const out = renderApprovalsPage(object, route, Date.parse("2026-09-11T13:41:00Z"));
  for (const column of ["SUBJECT", "VERIFIED", "APPROVER", "KEY-ID", "AGE"]) {
    assert.ok(out.includes("<th scope=\"col\">" + column + "</th>"), "column " + column);
  }
  assert.ok(out.includes("Restore/restore-1a2b3c4d"), "the subject names its kind");
  assert.ok(out.includes(object.items[0].status.matchedKeyId), "the matched key id");
  assert.ok(out.includes(">60m<"), "the age, from creationTimestamp against the given instant");
  assert.equal(
    out.includes("verified in your browser"),
    false,
    "this page holds no key and verified nothing",
  );
  assert.equal(
    out.indexOf("id=\"approval-form\""),
    -1,
    "THE LIST CARRIES NO FORM. An approval is recorded on one Restore's own page, for the " +
      "subject read from that Restore; a form beside a list would have to assume one",
  );

  // The form, on that page: the five subject values all read-only, and the two
  // documents.
  const { subject } = await approvalSubjectFor(renderPlanBytes(fixture("plan-fields.json")), "logweir-t27");
  const form = renderApprovalForm(subject, {});
  const text = decode(form);
  for (const caption of ["SUBJECT KIND", "SUBJECT NAME", "SUBJECT UID", "PLAN HASH", "APPROVAL NAME"]) {
    assert.ok(text.includes(caption), caption);
  }
  for (const id of ["subject-kind", "subject-name", "subject-uid", "plan-hash", "approval-name"]) {
    assert.match(form, new RegExp("id=\"" + id + "\" name=\"[A-Za-z]+\" readonly value=\""), id + " is read-only");
  }
  assert.ok(text.includes("approval.json") && text.includes("approval.sig"));
});

test("the_keys_page_submits_nothing", async () => {
  const node = fakeNode();
  const api = throwingApi({ trustrosters: fixture("trustroster-default.json") });
  let writes = 0;
  api.create = () => {
    writes += 1;
    throw new Error("the keys page issued a create");
  };
  api.patchSuspend = () => {
    writes += 1;
    throw new Error("the keys page issued a patch");
  };
  // The mount is allowed to THROW here, and the write count is read first
  // either way: a page that issued a write and then failed inside its own
  // error branch would otherwise report whatever the error branch happened to
  // die on rather than the write that is the actual defect.
  let mountError = null;
  try {
    await mountKeys(node, fakeParse, api);
  } catch (error) {
    mountError = error;
  }
  assert.equal(
    writes,
    0,
    "THE KEYS PAGE ISSUES NO WRITE. The TrustRoster is cluster-scoped and admin-only, and " +
      "`trustrosters` is absent from api.js's frozen writable set, so the write would throw " +
      "a RangeError anyway -- but a page that TRIED is a page whose contract changed.",
  );
  assert.equal(
    mountError,
    null,
    "and the read half completed: " + String(mountError && mountError.message),
  );
  assert.equal(node.adopted.length, 1, "and it rendered");
  assert.ok(
    node.adopted[0].html.includes("TrustRoster"),
    "the roster page, not an error box: " + JSON.stringify(node.adopted[0]).slice(0, 200),
  );
});

test("the_keys_page_shows_expiry_from_the_roster_status", () => {
  const roster = fixture("trustroster-default.json");
  const spec = roster.items[0].spec;
  const expired = roster.items[0].status.expiredKeyIds;
  assert.ok(expired.indexOf(spec.approverKeys[0].keyId) !== -1, "the first approver key IS expired");
  assert.equal(expired.indexOf(spec.signingKeys[0].keyId), -1, "the first signing key is NOT");

  const out = renderKeysPage(roster);
  const rowOf = (keyId) => {
    const at = out.indexOf(keyId);
    assert.ok(at !== -1, "the key id " + keyId + " is rendered");
    const end = out.indexOf("</tr>", at);
    return out.slice(at, end);
  };
  assert.ok(rowOf(spec.approverKeys[0].keyId).includes("<td>expired</td>"), "marked expired");
  assert.ok(rowOf(spec.signingKeys[0].keyId).includes("<td>valid</td>"), "marked valid");
  assert.ok(
    rowOf(spec.approverKeys[1].keyId).includes("<td>valid</td>"),
    "and a second, unexpired approver key is valid -- so the column reads the status list " +
      "and not the position",
  );

  // The fingerprint command and the roster snippet, neither of them submitted.
  const text = decode(out);
  assert.ok(
    text.includes("openssl pkey -pubin -outform DER -in <key>.pub.pem | openssl dgst -sha256"),
    "the out-of-band fingerprint command, which is the only step that catches an " +
      "undisclosed key rotation",
  );
  assert.ok(text.includes("kubectl --context docker-desktop apply -f roster.yml"));
  assert.ok(text.includes("kind: TrustRoster"));
  // A Secret's value never reaches this page; the roster carries PUBLIC halves.
  assert.equal(text.indexOf("PRIVATE KEY"), -1, "no private key material anywhere on this page");
});

test("an_edit_creates_a_new_restore_and_says_so", async () => {
  const original = fixture("restore-for-edit.json");
  assert.ok(original.spec.planBytes.length > 0);

  const state = wizardState();
  state.fields = draftFrom(original, state.fields);
  state.editing = { name: original.metadata.name };
  assert.equal(
    state.fields.pointInTime,
    original.spec.pointInTime,
    "the draft is PREFILLED from the existing object",
  );

  const rendered = await renderRestoreWizard(state);
  assert.ok(
    rendered.includes(
      "Restore.spec is immutable. This creates a NEW Restore with a new plan hash; " +
        "the existing approval does not cover it.",
    ),
    "the exact sentence, in the page, above the form",
  );
  assert.ok(rendered.includes(RESTORE_IMMUTABLE_SENTENCE), "and it is the shared constant");

  // Now change something, as an edit does, and submit.
  state.fields.pointInTime = "2026-09-07T14:04:00Z";
  state.fields.target.topicPrefix = "incident-4471-";
  const api = recordingApi();
  await submitRestore(state, api);

  assert.equal(api.calls.length, 1, "one write");
  assert.equal(
    api.calls[0].plural,
    "restores",
    "A CREATE, NEVER A PATCH. Restore.spec is sealed by an object-level CEL rule, so there is " +
      "no edit that the API server would accept; a page that tried to patch one would be " +
      "offering an operation that cannot succeed.",
  );
  assert.ok(
    api.calls[0].body !== undefined,
    "and a create BODY was recorded, which a patch does not produce",
  );
  const body = api.calls[0].body;

  assert.notEqual(
    body.spec.planBytes,
    original.spec.planBytes,
    "a NEW document: Restore.spec is CEL-immutable, so an edit cannot be a patch",
  );
  assert.notEqual(
    body.metadata.name,
    original.metadata.name,
    "and a new name, because the name is a function of the bytes",
  );
  assert.notEqual(body.spec.approvalRef.name, original.spec.approvalRef.name);
});

test("the_wizard_prefills_the_default_prefix", async () => {
  // The DEFAULT route: the newest backup's covered `toMs` is
  // 2026-09-07T14:05:00Z, so the wizard's own default point in time is that
  // instant and the prefix follows from it with nothing typed.
  const state = wizardState();
  assert.equal(state.fields.pointInTime, "2026-09-07T14:05:00Z");
  assert.equal(
    state.fields.target.topicPrefix,
    "restore-20260907T140500Z-",
    "the same string logweir_core::spec::default_topic_prefix produces for that instant. " +
      "`ui_lint.rs::the_default_prefix_agrees_with_the_rust_one` computes the Rust half and " +
      "compares it with this literal, so the two cannot drift.",
  );

  // And it reaches the field, and stays editable.
  const blank = wizardState();
  blank.fields.target.topicPrefix = "";
  const out = renderTargetStep(blank);
  assert.ok(
    out.includes("value=\"restore-20260907T140500Z-\""),
    "the prefix field is prefilled: " + out,
  );
  assert.ok(out.includes("<option value=\"scratch\""), "mode option scratch");
  assert.ok(out.includes("<option value=\"newTopic\""), "mode option newTopic");
  assert.equal(
    (out.match(/<option value="/g) || []).length,
    4,
    "two modes and BOTH clusters. Task 28a: the target select lists every KafkaCluster in " +
      "the namespace and lets the runner's phase-0 guard decide, so this fixture's " +
      "`role: source` and `role: target` objects are both offered; the mode select still has " +
      "exactly the two values TargetMode accepts",
  );
});

test("the_client_side_window_check_is_labelled_a_convenience", () => {
  const state = wizardState();
  const out = renderPointInTimeStep(state);
  assert.ok(
    out.includes(
      "the archive covers [2026-09-07T12:00:00Z, 2026-09-07T14:05:00Z]; " +
        "a point outside it cannot be restored",
    ),
    "both bounds as RFC 3339, from windowCovered.fromMs and .toMs (interface I22). A page " +
      "that read covered.from/covered.to renders `Invalid Date` twice and defaults step 3 " +
      "to nothing.",
  );
  assert.equal(out.indexOf("Invalid Date"), -1);
  assert.equal(out.indexOf("1788782400000"), -1, "the raw integers are never printed");
  assert.ok(
    out.includes(
      "this check is a convenience and never the gate: the controller and phase 0 " +
        "are the gate, and they read the bytes you submit.",
    ),
    "the page says what the gate actually is",
  );
  assert.ok(out.includes(CONVENIENCE_SENTENCE));

  // A point outside the window complains, and a point inside does not.
  const outside = wizardState();
  outside.fields.pointInTime = "2026-09-08T00:00:00Z";
  assert.ok(renderPointInTimeStep(outside).includes("<p class=\"complaint\">"));
  assert.equal(renderPointInTimeStep(state).indexOf("<p class=\"complaint\">"), -1);
});

test("the_plan_step_shows_the_hash_the_names_the_caveat_and_the_command", async () => {
  const state = wizardState();
  const rendered = await renderRestoreWizard(state);
  const prepared = await preparePlan(state);
  const text = decode(rendered);

  assert.ok(rendered.includes(prepared.hash), "the hash, beside the bytes");
  assert.ok(rendered.includes(prepared.restoreName), "the minted Restore name");
  assert.ok(rendered.includes(prepared.approvalName), "the minted Approval name");
  assert.equal(prepared.restoreName.slice(8), prepared.approvalName.slice(9), "one suffix, both names");

  assert.ok(
    text.includes(
      "copy loses trailing whitespace in some browsers; download, or run kubectl " +
        "--context docker-desktop get restore <name> -o jsonpath='{.spec.planBytes}' > " +
        "<name>.yaml, and hash exactly what you downloaded.",
    ),
    "the copy caveat, verbatim, with the kubectl route to the same bytes",
  );
  assert.ok(text.includes(COPY_CAVEAT));
  assert.ok(
    text.includes(
      "logweir drill approve --spec <file> --key <privkey> --approver <id> " +
        "--ticket <id> --subject-kind Restore --out <file>",
    ),
    "the exact command, with --out shown because it defaults into the caller's cwd",
  );
  assert.ok(text.includes(APPROVE_COMMAND));
  assert.equal(
    rendered.toLowerCase().indexOf("generate a key"),
    -1,
    "this page offers no key generation of any kind",
  );

  // The preflight sentence, over the topics this plan actually names.
  assert.ok(
    rendered.includes(
      "Logweir will create 2 topics with message.timestamp.type=CreateTime and " +
        "retention.ms=-1 before the engine runs, and will refuse if the broker is " +
        "LogAppendTime and rejects the override.",
    ),
    "step 5 names what the run will do to the target before the engine starts",
  );

  // And the route the "Request approval" action navigates to.
  const route = approvalRoute(state, prepared);
  assert.ok(route.startsWith("#/approvals?"), "a hash route and nothing else: " + route);
  assert.ok(route.indexOf("ns=logweir-t27") !== -1);
});

test("the_approvals_route_values_reach_the_form_in_their_own_positions", async () => {
  // THE HOP FROM THE HASH TO THE PAGE. `subject` names which Restore; `hash`
  // and `name` are what the linking page reviewed. The extraction is
  // `approvalRouteParams`, exported by the page module itself because the
  // router cannot be imported here, and this row is the guard the hop did not
  // have.
  const digest = "sha256:" + "ab12cd34".repeat(8);
  const hash =
    "#/approvals?subject=restore-1a2b3c4d&hash=" + digest + "&name=approval-1a2b3c4d";
  assert.match(digest, /^sha256:[0-9a-f]{64}$/);

  const parsed = approvalRouteParams(hash);
  // DEEP EQUALITY, NOT THREE CONTAINMENTS. A swap of `subject` and `name` keeps
  // every value present and only moves it, so the assertion has to name which
  // key holds which string.
  assert.deepEqual(
    parsed,
    {
      ns: "",
      subject: "restore-1a2b3c4d",
      hash: digest,
      name: "approval-1a2b3c4d",
    },
    "the three route values arrive under their own keys, and a hash naming no " +
    "namespace carries no implicit default",
  );

  // AND THE PAGE PUTS EACH VALUE WHERE IT BELONGS -- read from the Restore, and
  // shown only when the route agrees with it.
  const bytes = renderPlanBytes(fixture("plan-fields.json"));
  const { restore, subject } = await approvalSubjectFor(bytes, "logweir-t27");
  const agreeing = { ns: "logweir-t27", subject: subject.name, hash: subject.planHash, name: subject.approvalName };
  const view = {
    ns: "logweir-t27",
    route: agreeing,
    restore: restore,
    subject: subject,
    approval: null,
    found: approvalState(null, subject),
    mismatches: routeMismatches(agreeing, subject),
  };
  assert.deepEqual(view.mismatches, [], "a route that names exactly the Restore's identity agrees");
  const html = renderApprovalSubject(view, 0);
  assert.ok(
    html.includes("<input id=\"subject-name\" name=\"subjectName\" readonly value=\"" + subject.name + "\">"),
    "SUBJECT NAME carries the RESTORE's name, read-only -- the subject of the approval, not the " +
      "approval's own name; the rendered page was:\n" + html,
  );
  assert.ok(
    html.includes("<input id=\"plan-hash\" name=\"planHash\" readonly value=\"" + subject.planHash + "\">"),
    "PLAN HASH carries the hash of the Restore's own bytes and is READ-ONLY",
  );
  assert.ok(
    html.includes("<input id=\"approval-name\" name=\"approvalName\" readonly value=\"" + subject.approvalName + "\">"),
    "and APPROVAL NAME is the name the Restore's approvalRef names",
  );
  // The two names are distinguishable, so the assertions above really do
  // separate the positions rather than agreeing on one string.
  assert.notEqual(subject.name, subject.approvalName);

  // A SWAPPED OR FOREIGN ROUTE VALUE IS A MISMATCH, AND NO FORM IS OFFERED.
  const swapped = Object.assign({}, agreeing, { hash: digest, name: subject.name });
  const mismatches = routeMismatches(swapped, subject);
  assert.deepEqual(
    mismatches.map((m) => m.field),
    ["plan hash", "Approval name"],
    "both disagreeing values are named",
  );
  const refused = renderApprovalSubject(Object.assign({}, view, { route: swapped, mismatches: mismatches }), 0);
  assert.equal(refused.indexOf("id=\"approval-form\""), -1, "a disagreeing route gets no form");
  assert.ok(decode(refused).includes("This link does not match Restore " + subject.name));

  // The namespace travels in the same hash and nowhere else.
  assert.equal(approvalRouteParams(hash + "&ns=logweir-t27").ns, "logweir-t27");
  // A hash with no parameters at all is four empty-ish values and never
  // `undefined`: the standalone page, which assumes no subject.
  assert.deepEqual(approvalRouteParams("#/approvals"), {
    ns: "",
    subject: "",
    hash: "",
    name: "",
  });
});

test("the_literal_default_namespace_survives_the_restore_to_approval_handoff", async () => {
  const state = wizardState();
  state.ns = "default";
  const route = approvalRoute(state, await preparePlan(state));
  assert.match(route, /(?:^|&)ns=default(?:&|$)/, "default is explicit when the router requires a namespace");
  assert.equal(approvalRouteParams(route).ns, "default");
});

test("the_restore_names_the_archive_credential_the_runner_reads_it_with", async () => {
  // WHY THIS ROW EXISTS. `spec.sourceArchive` is `ArchiveRef` -- a URL AND a
  // Secret name -- and weirkeeper injects the object-store credential into the
  // runner Job only when the second half is set. `secretRef` is optional in the
  // CRD, so a Restore created without it is ADMITTED and fails later, at the
  // archive, with the Job unable to read a single object. Nothing between this
  // page and that failure would have noticed.

  // (a) THE ARCHIVE NAMES A SECRET -> the body carries its name, beside the URL.
  const withSecret = wizardState();
  assert.equal(
    withSecret.archiveSecretName,
    "logweir-s3",
    "the name is read from the SAME Backup.spec.archive the URL came from",
  );
  const api = recordingApi();
  await submitRestore(withSecret, api);
  const archive = api.calls[0].body.spec.sourceArchive;
  assert.equal(
    archive.url,
    "s3://kafka-backups/drill-demo",
    "spec.sourceArchive.url is the archive the Backup names",
  );
  assert.equal(
    (archive.secretRef || {}).name,
    "logweir-s3",
    "AND spec.sourceArchive.secretRef.name is the credential that reaches it. Without this " +
      "the Restore is admitted -- the field is optional -- and the runner Job starts with no " +
      "object-store credential at all and fails at the archive.",
  );
  assert.deepEqual(
    Object.keys(archive).sort(),
    ["secretRef", "url"],
    "both halves of ArchiveRef and nothing else",
  );

  // (b) THE ARCHIVE NAMES NONE AND THE INPUT IS BLANK -> no key at all, and
  // never `null`. `secretRef` is an OBJECT in a structural schema: a literal
  // null is a 422 over a field the operator deliberately left empty.
  const blank = initialState(
    "logweir-t27",
    fixture("wizard-clusters.json"),
    fixture("wizard-backups-nocredential.json"),
  );
  assert.equal(blank.archiveSecretName, "", "an archive with no secretRef prefills empty");
  const blankApi = recordingApi();
  await submitRestore(blank, blankApi);
  const blankArchive = blankApi.calls[0].body.spec.sourceArchive;
  assert.deepEqual(
    Object.keys(blankArchive),
    ["url"],
    "the key is ABSENT, not present and null; the submitted sourceArchive was " +
      JSON.stringify(blankArchive),
  );
  assert.equal(
    Object.prototype.hasOwnProperty.call(blankArchive, "secretRef"),
    false,
    "`secretRef: null` would be a type error the API server reports as a 422",
  );

  // (c) THE OPERATOR TYPES ONE -> that name is what is sent.
  const typed = initialState(
    "logweir-t27",
    fixture("wizard-clusters.json"),
    fixture("wizard-backups-nocredential.json"),
  );
  typed.archiveSecretName = "logweir-s3-readonly";
  const typedApi = recordingApi();
  await submitRestore(typed, typedApi);
  assert.equal(
    typedApi.calls[0].body.spec.sourceArchive.secretRef.name,
    "logweir-s3-readonly",
    "the name the operator gave, verbatim",
  );

  // AND THE PLAN BYTES DO NOT CHANGE. The credential is a `Restore` spec field
  // and reaches the runner as an environment variable the controller fills from
  // the Secret; it is not in the document an approver signs, so naming it must
  // not move the hash the approval binds.
  const before = await preparePlan(blank);
  const after = await preparePlan(typed);
  assert.equal(after.bytes, before.bytes, "the plan document is byte-identical either way");
  assert.equal(after.hash, before.hash, "so the approval still covers it");
});

test("the_archive_step_shows_the_credential_name_and_says_what_it_is_for", async () => {
  const state = wizardState();
  const rendered = await renderRestoreWizard(state);
  const text = decode(rendered);

  assert.ok(
    rendered.includes("<input id=\"archive-secret\" name=\"archiveSecret\" value=\"logweir-s3\">"),
    "step 1 offers the credential as an input, prefilled from the archive reference",
  );
  assert.ok(
    text.includes("ARCHIVE CREDENTIAL (Secret name)"),
    "labelled for what it is: a NAME",
  );
  assert.ok(
    text.includes(
      "The runner reads the archive with this credential: weirkeeper mounts the named " +
        "Secret's keys into the runner Job as its object-store credential, and does so only " +
        "when spec.sourceArchive.secretRef is set. A Restore created without it is ADMITTED " +
        "and then fails at the archive, not at admission",
    ),
    "and step 1 says where a Restore without it fails -- at the archive, not at admission",
  );
  assert.ok(
    text.includes("ARCHIVE CREDENTIAL</th>"),
    "the source-cluster table carries the name beside the archive URL",
  );

  // THE NAME ONLY. A fixture's Secret carries no value, and this page reads
  // none: the rendered output names the Secret and nothing that could be in it.
  assert.equal(
    rendered.indexOf("access-key"),
    -1,
    "no key material, no key NAME inside the Secret, nothing but the Secret's own name",
  );

  // An archive that names none renders an empty control rather than the word
  // `undefined`, so a blank field reads as a decision and not as a bug.
  const blank = await renderRestoreWizard(
    initialState(
      "logweir-t27",
      fixture("wizard-clusters.json"),
      fixture("wizard-backups-nocredential.json"),
    ),
  );
  assert.ok(
    blank.includes("<input id=\"archive-secret\" name=\"archiveSecret\" value=\"\">"),
    "prefilled empty when the archive names no Secret",
  );
  assert.equal(blank.indexOf("undefined"), -1);
});

// -- Task 28a: the two defects the laptop walkthrough found ------------------

test("the_target_step_lists_every_cluster_and_lets_the_runner_decide", async () => {
  // DEFECT 2, measured by Task 28 on a live cluster. `renderTargetStep` and
  // `firstTarget` listed only `KafkaCluster`s with `spec.role == "target"`, so
  // a namespace whose only cluster is `role: source` rendered an EMPTY select,
  // left `target.bootstrapServers` empty, and made `plan.js`'s grammar throw:
  //   `target.bootstrapServers is required by the runner's grammar and must
  //    not be empty`
  // — the whole page an error box. And the RUNNER has no such rule:
  // `drill/phase0_admit.rs` branches on `spec.target.mode` and puts all three
  // cluster checks (the allowlist, target != source, the marker topic) inside
  // the `Scratch` arm; `TargetMode::NewTopic` is empty, with a comment saying
  // the source cluster is exactly where a point-in-time recovery belongs.
  // `scripts/k8s-demo.sh` proves it by running: one cluster, `role: source`,
  // `mode: newTopic`, green.
  //
  // ARM 1: one cluster, `role: source`. It renders, it is preselected, and the
  // page says what the role is and is not.
  const only = initialState(
    "logweir-t28",
    fixture("wizard-clusters-source-only.json"),
    fixture("wizard-backups-schedule-running.json"),
  );
  assert.equal(
    only.targetClusterName,
    "demo",
    "the source cluster is the preselected target when nothing is labelled `role: target`",
  );
  assert.deepEqual(
    only.fields.target.bootstrapServers,
    ["host.docker.internal:9095"],
    "…so the plan document carries an address and the grammar has something to accept. This " +
      "is the value that was EMPTY before Task 28a, and an empty list is what threw",
  );
  const step4 = renderTargetStep(only);
  assert.ok(
    step4.includes("<option value=\"demo\" selected>demo (role: source)</option>"),
    "step 4 offers it, marks it selected, and prints its role beside the name: " + step4,
  );
  assert.ok(
    step4.includes(TARGET_ROLE_SENTENCE),
    "and says, in the page, that the role is a label and the runner is the gate: " + step4,
  );
  assert.equal(
    step4.indexOf("target-cluster\" name=\"targetCluster\"></select>"),
    -1,
    "the select is NOT empty, which is the shape the defect produced",
  );

  // AND THE WHOLE WIZARD RENDERS — the assertion the defect actually broke.
  const whole = await renderRestoreWizard(only);
  assert.ok(whole.includes("id=\"plan-bytes\""), "all six steps render, plan bytes included");
  assert.ok(
    planPreOf(whole).includes("bootstrap_servers"),
    "and the plan document names the target's address",
  );

  // ARM 2: two clusters, one labelled `role: target`. THAT one is preselected,
  // both are offered, and the sentence is absent because it does not apply.
  const both = initialState(
    "logweir-t27",
    fixture("wizard-clusters.json"),
    fixture("wizard-backups.json"),
  );
  assert.equal(both.targetClusterName, "orders-recovery", "a `role: target` cluster still wins");
  const step4both = renderTargetStep(both);
  assert.ok(
    step4both.includes("<option value=\"orders-prod\">orders-prod (role: source)</option>"),
    "the source cluster is offered too — the runner accepts it for `newTopic`: " + step4both,
  );
  assert.ok(
    step4both.includes(
      "<option value=\"orders-recovery\" selected>orders-recovery (role: target)</option>",
    ),
    "and the labelled one is the selected option: " + step4both,
  );
  assert.equal(
    step4both.indexOf(TARGET_ROLE_SENTENCE),
    -1,
    "the sentence is printed only when nothing is labelled",
  );

  // ARM 3: THE CREATE BODY FOLLOWS THE SELECTION, which is the only thing that
  // reaches the cluster. A page that rendered the right option and submitted a
  // different cluster would pass every assertion above.
  const chosen = initialState(
    "logweir-t27",
    fixture("wizard-clusters.json"),
    fixture("wizard-backups.json"),
  );
  chosen.targetClusterName = "orders-prod";
  chosen.fields.target.bootstrapServers = ["kafka-0.orders.svc:9093"];
  const api = recordingApi();
  await submitRestore(chosen, api);
  assert.equal(
    api.calls[0].body.spec.target.clusterRef.name,
    "orders-prod",
    "`target.clusterRef.name` is the cluster the operator picked, `role: source` included",
  );

  // ARM 4: `mode: scratch` against a cluster with no `markerTopic` WARNS and
  // does not refuse. The runner refuses — at phase 0, against the broker it
  // actually reaches — and a page that refused here would be inventing a
  // second gate over a spec field that is a statement of intent.
  const scratch = initialState(
    "logweir-t28",
    fixture("wizard-clusters-source-only.json"),
    fixture("wizard-backups-schedule-running.json"),
  );
  scratch.fields.target.mode = "scratch";
  const warned = renderTargetStep(scratch);
  assert.ok(warned.includes(SCRATCH_MARKER_WARNING), "the warning is printed: " + warned);
  assert.ok(
    warned.includes("<option value=\"demo\" selected>"),
    "…and the cluster is still offered: a warning is not a refusal",
  );
  const stillRenders = await renderRestoreWizard(scratch);
  assert.ok(stillRenders.includes("id=\"plan-bytes\""), "the plan still renders in scratch mode");
});

test("the_wizard_restores_from_the_newest_succeeded_backup_and_names_it", async () => {
  // DEFECT 3. `initialState` and `chosenBackup` both took
  // `objects[objects.length - 1]`, so a live schedule swapped the chosen set —
  // and with it `windowCovered`, the plan bytes, the plan hash and BOTH minted
  // names — on every reload. Task 28 measured three `Backup` objects in six
  // minutes. Worse, the last row is usually the one still RUNNING, which has
  // no `backupId` and no `windowCovered` at all.
  //
  // ARM 1: a running backup listed AFTER two succeeded ones. The newest
  // SUCCEEDED one is chosen, and the page names it.
  //
  // Step 2 is asserted FIRST and off a bare state, because it is the assertion
  // that still speaks when the choice is wrong: `initialState` on the running
  // object THROWS out of `defaultTopicPrefix` (there is no covered window to
  // derive an instant from), and a row whose first clause is a TypeError says
  // less about which object was picked than one that names it.
  const backups = fixture("wizard-backups-schedule-running.json");
  const step2 = renderBackupSetStep({ backups: backups, fields: {} });
  assert.ok(
    step2.includes("chosen: logweir-backup-laptop-20260911-183600"),
    "step 2 names the object it chose, and it is the 18:36 run - Succeeded, and the later of " +
      "the two completions. The 18:38 object is LAST in the list and still Running, so it has " +
      "no backup set at all: " + step2,
  );

  const state = initialState(
    "logweir-t28",
    fixture("wizard-clusters-source-only.json"),
    backups,
  );
  assert.equal(
    state.fields.backupSetRef,
    "01M28ZBBBBBBBBBBBBBBBBBBBB",
    "and the wizard's own state carries that run's set; taking the last row leaves " +
      "`backupSetRef` undefined and the runner's grammar refuses the document",
  );
  assert.equal(
    state.fields.pointInTime,
    "2026-09-11T18:36:00Z",
    "…and the point in time is THAT run's covered `toMs`, not the running one's absent window",
  );
  assert.ok(
    renderBackupSetStep(state).includes("chosen: logweir-backup-laptop-20260911-183600"),
    "…and the two agree: the state's chosen set and step 2's named object are one decision",
  );
  assert.ok(
    step2.includes("backup set 01M28ZBBBBBBBBBBBBBBBBBBBB"),
    "…and its backup set: " + step2,
  );
  assert.ok(
    step2.includes("logweir-backup-laptop-20260911-183600 (chosen)"),
    "…and marks the row, so the table and the sentence cannot disagree: " + step2,
  );
  assert.ok(
    step2.includes(RELOAD_SENTENCE),
    "…and says the choice can move under a running schedule: " + step2,
  );
  assert.ok(
    step2.includes("PHASE</th>"),
    "the table carries the phase, so a reader can see which rows were candidates",
  );

  // ARM 2: THE LATER COMPLETION WINS BETWEEN TWO SUCCEEDED RUNS, and it is the
  // `Complete` condition's transition time that decides — not list order and
  // not the covered window. Reversing the two completion times reverses the
  // choice while the list order stays exactly as it was.
  const reversed = fixture("wizard-backups-schedule-running.json");
  reversed.items[0].status.conditions[0].lastTransitionTime = "2026-09-11T18:37:10Z";
  const later = initialState(
    "logweir-t28",
    fixture("wizard-clusters-source-only.json"),
    reversed,
  );
  assert.equal(
    later.fields.backupSetRef,
    "01M28ZAAAAAAAAAAAAAAAAAAAA",
    "the FIRST-listed run now completed last, so it is the one chosen. A page reading list " +
      "order would still answer `…BBBB` here",
  );

  // ARM 3: NO SUCCEEDED RUN AT ALL. Step 2 falls back to the last listed row —
  // the object the page used to take unconditionally — and SAYS it did.
  //
  // Step 2 is reached directly here rather than through `initialState`,
  // because a namespace in which nothing has completed has no covered window
  // to default a point in time from and therefore no plan to render at all;
  // that is a true statement about the cluster and not a defect of this page,
  // and the row is about what step 2 says.
  const none = fixture("wizard-backups-schedule-running.json");
  for (const item of none.items) {
    item.status = { phase: "Running" };
  }
  const step2none = renderBackupSetStep({ backups: none, fields: {} });
  assert.ok(
    step2none.includes(NO_SUCCEEDED_SENTENCE),
    "the fallback is stated in the page: " + step2none,
  );
  assert.ok(
    step2none.includes("chosen: logweir-backup-laptop-20260911-183800"),
    "…and it is the last listed object, which is what the page used to take unconditionally",
  );
  assert.equal(
    step2none.indexOf(RELOAD_SENTENCE),
    -1,
    "the reload sentence belongs to a chosen SUCCEEDED run and is not printed here",
  );
});

test("the_wizard_renders_a_sentence_and_step_2_when_no_backup_has_completed", () => {
  // Task 28a's review: with no Succeeded Backup the mount threw inside the
  // plan renderer and the page was an error box, pre-existing at 352a4b8.
  const list = fixture("wizard-backups.json");
  assert.ok(completedBackups(list).length >= 1, "the fixture has a completed run (control)");
  const running = JSON.parse(JSON.stringify(list));
  for (const item of running.items) {
    item.status.phase = "Running";
    delete item.status.backupId;
  }
  assert.equal(completedBackups(running).length, 0, "a Running-only list has no completed run");
  const page = renderNoCompletedBackup("ns", running);
  assert.ok(page.includes(NO_COMPLETED_BACKUP_SENTENCE), "the sentence is printed: " + page);
  assert.ok(page.includes("<h3>2. Backup set</h3>"), "and step 2's table follows it");
  assert.equal(page.includes("<h3>6."), false, "and no later step is rendered");
  const empty = renderNoCompletedBackup("ns", { items: [] });
  assert.ok(empty.includes(NO_COMPLETED_BACKUP_SENTENCE), "an empty list renders too: " + empty);
  const stale = JSON.parse(JSON.stringify(list));
  for (const item of stale.items) {
    delete item.status.backupId;
  }
  assert.equal(
    completedBackups(stale).length,
    0,
    "a Succeeded run without a backupId (written before Task 28a) is not a completed set",
  );
});
