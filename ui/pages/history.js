// pages/history.js -- Restores and Backups interleaved by creation time, and
// the Restore detail view.
//
// THE RESTORE HALF OF THE BADGE RULE lives here: green requires
// `evidence.verification.result == "Valid"` (on a basis `validVerification`
// admits) AND `status.outcome === "pass"` AND no recorded non-zero `status.exitCode`
// (an exit-2 run publishes a signed failure since interface I8's amendment). Only `pass` is green. A `fail-integrity` run produced a perfectly valid,
// perfectly signed document SAYING THE RESTORE DID NOT RECONCILE, and a green
// badge over it would invert the single most valuable thing this product
// reports. The verification half is shared with the Backup rule and imported
// from `./backups.js`; the second condition is not shared, because the two
// kinds do not carry the same field.
//
// THREE ROWS THE FROZEN `drill show` TABLE OMITS ARE RENDERED HERE, BECAUSE A
// PAGE THAT OMITS THEM MAKES A DIFFERENT CLAIM FROM THE TRUTH.
//
//   `engine sub-report` -- `engine_subreport` is null in EVERY scorecard this
//   product writes, because the engine adapter never overrides the validation
//   -run hook. Omitting the row renders "no caveat"; the row says "not
//   produced by this engine version", which is what actually happened.
//
//   `integrity.partialReason` -- a `partial` integrity result with no reason
//   beside it is a verdict an auditor cannot act on.
//
//   `objectives` -- what was ASKED FOR. `measured` alone says what happened
//   without saying whether it was enough.

//
// THE RESTORE DETAIL IS THE DURABLE OPERATION VIEW (PLAT-12.1). The restore
// wizard's guided submit ends here when an Approval already authorises the
// Restore it created, so the view opens with where that operation stands --
// its recorded progress, the plan hash of its own bytes, and its Approval's
// state -- read from the objects themselves and never remembered by the page.
// A refresh reads them again and creates nothing.

import { apiClient } from "../client.js";
import { active, cancelled, readOptions } from "../lifecycle.js";
import {
  ENGINE_SUBREPORT_LINE,
  badge,
  bucketOf,
  cell,
  detailLink,
  errorBox,
  esc,
  evidenceBlock,
  facts,
  independentCheck,
  listFooter,
  phaseBadge,
  planEvidenceBucket,
  replace,
  SCOPE_LEVEL_OF_INTEGRITY,
  table,
  unverifiedCaption,
  verificationScopeSentence,
  scorecardClaim,
  SCORECARD_CLAIM_SENTENCE,
  summaryBadge,
} from "../render.js";
import { planHash } from "../plan.js";
import { itemsOf } from "./clusters.js";
import { backupBadge, greenLabel, operationCell, validVerification } from "./backups.js";
import { operationRoute } from "./operation.js";
import { isRecoveryPoint, restorePointRoute } from "./restore-wizard.js";
import {
  approvalState,
  approvalSubjectRoute,
  renderApprovalState,
  restoreProgressSentence,
  subjectOf,
} from "./approvals.js";

const PLURAL = "restores";
const BACKUPS = "backups";
const APPROVALS = "approvals";

const API = apiClient();

/** The sentence the history table carries when the namespace holds no run. */
export const NO_HISTORY_SENTENCE =
  "No run in this namespace yet. Backups and Restores appear here, newest first, as soon " +
  "as they exist.";

/** **The Restore badge rule.** Green if and only if the recorded verification
 *  is `Valid`, its trust basis is one a green badge may carry, AND the run's
 *  own outcome is `pass`, AND no recorded `exitCode` other than 0 -- the
 *  controller's own rule (`weirkeeper::verification::restore_badge`): since
 *  interface I8's amendment an exit-2 run publishes a signed failure that
 *  verifies Valid, and the exit code stays authoritative for success. An
 *  absent `exitCode` is judged on the outcome, as the controller does.
 *
 *  THE NOT-GREEN CAPTION NAMES ITS CASE, exactly as the Backup rule's does.
 *  `Untrusted` means the bytes are authentic and this installation does not
 *  accept the key; `NotAttempted` means the controller could not check at all;
 *  `Invalid` means the document itself did not verify. Three different repairs
 *  behind one word was the defect D3 section 7.4 names, and the word is kept while
 *  the case is added beside it. */
export function restoreBadge(status) {
  const s = status || {};
  if (((s.evidence || {}).verification) === undefined && s.__summary !== undefined) {
    return summaryBadge(s.__summary);
  }
  const verification = ((s.evidence || {}).verification) || {};
  const verified = validVerification(s);
  const succeeded = s.outcome === "pass" && (s.exitCode === undefined || s.exitCode === 0);
  if (verified === null || !succeeded) {
    return badge("unverified", unverifiedCaption(verification, succeeded));
  }
  return badge("green", greenLabel(verified[0], verified[1], verified[2]));
}

/** HOW MUCH OF ONE RESTORE WAS COMPARED, from whichever document this page
 *  is looking at.
 *
 *  `status.verificationScope` is the product API's own block and is used
 *  verbatim when it is there. A custom resource carries the same facts under
 *  two other names -- `status.integrity.level` and `status.completion`'s
 *  counts -- and they are read through D3 section 2.5's own table
 *  ([`SCOPE_LEVEL_OF_INTEGRITY`]), which is a vocabulary map and not a verdict:
 *  the level is whatever the runner recorded, translated, never inferred from
 *  a count or from a pass.
 *
 *  A LEVEL THIS TABLE DOES NOT KNOW PRODUCES NO SCOPE AT ALL, and the sentence
 *  for an absent scope says how much was compared is not known here -- which is
 *  true, and is not "complete". */
export function scopeOf(object) {
  const status = (object && object.status) || {};
  if (status.verificationScope !== null && status.verificationScope !== undefined) {
    return status.verificationScope;
  }
  const level = SCOPE_LEVEL_OF_INTEGRITY[String((status.integrity || {}).level)];
  if (level === undefined) {
    return null;
  }
  const completion = status.completion || {};
  return {
    level: level,
    recordsSampled: completion.recordsSampled,
    recordsSampledMatching: completion.recordsSampledMatching,
    recordsExpected: completion.recordsExpected,
  };
}

/** THE LINK A ROW OF EITHER KIND CARRIES TO THE DURABLE OPERATION VIEW.
 *
 *  Two kinds, two operation kinds, one route. The uid travels with the name so
 *  a link about a run that was deleted and recreated lands on a refusal rather
 *  than on a different run's evidence. */
export function rowOperationCell(object, ns) {
  const meta = (object && object.metadata) || {};
  if (typeof meta.name !== "string" || meta.name.length === 0) {
    return cell(null);
  }
  const kind = kindOf(object) === "Backup" ? "backup" : "restore";
  const target = operationRoute(ns || meta.namespace || "", kind, meta.name, meta.uid || "");
  return "<a href=\"" + esc(target) + "\">Follow this run</a>";
}

function nameOf(object) {
  const meta = (object && object.metadata) || {};
  return cell(meta.name);
}

/** The name as a link to the detail view FOR THAT KIND: a Backup's own view,
 *  or this page's Restore view. Two kinds, two destinations. */
function nameCell(object, ns) {
  const meta = (object && object.metadata) || {};
  if (typeof meta.name !== "string" || meta.name.length === 0) {
    return cell(null);
  }
  const route = kindOf(object) === "Backup" ? "backups" : "history";
  return detailLink(route, ns || (meta.namespace || "default"), meta.name);
}

function kindOf(object) {
  const kind = (object && object.kind) || "";
  return kind === "" ? "-" : kind;
}

function createdAt(object) {
  const meta = (object && object.metadata) || {};
  return typeof meta.creationTimestamp === "string" ? meta.creationTimestamp : "";
}

/** The run's own result, per kind: a `Restore`'s `outcome`, a `Backup`'s exit
 *  code. Two kinds, two fields, and neither reads the other's. */
function resultCell(object) {
  const status = (object && object.status) || {};
  return kindOf(object) === "Backup"
    ? cell(status.exitCode)
    : scorecardClaim(cell(status.outcome), validVerification(status) !== null);
}

/** The badge for a row, per kind. */
function rowBadge(object) {
  const status = (object && object.status) || {};
  return kindOf(object) === "Backup" ? backupBadge(status) : restoreBadge(status);
}

/** THE LINK THAT CARRIES A RECOVERY POINT'S IDENTITY INTO THE WIZARD
 *  (PLAT-11.1).
 *
 *  Only a `Backup` that IS a recovery point gets one -- Succeeded, with a
 *  backup set and a covered window -- because the wizard has nothing to build
 *  a plan from otherwise, and a link that opened a refusal would be this page
 *  offering an action it knows cannot work. The `Restore` rows get none: a
 *  restore is not a point to restore from.
 *
 *  BOTH HALVES OF THE IDENTITY TRAVEL. `restorePointRoute` spells the UID
 *  beside the name, and the UID is what the wizard resolves by, so a row
 *  clicked after the object was deleted and recreated under the same name ends
 *  in a refusal naming the point that is gone rather than in a plan over a
 *  different run. */
export function restorePointCell(object, ns) {
  if (kindOf(object) !== "Backup" || !isRecoveryPoint(object)) {
    return cell(null);
  }
  return (
    "<a href=\"" + esc(restorePointRoute(ns, object)) + "\">Restore this point</a>"
  );
}

/** Restores and Backups interleaved, newest first.
 *
 *  Accepts one collection, an array, or two collections
 *  (`renderHistoryList(restores, backups)`). Objects with no creation instant
 *  sort last, in the order they arrived: a missing timestamp is not a reason
 *  to invent one. */
export function renderHistoryList(input, second, ns) {
  const objects = itemsOf(input).concat(second === undefined ? [] : itemsOf(second));
  const ordered = objects
    .map((object, index) => ({ object: object, index: index }))
    .sort((a, b) => {
      const left = createdAt(a.object);
      const right = createdAt(b.object);
      if (left === right) {
        return a.index - b.index;
      }
      if (left === "") {
        return 1;
      }
      if (right === "") {
        return -1;
      }
      return left < right ? 1 : -1;
    })
    .map((entry) => entry.object);

  const rows = ordered.map((object) => [
    nameCell(object, ns),
    esc(kindOf(object)),
    cell(createdAt(object)),
    phaseBadge((object.status || {}).phase),
    resultCell(object),
    rowBadge(object),
    rowOperationCell(object, ns),
    restorePointCell(object, ns),
  ]);

  return (
    "<h2>History</h2>" +
    "<p class=\"blurb\">Completed runs of both kinds, newest first. A Backup's result " +
    "is its exit code; a Restore's is its outcome. The two kinds do not share a badge " +
    "rule, because they do not share a field. A completed Backup carries a link that opens " +
    "the restore wizard on THAT recovery point, by uid.</p>" +
    table(
      ["NAME", "KIND", "CREATED", "PHASE", "RESULT", "SIGNED", "OPERATION", "RESTORE"],
      rows,
      NO_HISTORY_SENTENCE,
      undefined,
      { id: "history", label: "runs", scope: ns },
    ) +
    listFooter()
  );
}

/** Where one Restore's operation stands: its progress as weirkeeper recorded
 *  it, the hash of its own plan bytes, and its Approval's state -- with the way
 *  to its approval page while it is not approved. `operation` is what
 *  `loadRestoreOperation` read: `{ns, subject, approval, found, approvalError}`. */
export function renderRestoreOperation(object, operation) {
  const o = operation || {};
  const s = o.subject || {};
  const approval = o.approvalError
    ? "<p class=\"complaint\">Approval " + esc(s.approvalName) + " could not be read, so its " +
      "state is unknown: " + esc(o.approvalError.status ? String(o.approvalError.status) + " " +
        String(o.approvalError.reason || "") : "error") + " " + esc(o.approvalError.message) + "</p>"
    : renderApprovalState(o.found, s, s.approvalName);
  const verified = ((o.found || {}).state) === "verified";
  return (
    "<section class=\"operation\" id=\"restore-operation\"><h3>Operation</h3>" +
    facts([
      ["progress", restoreProgressSentence(object)],
      ["uid", "<code>" + esc(s.uid) + "</code>"],
      ["plan hash", "<code>" + esc(s.planHash) + "</code>"],
      ["Approval", "<code>" + esc(s.approvalName) + "</code>"],
    ]) +
    approval +
    (verified || !s.name
      ? ""
      : "<p class=\"note\"><a href=\"" + esc(approvalSubjectRoute(o.ns, s.name)) + "\">Open " +
        "the approval page for Restore " + esc(s.name) + "</a></p>") +
    "</section>"
  );
}

/** One Restore, in full. With `operation`, the view opens with where the
 *  operation stands (see [`renderRestoreOperation`]). */
export function renderRestoreDetail(object, operation) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const evidence = status.evidence || {};
  const integrity = status.integrity || {};
  const objectives = status.objectives || {};
  const measured = status.measured || {};
  const preflight = status.topicPreflight || {};
  const newTopics = Array.isArray(status.newTopics) ? status.newTopics : [];
  const oldTopics = Array.isArray(status.oldTopics) ? status.oldTopics : [];
  // THE SCORECARD'S FACTS ARE ITS CLAIM UNTIL IT VERIFIED -- by the same rule
  // the verdict badge uses (`validVerification`).
  const verified = validVerification(status) !== null;
  const claim = (value) => scorecardClaim(cell(value), verified);

  return (
    "<h2>Restore " + nameOf(object) + "</h2>" +
    restoreBadge(status) +
    (operation ? renderRestoreOperation(object, operation) : "") +
    facts([
      ["phase", phaseBadge(status.phase)],
      ["exit code", cell(status.exitCode)],
      ["reason", cell(status.reason)],
      ["last phase completed", cell(status.lastPhaseCompleted)],
      ["outcome", claim(status.outcome)],
      ["target mode", cell((spec.target || {}).mode)],
      ["operation", rowOperationCell(object, (object.metadata || {}).namespace)],
      ["point in time", cell(spec.pointInTime)],
      ["backup set", cell(spec.backupSetRef)],
    ]) +
    (verified
      ? ""
      : "<p class=\"caveat\" data-scorecard-claim=\"note\">" + esc(SCORECARD_CLAIM_SENTENCE) +
        "</p>") +
    "<h3>Integrity</h3>" +
    facts([
      ["level", claim(integrity.level)],
      ["result", claim(integrity.result)],
      ["partial reason", claim(integrity.partialReason)],
    ]) +
    // HOW MUCH OF THIS RESTORE WAS ACTUALLY COMPARED, BESIDE THE RESULT AND
    // NEVER AWAY FROM IT (D3 section 2.5, section 3.5). A pass is a pass over a SAMPLE, and
    // a result printed without its scope reads as an exhaustive comparison --
    // which no level in v1 performs and none is called `complete`.
    "<p class=\"scope\">" + esc(verificationScopeSentence(scopeOf(object))) + "</p>" +
    "<h3>Objectives asked for, and what the run achieved</h3>" +
    facts([
      ["objectives.rtoSeconds", cell(objectives.rtoSeconds)],
      ["objectives.rpoSeconds", cell(objectives.rpoSeconds)],
      ["objectives.passRate", cell(objectives.passRate)],
      ["objectives.met", claim(objectives.met)],
      ["measured.rtoSeconds", claim(measured.rtoSeconds)],
      ["measured.rpoSeconds", claim(measured.rpoSeconds)],
    ]) +
    "<h3>Target topic preflight</h3>" +
    facts([
      ["timestampType", cell(preflight.timestampType)],
      ["retentionMs", cell(preflight.retentionMs)],
      ["timestampBound", cell(preflight.timestampBound)],
    ]) +
    "<h3>Topics</h3>" +
    facts([
      ["new topics", newTopics.length === 0 ? cell(null) : esc(newTopics.join(", "))],
      ["old topics -- written to by nothing, in any tag",
        oldTopics.length === 0 ? cell(null) : esc(oldTopics.join(", "))],
    ]) +
    evidenceBlock(evidence) +
    "<p class=\"engine-subreport\">" + ENGINE_SUBREPORT_LINE + "</p>" +
    "<section class=\"check\"><h3>Check it yourself</h3>" +
    "<p class=\"note\">The two keys above name objects in the evidence bucket this " +
    "restore's approved plan wrote to; the verifiers take local files. So the first two " +
    "lines fetch, and the last two verify -- once with the Rust reader and once with the " +
    "Python one.</p>" +
    independentCheck(
      "scorecard",
      // THE PLAN'S EVIDENCE BUCKET, NEVER THE SOURCE ARCHIVE'S (PoC P5's class
      // sweep): the scorecard is where the approved plan wrote it.
      planEvidenceBucket(spec.planBytes) || bucketOf(""),
      evidence.scorecardKey || "",
      evidence.sidecarKey || "",
      "scorecard.json",
      "scorecard.sig",
    ) +
    "</section>"
  );
}

// --------------------------------------------------------------- mount half

export async function mountHistory(node, ns, parse, lifecycle) {
  try {
    const collections = await Promise.all([
      API.list(ns, PLURAL, readOptions(lifecycle)),
      API.list(ns, BACKUPS, readOptions(lifecycle)),
    ]);
    if (active(lifecycle)) {
      replace(node, parse(renderHistoryList(collections[0], collections[1], ns)));
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** Reads what the operation view needs beside the Restore: the hash of its own
 *  plan bytes and the Approval its `spec.approvalRef` names. A missing
 *  Approval is a state; an unreadable one is reported, not guessed. */
export async function loadRestoreOperation(api, ns, restore, lifecycle) {
  const hash = await planHash((((restore || {}).spec) || {}).planBytes || "");
  const subject = subjectOf(restore, hash, ns);
  let approval = null;
  let approvalError = null;
  if (subject.approvalName.length > 0) {
    try {
      approval = await api.get(ns, APPROVALS, subject.approvalName, readOptions(lifecycle));
    } catch (error) {
      if (cancelled(error, lifecycle)) {
        throw error;
      }
      if (!(error !== null && typeof error === "object" && error.status === 404)) {
        approvalError = error;
      }
    }
  }
  return {
    ns: ns,
    subject: subject,
    approval: approval,
    found: approvalError === null ? approvalState(approval, subject) : null,
    approvalError: approvalError,
  };
}

export async function mountRestoreDetail(node, ns, name, parse, lifecycle, deps) {
  const api = deps || API;
  try {
    const object = await api.get(ns, PLURAL, name, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return;
    }
    const operation = await loadRestoreOperation(api, ns, object, lifecycle);
    if (active(lifecycle)) {
      replace(node, parse(renderRestoreDetail(object, operation)));
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}
