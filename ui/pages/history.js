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

import { CONSOLE, apiClient, granted } from "../client.js";
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
  planEvidenceBucket,
  planTimeBasis,
  runPhaseBadge,
  LEFT_TOPIC_SENTENCE,
  UNCONFIRMED_TOPIC_SENTENCE,
  UNCONFIRMED_UNLISTED_TOPIC_SENTENCE,
  QUEUED_RUN_SENTENCE,
  replace,
  restorePointLink,
  SCOPE_LEVEL_OF_INTEGRITY,
  UNVERIFIED,
  badge as captionBadge,
  coverageWords,
  notCovered,
  renderCompleteCoverage,
  selectionIn,
  selectionWords,
  COMPLETE_COVERAGE_COST,
  messageText,
  table,
  unverifiedCaption,
  verificationScopeSentence,
  scorecardClaim,
  SCORECARD_CLAIM_SENTENCE,
  summaryBadge,
  listVerifiedNote,
  when,
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
  // PROD-08.1a: A COMPLETE VERIFICATION THAT DID NOT COVER THE RESTORE IS NEVER
  // GREEN, whatever else the status says -- the controller's own rule
  // (`CompleteNotCovered`), applied here too so no row or detail can read it
  // as a pass, in either mode.
  if (notCovered(completeOf(s))) {
    return captionBadge("unverified", UNVERIFIED + ": " + NOT_COVERED_CAPTION);
  }
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

/** The caption a Restore badge carries over a complete verification that did
 *  not cover the restore (PROD-08.1a). */
export const NOT_COVERED_CAPTION =
  "complete coverage did not cover this restore -- not a pass";

/** PROD-08.1a: the recorded complete block, from whichever document this page
 *  is looking at -- the product API's full `verificationScope.complete` on a
 *  console detail, else the custom resource's (or a console list row's)
 *  `status.integrity.complete`. `null` when none is recorded. */
export function completeOf(status) {
  const s = status || {};
  const scope = s.verificationScope || null;
  if (scope !== null && typeof scope === "object" && scope.complete) {
    return scope.complete;
  }
  const integrity = s.integrity || {};
  return integrity.complete && typeof integrity.complete === "object" ? integrity.complete : null;
}

/** PROD-08.1a: one Restore's coverage -- what it ASKS for (`spec.coverage`,
 *  absent read as sampled; the controller has held it to the plan) and what
 *  its signed scorecard RECORDED (`status.integrity.coverage`, or the API's
 *  `verificationScope.coverage`; absent is not recorded, never complete). */
export function coverageOf(object) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const scope = status.verificationScope || {};
  const recorded = typeof scope.coverage === "string"
    ? scope.coverage
    : (status.integrity || {}).coverage;
  return {
    requested: spec.coverage === "complete" ? "complete" : "sampled",
    maxRecords: typeof spec.completeMaxRecords === "number" ? spec.completeMaxRecords : null,
    recorded: recorded === "sampled" || recorded === "complete" ? recorded : null,
    complete: completeOf(status),
  };
}

/** PROD-11.1b: one Restore's signed selection, from whichever document this
 *  page is looking at -- the product API's `verificationScope.selection` on a
 *  console detail, else the custom resource's (or a console list row's)
 *  `status.integrity.selection`. `null` when none is recorded: every partition
 *  of every restored topic, from the archive's floor. */
export function selectionOf(object) {
  const status = (object && object.status) || {};
  return selectionIn(status.verificationScope || {}) || selectionIn(status.integrity || {});
}

/** The list row's coverage line, under the RESULT -- and, for a narrowed
 *  restore, the line that says so (PROD-11.1b). */
function coverageLine(object) {
  const c = coverageOf(object);
  const narrowed = selectionWords(selectionOf(object));
  return "<span class=\"cell-sub\" data-coverage=\"" + esc(c.recorded || "not-recorded") +
    "\"" + (notCovered(c.complete) ? " data-covered=\"false\"" : "") + ">coverage: " +
    esc(coverageWords(c.recorded, c.requested, c.complete)) + "</span>" +
    (narrowed === ""
      ? ""
      : "<span class=\"cell-sub\" data-selection=\"partial\">" + esc(narrowed) + "</span>");
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
  // A CONSOLE PROJECTION'S SCOPE IS THE API'S OWN BLOCK, OR NONE (FX-48). The
  // table below is the custom resource's vocabulary, for the mode that has no
  // `logweir-api` to do the mapping. A console detail now carries the
  // scorecard's integrity level too (`status.integrity.level`, from the
  // operation route's `completion`), and reading a scope out of it here would
  // be this page computing what the API is there to compute -- and printing a
  // sampled-check sentence with no counts for a run whose route published no
  // scope at all.
  if (((object || {}).__contract || {}).mode === CONSOLE) {
    return null;
  }
  const level = SCOPE_LEVEL_OF_INTEGRITY[String((status.integrity || {}).level)];
  if (level === undefined) {
    return null;
  }
  const completion = status.completion || {};
  const integrity = status.integrity || {};
  return {
    level: level,
    recordsSampled: completion.recordsSampled,
    recordsSampledMatching: completion.recordsSampledMatching,
    recordsExpected: completion.recordsExpected,
    // PROD-08.1a / FX-23: the custom resource's copies of the signed facts,
    // under the API's names.
    coverage: integrity.coverage,
    complete: integrity.complete,
    unsampledTopics: integrity.unsampledTopics,
    selection: integrity.selection,
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
  return "<a class=\"action\" href=\"" + esc(target) + "\">Follow this run</a>";
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
    : scorecardClaim(cell(status.outcome), validVerification(status) !== null ||
      listRowVerified(status)) + coverageLine(object);
}

// A CONSOLE LIST ROW'S OWN GREEN (MCP-17): it carries no recorded verification
// block, only the API's summary, whose `verifiedSuccess` is the controller's
// badge rule computed server side -- the same word the SIGNED column reads. A
// row whose SIGNED cell says verified must not call its outcome unverified.
function listRowVerified(status) {
  const s = status || {};
  return ((s.evidence || {}).verification) === undefined && s.__summary !== undefined &&
    s.__summary.verifiedSuccess === true && s.__summary.verificationState === "valid" &&
    !notCovered(completeOf(s));
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
  // A ROLE THAT CANNOT RESTORE HERE IS TOLD WHO CAN (MCP round 3, R3-2).
  return restorePointLink(restorePointRoute(ns, object), granted(ns, "restoreCreate"));
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

  // The run's own page and its operation view in one cell, as on Backups: one
  // column fewer, so RESTORE -- the row's action -- stays on screen.
  const rows = ordered.map((object) => {
    const follow = rowOperationCell(object, ns);
    return [
      nameCell(object, ns) + (follow === cell(null) ? "" : "<span class=\"cell-sub\">" + follow + "</span>"),
      // THE ROW'S ACTION BESIDE ITS NAME (MCP-25's rule): as the last column
      // "Restore this point" was what a 1024 px window scrolled out of sight.
      restorePointCell(object, ns),
      esc(kindOf(object)),
      when(createdAt(object)),
      runPhaseBadge(object.status),
      resultCell(object),
      rowBadge(object),
    ];
  });

  return (
    "<h2>History</h2>" +
    "<p class=\"blurb\">Completed runs of both kinds, newest first. A Backup's result " +
    "is its exit code; a Restore's is its outcome. The two kinds do not share a badge " +
    "rule, because they do not share a field. A completed Backup carries a link that opens " +
    "the restore wizard on THAT recovery point, by uid.</p>" +
    table(
      ["NAME", "RESTORE", "KIND", "CREATED", "PHASE", "RESULT", "SIGNED"],
      rows,
      NO_HISTORY_SENTENCE,
      undefined,
      { id: "history", label: "runs", scope: ns },
    ) +
    listVerifiedNote(ordered) +
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

/** The signed time basis (FX-8 review M-2), in words: `status.timeBasis` --
 *  the scorecard's `source.time_basis` as the controller copied it. ABSENT is
 *  not recorded and is never read as "every selection used the topics' own
 *  clocks"; two empty lists are the run's claim, as far as the archive
 *  manifest's segment bounds show. */
export function signedTimeBasisText(timeBasis) {
  if (timeBasis === null || timeBasis === undefined || typeof timeBasis !== "object") {
    return "not recorded: this run carries no signed time basis (a scorecard before format " +
      "1.3.0, a refused run, or one whose scorecard has not been read)";
  }
  const producer = Array.isArray(timeBasis.producerTime) ? timeBasis.producerTime : [];
  const unknown = Array.isArray(timeBasis.notRecorded) ? timeBasis.notRecorded : [];
  const parts = [];
  if (producer.length > 0) {
    parts.push("selected by producer time (the plan accepted it): " + producer.join(", "));
  }
  if (unknown.length > 0) {
    parts.push("selected by time with the timestamp type NOT RECORDED: " + unknown.join(", "));
  }
  return parts.length > 0
    ? parts.join("; ")
    : "no topic selected by producer time or with an unrecorded timestamp type, as far as " +
      "the archive manifest's segment bounds show";
}

/** How many names a list of a stopped creation step has BEYOND the ones it
 *  shows: its count (`<list>Count`, how many the runner named) less its
 *  length. 0 when the status carries no count, a count that is not a whole
 *  number, or one smaller than the list. */
export function creationStopMore(stopped, key) {
  const list = Array.isArray(stopped[key]) ? stopped[key] : [];
  const count = stopped[key + "Count"];
  return Number.isInteger(count) && count > list.length ? count - list.length : 0;
}

/** What a Restore whose creation step stopped left on the target cluster
 *  (PROD-15.1), from `status.targetTopicsAppeared`: the mapped names someone
 *  else created while the restore was admitted (never written to), EVERY
 *  topic this restore created and left, empty, with what to do about it, and
 *  every name it asked for and CANNOT ACCOUNT FOR, with its own sentence
 *  (review 2, M2) -- never called this restore's. A list the 100-name bound
 *  cut says how many more there are. Logweir deletes none of them, so the
 *  operator must be told they are there. Empty when the status carries no
 *  such block. Every name is escaped, in all three lists. */
export function creationStoppedWarning(stopped) {
  if (stopped === null || stopped === undefined || typeof stopped !== "object") {
    return "";
  }
  const names = (list) => (Array.isArray(list) ? list.filter((n) => typeof n === "string") : []);
  const appeared = names(stopped.appeared);
  const left = names(stopped.left);
  const unconfirmed = names(stopped.unconfirmed);
  const more = (key) => {
    const n = creationStopMore(stopped, key);
    return n === 0 ? "" : " and " + n + " more";
  };
  const moreItem = (key) => {
    const n = creationStopMore(stopped, key);
    return n === 0 ? "" : "<li>" + esc("and " + n + " more") + "</li>";
  };
  const cut = ["appeared", "left", "unconfirmed"].some((key) => creationStopMore(stopped, key) > 0);
  // "exists now" only when the status says the runner saw the names; anything
  // else, an absent flag included, is the weaker sentence.
  const unconfirmedSentence = stopped.unconfirmedSeen === true
    ? UNCONFIRMED_TOPIC_SENTENCE
    : UNCONFIRMED_UNLISTED_TOPIC_SENTENCE;
  return "<div class=\"caveat\" id=\"restore-creation-stopped\">" +
    "<p>" + esc("This restore stopped while creating its target topics, before anything " +
      "was restored.") + "</p>" +
    (appeared.length === 0
      ? ""
      : "<p id=\"restore-topics-appeared\">" + esc(appeared.join(", ") + more("appeared") +
        ": created by someone else after this restore was admitted. The restore wrote " +
        "nothing into " + (appeared.length === 1 && more("appeared") === "" ? "it." : "them.")) +
        "</p>") +
    (left.length === 0
      ? "<p id=\"restore-topics-left\">" + esc(unconfirmed.length === 0
        ? "This restore created no topic."
        : "No CreateTopics answer says this restore created a topic.") + "</p>"
      : "<ul id=\"restore-topics-left\">" + left.map((name) =>
        "<li><strong>" + esc(name) + "</strong>" + esc(": " + LEFT_TOPIC_SENTENCE + ".") +
        "</li>").join("") + moreItem("left") + "</ul>") +
    (unconfirmed.length === 0
      ? ""
      : "<ul id=\"restore-topics-unconfirmed\">" + unconfirmed.map((name) =>
        "<li><strong>" + esc(name) + "</strong>" + esc(": " + unconfirmedSentence + ".") +
        "</li>").join("") + moreItem("unconfirmed") + "</ul>") +
    (left.length === 0 && unconfirmed.length === 0
      ? ""
      : "<p>" + esc("Logweir never deletes a topic under a name it may not own: a producer " +
        "could write to it between any check and the delete.") + "</p>") +
    (cut
      ? "<p id=\"restore-topics-cut\">" + esc("A list shows its first 100 names. Each name " +
        "is one of this restore's mapped target topics, and the runner's log names every " +
        "one.") + "</p>"
      : "") +
    "</div>";
}

/** The warning a Restore detail carries when its signed time basis names a
 *  topic whose timestamp type was not recorded (FX-8 review M-2): the clock
 *  that topic's point in time was read on is unknown. Empty otherwise. */
export function unrecordedTimeBasisWarning(timeBasis) {
  const unknown = timeBasis && Array.isArray(timeBasis.notRecorded) ? timeBasis.notRecorded : [];
  if (unknown.length === 0) {
    return "";
  }
  return "<p class=\"caveat\" id=\"restore-time-basis-unrecorded\">" +
    esc("The timestamp type of " + unknown.join(", ") + " was not recorded when the archive " +
      "was written, so the clock this restore's point in time was read on is unknown: if a " +
      "topic is LogAppendTime, its records were selected by the producers' clocks, not by when " +
      "the broker appended them. A backup taken since FX-4, with the restore bound to its " +
      "point, records it.") + "</p>";
}

/** What a Restore detail's cell says for a value the controller keeps on the
 *  Restore object and the product API serving this console does not publish
 *  (FX-48, PoC batch 6 F-4). */
export const NOT_PUBLISHED = "not published by the product API";

/** Said once, under the Integrity table, when any cell of the view reads
 *  [`NOT_PUBLISHED`]. */
export const NOT_PUBLISHED_SENTENCE =
  "A cell that reads \"" + NOT_PUBLISHED + "\" is not an empty one. It is a value the " +
  "controller keeps on the Restore object's status and the product API serving this console " +
  "does not publish, so this page cannot say whether one is recorded. The integrity result " +
  "and partial reason, the objectives and the measured values are also in the run's signed " +
  "scorecard, which the commands under Check it yourself fetch and verify.";

/** Whether a console projection names `path`, or a block that holds it, among
 *  the fields it could not supply (`__contract.absent`, `ui/client.js`). False
 *  for a custom resource: in legacy mode an absent field is one the controller
 *  did not record, and the cell says "-" as it always has. */
export function notPublishedIn(object, path) {
  const absent = ((object || {}).__contract || {}).absent;
  if (!Array.isArray(absent)) {
    return false;
  }
  return absent.some((named) => path === named || path.indexOf(named + ".") === 0);
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
  const stopped = status.targetTopicsAppeared;
  const isStopped = stopped !== null && stopped !== undefined && typeof stopped === "object";
  const createdTopics = isStopped
    ? (Array.isArray(stopped.left) ? stopped.left.filter((n) => typeof n === "string") : [])
    : newTopics;
  const stoppedLeftMore = isStopped ? creationStopMore(stopped, "left") : 0;
  // THE SCORECARD'S FACTS ARE ITS CLAIM UNTIL IT VERIFIED -- by the same rule
  // the verdict badge uses (`validVerification`).
  const verified = validVerification(status) !== null;
  const claim = (value) => scorecardClaim(cell(value), verified);
  const coverage = coverageOf(object);
  const selection = selectionOf(object);
  // A VALUE THIS VIEW WAS NOT GIVEN IS ONE OF TWO THINGS, AND THE CELL SAYS
  // WHICH (FX-48, PoC batch 6 F-4). On a custom resource an absent field is
  // one the controller did not record: "-". On a console projection that
  // NAMES the field among what it could not supply, "-" would say the same
  // thing about a value the product API simply does not publish -- which is
  // how a shared console came to show an empty Integrity table for a restore
  // whose status read `byte-fingerprint` / `pass`. `shown` renders the value
  // when there is one and the not-published cell when the projection named
  // its absence; the sentence under the Integrity table is printed once.
  let unpublished = false;
  const shown = (path, value, render) => {
    if ((value === undefined || value === null) && notPublishedIn(object, path)) {
      unpublished = true;
      return "<span class=\"note\" data-not-published=\"" + esc(path) + "\">" +
        esc(NOT_PUBLISHED) + "</span>";
    }
    return render(value);
  };
  const integrityFacts = facts([
    ["level", shown("status.integrity.level", integrity.level, claim)],
    ["result", shown("status.integrity.result", integrity.result, claim)],
    ["partial reason", shown("status.integrity.partialReason", integrity.partialReason, claim)],
  ]);
  const objectiveFacts = facts([
    ["objectives.rtoSeconds", shown("status.objectives.rtoSeconds", objectives.rtoSeconds, cell)],
    ["objectives.rpoSeconds", shown("status.objectives.rpoSeconds", objectives.rpoSeconds, cell)],
    ["objectives.passRate", shown("status.objectives.passRate", objectives.passRate, cell)],
    ["objectives.met", shown("status.objectives.met", objectives.met, claim)],
    ["measured.rtoSeconds", shown("status.measured.rtoSeconds", measured.rtoSeconds, claim)],
    ["measured.rpoSeconds", shown("status.measured.rpoSeconds", measured.rpoSeconds, claim)],
  ]);
  const preflightFacts = facts([
    ["timestampType", shown("status.topicPreflight.timestampType", preflight.timestampType, cell)],
    ["retentionMs", shown("status.topicPreflight.retentionMs", preflight.retentionMs, cell)],
    ["timestampBound", shown("status.topicPreflight.timestampBound", preflight.timestampBound,
      cell)],
  ]);
  const oldTopicsCell = oldTopics.length === 0
    ? shown("status.oldTopics", undefined, cell)
    : esc(oldTopics.join(", "));
  const notPublishedNote = unpublished
    ? "<p class=\"note\" id=\"restore-not-published\">" + esc(NOT_PUBLISHED_SENTENCE) + "</p>"
    : "";

  return (
    "<h2>Restore " + nameOf(object) + "</h2>" +
    restoreBadge(status) +
    (status.phase === "Queued" ? "<p class=\"note queued\">" + esc(QUEUED_RUN_SENTENCE) + "</p>" : "") +
    // PROD-15.1: WHAT A STOPPED CREATION STEP LEFT ON THE CLUSTER, first, where
    // it cannot be missed: nothing else on this page says a topic exists.
    creationStoppedWarning(status.targetTopicsAppeared) +
    (operation ? renderRestoreOperation(object, operation) : "") +
    facts([
      ["phase", runPhaseBadge(status)],
      ["exit code", cell(status.exitCode)],
      ["reason", cell(status.reason)],
      ["last phase completed", cell(status.lastPhaseCompleted)],
      ["outcome", claim(status.outcome)],
      ["target mode", cell((spec.target || {}).mode)],
      ["operation", rowOperationCell(object, (object.metadata || {}).namespace)],
      ["point in time", when(spec.pointInTime)],
      // FX-8: WHAT THE APPROVED PLAN ACCEPTED, read from its bytes. A
      // `LogAppendTime` topic at a point is refused unless the plan accepted
      // producer time. "Not found" is said as such and never as "the plan
      // states none" (review L-3): the signed result is the row below.
      ["time basis (plan)", "<span id=\"restore-time-basis\">" +
        esc(planTimeBasis(spec.planBytes) === "producerTime"
          ? "producer time (restore.time_basis: producerTime): a LogAppendTime topic at this " +
            "point is selected by its producers' clocks"
          : "no restore.time_basis: producerTime found in the plan by this page; without it a " +
            "LogAppendTime topic at a point is refused, and the signed result below is " +
            "authoritative") +
        "</span>"],
      // FX-8 (review M-2): WHAT THE RUN SIGNED, from `status.timeBasis` -- the
      // scorecard's `source.time_basis` as the controller read it. A claim
      // until the evidence verifies, like every scorecard fact on this page.
      ["time basis (signed)", "<span id=\"restore-time-basis-signed\">" +
        claim(signedTimeBasisText(status.timeBasis)) + "</span>"],
      ["backup set", cell(spec.backupSetRef)],
      // PROD-08.1a: WHAT THIS RESTORE ASKED FOR, from `spec` (which the
      // controller holds to the plan), and WHAT ITS SCORECARD SIGNED.
      ["coverage (asked for)", "<span id=\"restore-coverage-requested\">" +
        esc(coverage.requested === "complete"
          ? "complete -- every record of every restored partition" +
            (coverage.maxRecords === null
              ? ", no record bound"
              : ", at most " + String(coverage.maxRecords) + " archived records")
          : "sampled -- the canary and the manifest's count bound") + "</span>"],
      ["coverage (signed)", "<span id=\"restore-coverage-signed\"" +
        (notCovered(coverage.complete) ? " data-covered=\"false\"" : "") + ">" +
        claim(coverageWords(coverage.recorded, coverage.requested, coverage.complete)) +
        "</span>"],
      // PROD-11.1b: WHAT WAS RESTORED, when the signed scorecard says it was
      // a selection; a restore of everything shows no row, as before.
      ...(selection === null
        ? []
        : [["restored (signed)", "<span id=\"restore-selection-signed\" data-selection=\"partial\">" +
          claim(selectionWords(selection)) + "</span>"]]),
    ]) +
    (verified
      ? ""
      : "<p class=\"caveat\" data-scorecard-claim=\"note\">" + esc(SCORECARD_CLAIM_SENTENCE) +
        "</p>") +
    unrecordedTimeBasisWarning(status.timeBasis) +
    "<h3>Integrity</h3>" +
    integrityFacts +
    notPublishedNote +
    // HOW MUCH OF THIS RESTORE WAS ACTUALLY COMPARED, BESIDE THE RESULT AND
    // NEVER AWAY FROM IT (D3 section 2.5, section 3.5). A pass is a pass over a SAMPLE, and
    // a result printed without its scope reads as an exhaustive comparison --
    // which no level in v1 performs and none is called `complete`.
    "<p class=\"scope\">" + esc(verificationScopeSentence(scopeOf(object))) + "</p>" +
    // PROD-08.1a: A RECORDED COMPLETE VERIFICATION, IN FULL -- covered or not,
    // the reason, and every partition's exact counts. A sampled run renders
    // nothing here.
    (coverage.recorded === "complete"
      ? renderCompleteCoverage(coverage.complete, claim, selection)
      : "") +
    (coverage.requested === "complete" && coverage.recorded === null
      ? "<p class=\"note\" id=\"restore-coverage-cost\">" + messageText(COMPLETE_COVERAGE_COST) +
        "</p>"
      : "") +
    "<h3>Objectives asked for, and what the run achieved</h3>" +
    objectiveFacts +
    "<h3>Target topic preflight</h3>" +
    preflightFacts +
    "<h3>Topics</h3>" +
    facts([
      // PROD-15.1: for a run whose creation step stopped, the row says what
      // the restore actually created -- the topics it left -- so a name
      // someone else created, or one the restore cannot account for, is never
      // listed as this restore's. (This controller writes the same list into
      // `status.newTopics`; an older one wrote the plan's mapped names.)
      ["new topics", createdTopics.length === 0
        ? cell(null)
        : esc(createdTopics.join(", ") + (stoppedLeftMore === 0
          ? ""
          : " and " + stoppedLeftMore + " more"))],
      ["old topics -- written to by nothing, in any tag", oldTopicsCell],
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
