// pages/history.js -- Restores and Backups interleaved by creation time, and
// the Restore detail view.
//
// THE RESTORE HALF OF THE BADGE RULE lives here (see [`restoreBadge`]): a
// `fail-integrity` run produced a perfectly valid, signed document SAYING THE
// RESTORE DID NOT RECONCILE, and a green badge over it would invert the most
// valuable thing this product reports. The verification half is shared with
// the Backup rule and imported from `./backups.js`; the outcome half is not,
// because the two kinds do not carry the same field.
//
// THREE ROWS THE FROZEN `drill show` TABLE OMITS ARE RENDERED HERE, because
// omitting them makes a different claim:
//
//   `engine sub-report` -- null in every scorecard this product writes (the
//   engine adapter never overrides the validation-run hook); omitting the
//   row reads as "no caveat", so it says "not produced by this engine
//   version".
//
//   `integrity.partialReason` -- a `partial` result with no reason beside it
//   is a verdict an auditor cannot act on.
//
//   `objectives` -- what was ASKED FOR; `measured` alone does not say whether
//   it was enough.
//
// THE RESTORE DETAIL OPENS WITH WHERE ITS OPERATION STANDS -- its recorded
// progress, the hash of its own plan bytes and its Approval's state -- read
// from the objects and never remembered by the page; a refresh reads them
// again and creates nothing.

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
  inert,
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
  NOT_PUBLISHED,
  notPublishedCell,
  notPublishedIn,
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
 *  THE NOT-GREEN CAPTION NAMES ITS CASE, as the Backup rule's does:
 *  `Untrusted` (authentic bytes, a key this installation does not accept),
 *  `NotAttempted` (the controller could not check), `Invalid` (the document
 *  did not verify) -- three repairs behind one word (D3 section 7.4). */
export function restoreBadge(status) {
  const s = status || {};
  // A COMPLETE VERIFICATION THAT DID NOT COVER THE RESTORE IS NEVER GREEN,
  // whatever else the status says: the controller's own rule
  // (`CompleteNotCovered`), applied in both modes.
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
 *  not cover the restore. */
export const NOT_COVERED_CAPTION =
  "complete coverage did not cover this restore -- not a pass";

/** The recorded complete block, from whichever document this page
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

/** One Restore's coverage -- what it ASKS for (`spec.coverage`,
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

/** One Restore's signed selection, from whichever document this
 *  page is looking at -- the product API's `verificationScope.selection` on a
 *  console detail, else the custom resource's (or a console list row's)
 *  `status.integrity.selection`. `null` when none is recorded: every partition
 *  of every restored topic, from the archive's floor. */
export function selectionOf(object) {
  const status = (object && object.status) || {};
  return selectionIn(status.verificationScope || {}) || selectionIn(status.integrity || {});
}

/** The list row's coverage line, under the RESULT -- and, for a narrowed
 *  restore, the line that says so. */
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
 *  The product API's `status.verificationScope` is used verbatim. A custom
 *  resource carries the same facts as `status.integrity.level` and
 *  `status.completion`'s counts, read through D3 section 2.5's table
 *  ([`SCOPE_LEVEL_OF_INTEGRITY`]): a vocabulary map, not a verdict, never
 *  inferred from a count or a pass. A level the table does not know produces
 *  no scope, and the absent-scope sentence says the amount compared is not
 *  known here -- which is true, and is not "complete". */
export function scopeOf(object) {
  const status = (object && object.status) || {};
  if (status.verificationScope !== null && status.verificationScope !== undefined) {
    return status.verificationScope;
  }
  // A CONSOLE PROJECTION'S SCOPE IS THE API'S OWN BLOCK, OR NONE. The table
  // below is for the mode with no `logweir-api` to do the mapping; reading a
  // scope out of a console detail's `status.integrity.level` would be this page
  // computing what the API is there to compute, and would print a sampled-check
  // sentence for a run whose route published no scope at all.
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
    // The custom resource's copies of the signed facts, under the API's names.
    coverage: integrity.coverage,
    complete: integrity.complete,
    unsampledTopics: integrity.unsampledTopics,
    selection: integrity.selection,
  };
}

/** The link a row of either kind carries to the durable operation view. The
 *  uid travels with the name, so a link about a run deleted and recreated
 *  lands on a refusal rather than on a different run's evidence. */
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

// A CONSOLE LIST ROW'S OWN GREEN: it carries no verification block, only the
// API's summary, whose `verifiedSuccess` is the controller's badge rule
// computed server side -- the word the SIGNED column reads -- so a row whose
// SIGNED cell says verified never calls its outcome unverified.
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

/** THE LINK THAT CARRIES A RECOVERY POINT'S IDENTITY INTO THE WIZARD.
 *
 *  Only a `Backup` that IS a recovery point gets one -- Succeeded, with a
 *  backup set and a covered window -- because otherwise the link would open a
 *  refusal. `Restore` rows get none. The UID travels beside the name and is
 *  what the wizard resolves by, so a row clicked after the object was
 *  recreated under the same name ends in a refusal naming the point that is
 *  gone, not in a plan over a different run. */
export function restorePointCell(object, ns) {
  if (kindOf(object) !== "Backup" || !isRecoveryPoint(object)) {
    return cell(null);
  }
  // A role that cannot restore here is told who can.
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
      // The row's action beside its name: as the last column it was what a
      // 1024 px window scrolled out of sight.
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
        String(o.approvalError.reason || "") : "error") + " " + esc(inert(o.approvalError.message)) + "</p>"
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

/** The signed time basis, in words: `status.timeBasis`, the scorecard's
 *  `source.time_basis` as the controller copied it. ABSENT is not recorded
 *  and is never read as "every selection used the topics' own clocks"; two
 *  empty lists are the run's claim, as far as the archive manifest's segment
 *  bounds show. */
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

/** What a Restore whose creation step stopped left on the target cluster,
 *  from `status.targetTopicsAppeared`: the mapped names someone else created
 *  while the restore was admitted (never written to), every topic this
 *  restore created and left empty, with what to do about it, and every name
 *  it asked for and CANNOT ACCOUNT FOR, with its own sentence -- never called
 *  this restore's. A list the 100-name bound cut says how many more there
 *  are. Logweir deletes none of them, so the operator must be told they are
 *  there. Empty when the status carries no such block; every name escaped. */
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
 *  topic whose timestamp type was not recorded: the clock that topic's point
 *  in time was read on is unknown. Empty otherwise. */
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

// What a Restore detail's cell says for a value the controller keeps on the
// Restore and the product API serving this console does not publish:
// `ui/render.js`'s words and test, shared with the connection detail and
// re-exported here, where this view's rows read them.
export { NOT_PUBLISHED, notPublishedIn };

/** Said once, under the Integrity table, when any cell of the view reads
 *  [`NOT_PUBLISHED`]. */
export const NOT_PUBLISHED_SENTENCE =
  "A cell that reads \"" + NOT_PUBLISHED + "\" is not an empty one. It is a value the " +
  "controller keeps on the Restore object's status and the product API serving this console " +
  "does not publish, so this page cannot say whether one is recorded. The integrity result " +
  "and partial reason, the objectives and the measured values are also in the run's signed " +
  "scorecard, which the commands under Check it yourself fetch and verify.";

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
  // WHICH. On a custom resource an absent field is one the controller did not
  // record: "-". On a console projection that NAMES the field among what it
  // could not supply, "-" would say the same about a value the API simply does
  // not publish (an empty Integrity table for a restore whose status read
  // `byte-fingerprint` / `pass`). `shown` renders the value, or the
  // not-published cell when the projection named its absence; the sentence
  // under the Integrity table is printed once.
  let unpublished = false;
  const shown = (field, value, render) => {
    if ((value === undefined || value === null) && notPublishedIn(object, field)) {
      unpublished = true;
      return notPublishedCell(field);
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
    // WHAT A STOPPED CREATION STEP LEFT ON THE CLUSTER, first, where it cannot
    // be missed: nothing else on this page says a topic exists.
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
      // WHAT THE APPROVED PLAN ACCEPTED, read from its bytes. A `LogAppendTime`
      // topic at a point is refused unless the plan accepted producer time. "Not
      // found" is said as such and never as "the plan states none"; the signed
      // result is the row below.
      ["time basis (plan)", "<span id=\"restore-time-basis\">" +
        esc(planTimeBasis(spec.planBytes) === "producerTime"
          ? "producer time (restore.time_basis: producerTime): a LogAppendTime topic at this " +
            "point is selected by its producers' clocks"
          : "no restore.time_basis: producerTime found in the plan by this page; without it a " +
            "LogAppendTime topic at a point is refused, and the signed result below is " +
            "authoritative") +
        "</span>"],
      // WHAT THE RUN SIGNED, from `status.timeBasis` (the scorecard's
      // `source.time_basis`). A claim until the evidence verifies, like every
      // scorecard fact on this page.
      ["time basis (signed)", "<span id=\"restore-time-basis-signed\">" +
        claim(signedTimeBasisText(status.timeBasis)) + "</span>"],
      ["backup set", cell(spec.backupSetRef)],
      // WHAT THIS RESTORE ASKED FOR, from `spec` (which the controller holds to
      // the plan), and WHAT ITS SCORECARD SIGNED.
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
      // WHAT WAS RESTORED, when the signed scorecard says it was a selection; a
      // restore of everything shows no row.
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
    // HOW MUCH WAS ACTUALLY COMPARED, BESIDE THE RESULT AND NEVER AWAY FROM IT
    // (D3 sections 2.5 and 3.5): a pass is a pass over a SAMPLE, and a result
    // without its scope reads as an exhaustive comparison.
    "<p class=\"scope\">" + esc(verificationScopeSentence(scopeOf(object))) + "</p>" +
    // A RECORDED COMPLETE VERIFICATION, IN FULL -- covered or not, the reason,
    // and every partition's exact counts. A sampled run renders nothing here.
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
      // For a run whose creation step stopped, the row says what the restore
      // actually created -- the topics it left -- so a name someone else created,
      // or one it cannot account for, is never listed as this restore's. (An
      // older controller wrote the plan's mapped names into `status.newTopics`.)
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
      // The plan's evidence bucket, never the source archive's: the scorecard is
      // where the approved plan wrote it.
      planEvidenceBucket(spec.planBytes) || bucketOf(""),
      evidence.scorecardKey || "",
      evidence.sidecarKey || "",
      "scorecard.json",
      "scorecard.sig",
    ) +
    "</section>"
  );
}

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
