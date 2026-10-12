// pages/operation.js -- the durable operation view.
//
// One submission, one place to watch it, and a route that survives a reload:
// where the run is, why, what the controller could see, what it produced and
// whether the evidence verified. The route carries the whole identity --
// `#/operations?ns=&kind=&name=&uid=` -- so a refresh, a new tab and a pasted
// link all open the same run. Nothing is kept in this page; the custom
// resource is the truth.
//
// FOUR THINGS THIS VIEW REFUSES TO DO.
//
//   1. IT COMPUTES NO STATE. In console mode it prints the API's normalized
//      `state` (D3 section 2.5's table). Legacy mode has none, and the page
//      says so and shows the controller's `status.progress.stage` rather than
//      implementing that table a second time in a browser.
//   2. IT NEVER READS A CLOCK FOR A VERDICT. Instants are printed as they are;
//      whether an active stage has gone stale is the API's decision against a
//      clock the cluster shares, and `Date.now()` would disagree with it by
//      the skew between them.
//   3. IT NEVER MERGES THE RESULT AND THE EVIDENCE. "The run exited 0" and
//      "the document it signed verifies" are two facts, and a `succeeded`
//      result whose evidence is `NotAttempted` is never shown as a verified
//      success (D3 section 2.5).
//   4. IT CANCELS NOTHING. Leaving the route closes the watch's connection;
//      the Job keeps running, and there is no cancel route in v1.
//
// THE UID IN THE ROUTE IS A GUARD. A Backup deleted and recreated under the
// same name is a different run with different evidence, so when the object
// answers to a different uid this page says "this name now refers to a
// different run" and renders nothing about it.

import {
  ABSENT,
  announce,
  COMPLETION_GUIDANCE,
  GREEN_TRUST_STATES,
  HISTORICAL_SUFFIX,
  LEGACY_OPERATION_SENTENCE,
  STATE_UNKNOWN_SENTENCE,
  TARGET_MODE_MEANING,
  WATCH_SENTENCE,
  badge,
  basisAllowsGreen,
  cell,
  detailLink,
  diagnosticsTable,
  errorBlock,
  esc,
  facts,
  messageText,
  phaseBadge,
  replace,
  restoreNeedsRole,
  scorecardClaim,
  stateBadge,
  table,
  trustBlockAllowsGreen,
  unverifiedCaption,
  unverifiedTrustCaption,
  verificationScopeSentence,
  coverageWords,
  notCovered,
  renderCompleteCoverage,
  selectionIn,
  selectionWords,
  flagBadge,
  when,
} from "../render.js";
import { granted } from "../client.js";
import { active, cancelled } from "../lifecycle.js";
import { inConsole, readOperation, watchOperation } from "../operation-watch.js";

/** The route one operation lives at. Both halves of the identity travel: the
 *  name says which object to read and the uid which RUN the link was about. */
export function operationRoute(ns, kind, name, uid) {
  return (
    "#/operations?ns=" + encodeURIComponent(String(ns || "")) +
    "&kind=" + encodeURIComponent(String(kind || "")) +
    "&name=" + encodeURIComponent(String(name || "")) +
    (typeof uid === "string" && uid.length > 0 ? "&uid=" + encodeURIComponent(uid) : "")
  );
}

/** The three values the route carries, out of the hash string. A pure
 *  function of the hash, for the reason `restoreRouteParams` is: `ui/app.js`
 *  touches `window` at module scope and cannot be imported under `node
 *  --test`. */
export function operationRouteParams(hash) {
  const text = typeof hash === "string" ? hash : "";
  const question = text.indexOf("?");
  const out = { kind: "", name: "", uid: "" };
  if (question === -1) {
    return out;
  }
  for (const pair of text.slice(question + 1).split("&")) {
    const equals = pair.indexOf("=");
    if (equals === -1) {
      continue;
    }
    const key = pair.slice(0, equals);
    const value = decodeURIComponent(pair.slice(equals + 1)).trim();
    if (key === "kind" || key === "name" || key === "uid") {
      out[key] = value;
    }
  }
  return out;
}

/** The two kinds this route serves. A third is refused before a read. */
export const OPERATION_KINDS = Object.freeze(["backup", "restore"]);

/** The refusal a route whose uid does not answer gets, verbatim. */
export const DIFFERENT_RUN_SENTENCE =
  "This name now refers to a different run. The link you followed named a uid, and the object " +
  "in the cluster answers to another one -- the run it was about was deleted and a new one was " +
  "created under the same name. Nothing about the new run is shown here, because you did not " +
  "ask about it.";

/** What the page says while it has no document yet. */
export const READING_SENTENCE = "Reading this operation...";

/** THE FACTS, SELECTED AND NEVER DERIVED.
 *
 *  Both documents come out as the same bag of already recorded values; what
 *  differs is which fields EXIST. `state` is `null` in legacy mode because
 *  the custom resource has none, and `null` renders as a sentence saying
 *  which API computes it -- never as `unknown`, which is one of the ten words
 *  and would be a verdict this page invented. */
export function operationFacts(document, console_) {
  const o = document || {};
  if (console_ === true) {
    return {
      console: true,
      kind: o.kind || "",
      name: o.name || "",
      uid: o.uid || "",
      state: typeof o.state === "string" ? o.state : null,
      terminal: o.terminal === true,
      stale: o.stale === true,
      reason: o.stateReason || null,
      message: o.message || null,
      lastUpdate: o.lastUpdatedAt || null,
      createdAt: o.createdAt || null,
      awaitingApproval: o.awaitingApproval === true,
      // Selected and deliberately NOT PRINTED. `logweir-api` can only produce
      // `ReadinessView::not_implemented()` today (`{state: "unknown", basis:
      // "notImplemented"}`), and a row saying so on every run is noise a reader
      // learns to skip -- which makes the row useless the day it carries an
      // answer. It is kept in the facts for the page that renders it then.
      readiness: o.readiness || null,
      stage: o.stage || null,
      progress: o.progress || null,
      diagnostics: diagnosesOf(o),
      result: o.result || null,
      evidence: o.evidence || null,
      verification: o.verification || null,
      // The console's verdict is one word, arriving combined (D3 section 2.5):
      // `trust.state` is the API's answer over the controller's `result` and
      // `trust.basis`, which travel beside it with the key state and policy.
      trust: o.trust || null,
      trustState: (o.trust || {}).state || null,
      evidenceVerification: null,
      verifiedSuccess: o.verifiedSuccess === true,
      verificationScope: o.verificationScope || null,
      integrity: null,
      capture: o.capture || null,
      completion: o.completion || null,
      teardown: o.teardown || null,
      targetMode: o.targetMode || null,
      conditions: Array.isArray(o.conditions) ? o.conditions : [],
    };
  }
  const meta = o.metadata || {};
  const spec = o.spec || {};
  const status = o.status || {};
  const progress = status.progress || null;
  return {
    console: false,
    kind: (o.kind || "").toLowerCase(),
    name: meta.name || "",
    uid: meta.uid || "",
    state: null,
    terminal: false,
    reason: status.reason || (progress === null ? null : progress.reason) || null,
    message: progress === null ? null : progress.message || null,
    lastUpdate: progress === null
      ? null
      : (progress.lastObservedTime || progress.lastTransitionTime || null),
    createdAt: meta.creationTimestamp || null,
    awaitingApproval: status.phase === "Pending",
    readiness: null,
    stale: false,
    stage: progress === null ? null : progress.stage || null,
    trust: null,
    trustState: null,
    progress: progress,
    diagnostics: diagnosesOf(o),
    phase: status.phase || null,
    capture: status.capture || null,
    result: {
      exitCode: status.exitCode,
      exitReason: status.exitReason,
      outcome: status.outcome,
      lastPhaseCompleted: status.lastPhaseCompleted,
    },
    evidence: status.evidence || null,
    verification: null,
    evidenceVerification: (status.evidence || {}).verification || null,
    verifiedSuccess: false,
    verificationScope: status.verificationScope || null,
    // The custom resource's copy of the signed coverage, read by
    // [`renderCoverage`] where the console reads `verificationScope`.
    integrity: status.integrity || null,
    completion: status.completion || null,
    teardown: status.teardown || null,
    targetMode: (spec.target || {}).mode || null,
    conditions: Array.isArray(status.conditions) ? status.conditions : [],
  };
}

/** The diagnoses, from wherever this document carries them. The console DTO
 *  may publish them beside the progress block and the custom resource carries
 *  them inside it; neither is invented and an absent list is an absent list. */
function diagnosesOf(document) {
  const o = document || {};
  if (Array.isArray(o.diagnostics)) {
    return o.diagnostics;
  }
  const progress = o.progress || (o.status || {}).progress || {};
  return Array.isArray(progress.diagnostics) ? progress.diagnostics : [];
}

/** The runner facts, and there are none in console mode.
 *
 *  `progress.runner` is not published, deliberately: a Job or pod name is
 *  infrastructure detail, and a structural test on the API side keeps it off
 *  the contract. Legacy mode renders it because there the page reads the
 *  object itself. In console mode an incident has `progress.reason`,
 *  `progress.message` and a diagnostic's own `object {kind, name}`. */
export const NO_RUNNER_DETAIL_SENTENCE =
  "The product API publishes no Job or pod name for a run: they are infrastructure detail and " +
  "the console contract does not carry them. What the controller could see is in the diagnoses " +
  "below, each naming the object it is about.";

/** The state cell: the API's own word, or the sentence that says there is none
 *  in this mode. */
export function renderState(v) {
  if (v.state === null) {
    return "<p class=\"note\" data-no-normalized-state=\"true\">" +
      messageText(LEGACY_OPERATION_SENTENCE) + "</p>" +
      (v.phase ? "<p class=\"phase\">controller phase " + phaseBadge(v.phase) + "</p>" : "");
  }
  return (
    "<p class=\"state\">" + stateBadge(v.state) + "</p>" +
    (v.state === "unknown" ? "<p class=\"note\">" + messageText(STATE_UNKNOWN_SENTENCE) + "</p>" : "")
  );
}

/** The progress block: stage, reason, the instants and the one runner pod.
 *  Every value is the controller's own, printed as written. */
export function renderProgress(v) {
  const p = v.progress || null;
  if (p === null) {
    return (
      "<section class=\"progress\"><h3>Progress</h3>" +
      "<p class=\"note\">This object carries no <code>status.progress</code>. That is what an " +
      "object reconciled only by an older controller looks like; it is an ABSENT observation " +
      "and not a stalled run, and no stage is inferred from it.</p></section>"
    );
  }
  const runner = p.runner || null;
  const phase = p.runnerPhase || {};
  const rows = [
    ["stage", cell(p.stage)],
    ["reason", cell(p.reason)],
    ["message", cell(p.message)],
    ["last transition", when(p.lastTransitionTime)],
    ["last observed", when(p.lastObservedTime)],
    ["runner phase", typeof phase.name === "string" && phase.name.length > 0
      ? cell(phase.number) + " " + cell(phase.name)
      : ABSENT],
  ];
  if (runner !== null) {
    rows.push(["job", cell(runner.jobName)]);
    rows.push(["pod", cell(runner.podName) + " " + cell(runner.podPhase)]);
    rows.push(["container", cell(runner.containerState) + " " + cell(runner.waitingReason)]);
    rows.push(["runner started", when(runner.startedAt)]);
  }
  return (
    "<section class=\"progress\"><h3>Progress</h3>" +
    facts(rows) +
    (runner === null
      ? "<p class=\"note\" data-no-runner-detail=\"true\">" +
        esc(NO_RUNNER_DETAIL_SENTENCE) + "</p>"
      : "") +
    "</section>"
  );
}

/** The diagnoses the controller recorded, as the repairs they are. */
export function renderDiagnostics(v) {
  return (
    "<section class=\"diagnostics\"><h3>What the controller could see</h3>" +
    "<p class=\"note\">Each row is a CAUSE the controller observed on this run's Job, its pod " +
    "or the events about them, with the object it is about. The code names the repair; the " +
    "message is the controller's own and is not reworded here.</p>" +
    diagnosticsTable(v.diagnostics) +
    "</section>"
  );
}

/** THE ONE ACTION A FAILED RESTORE OFFERS: retry to a FRESH TARGET.
 *
 *  A link, not a button: nothing is retried from here. It opens the wizard,
 *  carrying which run failed, and the operator reviews a whole new plan. The
 *  retry is a NEW Restore with a new topic prefix and plan bytes, so a new
 *  plan hash and -- both names being minted from those bytes -- a new
 *  Restore name and Approval name; a governed namespace makes it wait for an
 *  approval of its own.
 *
 *  Nothing is offered for a backup or a run that has not failed: retrying a
 *  running restore would be a second execution against the same target
 *  names, and retrying a successful one is just a restore. */
export function renderRetryAction(v, ns) {
  if (v.kind !== "restore") {
    return "";
  }
  // BOTH MODES, EACH IN ITS OWN VOCABULARY: the console DTO says `terminal`
  // and a lower-case `state`; the custom resource says `status.phase`, and its
  // projection leaves `terminal` false, so reading only the first would offer
  // nothing behind `kubectl proxy`, where an incident without the product API
  // happens. `failed` AND `refused`: both are work that will never proceed,
  // and a terminal refusal (`ApprovalSubjectMismatch`, `PlanHashMismatch`) is
  // the sharper case -- that Restore's and Approval's names are taken, so a
  // same-prefix retry would collide. The custom resource spells both `Failed`.
  const failed = v.console === true
    ? v.terminal === true &&
      ["failed", "refused"].includes(String(v.state || "").toLowerCase())
    : String(v.phase || "").toLowerCase() === "failed";
  if (!failed) {
    return "";
  }
  return (
    "<section class=\"retry\" id=\"restore-retry\"><h3>Retry</h3>" +
    "<p class=\"note\">" + esc(RETRY_FRESH_TARGET_SENTENCE) + "</p>" +
    // A retry is a new Restore, and a role that cannot create one reads who can;
    // the wizard's route refuses it by name.
    (granted(String(ns || ""), "restoreCreate")
      ? "<p class=\"actions\"><a id=\"retry-fresh-target\" href=\"" +
        esc(retryRoute(String(ns || ""), String(v.name || ""))) +
        "\">Retry to a fresh target</a></p>"
      : "<p class=\"actions\">" + restoreNeedsRole(true) + "</p>") +
    "</section>"
  );
}

/** The wizard's selector, carrying which failed run is being retried.
 *
 *  Spelled here, not imported from the wizard
 *  (`restore-wizard.js::restoreRetryFromOperationRoute`): that would pull
 *  `ui/plan.js` into this page's module graph, and `plan.js` THROWS AT LOAD
 *  outside a secure context, so a page that only reads a run would stop
 *  rendering there. `pages.spec.js` asserts both produce the identical
 *  string.
 *
 *  It names NO recovery point: `Restore.spec` carries a backup SET id and no
 *  reference to its `Backup`, so the point is chosen on the selector. */
export function retryRoute(ns, restoreName) {
  const n = typeof ns === "string" ? ns.trim() : "";
  return (
    "#/restore" +
    (n.length > 0 ? "?ns=" + encodeURIComponent(n) + "&" : "?") +
    "retryOf=" + encodeURIComponent(typeof restoreName === "string" ? restoreName : "")
  );
}

/** What the retry link does, said beside it. */
export const RETRY_FRESH_TARGET_SENTENCE =
  "This run failed. There is no resume: a restore cannot be continued from where it stopped, " +
  "and any topics it had already created stay as they are. Retrying opens the wizard for a NEW " +
  "restore with a fresh topic prefix, so no name this run used is written to -- a new plan, a " +
  "new plan hash, and a new approval of its own where the policy asks for one. This run is not " +
  "modified.";

/** THE RESULT: what the RUN did. Never the evidence, which is the next block. */
export function renderResult(v) {
  const r = v.result || {};
  // A RESTORE'S OUTCOME IS ITS SCORECARD'S, and is the scorecard's claim until
  // the evidence is green by the same rule the evidence section draws.
  return (
    "<section class=\"result\"><h3>Result</h3>" +
    facts([
      ["result", cell(r.status)],
      ["exit code", cell(r.exitCode)],
      ["exit reason", cell(r.exitReason)],
      ["outcome", v.kind === "restore" ? scorecardClaim(cell(r.outcome), evidenceGreen(v)) : cell(r.outcome)],
      ["last phase completed", cell(r.lastPhaseCompleted)],
    ]) +
    "<p class=\"note\">This is what the RUN recorded. Whether the document it produced verifies " +
    "is the separate question below, and a successful run whose evidence was never checked is " +
    "not a verified one.</p>" +
    "</section>"
  );
}

/** Whether this run's evidence is green, by the evidence section's own rule:
 *  the API's combined trust word in console mode, `result` + the raw `trust`
 *  block in legacy mode (render.js `trustBlockAllowsGreen`).
 *
 *  THE BASIS STILL VETOES IN CONSOLE MODE. An older `logweir-api` projects a
 *  `Valid` verdict on `RecordedBeforeRevocation` as `trust.state: verified`
 *  (a current one says `untrusted`), and D3 section 7.4 says that case is
 *  "never green". So a green word is green only on a basis a green badge may
 *  carry -- the same [`basisAllowsGreen`] legacy mode applies -- and the two
 *  modes cannot disagree about one run, whichever server answered. */
export function evidenceGreen(v) {
  const console_ = v.console === true;
  const trust = (console_ ? v.trust : ((v.evidenceVerification || {}).trust)) || {};
  const ver = (console_ ? v.verification : v.evidenceVerification) || {};
  return console_
    ? (GREEN_TRUST_STATES.indexOf(v.trustState) !== -1 && basisAllowsGreen(trust.basis))
    : (ver.result === "Valid" && trustBlockAllowsGreen((v.evidenceVerification || {}).trust));
}

/** The caption of a console evidence badge that is not green. A green trust
 *  word the basis vetoed is named by the basis -- the words legacy mode uses
 *  for the same `Valid` + basis -- and every other word by its own case. */
function consoleUnverifiedCaption(v, trust) {
  // A `RecordedBeforeRevocation` verdict is named by its basis whichever word
  // carried it -- `untrusted` from a current server, `verified` from an older
  // one -- exactly as legacy mode names the same object.
  if (trust.basis === "RecordedBeforeRevocation" && v.trustState === "untrusted") {
    return unverifiedCaption({ result: "Untrusted", trust: trust }, v.verifiedSuccess);
  }
  if (GREEN_TRUST_STATES.indexOf(v.trustState) !== -1 && !basisAllowsGreen(trust.basis)) {
    return unverifiedCaption({ result: "Valid", trust: trust }, v.verifiedSuccess);
  }
  return unverifiedTrustCaption(v.trustState, v.verifiedSuccess);
}

/** THE EVIDENCE: what the CONTROLLER checked, with the case named. Three
 *  results, three claims (D3 section 7.4): `Invalid` is about the DOCUMENT,
 *  `NotAttempted` about the CONTROLLER, `Untrusted` about the SIGNER; never
 *  flattened into one word. */
export function renderEvidence(v) {
  const e = v.evidence || {};
  // TWO DOCUMENTS, TWO RULES. In console mode the verdict arrives combined
  // (`trust.state`, D3 section 2.5), and re-deriving it here would implement
  // that table a second time in a browser. In legacy mode there is no such
  // word -- the custom resource carries `result` and a PascalCase `basis` --
  // so the page keeps its own rule, the one `#/backups` and `#/history` use.
  const console_ = v.console === true;
  const trust = (console_ ? v.trust : ((v.evidenceVerification || {}).trust)) || {};
  const ver = (console_ ? v.verification : v.evidenceVerification) || {};
  const policy = trust.policy || {};
  const green = evidenceGreen(v);
  const historical = console_
    ? v.trustState === "verifiedHistorical"
    : trust.basis === "Historical";
  const mark = green
    ? badge(
      "green",
      "verified by weirkeeper at " + String(ver.verifiedAt || "") + " against key " +
        String(ver.matchedKeyId || "") + (historical ? HISTORICAL_SUFFIX : ""),
    )
    : badge(
      "unverified",
      console_
        ? consoleUnverifiedCaption(v, trust)
        : unverifiedCaption(ver, v.verifiedSuccess),
    );
  return (
    "<section class=\"evidence\"><h3>Evidence</h3>" +
    mark +
    facts([
      ["recorded result", cell(console_ ? v.trustState : ver.result)],
      ["signature result", cell(console_ ? ver.state : null)],
      ["matched key id", cell(ver.matchedKeyId)],
      ["payload type", cell(ver.payloadType)],
      ["verified at", when(ver.verifiedAt)],
      ["signed at (the document's own claim)", when(trust.signedAt || ver.signedAt)],
      ["signing time read", cell(trust.signingTimeRead)],
      ["trust basis", cell(trust.basis)],
      ["key state", cell(trust.keyState)],
      ["trust policy", cell(policy.name) +
        (typeof policy.generation === "number" ? " g" + String(policy.generation) : "")],
      ["detail", cell(ver.detail)],
      ["receipt key", cell(e.receiptKey || e.payloadKey)],
      ["receipt sha256", cell(e.receiptSha256 || e.payloadSha256)],
      ["scorecard key", cell(e.scorecardKey)],
      ["sidecar key", cell(e.sidecarKey)],
      ["offset report key", cell(e.offsetReportKey)],
    ]) +
    "</section>"
  );
}

/** WHICH MODE THIS RESTORE IS IN, FROM THE MOMENT IT EXISTS.
 *
 *  `targetMode` is top level because D3 section 3.5 keys its guidance on
 *  `spec.target.mode`, a fact about the run from creation: a running,
 *  pending or refused rehearsal must be labelled, and "these topics are
 *  deleted by teardown" is worth saying BEFORE the fact. This is not the
 *  completion guidance, which is about what a run produced and stays beside
 *  the scorecard; this says what the run IS. */
export function renderTargetMode(v) {
  if (v.kind !== "restore") {
    return "";
  }
  const mode = v.targetMode;
  if (typeof mode !== "string" || mode.length === 0) {
    return (
      "<p class=\"note\" data-target-mode=\"\">This run records no target mode, so this page " +
      "does not say which one it used: the mode is what decides whether its topics are a " +
      "rehearsal, and it will not be guessed.</p>"
    );
  }
  const meaning = TARGET_MODE_MEANING[mode];
  return (
    "<p class=\"target-mode\" data-target-mode=\"" + esc(mode) + "\">target mode " +
    badge(mode === "scratch" ? "warn" : "flat", mode) +
    (typeof meaning === "string" ? " " + esc(meaning) : "") +
    "</p>"
  );
}

/** HOW MUCH OF THIS RESTORE WAS VERIFIED, for every Restore and whatever its
 *  outcome -- the completion panel is shown only for a run that succeeded,
 *  and a complete verification that did NOT cover the restore is exactly
 *  the run that did not. The signed coverage in words, and a recorded
 *  complete verification in full: covered or not, its reason, and every
 *  partition's exact counts. A claim until the evidence is green. */
export function renderCoverage(v) {
  if (v.kind !== "restore") {
    return "";
  }
  const scope = v.verificationScope || {};
  const integrity = v.integrity || {};
  const recorded = typeof scope.coverage === "string" ? scope.coverage : integrity.coverage;
  const complete = scope.complete || integrity.complete || null;
  const green = evidenceGreen(v);
  const claim = (value) => scorecardClaim(cell(value), green);
  const known = recorded === "sampled" || recorded === "complete" ? recorded : null;
  // A narrowed restore says so beside its coverage.
  const selection = selectionIn(scope) || selectionIn(integrity);
  const narrowed = selectionWords(selection);
  return (
    "<section class=\"coverage\" id=\"operation-coverage\"" +
      (notCovered(complete) ? " data-covered=\"false\"" : "") +
      (narrowed === "" ? "" : " data-selection=\"partial\"") + "><h3>Verification coverage</h3>" +
    facts([["coverage (signed)", claim(coverageWords(known, null, complete))]].concat(
      narrowed === "" ? [] : [["restored (signed)", claim(narrowed)]])) +
    (known === "complete" ? renderCompleteCoverage(complete, claim, selection) : "") +
    "</section>"
  );
}

/** THE COMPLETION PANEL (D3 section 3.5): what a terminal Restore produced,
 *  the sampled counts labelled exactly, and the fixed guidance for its
 *  target mode. The guidance sentences are `ui/render.js`'s, never the
 *  server's: server-authored prose here would be an instruction no review
 *  in this repository ever read. */
export function renderCompletion(v) {
  const c = v.completion || null;
  // A RUN THAT DID NOT SUCCEED PRODUCED NOTHING THIS PANEL COUNTS, AND THE
  // PANEL IS NOT SHOWN. The controller copies `status.completion` only for
  // exit 0 and outcome `pass`; the result and evidence sections above say why
  // a failed run ended. A completion an older controller copied onto a failed
  // run is still not what the run produced.
  const finished = v.terminal === true ||
    ["Succeeded", "Failed", "Cancelled"].indexOf(String(v.phase)) !== -1;
  const succeeded = v.phase === "Succeeded" || v.state === "succeeded";
  if (finished && !succeeded) {
    return "";
  }
  if (c === null) {
    // A finished restore without a completion is said to be without one, never
    // shown as zero: the controller copies `status.completion` only once the
    // signed scorecard verifies, and a zero row would read as "nothing was
    // restored". A run still going has no panel at all.
    if (v.kind !== "restore" || !finished) {
      return "";
    }
    return "<section class=\"completion\"><h3>What this restore produced</h3>" +
      "<p class=\"note\" data-completion=\"unverified\">" + esc(COMPLETION_NOT_VERIFIED) +
      "</p></section>";
  }
  const sampleWindow = c.sampleWindow || null;
  const topics = Array.isArray(c.newTopics) ? c.newTopics : [];
  const guidance = COMPLETION_GUIDANCE[String(v.targetMode)];
  return (
    "<section class=\"completion\"><h3>What this restore produced</h3>" +
    table(
      ["TOPIC", "PARTITIONS"],
      topics.map((t) => [cell((t || {}).name), cell((t || {}).partitions)]),
      "This restore recorded no created topic.",
    ) +
    facts([
      ["records expected", cell(c.recordsExpected)],
      // `recordsRestored` is `sample.records_restored`: the records READ BACK in
      // the sampled window, not everything this restore wrote (D3 section 3.5).
      [RECORDS_IN_WINDOW_LABEL, cell(c.recordsRestored)],
      ["sampled window", sampleWindow === null || (!sampleWindow.start && !sampleWindow.end)
        ? ABSENT
        : cell(sampleWindow.start) + " to " + cell(sampleWindow.end) + " (inclusive)"],
      ["records sampled", cell(c.recordsSampled)],
      ["records sampled and matching", cell(c.recordsSampledMatching)],
      ["integrity level", cell(c.integrityLevel)],
    ]) +
    "<p class=\"scope\">" + esc(verificationScopeSentence(v.verificationScope)) + "</p>" +
    (typeof guidance === "string"
      ? "<p class=\"guidance\" data-target-mode=\"" + esc(String(v.targetMode)) + "\">" +
        esc(guidance) + "</p>"
      : "<p class=\"note\">This run records no target mode, so no cutover guidance is shown: " +
        "the guidance depends on which mode it used and this page will not guess one.</p>") +
    "</section>"
  );
}

/** The caption of `completion.recordsRestored`, which is a count of the
 *  SAMPLED WINDOW and never the total a restore wrote. */
export const RECORDS_IN_WINDOW_LABEL = "records verified in the sampled window";

/** What a SUCCEEDED restore with no `status.completion` shows instead of
 *  counts. A restore that did not succeed shows no completion section at all. */
export const COMPLETION_NOT_VERIFIED =
  "Completion not yet verified: the counts of what this restore produced are copied from its " +
  "signed scorecard only once that scorecard has verified, and it has not. Nothing here is a " +
  "zero; there is no count to show yet.";

/** The teardown a rehearsal recorded: what was deleted, and what was not. */
export function renderTeardown(v) {
  const t = v.teardown || null;
  if (t === null) {
    return "";
  }
  const deleted = Array.isArray(t.deleted) ? t.deleted : [];
  const failed = Array.isArray(t.failed) ? t.failed : [];
  return (
    "<section class=\"teardown\"><h3>Teardown</h3>" +
    facts([
      ["attestation key", cell(t.attestationKey)],
      // A list and not a count, with a flag for when it is the bounded head of a
      // longer one: "which topics went" is the incident question, and a silently
      // truncated list would answer it wrongly and confidently.
      ["deleted", deleted.length === 0 ? ABSENT : esc(deleted.join(", "))],
      ["deleted list truncated", cell(t.deletedTruncated)],
    ]) +
    table(
      ["TOPIC", "ERROR"],
      failed.map((f) => [cell((f || {}).topic), cell((f || {}).error)]),
      "Every topic this teardown named was deleted.",
    ) +
    "</section>"
  );
}

/** The whole view. `view` is `{ns, kind, name, uid, document, console, meta,
 *  error}`. */
export function renderOperation(view) {
  const v = view || {};
  const heading = "<h2>Operation " + esc(String(v.name || "")) + "</h2>" +
    "<p class=\"blurb\">" + esc(String(v.kind || "")) + " in namespace " +
    esc(String(v.ns || "")) + ". " + esc(WATCH_SENTENCE) + "</p>";
  if (v.mismatch === true) {
    return heading + "<p class=\"complaint\" data-uid-mismatch=\"true\">" +
      esc(DIFFERENT_RUN_SENTENCE) + "</p>";
  }
  if (v.document === null || v.document === undefined) {
    return heading + "<p class=\"pending\" role=\"status\">" + esc(READING_SENTENCE) + "</p>" +
      (v.error ? errorBlock(v.error) : "");
  }
  const f = operationFacts(v.document, v.console === true);
  const meta = v.meta || {};
  return (
    heading +
    renderState(f) +
    facts([
      ["reason", cell(f.reason)],
      ["message", cell(f.message)],
      ["last update", cell(f.lastUpdate)],
      ["created", when(f.createdAt)],
      ["uid", "<code>" + cell(f.uid) + "</code>"],
      ["awaiting approval", f.awaitingApproval
        ? "yes: waiting for approval; no Job runs until weirkeeper verifies it"
        : "no"],
      ["stage", cell(f.stage)],
      ["status too old to believe", f.console ? flagBadge(f.stale, "yes -- stale", "no") : ABSENT],
      ["object", f.name.length === 0
        ? ABSENT
        : detailLink(f.kind === "backup" ? "backups" : "history", String(v.ns || ""), f.name)],
    ]) +
    renderTargetMode(f) +
    "<p class=\"note\" data-transport=\"" + esc(String(meta.transport || "")) + "\">" +
    (meta.transport === "stream"
      ? "Following this operation as a stream."
      : "Following this operation by re-reading it; this build is not streaming it.") +
    (typeof meta.attempt === "number" && meta.attempt > 0
      ? " " + String(meta.attempt) + " connection attempt(s) have failed."
      : "") +
    "</p>" +
    (meta.error ? errorBlock(meta.error, false) : "") +
    renderProgress(f) +
    renderDiagnostics(f) +
    renderResult(f) +
    renderRetryAction(f, String(v.ns || "")) +
    renderEvidence(f) +
    renderCoverage(f) +
    renderCompletion(f) +
    renderTeardown(f)
  );
}

/** What the page's live region says when an operation moves: its name, its
 *  state (or, for a custom resource, its phase), its stage and its reason --
 *  the facts the view shows, in one sentence. Pure, so the suite reads it. */
export function operationAnnouncement(f, name) {
  const facts_ = f || {};
  const state = facts_.state || facts_.phase || "unknown";
  return (
    "Operation " + String(name || facts_.name || "") + ": " + String(state) +
    (facts_.stage ? ", stage " + String(facts_.stage) : "") +
    (facts_.reason ? " (" + String(facts_.reason) + ")" : "") + "."
  );
}

/** What `#/operations` says when the address names no run: it is one run's
 *  page, reached from a run, so a visit without one is pointed at the two
 *  lists every run is listed in. A kind this route does not serve is named.
 *  Pure. */
export function renderNoOperation(ns, kind) {
  const n = encodeURIComponent(String(ns || ""));
  const k = String(kind || "");
  return (
    "<h2>Operation</h2>" +
    "<div class=\"empty-state\" id=\"no-operation\">" +
    "<p class=\"note\">" +
    (k.length === 0
      ? "This page follows one backup or restore run. Open a run from its list to see where it " +
        "is, why, what it produced and whether its evidence verified."
      : "This page follows backup and restore runs, and the link that opened it named " +
        "&quot;" + esc(k) + "&quot;, which is neither.") +
    "</p>" +
    "<p class=\"actions\"><a class=\"button\" href=\"#/backups?ns=" + esc(n) +
    "\">Backups</a><a class=\"button\" href=\"#/history?ns=" + esc(n) +
    "\">History</a></p></div>"
  );
}

/** Reads one operation, renders it, and follows it until it settles. */
export async function mountOperation(node, ns, params, parse, deps, lifecycle) {
  const p = params || {};
  const kind = String(p.kind || "");
  const d = deps || {};
  if (OPERATION_KINDS.indexOf(kind) === -1) {
    replace(node, parse(renderNoOperation(ns, kind)));
    return null;
  }
  const console_ = inConsole(d.modeOf);
  const view = {
    ns: ns, kind: kind, name: String(p.name || ""), uid: String(p.uid || ""),
    document: null, console: console_, meta: {}, error: null, mismatch: false,
  };
  const paint = () => {
    if (active(lifecycle)) {
      replace(node, parse(renderOperation(view)));
    }
  };
  paint();
  const onUpdate = (document, meta) => {
    view.meta = meta || {};
    if (document !== null && document !== undefined) {
      const uid = console_
        ? document.uid
        : ((document.metadata || {}).uid);
      // The uid guard, on every document and not only the first: an object
      // deleted and recreated while this view is open would otherwise put the new
      // run's facts under the old run's heading.
      if (view.uid.length > 0 && typeof uid === "string" && uid.length > 0 && uid !== view.uid) {
        view.mismatch = true;
        view.document = null;
      } else {
        view.document = document;
        announce(operationAnnouncement(operationFacts(document, console_), view.name));
      }
    }
    paint();
  };
  try {
    const first = await readOperation(ns, kind, view.name, { signal: signalOf(lifecycle) }, d);
    onUpdate(first, { transport: "read", attempt: 0, error: null });
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      return null;
    }
    view.error = error;
    paint();
  }
  if (!active(lifecycle) || view.mismatch === true) {
    return null;
  }
  return watchOperation(ns, kind, view.name, onUpdate, lifecycle, d);
}

function signalOf(lifecycle) {
  return lifecycle === undefined || lifecycle === null ? undefined : lifecycle.signal;
}
