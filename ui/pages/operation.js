// pages/operation.js -- THE DURABLE OPERATION VIEW (PLAT-12.1, PLAT-14.1).
//
// ONE SUBMISSION, ONE PLACE TO WATCH IT, AND A ROUTE THAT SURVIVES A RELOAD.
// Everything a person needs after clicking "Back up now" or submitting a
// Restore is here: where the run is, why it is there, what the controller
// could see, what it produced and whether the evidence verified. The route
// carries the whole identity -- `#/operations?ns=&kind=&name=&uid=` -- so a
// refresh, a new tab and a link pasted into an incident channel all open the
// same run. Nothing is kept in this page; the custom resource is the truth.
//
// FOUR THINGS THIS VIEW REFUSES TO DO.
//
//   1. IT COMPUTES NO STATE. In console mode the normalized `state` is one of
//      ten words `logweir-api` derives from D3 section 2.5's table, and this page
//      prints the API's word. In legacy mode there IS no normalized state, and
//      this page says so and shows `status.progress.stage` -- the controller's
//      own -- rather than implementing that table a second time in a browser.
//   2. IT NEVER READS A CLOCK FOR A VERDICT. `lastUpdate`, `lastObservedTime`
//      and `lastTransitionTime` are printed as the instants they are. Whether
//      an active stage has gone stale is a decision the API makes against a
//      clock the cluster shares; a page that made it from `Date.now()` would
//      disagree with the cluster by exactly the skew between them.
//   3. IT RENDERS THE RESULT AND THE EVIDENCE SEPARATELY, AND NEVER MERGES
//      THEM. "The run exited 0" and "the document it signed verifies" are two
//      facts about two things, and a `succeeded` result whose evidence is
//      `NotAttempted` is never shown as a verified success (D3 section 2.5).
//   4. IT CANCELS NOTHING. Leaving this route closes the watch's connection.
//      The Job keeps running; there is no cancel route in v1 and this page
//      offers no control that pretends there is.
//
// THE UID IN THE ROUTE IS A GUARD AND NOT DECORATION. A name is reused: a
// Backup deleted and recreated under the same name is a different run with
// different evidence. When the route carries a `uid` and the object answers to
// a different one, this page says "this name now refers to a different run"
// and renders nothing about it, rather than silently switching to the new one
// under a reader who came from a link about the old one.

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
  phaseBadge,
  replace,
  scorecardClaim,
  stateBadge,
  table,
  unverifiedCaption,
  unverifiedTrustCaption,
  verificationScopeSentence,
} from "../render.js";
import { active, cancelled } from "../lifecycle.js";
import { inConsole, readOperation, watchOperation } from "../operation-watch.js";

/** The route one operation lives at. BOTH HALVES OF THE IDENTITY TRAVEL: the
 *  name says which object to read and the uid says which RUN the link was
 *  about. */
export function operationRoute(ns, kind, name, uid) {
  return (
    "#/operations?ns=" + encodeURIComponent(String(ns || "")) +
    "&kind=" + encodeURIComponent(String(kind || "")) +
    "&name=" + encodeURIComponent(String(name || "")) +
    (typeof uid === "string" && uid.length > 0 ? "&uid=" + encodeURIComponent(uid) : "")
  );
}

/** The three values the route carries, out of the hash string.
 *
 *  A PURE FUNCTION OF THE HASH, exported for the same reason
 *  `restoreRouteParams` is: `ui/app.js` touches `window` at module scope and
 *  cannot be imported under `node --test`, so an extraction written inline
 *  there would be a hop no test could reach. */
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
 *  Both documents go through here and come out as the same bag of ALREADY
 *  RECORDED values. What differs between the modes is which fields EXIST, and
 *  that difference is carried honestly: `state` is `null` in legacy mode
 *  because the custom resource has none, and `null` renders as a sentence
 *  saying which API computes it -- never as `unknown`, which is one of the ten
 *  words and would be a verdict this page invented. */
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
      // SELECTED AND DELIBERATELY NOT PRINTED, which is a decision and not an
      // oversight. `ReadinessView` is published as required on every
      // OperationView and `ReadinessView::not_implemented()` is the only value
      // `logweir-api` can produce for it today: `{state: "unknown", basis:
      // "notImplemented"}`, until PLAT-03.1 lands. A row saying "readiness
      // unknown, because it is not implemented" on every run is noise a reader
      // learns to skip, and skipping is what makes the row useless on the day
      // it starts carrying an answer. It is kept in the facts so the page that
      // renders it then has it, and the field is asserted by `contract.js`.
      readiness: o.readiness || null,
      stage: o.stage || null,
      progress: o.progress || null,
      diagnostics: diagnosesOf(o),
      result: o.result || null,
      evidence: o.evidence || null,
      verification: o.verification || null,
      // THE CONSOLE'S VERDICT IS ONE WORD AND IT ARRIVES COMBINED (D3
      // section 2.5). `trust.state` is the API's own answer over the
      // controller's `result` and `trust.basis`; the basis, the key state and
      // the policy travel beside it as the facts behind that answer.
      trust: o.trust || null,
      trustState: (o.trust || {}).state || null,
      evidenceVerification: null,
      verifiedSuccess: o.verifiedSuccess === true,
      verificationScope: o.verificationScope || null,
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
 *  `progress.runner` IS NOT PUBLISHED, DELIBERATELY. A Job name and a pod name
 *  are infrastructure detail; D0's visible list is reason, message, exit code,
 *  last phase, timestamps and evidence references, and a structural test on the
 *  API side keeps them off the contract. The custom resource DOES carry them
 *  and legacy mode renders them, because there the page is reading the object
 *  itself. What replaces them in console mode is the same thing an incident
 *  needs: `progress.reason`, `progress.message`, and a DIAGNOSTIC's own
 *  `object {kind, name}`, which IS published. */
export const NO_RUNNER_DETAIL_SENTENCE =
  "The product API publishes no Job or pod name for a run: they are infrastructure detail and " +
  "the console contract does not carry them. What the controller could see is in the diagnoses " +
  "below, each naming the object it is about.";

/** The state cell: the API's own word, or the sentence that says there is none
 *  in this mode. */
export function renderState(v) {
  if (v.state === null) {
    return "<p class=\"note\" data-no-normalized-state=\"true\">" +
      esc(LEGACY_OPERATION_SENTENCE) + "</p>" +
      (v.phase ? "<p class=\"phase\">controller phase " + phaseBadge(v.phase) + "</p>" : "");
  }
  return (
    "<p class=\"state\">" + stateBadge(v.state) + "</p>" +
    (v.state === "unknown" ? "<p class=\"note\">" + esc(STATE_UNKNOWN_SENTENCE) + "</p>" : "")
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
    ["last transition", cell(p.lastTransitionTime)],
    ["last observed", cell(p.lastObservedTime)],
    ["runner phase", typeof phase.name === "string" && phase.name.length > 0
      ? cell(phase.number) + " " + cell(phase.name)
      : ABSENT],
  ];
  if (runner !== null) {
    rows.push(["job", cell(runner.jobName)]);
    rows.push(["pod", cell(runner.podName) + " " + cell(runner.podPhase)]);
    rows.push(["container", cell(runner.containerState) + " " + cell(runner.waitingReason)]);
    rows.push(["runner started", cell(runner.startedAt)]);
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

/** THE ONE ACTION A FAILED RESTORE OFFERS: retry to a FRESH TARGET
 *  (PLAT-11.2, PLAT-12.2).
 *
 *  A LINK AND NOT A BUTTON, because nothing is retried from here: it opens the
 *  wizard, carrying which run failed, and the operator chooses the point and
 *  reviews a whole new plan. The retry is a NEW Restore with a new topic
 *  prefix, new plan bytes, a new plan hash and -- because both names are
 *  minted from those bytes -- a new Restore name and a new Approval name. The
 *  failed run's Approval is not reused and cannot be reached from here; where
 *  the namespace's policy is governed the new restore waits for an approval of
 *  its own, exactly as a first restore does.
 *
 *  AND NOTHING IS OFFERED FOR A BACKUP OR FOR A RUN THAT HAS NOT FAILED. A
 *  retry of a running restore would be a second execution against the same
 *  target names; a retry of a successful one is a restore, and the ordinary
 *  wizard is where that starts. */
export function renderRetryAction(v, ns) {
  if (v.kind !== "restore") {
    return "";
  }
  // BOTH MODES, AND EACH READ IN ITS OWN VOCABULARY. The console DTO says
  // `terminal` and a lower-case `state`; the custom resource says
  // `status.phase` with a capital, and its projection leaves `terminal` false
  // because nothing there computes one. A predicate that read only the first
  // would silently offer nothing behind `kubectl proxy`, which is where an
  // incident without the product API happens.
  // `failed` AND `refused`, because the product API has two terminal words for
  // one operator situation and both are work that will never proceed. A
  // terminal refusal -- `ApprovalSubjectMismatch`, `PlanHashMismatch` -- is in
  // fact the SHARPER case for this affordance: that Restore's name and its
  // Approval name are already taken, so a retry of the same point with the
  // same prefix would mint exactly them again and collide, which is the defect
  // PLAT-12.2 names. The custom resource spells both `Failed` in
  // `status.phase`, so the legacy arm needs no second word. (A live journey
  // found this: the lab refused a Restore terminally and the affordance,
  // written for `failed` alone, was not offered.)
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
    "<p class=\"actions\"><a id=\"retry-fresh-target\" href=\"" +
    esc(retryRoute(String(ns || ""), String(v.name || ""))) +
    "\">Retry to a fresh target</a></p></section>"
  );
}

/** The wizard's selector, carrying which failed run is being retried.
 *
 *  SPELLED HERE AND NOT IMPORTED, ON PURPOSE. The wizard owns this route and
 *  exports the same helper (`restore-wizard.js::restoreRetryFromOperationRoute`),
 *  but importing that module here would pull `ui/plan.js` into the operation
 *  page's module graph -- and `plan.js` THROWS AT LOAD outside a secure
 *  context. A page that only reads a run would then stop rendering on an
 *  origin where it renders today, to support a link. So the four-token route
 *  is written out, and `pages.spec.js` asserts this function and the wizard's
 *  helper produce the identical string, which is the property that could
 *  otherwise drift.
 *
 *  It names NO recovery point: `Restore.spec` carries a backup SET id and no
 *  reference to the `Backup` it came from, so the point is chosen on the
 *  selector rather than guessed here. */
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

/** THE EVIDENCE: what the CONTROLLER checked, with the case named.
 *
 *  Three results, three different claims (D3 section 7.4): `Invalid` is about the
 *  DOCUMENT, `NotAttempted` is about the CONTROLLER, and `Untrusted` is about
 *  the SIGNER. They are never flattened into one word here. */
/** Whether this run's evidence is green, by the evidence section's own rule:
 *  the API's combined trust word in console mode, `result` + `basis` in
 *  legacy mode.
 *
 *  THE BASIS STILL VETOES IN CONSOLE MODE (CONSOLE-DETAIL-TRUST-BASIS-DROPPED).
 *  `logweir-api` projects a `Valid` verdict on `RecordedBeforeRevocation` as
 *  `trust.state: verified` (`crates/logweir-api/src/status.rs`, `trust_of`),
 *  and D3 section 7.4 says that case is "never green". So a green word is
 *  green only on a basis a green badge may carry -- the SAME
 *  [`basisAllowsGreen`] legacy mode applies -- and the two modes cannot
 *  disagree about one run. */
export function evidenceGreen(v) {
  const console_ = v.console === true;
  const trust = (console_ ? v.trust : ((v.evidenceVerification || {}).trust)) || {};
  const ver = (console_ ? v.verification : v.evidenceVerification) || {};
  return console_
    ? (GREEN_TRUST_STATES.indexOf(v.trustState) !== -1 && basisAllowsGreen(trust.basis))
    : (ver.result === "Valid" && basisAllowsGreen(trust.basis));
}

/** The caption of a console evidence badge that is not green. A green trust
 *  word the basis vetoed is named by the basis -- the words legacy mode uses
 *  for the same `Valid` + basis -- and every other word by its own case. */
function consoleUnverifiedCaption(v, trust) {
  if (GREEN_TRUST_STATES.indexOf(v.trustState) !== -1 && !basisAllowsGreen(trust.basis)) {
    return unverifiedCaption({ result: "Valid", trust: trust }, v.verifiedSuccess);
  }
  return unverifiedTrustCaption(v.trustState, v.verifiedSuccess);
}

export function renderEvidence(v) {
  const e = v.evidence || {};
  // TWO DOCUMENTS, TWO RULES, EACH READING WHAT ITS OWN DOCUMENT CARRIES.
  //
  // In CONSOLE mode the verdict arrives already combined: `trust.state` is D3
  // section 2.5's own word, which `logweir-api` computed from the controller's
  // `result` and its `trust.basis`. Reading it is the whole point of asking a
  // normalizing API for a normalized status, and re-deriving it here would be
  // that table implemented a second time in a browser.
  //
  // In LEGACY mode there is no such word -- the custom resource carries
  // `result` and a PascalCase `basis` -- so the page keeps its own rule, which
  // is the same rule the badge on `#/backups` and `#/history` uses.
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
      ["verified at", cell(ver.verifiedAt)],
      ["signed at (the document's own claim)", cell(trust.signedAt || ver.signedAt)],
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
 *  THE RECONCILIATION MOVED `targetMode` TO THE TOP LEVEL AND THIS IS WHAT
 *  READS IT THERE. The published DTO says why in its own field documentation:
 *  D3 section 3.5 keys its guidance on `spec.target.mode`, "and that is a fact
 *  about the run from the moment it is created -- a rehearsal is a rehearsal
 *  before its scorecard exists. The first round hid it inside `completion`, so
 *  a Restore that had not finished could not be labelled." The console had the
 *  field at the top level after the renames and still read it ONLY inside the
 *  completion panel, which returns nothing at all without a scorecard -- so a
 *  running, pending or refused rehearsal was unlabelled on screen, which is
 *  the one case where "these topics are deleted by teardown" is worth saying
 *  BEFORE the fact.
 *
 *  It is deliberately NOT the completion guidance. That guidance is about what
 *  a run produced and stays beside the scorecard; this says what the run IS. */
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

/** THE COMPLETION PANEL (D3 section 3.5): what a terminal Restore actually produced,
 *  the sampled counts labelled exactly, and the fixed guidance for the target
 *  mode this restore used.
 *
 *  The guidance sentences are `ui/render.js`'s and never the server's: a
 *  console that rendered server-authored prose here would be putting an
 *  instruction in front of an operator that no review in this repository ever
 *  read. */
export function renderCompletion(v) {
  const c = v.completion || null;
  if (c === null) {
    // A FINISHED RESTORE WITHOUT A COMPLETION IS SAID TO BE WITHOUT ONE, NEVER
    // SHOWN AS ZERO. The controller copies `status.completion` only once the
    // run's signed scorecard has verified; before that there are no counts to
    // show, and an empty or zero row would read as "nothing was restored". A
    // run still going has no panel at all, which is the same honesty.
    //
    // A RUN THAT DID NOT SUCCEED IS NOT "NOT YET" ANYTHING (review LOW-3): a
    // failed, refused or cancelled restore will never write the counts this
    // panel shows, and "not yet verified" would read as a pending state.
    const finished = v.terminal === true ||
      ["Succeeded", "Failed", "Cancelled"].indexOf(String(v.phase)) !== -1;
    if (v.kind !== "restore" || !finished) {
      return "";
    }
    const succeeded = v.phase === "Succeeded" || v.state === "succeeded";
    return "<section class=\"completion\"><h3>What this restore produced</h3>" +
      (succeeded
        ? "<p class=\"note\" data-completion=\"unverified\">" + esc(COMPLETION_NOT_VERIFIED)
        : "<p class=\"note\" data-completion=\"none\">" + esc(COMPLETION_NOT_RECORDED)) +
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
      // `recordsRestored` IS `sample.records_restored`: the records READ BACK
      // in the sampled window, not everything this restore wrote (the CRD's
      // field description, D3 section 3.5). Labelled as what it is, with the
      // window beside it.
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

/** What a finished restore with no `status.completion` shows instead of counts. */
/** What a restore that did not succeed shows where the counts would be. */
export const COMPLETION_NOT_RECORDED =
  "No completion was recorded for this run: it did not succeed, so there are no counts of what " +
  "it produced. Its result and its evidence are above.";

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
      // A LIST AND NOT A COUNT, and the flag beside it says when the list is
      // the bounded head of a longer one: "which topics went" is the incident
      // question, and a truncated list that did not say so would answer it
      // wrongly and confidently.
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
      ["created", cell(f.createdAt)],
      ["uid", "<code>" + cell(f.uid) + "</code>"],
      ["awaiting approval", f.awaitingApproval ? "yes" : "no"],
      ["stage", cell(f.stage)],
      ["status too old to believe", f.console ? cell(f.stale) : ABSENT],
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

// --------------------------------------------------------------- mount half

/** Reads one operation, renders it, and follows it until it settles. */
export async function mountOperation(node, ns, params, parse, deps, lifecycle) {
  const p = params || {};
  const kind = String(p.kind || "");
  const d = deps || {};
  if (OPERATION_KINDS.indexOf(kind) === -1) {
    replace(node, parse(
      "<h2>Operation</h2><p class=\"complaint\">This route serves " +
      esc(OPERATION_KINDS.join(" and ")) + " operations. The address bar named " +
      esc(kind.length === 0 ? "none" : kind) + ".</p>",
    ));
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
      // THE UID GUARD, APPLIED TO EVERY DOCUMENT AND NOT ONLY THE FIRST. An
      // object deleted and recreated while this view is open would otherwise
      // start rendering the new run's facts under the old run's heading.
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
