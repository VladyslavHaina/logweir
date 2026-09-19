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
  COMPLETION_GUIDANCE,
  HISTORICAL_SUFFIX,
  LEGACY_OPERATION_SENTENCE,
  STATE_UNKNOWN_SENTENCE,
  WATCH_SENTENCE,
  badge,
  cell,
  detailLink,
  diagnosticsTable,
  errorBox,
  esc,
  facts,
  phaseBadge,
  replace,
  stateBadge,
  table,
  unverifiedCaption,
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
      reason: o.stateReason || null,
      message: o.message || null,
      lastUpdate: o.lastUpdate || o.lastUpdatedAt || null,
      createdAt: o.createdAt || null,
      awaitingApproval: o.awaitingApproval === true,
      readiness: o.readiness || null,
      progress: o.progress || null,
      diagnostics: diagnosesOf(o),
      result: o.result || null,
      evidence: o.evidence || null,
      verification: o.verification || null,
      evidenceVerification: o.evidenceVerification || null,
      verifiedSuccess: o.verifiedSuccess === true,
      verificationScope: o.verificationScope || null,
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
    progress: progress,
    diagnostics: diagnosesOf(o),
    phase: status.phase || null,
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
  const runner = p.runner || {};
  const phase = p.runnerPhase || {};
  return (
    "<section class=\"progress\"><h3>Progress</h3>" +
    facts([
      ["stage", cell(p.stage)],
      ["reason", cell(p.reason)],
      ["message", cell(p.message)],
      ["last transition", cell(p.lastTransitionTime)],
      ["last observed", cell(p.lastObservedTime)],
      ["runner phase", typeof phase.name === "string" && phase.name.length > 0
        ? cell(phase.number) + " " + cell(phase.name)
        : ABSENT],
      ["job", cell(runner.jobName)],
      ["pod", cell(runner.podName) + " " + cell(runner.podPhase)],
      ["container", cell(runner.containerState) + " " + cell(runner.waitingReason)],
      ["runner started", cell(runner.startedAt)],
    ]) +
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

/** THE RESULT: what the RUN did. Never the evidence, which is the next block. */
export function renderResult(v) {
  const r = v.result || {};
  return (
    "<section class=\"result\"><h3>Result</h3>" +
    facts([
      ["result", cell(r.status)],
      ["exit code", cell(r.exitCode)],
      ["exit reason", cell(r.exitReason)],
      ["outcome", cell(r.outcome)],
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
export function renderEvidence(v) {
  const e = v.evidence || {};
  const ver = v.evidenceVerification || {};
  const trust = ver.trust || {};
  const policy = trust.policy || {};
  const green = ver.result === "Valid" &&
    (typeof trust.basis !== "string" || trust.basis === "Current" || trust.basis === "Historical");
  const mark = green
    ? badge(
      "green",
      "verified by weirkeeper at " + String(ver.verifiedAt || "") + " against key " +
        String(ver.matchedKeyId || "") +
        (trust.basis === "Historical" ? HISTORICAL_SUFFIX : ""),
    )
    : badge("unverified", unverifiedCaption(ver, v.verifiedSuccess));
  return (
    "<section class=\"evidence\"><h3>Evidence</h3>" +
    mark +
    facts([
      ["recorded result", cell(ver.result)],
      ["matched key id", cell(ver.matchedKeyId)],
      ["payload type", cell(ver.payloadType)],
      ["verified at", cell(ver.verifiedAt)],
      ["signed at (the document's own claim)", cell(ver.signedAt)],
      ["trust basis", cell(trust.basis)],
      ["key state", cell(trust.keyState)],
      ["trust policy", cell(policy.name) +
        (typeof policy.generation === "number" ? " g" + String(policy.generation) : "")],
      ["detail", cell(ver.detail)],
      ["receipt key", cell(e.receiptKey || e.payloadKey)],
      ["scorecard key", cell(e.scorecardKey)],
      ["sidecar key", cell(e.sidecarKey)],
      ["offset report key", cell(e.offsetReportKey)],
    ]) +
    "</section>"
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
    return "";
  }
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
      ["records restored", cell(c.recordsRestored)],
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
      ["deleted", deleted.length === 0 ? ABSENT : esc(deleted.join(", "))],
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
      (v.error ? errorBox(v.error) : "");
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
      ["object", f.name.length === 0
        ? ABSENT
        : detailLink(f.kind === "backup" ? "backups" : "history", String(v.ns || ""), f.name)],
    ]) +
    "<p class=\"note\" data-transport=\"" + esc(String(meta.transport || "")) + "\">" +
    (meta.transport === "stream"
      ? "Following this operation as a stream."
      : "Following this operation by re-reading it; this build is not streaming it.") +
    (typeof meta.attempt === "number" && meta.attempt > 0
      ? " " + String(meta.attempt) + " connection attempt(s) have failed."
      : "") +
    "</p>" +
    (meta.error ? errorBox(meta.error) : "") +
    renderProgress(f) +
    renderDiagnostics(f) +
    renderResult(f) +
    renderEvidence(f) +
    renderCompletion(f) +
    renderTeardown(f)
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
