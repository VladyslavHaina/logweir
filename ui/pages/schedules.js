// pages/schedules.js -- the BackupSchedule list, the create form, the suspend
// toggle, and the retention panel.
//
// THE SUSPEND TOGGLE IS THE ONE UPDATE THIS PAGE MAKES, and `api.patchSuspend`
// is the one function that can make it: a JSON-merge patch touching
// `spec.suspend` and nothing else. Every other field of a BackupSchedule's
// spec is sealed by an object-level CEL rule for every subject, cluster-admin
// included, so a generic patch helper here would not widen what the API server
// accepts -- it would widen what this page can try, which is the half a review
// of the page can still see. `the_suspend_toggle_is_the_only_update` asserts
// that the api.js identifiers reachable from any page module are exactly the
// six readers and writers this product has.
//
// THE RETENTION PANEL REPORTS AND NEVER DELETES. Logweir holds no delete
// capability of any kind against an archive: the controller's archive handle
// is opened read-only and its evidence credential is read-only (Global
// Constraint 6). What `status.retentionReport` carries is an EVALUATION -- the
// sets a policy keeps, the sets it would remove and why -- plus the exact
// removal command for each set, in the CLI of that archive's own scheme, as a
// string. The panel prints those strings verbatim. It runs none of them, and
// nothing in this product runs any of them.
//
// BOTH WRITES SHARE ONE MUTATION STATE (PLAT-13.2). The create keeps its draft
// through every recoverable failure and is idempotent by name; the toggle
// refuses a second click while its patch is pending and reports a refusal in
// the schedule's own card, leaving the rest of the page -- the create form's
// draft included -- where it was.
//
// THE SOURCE IS CHOSEN, NOT TYPED (PLAT-07.2). `spec.sourceRef.name` used to be
// a free-text input, so a typo was a schedule the controller could not resolve
// and a correct name was a reference that silently followed whatever object
// held that name next. It is now `../select.js`'s searchable selector: the
// draft keeps the chosen cluster's UID beside its name, the UID is what the
// form is bound to, and a UID that no longer resolves is a REFUSAL naming it
// -- including the case where a different object has since taken that name,
// which is what a KafkaCluster deleted and recreated looks like from here. The
// request body still spells `sourceRef.name`, because that is what the CRD
// takes; the name it spells is the one the resolved object carries NOW, so a
// cluster renamed since the draft was started is sent correctly rather than
// under its old name.

import { apiClient, mayOperate } from "../client.js";
import {
  active,
  cancelled,
  createOnce,
  dropDraft,
  fieldErrors,
  formKey,
  invalidInput,
  keepDraft,
  listen,
  mutationFor,
  readDraft,
  readOptions,
  refusal,
  watchMutation,
} from "../lifecycle.js";
import { INCOMPLETE_DISCOVERY_POLICIES } from "../contract.js";
import {
  ABSENT,
  ENFORCEMENT_DEGRADED_SENTENCE,
  ENFORCEMENT_SENTENCES,
  GUARANTEE_WORDS,
  IRREVERSIBLE_SENTENCE,
  RETENTION_SENTENCE,
  SUPERSEDED_SENTENCE,
  badge,
  cell,
  copyBlock,
  detailLink,
  errorBlock,
  errorBox,
  esc,
  facts,
  fieldErrorLine,
  invalidAttributes,
  listFooter,
  mutationStatus,
  nextRunsPanel,
  phaseBadge,
  replace,
  revisionLine,
  rfc3339,
  table,
  triggerBadge,
} from "../render.js";
import {
  clusterName,
  clusterUid,
  filterSelectorOptions,
  readClusterSelection,
  renderClusterSelector,
  resolveClusterSelection,
} from "../select.js";
import { focusFirstProblem, isObjectName, itemsOf, readFormValues } from "./clusters.js";
import { renderPreflight } from "./destinations.js";
import { isRecoveryPoint, recoveryPoints, restorePointRoute } from "./restore-wizard.js";
import { listD3 } from "../operation-watch.js";
import { operationRoute } from "./operation.js";

const PLURAL = "backupschedules";
const BACKUPS = "backups";
const CLUSTERS = "kafkaclusters";

const API = apiClient();

/** The create form's identity in the draft and mutation registries. */
export const SCHEDULE_FORM = "schedule-form";

/** The suspend toggle's identity; each schedule has its own record. */
export const SUSPEND_FORM = "schedule-suspend";

/** The fields a draft of the create form keeps: names, a cron line, a topic
 *  list, an archive URL, a Secret NAME and two numbers. No credential. */
export const SCHEDULE_DRAFT_FIELDS = Object.freeze([
  "name", "cron", "source", "sourceUid", "topics", "archive", "archiveSecret",
  "keepLast", "keepDays",
]);

/** The API server's field paths, mapped to the create form's inputs. */
export const SCHEDULE_FIELD_PATHS = Object.freeze([
  ["metadata.name", "name"],
  ["spec.schedule", "cron"],
  ["spec.sourceRef", "source"],
  ["spec.topics", "topics"],
  ["spec.archive.url", "archive"],
  ["spec.archive.secretRef", "archiveSecret"],
  ["spec.archive", "archive"],
  ["spec.retention.keepLast", "keepLast"],
  ["spec.retention.keepDays", "keepDays"],
]);

/** The defaults the CRD applies to an omitted field, and the ONE field that
 *  may change after creation -- so a schedule someone suspended since is still
 *  the schedule this draft describes. */
const SCHEDULE_SPEC_RULES = Object.freeze({
  defaults: { concurrencyPolicy: "Forbid", suspend: false },
  ignore: ["suspend"],
});

function nameOf(object) {
  const meta = (object && object.metadata) || {};
  return cell(meta.name);
}

/** The `status` of the condition of this type, or the absent marker. */
export function conditionStatus(object, type) {
  const status = (object && object.status) || {};
  const conditions = Array.isArray(status.conditions) ? status.conditions : [];
  for (const condition of conditions) {
    if (condition && condition.type === type) {
      return cell(condition.status);
    }
  }
  return cell(null);
}

/** `spec.suspend` as a badge that says the state in words: `suspended`, or
 *  `not suspended`. The field is a boolean the operator set; the badge
 *  renders it and decides nothing. */
export function suspendBadge(spec) {
  return (spec || {}).suspend === true
    ? badge("warn", "suspended")
    : badge("flat", "not suspended");
}

/** The sentence the schedules table carries when the namespace holds none. */
export const NO_SCHEDULE_SENTENCE =
  "No BackupSchedule in this namespace yet. Create one with the form below; it fires a " +
  "Backup at each slot of its cron schedule.";

/** The schedules table. NAME, SCHEDULE, SUSPEND, LAST, NEXT, READY. */
export function renderScheduleList(input, destinations) {
  const rows = itemsOf(input).map((object) => {
    const spec = object.spec || {};
    const status = object.status || {};
    return [
      nameOf(object),
      "<code>" + cell(spec.schedule) + "</code>",
      destinationCell(object, destinations),
      suspendBadge(spec),
      cell(status.lastFireTime),
      cell(status.nextFireTime),
      conditionStatus(object, "Ready"),
    ];
  });
  return (
    "<h2>Schedules</h2>" +
    "<p class=\"blurb\">Every BackupSchedule in this namespace. " +
    "<code>suspend</code> is the only field of a schedule's spec that THIS PAGE can change " +
    "after it is created.</p>" +
    table(
      ["NAME", "SCHEDULE", "DESTINATION", "SUSPEND", "LAST", "NEXT", "READY"],
      rows,
      NO_SCHEDULE_SENTENCE,
    ) +
    listFooter()
  );
}

/** WHERE A SCHEDULE WRITES, in one cell, and by IDENTITY where it can be.
 *
 *  A schedule that names a destination (`spec.destinationRef`, PLAT-06.2) is
 *  resolved against the destinations this page actually read, with the same
 *  rules the selector uses: a name that resolves is shown with its location, a
 *  name that resolves to nothing is a REFUSAL to claim where this schedule
 *  writes -- because a destination deleted and recreated under one name is a
 *  different archive location reached with a different credential, and a cell
 *  that printed the new one would be answering a question about the old.
 *
 *  A schedule with no `destinationRef` carries an inline archive, which is
 *  what every schedule written before PLAT-06.2 carries, and the cell says so
 *  rather than leaving a blank that reads as "none". */
export function destinationCell(object, destinations) {
  const spec = (object || {}).spec || {};
  const ref = spec.destinationRef;
  const name = ref === null || ref === undefined ? "" : String(ref.name || "");
  if (name.length === 0) {
    const url = (spec.archive || {}).url;
    return typeof url === "string" && url.length > 0
      ? badge("pending", "inline archive") + " <code>" + esc(url) + "</code>"
      : ABSENT;
  }
  const all = Array.isArray(destinations) ? destinations : [];
  const found = all.find((d) => d.name === name);
  if (found === undefined) {
    return badge("unverified", "names " + name) +
      " <span class=\"note\">no destination of that name is in this namespace now, so this " +
      "page will not say where this schedule writes</span>";
  }
  return badge("green", found.name) + " <code>" + esc(found.canonicalUrl) + "</code>";
}

/** The suspend toggle's control for one schedule, disabled while its patch is
 *  pending. `state` is that schedule's own mutation record. */
export function renderSuspendToggle(object, state) {
  const spec = (object && object.spec) || {};
  const name = ((object && object.metadata) || {}).name || "";
  const suspended = spec.suspend === true;
  const pending = (state || {}).phase === "pending";
  return (
    "<form class=\"suspend\" data-name=\"" + esc(name) + "\" data-next=\"" +
    (suspended ? "false" : "true") + "\"" + (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<button type=\"submit\"" + (pending ? " disabled" : "") + ">" +
    (suspended ? "Resume" : "Suspend") +
    "</button></form>"
  );
}

/** The words under a toggle: pending, or what the API server said. Success
 *  needs no words -- the badge above changes.
 *
 *  THE STATUS IS TOLD WHAT THE REQUEST WAS. This region renders a PATCH on an
 *  object that already exists, not a create, so it hands `mutationStatus` the
 *  verb and the one field the patch sets: an unknown outcome here is safe to
 *  repeat because repeating it sets the same field to the same value, and not
 *  because a name is reused and a duplicate would be recognised -- which is
 *  what the create sentence says, and is not true of this request.
 *
 *  `next` is read the same way `renderSuspendToggle` reads it, from the object
 *  the page last listed: the patch that is pending or failed has not changed
 *  that object, so its `spec.suspend` is still the value the click inverted. */
export function renderSuspendStatus(object, state) {
  const s = state || {};
  const name = ((object && object.metadata) || {}).name || "";
  if (s.phase !== "pending" && s.phase !== "failed") {
    return "";
  }
  const next = ((object && object.spec) || {}).suspend === true ? "false" : "true";
  return mutationStatus(s, {
    kind: "BackupSchedule",
    name: name,
    verb: "patch",
    field: "spec.suspend",
    value: next,
  });
}

/** One removable set as a line an operator can read. */
function removableLine(set) {
  const s = set || {};
  const why = [];
  if (typeof s.reason === "string") {
    why.push(s.reason);
  }
  if (typeof s.days === "number") {
    why.push("keepDays " + s.days);
  }
  if (typeof s.rank === "number") {
    why.push("rank " + s.rank);
  }
  return (
    cell(s.backupId) +
    " (newest record " + cell(s.newestRecordAt) + ")" +
    (why.length === 0 ? "" : " -- " + esc(why.join(", ")))
  );
}

/** THE RETENTION PANEL for one schedule.
 *
 *  The kept sets, the sets that WOULD be removed with their reason, and the
 *  removal commands the status carries -- `awsCli` in that archive's own
 *  scheme and `mcCli` in `mc`'s spelling -- printed verbatim in a block a
 *  viewer copies, above the sentence that says who would be running them. */
export function renderRetentionPanel(object, policy) {
  const status = (object && object.status) || {};
  const report = status.retentionReport;
  const enforcement = renderEnforcement(report, policy);
  if (!report) {
    return (
      "<section class=\"retention\"><h3>Retention</h3>" +
      "<p class=\"note\">No retention evaluation has been recorded for this schedule.</p>" +
      enforcement +
      retentionSentenceFor(report, policy) + "</section>"
    );
  }
  const kept = Array.isArray(report.setsKept) ? report.setsKept : [];
  const removable = Array.isArray(report.setsThatWouldBeRemoved)
    ? report.setsThatWouldBeRemoved
    : [];
  const skipped = Array.isArray(report.skipped) ? report.skipped : [];
  const awsCli = Array.isArray(report.awsCli) ? report.awsCli : [];
  const mcCli = Array.isArray(report.mcCli) ? report.mcCli : [];

  const keptList = kept.length === 0
    ? "<p class=\"note\">none</p>"
    : "<ul class=\"sets\">" + kept.map((id) => "<li>" + esc(id) + "</li>").join("") + "</ul>";
  const removableList = removable.length === 0
    ? "<p class=\"note\">none</p>"
    : "<ul class=\"sets\">" +
      removable.map((s) => "<li>" + removableLine(s) + "</li>").join("") +
      "</ul>";
  const skippedList = skipped.length === 0
    ? ""
    : "<h4>Manifests that could not be read</h4><ul class=\"sets\">" +
      skipped
        .map((s) => "<li>" + cell((s || {}).key) + " -- " + cell((s || {}).reason) + "</li>")
        .join("") +
      "</ul>";

  return (
    "<section class=\"retention\"><h3>Retention</h3>" +
    enforcement +
    facts([
      ["evaluated at", cell(report.evaluatedAt)],
      ["keepLast applied", cell(report.keepLast)],
      ["keepDays applied", cell(report.keepDays)],
      ["note", cell(report.note)],
    ]) +
    "<h4>Kept</h4>" + keptList +
    "<h4>Would be removed -- still in the archive</h4>" + removableList +
    skippedList +
    "<h4>The commands</h4>" +
    copyBlock(awsCli.concat(mcCli)) +
    retentionSentenceFor(report, policy) +
    "</section>"
  );
}

// ===========================================================================
// D3 (PLAT-16.1, PLAT-16.2): WHICH SENTENCE THIS PANEL IS ALLOWED TO PRINT
// ===========================================================================
//
// THE OLD SENTENCE IS STILL TRUE, AND ONLY WHERE IT IS. "Logweir never deletes
// from your archive" was a statement about the whole product, and PLAT-16.2
// made it a statement about a MODE: a RetentionPolicy in `Enforce` runs an
// isolated worker that does delete, with its own delete-capable credential.
// Printing the old line beside a policy that is deleting nightly would be the
// most consequential false sentence this console could render.
//
// SO THE PANEL READS `status.enforcement` -- WHAT IS HAPPENING -- AND NOT
// `spec.mode`, WHICH IS WHAT WAS ASKED FOR. A policy in `mode: Enforce` whose
// destination will not resolve reports `RecommendationOnly`, and this panel
// says the thing that is true of it.
//
// AND THE LEGACY REPORT SAYS WHAT IT IS. A schedule's own
// `status.retentionReport` is a per-schedule RECOMMENDATION and nothing else;
// once a RetentionPolicy covers the same destination the report carries
// `supersededBy` and this panel says which evaluation counts.

/** The one sentence this panel prints about deletion, chosen by what is
 *  actually happening. `RETENTION_SENTENCE` verbatim for a schedule-level
 *  report and for `RecommendationOnly`; the mode's own sentence otherwise. */
export function retentionSentenceFor(report, policy) {
  const state = enforcementOf(policy);
  if (state === null || state === "RecommendationOnly") {
    return "<p class=\"never-deletes\">" + RETENTION_SENTENCE + "</p>";
  }
  return "<p class=\"never-deletes\" data-enforcement=\"" + esc(state) + "\">" +
    esc(ENFORCEMENT_SENTENCES[state] || "") + "</p>";
}

/** WHICH RetentionPolicy COVERS THIS SCHEDULE, FROM THE CONTROLLER'S OWN
 *  ANSWER AND NOT FROM A GUESS.
 *
 *  `status.retentionReport.supersededBy` is written by the schedule controller
 *  when a RetentionPolicy covers the same destination (D3 section 6.3). Matching on
 *  anything else -- a destination name, a bucket prefix -- would be this page
 *  deciding which policy governs a schedule, which is a decision with two
 *  possible answers (two policies for one destination are a `Conflict` and
 *  NEITHER evaluates) and one authority, and the authority is not a browser. */
export function policyForSchedule(object, policies) {
  const superseded = (((object || {}).status || {}).retentionReport || {}).supersededBy;
  const name = (superseded || {}).name;
  if (typeof name !== "string" || name.length === 0) {
    return null;
  }
  for (const policy of Array.isArray(policies) ? policies : []) {
    if (((policy || {}).metadata || {}).name === name) {
      return policy;
    }
  }
  return null;
}

/** `status.enforcement` off the RetentionPolicy covering this destination, or
 *  `null` when there is no policy to read one from. */
export function enforcementOf(policy) {
  const state = ((policy || {}).status || {}).enforcement;
  return typeof state === "string" && state.length > 0 ? state : null;
}

/** One condition out of a policy's list, by type. */
export function policyCondition(policy, type) {
  const conditions = ((policy || {}).status || {}).conditions;
  for (const condition of Array.isArray(conditions) ? conditions : []) {
    if ((condition || {}).type === type) {
      return condition;
    }
  }
  return null;
}

/** THE ENFORCEMENT BLOCK: which evaluation counts, what it guarantees, and --
 *  in `Enforce` -- the approved-plan state and the irreversibility sentence.
 *
 *  THE APPROVED-PLAN STATE IS THREE FACTS AND NOT A TICK. The digest an
 *  administrator approved, the digest the newest evaluation produced, and when
 *  that plan expires. A run happens only when the first two are equal and the
 *  plan is still young; printing "approved" alone would hide a plan that was
 *  approved and then superseded by a later evaluation, which is the case the
 *  two-step approval exists for. */
export function renderEnforcement(report, policy) {
  const superseded = (report || {}).supersededBy || null;
  const state = enforcementOf(policy);
  if (policy === null || policy === undefined) {
    return (
      "<p class=\"note\" data-enforcement=\"none\">" +
      esc(report && report.enforcement
        ? "This schedule's own report is " + String(report.enforcement) + ": a recommendation " +
          "about this schedule's sets and nothing more."
        : "No RetentionPolicy was read for this schedule's destination, so what is below is " +
          "this schedule's own recommendation and nothing else.") +
      (superseded === null
        ? ""
        : " " + esc(SUPERSEDED_SENTENCE) + " The policy is " + cell(superseded.name) + ".") +
      "</p>"
    );
  }
  const status = policy.status || {};
  const guarantees = status.guarantees || {};
  const spec = policy.spec || {};
  const enforcement = spec.enforcement || {};
  const evaluation = status.lastEvaluation || {};
  const degraded = policyCondition(policy, "EnforcementDegraded");
  const enforced = policyCondition(policy, "Enforced");
  const external = spec.externalLifecycle || {};
  return (
    "<div class=\"enforcement\" data-enforcement=\"" + esc(String(state)) + "\">" +
    "<h4>Enforcement</h4>" +
    (superseded === null ? "" : "<p class=\"note\">" + esc(SUPERSEDED_SENTENCE) + "</p>") +
    facts([
      ["policy", cell(((policy.metadata || {}).name))],
      ["mode asked for", cell(spec.mode)],
      ["what is happening", cell(state)],
      ["age expiry", esc(GUARANTEE_WORDS[guarantees.ageExpiry] || "")],
      ["minimum usable points", esc(GUARANTEE_WORDS[guarantees.minUsablePoints] || "")],
      ["active restore protection",
        esc(GUARANTEE_WORDS[guarantees.activeRestoreProtection] || "")],
      ["shared segments", esc(GUARANTEE_WORDS[guarantees.sharedSegments] || "")],
      ["legal hold", esc(GUARANTEE_WORDS[guarantees.legalHold] || "")],
    ]) +
    (state === "ExternalLifecycleDeclared"
      ? "<p class=\"note\">The declared rule is <code>" + cell(external.ruleId) +
        "</code> on " + cell(external.provider) + ", expiring objects after " +
        cell(external.expirationDays) + " day(s). Logweir cannot read a bucket lifecycle " +
        "configuration back, so every guarantee above that says so is a DECLARATION.</p>"
      : "") +
    (state === "LogweirWorker"
      ? "<h5>The approved plan</h5>" +
        facts([
          ["approval required", cell(enforcement.requireApprovedPlan)],
          ["digest an administrator approved", cell(enforcement.approvedPlanSha256)],
          ["digest of the newest evaluation", cell(evaluation.planSha256)],
          ["that plan expires at", cell(evaluation.planExpiresAt)],
          ["points it would remove", cell(evaluation.candidateCount)],
        ]) +
        (enforced === null
          ? ""
          : "<p class=\"note\">Enforced=" + esc(String(enforced.status)) + " " +
            esc(String(enforced.reason || "")) + ": " + esc(String(enforced.message || "")) +
            "</p>") +
        "<p class=\"irreversible\">" + esc(IRREVERSIBLE_SENTENCE) + "</p>"
      : "") +
    (degraded !== null && String(degraded.status) === "True"
      ? "<p class=\"complaint\" data-enforcement-degraded=\"true\">" +
        esc(ENFORCEMENT_DEGRADED_SENTENCE) + " " + esc(String(degraded.message || "")) + "</p>"
      : "") +
    "</div>"
  );
}

// ===========================================================================
// D2: the destination, the coverage label, the topic picker and the readiness
// panel
// ===========================================================================
//
// WHAT LANDED HERE AND WHAT DID NOT, SAID PLAINLY AT THE TOP.
//
// D2 section 9 asks this form to replace the archive URL and Secret inputs with a
// destination selector. IT CANNOT, YET, AND THE FORM SAYS SO RATHER THAN
// PRETENDING. The product API's `CreateScheduleRequest` REQUIRES an inline
// `archive` and has no `destinationRef` field at all: a schedule created
// through this page has nowhere to put a destination's name until PLAT-06.2
// adds one. The two honest options were to leave the inline fields alone with
// a disclosure, or to DERIVE an inline archive from the chosen destination --
// and the second is exactly the failure a destination exists to prevent. A
// destination carries an endpoint, a region, an addressing mode and a CA
// bundle; a legacy inline archive carries a URL and a Secret name. Deriving
// one from the other would silently drop four of those, and a schedule that
// quietly wrote to AWS S3 instead of the MinIO endpoint the operator chose is
// worse than a schedule that refused.
//
// SO THE SELECTOR IS REAL WHERE IT WORKS. `BackupPreflightRequest` DOES take a
// destination, so the readiness panel below is a genuine, submittable use of
// exactly the same control: choose the destination, choose the source, name
// the topics, and the check runs against them. When `POST /schedules` grows a
// `destinationRef`, this selector moves into the create form unchanged.
//
// THE COVERAGE LABELS ARE THE CONTROLLER'S OWN WORDS, VERBATIM.
// `weirkeeper::crds::selection::Coverage::label()` renders three strings and
// says why there is one function for them: "a second wording somewhere else is
// how 'visible user topics only' becomes 'all topics' in the one place an
// auditor reads." This page is one of those surfaces, so it copies the strings
// and `ui/tests/pages.spec.js` holds the copies against nothing -- there is no
// JavaScript link to a Rust constant -- which is why they are in ONE exported
// table here and asserted character for character.

/** The three coverage labels, VERBATIM from
 *  `weirkeeper::crds::selection::Coverage::label()`. Exactly one of them means
 *  "everything", and it is reachable only through an administrator
 *  attestation, because Kafka cannot be asked. */
export const COVERAGE_LABELS = Object.freeze({
  NamedTopics: "Named topics",
  AllUserTopicsAttested: "All user topics (attested complete)",
  VisibleUserTopicsOnly: "Visible user topics only \u2014 completeness not established",
});

/** The only coverage that may be rendered as "all topics". One expression, for
 *  the reason `Coverage::claims_whole_cluster` gives: a caller who asks the
 *  question by hand is a caller who can get it wrong once. */
export function claimsWholeCluster(coverage) {
  return coverage === "AllUserTopicsAttested";
}

/** What a dynamic selection is, in words, for the operator reading the form. */
export const DYNAMIC_SELECTION_SENTENCE =
  "A dynamic selection resolves per run from a topic discovery, so what it covers is decided " +
  "when the run starts and not when the schedule is written. What each run actually covered is " +
  "recorded on that run, with a coverage label, and only an attested one ever means everything.";

/** What a schedule naming no topic and carrying no dynamic block is.
 *
 *  THIS SENTENCE GOT SHORTER WHEN `ScheduleView.allUserTopics` LANDED (D1 W6,
 *  PLAT-06.2/09.2). Before it, an empty `topics` could have been either shape
 *  and this page could not tell; now the block itself is published, so the two
 *  cases are distinguishable and only the genuinely empty one is left. */
export const SELECTION_UNKNOWN_SENTENCE =
  "This schedule names no topic and carries no dynamic-selection block. No run under it will " +
  "back anything up: an empty allowlist is not an allowlist. Read the object with kubectl.";

/** Why no run's COVERAGE is rendered, and by whom that is owed.
 *
 *  `weirkeeper::crds::selection::Coverage::label()` renders the three strings
 *  every surface must use, and the controller records one on every run. NO API
 *  PROJECTION PUBLISHES IT: neither `status.selection` nor any `coverage` field
 *  appears anywhere in `logweir-api`'s contract, so the console has nothing to
 *  render. Saying which task owes it is the difference between a gap and an
 *  omission. */
export const COVERAGE_NOT_PUBLISHED =
  "What each RUN actually covered is recorded on that run with a coverage label, and only an " +
  "attested one ever means everything. THE SURFACE EXISTS NOW: this page renders " +
  "status.selection's mode, coverage and counts wherever the object carries them, which in " +
  "legacy mode is every run. What is still missing is the PROJECTION -- logweir-api's Backup " +
  "view publishes trigger and scheduleRef and no status.selection at all -- so in console mode " +
  "there is nothing to read, and this page will not infer a coverage from the mode, the frozen " +
  "topic list or a successful phase. Read the run with kubectl until that projection lands.";

/** Why a saved destination still cannot be named on a NEW schedule. */
export const CREATE_HAS_NO_DESTINATION =
  "A saved destination cannot be named on a NEW schedule from this page: POST /schedules takes " +
  "an inline archive and has no destinationRef field. PLAT-06.2 owes that one. The inline URL " +
  "and Secret below carry no endpoint, region, addressing mode or CA bundle -- which is what a " +
  "destination exists to hold -- so this page will not derive one from the other and drop all " +
  "four silently.";

/** Why this page cannot CHANGE an existing schedule's destination either, and
 *  the reason is this page's own contract rather than a missing field.
 *
 *  `PUT .../schedules/{name}` LANDED WITH D1 W6 and does take a
 *  `destinationRef` under `expectedGeneration`. This page still cannot send
 *  it: `ui/api.js` exports `create` plus exactly ONE narrow update -- a
 *  JSON-merge patch touching `spec.suspend` -- and
 *  `ui_lint::the_api_module_offers_no_delete_and_no_put` fails on the bare
 *  token for a replace anywhere in that module. That is a deliberate boundary
 *  and widening it is not this task's to decide. The route is a WHOLE-POLICY
 *  replace in which an omitted field is REMOVED, so the form that owns it is
 *  the one that owns every field of that policy: D1 W7's. */
export const EDIT_IS_A_REPLACE =
  "An EXISTING schedule can be pointed at a saved destination, and the Future policy panel on " +
  "that schedule's card is where: it sends PUT .../schedules/{name} under expectedGeneration. " +
  "That route replaces the WHOLE future policy -- a field omitted from the request is REMOVED " +
  "from the schedule -- so the panel shows every field of the policy and sends every one of " +
  "them, and a field left blank there is a field being cleared rather than a field left alone.";

/** The two selection MODES, verbatim from `weirkeeper::crds::selection::Mode`.
 *
 *  A MODE IS NOT A COVERAGE, and the two are rendered side by side because
 *  they answer different questions. The mode says what the POLICY asked for --
 *  a named allowlist or every user topic -- and the coverage says what the RUN
 *  may claim to have got. A dynamic run whose discovery was incomplete is mode
 *  `AllUserTopics` and coverage `VisibleUserTopicsOnly`, and collapsing those
 *  two into one word is exactly how "visible user topics only" becomes "all
 *  topics" in the one place an auditor reads. */
export const SELECTION_MODES = Object.freeze({
  SelectedTopics: "named topics",
  AllUserTopics: "all user topics",
});

/** The condition type a dynamic run's topic resolution is recorded under. */
export const TOPICS_RESOLVED = "TopicsResolved";

/** WHY A DYNAMIC RUN HAS NO TOPIC LIST, in the controller's own reason and
 *  message, or the empty string when `TopicsResolved` is not `False`.
 *
 *  `TopicsResolved=False` WITH REASON `DiscoveryRunning` IS NOT A FAILURE --
 *  it is the discovery Job still working -- so the reason is rendered rather
 *  than translated into a verdict here. Every other reason the controller
 *  writes on that condition (`DiscoveryIncomplete`, `SelectionEmpty`,
 *  `DiscoveryResultUnreadable`, `SourceChangedDuringResolution`,
 *  `SelectionTooLarge`) is terminal, and the message beside it is the one the
 *  controller composed. */
export function discoveryFailure(object) {
  const conditions = ((object || {}).status || {}).conditions;
  if (!Array.isArray(conditions)) {
    return null;
  }
  const found = conditions.find((c) => (c || {}).type === TOPICS_RESOLVED);
  if (found === undefined || found.status !== "False") {
    return null;
  }
  return { reason: found.reason, message: found.message };
}

/** That refusal as a line, or the empty string. */
export function renderDiscoveryFailure(object) {
  const failure = discoveryFailure(object);
  if (failure === null) {
    return "";
  }
  return (
    "<p class=\"coverage\" data-topics-resolved=\"False\">" +
    badge("unverified", "topics not resolved") + " " + cell(failure.reason) +
    (typeof failure.message === "string" && failure.message.length > 0
      ? " -- " + esc(failure.message)
      : "") +
    "</p>"
  );
}

/** The coverage label for one object, or the empty string.
 *
 *  READS THE OBJECT IT WAS GIVEN AND NEVER INFERS. In legacy mode the page
 *  holds the custom resource and `status.selection.coverage` is there; in
 *  console mode it holds a projection that carries neither, and the answer is
 *  the empty string -- which the caller renders as "not published by this
 *  build", never as a default label. */
export function coverageOf(object) {
  const status = (object || {}).status || {};
  const selection = status.selection || {};
  const coverage = selection.coverage;
  return typeof coverage === "string" && COVERAGE_LABELS[coverage] !== undefined ? coverage : "";
}

/** The coverage label for one run as a CELL: the controller's own words, or
 *  [`ABSENT`]. The sentence explaining the missing projection belongs beside a
 *  run and not inside every row of a table, so this is the short form and
 *  [`renderCoverageLine`] is the long one. */
export function coverageCell(object) {
  const coverage = coverageOf(object);
  return coverage.length === 0
    ? ABSENT
    : (claimsWholeCluster(coverage) ? badge("green", COVERAGE_LABELS[coverage])
      : badge("pending", COVERAGE_LABELS[coverage]));
}

/** The coverage line: the controller's own label, or the sentence that says
 *  this build does not publish one. */
export function renderCoverageLine(object) {
  const o = object || {};
  const spec = o.spec || {};
  const topics = Array.isArray(spec.topics) ? spec.topics : [];
  const coverage = coverageOf(o);
  if (coverage.length > 0) {
    const selection = (o.status || {}).selection || {};
    const mode = SELECTION_MODES[selection.mode];
    const counts = [];
    for (const [field, words] of [
      ["resolvedTopicCount", "topics frozen"],
      ["internalExcludedCount", "internal excluded"],
      ["excludedByRuleCount", "excluded by rule"],
      ["limitedTopicCount", "the broker refused to describe"],
    ]) {
      if (typeof selection[field] === "number") {
        counts.push(String(selection[field]) + " " + words);
      }
    }
    return (
      "<p class=\"coverage\" data-coverage=\"" + esc(coverage) + "\"" +
      (mode === undefined ? "" : " data-selection-mode=\"" + esc(String(selection.mode)) + "\"") +
      ">" +
      (claimsWholeCluster(coverage) ? badge("green", "coverage") : badge("pending", "coverage")) +
      " " + esc(COVERAGE_LABELS[coverage]) +
      // THE MODE IS PRINTED FROM THE FIELD OR NOT AT ALL. A mode this build
      // does not know is shown as the raw word rather than guessed at; there
      // is no third mode in the CRD and a fourth would be a contract change.
      (selection.mode === undefined || selection.mode === null
        ? ""
        : " The policy asked for " +
          esc(mode === undefined ? String(selection.mode) : mode) + ".") +
      (counts.length === 0 ? "" : " " + esc(counts.join(", ")) + ".") +
      "</p>" +
      renderDiscoveryFailure(o)
    );
  }
  if (spec.allUserTopics !== undefined && spec.allUserTopics !== null) {
    const exclude = spec.allUserTopics.exclude || {};
    const left = []
      .concat(Array.isArray(exclude.topics) ? exclude.topics : [])
      .concat(Array.isArray(exclude.prefixes) ? exclude.prefixes.map((x) => x + "*") : []);
    return (
      "<p class=\"coverage\" data-coverage=\"dynamic\">" + badge("pending", "dynamic selection") +
      " " + esc(DYNAMIC_SELECTION_SENTENCE) +
      " On incomplete visibility this policy says: <code>" +
      cell(spec.allUserTopics.incompleteDiscovery) + "</code>." +
      (left.length === 0 ? "" : " Excluded: " + esc(left.join(", ")) + ".") +
      " " + esc(COVERAGE_NOT_PUBLISHED) + "</p>" + renderDiscoveryFailure(o)
    );
  }
  if (topics.length === 0) {
    return "<p class=\"coverage\" data-coverage=\"unknown\">" + badge("pending", "selection") +
      " " + esc(SELECTION_UNKNOWN_SENTENCE) + "</p>";
  }
  return "";
}

// ------------------------------------------------------- the destination selector

/** The option a destination selector with nothing chosen opens on. */
export const DESTINATION_EMPTY_OPTION =
  "<option value=\"\" selected data-name=\"\">choose a saved destination</option>";

/** Resolves a remembered `{uid, name}` against the destinations this page
 *  actually read.
 *
 *  THE SAME CONTRACT `ui/select.js` HOLDS FOR CONNECTIONS, and for the same
 *  reason: a destination deleted and recreated under one name is a DIFFERENT
 *  archive location reached with a different credential, and a form that
 *  followed the name onto it would run a readiness check against a place
 *  nobody chose. States: `none`, `selected`, `recreated`, `missing`. */
export function resolveDestinationSelection(destinations, selection) {
  const all = Array.isArray(destinations) ? destinations : [];
  const s = selection || {};
  const uid = typeof s.uid === "string" ? s.uid.trim() : "";
  const name = typeof s.name === "string" ? s.name.trim() : "";
  if (uid.length === 0 && name.length === 0) {
    return { state: "none" };
  }
  if (uid.length === 0) {
    const byName = all.find((d) => d.name === name);
    return byName === undefined
      ? { state: "missing", uid: "", name: name }
      : { state: "selected", uid: byName.uid, name: byName.name, pinned: true, item: byName };
  }
  const byUid = all.find((d) => d.uid === uid);
  if (byUid !== undefined) {
    return {
      state: "selected", uid: byUid.uid, name: byUid.name, item: byUid,
      renamedFrom: name.length > 0 && name !== byUid.name ? name : null,
    };
  }
  const taken = all.find((d) => d.name === name);
  return taken === undefined
    ? { state: "missing", uid: uid, name: name }
    : { state: "recreated", uid: uid, name: name, recreatedUid: taken.uid };
}

/** The namespace's default destination, or `null`.
 *
 *  TWO DEFAULTS ARE NO DEFAULT. The product API refuses a second one, but a
 *  default set by hand with only the annotation carries no label and its 409
 *  cannot see it -- so this returns `null` for a list with more than one, and
 *  the selector says why rather than picking whichever sorted first. */
export function defaultDestination(destinations) {
  const flagged = (Array.isArray(destinations) ? destinations : []).filter((d) => d.default === true);
  return flagged.length === 1 ? flagged[0] : null;
}

/** The destination selector. */
export function renderDestinationSelector(view) {
  const v = view || {};
  const id = typeof v.id === "string" && v.id.length > 0 ? v.id : "destination-select";
  const field = typeof v.name === "string" && v.name.length > 0 ? v.name : "destination";
  const all = Array.isArray(v.destinations) ? v.destinations : [];
  const resolved = resolveDestinationSelection(all, v.selection);
  const fallback = resolved.state === "none" ? defaultDestination(all) : null;
  const chosenUid = resolved.state === "selected"
    ? resolved.uid
    : (fallback === null ? "" : fallback.uid);
  const refused = resolved.state === "recreated" || resolved.state === "missing";
  return (
    "<div class=\"field selector\" id=\"" + esc(id) + "-field\">" +
    "<label for=\"" + esc(id) + "\">" + esc(v.label || "destination") + "</label>" +
    (refused ? renderDestinationRefusal(id, resolved) : "") +
    "<select id=\"" + esc(id) + "\" name=\"" + esc(field) + "\">" +
    (refused || chosenUid.length === 0 ? DESTINATION_EMPTY_OPTION : "") +
    all.map((d) =>
      "<option value=\"" + esc(d.uid) + "\" data-name=\"" + esc(d.name) + "\"" +
      (!refused && d.uid === chosenUid ? " selected" : "") + ">" +
      esc(d.name) + (d.default === true ? " (namespace default)" : "") +
      " -- " + esc(d.canonicalUrl) + ", " + esc(String(d.transport)) +
      "</option>").join("") +
    "</select>" +
    "<input type=\"hidden\" id=\"" + esc(id) + "-name\" name=\"" + esc(field + "Name") +
    "\" value=\"" + esc(resolved.state === "selected"
      ? resolved.name
      : (fallback === null ? "" : fallback.name)) + "\">" +
    (all.length === 0
      ? "<p class=\"note\" id=\"" + esc(id) + "-none\">No destination in this namespace. Create " +
        "one on the Destinations page first.</p>"
      : "") +
    (resolved.state === "none" && fallback === null && all.length > 0
      ? "<p class=\"note\" id=\"" + esc(id) + "-no-default\">No single destination in this " +
        "namespace is marked default, so nothing is preselected. A namespace with two defaults " +
        "has none.</p>"
      : "") +
    (fallback !== null
      ? "<p class=\"note\" id=\"" + esc(id) + "-default\">Preselected: the namespace default " +
        "<code>" + esc(fallback.name) + "</code>.</p>"
      : "") +
    (resolved.state === "selected" && resolved.renamedFrom
      ? "<p class=\"note\" data-selection-renamed=\"true\">This destination has been renamed " +
        "since it was chosen: it was <code>" + esc(resolved.renamedFrom) + "</code> and is now " +
        "<code>" + esc(resolved.name) + "</code>. The selection did not move -- it is the same " +
        "object, uid <code>" + esc(resolved.uid) + "</code>.</p>"
      : "") +
    "<p class=\"help\">" + esc(v.help || "") + "</p>" +
    "</div>"
  );
}

/** The refusal a destination selection whose UID is gone gets. */
export function renderDestinationRefusal(id, resolved) {
  const r = resolved || {};
  const recreated = r.state === "recreated";
  return (
    "<div class=\"refusal-block\" id=\"" + esc(id) + "-refusal\" role=\"alert\">" +
    (recreated
      ? "<p class=\"refusal\">The destination this form had selected is gone, and a DIFFERENT " +
        "object now answers to the name <code>" + esc(r.name) + "</code>. A recreated " +
        "destination is a different archive location reached with a different credential, so " +
        "the selection is refused rather than moved onto it.</p>"
      : "<p class=\"refusal\">No destination in this namespace carries that uid, so the one " +
        "this form had selected is gone. It was not replaced by another.</p>") +
    "<p class=\"note\">Selected was: <code>" + esc(String(r.name || "")) + "</code>, uid <code>" +
    esc(String(r.uid || "")) + "</code>." +
    (recreated ? " The object now under that name has uid <code>" + esc(r.recreatedUid) +
      "</code>." : "") +
    "</p></div>"
  );
}

// --------------------------------------------------------- the topic picker

/** What a topic picker offers, and where the offer came from.
 *
 *  THE PICKER NEVER REPLACES THE TEXT FIELD. A discovery is an observation
 *  with a date on it and a visibility state; the allowlist a schedule carries
 *  is a decision. So the names are offered as a datalist beside the same
 *  free-text input the form has always had -- typing a topic Kafka hid from
 *  this principal is exactly what an operator with an ACL-limited principal
 *  has to be able to do, and a picker that was the only way in would have made
 *  that impossible. */
export function renderTopicPicker(view) {
  const v = view || {};
  const discovery = v.lastSuccessful || null;
  const attempt = v.latestAttempt || null;
  const topics = Array.isArray(v.topics) ? v.topics : [];
  if (discovery === null && attempt === null) {
    return "<p class=\"note\" id=\"topic-picker-none\">No topic discovery has run for the " +
      "selected connection, so there are no observed names to offer. Type them.</p>";
  }
  const stale = discovery !== null && discovery.stale === true;
  const limited = discovery !== null && (discovery.visibility || {}).state === "limited";
  const failed = attempt !== null && attempt.state === "failed";
  return (
    "<div class=\"topic-picker\" id=\"topic-picker\">" +
    (discovery === null
      ? "<p class=\"note\">The newest attempt produced no inventory, so nothing is offered.</p>"
      : "<datalist id=\"topic-options\">" +
        topics.map((t) => "<option value=\"" + esc(t.name) + "\"></option>").join("") +
        "</datalist>") +
    (failed
      ? "<p class=\"note\" data-picker=\"failed\">" + badge("unverified", "latest attempt failed") +
        " The newest discovery failed (" + cell((attempt.error || {}).code) + "). The names " +
        "offered, if any, are from an older run.</p>"
      : "") +
    (stale
      ? "<p class=\"note\" data-picker=\"stale\">" + badge("unverified", "stale inventory") +
        " These names are past their freshness or their connection changed under them.</p>"
      : "") +
    (limited
      ? "<p class=\"note\" data-picker=\"limited\">" + badge("unverified", "limited visibility") +
        " An authorization omission was observed: this list is a subset, and Logweir cannot say " +
        "how large a subset. Type any topic it does not offer.</p>"
      : "") +
    (discovery !== null && !limited && !stale
      ? "<p class=\"note\" data-picker=\"unknown\">Offered from the inventory observed at " +
        cell(discovery.observedAt) + ". A successful Kafka listing is not a complete one, so " +
        "this list is an offer and never a bound.</p>"
      : "") +
    "</div>"
  );
}

// ------------------------------------------------------- the readiness panel

/** The identity of the readiness form in the draft and mutation registries. */
export const READINESS_FORM = "backup-readiness";

/** What a backup readiness check is, and what a ready verdict does not cover. */
export const READINESS_SENTENCE =
  "A readiness check starts a Preflight: a real Job that resolves the connection and the " +
  "destination, projects their credentials, dials the broker and lists the archive prefix. The " +
  "verdict below is that check's own recorded result. It is not a promise about the next run -- " +
  "a credential can be rotated, a topic created and an ACL changed in the minute after it.";

/** The readiness panel: choose a destination, a source and the topics, start a
 *  `Preflight` of operation `backup`, and render what it recorded. */
export function renderReadinessPanel(view) {
  const v = view || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  const result = v.preflight || null;
  return (
    "<section class=\"readiness\" id=\"backup-readiness\"><h3>Backup readiness</h3>" +
    "<p class=\"note\">" + esc(READINESS_SENTENCE) + "</p>" +
    (v.unavailable === true
      ? "<p class=\"note\" id=\"readiness-unavailable\">" + cell(v.unavailableReason) + "</p>"
      : (v.mayOperate === false
        ? "<p class=\"note\">This login may read readiness results in this namespace and not " +
          "start one.</p>"
        : "<form id=\"readiness-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
          "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
          renderClusterSelector({
            id: "readiness-source",
            name: "source",
            label: "source KafkaCluster",
            help: "The connection the check dials.",
            prefer: "source",
            clusters: v.clusters,
            selection: { uid: "", name: "" },
            now: v.now,
            freshSeconds: v.freshSeconds,
            errors: {},
          }) +
          renderDestinationSelector({
            id: "readiness-destination",
            name: "destination",
            label: "destination",
            help: "The archive location the check lists. Chosen by identity: a destination " +
              "deleted and recreated under this name is refused, not followed.",
            destinations: v.destinations,
            selection: v.destinationSelection,
          }) +
          "<div class=\"field\"><label for=\"readiness-topics\">topics, comma separated</label>" +
          "<input id=\"readiness-topics\" name=\"topics\" list=\"topic-options\" value=\"" +
          esc(String(v.topics || "")) + "\">" +
          "<p class=\"help\">The names the check asks the broker to describe. 1 to 1000.</p>" +
          "</div>" +
          renderTopicPicker(v.picker || {}) +
          "<div class=\"actions\"><button type=\"submit\">Check readiness</button>" +
          (result !== null && result.terminal === false
            ? "<button type=\"button\" id=\"readiness-cancel\">Cancel</button>"
            : "") +
          "</div></fieldset>" +
          "<div class=\"form-status\" id=\"readiness-status\" tabindex=\"-1\">" +
          mutationStatus(state, { kind: "Preflight", name: (result || {}).id || "" }, null) +
          "</div></form>")) +
    (result === null ? "" : renderPreflight(result)) +
    "</section>"
  );
}

const SCHEDULE_DEFAULTS = Object.freeze({
  name: "", cron: "0 * * * *", source: "", sourceUid: "", topics: "", archive: "",
  archiveSecret: "logweir-s3", keepLast: "", keepDays: "",
});

/** The five glob metacharacters `logweir_core::guard::GLOB_METACHARACTERS`
 *  refuses, plus the sixth, in one string. */
const GLOB = "*?[]{}";

const CRON_FIELD = /^[0-9*,/-]+$/;

/** The scheme separator, built rather than spelled, as in `../api.js`. */
const SCHEME_SEPARATOR = ":" + "//";

/** The page's own checks for the create form, by field. A CONVENIENCE: the
 *  controller's `Ready` condition and the runner's G-GLOB guard are the gate. */
export function validateSchedule(values) {
  const v = values || {};
  const problems = Object.create(null);
  if (!isObjectName(v.name)) {
    problems.name = "a BackupSchedule name is lowercase letters, digits, '-' and '.', starting " +
      "and ending with a letter or digit";
  }
  const cron = String(v.cron || "").trim();
  const fields = cron.split(/\s+/).filter((f) => f.length > 0);
  const macro = cron === "@hourly" || cron === "@daily" || cron === "@weekly";
  if (!macro && (fields.length !== 5 || fields.some((f) => !CRON_FIELD.test(f)))) {
    problems.cron = "five cron fields (minute hour day-of-month month day-of-week), or @hourly, " +
      "@daily or @weekly";
  }
  // THE EMPTY SELECTION IS ITS OWN REFUSAL (review finding F1). A refused
  // selector opens on `<option value="" selected>`, so a Create click that did
  // not choose anything arrives here with both halves empty -- and is refused
  // by name rather than by the DNS-1123 check happening to reject "".
  const uid = String(v.sourceUid === undefined || v.sourceUid === null ? "" : v.sourceUid).trim();
  if (String(v.source || "").trim().length === 0 && uid.length === 0) {
    problems.source = "choose a saved connection: nothing is selected, so there is no cluster " +
      "for this schedule to read from";
  } else if (!isObjectName(v.source)) {
    problems.source = "choose a saved KafkaCluster in this namespace";
  }
  const topics = String(v.topics || "").split(",").map((t) => t.trim()).filter((t) => t.length > 0);
  if (topics.length === 0) {
    problems.topics = "name at least one topic; an empty list is not an allowlist";
  } else {
    const globbed = topics.filter((t) => t.split("").some((c) => GLOB.indexOf(c) !== -1));
    if (globbed.length > 0) {
      problems.topics = "names, never patterns: " + globbed.join(", ") + " carries a glob metacharacter";
    }
  }
  const archive = String(v.archive || "").trim();
  if (archive.length === 0 || archive.indexOf(SCHEME_SEPARATOR) <= 0) {
    problems.archive = "an object-store URL with its scheme, such as s3" + SCHEME_SEPARATOR + "bucket/prefix";
  }
  const secret = String(v.archiveSecret || "").trim();
  if (secret.length > 0 && !isObjectName(secret)) {
    problems.archiveSecret = "a Secret name is lowercase letters, digits, '-' and '.'";
  }
  for (const key of ["keepLast", "keepDays"]) {
    const raw = v[key];
    if (raw !== "" && raw !== undefined && raw !== null && !/^[0-9]+$/.test(String(raw).trim())) {
      problems[key] = "a whole number of 0 or more, or blank";
    }
  }
  return problems;
}

/** The create form for a BackupSchedule, rendered from its draft, its field
 *  messages and its mutation record. Called with nothing it is the empty form. */
export function renderScheduleForm(view) {
  const v = view || {};
  const d = Object.assign({}, SCHEDULE_DEFAULTS, v.draft || {});
  const errors = ((v.errors || {}).fields) || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  const field = (id, name) => invalidAttributes(id, errors[name]);
  const line = (id, name) => fieldErrorLine(id, errors[name]);
  return (
    "<section class=\"create\" id=\"schedule-create\"><h3>Create a BackupSchedule</h3>" +
    "<p class=\"note\">A schedule fires a Backup of the named topics at each slot and writes " +
    "it to the archive. Every field but suspend is sealed once the object exists.</p>" +
    "<form id=\"schedule-form\" novalidate" + (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
    "<div class=\"field\"><label for=\"schedule-name\">name</label>" +
    "<input id=\"schedule-name\" name=\"name\" required value=\"" + esc(d.name) + "\"" +
    field("schedule-name", "name") + ">" + line("schedule-name", "name") + "</div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"schedule-cron\">schedule, five cron fields</label>" +
    "<input id=\"schedule-cron\" name=\"cron\" value=\"" + esc(d.cron) + "\" required" +
    field("schedule-cron", "cron") + ">" +
    "<p class=\"help\">minute hour day-of-month month day-of-week, in UTC.</p>" +
    line("schedule-cron", "cron") + "</div>" +
    "</div>" +
    renderClusterSelector({
      id: "schedule-source",
      name: "source",
      label: "source KafkaCluster",
      help: "The saved connection each run of this schedule reads from. Chosen by identity: " +
        "the draft remembers this object's uid, not its name.",
      prefer: "source",
      clusters: v.clusters,
      selection: { uid: d.sourceUid, name: d.source },
      now: v.now,
      freshSeconds: v.freshSeconds,
      errors: errors,
    }) +
    line("schedule-source", "source") +
    "<div class=\"field\"><label for=\"schedule-topics\">topics, comma separated -- names, never patterns</label>" +
    "<input id=\"schedule-topics\" name=\"topics\" required value=\"" + esc(d.topics) + "\"" +
    field("schedule-topics", "topics") + ">" +
    "<p class=\"help\">An explicit allowlist. A wildcard is refused before anything runs.</p>" +
    line("schedule-topics", "topics") + "</div>" +
    "<fieldset class=\"legacy-archive\" id=\"schedule-legacy-archive\">" +
    "<legend>archive (inline)</legend>" +
    "<p class=\"help\" id=\"schedule-destination-gap\">" + esc(CREATE_HAS_NO_DESTINATION) +
    "</p>" +
    "<p class=\"help\" id=\"schedule-destination-edit\">" + esc(EDIT_IS_A_REPLACE) + "</p>" +
    "<div class=\"field\"><label for=\"schedule-archive\">archive URL</label>" +
    "<input id=\"schedule-archive\" name=\"archive\" required value=\"" + esc(d.archive) + "\"" +
    field("schedule-archive", "archive") + ">" +
    "<p class=\"help\">The bucket and prefix the runner writes the backup set under.</p>" +
    line("schedule-archive", "archive") + "</div>" +
    "<div class=\"field\"><label for=\"schedule-archive-secret\">archive credential (Secret name)</label>" +
    "<input id=\"schedule-archive-secret\" name=\"archiveSecret\" value=\"" + esc(d.archiveSecret) + "\"" +
    field("schedule-archive-secret", "archiveSecret") + ">" +
    "<p class=\"help\">An existing Secret in this namespace, with access-key-id and secret-access-key. " +
    "Only its name is sent. Leave blank only for anonymous or instance-role access.</p>" +
    line("schedule-archive-secret", "archiveSecret") + "</div></fieldset>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"schedule-keeplast\">retention keepLast</label>" +
    "<input id=\"schedule-keeplast\" name=\"keepLast\" type=\"number\" min=\"0\" value=\"" +
    esc(d.keepLast) + "\"" + field("schedule-keeplast", "keepLast") + ">" +
    "<p class=\"help\">How many sets a retention evaluation keeps. It reports; it never deletes.</p>" +
    line("schedule-keeplast", "keepLast") + "</div>" +
    "<div class=\"field\"><label for=\"schedule-keepdays\">retention keepDays</label>" +
    "<input id=\"schedule-keepdays\" name=\"keepDays\" type=\"number\" min=\"0\" value=\"" +
    esc(d.keepDays) + "\"" + field("schedule-keepdays", "keepDays") + ">" +
    "<p class=\"help\">How many days of sets it keeps. Leave both blank for no evaluation.</p>" +
    line("schedule-keepdays", "keepDays") + "</div>" +
    "</div>" +
    "<div class=\"actions\"><button type=\"submit\" class=\"primary\">Create</button></div>" +
    "</fieldset>" +
    "<div class=\"form-status\" id=\"schedule-form-status\" tabindex=\"-1\">" +
    mutationStatus(state, { kind: "BackupSchedule", name: d.name }, ((v.errors || {}).unmatched)) +
    "</div>" +
    "</form></section>"
  );
}

/** The request body a filled-in form produces. */
export function scheduleBody(values) {
  const spec = {
    schedule: values.cron,
    sourceRef: { name: values.source },
    topics: String(values.topics || "")
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0),
    archive: { url: values.archive },
    suspend: false,
  };
  const archiveSecret = String(values.archiveSecret || "").trim();
  if (archiveSecret.length > 0) {
    spec.archive.secretRef = { name: archiveSecret };
  }
  const retention = {};
  if (values.keepLast !== "" && values.keepLast !== undefined && values.keepLast !== null) {
    retention.keepLast = Number(values.keepLast);
  }
  if (values.keepDays !== "" && values.keepDays !== undefined && values.keepDays !== null) {
    retention.keepDays = Number(values.keepDays);
  }
  if (Object.keys(retention).length > 0) {
    spec.retention = retention;
  }
  return {
    apiVersion: "logweir.dev/v1alpha1",
    kind: "BackupSchedule",
    metadata: { name: values.name },
    spec: spec,
  };
}

/** The message a schedule create is refused with when its chosen source no
 *  longer resolves. Named so the same words are asserted rather than guessed. */
export function sourceRefusal(resolved) {
  const r = resolved || {};
  if (r.state === "recreated") {
    return (
      "the KafkaCluster this form selected (uid " + r.uid + ") is gone and a different object " +
      "now answers to the name " + r.name + " (uid " + r.recreatedUid + "). A recreated " +
      "connection is a different set of brokers reached with a different credential, so " +
      "nothing was sent; choose the connection you mean"
    );
  }
  return (
    "the KafkaCluster this form selected (" + r.name + ", uid " + r.uid + ") is not in this " +
    "namespace any more, so nothing was sent; choose a saved connection"
  );
}

/** Checks the values against the page's own rules AND against the saved
 *  connections the page actually read, then creates the schedule idempotently
 *  by name.
 *
 *  `clusters` is the list this page read. RESOLVING AGAINST IT IS THE POINT:
 *  the draft carries a UID, and a UID that no longer answers -- because the
 *  cluster was deleted, or deleted and recreated under the same name while this
 *  form sat open -- is refused here, before a request, rather than sent as a
 *  name that would resolve to whatever holds it now. The NAME sent is the one
 *  the resolved object carries at this moment, so a rename between opening the
 *  form and submitting it is followed rather than fought.
 *
 *  Called without `clusters` -- which is what a caller that has not read them
 *  looks like -- it falls back to the typed values, which is the pre-PLAT-07.2
 *  behaviour and is what keeps an existing draft usable. */
export async function submitSchedule(ns, values, deps, clusters) {
  const problems = validateSchedule(values);
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  let sent = values;
  if (clusters !== undefined && clusters !== null) {
    const resolved = resolveClusterSelection(clusters, {
      uid: values.sourceUid,
      name: values.source,
    });
    if (resolved.state !== "selected") {
      throw invalidInput({ source: sourceRefusal(resolved) });
    }
    sent = Object.assign({}, values, { source: resolved.name, sourceUid: resolved.uid });
  }
  return createOnce(deps || API, ns, PLURAL, scheduleBody(sent), SCHEDULE_SPEC_RULES);
}

/** What the create form renders from in namespace `ns`: its draft, its record,
 *  the messages the record's failure carries, and the saved connections its
 *  source selector offers. */
export function scheduleFormView(ns, clusters, now, freshSeconds) {
  const key = formKey(ns, SCHEDULE_FORM);
  const state = mutationFor(key).state;
  if (state.phase === "succeeded") {
    dropDraft(key);
  }
  return {
    draft: readDraft(key),
    state: state,
    errors: state.phase === "failed" ? fieldErrors(state.error, SCHEDULE_FIELD_PATHS) : null,
    clusters: clusters,
    now: now,
    freshSeconds: freshSeconds,
  };
}

/** The sentence the recovery-point panel carries when a schedule has fired no
 *  completed run yet. */
export const NO_POINTS_SENTENCE =
  "this schedule has no completed run with a backup set yet; there is nothing to restore " +
  "from until one reaches phase Succeeded.";

/** THE RECOVERY POINTS ONE SCHEDULE HAS PRODUCED, each with the link that
 *  opens the restore wizard ON IT (PLAT-11.1).
 *
 *  The rows are `Backup` objects whose `spec.scheduleRef.name` is this
 *  schedule -- the field `weirkeeper` writes when the cron reconciler creates
 *  a run -- filtered to the ones a plan can actually be built from and ordered
 *  newest completion first. The link carries the Backup's UID beside its name,
 *  because the wizard resolves by UID: a point deleted and recreated under the
 *  same name is a different run, and following it silently is the defect this
 *  identity exists to prevent.
 *
 *  It is a READ and a LINK. This panel creates nothing, patches nothing and
 *  decides nothing about the restore; the wizard reads the point again for
 *  itself and refuses if it is gone. */
export function renderRecoveryPoints(ns, object, backups) {
  const schedule = ((object && object.metadata) || {}).name || "";
  const mine = itemsOf(backups).filter(
    (backup) => ((((backup || {}).spec) || {}).scheduleRef || {}).name === schedule,
  );
  const rows = recoveryPoints(mine).map((point) => {
    const meta = point.metadata || {};
    const spec = point.spec || {};
    const status = point.status || {};
    const covered = status.windowCovered || {};
    return [
      cell(meta.name),
      triggerBadge(spec.trigger, ((object || {}).spec || {}).retry === undefined
        ? undefined
        : object.spec.retry.maxRetries),
      coverageCell(point),
      cell(spec.slot),
      cell(status.backupId),
      cell(rfc3339(covered.fromMs)),
      cell(rfc3339(covered.toMs)),
      cell(status.records),
      "<a href=\"" + esc(restorePointRoute(ns, point)) + "\">Restore this point</a>",
    ];
  });
  const running = mine.filter((backup) => !isRecoveryPoint(backup)).length;
  return (
    "<section class=\"retention\"><h3>Recovery points</h3>" +
    table(
      ["BACKUP", "TRIGGER", "COVERAGE", "SLOT", "BACKUP SET", "COVERED FROM", "COVERED TO",
        "RECORDS", ""],
      rows,
      NO_POINTS_SENTENCE,
    ) +
    (running === 0
      ? ""
      : "<p class=\"note\">" + String(running) + " further run(s) of this schedule are not " +
        "offered: a run still in flight, or one that completed without a backup set and a " +
        "covered window, is not a point a plan can be built from.</p>") +
    "</section>"
  );
}

/** ONE SCHEDULE'S CARD: its name, its toggle, the revision in force, what its
 *  last slot did, its next firings, the runs in flight with the revisions they
 *  froze, the manual-run panel, the policy form, its recovery points and the
 *  retention panel.
 *
 *  `extra` carries what the card cannot read off the object: the destinations
 *  the page listed, the readiness verdict, and the per-schedule view state the
 *  mount half holds. Called without it -- which is what a spec row and the
 *  first paint look like -- every panel renders from the object alone. */
export function renderScheduleCard(ns, object, backups, extra) {
  const name = ((object && object.metadata) || {}).name || "";
  const state = mutationFor(formKey(ns, SUSPEND_FORM, name)).state;
  const e = extra || {};
  const own = (e.cards || {})[name] || {};
  return (
    "<section class=\"schedule\" data-schedule=\"" + esc(name) + "\"><div class=\"card-head\"><h3>" +
    nameOf(object) + "</h3>" +
    renderSuspendToggle(object, state) +
    "</div>" +
    "<div class=\"form-status\" data-suspend-status=\"" + esc(name) + "\" tabindex=\"-1\">" +
    renderSuspendStatus(object, state) + "</div>" +
    renderScheduleRevision(object) +
    renderCoverageLine(object) +
    renderLastSlot(object) +
    nextRunsPanel({
      runs: ((object || {}).status || {}).nextRuns,
      timeZone: (((object || {}).status || {}).policy || {}).timeZone ||
        ((object || {}).spec || {}).timeZone,
      tzdb: (((object || {}).status || {}).policy || {}).tzdb,
      heading: "Next runs",
      now: e.now,
    }) +
    renderActiveRuns(ns, object, backups) +
    "<div class=\"run-now-slot\" data-run-now-slot=\"" + esc(name) + "\">" +
    renderRunNowPanel({
      ns: ns,
      name: name,
      object: object,
      state: mutationFor(formKey(ns, RUN_NOW_FORM, name)).state,
      mayOperate: e.mayOperate,
      readiness: own.readiness || null,
      acknowledged: own.acknowledged === true,
      runs: manualRunsOf(name, backups),
      result: own.result || null,
    }) + "</div>" +
    "<div class=\"policy-slot\" data-policy-slot=\"" + esc(name) + "\">" +
    renderPolicyForm(policyFormView(ns, object, own, e.destinations, e.mayOperate)) + "</div>" +
    renderRecoveryPoints(ns, object, backups) +
    renderRetentionPanel(object, policyForSchedule(object, e.retentionPolicies)) +
    "</section>"
  );
}

/** THE REVISION IN FORCE, and the one the controller has actually evaluated.
 *
 *  THEY ARE PRINTED SEPARATELY BECAUSE THEY MEAN DIFFERENT THINGS. A
 *  `metadata.generation` ahead of `status.observedGeneration` is an edit the
 *  controller has not seen yet -- the policy on screen is not yet the policy
 *  that schedules -- and a console that printed one number would be answering
 *  the wrong question half the time. `status.policy.evaluatedAt` is when the
 *  status last MOVED and is never rendered as a liveness signal: the
 *  controller writes nothing when nothing has changed. */
export function renderScheduleRevision(object) {
  const o = object || {};
  const meta = o.metadata || {};
  const status = o.status || {};
  const policy = status.policy;
  if (typeof meta.generation !== "number" && (policy === undefined || policy === null)) {
    return "";
  }
  const behind = typeof meta.generation === "number" &&
    typeof status.observedGeneration === "number" &&
    status.observedGeneration < meta.generation;
  return (
    "<p class=\"revision\" data-generation=\"" +
    (typeof meta.generation === "number" ? String(meta.generation) : "") + "\">" +
    revisionLine({
      generation: meta.generation,
      runPolicySha256: (policy || {}).runPolicySha256,
    }) +
    (policy === undefined || policy === null
      ? ""
      : " In force since <code>" + esc(String(policy.effectiveSince)) + "</code>, read in " +
        "<code>" + esc(String(policy.timeZone)) + "</code> against <code>" +
        esc(String(policy.tzdb)) + "</code>.") +
    (behind
      ? " " + badge("pending", "not yet evaluated") + " <span class=\"note\">The controller has " +
        "evaluated revision g" + String(status.observedGeneration) + "; the policy saved above " +
        "is not yet the policy that schedules.</span>"
      : "") +
    "</p>"
  );
}

/** What one schedule's policy form renders from: its draft (or the object's
 *  own values when there is no draft), its mutation record, the messages a
 *  failure carries and the preview last taken for it. */
export function policyFormView(ns, object, own, destinations, may) {
  const name = ((object || {}).metadata || {}).name || "";
  const key = formKey(ns, POLICY_FORM, name);
  const state = mutationFor(key).state;
  if (state.phase === "succeeded") {
    dropDraft(key);
  }
  const draft = readDraft(key);
  return {
    name: name,
    generation: ((object || {}).metadata || {}).generation,
    values: draft === null ? policyValuesOf(object) : Object.assign(policyValuesOf(object), draft),
    state: state,
    errors: state.phase === "failed" ? fieldErrors(state.error, POLICY_FIELD_PATHS) : null,
    preview: (own || {}).preview || null,
    destinations: destinations,
    mayOperate: may,
  };
}

/** The API's field paths, mapped to the policy form's inputs. The paths are
 *  the REQUEST's, because that is what a `422` from `PUT .../schedules/{name}`
 *  names. */
export const POLICY_FIELD_PATHS = Object.freeze([
  ["schedule", "cron"],
  ["timeZone", "timeZone"],
  ["topicSelection", "topics"],
  ["topicSelection.topics", "topics"],
  ["topicSelection.allUserTopics", "incompleteDiscovery"],
  ["topicSelection.allUserTopics.incompleteDiscovery", "incompleteDiscovery"],
  ["archive", "archive"],
  ["archive.url", "archive"],
  ["archive.credentialRef", "archiveSecret"],
  ["destinationRef", "destination"],
  ["concurrencyPolicy", "concurrencyPolicy"],
  ["startingDeadlineSeconds", "startingDeadlineSeconds"],
  ["catchUpPolicy", "catchUpPolicy"],
  ["retry.maxRetries", "maxRetries"],
  ["retry.delaySeconds", "retryDelaySeconds"],
  ["activeDeadlineSeconds", "activeDeadlineSeconds"],
  ["retention.keepLast", "keepLast"],
  ["retention.keepDays", "keepDays"],
  ["expectedGeneration", "cron"],
]);

// --------------------------------------------------------------- mount half

/** The destinations the readiness panel offers, or the reason there are none.
 *  Never throws for anything but a cancellation: a console that cannot serve
 *  destinations is a panel with a sentence, not a page with an error box. */
async function readReadiness(api, ns, lifecycle, clusters) {
  const base = { mayOperate: mayOperate(ns), clusters: clusters, preflight: null, picker: {} };
  try {
    const page = await api.destinations(ns, readOptions(lifecycle));
    return Object.assign(base, { destinations: page.items });
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      throw error;
    }
    return Object.assign(base, {
      destinations: [],
      unavailable: true,
      unavailableReason: error.message,
    });
  }
}

/** Every RetentionPolicy in `ns`, or an empty list when the read is refused. */
async function readRetentionPolicies(ns, lifecycle) {
  try {
    return itemsOf(await listD3("retention", ns, readOptions(lifecycle)));
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      throw error;
    }
    return [];
  }
}

function readinessView(ns, readiness) {
  return Object.assign({ state: mutationFor(formKey(ns, READINESS_FORM)).state }, readiness || {});
}

/** The readiness panel's controls: start a `Preflight` of operation `backup`
 *  against the chosen connection, destination and topics, and offer the names
 *  the newest inventory for that connection observed.
 *
 *  THE TOPIC OFFER IS READ WHEN THE CONNECTION IS CHOSEN, and not before: a
 *  discovery is about ONE connection, and offering the names observed against
 *  a different one would be worse than offering none. */
function wireReadiness(node, ns, parse, lifecycle, api, readiness) {
  const form = node.querySelector("#readiness-form");
  if (form === null) {
    return;
  }
  const key = formKey(ns, READINESS_FORM);
  const mutation = mutationFor(key);
  const repaint = (extra) => {
    if (!active(lifecycle)) {
      return;
    }
    const slot = node.querySelector("#readiness-slot");
    if (slot === null) {
      return;
    }
    const merged = Object.assign({}, readiness, extra || {});
    replace(slot, parse(renderReadinessPanel(readinessView(ns, merged))));
    wireReadiness(node, ns, parse, lifecycle, api, merged);
  };

  watchMutation(node, key, mutation, (state) => {
    repaint(state.phase === "succeeded"
      ? { preflight: (state.result || {}).item }
      : {});
  }, lifecycle);

  const source = node.querySelector("#readiness-source");
  if (source !== null) {
    listen(source, "change", () => {
      const selection = readClusterSelection(form, "readiness-source");
      if (!active(lifecycle) || selection.name.length === 0) {
        return;
      }
      api.latestDiscoveries(ns, selection.name, readOptions(lifecycle)).then(
        (answer) => {
          if (!active(lifecycle)) {
            return;
          }
          const best = answer.lastSuccessful;
          if (best === null || best === undefined) {
            repaint({ picker: { latestAttempt: answer.latestAttempt, lastSuccessful: null } });
            return;
          }
          api.discoveryTopics(ns, best.id, readOptions(lifecycle)).then(
            (page) => {
              if (active(lifecycle)) {
                repaint({
                  picker: {
                    latestAttempt: answer.latestAttempt,
                    lastSuccessful: best,
                    topics: page.items,
                  },
                });
              }
            },
            () => {
              if (active(lifecycle)) {
                repaint({
                  picker: { latestAttempt: answer.latestAttempt, lastSuccessful: best, topics: [] },
                });
              }
            },
          );
        },
        () => {
          // A connection with no discovery route reachable is a picker with no
          // names, and the panel already says what that means.
        },
      );
    }, lifecycle);
  }

  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = readFormValues(form);
    const selection = readClusterSelection(form, "readiness-source");
    const destination = node.querySelector("#readiness-destination-name");
    const topics = String(values.topics || "")
      .split(",").map((t) => t.trim()).filter((t) => t.length > 0);
    const request = {
      operation: "backup",
      backup: {
        sourceConnection: selection.name,
        topics: topics,
      },
    };
    const chosen = destination === null ? "" : String(destination.value || "").trim();
    if (chosen.length > 0) {
      request.backup.destination = chosen;
    }
    mutation.run(() => api.startPreflight(ns, request));
  }, lifecycle);

  const cancel = node.querySelector("#readiness-cancel");
  if (cancel !== null) {
    listen(cancel, "click", () => {
      const current = readiness.preflight;
      if (!active(lifecycle) || current === null || current === undefined) {
        return;
      }
      cancel.disabled = true;
      api.cancelPreflight(ns, current.id).then(
        () => repaint({}),
        () => {
          cancel.disabled = false;
        },
      );
    }, lifecycle);
  }
}

export async function mountSchedules(node, ns, parse, lifecycle, deps) {
  const api = deps || API;
  try {
    // The schedules AND the runs they produced: a schedule card's recovery
    // points are `Backup` objects naming it, and there is no field on the
    // schedule that carries them.
    // THE SCHEDULES, THE RUNS THEY PRODUCED, AND THE SAVED CONNECTIONS. The
    // third read is the source selector's list: the form offers a choice among
    // objects that exist, and the submit resolves the chosen UID against this
    // same list rather than against a name typed some time ago.
    const collections = await Promise.all([
      api.list(ns, PLURAL, readOptions(lifecycle)),
      api.list(ns, BACKUPS, readOptions(lifecycle)),
      api.list(ns, CLUSTERS, readOptions(lifecycle)),
    ]);
    if (!active(lifecycle)) {
      return;
    }
    const collection = collections[0];
    const backups = collections[1];
    const clusters = collections[2];
    const objects = itemsOf(collection);
    // THE DESTINATIONS ARE A FOURTH READ AND ITS FAILURE IS NOT THE PAGE'S.
    // In legacy mode it is refused by name; the readiness panel then says so
    // and the rest of this view -- the schedules, their runs, the create form
    // -- renders exactly as it did before.
    const readiness = await readReadiness(api, ns, lifecycle, clusters);
    if (!active(lifecycle)) {
      return;
    }
    // THE PER-CARD VIEW STATE, HELD FOR THIS MOUNT. A draft preview, a
    // readiness verdict a person asked for and the run one click produced are
    // none of them facts about the object -- they are what this reader has
    // done since the page was painted -- so they live here and not in the
    // projection, and a repaint of one card carries them forward.
    const cards = Object.create(null);
    for (const object of objects) {
      cards[((object.metadata || {}).name) || ""] = {};
    }
    // THE RETENTION POLICIES ARE A FIFTH READ AND ITS FAILURE IS NOT THE
    // PAGE'S EITHER. A build whose API has no retention route yet, or an
    // identity with no grant on the kind, must not take the schedules page
    // down with it: the panel then shows this schedule's own recommendation
    // and says that is what it is.
    const extra = {
      cards: cards,
      destinations: readiness.destinations,
      mayOperate: mayOperate(ns),
      retentionPolicies: await readRetentionPolicies(ns, lifecycle),
    };
    if (!active(lifecycle)) {
      return;
    }
    const panels = objects
      .map((object) => renderScheduleCard(ns, object, backups, extra)).join("");
    replace(
      node,
      parse(
        renderScheduleList(collection, readiness.destinations) + panels +
          "<div class=\"form-slot\" id=\"schedule-form-slot\">" +
          renderScheduleForm(scheduleFormView(ns, clusters)) + "</div>" +
          "<div class=\"readiness-slot\" id=\"readiness-slot\">" +
          renderReadinessPanel(readinessView(ns, readiness)) + "</div>",
      ),
    );
    wire(node, ns, parse, lifecycle, api, objects, clusters);
    wireReadiness(node, ns, parse, lifecycle, api, readiness);
    for (const object of objects) {
      wirePolicy(node, ns, parse, lifecycle, api, object, backups, extra);
      wireRunNow(node, ns, parse, lifecycle, api, object, backups, extra);
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** The create form's values, read from the DOM. */
export function readScheduleValues(form) {
  const e = form.elements;
  const source = readClusterSelection(form, "schedule-source");
  return {
    name: String(e.name.value).trim(),
    cron: String(e.cron.value).trim(),
    source: source.name,
    sourceUid: source.uid,
    topics: String(e.topics.value),
    archive: String(e.archive.value).trim(),
    archiveSecret: String(e.archiveSecret.value).trim(),
    keepLast: String(e.keepLast.value),
    keepDays: String(e.keepDays.value),
  };
}

function wire(node, ns, parse, lifecycle, api, objects, clusters) {
  for (const toggle of node.querySelectorAll("form.suspend")) {
    wireToggle(node, ns, parse, lifecycle, api, objects, toggle);
  }
  wireCreate(node, ns, parse, lifecycle, api, clusters);
}

function wireToggle(node, ns, parse, lifecycle, api, objects, toggle) {
  const name = toggle.getAttribute("data-name");
  const key = formKey(ns, SUSPEND_FORM, name);
  const mutation = mutationFor(key);
  const object = (objects || []).find((o) => ((o || {}).metadata || {}).name === name) || null;
  watchMutation(node, key, mutation, (state) => {
    if (state.phase === "succeeded") {
      mutation.clear();
      mountSchedules(node, ns, parse, lifecycle, api);
      return;
    }
    const button = toggle.querySelector("button");
    if (button !== null) {
      button.disabled = state.phase === "pending";
    }
    for (const slot of node.querySelectorAll("[data-suspend-status]")) {
      if (slot.getAttribute("data-suspend-status") === name) {
        replace(slot, parse(renderSuspendStatus(object, state)));
        if (state.phase === "failed" && typeof slot.focus === "function") {
          slot.focus();
        }
      }
    }
  }, lifecycle);
  listen(toggle, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const next = toggle.getAttribute("data-next") === "true";
    mutation.run(() => api.patchSuspend(ns, name, next));
  }, lifecycle);
}

function wireCreate(node, ns, parse, lifecycle, api, clusters) {
  const form = node.querySelector("#schedule-form");
  if (form === null) {
    return;
  }
  const key = formKey(ns, SCHEDULE_FORM);
  const mutation = mutationFor(key);
  const remember = () => {
    if (!active(lifecycle)) {
      return;
    }
    keepDraft(key, readScheduleValues(form), SCHEDULE_DRAFT_FIELDS);
    if (mutation.state.phase === "succeeded") {
      mutation.clear();
      const status = node.querySelector("#schedule-form-status");
      if (status !== null) {
        replace(status, []);
      }
    }
  };
  listen(form, "input", remember, lifecycle);
  listen(form, "change", remember, lifecycle);
  wireSourceSelector(node, form, remember, lifecycle);
  watchMutation(node, key, mutation, (state) => {
    if (state.phase === "succeeded") {
      dropDraft(key);
      mountSchedules(node, ns, parse, lifecycle, api);
      return;
    }
    const slot = node.querySelector("#schedule-form-slot");
    if (slot === null) {
      return;
    }
    replace(slot, parse(renderScheduleForm(scheduleFormView(ns, clusters))));
    wireCreate(node, ns, parse, lifecycle, api, clusters);
    if (state.phase === "failed") {
      focusFirstProblem(node, "#schedule-form-status");
    }
  }, lifecycle);
  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = readScheduleValues(form);
    keepDraft(key, values, SCHEDULE_DRAFT_FIELDS);
    // THE CONNECTIONS ARE READ AGAIN, HERE, BEFORE THE CREATE. Resolving the
    // chosen uid against the list this view read at mount would only catch a
    // connection that was already gone when the form opened; the window that
    // matters is the one between opening the form and submitting it, which is
    // exactly when somebody rebuilds a cluster. The read carries NO ROUTE
    // SIGNAL: a submit in flight is a durable operation and navigation must
    // not cancel it (PLAT-13.2), and this read is part of that operation.
    mutation.run(() => confirmThenCreate(ns, values, api));
  }, lifecycle);
}

/** Re-reads the namespace's saved connections and creates the schedule against
 *  that list, or refuses.
 *
 *  A FAILED RE-READ IS A REFUSAL, not a shrug. The whole point of the read is
 *  to find out whether the connection this draft names still exists; "I could
 *  not find out" is not "it does", and the draft is kept either way, so the
 *  cost of refusing is one more click and the cost of not refusing is a
 *  schedule pointed at a connection nobody chose. */
export async function confirmThenCreate(ns, values, api) {
  let clusters;
  try {
    clusters = await api.list(ns, CLUSTERS);
  } catch (unread) {
    throw invalidInput({
      source: "the saved connections could not be read again before creating this schedule, so " +
        "the connection it names could not be confirmed (" + String(unread && unread.message) +
        "). Nothing was sent; everything you typed is still here.",
    });
  }
  return submitSchedule(ns, values, api, clusters);
}

/** THE SOURCE SELECTOR'S TWO BEHAVIOURS, both local to the form.
 *
 *  The search box filters the options and nothing else -- it issues no request,
 *  changes no selection and never removes an option, so the answer the form
 *  would submit is the same before and after a search. Changing the select
 *  writes the chosen object's NAME back into the hidden input beside it, so the
 *  draft keeps the pair `{uid, name}` a refusal has to print even after the
 *  object is gone. */
export function wireSourceSelector(node, form, remember, lifecycle) {
  const search = form.querySelector("#schedule-source-search");
  const select = form.querySelector("#schedule-source");
  if (select === null) {
    return;
  }
  const hiddenName = form.querySelector("#schedule-source-name");
  const hiddenUid = form.querySelector("#schedule-source-uid");
  const note = form.querySelector("#schedule-source-no-match");
  const sync = () => {
    const option = select.options === undefined ? null : select.options[select.selectedIndex];
    if (hiddenUid !== null) {
      hiddenUid.value = String(select.value === undefined || select.value === null ? "" : select.value);
    }
    if (hiddenName !== null && option !== null && option !== undefined) {
      hiddenName.value = String(option.getAttribute("data-name") || "");
    }
  };
  listen(select, "change", () => {
    if (!active(lifecycle)) {
      return;
    }
    sync();
    remember();
  }, lifecycle);
  if (search !== null) {
    listen(search, "input", () => {
      if (!active(lifecycle)) {
        return;
      }
      const visible = filterSelectorOptions(select, search.value);
      if (note !== null) {
        note.hidden = visible > 0;
      }
    }, lifecycle);
  }
}

// ===========================================================================
// D1 W7: the future policy, its preview, and Back up now
// ===========================================================================
//
// WHAT CHANGED UNDER THIS PAGE. Until PLAT-05.1 a BackupSchedule's spec was
// SEALED: an object-level CEL rule refused every update but `spec.suspend`,
// for every subject, cluster-admin included. That is why this page's one
// update is the suspend toggle and why `ui/api.js` exports no replace. D1 W2
// replaced that rule with three narrower ones -- `sourceRef` is immutable, a
// dynamic selection needs an empty `topics`, a retrying schedule needs a short
// name -- and D1 W6 added the route that edits everything else:
// `PUT .../schedules/{name}`, under an `expectedGeneration` precondition.
//
// IT IS A REPLACE AND THE FORM IS BUILT AROUND THAT. A field omitted from the
// request is REMOVED from the schedule, so a panel that showed three fields
// and sent three fields would silently clear the other nine. Every field of
// the policy is therefore on screen, seeded from the object this page read,
// and every one of them is sent. The panel says so above its button.
//
// AND THE BROWSER STILL NEVER EVALUATES CRON. A preset is compiled to its
// canonical expression by `GET /api/v1/cadence-previews`, which is the
// controller's own engine and its own tz database; the form sends the preset's
// parameters, receives the canonical `schedule` string, and SAVES THAT STRING.
// A preset whose parameters have changed since the last preview cannot be
// saved until it is previewed again -- not as a nag, but because this page has
// no expression to save until the server has produced one.

/** The identity of one schedule's policy form in the draft and mutation
 *  registries. Each schedule has its own record, exactly as the toggle does. */
export const POLICY_FORM = "schedule-policy";

/** The identity of one schedule's manual-run intent. */
export const RUN_NOW_FORM = "schedule-run-now";

/** The fields a policy draft keeps. Names, numbers, a cron line, a zone and a
 *  Secret NAME -- and no credential, exactly as the create form's list. */
export const POLICY_DRAFT_FIELDS = Object.freeze([
  "mode", "cron", "minute", "hour", "dayOfWeek", "dayOfMonth", "n",
  "timeZone", "topics", "selection", "incompleteDiscovery", "excludeTopics",
  "excludePrefixes", "archive", "archiveSecret", "destination",
  "concurrencyPolicy", "startingDeadlineSeconds", "catchUpPolicy",
  "maxRetries", "retryDelaySeconds", "activeDeadlineSeconds",
  "keepLast", "keepDays", "suspended",
]);

/** THE PRESET CATALOGUE, AS DISPLAY DATA AND NOTHING ELSE.
 *
 *  `weirkeeper::cadence::presets` is the single catalogue and it emits
 *  `ui/tests/fixtures/cadence-presets.json`; `ui/tests/d1.spec.js` compares
 *  the kinds and the parameter bounds below with that file, so this table
 *  cannot drift from Rust without a red row. What is deliberately NOT here is
 *  the `cronTemplate`: filling it in would be this page compiling a preset,
 *  which is the second implementation D1 section 4.2 forbids. The templates stay in
 *  the fixture as evidence of what the server will produce, and the server
 *  produces it. */
export const CADENCE_PRESETS = Object.freeze([
  Object.freeze({
    kind: "hourly",
    words: "Every hour, at a fixed minute",
    parameters: Object.freeze([Object.freeze({ name: "minute", min: 0, max: 59 })]),
  }),
  Object.freeze({
    kind: "everyNHours",
    words: "Every N hours, at a fixed minute",
    parameters: Object.freeze([
      Object.freeze({ name: "n", min: 2, max: 12, values: Object.freeze([2, 3, 4, 6, 8, 12]) }),
      Object.freeze({ name: "minute", min: 0, max: 59 }),
    ]),
  }),
  Object.freeze({
    kind: "daily",
    words: "Every day, at a fixed time",
    parameters: Object.freeze([
      Object.freeze({ name: "hour", min: 0, max: 23 }),
      Object.freeze({ name: "minute", min: 0, max: 59 }),
    ]),
  }),
  Object.freeze({
    kind: "weekly",
    words: "Every week, on one day",
    parameters: Object.freeze([
      Object.freeze({ name: "dayOfWeek", min: 0, max: 6 }),
      Object.freeze({ name: "hour", min: 0, max: 23 }),
      Object.freeze({ name: "minute", min: 0, max: 59 }),
    ]),
  }),
  Object.freeze({
    kind: "monthly",
    words: "Every month, on one day of the month",
    parameters: Object.freeze([
      Object.freeze({ name: "dayOfMonth", min: 1, max: 28 }),
      Object.freeze({ name: "hour", min: 0, max: 23 }),
      Object.freeze({ name: "minute", min: 0, max: 59 }),
    ]),
  }),
]);

/** The cadence mode a form opens on when the saved expression is not one of
 *  the five. Everything else is "Advanced cron" -- D1 section 4.2's own phrase. */
export const ADVANCED_CRON = "advanced";

/** What an interval preset counts from, said because the obvious reading is
 *  wrong. */
export const EVERY_N_HOURS_SENTENCE =
  "Every N hours means local wall-clock hours divisible by N -- 0, 6, 12 and 18 for N=6 -- and " +
  "NOT N hours after the schedule was created. Across a daylight-saving transition the UTC " +
  "cadence is kept, so an interval schedule neither doubles up nor skips an interval.";

/** The defaults the controller applies to an omitted policy field
 *  (D1 section 4.1), as the words the form shows beside each input.
 *
 *  THEY ARE NOT PREFILLED INTO THE INPUTS, and that is the point. This route
 *  REMOVES a field the request omits, so an input left blank means "do not set
 *  this field" and the schedule goes back to the documented default; an input
 *  prefilled with 3600 would turn every save into a schedule that explicitly
 *  sets what it used to inherit. */
export const POLICY_DEFAULTS = Object.freeze({
  timeZone: "UTC",
  startingDeadlineSeconds: "3600 (one hour)",
  catchUpPolicy: "None -- a slot past its deadline is counted and skipped",
  retry: "no retries",
  retryDelaySeconds: "300, when a retry policy is set without one",
  activeDeadlineSeconds: "3600",
});

/** What the panel says above its button, verbatim. */
export const WHOLE_POLICY_SENTENCE =
  "This sends the WHOLE future policy. A field left blank here is a field being REMOVED from " +
  "the schedule, not a field left alone: the route replaces the policy under the revision this " +
  "panel was opened at. Runs already created are untouched -- each one froze its own copy -- " +
  "and the next admission uses what you save here.";

/** Why a preset cannot be saved before it is previewed. */
export const PREVIEW_BEFORE_SAVE =
  "Preview this cadence before saving it. A preset is compiled to its canonical cron expression " +
  "by the API, against the same tz database the controller schedules with, and that expression " +
  "is what gets saved -- this page does not compile one, because a second cron implementation " +
  "in a browser is a second opinion about when a backup runs.";

/** The catalogue entry for one preset kind, or `undefined`. */
export function presetOf(kind) {
  return CADENCE_PRESETS.find((preset) => preset.kind === kind);
}

/** The cadence mode a SAVED schedule opens its form on: the preset the product
 *  API matched its expression to, or "Advanced cron".
 *
 *  `__preset` IS THE SERVER'S MATCH AND NOT THIS PAGE'S. `projectSchedule`
 *  carries it outside `spec`, because a preset is not a field of the object --
 *  `spec.schedule` is the single source of truth and this is the catalogue
 *  entry that expression IS. In legacy mode there is no match, so every
 *  schedule opens on Advanced cron with its stored expression, which is
 *  exactly what it is. */
export function cadenceModeOf(object) {
  const preset = (object || {}).__preset;
  const kind = preset === null || preset === undefined ? "" : String(preset.kind || "");
  return presetOf(kind) === undefined ? ADVANCED_CRON : kind;
}

/** The draft a policy form opens on, read from the object this page last
 *  listed. Every value is a string, as a form's values are. */
export function policyValuesOf(object) {
  const o = object || {};
  const spec = o.spec || {};
  const preset = o.__preset || {};
  const retry = spec.retry || {};
  const retention = spec.retention || {};
  const archive = spec.archive || {};
  const dynamic = spec.allUserTopics || null;
  const exclude = (dynamic || {}).exclude || {};
  const number = (value) => (typeof value === "number" ? String(value) : "");
  return {
    mode: cadenceModeOf(o),
    cron: String(spec.schedule || ""),
    minute: number(preset.minute),
    hour: number(preset.hour),
    dayOfWeek: number(preset.dayOfWeek),
    dayOfMonth: number(preset.dayOfMonth),
    n: number(preset.n),
    timeZone: String(spec.timeZone || ""),
    selection: dynamic === null ? "named" : "dynamic",
    topics: (Array.isArray(spec.topics) ? spec.topics : []).join(", "),
    incompleteDiscovery: String((dynamic || {}).incompleteDiscovery || ""),
    excludeTopics: (Array.isArray(exclude.topics) ? exclude.topics : []).join(", "),
    excludePrefixes: (Array.isArray(exclude.prefixes) ? exclude.prefixes : []).join(", "),
    archive: String(archive.url || ""),
    archiveSecret: String((archive.secretRef || {}).name || ""),
    destination: String((spec.destinationRef || {}).name || ""),
    concurrencyPolicy: String(spec.concurrencyPolicy || ""),
    startingDeadlineSeconds: number(spec.startingDeadlineSeconds),
    catchUpPolicy: String(spec.catchUpPolicy || ""),
    maxRetries: number(retry.maxRetries),
    retryDelaySeconds: number(retry.delaySeconds),
    activeDeadlineSeconds: number(spec.activeDeadlineSeconds),
    keepLast: number(retention.keepLast),
    keepDays: number(retention.keepDays),
    suspended: spec.suspend === true ? "true" : "false",
  };
}

/** The cadence-preview query one set of form values asks for, or `null` when
 *  the values do not describe a cadence yet.
 *
 *  EXACTLY ONE OF `schedule` OR `preset`. The route answers `400` for both and
 *  `422` for a parameter belonging to a DIFFERENT preset, so this sends the
 *  named preset's own parameters and nothing else. */
export function previewQueryFor(values) {
  const v = values || {};
  const query = { count: PREVIEW_COUNT };
  const zone = String(v.timeZone || "").trim();
  if (zone.length > 0) {
    query.timeZone = zone;
  }
  if (v.mode === ADVANCED_CRON) {
    const cron = String(v.cron || "").trim();
    if (cron.length === 0) {
      return null;
    }
    query.schedule = cron;
    return query;
  }
  const preset = presetOf(v.mode);
  if (preset === undefined) {
    return null;
  }
  query.preset = preset.kind;
  for (const parameter of preset.parameters) {
    const raw = String(v[parameter.name] === undefined ? "" : v[parameter.name]).trim();
    if (!/^[0-9]+$/.test(raw)) {
      return null;
    }
    query[parameter.name] = Number(raw);
  }
  return query;
}

/** How many firings a draft preview asks for. Five, which is what the
 *  controller stores in `status.nextRuns`, so a draft and a saved schedule
 *  show the same number of rows and a reader is comparing like with like. */
export const PREVIEW_COUNT = 5;

/** Whether the preview on screen is a preview OF these values. Compared as the
 *  query, not as the form: two drafts that ask the same question get the same
 *  answer, and a change to a field the preview does not depend on -- retention,
 *  the archive -- does not invalidate it. */
export function previewMatches(preview, values) {
  const p = preview || null;
  if (p === null || p.query === undefined || p.query === null) {
    return false;
  }
  const wanted = previewQueryFor(values);
  if (wanted === null) {
    return false;
  }
  return JSON.stringify(p.query) === JSON.stringify(wanted);
}

/** The page's own checks over a policy draft, by field. A CONVENIENCE: the
 *  API's `422` and the controller's `Ready` condition are the gate, and this
 *  refuses only what this page can be sure of. */
export function validatePolicy(values) {
  const v = values || {};
  const problems = Object.create(null);
  if (v.mode === ADVANCED_CRON) {
    const cron = String(v.cron || "").trim();
    const fields = cron.split(/\s+/).filter((f) => f.length > 0);
    const macro = cron === "@hourly" || cron === "@daily" || cron === "@weekly";
    if (!macro && (fields.length !== 5 || fields.some((f) => !CRON_FIELD.test(f)))) {
      problems.cron = "five cron fields (minute hour day-of-month month day-of-week), or " +
        "@hourly, @daily or @weekly";
    }
  } else {
    const preset = presetOf(v.mode);
    if (preset === undefined) {
      problems.mode = "choose a cadence: one of the five presets, or Advanced cron";
    } else {
      for (const parameter of preset.parameters) {
        const raw = String(v[parameter.name] === undefined ? "" : v[parameter.name]).trim();
        if (!/^[0-9]+$/.test(raw)) {
          problems[parameter.name] = "a whole number from " + String(parameter.min) + " to " +
            String(parameter.max);
          continue;
        }
        const value = Number(raw);
        if (value < parameter.min || value > parameter.max) {
          problems[parameter.name] = "from " + String(parameter.min) + " to " +
            String(parameter.max);
        } else if (parameter.values !== undefined &&
          parameter.values.indexOf(value) === -1) {
          problems[parameter.name] = "one of " + parameter.values.join(", ");
        }
      }
    }
  }
  // THE SELECTION'S TWO SHAPES, AND NOTHING BETWEEN THEM. The CRD's own rule
  // R2 refuses `allUserTopics` beside a non-empty `topics`, and an empty
  // allowlist with no dynamic block is a schedule that backs nothing up; both
  // are refused here so neither reaches the API as a 422 a reader has to
  // decode.
  if (v.selection === "dynamic") {
    if (INCOMPLETE_DISCOVERY_POLICIES.indexOf(String(v.incompleteDiscovery || "")) === -1) {
      problems.incompleteDiscovery = "choose what a run does when discovery cannot prove it saw " +
        "everything: " + INCOMPLETE_DISCOVERY_POLICIES.join(" or ") + ". There is no default, " +
        "because both possible defaults are wrong in a way you would not notice";
    }
  } else {
    const topics = String(v.topics || "").split(",").map((t) => t.trim())
      .filter((t) => t.length > 0);
    if (topics.length === 0) {
      problems.topics = "name at least one topic; an empty list is not an allowlist";
    } else {
      const globbed = topics.filter((t) => t.split("").some((c) => GLOB.indexOf(c) !== -1));
      if (globbed.length > 0) {
        problems.topics = "names, never patterns: " + globbed.join(", ") +
          " carries a glob metacharacter";
      }
    }
  }
  const archive = String(v.archive || "").trim();
  const destination = String(v.destination || "").trim();
  if (destination.length === 0 && (archive.length === 0 ||
    archive.indexOf(SCHEME_SEPARATOR) <= 0)) {
    problems.archive = "an object-store URL with its scheme, such as s3" + SCHEME_SEPARATOR +
      "bucket/prefix -- or choose a saved destination instead";
  }
  for (const [key, min, max] of [
    ["startingDeadlineSeconds", 60, 604800],
    ["activeDeadlineSeconds", 60, 86400],
    ["maxRetries", 0, 3],
    ["retryDelaySeconds", 60, 21600],
    ["keepLast", 0, 1000000],
    ["keepDays", 0, 1000000],
  ]) {
    const raw = String(v[key] === undefined || v[key] === null ? "" : v[key]).trim();
    if (raw.length === 0) {
      continue;
    }
    if (!/^[0-9]+$/.test(raw)) {
      problems[key] = "a whole number, or blank for the default";
    } else if (Number(raw) < min || Number(raw) > max) {
      problems[key] = "from " + String(min) + " to " + String(max) + ", or blank";
    }
  }
  return problems;
}

/** The `UpdateSchedulePolicyRequest` a filled-in panel produces.
 *
 *  `schedule` IS THE CANONICAL EXPRESSION THE PREVIEW RETURNED for a preset,
 *  and the typed line for Advanced cron. `expectedGeneration` is the revision
 *  the panel was opened at, and `sourceRef` is NEVER sent: the route carries it
 *  only to refuse it, and this page has no reason to ask for that refusal. */
export function policyBody(values, generation, canonicalSchedule) {
  const v = values || {};
  const text = (key) => String(v[key] === undefined || v[key] === null ? "" : v[key]).trim();
  const whole = (key) => (text(key).length === 0 ? null : Number(text(key)));
  const topics = text("topics").split(",").map((t) => t.trim()).filter((t) => t.length > 0);
  const selection = v.selection === "dynamic"
    ? { topics: [], allUserTopics: { incompleteDiscovery: text("incompleteDiscovery") } }
    : { topics: topics };
  if (v.selection === "dynamic") {
    const exclude = {};
    const names = text("excludeTopics").split(",").map((t) => t.trim())
      .filter((t) => t.length > 0);
    const prefixes = text("excludePrefixes").split(",").map((t) => t.trim())
      .filter((t) => t.length > 0);
    if (names.length > 0) {
      exclude.topics = names;
    }
    if (prefixes.length > 0) {
      exclude.prefixes = prefixes;
    }
    if (Object.keys(exclude).length > 0) {
      selection.allUserTopics.exclude = exclude;
    }
  }
  const body = {
    expectedGeneration: generation,
    schedule: String(canonicalSchedule),
    topicSelection: selection,
    suspended: v.suspended === "true" || v.suspended === true,
  };
  if (text("timeZone").length > 0) {
    body.timeZone = text("timeZone");
  }
  // ONE LOCATION, NEVER BOTH. `archive` and `destinationRef` are two spellings
  // of one place, and the CRD's own sentinel rule refuses a schedule carrying
  // a real URL beside a destination. A chosen destination wins and the inline
  // fields are not sent at all; the API builds the sentinel URL itself.
  if (text("destination").length > 0) {
    body.destinationRef = { name: text("destination") };
  } else {
    const archive = { url: text("archive") };
    if (text("archiveSecret").length > 0) {
      archive.credentialRef = { name: text("archiveSecret") };
    }
    body.archive = archive;
  }
  if (text("concurrencyPolicy").length > 0) {
    body.concurrencyPolicy = text("concurrencyPolicy");
  }
  if (whole("startingDeadlineSeconds") !== null) {
    body.startingDeadlineSeconds = whole("startingDeadlineSeconds");
  }
  if (text("catchUpPolicy").length > 0) {
    body.catchUpPolicy = text("catchUpPolicy");
  }
  if (whole("maxRetries") !== null) {
    const retry = { maxRetries: whole("maxRetries") };
    if (whole("retryDelaySeconds") !== null) {
      retry.delaySeconds = whole("retryDelaySeconds");
    }
    body.retry = retry;
  }
  if (whole("activeDeadlineSeconds") !== null) {
    body.activeDeadlineSeconds = whole("activeDeadlineSeconds");
  }
  const retention = {};
  if (whole("keepLast") !== null) {
    retention.keepLast = whole("keepLast");
  }
  if (whole("keepDays") !== null) {
    retention.keepDays = whole("keepDays");
  }
  if (Object.keys(retention).length > 0) {
    body.retention = retention;
  }
  return body;
}

// ------------------------------------------------------ the policy form

function policyId(name, field) {
  return "policy-" + name + "-" + field;
}

function policyNumber(name, values, errors, field, label, help) {
  return (
    "<div class=\"field\"><label for=\"" + esc(policyId(name, field)) + "\">" + esc(label) +
    "</label><input id=\"" + esc(policyId(name, field)) + "\" name=\"" + esc(field) +
    "\" type=\"number\" value=\"" + esc(String(values[field] || "")) + "\"" +
    invalidAttributes(policyId(name, field), errors[field]) + ">" +
    "<p class=\"help\">" + esc(help) + "</p>" +
    fieldErrorLine(policyId(name, field), errors[field]) + "</div>"
  );
}

function optionList(id, field, chosen, options) {
  return options.map((option) => {
    const value = option[0];
    return "<option value=\"" + esc(value) + "\"" +
      (String(chosen) === value ? " selected" : "") + ">" + esc(option[1]) + "</option>";
  }).join("");
}

/** The cadence half of the panel: the mode, its parameters and the zone. */
function renderCadenceFields(name, values, errors) {
  const preset = presetOf(values.mode);
  const modes = [[ADVANCED_CRON, "Advanced cron"]].concat(
    CADENCE_PRESETS.map((p) => [p.kind, p.words]),
  );
  return (
    "<fieldset class=\"cadence\"><legend>cadence</legend>" +
    "<div class=\"field\"><label for=\"" + esc(policyId(name, "mode")) + "\">how often</label>" +
    "<select id=\"" + esc(policyId(name, "mode")) + "\" name=\"mode\">" +
    optionList(policyId(name, "mode"), "mode", values.mode, modes) + "</select>" +
    "<p class=\"help\">A preset is compiled to a canonical cron expression by the API and that " +
    "expression is what is stored: spec.schedule is the single source of truth, and a preset " +
    "is never saved as one.</p>" +
    fieldErrorLine(policyId(name, "mode"), errors.mode) + "</div>" +
    (preset === undefined
      ? "<div class=\"field\"><label for=\"" + esc(policyId(name, "cron")) +
        "\">schedule, five cron fields</label><input id=\"" + esc(policyId(name, "cron")) +
        "\" name=\"cron\" value=\"" + esc(String(values.cron || "")) + "\"" +
        invalidAttributes(policyId(name, "cron"), errors.cron) + ">" +
        "<p class=\"help\">minute hour day-of-month month day-of-week, read in the time zone " +
        "below.</p>" + fieldErrorLine(policyId(name, "cron"), errors.cron) + "</div>"
      : preset.parameters.map((parameter) => policyNumber(
        name, values, errors, parameter.name, parameter.name,
        "from " + String(parameter.min) + " to " + String(parameter.max) +
          (parameter.values === undefined ? "" : "; one of " + parameter.values.join(", ")),
      )).join("") +
        (preset.kind === "everyNHours"
          ? "<p class=\"help\">" + esc(EVERY_N_HOURS_SENTENCE) + "</p>"
          : "")) +
    "<div class=\"field\"><label for=\"" + esc(policyId(name, "timeZone")) + "\">time zone</label>" +
    "<input id=\"" + esc(policyId(name, "timeZone")) + "\" name=\"timeZone\" value=\"" +
    esc(String(values.timeZone || "")) + "\"" +
    invalidAttributes(policyId(name, "timeZone"), errors.timeZone) + ">" +
    "<p class=\"help\">An IANA name such as Europe/Berlin. Blank means " +
    esc(POLICY_DEFAULTS.timeZone) + ": the cron fields are read as UTC times. A name this " +
    "build's tz database does not have is refused by the controller with reason " +
    "UnknownTimeZone -- never a silent fall back to UTC.</p>" +
    fieldErrorLine(policyId(name, "timeZone"), errors.timeZone) + "</div>" +
    "<div class=\"actions\"><button type=\"button\" data-preview=\"" + esc(name) +
    "\">Preview next runs</button></div>" +
    "</fieldset>"
  );
}

/** The selection half: a named allowlist, or the dynamic block. */
function renderSelectionFields(name, values, errors) {
  const dynamic = values.selection === "dynamic";
  return (
    "<fieldset class=\"selection\"><legend>topics</legend>" +
    "<div class=\"field\"><label for=\"" + esc(policyId(name, "selection")) + "\">selection</label>" +
    "<select id=\"" + esc(policyId(name, "selection")) + "\" name=\"selection\">" +
    optionList(policyId(name, "selection"), "selection", values.selection, [
      ["named", "Named topics -- an explicit allowlist"],
      ["dynamic", "All user topics -- resolved per run from a discovery"],
    ]) + "</select>" +
    "<p class=\"help\">" + esc(DYNAMIC_SELECTION_SENTENCE) + "</p></div>" +
    (dynamic
      ? "<div class=\"field\"><label for=\"" + esc(policyId(name, "incompleteDiscovery")) +
        "\">when discovery cannot prove it saw everything</label>" +
        "<select id=\"" + esc(policyId(name, "incompleteDiscovery")) +
        "\" name=\"incompleteDiscovery\">" +
        optionList(policyId(name, "incompleteDiscovery"), "incompleteDiscovery",
          values.incompleteDiscovery, [
            ["", "choose one -- there is no default"],
            ["Refuse", "Refuse -- fail the run"],
            ["BackUpVisibleTopics",
              "BackUpVisibleTopics -- run, and label the coverage visible-only"],
          ]) + "</select>" +
        "<p class=\"help\">Required, with no default: both possible defaults are wrong in a way " +
        "you would not notice.</p>" +
        fieldErrorLine(policyId(name, "incompleteDiscovery"), errors.incompleteDiscovery) +
        "</div>" +
        "<div class=\"field\"><label for=\"" + esc(policyId(name, "excludeTopics")) +
        "\">exclude these exact names, comma separated</label><input id=\"" +
        esc(policyId(name, "excludeTopics")) + "\" name=\"excludeTopics\" value=\"" +
        esc(String(values.excludeTopics || "")) + "\">" +
        "<p class=\"help\">Exact names, never patterns. Internal topics are always excluded.</p>" +
        "</div>" +
        "<div class=\"field\"><label for=\"" + esc(policyId(name, "excludePrefixes")) +
        "\">exclude these literal prefixes, comma separated</label><input id=\"" +
        esc(policyId(name, "excludePrefixes")) + "\" name=\"excludePrefixes\" value=\"" +
        esc(String(values.excludePrefixes || "")) + "\">" +
        "<p class=\"help\">Literal prefixes, never patterns: dev- matches dev-orders and not " +
        "orders-dev.</p></div>"
      : "<div class=\"field\"><label for=\"" + esc(policyId(name, "topics")) +
        "\">topics, comma separated -- names, never patterns</label><input id=\"" +
        esc(policyId(name, "topics")) + "\" name=\"topics\" value=\"" +
        esc(String(values.topics || "")) + "\"" +
        invalidAttributes(policyId(name, "topics"), errors.topics) + ">" +
        "<p class=\"help\">An explicit allowlist. Every run under this policy backs up exactly " +
        "these.</p>" + fieldErrorLine(policyId(name, "topics"), errors.topics) + "</div>") +
    "</fieldset>"
  );
}

/** The deadlines, the catch-up policy and the retry policy, each with the
 *  value an absent field means. */
function renderPolicyFields(name, values, errors) {
  return (
    "<fieldset class=\"run-policy\"><legend>deadlines, catch-up and retries</legend>" +
    policyNumber(name, values, errors, "startingDeadlineSeconds", "startingDeadlineSeconds",
      "How long after its instant a slot may still start. 60 to 604800. Blank means the " +
      "default, " + POLICY_DEFAULTS.startingDeadlineSeconds + ".") +
    "<div class=\"field\"><label for=\"" + esc(policyId(name, "catchUpPolicy")) +
    "\">catchUpPolicy</label><select id=\"" + esc(policyId(name, "catchUpPolicy")) +
    "\" name=\"catchUpPolicy\">" +
    optionList(policyId(name, "catchUpPolicy"), "catchUpPolicy", values.catchUpPolicy, [
      ["", "not set -- the default, " + POLICY_DEFAULTS.catchUpPolicy],
      ["None", "None -- count the missed slot and move on"],
      ["Latest", "Latest -- run the latest missed slot once, and only that one"],
    ]) + "</select>" +
    "<p class=\"help\">Latest never runs more than one catch-up, ever, and never a slot older " +
    "than the revision in force: a schedule edited at noon does not retroactively back up the " +
    "morning under the new policy.</p></div>" +
    policyNumber(name, values, errors, "maxRetries", "retry.maxRetries",
      "0 to 3. Blank means " + POLICY_DEFAULTS.retry + ". A retry is a NEW Backup with a new " +
      "execution id; nothing re-runs an existing one.") +
    policyNumber(name, values, errors, "retryDelaySeconds", "retry.delaySeconds",
      "60 to 21600. Blank means " + POLICY_DEFAULTS.retryDelaySeconds + ".") +
    policyNumber(name, values, errors, "activeDeadlineSeconds", "activeDeadlineSeconds",
      "The run's own deadline, copied into each Backup. 60 to 86400. Blank means the default, " +
      POLICY_DEFAULTS.activeDeadlineSeconds + ". A dynamic selection needs at least 120.") +
    "<div class=\"field\"><label for=\"" + esc(policyId(name, "concurrencyPolicy")) +
    "\">concurrencyPolicy</label><select id=\"" + esc(policyId(name, "concurrencyPolicy")) +
    "\" name=\"concurrencyPolicy\">" +
    optionList(policyId(name, "concurrencyPolicy"), "concurrencyPolicy",
      values.concurrencyPolicy, [
        ["", "not set -- the default, Forbid"],
        ["Forbid", "Forbid -- never overlap"],
        ["Allow", "Allow -- permit overlapping slots"],
      ]) + "</select></div>" +
    "<div class=\"field\"><label for=\"" + esc(policyId(name, "suspended")) +
    "\">suspend</label><select id=\"" + esc(policyId(name, "suspended")) + "\" name=\"suspended\">" +
    optionList(policyId(name, "suspended"), "suspended", values.suspended, [
      ["false", "not suspended"],
      ["true", "suspended -- admit no further slots"],
    ]) + "</select>" +
    "<p class=\"help\">A suspend flip moves the schedule's generation and leaves the run-policy " +
    "digest unchanged, which is why both are printed on every run.</p></div>" +
    "</fieldset>"
  );
}

/** Where the runs are written, and what a saved destination costs to change. */
function renderPolicyLocation(name, values, errors, destinations) {
  const all = Array.isArray(destinations) ? destinations : [];
  return (
    "<fieldset class=\"legacy-archive\"><legend>where runs are written</legend>" +
    "<div class=\"field\"><label for=\"" + esc(policyId(name, "destination")) +
    "\">saved destination</label><select id=\"" + esc(policyId(name, "destination")) +
    "\" name=\"destination\">" +
    optionList(policyId(name, "destination"), "destination", values.destination,
      [["", "none -- use the inline archive below"]]
        .concat(all.map((d) => [d.name, d.name + " -- " + d.canonicalUrl]))) + "</select>" +
    "<p class=\"help\">Choosing one sends destinationRef and NOT the inline fields: the two are " +
    "two spellings of one location, and the API writes the sentinel URL itself.</p></div>" +
    "<div class=\"field\"><label for=\"" + esc(policyId(name, "archive")) +
    "\">archive URL</label><input id=\"" + esc(policyId(name, "archive")) +
    "\" name=\"archive\" value=\"" + esc(String(values.archive || "")) + "\"" +
    invalidAttributes(policyId(name, "archive"), errors.archive) + ">" +
    fieldErrorLine(policyId(name, "archive"), errors.archive) + "</div>" +
    "<div class=\"field\"><label for=\"" + esc(policyId(name, "archiveSecret")) +
    "\">archive credential (Secret name)</label><input id=\"" +
    esc(policyId(name, "archiveSecret")) + "\" name=\"archiveSecret\" value=\"" +
    esc(String(values.archiveSecret || "")) + "\">" +
    "<p class=\"help\">Only its name is sent.</p></div>" +
    policyNumber(name, values, errors, "keepLast", "retention keepLast",
      "How many sets a retention evaluation keeps. It reports; it never deletes.") +
    policyNumber(name, values, errors, "keepDays", "retention keepDays",
      "How many days of sets it keeps.") +
    "</fieldset>"
  );
}

/** ONE SCHEDULE'S FUTURE POLICY, editable.
 *
 *  `view` is `{name, generation, values, errors, state, preview, destinations,
 *  mayOperate, now}`. `generation` absent is the one case the panel refuses to
 *  open a form for: the route's precondition IS the generation, and a page
 *  that sent an edit without one would be asking the API to apply a policy to
 *  whatever revision happens to be current -- which is the lost update this
 *  precondition exists to prevent. */
export function renderPolicyForm(view) {
  const v = view || {};
  const name = String(v.name || "");
  const values = Object.assign({}, v.values || {});
  const errors = ((v.errors || {}).fields) || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  const generation = v.generation;
  const preview = v.preview || null;
  const previewed = previewMatches(preview, values);
  if (v.mayOperate === false) {
    return (
      "<section class=\"policy\" data-policy=\"" + esc(name) + "\"><h4>Future policy</h4>" +
      "<p class=\"note\">This login may read this schedule and not edit its policy.</p></section>"
    );
  }
  if (typeof generation !== "number") {
    return (
      "<section class=\"policy\" data-policy=\"" + esc(name) + "\"><h4>Future policy</h4>" +
      "<p class=\"note\" data-no-generation=\"1\">" + esc(NO_GENERATION_SENTENCE) +
      "</p></section>"
    );
  }
  return (
    "<section class=\"policy\" data-policy=\"" + esc(name) + "\"><h4>Future policy</h4>" +
    "<p class=\"note\">" + esc(WHOLE_POLICY_SENTENCE) + "</p>" +
    "<p class=\"note\" data-editing-generation=\"" + String(generation) + "\">Editing revision " +
    "<code>g" + String(generation) + "</code>. Saving makes the next revision, and the NEXT run " +
    "admitted carries it; a run already created keeps the revision it froze.</p>" +
    "<form class=\"policy-form\" data-name=\"" + esc(name) + "\" novalidate" +
    (pending ? " aria-busy=\"true\"" : "") + ">" +
    "<fieldset class=\"form-body\"" + (pending ? " disabled" : "") + ">" +
    renderCadenceFields(name, values, errors) +
    renderSelectionFields(name, values, errors) +
    renderPolicyLocation(name, values, errors, v.destinations) +
    renderPolicyFields(name, values, errors) +
    "<div class=\"actions\"><button type=\"submit\" class=\"primary\"" +
    (previewed || values.mode === ADVANCED_CRON ? "" : " disabled") + ">Save policy</button>" +
    "</div></fieldset>" +
    "<div class=\"form-status\" data-policy-status=\"" + esc(name) + "\" tabindex=\"-1\">" +
    mutationStatus(state, {
      kind: "BackupSchedule", name: name, verb: "replace", field: "spec", value: "the policy",
    }, ((v.errors || {}).unmatched)) + "</div>" +
    "</form>" +
    (previewed || values.mode === ADVANCED_CRON
      ? ""
      : "<p class=\"note\" data-preview-first=\"1\">" + esc(PREVIEW_BEFORE_SAVE) + "</p>") +
    "<div class=\"preview-slot\" data-preview-slot=\"" + esc(name) + "\">" +
    renderPolicyPreview(preview, values) + "</div>" +
    "</section>"
  );
}

/** Why a schedule whose revision this build does not publish cannot be
 *  edited from here. */
export const NO_GENERATION_SENTENCE =
  "This schedule's policy cannot be edited from this page: the edit route takes the revision " +
  "the form was opened at as a precondition, and this build is not publishing a generation for " +
  "this object. Without it the request would ask the API to replace whatever revision happens " +
  "to be current when it lands, which is the lost update the precondition exists to prevent. " +
  "Edit it with kubectl, which carries its own resourceVersion precondition.";

/** A draft cadence's preview, or the words that say none has been taken. */
export function renderPolicyPreview(preview, values) {
  const p = preview || null;
  if (p === null) {
    return "";
  }
  if (p.error !== undefined && p.error !== null) {
    return errorBlock(p.error, true);
  }
  const answer = p.answer || {};
  const stale = !previewMatches(p, values || {});
  return (
    (stale
      ? "<p class=\"note\" data-preview-stale=\"1\">" + badge("unverified", "out of date") +
        " The cadence on screen has changed since this preview was taken, so these instants are " +
        "the previous cadence's. Preview again before saving.</p>"
      : "<p class=\"note\" data-canonical=\"" + esc(String(answer.schedule || "")) + "\">" +
        "This cadence compiles to <code>" + esc(String(answer.schedule || "")) + "</code>, and " +
        "that expression is what gets saved.</p>") +
    nextRunsPanel({
      runs: answer.runs,
      timeZone: answer.timeZone,
      tzdb: answer.tzdb,
      heading: "The next " + String(PREVIEW_COUNT) + " firings of this draft",
    })
  );
}

// ------------------------------------------------------- Back up now

// THE INTENT IS A FIELD OF THE DRAFT, AND ITS LIFETIME IS THE DRAFT'S.
//
// A manual run has NO NAME UNTIL THE SERVER DERIVES ONE, so the trick every
// other create on this page uses -- creating the same name twice is
// `AlreadyExists`, which IS the idempotence -- is not available. What replaces
// it is the product API's `Idempotency-Key`: the same key returns the same
// run, with `replayed: true`, however many times it is sent.
//
// THE FIRST CUT OF THIS COMPOSED THE KEY FROM A MODULE-LEVEL COUNTER, and that
// was a defect a live review caught (review F3). `intentCounter` resets on
// every page load, so the FIRST intent minted after a reload reproduced the
// first intent minted before it, byte for byte -- and the second one did not.
// A click after a reload was therefore sometimes a replay of the earlier run,
// sometimes a `409 idempotency_conflict`, and sometimes a new run, decided by
// the order the previous page load happened to mint intents in. The page's own
// sentence, `ui/README.md` and `docs/kubernetes.md` section 16 all said the
// opposite, and the row that "held" it asserted only that the sentence was ON
// SCREEN, never that it was true.
//
// SO THE KEY IS NOW A DRAFT FIELD, WITH A RANDOM BODY.
//
//   * ONE INTENT PER DRAFT. `lifecycle.js`'s draft registry is keyed by
//     `formKey(ns, RUN_NOW_FORM, schedule)`, which is exactly the scope an
//     intent has. Every resend of that intent -- a double click, a retry after
//     a timeout, a "Check status" on an unknown outcome -- reads the same
//     field and sends the same key, and the API answers with the same run.
//   * A RELOAD LOSES IT, GENUINELY. The draft registry is module state and
//     there is no browser storage anywhere in this tree
//     (`scripts/check-ui-offline.sh` fails the build over the byte sequences
//     that would introduce one), so the draft and the key go together -- and
//     because the body is RANDOM rather than a counter, a new page load cannot
//     reproduce an old key even by accident. The sentence "a click after a
//     reload is a deliberate NEW run" is now true.
//   * A DURABLE RUN ENDS THE DRAFT. Once a run exists the panel offers "Back
//     up again", which drops the draft and therefore mints a new intent --
//     PLAT-06.2's second acceptance clause, "a deliberate later backup creates
//     another", which was unreachable before (review F2).
//
// PLAT-13.2's draft rules apply unchanged: the field is a declared one, it is
// a string, and it is no more a credential than a Secret's name is.

/** The one field a manual-run draft keeps: the idempotency intent. */
export const RUN_NOW_DRAFT_FIELDS = Object.freeze(["intent"]);

/** A fresh intent. `logweir-ui.manual.` plus 32 random hex characters -- 50
 *  characters, well inside the product API's 8-to-128 budget.
 *
 *  RANDOM, NOT ORDERED AND NOT TIMED. A counter collides across page loads
 *  (review F3) and a clock collides between two tabs opened in the same
 *  millisecond; `getRandomValues` does neither, and it is available in an
 *  insecure context, unlike `randomUUID`, so this does not narrow where the
 *  page can be served from.
 *
 *  A BROWSER WITH NO `crypto` GETS A REFUSAL AND NOT A WEAKER KEY. There is no
 *  fallback here on purpose: a key this page could not make unique is a key
 *  that could silently replay somebody else's run, and refusing to mint one is
 *  the only honest answer. Nothing in this tree can reach that branch --
 *  `ui/plan.js` already refuses to load outside a secure context -- and it is
 *  written down rather than assumed. */
export function mintIntent() {
  const source = globalThis.crypto;
  if (source === undefined || source === null || typeof source.getRandomValues !== "function") {
    throw refusal(
      "this page will not create a manual backup here: minting an idempotency key needs the " +
        "platform's random source, and it is unavailable. Without a unique key a second click " +
        "could return somebody else's run instead of starting yours.",
    );
  }
  const bytes = source.getRandomValues(new Uint8Array(16));
  let hex = "";
  for (const byte of bytes) {
    hex += byte.toString(16).padStart(2, "0");
  }
  return "logweir-ui.manual." + hex;
}

/** The idempotency intent held by the draft at `key`, minted on first use and
 *  kept in that draft for as long as it lives. */
export function intentFor(key) {
  const draft = readDraft(key);
  const held = draft === null ? undefined : draft.intent;
  if (typeof held === "string" && held.length >= 8) {
    return held;
  }
  const minted = mintIntent();
  keepDraft(key, { intent: minted }, RUN_NOW_DRAFT_FIELDS);
  return minted;
}

/** Ends the intent at `key` and mints the next one. This is what "Back up
 *  again" does: the run the previous intent made still exists and is still on
 *  screen, and the next click is a DIFFERENT request that creates a second
 *  run. */
export function newIntent(key) {
  dropDraft(key);
  return intentFor(key);
}

/** What a manual run is, and what it is not. */
export const RUN_NOW_SENTENCE =
  "A manual run creates one Backup right now, with the topics, archive and deadline of this " +
  "schedule's CURRENT revision, copied at the moment it is created. It does not move the " +
  "schedule: no slot is consumed, no missed slot is caught up, and the next scheduled run is " +
  "the one it was going to be.";

/** Why a suspended schedule still offers the button. */
export const SUSPENDED_NOTICE =
  "This schedule is suspended, so it admits no slots of its own. A manual run is still allowed " +
  "-- nothing about a schedule blocks one -- and running it does NOT resume the schedule. " +
  "Confirm below if that is what you mean.";

/** Why an active run does not block one either. */
export const ACTIVE_RUN_NOTICE =
  "A run of this schedule is already active. A manual run is neither counted against " +
  "concurrencyPolicy nor blocked by it, so this would be a second run against the same topics " +
  "at the same time.";

/** What the page says when readiness was never checked. */
export const READINESS_NOT_CHECKED =
  "Readiness not checked; the run reports its own prerequisites.";

/** What a refresh costs, on screen. */
export const AFTER_REFRESH_SENTENCE =
  "Reloading this page forgets the idempotency intent a click holds, because that intent is a " +
  "field of this form's draft and nothing in this console is stored in the browser. The intent " +
  "is random, not counted, so a new page load cannot reproduce an old one by accident. The manual " +
  "runs below are what already exists: read them before clicking again, because a click after a " +
  "reload is a deliberate NEW run and will create a second one.";

/** The manual runs of one schedule, newest first, from the Backups this page
 *  read. A run is manual when its own recorded trigger says so. */
export function manualRunsOf(schedule, backups) {
  const name = String(schedule || "");
  return itemsOf(backups)
    .filter((backup) => {
      const spec = (backup || {}).spec || {};
      const trigger = spec.trigger || {};
      return trigger.kind === "Manual" && ((spec.scheduleRef || {}).name === name);
    })
    .slice()
    .sort((a, b) => String(((b || {}).metadata || {}).creationTimestamp || "")
      .localeCompare(String(((a || {}).metadata || {}).creationTimestamp || "")));
}

/** What "Back up again" does, said before it is clicked. */
export const RUN_AGAIN_SENTENCE =
  "Back up again starts a SECOND run. The run below keeps its own identity and its own frozen " +
  "revision; this mints a new idempotency intent, so the API treats the next click as the new " +
  "request it is rather than as a repeat of the one that made that run.";

/** What a `409 policy_changed` means here, in the words D1 section 8.5 uses. */
export const POLICY_CHANGED_SENTENCE =
  "The schedule's policy moved after the revision this panel was rendered at, so nothing was " +
  "created: a manual run copies the revision it names, and running a revision you have not seen " +
  "is exactly what expectedGeneration exists to prevent. Reload the page to see the policy that " +
  "is in force, then Back up again to run it.";

/** What a `409 idempotency_conflict` means here. */
export const INTENT_USED_SENTENCE =
  "The idempotency intent this panel is holding was already spent on a DIFFERENT request, so the " +
  "API refused rather than guessing which of the two you meant. Back up again mints a new intent, " +
  "which is the answer the problem document asks for.";

/** What a `409 state_conflict` means here: the derived name is taken by an
 *  object this scope did not create, and it is never adopted. */
export const NAME_TAKEN_SENTENCE =
  "The name this request derives is already taken by a run this console did not create -- a " +
  "kubectl run, an operator's, or another console's. Nothing was created and nothing was " +
  "adopted: a run is only ever answered as yours when it carries this request's own hash. " +
  "Back up again mints a new intent, which derives a different name.";

/** THE REFUSAL THAT CARRIES A FACT, RENDERED (review F2).
 *
 *  `ui/client.js` decodes the `policy` extension member of a
 *  `409 policy_changed` onto the error -- the only extension member this API
 *  defines -- and until this existed no page read it. The revision that is in
 *  force NOW is the one thing a person needs in order to decide what to do
 *  next, so it is on screen, and both 409s are followed by the control that
 *  makes the next click a new request. */
export function renderRunNowConflict(state) {
  const s = state || {};
  if (s.phase !== "failed") {
    return "";
  }
  const error = s.error || {};
  if (error.reason === "policy_changed") {
    const policy = error.policy || null;
    return (
      "<p class=\"note\" data-run-now-conflict=\"policy_changed\">" +
      badge("unverified", "policy changed") + " " + esc(POLICY_CHANGED_SENTENCE) +
      (policy === null
        ? ""
        : " The revision in force now is " + revisionLine({
          generation: policy.currentGeneration,
          runPolicySha256: policy.currentRunPolicySha256,
        }) + ".") +
      "</p>"
    );
  }
  if (error.reason === "idempotency_conflict") {
    return (
      "<p class=\"note\" data-run-now-conflict=\"idempotency_conflict\">" +
      badge("unverified", "intent already used") + " " + esc(INTENT_USED_SENTENCE) + "</p>"
    );
  }
  // AND THE THIRD 409, WHICH IS ABOUT SOMEBODY ELSE'S OBJECT (review F5).
  // `state_conflict` means the name this request derives is taken by an object
  // THIS SCOPE DID NOT CREATE -- a `kubectl` run, an operator's, another
  // console's -- and it is never adopted. Printing INTENT_USED_SENTENCE for it
  // said "the intent this panel is holding already created a DIFFERENT
  // request's run", which is a false statement about a stranger's run and
  // sends a reader looking for a click nobody made.
  if (error.reason === "state_conflict") {
    return (
      "<p class=\"note\" data-run-now-conflict=\"state_conflict\">" +
      badge("unverified", "name already taken") + " " + esc(NAME_TAKEN_SENTENCE) + "</p>"
    );
  }
  return "";
}

/** Whether the panel should offer "Back up again": a run this click produced,
 *  or a refusal whose only answer is a new intent. */
export function offersAnotherRun(view) {
  const v = view || {};
  if (v.result !== null && v.result !== undefined) {
    return true;
  }
  const state = v.state || {};
  const reason = (state.error || {}).reason;
  // `state_conflict` JOINS THE TWO (review F5): its only answer is a new
  // intent, because the derived name is taken and a new intent derives a
  // different one. A 403 or a 422 is still not answered by spending a fresh
  // key on the same refused request.
  return state.phase === "failed" &&
    (reason === "policy_changed" || reason === "idempotency_conflict" ||
      reason === "state_conflict");
}

/** THE "BACK UP NOW" PANEL for one schedule.
 *
 *  `view` is `{ns, name, object, state, mayOperate, unavailable,
 *  unavailableReason, readiness, acknowledged, runs, result}`.
 *
 *  THE BUTTON'S FIRST STATE IS DISABLED WHEN SOMETHING IS WRONG, AND THE WORDS
 *  BESIDE IT ARE THE OBJECT'S OWN. A suspended schedule and a not-ready
 *  preflight are the two cases; neither is a refusal -- the API gates on
 *  neither, by design -- so what the panel does is require a second, explicit
 *  confirmation and carry the reason THE CONTROLLER OR THE CHECK RECORDED
 *  rather than a sentence this page composed. Confirming a not-ready verdict
 *  sends it as `readinessAcknowledgement`, which becomes an annotation on the
 *  run: a backup taken past a red check says so, for ever, on the object. */
export function renderRunNowPanel(view) {
  const v = view || {};
  const name = String(v.name || "");
  const object = v.object || {};
  const spec = object.spec || {};
  const status = object.status || {};
  const state = v.state || {};
  const pending = state.phase === "pending";
  const runs = Array.isArray(v.runs) ? v.runs : [];
  const suspended = spec.suspend === true;
  const active = Array.isArray(status.activeRuns) && status.activeRuns.length > 0;
  const readiness = v.readiness || null;
  const verdict = readiness === null ? null : readiness.state;
  const mustConfirm = suspended || verdict === "notReady";
  const acknowledged = v.acknowledged === true;
  const another = offersAnotherRun(v);
  const first = runs.length === 0 && status.lastFireTime === undefined;
  if (v.mayOperate === false) {
    return (
      "<section class=\"run-now\" data-run-now=\"" + esc(name) + "\"><h4>Back up now</h4>" +
      "<p class=\"note\">This login may read this schedule and not create a run in this " +
      "namespace.</p></section>"
    );
  }
  return (
    "<section class=\"run-now\" data-run-now=\"" + esc(name) + "\"><h4>" +
    (first ? "Run first backup now" : "Back up now") + "</h4>" +
    "<p class=\"note\">" + esc(RUN_NOW_SENTENCE) + "</p>" +
    (v.unavailable === true
      ? "<p class=\"note\" data-run-now-unavailable=\"" + esc(name) + "\">" +
        cell(v.unavailableReason) + "</p>"
      : "<form class=\"run-now-form\" data-name=\"" + esc(name) + "\" novalidate" +
        (pending ? " aria-busy=\"true\"" : "") + ">" +
        (suspended
          ? "<p class=\"note\" data-suspended-notice=\"" + esc(name) + "\">" +
            badge("pending", "suspended") + " " + esc(SUSPENDED_NOTICE) + "</p>"
          : "") +
        (active
          ? "<p class=\"note\" data-active-notice=\"" + esc(name) + "\">" +
            badge("pending", String(status.activeRuns.length) + " active") + " " +
            esc(ACTIVE_RUN_NOTICE) + "</p>"
          : "") +
        (readiness === null
          ? "<p class=\"note\" data-readiness=\"unchecked\">" + esc(READINESS_NOT_CHECKED) + "</p>"
          : renderPreflight(readiness)) +
        (mustConfirm
          ? "<div class=\"field\"><label><input type=\"checkbox\" name=\"acknowledge\"" +
            (acknowledged ? " checked" : "") + " data-acknowledge=\"" + esc(name) + "\"> " +
            esc(verdict === "notReady"
              ? "Run anyway. The check above says a prerequisite is not met; the run will " +
                "record that it was taken past that verdict."
              : "Run anyway. This schedule is suspended and will stay suspended.") +
            "</label></div>"
          : "") +
        "<div class=\"actions\"><button type=\"submit\"" +
        (pending || (mustConfirm && !acknowledged) ? " disabled" : "") + ">" +
        (first ? "Run first backup now" : "Back up now") + "</button>" +
        // THE SECOND RUN'S CONTROL, AND THE ONLY THING THAT MINTS A NEW INTENT
        // (review F2). PLAT-06.2's acceptance is "repeated clicks create one
        // requested run; a DELIBERATE LATER BACKUP CREATES ANOTHER", and until
        // this button existed the second clause had no route at all: every
        // click on a schedule resent the one intent that schedule ever had, so
        // a console could take exactly one manual backup of it, ever.
        (another
          ? "<button type=\"button\" data-run-again=\"" + esc(name) + "\"" +
            (pending ? " disabled" : "") + ">Back up again</button>"
          : "") +
        "</div>" +
        (another ? "<p class=\"help\">" + esc(RUN_AGAIN_SENTENCE) + "</p>" : "") +
        "<div class=\"form-status\" data-run-now-status=\"" + esc(name) + "\" tabindex=\"-1\">" +
        // `idempotencyKey: true` PICKS THE SENTENCE THAT IS TRUE OF THIS ROUTE
        // (review F4): what makes a resend safe here is the key this click
        // holds, not a name -- there is no name until the server derives one.
        mutationStatus(state, { kind: "Backup", name: "", idempotencyKey: true }, null) +
        renderRunNowConflict(state) +
        "</div>" +
        "</form>") +
    renderRunNowResult(v.ns, v.result) +
    "<p class=\"note\">" + esc(AFTER_REFRESH_SENTENCE) + "</p>" +
    renderManualRuns(v.ns, runs, spec) +
    "</section>"
  );
}

/** The durable run a click produced: its name as a link, what kind of run it
 *  is, and which revision of the schedule it copied. */
export function renderRunNowResult(ns, result) {
  const r = result || null;
  if (r === null) {
    return "";
  }
  const run = r.run || {};
  const meta = run.metadata || {};
  const spec = run.spec || {};
  const context = r.schedule || null;
  return (
    "<p class=\"run-now-result\" data-replayed=\"" + (r.replayed === true ? "true" : "false") +
    "\">" +
    (r.replayed === true
      ? badge("pending", "already started") +
        " That click had already started this run, so the API answered with the run it made " +
        "the first time rather than starting a second one. "
      : badge("green", "started") + " ") +
    detailLink("backups", String(ns || meta.namespace || ""), String(meta.name || "")) + " " +
    // WHERE ONE CLICK'S DURABLE PROGRESS IS (PLAT-12.1). The run exists; this
    // is the view that follows it to a terminal state without a reload, and
    // the link carries the uid so a name reused later lands on a refusal
    // rather than on a different run.
    "<a href=\"" +
    esc(operationRoute(String(ns || meta.namespace || ""), "backup", String(meta.name || ""),
      String(meta.uid || ""))) +
    "\">Follow this run</a> " +
    triggerBadge(spec.trigger) + " " + revisionLine(spec.scheduleRef) +
    (context === null
      ? ""
      : " The schedule it copied is revision <code>g" + String(context.generation) +
        "</code>" + (context.suspended === true ? " and is suspended" : "") + ".") +
    "</p>"
  );
}

/** The manual runs that already exist, so a reload shows the run rather than
 *  inviting a second click. */
export function renderManualRuns(ns, runs, spec) {
  const maxRetries = ((spec || {}).retry || {}).maxRetries;
  const rows = runs.map((run) => {
    const meta = run.metadata || {};
    const s = run.spec || {};
    return [
      detailLink("backups", String(ns || meta.namespace || ""), String(meta.name || "")),
      triggerBadge(s.trigger, maxRetries),
      revisionLine(s.scheduleRef),
      phaseBadge((run.status || {}).phase),
      cell(meta.creationTimestamp),
    ];
  });
  return table(
    ["RUN", "TRIGGER", "REVISION", "PHASE", "CREATED"],
    rows,
    "no manual run of this schedule exists in this namespace",
  );
}

// ------------------------------------------- what the schedule last decided

/** What happened to the most recent slot, from `status.lastSlot`, or the
 *  sentence that says this build does not publish it.
 *
 *  A DISPOSITION IS PRINTED, NEVER JUDGED. `Missed`, `Blocked`,
 *  `NameUnavailable` and `Exhausted` are things that happened and the reason
 *  beside each is the `Ready` reason the controller recorded with it; this
 *  page does not decide which of them is bad. */
export function renderLastSlot(object) {
  const status = (object || {}).status || {};
  const slot = status.lastSlot;
  const missed = status.missedSlots;
  if ((slot === undefined || slot === null) && (missed === undefined || missed === null)) {
    const absent = ((object || {}).__contract || {}).absent;
    return Array.isArray(absent) && absent.indexOf("status.lastSlot") !== -1
      ? "<p class=\"note\" data-last-slot=\"absent\">" + esc(LAST_SLOT_NOT_PUBLISHED) + "</p>"
      : "";
  }
  const rows = [];
  if (slot !== undefined && slot !== null) {
    rows.push(["last slot", "<code>" + cell(slot.slot) + "</code>"]);
    rows.push(["due at", cell(slot.dueAt)]);
    rows.push(["attempt", cell(slot.attempt)]);
    rows.push(["disposition", badge("pending", String(slot.disposition || ""))]);
    rows.push(["reason", cell(slot.reason)]);
    rows.push(["decided at", cell(slot.decidedAt)]);
    rows.push(["backup", cell((slot.backupRef || {}).name)]);
  }
  if (missed !== undefined && missed !== null) {
    rows.push([
      "missed slots",
      cell(missed.count) + (missed.countCapped === true
        ? " " + badge("unverified", "a floor, not a total") +
          " <span class=\"note\">an evaluation stopped at its 1000-slot cap, so this counts at " +
          "least this many.</span>"
        : ""),
    ]);
    rows.push(["last evaluated slot", cell(missed.lastEvaluatedSlot)]);
  }
  return "<div class=\"last-slot\" data-last-slot=\"1\">" + facts(rows) + "</div>";
}

/** Why the console cannot show what the last slot did. */
export const LAST_SLOT_NOT_PUBLISHED =
  "What happened to this schedule's most recent slot -- whether it was admitted, caught up, " +
  "missed, blocked or exhausted -- and how many slots have been skipped are recorded by the " +
  "controller in status.lastSlot and status.missedSlots. This build's product API projects " +
  "neither, so the console has nothing to render; read the schedule with kubectl, or use the " +
  "kubectl-proxy mode, which reads the object itself.";

/** The runs of this schedule that are in flight right now, each with the
 *  revision it FROZE -- which is the point of the panel.
 *
 *  A RUNNING RUN KEEPS SHOWING ITS OWN REVISION. Saving a new policy moves the
 *  schedule's generation; it does not touch a Backup that already exists, its
 *  frozen inputs or its Job. So this table reads the runs and not the
 *  schedule, and the two numbers being different on screen is the invariant
 *  PLAT-05.1 is about rather than a rendering mistake. */
export function renderActiveRuns(ns, object, backups) {
  const status = (object || {}).status || {};
  const spec = (object || {}).spec || {};
  const listed = status.activeRuns;
  if (listed === undefined || listed === null) {
    const absent = ((object || {}).__contract || {}).absent;
    return Array.isArray(absent) && absent.indexOf("status.activeRuns") !== -1
      ? "<p class=\"note\" data-active-runs=\"absent\">This build does not publish which runs " +
        "of this schedule are active; an absent list is not an empty one.</p>"
      : "";
  }
  const byName = Object.create(null);
  for (const backup of itemsOf(backups)) {
    byName[String(((backup || {}).metadata || {}).name || "")] = backup;
  }
  const rows = listed.map((run) => {
    const object2 = byName[String(run.name || "")];
    const s = (object2 || {}).spec || {};
    return [
      detailLink("backups", String(ns || ""), String(run.name || "")),
      triggerBadge(s.trigger === undefined ? { kind: run.kind, attempt: run.attempt } : s.trigger,
        (spec.retry || {}).maxRetries),
      revisionLine(s.scheduleRef),
    ];
  });
  return (
    "<div class=\"active-runs\" data-active-runs=\"" + String(listed.length) + "\">" +
    "<p class=\"note\">A run already created keeps the revision it froze. Editing the policy " +
    "above changes what the NEXT admission carries and touches none of these.</p>" +
    table(["RUN", "TRIGGER", "FROZEN REVISION"], rows,
      "no run of this schedule is active") + "</div>"
  );
}

// ---------------------------------------------------- the wiring, one card

/** Repaints ONE card in place, so a preview taken on one schedule does not
 *  discard a draft being typed into another. */
function repaintCard(node, ns, parse, lifecycle, api, object, backups, extra) {
  if (!active(lifecycle)) {
    return;
  }
  const name = ((object.metadata || {}).name) || "";
  const own = extra.cards[name] || {};
  const policySlot = node.querySelector("[data-policy-slot=\"" + name + "\"]");
  if (policySlot !== null) {
    replace(
      policySlot,
      parse(renderPolicyForm(policyFormView(ns, object, own, extra.destinations, extra.mayOperate))),
    );
  }
  const runSlot = node.querySelector("[data-run-now-slot=\"" + name + "\"]");
  if (runSlot !== null) {
    replace(runSlot, parse(renderRunNowPanel({
      ns: ns,
      name: name,
      object: object,
      state: mutationFor(formKey(ns, RUN_NOW_FORM, name)).state,
      mayOperate: extra.mayOperate,
      readiness: own.readiness || null,
      acknowledged: own.acknowledged === true,
      runs: manualRunsOf(name, backups),
      result: own.result || null,
    })));
  }
  wirePolicy(node, ns, parse, lifecycle, api, object, backups, extra);
  wireRunNow(node, ns, parse, lifecycle, api, object, backups, extra);
}

/** The policy form's values, read from the DOM. Every input is a string, and
 *  the fields a preset does not use simply are not there. */
export function readPolicyValues(form) {
  const values = Object.create(null);
  for (const field of POLICY_DRAFT_FIELDS) {
    const input = form.elements[field];
    values[field] = input === undefined || input === null ? "" : String(input.value);
  }
  return values;
}

/** One schedule's policy form: the cadence selector repaints the parameters,
 *  the preview button asks the API, and the submit replaces the policy. */
function wirePolicy(node, ns, parse, lifecycle, api, object, backups, extra) {
  const name = ((object.metadata || {}).name) || "";
  const form = node.querySelector("form.policy-form[data-name=\"" + name + "\"]");
  if (form === null) {
    return;
  }
  const key = formKey(ns, POLICY_FORM, name);
  const mutation = mutationFor(key);
  const own = extra.cards[name] || (extra.cards[name] = {});
  const remember = () => keepDraft(key, readPolicyValues(form), POLICY_DRAFT_FIELDS);

  watchMutation(node, key, mutation, (state) => {
    // A SUCCESSFUL SAVE RE-READS, EXACTLY AS THE SUSPEND TOGGLE DOES
    // (review F1). `repaintCard` renders from `object`, which is the copy this
    // mount captured BEFORE the edit, so repainting on success showed the
    // operator the policy they had just replaced, at a revision that no longer
    // existed, with nothing saying so -- and a second save from that screen
    // resent the pre-edit policy under a stale `expectedGeneration` and was
    // refused `412` for a reason the reader could not see. The answer is not a
    // cleverer repaint: it is to read the object the API server now holds. The
    // record is cleared first so the remount does not re-enter this arm.
    if (state.phase === "succeeded") {
      mutation.clear();
      mountSchedules(node, ns, parse, lifecycle, api);
      return;
    }
    repaintCard(node, ns, parse, lifecycle, api, object, backups, extra);
  }, lifecycle);

  // THE CADENCE SELECTOR AND THE SELECTION SELECTOR CHANGE WHICH INPUTS EXIST,
  // so a change on either keeps the draft and repaints the card rather than
  // leaving inputs on screen that the chosen shape has no field for.
  for (const field of ["mode", "selection"]) {
    const control = form.elements[field];
    if (control !== undefined && control !== null) {
      listen(control, "change", () => {
        remember();
        repaintCard(node, ns, parse, lifecycle, api, object, backups, extra);
      }, lifecycle);
    }
  }
  for (const input of form.querySelectorAll("input, select")) {
    listen(input, "input", remember, lifecycle);
  }

  const preview = node.querySelector("button[data-preview=\"" + name + "\"]");
  if (preview !== null) {
    listen(preview, "click", () => {
      if (!active(lifecycle)) {
        return;
      }
      const values = readPolicyValues(form);
      keepDraft(key, values, POLICY_DRAFT_FIELDS);
      const problems = validatePolicy(values);
      const query = previewQueryFor(values);
      if (query === null) {
        own.preview = {
          query: null,
          error: invalidInput(problems, "this cadence is not complete enough to preview"),
        };
        repaintCard(node, ns, parse, lifecycle, api, object, backups, extra);
        return;
      }
      preview.disabled = true;
      api.previewCadence(query, readOptions(lifecycle)).then(
        (answer) => {
          if (!active(lifecycle)) {
            return;
          }
          own.preview = { query: query, answer: answer, error: null };
          repaintCard(node, ns, parse, lifecycle, api, object, backups, extra);
        },
        (error) => {
          if (!cancelled(error, lifecycle) && active(lifecycle)) {
            own.preview = { query: query, answer: null, error: error };
            repaintCard(node, ns, parse, lifecycle, api, object, backups, extra);
          }
        },
      );
    }, lifecycle);
  }

  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    const values = readPolicyValues(form);
    keepDraft(key, values, POLICY_DRAFT_FIELDS);
    mutation.run(() => submitPolicy(ns, object, values, own.preview, api));
  }, lifecycle);
}

/** Checks a policy draft, decides which expression is being saved, and sends
 *  the replace under the revision the form was opened at.
 *
 *  A PRESET IS SAVED AS THE EXPRESSION THE SERVER COMPILED IT TO, and nothing
 *  else: if the preview on screen is not a preview of THESE values, this
 *  refuses rather than sending a cron line the page made up or an expression
 *  that answers a different question. */
export async function submitPolicy(ns, object, values, preview, api) {
  const problems = validatePolicy(values);
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  const generation = ((object || {}).metadata || {}).generation;
  if (typeof generation !== "number") {
    throw invalidInput({ cron: NO_GENERATION_SENTENCE });
  }
  let expression = String(values.cron || "").trim();
  if (values.mode !== ADVANCED_CRON) {
    if (!previewMatches(preview, values)) {
      throw invalidInput({ mode: PREVIEW_BEFORE_SAVE });
    }
    expression = String((preview.answer || {}).schedule || "");
  }
  const name = ((object || {}).metadata || {}).name || "";
  return api.editSchedulePolicy(ns, name, policyBody(values, generation, expression));
}

/** One schedule's manual-run panel: the acknowledgement checkbox, and the one
 *  click that creates a run under an idempotency intent. */
function wireRunNow(node, ns, parse, lifecycle, api, object, backups, extra) {
  const name = ((object.metadata || {}).name) || "";
  const form = node.querySelector("form.run-now-form[data-name=\"" + name + "\"]");
  if (form === null) {
    return;
  }
  const key = formKey(ns, RUN_NOW_FORM, name);
  const mutation = mutationFor(key);
  const own = extra.cards[name] || (extra.cards[name] = {});

  watchMutation(node, key, mutation, (state) => {
    if (state.phase === "succeeded") {
      own.result = state.result;
    }
    repaintCard(node, ns, parse, lifecycle, api, object, backups, extra);
  }, lifecycle);

  const acknowledge = node.querySelector("input[data-acknowledge=\"" + name + "\"]");
  if (acknowledge !== null) {
    listen(acknowledge, "change", () => {
      own.acknowledged = acknowledge.checked === true;
      repaintCard(node, ns, parse, lifecycle, api, object, backups, extra);
    }, lifecycle);
  }

  // BACK UP AGAIN: drop this draft, which ends its intent, and clear the
  // record so the panel goes back to an idle control. The run that exists is
  // still listed and still on screen; what changes is that the NEXT click is a
  // different request rather than a repeat of the one that made it.
  const again = node.querySelector("button[data-run-again=\"" + name + "\"]");
  if (again !== null) {
    listen(again, "click", () => {
      if (!active(lifecycle) || mutation.pending()) {
        return;
      }
      newIntent(key);
      own.result = null;
      own.acknowledged = false;
      mutation.clear();
      repaintCard(node, ns, parse, lifecycle, api, object, backups, extra);
    }, lifecycle);
  }

  listen(form, "submit", (event) => {
    event.preventDefault();
    if (!active(lifecycle) || mutation.pending()) {
      return;
    }
    mutation.run(() => submitRunNow(ns, object, own, api, intentFor(key)));
  }, lifecycle);
}

/** Creates one manual run of `object` under the idempotency intent `key`.
 *
 *  THE BODY IS BODY A AND NOTHING ELSE: the schedule's name, and the revision
 *  this card was rendered from. The API copies the source, the selection, the
 *  archive and the deadline off the schedule at that generation, which is what
 *  makes "the copied schedule revision" a fact about the run rather than a
 *  form's guess -- a policy field in the body is a 422 by design.
 *
 *  `expectedGeneration` IS SENT WHEN THERE IS ONE. It turns a stale card into
 *  a `409 policy_changed` naming the revision that is in force, instead of a
 *  run quietly taken under a policy the person never saw. */
export async function submitRunNow(ns, object, own, api, key) {
  const name = ((object || {}).metadata || {}).name || "";
  const generation = ((object || {}).metadata || {}).generation;
  const scheduleRef = { name: name };
  if (typeof generation === "number") {
    scheduleRef.expectedGeneration = generation;
  }
  const body = { scheduleRef: scheduleRef };
  const readiness = (own || {}).readiness || null;
  if (readiness !== null && (readiness.state === "notReady" || readiness.state === "unknown")) {
    body.readinessAcknowledgement = { preflight: String(readiness.id), state: readiness.state };
  }
  return api.runBackupNow(ns, body, key);
}
