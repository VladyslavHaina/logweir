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
  watchMutation,
} from "../lifecycle.js";
import {
  RETENTION_SENTENCE,
  badge,
  cell,
  copyBlock,
  errorBox,
  esc,
  facts,
  fieldErrorLine,
  invalidAttributes,
  listFooter,
  mutationStatus,
  replace,
  rfc3339,
  table,
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
export function renderScheduleList(input) {
  const rows = itemsOf(input).map((object) => {
    const spec = object.spec || {};
    const status = object.status || {};
    return [
      nameOf(object),
      "<code>" + cell(spec.schedule) + "</code>",
      suspendBadge(spec),
      cell(status.lastFireTime),
      cell(status.nextFireTime),
      conditionStatus(object, "Ready"),
    ];
  });
  return (
    "<h2>Schedules</h2>" +
    "<p class=\"blurb\">Every BackupSchedule in this namespace. " +
    "<code>suspend</code> is the only field of a schedule's spec that can be changed " +
    "after it is created.</p>" +
    table(["NAME", "SCHEDULE", "SUSPEND", "LAST", "NEXT", "READY"], rows, NO_SCHEDULE_SENTENCE) +
    listFooter()
  );
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
export function renderRetentionPanel(object) {
  const status = (object && object.status) || {};
  const report = status.retentionReport;
  if (!report) {
    return (
      "<section class=\"retention\"><h3>Retention</h3>" +
      "<p class=\"note\">No retention evaluation has been recorded for this schedule.</p>" +
      "<p class=\"never-deletes\">" + RETENTION_SENTENCE + "</p></section>"
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
    "<p class=\"never-deletes\">" + RETENTION_SENTENCE + "</p>" +
    "</section>"
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

/** The sentence a schedule with an empty topic list carries when this build
 *  cannot tell which of the two shapes it is.
 *
 *  THE PRODUCT API PUBLISHES NEITHER `allUserTopics` NOR `status.selection` ON
 *  ITS `Schedule` DTO. D1 added dynamic selection to the CRD and the
 *  controller; the console's own projection has not caught up, so in console
 *  mode an empty `topics` is all this page is given. Guessing "all user topics"
 *  from an empty list would be inventing the very claim the coverage labels
 *  exist to bound, so the page says what it does not know. */
export const SELECTION_UNKNOWN_SENTENCE =
  "This schedule names no topic. That is the shape a dynamic (all user topics) selection has, " +
  "and it is also the shape of an empty allowlist, which no run will back anything up under. " +
  "This build's product API publishes neither the selection block nor the recorded coverage, so " +
  "this page cannot tell you which it is -- read the object with kubectl.";

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

/** The coverage line: the controller's own label, or the sentence that says
 *  this build does not publish one. */
export function renderCoverageLine(object) {
  const o = object || {};
  const spec = o.spec || {};
  const topics = Array.isArray(spec.topics) ? spec.topics : [];
  const coverage = coverageOf(o);
  if (coverage.length > 0) {
    return (
      "<p class=\"coverage\" data-coverage=\"" + esc(coverage) + "\">" +
      (claimsWholeCluster(coverage) ? badge("green", "coverage") : badge("pending", "coverage")) +
      " " + esc(COVERAGE_LABELS[coverage]) + "</p>"
    );
  }
  if (spec.allUserTopics !== undefined && spec.allUserTopics !== null) {
    return (
      "<p class=\"coverage\" data-coverage=\"dynamic\">" + badge("pending", "dynamic selection") +
      " " + esc(DYNAMIC_SELECTION_SENTENCE) +
      " On incomplete visibility this policy says: <code>" +
      cell(spec.allUserTopics.incompleteDiscovery) + "</code>.</p>"
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
    "<p class=\"help\" id=\"schedule-destination-gap\">A saved destination cannot be named " +
    "here yet: this build's <code>POST /schedules</code> takes an inline archive and has no " +
    "destinationRef field (PLAT-06.2 adds one). The inline URL and Secret below are therefore " +
    "the only way to write a schedule from this page, and they carry no endpoint, region, " +
    "addressing mode or CA bundle -- which is what a destination exists to hold. Deriving one " +
    "from the other would drop all four silently, so this page will not. Use the readiness " +
    "panel to check a destination, and kubectl or the CLI to bind a schedule to one.</p>" +
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
      ["BACKUP", "SLOT", "BACKUP SET", "COVERED FROM", "COVERED TO", "RECORDS", ""],
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

/** One schedule's card: its name, its toggle, the toggle's status, its
 *  recovery points and the retention panel. */
export function renderScheduleCard(ns, object, backups) {
  const name = ((object && object.metadata) || {}).name || "";
  const state = mutationFor(formKey(ns, SUSPEND_FORM, name)).state;
  return (
    "<section class=\"schedule\" data-schedule=\"" + esc(name) + "\"><div class=\"card-head\"><h3>" +
    nameOf(object) + "</h3>" +
    renderSuspendToggle(object, state) +
    "</div>" +
    "<div class=\"form-status\" data-suspend-status=\"" + esc(name) + "\" tabindex=\"-1\">" +
    renderSuspendStatus(object, state) + "</div>" +
    renderCoverageLine(object) +
    renderRecoveryPoints(ns, object, backups) +
    renderRetentionPanel(object) +
    "</section>"
  );
}

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
    const panels = objects.map((object) => renderScheduleCard(ns, object, backups)).join("");
    // THE DESTINATIONS ARE A FOURTH READ AND ITS FAILURE IS NOT THE PAGE'S.
    // In legacy mode it is refused by name; the readiness panel then says so
    // and the rest of this view -- the schedules, their runs, the create form
    // -- renders exactly as it did before.
    const readiness = await readReadiness(api, ns, lifecycle, clusters);
    if (!active(lifecycle)) {
      return;
    }
    replace(
      node,
      parse(
        renderScheduleList(collection) + panels +
          "<div class=\"form-slot\" id=\"schedule-form-slot\">" +
          renderScheduleForm(scheduleFormView(ns, clusters)) + "</div>" +
          "<div class=\"readiness-slot\" id=\"readiness-slot\">" +
          renderReadinessPanel(readinessView(ns, readiness)) + "</div>",
      ),
    );
    wire(node, ns, parse, lifecycle, api, objects, clusters);
    wireReadiness(node, ns, parse, lifecycle, api, readiness);
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
