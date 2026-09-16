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

import { list, create, get, patchSuspend } from "../api.js";
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
import { focusFirstProblem, isObjectName, itemsOf } from "./clusters.js";
import { isRecoveryPoint, recoveryPoints, restorePointRoute } from "./restore-wizard.js";

const PLURAL = "backupschedules";
const BACKUPS = "backups";

const API = { list: list, get: get, create: create, patchSuspend: patchSuspend };

/** The create form's identity in the draft and mutation registries. */
export const SCHEDULE_FORM = "schedule-form";

/** The suspend toggle's identity; each schedule has its own record. */
export const SUSPEND_FORM = "schedule-suspend";

/** The fields a draft of the create form keeps: names, a cron line, a topic
 *  list, an archive URL, a Secret NAME and two numbers. No credential. */
export const SCHEDULE_DRAFT_FIELDS = Object.freeze([
  "name", "cron", "source", "topics", "archive", "archiveSecret", "keepLast", "keepDays",
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

const SCHEDULE_DEFAULTS = Object.freeze({
  name: "", cron: "0 * * * *", source: "", topics: "", archive: "", archiveSecret: "logweir-s3",
  keepLast: "", keepDays: "",
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
  if (!isObjectName(v.source)) {
    problems.source = "the name of a KafkaCluster in this namespace";
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
    "<div class=\"field\"><label for=\"schedule-source\">source KafkaCluster</label>" +
    "<input id=\"schedule-source\" name=\"source\" required value=\"" + esc(d.source) + "\"" +
    field("schedule-source", "source") + ">" +
    "<p class=\"help\">The name of a KafkaCluster in this namespace.</p>" +
    line("schedule-source", "source") + "</div>" +
    "</div>" +
    "<div class=\"field\"><label for=\"schedule-topics\">topics, comma separated -- names, never patterns</label>" +
    "<input id=\"schedule-topics\" name=\"topics\" required value=\"" + esc(d.topics) + "\"" +
    field("schedule-topics", "topics") + ">" +
    "<p class=\"help\">An explicit allowlist. A wildcard is refused before anything runs.</p>" +
    line("schedule-topics", "topics") + "</div>" +
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
    line("schedule-archive-secret", "archiveSecret") + "</div>" +
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

/** Checks the values, then creates the schedule idempotently by name. */
export async function submitSchedule(ns, values, deps) {
  const problems = validateSchedule(values);
  if (Object.keys(problems).length > 0) {
    throw invalidInput(problems);
  }
  return createOnce(deps || API, ns, PLURAL, scheduleBody(values), SCHEDULE_SPEC_RULES);
}

/** What the create form renders from in namespace `ns`. */
export function scheduleFormView(ns) {
  const key = formKey(ns, SCHEDULE_FORM);
  const state = mutationFor(key).state;
  if (state.phase === "succeeded") {
    dropDraft(key);
  }
  return {
    draft: readDraft(key),
    state: state,
    errors: state.phase === "failed" ? fieldErrors(state.error, SCHEDULE_FIELD_PATHS) : null,
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
    renderRecoveryPoints(ns, object, backups) +
    renderRetentionPanel(object) +
    "</section>"
  );
}

// --------------------------------------------------------------- mount half

export async function mountSchedules(node, ns, parse, lifecycle, deps) {
  const api = deps || API;
  try {
    // The schedules AND the runs they produced: a schedule card's recovery
    // points are `Backup` objects naming it, and there is no field on the
    // schedule that carries them.
    const collections = await Promise.all([
      api.list(ns, PLURAL, readOptions(lifecycle)),
      api.list(ns, BACKUPS, readOptions(lifecycle)),
    ]);
    if (!active(lifecycle)) {
      return;
    }
    const collection = collections[0];
    const backups = collections[1];
    const objects = itemsOf(collection);
    const panels = objects.map((object) => renderScheduleCard(ns, object, backups)).join("");
    replace(
      node,
      parse(
        renderScheduleList(collection) + panels +
          "<div class=\"form-slot\" id=\"schedule-form-slot\">" +
          renderScheduleForm(scheduleFormView(ns)) + "</div>",
      ),
    );
    wire(node, ns, parse, lifecycle, api, objects);
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** The create form's values, read from the DOM. */
export function readScheduleValues(form) {
  const e = form.elements;
  return {
    name: String(e.name.value).trim(),
    cron: String(e.cron.value).trim(),
    source: String(e.source.value).trim(),
    topics: String(e.topics.value),
    archive: String(e.archive.value).trim(),
    archiveSecret: String(e.archiveSecret.value).trim(),
    keepLast: String(e.keepLast.value),
    keepDays: String(e.keepDays.value),
  };
}

function wire(node, ns, parse, lifecycle, api, objects) {
  for (const toggle of node.querySelectorAll("form.suspend")) {
    wireToggle(node, ns, parse, lifecycle, api, objects, toggle);
  }
  wireCreate(node, ns, parse, lifecycle, api);
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

function wireCreate(node, ns, parse, lifecycle, api) {
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
    replace(slot, parse(renderScheduleForm(scheduleFormView(ns))));
    wireCreate(node, ns, parse, lifecycle, api);
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
    mutation.run(() => submitSchedule(ns, values, api));
  }, lifecycle);
}
