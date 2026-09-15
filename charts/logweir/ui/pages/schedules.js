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

import { list, create, patchSuspend } from "../api.js";
import {
  RETENTION_SENTENCE,
  badge,
  cell,
  copyBlock,
  errorBox,
  esc,
  facts,
  listFooter,
  replace,
  table,
} from "../render.js";
import { itemsOf } from "./clusters.js";

const PLURAL = "backupschedules";

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

/** The suspend toggle's control for one schedule. */
export function renderSuspendToggle(object) {
  const spec = (object && object.spec) || {};
  const name = ((object && object.metadata) || {}).name || "";
  const suspended = spec.suspend === true;
  return (
    "<form class=\"suspend\" data-name=\"" + esc(name) + "\" data-next=\"" +
    (suspended ? "false" : "true") + "\">" +
    "<button type=\"submit\">" +
    (suspended ? "Resume" : "Suspend") +
    "</button></form>"
  );
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

/** The create form for a BackupSchedule. */
export function renderScheduleForm() {
  return (
    "<section class=\"create\"><h3>Create a BackupSchedule</h3>" +
    "<p class=\"note\">A schedule fires a Backup of the named topics at each slot and writes " +
    "it to the archive. Every field but suspend is sealed once the object exists.</p>" +
    "<form id=\"schedule-form\">" +
    "<div class=\"field\"><label for=\"schedule-name\">name</label>" +
    "<input id=\"schedule-name\" name=\"name\" required></div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"schedule-cron\">schedule, five cron fields</label>" +
    "<input id=\"schedule-cron\" name=\"cron\" value=\"0 * * * *\" required>" +
    "<p class=\"help\">minute hour day-of-month month day-of-week, in UTC.</p></div>" +
    "<div class=\"field\"><label for=\"schedule-source\">source KafkaCluster</label>" +
    "<input id=\"schedule-source\" name=\"source\" required>" +
    "<p class=\"help\">The name of a KafkaCluster in this namespace.</p></div>" +
    "</div>" +
    "<div class=\"field\"><label for=\"schedule-topics\">topics, comma separated -- names, never patterns</label>" +
    "<input id=\"schedule-topics\" name=\"topics\" required>" +
    "<p class=\"help\">An explicit allowlist. A wildcard is refused before anything runs.</p></div>" +
    "<div class=\"field\"><label for=\"schedule-archive\">archive URL</label>" +
    "<input id=\"schedule-archive\" name=\"archive\" required>" +
    "<p class=\"help\">The bucket and prefix the runner writes the backup set under.</p></div>" +
    "<div class=\"field\"><label for=\"schedule-archive-secret\">archive credential (Secret name)</label>" +
    "<input id=\"schedule-archive-secret\" name=\"archiveSecret\" value=\"logweir-s3\">" +
    "<p class=\"help\">An existing Secret in this namespace, with access-key-id and secret-access-key. " +
    "Only its name is sent. Leave blank only for anonymous or instance-role access.</p></div>" +
    "<div class=\"field-row\">" +
    "<div class=\"field\"><label for=\"schedule-keeplast\">retention keepLast</label>" +
    "<input id=\"schedule-keeplast\" name=\"keepLast\" type=\"number\" min=\"0\">" +
    "<p class=\"help\">How many sets a retention evaluation keeps. It reports; it never deletes.</p></div>" +
    "<div class=\"field\"><label for=\"schedule-keepdays\">retention keepDays</label>" +
    "<input id=\"schedule-keepdays\" name=\"keepDays\" type=\"number\" min=\"0\">" +
    "<p class=\"help\">How many days of sets it keeps. Leave both blank for no evaluation.</p></div>" +
    "</div>" +
    "<div class=\"actions\"><button type=\"submit\" class=\"primary\">Create</button></div>" +
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

// --------------------------------------------------------------- mount half

export async function mountSchedules(node, ns, parse) {
  try {
    const collection = await list(ns, PLURAL);
    const objects = itemsOf(collection);
    const panels = objects
      .map(
        (object) =>
          "<section class=\"schedule\"><div class=\"card-head\"><h3>" + nameOf(object) + "</h3>" +
          renderSuspendToggle(object) +
          "</div>" +
          renderRetentionPanel(object) +
          "</section>",
      )
      .join("");
    replace(
      node,
      parse(renderScheduleList(collection) + panels + renderScheduleForm()),
    );
    wire(node, ns, parse);
  } catch (error) {
    replace(node, errorBox(error));
  }
}

function wire(node, ns, parse) {
  for (const form of node.querySelectorAll("form.suspend")) {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        await patchSuspend(
          ns,
          form.getAttribute("data-name"),
          form.getAttribute("data-next") === "true",
        );
        await mountSchedules(node, ns, parse);
      } catch (error) {
        replace(node, errorBox(error));
      }
    });
  }
  const form = node.querySelector("#schedule-form");
  if (form === null) {
    return;
  }
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const values = {
      name: form.elements.name.value.trim(),
      cron: form.elements.cron.value.trim(),
      source: form.elements.source.value.trim(),
      topics: form.elements.topics.value,
      archive: form.elements.archive.value.trim(),
      archiveSecret: form.elements.archiveSecret.value.trim(),
      keepLast: form.elements.keepLast.value,
      keepDays: form.elements.keepDays.value,
    };
    try {
      await create(ns, PLURAL, scheduleBody(values));
      await mountSchedules(node, ns, parse);
    } catch (error) {
      replace(node, errorBox(error));
    }
  });
}
