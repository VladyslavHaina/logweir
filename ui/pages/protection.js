// pages/protection.js -- PROTECTION HEALTH (PLAT-14.2).
//
// THE QUESTION THIS PAGE ANSWERS IS NOT "DID THE BACKUP RUN". It is "is there
// a recovery point I could actually restore from, and how old is it". Those
// are different questions and a console that answered the first while somebody
// asked the second is the failure this whole surface exists to prevent: an
// enabled, green, never-failing schedule can have no recoverable backup at all
// -- because its evidence never verified, because the archive lost the object,
// or because every run covered topics the objective is not about.
//
// SO THE TWO HEALTHS SIT SIDE BY SIDE AND ARE NEVER COLLAPSED. Schedule health
// is `BackupSchedule.status.conditions[Ready]`, and it is rendered in its own
// column beside protection health, which is `ProtectionPolicy.status.health`.
// D3 section 3.2 is explicit about this, and `TWO_HEALTHS_SENTENCE` says it on the
// page rather than only in a decision document.
//
// THREE MORE RULES THIS PAGE CARRIES.
//
//   1. `Protected` HAS THREE STATUSES, NOT TWO. `True` only for `Healthy`,
//      `Unknown` when the evaluation could not happen, `False` otherwise. A
//      `False` rendered for an evaluation that never ran reads as "Logweir
//      checked and you are not protected", which is a claim nothing made. The
//      condition's own `status` is printed, and `Unknown` stays `Unknown`.
//   2. TWO INSTANTS, TWO LABELS. `recoveryPointAt` is when the capture
//      STARTED -- what the objective is measured against -- and
//      `newestRecordAt` is the newest archived record. An idle topic makes the
//      second look old for a reason that is not a gap in protection, so they
//      are never printed as one number (D3 section 3.2).
//   3. A NOTIFICATION IS NOT EVIDENCE, AND A DELIVERY FAILURE IS NOT A RUN
//      FAILURE. The alert ledger shows each alert's transition and what
//      happened to its delivery; a `Failed` delivery changes nothing about a
//      backup's own recorded result, and the page says so beside the ledger
//      (D3 section 3.4, which makes the same claim structurally in the controller).
//
// IT SUBMITS NOTHING. There is no test-notification button here: D3 section 10 gives
// that route an operator role and a rate limit, and a control that fired a
// page at an on-call rota is not one this task adds without the role check
// that belongs with it.

import {
  ABSENT,
  NOTIFICATION_NOT_EVIDENCE_SENTENCE,
  TWO_HEALTHS_SENTENCE,
  TWO_INSTANTS_SENTENCE,
  badge,
  cell,
  detailLink,
  errorBox,
  esc,
  facts,
  healthBadge,
  listFooter,
  replace,
  table,
} from "../render.js";
import { active, cancelled, readOptions } from "../lifecycle.js";
import { listD3, readD3 } from "../operation-watch.js";
import { operationRoute } from "./operation.js";

/** The sentence the list carries when the namespace holds no policy. */
export const NO_POLICY_SENTENCE =
  "No ProtectionPolicy exists in this namespace, so nothing here is evaluating whether a " +
  "recoverable backup exists or how old it is. A schedule that runs is not the same as an " +
  "objective that is met; create a ProtectionPolicy with kubectl to state one.";

/** The sentence a policy with no status carries. An absent evaluation is an
 *  ABSENT one and never a healthy one. */
export const NOT_EVALUATED_SENTENCE =
  "This policy carries no evaluation yet. That is an absent observation, not a verdict: " +
  "nothing here says this namespace is protected and nothing here says it is not.";

/** The items of a collection, whatever envelope it arrived in. */
function itemsOf(collection) {
  if (Array.isArray(collection)) {
    return collection;
  }
  const items = (collection || {}).items;
  return Array.isArray(items) ? items : [];
}

function statusOf(object) {
  return (object && object.status) || {};
}

function specOf(object) {
  return (object && object.spec) || {};
}

/** One condition out of a list, by type, or `null`. */
export function conditionOf(conditions, type) {
  for (const condition of Array.isArray(conditions) ? conditions : []) {
    if ((condition || {}).type === type) {
      return condition;
    }
  }
  return null;
}

/** The `Protected` condition as a badge, with its three statuses kept apart.
 *
 *  `Unknown` IS NOT ROUNDED DOWN. That is the whole of this function. */
export function protectedBadge(conditions) {
  const condition = conditionOf(conditions, "Protected");
  if (condition === null) {
    return ABSENT;
  }
  const status = String(condition.status || "");
  const kind = status === "True" ? "green" : (status === "Unknown" ? "flat" : "unverified");
  return badge(kind, "Protected=" + status + " " + String(condition.reason || ""));
}

/** The objective, as the number the spec states. */
export function objectiveLine(object) {
  const objectives = specOf(object).objectives || {};
  const age = objectives.maxRecoveryPointAgeSeconds;
  return typeof age === "number"
    ? "a recovery point no older than " + String(age) + " seconds"
    : ABSENT;
}

/** The list: one row per policy, with both healths in their own columns. */
export function renderProtectionList(collection, ns) {
  const rows = itemsOf(collection).map((object) => {
    const meta = object.metadata || {};
    const status = statusOf(object);
    const point = status.lastAvailablePoint || {};
    const schedules = Array.isArray(status.schedules) ? status.schedules : [];
    return [
      typeof meta.name === "string" && meta.name.length > 0
        ? detailLink("protection", ns || meta.namespace || "", meta.name)
        : cell(null),
      healthBadge(status.health),
      esc(objectiveLine(object)),
      cell(point.ageSeconds),
      cell(status.consecutiveFailedRuns),
      scheduleHealthCell(schedules),
      cell(openAlerts(status).length),
    ];
  });
  return (
    "<h2>Protection</h2>" +
    "<p class=\"blurb\">Every ProtectionPolicy in this namespace, with the objective it " +
    "states and the newest recovery point that actually satisfies it.</p>" +
    "<p class=\"note\">" + esc(TWO_HEALTHS_SENTENCE) + "</p>" +
    table(
      ["NAME", "PROTECTION", "OBJECTIVE", "POINT AGE (s)", "FAILED RUNS", "SCHEDULES", "OPEN ALERTS"],
      rows,
      NO_POLICY_SENTENCE,
    ) +
    listFooter()
  );
}

/** The schedules column: the schedules' OWN health, never the policy's. */
function scheduleHealthCell(schedules) {
  if (schedules.length === 0) {
    return ABSENT;
  }
  const words = schedules.map((s) => {
    const entry = s || {};
    const ready = String(entry.ready || "");
    const suspended = entry.suspended === true ? " suspended" : "";
    return String(entry.name || "") + " Ready=" + (ready.length === 0 ? "?" : ready) + suspended;
  });
  return esc(words.join("; "));
}

/** Every alert still open. */
export function openAlerts(status) {
  return (Array.isArray(status.alerts) ? status.alerts : []).filter(
    (alert) => (alert || {}).state === "Open",
  );
}

/** THE LAST AVAILABLE POINT, WITH ITS TWO INSTANTS LABELLED SEPARATELY. */
export function renderLastPoint(object, ns) {
  const status = statusOf(object);
  const point = status.lastAvailablePoint || null;
  if (point === null) {
    return (
      "<section class=\"last-point\"><h3>Newest available recovery point</h3>" +
      "<p class=\"complaint\">There is no available recovery point for this policy. " +
      "`availabilityBasis` is " + cell(status.availabilityBasis) + ": that is HOW availability " +
      "was established, and a basis of CatalogStale means the catalog could not answer -- " +
      "which is not the same as an archive with nothing in it.</p></section>"
    );
  }
  const topics = Array.isArray(point.topics) ? point.topics : [];
  return (
    "<section class=\"last-point\"><h3>Newest available recovery point</h3>" +
    facts([
      ["point id", "<code>" + cell(point.pointId) + "</code>"],
      ["backup", typeof (point.backupRef || {}).name === "string"
        ? detailLink("backups", String(ns || ""), String(point.backupRef.name))
        : ABSENT],
      ["recovery point (capture started)", cell(point.recoveryPointAt)],
      ["newest archived record", cell(point.newestRecordAt)],
      ["age at evaluation (s)", cell(point.ageSeconds)],
      ["evidence", cell(point.evidence)],
      ["availability basis", cell(status.availabilityBasis)],
      ["topics", topics.length === 0
        ? ABSENT
        : esc(topics.join(", ")) + (point.topicsTruncated === true ? " (truncated)" : "")],
    ]) +
    "<p class=\"note\">" + esc(TWO_INSTANTS_SENTENCE) + "</p>" +
    "</section>"
  );
}

/** Schedule health beside protection health, as a table of the schedules'
 *  own recorded facts. */
export function renderScheduleHealth(object, ns) {
  const schedules = Array.isArray(statusOf(object).schedules) ? statusOf(object).schedules : [];
  const rows = schedules.map((s) => {
    const entry = s || {};
    return [
      typeof entry.name === "string" && entry.name.length > 0
        ? detailLink("schedules", String(ns || ""), entry.name)
        : cell(null),
      cell(entry.ready),
      entry.suspended === true ? badge("pending", "suspended") : "no",
      cell(entry.nextFireTime),
      cell(entry.lastMissedSlot),
    ];
  });
  return (
    "<section class=\"schedule-health\"><h3>Schedule health</h3>" +
    "<p class=\"note\">" + esc(TWO_HEALTHS_SENTENCE) + "</p>" +
    table(
      ["SCHEDULE", "READY", "SUSPENDED", "NEXT FIRE", "LAST MISSED SLOT"],
      rows,
      "This policy names no schedule, so no slot is expected and no missed slot can be counted.",
    ) +
    "</section>"
  );
}

/** THE ALERT LEDGER AND ITS DELIVERY STATE.
 *
 *  `Suppressed` is a CONFIGURATION CHOICE and not a failure: it means no route
 *  carries this kind. It is rendered as itself. */
export function renderAlerts(object) {
  const alerts = Array.isArray(statusOf(object).alerts) ? statusOf(object).alerts : [];
  const rows = alerts.map((a) => {
    const alert = a || {};
    const delivery = alert.delivery || {};
    return [
      cell(alert.kind),
      badge(alert.state === "Open" ? "unverified" : "green", String(alert.state || "")),
      cell(alert.openedAt),
      cell(alert.resolvedAt),
      cell(alert.transition) + " / " + cell(alert.notifiedTransition),
      cell(delivery.state) + " (" + cell(delivery.attempts) + ")",
      cell(delivery.lastError),
    ];
  });
  return (
    "<section class=\"alerts\"><h3>Alerts</h3>" +
    table(
      ["KIND", "STATE", "OPENED", "RESOLVED", "TRANSITION / NOTIFIED", "DELIVERY", "LAST ERROR"],
      rows,
      "This policy has opened no alert. An empty ledger is an empty ledger: it says nothing " +
        "about whether a sink would accept one.",
    ) +
    "<p class=\"note\">" + esc(NOTIFICATION_NOT_EVIDENCE_SENTENCE) + "</p>" +
    "</section>"
  );
}

/** The rehearsal block, when the policy states a rehearsal objective. */
export function renderRehearsal(object, ns) {
  const rehearsal = statusOf(object).rehearsal || null;
  if (rehearsal === null) {
    return "";
  }
  const last = rehearsal.lastRestoreRef || {};
  return (
    "<section class=\"rehearsal\"><h3>Rehearsal</h3>" +
    facts([
      ["last succeeded", cell(rehearsal.lastSucceededAt)],
      ["last restore", typeof last.name === "string" && last.name.length > 0
        ? "<a href=\"" + esc(operationRoute(String(ns || ""), "restore", last.name, "")) + "\">" +
          esc(last.name) + "</a>"
        : ABSENT],
      ["last failed", cell(rehearsal.lastFailedAt)],
      ["last reason", cell(rehearsal.lastReason)],
    ]) +
    "</section>"
  );
}

/** One policy, in full. */
export function renderProtectionDetail(object, ns) {
  const meta = (object && object.metadata) || {};
  const status = statusOf(object);
  const spec = specOf(object);
  const protects = spec.protects || {};
  const objectives = spec.objectives || {};
  const missed = status.missed || {};
  const attempt = status.lastAttempt || {};
  const evaluated = typeof status.evaluatedAt === "string" && status.evaluatedAt.length > 0;
  return (
    "<h2>Protection " + cell(meta.name) + "</h2>" +
    healthBadge(status.health) + " " + protectedBadge(status.conditions) +
    (evaluated ? "" : "<p class=\"note\">" + esc(NOT_EVALUATED_SENTENCE) + "</p>") +
    facts([
      ["health", cell(status.health)],
      ["evaluated at", cell(status.evaluatedAt)],
      ["stale since", cell(status.staleSince)],
      ["source", cell((protects.sourceRef || {}).name)],
      ["destination", cell((protects.destinationRef || {}).name)],
      ["catalog", cell((protects.catalogRef || {}).name)],
      ["topics", Array.isArray(protects.topics) && protects.topics.length > 0
        ? esc(protects.topics.join(", "))
        : ABSENT],
      ["objective: max recovery point age (s)", cell(objectives.maxRecoveryPointAgeSeconds)],
      ["objective: max consecutive failed runs", cell(objectives.maxConsecutiveFailedRuns)],
      ["objective: verified evidence required", cell(objectives.requireVerifiedEvidence)],
      ["objective: catalog availability required", cell(objectives.requireCatalogAvailability)],
      ["consecutive failed runs", cell(status.consecutiveFailedRuns)],
      ["last missed slot", cell(missed.lastMissedSlot)],
      ["runs since last fire", cell(missed.sinceLastFire)],
      ["last attempt", cell((attempt.backupRef || {}).name) + " " + cell(attempt.phase) + " " +
        cell(attempt.reason) + " " + cell(attempt.at)],
    ]) +
    renderLastPoint(object, ns) +
    renderScheduleHealth(object, ns) +
    renderRehearsal(object, ns) +
    renderAlerts(object) +
    renderConditions(status.conditions)
  );
}

/** Every condition the controller wrote, verbatim. */
export function renderConditions(conditions) {
  const rows = (Array.isArray(conditions) ? conditions : []).map((c) => {
    const condition = c || {};
    return [
      cell(condition.type),
      cell(condition.status),
      cell(condition.reason),
      cell(condition.message),
      cell(condition.lastTransitionTime),
    ];
  });
  return (
    "<section class=\"conditions\"><h3>Conditions</h3>" +
    table(
      ["TYPE", "STATUS", "REASON", "MESSAGE", "LAST TRANSITION"],
      rows,
      "This object carries no condition yet.",
    ) +
    "</section>"
  );
}

// --------------------------------------------------------------- mount half

export async function mountProtection(node, ns, parse, lifecycle, deps) {
  try {
    const collection = await listD3("protection", ns, readOptions(lifecycle), deps);
    if (active(lifecycle)) {
      replace(node, parse(renderProtectionList(collection, ns)));
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

export async function mountProtectionDetail(node, ns, name, parse, lifecycle, deps) {
  try {
    const object = await readD3("protection", ns, name, readOptions(lifecycle), deps);
    if (active(lifecycle)) {
      replace(node, parse(renderProtectionDetail(object, ns)));
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}
