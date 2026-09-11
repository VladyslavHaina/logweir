// pages/history.js -- Restores and Backups interleaved by creation time, and
// the Restore detail view.
//
// THE RESTORE HALF OF THE BADGE RULE lives here: green requires
// `evidence.verification.result == "Valid"` AND `status.outcome === "pass"`.
// Only `pass` is green. A `fail-integrity` run produced a perfectly valid,
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

import { get, list } from "../api.js";
import {
  ENGINE_SUBREPORT_LINE,
  UNVERIFIED,
  badge,
  bucketOf,
  cell,
  errorBox,
  esc,
  evidenceBlock,
  facts,
  independentCheck,
  listFooter,
  replace,
  table,
} from "../render.js";
import { itemsOf } from "./clusters.js";
import { backupBadge, greenLabel, validVerification } from "./backups.js";

const PLURAL = "restores";
const BACKUPS = "backups";

/** **The Restore badge rule.** Green if and only if the recorded verification
 *  is `Valid` AND the run's own outcome is `pass`. */
export function restoreBadge(status) {
  const verified = validVerification(status);
  if (verified === null) {
    return badge("unverified", UNVERIFIED);
  }
  if ((status || {}).outcome !== "pass") {
    return badge("unverified", UNVERIFIED);
  }
  return badge("green", greenLabel(verified[0], verified[1]));
}

function nameOf(object) {
  const meta = (object && object.metadata) || {};
  return cell(meta.name);
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
  return kindOf(object) === "Backup" ? cell(status.exitCode) : cell(status.outcome);
}

/** The badge for a row, per kind. */
function rowBadge(object) {
  const status = (object && object.status) || {};
  return kindOf(object) === "Backup" ? backupBadge(status) : restoreBadge(status);
}

/** Restores and Backups interleaved, newest first.
 *
 *  Accepts one collection, an array, or two collections
 *  (`renderHistoryList(restores, backups)`). Objects with no creation instant
 *  sort last, in the order they arrived: a missing timestamp is not a reason
 *  to invent one. */
export function renderHistoryList(input, second) {
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

  const rows = ordered.map((object) => [
    nameOf(object),
    esc(kindOf(object)),
    cell(createdAt(object)),
    cell((object.status || {}).phase),
    resultCell(object),
    rowBadge(object),
  ]);

  return (
    "<h2>History</h2>" +
    "<p class=\"blurb\">Completed runs of both kinds, newest first. A Backup's result " +
    "is its exit code; a Restore's is its outcome. The two kinds do not share a badge " +
    "rule, because they do not share a field.</p>" +
    table(["NAME", "KIND", "CREATED", "PHASE", "RESULT", "SIGNED"], rows) +
    listFooter()
  );
}

/** One Restore, in full. */
export function renderRestoreDetail(object) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const evidence = status.evidence || {};
  const integrity = status.integrity || {};
  const objectives = status.objectives || {};
  const measured = status.measured || {};
  const preflight = status.topicPreflight || {};
  const archive = spec.sourceArchive || {};
  const newTopics = Array.isArray(status.newTopics) ? status.newTopics : [];
  const oldTopics = Array.isArray(status.oldTopics) ? status.oldTopics : [];

  return (
    "<h2>Restore " + nameOf(object) + "</h2>" +
    restoreBadge(status) +
    facts([
      ["phase", cell(status.phase)],
      ["exit code", cell(status.exitCode)],
      ["reason", cell(status.reason)],
      ["last phase completed", cell(status.lastPhaseCompleted)],
      ["outcome", cell(status.outcome)],
      ["target mode", cell((spec.target || {}).mode)],
      ["point in time", cell(spec.pointInTime)],
      ["backup set", cell(spec.backupSetRef)],
    ]) +
    "<h3>Integrity</h3>" +
    facts([
      ["level", cell(integrity.level)],
      ["result", cell(integrity.result)],
      ["partial reason", cell(integrity.partialReason)],
    ]) +
    "<h3>Objectives asked for, and what the run achieved</h3>" +
    facts([
      ["objectives.rtoSeconds", cell(objectives.rtoSeconds)],
      ["objectives.rpoSeconds", cell(objectives.rpoSeconds)],
      ["objectives.passRate", cell(objectives.passRate)],
      ["objectives.met", cell(objectives.met)],
      ["measured.rtoSeconds", cell(measured.rtoSeconds)],
      ["measured.rpoSeconds", cell(measured.rpoSeconds)],
    ]) +
    "<h3>Target topic preflight</h3>" +
    facts([
      ["timestampType", cell(preflight.timestampType)],
      ["retentionMs", cell(preflight.retentionMs)],
      ["timestampBound", cell(preflight.timestampBound)],
    ]) +
    "<h3>Topics</h3>" +
    facts([
      ["new topics", newTopics.length === 0 ? cell(null) : esc(newTopics.join(", "))],
      ["old topics -- written to by nothing, in any tag",
        oldTopics.length === 0 ? cell(null) : esc(oldTopics.join(", "))],
    ]) +
    evidenceBlock(evidence) +
    "<p class=\"engine-subreport\">" + ENGINE_SUBREPORT_LINE + "</p>" +
    "<h3>Check it yourself</h3>" +
    "<p class=\"note\">The two keys above name objects in your archive; the verifiers " +
    "take local files. So the first two lines fetch, and the last two verify -- once " +
    "with the Rust reader and once with the Python one.</p>" +
    independentCheck(
      "scorecard",
      bucketOf(archive.url),
      evidence.scorecardKey || "",
      evidence.sidecarKey || "",
      "scorecard.json",
      "scorecard.sig",
    )
  );
}

// --------------------------------------------------------------- mount half

export async function mountHistory(node, ns, parse) {
  try {
    const restores = await list(ns, PLURAL);
    const backups = await list(ns, BACKUPS);
    replace(node, parse(renderHistoryList(restores, backups)));
  } catch (error) {
    replace(node, errorBox(error));
  }
}

export async function mountRestoreDetail(node, ns, name, parse) {
  try {
    const object = await get(ns, PLURAL, name);
    replace(node, parse(renderRestoreDetail(object)));
  } catch (error) {
    replace(node, errorBox(error));
  }
}
