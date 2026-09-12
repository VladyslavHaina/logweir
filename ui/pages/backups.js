// pages/backups.js -- the Backup list, one Backup's detail, and THE BACKUP
// HALF OF THE BADGE RULE.
//
// THERE ARE TWO BADGE RULES, ONE PER KIND, AND THIS FILE HOLDS ONE OF THEM.
//
//   Backup  -- green requires `evidence.verification.result == "Valid"` AND
//              `status.exitCode === 0`.
//   Restore -- green requires `evidence.verification.result == "Valid"` AND
//              `status.outcome === "pass"`.  (pages/history.js)
//
// A `Backup` HAS NO `outcome` FIELD AT ALL. Not on the status
// (`crates/weirkeeper/src/crds/backup.rs`), not on the recorded verification,
// not on the signed receipt. So the `Backup` rule cannot be the `Restore` rule
// with a different field name: reading `outcome` here would compare `undefined`
// against a string, every badge would go grey, and the failure would look like
// a data problem rather than like a bug in the page. The two rules are written
// out separately, tested separately against four fixtures each, and neither
// reads the other's field.
//
// WHAT A GREEN BADGE CLAIMS, AND WHAT IT DOES NOT. It claims that WEIRKEEPER
// -- the controller, in the cluster, with its own read-only evidence
// credential and the TrustRoster's public keys -- fetched the signed document
// and verified it, at the instant and against the key id the label names. It
// does not claim that this page verified anything. This page holds no key,
// mints no approval and performs no cryptography, so the string
// "verified in your browser" is one this product must never render, and the
// specs assert its absence from every rendered badge.
//
// WHEN IT IS NOT GREEN, THE WORD IS `unverified`. Never "pass": a `pass` is a
// statement about a RUN, and the badge is a statement about a DOCUMENT. Every
// combination that is not (Valid AND the run's own success) gets the same
// single word, and the recorded `result` and `detail` are rendered in the
// evidence block beside it so a reader can see which of the four cases it was.

import { get, list } from "../api.js";
import {
  UNVERIFIED,
  badge,
  bucketOf,
  cell,
  detailLink,
  coveredWindow,
  errorBox,
  esc,
  evidenceBlock,
  facts,
  independentCheck,
  listFooter,
  phaseBadge,
  replace,
  table,
} from "../render.js";
import { itemsOf } from "./clusters.js";

const PLURAL = "backups";

/** The sentence the backups table carries when the namespace holds none. */
export const NO_BACKUP_SENTENCE =
  "No Backup has run in this namespace yet. A BackupSchedule creates one at its next slot, " +
  "and it appears here with the evidence weirkeeper recorded for it.";

/** The verification half of BOTH rules, and nothing else.
 *
 *  `Ok` is `[verifiedAt, matchedKeyId]`; anything else is `null`. A `Valid`
 *  carrying no `matchedKeyId` or no `verifiedAt` is NOT a verdict the
 *  controller ever writes -- it sets both on every `Valid`
 *  (`crates/weirkeeper/src/verification.rs`) -- so a block shaped like that is
 *  treated here as no verdict at all rather than as a green badge whose label
 *  cannot be written. */
export function validVerification(status) {
  const evidence = (status && status.evidence) || {};
  const verification = evidence.verification || {};
  if (verification.result !== "Valid") {
    return null;
  }
  const at = verification.verifiedAt;
  const key = verification.matchedKeyId;
  if (typeof at !== "string" || at.length === 0) {
    return null;
  }
  if (typeof key !== "string" || key.length === 0) {
    return null;
  }
  return [at, key];
}

/** The label a green badge carries, verbatim. */
export function greenLabel(verifiedAt, matchedKeyId) {
  return "verified by weirkeeper at " + verifiedAt + " against key " + matchedKeyId;
}

/** **The Backup badge rule.** Green if and only if the recorded verification
 *  is `Valid` AND the run's own exit code is `0`. Reads no `outcome`, because
 *  a `Backup` has none. */
export function backupBadge(status) {
  const verified = validVerification(status);
  if (verified === null) {
    return badge("unverified", UNVERIFIED);
  }
  if ((status || {}).exitCode !== 0) {
    return badge("unverified", UNVERIFIED);
  }
  return badge("green", greenLabel(verified[0], verified[1]));
}

function nameOf(object) {
  const meta = (object && object.metadata) || {};
  return cell(meta.name);
}

/** The name as a link to this run's detail view, when it has a name. */
function nameCell(object, ns) {
  const meta = (object && object.metadata) || {};
  if (typeof meta.name !== "string" || meta.name.length === 0) {
    return cell(null);
  }
  return detailLink("backups", ns || (meta.namespace || "default"), meta.name);
}

/** The backups table. NAME, PHASE, EXIT, RECORDS, SIGNED, AGE. */
export function renderBackupList(input, ns) {
  const rows = itemsOf(input).map((object) => {
    const status = object.status || {};
    const meta = object.metadata || {};
    return [
      nameCell(object, ns),
      phaseBadge(status.phase),
      cell(status.exitCode),
      cell(status.records),
      backupBadge(status),
      cell(meta.creationTimestamp),
    ];
  });
  return (
    "<h2>Backups</h2>" +
    "<p class=\"blurb\">Every Backup run in this namespace. The AGE column is the " +
    "object's own creation instant, not a duration: these views are computed without " +
    "reading a clock.</p>" +
    table(["NAME", "PHASE", "EXIT", "RECORDS", "SIGNED", "AGE"], rows, NO_BACKUP_SENTENCE) +
    listFooter()
  );
}

/** One Backup, in full: the run, the covered window as two RFC 3339 instants,
 *  the evidence block, and the four-line independent check. */
export function renderBackupDetail(object) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const evidence = status.evidence || {};
  const auth = status.auth || {};
  const archive = spec.archive || {};
  return (
    "<h2>Backup " + nameOf(object) + "</h2>" +
    backupBadge(status) +
    facts([
      ["phase", phaseBadge(status.phase)],
      ["exit code", cell(status.exitCode)],
      ["exit reason", cell(status.exitReason)],
      ["backup id", cell(status.backupId)],
      ["records", cell(status.records)],
      ["manifest key", cell(status.manifestKey)],
      ["triggered by", cell(spec.triggeredBy)],
      ["slot", cell(spec.slot)],
      ["archive", cell(archive.url)],
      ["identity presented", cell(auth.mode) + " " + cell(auth.username)],
    ]) +
    "<p class=\"covered\">" + esc(coveredWindow(status.windowCovered)) + "</p>" +
    evidenceBlock(evidence) +
    "<section class=\"check\"><h3>Check it yourself</h3>" +
    "<p class=\"note\">The two keys above name objects in your archive; the verifiers " +
    "take local files. So the first two lines fetch, and the last two verify -- once " +
    "with the Rust reader and once with the Python one.</p>" +
    independentCheck(
      "backup-receipt",
      bucketOf(archive.url),
      evidence.receiptKey || "",
      evidence.sidecarKey || "",
      "receipt.json",
      "receipt.sig",
    ) +
    "</section>"
  );
}

// --------------------------------------------------------------- mount half

export async function mountBackups(node, ns, parse) {
  try {
    const collection = await list(ns, PLURAL);
    replace(node, parse(renderBackupList(collection, ns)));
  } catch (error) {
    replace(node, errorBox(error));
  }
}

export async function mountBackupDetail(node, ns, name, parse) {
  try {
    const object = await get(ns, PLURAL, name);
    replace(node, parse(renderBackupDetail(object)));
  } catch (error) {
    replace(node, errorBox(error));
  }
}
