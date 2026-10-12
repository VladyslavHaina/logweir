// pages/backups.js -- the Backup list, one Backup's detail, and THE BACKUP
// HALF OF THE BADGE RULE.
//
// THERE ARE TWO BADGE RULES, ONE PER KIND, AND THIS FILE HOLDS ONE OF THEM.
//
//   Backup  -- green requires `evidence.verification.result == "Valid"` AND
//              `status.exitCode === 0`.
//   Restore -- green requires `evidence.verification.result == "Valid"` AND
//              `status.outcome === "pass"`, and never over a recorded non-zero
//              `status.exitCode`.  (pages/history.js)
//
// In both, `Valid` counts only beside no `trust` block or a `Current` /
// `Historical` basis (`validVerification`, D3 sections 7.4 and 12).
//
// A `Backup` HAS NO `outcome` FIELD AT ALL -- not on the status, the recorded
// verification or the signed receipt -- so reading `outcome` here would
// compare `undefined` against a string and every badge would go grey,
// looking like a data problem. The two rules are written out and tested
// separately, and neither reads the other's field.
//
// WHAT A GREEN BADGE CLAIMS: that WEIRKEEPER -- the controller, with its own
// read-only evidence credential and the trusted public keys --
// fetched the signed document and verified it, at the instant and against
// the key id the label names. Never that this page verified anything: it
// holds no key and performs no cryptography, so "verified in your browser"
// must never render, and the specs assert its absence from every badge.
//
// WHEN IT IS NOT GREEN, THE WORD IS `unverified`, never "pass": a `pass` is a
// statement about a RUN, and the badge is about a DOCUMENT. The recorded
// `result` and `detail` are rendered beside it so a reader can see which
// case it was.

import { apiClient } from "../client.js";
import { active, cancelled, readOptions } from "../lifecycle.js";
import {
  HISTORICAL_SUFFIX,
  badge,
  trustBlockAllowsGreen,
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
  runPhaseBadge,
  QUEUED_RUN_SENTENCE,
  replace,
  revisionLine,
  table,
  triggerBadge,
  unverifiedCaption,
  summaryBadge,
  listVerifiedNote,
  when,
} from "../render.js";
import { itemsOf } from "./clusters.js";
import { renderCoverageLine } from "./schedules.js";
import { operationRoute } from "./operation.js";

const PLURAL = "backups";

// The adapter: one object, whichever API is in front of the page
// (`ui/client.js` decides once at boot).
const API = apiClient();

/** The sentence the backups table carries when the namespace holds none. */
export const NO_BACKUP_SENTENCE =
  "No Backup has run in this namespace yet. A BackupSchedule creates one at its next slot, " +
  "and it appears here with the evidence weirkeeper recorded for it.";

/** The verification half of BOTH rules, and nothing else.
 *
 *  `Ok` is `[verifiedAt, matchedKeyId, basis]`; anything else is `null`. The
 *  controller sets both `matchedKeyId` and `verifiedAt` on every `Valid`, so a
 *  `Valid` missing either is treated as no verdict rather than as a green
 *  badge whose label cannot be written.
 *
 *  THE TRUST BASIS IS PART OF THE RULE (D3 section 7.4), the controller's own
 *  (`weirkeeper::verification::ValidBasis`, read by render.js
 *  `trustBlockAllowsGreen`): a PRESENT `trust` block is green only on
 *  `Current` or `Historical`. `RecordedBeforeRevocation` is an observation,
 *  not a signature this installation still accepts; `Unverified`, `None`, a
 *  missing or `null` basis and an unknown word are not green either.
 *
 *  NO `trust` KEY, OR `trust: null`, STAYS GREEN: the block is additive, an
 *  absent field is NOT OBSERVED (D3 section 12), and reading an older
 *  controller's object as a downgrade would turn every archive in an
 *  upgraded cluster red. A console detail (`client.js` `mergeOperation`)
 *  writes no block where the API said it was absent. */
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
  if (!trustBlockAllowsGreen(verification.trust)) {
    return null;
  }
  const basis = (verification.trust || {}).basis;
  return [at, key, typeof basis === "string" ? basis : null];
}

/** The label a green badge carries, verbatim.
 *
 *  A `Historical` badge is a pass, not a warning: a supported key rotation
 *  (D3 section 7.6) produces exactly this -- the key was valid when it
 *  signed and old archives keep verifying -- and the qualifier says why a
 *  retired key id appears on a green row. */
export function greenLabel(verifiedAt, matchedKeyId, basis) {
  return "verified by weirkeeper at " + verifiedAt + " against key " + matchedKeyId +
    (basis === "Historical" ? HISTORICAL_SUFFIX : "");
}

/** **The Backup badge rule.** Green if and only if the recorded verification
 *  is `Valid`, its trust basis is one a green badge may carry, AND the run's
 *  own exit code is `0`. Reads no `outcome`, because a `Backup` has none.
 *
 *  When it is not green the caption still carries the word `unverified` (what
 *  every surface looks for) and says WHICH case it is: `Invalid` is about
 *  the DOCUMENT, `NotAttempted` the CONTROLLER and `Untrusted` the SIGNER (D3
 *  section 7.4) -- the distinction that says what to fix. */
export function backupBadge(status) {
  const s = status || {};
  // A CONSOLE LIST ROW carries a summary verdict and no recorded block: read
  // the summary (render.js `summaryBadge`) rather than calling it unrecorded.
  if (((s.evidence || {}).verification) === undefined && s.__summary !== undefined) {
    return summaryBadge(s.__summary);
  }
  const verification = ((s.evidence || {}).verification) || {};
  const verified = validVerification(s);
  if (verified === null || s.exitCode !== 0) {
    return badge("unverified", unverifiedCaption(verification, s.exitCode === 0));
  }
  return badge("green", greenLabel(verified[0], verified[1], verified[2]));
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

/** What the TRIGGER column is for, and what it is not.
 *
 *  `spec.trigger.kind` IS THE ONLY FIELD READ. The older `spec.triggeredBy`
 *  (`manual | schedule`) is rendered on the detail view but cannot tell a
 *  catch-up or a retry from an ordinary slot. A run older than triggers has
 *  none, and the column says that rather than filling in `Scheduled`.
 *
 *  The retry ceiling is not guessed: "of N" needs the schedule's CURRENT
 *  `retry.maxRetries`, which a run carries no copy of, so a retry renders
 *  "Retry, attempt 2" here and "Retry 2 of 3" on the schedule's own card. */
export const TRIGGER_COLUMN_SENTENCE =
  "TRIGGER is the run's own spec.trigger: Scheduled for a slot that fired at its instant, " +
  "CatchUp for the same slot started late, Retry for attempt k of it with a new execution id, " +
  "and Manual for a run a person asked for. A run created before triggers were recorded carries none and " +
  "says so; it is never read from triggeredBy, which cannot tell those four apart.";

/** THE LINK EVERY ROW CARRIES TO THE DURABLE OPERATION VIEW. The uid travels
 *  with the name, because a run deleted and recreated under one name is a
 *  different run; the operation view refuses a uid that does not answer
 *  rather than switching under a reader. */
export function operationCell(object, ns) {
  const meta = (object && object.metadata) || {};
  if (typeof meta.name !== "string" || meta.name.length === 0) {
    return cell(null);
  }
  const target = operationRoute(
    ns || meta.namespace || "", "backup", meta.name, meta.uid || "",
  );
  return "<a class=\"action\" href=\"" + esc(target) + "\">Follow this run</a>";
}

/** The backups table. NAME, TRIGGER, PHASE, EXIT, RECORDS, SIGNED, AGE,
 *  OPERATION. */
export function renderBackupList(input, ns) {
  const rows = itemsOf(input).map((object) => {
    const status = object.status || {};
    const meta = object.metadata || {};
    const spec = object.spec || {};
    // The run's two links in one cell, its page and the operation view beneath
    // it, so neither is the last column a narrow window scrolls away.
    const follow = operationCell(object, ns);
    return [
      nameCell(object, ns) + (follow === cell(null) ? "" : "<span class=\"cell-sub\">" + follow + "</span>"),
      triggerBadge(spec.trigger),
      runPhaseBadge(status),
      cell(status.exitCode),
      cell(status.records),
      backupBadge(status),
      when(meta.creationTimestamp),
    ];
  });
  const partial = ((input || {}).__page || {}).partial === true;
  return (
    "<h2>Backups</h2>" +
    "<p class=\"blurb\">Every Backup run in this namespace, with the evidence weirkeeper " +
    "recorded for it.</p>" +
    (partial
      ? "<p class=\"pending\" id=\"backups-partial\" role=\"status\">Showing the first " +
        String(itemsOf(input).length) + " runs while the rest are read...</p>"
      : "") +
    "<p class=\"note\">" + esc(TRIGGER_COLUMN_SENTENCE) + "</p>" +
    table(
      // CREATED, not AGE: the column holds the creation instant.
      ["NAME", "TRIGGER", "PHASE", "EXIT", "RECORDS", "SIGNED", "CREATED"],
      rows,
      NO_BACKUP_SENTENCE,
      undefined,
      { id: "backups", label: "backups", scope: ns },
    ) +
    listVerifiedNote(itemsOf(input)) +
    listFooter()
  );
}

/** The identity a run presented to the brokers, from `status.auth`: its mode
 *  and username, as many of them as were recorded, or "not recorded" (it read
 *  "- -"; PROD-14.1 visual pass, L7). */
export function identityPresented(auth) {
  const a = auth || {};
  const parts = [a.mode, a.username].filter((v) => typeof v === "string" && v.length > 0);
  return parts.length === 0 ? "not recorded" : parts.map((v) => cell(v)).join(" ");
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
    (status.phase === "Queued" ? "<p class=\"note queued\">" + esc(QUEUED_RUN_SENTENCE) + "</p>" : "") +
    facts([
      ["phase", runPhaseBadge(status)],
      ["exit code", cell(status.exitCode)],
      ["exit reason", cell(status.exitReason)],
      ["backup id", cell(status.backupId)],
      ["records", cell(status.records)],
      ["manifest key", cell(status.manifestKey)],
      ["triggered by", cell(spec.triggeredBy)],
      ["trigger", triggerBadge(spec.trigger)],
      ["retry of", cell(((spec.trigger || {}).retryOf || {}).name)],
      ["slot", cell(spec.slot)],
      ["schedule", cell((spec.scheduleRef || {}).name)],
      ["schedule revision", revisionLine(spec.scheduleRef)],
      ["archive", cell(archive.url)],
      ["identity presented", identityPresented(auth)],
      ["operation", operationCell(object, (object.metadata || {}).namespace)],
    ]) +
    // WHAT THIS RUN COVERED, from its own `status.selection` alone.
    // `renderCoverageLine` is shared with the schedules page so a coverage label
    // cannot read differently on a run and on its schedule; with no selection
    // block it names the missing projection, and a refused dynamic resolution
    // shows the recorded `TopicsResolved=False` reason.
    renderCoverageLine(object) +
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

export async function mountBackups(node, ns, parse, lifecycle, deps) {
  const api = (deps || {}).api || API;
  try {
    // The first page is shown while the rest are read, rather than a spinner
    // until the last page answers.
    const options = Object.assign({}, readOptions(lifecycle) || {}, {
      onPage: (partial) => {
        if (active(lifecycle)) {
          replace(node, parse(renderBackupList(partial, ns)));
        }
      },
    });
    const collection = await api.list(ns, PLURAL, options);
    if (active(lifecycle)) {
      replace(node, parse(renderBackupList(collection, ns)));
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

export async function mountBackupDetail(node, ns, name, parse, lifecycle) {
  try {
    const object = await API.get(ns, PLURAL, name, readOptions(lifecycle));
    if (active(lifecycle)) {
      replace(node, parse(renderBackupDetail(object)));
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}
