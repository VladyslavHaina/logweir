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

// THE ADAPTER (PLAT-18.1). One object, whichever API is in front of the page:
// `ui/client.js` decides that once at boot and dispatches every call.
const API = apiClient();

/** The sentence the backups table carries when the namespace holds none. */
export const NO_BACKUP_SENTENCE =
  "No Backup has run in this namespace yet. A BackupSchedule creates one at its next slot, " +
  "and it appears here with the evidence weirkeeper recorded for it.";

/** The verification half of BOTH rules, and nothing else.
 *
 *  `Ok` is `[verifiedAt, matchedKeyId, basis]`; anything else is `null`. A
 *  `Valid` carrying no `matchedKeyId` or no `verifiedAt` is NOT a verdict the
 *  controller ever writes -- it sets both on every `Valid`
 *  (`crates/weirkeeper/src/verification.rs`) -- so a block shaped like that is
 *  treated here as no verdict at all rather than as a green badge whose label
 *  cannot be written.
 *
 *  THE TRUST BASIS IS PART OF THE RULE NOW (D3 section 7.4), and it is the
 *  controller's rule (`weirkeeper::verification::ValidBasis`, read here by
 *  render.js `trustBlockAllowsGreen`). A PRESENT `trust` block is green only
 *  on `Current` or `Historical`. Every other present block is NOT green:
 *  `RecordedBeforeRevocation` (a compromised key signed it and a controller
 *  happened to have seen it first, which is an observation and not a
 *  signature this installation still accepts), `Unverified` (nothing has been
 *  compared yet), the string `None`, a block with no basis or a `null` one,
 *  and a word this build does not know. The controller's badge and
 *  `logweir-api` refuse each of those too.
 *
 *  AN OBJECT WITH NO `trust` KEY AT ALL -- OR `trust: null`, THE SAME ABSENCE --
 *  STAYS GREEN. The block is additive
 *  and every object written before D3 carries none; D3 section 12's rule is that an
 *  absent field is NOT OBSERVED, and treating "an older controller wrote this"
 *  as a downgrade would turn every archive in an upgraded cluster red. A console
 *  detail (`client.js` `mergeOperation`) writes no block where the API said
 *  the block was absent, so it reaches this rule the same way. */
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
 *  A `Historical` BADGE IS A PASS AND NOT A WARNING. D3 section 7.6's supported key
 *  rotation is meant to produce exactly this: the key was valid when it
 *  signed, the old archives keep verifying, and the public material can never
 *  be edited out of the policy. The qualifier says so on the badge so a reader
 *  is not left wondering why a retired key id appears on a green row. */
export function greenLabel(verifiedAt, matchedKeyId, basis) {
  return "verified by weirkeeper at " + verifiedAt + " against key " + matchedKeyId +
    (basis === "Historical" ? HISTORICAL_SUFFIX : "");
}

/** **The Backup badge rule.** Green if and only if the recorded verification
 *  is `Valid`, its trust basis is one a green badge may carry, AND the run's
 *  own exit code is `0`. Reads no `outcome`, because a `Backup` has none.
 *
 *  WHEN IT IS NOT GREEN THE CAPTION STILL CARRIES THE WORD `unverified`, and
 *  now says WHICH of the cases it is. The word is what every older surface
 *  looks for and what the existing badge rule promises; the case is what says
 *  where to go and look. `Invalid` is a claim about the DOCUMENT,
 *  `NotAttempted` about the CONTROLLER and `Untrusted` about the SIGNER
 *  (D3 section 7.4), and a console that printed one word for all three would have
 *  destroyed the only distinction that says what to fix. */
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
 *  `spec.trigger.kind` IS THE ONLY FIELD READ. `spec.triggeredBy` -- the older
 *  `manual | schedule` string -- is still on every run and is still rendered on
 *  the detail view, but it cannot tell a catch-up or a retry from an ordinary
 *  slot, so reading it as a kind would flatten four facts into two. A run
 *  frozen before PLAT-05.1 has no trigger, and the column says that rather
 *  than filling in `Scheduled`.
 *
 *  AND THE RETRY CEILING IS NOT GUESSED. A retry renders "Retry, attempt 2"
 *  here: the "of N" needs the schedule's CURRENT `retry.maxRetries`, which a
 *  run carries no copy of, and this table does not read schedules. The
 *  schedule's own card, which does hold the policy, renders "Retry 2 of 3". */
export const TRIGGER_COLUMN_SENTENCE =
  "TRIGGER is the run's own spec.trigger: Scheduled for a slot that fired at its instant, " +
  "CatchUp for the same slot started late, Retry for attempt k of it with a new execution id, " +
  "and Manual for a run a person asked for. A run created before triggers were recorded carries none and " +
  "says so; it is never read from triggeredBy, which cannot tell those four apart.";

/** THE LINK EVERY ROW CARRIES TO THE DURABLE OPERATION VIEW (PLAT-12.1).
 *
 *  BOTH HALVES OF THE IDENTITY TRAVEL, for the same reason the restore link
 *  carries a uid: a name is reused, and a run deleted and recreated under one
 *  is a different run with different evidence. The operation view refuses a
 *  uid that does not answer rather than switching under a reader. */
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
    // THE RUN'S TWO LINKS IN ONE CELL (MCP-25's class, on this table): its
    // page, and the operation view beneath it. As the eighth column "Follow
    // this run" was the one a 1024 px window scrolled out of sight.
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
      // CREATED, NOT AGE (MCP-18): the column holds the creation instant, and a
      // column named for a duration needed a paragraph to say it was not one.
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
      ["identity presented", cell(auth.mode) + " " + cell(auth.username)],
      ["operation", operationCell(object, (object.metadata || {}).namespace)],
    ]) +
    // WHAT THIS RUN COVERED, from its own `status.selection` and from nowhere
    // else. `renderCoverageLine` is shared with the schedules page so a
    // coverage label cannot read one way on a run and another on the schedule
    // that made it; it renders the sentence naming the missing projection when
    // the object carries no selection block, and the recorded
    // `TopicsResolved=False` reason when a dynamic resolution refused.
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

// --------------------------------------------------------------- mount half

export async function mountBackups(node, ns, parse, lifecycle, deps) {
  const api = (deps || {}).api || API;
  try {
    // THE FIRST PAGE IS SHOWN WHILE THE REST ARE READ (MCP-26): at 258 runs the
    // list was a spinner until the last page answered.
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
