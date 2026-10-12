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

/** WHERE THE TWO FETCHES READ, or why this page cannot say.
 *
 *  A destination-backed run's `spec.archive.url` is the sentinel
 *  `logweir-destination://<name>`, NOT a bucket: reading a bucket out of it
 *  names the destination (`s3://primary/...`). The bucket, endpoint and
 *  region are the destination's, as the product API projects them
 *  (`storage`), read beside the run; `where` is `{destination}` or
 *  `{destinationError}`. A destination whose location digest is not the one
 *  this run froze is not where the run wrote, and is not used.
 *
 *  THE PREFIX IS NOT IN THE PATH, ON PURPOSE: the receipt and sidecar keys are
 *  bucket-absolute under `logweir/`, the evidence root every run writes to
 *  whatever the destination's prefix (`DestinationLocation::
 *  evidence_storage_url`). A legacy inline run reads its bucket off its own
 *  URL, as it always did. */
export function evidenceRoute(object, where) {
  const spec = (object && object.spec) || {};
  const ref = (spec.destinationRef || {}).name;
  if (typeof ref !== "string" || ref.length === 0) {
    return { bucket: bucketOf((spec.archive || {}).url) };
  }
  const w = where || {};
  const destination = w.destination || null;
  const missing = (why) => ({ bucket: "<bucket of destination " + ref + ">", missing: why });
  if (destination === null) {
    return missing("destination " + ref + " could not be read" +
      (w.destinationError ? ": " + String(w.destinationError.message || w.destinationError) : "") +
      ", so this page does not know its bucket.");
  }
  const frozen = ((object && object.status) || {}).locationDigest;
  const live = destination.locationDigest;
  if (typeof frozen === "string" && typeof live === "string" && frozen !== live) {
    return missing("destination " + ref + " now names location " + live + ", and this run " +
      "froze " + frozen + ", so its bucket is not where this run wrote.");
  }
  const storage = destination.storage || {};
  return { bucket: storage.bucket, endpoint: storage.endpoint, region: storage.region };
}

/** Said in place of the four lines while a run has recorded no receipt key. */
export const NO_EVIDENCE_KEYS_SENTENCE =
  "This run has recorded no evidence keys yet, so there is nothing to fetch. The commands " +
  "appear here once it records them.";

/** One Backup, in full: the run, the covered window as two RFC 3339 instants,
 *  the evidence block, and the four-line independent check, against the
 *  bucket [`evidenceRoute`] names. */
export function renderBackupDetail(object, where) {
  const spec = (object && object.spec) || {};
  const status = (object && object.status) || {};
  const evidence = status.evidence || {};
  const auth = status.auth || {};
  const archive = spec.archive || {};
  const route = evidenceRoute(object, where);
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
    // WHAT THIS RUN COVERED, from its own `status.selection` alone.
    // `renderCoverageLine` is shared with the schedules page so a coverage label
    // cannot read differently on a run and on its schedule; with no selection
    // block it names the missing projection, and a refused dynamic resolution
    // shows the recorded `TopicsResolved=False` reason.
    renderCoverageLine(object) +
    "<p class=\"covered\">" + esc(coveredWindow(status.windowCovered)) + "</p>" +
    evidenceBlock(evidence) +
    "<section class=\"check\" id=\"backup-check\"><h3>Check it yourself</h3>" +
    (typeof evidence.receiptKey !== "string" || evidence.receiptKey.length === 0
      ? "<p class=\"note\" id=\"backup-check-no-keys\">" + esc(NO_EVIDENCE_KEYS_SENTENCE) + "</p>"
      : "<p class=\"note\">The two keys above name objects in your archive; the verifiers " +
        "take local files. So the first two lines fetch, and the last two verify -- once " +
        "with the Rust reader and once with the Python one.</p>" +
        (route.missing
          ? "<p class=\"caveat\" id=\"backup-check-bucket\">" + esc(route.missing) + "</p>"
          : "") +
        independentCheck(
          "backup-receipt",
          route.bucket,
          evidence.receiptKey,
          evidence.sidecarKey || "",
          "receipt.json",
          "receipt.sig",
          route,
        )) +
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

export async function mountBackupDetail(node, ns, name, parse, lifecycle, deps) {
  const api = (deps || {}).api || API;
  try {
    const object = await api.get(ns, PLURAL, name, readOptions(lifecycle));
    if (!active(lifecycle)) {
      return;
    }
    const where = await readDestination(api, ns, object, lifecycle);
    if (active(lifecycle)) {
      replace(node, parse(renderBackupDetail(object, where)));
    }
  } catch (error) {
    if (!cancelled(error, lifecycle) && active(lifecycle)) {
      replace(node, errorBox(error));
    }
  }
}

/** The destination a run names, read for its bucket: `null` for a legacy
 *  inline run, `{destination}`, or `{destinationError}` -- a refusal (a role
 *  that may read backups and not destinations) is the check's caveat, not the
 *  page's error. */
async function readDestination(api, ns, object, lifecycle) {
  const ref = ((((object || {}).spec) || {}).destinationRef || {}).name;
  if (typeof ref !== "string" || ref.length === 0) {
    return null;
  }
  try {
    return { destination: (await api.destination(ns, ref, readOptions(lifecycle))).item };
  } catch (error) {
    if (cancelled(error, lifecycle)) {
      throw error;
    }
    return { destinationError: error };
  }
}
