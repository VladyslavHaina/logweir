// THE BACKUP HALF OF THE GREEN-BADGE RULE, carried over from the old console
// (ui/pages/backups.js, ui/pages/operation.js, ui/client.js `mergeOperation`).
//
// A run is green only when the CONTROLLER fetched its signed receipt and
// verified it, against a key this installation accepts, and the run itself
// exited 0. This page holds no key and checks nothing itself, so it never says
// "verified in your browser". A `Backup` has no `outcome`: the run's half of
// the rule is its exit code.
//
// TWO DOCUMENTS, TWO RULES:
// - a LIST row carries the API's summary only (`verificationState`,
//   `verifiedSuccess`), and is green exactly when that says so;
// - the run's own PAGE reads the operation view: the signature result, the
//   combined trust word, its basis, the key and the instant, and the exit code.
// When not green, the words name which case it is: Invalid is about the
// document, NotAttempted the controller, Untrusted the signer (D3 section 7.4).
import { hasShape, s3Arg, shellArg } from "../../lib/shell";
import type { Backup, OperationSummary, OperationView } from "../../api/backups";

/** The two combined trust words a green badge may carry (the API's own vocabulary). */
export const GREEN_TRUST_STATES: readonly string[] = ["verified", "verifiedHistorical"];
/** The trust bases a green badge may carry. */
export const GREEN_BASES: readonly string[] = ["Current", "Historical"];
/** How the API spells an absent `trust` block. It is not a downgrade (D3 section 12). */
export const TRUST_BASIS_NOT_OBSERVED = "None";

/** Whether a basis the PRODUCT API published leaves a `Valid` verdict green: Current, Historical, or the absence it spells `None`. */
export function basisAllowsGreen(basis: unknown): boolean {
  if (typeof basis !== "string" || basis === TRUST_BASIS_NOT_OBSERVED) return true;
  return GREEN_BASES.includes(basis);
}

/** The cases that are not green, each named by what it is a claim about. */
export const CASES = {
  untrusted: "Untrusted signer: the bytes are authentic and this installation does not accept the key.",
  invalid: "Invalid: the signature did not verify, or a digest did not match.",
  notAttempted: "Not attempted: the controller could not check this document.",
  notApplicable: "Not applicable: this run writes no signed document.",
  noEvidence: "No evidence: this run recorded no signed document.",
  pending: "Pending: the run has not finished, or its verdict is not written yet.",
  unknown: "Unknown: the controller's verdict could not be read.",
  recordedBeforeRevocation:
    "Recorded before revocation: a compromised key signed it, and a controller had seen it before the revocation took effect.",
  notRecorded: "No verification was recorded for this run.",
  runNotSucceeded: "The document verified, and the run itself did not succeed.",
  listValidNotGreen:
    "The document verified, and the run did not succeed or its signer is not accepted now; the run's own page says which.",
} as const;

/** The short word a list row shows under its status for each case. */
export const CASE_WORDS: Record<string, string> = {
  [CASES.untrusted]: "untrusted signer",
  [CASES.invalid]: "invalid evidence",
  [CASES.notAttempted]: "not attempted",
  [CASES.notApplicable]: "no signed document",
  [CASES.noEvidence]: "no evidence",
  [CASES.pending]: "verdict pending",
  [CASES.unknown]: "verdict unreadable",
  [CASES.recordedBeforeRevocation]: "revoked signer",
  [CASES.notRecorded]: "not recorded",
  [CASES.runNotSucceeded]: "run did not succeed",
  [CASES.listValidNotGreen]: "not accepted now",
};

/** What a green list row does not carry, said once under the list. */
export const LIST_VERIFIED_NOTE =
  "A list row carries the controller's verdict, not the key or the instant it verified at; the run's own page shows both.";

/** The suffix a green verdict carries when the key has since been retired. It is a pass, not a warning. */
export const HISTORICAL_SUFFIX = " (signed before that key was retired)";

export type Verdict =
  | { green: true; label: string; historical: boolean; verifiedAt: string | null; keyId: string | null }
  | { green: false; reason: string };

const LIST_CASES: Record<string, string> = {
  valid: CASES.listValidNotGreen,
  invalid: CASES.invalid,
  notAttempted: CASES.notAttempted,
  noEvidence: CASES.noEvidence,
  pending: CASES.pending,
  unknown: CASES.unknown,
};

/** **The list rule.** Green exactly when the API's summary says the controller's rule passed and the signature is valid. */
export function listVerdict(summary: Pick<OperationSummary, "verificationState" | "verifiedSuccess"> | undefined): Verdict {
  if (summary?.verifiedSuccess === true && summary.verificationState === "valid") {
    return { green: true, label: "Verified by the controller", historical: false, verifiedAt: null, keyId: null };
  }
  return { green: false, reason: LIST_CASES[String(summary?.verificationState)] ?? CASES.notRecorded };
}

const RECORDED: Record<string, "Valid" | "Invalid" | "NotAttempted"> = {
  valid: "Valid",
  invalid: "Invalid",
  notAttempted: "NotAttempted",
};

type Trust = { state?: string | null; basis?: string | null } | null | undefined;

/**
 * The recorded result in the resource's own spelling. The signature column has no word for the
 * controller's `Untrusted` (it arrives as `unknown`), so an `untrusted` trust word restores it
 * (`mergeOperation`'s `foldedResult`). A `Valid` under an `untrusted` word is refused below by
 * the trust-word check, with the same case.
 */
function recordedResult(state: string | undefined, trust: Trust): string | undefined {
  const recorded = state ? RECORDED[state] : undefined;
  return recorded === undefined && trust?.state === "untrusted" ? "Untrusted" : recorded;
}

const TRUST_CASES: Record<string, string> = {
  untrusted: CASES.untrusted,
  invalid: CASES.invalid,
  notAttempted: CASES.notAttempted,
  notApplicable: CASES.notApplicable,
  pending: CASES.pending,
};

/**
 * **The run's own rule**, over its operation view. Green when, and only when:
 * - the signature result is `valid` and the controller recorded the key and the instant;
 * - the combined trust word, where the API sends one, is `verified` or `verifiedHistorical`;
 * - the basis is Current, Historical, or absent (`None`): RecordedBeforeRevocation, Unverified
 *   and a word this build does not know are never green, whichever word carried them;
 * - the run exited 0, and the API's own `verifiedSuccess` agrees.
 * An absent `trust` block (an older API) keeps the pre-existing rule: it is not a downgrade.
 */
export function detailVerdict(op: Pick<OperationView, "verification" | "result" | "verifiedSuccess"> & { trust?: Trust }): Verdict {
  const v = op.verification ?? { state: "unknown" };
  const trust: Trust = op.trust && typeof op.trust === "object" ? op.trust : null;
  const recorded = recordedResult(v.state, trust);
  const basis = trust?.basis ?? undefined;
  const runOk = op.result?.exitCode === 0;

  if (basis === "RecordedBeforeRevocation" && (recorded === "Valid" || recorded === "Untrusted")) {
    return { green: false, reason: CASES.recordedBeforeRevocation };
  }
  if (recorded === undefined) {
    // No result was recorded: the verification state says which case (pending, no evidence, unreadable).
    return listVerdict({ verificationState: v.state, verifiedSuccess: false });
  }
  if (recorded === "Untrusted") return { green: false, reason: CASES.untrusted };
  if (recorded === "Invalid") return { green: false, reason: CASES.invalid };
  if (recorded === "NotAttempted") return { green: false, reason: CASES.notAttempted };

  // recorded === "Valid"
  if (!basisAllowsGreen(basis)) return { green: false, reason: CASES.notRecorded };
  if (trust && typeof trust.state === "string" && !GREEN_TRUST_STATES.includes(trust.state)) {
    return { green: false, reason: TRUST_CASES[trust.state] ?? trust.state };
  }
  const verifiedAt = typeof v.verifiedAt === "string" && v.verifiedAt.length > 0 ? v.verifiedAt : null;
  const keyId = typeof v.matchedKeyId === "string" && v.matchedKeyId.length > 0 ? v.matchedKeyId : null;
  if (verifiedAt === null || keyId === null) return { green: false, reason: CASES.notRecorded };
  if (!runOk || op.verifiedSuccess !== true) return { green: false, reason: CASES.runNotSucceeded };
  const historical = trust?.state === "verifiedHistorical" || basis === "Historical";
  return {
    green: true,
    label: `Verified by the controller at ${verifiedAt} against key ${keyId}${historical ? HISTORICAL_SUFFIX : ""}`,
    historical,
    verifiedAt,
    keyId,
  };
}

// ------------------------------------------------------------ run facts

/** A run that IS a recovery point: succeeded, with a backup set and a covered window (the old `isRecoveryPoint`). */
export function isRecoveryPoint(b: Backup): boolean {
  return (
    b.operation.state === "succeeded" &&
    typeof b.backupId === "string" &&
    b.backupId.length > 0 &&
    typeof b.windowCovered?.fromMs === "number" &&
    typeof b.windowCovered?.toMs === "number"
  );
}

export type RestoreOffer = { offer: true } | { offer: false; reason: string };

/**
 * Whether "Restore this point" is offered for a run, and if not, why. The wizard lists verified
 * points, so a link to anything else would open a refusal; a role that cannot restore is told who can.
 */
export function restoreOffer(b: Backup, mayRestore: boolean): RestoreOffer {
  if (!isRecoveryPoint(b)) {
    return { offer: false, reason: b.operation.terminal ? "This run made no recovery point." : "This run has not finished." };
  }
  if (!listVerdict(b.operation).green) {
    return { offer: false, reason: "The controller has not verified this run's receipt, so it is not offered as a recovery point." };
  }
  if (!mayRestore) return { offer: false, reason: RESTORE_NEEDS_ROLE };
  return { offer: true };
}

/** What a role that cannot restore reads where the action would be. */
export const RESTORE_NEEDS_ROLE = "An operator or administrator can restore this point.";

/** The trigger, from `trigger` and nothing else: `triggeredBy` cannot tell a catch-up or a retry from a slot. */
export function triggerWords(trigger: Backup["trigger"]): string | null {
  if (!trigger || typeof trigger.kind !== "string") return null;
  if (trigger.kind === "Retry") return `Retry, attempt ${typeof trigger.attempt === "number" ? trigger.attempt : "?"}`;
  if (trigger.kind === "CatchUp") return "Catch-up";
  return trigger.kind;
}

export const NO_TRIGGER = "Trigger not recorded (the run predates recorded triggers, or this API does not publish it)";
export const NO_REVISION = "Revision not recorded (the run predates recorded revisions, or this API does not publish it)";

/** `revision g7 · policy sha256:…`, or null when the run recorded neither. */
export function revisionWords(ref: Backup["scheduleRef"]): string | null {
  const parts: string[] = [];
  if (typeof ref?.generation === "number") parts.push(`revision g${ref.generation}`);
  if (typeof ref?.runPolicySha256 === "string" && ref.runPolicySha256.length > 0) parts.push(`policy ${ref.runPolicySha256}`);
  return parts.length === 0 ? null : parts.join(" · ");
}

const IN_PROGRESS: Record<string, string> = {
  pending: "Starting",
  queued: "Queued",
  preparing: "Preparing",
  running: "Running",
  verifying: "Verifying",
};

/** The lifecycle word for a run that is not finished. A queued manual run names its ceiling, copied, never computed. */
export function progressWord(b: Pick<Backup, "operation" | "queue">): string {
  const state = b.operation.state;
  const limit = b.queue?.limit;
  if (state === "queued" && Number.isInteger(limit) && (limit as number) > 0) return `Queued (limit ${limit} active)`;
  return IN_PROGRESS[state] ?? "In progress";
}

/** What a queued manual run is, in fixed words. */
export const QUEUED_RUN_SENTENCE =
  "This manual run is waiting for a slot. The namespace lets a fixed number of manual runs be active at once, and this one starts, in creation order, when one of them finishes. Nothing has been created for it yet: no plan and no Job. Scheduled runs are not counted and are never queued.";

/** The covered window, both ends as UTC instants (end exclusive), or what is true instead. */
export function windowWords(b: Pick<Backup, "windowCovered" | "operation">): string {
  if (!b.windowCovered) return b.operation.terminal ? "Not recorded" : "When it finishes";
  const t = (ms: number) => new Date(ms).toISOString().replace("T", " ").replace(/\.\d{3}Z$/, " UTC");
  return `${t(b.windowCovered.fromMs)} to ${t(b.windowCovered.toMs)}`;
}

/** The covered window as the list shows it: hours and minutes only. */
export function windowShort(b: Pick<Backup, "windowCovered" | "operation">, now = Date.now()): string {
  if (!b.windowCovered) return b.operation.terminal ? "Not recorded" : "When it finishes";
  const { fromMs, toMs } = b.windowCovered;
  const day = (ms: number) => new Date(ms).toISOString().slice(0, 10);
  const hm = (ms: number) => new Date(ms).toISOString().slice(11, 16);
  const md = (ms: number) => new Date(ms).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
  // Hours alone only when the whole window is on one UTC day, today; otherwise each end names its day.
  if (day(fromMs) === day(toMs) && day(toMs) === day(now)) return `${hm(fromMs)} to ${hm(toMs)} UTC`;
  if (day(fromMs) === day(toMs)) return `${md(fromMs)}, ${hm(fromMs)} to ${hm(toMs)} UTC`;
  return `${md(fromMs)}, ${hm(fromMs)} to ${md(toMs)}, ${hm(toMs)} UTC`;
}

/** The identity a run presented to the brokers: mode and username, never a password. */
export function identityWords(auth: Backup["observedAuth"]): string {
  const parts = [auth?.mode, auth?.username].filter((v): v is string => typeof v === "string" && v.length > 0);
  return parts.length === 0 ? "Not recorded" : parts.join(" ");
}

// ------------------------------------------------------ check it yourself


/** The bucket an object-store URL names, or null. */
export function bucketOf(url: string | null | undefined): string | null {
  if (typeof url !== "string" || url.length === 0) return null;
  const at = url.indexOf("://");
  const rest = at === -1 ? url : url.slice(at + 3);
  const bucket = rest.split("/")[0] ?? "";
  return bucket.length === 0 ? null : bucket;
}

export interface EvidenceRoute {
  bucket: string;
  endpoint?: string | null;
  region?: string | null;
  pathStyle?: boolean;
  /** Why this page does not know the bucket, when it does not. */
  missing?: string;
}

/**
 * WHERE THE TWO FETCHES READ (the old `evidenceRoute`). A destination-backed run's archive URL is
 * the sentinel `logweir-destination://<name>`, not a bucket: the bucket is the destination's, read
 * beside the run, and a destination whose location digest is not the one the run froze is not used.
 * Evidence keys are bucket-absolute under `logweir/`, so the destination's prefix is not in the path.
 */
export function evidenceRoute(
  b: Pick<Backup, "destinationRef" | "archive" | "locationDigest">,
  destination: { storage: { bucket: string; endpoint?: string | null; region?: string | null; addressing?: string }; locationDigest?: string | null } | undefined,
  destinationError: unknown,
): EvidenceRoute {
  const ref = b.destinationRef?.name;
  if (!ref) return { bucket: bucketOf(b.archive?.url) ?? "<your evidence bucket>" };
  // The placeholder goes into a command: the name only when it is a Kubernetes name.
  const placeholder = hasShape(ref, "name") ? `<bucket of destination ${ref}>` : "<bucket of the destination>";
  if (!destination) {
    const why = destinationError instanceof Error ? `: ${destinationError.message}` : "";
    return { bucket: placeholder, missing: `Destination ${ref} could not be read${why}, so this page does not know its bucket.` };
  }
  const frozen = b.locationDigest;
  const live = destination.locationDigest;
  if (typeof frozen === "string" && typeof live === "string" && frozen !== live) {
    return {
      bucket: placeholder,
      missing: `Destination ${ref} now names location ${live}, and this run froze ${frozen}, so its bucket is not where this run wrote.`,
    };
  }
  return {
    bucket: destination.storage.bucket,
    endpoint: destination.storage.endpoint,
    region: destination.storage.region,
    pathStyle: destination.storage.addressing === "pathStyle",
  };
}

/** Whether a run recorded BOTH keys the fetches name; a fetch of an empty key is a fetch of the bucket root. */
export function hasEvidenceKeys(receiptKey: string | null | undefined, sidecarKey: string | null | undefined): boolean {
  return typeof receiptKey === "string" && receiptKey.length > 0 && typeof sidecarKey === "string" && sidecarKey.length > 0;
}

export const NO_EVIDENCE_KEYS =
  "This run has recorded no evidence keys, so there is nothing to fetch. The commands appear here if it records them.";

export const PATH_STYLE_SENTENCE =
  "This store is addressed in path style. With --endpoint-url the AWS CLI uses path style unless your profile sets s3.addressing_style to virtual; if it does, run `aws configure set s3.addressing_style path` before the two fetches.";

/** The four lines: fetch the receipt and its signature, then verify them with the Rust reader and the Python one. */
export function checkCommands(route: EvidenceRoute, receiptKey: string, sidecarKey: string): string[] {
  // Server strings enter only through shellArg/s3Arg: shaped and quoted, or a placeholder.
  const flags =
    (route.endpoint ? ` --endpoint-url ${shellArg(route.endpoint, "url", "<endpoint>")}` : "") +
    (route.region ? ` --region ${shellArg(route.region, "region", "<region>")}` : "");
  const type = "backup-receipt";
  return [
    `aws s3 cp ${s3Arg(route.bucket, receiptKey)} ./receipt.json${flags}`,
    `aws s3 cp ${s3Arg(route.bucket, sidecarKey)} ./receipt.sig${flags}`,
    `logweir drill verify --payload-type ${type} --scorecard ./receipt.json --signature ./receipt.sig --public-key <your key>`,
    `python3 verify_scorecard.py --payload-type ${type} ./receipt.json ./receipt.sig <your key>`,
  ];
}
