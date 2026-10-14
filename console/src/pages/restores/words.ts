// What a restore's facts say, in the console's words: the green rule, coverage and
// selection, the scope sentence, the guidance per target mode, and the fixed sentences
// the wizard shows. Ported from ui/render.js, ui/pages/history.js and operation.js.
import { s3Arg, shellArg } from "../../lib/shell";
import type { components } from "../../api/schema";

type S = components["schemas"];
export type Restore = S["Restore"];
export type OperationView = S["OperationView"];
export type CompleteView = S["CompleteVerificationView"];
export type Selection = S["RestoreSelectionView"];

// ------------------------------------------------------------- the green rule

/** The two trust states a green badge may carry; any other word is not green. */
export const GREEN_TRUST_STATES = ["verified", "verifiedHistorical"];
/** `None` is what an absent trust block projects to: not a downgrade. */
export function basisAllowsGreen(basis: string | null | undefined): boolean {
  return typeof basis !== "string" || basis === "None" || basis === "Current" || basis === "Historical";
}

/** `covered: false` is never a pass, whatever else says so. */
export function notCovered(complete: { covered?: boolean } | null | undefined): boolean {
  return !!complete && complete.covered === false;
}

/** A list row is green exactly when the API's verdict (the controller's restore badge
 *  rule: outcome pass AND evidence Valid on an accepted basis) says so, and its complete
 *  verification, if any, covered the restore. */
export function restoreVerified(r: Pick<Restore, "operation" | "coverage">): boolean {
  return r.operation.verifiedSuccess && r.operation.verificationState === "valid" && r.coverage?.covered !== false;
}

/** The run's own page: the operation's verdict, its trust word and basis, and coverage. */
export function operationVerified(op: OperationView): boolean {
  return (
    op.verifiedSuccess &&
    GREEN_TRUST_STATES.includes(op.trust.state) &&
    basisAllowsGreen(op.trust.basis) &&
    !notCovered(op.verificationScope.complete)
  );
}

/** Whether the signed scorecard itself verified (on an accepted basis), whatever the run's
 *  outcome: its facts are then facts, not claims, even on a run that did not pass. */
export function evidenceVerified(op: OperationView): boolean {
  return op.verification.state === "valid" && GREEN_TRUST_STATES.includes(op.trust.state) && basisAllowsGreen(op.trust.basis);
}

/** Why the evidence is not green, by what the claim is about: the document, the
 *  controller or the signer. Never flattened into one word. */
export function evidenceCase(op: OperationView): string {
  if (notCovered(op.verificationScope.complete)) return "complete coverage did not cover this restore, which is never a pass";
  const t = op.trust;
  if (t.basis === "RecordedBeforeRevocation") return "recorded before the signing key was revoked; it is not green";
  if (GREEN_TRUST_STATES.includes(t.state) && !basisAllowsGreen(t.basis)) return `trust basis ${t.basis} is not one a verified result may carry`;
  if (GREEN_TRUST_STATES.includes(t.state)) return op.verifiedSuccess ? "" : "the document verified, and the run did not pass";
  const cases: Record<string, string> = {
    untrusted: "untrusted signer: the bytes are authentic and this installation does not accept the key",
    invalid: "invalid: the signature did not verify, or a digest did not match",
    notAttempted: "not attempted: the controller could not check this document",
    notApplicable: "not applicable: this run writes no signed document",
    pending: "pending: the run has not finished, or its verdict is not written yet",
  };
  return cases[t.state] ?? (t.state || "no verification was recorded");
}

/** A list summary's non-green case, from its own word. */
export function summaryCase(op: Restore["operation"]): string {
  const cases: Record<string, string> = {
    valid: "the document verified, and the run did not pass or its signer is not accepted now",
    invalid: "the signature did not verify, or a digest did not match",
    notAttempted: "the controller could not check this document",
    noEvidence: "this run recorded no signed document",
    pending: "the run has not finished, or its verdict is not written yet",
    unknown: "the controller's verdict could not be read",
  };
  return cases[op.verificationState] ?? "no verification was recorded";
}

// ------------------------------------------------------- coverage and selection

/** One restore's coverage in a few words: `recorded` is the signed claim (absent is not
 *  recorded, never complete), `requested` what the Restore asked for. */
export function coverageWords(recorded: string | null | undefined, requested: string | null | undefined, complete: { covered?: boolean } | null | undefined): string {
  if (recorded === "complete") {
    if (notCovered(complete)) return "complete, not covered: not a pass";
    return complete?.covered === true ? "complete, covered" : "complete, counts not read";
  }
  if (recorded === "sampled") return "sampled";
  return requested === "complete" ? "complete asked for; not recorded yet" : "sampled (not recorded yet)";
}

/** What a NARROWED restore restored; empty when nothing narrowed it. Never reads a
 *  narrowed restore as a restore of everything. */
export function selectionWords(sel: Selection | null | undefined): string {
  if (!sel) return "";
  const at = (ms: number | null | undefined) => (typeof ms === "number" ? new Date(ms).toISOString() : "Not recorded");
  const window = typeof sel.windowStartMs === "number" ? `, from ${at(sel.windowStartMs)} (the plan's stated window start) to ${at(sel.windowEndMs)}` : "";
  const rows = sel.partitions ?? null;
  if (rows && rows.length > 0) {
    return `partial: ${rows.map((r) => `partitions ${r.partitions.join(", ")} of topic ${r.topic}`).join("; ")} (every partition of any other restored topic)${window}`;
  }
  if (typeof sel.narrowedTopics === "number" && sel.narrowedTopics > 0) {
    return `partial: a partition subset of ${sel.narrowedTopics} ${sel.narrowedTopics === 1 ? "topic" : "topics"}, listed in the signed scorecard${window}`;
  }
  if (window) return `partial: every partition${window}`;
  return "partial: a selection this page cannot read; the signed scorecard names it";
}

const n = (v: number | null | undefined) => (typeof v === "number" ? v.toLocaleString("en-US") : "Not recorded");

/** The sentence for a recorded complete verification. */
export function completeCoverageSentence(c: CompleteView | null | undefined, sel: Selection | null | undefined): string {
  if (!c || typeof c.covered !== "boolean") {
    return "The signed scorecard records complete coverage and its counts could not be read here, so whether it covered every partition is not known. It is not shown as a pass.";
  }
  if (!c.covered) {
    const rows = c.partitions ?? [];
    const compared = rows.filter((p) => p.compared).length;
    return `Complete coverage was asked for and did NOT cover this restore: ${c.incompleteReason || "the scorecard names no reason"}. ${
      rows.length > 0 ? `${compared} of ${n(c.partitionCount)} partitions were compared. ` : ""
    }A verification that did not cover the restore is never a pass.`;
  }
  const r = c.replay;
  const narrowed = selectionWords(sel);
  return `${
    narrowed
      ? `Complete coverage of a ${narrowed}: every record of every SELECTED partition was compared with the archive; no other partition of a narrowed topic was restored. `
      : "Complete coverage: every record of every restored partition was compared with the archive. "
  }${n(r.matching)} of ${n(r.expected)} expected records matched byte for byte, headers in order; ${n(r.restored)} were restored; missing ${n(r.missing)}, unexpected ${n(
    r.unexpected,
  )}, duplicated ${n(r.duplicates)}, out of order ${n(r.outOfOrder)}, different ${n(r.mismatched)}.`;
}

/** The sentence beside every restore result. A sampled comparison always says it was a sample. */
export function verificationScopeSentence(scope: S["VerificationScopeView"] | null | undefined): string {
  if (!scope) return "No verification scope was recorded for this run, so how much of it was compared is not known here. An absent scope is not a complete one.";
  if (scope.coverage === "complete") return completeCoverageSentence(scope.complete ?? null, scope.selection ?? null);
  let sentence: string;
  if (scope.level === "none") {
    sentence = "No record check ran for this restore: its evidence attests what was written and by whom, and no records were compared.";
  } else if (typeof scope.recordsSampled !== "number" && typeof scope.recordsSampledMatching !== "number") {
    sentence = `The record check ran at level ${scope.level} and recorded no sampled counts, so how many records were compared is not known. Whatever it compared, it was a sample.`;
  } else {
    sentence = `${n(scope.recordsSampledMatching)} of ${n(scope.recordsSampled)} sampled records matched byte for byte; ${n(
      scope.recordsExpected,
    )} records were expected in the sampled window. This is a sampled check, not an exhaustive comparison.${
      scope.level === "degraded" ? " The comparison was degraded: the records were consumed and counted, not compared byte for byte." : ""
    }`;
  }
  const unsampled = scope.unsampledTopics?.length
    ? ` The partition cap left ${scope.unsampledTopics.join(", ")} without a sampled partition: counted against the bound, not compared record by record.`
    : "";
  const narrowed = selectionWords(scope.selection ?? null);
  return `${sentence}${unsampled}${narrowed ? ` This was a ${narrowed}.` : ""}`;
}

// ----------------------------------------------------------- fixed sentences

/** What a restore's target mode means, from the moment it exists. */
export const TARGET_MODE_MEANING: Record<string, string> = {
  newTopic: "Restores into NEW topics. Logweir writes nothing to the original topics and moves no consumer.",
  scratch: "A REHEARSAL into a scratch cluster: its topics are deleted by teardown when it finishes, or at its expiry when it is kept as a clone, so nothing an application depends on may be pointed at them.",
};

/** What a restore IS, by its mode and naming: a restore under the original names is
 *  still newTopic mode, and is not "new topics beside the originals". */
export function modeMeaning(mode: string, originalName: boolean): string {
  if (mode === "newTopic" && originalName) {
    return "Restores under the ORIGINAL names, into topics that did not exist. It moves no consumer.";
  }
  return TARGET_MODE_MEANING[mode] ?? `This run records target mode ${mode}, which this console does not know.`;
}

/** The guidance after a restore, by mode and naming. */
export function completionGuidance(mode: string, originalName: boolean): string {
  if (mode === "newTopic" && originalName) {
    return "Consumers are not moved: consumer group offsets were not restored, and the engine's offset report is informational only. Validate reads before producers and consumers use these names again.";
  }
  return COMPLETION_GUIDANCE[mode] ?? "This run records no target mode this console knows, so no guidance is shown.";
}

/** The guidance beside a terminal restore's result, by target mode. */
export const COMPLETION_GUIDANCE: Record<string, string> = {
  newTopic:
    "Consumers are not moved. Logweir wrote nothing to the original topics. Consumer group offsets were not restored; the engine's offset report is informational only. Point applications at the new names and validate reads before retiring anything.",
  scratch: "These topics are a rehearsal and are deleted by teardown (a clone's at its expiry); never point an application at them.",
};

export const COMPLETION_NOT_VERIFIED =
  "Completion not yet verified: the counts of what this restore produced are copied from its signed scorecard only once that scorecard has verified. Nothing here is a zero.";

export const RECORDS_IN_WINDOW_LABEL = "Records verified in the sampled window";

export const SCORECARD_CLAIM_SENTENCE =
  "The outcome, integrity and counts on this run are copied from its scorecard, whose signature has not verified: each is the scorecard's claim, not a verified fact.";

export const QUEUED_RUN_SENTENCE =
  "Queued: this manual restore waits for a slot in the namespace's manual-restore pool and starts, in creation order, when one finishes. Nothing has been created for it yet. The queue does not extend its approval: a restore still waiting when the approval expires is refused and must be confirmed again.";

export const RETRY_IDENTITY_SENTENCE =
  "The topic prefix is a fresh one derived from the run being retried, so every target topic name is new and nothing the failed run created is written to. A different prefix is a different plan, plan hash and minted Restore and Approval names: the approval that authorised the failed run is not reused and cannot be (the controller refuses it with PlanHashMismatch). Where the policy asks for one, this restore waits for an approval of its own.";

export const RETRY_OLD_RUN_SENTENCE =
  "The failed restore is not touched: its object, its evidence and the topics it had already created stay as they are. Resume is not implemented; this is a new run from the beginning.";

export const RETRY_FRESH_TARGET_SENTENCE =
  "This run did not complete. A restore cannot be resumed, and any topics it had already created stay as they are. Retrying opens the wizard for a NEW restore of a point you choose, with a fresh topic prefix: a new plan, a new plan hash, and a new approval of its own where the policy asks for one. This run is not modified.";

export const ORIGINAL_NAME_SENTENCE =
  "Each topic is recreated under its own name, a new generation of the name, instead of beside it under a prefix. Only into topics that do not exist: the runner refuses if a name exists, refuses while a broker auto-creates topics (unless the target is a different cluster from the archive's source), and refuses when a declarative owner manages a name. It needs its own approval subject, originalName. Stop every producer of these names first.";

export const ORIGINAL_NAME_NO_OWNER_STATEMENT =
  "No declarative owner (a Strimzi KafkaTopic, a GitOps or Terraform definition) manages any of these names.";

export const ORIGINAL_NAME_COVERAGE_SENTENCE =
  "Required for a restore under the original names, so it is selected and cannot be turned off: under a production name another producer may still be writing, and only the complete check names a record the archive does not hold (OriginalNameNeedsCompleteCoverage).";

export const ORIGINAL_NAME_TYPED_SENTENCE =
  "This namespace is confirmed by one person. A restore under the original names is confirmed only by re-typing every original topic name, exactly, one per line: the console signs what you typed, and the controller and the runner hold it to the plan again (OD-10).";

export const COMPLETE_COVERAGE_COST =
  "Complete coverage reads every archived record of the restored topics and every restored record back, and compares each one with the archive by its source offset. It costs far more than the sampled check: about a minute per GiB of one-KiB records on a laptop, against seconds for the sample. A record bound that stops it, or an archive it cannot compare, signs covered: false, which is never a pass.";

export const COVERAGE_BOUND_HELP =
  "The most archived records the complete verification may decode. Past it, the partitions left are not compared and the run signs covered: false. Leave it empty for no bound beyond the deadline.";

export const TIME_BASIS_NOTE =
  "Ticked, the plan states restore.time_basis: producerTime. The archive holds each record's producer timestamp; for a LogAppendTime topic the runner refuses a point in time (PointInTimeByProducerTime) unless this is ticked, and then restores that topic by its producers' clocks and the signed scorecard says so. This page cannot see each topic's timestamp type.";

export const TIME_BASIS_OPTED_WARNING =
  "This plan accepts a point in time read on the producers' clocks: for a LogAppendTime topic the restore takes the records whose producer timestamps are at or before the point, not the ones the broker had appended by then.";

export const RESTORE_SEMANTICS_SENTENCE =
  "Restores copy the archive as written: an archive holding transaction markers is refused unless the plan allows them, and under that allowance aborted transactions and markers are restored as ordinary records, LogAppendTime timestamps come back as producer CreateTime, repeated header keys keep one copy, and when timestamps are out of order a point-in-time restore can miss records.";

export const READINESS_CAVEAT_SENTENCE =
  "A ready verdict says these checks passed when they ran. A credential can be rotated, a topic created or an ACL changed a minute later; the runner's own guards remain the authority.";

export const READINESS_NOT_RUN_WARNING =
  "No readiness check can run here, so nothing has looked for an existing target topic with a mapped name, for a reachable target, or at whether the brokers hold the replication factor. The restore may still be created: the runner's phase 0 refuses a mapped topic that exists, and nothing is overwritten either way.";

export const REPLICATION_DIFFERS_NOTE =
  "This factor can differ from the source's, and the target's storage follows it: a topic the source kept at factor 1, restored at 3, takes three times the storage; a factor below the source's keeps fewer copies than the source had.";

export const REPLICATION_UNKNOWN_WARNING =
  "This plan asks for 1 replica because neither the source's replication factor nor the target's broker count is known: losing that broker loses the restored topic. Set the factor, or run Discover topics on the target connection.";

export const CATALOG_BINDING_SENTENCE =
  "This plan is bound to the recovery point itself (source.point): the runner re-reads that receipt and the manifest it attests before it contacts any broker, and refuses a mismatch (PointBindingMismatch).";

export const POINT_UNBOUND_SENTENCE =
  "This plan is not bound to a recovery point: it names the backup set only, so the run reads no backup receipt and its signed scorecard reports each topic's timestamp type and configuration as not recorded.";

export const CATALOG_TOPICS_SENTENCE =
  "Name the topics to restore, separated by commas. The readiness check reads the point's manifest for exactly these names, and the runner restores nothing the backup set does not hold.";

export const SCRATCH_MARKER_WARNING =
  "This connection declares no marker topic, and a scratch restore is refused at phase 0 unless the target proves it is scratch by carrying one.";

export const SCRATCH_SENTENCE =
  "A scratch restore is a rehearsal: the runner deletes the topics it created when the run ends. It is refused unless the target is among the trust policy's allowed clusters, differs from the source and carries its marker topic. A scratch restore is never under the original names.";

// ------------------------------------------------------- the plan, read back

/** One plan line without its comment: a `#` at the start or after whitespace, outside quotes. */
function withoutComment(line: string): string {
  let quote: string | null = null;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i]!;
    if (quote !== null) {
      if (ch === quote) quote = null;
    } else if (ch === '"' || ch === "'") {
      quote = ch;
    } else if (ch === "#" && (i === 0 || /\s/.test(line[i - 1]!))) {
      return line.slice(0, i);
    }
  }
  return line;
}

/** The scalar `key` directly inside the top-level `block` of the plan: a tolerant line
 *  reader, not a YAML parser (block or flow mapping, quoted or bare). Null when not found. */
export function planBlockValue(planBytes: string | null | undefined, block: string, key: string): string | null {
  if (!planBytes) return null;
  const lines = planBytes.split("\n").map((raw) => withoutComment(raw.replace(/\r$/, "")));
  const valueIn = (text: string) => {
    const quoted = /^"([^"]*)"/.exec(text) ?? /^'([^']*)'/.exec(text);
    return quoted ? quoted[1]! : (/^[^,}\s]*/.exec(text)?.[0] ?? "");
  };
  const head = new RegExp(`^${block}\\s*:\\s*(.*)$`);
  const keyAt = new RegExp(`^\\s*${key}\\s*:\\s*(.*)$`);
  for (let i = 0; i < lines.length; i += 1) {
    const top = head.exec(lines[i]!);
    if (!top) continue;
    const rest = top[1]!.trim();
    if (rest.startsWith("{")) {
      let flow = rest;
      for (let j = i + 1; !flow.includes("}") && j < lines.length; j += 1) flow += ` ${lines[j]!.trim()}`;
      const m = new RegExp(`(?:^|[{,\\s])${key}\\s*:\\s*(.*)$`).exec(flow);
      return m ? valueIn(m[1]!.trim()) : null;
    }
    let indent: number | null = null;
    for (let j = i + 1; j < lines.length; j += 1) {
      const line = lines[j]!;
      if (!line.trim()) continue;
      const lead = /^\s*/.exec(line)![0].length;
      if (lead === 0) break;
      indent ??= lead;
      if (lead !== indent) continue;
      const m = keyAt.exec(line);
      if (m) return valueIn(m[1]!.trim());
    }
    return null;
  }
  return null;
}

export function planTimeBasis(planBytes: string | null | undefined): string {
  return planBlockValue(planBytes, "restore", "time_basis") === "producerTime" ? "producerTime" : "";
}

/** A bucket name or nothing: plan text goes into a shell command, so only the S3 grammar passes. */
export function planEvidenceBucket(planBytes: string | null | undefined): string {
  const b = planBlockValue(planBytes, "evidence", "bucket");
  return b !== null && /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(b) ? b : "";
}

export function planEvidenceRoute(planBytes: string | null | undefined) {
  const v = (k: string) => {
    const x = planBlockValue(planBytes, "evidence", k);
    return x === null || x === "null" || x === "~" ? "" : x;
  };
  return { endpoint: v("endpoint"), region: v("region"), pathStyle: v("path_style") === "true" };
}


/** The commands an operator runs to fetch and verify the signed scorecard on their own machine. */
export function independentCheck(bucket: string, scorecardKey: string, sidecarKey: string, route: ReturnType<typeof planEvidenceRoute>): string[] {
  // Plan and server strings enter only through shellArg/s3Arg: shaped and quoted, or a placeholder.
  const flags = `${route.endpoint ? ` --endpoint-url ${shellArg(route.endpoint, "url", "<endpoint>")}` : ""}${route.region ? ` --region ${shellArg(route.region, "region", "<region>")}` : ""}`;
  return [
    `aws s3 cp ${s3Arg(bucket, scorecardKey)} ./scorecard.json${flags}`,
    `aws s3 cp ${s3Arg(bucket, sidecarKey)} ./scorecard.sig${flags}`,
    "logweir drill verify --payload-type scorecard --scorecard ./scorecard.json --signature ./scorecard.sig --public-key <your key>",
    "python3 verify_scorecard.py --payload-type scorecard ./scorecard.json ./scorecard.sig <your key>",
  ];
}

/** The signed time basis in words; absent is not recorded, never "every topic used its own clock". */
export function signedTimeBasisText(tb: S["RestoreTimeBasisView"] | null | undefined): string {
  if (!tb) return "Not recorded: this run carries no signed time basis.";
  const parts = [
    ...(tb.producerTime.length ? [`selected by producer time (the plan accepted it): ${tb.producerTime.join(", ")}`] : []),
    ...(tb.notRecorded.length ? [`selected by time with the timestamp type NOT RECORDED: ${tb.notRecorded.join(", ")}`] : []),
  ];
  return parts.length ? parts.join("; ") : "no topic was selected by producer time or with an unrecorded timestamp type";
}
