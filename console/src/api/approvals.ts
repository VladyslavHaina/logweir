// Approvals (OD-8): the namespace's approval policy, the Approval records, a
// two-person request as the second person is shown it, and the three ways an
// approval is given here: the second person's click, a strict countersignature
// made with a personal key, and an unbound namespace's two `logweir drill
// approve` files. Under `confirm` the requester's own confirmation in the
// wizard is the whole authorization, and nothing is given here.
//
// THIS PAGE SIGNS NOTHING AND HOLDS NO KEY. The click sends one value, the hash
// of the request the server showed; the countersignature and the v1 files are
// sent as the text the approver's own CLI wrote. Text that is, or is named like,
// a private key is refused before anything is sent (`refuseKeyMaterial`).
//
// EVERY RULE HERE DECIDES WHAT IS DRAWN, NEVER WHAT IS ALLOWED: the API and the
// controller check each one again.
import { useMemo } from "react";
import { useMutation, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, LocalRefusal as Refusal, api, call, listAll, LIST_PAGE_SIZE, outcomeUnknown } from "./client";
import { keys, useRestores } from "./queries";
import { useCan, useSessionDoc } from "./session";
import type { components } from "./schema";
import type { Restore, Session } from "./types";
import type { NavCount } from "../app/attention";
import { sha256 } from "../components/hash";
import { duration } from "../components/When";

type S = components["schemas"];
export type Approval = S["Approval"];
export type ApprovalPolicy = S["ApprovalPolicyView"];
export type ApprovalRequest = S["ApprovalRequestView"];
export type ApprovalScope = S["ApprovalScopeView"];
export type ApprovalPacket = S["ApprovalPacket"];
export type ScopeOwnerStatement = S["ScopeOwnerStatementView"];
export type FieldError = S["FieldError"];

/** The one subject kind this page approves. The kind is part of what an approval
 *  binds. Theme A's `Switchover` approvals (R7) add their own kind and page. */
export const SUBJECT_KIND = "Restore";

// ------------------------------------------------------------------ grants

/** What this session may do with approvals in one namespace, from GET /session. */
export interface ApprovalGrant {
  /** False until the session has answered. */
  known: boolean;
  approvalsRead: boolean;
  approvalSubmit: boolean;
  approvalPacketRead: boolean;
  restoresRead: boolean;
  /** `<issuer>#<subject>` of the signed-in actor. */
  actorId: string;
  displayName: string;
}

export function approvalGrant(session: Session | undefined, ns: string): ApprovalGrant {
  const grant = session?.namespaces.find((g) => g.name === ns);
  const can = grant?.capabilities;
  return {
    known: session !== undefined,
    approvalsRead: can?.approvalsRead === true,
    approvalSubmit: can?.approvalSubmit === true,
    approvalPacketRead: can?.approvalPacketRead === true,
    restoresRead: can?.restoresRead === true,
    actorId: session?.actor.id ?? "",
    displayName: session?.actor.displayName ?? "",
  };
}

/** The approval flags in `ns`, from the gate's session. */
export function useApprovalGrant(ns: string): ApprovalGrant {
  return approvalGrant(useSessionDoc(), ns);
}

// ------------------------------------------------------------------ modes

/** OD-8's modes as this page acts on them. `files` is an unbound namespace
 *  (`legacy-governed-v1`): the approver records the two files `logweir drill
 *  approve` wrote. */
export type ApprovalMode = "confirm" | "twoPerson" | "strict" | "files";

/**
 * The mode a policy takes its approval in. A Governed policy whose operator word
 * this page does not know reads as strict, never as two-person: a word nobody
 * recognises never turns a key-based approval into a click. A policy that is
 * not an explicit binding (legacy, or a mode word unknown here) keeps the v1
 * files, as the old console did.
 */
export function approvalMode(policy: ApprovalPolicy): ApprovalMode {
  if (policy.legacy !== false) return "files";
  if (policy.mode === "ordinary") return "confirm";
  if (policy.mode === "governed") return policy.operatorMode === "two-person" ? "twoPerson" : "strict";
  return "files";
}

/** The mode in a few words, for headers and summaries. */
export const MODE_WORDS: Record<ApprovalMode, string> = {
  confirm: "One-person confirmation",
  twoPerson: "Two-person approval",
  strict: "Strict: personal-key countersignature",
  files: "Unbound: the approver's own files",
};

/** What approving means in this namespace, in one or two sentences. */
export function modeSentence(mode: ApprovalMode, policyName: string): string {
  switch (mode) {
    case "confirm":
      return `Policy ${policyName} is confirm: creating a restore in the wizard is the confirmation. No key is involved, and nothing waits for an approver here.`;
    case "twoPerson":
      return `Policy ${policyName} is two-person: a second person, signed in as an approver and not the requester, approves each restore here. No key is involved.`;
    case "strict":
      return `Policy ${policyName} is strict: an approver other than the requester countersigns each restore with their personal key, using logweir drill countersign.`;
    case "files":
      return `No approval policy binds this namespace (${policyName}): an approver records the two files logweir drill approve wrote on their own machine.`;
  }
}

/**
 * THE EMPTY APPROVALS TABLE, IN THE WORDS OF THE NAMESPACE'S POLICY (the old
 * console's sentences): under confirm the approval is the console's own signed
 * confirmation; under two-person a second person approves in the console;
 * under strict an approver countersigns; only an unbound namespace records files.
 */
export function noApprovalSentence(mode: ApprovalMode): string {
  switch (mode) {
    case "confirm":
      return "No Approval in this namespace yet. Its approval mode is confirm: creating a Restore in the wizard is the confirmation, no key is involved, and nothing is recorded here by hand.";
    case "twoPerson":
      return "No Approval in this namespace yet. Its approval mode is two-person: a second person, signed in to this console as an approver and not the requester, opens a Restore waiting above and approves it there. No key is involved.";
    case "strict":
      return "No Approval in this namespace yet. Its approval mode is strict: an approver other than the requester opens a Restore waiting above and records the countersignature there.";
    case "files":
      return "No Approval in this namespace yet. Choose a Restore waiting for one above, and record it there from the two files logweir drill approve wrote on the approver's own machine.";
  }
}

// ------------------------------------------------------- key material

/** The refusal, verbatim (the old console's `PRIVATE_KEY_REFUSAL`). */
export const PRIVATE_KEY_REFUSAL = "this page never accepts a private key";

/** The words that open a private-key PEM, in any case and across any spacing,
 *  or a PEM BEGIN line joining them with a dash or an underscore. */
const KEY_MARKER = /private\s+key|-{3,}\s*begin[^\n]{0,40}?private[\s_-]*key/i;

/** The names OpenSSL, OpenSSH, Java and PuTTY write a key under. */
const KEY_SUFFIXES = [".pem", ".key", ".p8", ".p12", ".pfx", ".jks", ".ppk"];
/** `ssh-keygen`'s default names, matched as a prefix of the file's own name. */
const KEY_NAME_PREFIX = "id_";

export function carriesKeyMaterial(text: unknown): boolean {
  return typeof text === "string" && KEY_MARKER.test(text);
}

/**
 * The refusal, or null. The name rule catches a key that spells nothing (a DER
 * or PKCS#12 blob, a header-stripped body); the content rule catches a pasted
 * PEM. A renamed, headerless blob is caught by neither: the API refuses it as a
 * document that is not a sidecar.
 */
export function refuseKeyMaterial(fileName: string, text: string): string | null {
  const name = fileName.toLowerCase();
  const base = name.slice(name.lastIndexOf("/") + 1);
  if (KEY_SUFFIXES.some((suffix) => name.endsWith(suffix))) return PRIVATE_KEY_REFUSAL;
  if (base.startsWith(KEY_NAME_PREFIX)) return PRIVATE_KEY_REFUSAL;
  if (carriesKeyMaterial(text)) return PRIVATE_KEY_REFUSAL;
  return null;
}

// ------------------------------------------------------- the subject

/** What an approval for one Restore must name, read from the Restore itself. */
export interface Subject {
  name: string;
  uid: string;
  namespace: string;
  /** The sha256 of the Restore's plan bytes, or the service's own when the read carried none. */
  planHash: string;
  approvalName: string;
}

export function subjectOf(restore: Restore, planHash: string): Subject {
  return {
    name: restore.name,
    uid: restore.uid,
    namespace: restore.namespace,
    planHash,
    approvalName: restore.approvalRef.name,
  };
}

/** What a link to this page claimed (`?hash=`, `?approval=`); the page refuses one that disagrees. */
export interface RouteClaim {
  hash?: string;
  approval?: string;
}

export interface Mismatch {
  field: string;
  claimed: string;
  actual: string;
}

/** Where a link disagrees with the Restore it names. Empty when they agree or it claims nothing. */
export function routeMismatches(claim: RouteClaim, subject: Subject): Mismatch[] {
  const out: Mismatch[] = [];
  if (claim.hash && claim.hash !== subject.planHash) out.push({ field: "plan hash", claimed: claim.hash, actual: subject.planHash });
  if (claim.approval && claim.approval !== subject.approvalName) {
    out.push({ field: "Approval name", claimed: claim.approval, actual: subject.approvalName });
  }
  return out;
}

/** A restore that has neither run nor been refused: the ones an approval is still for. */
export function awaitingApproval(restore: Restore): boolean {
  return restore.operation.state === "pending";
}

/** A Pending Restore whose approval the controller has not verified yet. That is
 *  the first state of every Restore, a confirm-mode one included for the seconds
 *  its confirmation takes to verify, so it reads as waiting and not as a refusal. */
export const WAITING_FOR_APPROVAL_SENTENCE =
  "Waiting for approval: no Job runs until the controller verifies its Approval.";

/** A Restore's own progress, as the controller recorded it. */
export function restoreProgressSentence(restore: Restore): string {
  const { state, stateReason } = restore.operation;
  if (state === "pending" && stateReason === "ApprovalNotVerified") return WAITING_FOR_APPROVAL_SENTENCE;
  if (state === "pending" && !stateReason) return "Pending: the controller has not admitted it yet.";
  if (state === "failed" || state === "refused") {
    return `${state === "failed" ? "Failed" : "Refused"}, reason ${stateReason ?? "not recorded"}. A Restore's spec is immutable; a refused Restore is never retried under this name.`;
  }
  return `${state[0]!.toUpperCase()}${state.slice(1)}${stateReason ? `, reason ${stateReason}` : ""}.`;
}

// ------------------------------------------------------- an Approval's state

/** The two `Verified` reasons on a CONSUMED Approval one of whose keys was later
 *  revoked for KeyCompromise. Neither is a pass, and neither is ever green. */
export const COMPROMISE_REASONS: readonly string[] = ["RecordedBeforeRevocation", "KeyRevoked"];

export type ApprovalStateWord =
  | "absent"
  | "foreign-subject"
  | "foreign-execution"
  | "plan-mismatch"
  | "verified"
  | "revoked-after-use"
  | "expired"
  | "refused"
  | "awaiting-verification";

export interface Found {
  state: ApprovalStateWord;
  detail?: string;
  key?: string;
  reason?: string;
  message?: string;
  /** When the Restore was admitted under it (`Consumed=True`). */
  consumedAt?: string;
}

function condition(approval: Approval, type: string) {
  return approval.conditions.find((c) => c.type === type);
}

function consumedAt(approval: Approval): string | undefined {
  const c = condition(approval, "Consumed");
  return c && c.status === "True" ? (c.lastTransitionTime ?? "") : undefined;
}

/**
 * The controller's verdict on an Approval, from its own status and never from a
 * clock or a guess, without asking which Restore it is for (the list's view).
 */
export function recordState(approval: Approval): Found {
  const verified = condition(approval, "Verified");
  const consumed = consumedAt(approval);
  if (approval.verified === true && approval.verifiedSubject) {
    return { state: "verified", key: approval.matchedKeyId ?? "", ...(consumed !== undefined ? { consumedAt: consumed } : {}) };
  }
  if (verified && verified.status === "False" && consumed !== undefined && COMPROMISE_REASONS.includes(verified.reason ?? "")) {
    return {
      state: "revoked-after-use",
      reason: verified.reason ?? "",
      message: verified.message ?? "",
      key: approval.matchedKeyId ?? "",
      consumedAt: consumed,
    };
  }
  if (verified && verified.status === "False") {
    return { state: verified.reason === "KeyIdExpired" ? "expired" : "refused", reason: verified.reason ?? "", message: verified.message ?? "" };
  }
  return { state: "awaiting-verification" };
}

/**
 * Where one Approval stands for one Restore. The three states that bind another
 * subject, run or plan are checked before the verdict: a verdict about something
 * else is not a verdict about this Restore.
 */
export function approvalState(approval: Approval | null, subject: Subject): Found {
  if (approval === null) return { state: "absent" };
  if (approval.subjectRef.kind !== SUBJECT_KIND || approval.subjectRef.name !== subject.name) {
    return { state: "foreign-subject", detail: `${approval.subjectRef.kind}/${approval.subjectRef.name}` };
  }
  const bound = approval.verifiedSubject;
  if (bound && (bound.uid !== subject.uid || bound.name !== subject.name || bound.kind !== SUBJECT_KIND || bound.namespace !== subject.namespace)) {
    return { state: "foreign-execution", detail: bound.uid };
  }
  if (subject.planHash && approval.planHash !== subject.planHash) return { state: "plan-mismatch", detail: approval.planHash };
  return recordState(approval);
}

/** The few words a table cell says for each state. */
export const STATE_WORDS: Record<ApprovalStateWord, string> = {
  absent: "None recorded",
  "awaiting-verification": "Awaiting verification",
  verified: "Verified",
  "revoked-after-use": "Signer revoked after use",
  expired: "Expired",
  refused: "Refused",
  "foreign-subject": "Bound to another subject",
  "foreign-execution": "Bound to another run",
  "plan-mismatch": "Names another plan",
};

/** The badge words for a consumed Approval whose signer was revoked for
 *  compromise afterwards: not verified, not refused. */
export const REVOKED_AFTER_USE_WORDS = "signer revoked for compromise after use";

/** One sentence for a state, every value in it the server's text. */
export function stateSentence(found: Found, subject: Subject): string {
  const name = subject.approvalName;
  switch (found.state) {
    case "absent":
      return `No Approval named ${name} exists yet. The Restore holds at Pending (reason ApprovalNotVerified), creates no Job, and the controller looks again every 30 s.`;
    case "awaiting-verification":
      return `Approval ${name} is recorded and the controller has not verified it yet. The Restore runs only once it is verified for this Restore's UID.`;
    case "verified":
      return `The controller verified Approval ${name} against key ${found.key} for Restore ${subject.name} (uid ${subject.uid}).${
        found.consumedAt ? ` The Restore was admitted under it at ${found.consumedAt}; the verdict is now the record of that admission and authorises nothing else.` : ""
      }`;
    case "revoked-after-use":
      return `Restore ${subject.name} was admitted under Approval ${name} at ${found.consumedAt}, and a key it names was later revoked for KeyCompromise (reason ${found.reason}): ${found.message} This is never green: the record (key ${found.key}, the authorization, the admission) is kept so the run can be investigated.`;
    case "expired":
      return `The controller refused Approval ${name} because the approver key is past its notAfter (reason KeyIdExpired): ${found.message}`;
    case "refused":
      return `The controller refused Approval ${name} (reason ${found.reason}): ${found.message} The Restore does not run while this stands. A refusal about the roster can clear once the roster is fixed; a refusal about the documents cannot, because an Approval's spec is immutable.`;
    case "foreign-subject":
      return `Approval ${name} exists but names ${found.detail}, not this Restore. It cannot approve this Restore, and this page will not reuse it.`;
    case "foreign-execution":
      return `Approval ${name} was verified for an earlier object with this name (uid ${found.detail}), not for this Restore (uid ${subject.uid}). It is never rebound: this Restore needs a new plan, and so a new Restore and a new Approval.`;
    case "plan-mismatch":
      return `Approval ${name} names plan hash ${found.detail}, but this Restore's plan hashes to ${subject.planHash}. It does not describe this plan, and this page will not reuse it.`;
  }
}

/** `selfAttestedRisk` as a SENTENCE, never a bare boolean: `false` means only
 *  that the two keys differ, and one operator may hold both. */
export const SELF_ATTESTED_TRUE =
  'self-attested: the approver key and the signing key are the same; "not self-attested" means only two different keys, and one operator may hold both';
export const SELF_ATTESTED_FALSE = '"not self-attested" means only two different keys, and one operator may hold both';

export function selfAttestedSentence(value: boolean | null | undefined): string {
  if (value === true) return SELF_ATTESTED_TRUE;
  if (value === false) return SELF_ATTESTED_FALSE;
  return "self-attestation: not evaluated yet";
}

/** The approval subject in words; the original-name one distinct. */
export function approvalSubjectWords(subject: string | null | undefined): string {
  if (subject === "originalName") return "Original names: a restore under the ORIGINAL topic names";
  if (subject === "ordinary") return "Ordinary: a restore into new topic names";
  return "Unknown (not readable here)";
}

/**
 * OD-10's typed names: the original topic names a one-person confirmation's
 * SIGNED bytes carry (`originalNameConfirmation.typedTopics`), or null when the
 * document carries none or cannot be read. Read from the packet, never guessed.
 */
export function typedTopicsOf(packet: ApprovalPacket | null | undefined): string[] | null {
  if (!packet) return null;
  let doc: unknown;
  try {
    doc = JSON.parse(packet.approvalBytes);
  } catch {
    return null;
  }
  const typed = (doc as { originalNameConfirmation?: { typedTopics?: unknown } } | null)?.originalNameConfirmation?.typedTopics;
  return Array.isArray(typed) && typed.every((t) => typeof t === "string") ? (typed as string[]) : null;
}

/** "you (Alice Martin)" when `id` IS the signed-in actor's own principal, compared exactly. */
export function youOf(id: string | null | undefined, grant: Pick<ApprovalGrant, "actorId" | "displayName">): string | null {
  return id && grant.actorId && id === grant.actorId && grant.displayName ? `you (${grant.displayName})` : null;
}

// ------------------------------------------------------- the two-person request

const SHA256 = /^sha256:[0-9a-f]{64}$/;

/** The scope a request carries when the SERVER said it is complete and it is
 *  whole on arrival: a topic list as long as the count stated, and not empty. */
export function scopeShowable(request: ApprovalRequest | null | undefined): ApprovalScope | null {
  const scope = request?.scope;
  if (!request || request.scopeComplete !== true || !scope) return null;
  if (!Array.isArray(scope.topics) || scope.topics.length === 0 || scope.topics.length !== scope.topicsCount) return null;
  return scope;
}

export interface OfferView {
  policy: ApprovalPolicy | null | undefined;
  restoreExists: boolean;
  maySubmit: boolean;
  mismatches: Mismatch[];
  request: ApprovalRequest | null | undefined;
  subject: Subject | null;
}

/**
 * Whether the Approve button is drawn: the SERVER offered it to this session for
 * a PENDING request of THIS Restore (name and UID) named by a well-formed hash,
 * the session holds the approver capability, the link agrees with the Restore,
 * this console takes approvals under the namespace's policy, and the scope can
 * be shown whole. Each is checked again by the API on the click.
 */
export function consoleApprovalOffered(v: OfferView): boolean {
  if (v.policy?.consoleApprovalAvailable !== true || !v.policy || approvalMode(v.policy) !== "twoPerson") return false;
  if (!v.restoreExists || !v.maySubmit || v.mismatches.length > 0 || !v.subject) return false;
  const r = v.request;
  if (!r || r.state !== "pending" || r.approve?.offered !== true) return false;
  if (r.restore !== v.subject.name || r.restoreUid !== v.subject.uid) return false;
  if (scopeShowable(r) === null) return false;
  return typeof r.confirmationSha256 === "string" && SHA256.test(r.confirmationSha256);
}

/** How many requests wait for THIS session: pending, and offered to it by the server. */
export function waitingForYou(requests: (ApprovalRequest | null | undefined)[]): number {
  return requests.filter((r) => r?.state === "pending" && r.approve.offered === true && scopeShowable(r) !== null).length;
}

/** The navigation's Approvals count, or undefined when nothing waits for you. */
export function approvalsNavCount(n: number): NavCount | undefined {
  return n > 0 ? { n, tone: "brand", text: `${n} ${n === 1 ? "request waits" : "requests wait"} for you` } : undefined;
}

/** What the two-person panel says about an original-name restore. */
export const TWO_PERSON_ORIGINAL_NAME_SENTENCE =
  "This Restore writes under the ORIGINAL topic names, into topics that do not exist. The runner refuses again if a name exists, or if the target may be the source cluster and any broker auto-creates topics. Whether a declarative owner manages a name is decided by what the requester states below: approving accepts that statement.";

/** What an approver of an original-name restore is told outside two-person. */
export const ORIGINAL_NAME_APPROVAL_SENTENCE =
  "This Restore writes under the ORIGINAL topic names, into topics that do not exist. Only an approval signed for the approval subject originalName authorises it: an ordinary approval is refused before any Job exists. On the command line: logweir drill approve --approval-subject original-name. The runner refuses again if a name exists, if the target may be the source cluster and any broker auto-creates topics, or if a declarative owner manages a name.";

/** What an approver of a confirm namespace's original-name restore is told (OD-10). */
export const CONFIRM_ORIGINAL_NAME_SENTENCE =
  "This Restore writes under the ORIGINAL topic names, into topics that do not exist. Under confirm the requester re-typed every original topic name before confirming, and the console signed what was typed; the approval record below shows the names. The runner refuses again if a name exists, if the target may be the source cluster and any broker auto-creates topics, or if a declarative owner manages a name.";

/** The transaction-marker override, shown to the second person: it changes what the run writes. */
export const TRANSACTION_MARKERS_SENTENCE =
  "This plan restores transaction markers and the records of aborted transactions as ordinary data (restore.allow_transaction_markers: true).";

/** The requester's owner statement in the four cases the plan can sign. */
export type OwnerCase = "nothingStated" | "ownerPath" | "noOwner" | "ownersWithoutPath";

export function ownerCase(statement: ScopeOwnerStatement | null | undefined): OwnerCase {
  const owners = statement?.owners;
  // Not declared reads as the run acts, WHATEVER owner_path says: from the console
  // the run has nowhere to look and refuses.
  if (owners === null || owners === undefined) return "nothingStated";
  if (statement?.ownerPath === true) return "ownerPath";
  return owners.length === 0 ? "noOwner" : "ownersWithoutPath";
}

export const OWNER_WORDS: Record<OwnerCase, string> = {
  nothingStated:
    "The requester states nothing about declarative owners of these names. This console gives the run nowhere else to look, so the run will refuse before it writes anything.",
  ownerPath:
    "Restore despite an owner (owner_path): the requester states that each declarative owner's reconciliation is paused for this restore and that it adopts the topic afterwards; the restore writes under these names although an owner manages them.",
  noOwner:
    "The requester states that no declarative owner (a Strimzi KafkaTopic, a GitOps or Terraform definition) manages any of these names. Nothing in the console checks this statement: approving accepts it.",
  ownersWithoutPath: "The requester states these declarative owners and does not set owner_path, so the run will refuse before it writes anything.",
};

/** An instant to the millisecond the run uses: whole seconds as they are, any other with its milliseconds, in UTC. */
export function instantText(value: string | null | undefined): string {
  const at = value ? Date.parse(value) : NaN;
  if (!Number.isFinite(at)) return "Not recorded";
  const iso = new Date(at).toISOString();
  return `${iso.slice(0, 10)} ${iso.slice(11, at % 1000 === 0 ? 19 : 23)} UTC`;
}

/** What happens to the topics afterwards, as the runner acts (a newTopic restore never deletes;
 *  a clone's topics are deleted at its expiry, `teardown: ttl` with `teardownAfterSeconds`). */
export function afterwardsWords(mode: string, teardown: string, ttlSeconds?: number | null): string {
  if (mode === "scratch" && (teardown === "ttl" || (typeof ttlSeconds === "number" && ttlSeconds > 0))) {
    const when = typeof ttlSeconds === "number" && ttlSeconds > 0 ? `${duration(ttlSeconds)} after the run` : "when the clone expires";
    return `A clone: the topics this run creates are kept, then deleted ${when}. A run that does not pass deletes them at once.`;
  }
  if (mode === "scratch" && teardown === "delete") return "The topics this run creates are deleted.";
  if (mode === "newTopic") return "The topics this run creates stay on the target cluster: a newTopic restore never deletes them (teardown applies to scratch drills only).";
  return `Nothing is deleted (teardown ${teardown}).`;
}

/** The plan's topic configuration choice (`target.topic_configuration`), as the approver signs it. */
export function topicConfigurationWords(tc: S["ScopeTopicConfigurationView"] | null | undefined): string {
  if (!tc) return "The target's defaults: topics are created as a restore always did.";
  const settings =
    tc.transition === "afterVerification"
      ? "created from the recovery point's configuration; the settings held during the restore are set to the source's once it passes verification"
      : tc.transition === "none"
        ? "created from the recovery point's configuration; the source's held settings are reported, not applied"
        : `created from the recovery point's configuration (transition ${tc.transition})`;
  const owners = !tc.owners
    ? ""
    : tc.owners.length === 0
      ? " The requester states that no topic has a declarative owner."
      : ` Declared owners: ${tc.owners.map((o) => `${o.topic} (${o.kind} ${o.reference})`).join(", ")}.`;
  return `The source's: ${settings}.${owners}`;
}

// ------------------------------------------------------- reads

/** A read whose 404 is an answer (nothing there), not an error. */
async function orNull<T>(read: () => Promise<T>): Promise<T | null> {
  try {
    return await read();
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}

/** R1's keys: [kind, ns] for the list and [kind, ns, name] for one; the reads that
 *  are not an Approval or a Restore each have a kind of their own. */
export const approvalKeys = {
  policy: (ns: string) => ["approval-policy", ns] as const,
  list: (ns: string) => keys.list("approvals", ns),
  one: (ns: string, name: string) => keys.item("approvals", ns, name),
  packet: (ns: string, name: string) => ["approval-packets", ns, name] as const,
  request: (ns: string, restore: string) => ["approval-requests", ns, restore] as const,
  /** A Restore as the approval page reads it: with its plan bytes, and null when absent. */
  subject: (ns: string, name: string) => ["approval-subjects", ns, name] as const,
};

export function useApprovalPolicy(ns: string, enabled = true) {
  return useQuery({
    queryKey: approvalKeys.policy(ns),
    enabled,
    queryFn: async ({ signal }) =>
      // The OpenAPI document declares a `name` path parameter this route does not have;
      // it fills no placeholder, so the cast only satisfies the generated type.
      (await call(() => api.GET("/api/v1/namespaces/{ns}/approval-policy", { params: { path: { ns } as { ns: string; name: string } }, signal }))).item,
  });
}

export function useApprovals(ns: string, enabled = true) {
  const readable = useCan(ns, "approvalsRead");
  return useQuery({
    queryKey: approvalKeys.list(ns),
    enabled: enabled && readable,
    queryFn: ({ signal }) =>
      listAll("Approvals", (cursor) =>
        call(() =>
          api.GET("/api/v1/namespaces/{ns}/approvals", { params: { path: { ns }, query: { limit: LIST_PAGE_SIZE, cursor } }, signal }),
        ),
      ),
    // A recorded approval the controller has not decided yet is re-read until it has.
    refetchInterval: (q) => (q.state.data?.some((a) => recordState(a).state === "awaiting-verification") ? 3000 : false),
  });
}

/** Every Restore in the namespace that still waits for an approval: the Restores list
 *  (one cache with the Restores page), filtered. */
export function useWaitingRestores(ns: string, enabled = true) {
  const all = useRestores(ns, enabled);
  const waiting = useMemo(() => all.data?.filter(awaitingApproval), [all.data]);
  return { ...all, data: waiting } as typeof all;
}

/** One Restore, with its plan bytes; null when it does not exist. */
export function useSubjectRestore(ns: string, name: string, enabled = true) {
  return useQuery({
    queryKey: approvalKeys.subject(ns, name),
    enabled,
    queryFn: ({ signal }) =>
      orNull(async () => (await call(() => api.GET("/api/v1/namespaces/{ns}/restores/{name}", { params: { path: { ns, name } }, signal }))).item),
    refetchInterval: (q) => (q.state.data && !q.state.data.operation.terminal ? 3000 : false),
  });
}

/** One Approval's metadata; null when none has that name. */
export function useApproval(ns: string, name: string, enabled = true) {
  const readable = useCan(ns, "approvalsRead");
  return useQuery({
    queryKey: approvalKeys.one(ns, name),
    enabled: enabled && readable && name.length > 0,
    queryFn: ({ signal }) =>
      orNull(async () => (await call(() => api.GET("/api/v1/namespaces/{ns}/approvals/{name}", { params: { path: { ns, name } }, signal }))).item),
    refetchInterval: (q) => (q.state.data && recordState(q.state.data).state === "awaiting-verification" ? 2000 : false),
  });
}

/** An Approval's two documents, verbatim; null when none has that name. */
export function useApprovalPacket(ns: string, name: string, enabled = true) {
  const readable = useCan(ns, "approvalPacketRead");
  return useQuery({
    queryKey: approvalKeys.packet(ns, name),
    enabled: enabled && readable && name.length > 0,
    queryFn: ({ signal }) =>
      orNull(
        async () =>
          (await call(() => api.GET("/api/v1/namespaces/{ns}/approvals/{name}/packet", { params: { path: { ns, name } }, signal }))).item,
      ),
    staleTime: Infinity,
  });
}

async function readRequest(ns: string, name: string, signal?: AbortSignal): Promise<ApprovalRequest> {
  return (await call(() => api.GET("/api/v1/namespaces/{ns}/restores/{name}/approval-request", { params: { path: { ns, name } }, signal }))).item;
}

/** A two-person request as the second person is shown it. */
export function useApprovalRequest(ns: string, restore: string, enabled = true) {
  return useQuery({
    queryKey: approvalKeys.request(ns, restore),
    enabled,
    queryFn: ({ signal }) => readRequest(ns, restore, signal),
  });
}

/** The requests of several Restores, for the list and the navigation's count. */
export function useApprovalRequests(ns: string, restores: string[], enabled = true) {
  return useQueries({
    queries: restores.map((name) => ({
      queryKey: approvalKeys.request(ns, name),
      enabled,
      queryFn: ({ signal }: { signal: AbortSignal }) => readRequest(ns, name, signal),
    })),
  });
}

/**
 * The navigation's Approvals count: requests waiting for THIS session. Only a
 * two-person namespace on a console that takes approvals has any (record §4.1):
 * under confirm nothing waits for anyone, and a strict request has no
 * server-side "offered to you" view, so it is listed on the page and not counted.
 * R1 wires it into the Shell: `approvals: useApprovalsNavCount(ns)` in useNavCounts.
 */
export function useApprovalsNavCount(ns: string): NavCount | undefined {
  const grant = useApprovalGrant(ns);
  const policy = useApprovalPolicy(ns, grant.approvalsRead);
  const twoPerson = Boolean(policy.data && approvalMode(policy.data) === "twoPerson" && policy.data.consoleApprovalAvailable);
  const restores = useWaitingRestores(ns, twoPerson && grant.restoresRead);
  const requests = useApprovalRequests(
    ns,
    (restores.data ?? []).map((r) => r.name),
    twoPerson,
  );
  return twoPerson ? approvalsNavCount(waitingForYou(requests.map((q) => q.data))) : undefined;
}

// ------------------------------------------------------- writes

/** Nothing was sent: this page refused before any request, naming the fields it is about. */
export class LocalRefusal extends Refusal {
  readonly fields: ("approvalBytes" | "sidecarBytes")[];
  constructor(message: string, fields: ("approvalBytes" | "sidecarBytes")[] = []) {
    super(message);
    this.fields = fields;
  }
}

/**
 * What became of a write:
 * - `unknown`: no answer reached the page, or a gateway gave up; it may or may
 *   not be recorded, and submitting the same again is safe (the API answers a
 *   replay with the object it stored);
 * - `invalid`: the API refused fields, placed beside them;
 * - `local`: nothing was sent;
 * - `refused`: the API answered with a refusal, shown as one.
 */
export type Outcome = "unknown" | "invalid" | "local" | "refused";

export function outcomeOf(error: unknown): Outcome {
  if (error instanceof Refusal) return "local";
  if (outcomeUnknown(error) || (error instanceof ApiError && error.code === "upstream_timeout")) return "unknown";
  if (error instanceof ApiError && error.fieldErrors.length > 0) return "invalid";
  return "refused";
}

export const UNKNOWN_OUTCOME_SENTENCE =
  "No answer reached this page, so the approval may or may not be recorded. Submitting again is safe: the same submission is recognised rather than recorded twice.";

/** Each API field error beside the form field it is about, or listed when no field claims it. */
export function placeFieldErrors<F extends string>(error: unknown, fields: readonly F[]): { fields: Partial<Record<F, string>>; unmatched: string[] } {
  const placed: Partial<Record<F, string>> = {};
  const unmatched: string[] = [];
  for (const e of error instanceof ApiError ? error.fieldErrors : []) {
    const field = fields.find((f) => e.field === f || e.field === `spec.${f}`);
    if (field === undefined) unmatched.push(`${e.field}: ${e.message}`);
    else placed[field] = placed[field] ? `${placed[field]} ${e.message}` : e.message;
  }
  return { fields: placed, unmatched };
}

/** The `Origin` the API checks on every unsafe request. The browser sets it; the type names it. */
function originHeader() {
  return { Origin: window.location.origin };
}

export interface Written {
  approval: Approval;
  /** The API answered a replay with the object it already stored (HTTP 200). */
  replayed: boolean;
}

/** The second person's click: ONE value, the hash of the request that was shown. */
export async function sendConsoleApproval(ns: string, view: OfferView): Promise<Written> {
  if (!consoleApprovalOffered(view) || !view.request || !view.subject) {
    throw new LocalRefusal("This session is not offered an approval for this request; nothing was sent.");
  }
  const { subject, request } = view;
  const answer = await call(() =>
    api.POST("/api/v1/namespaces/{ns}/restores/{name}/console-approval", {
      params: { path: { ns, name: subject.name }, header: originHeader() },
      body: { confirmationSha256: request.confirmationSha256! },
    }),
  );
  return { approval: answer.item, replayed: answer.replayed === true };
}

/** The hash an approval binds: the sha256 of the plan bytes, computed here, or the service's own when the read carried none. */
export async function planHashOf(restore: Restore): Promise<string> {
  return typeof restore.planBytes === "string" ? sha256(restore.planBytes) : restore.planHash;
}

/**
 * The Restore must still be the object the page showed: the same UID, the same
 * approvalRef and the same plan, re-read and re-hashed now. Nothing is sent otherwise.
 */
export async function assertUnchanged(ns: string, expected: Subject): Promise<void> {
  const current = await orNull(
    async () => (await call(() => api.GET("/api/v1/namespaces/{ns}/restores/{name}", { params: { path: { ns, name: expected.name } } }))).item,
  );
  if (!current) throw new LocalRefusal(`Restore ${expected.name} no longer exists in this namespace; nothing was sent.`);
  const now = subjectOf(current, await planHashOf(current));
  for (const field of ["uid", "approvalName", "planHash"] as const) {
    if (now[field] !== expected[field]) {
      throw new LocalRefusal(
        `Restore ${expected.name} changed since this page read it (${field} was ${expected[field]}, is now ${now[field]}); reload its approval page. Nothing was sent.`,
      );
    }
  }
}

/** A strict approver's countersigned sidecar, sent as the text the CLI wrote. */
export async function sendCountersignature(ns: string, expected: Subject, sidecar: { text: string; fileName: string }): Promise<Written> {
  if (refuseKeyMaterial(sidecar.fileName, sidecar.text) !== null) throw new LocalRefusal(PRIVATE_KEY_REFUSAL, ["sidecarBytes"]);
  if (sidecar.text.trim().length === 0) throw new LocalRefusal("Paste the sidecar logweir drill countersign wrote, or choose its file.", ["sidecarBytes"]);
  await assertUnchanged(ns, expected);
  const restore = expected.name;
  const answer = await call(() =>
    api.POST("/api/v1/namespaces/{ns}/restores/{name}/approval", {
      params: { path: { ns, name: restore }, header: originHeader() },
      body: { sidecarBytes: sidecar.text },
    }),
  );
  return { approval: answer.item, replayed: answer.replayed === true };
}

export interface ApprovalFiles {
  approvalBytes: string;
  sidecarBytes: string;
  approvalFileName: string;
  sidecarFileName: string;
}

/** Which of the two documents carry key material, by field. */
export function keyMaterialFields(files: ApprovalFiles): ("approvalBytes" | "sidecarBytes")[] {
  const out: ("approvalBytes" | "sidecarBytes")[] = [];
  if (refuseKeyMaterial(files.approvalFileName, files.approvalBytes) !== null) out.push("approvalBytes");
  if (refuseKeyMaterial(files.sidecarFileName, files.sidecarBytes) !== null) out.push("sidecarBytes");
  return out;
}

/**
 * An unbound namespace's two files, byte for byte (never trimmed, parsed or
 * re-encoded: the controller hashes the bytes the approver signed).
 */
export async function sendApprovalFiles(ns: string, expected: Subject, files: ApprovalFiles): Promise<Written> {
  const keyed = keyMaterialFields(files);
  if (keyed.length > 0) throw new LocalRefusal(PRIVATE_KEY_REFUSAL, keyed);
  const missing: ("approvalBytes" | "sidecarBytes")[] = [];
  if (files.approvalBytes.length === 0) missing.push("approvalBytes");
  if (files.sidecarBytes.length === 0) missing.push("sidecarBytes");
  if (missing.length > 0) throw new LocalRefusal("Choose both files, or paste their text.", missing);
  await assertUnchanged(ns, expected);
  const restore = expected.name;
  const answer = await call(() =>
    api.POST("/api/v1/namespaces/{ns}/restores/{name}/approval", {
      params: { path: { ns, name: restore }, header: originHeader() },
      body: { approvalBytes: files.approvalBytes, sidecarBytes: files.sidecarBytes },
    }),
  );
  return { approval: answer.item, replayed: answer.replayed === true };
}

function useApprovalWrite<V>(ns: string, send: (v: V) => Promise<Written>) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: send,
    onSettled: () => {
      for (const kind of ["approvals", "approval-requests", "approval-subjects", "restores"]) {
        void client.invalidateQueries({ queryKey: keys.list(kind, ns) });
      }
    },
  });
}

export function useConsoleApproval(ns: string) {
  return useApprovalWrite(ns, (view: OfferView) => sendConsoleApproval(ns, view));
}

export function useCountersign(ns: string, expected: Subject) {
  return useApprovalWrite(ns, (sidecar: { text: string; fileName: string }) => sendCountersignature(ns, expected, sidecar));
}

export function useRecordApprovalFiles(ns: string, expected: Subject) {
  return useApprovalWrite(ns, (files: ApprovalFiles) => sendApprovalFiles(ns, expected, files));
}

/** The countersign command, verbatim (the words the old wizard and page showed). */
export const COUNTERSIGN_COMMAND =
  "logweir drill countersign --document <approvalBytes> --confirmation <sidecarBytes> --key <privkey> --out approval.sig";

/** The console's confirmation object for a governed request. */
export function confirmationNameFor(approvalName: string): string {
  return `${approvalName}-confirmation`;
}
