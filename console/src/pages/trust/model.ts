// Trust, read-only, and `unknown` is not `valid` (the old console's keys page).
//
// The page writes nothing: TrustPolicy objects are cluster-scoped, and the API
// serves no trust write in v1 (`trustAdministration: false`). A key's verdict is
// shown only for a FRESH evaluation, and freshness is the API's own decision
// against its own clock (`TrustPolicyView.evaluation`), never the browser's.
// Retirement and revocation are different: a retired key still verifies what
// it signed before; a key revoked for compromise gets no such courtesy.
import type { TrustKey, TrustPolicy } from "../../api/backups";
import { exactUtc } from "../../components/When";

const at = (v: string | null | undefined): string => (v ? exactUtc(v) : "an instant not recorded");

export const UNKNOWN = "unknown";

/** Why an evaluation reads `unknown`, by the API's own reason. */
export const UNKNOWN_REASONS: Record<string, string> = {
  notEvaluated: "Nothing has evaluated these keys yet: the policy carries no status.",
  noEvaluationTime: "Nothing has evaluated these keys yet: the policy records no evaluation time.",
  generationBehind: "The verdicts were computed from an earlier version of this policy (its observed generation is behind).",
  stale: "The last evaluation is older than the freshness window, measured against the server's own clock and never the browser's.",
  evaluated: "This key has no verdict in the policy's status.",
};

export const UNKNOWN_IS_NOT_VALID_SENTENCE =
  "A verdict that is not fresh reads unknown, never valid. Valid and expired are shown only for a verdict the controller computed from this version of the policy, recently enough to still be about it.";

export const KEYS_FORBIDDEN_SENTENCE =
  "Only administrators can view trust material. Your role does not include reading the installation's TrustPolicy objects; that says nothing about whether approvals and evidence verify, which the runs' own pages show.";

export const NO_TRUST_SENTENCE =
  "No TrustPolicy answered: none governs a namespace you administer, and the installation has no default. Until one exists, no approval can verify and no evidence can be checked against a key.";

export const CONFLICT_SENTENCE =
  "These namespaces are claimed by more than one TrustPolicy, so each of them resolves to no policy at all: every approval and every evidence verification in them is refused with TrustPolicyConflict rather than silently taking one policy's answer.";

export const FINGERPRINT_SENTENCE =
  "A row here says which key id the controller accepts. It cannot say that the material behind that id is still the one its holder has. Run this against the public half they give you and compare the digest with the key id.";

export const EDIT_SENTENCE =
  "This page shows the document and does not apply it. TrustPolicy objects are cluster-scoped and the product API serves no trust write. Save this as trustpolicy.yml and apply it yourself. Editing is monotonic: a key's public material can never be edited out, notAfter may only move earlier, and state may only move Active to Retired to Revoked.";

/** The document a cluster admin applies. Rendered, never submitted. */
export const POLICY_SNIPPET = [
  "# From an existing roster, the reviewable way across is a migration, not a rewrite:",
  "#   kubectl --context <ctx> get trustroster default -o json \\",
  "#     | logweir trust migrate-roster --stdin --name org-default --default > trustpolicy.yml",
  "",
  "apiVersion: logweir.dev/v1alpha1",
  "kind: TrustPolicy",
  "metadata:",
  "  name: org-default",
  "spec:",
  "  default: false",
  "  namespaces: [<the namespaces this policy governs>]",
  "  allowedTargetClusterIds: []",
  "  keys:",
  "    - keyId: <sha256 of the DER SPKI, lowercase hex>",
  "      spkiPem: |",
  "        -----BEGIN PUBLIC KEY-----",
  "        <the PUBLIC half; never a private key, here or anywhere>",
  "        -----END PUBLIC KEY-----",
  "      algorithm: p256",
  "      usages: [EvidenceSigning]",
  '      principal: {id: "install:<who holds it>", display: "<what it is>"}',
  '      notBefore: "2026-01-01T00:00:00Z"',
  '      notAfter: "2027-01-01T00:00:00Z"',
  "      state: Active",
  "",
  "kubectl --context <ctx> apply -f trustpolicy.yml",
].join("\n");

export function isFresh(p: Pick<TrustPolicy, "evaluation">): boolean {
  return p.evaluation?.state === "fresh";
}

/** Why a policy's evaluation is unknown, or null when it is fresh. */
export function unknownReason(p: Pick<TrustPolicy, "evaluation">): string | null {
  if (isFresh(p)) return null;
  return UNKNOWN_REASONS[String(p.evaluation?.reason)] ?? UNKNOWN_REASONS.notEvaluated!;
}

export type KeyEvaluation = { word: string; tone: "success" | "warning" | "danger" | "neutral"; reason?: string };

/** The evaluation cell: the controller's effective state only for a fresh evaluation; otherwise `unknown` and why. */
export function keyEvaluation(p: Pick<TrustPolicy, "evaluation">, k: Pick<TrustKey, "effectiveState">): KeyEvaluation {
  const why = unknownReason(p);
  if (why !== null) return { word: UNKNOWN, tone: "neutral", reason: why };
  const state = typeof k.effectiveState === "string" && k.effectiveState.length > 0 ? k.effectiveState : UNKNOWN;
  if (state === UNKNOWN) return { word: UNKNOWN, tone: "neutral", reason: UNKNOWN_REASONS.evaluated };
  if (state === "Active") return { word: state, tone: "success" };
  if (state === "Unparseable" || state === "Revoked") return { word: state, tone: "danger" };
  return { word: state, tone: "warning" };
}

/** What a key may still do, in words; `unknown` unless the evaluation is fresh. */
export function usabilityWords(p: Pick<TrustPolicy, "evaluation">, k: Pick<TrustKey, "usableForNewSignatures" | "usableForVerification">): string {
  if (!isFresh(p)) return UNKNOWN;
  const signing = k.usableForNewSignatures === true ? "May sign something new" : "May not sign anything new";
  const verification = typeof k.usableForVerification === "string" ? k.usableForVerification : UNKNOWN;
  return `${signing}; verification: ${verification}`;
}

/**
 * Retirement and revocation, per key, in the words that tell them apart. A fresh `Revoked` verdict
 * over a key this policy still declares alive says where the revocation lives, instead of vouching
 * for a compromised key. A verdict that is not fresh is not read at all.
 */
export function lifecycleWords(p: Pick<TrustPolicy, "evaluation">, k: TrustKey): string {
  const evaluated = isFresh(p) ? k.effectiveState : null;
  if (evaluated === "Revoked" && k.state !== "Revoked") {
    return `Revoked for compromise by another TrustPolicy's record. This policy still declares it ${k.state}, and nothing it signed or authorises is accepted here; record the revocation on this policy too, so an older controller agrees.`;
  }
  if (k.state === "Revoked") {
    const reason = k.revocationReason || "Unspecified";
    const from = at(k.revocationEffectiveFrom);
    return reason === "KeyCompromise"
      ? `Revoked for compromise, effective ${from}. Evidence it signed verifies only where a controller recorded seeing it before that instant; the document's own claimed signing time is not accepted.`
      : `Revoked (${reason}), effective ${from}. Treated as a retirement at that instant: what it signed before still verifies.`;
  }
  if (k.state === "Retired") {
    return `Retired at ${at(k.retiredAt)}. It authorises nothing new, and everything it signed before that instant still verifies.`;
  }
  return "Active: it may sign new documents inside its validity window.";
}

/** The compromise guard, when its condition is True: the reason, and whether it asks an administrator to act. */
export function compromiseGuard(p: Pick<TrustPolicy, "conditions">): { reason: string; message: string; act: boolean } | null {
  const c = (p.conditions ?? []).find((x) => x.type === "CompromiseGuard");
  if (!c || c.status !== "True") return null;
  const reason = c.reason ?? "";
  return { reason, message: c.message ?? "", act: reason !== "CompromiseRecorded" };
}
