// Trust, read-only, and unknown is not valid: ported from ui/tests/d3.spec.js
// (the keys view in console mode). Freshness is the API's decision; the page
// renders it and never decides it. Fixtures are the API's.
import { describe, expect, it } from "vitest";
import type { TrustPolicy } from "../../api/backups";
import { UNKNOWN, compromiseGuard, keyEvaluation, lifecycleWords, unknownReason, usabilityWords } from "./model";
import policyDoc from "../../../../ui/tests/fixtures/console/trust-policy.json";
import policyList from "../../../../ui/tests/fixtures/console/trust-policies-list.json";

const policy = (): TrustPolicy => JSON.parse(JSON.stringify(policyDoc.item)) as TrustPolicy;

describe("the evaluation", () => {
  it("renders the controller's own verdict for a fresh evaluation, retirement and revocation apart", () => {
    const p = policy();
    expect(unknownReason(p)).toBeNull();
    const [active, retired, compromised, superseded] = p.keys!;
    expect(keyEvaluation(p, active!)).toEqual({ word: "Active", tone: "success" });
    expect(keyEvaluation(p, retired!).word).toBe("Retired");
    expect(usabilityWords(p, active!)).toBe("May sign something new; verification: Full");
    expect(usabilityWords(p, retired!)).toBe("May not sign anything new; verification: Historical");
    expect(lifecycleWords(p, retired!)).toMatch(/authorises nothing new, and everything it signed before that instant still verifies/);
    expect(lifecycleWords(p, compromised!)).toMatch(/Revoked for compromise, effective 2026-07-25 00:00 UTC/);
    expect(lifecycleWords(p, compromised!)).toMatch(/the document's own claimed signing time is not accepted/);
    expect(lifecycleWords(p, superseded!)).toMatch(/Treated as a retirement at that instant/);
  });

  it("reads unknown, with the API's reason, for every key of an evaluation that is not fresh", () => {
    const stale = policy();
    stale.evaluation = { ...stale.evaluation, state: "unknown", reason: "stale" };
    expect(unknownReason(stale)).toMatch(/older than the freshness window/);
    for (const k of stale.keys!) {
      const ev = keyEvaluation(stale, k);
      expect(ev.word).toBe(UNKNOWN);
      expect(ev.tone).toBe("neutral");
      expect(usabilityWords(stale, k)).toBe(UNKNOWN);
    }
    // The list fixture's second policy is generation-behind: the API's reason, not this page's guess.
    const behind = (policyList.items as unknown as TrustPolicy[])[1]!;
    expect(behind.evaluation.reason).toBe("generationBehind");
    expect(unknownReason(behind)).toMatch(/earlier version of this policy/);
    // NEGATIVE CONTROL: the same keys under the fresh evaluation are verdicts, and one of them is Active.
    expect(policy().keys!.map((k) => keyEvaluation(policy(), k).word)).toContain("Active");
  });

  it("never renders a key another policy revoked for compromise as active", () => {
    const p = policy();
    const inherited = { ...p.keys![0]!, state: "Active", effectiveState: "Revoked", usableForNewSignatures: false, usableForVerification: "None" };
    expect(lifecycleWords(p, inherited)).toMatch(/Revoked for compromise by another TrustPolicy's record/);
    expect(lifecycleWords(p, inherited)).not.toMatch(/^Active/);
    expect(keyEvaluation(p, inherited).tone).toBe("danger");
    // CONTROL: the same key evaluated Active is the ordinary sentence...
    expect(lifecycleWords(p, { ...inherited, effectiveState: "Active" })).toMatch(/^Active: it may sign new documents/);
    // ...and a verdict that is not fresh is not read at all.
    const stale = { ...p, evaluation: { ...p.evaluation, state: "unknown" as const, reason: "stale" as const } };
    expect(lifecycleWords(stale, inherited)).toMatch(/^Active/);
  });

  it("names the compromise guard, and asks for action only when it is not merely recorded", () => {
    const p = policy();
    expect(compromiseGuard(p)).toBeNull();
    p.conditions = [...(p.conditions ?? []), { type: "CompromiseGuard", status: "True", reason: "CompromiseInherited", message: "recorded by TrustPolicy/incident" }];
    expect(compromiseGuard(p)).toEqual({ reason: "CompromiseInherited", message: "recorded by TrustPolicy/incident", act: true });
    p.conditions = p.conditions.map((c) => (c.type === "CompromiseGuard" ? { ...c, reason: "CompromiseRecorded" } : c));
    expect(compromiseGuard(p)?.act).toBe(false);
    // CONTROL: a guard that is not True says nothing.
    p.conditions = p.conditions.map((c) => (c.type === "CompromiseGuard" ? { ...c, status: "False" } : c));
    expect(compromiseGuard(p)).toBeNull();
  });
});
