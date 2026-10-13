// @vitest-environment jsdom
// The approvals module's rules and its requests, over the console fixtures the
// API produced (ui/tests/fixtures/console/). Ported from ui/tests/
// console-approval.spec.js and approval-policy.spec.js (the approvals page's
// rows); every row sits beside the input that must make it say no.
import { beforeEach, describe, expect, it, vi } from "vitest";
import approvalFixture from "../../../ui/tests/fixtures/console/approval.json";
import revokedFixture from "../../../ui/tests/fixtures/console/approval-revoked-after-use.json";
import twoPersonFixture from "../../../ui/tests/fixtures/console/approval-policy-two-person.json";
import pendingFixture from "../../../ui/tests/fixtures/console/approval-request-pending.json";
import approvedFixture from "../../../ui/tests/fixtures/console/approval-request-approved.json";
import notConfirmedFixture from "../../../ui/tests/fixtures/console/approval-request-not-confirmed.json";
import incompleteFixture from "../../../ui/tests/fixtures/console/approval-request-scope-incomplete.json";
import originalFixture from "../../../ui/tests/fixtures/console/approval-request-original-name.json";
import {
  afterwardsWords,
  approvalMode,
  approvalsNavCount,
  approvalState,
  ApprovalProblem,
  consoleApprovalOffered,
  instantText,
  listAll,
  LocalRefusal,
  noApprovalSentence,
  outcomeOf,
  ownerCase,
  placeFieldErrors,
  PRIVATE_KEY_REFUSAL,
  recordState,
  refuseKeyMaterial,
  restoreProgressSentence,
  routeMismatches,
  scopeShowable,
  SELF_ATTESTED_FALSE,
  SELF_ATTESTED_TRUE,
  selfAttestedSentence,
  sendApprovalFiles,
  sendConsoleApproval,
  sendCountersignature,
  stateSentence,
  typedTopicsOf,
  waitingForYou,
  type Approval,
  type ApprovalPolicy,
  type ApprovalRequest,
  type OfferView,
  type Subject,
} from "./approvals";
import { sha256 } from "../components/hash";
import type { Restore } from "./types";

const net = vi.hoisted(() => ({
  sent: [] as { method: string; path: string; headers: Headers; body: unknown }[],
  answer: (_r: Request, _path: string): Response | Promise<Response | null> | null => null,
}));

vi.mock("../mock/fetch", () => ({
  mockFetch: async (request: Request) => {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^.*?\/api\/v1/, "") + url.search;
    const text = request.method === "GET" ? "" : await request.text();
    net.sent.push({ method: request.method, path, headers: request.headers, body: text ? JSON.parse(text) : undefined });
    const r = await net.answer(request, path);
    if (r) return r;
    return json(404, { code: "not_found", detail: `no fake for ${request.method} ${path}` });
  },
}));

function json(status: number, body: unknown) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" } });
}

beforeEach(() => {
  net.sent = [];
  net.answer = () => null;
});

const posts = () => net.sent.filter((s) => s.method === "POST");

const TWO_PERSON = twoPersonFixture.item as ApprovalPolicy;
const STRICT: ApprovalPolicy = { ...TWO_PERSON, name: "prod-governed", operatorMode: "strict", consoleApprovalAvailable: false };
const CONFIRM: ApprovalPolicy = { ...TWO_PERSON, name: "team-ordinary", mode: "ordinary", operatorMode: "confirm", consoleApprovalAvailable: false, ordinaryConfirmationAvailable: true };
const LEGACY: ApprovalPolicy = { ...TWO_PERSON, name: "legacy-governed-v1", legacy: true, operatorMode: "strict", consoleApprovalAvailable: false };

const request = (f: { item: unknown }) => structuredClone(f.item) as ApprovalRequest;
const SUBJECT: Subject = { name: "restore-x", uid: "uid-x", namespace: "team-a", planHash: "sha256:" + "e".repeat(64), approvalName: "approval-x" };

function view(over: Partial<OfferView> = {}): OfferView {
  return { policy: TWO_PERSON, restoreExists: true, maySubmit: true, mismatches: [], request: request(pendingFixture), subject: SUBJECT, ...over };
}

describe("OD-8's modes", () => {
  it("reads two-person, strict, confirm and unbound from the policy, and an unknown operator word as strict", () => {
    expect(approvalMode(TWO_PERSON)).toBe("twoPerson");
    expect(approvalMode(STRICT)).toBe("strict");
    expect(approvalMode(CONFIRM)).toBe("confirm");
    expect(approvalMode(LEGACY)).toBe("files");
    // A word nobody recognises never turns a key-based approval into a click.
    for (const word of ["Two-Person", "twoPerson", "", "console"]) {
      expect(approvalMode({ ...TWO_PERSON, operatorMode: word as never })).toBe("strict");
    }
    // A mode word this page does not know keeps the v1 files, as the old console did.
    expect(approvalMode({ ...TWO_PERSON, mode: "other" as never })).toBe("files");
  });

  it("says what the empty approvals table means under each mode", () => {
    const two = noApprovalSentence("twoPerson");
    expect(two).toContain("two-person");
    expect(two).toContain("No key is involved");
    expect(two).not.toContain("countersignature");
    expect(two).not.toContain("logweir drill");
    expect(noApprovalSentence("strict")).toContain("countersignature");
    expect(noApprovalSentence("confirm")).toContain("confirm");
    expect(noApprovalSentence("files")).toContain("logweir drill approve");
  });
});

describe("the private-key refusal", () => {
  it("refuses a key by its file name, before anything is read", () => {
    for (const name of ["approver.pem", "k.KEY", "a.p8", "b.p12", "c.pfx", "store.jks", "putty.ppk", "id_rsa", "id_ed25519.pub", "/home/me/.ssh/id_ecdsa"]) {
      expect(refuseKeyMaterial(name, ""), name).toBe(PRIVATE_KEY_REFUSAL);
    }
    // NEGATIVE CONTROL: the files an approver is asked for.
    for (const name of ["approval.json", "approval.sig", "approval.sig.txt", "my_id_rsa.txt"]) {
      expect(refuseKeyMaterial(name, '{"payloadType":"x"}'), name).toBeNull();
    }
  });

  it("refuses text spelling the words that open a private-key PEM, in any case and across a line break", () => {
    const pems = [
      "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----",
      "-----begin ec private key-----",
      "PRIVATE\nKEY",
      "-----BEGIN OPENSSH_PRIVATE_KEY-----",
      "-----BEGIN ENCRYPTED PRIVATE-KEY-----",
    ];
    for (const text of pems) expect(refuseKeyMaterial("", text), text).toBe(PRIVATE_KEY_REFUSAL);
    // NEGATIVE CONTROL: a sidecar, a document, and a Secret honestly called private-key.
    for (const text of ['{"payloadType":"application/vnd.logweir.drill-approval+json;version=1.0.0","signatures":[]}', '{"plan_hash":"sha256:ab"}', "minio-private-key"]) {
      expect(refuseKeyMaterial("approval.sig", text), text).toBeNull();
    }
  });
});

describe("an approval's state", () => {
  const approval = approvalFixture.item as Approval;
  const bound = approval.verifiedSubject!;
  const subject: Subject = { name: bound.name, uid: bound.uid, namespace: bound.namespace, planHash: approval.planHash, approvalName: approval.name };

  it("is verified for exactly this Restore's UID, and bound to another run for an older object of the name", () => {
    expect(approvalState(approval, subject).state).toBe("verified");
    expect(approvalState(approval, { ...subject, uid: "uid-of-a-newer-restore" })).toEqual({ state: "foreign-execution", detail: bound.uid });
  });

  it("names another plan, or another subject, before any verdict", () => {
    expect(approvalState(approval, { ...subject, planHash: "sha256:" + "0".repeat(64) }).state).toBe("plan-mismatch");
    expect(approvalState(approval, { ...subject, name: "another" }).state).toBe("foreign-subject");
    expect(approvalState({ ...approval, subjectRef: { kind: "Switchover", name: bound.name } }, subject).state).toBe("foreign-subject");
    expect(approvalState(null, subject).state).toBe("absent");
  });

  it("never reads a consumed approval whose signer was later compromised as verified", () => {
    const revoked = revokedFixture.item as Approval;
    const found = recordState(revoked);
    expect(found.state).toBe("revoked-after-use");
    expect(found.reason).toBe("RecordedBeforeRevocation");
    expect(found.consumedAt).toBe("2026-09-09T13:01:00Z");
    const said = stateSentence(found, { ...subject, name: "r1" });
    expect(said).toContain("KeyCompromise");
    expect(said).toContain("never green");
    // NEGATIVE CONTROL: the same refusal without the admission is a plain refusal.
    const unconsumed = { ...revoked, conditions: revoked.conditions.filter((c) => c.type !== "Consumed") };
    expect(recordState(unconsumed).state).toBe("refused");
  });

  it("is expired, refused or awaiting verification from its own conditions", () => {
    const refusedBy = (reason: string): Approval => ({ ...approval, verified: false, verifiedSubject: undefined, conditions: [{ type: "Verified", status: "False", reason, message: "m" }] });
    expect(recordState(refusedBy("KeyIdExpired")).state).toBe("expired");
    expect(recordState(refusedBy("SignatureInvalid")).state).toBe("refused");
    expect(recordState({ ...approval, verified: null, verifiedSubject: undefined, conditions: [] }).state).toBe("awaiting-verification");
    // `verified: true` without the bound subject is not a verdict about anything.
    expect(recordState({ ...approval, verifiedSubject: undefined }).state).toBe("awaiting-verification");
  });

  it("says self-attestation in a sentence, never as a bare boolean", () => {
    expect(selfAttestedSentence(true)).toBe(SELF_ATTESTED_TRUE);
    expect(selfAttestedSentence(false)).toBe(SELF_ATTESTED_FALSE);
    expect(selfAttestedSentence(null)).toBe("self-attestation: not evaluated yet");
    expect(SELF_ATTESTED_FALSE).not.toMatch(/^false|two people/);
  });
});

describe("the two-person offer", () => {
  it("draws Approve only when the server offered it to this session, for this Restore, with a scope shown whole", () => {
    expect(consoleApprovalOffered(view())).toBe(true);
    const withOffer = (approve: ApprovalRequest["approve"]) => ({ ...request(pendingFixture), approve });
    const cases: [string, Partial<OfferView>][] = [
      ["the server did not offer it", { request: withOffer({ offered: false, refusal: "requester", sentence: "You requested this restore." }) }],
      ["offered is not exactly true", { request: withOffer({ offered: "true" as never, sentence: "x" }) }],
      ["no approver capability", { maySubmit: false }],
      ["expired", { request: { ...request(pendingFixture), state: "expired" } }],
      ["already approved", { request: request(approvedFixture) }],
      ["not confirmed", { request: request(notConfirmedFixture) }],
      ["a state this page does not know", { request: { ...request(pendingFixture), state: "approvedByMagic" as never } }],
      ["another Restore of this name", { request: { ...request(pendingFixture), restoreUid: "uid-older" } }],
      ["another name", { request: { ...request(pendingFixture), restore: "restore-y" } }],
      ["no hash", { request: { ...request(pendingFixture), confirmationSha256: undefined } }],
      ["a malformed hash", { request: { ...request(pendingFixture), confirmationSha256: "sha256:XYZ" } }],
      ["a link that does not match", { mismatches: [{ field: "plan hash", claimed: "a", actual: "b" }] }],
      ["this console takes no approvals", { policy: { ...TWO_PERSON, consoleApprovalAvailable: false } }],
      ["a strict namespace", { policy: { ...STRICT, consoleApprovalAvailable: true } }],
      ["the request was not read", { request: null }],
      ["no Restore", { restoreExists: false }],
      ["an incomplete scope the server offered anyway", { request: { ...request(incompleteFixture), approve: { offered: true, sentence: "go" } } }],
    ];
    for (const [label, change] of cases) expect(consoleApprovalOffered(view(change)), label).toBe(false);
  });

  it("shows a scope only whole: complete, every topic present, and not empty", () => {
    expect(scopeShowable(request(pendingFixture))?.topicsCount).toBe(2);
    const broken: ApprovalRequest[] = [
      request(incompleteFixture),
      { ...request(pendingFixture), scopeComplete: false },
      (() => {
        const r = request(pendingFixture);
        r.scope!.topicsCount = 3;
        return r;
      })(),
      (() => {
        const r = request(pendingFixture);
        r.scope!.topics = [];
        r.scope!.topicsCount = 0;
        return r;
      })(),
      (() => {
        const r = request(pendingFixture);
        delete r.scope;
        return r;
      })(),
    ];
    for (const r of broken) expect(scopeShowable(r)).toBeNull();
  });

  it("counts for the navigation only what waits for this session", () => {
    const own = { ...request(pendingFixture), approve: { offered: false, refusal: "requester" as const, sentence: "You requested this restore." } };
    expect(waitingForYou([request(pendingFixture), own, request(approvedFixture), request(notConfirmedFixture), null, undefined])).toBe(1);
    expect(approvalsNavCount(1)).toEqual({ n: 1, tone: "brand", text: "1 request waits for you" });
    expect(approvalsNavCount(2)?.text).toBe("2 requests wait for you");
    // NEGATIVE CONTROL: nothing waiting is no count at all, never a "0" pill.
    expect(approvalsNavCount(0)).toBeUndefined();
    expect(waitingForYou([own, request(incompleteFixture)])).toBe(0);
  });

  it("reads the requester's owner statement as the run acts on it", () => {
    expect(ownerCase(request(originalFixture).scope!.target.ownerStatement)).toBe("noOwner");
    expect(ownerCase({ ownerPath: false })).toBe("nothingStated");
    // owner_path with no owners declared: the run has nowhere to look and refuses.
    expect(ownerCase({ ownerPath: true })).toBe("nothingStated");
    expect(ownerCase({ ownerPath: true, owners: [{ topic: "orders", kind: "strimzi", reference: "kafka/orders" }] })).toBe("ownerPath");
    expect(ownerCase({ ownerPath: false, owners: [{ topic: "orders", kind: "strimzi", reference: "kafka/orders" }] })).toBe("ownersWithoutPath");
  });

  it("shows a window end to the millisecond the run uses, and a whole second as it is", () => {
    expect(instantText("2026-09-07T14:05:00.999Z")).toBe("2026-09-07 14:05:00.999 UTC");
    expect(instantText("2026-09-07T14:00:00.001Z")).toBe("2026-09-07 14:00:00.001 UTC");
    expect(instantText("2026-09-07T14:05:00Z")).toBe("2026-09-07 14:05:00 UTC");
    expect(instantText(undefined)).toBe("Not recorded");
  });

  it("says a newTopic restore's created topics stay, whatever teardown says (FX-49)", () => {
    expect(afterwardsWords("newTopic", "delete")).toContain("stay on the target cluster");
    expect(afterwardsWords("scratch", "delete")).toBe("The topics this run creates are deleted.");
    expect(afterwardsWords("scratch", "keep")).toBe("Nothing is deleted (teardown keep).");
  });
});

describe("the subject", () => {
  it("refuses a link whose hash or Approval name is not the Restore's", () => {
    expect(routeMismatches({}, SUBJECT)).toEqual([]);
    expect(routeMismatches({ hash: SUBJECT.planHash, approval: SUBJECT.approvalName }, SUBJECT)).toEqual([]);
    expect(routeMismatches({ hash: "sha256:other", approval: "other" }, SUBJECT).map((m) => m.field)).toEqual(["plan hash", "Approval name"]);
  });

  it("reads a pending restore without an admission as waiting, never as refused", () => {
    const r = { operation: { state: "pending", stateReason: "ApprovalNotVerified", terminal: false, verificationState: "pending", verifiedSuccess: false } } as Restore;
    expect(restoreProgressSentence(r)).toMatch(/^Waiting for approval/);
    const failed = { operation: { state: "failed", stateReason: "ApprovalSubjectMismatch", terminal: true, verificationState: "notAttempted", verifiedSuccess: false } } as Restore;
    expect(restoreProgressSentence(failed)).toMatch(/^Failed, reason ApprovalSubjectMismatch/);
  });

  it("reads OD-10's typed names from the signed bytes only", () => {
    const packet = (approvalBytes: string) => ({ approvalBytes, sidecarBytes: "{}", name: "a", namespace: "n", planHash: "h", subjectRef: { kind: "Restore", name: "r" }, uid: "u" });
    expect(typedTopicsOf(packet(JSON.stringify({ originalNameConfirmation: { typedTopics: ["orders", "payments"] } })))).toEqual(["orders", "payments"]);
    // NEGATIVE CONTROLS: an ordinary document, an unreadable one, and a wrong shape name nothing.
    expect(typedTopicsOf(packet(JSON.stringify({ planHash: "h" })))).toBeNull();
    expect(typedTopicsOf(packet("not json"))).toBeNull();
    expect(typedTopicsOf(packet(JSON.stringify({ originalNameConfirmation: { typedTopics: [1, 2] } })))).toBeNull();
    expect(typedTopicsOf(null)).toBeNull();
  });
});

// ------------------------------------------------------------------ requests

const PLAN = "name: p\nsource:\n  topics: [orders]\n";

async function restoreAnswer(over: Partial<Restore> = {}) {
  return json(200, {
    item: { name: "restore-x", namespace: "team-a", uid: "uid-x", approvalRef: { name: "approval-x" }, planBytes: PLAN, planHash: await sha256(PLAN), operation: { state: "pending" }, ...over },
    requestId: "r",
  });
}

const created = (status = 201) => json(status, { item: { ...(approvalFixture.item as object), name: "approval-x" }, replayed: status === 200, requestId: "r" });

describe("the requests", () => {
  it("the click sends the hash that was shown and nothing else, to the Restore's own route", async () => {
    net.answer = (r, path) => (r.method === "POST" && path === "/namespaces/team-a/restores/restore-x/console-approval" ? created() : null);
    const written = await sendConsoleApproval("team-a", view());
    expect(written.replayed).toBe(false);
    expect(posts()).toHaveLength(1);
    expect(posts()[0]!.body).toEqual({ confirmationSha256: request(pendingFixture).confirmationSha256 });
    // The route takes no key: the Approval is named by the Restore's own approvalRef.
    expect(posts()[0]!.headers.get("Idempotency-Key")).toBeNull();
    // NEGATIVE CONTROLS: whatever the page was not offered, it does not send.
    for (const change of [{ maySubmit: false }, { request: request(approvedFixture) }, { policy: STRICT }, { policy: CONFIRM }, { policy: null }]) {
      await expect(sendConsoleApproval("team-a", view(change))).rejects.toBeInstanceOf(LocalRefusal);
    }
    expect(posts()).toHaveLength(1);
  });

  it("a countersignature is the sidecar alone, after the Restore is read again; key material and an empty field send nothing", async () => {
    const subject: Subject = { ...SUBJECT, planHash: await sha256(PLAN) };
    net.answer = async (r, path) => {
      if (r.method === "GET" && path === "/namespaces/team-a/restores/restore-x") return restoreAnswer();
      if (r.method === "POST" && path === "/namespaces/team-a/restores/restore-x/approval") return created();
      return null;
    };
    await sendCountersignature("team-a", subject, { text: '{"payloadType":"x","signatures":[1,2]}', fileName: "" });
    expect(posts().map((p) => p.body)).toEqual([{ sidecarBytes: '{"payloadType":"x","signatures":[1,2]}' }]);
    const pem = "-----BEGIN " + "PRIVATE KEY-----\nabc\n-----END " + "PRIVATE KEY-----\n";
    await expect(sendCountersignature("team-a", subject, { text: pem, fileName: "" })).rejects.toThrow(PRIVATE_KEY_REFUSAL);
    await expect(sendCountersignature("team-a", subject, { text: "{}", fileName: "approver.pem" })).rejects.toThrow(PRIVATE_KEY_REFUSAL);
    await expect(sendCountersignature("team-a", subject, { text: "   ", fileName: "" })).rejects.toBeInstanceOf(LocalRefusal);
    expect(posts()).toHaveLength(1);
  });

  it("an unbound namespace's two files are sent byte for byte, and only for the Restore the page showed", async () => {
    const subject: Subject = { ...SUBJECT, planHash: await sha256(PLAN) };
    let uid = "uid-x";
    net.answer = async (r, path) => {
      if (r.method === "GET" && path === "/namespaces/team-a/restores/restore-x") return restoreAnswer({ uid });
      if (r.method === "POST" && path === "/namespaces/team-a/restores/restore-x/approval") return created(200);
      return null;
    };
    const files = { approvalBytes: '{"plan_hash":"sha256:ab"}\n  ', sidecarBytes: '{"payloadType":"v1","signatures":[]}\r\n', approvalFileName: "approval.json", sidecarFileName: "approval.sig" };
    const written = await sendApprovalFiles("team-a", subject, files);
    expect(written.replayed).toBe(true);
    expect(posts()[0]!.body).toEqual({ approvalBytes: files.approvalBytes, sidecarBytes: files.sidecarBytes });
    // NEGATIVE CONTROLS: a key in either document, a missing one, and a Restore recreated under the name.
    await expect(sendApprovalFiles("team-a", subject, { ...files, sidecarFileName: "id_ed25519" })).rejects.toMatchObject({ fields: ["sidecarBytes"] });
    await expect(sendApprovalFiles("team-a", subject, { ...files, approvalBytes: "PRIVATE KEY" })).rejects.toMatchObject({ fields: ["approvalBytes"] });
    await expect(sendApprovalFiles("team-a", subject, { ...files, sidecarBytes: "" })).rejects.toMatchObject({ fields: ["sidecarBytes"] });
    uid = "uid-recreated";
    await expect(sendApprovalFiles("team-a", subject, files)).rejects.toThrow(/changed since this page read it \(uid was uid-x, is now uid-recreated\)/);
    expect(posts()).toHaveLength(1);
  });

  it("a refusal keeps its field errors beside their fields; no answer is an unknown outcome, never a refusal", async () => {
    const problem = new ApprovalProblem(422, {
      code: "validation_failed",
      detail: "d",
      errors: [
        { field: "approvalBytes", code: "plan_mismatch", message: "the approval document names plan hash x" },
        { field: "ticket", code: "x", message: "elsewhere" },
      ],
    });
    expect(outcomeOf(problem)).toBe("invalid");
    expect(placeFieldErrors(problem, ["approvalBytes", "sidecarBytes"] as const)).toEqual({
      fields: { approvalBytes: "the approval document names plan hash x" },
      unmatched: ["ticket: elsewhere"],
    });
    expect(outcomeOf(new ApprovalProblem(403, { code: "forbidden", detail: "You requested this restore." }))).toBe("refused");
    expect(outcomeOf(new ApprovalProblem(503, { code: "kubernetes_unavailable", detail: "later" }))).toBe("unknown");
    expect(outcomeOf(new TypeError("Failed to fetch"))).toBe("unknown");
    expect(outcomeOf(new LocalRefusal("no"))).toBe("local");
  });

  it("a list follows its cursor to the end, and refuses rather than present a prefix as the whole", async () => {
    let reads = 0;
    const all = await listAll(async (cursor) => {
      reads += 1;
      return { items: [cursor ?? "first"], page: { nextCursor: reads < 3 ? `c${reads}` : null } };
    }, "Approvals");
    expect(all).toEqual(["first", "c1", "c2"]);
    await expect(listAll(async () => ({ items: [1], page: { nextCursor: "again" } }), "Approvals")).rejects.toThrow(/more than 25 Approvals/);
  });
});
