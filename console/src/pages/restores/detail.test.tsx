// @vitest-environment jsdom
// A restore's page and the list's words, over the console fixtures ui/tests reads too
// (console/fixtures/console/): green only by the restore badge rule, covered: false is
// never a pass, a narrowed restore is said as partial, a stopped creation step names
// every topic it left, and a failed run offers a retry to a fresh target.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, render, screen, within } from "@testing-library/react";
import { SessionGate } from "../../app/SessionGate";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Outlet, RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import recorded from "../../../fixtures/legacy-console.json";
import subsetRestore from "../../../fixtures/console/restore-subset-pass.json";
import subsetOperation from "../../../fixtures/console/operation-restore-subset-pass.json";
import uncoveredRestore from "../../../fixtures/console/restore-complete-uncovered.json";
import uncoveredOperation from "../../../fixtures/console/operation-restore-complete-uncovered.json";
import untrustedOperation from "../../../fixtures/console/operation-restore-untrusted.json";
import unconfirmedRestore from "../../../fixtures/console/restore-creation-unconfirmed.json";
import timeBasisRestore from "../../../fixtures/console/restore-time-basis.json";
import plainRestore from "../../../fixtures/console/restore.json";
import type { OperationView, Restore } from "./words";
import {
  completeCoverageSentence,
  coverageWords,
  evidenceCase,
  operationVerified,
  outcomeShown,
  planBlockValue,
  planEvidenceBucket,
  planTimeBasis,
  refusalSentence,
  restoreVerified,
  selectionWords,
  signedTimeBasisText,
  teardownLeftWords,
  timestampBoundWords,
  verificationScopeSentence,
} from "./words";
import { When, exactUtc, relative } from "../../components/When";
import { RestoreDetail } from "./RestoreDetail";
import { CheckStep } from "./WizardSteps";
import { readinessRefusal, type Preflight } from "./model";

configure({ asyncUtilTimeout: 10_000 });
vi.setConfig({ testTimeout: 30_000 });

/** Kafka's Long.MAX "no limit", as JSON.parse reads it: no Date can hold it. */
const LONG_MAX = Number("9223372036854775807");

const R = (raw: unknown) => (raw as { item: Restore }).item;
const O = (raw: unknown) => (raw as { item: OperationView }).item;

describe("the words", () => {
  it("covered: false is never green, even beside a forged pass", () => {
    const forged = { ...R(uncoveredRestore), operation: { ...R(uncoveredRestore).operation, verifiedSuccess: true, verificationState: "valid" as const } };
    expect(restoreVerified(forged)).toBe(false);
    expect(restoreVerified({ ...forged, coverage: { ...forged.coverage, covered: true } })).toBe(true);
    const op = { ...O(uncoveredOperation), verifiedSuccess: true };
    expect(operationVerified(op)).toBe(false);
    expect(evidenceCase(op)).toMatch(/did not cover/);
  });
  it("the list row says complete and not covered, and a sampled run never claims complete", () => {
    const r = R(uncoveredRestore);
    expect(coverageWords(r.coverage.recorded, r.coverage.requested, { covered: r.coverage.covered ?? undefined })).toBe("complete, not covered: not a pass");
    expect(coverageWords("sampled", "complete", null)).toBe("sampled");
    expect(coverageWords(null, "complete", null)).toBe("complete asked for; not recorded yet");
    expect(coverageWords(null, "sampled", null)).not.toMatch(/complete/);
    // DUI-11: a finished run that recorded nothing will record nothing: no "yet" on a terminal row.
    expect(coverageWords(null, "sampled", null, true)).toBe("sampled asked for; not recorded");
    expect(coverageWords(null, "complete", null, true)).toBe("complete asked for; not recorded");
    expect(coverageWords(null, "sampled", null, false)).toBe("sampled (not recorded yet)");
  });
  it("a narrowed restore is said as partial, naming the partitions, and never as everything", () => {
    const sel = R(subsetRestore).selection!;
    expect(selectionWords(sel)).toMatch(/^partial: partitions 0, 2 of topic orders/);
    expect(selectionWords({ scope: "partial", narrowedTopics: 2 })).toMatch(/^partial: a partition subset of 2 topics/);
    expect(selectionWords({ scope: "partial", windowStartMs: 0, windowEndMs: 1000 })).toMatch(/^partial: every partition, from 1970/);
    expect(selectionWords({ scope: "partial" })).toMatch(/^partial: a selection this page cannot read/);
    expect(selectionWords(null)).toBe("");
  });
  it("the complete sentence says SELECTED partitions for a subset, and covered: false is never a pass", () => {
    const op = O(subsetOperation);
    expect(completeCoverageSentence(op.verificationScope.complete, op.verificationScope.selection)).toMatch(/every SELECTED partition/);
    expect(verificationScopeSentence(O(uncoveredOperation).verificationScope)).toMatch(/did NOT cover.*never a pass/);
    expect(verificationScopeSentence(null)).toMatch(/not a complete one/);
  });
  it("an untrusted signer is named as the signer's case, not the document's", () => {
    expect(evidenceCase(O(untrustedOperation))).toMatch(/^untrusted signer/);
  });
  it("the signed time basis names its lists, and absent is not recorded", () => {
    expect(signedTimeBasisText(R(timeBasisRestore).timeBasis)).toMatch(/producer time.*lat.*NOT RECORDED: old/);
    expect(signedTimeBasisText(null)).toMatch(/^Not recorded/);
  });
  it("the plan line readers agree with what ui/render.js read from block, flow and commented plans", () => {
    const plans = [
      (R(plainRestore) as { planBytes?: string }).planBytes ?? "",
      'restore: {point_in_time: "2026-09-07T14:05:00Z", time_basis: producerTime}\nevidence:\n  bucket: "logweir-evidence"\n',
      "restore:\n  point_in_time: x\n  # time_basis: producerTime\nevidence:\n    bucket: Bad_Bucket\n",
      "restore:\n  time_basis: 'producerTime' # opted in\n",
      "",
    ];
    expect(recorded.planLines.map((l) => l.plan)).toEqual(plans);
    for (const { plan, timeBasis, evidenceBucket, block } of recorded.planLines) {
      expect(planTimeBasis(plan)).toBe(timeBasis);
      expect(planEvidenceBucket(plan)).toBe(evidenceBucket);
      expect(planBlockValue(plan, "evidence", "bucket")).toBe(block);
    }
    expect(planTimeBasis(plans[1])).toBe("producerTime");
    expect(planTimeBasis(plans[2])).toBe("");
  });
});

// --------------------------------------------------------------- the page

const net = vi.hoisted(() => ({ answers: new Map<string, unknown>() }));
vi.mock("../../mock/fetch", () => ({
  mockFetch: async (request: Request) => {
    const path = new URL(request.url).pathname.replace(/^.*?\/api\/v1/, "");
    const body = net.answers.get(path);
    if (path === "/session") {
      const caps = { restoreCreate: true, operationsRead: true, restoresRead: true };
      return new Response(JSON.stringify({ actor: { id: "a", displayName: "A", issuer: "i", subject: "s" }, authenticationMode: "oidc", bindingRevision: "1", capabilities: caps, namespaces: [{ name: "t", roles: ["operator"], capabilities: caps }], requestId: "r" }), { status: 200, headers: { "content-type": "application/json" } });
    }
    if (body === undefined) return new Response(JSON.stringify({ status: 404, code: "not_found", title: "x", detail: `no ${path}`, requestId: "r" }), { status: 404, headers: { "content-type": "application/problem+json" } });
    return new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });
  },
}));

function showRestore(restore: Restore, operation?: OperationView) {
  net.answers.clear();
  net.answers.set(`/namespaces/t/restores/${restore.name}`, { item: restore, requestId: "r" });
  if (operation) net.answers.set(`/namespaces/t/operations/restore/${restore.name}`, { item: { ...operation, name: restore.name }, requestId: "r" });
  net.answers.set("/namespaces/t/approval-policy", { item: null, requestId: "r" });
  const root = createRootRoute({ component: Outlet });
  const ns = createRoute({ getParentRoute: () => root, path: "$ns", component: Outlet });
  const tree = root.addChildren([ns.addChildren([createRoute({ getParentRoute: () => ns, path: "restores/$name", component: RestoreDetail })])]);
  const router = createRouter({ routeTree: tree, history: createMemoryHistory({ initialEntries: [`/t/restores/${restore.name}`] }) });
  render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <SessionGate>{() => <RouterProvider router={router} />}</SessionGate>
    </QueryClientProvider>,
  );
}

afterEach(() => cleanup());

describe("a restore's page", () => {
  it("a covered: false run is never green, and says so first", async () => {
    showRestore(R(uncoveredRestore), O(uncoveredOperation));
    await screen.findByText("Complete coverage did not cover this restore");
    expect(screen.getAllByText("Not covered").length).toBeGreaterThan(0);
    expect(screen.queryByText("Restored and verified")).toBeNull();
    expect(screen.getByRole("button", { name: /Retry to a fresh target/ })).toBeTruthy();
  });
  it("DUI-16: a finished restore whose target sets no timestamp bound (Long.MAX) renders, and says so", async () => {
    const r = R(subsetRestore);
    showRestore({ ...r, topicPreflight: { ...r.topicPreflight!, timestampBound: LONG_MAX } }, O(subsetOperation));
    await screen.findByText("Restored and verified");
    expect(screen.getByText("Any age: the broker sets no bound (its default)")).toBeTruthy();
    expect(screen.queryByText(/Oldest timestamp accepted/)).toBeNull();
  });
  it("a narrowed, verified restore is green and still says partial", async () => {
    showRestore(R(subsetRestore), O(subsetOperation));
    await screen.findByText("Restored and verified");
    expect(screen.getAllByText(/partial: partitions 0, 2 of topic orders/).length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: /Retry to a fresh target/ })).toBeNull();
  });
  it("a stopped creation step names the topic it left and the one it cannot account for", async () => {
    const r = R(unconfirmedRestore);
    showRestore(r, { ...O(uncoveredOperation), state: "failed", verifiedSuccess: false });
    await screen.findByText(/stopped while creating its target topics/);
    expect(screen.getByText(/created by this restore and left empty/)).toBeTruthy();
    expect(screen.getByText(/may be this restore's or someone else's/)).toBeTruthy();
  });
  it("an untrusted signer's run is not green and names the case (negative control: the same run trusted is)", async () => {
    const op = O(untrustedOperation);
    const r = { ...R(plainRestore), operation: { ...R(plainRestore).operation, verifiedSuccess: false } };
    showRestore(r, op);
    await screen.findByText(/untrusted signer/);
    expect(screen.queryByText("Restored and verified")).toBeNull();
    cleanup();
    showRestore(R(plainRestore), { ...op, verifiedSuccess: true, trust: { ...op.trust, state: "verified", basis: "Current" }, verification: { ...op.verification, state: "valid" } });
    await screen.findByText("Restored and verified");
  });
});

// The refusal sentences the runner writes (crates/logweir/src/drill/binding.rs, phase0_admit.rs).
const BINDING_REFUSAL =
  "PointBindingMismatch: PointBindingMismatch. This archive does not hold the receipt the plan binds as recovery point lwp1-0123456789abcdef0123456789abcdef at logweir/backups/set-a/run-1.receipt.json: it is absent, its bytes do not hash to the bound sha256:aa, or it is larger than the 67108864-byte read cap for a receipt; no data operation was started.";
const ENGINE_REFUSAL =
  "target.engineProtocol is notReady (EngineProtocolUnsupported): the engine sends Produce v8 and this endpoint serves Produce v0-v7 Refused at phase 0: nothing was created on the target and no record was restored";
const refused = (message: string): OperationView => ({
  ...O(uncoveredOperation),
  state: "refused",
  stateReason: "GuardRefused",
  verifiedSuccess: false,
  message,
  result: { status: "refused", exitCode: 3, exitReason: "GuardRefused" },
  conditions: [{ type: "Failed", status: "True", reason: "GuardRefused", message }],
  verificationScope: { ...O(uncoveredOperation).verificationScope, complete: undefined },
});

describe("v1.1: what the restore page says", () => {
  it("FX-36, FX-45: a run refused before the engine shows its refusal sentence verbatim", () => {
    expect(refusalSentence(refused(BINDING_REFUSAL))).toBe(BINDING_REFUSAL);
    expect(refusalSentence({ ...refused(""), message: undefined, conditions: [{ type: "Failed", status: "False", reason: "GuardRefused", message: ENGINE_REFUSAL }] })).toBe(ENGINE_REFUSAL);
    // CONTROL: a run that did not pass is not a refusal.
    expect(refusalSentence(O(uncoveredOperation))).toBeNull();
  });

  it("FX-30: a complete verification that did not cover the restore names its reason, its unverified segments and the partitions not compared, and is never a pass", () => {
    const c = O(uncoveredOperation).verificationScope.complete!;
    const sentence = completeCoverageSentence(c, null);
    expect(sentence).toContain(c.incompleteReason!);
    expect(sentence).toContain("2 archive segment(s) were not verified");
    expect(sentence).toContain("(not compared: orders/1)");
    expect(sentence).toMatch(/never a pass/);
    const forged = { ...O(uncoveredOperation), result: { ...O(uncoveredOperation).result, outcome: "pass" } };
    expect(outcomeShown(forged)).toBe("not a pass: complete coverage did not cover this restore");
    // CONTROL: a covered run's pass is its pass.
    expect(outcomeShown({ ...forged, verificationScope: { ...forged.verificationScope, complete: { ...c, covered: true } } })).toBe("pass");
  });

  it("FX-38: a teardown that left more than it lists says how many", () => {
    const failed = Array.from({ length: 256 }, (_, i) => ({ topic: `t-${i}`, error: "TopicAuthorizationFailed" }));
    expect(teardownLeftWords({ failed, failedCount: 300 })).toBe("300 topics were left; the first 256 are listed");
    // CONTROLS: a whole list, and an older status with no count.
    expect(teardownLeftWords({ failed, failedCount: 256 })).toBeNull();
    expect(teardownLeftWords({ failed })).toBeNull();
  });
});

describe("v1.1: a restore's page", () => {
  it("FX-36: a refused restore shows the binding refusal verbatim and nothing about which case it was", async () => {
    const r = { ...R(plainRestore), operation: { ...R(plainRestore).operation, state: "refused" as const, stateReason: "GuardRefused", verifiedSuccess: false } };
    showRestore(r, refused(BINDING_REFUSAL));
    await screen.findByText("This restore was refused: GuardRefused");
    const alert = screen.getByText("This restore was refused: GuardRefused").closest(".pf-v6-c-alert") as HTMLElement;
    expect(alert.textContent).toContain(BINDING_REFUSAL);
    expect(alert.textContent).not.toMatch(/\b(was absent|was altered|was oversized)\b/);
  });

  it("FX-38: the teardown card says how many topics were left beyond the 256 it lists", async () => {
    const failed = Array.from({ length: 256 }, (_, i) => ({ topic: `t-${i}`, error: "TopicAuthorizationFailed" }));
    showRestore(R(plainRestore), { ...O(subsetOperation), teardown: { deleted: [], deletedTruncated: false, failed, failedCount: 300 } });
    await screen.findByText("300 topics were left; the first 256 are listed");
    cleanup();
    // CONTROL: the same list with a matching count says nothing more.
    showRestore(R(plainRestore), { ...O(subsetOperation), teardown: { deleted: [], deletedTruncated: false, failed, failedCount: 256 } });
    await screen.findByText("Teardown");
    expect(screen.queryByText(/topics were left; the first/)).toBeNull();
  });
});

describe("FX-38: the restore readiness check", () => {
  it("a catalog point the controller could not find for an unreadable view line shows its message, and is not ready", () => {
    const MESSAGE =
      "the view does not list recovery point `lwp1-0123456789abcdef0123456789abcdef`, and view page(s) primary-view-2 hold a line this check cannot read, which may be that point";
    const result = {
      id: "pf-r1", namespace: "t", uid: "u", resourceVersion: "1", operation: "restore", state: "unknown", terminal: true, applicable: true,
      stale: false, staleBasis: [], staleReasons: [], binding: { planHash: "sha256:aa", referents: [] }, warnings: [], executionOnly: [], conditions: [], detailsAvailable: false,
      checks: [
        { id: "target.mappedTopics", code: "MappedTopicsAbsent", state: "ready", gating: "blocking" },
        { id: "recoveryPoint.state", code: "CatalogPointViewUnavailable", state: "unknown", gating: "blocking", message: MESSAGE, remedy: "Request a catalog sync and run this check again once the view is published." },
      ],
    } as unknown as Preflight;
    const refusal = readinessRefusal(result, "sha256:aa", "sha256:aa");
    expect(refusal).toMatch(/recoveryPoint\.state \(CatalogPointViewUnavailable\)/);
    render(
      <CheckStep
        readiness={{ result, boundHash: "sha256:aa", boundKey: "k", error: null, starting: false, stoppedAt: null }}
        refusal={refusal}
        canCheck
        hash="sha256:aa"
        planProblem={null}
        connection={null}
        onStart={() => undefined}
        onCancel={() => undefined}
      />,
    );
    const list = screen.getByRole("list", { name: "Readiness checks" });
    expect(list.textContent).toContain(MESSAGE);
    expect(list.textContent).toContain("CatalogPointViewUnavailable");
    expect(list.textContent).toContain("Request a catalog sync and run this check again once the view is published.");
    expect(screen.getByText("Not ready", { selector: ".pf-v6-c-label__text" })).toBeTruthy();
  });
});

describe("theme A's D6: the restore readiness check on an older runner", () => {
  it("lists runner.image not ready by CheckContractMismatch and runner.pod as a pod that ran", () => {
    const result = {
      id: "pf-r2", namespace: "t", uid: "u", resourceVersion: "1", operation: "restore", state: "notReady", terminal: true, applicable: true,
      stale: false, staleBasis: [], staleReasons: [], binding: { planHash: "sha256:aa", referents: [] }, warnings: [], executionOnly: [], conditions: [], detailsAvailable: false,
      checks: [
        { id: "runner.image", code: "CheckContractMismatch", state: "notReady", gating: "blocking", message: "RunnerOlderThanController: the runner image is older than this controller" },
        { id: "runner.pod", code: "PodCompleted", state: "ready", gating: "blocking", message: "the check pod ran and its runner exited 1; the runner image is older than this controller (runner.image)" },
      ],
    } as unknown as Preflight;
    const refusal = readinessRefusal(result, "sha256:aa", "sha256:aa");
    expect(refusal).toMatch(/runner\.image \(CheckContractMismatch\)/);
    expect(refusal).not.toMatch(/runner\.pod/);
    render(
      <CheckStep
        readiness={{ result, boundHash: "sha256:aa", boundKey: "k", error: null, starting: false, stoppedAt: null }}
        refusal={refusal}
        canCheck
        hash="sha256:aa"
        planProblem={null}
        connection={null}
        onStart={() => undefined}
        onCancel={() => undefined}
      />,
    );
    const items = within(screen.getByRole("list", { name: "Readiness checks" })).getAllByRole("listitem");
    const image = items.find((li) => li.textContent?.includes("runner.image"))!;
    const pod = items.find((li) => li.textContent?.includes("runner.pod"))!;
    expect(image.textContent).toContain("CheckContractMismatch");
    expect(image.textContent).toContain("Not ready");
    expect(pod.textContent).toContain("Passed");
    expect(pod.textContent).toContain("the check pod ran and its runner exited 1");
  });
});

describe("DUI-16: a value that is not a time never throws", () => {
  it("prints an out-of-range instant as not a time, and a real one as a time (the control)", () => {
    expect(exactUtc(LONG_MAX)).toBe("not a time");
    expect(relative(LONG_MAX)).toBe("not a time");
    expect(exactUtc("garbage")).toBe("not a time");
    expect(exactUtc(Date.parse("2026-10-14T13:35:23Z"))).toBe("2026-10-14 13:35 UTC");
    render(<When value={LONG_MAX} exact />);
    expect(screen.getByText("Not a time")).toBeTruthy();
  });

  it("reads the timestamp bound as a duration, never as an instant", () => {
    expect(timestampBoundWords(LONG_MAX)).toBe("Any age: the broker sets no bound (its default)");
    expect(timestampBoundWords(7 * 86_400_000)).toBe("Up to 7 days old (604,800,000 ms); an older record is refused or rewritten");
    expect(timestampBoundWords(null)).toBe("Not recorded");
  });
});
