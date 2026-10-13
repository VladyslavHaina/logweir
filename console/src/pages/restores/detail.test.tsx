// @vitest-environment jsdom
// A restore's page and the list's words, over the console fixtures ui/tests reads too
// (ui/tests/fixtures/console/): green only by the restore badge rule, covered: false is
// never a pass, a narrowed restore is said as partial, a stopped creation step names
// every topic it left, and a failed run offers a retry to a fresh target.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, render, screen } from "@testing-library/react";
import { SessionGate } from "../../app/SessionGate";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Outlet, RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
// @ts-expect-error ui/ is plain JavaScript; the line readers are compared with it.
import * as oldRender from "../../../../ui/render.js";
import subsetRestore from "../../../../ui/tests/fixtures/console/restore-subset-pass.json";
import subsetOperation from "../../../../ui/tests/fixtures/console/operation-restore-subset-pass.json";
import uncoveredRestore from "../../../../ui/tests/fixtures/console/restore-complete-uncovered.json";
import uncoveredOperation from "../../../../ui/tests/fixtures/console/operation-restore-complete-uncovered.json";
import untrustedOperation from "../../../../ui/tests/fixtures/console/operation-restore-untrusted.json";
import unconfirmedRestore from "../../../../ui/tests/fixtures/console/restore-creation-unconfirmed.json";
import timeBasisRestore from "../../../../ui/tests/fixtures/console/restore-time-basis.json";
import plainRestore from "../../../../ui/tests/fixtures/console/restore.json";
import type { OperationView, Restore } from "./words";
import {
  completeCoverageSentence,
  coverageWords,
  evidenceCase,
  operationVerified,
  planBlockValue,
  planEvidenceBucket,
  planTimeBasis,
  restoreVerified,
  selectionWords,
  signedTimeBasisText,
  verificationScopeSentence,
} from "./words";
import { RestoreDetail } from "./RestoreDetail";

configure({ asyncUtilTimeout: 10_000 });
vi.setConfig({ testTimeout: 30_000 });

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
  it("the plan line readers agree with ui/render.js on block, flow and commented plans", () => {
    const plans = [
      (R(plainRestore) as { planBytes?: string }).planBytes ?? "",
      'restore: {point_in_time: "2026-09-07T14:05:00Z", time_basis: producerTime}\nevidence:\n  bucket: "logweir-evidence"\n',
      "restore:\n  point_in_time: x\n  # time_basis: producerTime\nevidence:\n    bucket: Bad_Bucket\n",
      "restore:\n  time_basis: 'producerTime' # opted in\n",
      "",
    ];
    for (const plan of plans) {
      expect(planTimeBasis(plan)).toBe(oldRender.planTimeBasis(plan));
      expect(planEvidenceBucket(plan)).toBe(oldRender.planEvidenceBucket(plan));
      expect(planBlockValue(plan, "evidence", "bucket")).toBe(oldRender.planBlockValue(plan, "evidence", "bucket"));
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
