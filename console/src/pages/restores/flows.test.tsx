// @vitest-environment jsdom
// The restore wizard as an operator meets it, on the mock the demo build runs: the plan
// sent is the plan reviewed, one plan makes one object however often it is sent, the
// readiness gate, approval routing (confirm with OD-10's typed names, two-person with
// its ticket), and a refusal or a lost answer said where it belongs. The typed client's
// fetch is the real mock with a recorder in front, so every request is seen.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { SessionGate } from "../../app/SessionGate";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Outlet, RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import type { components } from "../../api/schema";
import { RestoreWizard, restoreSearch } from "./RestoreWizard";
import { RestoreDetail } from "./RestoreDetail";

type S = components["schemas"];

configure({ asyncUtilTimeout: 15_000 });
vi.setConfig({ testTimeout: 90_000 });

const net = vi.hoisted(() => ({
  sent: [] as { method: string; path: string; key: string | null; body: unknown }[],
  /** Answer a request instead of the mock (return null to let the mock answer). */
  override: null as null | ((path: string, method: string, body: unknown) => Response | "lost" | null),
}));

vi.mock("../../mock/fetch", async (importOriginal) => {
  const real = await importOriginal<typeof import("../../mock/fetch")>();
  return {
    mockFetch: async (request: Request) => {
      const url = new URL(request.url);
      const path = url.pathname.replace(/^.*?\/api\/v1/, "");
      const text = request.method === "GET" ? "" : await request.clone().text();
      const body = text ? JSON.parse(text) : undefined;
      net.sent.push({ method: request.method, path, key: request.headers.get("Idempotency-Key"), body });
      const answer = net.override?.(path, request.method, body) ?? null;
      if (answer === "lost") throw new TypeError("Failed to fetch");
      if (answer) return answer;
      return real.mockFetch(request);
    },
  };
});

const modes = () => (globalThis as unknown as { __lwMockApprovalMode: Record<string, string> }).__lwMockApprovalMode;

function renderAt(path: string) {
  const root = createRootRoute({ component: Outlet });
  const ns = createRoute({ getParentRoute: () => root, path: "$ns", component: Outlet });
  const tree = root.addChildren([
    ns.addChildren([
      createRoute({ getParentRoute: () => ns, path: "restores/new", component: RestoreWizard, validateSearch: restoreSearch }),
      createRoute({ getParentRoute: () => ns, path: "restores/$name", component: RestoreDetail }),
    ]),
  ]);
  const router = createRouter({ routeTree: tree, history: createMemoryHistory({ initialEntries: [path] }) });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <SessionGate>{() => <RouterProvider router={router} />}</SessionGate>
    </QueryClientProvider>,
  );
  return router;
}

const next = async () => {
  const button = screen.getByRole("button", { name: "Next" });
  await waitFor(() => expect((button as HTMLButtonElement).disabled).toBe(false));
  fireEvent.click(button);
};

/** From the point to the review, running the readiness check on the way. */
async function toReview(change?: { prefix?: string; original?: boolean }) {
  await screen.findByText("Bound to", {}, { timeout: 15_000 });
  await next();
  await screen.findByRole("heading", { name: "What to restore" });
  await next();
  await screen.findByRole("heading", { name: "Where to restore" });
  if (change?.prefix !== undefined) fireEvent.change(screen.getByLabelText("Prefix"), { target: { value: change.prefix } });
  if (change?.original) {
    fireEvent.click(screen.getByLabelText("The original names"));
    fireEvent.click(await screen.findByLabelText(/No declarative owner/));
  }
  await next();
  await screen.findByRole("heading", { name: /ready\?/ });
  expect((screen.getByRole("button", { name: "Next" }) as HTMLButtonElement).disabled).toBe(true);
  fireEvent.click(screen.getByRole("button", { name: "Run the readiness check" }));
  await screen.findByText(/Ready for this draft/, {}, { timeout: 15_000 });
  await next();
  await screen.findByRole("heading", { name: "Review and create" });
}

const posts = () => net.sent.filter((r) => r.method === "POST" && r.path.endsWith("/restores"));

async function sha256(text: string) {
  const d = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return `sha256:${Array.from(new Uint8Array(d), (b) => b.toString(16).padStart(2, "0")).join("")}`;
}

beforeEach(() => {
  net.sent.length = 0;
  net.override = null;
  modes()["team-a"] = "confirm";
});
afterEach(() => cleanup());

describe("the restore wizard", () => {
  it("sends the reviewed plan unchanged, and two clicks make one restore with one key", async () => {
    const router = renderAt("/team-a/restores/new");
    await toReview();
    const create = screen.getByRole("button", { name: "Create restore" });
    fireEvent.click(create);
    fireEvent.click(create);
    await waitFor(() => expect(router.state.location.pathname).toMatch(/^\/team-a\/restores\/rst-/), { timeout: 15_000 });
    const sent = posts();
    expect(sent.length).toBeGreaterThanOrEqual(1);
    expect(new Set(sent.map((s) => s.key)).size).toBe(1);
    const body = sent[0]!.body as S["CreateRestoreRequest"];
    expect(body.planHash).toBe(await sha256(body.planBytes));
    expect(sent[0]!.key).toBe(`logweir-ui.restores.team-a.restore-${body.planHash.slice(7, 15)}`);
    expect(body.approvalRef.name).toBe(`approval-${body.planHash.slice(7, 15)}`);
    // The plan the readiness check was bound to is the plan sent.
    const check = net.sent.find((r) => r.method === "POST" && r.path.endsWith("/preflights"))!.body as S["CreatePreflightRequest"];
    expect(check.restore?.planBytes).toBe(body.planBytes);
    // One restore exists for it however many times it was sent: the second answer replays the first.
    const list = (await (await import("../../mock/fetch")).mockFetch(new Request("https://logweir.invalid/api/v1/namespaces/team-a/restores"))).json() as Promise<{ items: S["Restore"][] }>;
    expect((await list).items.filter((r) => r.planHash === body.planHash)).toHaveLength(1);
  });

  it("theme A's D3: a create refused for a taken approval name shows the API's sentence, and that nothing was created", async () => {
    const DETAIL =
      "An Approval named approval-1234abcd already exists in team-a and was not made for this request: it binds another object, and it is never adopted. Nothing was created. Submit this Restore with another approval name.";
    // The demo's first row already made a restore of this plan, so its check answers ready
    // for the draft here, bound to the plan it was asked about (as the OD-10 row does).
    let item: unknown = null;
    net.override = (path, method, body) => {
      const answer = (status: number) => new Response(JSON.stringify({ item, requestId: "r" }), { status, headers: { "content-type": "application/json" } });
      if (method === "POST" && path === "/namespaces/team-a/restores") {
        return new Response(JSON.stringify({ type: "https://logweir.dev/problems/state-conflict", title: "state_conflict", status: 409, code: "state_conflict", detail: DETAIL, requestId: "r-d3", retryable: false }), {
          status: 409,
          headers: { "content-type": "application/problem+json" },
        });
      }
      if (method === "GET" && path.endsWith("/preflights/pf-d3")) return answer(200);
      if (method !== "POST" || !path.endsWith("/preflights")) return null;
      const r = (body as S["CreatePreflightRequest"]).restore!;
      item = {
        id: "pf-d3", namespace: "team-a", uid: "u", resourceVersion: "1", operation: "restore", state: "unknown", terminal: true, applicable: true,
        stale: false, staleBasis: [], staleReasons: [], binding: { planHash: r.planHash, referents: [] }, warnings: [], executionOnly: [], conditions: [], detailsAvailable: false,
        checks: [
          { id: "target.mappedTopics", code: "MappedTopicsAbsent", state: "ready", gating: "blocking" },
          { id: "approval.state", code: "SubjectNotCreated", state: "skipped", gating: "blocking" },
        ],
      };
      return answer(201);
    };
    const router = renderAt("/team-a/restores/new");
    await toReview();
    fireEvent.click(screen.getByRole("button", { name: "Create restore" }));
    const title = await screen.findByText("The restore was not created");
    const alert = title.closest(".pf-v6-c-alert") as HTMLElement;
    expect(alert.textContent).toContain(DETAIL);
    expect(alert.textContent).toContain("409 state_conflict");
    expect(alert.textContent).not.toMatch(/The answer was lost/);
    expect(router.state.location.pathname).toBe("/team-a/restores/new");
  });

  it("OD-10: under one-person confirmation the original names are re-typed before anything is sent", async () => {
    // The demo's orders-dr already holds these names (the mock refuses MappedTopicExists),
    // so this check answers ready for the draft, bound to the plan it was asked about.
    let item: unknown = null;
    net.override = (path, method, body) => {
      const answer = (status: number) => new Response(JSON.stringify({ item, requestId: "r" }), { status, headers: { "content-type": "application/json" } });
      if (method === "GET" && path.endsWith("/preflights/pf-od10")) return answer(200);
      if (method !== "POST" || !path.endsWith("/preflights")) return null;
      const r = (body as S["CreatePreflightRequest"]).restore!;
      item = {
        id: "pf-od10", namespace: "team-a", uid: "u", resourceVersion: "1", operation: "restore", state: "unknown", terminal: true, applicable: true,
        stale: false, staleBasis: [], staleReasons: [], binding: { planHash: r.planHash, referents: [] }, warnings: [], executionOnly: [], conditions: [], detailsAvailable: false,
        checks: [
          { id: "target.mappedTopics", code: "MappedTopicsAbsent", state: "ready", gating: "blocking" },
          { id: "approval.state", code: "SubjectNotCreated", state: "skipped", gating: "blocking" },
        ],
      };
      return answer(201);
    };
    renderAt("/team-a/restores/new");
    await toReview({ original: true });
    const field = await screen.findByLabelText(/Re-type the original topic names/);
    fireEvent.click(screen.getByRole("button", { name: "Create restore" }));
    await screen.findByText(/re-type every original topic name/);
    expect(posts()).toHaveLength(0);
    fireEvent.change(field, { target: { value: "orders\norders-dlq\npayments" } });
    fireEvent.click(screen.getByRole("button", { name: "Create restore" }));
    await waitFor(() => expect(posts()).toHaveLength(1));
    const body = posts()[0]!.body as S["CreateRestoreRequest"];
    expect(body.originalNameConfirmation).toEqual({ typedTopics: ["orders", "orders-dlq", "payments"] });
    expect(body.target.topicNaming).toEqual({ prefix: "", originalName: true });
    expect(body.coverage).toBe("complete");
  });

  it("two-person: no typed names, a ticket is required, and the restore waits for a second person", async () => {
    modes()["team-a"] = "two-person";
    const router = renderAt("/team-a/restores/new");
    await toReview({ prefix: "pair-" });
    expect(screen.queryByLabelText(/Re-type the original topic names/)).toBeNull();
    await screen.findByText(/Two-person: a second person approves in the console/);
    fireEvent.click(screen.getByRole("button", { name: "Create restore" }));
    await screen.findByText(/requires a change ticket/);
    expect(posts()).toHaveLength(0);
    fireEvent.change(screen.getByLabelText(/Change ticket/), { target: { value: "CHG-4711" } });
    fireEvent.click(screen.getByRole("button", { name: "Create restore" }));
    await waitFor(() => expect(router.state.location.pathname).toMatch(/\/restores\/rst-/), { timeout: 15_000 });
    expect((posts()[0]!.body as S["CreateRestoreRequest"]).ticket).toBe("CHG-4711");
    await screen.findByText("Waiting for a second person to approve");
    // The restore's own approval page (R6), held to this restore by its plan hash and approval name.
    const sent = posts()[0]!.body as S["CreateRestoreRequest"];
    const link = screen.getByRole("link", { name: "Open its approval" }).getAttribute("href") ?? "";
    expect(link).toMatch(/\/team-a\/approvals\/rst-[a-z0-9-]+\?/);
    expect(new URLSearchParams(link.split("?")[1]).get("hash")).toBe(sent.planHash);
    expect(new URLSearchParams(link.split("?")[1]).get("approval")).toMatch(/^approval-/);
  });

  it("an API field error lands beside its field, and a lost answer resends the same key", async () => {
    let calls = 0;
    net.override = (path, method) => {
      if (method !== "POST" || !path.endsWith("/restores")) return null;
      calls += 1;
      if (calls === 1) return "lost";
      if (calls === 2) {
        return new Response(
          JSON.stringify({ status: 422, code: "validation_failed", title: "x", detail: "One or more fields are invalid.", requestId: "r1", retryable: false, type: "t", errors: [{ field: "target.topicNaming.prefix", code: "mapping_mismatch", message: "is not what the plan maps through" }] }),
          { status: 422, headers: { "content-type": "application/problem+json" } },
        );
      }
      return null;
    };
    renderAt("/team-a/restores/new");
    await toReview({ prefix: "lost-" });
    fireEvent.click(screen.getByRole("button", { name: "Create restore" }));
    await screen.findByText("The answer was lost");
    await screen.findByText(/Submitting again is safe/);
    fireEvent.click(screen.getByRole("button", { name: "Create restore" }));
    await screen.findByText(/is not what the plan maps through \(mapping_mismatch\)/);
    // The refusal opened the step its field is on.
    await screen.findByRole("heading", { name: "Where to restore" });
    const keys = posts().map((p) => p.key);
    expect(keys).toHaveLength(2);
    expect(keys[0]).toBe(keys[1]);
  });

  it("a check that expired while the plan was reviewed refuses Create before sending, and says so", async () => {
    let item: Record<string, unknown> | null = null;
    let expired = false;
    net.override = (path, method, body) => {
      const answer = (status: number) =>
        new Response(JSON.stringify({ item: expired ? { ...item, stale: true, staleReasons: [{ reason: "expired" }] } : item, requestId: "r" }), { status, headers: { "content-type": "application/json" } });
      if (method === "GET" && /\/preflights\/pf-expiring$/.test(path)) return answer(200);
      if (method !== "POST" || !path.endsWith("/preflights")) return null;
      item = {
        id: "pf-expiring", namespace: "team-a", uid: "u", resourceVersion: "1", operation: "restore", state: "unknown", terminal: true, applicable: true,
        stale: false, staleBasis: ["expiry"], staleReasons: [], binding: { planHash: (body as S["CreatePreflightRequest"]).restore!.planHash, referents: [] },
        warnings: [], executionOnly: [], conditions: [], detailsAvailable: false,
        checks: [
          { id: "target.mappedTopics", code: "MappedTopicsAbsent", state: "ready", gating: "blocking" },
          { id: "approval.state", code: "SubjectNotCreated", state: "skipped", gating: "blocking" },
        ],
      };
      return answer(201);
    };
    renderAt("/team-a/restores/new");
    await toReview({ prefix: "expiring-" });
    expired = true;
    fireEvent.click(screen.getByRole("button", { name: "Create restore" }));
    await screen.findByText("The restore was not created");
    expect(screen.getByText(/Nothing was sent: The readiness check no longer applies.*expired.*Run it again/)).toBeTruthy();
    // Nothing was sent, so nothing reads as a lost answer (the row above is the sent case).
    expect(screen.queryByText("The answer was lost")).toBeNull();
    expect(posts()).toHaveLength(0);
  });

  it("a check run for one plan does not let another plan through", async () => {
    renderAt("/team-a/restores/new");
    await toReview({ prefix: "first-" });
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    await screen.findByRole("heading", { name: /ready\?/ });
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    fireEvent.change(await screen.findByLabelText("Prefix"), { target: { value: "second-" } });
    await next();
    await screen.findByText(/something that changes the document changed since it ran/);
    expect((screen.getByRole("button", { name: "Next" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("a link to a Backup that is not a recovery point is refused by name, never replaced by another", async () => {
    renderAt("/team-a/restores/new?backup=orders-hourly-gone");
    await screen.findByText(/No Backup named orders-hourly-gone is in this namespace/);
    expect(screen.queryByText("Bound to")).toBeNull();
  });
});
