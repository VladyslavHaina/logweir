// @vitest-environment jsdom
// Protection: the age against the objective is the Overview's derivation, the
// three statuses of Protected stay apart, and an evaluated Unknown is never
// "not evaluated yet". Fixtures are the API's (ui/tests/fixtures/console/).
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, render, screen, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Outlet, RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import { ProgressVariant } from "@patternfly/react-core";
import type { components } from "../../api/schema";
import { ageVsObjective } from "../../app/policyAge";
import { scheduleRoutes } from "../schedules/routes";
import { openProblems, protectedWords } from "./Protection";
import listFixture from "../../../../ui/tests/fixtures/console/protection-policies-list.json";
import healthy from "../../../../ui/tests/fixtures/console/protection-policy.json";
import unknown from "../../../../ui/tests/fixtures/console/protection-policy-unknown.json";

type P = components["schemas"]["ProtectionPolicyView"];
configure({ asyncUtilTimeout: 5000 });
// A PatternFly form in jsdom is slow to render on a loaded host: allow a whole flow 30 s.
vi.setConfig({ testTimeout: 30_000 });

const net = vi.hoisted(() => ({ answer: (_path: string): unknown => undefined }));
vi.mock("../../mock/fetch", () => ({
  mockFetch: async (request: Request) => {
    const path = new URL(request.url).pathname.replace(/^.*?\/api\/v1/, "");
    const body = net.answer(path);
    return new Response(JSON.stringify(body ?? { code: "not_found", detail: "no fake" }), { status: body ? 200 : 404 });
  },
}));

function renderAt(path: string) {
  const root = createRootRoute({ component: Outlet });
  const ns = createRoute({ getParentRoute: () => root, path: "$ns", component: Outlet });
  const router = createRouter({
    routeTree: root.addChildren([ns.addChildren([...scheduleRoutes(ns as never)])]),
    history: createMemoryHistory({ initialEntries: [path] }),
  });
  render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  );
}
afterEach(cleanup);

const policy = (over: Partial<P>): P => ({ ...(healthy.item as unknown as P), ...over });

describe("age against the objective", () => {
  it("is the Overview's derivation: under, near and past the objective, and no point at all", () => {
    const objectives = { ...(healthy.item.objectives as P["objectives"]), maxRecoveryPointAgeSeconds: 3600 };
    const at = (ageSeconds?: number) => ageVsObjective(policy({ objectives, lastAvailablePoint: ageSeconds === undefined ? undefined : { ageSeconds, topicsTruncated: false } }));
    expect(at(1800)).toMatchObject({ percent: 50, variant: ProgressVariant.success, text: "30 min of 60 min" });
    expect(at(2880)).toMatchObject({ percent: 80, variant: ProgressVariant.warning });
    expect(at(7200)).toMatchObject({ percent: 100, variant: ProgressVariant.danger });
    // Negative control: no point is never a young one.
    expect(at(undefined)).toMatchObject({ percent: 100, variant: ProgressVariant.danger, text: "No recovery point" });
  });
});

describe("the policy's statuses", () => {
  it("keeps Protected=Unknown apart from False", () => {
    expect(protectedWords(healthy.item as unknown as P)).toMatchObject({ tone: "success" });
    expect(protectedWords(unknown.item as unknown as P)).toMatchObject({ tone: "neutral", text: "Could not be evaluated" });
    const list = listFixture.items as unknown as P[];
    expect(protectedWords(list[0]!)).toMatchObject({ tone: "danger" });
    expect(protectedWords(policy({ conditions: [] }))).toBeNull();
  });

  it("counts only open problems", () => {
    expect(openProblems(listFixture.items[0] as unknown as P)).toBe(1);
    // Negative control: a resolved one is history, not an open problem.
    expect(openProblems(healthy.item as unknown as P)).toBe(0);
  });

  it("an evaluated Unknown is shown as Unknown, never as not evaluated yet", async () => {
    net.answer = (path) => (path === "/namespaces/team-a/protection-policies/protect-unknown" ? unknown : undefined);
    renderAt("/team-a/protection/protect-unknown");
    const header = await screen.findByRole("heading", { level: 1 });
    const region = header.closest("section")!;
    await within(region).findByText("Unknown");
    expect(within(region).queryByText("Not evaluated yet")).toBeNull();
    expect(within(region).getByText("Could not be evaluated")).toBeTruthy();
  });

  it("the list draws each policy's age from the shared derivation", async () => {
    const items = [policy({ name: "p-ok", objectives: { ...(healthy.item.objectives as P["objectives"]), maxRecoveryPointAgeSeconds: 7200 }, lastAvailablePoint: { ageSeconds: 3600, topicsTruncated: false } })];
    net.answer = (path) => (path === "/namespaces/team-a/protection-policies" ? { items, page: { limit: 50 }, requestId: "r" } : undefined);
    renderAt("/team-a/protection");
    const bar = await screen.findByRole("progressbar", { name: /p-ok: newest recovery point/ });
    expect(bar.getAttribute("aria-label")).toBe(`p-ok: newest recovery point ${ageVsObjective(items[0]!).text}`);
    expect(bar.getAttribute("aria-valuenow")).toBe("50");
  });
});
