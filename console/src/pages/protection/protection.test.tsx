// @vitest-environment jsdom
// Protection: the age against the objective is the Overview's derivation, the
// three statuses of Protected stay apart, and an evaluated Unknown is never
// "not evaluated yet". Fixtures are the API's (console/fixtures/console/).
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, render, screen, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Outlet, RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import { ProgressVariant } from "@patternfly/react-core";
import type { components } from "../../api/schema";
import { ageVsObjective, historyTooLarge } from "../../app/policyAge";
import { attentionItems } from "../../app/attention";
import { HealthStatus } from "../../components/Status";
import { SessionProvider } from "../../api/session";
import type { Session } from "../../api/types";
import { scheduleRoutes } from "../schedules/routes";
import sessionDoc from "../../../fixtures/console/session.json";
import { attemptWords, failedWords, ledgerOf, openProblems, protectedWords } from "./Protection";
import listFixture from "../../../fixtures/console/protection-policies-list.json";
import healthy from "../../../fixtures/console/protection-policy.json";
import unknown from "../../../fixtures/console/protection-policy-unknown.json";

type P = components["schemas"]["ProtectionPolicyView"];
configure({ asyncUtilTimeout: 5000 });
// A PatternFly form in jsdom is slow to render on a loaded host: allow a whole flow 30 s.
vi.setConfig({ testTimeout: 30_000 });

const net = vi.hoisted(() => ({ answer: (() => undefined) as (path: string) => unknown }));
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
      <SessionProvider session={sessionDoc as unknown as Session}>
        <RouterProvider router={router} />
      </SessionProvider>
    </QueryClientProvider>,
  );
}
afterEach(cleanup);

const policy = (over: Partial<P>): P => ({ ...(healthy.item as unknown as P), ...over });

describe("age against the objective", () => {
  it("is measured when the page reads it, from the point's capture start (the Overview's rule)", () => {
    const now = Date.parse("2026-10-13T12:00:00Z");
    const objectives = { ...(healthy.item.objectives as P["objectives"]), maxRecoveryPointAgeSeconds: 3600 };
    const captured = policy({ objectives, lastAvailablePoint: { recoveryPointAt: "2026-10-13T11:30:00Z", ageSeconds: 60, topicsTruncated: false } });
    expect(ageVsObjective(captured, now)).toMatchObject({ percent: 50, text: "30 min of 60 min" });
    // Control: the controller's own age stands in only when no capture start is recorded.
    const aged = policy({ objectives, lastAvailablePoint: { ageSeconds: 60, topicsTruncated: false } });
    expect(ageVsObjective(aged, now)).toMatchObject({ percent: 2 });
  });

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

describe("DUI-31.6: the last attempt's words", () => {
  it("say a reason without a phase alone, never beside \"Not recorded\"", () => {
    expect(attemptWords({ phase: null, reason: "GuardRefused" })).toBe("GuardRefused");
    expect(attemptWords({ phase: "Failed", reason: "GuardRefused" })).toBe("Failed (GuardRefused)");
    expect(attemptWords({ phase: "Succeeded" })).toBe("Succeeded");
    // Control: neither is recorded.
    expect(attemptWords({})).toBe("Not recorded");
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

  it("the shared health label keeps an evaluated Unknown apart from a policy never evaluated", () => {
    render(<HealthStatus health="Unknown" />);
    expect(screen.getByText("Unknown")).toBeTruthy();
    expect(screen.queryByText("Not evaluated yet")).toBeNull();
    cleanup();
    // Negative control: an absent health is the one that reads "Not evaluated yet".
    render(<HealthStatus health={undefined} />);
    expect(screen.getByText("Not evaluated yet")).toBeTruthy();
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

describe("FX-38: a Backup listing cut at its bound (BackupHistoryTooLarge)", () => {
  const REMEDY =
    "the namespace holds more Backups than one evaluation reads (5 pages of 200), and they are listed by name, so the newest runs may be the ones not read; delete old terminal Backup objects (the archive keeps their points) and the policy evaluates again";
  const cut = (): P =>
    policy({
      name: "protect-cut",
      health: "Unknown",
      lastAvailablePoint: undefined,
      lastAttempt: undefined,
      consecutiveFailedRuns: undefined,
      conditions: [{ type: "Protected", status: "Unknown", reason: "BackupHistoryTooLarge", message: REMEDY }],
    });

  it("reads the point, the attempt and the streak as not known, never as none or 0", () => {
    expect(historyTooLarge(cut())).toBe(true);
    expect(ageVsObjective(cut())).toMatchObject({ text: "Not known", percent: 0 });
    expect(ageVsObjective(cut()).variant).toBeUndefined();
    expect(failedWords(cut())).toMatch(/^Not known \(/);
    // CONTROLS: the same absent point on another reason is no recovery point, and a recorded 0 is 0.
    const none = policy({ lastAvailablePoint: undefined });
    expect(historyTooLarge(none)).toBe(false);
    expect(ageVsObjective(none)).toMatchObject({ text: "No recovery point", variant: ProgressVariant.danger });
    expect(failedWords(policy({ consecutiveFailedRuns: 0 }))).toMatch(/^0 \(/);
  });

  it("the attention item gives the controller's message, not \"no recovery point\"", () => {
    const [item] = attentionItems("team-a", [cut()], [], []);
    expect(item!.detail).toBe(REMEDY);
    expect(item!.title).toBe("protect-cut could not be evaluated");
  });

  it("the page shows the message with its remedy, and not known for the point, the attempt and the streak", async () => {
    net.answer = (path) => (path === "/namespaces/team-a/protection-policies/protect-cut" ? { item: cut(), requestId: "r" } : undefined);
    renderAt("/team-a/protection/protect-cut");
    await screen.findByText("Could not be evaluated: too many Backup objects to read");
    const body = () => document.body.textContent ?? "";
    expect(body()).toContain(REMEDY);
    expect(body()).toContain("neither healthy nor a failure");
    expect(body()).toMatch(/Not known: the controller could not read every Backup/);
    expect(body()).toContain("Not known (");
    expect(body()).not.toContain("None yet");
    expect(body()).not.toContain("There is no recovery point this objective can use");
    expect(body()).not.toMatch(/\b0 \(\d+ allowed\)/);
  });
});

describe("DUI-12: the ledger", () => {
  it("lists a finished recovery apart from the problems, and never counts it as one", () => {
    const alerts = [
      { key: "a", kind: "RecoveryCompleted", state: "Resolved", openedAt: "2026-10-12T03:00:00Z" },
      { key: "b", kind: "BackupFailure", state: "Open", openedAt: "2026-10-14T03:00:00Z" },
    ];
    const ledger = ledgerOf({ alerts } as never);
    expect(ledger.problems.map((a) => a.kind)).toEqual(["BackupFailure"]);
    expect(ledger.recoveries.map((a) => a.kind)).toEqual(["RecoveryCompleted"]);
    // The control: an open BackupFailure is a problem; the recovery adds none.
    expect(openProblems({ alerts } as never)).toBe(1);
    expect(openProblems({ alerts: [alerts[0]] } as never)).toBe(0);
  });
});
