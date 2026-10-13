// The session gate over the whole console: what GET /api/v1/session answers decides the
// page, and the namespaces, roles and capability flags come from that document. The
// session reads are the API's recorded documents (console/fixtures/console/); the
// Overview's own reads go to the in-page mock, which holds team-a and team-b.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import localAdmin from "../../fixtures/console/session.json";
import viewer from "../../fixtures/console/session-viewer.json";
import { ApiError, api, readSession } from "../api/client";
import type { Session } from "../api/types";
import { createConsoleRouter } from "../router";
import { SessionGate } from "./SessionGate";

vi.mock("../api/client", async (original) => ({ ...(await original<typeof import("../api/client")>()), readSession: vi.fn() }));
const read = vi.mocked(readSession);

const problem = (status: number, code: string, detail: string) =>
  new ApiError(status, { status, code: code as never, detail, requestId: "01REQ", retryable: true });

function renderAt(path: string) {
  // A page load at `path`: a fresh router reading the address, as main.tsx makes one.
  window.history.replaceState(null, "", `/#${path}`);
  const router = createConsoleRouter();
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <SessionGate>{(session) => <RouterProvider router={router} context={{ session }} />}</SessionGate>
    </QueryClientProvider>,
  );
  return router;
}

beforeEach(() => {
  // PatternFly and the console read the viewport; jsdom has no media queries.
  window.matchMedia ??= ((query: string) => ({
    matches: false,
    media: query,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
  })) as unknown as typeof window.matchMedia;
});
afterEach(() => {
  cleanup();
  read.mockReset();
});

describe("signed out", () => {
  it("hands over to the existing sign-in route, coming back to the route asked for", async () => {
    read.mockRejectedValue(problem(401, "unauthenticated", "Sign in."));
    renderAt("/team-a/backups");
    const signIn = await screen.findByRole("link", { name: "Sign in" });
    expect(screen.getByRole("heading", { name: "Sign in to Logweir" })).toBeTruthy();
    expect(signIn.getAttribute("href")).toBe(`/auth/login?next=${encodeURIComponent("/#/team-a/backups")}`);
    expect(screen.queryByRole("navigation")).toBeNull();
  });

  it("says when the session ended rather than never started", async () => {
    read.mockRejectedValue(problem(401, "session_expired", "Expired."));
    renderAt("/team-a");
    expect(await screen.findByRole("heading", { name: "Your session has ended" })).toBeTruthy();
  });
});

describe("unavailable", () => {
  it("shows what the read got and tries again", async () => {
    read.mockRejectedValueOnce(problem(503, "kubernetes_unavailable", "The Kubernetes API did not answer.")).mockResolvedValue(localAdmin as Session);
    renderAt("/team-a");
    expect(await screen.findByRole("heading", { name: "Can't reach the Logweir service" })).toBeTruthy();
    expect(screen.getByText(/The Kubernetes API did not answer\./)).toBeTruthy();
    expect(screen.getByText(/503 kubernetes_unavailable · request 01REQ/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(await screen.findByRole("heading", { name: "Overview" })).toBeTruthy();
    expect(read).toHaveBeenCalledTimes(2);
  });

  it("is not a sign-in page for a 401 that is not the API asking for one", async () => {
    read.mockRejectedValue(problem(401, "forbidden", "Something else."));
    renderAt("/team-a");
    expect(await screen.findByRole("heading", { name: "Can't reach the Logweir service" })).toBeTruthy();
  });
});

describe("signed in", () => {
  it("localAdmin: lands on its namespace, names the local administrator, offers no sign-out", async () => {
    read.mockResolvedValue(localAdmin as Session);
    const router = renderAt("/");
    expect(await screen.findByRole("heading", { name: "Overview" })).toBeTruthy();
    expect(router.state.location.pathname).toBe("/team-a");
    fireEvent.click(screen.getByRole("button", { name: "Local administrator: account and appearance" }));
    const menu = await screen.findByText("Every action (local administrator)");
    expect(menu).toBeTruthy();
    expect(screen.getByText(/admin · local administrator/)).toBeTruthy();
    expect(screen.queryByText("Sign out")).toBeNull();
    // Its grant has restoreCreate and not manualBackupCreate.
    expect(screen.getByRole("button", { name: "Restore" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Back up now" })).toBeNull();
  });

  it("takes the namespaces, the roles and the navigation from the session", async () => {
    const grant = viewer.namespaces[0]!;
    const session = {
      ...viewer,
      namespaces: [
        { ...grant, name: "team-b" },
        { ...grant, name: "team-a", roles: ["operator"] },
      ],
    } as Session;
    read.mockResolvedValue(session);
    const router = renderAt("/");
    expect(await screen.findByRole("heading", { name: "Overview" })).toBeTruthy();
    expect(router.state.location.pathname).toBe("/team-b");
    // jsdom lays nothing out, so PatternFly measures a phone-width page and closes the drawer.
    const nav = screen.getByRole("navigation", { name: "Main", hidden: true });
    expect(within(nav).getByText("Backups")).toBeTruthy();
    // trustPoliciesRead is false for a viewer: no Trust entry.
    expect(within(nav).queryByText("Trust")).toBeNull();
    // A viewer may neither back up nor restore.
    expect(screen.queryByRole("button", { name: "Restore" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Dana Viewer: account and appearance" }));
    expect(await screen.findByText("Roles in team-b")).toBeTruthy();
    expect(screen.getByText("viewer")).toBeTruthy();
    expect(screen.getByText("Sign out")).toBeTruthy();
  });

  it("says so for a namespace the session holds no role in", async () => {
    read.mockResolvedValue(localAdmin as Session);
    renderAt("/team-z");
    expect(await screen.findByText("You hold no role in team-z")).toBeTruthy();
    expect(screen.getByRole("link", { name: "team-a" }).getAttribute("href")).toBe("#/team-a");
  });

  it("says a page whose read the role lacks is not available, and sends nothing for it", async () => {
    const grant = viewer.namespaces[0]!;
    const approver = { ...viewer, namespaces: [{ ...grant, roles: ["approver"], capabilities: { ...grant.capabilities, connectionsRead: false } }] };
    read.mockResolvedValue(approver as Session);
    const fetches = vi.spyOn(api, "GET");
    renderAt("/team-a/connections");
    expect(await screen.findByRole("heading", { name: "Not available to you here" })).toBeTruthy();
    expect((fetches.mock.calls as unknown[][]).some((call) => String(call[0]).endsWith("/connections"))).toBe(false);
    cleanup();
    // Control: the viewer, who holds connectionsRead, gets the page and its read.
    read.mockResolvedValue(viewer as Session);
    renderAt("/team-a/connections");
    expect(await screen.findByRole("heading", { name: "Connections" })).toBeTruthy();
    expect(screen.queryByRole("heading", { name: "Not available to you here" })).toBeNull();
    expect((fetches.mock.calls as unknown[][]).some((call) => String(call[0]).endsWith("/connections"))).toBe(true);
  });

  it("names the actor and offers sign-out when no namespace is granted", async () => {
    read.mockResolvedValue({ ...viewer, namespaces: [] } as unknown as Session);
    renderAt("/");
    expect(await screen.findByRole("heading", { name: "No namespace is granted to you" })).toBeTruthy();
    expect(screen.getByText("Dana Viewer")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Sign out" })).toBeTruthy();
  });
});
