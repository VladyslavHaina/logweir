// DUI-16: one page that throws while rendering shows its own error in its place;
// the masthead and the navigation stay, and the rest of the console still works.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import localAdmin from "../../fixtures/console/session.json";
import { readSession } from "../api/client";
import type { Session } from "../api/types";
import { SessionGate } from "../app/SessionGate";
import { createConsoleRouter } from "../router";

vi.mock("../api/client", async (original) => ({ ...(await original<typeof import("../api/client")>()), readSession: vi.fn() }));
vi.mock("../pages/restores/RestoreList", async (original) => ({
  ...(await original<typeof import("../pages/restores/RestoreList")>()),
  Restores: () => {
    throw new RangeError("Invalid time value");
  },
}));

beforeEach(() => {
  window.matchMedia ??= ((query: string) => ({ matches: false, media: query, addEventListener: () => undefined, removeEventListener: () => undefined })) as unknown as typeof window.matchMedia;
  vi.mocked(readSession).mockResolvedValue(localAdmin as Session);
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function renderAt(path: string) {
  window.history.replaceState(null, "", `/#${path}`);
  render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <SessionGate>{(session) => <RouterProvider router={createConsoleRouter()} context={{ session }} />}</SessionGate>
    </QueryClientProvider>,
  );
}

describe("a page that cannot be shown", () => {
  it("says so in the page's place and keeps the navigation", async () => {
    renderAt("/team-a/restores");
    expect(await screen.findByRole("heading", { name: "This page could not be shown" })).toBeTruthy();
    expect(screen.getByText("Invalid time value")).toBeTruthy();
    expect(screen.getByRole("navigation", { name: "Main", hidden: true })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Try again" })).toBeTruthy();
  });

  it("leaves every other page alone (the control)", async () => {
    renderAt("/team-a");
    expect(await screen.findByRole("heading", { name: "Overview" })).toBeTruthy();
    expect(screen.queryByRole("heading", { name: "This page could not be shown" })).toBeNull();
  });
});
