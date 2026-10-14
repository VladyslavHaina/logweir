// @vitest-environment jsdom
// DUI-24: a new page opens at its top. The shell's main region is the scroller, so a
// route change resets it; a change of search alone (a filter) keeps the place.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, configure, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import localAdmin from "../../fixtures/console/session.json";
import { readSession } from "../api/client";
import type { Session } from "../api/types";
import { SessionGate } from "./SessionGate";
import { createConsoleRouter } from "../router";

vi.mock("../api/client", async (original) => ({ ...(await original<typeof import("../api/client")>()), readSession: vi.fn() }));
configure({ asyncUtilTimeout: 10_000 });

beforeEach(() => {
  window.matchMedia ??= ((query: string) => ({ matches: false, media: query, addEventListener: () => undefined, removeEventListener: () => undefined })) as unknown as typeof window.matchMedia;
  vi.mocked(readSession).mockResolvedValue(localAdmin as Session);
});
afterEach(cleanup);

describe("DUI-24: the main region on a route change", () => {
  it("returns to the top for a new page, and stays put when only the search changes", async () => {
    window.history.replaceState(null, "", "/#/team-a/backups");
    const router = createConsoleRouter();
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <SessionGate>{(session) => <RouterProvider router={router} context={{ session }} />}</SessionGate>
      </QueryClientProvider>,
    );
    await screen.findByRole("heading", { name: "Backups", level: 1 });
    // jsdom has no layout: record what the shell writes to the scroller.
    const main = document.getElementById("lw-main")!;
    let top = 640;
    Object.defineProperty(main, "scrollTop", { configurable: true, get: () => top, set: (v: number) => (top = v) });
    // CONTROL: the same page with another filter keeps the reader's place.
    await act(() => router.navigate({ to: "/$ns/backups", params: { ns: "team-a" }, search: { filter: "unverified" } }));
    expect(top).toBe(640);
    await act(() => router.navigate({ to: "/$ns/connections", params: { ns: "team-a" } }));
    await screen.findByRole("heading", { name: "Connections", level: 1 });
    expect(top).toBe(0);
  });
});
