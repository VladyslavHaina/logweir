// DUI-23: a kept clone reads as a clone, with when it goes, on the Restores list; a scratch
// rehearsal whose topics are already gone still reads as one.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import localAdmin from "../../../fixtures/console/session.json";
import { readSession } from "../../api/client";
import type { Restore, Session } from "../../api/types";
import type { Clone } from "../../api/switchovers";
import { SessionGate } from "../../app/SessionGate";
import { createConsoleRouter } from "../../router";
import { restoreFacts } from "./RestoreList";

vi.mock("../../api/client", async (original) => ({ ...(await original<typeof import("../../api/client")>()), readSession: vi.fn() }));
configure({ asyncUtilTimeout: 10_000 });

beforeEach(() => {
  window.matchMedia ??= ((query: string) => ({ matches: false, media: query, addEventListener: () => undefined, removeEventListener: () => undefined })) as unknown as typeof window.matchMedia;
  vi.mocked(readSession).mockResolvedValue(localAdmin as Session);
});
afterEach(cleanup);

describe("a clone on the Restores list", () => {
  const scratch = { target: { mode: "scratch", originalName: false, topicPrefix: "clone-" }, coverage: null, selection: null, operation: { terminal: true } } as unknown as Restore;

  it("says clone and when it is deleted, from the operation view's status.clone", () => {
    const clone = { phase: "Active", expiresAt: new Date(Date.now() + 45 * 60_000).toISOString() } as Clone;
    expect(restoreFacts(scratch, clone)).toMatch(/^clone, deletes in 45 min · /);
    // The control: a scratch restore with no clone is a rehearsal.
    expect(restoreFacts(scratch)).toMatch(/^scratch rehearsal · /);
  });

  it("reads each recent scratch restore's clone for the list (the demo's kept clone)", async () => {
    window.history.replaceState(null, "", "/#/team-a/restores");
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <SessionGate>{(session) => <RouterProvider router={createConsoleRouter()} context={{ session }} />}</SessionGate>
      </QueryClientProvider>,
    );
    expect((await screen.findAllByText(/^clone, (deletes|deleting|removed|not kept)/)).length).toBeGreaterThan(0);
  });
});
