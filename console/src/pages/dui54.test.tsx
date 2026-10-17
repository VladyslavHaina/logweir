// @vitest-environment jsdom
// DUI-54 (deep UI pass v05, part 1): the small slips that have a cheap row. The catalog
// page follows its first sync (item 2), sign-out forgets the workspace (item 6), the
// short subject tells Dex local users apart (item 7), and Trust hides namespace rows
// that say nothing in a one-namespace install (item 9). Item 4 is in restores/flows.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { api, leaveAfterSignOut } from "../api/client";
import { useCatalog } from "../api/backups";
import { SessionProvider } from "../api/session";
import { session } from "../mock/data";
import { dexSubject, shortSubject } from "../app/subject";
import { namesNamespaces } from "./trust/Trust";
import catalogFixture from "../../fixtures/console/catalog.json";

vi.setConfig({ testTimeout: 40_000 });

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

function answer(data: unknown) {
  return { data, error: undefined, response: new Response(null, { status: 200 }) } as never;
}

describe("DUI-54.2: a catalog whose view is not built yet is re-read until a sync builds it", () => {
  it("reads again while viewExpired, and stops once the view is there", async () => {
    let reads = 0;
    vi.spyOn(api, "GET").mockImplementation((() => {
      reads += 1;
      const built = reads >= 2;
      return Promise.resolve(answer({ item: { ...catalogFixture.item, viewExpired: !built, syncedAt: built ? "2026-10-16T00:52:00Z" : null }, requestId: "r" }));
    }) as never);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={qc}>
        <SessionProvider session={session}>{children}</SessionProvider>
      </QueryClientProvider>
    );
    const { result } = renderHook(() => useCatalog("team-a", "primary"), { wrapper });
    await waitFor(() => expect(result.current.data?.viewExpired).toBe(true));
    await waitFor(() => expect(result.current.data?.viewExpired).toBe(false), { timeout: 12_000 });
    // Control: a built view is not polled.
    await new Promise((r) => setTimeout(r, 6000));
    expect(reads).toBe(2);
  });
});

describe("DUI-54.6: sign-out forgets the workspace in the address", () => {
  it("loads the console's root with no #/<workspace>", () => {
    const replace = vi.fn();
    leaveAfterSignOut({ pathname: "/ui/", search: "", replace } as unknown as Location);
    expect(replace).toHaveBeenCalledWith("/ui/");
  });
});

describe("DUI-54.7: the short subject tells Dex local users apart", () => {
  const dex = (id: string) => btoa(`\n${String.fromCharCode(id.length)}${id}\x12\x05local`);
  it("shows the end of the decoded user id and the connector", () => {
    const a = dex("1f0c9a6f-0505-4ed2-9d4e-000000000505");
    const b = dex("1f0c9a6f-0506-4ed2-9d4e-000000000506");
    expect(dexSubject(a)).toEqual({ userId: "1f0c9a6f-0505-4ed2-9d4e-000000000505", connector: "local" });
    expect(shortSubject(a)).toBe("…000000000505@local");
    expect(shortSubject(b)).not.toBe(shortSubject(a));
  });
  it("control: any other long subject keeps its ends, and a short one is whole", () => {
    expect(dexSubject("auth0|0123456789abcdef0123456789")).toBeNull();
    expect(shortSubject("auth0|0123456789abcdef0123456789")).toBe("auth0|0123…23456789");
    expect(shortSubject("alice")).toBe("alice");
  });
});

describe("DUI-54.9: Trust's namespace rows only where a policy names a namespace", () => {
  it("hides them for the one-namespace install's default policy, shows them when one is named", () => {
    expect(namesNamespaces({ namespaces: [], boundNamespaces: [], namespacesFiltered: false, namespacesTruncated: false })).toBe(false);
    expect(namesNamespaces({ namespaces: ["team-a"], boundNamespaces: [], namespacesFiltered: false, namespacesTruncated: false })).toBe(true);
    expect(namesNamespaces({ namespaces: [], boundNamespaces: [], namespacesFiltered: true, namespacesTruncated: false })).toBe(true);
  });
});
