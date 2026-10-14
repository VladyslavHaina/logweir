// The session gate over the whole console: what GET /api/v1/session answers decides the
// page, and the namespaces, roles and capability flags come from that document. The
// session reads are the API's recorded documents (console/fixtures/console/); the
// Overview's own reads go to the in-page mock, which holds team-a and team-b.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider, useQuery } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import localAdmin from "../../fixtures/console/session.json";
import viewer from "../../fixtures/console/session-viewer.json";
import { ApiError, api, readSession } from "../api/client";
import type { Session } from "../api/types";
import { createConsoleRouter } from "../router";
import { SessionGate } from "./SessionGate";
import { leftWords } from "./SessionClock";
import { makeQueryClient } from "./queryClient";
import { shortSubject } from "./subject";

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

// ------------------------------------------------- DUI-4, DUI-9, DUI-14

describe("DUI-9: the sign-in link", () => {
  it("follows the address when it changes in the same tab", async () => {
    read.mockRejectedValue(problem(401, "unauthenticated", "Sign in."));
    renderAt("/team-a");
    const signIn = await screen.findByRole("link", { name: "Sign in" });
    // The control: the address the page opened at.
    expect(signIn.getAttribute("href")).toBe(`/auth/login?next=${encodeURIComponent("/#/team-a")}`);
    act(() => {
      window.history.replaceState(null, "", "/#/team-a/backups");
      window.dispatchEvent(new HashChangeEvent("hashchange"));
    });
    expect(screen.getByRole("link", { name: "Sign in" }).getAttribute("href")).toBe(`/auth/login?next=${encodeURIComponent("/#/team-a/backups")}`);
  });
});

describe("DUI-4: the session's end", () => {
  const inAWhile = (ms: number) => new Date(Date.now() + ms).toISOString();
  const withExpiry = (ms: number) => ({ ...viewer, expiresAt: inAWhile(ms) }) as Session;

  /** A page with a half-typed form and one read that the test can make answer 401. */
  function renderForm(client = makeQueryClient()) {
    const state = { refuse: false };
    function Draft() {
      const probe = useQuery({
        queryKey: ["probe"],
        queryFn: async () => {
          if (state.refuse) throw problem(401, "session_expired", "The session has expired.");
          return "read";
        },
        retry: false,
      });
      return (
        <div>
          <input aria-label="Topic names" defaultValue="" />
          <span>{probe.isError ? "probe refused" : probe.data === "read" ? "probe read" : "probe pending"}</span>
          <button onClick={() => void probe.refetch()}>Read again</button>
        </div>
      );
    }
    render(
      <QueryClientProvider client={client}>
        <SessionGate>{() => <Draft />}</SessionGate>
      </QueryClientProvider>,
    );
    return state;
  }

  it("a 401 later keeps the page and what was typed under a blocking dialog, until the session reads again", async () => {
    read.mockResolvedValue(withExpiry(3_600_000));
    const state = renderForm();
    const input = (await screen.findByLabelText("Topic names")) as HTMLInputElement;
    fireEvent.change(input, { target: { value: "orders, payme" } });
    expect(await screen.findByText("probe read")).toBeTruthy();

    // The session ends under the page: the next read is refused, and so is the session's.
    read.mockRejectedValue(problem(401, "session_expired", "Expired."));
    state.refuse = true;
    fireEvent.click(screen.getByRole("button", { name: "Read again" }));
    const dialog = await screen.findByRole("dialog", { name: /Your session ended: sign in again in a new tab, then continue here/ });
    // The page is still there, with the half-typed value: no sign-in page replaced it.
    expect((screen.getByLabelText("Topic names", { selector: "input" }) as HTMLInputElement).value).toBe("orders, payme");
    expect(screen.queryByRole("heading", { name: "Your session has ended" })).toBeNull();
    const link = within(dialog).getByRole("link", { name: "Sign in again in a new tab" });
    expect([link.getAttribute("target"), link.getAttribute("rel")]).toEqual(["_blank", "noopener noreferrer"]);
    expect(link.getAttribute("href")).toMatch(/^\/auth\/login\?next=/);

    // Continue here before signing in elsewhere: still signed out, and said so.
    fireEvent.click(within(dialog).getByRole("button", { name: "Continue here" }));
    expect(await within(dialog).findByText(/Still signed out here/)).toBeTruthy();

    // Signed in in another tab: Continue here reads the session, the dialog goes, and the refused read is read again.
    read.mockResolvedValue(withExpiry(900_000));
    state.refuse = false;
    fireEvent.click(within(dialog).getByRole("button", { name: "Continue here" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect((screen.getByLabelText("Topic names", { selector: "input" }) as HTMLInputElement).value).toBe("orders, payme");
    expect(await screen.findByText("probe read")).toBeTruthy();
  });

  it("warns about two minutes before the end, offering a sign-in in a new tab; not ten minutes before", async () => {
    read.mockResolvedValue(withExpiry(90_000));
    renderForm();
    const warning = await screen.findByText(/Your session ends in 1 min (2|3)\d s/);
    expect(warning).toBeTruthy();
    const link = screen.getByRole("link", { name: "Sign in again in a new tab" });
    expect(link.getAttribute("target")).toBe("_blank");
    cleanup();
    // The control: ten minutes left is no warning.
    read.mockResolvedValue(withExpiry(600_000));
    renderForm();
    expect(await screen.findByLabelText("Topic names")).toBeTruthy();
    expect(screen.queryByText(/Your session ends in/)).toBeNull();
  });

  it("puts the words of the time left in minutes and seconds", () => {
    expect([leftWords(100_000), leftWords(45_000), leftWords(120_000), leftWords(-5)]).toEqual(["1 min 40 s", "45 s", "2 min", "0 s"]);
  });
});

describe("DUI-14: the user menu", () => {
  const LONG = "CiQxZjBjOWE0ZS0wMTAxLTRhMWUtOWUxYS0wMDAwMDAwMDAxMDESBWxvY2Fs";

  it("shortens an opaque subject, keeps Appearance below the page's h1, labels the roles, and closes on a choice", async () => {
    read.mockResolvedValue({ ...viewer, actor: { ...viewer.actor, displayName: "", subject: LONG } } as Session);
    renderAt("/team-a");
    expect(await screen.findByRole("heading", { name: "Overview" })).toBeTruthy();
    const short = shortSubject(LONG);
    expect(short.length).toBeLessThan(LONG.length);
    fireEvent.click(screen.getByRole("button", { name: `${short}: account and appearance` }));
    expect(await screen.findByRole("heading", { name: "Appearance", level: 2 })).toBeTruthy();
    expect(document.body.textContent).not.toContain(LONG);
    expect(screen.getByTitle(LONG)).toBeTruthy();
    expect(screen.getByRole("list", { name: "Roles in team-a" })).toBeTruthy();
    fireEvent.click(screen.getByRole("menuitem", { name: /Dark/ }));
    await waitFor(() => expect(screen.queryByRole("heading", { name: "Appearance" })).toBeNull());
  });

  it("shortens the subject on the no-namespace page too, whole in its title", async () => {
    read.mockResolvedValue({ ...viewer, namespaces: [], actor: { ...viewer.actor, subject: LONG } } as unknown as Session);
    renderAt("/");
    expect(await screen.findByRole("heading", { name: "No namespace is granted to you" })).toBeTruthy();
    expect(document.body.textContent).not.toContain(LONG);
    expect(screen.getByTitle(LONG).textContent).toContain(shortSubject(LONG));
  });
});
