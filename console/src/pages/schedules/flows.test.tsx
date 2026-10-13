// @vitest-environment jsdom
// The schedule flows as an operator meets them: the requests the pages send
// (one click, one object; the same key on a resend), where a refusal lands,
// and what a login without the grant is offered. The network is a fake the
// typed client is handed in place of the mock, so every request is recorded.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Outlet, RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import type { components } from "../../api/schema";
import type { Schedule } from "../../api/schedules";
import { SessionGate } from "../../app/SessionGate";
import { scheduleRoutes } from "./routes";
import { BackUpNow } from "./BackUpNow";
import { NextRuns } from "./parts";
import repeated from "../../../../ui/tests/fixtures/console/cadence-preview-repeated.json";
import policyFixture from "../../../../ui/tests/fixtures/console/schedule-policy.json";

type S = components["schemas"];

// The form waits 350 ms for typing to stop before it asks for a preview; give
// every wait room on a loaded host.
configure({ asyncUtilTimeout: 5000 });
// A PatternFly form in jsdom is slow to render on a loaded host: allow a whole flow 30 s.
vi.setConfig({ testTimeout: 30_000 });

const net = vi.hoisted(() => ({
  sent: [] as { method: string; path: string; key: string | null; body: unknown }[],
  answer: (() => null) as (r: Request, path: string, body: unknown) => Response | Promise<Response> | null,
}));

vi.mock("../../mock/fetch", () => ({
  mockFetch: async (request: Request) => {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^.*?\/api\/v1/, "") + url.search;
    const text = request.method === "GET" ? "" : await request.text();
    const body = text ? JSON.parse(text) : undefined;
    net.sent.push({ method: request.method, path, key: request.headers.get("Idempotency-Key"), body });
    const r = await net.answer(request, path, body);
    if (r) return r;
    // The session gate's read, when a row's network does not answer it itself.
    if (path === "/session") return json(200, session(ALL));
    return json(404, { code: "not_found", detail: `no fake for ${request.method} ${path}` });
  },
}));

function json(status: number, body: unknown) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" } });
}

const ALL = {
  scheduleCreate: true,
  scheduleSetSuspension: true,
  manualBackupCreate: true,
  restoreCreate: true,
  preflight: true,
} as Partial<S["Capabilities"]>;

/** The reads every role here holds (the console sends no read the session does not grant). */
const READS = {
  schedulesRead: true,
  backupsRead: true,
  connectionsRead: true,
  destinations: true,
  topicDiscovery: true,
  retention: true,
  protection: true,
} as Partial<S["Capabilities"]>;

function session(caps: Partial<S["Capabilities"]>) {
  const all = { ...READS, ...caps };
  return { actor: { id: "a", displayName: "A", issuer: "i", subject: "s" }, authenticationMode: "oidc", bindingRevision: "1", capabilities: all, namespaces: [{ name: "team-a", roles: ["operator"], capabilities: all }], requestId: "r" };
}

const schedule = (over: Partial<Schedule> = {}): Schedule => ({ ...(policyFixture.item as unknown as Schedule), namespace: "team-a", name: "nightly", ...over });

/** A router with the R3 routes under `#/<ns>/`, starting at `path`. */
function renderAt(path: string) {
  const root = createRootRoute({ component: Outlet });
  const ns = createRoute({ getParentRoute: () => root, path: "$ns", component: Outlet });
  const router = createRouter({
    routeTree: root.addChildren([ns.addChildren([...scheduleRoutes(ns as never)])]),
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

/** One component inside a router (Links need one) and a query client. */
function renderWith(element: React.ReactNode) {
  const root = createRootRoute({ component: () => <>{element}</> });
  const router = createRouter({ routeTree: root, history: createMemoryHistory({ initialEntries: ["/"] }) });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <SessionGate>{() => <RouterProvider router={router} />}</SessionGate>
    </QueryClientProvider>,
  );
}

beforeEach(() => {
  net.sent = [];
  net.answer = () => null;
});
afterEach(cleanup);

const posts = () => net.sent.filter((s) => s.method === "POST");

const created = (key: string | null) =>
  json(201, {
    item: { name: `logweir-manual-${key!.slice(-6)}`, scheduleRef: { name: "nightly", generation: 1 }, operation: { state: "pending", terminal: false, verifiedSuccess: false, verificationState: "pending" } },
    replayed: false,
    schedule: { name: "nightly", generation: 1, suspended: false, uid: "u", activeRuns: [] },
    requestId: "r",
  });

describe("Back up now", () => {
  it("one click is one run under one key, and only Back up again starts a second", async () => {
    net.answer = (r) => (r.method === "POST" ? created(r.headers.get("Idempotency-Key")) : null);
    renderWith(<BackUpNow ns="team-a" schedule={schedule()} open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up now" }));
    await within(dialog).findByText("Backup started");
    expect(posts()).toHaveLength(1);
    expect(posts()[0]!.body).toEqual({ scheduleRef: { name: "nightly", expectedGeneration: 1 } });
    const first = posts()[0]!.key!;
    expect(first).toMatch(/^logweir-console\.manual\.[0-9a-f]{32}$/);
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up again" }));
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up now" }));
    await waitFor(() => expect(posts()).toHaveLength(2));
    // A deliberate second backup is a different request.
    expect(posts()[1]!.key).not.toBe(first);
  });

  it("after no answer, resending carries the SAME key, so the API answers with the first run", async () => {
    let calls = 0;
    net.answer = (r) => {
      if (r.method !== "POST") return null;
      calls += 1;
      if (calls === 1) throw new TypeError("Failed to fetch");
      return json(200, { ...JSON.parse("{}"), item: { name: "logweir-manual-x", scheduleRef: { name: "nightly", generation: 1 }, operation: { state: "pending", terminal: false, verifiedSuccess: false, verificationState: "pending" } }, replayed: true, requestId: "r" });
    };
    renderWith(<BackUpNow ns="team-a" schedule={schedule()} open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up now" }));
    await within(dialog).findByText("No answer: whether the backup was started is unknown");
    expect(within(dialog).getByText(/resends the same request key/)).toBeTruthy();
    fireEvent.click(within(dialog).getByRole("button", { name: "Send the same request again" }));
    await within(dialog).findByText("Already started");
    expect(posts()).toHaveLength(2);
    expect(posts()[1]!.key).toBe(posts()[0]!.key);
  });

  it("a policy change refuses with the revision in force and offers only a new request", async () => {
    net.answer = (r) =>
      r.method === "POST"
        ? json(409, { code: "policy_changed", status: 409, detail: "The schedule's policy changed.", policy: { currentGeneration: 7 }, requestId: "r" })
        : null;
    renderWith(<BackUpNow ns="team-a" schedule={schedule()} open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up now" }));
    await within(dialog).findByText(/it is at revision g7 now/);
    // Resending the refused request cannot help: the button is off until a new request is started.
    expect((within(dialog).getByRole("button", { name: "Back up now" }) as HTMLButtonElement).disabled).toBe(true);
    const first = posts()[0]!.key;
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up again" }));
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up now" }));
    await waitFor(() => expect(posts()).toHaveLength(2));
    expect(posts()[1]!.key).not.toBe(first);
  });

  it("a suspended schedule asks for a confirmation first, and is not refused", async () => {
    net.answer = (r) => (r.method === "POST" ? created(r.headers.get("Idempotency-Key")) : null);
    renderWith(<BackUpNow ns="team-a" schedule={schedule({ suspended: true })} open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    const button = within(dialog).getByRole("button", { name: "Back up now" }) as HTMLButtonElement;
    expect(button.disabled).toBe(true);
    fireEvent.click(within(dialog).getByLabelText(/Back up anyway; the schedule stays suspended/));
    expect(button.disabled).toBe(false);
    fireEvent.click(button);
    await within(dialog).findByText("Backup started");
  });
});

describe("next runs", () => {
  it("shows both runs of a repeated local hour, told apart by their offsets", () => {
    renderWith(<NextRuns runs={repeated.runs as S["NextRunView"][]} zone="Europe/Berlin" now={Date.parse("2026-10-24T12:00:00Z")} label="Next runs" />);
    return screen.findByRole("grid", { name: "Next runs" }).then((table) => {
      const rows = within(table).getAllByRole("row").slice(1);
      expect(rows).toHaveLength(3);
      expect(rows[0]!.textContent).toContain("2026-10-25 02:30 +02:00");
      expect(rows[0]!.textContent).toContain("the first of two runs");
      expect(rows[1]!.textContent).toContain("2026-10-25 02:30 +01:00");
      expect(rows[1]!.textContent).toContain("the second of two runs");
      // Negative control: an ordinary day carries no clock-change words.
      expect(rows[2]!.textContent).toContain("None");
    });
  });

  it("an absent list is 'not computed', an empty one 'no further run', a past first entry 'out of date'", async () => {
    renderWith(
      <>
        <NextRuns runs={undefined} zone="UTC" label="a" />
        <NextRuns runs={[]} zone="UTC" label="b" />
        <NextRuns runs={[{ at: "2026-01-01T00:00:00Z", localTime: "2026-01-01T00:00:00+00:00" }]} zone="UTC" now={Date.parse("2026-02-01T00:00:00Z")} label="c" />
      </>,
    );
    expect(await screen.findByText(/Not computed yet/)).toBeTruthy();
    expect(screen.getByText(/No further run/)).toBeTruthy();
    expect(screen.getByText("These runs are out of date")).toBeTruthy();
  });
});

// ------------------------------------------------------------ the form

const CONNECTIONS = [{ name: "orders-prod", uid: "c-1", namespace: "team-a", role: "source", reachability: { state: "reachable" } }];
const DESTINATIONS = [{ name: "primary", uid: "d1", default: true, canonicalUrl: "s3://b/p", generation: 1, addressing: "pathStyle", transport: "tls", status: {} }];

function formNetwork(over: (r: Request, path: string, body: unknown) => Response | null = () => null, caps = ALL) {
  net.answer = (r, path, body) => {
    const o = over(r, path, body);
    if (o) return o;
    if (path === "/session") return json(200, session(caps));
    if (path.startsWith("/namespaces/team-a/connections?") || path === "/namespaces/team-a/connections") return json(200, { items: CONNECTIONS, page: { limit: 50 }, requestId: "r" });
    if (path.startsWith("/namespaces/team-a/destinations")) return json(200, { items: DESTINATIONS, page: { limit: 50 }, requestId: "r" });
    if (path.includes("/topic-discoveries")) return json(200, { requestId: "r" });
    if (path.startsWith("/cadence-previews")) {
      const q = new URL(r.url).searchParams;
      if (q.get("timeZone") === "Europe/Atlantis") {
        return json(422, { code: "validation_failed", status: 422, detail: "invalid", errors: [{ field: "timeZone", code: "timezone_unknown", message: '"Europe/Atlantis" is not a time zone' }], requestId: "r" });
      }
      // A marker the console cannot have produced itself: the body must carry exactly this.
      return json(200, { after: "2026-10-13T00:00:00Z", schedule: "0 2 * * * ", timeZone: q.get("timeZone") ?? "UTC", tzdb: "chrono-tz 0.10.4", runs: [], requestId: "r" });
    }
    return null;
  };
}

async function fillGuided() {
  await screen.findByRole("option", { name: /orders-prod/ });
  fireEvent.change(screen.getByLabelText(/Source connection/), { target: { value: "orders-prod" } });
  fireEvent.change(screen.getByLabelText(/Topic names/), { target: { value: "orders, payments" } });
}

describe("guided create", () => {
  it("saves the line the API compiled, once the preview of these values is back", async () => {
    formNetwork((r) => (r.method === "POST" ? json(201, { item: { ...schedule(), name: "sch-abc" }, replayed: false, requestId: "r" }) : null));
    renderAt("/team-a/schedules/new");
    await fillGuided();
    const create = await screen.findByRole("button", { name: "Create schedule" });
    await screen.findByText(/Saved as/, {}, { timeout: 3000 });
    await waitFor(() => expect((create as HTMLButtonElement).disabled).toBe(false));
    fireEvent.click(create);
    await waitFor(() => expect(posts()).toHaveLength(1));
    const sent = posts()[0]!;
    expect(sent.path).toBe("/namespaces/team-a/schedules");
    expect(sent.key).toMatch(/^logweir-console\.schedule\./);
    expect(sent.body).toEqual({ schedule: "0 2 * * * ", suspended: false, topics: ["orders", "payments"], destinationRef: { name: "primary" }, sourceRef: { name: "orders-prod" } });
  });

  it("an unknown zone is refused beside the zone field, and nothing can be created", async () => {
    formNetwork();
    renderAt("/team-a/schedules/new");
    await fillGuided();
    fireEvent.change(screen.getByLabelText("Time zone"), { target: { value: "Europe/Atlantis" } });
    await waitFor(() => expect(document.getElementById("sf-timeZone-helper")?.textContent).toMatch(/is not a time zone/), { timeout: 3000 });
    const create = screen.getByRole("button", { name: "Create schedule" }) as HTMLButtonElement;
    expect(create.disabled).toBe(true);
    fireEvent.click(create);
    expect(posts()).toHaveLength(0);
  });

  it("a refused create places the API's field error beside its input", async () => {
    formNetwork((r) =>
      r.method === "POST"
        ? json(422, { code: "validation_failed", status: 422, detail: "One or more fields are invalid.", errors: [{ field: "topics[1]", code: "not_found", message: "payments is not on orders-prod" }, { field: "spec.mystery", code: "x", message: "no input owns this" }], requestId: "r" })
        : null,
    );
    renderAt("/team-a/schedules/new");
    await fillGuided();
    const create = await screen.findByRole("button", { name: "Create schedule" });
    await waitFor(() => expect((create as HTMLButtonElement).disabled).toBe(false), { timeout: 3000 });
    fireEvent.click(create);
    await waitFor(() => expect(document.getElementById("sf-topics-helper")?.textContent).toMatch(/^payments is not on orders-prod/));
    expect(screen.getByText("spec.mystery: no input owns this")).toBeTruthy();
    expect(screen.getByText("The schedule was not created: fix the fields marked below")).toBeTruthy();
  });

  it("after no answer, submitting again resends the same key", async () => {
    let calls = 0;
    formNetwork((r) => {
      if (r.method !== "POST") return null;
      calls += 1;
      return calls === 1 ? json(503, { code: "kubernetes_unavailable", status: 503, detail: "upstream", requestId: "r" }) : json(200, { item: { ...schedule(), name: "sch-abc" }, replayed: true, requestId: "r" });
    });
    renderAt("/team-a/schedules/new");
    await fillGuided();
    const create = await screen.findByRole("button", { name: "Create schedule" });
    await waitFor(() => expect((create as HTMLButtonElement).disabled).toBe(false), { timeout: 3000 });
    fireEvent.click(create);
    await screen.findByText("No answer: whether the schedule was created is unknown");
    fireEvent.click(screen.getByRole("button", { name: "Create schedule" }));
    await waitFor(() => expect(posts()).toHaveLength(2));
    expect(posts()[1]!.key).toBe(posts()[0]!.key);
  });

  it("a connection deleted and created again under the chosen name is refused, not followed", async () => {
    let reads = 0;
    formNetwork((_r, path) => {
      if (!path.startsWith("/namespaces/team-a/connections")) return null;
      reads += 1;
      // The second read (the one a submit makes) finds another object under the name.
      return json(200, { items: [{ ...CONNECTIONS[0], uid: reads === 1 ? "c-1" : "c-2" }], page: { limit: 50 }, requestId: "r" });
    });
    renderAt("/team-a/schedules/new");
    await fillGuided();
    const create = await screen.findByRole("button", { name: "Create schedule" });
    await waitFor(() => expect((create as HTMLButtonElement).disabled).toBe(false));
    fireEvent.click(create);
    await waitFor(() => expect(document.getElementById("sf-source-helper")?.textContent).toMatch(/was deleted and created again/));
    expect(posts()).toHaveLength(0);
  });

  it("a login without the grant is told so and offered no form", async () => {
    formNetwork(undefined, { ...ALL, scheduleCreate: false });
    renderAt("/team-a/schedules/new");
    await screen.findByText("Not available to this login");
    expect(screen.queryByRole("button", { name: "Create schedule" })).toBeNull();
  });
});

describe("the schedules list, by grant", () => {
  const listed = (path: string) =>
    path.startsWith("/namespaces/team-a/schedules") ? json(200, { items: [schedule()], page: { limit: 50 }, requestId: "r" }) : null;

  it("offers Create a schedule to an operator", async () => {
    formNetwork((_r, path) => listed(path));
    renderAt("/team-a/schedules");
    await screen.findByText("nightly");
    expect(await screen.findByRole("button", { name: "Create a schedule" })).toBeTruthy();
  });

  it("a viewer reads the list and is offered no create", async () => {
    formNetwork((_r, path) => listed(path), { scheduleCreate: false, manualBackupCreate: false, scheduleSetSuspension: false });
    renderAt("/team-a/schedules");
    await screen.findByText(/may read the schedules here and not create or change one/);
    expect(screen.queryByRole("button", { name: "Create a schedule" })).toBeNull();
  });
});

describe("suspend and resume", () => {
  it("sends the value and the revision it was read at; a stale one is re-read and said", async () => {
    let read = 0;
    formNetwork((_r, path) => {
      if (path === "/namespaces/team-a/schedules/nightly") {
        read += 1;
        return json(200, { item: schedule({ resourceVersion: String(100 + read) }), requestId: "r" });
      }
      if (path.endsWith(":set-suspension")) return json(412, { code: "precondition_failed", status: 412, detail: "The schedule changed since it was read.", requestId: "r" });
      if (path.startsWith("/namespaces/team-a/")) return json(200, { items: [], page: { limit: 50 }, requestId: "r" });
      return null;
    });
    renderAt("/team-a/schedules/nightly");
    fireEvent.click(await screen.findByRole("button", { name: "Suspend" }));
    await screen.findByText(/Somebody changed this schedule after this page read it/);
    const sent = posts().find((s) => s.path.endsWith(":set-suspension"))!;
    expect(sent.body).toEqual({ suspended: true, expectedResourceVersion: "101" });
    expect(sent.key).toBeNull();
    // The refusal re-read the schedule.
    await waitFor(() => expect(read).toBeGreaterThan(1));
  });
});
