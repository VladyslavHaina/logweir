// @vitest-environment jsdom
// The R4 flows as an operator meets them: the requests the pages send (one
// click, one object; the same key on a resend; a new key for a new request),
// where a refusal lands, what a role without the grant is offered, and what the
// pages refuse to claim. The network is a fake the typed client is handed in
// place of the mock, so every request is recorded. Session and document
// fixtures are the API's own (console/fixtures/console/).
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Outlet, RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import { backupRoutes } from "./routes";
import { BackUpNow, UNKNOWN_RUN_OUTCOME } from "./BackUpNow";
import { ConnectArchive } from "../catalog/ConnectArchive";
import { KEYS_FORBIDDEN_SENTENCE } from "../trust/model";
import { DIFFERENT_RUN_SENTENCE } from "./BackupDetail";
import { CASES } from "./verdict";
import { CATALOG_WINDOW_SENTENCE, PARTIAL_POINTS_SENTENCE } from "../catalog/model";
import catalogPoints from "../../../fixtures/console/catalog-points.json";
import { SessionGate } from "../../app/SessionGate";
import operator from "../../../fixtures/console/session-manual-backups.json";
import viewer from "../../../fixtures/console/session-viewer.json";
import schedulesList from "../../../fixtures/console/schedules-list.json";
import backupDoc from "../../../fixtures/console/backup.json";
import destinationDoc from "../../../fixtures/console/destination.json";
import backupsList from "../../../fixtures/console/backups-list.json";
import operationBackup from "../../../fixtures/console/operation-backup.json";
import catalogsList from "../../../fixtures/console/catalogs-list.json";
import catalogDoc from "../../../fixtures/console/catalog.json";
import signers from "../../../fixtures/console/catalog-signers.json";
import trustList from "../../../fixtures/console/trust-policies-list.json";

configure({ asyncUtilTimeout: 5000 });
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
    if (path === "/session") return json(200, operator);
    return json(404, { code: "not_found", detail: `no fake for ${request.method} ${path}`, status: 404, requestId: "r" });
  },
}));

function json(status: number, body: unknown) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" } });
}
const copy = <T,>(v: T): T => JSON.parse(JSON.stringify(v)) as T;

/** A router with the R4 routes under `/<ns>/`, starting at `path`. */
function renderAt(path: string) {
  const root = createRootRoute({ component: Outlet });
  const ns = createRoute({ getParentRoute: () => root, path: "$ns", component: Outlet });
  const router = createRouter({
    routeTree: root.addChildren([ns.addChildren([...backupRoutes(ns as never)])]),
    history: createMemoryHistory({ initialEntries: [path] }),
  });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <SessionGate>{() => <RouterProvider router={router} />}</SessionGate>
    </QueryClientProvider>,
  );
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

const posts = () => net.sent.filter((s) => s.method === "POST");
const gets = (prefix: string) => net.sent.filter((s) => s.method === "GET" && s.path.startsWith(prefix));

/** Answers the reads every page makes; `extra` answers first. */
function reads(session: unknown, extra: (r: Request, path: string, body: unknown) => Response | null = () => null) {
  net.answer = (r, path, body) => {
    const own = extra(r, path, body);
    if (own) return own;
    if (path === "/session") return json(200, session);
    if (path.startsWith("/namespaces/team-a/schedules")) return json(200, schedulesList);
    return null;
  };
}

beforeEach(() => {
  net.sent = [];
  net.answer = () => null;
});
afterEach(cleanup);

const started = (key: string | null, replayed = false) =>
  json(replayed ? 200 : 201, {
    item: { ...copy(backupDoc.item), name: `logweir-manual-${key!.slice(-6)}`, scheduleRef: { name: "orders-hourly", generation: 4 } },
    replayed,
    requestId: "r",
  });

describe("Back up now from the list", () => {
  it("sends one run per click under one key, and only Back up again mints another", async () => {
    let release: () => void = () => {};
    const gate = new Promise<void>((r) => (release = r));
    reads(operator, (r) => (r.method === "POST" ? (gate.then(() => started(r.headers.get("Idempotency-Key"))) as unknown as Response) : null));
    renderWith(<BackUpNow ns="team-a" open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    const select = await within(dialog).findByLabelText(/Schedule/);
    await within(dialog).findByRole("option", { name: /orders-hourly/ });
    fireEvent.change(select, { target: { value: "orders-hourly" } });
    const send = within(dialog).getByRole("button", { name: "Back up now" });
    fireEvent.click(send);
    fireEvent.click(send); // a double click while the first is in flight
    await waitFor(() => expect(posts()).toHaveLength(1));
    release();
    await within(dialog).findByText("Backup started");
    expect(posts()).toHaveLength(1);
    expect(posts()[0]!.body).toEqual({ scheduleRef: { name: "orders-hourly" } });
    const first = posts()[0]!.key!;
    expect(first).toMatch(/^logweir-console\.manual\.[0-9a-f]{32}$/);
    // NEGATIVE CONTROL: Back up again is a new request with a new key.
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up again" }));
    fireEvent.change(within(dialog).getByLabelText(/Schedule/), { target: { value: "orders-hourly" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up now" }));
    await waitFor(() => expect(posts()).toHaveLength(2));
    expect(posts()[1]!.key).not.toBe(first);
  });

  it("says an unanswered request may have started a run, and resends the same key", async () => {
    let calls = 0;
    reads(operator, (r) => {
      if (r.method !== "POST") return null;
      calls += 1;
      return calls === 1 ? json(503, { code: "kubernetes_unavailable", detail: "the API server did not answer", status: 503, requestId: "r1" }) : started(r.headers.get("Idempotency-Key"), true);
    });
    renderWith(<BackUpNow ns="team-a" open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    await within(dialog).findByRole("option", { name: /orders-hourly/ });
    fireEvent.change(within(dialog).getByLabelText(/Schedule/), { target: { value: "orders-hourly" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up now" }));
    await within(dialog).findByText("The outcome is unknown");
    expect(within(dialog).getByText(UNKNOWN_RUN_OUTCOME)).toBeTruthy();
    fireEvent.click(within(dialog).getByRole("button", { name: "Send the same request again" }));
    await within(dialog).findByText("Already started");
    expect(posts()).toHaveLength(2);
    expect(posts()[1]!.key).toBe(posts()[0]!.key);
  });

  it("answers a changed policy with its revision, and its only answer is a new key", async () => {
    reads(operator, (r) =>
      r.method === "POST"
        ? json(409, { code: "policy_changed", detail: "the schedule changed", status: 409, requestId: "r", policy: { currentGeneration: 7 } })
        : null,
    );
    renderWith(<BackUpNow ns="team-a" open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    await within(dialog).findByRole("option", { name: /orders-hourly/ });
    fireEvent.change(within(dialog).getByLabelText(/Schedule/), { target: { value: "orders-hourly" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up now" }));
    await within(dialog).findByText(/it is at revision g7 now/);
    // No resend of the refused key is offered.
    expect(within(dialog).queryByRole("button", { name: "Back up now" })).toBeNull();
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up again" }));
    fireEvent.change(within(dialog).getByLabelText(/Schedule/), { target: { value: "orders-hourly" } });
    fireEvent.click(within(dialog).getByRole("button", { name: "Back up now" }));
    await waitFor(() => expect(posts()).toHaveLength(2));
    expect(posts()[1]!.key).not.toBe(posts()[0]!.key);
  });
});

describe("Connect an archive", () => {
  const fill = (dialog: HTMLElement, name: string, destination: string) => {
    fireEvent.input(within(dialog).getByLabelText(/Catalog name/), { target: { value: name } });
    fireEvent.change(within(dialog).getByLabelText(/Destination/), { target: { value: destination } });
  };

  it("refuses an empty form beside its fields and sends nothing", async () => {
    reads(operator);
    renderWith(<ConnectArchive ns="team-a" open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    // DUI-40.1: the Sync mode helper names its command as code, never in raw backticks.
    expect(within(dialog).getByText("logweir catalog sync", { selector: "code" })).toBeTruthy();
    expect(dialog.textContent).not.toContain("`");
    fireEvent.click(within(dialog).getByRole("button", { name: "Connect archive" }));
    await within(dialog).findByText(/A catalog needs a name/);
    expect(within(dialog).getByText(/Name the saved destination/)).toBeTruthy();
    expect(posts()).toHaveLength(0);
  });

  it("holds one key for one request: a resend reuses it, a corrected field gets a new one", async () => {
    let calls = 0;
    reads(operator, (r) => {
      if (r.method !== "POST") return null;
      calls += 1;
      if (calls === 1) throw new TypeError("Failed to fetch");
      return json(201, { item: { ...copy(catalogDoc.item), name: "archive-b" }, replayed: false, requestId: "r" });
    });
    renderWith(<ConnectArchive ns="team-a" open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    fill(dialog, "archive-a", "primary");
    fireEvent.click(within(dialog).getByRole("button", { name: "Connect archive" }));
    await within(dialog).findByText("The outcome is unknown");
    expect(within(dialog).getByText(/whether catalog archive-a was created is unknown/)).toBeTruthy();
    expect(posts()[0]!.body).toEqual({ name: "archive-a", destinationRef: { name: "primary" }, syncMode: "full" });
    // The corrected name is a different request: a new key, never the spent one.
    fill(dialog, "archive-b", "primary");
    fireEvent.click(within(dialog).getByRole("button", { name: "Connect archive" }));
    await within(dialog).findByText("Connected");
    expect(posts()).toHaveLength(2);
    expect(posts()[1]!.key).not.toBe(posts()[0]!.key);
  });

  it("resends the same body under the same key after no answer", async () => {
    let calls = 0;
    reads(operator, (r) => {
      if (r.method !== "POST") return null;
      calls += 1;
      if (calls === 1) throw new TypeError("Failed to fetch");
      return json(200, { item: { ...copy(catalogDoc.item), name: "archive-a" }, replayed: true, requestId: "r" });
    });
    renderWith(<ConnectArchive ns="team-a" open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    fill(dialog, "archive-a", "primary");
    fireEvent.click(within(dialog).getByRole("button", { name: "Connect archive" }));
    await within(dialog).findByText("The outcome is unknown");
    fireEvent.click(within(dialog).getByRole("button", { name: "Connect archive" }));
    await within(dialog).findByText("Already connected");
    expect(posts()[1]!.key).toBe(posts()[0]!.key);
  });

  it("places the API's field errors beside their fields and lists the ones no field claims", async () => {
    reads(operator, (r) =>
      r.method === "POST"
        ? json(422, {
            code: "validation_failed",
            detail: "One or more fields are invalid.",
            status: 422,
            requestId: "r",
            errors: [
              { field: "destinationRef.name", code: "not_found", message: "no BackupDestination named nowhere" },
              { field: "viewLimit", code: "range", message: "must be 1 to 2000" },
            ],
          })
        : null,
    );
    renderWith(<ConnectArchive ns="team-a" open onClose={() => {}} />);
    const dialog = await screen.findByRole("dialog");
    fill(dialog, "archive-a", "nowhere");
    fireEvent.click(within(dialog).getByRole("button", { name: "Connect archive" }));
    const message = await within(dialog).findByText("no BackupDestination named nowhere");
    expect(message.closest(".pf-v6-c-form__group")?.textContent).toMatch(/Destination/);
    expect(within(dialog).getByText("viewLimit: must be 1 to 2000")).toBeTruthy();
  });
});

describe("what each role is offered", () => {
  const list = () => {
    const l = copy(backupsList);
    return json(200, l);
  };

  it("offers Back up now and Restore a point only to a role that holds them", async () => {
    reads(operator, (_r, path) => (path.startsWith("/namespaces/team-a/backups") ? list() : null));
    renderAt("/team-a/backups");
    await screen.findByRole("button", { name: "Back up now" });
    expect(screen.getByRole("button", { name: "Restore a point" })).toBeTruthy();
    cleanup();
    // NEGATIVE CONTROL: the viewer's grant holds neither.
    reads(viewer, (_r, path) => (path.startsWith("/namespaces/team-a/backups") ? list() : null));
    renderAt("/team-a/backups");
    await screen.findByText(/A list row carries the controller's verdict/);
    expect(screen.queryByRole("button", { name: "Back up now" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Restore a point" })).toBeNull();
  });

  it("offers Connect an archive only with the catalogConnect grant", async () => {
    reads(operator, (_r, path) => (path.startsWith("/namespaces/team-a/catalogs") ? json(200, catalogsList) : null));
    renderAt("/team-a/catalog");
    await screen.findByRole("button", { name: "Connect an archive" });
    cleanup();
    reads(viewer, (_r, path) => (path.startsWith("/namespaces/team-a/catalogs") ? json(200, catalogsList) : null));
    renderAt("/team-a/catalog");
    await screen.findByText(/needs the operator or administrator role/);
    expect(screen.queryByRole("button", { name: "Connect an archive" })).toBeNull();
  });

  it("does not read trust material without the grant, and says who may", async () => {
    reads(viewer, (_r, path) => (path.startsWith("/trust-policies") ? json(200, trustList) : null));
    renderAt("/team-a/trust");
    await screen.findByText(KEYS_FORBIDDEN_SENTENCE);
    expect(gets("/trust-policies")).toHaveLength(0);
    cleanup();
    // CONTROL: with the grant, the read is made and the policies are shown.
    const admin = copy(viewer);
    admin.capabilities.trustPoliciesRead = true;
    reads(admin, (_r, path) => (path.startsWith("/trust-policies") ? json(200, trustList) : null));
    renderAt("/team-a/trust");
    await screen.findByText(trustList.items[0]!.name);
    expect(gets("/trust-policies").length).toBeGreaterThan(0);
  });
});

describe("one backup's page", () => {
  const detail = (operation: unknown, session: unknown = operator) =>
    reads(session, (_r, path) => {
      if (path === "/namespaces/team-a/backups/orders-hourly-20260912-080000") return json(200, backupDoc);
      if (path.startsWith("/namespaces/team-a/backups")) return json(200, backupsList);
      if (path.startsWith("/namespaces/team-a/operations/backup/")) return json(200, operation);
      if (path.startsWith("/namespaces/team-a/catalogs")) return json(200, { items: [], page: { limit: 200 }, requestId: "r" });
      return null;
    });

  it("DUI-41.2: the Check it yourself note names its command as code, never in raw backticks", async () => {
    reads(operator, (_r, path) => {
      if (path === "/namespaces/team-a/backups/orders-hourly-20260912-080000") return json(200, backupDoc);
      if (path.startsWith("/namespaces/team-a/backups")) return json(200, backupsList);
      if (path.startsWith("/namespaces/team-a/operations/backup/")) return json(200, operationBackup);
      if (path.startsWith("/namespaces/team-a/catalogs")) return json(200, { items: [], page: { limit: 200 }, requestId: "r" });
      if (path.startsWith("/namespaces/team-a/destinations/")) return json(200, { ...destinationDoc, item: { ...destinationDoc.item, locationDigest: backupDoc.item.locationDigest ?? null } });
      return null;
    });
    renderAt("/team-a/backups/orders-hourly-20260912-080000");
    const code = await screen.findByText("aws configure set s3.addressing_style path", { selector: "code" });
    expect(code.closest("p")?.textContent).not.toContain("`");
  });

  it("shows the controller's verdict and never claims the page checked anything", async () => {
    detail(operationBackup);
    renderAt("/team-a/backups/orders-hourly-20260912-080000");
    await screen.findByText(/Verified by the controller at 2026-09-12 08:02 UTC against the key below/);
    expect(document.body.textContent).toMatch(/this page holds no key and checks nothing itself/);
    expect(document.body.textContent).not.toMatch(/in your browser/);
    expect((screen.getByRole("button", { name: "Restore this point" }) as HTMLButtonElement).getAttribute("aria-disabled")).not.toBe("true");
  });

  it("is not green over a key revoked for compromise, and names the case", async () => {
    const op = copy(operationBackup) as { item: Record<string, unknown> };
    op.item.trust = { state: "verified", basis: "RecordedBeforeRevocation", keyState: "Revoked" };
    detail(op);
    renderAt("/team-a/backups/orders-hourly-20260912-080000");
    await screen.findByText(new RegExp(CASES.recordedBeforeRevocation.slice(0, 40)));
    expect(document.body.textContent).not.toMatch(/Verified by the controller at/);
  });

  it("refuses to show an operation that answers to another uid", async () => {
    const op = copy(operationBackup) as { item: Record<string, unknown> };
    op.item.uid = "11111111-2222-4333-8444-555555555555";
    detail(op);
    renderAt("/team-a/backups/orders-hourly-20260912-080000");
    await screen.findByText(DIFFERENT_RUN_SENTENCE);
    expect(document.body.textContent).not.toMatch(/Verified by the controller at/);
  });

  it("FX-45: a run refused before the engine shows the refusal sentence verbatim", async () => {
    const REFUSAL =
      "the backup principal may not READ 1 of 2 selected topic(s): `payments` (the broker reports its operations exclude READ). Grant READ on each named topic to the principal this connection authenticates as";
    const doc = copy(backupDoc);
    doc.item.operation = { state: "refused", stateReason: "GuardRefused", terminal: true, verificationState: "notAttempted", verifiedSuccess: false };
    const op = copy(operationBackup) as { item: Record<string, unknown> };
    Object.assign(op.item, {
      state: "refused",
      stateReason: "GuardRefused",
      verifiedSuccess: false,
      message: REFUSAL,
      result: { status: "refused", exitCode: 3, exitReason: "GuardRefused" },
      // DUI-40.3: the Activity list's condition messages carry code spans too.
      conditions: [{ type: "Failed", status: "True", reason: "GuardRefused", message: "the `runner` container exited 3", lastTransitionTime: "2026-09-12T08:01:00Z" }],
    });
    reads(operator, (_r, path) => {
      if (path === "/namespaces/team-a/backups/orders-hourly-20260912-080000") return json(200, doc);
      if (path.startsWith("/namespaces/team-a/backups")) return json(200, backupsList);
      if (path.startsWith("/namespaces/team-a/operations/backup/")) return json(200, op);
      if (path.startsWith("/namespaces/team-a/catalogs")) return json(200, { items: [], page: { limit: 200 }, requestId: "r" });
      return null;
    });
    renderAt("/team-a/backups/orders-hourly-20260912-080000");
    const title = await screen.findByText("This run was refused: GuardRefused");
    const alert = title.closest(".pf-v6-c-alert") as HTMLElement;
    // DUI-35: the sentence is the runner's, word for word, and its code span is code, never raw backticks.
    const said = REFUSAL.replace(/`/g, "");
    await waitFor(() => expect(alert.textContent).toContain(said));
    expect(within(alert).getByText("payments", { selector: "code" })).toBeTruthy();
    expect(alert.textContent).not.toContain("`");
    // The same message under "Where it stands", and in a condition and a diagnosis, is prose too.
    const where = screen.getByRole("heading", { name: "Where it stands" }).closest(".pf-v6-c-card") as HTMLElement;
    expect(where.textContent).toContain(said);
    expect(within(where).getAllByText("payments", { selector: "code" }).length).toBeGreaterThan(0);
    expect(where.textContent).not.toContain("`");
    expect(within(where).getByText("runner", { selector: "code" })).toBeTruthy();
    expect(where.textContent).toContain("the runner container exited 3");
  });

  it("DUI-35: a message with an unpaired backtick is shown as it came, never guessed into code (the control)", async () => {
    const ODD = "the runner's own reason: topic `orders does not exist";
    const doc = copy(backupDoc);
    doc.item.operation = { state: "refused", stateReason: "GuardRefused", terminal: true, verificationState: "notAttempted", verifiedSuccess: false };
    const op = copy(operationBackup) as { item: Record<string, unknown> };
    Object.assign(op.item, { state: "refused", stateReason: "GuardRefused", verifiedSuccess: false, message: ODD, result: { status: "refused", exitCode: 3, exitReason: "GuardRefused" } });
    reads(operator, (_r, path) => {
      if (path === "/namespaces/team-a/backups/orders-hourly-20260912-080000") return json(200, doc);
      if (path.startsWith("/namespaces/team-a/backups")) return json(200, backupsList);
      if (path.startsWith("/namespaces/team-a/operations/backup/")) return json(200, op);
      if (path.startsWith("/namespaces/team-a/catalogs")) return json(200, { items: [], page: { limit: 200 }, requestId: "r" });
      return null;
    });
    renderAt("/team-a/backups/orders-hourly-20260912-080000");
    const title = await screen.findByText("This run was refused: GuardRefused");
    const alert = title.closest(".pf-v6-c-alert") as HTMLElement;
    await waitFor(() => expect(alert.textContent).toContain(ODD));
    expect(alert.querySelector("code")).toBeNull();
  });

  it("tells a role that cannot restore who can, instead of offering the action", async () => {
    detail(operationBackup, viewer);
    renderAt("/team-a/backups/orders-hourly-20260912-080000");
    const button = await screen.findByRole("button", { name: "Restore this point" });
    await waitFor(() => expect(button.getAttribute("aria-disabled")).toBe("true"));
  });
});

describe("one catalog's page", () => {
  it("keeps its status when the point list is refused, and never shows the refusal as an empty view", async () => {
    reads(operator, (_r, path) => {
      if (path === "/namespaces/team-a/catalogs/primary") return json(200, catalogDoc);
      if (path.startsWith("/namespaces/team-a/catalogs/primary/points")) return json(503, { code: "kubernetes_unavailable", detail: "the point pages could not be read", status: 503, requestId: "r" });
      if (path.startsWith("/namespaces/team-a/catalogs/primary/signers")) return json(200, signers);
      return null;
    });
    renderAt("/team-a/catalog/primary");
    await screen.findByText("the point pages could not be read");
    expect(screen.getByText("The view")).toBeTruthy();
    expect(document.body.textContent).not.toMatch(/This catalog's view holds no point/);
  });

  it("offers no control that trusts an untrusted signer", async () => {
    reads(operator, (_r, path) => {
      if (path === "/namespaces/team-a/catalogs/primary") return json(200, catalogDoc);
      if (path.startsWith("/namespaces/team-a/catalogs/primary/points")) return json(200, { items: [], page: { limit: 200 }, truncated: false, viewExpired: false, requestId: "r" });
      if (path.startsWith("/namespaces/team-a/catalogs/primary/signers")) return json(200, signers);
      return null;
    });
    renderAt("/team-a/catalog/primary");
    const panel = (await screen.findByText("Who signed these points")).closest(".pf-v6-c-card") as HTMLElement;
    await within(panel).findByText(/This page offers no control that adds a key to a TrustPolicy/);
    expect(within(panel).getByText(signers.fingerprintCommand)).toBeTruthy();
    // The only buttons in the panel copy text; none adds, trusts or applies.
    const names = within(panel).queryAllByRole("button").map((b) => b.getAttribute("aria-label") ?? b.textContent ?? "");
    expect(names.length).toBeGreaterThan(0);
    for (const n of names) expect(n, n).toMatch(/^Copy|key id|sha256/i);
    expect(posts()).toHaveLength(0);
  });
});

describe("FX-38: one catalog's page says what its view does not prove", () => {
  const show = (catalog: unknown, points: unknown) => {
    reads(operator, (_r, path) => {
      if (path === "/namespaces/team-a/catalogs/primary") return json(200, catalog);
      if (path.startsWith("/namespaces/team-a/catalogs/primary/points")) return json(200, points);
      if (path.startsWith("/namespaces/team-a/catalogs/primary/signers")) return json(200, signers);
      return null;
    });
    renderAt("/team-a/catalog/primary");
  };
  const windowSentence = CATALOG_WINDOW_SENTENCE.replace(/`/g, "");

  it("a truncated view that is not a window is not known to be whole, and is not called a window", async () => {
    const doc = copy(catalogDoc);
    doc.item.conditions = doc.item.conditions.map((c) => (c.type === "Synced" ? { ...c, message: "the walk completed; the newest 4 of 4 points are materialised across 1 page(s). 1 entr(y/ies) did not fit the view's 1 page(s) and are not listed" } : c));
    show(doc, catalogPoints);
    await screen.findByText("Not known to be the whole archive; the Synced condition says why");
    expect(document.body.textContent).toContain("1 entr(y/ies) did not fit the view's 1 page(s) and are not listed");
    expect(document.body.textContent).not.toContain(windowSentence);
    expect(document.body.textContent).not.toMatch(/the archive holds more points than the view carries/);
  });

  it("a window says so, from the Synced message (control)", async () => {
    show(catalogDoc, catalogPoints);
    // DUI-40.1: its command is code now; the sentence reads the same.
    await screen.findByText((_content, el) => el?.classList.contains("lw-prose") === true && el.textContent === windowSentence);
    expect(screen.getAllByText("logweir catalog list", { selector: "code" }).length).toBeGreaterThan(0);
  });

  it("a points page with incomplete: true says the list is partial", async () => {
    show(catalogDoc, { ...copy(catalogPoints), incomplete: true });
    await screen.findByText(PARTIAL_POINTS_SENTENCE);
    cleanup();
    // CONTROL: a whole page says nothing of the kind.
    show(catalogDoc, catalogPoints);
    await screen.findByText("Recovery points");
    await screen.findAllByText("Restore this point");
    expect(screen.queryByText(PARTIAL_POINTS_SENTENCE)).toBeNull();
  });
});
