// @vitest-environment jsdom
// The Approvals pages as an approver meets them: what is shown before Approve,
// the one request each action sends, where a refusal lands, and what a sign-in
// that may not act is offered. The network is a fake the typed client is handed
// in place of the mock, so every request is recorded.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, fireEvent, render, renderHook, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Outlet, RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import type { ReactNode } from "react";
import sessionFixture from "../../../fixtures/console/session.json";
import restoreFixture from "../../../fixtures/console/restore.json";
import approvalFixture from "../../../fixtures/console/approval.json";
import twoPersonFixture from "../../../fixtures/console/approval-policy-two-person.json";
import pendingFixture from "../../../fixtures/console/approval-request-pending.json";
import packetFixture from "../../../fixtures/console/approval-packet.json";
import { approvalRoutes } from "./routes";
import { sha256 } from "../../components/hash";
import { PRIVATE_KEY_REFUSAL, useApprovalsNavCount, type ApprovalPolicy } from "../../api/approvals";
import { SessionGate } from "../../app/SessionGate";

configure({ asyncUtilTimeout: 5000 });
// A PatternFly page in jsdom is slow to render on a loaded host: allow a whole flow 30 s.
vi.setConfig({ testTimeout: 30_000 });

const net = vi.hoisted(() => ({
  sent: [] as { method: string; path: string; key: string | null; body: unknown }[],
  answer: (() => null) as (r: Request, path: string) => Response | Promise<Response | null> | null,
}));

vi.mock("../../mock/fetch", () => ({
  mockFetch: async (request: Request) => {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^.*?\/api\/v1/, "") + url.search;
    const text = request.method === "GET" ? "" : await request.text();
    net.sent.push({ method: request.method, path, key: request.headers.get("Idempotency-Key"), body: text ? JSON.parse(text) : undefined });
    const r = await net.answer(request, path);
    if (r) return r;
    return json(404, { code: "not_found", detail: `no fake for ${request.method} ${path}` });
  },
}));

function json(status: number, body: unknown) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": status >= 400 ? "application/problem+json" : "application/json" } });
}

const BOB = "https://idp.example#bob";
const PLAN = "name: p\nsource:\n  topics: [orders, payments]\n";
const TWO_PERSON = twoPersonFixture.item as ApprovalPolicy;
const STRICT: ApprovalPolicy = { ...TWO_PERSON, name: "prod-strict", operatorMode: "strict", consoleApprovalAvailable: false };
const CONFIRM: ApprovalPolicy = { ...TWO_PERSON, name: "team-ordinary", mode: "ordinary", operatorMode: "confirm", consoleApprovalAvailable: false };
const LEGACY: ApprovalPolicy = { ...TWO_PERSON, name: "legacy-governed-v1", legacy: true, operatorMode: "strict", consoleApprovalAvailable: false };

function session(approver = true) {
  const caps = { ...sessionFixture.capabilities, approvalSubmit: approver };
  return { ...sessionFixture, actor: { displayName: "Bob", id: BOB, issuer: "https://idp.example", subject: "bob" }, capabilities: caps, namespaces: [{ name: "team-a", roles: ["approver"], capabilities: caps }] };
}

async function restore(over: Record<string, unknown> = {}) {
  return {
    ...restoreFixture.item,
    name: "restore-x",
    uid: "uid-x",
    approvalRef: { name: "approval-x" },
    approvalSubject: "ordinary",
    planBytes: PLAN,
    planHash: await sha256(PLAN),
    operation: { state: "pending", stateReason: "ApprovalNotVerified", terminal: false, verificationState: "pending", verifiedSuccess: false },
    ...over,
  };
}

const pending = () => structuredClone(pendingFixture.item);

/** DUI-27: a Switchover held for its approval, and the two-person request a second person is shown. */
const swo = (over: Record<string, unknown> = {}) => ({
  name: "swo-1",
  namespace: "team-a",
  uid: "swo-uid",
  approvalName: "swo-approval-1",
  planHash: `sha256:${"d".repeat(64)}`,
  restore: { name: "restore-x", uid: "uid-x" },
  groups: [],
  phase: "Pending",
  reason: "ApprovalNotVerified",
  ...over,
});
const swoRequest = (over: Record<string, unknown> = {}) => ({
  switchover: "swo-1",
  switchoverUid: "swo-uid",
  namespace: "team-a",
  approvalName: "swo-approval-1",
  confirmationName: "swo-approval-1-confirmation",
  confirmationSha256: `sha256:${"e".repeat(64)}`,
  policy: "prod-pair",
  policyDigest: `sha256:${"f".repeat(64)}`,
  requester: "https://idp.example#alice",
  issuedAt: "2026-09-07T14:05:00Z",
  state: "pending",
  stateSentence: "Waiting for a second person.",
  scopeComplete: true,
  approve: { offered: true, sentence: "Approve to run it." },
  ...over,
});
const approvalX = (over: Record<string, unknown> = {}) => ({ ...approvalFixture.item, name: "approval-x", subjectRef: { kind: "Restore", name: "restore-x" }, ...over });

interface World {
  policy: ApprovalPolicy;
  approver?: boolean;
  restore?: Record<string, unknown> | null;
  approval?: Record<string, unknown> | null | "error";
  request?: Record<string, unknown>;
  packet?: Record<string, unknown> | null;
  confirmation?: Record<string, unknown> | null;
  approvals?: Record<string, unknown>[] | "error";
  restores?: Record<string, unknown>[];
  requests?: Record<string, Record<string, unknown>>;
  /** DUI-27: the namespace's Switchovers, and their two-person requests by name. */
  switchovers?: Record<string, unknown>[];
  switchoverRequests?: Record<string, Record<string, unknown>>;
  post?: (path: string, body: unknown) => Response;
}

/** Answers every read the pages make from `w`; a POST from `w.post`. */
function serve(w: World) {
  net.answer = async (r, path) => {
    const ok = (item: unknown) => json(200, { item, requestId: "r" });
    if (r.method === "POST") return w.post ? w.post(path, net.sent.at(-1)!.body) : null;
    if (path === "/session") return json(200, session(w.approver ?? true));
    if (path === "/namespaces/team-a/approval-policy") return ok(w.policy);
    if (path.startsWith("/namespaces/team-a/approvals?")) {
      return w.approvals === "error" ? json(500, { code: "internal", detail: "the list failed", requestId: "q" }) : json(200, { items: w.approvals ?? [], page: { limit: 200 }, requestId: "r" });
    }
    if (path.startsWith("/namespaces/team-a/restores?")) return json(200, { items: w.restores ?? [], page: { limit: 200 }, requestId: "r" });
    if (path.startsWith("/namespaces/team-a/switchovers?")) return json(200, { items: w.switchovers ?? [], page: { limit: 200 }, requestId: "r" });
    const swReq = path.match(/^\/namespaces\/team-a\/switchovers\/([^/]+)\/approval-request$/);
    if (swReq) return w.switchoverRequests?.[swReq[1]!] ? ok(w.switchoverRequests[swReq[1]!]) : json(404, { code: "not_found", detail: "none" });
    if (path === "/namespaces/team-a/restores/restore-x") return w.restore === null ? json(404, { code: "not_found", detail: "none" }) : ok(w.restore ?? (await restore()));
    if (path === "/namespaces/team-a/approvals/approval-x") {
      if (w.approval === "error") return json(500, { code: "internal", detail: "the read failed", requestId: "q" });
      return w.approval ? ok(w.approval) : json(404, { code: "not_found", detail: "none" });
    }
    if (path === "/namespaces/team-a/restores/restore-y") return ok(await restore({ name: "restore-y", uid: "uid-y", approvalRef: { name: "approval-y" } }));
    if (path === "/namespaces/team-a/approvals/approval-y") return json(404, { code: "not_found", detail: "none" });
    if (path === "/namespaces/team-a/approvals/approval-x/packet") return w.packet ? ok(w.packet) : json(404, { code: "not_found", detail: "none" });
    if (path === "/namespaces/team-a/approvals/approval-x-confirmation/packet") return w.confirmation ? ok(w.confirmation) : json(404, { code: "not_found", detail: "none" });
    const req = path.match(/^\/namespaces\/team-a\/restores\/([^/]+)\/approval-request$/);
    if (req) return ok(w.requests?.[req[1]!] ?? w.request ?? pending());
    return null;
  };
}

/** The Approvals routes under `#/<ns>/`, starting at `path`. */
function renderAt(path: string) {
  const root = createRootRoute({ component: Outlet });
  const ns = createRoute({ getParentRoute: () => root, path: "$ns", component: Outlet });
  const router = createRouter({
    routeTree: root.addChildren([ns.addChildren([...approvalRoutes(ns as never)])]),
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

beforeEach(() => {
  net.sent = [];
  net.answer = () => null;
});
afterEach(cleanup);

const posts = () => net.sent.filter((s) => s.method === "POST");
const created = () => json(201, { item: approvalX({ verified: null, verifiedSubject: undefined, conditions: [] }), requestId: "r" });

describe("two-person: the second person's page", () => {
  it("shows who asked, the plan hash, the target, the window and every topic before one Approve, which sends only the hash shown", async () => {
    serve({ policy: TWO_PERSON, post: (path) => (path === "/namespaces/team-a/restores/restore-x/console-approval" ? created() : json(404, {})) });
    renderAt("/team-a/approvals/restore-x");
    const approve = await screen.findByRole("button", { name: "Approve" });
    // Every fact is the server's, shown before the button.
    expect(screen.getByText("https://idp.example#alice")).toBeTruthy();
    expect(screen.getByText(pending().planHash!)).toBeTruthy();
    expect(screen.getByText("kafka-0.target:9092")).toBeTruthy();
    expect(screen.getAllByText("2026-09-07 14:05:00 UTC").length).toBeGreaterThan(0);
    const topics = screen.getByLabelText("Every topic restored and the name it is restored under");
    expect(within(topics).getByText("restore-20260907T140500Z-orders")).toBeTruthy();
    expect(within(topics).getByText("restore-20260907T140500Z-payments")).toBeTruthy();
    expect(screen.getByText("2 topics, every one listed")).toBeTruthy();
    // Nothing to copy, sign or paste in this mode.
    expect(screen.queryAllByRole("textbox")).toHaveLength(0);
    expect(screen.queryByText(/logweir drill/)).toBeNull();
    fireEvent.click(approve);
    await screen.findByText("Recorded as Approval approval-x");
    expect(posts()).toHaveLength(1);
    expect(posts()[0]!.body).toEqual({ confirmationSha256: pending().confirmationSha256 });
    expect(posts()[0]!.key).toBeNull();
  });

  it("a clone's request shows when its topics are deleted and the topic configuration the approver signs for", async () => {
    const base = pending() as { scope: { target: Record<string, unknown> } } & Record<string, unknown>;
    const clone = {
      ...base,
      scope: {
        ...base.scope,
        target: { ...base.scope.target, mode: "scratch", teardown: "ttl", teardownAfterSeconds: 3600, topicConfiguration: { transition: "afterVerification", owners: [] } },
      },
    };
    serve({ policy: TWO_PERSON, request: clone });
    renderAt("/team-a/approvals/restore-x");
    await screen.findByRole("button", { name: "Approve" });
    expect(document.getElementById("scope-afterwards")!.textContent).toBe(
      "A clone: the topics this run creates are kept, then deleted 60 min after the run. A run that does not pass deletes them at once.",
    );
    expect(document.getElementById("scope-topic-configuration")!.textContent).toMatch(
      /^The source's: created from the recovery point's configuration; the settings held during the restore are set to the source's once it passes verification\. The requester states that no topic has a declarative owner\.$/,
    );
    expect(screen.queryByText(/Nothing is deleted/)).toBeNull();
  });

  it("refuses self-approval in the server's words, as a refusal and not an error", async () => {
    const own = { ...pending(), approve: { offered: false, refusal: "requester", sentence: "You requested this restore. A two-person approval needs a second person, and no role changes that." } };
    serve({ policy: TWO_PERSON, request: own });
    renderAt("/team-a/approvals/restore-x");
    await screen.findByText("You cannot approve your own request");
    expect(screen.getByText("You requested this restore. A two-person approval needs a second person, and no role changes that.")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Approve" })).toBeNull();
    expect(screen.queryByText(/Couldn't load|had a problem/)).toBeNull();
    // NEGATIVE CONTROL: a sign-in without the approver role is told which role, also without a button.
    cleanup();
    serve({ policy: TWO_PERSON, approver: false });
    renderAt("/team-a/approvals/restore-x");
    await screen.findByText(/needs the approver role in this namespace/);
    expect(screen.queryByRole("button", { name: "Approve" })).toBeNull();
  });

  it("shows the API's refusal of a click as a refusal, and sends nothing else", async () => {
    serve({
      policy: TWO_PERSON,
      post: () => json(403, { code: "forbidden", detail: "Policy prod-pair needs a second person: the approver is the requester. Nothing was approved.", requestId: "q" }),
    });
    renderAt("/team-a/approvals/restore-x");
    fireEvent.click(await screen.findByRole("button", { name: "Approve" }));
    await screen.findByText("Not approved");
    expect(screen.getByText(/needs a second person: the approver is the requester/)).toBeTruthy();
    expect(screen.queryByText(/Couldn't load|had a problem/)).toBeNull();
    expect(posts()).toHaveLength(1);
  });

  it("does not carry one Restore's outcome to the next Restore's page", async () => {
    serve({
      policy: TWO_PERSON,
      requests: { "restore-y": { ...pending(), restore: "restore-y", restoreUid: "uid-y", approve: { offered: false, refusal: "requester", sentence: "You requested this restore." } } },
      post: () => created(),
    });
    const router = renderAt("/team-a/approvals/restore-x");
    fireEvent.click(await screen.findByRole("button", { name: "Approve" }));
    await screen.findByText("Recorded as Approval approval-x");
    await router.navigate({ to: "/$ns/approvals/$name", params: { ns: "team-a", name: "restore-y" }, search: {} });
    await screen.findByText("You cannot approve your own request");
    expect(screen.queryByText(/Recorded as Approval/)).toBeNull();
  });

  it("refuses a link that disagrees with the Restore, and offers no Approve", async () => {
    serve({ policy: TWO_PERSON });
    renderAt("/team-a/approvals/restore-x?hash=sha256%3Aother");
    await screen.findByText(/This link does not match Restore restore-x/);
    await screen.findByText("2 topics, every one listed");
    expect(screen.queryByRole("button", { name: "Approve" })).toBeNull();
    // NEGATIVE CONTROL: the link that names the Restore's own hash is offered the button.
    cleanup();
    serve({ policy: TWO_PERSON });
    renderAt(`/team-a/approvals/restore-x?hash=${encodeURIComponent(await sha256(PLAN))}`);
    await screen.findByRole("button", { name: "Approve" });
  });
});

describe("strict: the countersignature", () => {
  const confirmation = { ...packetFixture.item, name: "approval-x-confirmation", approvalBytes: '{"formatVersion":"2.0.0"}', sidecarBytes: '{"signatures":[1]}' };

  it("clears pasted key material and sends nothing; sends the sidecar alone; places the API's field error beside the field", async () => {
    let answer = () => json(201, { item: approvalX(), requestId: "r" });
    serve({ policy: STRICT, confirmation, post: () => answer() });
    renderAt("/team-a/approvals/restore-x");
    const field = (await screen.findByLabelText(/approval\.sig \(countersigned\)/)) as HTMLTextAreaElement;
    // The console's confirmation, verbatim, to copy.
    expect(screen.getByDisplayValue('{"formatVersion":"2.0.0"}')).toBeTruthy();
    fireEvent.change(field, { target: { value: "-----BEGIN " + "PRIVATE KEY-----\nMIIE\n-----END " + "PRIVATE KEY-----" } });
    fireEvent.click(screen.getByRole("button", { name: "Countersign" }));
    await screen.findByText(PRIVATE_KEY_REFUSAL);
    expect(document.getElementById("approval-sidecarBytes-helper")?.textContent).toContain(PRIVATE_KEY_REFUSAL);
    expect(field.value).toBe("");
    expect(posts()).toHaveLength(0);
    answer = () =>
      json(422, { code: "validation_failed", detail: "d", requestId: "q", errors: [{ field: "sidecarBytes", code: "no_countersignature", message: "the sidecar carries no signature beside the console's" }] });
    fireEvent.change(field, { target: { value: '{"payloadType":"x","signatures":[1]}' } });
    fireEvent.click(screen.getByRole("button", { name: "Countersign" }));
    await screen.findByText("the sidecar carries no signature beside the console's");
    // Beside its field, not in a list under the form.
    expect(document.getElementById("approval-sidecarBytes-helper")?.textContent).toContain("the sidecar carries no signature beside the console's");
    expect(posts().map((p) => p.body)).toEqual([{ sidecarBytes: '{"payloadType":"x","signatures":[1]}' }]);
  });

  it("offers nothing to countersign when the console's confirmation does not exist", async () => {
    serve({ policy: STRICT, confirmation: null });
    renderAt("/team-a/approvals/restore-x");
    await screen.findByText("Nothing to countersign");
    expect(screen.queryByRole("button", { name: "Countersign" })).toBeNull();
  });
});

describe("unbound: the approver's two files", () => {
  it("refuses a key-named file before reading it, and places a plan-hash refusal beside approval.json", async () => {
    serve({
      policy: LEGACY,
      post: () =>
        json(422, { code: "validation_failed", detail: "d", requestId: "q", errors: [{ field: "approvalBytes", code: "plan_mismatch", message: "the approval document names plan hash sha256:aa" }] }),
    });
    renderAt("/team-a/approvals/restore-x");
    const json1 = (await screen.findByLabelText(/^approval\.json/)) as HTMLTextAreaElement;
    const inputs = document.querySelectorAll<HTMLInputElement>('input[type="file"]');
    expect(inputs).toHaveLength(2);
    const read = vi.fn(async () => "ignored");
    const key = Object.assign(new File(["k"], "id_ed25519"), { text: read });
    fireEvent.change(inputs[1]!, { target: { files: [key] } });
    await screen.findByText(PRIVATE_KEY_REFUSAL);
    expect(read).not.toHaveBeenCalled();
    fireEvent.change(json1, { target: { value: '{"plan_hash":"sha256:aa"}\n' } });
    fireEvent.change(screen.getByLabelText(/^approval\.sig/), { target: { value: '{"payloadType":"v1"}' } });
    fireEvent.click(screen.getByRole("button", { name: "Record the approval" }));
    await screen.findByText("the approval document names plan hash sha256:aa");
    expect(document.getElementById("approval-approvalBytes-helper")?.textContent).toContain("the approval document names plan hash sha256:aa");
    expect(posts().map((p) => p.body)).toEqual([{ approvalBytes: '{"plan_hash":"sha256:aa"}\n', sidecarBytes: '{"payloadType":"v1"}' }]);
  });
});

describe("the draft", () => {
  it("keeps the documents in memory across a visit elsewhere, never key text and never in browser storage", async () => {
    const stored = vi.spyOn(window.Storage.prototype, "setItem");
    serve({ policy: LEGACY });
    renderAt("/team-a/approvals/restore-x");
    fireEvent.change(await screen.findByLabelText(/^approval\.json/), { target: { value: '{"plan_hash":"sha256:bb"}' } });
    fireEvent.change(screen.getByLabelText(/^approval\.sig/), { target: { value: "PRIVATE KEY" } });
    cleanup();
    serve({ policy: LEGACY });
    renderAt("/team-a/approvals/restore-x");
    expect(((await screen.findByLabelText(/^approval\.json/)) as HTMLTextAreaElement).value).toBe('{"plan_hash":"sha256:bb"}');
    // NEGATIVE CONTROL: the key text typed beside it was never kept.
    expect((screen.getByLabelText(/^approval\.sig/) as HTMLTextAreaElement).value).toBe("");
    expect(stored).not.toHaveBeenCalled();
    stored.mockRestore();
  });
});

describe("confirm: the requester's confirmation is the authorization", () => {
  it("offers nothing to approve and shows OD-10's typed names from the signed bytes", async () => {
    const typed = { ...packetFixture.item, name: "approval-x", approvalBytes: JSON.stringify({ originalNameConfirmation: { typedTopics: ["orders", "payments"] } }) };
    serve({
      policy: CONFIRM,
      restore: await restore({ approvalSubject: "originalName" }),
      approval: approvalX({ approvalSubject: "originalName", verifiedSubject: { kind: "Restore", name: "restore-x", namespace: "team-a", uid: "uid-x" } }),
      packet: typed,
    });
    renderAt("/team-a/approvals/restore-x");
    await screen.findByText("Confirmed in the console");
    const names = await screen.findByText(/Confirmed by one person with every original topic name re-typed/);
    expect(names.textContent).toContain("orders, payments");
    for (const action of ["Approve", "Countersign", "Record the approval"]) expect(screen.queryByRole("button", { name: action })).toBeNull();
    // NEGATIVE CONTROL: an ordinary restore's record names no typed names and reads no packet.
    cleanup();
    net.sent = [];
    serve({ policy: CONFIRM, approval: approvalX({ verifiedSubject: { kind: "Restore", name: "restore-x", namespace: "team-a", uid: "uid-x" } }), packet: typed });
    renderAt("/team-a/approvals/restore-x");
    await screen.findByText("Confirmed in the console");
    await screen.findByText("The approval record");
    expect(screen.queryByText(/re-typed/)).toBeNull();
    expect(net.sent.some((s) => s.path.endsWith("/packet"))).toBe(false);
  });
});

describe("the Approvals list", () => {
  it("says which waiting request is this session's to approve, and keeps the list when Approvals cannot be read", async () => {
    const mine = await restore({ name: "restore-x" });
    const theirs = await restore({ name: "restore-y", uid: "uid-y", approvalRef: { name: "approval-y" } });
    serve({
      policy: TWO_PERSON,
      restores: [mine, theirs],
      approvals: "error",
      requests: {
        "restore-x": pending(),
        "restore-y": { ...pending(), restore: "restore-y", restoreUid: "uid-y", approve: { offered: false, refusal: "requester", sentence: "You requested this restore." } },
      },
    });
    renderAt("/team-a/approvals");
    await screen.findByText("Waits for you");
    expect(screen.getByText("You requested it: a second person approves")).toBeTruthy();
    expect(screen.getByText(/could not be listed/)).toBeTruthy();
    expect(screen.getAllByText("Unknown: not readable")).toHaveLength(2);
  });

  it("DUI-27: lists a switchover that waits for this approver, opening its own approval page", async () => {
    serve({
      policy: TWO_PERSON,
      restores: [],
      approvals: [],
      switchovers: [swo(), swo({ name: "swo-2", uid: "swo-2-uid", phase: "Succeeded", reason: "Succeeded" })],
      switchoverRequests: { "swo-1": swoRequest() },
    });
    renderAt("/team-a/approvals");
    await screen.findByText("Waits for you");
    expect(screen.getByText("Waiting for approval (1)")).toBeTruthy();
    expect(screen.getByRole("link", { name: "swo-1" }).getAttribute("href")).toContain("/team-a/approvals/switchover/swo-1");
    expect(screen.getByText("Switchover of restore restore-x")).toBeTruthy();
    expect(screen.queryByText("Nothing waits for approval")).toBeNull();
    // CONTROL: an ended switchover does not wait, and is not listed.
    expect(screen.queryByRole("link", { name: "swo-2" })).toBeNull();
  });

  it("DUI-46: a switchover's recorded approval opens its two-person page only in a two-person namespace", async () => {
    const used = {
      ...(approvalFixture.item as Record<string, unknown>),
      name: "swo-approval-1",
      subjectRef: { kind: "Switchover", name: "swo-1" },
    };
    serve({ policy: CONFIRM, restores: [], approvals: [used] });
    renderAt("/team-a/approvals");
    const link = await screen.findByRole("link", { name: "swo-1" });
    expect(link.getAttribute("href")).toBe("#/team-a/switchovers/swo-1");
    expect(net.sent.some((r) => r.path.includes("/approval-request"))).toBe(false);
    cleanup();
    // CONTROL: in a two-person namespace the record opens the switchover's approval page.
    serve({ policy: TWO_PERSON, restores: [], approvals: [used], switchovers: [] });
    renderAt("/team-a/approvals");
    const twoPerson = await screen.findByRole("link", { name: "swo-1" });
    expect(twoPerson.getAttribute("href")).toBe("#/team-a/approvals/switchover/swo-1");
  });

  it("under confirm shows no waiting card, and every recorded approval with its verdict", async () => {
    serve({ policy: CONFIRM, restores: [], approvals: [approvalFixture.item] });
    renderAt("/team-a/approvals");
    await screen.findByText("orders-drill-20260911");
    expect(screen.getByText("Verified")).toBeTruthy();
    expect(screen.queryByText(/Waiting for approval/)).toBeNull();
  });
});

describe("the navigation's count", () => {
  function hook() {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>
        <SessionGate>{() => children}</SessionGate>
      </QueryClientProvider>
    );
    return renderHook(() => useApprovalsNavCount("team-a"), { wrapper });
  }

  it("counts the requests that wait for this session in a two-person namespace, and is absent under confirm", async () => {
    const x = await restore();
    const y = await restore({ name: "restore-y", uid: "uid-y" });
    serve({
      policy: TWO_PERSON,
      restores: [x, y],
      requests: { "restore-x": pending(), "restore-y": { ...pending(), restore: "restore-y", approve: { offered: false, refusal: "requester", sentence: "s" } } },
    });
    const two = hook();
    await waitFor(() => expect(two.result.current).toEqual({ n: 1, tone: "brand", text: "1 request waits for you" }));
    two.unmount();
    // NEGATIVE CONTROL: the same restores under confirm wait for nobody, and no request is read.
    net.sent = [];
    serve({ policy: CONFIRM, restores: [x, y] });
    const confirm = hook();
    await waitFor(() => expect(net.sent.some((s) => s.path === "/namespaces/team-a/approval-policy")).toBe(true));
    await new Promise((r) => setTimeout(r, 50));
    expect(confirm.result.current).toBeUndefined();
    expect(net.sent.some((s) => s.path.endsWith("/approval-request"))).toBe(false);
  });

  it("DUI-27: counts a switchover that waits for this session, as a restore", async () => {
    serve({ policy: TWO_PERSON, restores: [], switchovers: [swo()], switchoverRequests: { "swo-1": swoRequest() } });
    const waiting = hook();
    await waitFor(() => expect(waiting.result.current).toEqual({ n: 1, tone: "brand", text: "1 request waits for you" }));
    waiting.unmount();
    net.sent = [];
    // CONTROL: the requester's own switchover is not offered to them, so nothing waits for this session.
    serve({ policy: TWO_PERSON, restores: [], switchovers: [swo()], switchoverRequests: { "swo-1": swoRequest({ approve: { offered: false, refusal: "requester", sentence: "s" } }) } });
    const own = hook();
    await waitFor(() => expect(net.sent.some((s) => s.path === "/namespaces/team-a/switchovers/swo-1/approval-request")).toBe(true));
    await new Promise((r) => setTimeout(r, 50));
    expect(own.result.current).toBeUndefined();
  });
});
