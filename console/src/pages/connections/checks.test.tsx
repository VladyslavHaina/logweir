// What a readiness check and a topic discovery may say, rendered from the
// console fixtures: nothing is a pass that did not pass, a successful listing
// is never complete, and Test connection sends one reference.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { api } from "../../api/client";
import type { Connection, Preflight } from "../../api/connections";
import type { components } from "../../api/schema";
import { SessionProvider } from "../../api/session";
import { session } from "../../mock/data";
import { ConnectionCheck } from "./ConnectionDetail";
import { Discovery } from "./Discovery";
import { CheckResult, ConnectionUsability, LastTestLine, ProbeReason, ReasonText, advisoryRows, basisWords } from "./parts";
import { probeState } from "../../api/connections";

type S = components["schemas"];
const fixtures = import.meta.glob("../../../fixtures/console/*.json", { eager: true, import: "default" });
function fixture<T>(name: string): T {
  const found = fixtures[`../../../fixtures/console/${name}`];
  if (found === undefined) throw new Error(`no fixture ${name}`);
  return structuredClone(found) as T;
}
const preflight = (name: string) => fixture<S["PreflightResponse"]>(name).item as Preflight;
const text = () => document.body.textContent ?? "";

function withClient(node: ReactNode) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <SessionProvider session={session}>{node}</SessionProvider>
    </QueryClientProvider>,
  );
}

function answer(data: unknown) {
  return { data, error: undefined, response: new Response(null, { status: 200 }) } as never;
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("a readiness check", () => {
  it("prints a not-ready row's code, message and remedy, and lists execution-only checks apart", () => {
    render(<CheckResult preflight={preflight("preflight-not-ready.json")} stopped="" />);
    expect(screen.getAllByText("Not ready").length).toBeGreaterThan(0);
    expect(text()).toContain("AccessDenied");
    expect(text()).toContain("the object store refused the listing for this prefix");
    expect(text()).toContain("Remedy: Grant s3:ListBucket");
    expect(text()).toContain("destination.archivePrefixWritable");
    expect(text()).toContain("Ready never means they passed");
  });

  it("never shows a skipped check or an empty result as a pass", () => {
    render(<CheckResult preflight={preflight("preflight-skipped.json")} stopped="" />);
    expect(screen.getByText("Skipped (never a pass)")).toBeTruthy();
    expect(screen.queryByText("Ready")).toBeNull();
    cleanup();
    render(<CheckResult preflight={preflight("preflight-cancelled.json")} stopped="" />);
    expect(screen.getByText("Cancelled: no result")).toBeTruthy();
    expect(text()).toContain("That is not a pass");
    // Control: a ready result does say Ready.
    cleanup();
    render(<CheckResult preflight={preflight("preflight-ready.json")} stopped="" />);
    expect(screen.getAllByText("Ready").length).toBeGreaterThan(0);
  });

  it("says a stale result no longer applies, why, and that an unverifiable comparison could not be checked", () => {
    render(<CheckResult preflight={preflight("preflight-stale.json")} stopped="" />);
    expect(screen.getByText("No longer applies")).toBeTruthy();
    expect(text()).toContain("planHashChanged");
    expect(text()).toContain("referentChanged (BackupDestination/primary)");
    expect(text()).toMatch(/could not be checked \(TrustRoster\/8f0a\)/);
    expect(text()).not.toMatch(/unverifiable/);
  });

  it("says an unfinished check is still being read, and why it stopped when it did", () => {
    const pending = preflight("preflight-pending.json");
    render(<CheckResult preflight={pending} stopped="" />);
    expect(text()).toContain("this page reads it again until then");
    cleanup();
    const again = vi.fn();
    render(<CheckResult preflight={pending} stopped="deadline" onRunAgain={again} />);
    expect(text()).toContain("did not finish in the longest time a check may take");
    expect(text()).not.toContain("reads it again until then");
    fireEvent.click(screen.getByRole("button", { name: "Run the check again" }));
    expect(again).toHaveBeenCalled();
  });

  it("labels a stale recorded test, and says when none is recorded", () => {
    render(<LastTestLine test={{ preflightId: "pf-1", state: "ready", stale: true, truncated: false }} noun="access test" subject="destination" />);
    expect(screen.getByText("Stale: not health")).toBeTruthy();
    cleanup();
    render(<LastTestLine test={null} noun="access test" subject="destination" />);
    expect(text()).toContain("No access test is recorded for this destination");
  });
});

describe("theme A's D6: a check run on a runner older than the controller", () => {
  it("shows runner.image not ready with CheckContractMismatch, and runner.pod as a pod that ran", () => {
    const p = preflight("preflight-ready.json");
    const at = "2026-09-17T04:58:00Z";
    const older: Preflight = {
      ...p,
      state: "notReady",
      checks: [
        { id: "runner.image", category: "runner", state: "notReady", gating: "blocking", authority: "podStatus", code: "CheckContractMismatch", message: "RunnerOlderThanController: the runner image is older than this controller (it did not know the plan field `capabilityChecks`)", observedAt: at },
        { id: "runner.pod", category: "runner", state: "ready", gating: "blocking", authority: "podStatus", code: "PodCompleted", message: "the check pod ran and its runner exited 1; the runner image is older than this controller (runner.image)", observedAt: at },
      ],
    };
    render(<CheckResult preflight={older} stopped="" />);
    const image = screen.getByText("runner.image").closest("tr") as HTMLElement;
    expect(image.textContent).toContain("Not ready");
    expect(image.textContent).toContain("CheckContractMismatch");
    expect(image.textContent).toContain("RunnerOlderThanController");
    const pod = screen.getByText("runner.pod").closest("tr") as HTMLElement;
    expect(pod.textContent).toContain("Ready");
    expect(pod.textContent).toContain("the check pod ran and its runner exited 1");
    expect(text()).not.toMatch(/has not started/);
  });
});

describe("Test connection", () => {
  const conn = (reason: string): Connection => ({
    ...(fixture<S["ConnectionResponse"]>("connection.json").item as Connection),
    reachability: reason ? { state: "unknown", reason } : { state: "reachable", reason: "Reachable" },
  });

  it("is not offered for a connection the controller refused, and prints the controller's own reason", () => {
    withClient(<ConnectionCheck ns="team-a" c={conn("ConnectionConfigInvalid")} mayStart readable />);
    expect((screen.getByRole("button", { name: "Test connection" }) as HTMLButtonElement).disabled).toBe(true);
    expect(text()).toContain("ConnectionConfigInvalid");
    expect(text()).toContain("the saved settings contradict each other");
  });

  it("tells a viewer it may read checks and not start one", () => {
    withClient(<ConnectionCheck ns="team-a" c={conn("")} mayStart={false} readable />);
    expect(screen.queryByRole("button", { name: "Test connection" })).toBeNull();
    expect(text()).toContain("may read connection checks in this namespace and not start one");
  });

  it("sends one reference and nothing it does not ask about, then follows the check it made", async () => {
    const bodies: unknown[] = [];
    const post = api.POST.bind(api);
    vi.spyOn(api, "POST").mockImplementation(((path: never, init: { body: unknown }) => {
      bodies.push(init.body);
      return post(path, init as never);
    }) as never);
    withClient(<ConnectionCheck ns="team-a" c={{ ...conn(""), name: "orders-prod" }} mayStart readable />);
    fireEvent.click(screen.getByRole("button", { name: "Test connection" }));
    // While the request is in flight a second click is not offered.
    await waitFor(() => expect((screen.getByRole("button", { name: /Test connection/ }) as HTMLButtonElement).disabled).toBe(true));
    await screen.findByText("Blocking checks");
    expect(bodies).toEqual([{ operation: "sourceConnection", sourceConnection: { connectionRef: "orders-prod" } }]);
  });
});

describe("FX-46: the probe's reason and what the endpoint is usable for", () => {
  type Row = S["CheckEntryView"];
  const row = (id: string, state: Row["state"], message: string): Row => ({ id, state, code: "X", gating: "advisory", message });
  /** A Test connection's check as the API serves it: an advisory notReady row is in `warnings`. */
  const sourceConnection = (rows: Row[]): Preflight => {
    const p = preflight("preflight-ready.json");
    return { ...p, id: "pf-test", operation: "sourceConnection", checks: [...p.checks, ...rows.filter((r) => r.state !== "notReady")], warnings: rows.filter((r) => r.state === "notReady") };
  };
  const redpanda = () =>
    sourceConnection([
      row("connection.engineProtocol", "ready", "serves the capture requests"),
      row("target.engineProtocol", "notReady", "the engine sends Produce v8 and this endpoint serves Produce v0-v7"),
      row("connection.groupTypes", "unknown", "BlockedByPrerequisite: not asked"),
    ]);

  it("says AdvertisedBrokersUnreachable in its own words", () => {
    render(<ReasonText reason="AdvertisedBrokersUnreachable" />);
    expect(text()).toContain("AdvertisedBrokersUnreachable");
    expect(text()).toContain("Not reachable: the bootstrap address answered, but the brokers this cluster advertises cannot be reached from the cluster");
    expect(text()).not.toMatch(/earlier probe/);
  });

  it("shows the three lines, each no or not determined with its message, and one summary", () => {
    render(<ConnectionUsability preflight={redpanda()} />);
    expect(screen.getByRole("heading", { name: "Usable as a source, not as a target" })).toBeTruthy();
    const region = screen.getByRole("region", { name: "What this endpoint is usable for" });
    expect(region.textContent).toMatch(/Usable as a backup source: Yes/);
    expect(region.textContent).toMatch(/Usable as a restore target: No the engine sends Produce v8 and this endpoint serves Produce v0-v7/);
    expect(region.textContent).toMatch(/Consumer group positions can be recorded: Not determined BlockedByPrerequisite: not asked/);
    // A yes carries no message.
    expect(region.textContent).not.toContain("serves the capture requests");
  });

  it("shows nothing for an older controller's check, which carries none of the rows", () => {
    render(<ConnectionUsability preflight={preflight("preflight-ready.json")} />);
    expect(screen.queryByRole("region", { name: "What this endpoint is usable for" })).toBeNull();
    expect(text()).not.toMatch(/Usable as/);
  });

  it("reads the recorded test's own check, by lastTest.preflightId, to say what the endpoint is usable for", async () => {
    const asked: string[] = [];
    vi.spyOn(api, "GET").mockImplementation(((path: string, init: { params: { path: { id?: string } } }) => {
      asked.push(`${path} ${init.params.path.id ?? ""}`);
      if (path === "/api/v1/namespaces/{ns}/preflights/{id}" && init.params.path.id === "pf-recorded") return Promise.resolve(answer({ item: redpanda(), requestId: "r" }));
      return Promise.reject(new Error(`unexpected ${path}`));
    }) as never);
    const c: Connection = {
      ...(fixture<S["ConnectionResponse"]>("connection.json").item as Connection),
      reachability: { state: "reachable", reason: "Reachable" },
      lastTest: { preflightId: "pf-recorded", state: "ready", stale: false, truncated: false },
    };
    withClient(<ConnectionCheck ns="team-a" c={c} mayStart readable />);
    await screen.findByRole("heading", { name: "Usable as a source, not as a target" });
    expect(asked).toEqual(["/api/v1/namespaces/{ns}/preflights/{id} pf-recorded"]);
    expect(text()).toContain("Last connectivity check");
  });
});

describe("topic discovery", () => {
  it("shows both slots apart, the failed attempt's error, and an inventory that is never called complete", async () => {
    vi.spyOn(api, "GET").mockImplementation(((path: string) => {
      if (path.endsWith("/topic-discoveries")) return Promise.resolve(answer(fixture("discovery-latest.json")));
      if (path.endsWith("/topics")) return Promise.resolve(answer(fixture("topics-page.json")));
      return Promise.reject(new Error(`unexpected ${path}`));
    }) as never);
    withClient(<Discovery ns="team-a" name="source" mayStart readable />);
    await screen.findByText("Last successful inventory");
    expect(screen.getByText("Latest attempt")).toBeTruthy();
    expect(text()).toContain("BrokerUnreachable");
    expect(text()).toContain("a successful listing is not proof that this is every topic");
    await screen.findByText("__consumer_offsets");
    expect(text()).toContain("UnknownTopicOrPartition");
    // A short page is not the last page: the scan stopped at its budget, and the cursor is offered.
    expect(text()).toContain("not at the end of the result");
    expect(screen.getByRole("button", { name: "Read the next page" })).toBeTruthy();
  });

  it("says an attestation is not verified by Logweir, and an empty inventory is not an empty cluster", async () => {
    const attested = fixture<{ item: S["TopicDiscovery"] }>("discovery-attested.json").item;
    const empty = fixture<{ item: S["TopicDiscovery"] }>("discovery-empty.json").item;
    let slots = { latestAttempt: attested, lastSuccessful: attested };
    vi.spyOn(api, "GET").mockImplementation(((path: string) =>
      Promise.resolve(answer(path.endsWith("/topics") ? { items: [], page: { limit: 25 }, scan: { chunksScanned: 1, complete: true }, requestId: "r" } : slots))) as never);
    withClient(<Discovery ns="team-a" name="source" mayStart readable />);
    await screen.findByText(/also the last successful inventory/);
    expect(text()).toContain("attested by platform-team@example.com at 2026-09-01T00:00:00Z; not verified by Logweir");
    cleanup();
    slots = { latestAttempt: empty, lastSuccessful: empty };
    withClient(<Discovery ns="team-a" name="source" mayStart readable />);
    await screen.findByText(/this is not proof that the cluster is empty/);
    await waitFor(() => expect(text()).toContain("This is the end of the stored result."));
  });
});

describe("DUI-7 and DUI-11: what a check's rows say", () => {
  type Entry = Preflight["checks"][number];
  const row = (id: string, gating: Entry["gating"], state: Entry["state"]): Entry => ({ id, gating, state, message: `${id} said ${state}` }) as Entry;

  it("lists a ready advisory row from checks under Advisory, once, and never under Blocking", () => {
    const p = preflight("preflight-ready.json");
    p.checks = [row("connection.resolved", "blocking", "ready"), row("connection.engineProtocol", "advisory", "ready")];
    p.warnings = [row("connection.groupTypes", "advisory", "notReady")];
    expect(advisoryRows(p).map((r) => r.id)).toEqual(["connection.engineProtocol", "connection.groupTypes"]);
    render(<CheckResult preflight={p} stopped="" />);
    const advisory = screen.getByRole("grid", { name: "Advisory checks" });
    expect(advisory.textContent).toContain("connection.engineProtocol");
    expect(advisory.textContent).toContain("connection.groupTypes");
    // The control: the blocking row stays in Blocking, and the advisory one is not there too.
    const blocking = screen.getByRole("grid", { name: "Blocking checks" });
    expect(blocking.textContent).toContain("connection.resolved");
    expect(blocking.textContent).not.toContain("connection.engineProtocol");
  });

  it("DUI-36: an advisory row that is not met is a warning, never red; a blocking one stays red (the control)", () => {
    const p = preflight("preflight-not-ready.json");
    p.checks = [row("connection.resolved", "blocking", "notReady"), row("connection.groupTypes", "advisory", "notReady")];
    p.warnings = [];
    render(<CheckResult preflight={p} stopped="" />);
    const tone = (table: HTMLElement) => within(table).getByText("Not ready").closest(".pf-v6-c-label")!.className;
    expect(tone(screen.getByRole("grid", { name: "Advisory checks" }))).toMatch(/pf-m-warning/);
    expect(tone(screen.getByRole("grid", { name: "Advisory checks" }))).not.toMatch(/pf-m-danger/);
    expect(tone(screen.getByRole("grid", { name: "Blocking checks" }))).toMatch(/pf-m-danger/);
  });

  it("does not list an advisory row twice when it is in checks and in warnings", () => {
    const twice = row("connection.groupTypes", "advisory", "notReady");
    expect(advisoryRows({ checks: [twice], warnings: [twice] })).toHaveLength(1);
  });

  it("puts the comparison basis in words, never the API's raw tokens", () => {
    expect(["expiry", "planHash", "referents:2", "referents:1"].map(basisWords)).toEqual(["the expiry", "the plan hash", "2 referenced objects", "1 referenced object"]);
    expect(basisWords("policyDigest:byController(the reconciler compares it each pass and downgrades the result)")).toBe(
      "the installation policy, compared by the controller (the reconciler compares it each pass and downgrades the result)",
    );
    // A token this build does not know is shown as it came, with a space before its note.
    expect(basisWords("future:thing(why)")).toBe("future:thing (why)");
    const p = preflight("preflight-ready.json");
    p.staleBasis = ["expiry", "referents:2", "policyDigest:byController(the reconciler compares it each pass and downgrades the result)"];
    render(<CheckResult preflight={p} stopped="" />);
    expect(text()).toContain("Compared: the expiry; 2 referenced objects; the installation policy, compared by the controller (the reconciler");
    expect(text()).not.toContain("referents:2");
  });

  it("glosses a probe that reported the cluster unreachable with the controller's words", () => {
    render(<ReasonText reason="ProbeReportedUnreachable" />);
    expect(text()).toContain("the broker did not answer, or it refused the credential this connection names");
  });
});

describe("DUI-31.6: the Reason while a probe runs again", () => {
  const at = new Date(Date.now() - 60_000).toISOString();

  it("says the new probe's reason alone, never \"Not recorded\" beside it", () => {
    render(<ProbeReason state={probeState({ state: "reachable", reason: "ProbeRunning", observedAt: at } as never)} />);
    expect(text()).toBe("ProbeRunning: a new probe is running now; the reachable reading is from an earlier probe");
    expect(text()).not.toContain("Not recorded");
  });

  it("says \"Not recorded\" only when there is no reason at all (the control), and the reading's own reason otherwise", () => {
    render(<ProbeReason state={probeState({ state: "reachable", observedAt: at } as never)} />);
    expect(text()).toBe("Not recorded");
    cleanup();
    render(<ProbeReason state={probeState({ state: "unreachable", reason: "ProbeReportedUnreachable", observedAt: at } as never)} />);
    expect(text()).toMatch(/^ProbeReportedUnreachable: the probe ran and reported reachable=false/);
  });
});
