// @vitest-environment jsdom
// OD-23-core's screens (OD-23-core.md §4, §6 B4), over the in-page mock: the Schema
// Registry on Add connection and its test line, the capture line of a backup and a
// point, the restore's byte-rate limit and schema preview and `schemas` line, and an
// interrupted restore's partial targets, Retry and Clean up.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, configure, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { api } from "../api/client";
import { SessionProvider } from "../api/session";
import { connections, session } from "../mock/data";
import { createdSecrets } from "../mock/connections";
import { SessionGate } from "../app/SessionGate";
import { createConsoleRouter } from "../router";
import { AddConnection } from "./connections/AddConnection";
import { RegistryLine } from "./connections/parts";
import { registryCaptureWords } from "./catalog/model";
import { renderPlanBytes } from "./restores/plan";
import { rateLimitBytes, restoreSchemasWords } from "./restores/words";
import { CheckStep } from "./restores/WizardSteps";
import type { Preflight } from "./restores/model";
import planFields from "../../fixtures/plan-fields.json";
import type { PlanFields } from "./restores/plan";

configure({ asyncUtilTimeout: 15_000 });
vi.setConfig({ testTimeout: 40_000 });

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const type = (id: string, value: string) => fireEvent.change(document.getElementById(id) as HTMLInputElement, { target: { value } });

describe("PROD-03.1: a Schema Registry on Add connection", () => {
  const REGISTRY_SECRET = "Reg1stry-typed-once-5521";

  function setup() {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
    const onCreated = vi.fn();
    render(
      <QueryClientProvider client={client}>
        <SessionProvider session={session}>
          <AddConnection ws="team-a" onClose={() => {}} onCreated={onCreated} />
        </SessionProvider>
      </QueryClientProvider>,
    );
    return { client, onCreated };
  }

  function spy(): unknown[] {
    const sent: unknown[] = [];
    const post = api.POST.bind(api);
    vi.spyOn(api, "POST").mockImplementation(((path: never, init: { body: unknown }) => {
      sent.push(JSON.parse(JSON.stringify(init.body)));
      return post(path, init as never);
    }) as never);
    return sent;
  }

  it("sends the URL, username and password in schemaRegistry, and keeps the password nowhere", async () => {
    const sent = spy();
    const { client, onCreated } = setup();
    type("connection-servers", "kafka-0.registry-row.svc:9093");
    type("connection-username", "logweir-backup");
    type("connection-password", "kafka-pass-1");
    fireEvent.click(screen.getByRole("button", { name: "Schema Registry (optional)" }));
    type("connection-registry-url", "https://schema-registry.kafka.svc:8081");
    type("connection-registry-username", "logweir");
    type("connection-registry-password", REGISTRY_SECRET);
    fireEvent.click(screen.getByRole("button", { name: "Add connection" }));
    await waitFor(() => expect(onCreated).toHaveBeenCalled());
    expect(sent).toHaveLength(1);
    expect((sent[0] as { schemaRegistry: unknown }).schemaRegistry).toEqual({ url: "https://schema-registry.kafka.svc:8081", username: "logweir", password: REGISTRY_SECRET });
    const held = [document.documentElement.outerHTML, JSON.stringify(client.getMutationCache().getAll().map((m) => m.state)), JSON.stringify(connections), createdSecrets.join()].join("\n");
    expect(held).not.toContain(REGISTRY_SECRET);
    const made = connections.find((c) => c.name === onCreated.mock.calls[0]![0]) as unknown as { schemaRegistry?: unknown };
    expect(made.schemaRegistry).toEqual({ url: "https://schema-registry.kafka.svc:8081", username: "logweir" });
  });

  it("sends no schemaRegistry when the block is left empty (the control)", async () => {
    const sent = spy();
    const { onCreated } = setup();
    type("connection-servers", "kafka-0.no-registry.svc:9093");
    type("connection-username", "logweir-backup");
    type("connection-password", "kafka-pass-2");
    fireEvent.click(screen.getByRole("button", { name: "Add connection" }));
    await waitFor(() => expect(onCreated).toHaveBeenCalled());
    expect(sent[0]).not.toHaveProperty("schemaRegistry");
  });

  it("refuses a registry password without a username, or a URL with userinfo, beside the field and sends nothing", async () => {
    const sent = spy();
    setup();
    type("connection-servers", "kafka-0.refused.svc:9093");
    type("connection-username", "logweir-backup");
    type("connection-password", "kafka-pass-3");
    fireEvent.click(screen.getByRole("button", { name: "Schema Registry (optional)" }));
    type("connection-registry-url", "https://user:pw@schema-registry.kafka.svc:8081");
    type("connection-registry-password", "x");
    fireEvent.click(screen.getByRole("button", { name: "Add connection" }));
    expect(await screen.findByText(/The URL carries no username or password/)).toBeTruthy();
    expect(screen.getByText(/A registry password goes with a username/)).toBeTruthy();
    expect(sent).toEqual([]);
  });
});

describe("PROD-03.1: the registry's test line and capture line", () => {
  const preflight = (rows: Preflight["checks"]) => ({ checks: rows, warnings: [] }) as unknown as Preflight;
  it("the connection test names the registry's verdict, and says nothing when the test had no registry row", () => {
    render(<RegistryLine preflight={preflight([{ id: "registry.reachable", state: "notReady", gating: "advisory", code: "RegistryUnauthorized", message: "answered 401" } as never])} />);
    expect(document.getElementById("registry-check-line")?.textContent).toMatch(/Schema Registry:.*answered 401/);
    cleanup();
    render(<RegistryLine preflight={preflight([{ id: "connection.authenticated", state: "ready", gating: "blocking" } as never])} />);
    expect(document.getElementById("registry-check-line")).toBeNull();
  });
  it("a capture says its counts, a failed capture says why, and an older receipt with no block says not captured", () => {
    expect(registryCaptureWords({ captured: true, subjects: 4, versions: 7 })).toBe("Captured: 4 subjects, 7 versions");
    expect(registryCaptureWords({ captured: false, incompleteReason: "registryUnreachable" })).toBe("Not captured: the registry did not answer");
    expect(registryCaptureWords({ captured: true, subjects: 1, versions: 1, incompleteReason: "snapshotIncomplete" })).toMatch(/^Captured: 1 subject, 1 version; incomplete:/);
    expect(registryCaptureWords(null)).toBe("Not captured");
  });
});

describe("PROD-00.3h and PROD-03.2: the restore's rate limit, schema preview and schemas line", () => {
  const base = planFields as unknown as PlanFields;
  it("a byte-rate limit is one plan line in the restore block, in bytes per second; absent changes no byte", () => {
    const without = renderPlanBytes(base);
    const limited = renderPlanBytes({ ...base, rateLimitBytesPerSec: 500_000 });
    expect(limited).toContain("\n  rate_limit_bytes_per_sec: 500000\n");
    expect(limited.replace("  rate_limit_bytes_per_sec: 500000\n", "")).toBe(without);
    expect(limited.indexOf("rate_limit_bytes_per_sec")).toBeGreaterThan(limited.indexOf("\nrestore:\n"));
    expect(limited.indexOf("rate_limit_bytes_per_sec")).toBeLessThan(limited.indexOf("\nsample:\n"));
    expect(() => renderPlanBytes({ ...base, rateLimitBytesPerSec: 0 })).toThrow(/at least 1/);
    expect(rateLimitBytes("0.5")).toBe(500_000);
    expect(rateLimitBytes("50")).toBe(50_000_000);
    expect(rateLimitBytes("")).toBeNull();
    for (const bad of ["0", "-1", "1e3", "fast"]) expect(rateLimitBytes(bad)).toBe("invalid");
  });

  it("the readiness step shows the target.schemas preview, and nothing when the check has none", () => {
    const result = (rows: unknown[]) =>
      ({
        id: "pf-s", namespace: "t", uid: "u", resourceVersion: "1", operation: "restore", state: "ready", terminal: true, applicable: true,
        stale: false, staleBasis: [], staleReasons: [], binding: { planHash: "sha256:aa", referents: [] }, warnings: [], executionOnly: [], conditions: [], detailsAvailable: false,
        checks: rows,
      }) as unknown as Preflight;
    const step = (rows: unknown[]) => (
      <CheckStep readiness={{ result: result(rows), boundHash: "sha256:aa", boundKey: "k", error: null, starting: false, stoppedAt: null }} refusal={null} canCheck hash="sha256:aa" planProblem={null} connection={null} onStart={() => undefined} onCancel={() => undefined} />
    );
    render(step([{ id: "target.schemas", code: "SchemasImportable", state: "ready", gating: "blocking", message: "will import 3 subjects, none conflict" }]));
    expect(screen.getByRole("heading", { name: "Schemas" })).toBeTruthy();
    expect(document.getElementById("schema-preview")?.textContent).toContain("will import 3 subjects, none conflict");
    cleanup();
    render(step([{ id: "target.mappedTopics", code: "MappedTopicsAbsent", state: "ready", gating: "blocking" }]));
    expect(screen.queryByRole("heading", { name: "Schemas" })).toBeNull();
  });

  it("the schemas line names what was imported, or why nothing was", () => {
    expect(restoreSchemasWords({ imported: [{ subject: "orders-value", version: 1, id: 4 }], alreadyPresent: 2 })).toBe("Imported 1 with the source's IDs; 2 already present: orders-value v1 (id 4).");
    expect(restoreSchemasWords({ notImported: "noTargetRegistry" })).toMatch(/names no Schema Registry/);
    expect(restoreSchemasWords(null)).toBeNull();
  });
});

describe("PROD-07.2: an interrupted restore on the mock", () => {
  function open(path: string) {
    window.history.replaceState(null, "", `/#${path}`);
    window.matchMedia ??= ((query: string) => ({ matches: false, media: query, addEventListener: () => undefined, removeEventListener: () => undefined })) as unknown as typeof window.matchMedia;
    const router = createConsoleRouter();
    render(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <SessionGate>{(s) => <RouterProvider router={router} context={{ session: s }} />}</SessionGate>
      </QueryClientProvider>,
    );
    return router;
  }

  it("shows the signed partial targets, offers Retry into new topics and Clean up, and Clean up removes them", async () => {
    const router = open("/team-a/restores/orders-drill-stopped-m3t8q");
    expect(await screen.findByText("Stopped before it finished; nothing was resumed")).toBeTruthy();
    expect(screen.getByText(/Its runner stopped/)).toBeTruthy();
    const table = await screen.findByLabelText("Complete coverage by partition");
    expect(within(table).getAllByText("orders").length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Clean up partial topics" }));
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByRole("list", { name: "Topics to delete" }).textContent).toContain("drill3-orders");
    fireEvent.click(within(dialog).getByRole("button", { name: "Delete these topics" }));
    expect(await screen.findByText("Removing the partial topics.")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Clean up partial topics" })).toBeNull();
    expect(await screen.findByText(/The partial topics were removed/, {}, { timeout: 15_000 })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Retry into new topics/ }));
    await waitFor(() => expect(router.state.location.pathname).toBe("/team-a/restores/new"));
    expect(router.state.location.search).toEqual({ retryOf: "orders-drill-stopped-m3t8q" });
  });

  it("a restore that passed shows its imported schemas and no Interrupted state (the control)", async () => {
    open("/team-a/restores/orders-drill-weekly-a7k2p");
    expect(await screen.findByText(/Imported 4 with the source's IDs/)).toBeTruthy();
    expect(screen.queryByText("Stopped before it finished; nothing was resumed")).toBeNull();
    expect(screen.queryByRole("button", { name: "Clean up partial topics" })).toBeNull();
  });
});
