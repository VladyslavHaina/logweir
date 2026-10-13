// @vitest-environment happy-dom
// Add a destination and Rotate access, end to end through the typed client and
// the mock: typed keys reach the request and nothing else; the rotation sends
// the complete block with the generation it read, and a moved object is
// refused, not overwritten.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { Destination } from "../../api/destinations";
import { createdSecrets } from "../../mock/connections";
import { destinationStore } from "../../mock/destinations";
import { AddDestination } from "./AddDestination";
import { RotateAccess } from "./RotateAccess";

const KEY_ID = "AKIA-TYPED-ONCE-4410";
const KEY_SECRET = "wJalr-typed-once-secret-9902";

function client() {
  return new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
}

function type(id: string, value: string) {
  fireEvent.change(document.getElementById(id) as HTMLInputElement, { target: { value } });
}

function everythingHeld(qc: QueryClient): string {
  const values = [...document.querySelectorAll("input, textarea")].map((e) => (e as HTMLInputElement).value);
  return [
    document.documentElement.outerHTML,
    ...values,
    JSON.stringify(qc.getQueryCache().getAll().map((q) => q.state)),
    JSON.stringify(qc.getMutationCache().getAll().map((m) => m.state)),
  ].join("\n");
}

function recordPosts() {
  const sent: { url: string; body: string; header: Record<string, string> }[] = [];
  const post = api.POST.bind(api);
  vi.spyOn(api, "POST").mockImplementation(((path: never, init: { body?: unknown; params: { header: Record<string, string> } }) => {
    sent.push({ url: String(path), body: JSON.stringify(init.body), header: init.params.header });
    return post(path, init as never);
  }) as never);
  return sent;
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Add a destination", () => {
  it("sends the typed keys once and keeps them nowhere: not the DOM, the inputs, either cache, or the mock's store", async () => {
    const sent = recordPosts();
    const qc = client();
    const onCreated = vi.fn();
    render(
      <QueryClientProvider client={qc}>
        <AddDestination ns="team-a" onClose={() => {}} onCreated={onCreated} />
      </QueryClientProvider>,
    );
    type("destination-name", "archive-secret-row");
    type("destination-bucket", "kafka-archive");
    type("create-archiveWrite-akid", KEY_ID);
    type("create-archiveWrite-sak", KEY_SECRET);
    expect(document.documentElement.outerHTML).not.toContain(KEY_SECRET);
    fireEvent.click(screen.getByRole("button", { name: "Add destination" }));
    await waitFor(() => expect(onCreated).toHaveBeenCalledWith("archive-secret-row"));
    // Control: the request carried both values.
    expect(sent[0]!.body).toContain(KEY_ID);
    expect(sent[0]!.body).toContain(KEY_SECRET);
    expect(sent[0]!.header["Idempotency-Key"]).toMatch(/^logweir-console\.destination\./);
    const held = everythingHeld(qc);
    expect(held).not.toContain(KEY_ID);
    expect(held).not.toContain(KEY_SECRET);
    const made = destinationStore().find((d) => d.name === "archive-secret-row");
    expect(made?.access.archiveWrite.secretName).toBe("lwd-archive-secret-row-archive-write");
    expect(JSON.stringify(destinationStore()) + createdSecrets.join()).not.toContain(KEY_SECRET);
  });

  it("refuses an http endpoint under TLS beside the field, and sends nothing", async () => {
    const sent = recordPosts();
    render(
      <QueryClientProvider client={client()}>
        <AddDestination ns="team-a" onClose={() => {}} onCreated={() => {}} />
      </QueryClientProvider>,
    );
    type("destination-name", "archive-http");
    type("destination-bucket", "kafka-archive");
    type("destination-endpoint", "http://minio:9000");
    type("create-archiveWrite-akid", KEY_ID);
    type("create-archiveWrite-sak", KEY_SECRET);
    fireEvent.click(screen.getByRole("button", { name: "Add destination" }));
    await waitFor(() => expect(document.getElementById("destination-endpoint-helper")?.textContent).toMatch(/plaintext http and the transport says TLS/));
    expect(sent).toHaveLength(0);
    // Control: the typed keys were not cleared, because nothing was sent.
    expect((document.getElementById("create-archiveWrite-sak") as HTMLInputElement).value).toBe(KEY_SECRET);
  });
});

describe("Rotate access", () => {
  function primary(): Destination {
    return structuredClone(destinationStore().find((d) => d.name === "primary")!);
  }

  function rotate(d: Destination, onRotated = vi.fn()) {
    const qc = client();
    render(
      <QueryClientProvider client={qc}>
        <RotateAccess ns="team-a" destination={d} onClose={() => {}} onRotated={onRotated} onReRead={() => {}} />
      </QueryClientProvider>,
    );
    return { qc, onRotated };
  }

  it("sends the complete block with the generation it read, writes the new key once, and keeps it nowhere", async () => {
    const sent = recordPosts();
    const d = primary();
    const { qc, onRotated } = rotate(d);
    fireEvent.change(document.getElementById("rotate-archiveWrite-source") as HTMLSelectElement, { target: { value: "new" } });
    await waitFor(() => expect(document.getElementById("rotate-archiveWrite-akid")).not.toBeNull());
    type("rotate-archiveWrite-akid", KEY_ID);
    type("rotate-archiveWrite-sak", KEY_SECRET);
    fireEvent.click(screen.getByRole("button", { name: "Rotate access" }));
    await waitFor(() => expect(onRotated).toHaveBeenCalled());
    const body = JSON.parse(sent[0]!.body);
    expect(body.expectedGeneration).toBe(d.generation);
    expect(body.access.archiveWrite.secret.new).toEqual({ accessKeyId: KEY_ID, secretAccessKey: KEY_SECRET });
    // Every other grant is kept as it is, so nothing is removed by omission.
    expect(body.access.archiveRead).toEqual({ mode: "secretKeys", secret: { existing: { name: "lwd-primary-archive-read" } } });
    expect(body.access.evidenceRead).toEqual({ mode: "archiveReadGrant" });
    expect(body.access.evidenceWrite).toBeUndefined();
    expect(sent[0]!.header["Idempotency-Key"]).toBeUndefined();
    expect(onRotated.mock.calls[0]![0].generation).toBe(d.generation + 1);
    expect(everythingHeld(qc)).not.toContain(KEY_SECRET);
    expect(JSON.stringify(destinationStore())).not.toContain(KEY_SECRET);
  });

  it("is refused, not applied, when the destination moved since this page read it", async () => {
    const stale = primary();
    stale.generation -= 1;
    const { onRotated } = rotate(stale);
    fireEvent.click(screen.getByRole("button", { name: "Rotate access" }));
    await screen.findByText("The destination changed since this page read it. Nothing was written.");
    expect(onRotated).not.toHaveBeenCalled();
    // Control: the same rotation from the generation as it is now lands.
    cleanup();
    const fresh = rotate(primary());
    fireEvent.click(screen.getByRole("button", { name: "Rotate access" }));
    await waitFor(() => expect(fresh.onRotated).toHaveBeenCalled());
  });
});
