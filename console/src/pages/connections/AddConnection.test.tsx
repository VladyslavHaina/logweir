// The Add a connection form, end to end through the typed client and the mock:
// the typed credential reaches the request and nothing else, one click makes
// one connection, the API's field errors land beside their fields, and a write
// with no answer says what is known.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { api } from "../../api/client";
import { SessionProvider } from "../../api/session";
import { connections, session } from "../../mock/data";
import { createdSecrets } from "../../mock/connections";
import { AddConnection } from "./AddConnection";

const SECRET = "Sup3r-s3cret-typed-once-7731";

function setup(ws = "team-a") {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const onCreated = vi.fn();
  const view = render(
    <QueryClientProvider client={client}>
      <SessionProvider session={session}>
        <AddConnection ws={ws} onClose={() => {}} onCreated={onCreated} />
      </SessionProvider>
    </QueryClientProvider>,
  );
  return { client, onCreated, view };
}

function type(id: string, value: string) {
  const el = document.getElementById(id) as HTMLInputElement;
  fireEvent.change(el, { target: { value } });
}

function fill(servers: string) {
  type("connection-servers", servers);
  type("connection-username", "logweir-backup");
  type("connection-password", SECRET);
}

const submit = () => fireEvent.click(screen.getByRole("button", { name: "Add connection" }));

/** Everything the page holds, as text: the DOM, every control's value, both caches. */
function everythingHeld(client: QueryClient): string {
  const values = [...document.querySelectorAll("input, textarea")].map((e) => (e as HTMLInputElement).value);
  const queries = client.getQueryCache().getAll().map((q) => q.state);
  const mutations = client.getMutationCache().getAll().map((m) => m.state);
  return [document.documentElement.outerHTML, ...values, JSON.stringify(queries), JSON.stringify(mutations)].join("\n");
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("Add a connection", () => {
  it("sends the typed password once and keeps it nowhere: not the DOM, the inputs, the query or mutation cache, or the mock's store", async () => {
    const sent: string[] = [];
    const post = api.POST.bind(api);
    vi.spyOn(api, "POST").mockImplementation(((path: never, init: { body: unknown }) => {
      sent.push(JSON.stringify(init.body));
      return post(path, init as never);
    }) as never);
    const { client, onCreated } = setup();
    fill("kafka-0.secret-row.svc:9093");
    // While typed, the value lives in the input's property only, never in an attribute.
    expect(document.documentElement.outerHTML).not.toContain(SECRET);
    submit();
    await waitFor(() => expect(onCreated).toHaveBeenCalled());
    // Control: the request carried it, so the search below would find it if anything kept it.
    expect(sent.join("")).toContain(SECRET);
    expect(everythingHeld(client)).not.toContain(SECRET);
    expect((document.getElementById("connection-password") as HTMLInputElement).value).toBe("");
    const made = connections.find((c) => c.name === onCreated.mock.calls[0]![0]);
    expect(made?.auth.credentialRef?.name).toBe(`${made?.name}-credential`);
    expect(JSON.stringify(connections) + createdSecrets.join()).not.toContain(SECRET);
  });

  it("does not carry a password typed for SASL into another kind of authentication", async () => {
    setup();
    type("connection-password", SECRET);
    const mode = document.getElementById("connection-mode") as HTMLSelectElement;
    fireEvent.change(mode, { target: { value: "scramSha256" } });
    // Control: another SASL mode keeps what was typed.
    expect((document.getElementById("connection-password") as HTMLInputElement).value).toBe(SECRET);
    fireEvent.change(mode, { target: { value: "mtls" } });
    await waitFor(() => expect(document.getElementById("connection-private-key")).not.toBeNull());
    fireEvent.change(mode, { target: { value: "scramSha512" } });
    await waitFor(() => expect((document.getElementById("connection-password") as HTMLInputElement).value).toBe(""));
  });

  it("makes one connection from a double click, and a new one from a deliberate second create", async () => {
    const keys: string[] = [];
    const post = api.POST.bind(api);
    vi.spyOn(api, "POST").mockImplementation(((path: never, init: { params: { header: Record<string, string> } }) => {
      keys.push(init.params.header["Idempotency-Key"]!);
      return post(path, init as never);
    }) as never);
    const before = connections.length;
    const { onCreated } = setup();
    fill("kafka-0.double-click.svc:9093");
    submit();
    submit();
    await waitFor(() => expect(onCreated).toHaveBeenCalled());
    await new Promise((r) => setTimeout(r, 500));
    // Both clicks reached the service, with one key between them.
    expect(keys.length).toBe(2);
    expect(new Set(keys).size).toBe(1);
    expect(connections.length).toBe(before + 1);
    // Control: the next create is another intent, so another key and another object.
    fill("kafka-0.second-create.svc:9093");
    submit();
    await waitFor(() => expect(onCreated).toHaveBeenCalledTimes(2));
    expect(new Set(keys).size).toBe(2);
    expect(connections.length).toBe(before + 2);
  });

  it("puts the API's field errors beside their fields and lists the rest", async () => {
    vi.spyOn(api, "POST").mockResolvedValue({
      data: undefined,
      error: {
        status: 422,
        code: "validation_failed",
        title: "Request validation failed",
        detail: "One or more fields are invalid.",
        requestId: "01M2N5SHWR0N2E7SDCW2H22Z8W",
        errors: [
          { field: "bootstrapServers[0]", code: "invalid_port", message: "must be host:port with no scheme or userinfo" },
          { field: "auth.username", code: "required", message: "scramSha512 requires a username" },
          { field: "markerTopic", code: "invalid", message: "is not a topic name" },
        ],
      },
      response: new Response(null, { status: 422 }),
    } as never);
    setup();
    fill("kafka-0.refused.svc:9093");
    submit();
    await screen.findByText("must be host:port with no scheme or userinfo");
    expect(document.getElementById("connection-servers-helper")?.textContent).toContain("must be host:port");
    expect(document.getElementById("connection-username-helper")?.textContent).toContain("requires a username");
    expect(screen.getByText("markerTopic: is not a topic name")).toBeTruthy();
    expect(screen.getByText(/The connection was not created: fix the fields marked below/)).toBeTruthy();
    expect(screen.getByText(/except the credential fields/)).toBeTruthy();
  });

  it("says a create with no answer may or may not exist, and resends the same key", async () => {
    const keys: string[] = [];
    vi.spyOn(api, "POST").mockImplementation(((_path: never, init: { params: { header: Record<string, string> } }) => {
      keys.push(init.params.header["Idempotency-Key"]!);
      return Promise.reject(new TypeError("Failed to fetch"));
    }) as never);
    setup();
    fill("kafka-0.lost-answer.svc:9093");
    submit();
    await screen.findByText("No answer reached this page");
    expect(screen.getByText(/Whether the connection was created is unknown/)).toBeTruthy();
    expect(screen.getByText(/resends the idempotency key this form is holding/)).toBeTruthy();
    type("connection-password", SECRET);
    submit();
    await waitFor(() => expect(keys.length).toBe(2));
    expect(keys[1]).toBe(keys[0]);
  });
});
