// The operation stream writes into TanStack Query's cache: each `operation` or `reset`
// replaces the run's operation view, a final one refreshes the lists, `maxDuration`
// reopens, and `settled` or a refused stream hands the run back to the poll.
import { describe, expect, it } from "vitest";
import { QueryClient } from "@tanstack/react-query";
import { followOperation, streamPath } from "./stream";
import { keys } from "./queries";

class FakeSource {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 2;
  static opened: FakeSource[] = [];
  readonly url: string;
  readyState = FakeSource.OPEN;
  private listeners = new Map<string, ((event: MessageEvent<string>) => void)[]>();
  constructor(url: string) {
    this.url = url;
    FakeSource.opened.push(this);
  }
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void) {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }
  emit(type: string, data = "") {
    for (const listener of this.listeners.get(type) ?? []) listener(new MessageEvent(type, { data }));
  }
  close() {
    this.readyState = FakeSource.CLOSED;
  }
}

function follow() {
  FakeSource.opened = [];
  const client = new QueryClient();
  const states: boolean[] = [];
  const invalidated: unknown[] = [];
  const invalidate = client.invalidateQueries.bind(client);
  client.invalidateQueries = (filters) => {
    invalidated.push(filters?.queryKey);
    return invalidate(filters);
  };
  const close = followOperation(client, "team-a", "restore", "orders-drill", (open) => states.push(open), FakeSource as unknown as typeof EventSource);
  return { client, states, invalidated, close };
}

const view = (state: string, terminal: boolean) => JSON.stringify({ kind: "restore", name: "orders-drill", state, terminal });

describe("the operation stream", () => {
  it("opens one same-origin stream with no token in the address", () => {
    follow();
    expect(FakeSource.opened.map((s) => s.url)).toEqual(["/api/v1/workspaces/team-a/operations/restore/orders-drill/events"]);
    expect(streamPath("team a", "backup", "x/y")).toBe("/api/v1/workspaces/team%20a/operations/backup/x%2Fy/events");
  });

  it("puts each operation and reset into the cache, and refreshes the lists when the run is final", () => {
    const { client, invalidated } = follow();
    const source = FakeSource.opened[0]!;
    const key = keys.operation("team-a", "restore", "orders-drill");
    source.emit("operation", view("running", false));
    expect(client.getQueryData<{ state: string }>(key)?.state).toBe("running");
    expect(invalidated).toEqual([]);
    source.emit("heartbeat");
    source.emit("operation", "not json");
    expect(client.getQueryData<{ state: string }>(key)?.state).toBe("running");
    source.emit("reset", view("succeeded", true));
    expect(client.getQueryData<{ state: string }>(key)?.state).toBe("succeeded");
    expect(invalidated).toEqual([keys.list("restores", "team-a"), keys.item("restores", "team-a", "orders-drill")]);
  });

  it("reopens after the server's 300 s bound and stops for good when the run settled", () => {
    const { states } = follow();
    FakeSource.opened[0]!.emit("end", JSON.stringify({ reason: "maxDuration" }));
    expect(FakeSource.opened).toHaveLength(2);
    expect(FakeSource.opened[0]!.readyState).toBe(FakeSource.CLOSED);
    FakeSource.opened[1]!.emit("end", JSON.stringify({ reason: "settled" }));
    expect(FakeSource.opened).toHaveLength(2);
    expect(states).toEqual([true, true, false]);
  });

  it("hands back to the poll when the browser gives up on the stream, not while it reconnects", () => {
    const { states } = follow();
    const source = FakeSource.opened[0]!;
    source.readyState = FakeSource.CONNECTING;
    source.emit("error");
    expect(states).toEqual([true]);
    source.readyState = FakeSource.CLOSED;
    source.emit("error");
    expect(states).toEqual([true, false]);
  });

  it("closes when the page closes it", () => {
    const { states, close } = follow();
    close();
    expect([FakeSource.opened[0]!.readyState, states]).toEqual([FakeSource.CLOSED, [true, false]]);
  });
});
