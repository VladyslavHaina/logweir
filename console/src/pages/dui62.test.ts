// DUI-62 (deep UI pass v05, part 2): the slips that have a cheap row.
import { describe, expect, it } from "vitest";
import { scopeText } from "./connections/parts";
import { catalogPoint } from "./restores/points";
import { timelineOf } from "./restores/RestoreDetail";

describe("DUI-62.2: a connection check's scope", () => {
  it("names the principal it exercised as the connection's, not as an object", () => {
    expect(scopeText({ kind: "KafkaCluster", name: "User:ANONYMOUS" } as never)).toBe("the connection, as User:ANONYMOUS");
    // Control: an object scope keeps kind/name.
    expect(scopeText({ kind: "BackupDestination", name: "primary" } as never)).toBe("BackupDestination/primary");
  });
});

describe("DUI-62.6: one topic picker", () => {
  const catalog = { name: "c", destinationRef: { name: "d" } } as never;
  const entry = (over: Record<string, unknown>) =>
    ({ pointId: "lwp1-x", backupId: "set", coveredFrom: "2026-10-16T00:00:00Z", coveredTo: "2026-10-16T01:00:00Z", recoveryPointAt: "2026-10-16T01:00:00Z", ...over }) as never;
  it("a catalog point whose layout is whole is chosen from like a Backup's topics", () => {
    expect(catalogPoint(catalog, entry({ topics: [{ name: "orders" }, { name: "payments" }] }), null).topics).toEqual(["orders", "payments"]);
  });
  it("control: a layout the view cut short asks for the names as text", () => {
    expect(catalogPoint(catalog, entry({ topics: [{ name: "orders" }], topicsOmitted: 3 }), null).topics).toBeNull();
  });
});

describe("DUI-62.8: a finished run's timeline", () => {
  it("is newest first and leaves out the waiting state that ended", () => {
    const conditions = [
      { type: "RunnerReady", status: "False", reason: "WaitingForPod", lastTransitionTime: "2026-10-16T02:00:00Z" },
      { type: "Admitted", status: "True", lastTransitionTime: "2026-10-16T01:59:00Z" },
      { type: "Refused", status: "True", lastTransitionTime: "2026-10-16T02:01:00Z" },
    ];
    expect(timelineOf({ terminal: true, conditions }).map((c) => c.type)).toEqual(["Refused", "Admitted"]);
    // Control: a live run still shows that it waits.
    expect(timelineOf({ terminal: false, conditions }).map((c) => c.type)).toEqual(["Refused", "RunnerReady", "Admitted"]);
  });
});
