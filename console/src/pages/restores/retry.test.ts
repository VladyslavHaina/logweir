// DUI-59: "Retry into new topics" starts from the retried run's point, target and rate limit.
import { describe, expect, it } from "vitest";
import { retryPrefill } from "./RestoreWizard";
import type { Offer } from "./WizardSteps";

const POINT = `lwp1-${"a".repeat(32)}`;
const bound = `name: "r"\nsource:\n  backup: "set-1"\n  point:\n    point_id: "${POINT}"\n    receipt_key: "k"\nrestore:\n  rate_limit_bytes_per_sec: 200000\n`;
const offers = [
  { catalog: { name: "pv05-dev-catalog" }, entry: { pointId: `lwp1-${"b".repeat(32)}` } },
  { catalog: { name: "pv05-dev-catalog" }, entry: { pointId: POINT } },
] as unknown as Offer[];
const points = [
  { uid: "uid-other", backupId: "set-0" },
  { uid: "uid-1", backupId: "set-1" },
];
const targets = [{ name: "conn-42n" }, { name: "conn-j3c" }];

describe("DUI-59, DUI-64: what a retry keeps", () => {
  it("the catalog point the plan was bound to when no Backup lists its set, its topics, the target connection and the rate limit", () => {
    expect(retryPrefill({ backupSetRef: "set-9", target: { clusterRef: { name: "conn-j3c" } }, planBytes: bound, oldTopics: ["bulk"] }, points, offers, targets)).toEqual({
      pointKey: `catalog/pv05-dev-catalog/${POINT}`,
      target: "conn-j3c",
      rateLimit: "0.2",
      topics: ["bulk"],
    });
  });
  it("DUI-64: a point a listed Backup produced is that Backup's row, the one the wizard shows", () => {
    expect(retryPrefill({ backupSetRef: "set-1", target: { clusterRef: { name: "conn-j3c" } }, planBytes: bound }, points, offers, targets).pointKey).toBe("backup/uid-1");
  });
  it("a Backup-bound plan keeps its Backup; no limit, no topics and a target no longer listed stay empty (the control)", () => {
    const plain = `name: "r"\nsource:\n  backup: "set-1"\n`;
    expect(retryPrefill({ backupSetRef: "set-1", target: { clusterRef: { name: "gone" } }, planBytes: plain }, points, offers, targets)).toEqual({
      pointKey: "backup/uid-1",
      target: "",
      rateLimit: "",
      topics: [],
    });
  });
});
