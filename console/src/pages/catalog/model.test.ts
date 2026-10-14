// The recovery catalog's rules, ported from ui/tests/restore-catalog.spec.js and
// d3.spec.js: which rows are offered for a restore (and why not), the redacted
// binding, the set ids the redactor keeps (the fixture the Rust side reads
// too), consumer positions in words, and a view judged by the controller's
// Ready condition, never the browser's clock. Each row has its negative control.
import { describe, expect, it } from "vitest";
import type { Catalog, PointView } from "../../api/backups";
import {
  catalogPointOffer,
  connectFieldOf,
  consumerPositionsWords,
  isMintedSetId,
  redactedBindingReason,
  schemaDependentTopics,
  truncatedWords,
  viewIsUsable,
  viewIsWindow,
} from "./model";
import { catalogPointOffer as wizardPointOffer } from "../restores/points";
import setIds from "../../../fixtures/set-ids.json";
import states from "../../../fixtures/console/catalog-points-states.json";
import catalogDoc from "../../../fixtures/console/catalog.json";
import schemaPoints from "../../../fixtures/console/catalog-point-schema-dependency.json";

const SET = "3f1c9d2e-8a7b-4c6d-9e0f-1a2b3c4d5e6f-20260922-140000";

function row(over: Partial<PointView> = {}): PointView {
  return {
    pointId: "lwp1-0123456789abcdef0123456789abcdef",
    backupId: SET,
    runId: "01JB7Z00000000000000000000",
    recoveryPointAt: "2026-09-22T14:00:00Z",
    coveredFrom: "2026-09-22T13:00:00Z",
    coveredTo: "2026-09-22T14:00:00Z",
    availability: "Available",
    verification: "Verified",
    selectable: true,
    signerKeyId: "c".repeat(64),
    receiptKey: `logweir/backups/${SET}/01JB7Z00000000000000000000.receipt.json`,
    receiptSha256: "sha256:" + "a1".repeat(32),
    manifestKey: `${SET}/manifest.json`,
    manifestSha256: "sha256:" + "b2".repeat(32),
    locations: [{ locationId: "s3://kafka-backups/team-a/prod", availability: "Available" }],
    ...over,
  };
}

describe("the restore offer", () => {
  it("offers a selectable, complete, unredacted row and refuses each missing fact by name", () => {
    expect(catalogPointOffer(row(), {})).toEqual({ offer: true });
    const refusals: [string, PointView, Parameters<typeof catalogPointOffer>[1], RegExp][] = [
      ["a controller refusal of its Backup", row({ backupVerdict: "Invalid" }), {}, /refused this point's own Backup evidence \(Invalid\)/],
      ["an incomplete verdict join", row(), { backupVerdictsIncomplete: "Unavailable" }, /could not read every Backup verdict/],
      ["a truncated verdict join", row(), { backupVerdictsIncomplete: "Truncated" }, /Truncated/],
      ["an expired view", row(), { viewExpired: true }, /aged out/],
      ["a row the catalog does not mark selectable", row({ selectable: false, verification: "UntrustedSigner" }), {}, /UntrustedSigner/],
      ["a redacted receipt key", row({ receiptKey: "[redacted].receipt.json" }), {}, /redactor/],
      ["no manifest digest", row({ manifestSha256: undefined }), {}, /manifest digest/],
      ["a malformed point id", row({ pointId: "p1" }), {}, /point id/],
      ["no covered window", row({ coveredTo: undefined }), {}, /covered window/],
      ["an empty window", row({ coveredTo: "2026-09-22T13:00:00Z" }), {}, /covered window/],
    ];
    for (const [why, entry, page, reason] of refusals) {
      const verdict = catalogPointOffer(entry, page);
      expect(verdict.offer, why).toBe(false);
      expect(!verdict.offer && verdict.reason, why).toMatch(reason);
    }
  });

  it("gives a redacted binding one reason naming the field and its cause", () => {
    const POC_SET = "89b585c5-5498-48dc-ae32-090809457ec8-20261008-020000";
    const poc = (redacted: boolean) =>
      row({
        pointId: "lwp1-324e405be6e21da94338e90966d61884",
        backupId: redacted ? "[redacted]" : POC_SET,
        receiptKey: redacted ? "[redacted].receipt.json" : `logweir/backups/${POC_SET}/01M4CKTADX268PREVHJAAYXEMZ.receipt.json`,
      });
    const refused = catalogPointOffer(poc(true), {});
    expect(refused.offer).toBe(false);
    const reason = !refused.offer ? refused.reason : "";
    expect(reason).toMatch(/backup set id as `\[redacted\]` and its receipt key as `\[redacted\]\.receipt\.json`/);
    expect(reason).toMatch(/Runners up to v0\.2\.0-rc\.1 withhold every scheduled run's set id/);
    expect(reason).not.toMatch(/no usable backup set id/);
    expect(redactedBindingReason(poc(true))).toBe(reason);
    // CONTROL: the same point as a current runner publishes it is offered.
    expect(catalogPointOffer(poc(false), {})).toEqual({ offer: true });
    expect(redactedBindingReason(poc(false))).toBeNull();
    // Only the key, beside a set id this product mints (upper case included): the older runner.
    for (const id of ["3f1c9d2e-8a7b-4c6d-9e0f-1a2b3c4d5e6f", "3F1C9D2E-8A7B-4C6D-9E0F-1A2B3C4D5E6F"]) {
      expect(redactedBindingReason(row({ backupId: id, receiptKey: "[redacted].receipt.json" }))).toMatch(/withheld the run id in the key/);
    }
    // Beside a set id someone chose: that id is named, with the rule.
    expect(redactedBindingReason(row({ backupId: "MyBackupSet01", receiptKey: "[redacted].receipt.json" }))).toMatch(
      /its set id `MyBackupSet01` and the run id are two chosen components/,
    );
    // The order is the order of repair: a real refusal outranks the redaction.
    const untrusted = catalogPointOffer({ ...poc(true), selectable: false, verification: "UntrustedSigner" }, {});
    expect(!untrusted.offer && untrusted.reason).toMatch(/does not list this point as restorable: availability Available, verification UntrustedSigner/);
  });

  it("refuses every row of the API's mixed-state page, each for its own reason", () => {
    const page = states as unknown as { items: PointView[]; viewExpired: boolean };
    const reasons = page.items.map((p) => catalogPointOffer(p, page));
    expect(reasons.every((r) => !r.offer)).toBe(true);
    // The two selectable rows are refused for their redacted receipt keys, and only for that.
    const selectable = page.items.filter((p) => p.selectable);
    expect(selectable).toHaveLength(2);
    for (const p of selectable) expect(redactedBindingReason(p)).not.toBeNull();
    // CONTROL: with the keys whole, the same rows are offered.
    for (const p of selectable) {
      expect(catalogPointOffer({ ...p, receiptKey: `logweir/backups/${p.backupId}/r1.receipt.json`, manifestSha256: p.manifestSha256 ?? "sha256:" + "b2".repeat(32) }, page).offer).toBe(true);
    }
  });

  it("agrees with the Rust side about which set ids the controller mints (shared fixture)", () => {
    expect(setIds.minted.length).toBeGreaterThanOrEqual(4);
    expect(setIds.notMinted.length).toBeGreaterThanOrEqual(9);
    for (const id of setIds.minted) expect(isMintedSetId(id), id).toBe(true);
    for (const id of setIds.notMinted) expect(isMintedSetId(id), id).toBe(false);
    for (const id of setIds.identityEdges) expect(isMintedSetId(id), id).toBe(true);
  });
});

describe("what a point says", () => {
  it("says how fresh its consumer positions are and every count beside the related ones", () => {
    const words = consumerPositionsWords(
      row({
        consumerPositions: {
          observedBeforeRecoveryPointMs: 1500,
          listing: "complete",
          groups: [
            { groupId: "billing", outcome: "captured", groupType: "consumer", active: true, positions: { related: 5, notRelated: 1, neverCommitted: 2, beyondEnd: 0, failed: 0, notObserved: 0 } },
            { groupId: "word-count", outcome: "excluded", reason: "GroupTypeNotCaptured", groupType: "other" },
          ],
        },
      }),
    );
    expect(words).toMatch(/read 1\.5 s before this point/);
    expect(words).toMatch(/billing: captured \(active\), 5 of 8 positions relate to archived data, 2 never committed/);
    expect(words).not.toMatch(/beyond the end/); // a zero count is not said
    expect(words).toMatch(/word-count: excluded \(GroupTypeNotCaptured\)/);
    expect(words).toMatch(/not atomic with the records/);
    expect(consumerPositionsWords(row({ consumerPositions: { listing: "complete", groupsOmitted: 40 } }))).toMatch(/40 groups not listed here/);
    // CONTROL: a point that publishes none says nothing, never "no positions".
    expect(consumerPositionsWords(row())).toBeNull();
  });

  it("names schema-dependent topics, and says nothing for a point whose topics are not listed", () => {
    // The chain fixture's API part, read here and by crates/logweir-api/tests/d3_reads.rs.
    const topics = schemaPoints.pointTopics as unknown as PointView["topics"];
    expect(schemaDependentTopics(row({ topics }))).toEqual([
      "orders (key and value: schema ids 2, 3, 4; sampled)",
      `payments (value: schema ids ${Array.from({ length: 16 }, (_, i) => 100001 + i).join(", ")} and more; complete)`,
    ]);
    // CONTROL: notDetected and notAssessed topics are not called dependent, and an unlisted point says nothing.
    expect(schemaDependentTopics(row({ topics: topics!.slice(0, 2) }))).toEqual([]);
    expect(schemaDependentTopics(row())).toEqual([]);
  });
});

describe("the view", () => {
  it("is judged by the controller's Ready condition and never by the browser's clock", () => {
    const c = JSON.parse(JSON.stringify(catalogDoc.item)) as Catalog;
    expect(viewIsUsable(c)).toBe(true);
    c.viewExpiresAt = "2000-01-01T00:00:00Z";
    expect(viewIsUsable(c)).toBe(true);
    // NEGATIVE CONTROL: Ready=False is not usable, whatever the instant says.
    const expired = JSON.parse(JSON.stringify(catalogDoc.item)) as Catalog;
    expired.viewExpiresAt = "2999-01-01T00:00:00Z";
    for (const x of expired.conditions ?? []) if (x.type === "Ready") Object.assign(x, { status: "False", reason: "ViewExpired" });
    expect(viewIsUsable(expired)).toBe(false);
    expect(viewIsUsable({ conditions: [] })).toBe(false);
  });

  it("places the API's field paths beside the connect form's inputs", () => {
    expect(connectFieldOf("name")).toBe("name");
    expect(connectFieldOf("destinationRef.name")).toBe("destination");
    expect(connectFieldOf("legacyArchive.url")).toBe("destination");
    expect(connectFieldOf("syncMode")).toBe("syncMode");
    expect(connectFieldOf("viewLimit")).toBeUndefined();
  });
});

describe("FX-36, FX-38: what a catalog's view does not prove", () => {
  const MALFORMED =
    "The object at this recovery point's receipt key is not a receipt: it is not JSON, or it is another document. This is the object's content; no permission or network change lists the point.";

  it("never offers an Unreadable or Conflict point, even one an older API marks selectable", () => {
    for (const availability of ["Unreadable", "Conflict"]) {
      const e = row({ availability, verification: "Unverified", remedy: MALFORMED });
      for (const offer of [catalogPointOffer, wizardPointOffer]) {
        const verdict = offer(e, {});
        expect(verdict.offer, availability).toBe(false);
        expect(verdict.offer ? "" : verdict.reason).toContain(MALFORMED);
      }
    }
    // CONTROL: the same row Available is offered by both.
    expect(catalogPointOffer(row(), {})).toEqual({ offer: true });
    expect(wizardPointOffer(row(), {})).toEqual({ offer: true });
  });

  it("reads truncated as not known to be the whole archive, and calls the view a window only when Synced says so", () => {
    const synced = (message: string) => ({ truncated: true, conditions: [{ type: "Synced", status: "True", reason: "Synced", message }] });
    const window = synced("the walk completed; the newest 500 of 900 points are materialised across 3 page(s). The view is a WINDOW: points beyond it are counted and histogrammed here and listed by `logweir catalog list` against the archive");
    expect(viewIsWindow(window)).toBe(true);
    expect(truncatedWords(window)).toBe("Not known to be the whole archive; the Synced condition says why");
    const space = synced("the walk completed; the newest 400 of 400 points are materialised across 3 page(s). 4 entr(y/ies) did not fit the view's 3 page(s) and are not listed");
    expect(viewIsWindow(space)).toBe(false);
    expect(truncatedWords({ truncated: true, conditions: [] })).toBe("Not known to be the whole archive; the catalog has not said why (no Synced message)");
    expect(truncatedWords({ truncated: true, conditions: [] })).not.toMatch(/holds more/);
    // CONTROL: a whole view is said whole.
    expect(truncatedWords({ truncated: false, conditions: [] })).toBe("Not truncated");
  });
});
