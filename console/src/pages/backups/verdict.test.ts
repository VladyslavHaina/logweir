// The backup half of the green-badge rule, ported from the old console's
// ui/tests/list-verdict.spec.js, detail-trust.spec.js, trust-basis-class.spec.js
// and manual-run-queue.spec.js. Fixtures are the API's (ui/tests/fixtures/console/).
// Every row has a negative control: a value that makes the assertion fail.
import { describe, expect, it } from "vitest";
import type { Backup, OperationView } from "../../api/backups";
import {
  CASES,
  GREEN_TRUST_STATES,
  HISTORICAL_SUFFIX,
  basisAllowsGreen,
  checkCommands,
  detailVerdict,
  evidenceRoute,
  isRecoveryPoint,
  listVerdict,
  progressWord,
  restoreOffer,
  triggerWords,
  windowShort,
} from "./verdict";
import backupsList from "../../../../ui/tests/fixtures/console/backups-list.json";
import backupDoc from "../../../../ui/tests/fixtures/console/backup.json";
import operationBackup from "../../../../ui/tests/fixtures/console/operation-backup.json";
import operationPreparing from "../../../../ui/tests/fixtures/console/operation-backup-preparing.json";

const copy = <T>(v: T): T => JSON.parse(JSON.stringify(v)) as T;
const op = (): OperationView => copy(operationBackup.item) as unknown as OperationView;
const backup = (): Backup => copy(backupDoc.item) as unknown as Backup;

/** The API's combined word for a `Valid` verdict on each basis (`trust_state`), and what an older server said. */
const STATE_OF: Record<string, string> = { Current: "verified", Historical: "verifiedHistorical", RecordedBeforeRevocation: "untrusted" };
const PRE_FIX_STATE_OF: Record<string, string> = { ...STATE_OF, RecordedBeforeRevocation: "verified" };

function withBasis(basis: string, states = STATE_OF): OperationView {
  const o = op();
  o.trust = { state: states[basis] as OperationView["trust"]["state"], basis, keyState: basis === "Current" ? "Active" : basis === "Historical" ? "Retired" : "Revoked" };
  o.verifiedSuccess = basis !== "RecordedBeforeRevocation";
  return o;
}

describe("the list rule (a row carries the API's summary only)", () => {
  it("is green exactly when verifiedSuccess and a valid signature say so, and names every other case", () => {
    const template = backupsList.items[0]!;
    const summaries = [
      { verificationState: "valid", verifiedSuccess: true },
      { verificationState: "invalid", verifiedSuccess: false },
      { verificationState: "noEvidence", verifiedSuccess: false },
      { verificationState: "valid", verifiedSuccess: false },
    ];
    const rows = summaries.map((s) => ({ ...copy(template), operation: { ...copy(template.operation), ...s } }) as unknown as Backup);
    const verdicts = rows.map((b) => listVerdict(b.operation));
    expect(verdicts.map((v) => v.green)).toEqual([true, false, false, false]);
    expect(verdicts[1]).toEqual({ green: false, reason: CASES.invalid });
    expect(verdicts[2]).toEqual({ green: false, reason: CASES.noEvidence });
    // NEGATIVE CONTROL: a Valid document on a run that is not verifiedSuccess is NOT green.
    expect(verdicts[3]).toEqual({ green: false, reason: CASES.listValidNotGreen });
    // None of the four is called unrecorded: each has a verdict.
    for (const v of verdicts) if (!v.green) expect(v.reason).not.toBe(CASES.notRecorded);
  });

  it("says unrecorded only when the summary carries no verdict at all", () => {
    expect(listVerdict(undefined)).toEqual({ green: false, reason: CASES.notRecorded });
    // CONTROL: the same rule over a verdict is green.
    expect(listVerdict({ verificationState: "valid", verifiedSuccess: true }).green).toBe(true);
    // A summary that contradicts itself is not green: both halves must say so.
    expect(listVerdict({ verificationState: "notAttempted", verifiedSuccess: true })).toEqual({ green: false, reason: CASES.notAttempted });
  });
});

describe("the run's own rule (the operation view)", () => {
  it("judges a Valid verdict by its trust basis, in front of a current and a pre-fix server", () => {
    for (const states of [STATE_OF, PRE_FIX_STATE_OF]) {
      const current = detailVerdict(withBasis("Current", states));
      expect(current.green).toBe(true);
      expect(current.green && current.label).not.toContain(HISTORICAL_SUFFIX);
      const historical = detailVerdict(withBasis("Historical", states));
      expect(historical.green && historical.label).toContain(HISTORICAL_SUFFIX);
      // RecordedBeforeRevocation is never green, whichever word carried it, and names its case.
      expect(detailVerdict(withBasis("RecordedBeforeRevocation", states))).toEqual({ green: false, reason: CASES.recordedBeforeRevocation });
    }
    // NEGATIVE CONTROL: reading the combined word alone would call the pre-fix answer green; the basis is what vetoes.
    expect(GREEN_TRUST_STATES.includes(PRE_FIX_STATE_OF.RecordedBeforeRevocation!)).toBe(true);
  });

  it("names the key and the instant the controller recorded", () => {
    const v = detailVerdict(withBasis("Current"));
    expect(v.green && v.label).toBe(
      "Verified by the controller at 2026-09-12T08:02:21Z against key 24f4f5a02694bdf2e800a471545287d2c5d4b4b361212bb270ab4a4722921cdd",
    );
    // CONTROL: a Valid with no key or no instant cannot carry that label, and is not green.
    const noKey = withBasis("Current");
    noKey.verification.matchedKeyId = null;
    expect(detailVerdict(noKey)).toEqual({ green: false, reason: CASES.notRecorded });
    const noTime = withBasis("Current");
    noTime.verification.verifiedAt = undefined;
    expect(detailVerdict(noTime).green).toBe(false);
  });

  it("is not green while the combined trust word is not a green one, even over a valid signature", () => {
    const o = withBasis("Current");
    o.trust.state = "pending";
    expect(detailVerdict(o)).toEqual({ green: false, reason: CASES.pending });
    // CONTROL: the same document with the green word is green.
    o.trust.state = "verified";
    expect(detailVerdict(o).green).toBe(true);
  });

  it("keeps a server with no trust block on the pre-existing rule (D3 section 12)", () => {
    const o = op();
    expect((o as { trust?: unknown }).trust).toBeUndefined();
    expect(detailVerdict(o).green).toBe(true);
    // The DTO spells an absent block `None`: green under `verified`, and an `untrusted` answer is the signer's case.
    const none = op();
    none.trust = { state: "verified", basis: "None" };
    expect(detailVerdict(none).green).toBe(true);
    const refused = op();
    refused.trust = { state: "untrusted", basis: "None" };
    refused.verifiedSuccess = false;
    expect(detailVerdict(refused)).toEqual({ green: false, reason: CASES.untrusted });
  });

  it("reads every basis shape the way the controller's ValidBasis does", () => {
    const shapes: [string | undefined, boolean][] = [
      ["Current", true],
      ["Historical", true],
      ["None", true],
      [undefined, true],
      ["Unverified", false],
      ["RecordedBeforeRevocation", false],
      ["SomeFutureBasis", false],
    ];
    for (const [basis, green] of shapes) {
      expect(basisAllowsGreen(basis), String(basis)).toBe(green);
      const o = op();
      o.trust = { state: "verified", basis: basis as string };
      expect(detailVerdict(o).green, String(basis)).toBe(green);
    }
  });

  it("keeps the exit code authoritative: a Valid receipt over a run that did not exit 0 is not green", () => {
    const at = (exitCode: number | undefined, verifiedSuccess = true) => {
      const o = withBasis("Current");
      o.result.exitCode = exitCode;
      o.verifiedSuccess = verifiedSuccess;
      return detailVerdict(o);
    };
    expect(at(0).green).toBe(true); // the control
    expect(at(2)).toEqual({ green: false, reason: CASES.runNotSucceeded });
    expect(at(undefined)).toEqual({ green: false, reason: CASES.runNotSucceeded });
    expect(at(0, false)).toEqual({ green: false, reason: CASES.runNotSucceeded });
  });

  it("names the document, the controller and the signer apart when there is no Valid result", () => {
    const at = (state: string, trustState: string) => {
      const o = op();
      o.verification = { state: state as OperationView["verification"]["state"] };
      o.trust = { state: trustState as OperationView["trust"]["state"], basis: "None" };
      o.verifiedSuccess = false;
      return detailVerdict(o);
    };
    expect(at("invalid", "invalid")).toEqual({ green: false, reason: CASES.invalid });
    expect(at("notAttempted", "notAttempted")).toEqual({ green: false, reason: CASES.notAttempted });
    expect(at("unknown", "untrusted")).toEqual({ green: false, reason: CASES.untrusted });
    // A run that has not been judged reads its verification state, not "unrecorded".
    expect(detailVerdict(copy(operationPreparing.item) as unknown as OperationView)).toEqual({ green: false, reason: CASES.pending });
  });
});

describe("run facts", () => {
  it("offers Restore this point only for a verified recovery point and a role that may restore", () => {
    const b = backup();
    expect(isRecoveryPoint(b)).toBe(true);
    expect(restoreOffer(b, true)).toEqual({ offer: true });
    expect(restoreOffer(b, false).offer).toBe(false);
    const unverified = backup();
    unverified.operation = { ...unverified.operation, verificationState: "notAttempted", verifiedSuccess: false };
    expect(restoreOffer(unverified, true).offer).toBe(false);
    const noWindow = backup();
    noWindow.windowCovered = undefined;
    expect(restoreOffer(noWindow, true)).toEqual({ offer: false, reason: "This run made no recovery point." });
  });

  it("reads the trigger from trigger alone and never from triggeredBy", () => {
    expect(triggerWords({ kind: "Retry", attempt: 2, retryOf: { name: "x" } })).toBe("Retry, attempt 2");
    expect(triggerWords({ kind: "CatchUp", attempt: 0 })).toBe("Catch-up");
    // CONTROL: a run older than triggers says nothing was recorded, even though triggeredBy is set.
    const b = backup();
    expect(b.triggeredBy).toBe("schedule");
    expect(triggerWords(b.trigger)).toBeNull();
  });

  it("names a queued manual run's ceiling, copied, and leaves every other state alone", () => {
    const at = (state: string, queue?: unknown) =>
      progressWord({ operation: { state, terminal: false, verificationState: "pending", verifiedSuccess: false }, queue } as unknown as Backup);
    expect(at("queued", { limit: 4 })).toBe("Queued (limit 4 active)");
    expect(at("queued")).toBe("Queued");
    expect(at("queued", { limit: 0 })).toBe("Queued");
    expect(at("running", { limit: 4 })).toBe("Running");
  });

  it("names each day of a window that is not all today", () => {
    const b = backup();
    expect(windowShort(b, b.windowCovered!.toMs)).toBe("07:00 to 08:00 UTC");
    expect(windowShort(b, b.windowCovered!.toMs + 86_400_000)).toBe("Sep 12, 07:00 to 08:00 UTC");
    b.windowCovered = { fromMs: b.windowCovered!.fromMs - 86_400_000, toMs: b.windowCovered!.toMs };
    expect(windowShort(b, b.windowCovered.toMs)).toBe("Sep 11, 07:00 to Sep 12, 08:00 UTC");
  });
});

describe("check it yourself", () => {
  it("fetches from the destination's bucket, and says why when the run froze another location", () => {
    const b = backup();
    const destination = { storage: { bucket: "kafka-backups", endpoint: "https://minio.local:9000", region: "us-east-1", addressing: "pathStyle" }, locationDigest: b.locationDigest };
    const route = evidenceRoute(b, destination, null);
    expect(route).toEqual({ bucket: "kafka-backups", endpoint: "https://minio.local:9000", region: "us-east-1", pathStyle: true });
    const lines = checkCommands(route, "logweir/backups/x/r1.receipt.json", "logweir/backups/x/r1.receipt.sig");
    expect(lines[0]).toBe("aws s3 cp s3://kafka-backups/logweir/backups/x/r1.receipt.json ./receipt.json --endpoint-url https://minio.local:9000 --region us-east-1");
    expect(lines).toHaveLength(4);
    // NEGATIVE CONTROL: a destination that moved since the run froze is not where it wrote.
    const moved = evidenceRoute(b, { ...destination, locationDigest: "sha256:" + "b".repeat(64) }, null);
    expect(moved.bucket).toBe("<bucket of destination primary>");
    expect(moved.missing).toMatch(/is not where this run wrote/);
    // A legacy inline run reads its bucket off its own URL.
    expect(evidenceRoute({ archive: { url: "s3://kafka-backups/drill-demo" } }, undefined, null).bucket).toBe("kafka-backups");
  });
});
