// The schedule form's rules and the facts the pages derive, each with the
// input that must make it say no. Fixtures are the API's own documents
// (ui/tests/fixtures/console/), the ones the old console's rows read.
import { describe, expect, it } from "vitest";
import { ApiError } from "../../api/client";
import type { RetentionPolicy, Schedule } from "../../api/schedules";
import type { components } from "../../api/schema";
import type { Backup } from "../../api/types";
import {
  createBody,
  draftValues,
  editBody,
  fieldOf,
  latestPoint,
  movesLocation,
  outcomeOf,
  placeFieldErrors,
  policySchema,
  previewQuery,
  retentionOf,
  runsOf,
  scheduleState,
  valuesOf,
  type PolicyValues,
} from "./model";
import policyFixture from "../../../../ui/tests/fixtures/console/schedule-policy.json";
import presetFixture from "../../../../ui/tests/fixtures/console/schedule-preset.json";
import listFixture from "../../../../ui/tests/fixtures/console/schedules-list.json";
import retentionFixture from "../../../../ui/tests/fixtures/console/retention-policies-list.json";

type S = components["schemas"];
// Cast, not checked: a JSON import widens every enum to string.
const withPolicy = policyFixture as unknown as S["ScheduleResponse"];
const withPreset = presetFixture as unknown as S["ScheduleResponse"];
const list = listFixture as unknown as S["ScheduleList"];
const retention = retentionFixture as unknown as S["RetentionPolicyList"];

const draft = (over: Partial<PolicyValues> = {}): PolicyValues => ({
  ...draftValues([]),
  source: "orders-prod",
  topics: "orders, payments",
  destination: "primary",
  ...over,
});

const issues = (v: PolicyValues) => {
  const r = policySchema.safeParse(v);
  return r.success ? {} : Object.fromEntries(r.error.issues.map((i) => [String(i.path[0]), i.message]));
};

describe("the cadence preview a draft asks for", () => {
  it("sends a preset with its own parameters only, and a custom line as the line", () => {
    expect(previewQuery(draft({ mode: "daily", hour: "2", minute: "30", dayOfWeek: "3", timeZone: "Europe/Berlin" }))).toEqual({
      count: 5,
      preset: "daily",
      hour: 2,
      minute: 30,
      timeZone: "Europe/Berlin",
    });
    expect(previewQuery(draft({ mode: "custom", cron: " 0 3 31 * * " }))).toEqual({ count: 5, schedule: "0 3 31 * *" });
    // Negative control: a blank zone is not sent (absent means UTC), and an incomplete cadence asks nothing.
    expect(previewQuery(draft({ mode: "hourly", minute: "" }))).toBeNull();
    expect(previewQuery(draft({ mode: "custom", cron: "" }))).toBeNull();
    expect(previewQuery(draft({ timeZone: "  " }))).not.toHaveProperty("timeZone");
  });
});

describe("the form's own refusals", () => {
  it("accepts a complete guided draft", () => {
    expect(issues(draft())).toEqual({});
  });

  it("refuses what it can be sure of, by field", () => {
    expect(issues(draft({ source: "" }))).toHaveProperty("source");
    expect(issues(draft({ topics: " , " }))).toHaveProperty("topics");
    expect(issues(draft({ topics: "orders, pay*" })).topics).toMatch(/never patterns: pay\*/);
    expect(issues(draft({ selection: "all", incompleteDiscovery: "" }))).toHaveProperty("incompleteDiscovery");
    expect(issues(draft({ mode: "monthly", dayOfMonth: "31" }))).toHaveProperty("dayOfMonth");
    expect(issues(draft({ mode: "everyNHours", n: "5" })).n).toMatch(/One of 2, 3, 4, 6, 8, 12/);
    expect(issues(draft({ mode: "custom", cron: "every day" }))).toHaveProperty("cron");
    expect(issues(draft({ where: "archive", archiveUrl: "bucket/prefix" }))).toHaveProperty("archiveUrl");
    expect(issues(draft({ destination: "" }))).toHaveProperty("destination");
    expect(issues(draft({ startingDeadlineSeconds: "59" }))).toHaveProperty("startingDeadlineSeconds");
    expect(issues(draft({ maxRetries: "4" }))).toHaveProperty("maxRetries");
  });

  it("leaves to the API what only the API can judge", () => {
    // `61 * * * *` has the shape of a cron line; the API refuses it, in its words.
    expect(issues(draft({ mode: "custom", cron: "61 * * * *" }))).toEqual({});
    expect(issues(draft({ timeZone: "Europe/Atlantis" }))).toEqual({});
  });
});

describe("the bodies the two routes get", () => {
  it("a guided create sends the compiled line, the destination by name, and nothing left blank", () => {
    const body = createBody(draft(), "0 2 * * *");
    expect(body).toEqual({
      schedule: "0 2 * * *",
      sourceRef: { name: "orders-prod" },
      topics: ["orders", "payments"],
      destinationRef: { name: "primary" },
      suspended: false,
    });
    // Negative control: no default is written into the object, no withdrawn retention, no inline archive beside a destination.
    for (const absent of ["timeZone", "archive", "retention", "concurrencyPolicy", "startingDeadlineSeconds", "retry", "activeDeadlineSeconds", "allUserTopics"]) {
      expect(body).not.toHaveProperty(absent);
    }
  });

  it("all user topics send the dynamic block and no named topic; an archive URL replaces the destination", () => {
    const body = createBody(
      draft({ selection: "all", incompleteDiscovery: "Refuse", excludePrefixes: "tmp-, dev-", where: "archive", archiveUrl: "s3://b/p", archiveSecret: "logweir-s3", maxRetries: "2" }),
      "0 2 * * *",
    );
    expect(body.topics).toEqual([]);
    expect(body.allUserTopics).toEqual({ incompleteDiscovery: "Refuse", exclude: { prefixes: ["tmp-", "dev-"] } });
    expect(body.archive).toEqual({ url: "s3://b/p", credentialRef: { name: "logweir-s3" } });
    expect(body).not.toHaveProperty("destinationRef");
    expect(body.retry).toEqual({ maxRetries: 2 });
  });

  it("an edit is the whole policy under the revision it was opened at, and never names the source", () => {
    const s = withPolicy.item;
    const body = editBody(valuesOf(s), s.schedule, s.generation!);
    expect(body).toMatchObject({
      expectedGeneration: 1,
      schedule: "7 9 * * *",
      timeZone: "Asia/Kathmandu",
      topicSelection: { topics: ["t1"] },
      destinationRef: { name: "dest" },
      catchUpPolicy: "Latest",
      startingDeadlineSeconds: 900,
      activeDeadlineSeconds: 600,
      retry: { maxRetries: 2, delaySeconds: 60 },
      suspended: false,
    });
    expect(body).not.toHaveProperty("sourceRef");
    expect(body).not.toHaveProperty("retention");
    // Negative control: a field cleared in the form is absent from the body, which removes it.
    expect(editBody({ ...valuesOf(s), catchUpPolicy: "" }, s.schedule, 1)).not.toHaveProperty("catchUpPolicy");
  });

  it("a saved schedule opens on the server's preset match, never one this page derives", () => {
    expect(valuesOf(withPreset.item)).toMatchObject({ mode: "daily", hour: "2", minute: "0" });
    // Negative control: `7 9 * * *` is daily-shaped, but the server matched nothing, so it is a custom line.
    expect(valuesOf(withPolicy.item)).toMatchObject({ mode: "custom", cron: "7 9 * * *" });
  });

  it("a destination change is a move; the same location is not", () => {
    const s = withPolicy.item;
    expect(movesLocation(s, valuesOf(s))).toBe(false);
    expect(movesLocation(s, { ...valuesOf(s), destination: "other" })).toBe(true);
    expect(movesLocation(s, { ...valuesOf(s), where: "archive", archiveUrl: "s3://x/y" })).toBe(true);
  });
});

describe("where a refusal lands", () => {
  it("maps both routes' field paths to the input, longest match first", () => {
    expect(fieldOf("schedule")).toBe("cron");
    expect(fieldOf("timeZone")).toBe("timeZone");
    expect(fieldOf("topics[2]")).toBe("topics");
    expect(fieldOf("topicSelection")).toBe("topics");
    expect(fieldOf("topicSelection.allUserTopics.incompleteDiscovery")).toBe("incompleteDiscovery");
    expect(fieldOf("topicSelection.allUserTopics.exclude.prefixes")).toBe("excludePrefixes");
    expect(fieldOf("archive.credentialRef.name")).toBe("archiveSecret");
    expect(fieldOf("archive.url")).toBe("archiveUrl");
    expect(fieldOf("retry.delaySeconds")).toBe("retryDelaySeconds");
    expect(fieldOf("retry.maxRetries")).toBe("maxRetries");
    expect(fieldOf("sourceRef")).toBe("source");
  });

  it("places each field error beside its input and lists the rest", () => {
    const refusal = new ApiError(422, {
      code: "validation_failed",
      detail: "One or more fields are invalid.",
      errors: [
        { field: "timeZone", code: "timezone_unknown", message: '"Europe/Atlantis" is not a time zone' },
        { field: "expectedGeneration", code: "required", message: "required" },
      ],
    });
    const placed = placeFieldErrors(refusal);
    expect(placed.fields).toEqual({ timeZone: '"Europe/Atlantis" is not a time zone' });
    // Negative control: a path no input owns is never dropped.
    expect(placed.unmatched).toEqual(["expectedGeneration: required"]);
    expect(placeFieldErrors(new Error("network"))).toEqual({ fields: {}, unmatched: [] });
  });
});

describe("what a failed write means", () => {
  it("no answer, a timeout or a 5xx is unknown; a refusal is a refusal", () => {
    expect(outcomeOf(new TypeError("Failed to fetch"))).toBe("unknown");
    // The live client's no answer (client.call): status 0, never a refusal.
    expect(outcomeOf(new ApiError(0, undefined))).toBe("unknown");
    expect(outcomeOf(new ApiError(503, { code: "kubernetes_unavailable" }))).toBe("unknown");
    expect(outcomeOf(new ApiError(408, {}))).toBe("unknown");
    expect(outcomeOf(new ApiError(412, { code: "precondition_failed" }))).toBe("precondition");
    expect(outcomeOf(new ApiError(409, { code: "idempotency_conflict" }))).toBe("conflict");
    expect(outcomeOf(new ApiError(422, { code: "validation_failed" }))).toBe("invalid");
    // Negative control: a 403 is a definite refusal, never "it may or may not have happened".
    expect(outcomeOf(new ApiError(403, { code: "forbidden" }))).toBe("refused");
  });
});

describe("a schedule's one status", () => {
  const base = withPolicy.item;
  const at = (iso: string) => Date.parse(iso);

  it("is Active while the controller is ready and its next run is ahead", () => {
    expect(scheduleState(base, at("2026-09-18T12:00:00Z")).text).toBe("Active");
    // Negative control: an old evaluatedAt is NOT staleness (nothing is written when nothing changed).
    expect(base.status.policy!.evaluatedAt < "2026-09-18T12:00:00Z").toBe(true);
  });

  it("says staleness only when the first next run is in the past", () => {
    expect(scheduleState(base, at("2026-09-20T00:00:00Z")).text).toBe("Not being evaluated");
  });

  it("puts a suspension first, and the controller's refusal before staleness", () => {
    expect(scheduleState({ ...base, suspended: true }).text).toBe("Suspended");
    const refused: Schedule = { ...base, status: { ...base.status, ready: { type: "Ready", status: "False", reason: "UnknownTimeZone" } } };
    expect(scheduleState(refused, at("2026-09-20T00:00:00Z"))).toMatchObject({ tone: "danger", text: "Not ready" });
    expect(scheduleState({ ...base, status: { ...base.status, ready: undefined } }).text).toBe("Not evaluated yet");
  });
});

describe("a schedule's runs, by identity", () => {
  const run = (name: string, uid: string | undefined, verified: boolean, createdAt: string): Backup =>
    ({
      name,
      createdAt,
      scheduleRef: { name: "nightly", ...(uid ? { uid } : {}) },
      operation: { state: verified ? "succeeded" : "failed", terminal: true, verifiedSuccess: verified, verificationState: verified ? "valid" : "notAttempted" },
    }) as unknown as Backup;
  const runs = [
    run("n-4", "uid-new", false, "2026-10-13T04:00:00Z"),
    run("n-3", "uid-new", true, "2026-10-13T03:00:00Z"),
    run("n-2", "uid-old", true, "2026-10-12T03:00:00Z"),
    run("n-1", undefined, true, "2026-10-11T03:00:00Z"),
  ];

  it("a recreated schedule does not inherit the previous one's runs, and a run with no uid is nobody's", () => {
    const g = runsOf("nightly", "uid-new", runs);
    expect(g.mine.map((b) => b.name)).toEqual(["n-4", "n-3"]);
    expect(g.earlier.map((b) => b.name)).toEqual(["n-2"]);
    expect(g.legacy.map((b) => b.name)).toEqual(["n-1"]);
    // The latest point is the newest VERIFIED run of this identity; the failed newer one is skipped.
    expect(latestPoint(g.mine)?.name).toBe("n-3");
  });

  it("a deleted schedule (no uid) claims no run as its own", () => {
    const g = runsOf("nightly", undefined, runs);
    expect(g.mine).toEqual([]);
    expect(latestPoint(g.mine)).toBeUndefined();
    expect(g.earlier.length + g.legacy.length).toBe(4);
  });
});

describe("retention for a schedule's destination", () => {
  const s = list.items.find((x) => x.name === "everything-nightly")!;
  const policy = (name: string, destination: string, enforcement: string): RetentionPolicy =>
    ({ ...retention.items[0]!, name, destinationRef: { name: destination }, enforcement }) as RetentionPolicy;

  it("never says 'Logweir never deletes' without having read the policies", () => {
    expect(retentionOf(s, undefined).kind).toBe("unread");
    expect(retentionOf(s, []).kind).toBe("none");
  });

  it("finds the one policy naming the destination, and a conflict when two do", () => {
    expect(retentionOf(s, [policy("a", "primary", "RecommendationOnly")])).toMatchObject({ kind: "one", elsewhere: [] });
    expect(retentionOf(s, [policy("a", "primary", "RecommendationOnly"), policy("b", "primary", "LogweirWorker")]).kind).toBe("conflict");
  });

  it("a deleting policy on another destination withholds the no-deletion sentence", () => {
    const c = retentionOf(s, [policy("x", "other", "LogweirWorker")]);
    expect(c).toMatchObject({ kind: "none" });
    expect(c.kind === "none" && c.elsewhere.map((p) => p.name)).toEqual(["x"]);
    // Negative control: one that only reports does not.
    const quiet = retentionOf(s, [policy("x", "other", "RecommendationOnly")]);
    expect(quiet.kind === "none" && quiet.elsewhere).toEqual([]);
  });

  it("an archive URL is named by no policy", () => {
    const inline = list.items.find((x) => x.name === "orders-hourly")!;
    expect(retentionOf(inline, retention.items).kind).toBe("inline");
  });
});
