// @vitest-environment node
// The wizard's rules, and the claim R5 rests on: for the same inputs the new wizard
// emits the old wizard's plan bytes. The differential below drives ui/pages/
// restore-wizard.js's own initialState and setters (the functions the old page runs on
// every edit) beside planFields, over one recovery point in product-API shape and its
// custom-resource projection, and compares the rendered bytes.
import { describe, expect, it } from "vitest";
// @ts-expect-error ui/ is plain JavaScript; the differential reads it untyped.
import * as old from "../../../../ui/pages/restore-wizard.js";
// @ts-expect-error ui/ is plain JavaScript.
import { renderPlanBytes as oldRender } from "../../../../ui/plan.js";
import twoPerson from "../../../../ui/tests/fixtures/console/approval-policy-two-person.json";
import readyFixture from "../../../../ui/tests/fixtures/console/preflight-ready.json";
import notReadyFixture from "../../../../ui/tests/fixtures/console/preflight-not-ready.json";
import staleFixture from "../../../../ui/tests/fixtures/console/preflight-stale.json";
import pendingFixture from "../../../../ui/tests/fixtures/console/preflight-pending.json";
import catalogPoints from "../../../../ui/tests/fixtures/console/catalog-points.json";
import setIds from "../../../../ui/tests/fixtures/set-ids.json";
import { renderPlanBytes, preparePlanDocument } from "./plan";
import { backupPoint, catalogPointOffer, frozenDestinationProblem, type Backup, type Destination, type PointView, type RecoveryPoint } from "./points";
import {
  BROKERS_NOT_READ,
  approvalRoute,
  defaultPrefixFor,
  endInstant,
  freshTargetPrefix,
  mappingProblems,
  planFields,
  pointInTimeProblem,
  policyRefusal,
  readinessRefusal,
  readyButForDraftApproval,
  replicationDefault,
  replicationProblem,
  restoreBody,
  restoreIdempotencyKey,
  targetBrokerFact,
  typedConfirmationRequired,
  typedTopicsProblem,
  validate,
  type ApprovalPolicy,
  type BrokerFact,
  type Connection,
  type Preflight,
  type WizardContext,
  type WizardValues,
} from "./model";

// ------------------------------------------------------------------ fixtures

const DEST_UID = "5f0c2a1e-7b3d-4c9a-8e21-6d4b9f0a1c37";
const DIGEST = `sha256:${"ab".repeat(32)}`;
const destination = {
  name: "primary",
  namespace: "team-a",
  uid: DEST_UID,
  locationDigest: DIGEST,
  storage: { provider: "s3", bucket: "kafka-backups", prefix: "team-a/prod", region: "eu-west-1", endpoint: "https://minio.storage.svc:9000", addressing: "pathStyle" },
  transport: { security: "tls" },
} as unknown as Destination;
const vault = { ...destination, name: "evidence-vault", uid: "a81d7e44-02c9-4b6f-9d3a-1f5e8c7b2a90", storage: { provider: "s3", bucket: "logweir-evidence", prefix: "", region: "eu-west-1", addressing: "virtualHosted" } } as unknown as Destination;

const backup = {
  name: "orders-hourly-20261013-120000",
  namespace: "team-a",
  uid: "11111111-2222-4333-8444-555555555555",
  createdAt: "2026-10-13T12:00:00Z",
  backupId: "89b585c5-5498-48dc-ae32-090809457ec8-20261013-120000",
  topics: ["orders", "orders-dlq", "payments"],
  sourceRef: { name: "orders-prod" },
  archive: { url: "logweir-destination://primary" },
  destinationRef: { name: "primary", uid: DEST_UID },
  locationDigest: DIGEST,
  windowCovered: { fromMs: Date.parse("2026-10-13T11:00:00Z"), toMs: Date.parse("2026-10-13T12:04:00.500Z") },
  operation: { state: "succeeded", terminal: true, verificationState: "valid", verifiedSuccess: true },
  triggeredBy: "schedule",
  deadlineSeconds: 1800,
} as unknown as Backup & { backupId: string; windowCovered: { fromMs: number; toMs: number } };

const legacyBackup = {
  ...backup,
  name: "orders-legacy-20260901",
  uid: "99999999-2222-4333-8444-555555555555",
  archive: { url: "s3://legacy-bucket/team-a/old", credentialRef: { name: "legacy-s3" } },
  destinationRef: undefined,
  locationDigest: undefined,
} as unknown as typeof backup;

const target = {
  name: "orders-dr",
  namespace: "team-a",
  uid: "77777777-2222-4333-8444-555555555555",
  role: "target",
  bootstrapServers: ["kafka-0.orders-dr.svc:9093", "kafka-1.orders-dr.svc:9093"],
  auth: { mode: "scramSha512", tls: true, username: "logweir-restorer", credentialRef: { name: "dr-scram" } },
  reachability: { state: "reachable" },
} as unknown as Connection;
const plainTarget = { ...target, name: "scratch", uid: "88888888-2222-4333-8444-555555555555", auth: { mode: "plaintext", tls: false }, bootstrapServers: ["localhost:9092"] } as unknown as Connection;

const row: PointView = {
  ...(catalogPoints.items[0] as unknown as PointView),
  backupId: backup.backupId,
  receiptKey: "logweir/receipts/a.receipt.json",
  coveredFrom: "2026-10-13T11:00:00Z",
  coveredTo: "2026-10-13T12:04:00.500Z",
  topics: [
    { name: "orders", partitions: 3, replicationFactor: 3, applyRoute: "adminApi", configCoverage: "captured" },
    { name: "orders-dlq", partitions: 1, replicationFactor: 2, applyRoute: "adminApi", configCoverage: "captured" },
    { name: "payments", partitions: 6, replicationFactor: 3, applyRoute: "adminApi", configCoverage: "captured" },
  ],
};

/** The custom-resource projection ui/client.js gives the old page. */
const oldBackup = (b: Backup) => ({
  metadata: { name: b.name, uid: b.uid, creationTimestamp: b.createdAt },
  spec: {
    sourceRef: b.sourceRef,
    topics: b.topics.slice(),
    archive: b.archive.credentialRef ? { url: b.archive.url, secretRef: { name: b.archive.credentialRef.name } } : { url: b.archive.url },
    ...(b.destinationRef ? { destinationRef: { ...b.destinationRef } } : {}),
  },
  status: { phase: "Succeeded", backupId: b.backupId, windowCovered: { ...b.windowCovered! }, ...(b.locationDigest ? { locationDigest: b.locationDigest } : {}) },
});
const oldCluster = (c: Connection) => ({
  metadata: { name: c.name, uid: c.uid },
  spec: { role: c.role, bootstrapServers: c.bootstrapServers.slice(), auth: { mode: c.auth.mode, tls: c.auth.tls, ...(c.auth.username ? { username: c.auth.username } : {}) } },
  status: {},
});

const BLANK: WizardValues = {
  pointKey: "",
  topics: ["orders", "orders-dlq", "payments"],
  topicsText: "",
  timeMode: "end",
  pointInTime: "",
  timeBasis: false,
  coverage: "sampled",
  completeMaxRecords: "",
  target: "orders-dr",
  naming: "prefix",
  prefix: "",
  noOwner: false,
  scratch: false,
  cloneTtl: null,
  topicConfiguration: { transition: null, noOwners: false },
  replication: "",
  evidenceDestination: "primary",
  archiveSecret: "",
  storeEndpoint: "",
  storeRegion: "",
  storePathStyle: false,
  storeAllowHttp: false,
  evidenceBucket: "",
  ticket: "",
  typedNames: "",
};

const fresh3: BrokerFact = { count: 3, fresh: true, discovery: "td-1", observedAt: "2026-10-13T11:30:00Z", why: "" };

function ctxFor(point: RecoveryPoint, over: Partial<WizardContext> = {}): WizardContext {
  return { point, retryOf: "", destination: point.destinationName ? destination : null, evidence: point.destinationName ? destination : null, connection: target, brokers: fresh3, ...over };
}

// ------------------------------------------------------- the differential

interface Scenario {
  name: string;
  backup?: typeof backup;
  bound?: boolean;
  connection?: Connection;
  retryOf?: string;
  evidence?: Destination;
  brokers?: BrokerFact;
  values: Partial<WizardValues>;
  /** The edits the old page applies for the same inputs, in its refresh order. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any -- the old wizard's untyped state (ui/pages/restore-wizard.js)
  edits: (state: Record<string, any>) => void;
}

const SCENARIOS: Scenario[] = [
  { name: "the defaults, sampled", values: {}, edits: () => {} },
  { name: "bound to its catalog row", bound: true, values: {}, edits: () => {} },
  {
    name: "complete coverage with a bound",
    values: { coverage: "complete", completeMaxRecords: "5000" },
    edits: (s) => old.setCoverage(s, true, "5000"),
  },
  {
    name: "a subset ticked out of order",
    values: { topics: ["payments", "orders"] },
    edits: (s) => {
      s.fields.topics = ["payments", "orders"];
      s.fields.topics = old.selectedTopics(s);
    },
  },
  { name: "a typed prefix", values: { prefix: "drill-x-" }, edits: (s) => old.setTopicPrefix(s, "drill-x-") },
  {
    name: "an earlier point in time (the prefix keeps the point's default)",
    values: { timeMode: "earlier", pointInTime: "2026-10-13T11:30:00Z" },
    edits: (s) => {
      s.fields.pointInTime = "2026-10-13T11:30:00Z";
    },
  },
  {
    name: "an earlier point with the prefix emptied (the default follows the point)",
    values: { timeMode: "earlier", pointInTime: "2026-10-13T11:30:00Z", prefix: "" },
    edits: (s) => {
      s.fields.pointInTime = "2026-10-13T11:30:00Z";
      old.setTopicPrefix(s, old.defaultPrefixFor(s)); // the emptied field falls back to the default
    },
  },
  { name: "producer time", values: { timeBasis: true }, edits: (s) => (s.fields.timeBasis = "producerTime") },
  { name: "scratch mode", values: { scratch: true }, edits: (s) => (s.fields.target.mode = "scratch") },
  {
    name: "original names with the owner statement",
    bound: true,
    values: { naming: "original", noOwner: true, coverage: "sampled" },
    edits: (s) => {
      old.setOriginalName(s, true, true);
      old.requireCompleteCoverage(s);
    },
  },
  { name: "a plaintext target", connection: plainTarget, values: { target: "scratch" }, edits: (s) => old.selectTarget(s, plainTarget.uid, plainTarget.name) },
  { name: "a typed replication factor", values: { replication: "2" }, edits: (s) => old.setReplicationFactor(s, "2") },
  { name: "brokers below the source's factor", bound: true, brokers: { ...fresh3, count: 2 }, values: {}, edits: () => {} },
  { name: "no broker count", brokers: BROKERS_NOT_READ, values: {}, edits: () => {} },
  {
    name: "a fresh-target retry",
    retryOf: "orders-restore-x7c2m",
    values: {},
    edits: (s) => old.setTopicPrefix(s, old.freshTargetPrefix(s.fields.pointInTime, "orders-restore-x7c2m")),
  },
  {
    name: "another evidence destination",
    evidence: vault,
    values: { evidenceDestination: "evidence-vault" },
    edits: (s) => {
      s.evidenceDestinations = [vault];
      old.selectEvidenceDestination(s, vault.uid, vault.name, vault);
    },
  },
  {
    name: "an inline (legacy) archive",
    backup: legacyBackup,
    values: { archiveSecret: "legacy-s3", storeEndpoint: "https://s3.legacy:9000", storeRegion: "us-east-1", storePathStyle: true, evidenceBucket: "legacy-bucket", evidenceDestination: "" },
    edits: (s) => {
      Object.assign(s.fields.source, { endpoint: "https://s3.legacy:9000", region: "us-east-1", pathStyle: true, allowHttp: false });
      old.syncEvidenceStore(s);
    },
  },
];

function oldPlan(sc: Scenario): string {
  const b = sc.backup ?? backup;
  const conn = sc.connection ?? target;
  const choice = sc.bound
    ? { point: old.catalogRecoveryPoint({ metadata: { name: "primary" }, spec: { destinationRef: { name: "primary" } } }, row, destination, oldBackup(b)) }
    : undefined;
  const state = old.initialState(
    "team-a",
    { items: [oldCluster(target), oldCluster(plainTarget)].filter((c) => c.metadata.uid === conn.uid || c.metadata.uid === target.uid) },
    { items: [oldBackup(b)] },
    { uid: b.uid, backup: b.name, ...(sc.retryOf ? { retryOf: sc.retryOf } : {}) },
    b.destinationRef ? destination : null,
    b.destinationRef ? [destination] : [],
    choice,
  );
  if (sc.bound) state.sourceFacts = old.sourceFactsOfEntry(row, "primary");
  const brokers = sc.brokers ?? fresh3;
  state.targetBrokers = brokers.count === null ? old.BROKERS_NOT_READ : { ...old.BROKERS_NOT_READ, count: brokers.count, fresh: brokers.fresh, uid: target.uid, name: target.name, discovery: brokers.discovery };
  sc.edits(state);
  old.syncReplicationDefault(state);
  return oldRender(state.fields);
}

function newPlan(sc: Scenario): string {
  const b = sc.backup ?? backup;
  const point = backupPoint(b, sc.bound ? { name: "primary", entry: row } : null);
  const ctx = ctxFor(point, {
    retryOf: sc.retryOf ?? "",
    connection: sc.connection ?? target,
    brokers: sc.brokers ?? fresh3,
    ...(sc.evidence ? { evidence: sc.evidence } : {}),
  });
  const end = endInstant(point);
  const v: WizardValues = { ...BLANK, pointInTime: end, prefix: defaultPrefixFor(end, ctx.retryOf), ...sc.values };
  if ("prefix" in sc.values && sc.values.prefix === "") v.prefix = "";
  return renderPlanBytes(planFields(ctx, v));
}

describe("the new wizard emits the old wizard's plan for the same inputs", () => {
  it.each(SCENARIOS)("$name", (sc) => {
    const before = oldPlan(sc);
    const after = newPlan(sc);
    expect(after).toBe(before);
  });

  it("the scenarios are different documents (a differential that compares one plan proves nothing)", () => {
    const plans = new Set(SCENARIOS.map(newPlan));
    expect(plans.size).toBe(SCENARIOS.length);
  });

  it("a wrong mapping of one input is caught (negative control)", () => {
    const sc = SCENARIOS.find((s) => s.name === "a typed prefix")!;
    expect(newPlan({ ...sc, values: { prefix: "drill-y-" } })).not.toBe(oldPlan(sc));
  });
});

// ------------------------------------------------------------- the rules

describe("the subset and the mapping", () => {
  const point = backupPoint(backup, null);
  const ctx = ctxFor(point);
  const end = endInstant(point);
  const v = (over: Partial<WizardValues>) => ({ ...BLANK, pointInTime: end, prefix: defaultPrefixFor(end, ""), ...over });
  const problem = (over: Partial<WizardValues>) => mappingProblems(point, v(over), planFields(ctx, v(over)));

  it("a duplicate mapping is refused before the submit and names both rows", () => {
    expect(problem({ topics: ["orders", "orders"] })?.message).toMatch(/`orders` and `orders` both map/);
    expect(problem({ topics: ["orders"] })).toBeNull();
  });
  it("an empty subset, and a topic the point did not freeze, are refused by name", () => {
    expect(problem({ topics: [] })?.message).toBe("choose at least one topic to restore");
    expect(problem({ topics: ["orders", "ghost"] })?.message).toMatch(/`ghost` is not in this recovery point \(PlanTopicsNotInRecoveryPoint\)/);
  });
  it("an invalid prefix and an illegal mapped name are refused on the prefix", () => {
    expect(problem({ prefix: "bad prefix!" })).toMatchObject({ field: "prefix" });
    expect(problem({ prefix: "x".repeat(245) })).toMatchObject({ field: "prefix", message: expect.stringMatching(/a broker refuses/) });
    expect(problem({ prefix: "ok-" })).toBeNull();
  });
  it("the mapping the preview shows is the mapping the request declares, in the point's order", async () => {
    const values = v({ topics: ["payments", "orders"], prefix: "p-" });
    const f = planFields(ctx, values);
    const body = restoreBody(ctx, values, f, await preparePlanDocument(f), null);
    expect(body.topicMapping).toEqual([
      { source: "orders", target: "p-orders" },
      { source: "payments", target: "p-payments" },
    ]);
    expect(body.target.topicNaming).toEqual({ prefix: "p-" });
  });
});

describe("the point in time", () => {
  const point = backupPoint(backup, null);
  it("is refused outside the window the point discloses; both bounds are inside it", () => {
    expect(pointInTimeProblem(point, "2026-10-13T10:59:59Z")).toMatch(/outside the coverage/);
    expect(pointInTimeProblem(point, "2026-10-13T11:00:00.001Z")).toBeNull();
    expect(pointInTimeProblem(point, "2026-10-13T12:04:00.499Z")).toBeNull();
    expect(pointInTimeProblem(point, "2026-10-13T12:04:00.500Z")).toMatch(/outside the coverage/);
    expect(pointInTimeProblem(point, "yesterday")).toMatch(/RFC 3339/);
  });
  it("defaults to the last instant the window restores, as the old wizard does", () => {
    expect(endInstant(point)).toBe("2026-10-13T12:04:00.499Z");
  });
});

describe("a fresh-target retry", () => {
  it("is deterministic, differs from the point's default, and agrees with the old wizard", () => {
    const at = "2026-10-13T12:04:00Z";
    expect(freshTargetPrefix(at, "orders-restore-x7c2m")).toBe("restore-20261013T120400Z-retry-rex7c2m-");
    expect(freshTargetPrefix(at, "orders-restore-x7c2m")).toBe(old.freshTargetPrefix(at, "orders-restore-x7c2m"));
    expect(defaultPrefixFor(at, "orders-restore-x7c2m")).not.toBe(defaultPrefixFor(at, ""));
  });
  it("mints a different Restore and Approval name, so the failed run's approval cannot authorise it", async () => {
    const point = backupPoint(backup, null);
    const end = endInstant(point);
    const a = await preparePlanDocument(planFields(ctxFor(point), { ...BLANK, prefix: defaultPrefixFor(end, "") }));
    const r = ctxFor(point, { retryOf: "orders-restore-x7c2m" });
    const b = await preparePlanDocument(planFields(r, { ...BLANK, prefix: defaultPrefixFor(end, "orders-restore-x7c2m") }));
    expect(b.approvalName).not.toBe(a.approvalName);
    expect(b.restoreName).not.toBe(a.restoreName);
  });
});

describe("the replication factor", () => {
  const table: [(number | null)[] | null, number | null][] = [
    [[3, 1], 5],
    [[3], 2],
    [null, 5],
    [null, 2],
    [null, null],
    [[null, null], 1],
    [[2], null],
  ];
  it.each(table)("the default for source %j and brokers %j agrees with the old rule", (factors, brokers) => {
    const now = replicationDefault(factors, brokers);
    const then = old.replicationDefault(factors, brokers);
    expect({ value: now.value, basis: now.basis }).toEqual({ value: then.value, basis: then.basis });
  });
  it("a factor above a FRESH broker count is refused by the readiness check's code; a stale count refuses nothing", () => {
    const choice = { value: 4, basis: "chosen" as const, source: null, brokers: 3 };
    expect(replicationProblem(choice, fresh3)).toMatch(/^ReplicationFactorExceedsBrokers/);
    expect(replicationProblem(choice, { ...fresh3, fresh: false })).toBeNull();
    expect(replicationProblem({ ...choice, value: 0 }, fresh3)).toMatch(/whole number from 1/);
  });
  it("the broker count comes from this target's newest fresh discovery, and never another connection's", () => {
    const d = { id: "td-1", brokerCount: 3, stale: false, staleReasons: [], connection: { uid: target.uid }, observedAt: "x" } as never;
    expect(targetBrokerFact({ lastSuccessful: d }, target).count).toBe(3);
    expect(targetBrokerFact({ lastSuccessful: { ...(d as object), connection: { uid: "other" } } as never }, target).count).toBeNull();
    expect(targetBrokerFact({ lastSuccessful: { ...(d as object), stale: true, staleReasons: ["expired"] } as never }, target)).toMatchObject({ count: 3, fresh: false });
    expect(targetBrokerFact({ lastSuccessful: { ...(d as object), stale: true, staleReasons: ["connectionChanged"] } as never }, target).count).toBeNull();
    expect(targetBrokerFact({ unread: "403" }, target).why).toMatch(/could not be read: 403/);
  });
});

// ----------------------------------------------------- approval routing

const confirm = { ...twoPerson.item, name: "default-confirm-v1", mode: "ordinary", operatorMode: "confirm", basis: "freshInstall", ordinaryConfirmationAvailable: true, consoleApprovalAvailable: false, ticketRequired: false } as unknown as ApprovalPolicy;
const pair = twoPerson.item as unknown as ApprovalPolicy;
const strict = { ...pair, name: "prod-strict", operatorMode: "strict", consoleApprovalAvailable: false } as ApprovalPolicy;
const legacy = { ...pair, name: "legacy-governed-v1", legacy: true } as ApprovalPolicy;

describe("approval routing on submit", () => {
  it("names the route in each mode, and an unknown or unread policy reads as strict, never as no key", () => {
    expect(approvalRoute(confirm)).toBe("confirm");
    expect(approvalRoute({ ...confirm, ordinaryConfirmationAvailable: false })).toBe("confirm-unavailable");
    expect(approvalRoute(pair)).toBe("two-person");
    expect(approvalRoute({ ...pair, consoleApprovalAvailable: false })).toBe("two-person-unavailable");
    expect(approvalRoute(strict)).toBe("strict");
    expect(approvalRoute(legacy)).toBe("legacy");
    expect(approvalRoute(null)).toBe("unknown");
    expect(approvalRoute({ ...pair, operatorMode: "something-new" } as unknown as ApprovalPolicy)).toBe("strict");
  });
  it("refuses before sending when this console cannot take part, and not otherwise", () => {
    expect(policyRefusal({ ...confirm, ordinaryConfirmationAvailable: false })).toMatch(/confirmation key is not there/);
    expect(policyRefusal({ ...pair, consoleApprovalAvailable: false })).toMatch(/shared console/);
    expect(policyRefusal(confirm)).toBeNull();
    expect(policyRefusal(pair)).toBeNull();
  });

  const point = backupPoint(backup, null);
  const ctx = ctxFor(point);
  const end = endInstant(point);
  const originalValues = { ...BLANK, naming: "original" as const, noOwner: true, coverage: "complete" as const, pointInTime: end, prefix: defaultPrefixFor(end, "") };
  const bodyFor = async (policy: ApprovalPolicy | null, over: Partial<WizardValues> = {}) => {
    const values = { ...originalValues, ...over };
    const f = planFields(ctx, values);
    return restoreBody(ctx, values, f, await preparePlanDocument(f), policy);
  };

  it("OD-10: a one-person confirmation asks for the original names and sends them typed", async () => {
    expect(typedConfirmationRequired(confirm, originalValues)).toBe(true);
    const topics = ["orders", "orders-dlq", "payments"];
    expect(typedTopicsProblem(confirm, originalValues, topics)).toMatch(/re-type every original topic name/);
    expect(typedTopicsProblem(confirm, { ...originalValues, typedNames: "orders\norders-dlq" }, topics)).toMatch(/not typed: payments/);
    expect(typedTopicsProblem(confirm, { ...originalValues, typedNames: "orders, orders-dlq, payments, extra" }, topics)).toMatch(/typed but not restored by this plan: extra/);
    expect(typedTopicsProblem(confirm, { ...originalValues, typedNames: "Orders\norders-dlq\npayments" }, topics)).toMatch(/not typed: orders/);
    const typed = "payments\norders\n orders-dlq ";
    expect(typedTopicsProblem(confirm, { ...originalValues, typedNames: typed }, topics)).toBeNull();
    const body = await bodyFor(confirm, { typedNames: typed });
    expect(body.originalNameConfirmation).toEqual({ typedTopics: ["payments", "orders", "orders-dlq"] });
    expect(body.target.topicNaming).toEqual({ prefix: "", originalName: true });
    expect(body.coverage).toBe("complete");
  });
  it("OD-10: a namespace with a second person (or the strict or legacy route) asks for no typed names", async () => {
    for (const p of [pair, strict, legacy, null]) {
      expect(typedConfirmationRequired(p, originalValues)).toBe(false);
      expect(typedTopicsProblem(p, originalValues, ["orders"])).toBeNull();
      expect((await bodyFor(p, { typedNames: "orders" })).originalNameConfirmation).toBeUndefined();
    }
    // And an ordinary-name restore under confirm sends none either.
    expect(typedConfirmationRequired(confirm, { ...originalValues, naming: "prefix" })).toBe(false);
  });
  it("the ticket travels only under an explicit binding; an unbound namespace signs nothing", async () => {
    expect((await bodyFor(pair, { ticket: " CHG-1 " })).ticket).toBe("CHG-1");
    expect((await bodyFor(legacy, { ticket: "CHG-1" })).ticket).toBeUndefined();
    expect((await bodyFor(null, { ticket: "CHG-1" })).ticket).toBeUndefined();
    expect((await bodyFor(pair, { ticket: "  " })).ticket).toBeUndefined();
  });
});

describe("the create request", () => {
  const point = backupPoint(backup, null);
  const ctx = ctxFor(point);
  const end = endInstant(point);
  const values = { ...BLANK, pointInTime: end, prefix: defaultPrefixFor(end, "") };

  it("sends the prepared bytes and hash unchanged, and the approval name minted from them", async () => {
    const f = planFields(ctx, values);
    const prepared = await preparePlanDocument(f);
    const body = restoreBody(ctx, values, f, prepared, confirm);
    expect(body.planBytes).toBe(prepared.bytes);
    expect(body.planHash).toBe(prepared.hash);
    expect(body.approvalRef).toEqual({ name: prepared.approvalName });
    expect(body.sourceArchive).toEqual({ url: "logweir-destination://primary" });
    expect(body.sourceDestinationRef).toEqual({ name: "primary" });
    expect(body.evidenceDestinationRef).toEqual({ name: "primary" });
    expect(body.coverage).toBeUndefined();
    expect(body.deadlineSeconds).toBe(3600);
  });
  it("declares no mapping in scratch mode (the API refuses one there), and complete coverage when asked", async () => {
    const scratch = { ...values, scratch: true, coverage: "complete" as const, completeMaxRecords: "100" };
    const f = planFields(ctx, scratch);
    const body = restoreBody(ctx, scratch, f, await preparePlanDocument(f), confirm);
    expect(body.topicMapping).toBeUndefined();
    expect(body.target.mode).toBe("scratch");
    expect(body).toMatchObject({ coverage: "complete", completeMaxRecords: 100 });
  });
  it("an inline archive sends its Secret NAME and no destination", async () => {
    const lp = backupPoint(legacyBackup, null);
    const lctx = ctxFor(lp);
    const lv = { ...values, archiveSecret: "legacy-s3", evidenceBucket: "legacy-bucket" };
    const f = planFields(lctx, lv);
    const body = restoreBody(lctx, lv, f, await preparePlanDocument(f), confirm);
    expect(body.sourceArchive).toEqual({ url: "s3://legacy-bucket/team-a/old", credentialRef: { name: "legacy-s3" } });
    expect(body.sourceDestinationRef).toBeUndefined();
  });
  it("one plan, one key: the same bytes resend the same key; another plan has another", async () => {
    const a = await preparePlanDocument(planFields(ctx, values));
    const b = await preparePlanDocument(planFields(ctx, { ...values, prefix: "other-" }));
    expect(restoreIdempotencyKey("team-a", a)).toBe(restoreIdempotencyKey("team-a", await preparePlanDocument(planFields(ctx, values))));
    expect(restoreIdempotencyKey("team-a", a)).not.toBe(restoreIdempotencyKey("team-a", b));
    expect(restoreIdempotencyKey("team-a", a)).toMatch(/^logweir-ui\.restores\.team-a\.restore-[0-9a-f]{8}$/);
  });
  it("validation refuses a missing owner statement, a missing evidence store and a bad Secret name by input", () => {
    const f = planFields(ctx, { ...values, naming: "original" });
    expect(validate(ctx, { ...values, naming: "original" }, f, null).noOwner).toMatch(/declarative owner/);
    expect(validate(ctxFor(point, { evidence: null }), values, planFields(ctxFor(point, { evidence: null }), values), null).evidenceDestination).toBeDefined();
    const lp = backupPoint(legacyBackup, null);
    const lv = { ...values, archiveSecret: "Bad_Name" };
    expect(validate(ctxFor(lp), lv, planFields(ctxFor(lp), lv), null)).toMatchObject({ archiveSecret: expect.any(String), evidenceBucket: expect.any(String) });
    expect(validate(ctx, values, planFields(ctx, values), null)).toEqual({});
  });
  it("theme A: a clone's lifetime and the topic configuration reach the plan only as chosen", () => {
    const clone = { ...values, scratch: true, cloneTtl: 86_400, topicConfiguration: { transition: "afterVerification" as const, noOwners: true } };
    const f = planFields(ctx, clone);
    expect(f.target).toMatchObject({ mode: "scratch", teardownTtlSeconds: 86_400, topicConfiguration: { transition: "afterVerification", owners: [] } });
    expect(renderPlanBytes(f)).toContain("  teardown:\n    ttl_seconds: 86400\n  topic_configuration:\n");
    // Controls: the lifetime is dropped once scratch is off, and no choice is no block.
    const kept = planFields(ctx, { ...clone, scratch: false });
    expect(kept.target.teardownTtlSeconds).toBeUndefined();
    expect(renderPlanBytes(planFields(ctx, values))).not.toMatch(/ttl_seconds|topic_configuration/);
    // A lifetime out of its bounds is refused beside its field, before any plan is sent.
    expect(validate(ctx, { ...clone, cloneTtl: 120 }, planFields(ctx, { ...clone, cloneTtl: 120 }), null).cloneTtl).toMatch(/CloneTtlOutOfRange/);
    expect(validate(ctx, clone, f, null).cloneTtl).toBeUndefined();
  });
});

// ------------------------------------------------------------- readiness

const pf = (raw: unknown, over: Partial<Preflight> = {}) => ({ ...((raw as { item: Preflight }).item), ...over }) as Preflight;
const draftRow = { id: "approval.state", state: "skipped", code: "SubjectNotCreated", gating: "blocking" } as Preflight["checks"][number];

describe("the readiness gate", () => {
  const H = `sha256:${"1".repeat(64)}`;
  const ready = pf(readyFixture, { binding: { planHash: H, referents: [] } });
  it("passes a ready check bound to this plan, and refuses one bound to another plan", () => {
    expect(readinessRefusal(ready, H, H)).toBeNull();
    expect(readinessRefusal(ready, H, `sha256:${"2".repeat(64)}`)).toMatch(/produced for plan/);
  });
  it("refuses nothing run, an unfinished, a stale and a not-ready check, naming the rows", () => {
    expect(readinessRefusal(null, "", H)).toMatch(/No readiness check has run/);
    expect(readinessRefusal(pf(pendingFixture), "", "")).toMatch(/has not finished/);
    expect(readinessRefusal(pf(staleFixture), "", "")).toMatch(/no longer applies/);
    expect(readinessRefusal(pf(notReadyFixture), "", "")).toMatch(/destination\.archiveListable \(AccessDenied\)/);
  });
  it("a draft-shaped verdict submits: the approval row alone, every other blocking row ready", () => {
    const draft = { ...ready, state: "unknown", checks: [...ready.checks, draftRow] } as Preflight;
    expect(readyButForDraftApproval(draft)).toBe(true);
    expect(readinessRefusal(draft, H, H)).toBeNull();
  });
  it("the draft row plus any other blocking non-ready row refuses, by id and code", () => {
    const other = { id: "target.mappedTopics", state: "notReady", code: "MappedTopicExists", gating: "blocking" } as Preflight["checks"][number];
    const draft = { ...ready, state: "unknown", checks: [...ready.checks, draftRow, other] } as Preflight;
    expect(readyButForDraftApproval(draft)).toBe(false);
    expect(readinessRefusal(draft, H, H)).toMatch(/target\.mappedTopics \(MappedTopicExists\)/);
  });
  it("approval.state notReady is not the draft shape; the draft row alone is not everything passed", () => {
    const notReady = { ...ready, state: "unknown", checks: [...ready.checks, { ...draftRow, state: "notReady" }] } as Preflight;
    expect(readyButForDraftApproval(notReady)).toBe(false);
    const alone = { ...ready, state: "unknown", checks: [draftRow] } as Preflight;
    expect(readyButForDraftApproval(alone)).toBe(false);
  });
});

// --------------------------------------------------- points and destinations

describe("recovery points", () => {
  it("a catalog row is offered by the old rules (selectable, unredacted, whole), agreeing with ui/", () => {
    const base = { ...row };
    const cases: [string, PointView, { viewExpired?: boolean; backupVerdictsIncomplete?: string | null }][] = [
      ["whole", base, {}],
      ["redacted receipt key", { ...base, receiptKey: "[redacted].receipt.json" }, {}],
      ["a Backup verdict", { ...base, backupVerdict: "Invalid" }, {}],
      ["not selectable", { ...base, selectable: false }, {}],
      ["no manifest digest", { ...base, manifestSha256: undefined }, {}],
      ["view expired", base, { viewExpired: true }],
      ["verdicts incomplete", base, { backupVerdictsIncomplete: "listFailed" }],
      ["bad point id", { ...base, pointId: "lwp1-XYZ" }, {}],
    ];
    for (const [, entry, page] of cases) expect(catalogPointOffer(entry, page).offer).toBe(old.catalogPointOffer(entry, page).offer);
    expect(cases.filter(([, e, p]) => catalogPointOffer(e, p).offer).map(([n]) => n)).toEqual(["whole"]);
  });
  it("a point frozen against a destination that was recreated or moved is refused, naming both", () => {
    const point = backupPoint(backup, null);
    expect(frozenDestinationProblem(point, destination)).toBeNull();
    expect(frozenDestinationProblem(point, { ...destination, uid: "new-uid" })).toMatch(/was recreated: recovery point UID .* does not match live UID new-uid/);
    expect(frozenDestinationProblem(point, { ...destination, locationDigest: "sha256:00" })).toMatch(/moved/);
    expect(frozenDestinationProblem(point, null)).toMatch(/absent/);
    expect(frozenDestinationProblem(backupPoint({ ...backup, locationDigest: undefined } as typeof backup, null), destination)).toMatch(/does not publish the frozen/);
  });
  it("the backup set ids the controller mints all make a plan", () => {
    for (const id of setIds.minted) {
      const point = backupPoint({ ...backup, backupId: id }, null);
      const end = endInstant(point);
      expect(() => renderPlanBytes(planFields(ctxFor(point), { ...BLANK, prefix: defaultPrefixFor(end, "") }))).not.toThrow();
    }
  });
});
