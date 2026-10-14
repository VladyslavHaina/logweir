// @vitest-environment node
// The plan goldens, the most important rows of R5: the plan hash is what an approver
// signs. These read the SAME files crates/logweir/tests/console_plan.rs deserialises into
// the runner's RestoreSpec (console/fixtures/plan*.golden.yaml), so "Rust parses the
// golden" and "the new console emits the golden" together say the runner parses what
// the new console emits. A differential row holds the port to what the retired
// ui/plan.js answered over a corpus of awkward inputs, errors included
// (console/fixtures/legacy-console.json, recorded before its removal).
// @ts-expect-error this package has no @types/node; the row wants Node's own digest as an independent check.
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import recorded from "../../../fixtures/legacy-console.json";
import plainFields from "../../../fixtures/plan-fields.json";
import completeFields from "../../../fixtures/plan-complete-fields.json";
import originalFields from "../../../fixtures/plan-original-name-fields.json";
import pointFields from "../../../fixtures/plan-point-fields.json";
import timeBasisFields from "../../../fixtures/plan-time-basis-fields.json";
import cloneFields from "../../../fixtures/plan-clone-fields.json";
import ownersFields from "../../../fixtures/plan-owners-fields.json";
import plainGolden from "../../../fixtures/plan.golden.yaml?raw";
import completeGolden from "../../../fixtures/plan-complete.golden.yaml?raw";
import originalGolden from "../../../fixtures/plan-original-name.golden.yaml?raw";
import pointGolden from "../../../fixtures/plan-point.golden.yaml?raw";
import timeBasisGolden from "../../../fixtures/plan-time-basis.golden.yaml?raw";
import cloneGolden from "../../../fixtures/plan-clone.golden.yaml?raw";
import ownersGolden from "../../../fixtures/plan-owners.golden.yaml?raw";
import {
  INSECURE_CONTEXT_SENTENCE,
  defaultTopicPrefix,
  mintNames,
  planHash,
  preparePlanDocument,
  preparedFor,
  renderPlanBytes,
  type PlanFields,
} from "./plan";

const fields = (raw: unknown): PlanFields => structuredClone(raw) as PlanFields;

const GOLDENS: [string, unknown, string][] = [
  ["plan.golden.yaml", plainFields, plainGolden],
  ["plan-complete.golden.yaml", completeFields, completeGolden],
  ["plan-original-name.golden.yaml", originalFields, originalGolden],
  ["plan-point.golden.yaml", pointFields, pointGolden],
  ["plan-time-basis.golden.yaml", timeBasisFields, timeBasisGolden],
  ["plan-clone.golden.yaml", cloneFields, cloneGolden],
  ["plan-owners.golden.yaml", ownersFields, ownersGolden],
];

const bytes = (s: string) => new TextEncoder().encode(s);

describe("the plan goldens", () => {
  it.each(GOLDENS)("%s: the new console renders the golden byte for byte, length included", (_name, raw, golden) => {
    const out = renderPlanBytes(fields(raw));
    expect(bytes(out).length).toBe(bytes(golden).length);
    expect(out).toBe(golden);
  });

  it("a golden is not matched by a neighbour's fields (negative control)", () => {
    // Each golden differs from the plain one; rendering the plain fields never yields them.
    const plain = renderPlanBytes(fields(plainFields));
    for (const [, , golden] of GOLDENS.slice(1)) expect(plain).not.toBe(golden);
    // And one changed value is one changed document.
    const moved = fields(plainFields);
    moved.pointInTime = "2026-09-07T14:05:01Z";
    expect(renderPlanBytes(moved)).not.toBe(plainGolden);
  });

  it("the values console_plan.rs asserts after the RestoreSpec parse are the ones the console wrote", () => {
    // The Rust arm deserialises each golden and compares these values with the fields file.
    expect(renderPlanBytes(fields(plainFields))).toContain('  mode: "newTopic"\n  topic_naming:\n    prefix: "restore-20260907T140500Z-"\n');
    expect(renderPlanBytes(fields(plainFields))).toContain('restore:\n  point_in_time: "2026-09-07T14:05:00Z"\nsample:');
    expect(renderPlanBytes(fields(timeBasisFields))).toContain('  time_basis: "producerTime"\n');
    expect(renderPlanBytes(fields(completeFields))).toContain('  coverage: "complete"\n  complete_max_records: 1000000\n');
    expect(renderPlanBytes(fields(originalFields))).toContain('    prefix: ""\n    original_name:\n      owners: []\n  topic_mapping_prefix: "drill-"');
    expect(renderPlanBytes(fields(pointFields))).toContain('  point:\n    point_id: "lwp1-0123456789abcdef0123456789abcdef"\n');
    // The plain plan carries none of the opt-ins (the Rust arm asserts the same absence).
    const plain = renderPlanBytes(fields(plainFields));
    for (const key of ["time_basis", "coverage:", "original_name", "  point:", "ttl_seconds", "topic_configuration"]) expect(plain).not.toContain(key);
  });

  it("theme A: a clone's lifetime is the teardown map in scratch mode only, inside its bounds", () => {
    const clone = renderPlanBytes(fields(cloneFields));
    expect(clone).toContain('  teardown:\n    ttl_seconds: 86400\n  topic_configuration:\n    transition: "afterVerification"\n    owners: []\nrestore:');
    const newTopic = fields(cloneFields);
    newTopic.target.mode = "newTopic";
    expect(() => renderPlanBytes(newTopic)).toThrow(/CloneTtlNeedsScratch/);
    for (const ttl of [299, 2_592_001, 3600.5]) {
      const bad = fields(cloneFields);
      bad.target.teardownTtlSeconds = ttl;
      expect(() => renderPlanBytes(bad)).toThrow(ttl === 3600.5 ? /whole number/ : /CloneTtlOutOfRange/);
    }
    // Controls: the bounds themselves pass, and no lifetime is the string policy.
    for (const ttl of [300, 2_592_000]) {
      const ok = fields(cloneFields);
      ok.target.teardownTtlSeconds = ttl;
      expect(renderPlanBytes(ok)).toContain(`    ttl_seconds: ${ttl}\n`);
    }
    const none = fields(cloneFields);
    none.target.teardownTtlSeconds = null;
    none.target.topicConfiguration = null;
    expect(renderPlanBytes(none)).toContain('  teardown: "delete"\nrestore:');
    const odd = fields(cloneFields);
    odd.target.topicConfiguration = { transition: "later" as never };
    expect(() => renderPlanBytes(odd)).toThrow(/afterVerification or none/);
  });
});

describe("the port agrees with what ui/plan.js answered", () => {
  /** The new renderer answers as the old one did: equal bytes, or the same error class and message. */
  function same(name: string, input: unknown) {
    let right: { ok: string } | { error: string };
    try {
      right = { ok: renderPlanBytes(structuredClone(input) as PlanFields) };
    } catch (error) {
      right = { error: `${(error as Error).constructor.name}: ${(error as Error).message}` };
    }
    const left = (recorded.planCorpus as Record<string, { ok: string } | { error: string }>)[name];
    expect(left, `a recorded answer for ${name}`).toBeDefined();
    expect(right).toEqual(left);
  }

  const base = () => fields(plainFields) as PlanFields & Record<string, unknown>;
  const corpus: [string, (f: PlanFields) => void][] = [
    ["plain", () => {}],
    ["awkward names", (f) => {
      f.name = 'restauração "quoted" #1: \\ back';
      f.topics = ["a:b", "#hash", "1lead", 'q"uote', "tab\there", "nl\nline", "ctl\u0001\u007f", "emoji-🦀", "ü"];
    }],
    ["scram auth", (f) => (f.target.auth = { mode: "scramSha512", username: "restorer", tls: true })],
    ["scram without a username", (f) => (f.target.auth = { mode: "scramSha256", tls: false })],
    ["plain without tls", (f) => (f.target.auth = { mode: "plain", username: "u" })],
    ["mtls without tls", (f) => (f.target.auth = { mode: "mtls" })],
    ["mtls", (f) => (f.target.auth = { mode: "mtls", tls: true })],
    ["unknown auth", (f) => (f.target.auth = { mode: "kerberos", tls: true })],
    ["plaintext auth", (f) => (f.target.auth = { mode: "plaintext" })],
    ["scratch", (f) => (f.target.mode = "scratch")],
    ["a third mode", (f) => (f.target.mode = "inPlace")],
    ["original names", (f) => Object.assign(f.target, { originalName: true, originalNameNoOwner: true, topicPrefix: "" }) && (f.sample.coverage = "complete")],
    ["original names, sampled", (f) => Object.assign(f.target, { originalName: true, originalNameNoOwner: true, topicPrefix: "" })],
    ["original names, no owner statement", (f) => Object.assign(f.target, { originalName: true }) && (f.sample.coverage = "complete")],
    ["original names in scratch", (f) => Object.assign(f.target, { originalName: true, originalNameNoOwner: true, mode: "scratch" }) && (f.sample.coverage = "complete")],
    ["complete with a bound", (f) => Object.assign(f.sample, { coverage: "complete", completeMaxRecords: 1 })],
    ["complete with bound 0", (f) => Object.assign(f.sample, { coverage: "complete", completeMaxRecords: 0 })],
    ["complete with a fractional bound", (f) => Object.assign(f.sample, { coverage: "complete", completeMaxRecords: 1.5 })],
    ["a bound beside sampled", (f) => Object.assign(f.sample, { coverage: "sampled", completeMaxRecords: 5 })],
    ["coverage typo", (f) => (f.sample.coverage = "Complete")],
    ["time basis", (f) => (f.timeBasis = "producerTime")],
    ["time basis empty", (f) => (f.timeBasis = "")],
    ["time basis typo", (f) => (f.timeBasis = "logAppendTime")],
    ["no objectives", (f) => (f.objectives = {})],
    ["objectives absent", (f) => delete f.objectives],
    ["pass rate 1", (f) => (f.objectives = { passRate: 1 })],
    ["pass rate tiny", (f) => (f.objectives = { passRate: 1e-7 })],
    ["pass rate NaN", (f) => (f.objectives = { passRate: Number.NaN })],
    ["rto fractional", (f) => (f.objectives = { rtoSeconds: 1.5 })],
    ["replication 0", (f) => (f.target.replicationFactor = 0)],
    ["replication max", (f) => (f.target.replicationFactor = 32767)],
    ["replication over", (f) => (f.target.replicationFactor = 32768)],
    ["replication NaN", (f) => (f.target.replicationFactor = Number.NaN)],
    ["no topics", (f) => (f.topics = [])],
    ["no servers", (f) => (f.target.bootstrapServers = [])],
    ["no prefix", (f) => (f.target.topicPrefix = "")],
    ["no point in time", (f) => (f.pointInTime = "")],
    ["no evidence bucket", (f) => (f.evidence.bucket = "")],
    ["bare storage", (f) => (f.source = { bucket: "b" })],
    ["null prefix", (f) => ((f.source as { prefix: unknown }).prefix = null)],
    ["point binding", (f) => (f.point = structuredClone(pointFields.point))],
    ["point id upper case", (f) => (f.point = { ...structuredClone(pointFields.point), pointId: "lwp1-0123456789ABCDEF0123456789abcdef" })],
    ["receipt key blank", (f) => (f.point = { ...structuredClone(pointFields.point), receiptKey: "  " })],
    ["manifest digest short", (f) => (f.point = { ...structuredClone(pointFields.point), manifestSha256: "sha256:abc" })],
  ];

  it.each(corpus)("%s", (name, change) => {
    const f = base();
    change(f);
    same(name, f);
  });

  it("the corpus exercises both arms: documents and refusals", () => {
    const outcomes = corpus.map(([, change]) => {
      const f = base();
      change(f);
      try {
        renderPlanBytes(f);
        return "ok";
      } catch {
        return "refused";
      }
    });
    expect(outcomes.filter((o) => o === "ok").length).toBeGreaterThan(10);
    expect(outcomes.filter((o) => o === "refused").length).toBeGreaterThan(10);
  });

  it("the default prefix agrees with ui/plan.js (and console_plan.rs with Rust's) for the same instant", () => {
    for (const [at, want] of Object.entries(recorded.defaultPrefixes)) expect(defaultTopicPrefix(at)).toBe(want);
    expect(defaultTopicPrefix("2026-09-07T14:05:00Z")).toBe("restore-20260907T140500Z-");
    expect(() => defaultTopicPrefix("not an instant")).toThrow(TypeError);
  });
});

describe("the hash and the two names", () => {
  it("the hash is sha256:<hex> of exactly these bytes, by an independent digest", async () => {
    const plan = renderPlanBytes(fields(plainFields));
    const independent = createHash("sha256").update(plan, "utf8").digest("hex");
    expect(await planHash(plan)).toBe(`sha256:${independent}`);
    // A trimmed or re-normalised document is another hash (negative control).
    expect(await planHash(plan.trimEnd())).not.toBe(`sha256:${independent}`);
  });

  it("both names are minted from the bytes, before either object exists", async () => {
    const plan = renderPlanBytes(fields(plainFields));
    const suffix = createHash("sha256").update(plan, "utf8").digest("hex").slice(0, 8);
    expect(await mintNames(plan)).toEqual({ restoreName: `restore-${suffix}`, approvalName: `approval-${suffix}` });
  });

  it("the prepared plan is one frozen object per document: the reviewed object is the submitted object", async () => {
    const f = fields(plainFields);
    const reviewed = await preparePlanDocument(f);
    const submitted = await preparePlanDocument(fields(plainFields));
    expect(submitted).toBe(reviewed);
    expect(Object.isFrozen(reviewed)).toBe(true);
    expect(preparedFor(reviewed.bytes)).toBe(reviewed);
    // Supplied bytes are used as given, never re-rendered: two trailing spaces survive.
    const crafted = `${reviewed.bytes.slice(0, -1)}  \n`;
    const kept = await preparePlanDocument(f, { bytes: crafted });
    expect(kept.bytes).toBe(crafted);
    expect(kept).not.toBe(reviewed);
    // Bytes never prepared have no answer (negative control).
    expect(preparedFor(`${reviewed.bytes} `)).toBeNull();
  });
});

describe("a page outside a secure context", () => {
  const real = Object.getOwnPropertyDescriptor(globalThis, "crypto")!;
  afterEach(() => Object.defineProperty(globalThis, "crypto", real));

  it("refuses to hash rather than show a plan with no hash", async () => {
    Object.defineProperty(globalThis, "crypto", { value: {}, configurable: true, writable: true });
    await expect(planHash("x")).rejects.toThrow(INSECURE_CONTEXT_SENTENCE);
    Object.defineProperty(globalThis, "crypto", real);
    await expect(planHash("x")).resolves.toMatch(/^sha256:[0-9a-f]{64}$/);
  });
});
