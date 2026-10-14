// The cutover's logic, on the API's own shapes: the review lines (`detail_lines` of
// crates/logweir/src/check/kinds/switchover.rs), the verdict that comes only from the
// preflight's word, the plan bytes the approver signs, and what a rollback can do.
// Every row has a negative control: the input that must NOT pass.
import { describe, expect, it } from "vitest";
import goldenPlan from "../pages/cutover/fixtures/switchover-plan.golden.yaml?raw";
import namesPlan from "../pages/cutover/fixtures/switchover-plan-names.golden.yaml?raw";
import { ApiError } from "./client";
import {
  chosenByDefault,
  REFUSAL_WORDS,
  evidenceDigests,
  groupsPartial,
  mintNames,
  offered,
  outcomeUnknown,
  parseOwnPlan,
  planDocument,
  reviewGroups,
  reviewState,
  rollbackPlan,
  verdictOf,
  yamlString,
  type CheckEntry,
  type DetailEntry,
  type Preflight,
  type Switchover,
} from "./switchovers";

const sha = (c: string) => `sha256:${c.repeat(64)}`;
const RESTORE = { name: "orders-recovery", uid: "6f1c2a3b-0000-4000-8000-000000000001", planHash: sha("a"), scorecardSha256: sha("b"), mappingSha256: sha("c") };

/** One details line, as `detail_lines` writes it. */
function line(group: string, over: Record<string, unknown> = {}, positions?: unknown[]): DetailEntry {
  return {
    entry: {
      group,
      target: group,
      admissible: true,
      absent: false,
      alreadyApplied: false,
      type: "classic",
      state: "Empty",
      members: 0,
      topic: "orders",
      positions: positions ?? [
        { partition: 1, class: "approximate", committed: null, offset: 50, reason: "selectionExcluded" },
        { partition: 0, class: "exact", committed: null, offset: 1200 },
      ],
      ...over,
    },
  };
}

function preflight(over: Partial<Preflight> = {}, rows: Partial<CheckEntry>[] = []): Preflight {
  const checks: CheckEntry[] = [
    { id: "switchover.restore", state: "ready" },
    { id: "switchover.evidence", state: "ready", message: `the restore's scorecard verifies (2 group(s)) [mappingSha256=${sha("c")}; scorecardSha256=${sha("b")}]` },
    { id: "switchover.target", state: "ready" },
    { id: "switchover.groups", state: "ready" },
  ].map((c) => ({ ...c, ...(rows.find((r) => r.id === c.id) ?? {}) }) as CheckEntry);
  return {
    id: "pf-sw1",
    namespace: "team-a",
    uid: "u",
    resourceVersion: "1",
    operation: "switchover",
    state: "ready",
    terminal: true,
    applicable: true,
    stale: false,
    staleBasis: ["expiry"],
    staleReasons: [],
    binding: { referents: [] },
    checks,
    warnings: [],
    executionOnly: [],
    conditions: [],
    detailsAvailable: true,
    ...over,
  };
}

describe("the review lines", () => {
  it("merges a group's lines over its topics and keeps every partition", () => {
    const { groups, truncated, unread } = reviewGroups([
      line("app"),
      line("app", { topic: "payments" }, [{ partition: 0, class: "unavailable", committed: null, reason: "BeforeArchive" }]),
      line("other", { admissible: false, reason: "GroupActive", message: "the group has 2 member(s)", members: 2, state: "Stable" }),
      { entry: { truncated: true, kept: 3, total: 9 } },
    ]);
    expect(truncated).toBe(true);
    expect(unread).toBe(0);
    expect(groups.map((g) => g.source)).toEqual(["app", "other"]);
    expect(groups[0]!.positions.map((p) => `${p.topic}:${p.partition}:${p.class}`)).toEqual(["orders:1:approximate", "orders:0:exact", "payments:0:unavailable"]);
    expect(groups[1]).toMatchObject({ admissible: false, reason: "GroupActive", members: 2 });
  });

  it("never offers a group with a line it cannot read", () => {
    const ok = reviewGroups([line("app")]).groups[0]!;
    const bad = reviewGroups([line("app", {}, [{ partition: 0, class: "exactish", committed: null, offset: 1 }])]).groups[0]!;
    expect(offered(ok)).toBe(true);
    // Negative control: the same group with one unknown class is not admissible.
    expect(bad.admissible).toBe(false);
    expect(offered(bad)).toBe(false);
  });

  it("a group whose lines disagree is not admissible", () => {
    const g = reviewGroups([line("app"), line("app", { topic: "payments", admissible: false, reason: "TargetPositionsChanged" })]).groups[0]!;
    expect(g.admissible).toBe(false);
    expect(verdictOf(g).verdict).toBe("blocked");
  });
});

describe("the verdict is the preflight's word", () => {
  const one = (over: Record<string, unknown>) => reviewGroups([line("app", over)]).groups[0]!;
  it("admissible is Ready, admissible and applied is Already switched", () => {
    expect(verdictOf(one({})).verdict).toBe("ready");
    expect(verdictOf(one({ alreadyApplied: true })).verdict).toBe("alreadyApplied");
  });
  it("a refused group is never Ready, whatever else it says", () => {
    expect(verdictOf(one({ admissible: false, reason: "GroupActive" }))).toEqual({ verdict: "stopConsumers", reason: "GroupActive" });
    expect(verdictOf(one({ admissible: false, reason: "TargetPositionsChanged", alreadyApplied: true }))).toEqual({ verdict: "blocked", reason: "TargetPositionsChanged" });
    expect(verdictOf(one({ admissible: false }))).toEqual({ verdict: "blocked", reason: "NotAdmissible" });
  });
  it("an unavailable partition is offered only on an explicit choice; nothing to commit is not offered", () => {
    const gap = reviewGroups([line("app", {}, [{ partition: 0, class: "exact", committed: null, offset: 1 }, { partition: 1, class: "unavailable", committed: null, reason: "ArchiveGap" }])]).groups[0]!;
    expect(offered(gap)).toBe(true);
    expect(chosenByDefault(gap)).toBe(false);
    const none = reviewGroups([line("app", {}, [{ partition: 1, class: "unavailable", committed: null, reason: "ArchiveGap" }])]).groups[0]!;
    expect(offered(none)).toBe(false);
    expect(chosenByDefault(one({}))).toBe(true);
    expect(offered(one({ alreadyApplied: true }))).toBe(false);
  });
});

describe("where the review stands", () => {
  it("is decided only by a finished, current review whose evidence and target rows are ready", () => {
    expect(reviewState(preflight()).kind).toBe("decided");
    expect(reviewState(preflight({ state: "notReady" }, [{ id: "switchover.groups", state: "notReady" }])).kind).toBe("decided");
    expect(reviewState(preflight({ terminal: false, state: "running" }))).toEqual({ kind: "running", state: "running" });
    // Negative controls: each missing condition offers nothing.
    expect(reviewState(preflight({ stale: true })).kind).toBe("undecided");
    expect(reviewState(preflight({ applicable: false })).kind).toBe("undecided");
    expect(reviewState(preflight({ state: "unknown" }, [{ id: "switchover.target", state: "unknown" }])).kind).toBe("undecided");
    expect(reviewState(preflight({ state: "failed" })).kind).toBe("undecided");
  });
  it("names the rows that refused", () => {
    const s = reviewState(preflight({ state: "notReady" }, [{ id: "switchover.evidence", state: "notReady", code: "RestoreEvidenceRefused", message: "VerificationNotComplete: ..." }]));
    expect(s.kind).toBe("refused");
    expect(s.kind === "refused" && s.checks.map((c) => c.code)).toEqual(["RestoreEvidenceRefused"]);
  });
});

describe("the digests the plan binds", () => {
  it("are read from the evidence row's folded facts", () => {
    expect(evidenceDigests(preflight())).toEqual({ scorecardSha256: sha("b"), mappingSha256: sha("c") });
  });
  it("are absent from a row that is not ready, or that carries no well-formed digest", () => {
    expect(evidenceDigests(preflight({}, [{ id: "switchover.evidence", state: "notReady" }]))).toBeNull();
    expect(evidenceDigests(preflight({}, [{ id: "switchover.evidence", state: "ready", message: `x [mappingSha256=sha256:${"c".repeat(63)}; scorecardSha256=${sha("b")}]` }]))).toBeNull();
  });
});

describe("the plan document", () => {
  const golden = goldenPlan;
  const groups = reviewGroups([
    line("orders-app"),
    line("orders-app", { topic: "payments" }, [
      { partition: 2, class: "exact", committed: null, offset: 7 },
      { partition: 0, class: "unavailable", committed: null, reason: "BeforeArchive" },
    ]),
    line("1e3", { topic: "true" }, [{ partition: 0, class: "exact", committed: null, offset: 0 }]),
  ]).groups;

  it("is byte-for-byte the golden every partition and class lands in", () => {
    const doc = planDocument(RESTORE, groups);
    expect(doc).toEqual({ bytes: golden });
  });
  it("writes a group id with a quote, a line separator and an astral character as the runner reads it back", () => {
    // The runner's own parse of this fixture gives back the id unchanged (scratch check, R7 report).
    const id = 'we"ird\u2028gr\u{1f600}up';
    const g = reviewGroups([line(id, {}, [{ partition: 0, class: "exact", committed: null, offset: 5 }])]).groups;
    expect(planDocument(RESTORE, g)).toEqual({ bytes: namesPlan });
    expect(parseOwnPlan(namesPlan)!.groups[0]!.source).toBe(id);
  });
  it("quotes every name, so none reads as a number, a boolean or YAML syntax", () => {
    expect(yamlString("1e3")).toBe('"1e3"');
    expect(yamlString('a"b\\c')).toBe('"a\\"b\\\\c"');
    expect(yamlString("a b")).toBe('"a\\u2028b"');
    expect(yamlString("é😀")).toBe('"\\u00e9\\U0001f600"');
  });
  it("refuses what the plan grammar refuses", () => {
    const one = groups.slice(0, 1);
    expect(planDocument({ ...RESTORE, mappingSha256: "sha256:abc" }, one)).toEqual({ refused: "restore.mapping_sha256 is not sha256:<64 lowercase hex>" });
    expect("refused" in planDocument(RESTORE, [])).toBe(true);
    expect("refused" in planDocument(RESTORE, [...one, { ...one[0]!, source: "other" }])).toBe(true); // the same target twice
    expect("refused" in planDocument(RESTORE, [{ ...one[0]!, source: "bad\u0007id", target: "bad\u0007id" }])).toBe(true);
    const nothing = reviewGroups([line("gap", {}, [{ partition: 0, class: "unavailable", committed: null }])]).groups;
    expect("refused" in planDocument(RESTORE, nothing)).toBe(true);
    const many = Array.from({ length: 101 }, (_, i) => ({ ...one[0]!, source: `g${i}`, target: `g${i}` }));
    expect(planDocument(RESTORE, many)).toEqual({ refused: "the plan names 101 groups; it names 1 to 100" });
    const wide = reviewGroups([line("w", {}, Array.from({ length: 8193 }, (_, p) => ({ partition: p, class: "exact", committed: null, offset: p })))]).groups;
    expect("refused" in planDocument(RESTORE, wide)).toBe(true);
    // Negative control: the bounds themselves pass.
    expect("bytes" in planDocument(RESTORE, many.slice(0, 100))).toBe(true);
  });
  it("reads back exactly what it wrote, and nothing it did not", () => {
    const parsed = parseOwnPlan(golden)!;
    expect(parsed.restore).toMatchObject({ name: RESTORE.name, uid: RESTORE.uid, mapping_sha256: RESTORE.mappingSha256 });
    expect(parsed.groups.map((g) => [g.target, g.partitions.map((p) => `${p.topic}:${p.partition}=${p.offset}${p.approximate ? "~" : ""}`)])).toEqual([
      ["orders-app", ["orders:0=1200", "orders:1=50~", "payments:2=7"]],
      ["1e3", ["true:0=0"]],
    ]);
    // Negative control: `logweir switchover plan`'s own layout is not misread as ours.
    expect(parseOwnPlan("format_version: 1.0.0\nrestore:\n  name: r\ngroups:\n- source: app\n")).toBeNull();
  });
});

describe("the rollback", () => {
  const sw = (groups: Switchover["groups"]): Switchover => ({ name: "swo-1", namespace: "team-a", uid: "u", approvalName: "a", planHash: sha("d"), restore: { name: "r", uid: "u" }, groups, phase: "PartiallyApplied", recordKey: "logweir/switchovers/RUN.record.json" });
  const plan = parseOwnPlan(goldenPlan);

  it("reverses only applied groups; a position that was absent is listed with the broker's command", () => {
    const rb = rollbackPlan(sw([
      { source: "orders-app", target: "orders-app", outcome: "applied", created: false, partitions: 3 },
      { source: "1e3", target: "1e3", outcome: "refused", reason: "GroupActive", created: false, partitions: 1 },
    ]), plan);
    expect(rb.restorable).toEqual([]);
    expect(rb.notRestorable.map((r) => `${r.target} ${r.topic}:${r.partition}`)).toEqual(["orders-app orders:0", "orders-app orders:1", "orders-app payments:2"]);
    expect(rb.text).toContain("kafka-consumer-groups.sh --bootstrap-server <bootstrap> --group 'orders-app' --topic 'orders:0' --delete-offsets");
    expect(rb.text).toContain("logweir switchover rollback-plan --record <a local copy of the switchover record> --out rollback.yaml");
    // Negative control: the refused group is not in it.
    expect(rb.text).not.toContain("'1e3'");
  });
  it("commits back a stated prior, and says when a group failed part-way", () => {
    const golden = goldenPlan;
    const withPrior = parseOwnPlan(golden.replace("        offsets: {2: 7}\n        expected: none", "        offsets: {2: 7}\n        expected: {2: 6}"));
    const rb = rollbackPlan(
      sw([
        { source: "orders-app", target: "orders-app", outcome: "applied", created: false, partitions: 3 },
        { source: "1e3", target: "1e3", outcome: "failed", reason: "CommitFailed", created: false, partitions: 1 },
      ]),
      withPrior,
    );
    expect(rb.restorable).toEqual([{ target: "orders-app", topic: "payments", partition: 2, prior: 6, applied: 7 }]);
    expect(rb.notRestorable.map((r) => r.partition)).toEqual([0, 1]);
    expect(rb.stuck).toEqual(["1e3"]);
    // Negative control: a plan the console did not write yields no partitions to guess from.
    expect(rollbackPlan(sw([{ source: "orders-app", target: "orders-app", outcome: "applied", created: false, partitions: 3 }]), null).notRestorable).toEqual([]);
  });
});

describe("no server-supplied string reaches a command or a plan unguarded", () => {
  it("the rollback text carries no server string in a comment, and none unguarded in a command", () => {
    const bad = "$(curl evil); `id`";
    const doc = planDocument(RESTORE, reviewGroups([line(bad, { topic: "t;rm" }), line("good"), line("nl")]).groups);
    // A plan from the service may still name a group with an escaped line break.
    const bytes = ("bytes" in doc ? doc.bytes : "").replaceAll('"nl"', '"n\\u000al"');
    const parsed = parseOwnPlan(bytes);
    expect(parsed!.groups.map((g) => g.target)).toEqual([bad, "good", "n\nl"]);
    const rb = rollbackPlan(
      {
        name: "swo-\n# injected",
        namespace: "team-a",
        uid: "u",
        approvalName: "a",
        planHash: sha("d"),
        restore: { name: "r\n--force", uid: "u" },
        recordKey: "k\nlogweir switchover run",
        phase: "Succeeded",
        groups: [
          { source: bad, target: bad, outcome: "applied", created: true, partitions: 2 },
          { source: "good", target: "good", outcome: "applied", created: true, partitions: 2 },
          { source: "n\nl", target: "n\nl", outcome: "applied", created: true, partitions: 2 },
          { source: "z", target: "z", outcome: "failed", reason: "CommitFailed", created: false, partitions: 1 },
        ],
      },
      parsed,
    );
    for (const l of rb.text.split("\n")) {
      // Every line is one of the text's own comments, or a command whose names are quoted or placeholders.
      expect(l === "" || l === "#" || (l.startsWith("# ") && !/curl|injected|--force|switchover run|`/.test(l)) || /^kafka-consumer-groups\.sh --bootstrap-server <bootstrap> --group ('[A-Za-z0-9._-]+'|<group>) --topic ('[A-Za-z0-9._-]+:\d+'|<topic:partition>) --delete-offsets$/.test(l)).toBe(true);
    }
    for (const x of ["injected", "--force", "logweir switchover run", "curl", "t;rm", "`id`", "'z'"]) expect(rb.text).not.toContain(x);
    expect(rb.text).toContain("--group <group> --topic <topic:partition>");
    expect(rb.text).toContain("--group <group> --topic 'orders:0'");
    // Negative control: the clean group's command is there, quoted.
    expect(rb.text).toContain("--group 'good' --topic 'orders:0' --delete-offsets");
  });

  it("the plan bytes hold a server string only inside a quoted scalar", () => {
    const inject = 'app": {}, "evil\\ ] # x';
    const doc = planDocument({ ...RESTORE, name: "r\nrestore: {}" }, reviewGroups([line(inject, { topic: "o\nexpected: none" })]).groups);
    const bytes = "bytes" in doc ? doc.bytes : "";
    const q = '"([^"\\\\]|\\\\.)*"';
    const shapes = [
      /^format_version: "1\.0\.0"$/,
      /^restore:$/,
      new RegExp(`^ {2}(name|uid|plan_hash|scorecard_sha256|mapping_sha256): ${q}$`),
      /^groups:$/,
      new RegExp(`^ {2}- source: ${q}$`),
      new RegExp(`^ {4}target: ${q}$`),
      /^ {4}topics:$/,
      new RegExp(`^ {6}${q}:$`),
      /^ {8}offsets: \{\d+: \d+(, \d+: \d+)*\}$/,
      /^ {8}approximate: \[\d+(, \d+)*\]$/,
      /^ {8}expected: none$/,
      /^$/,
    ];
    expect(bytes).not.toBe("");
    for (const l of bytes.split("\n")) expect(shapes.some((r) => r.test(l))).toBe(true);
    const back = parseOwnPlan(bytes)!;
    expect(back.groups.map((g) => g.source)).toEqual([inject]);
    expect(back.restore.name).toBe("r\nrestore: {}");
    expect(back.groups[0]!.partitions.map((p) => p.topic)).toEqual(["o\nexpected: none", "o\nexpected: none"]);
    // Negative controls: a group id with a control character is refused, as the grammar refuses
    // it; and the same strings pasted in raw would break the layout.
    expect("refused" in planDocument(RESTORE, reviewGroups([line("a\nb")]).groups)).toBe(true);
    expect(`      o\nexpected: none:`.split("\n").every((l) => shapes.some((r) => r.test(l)))).toBe(false);
  });
});

describe("names and outcomes", () => {
  it("one plan and attempt give one request key and approval name; a new attempt gives new ones", () => {
    const a = mintNames(sha("e"), "r1-pf-sw1");
    expect(mintNames(sha("e"), "r1-pf-sw1")).toEqual(a);
    expect(mintNames(sha("e"), "r2-pf-sw2").key).not.toBe(a.key);
    expect(mintNames(sha("f"), "r1-pf-sw1").approvalName).not.toBe(a.approvalName);
    expect(a.approvalName).toMatch(/^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$/);
    expect(a.key.length).toBeGreaterThanOrEqual(8);
    expect(a.key.length).toBeLessThanOrEqual(128);
  });
  it("a write the service did not answer, or failed, has an unknown outcome; a refusal does not", () => {
    expect(outcomeUnknown(new TypeError("Failed to fetch"))).toBe(true);
    expect(outcomeUnknown(new ApiError(503, { code: "upstream_timeout" }))).toBe(true);
    expect(outcomeUnknown(new ApiError(422, { code: "validation_failed" }))).toBe(false);
    expect(outcomeUnknown(new ApiError(409, { code: "idempotency_conflict" }))).toBe(false);
  });
});

describe("a review cut at its size bound", () => {
  it("never offers the group the cut may have split; the groups before it are whole", () => {
    const lines = [
      line("first", { topic: "a" }),
      line("split", { topic: "a" }),
      { entry: { truncated: true } } as DetailEntry,
    ];
    const read = reviewGroups(lines);
    expect(read.truncated).toBe(true);
    expect(read.groups.map((g) => [g.source, offered(g)])).toEqual([
      ["first", true],
      ["split", false],
    ]);
    expect(read.groups[1]!.reason).toBe("ReviewTruncated");
    // Control: the same lines with no cut offer both.
    expect(reviewGroups(lines.slice(0, 2)).groups.map(offered)).toEqual([true, true]);
  });
});

describe("FX-36, FX-38: the switchover's words", () => {
  it("MappingDigestMismatch says not held or does not verify, and NoPositionsMapping never claims the restore wrote none", () => {
    expect(REFUSAL_WORDS.MappingDigestMismatch).toBe("The positions mapping is not held by the archive or does not verify against the signed scorecard.");
    expect(REFUSAL_WORDS.NoPositionsMapping).not.toMatch(/wrote no positions mapping/);
    expect(REFUSAL_WORDS.NoPositionsMapping).toMatch(/the archive did not answer/);
  });

  it("a message that says the groups are partial, or the log was not read, makes the groups partial", () => {
    const at = (message?: string) => groupsPartial({ message });
    expect(at("the switchover Job exited 2: PartiallyApplied; 3 group line(s) are not listed here (1 unreadable, 2 past the 64-group bound), so the groups and counts are partial: the signed switchover record names every group")).toBe(true);
    expect(at("the switchover Job exited 0: Succeeded; its log tail was past the 1048576-byte bound and was not read, so which groups were applied is not known here: the signed switchover record names them")).toBe(true);
    // CONTROLS: an ordinary run's message, and none.
    expect(at("the switchover Job exited 0: Succeeded")).toBe(false);
    expect(at(undefined)).toBe(false);
  });
});
