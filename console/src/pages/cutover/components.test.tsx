// @vitest-environment jsdom
// The theme A components R5's and R6's pages take from this folder: what the approver is
// shown for a switchover, the topic-configuration preview and outcome, and the clone's
// lifetime field and status. Each row has the input that must NOT pass beside it.
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, configure, fireEvent, render, screen, within } from "@testing-library/react";
import { useState } from "react";
import { SwitchoverApprovalSummary } from "./SwitchoverApprovalSummary";
import { TopicConfigurationChoice, TopicConfigurationPreview, TopicConfigurationResult, desiredStateYaml, kubernetesName, topicConfigurationPlan, type TopicConfigurationValue, type TopicDesired } from "./TopicConfiguration";
import { CloneLifetime, CloneStatus, cloneListWords, cloneTeardown, cloneTtlError } from "./CloneLifetime";
import type { DetailEntry, Switchover, SwitchoverApprovalRequest } from "../../api/switchovers";
import { OutcomeCounts, OutcomeTable } from "./parts";

configure({ asyncUtilTimeout: 5000 });
afterEach(cleanup);

const sha = (c: string) => `sha256:${c.repeat(64)}`;

function request(over: Partial<SwitchoverApprovalRequest> = {}): SwitchoverApprovalRequest {
  return {
    switchover: "swo-1",
    switchoverUid: "s-uid",
    namespace: "team-b",
    approvalName: "swo-approval-x",
    confirmationName: "swo-approval-x-confirmation",
    confirmationSha256: sha("e"),
    policy: "prod-pair",
    policyDigest: sha("f"),
    planHash: sha("d"),
    state: "pending",
    stateSentence: "Waiting for a second person to approve.",
    approve: { offered: true, sentence: "Approving lets the controller run this switchover." },
    scopeComplete: true,
    scope: {
      restore: { name: "orders-recovery", uid: "r-uid", planHash: sha("a"), scorecardSha256: sha("b"), mappingSha256: sha("c") },
      groupsCount: 2,
      positionsCount: 3,
      groups: [
        { source: "app", target: "app", topics: [{ topic: "r-orders", positions: [{ partition: 0, offset: 1200, approximate: false }, { partition: 1, offset: 50, approximate: true }] }] },
        { source: "legacy", target: "legacy-v2", topics: [{ topic: "r-payments", positions: [{ partition: 3, offset: 7, approximate: false, expected: 5 }] }] },
      ],
    },
    ...over,
  };
}

describe("SwitchoverApprovalSummary (R6's approval page)", () => {
  it("shows every group, topic and position the approver signs for, and what each must hold first", () => {
    render(<SwitchoverApprovalSummary request={request()} />);
    expect(screen.getByText("app")).toBeTruthy();
    expect(screen.getByText("legacy → legacy-v2")).toBeTruthy();
    const app = within(screen.getByRole("grid", { name: "Positions of app" }));
    expect(app.getByText("r-orders / 0")).toBeTruthy();
    expect(app.getByText("1,200")).toBeTruthy();
    expect(app.getByText("Approximate")).toBeTruthy();
    const legacy = within(screen.getByRole("grid", { name: "Positions of legacy-v2" }));
    expect(legacy.getByText("5")).toBeTruthy();
    expect(screen.getAllByRole("row")).toHaveLength(2 + 3); // two header rows, three positions
    expect(screen.getByLabelText("positions mapping sha256")).toBeTruthy();
  });

  it("a request the service cannot show in full is a refusal, never a partial list", () => {
    render(<SwitchoverApprovalSummary request={request({ scopeComplete: false, scope: undefined, scopeIncomplete: "tooManyPositions" })} />);
    expect(screen.getByText("This request cannot be shown in full, so it is not approved here")).toBeTruthy();
    expect(screen.getByText("tooManyPositions")).toBeTruthy();
    // Negative control: no position table is drawn.
    expect(screen.queryByRole("grid")).toBeNull();
  });
});

const OWNED: TopicDesired = {
  source_topic: "payments",
  partitions: 6,
  replication_factor: { source: 3, target: 1, reason: "targetBrokers" },
  during_restore: { "cleanup.policy": "delete", "retention.ms": "-1" },
  at_creation: { "min.insync.replicas": "1" },
  deferred: { "cleanup.policy": "compact" },
  not_applied: [{ key: "leader.replication.throttled.replicas", value: "*", portability: "clusterBound", reason: "clusterBound" }],
  capped: { "min.insync.replicas": { source: "2", applied: "1" } },
  route: "desiredStateExport",
  owner: { kind: "strimzi", basis: "kafkaTopicResource", reference: "kafka/payments" },
};

const ADMIN: TopicDesired = {
  source_topic: "orders",
  partitions: 3,
  replication_factor: { source: 3, target: 3, reason: "sourceFactor" },
  during_restore: { "cleanup.policy": "delete", "retention.ms": "-1" },
  at_creation: { "max.message.bytes": "2097152" },
  deferred: { "retention.ms": "604800000" },
  not_applied: [{ key: "ssl.password", portability: "secret", reason: "secret" }],
  route: "adminApi",
};

const lines = (over: Record<string, unknown> = {}): DetailEntry[] => [
  { entry: { check: "target.topicConfiguration", topic: "r-orders", transition: "afterVerification", targetLine: "4.x", diff: ADMIN, atCreation: "TopicCreateValidated", afterVerification: "TopicCreateValidated", retentionAfterTransition: { retentionMs: 604800000, deletesRecordsBefore: "2026-10-06T12:00:00Z", deletesEveryRestoredRecord: true }, ...over } },
  { entry: { check: "target.topicConfiguration", topic: "r-payments", transition: "afterVerification", diff: OWNED, atCreation: "TopicCreateValidated" } },
  { entry: { check: "target.mappedTopics", topic: "ignored" } },
];

describe("TopicConfigurationPreview (R5's wizard)", () => {
  it("shows the factor with its reason, both sets, what is never copied, and an owned topic's export", () => {
    render(<TopicConfigurationPreview entries={lines()} />);
    expect(screen.getByText(/replication 3, as the source/)).toBeTruthy();
    expect(screen.getByText(/replication 1: the source's 3, bounded by the target's 1 broker$/)).toBeTruthy();
    expect(screen.getByText("Owned by Strimzi")).toBeTruthy();
    expect(screen.getByText("Exported for its owner")).toBeTruthy();
    expect(screen.getByText(/min.insync.replicas: the source's 2, applied as 1/)).toBeTruthy();
    expect(screen.getByText("retention.ms=604800000 (7 days)")).toBeTruthy();
    // A secret is never shown with a value.
    expect(screen.getByText(/ssl\.password/).textContent).not.toContain("=");
    expect(screen.getByText(/deletes every restored record/)).toBeTruthy();
    expect(screen.queryByText("ignored")).toBeNull();
  });

  it("names the target's refusal, and a refusal line replaces every topic", () => {
    render(<TopicConfigurationPreview entries={lines({ atCreation: "TopicConfigInvalid" })} />);
    expect(screen.getByText("The target refuses the creation set (TopicConfigInvalid)")).toBeTruthy();
    cleanup();
    render(<TopicConfigurationPreview entries={[{ entry: { check: "target.topicConfiguration", refusal: "TopicOwnerNotChecked: the point recorded no owner detection" } }]} />);
    expect(screen.getByText("The restore would be refused before creating anything")).toBeTruthy();
    expect(screen.queryByRole("list", { name: "Topic configuration preview" })).toBeNull();
  });

  it("exports an owned topic as the KafkaTopic the restore writes", () => {
    const yaml = desiredStateYaml("r-payments", OWNED, sha("1"));
    expect(yaml).toContain("kind: KafkaTopic");
    expect(yaml).toContain('  namespace: "kafka"');
    expect(yaml).toContain('  topicName: "r-payments"');
    expect(yaml).toContain("  replicas: 1");
    expect(yaml).toContain('    "cleanup.policy": "compact"');
    // Negative control: a name that is not a Kubernetes name gets the digest suffix.
    expect(kubernetesName("r-payments", sha("1"))).toBe("r-payments");
    expect(kubernetesName("Orders_EU", sha("1"))).toBe("orders-eu-11111111");
  });
});

describe("TopicConfigurationChoice (R5's wizard)", () => {
  function Harness({ asked }: { asked?: boolean }) {
    const [v, setV] = useState<TopicConfigurationValue>({ transition: null, noOwners: false });
    return (
      <>
        <TopicConfigurationChoice value={v} onChange={setV} target="orders-dr" ownersAsked={asked} />
        <output data-testid="plan">{JSON.stringify(topicConfigurationPlan(v))}</output>
      </>
    );
  }
  it("writes the plan's block only when chosen, and owners: [] only when stated", () => {
    render(<Harness asked />);
    expect(screen.getByTestId("plan").textContent).toBe("null");
    fireEvent.click(screen.getByLabelText("The source's, from the recovery point"));
    expect(screen.getByTestId("plan").textContent).toBe('{"transition":"afterVerification"}');
    fireEvent.click(screen.getByLabelText(/Apply the held settings/));
    expect(screen.getByTestId("plan").textContent).toBe('{"transition":"none"}');
    fireEvent.click(screen.getByLabelText(/No topic here is managed by an owner/));
    expect(screen.getByTestId("plan").textContent).toBe('{"transition":"none","owners":[]}');
  });
});

describe("TopicConfigurationResult (R5's restore page)", () => {
  it("shows each topic before and after, and a failed topic's whole set to apply", () => {
    render(
      <TopicConfigurationResult
        tc={{
          transition: "partial",
          applied: ["r-orders"],
          exported: [],
          failed: [{ topic: "r-dlq", error: "TopicAuthorizationFailed", desired: { "retention.ms": "2592000000" } }],
          topics: [
            { topic: "r-orders", before: { "retention.ms": "-1" }, after: { "retention.ms": "604800000" }, replicationFactor: 3, replicationFactorReason: "sourceFactor" },
            { topic: "r-dlq", before: { "retention.ms": "-1" }, after: { "retention.ms": "2592000000" }, replicationFactor: 1, replicationFactorReason: "targetBrokers", sourceReplicationFactor: 3 },
          ],
        }}
      />,
    );
    expect(screen.getByText(/failed for 1. Logweir does not retry/)).toBeTruthy();
    expect(screen.getByText("Not applied")).toBeTruthy();
    expect(screen.getByText("TopicAuthorizationFailed")).toBeTruthy();
    expect(screen.getByText(/bounded by the target's brokers \(source 3\)/)).toBeTruthy();
    // The failed topic still reads its old set as current, never the desired one.
    const row = screen.getByText("r-dlq").closest("tr")!;
    expect(within(row).getAllByText("retention.ms=-1")).toHaveLength(2);
  });
});

describe("CloneLifetime and CloneStatus (R5's wizard and restore page)", () => {
  it("holds the lifetime to 5 minutes and 30 days, and writes the plan's teardown map", () => {
    expect(cloneTtlError(300)).toBeNull();
    expect(cloneTtlError(2_592_000)).toBeNull();
    expect(cloneTtlError(299)).toMatch(/CloneTtlOutOfRange/);
    expect(cloneTtlError(2_592_001)).toMatch(/CloneTtlOutOfRange/);
    expect(cloneTeardown(86_400)).toEqual({ ttl_seconds: 86_400 });
    expect(cloneTeardown(null)).toBeNull();
  });

  it("offers a clone only where it may exist, and keeps the choice", () => {
    function Harness({ disabled }: { disabled?: string }) {
      const [v, setV] = useState<number | null>(null);
      return (
        <>
          <CloneLifetime value={v} onChange={setV} target="orders-dr" disabled={disabled} />
          <output data-testid="ttl">{String(v)}</output>
        </>
      );
    }
    render(<Harness />);
    fireEvent.click(screen.getByLabelText("Keep them as a clone for a while"));
    expect(screen.getByTestId("ttl").textContent).toBe("86400");
    fireEvent.change(screen.getByLabelText("Delete the clone after"), { target: { value: "3600" } });
    expect(screen.getByTestId("ttl").textContent).toBe("3600");
    expect(screen.getByText(/A clone cannot be switched over/)).toBeTruthy();
    cleanup();
    render(<Harness disabled="A restore under the original names is not a clone." />);
    expect((screen.getByLabelText("Keep them as a clone for a while") as HTMLInputElement).disabled).toBe(true);
  });

  it("says when the clone goes, and a failed removal with the steps to finish it", () => {
    const expires = new Date(Date.now() + 23 * 3600_000).toISOString();
    expect(cloneListWords({ phase: "Active", expiresAt: expires, ttlSeconds: 86400 })).toBe("clone, deletes in 23 hours");
    expect(cloneListWords({ phase: "RemovalFailed" })).toBe("clone, removal failed");
    render(<CloneStatus clone={{ phase: "RemovalFailed", attempts: 3, reason: "CloneTargetClusterChanged", ttlSeconds: 86400, expiresAt: expires }} teardown={{ deletedTruncated: false, deleted: [], failed: [{ topic: "clone-orders", error: "CloneTargetReplaced" }] }} restoreName="orders-clone" />);
    expect(screen.getByText("Removal failed")).toBeTruthy();
    expect(screen.getByText("CloneTargetClusterChanged")).toBeTruthy();
    expect(screen.getByText("CloneTargetReplaced")).toBeTruthy();
    expect(screen.getByText(/patch restore 'orders-clone' --type=json/)).toBeTruthy();
    cleanup();
    // A name that is not a Kubernetes name never reaches the command.
    render(<CloneStatus clone={{ phase: "RemovalFailed", attempts: 3 }} restoreName={"x; kubectl delete ns prod\n"} />);
    expect(screen.getByText(/patch restore <restore> --type=json/)).toBeTruthy();
    expect(screen.queryByText(/delete ns prod/)).toBeNull();
    cleanup();
    // Negative control: a clone that is kept does not tell anyone to patch a finalizer.
    render(<CloneStatus clone={{ phase: "Active", expiresAt: expires, ttlSeconds: 86400, attempts: 0 }} restoreName="orders-clone" />);
    expect(screen.queryByText(/patch restore/)).toBeNull();
    expect(screen.getByText(/Deletes in 23 hours/)).toBeTruthy();
  });
});

describe("FX-38: a switchover whose groups are partial, and a teardown cut at 256", () => {
  const sw = (over: Partial<Switchover>): Switchover => ({
    name: "swo-1", namespace: "team-a", uid: "u", approvalName: "a", planHash: sha("d"), restore: { name: "r", uid: "u" }, phase: "Succeeded",
    groups: [{ source: "app", target: "app", outcome: "applied", created: false, partitions: 2 }],
    ...over,
  });

  it("counts over a partial list are not known, never 0", () => {
    render(<OutcomeCounts sw={sw({ message: "the switchover Job exited 2: PartiallyApplied; 1 group line(s) are not listed here (1 unreadable, 0 past the 64-group bound), so the groups and counts are partial: the signed switchover record names every group" })} />);
    expect(screen.getByText(/Counts not known here/)).toBeTruthy();
    expect(screen.queryByText(/0 refused/)).toBeNull();
    cleanup();
    // CONTROL: a whole list is counted.
    render(<OutcomeCounts sw={sw({ message: "the switchover Job exited 0: Succeeded" })} />);
    expect(screen.getByText(/1 applied · 0 already applied · 0 refused/)).toBeTruthy();
  });

  it("a finished run whose log was not read names no group's outcome", () => {
    const unread = sw({ groups: [], message: "the switchover Job exited 0: Succeeded; its log tail was past the 1048576-byte bound and was not read" });
    const plan = { groups: [{ source: "app", target: "app", partitions: [{ topic: "r-orders", partition: 0, offset: 1 }] }] } as never;
    render(<OutcomeTable sw={unread} plan={plan} target="orders-dr" />);
    expect(screen.getAllByText(/Not known here: the signed switchover record names its outcome/).length).toBeGreaterThan(0);
    expect(screen.queryByText(/Waiting for the run's result/)).toBeNull();
  });

  it("the clone's teardown says how many topics were left beyond the 256 it lists", () => {
    const failed = Array.from({ length: 256 }, (_, i) => ({ topic: `clone-${i}`, error: "CloneTargetReplaced" }));
    render(<CloneStatus clone={{ phase: "RemovalFailed", attempts: 3 }} teardown={{ deletedTruncated: false, deleted: [], failed, failedCount: 300 }} restoreName="orders-clone" />);
    expect(screen.getByText("300 topics were left; the first 256 are listed")).toBeTruthy();
    cleanup();
    // CONTROL: a whole list adds no count.
    render(<CloneStatus clone={{ phase: "RemovalFailed", attempts: 3 }} teardown={{ deletedTruncated: false, deleted: [], failed: failed.slice(0, 3), failedCount: 3 }} restoreName="orders-clone" />);
    expect(screen.queryByText(/topics were left; the first/)).toBeNull();
  });
});
