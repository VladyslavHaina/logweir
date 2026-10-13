// For R5's restore wizard and restore page: the target topics' configuration
// (PROD-05.2; decisions/PROD-04.2-cutover.md §4).
//
// * `TopicConfigurationChoice`: the plan's opt-in block, `target.topic_configuration`
//   (`transition: afterVerification | none`, and the requester's `owners: []` statement
//   when the recovery point looked for no owner). `topicConfigurationPlan` gives the
//   value R5's plan renderer writes.
// * `TopicConfigurationPreview`: the restore preflight's per-topic diff (details lines,
//   `check: target.topicConfiguration`): the replication factor and its reason, the
//   settings while restoring and after verification, what is never copied, the target's
//   own verdict, what the transition's retention deletes, and an owned topic's settings
//   as the KafkaTopic YAML the restore exports for its owner.
// * `TopicConfigurationResult`: `Restore.topicConfiguration` on the restore page: each
//   topic before and after, and what the transition did.
import { useState } from "react";
import {
  Alert,
  Checkbox,
  CodeBlock,
  CodeBlockCode,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  ExpandableSection,
  FormGroup,
  Radio,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import { yamlString, type DetailEntry, type TopicConfiguration } from "../../api/switchovers";
import { StatusLabel } from "../../components/Status";
import { exactUtc, msValue } from "../../components/When";
import { useSha256 } from "../../components/hash";
import "./cutover.css";

/** The check id every preview line carries (`check/kinds/restore.rs`). */
export const TOPIC_CONFIGURATION_CHECK = "target.topicConfiguration";

// ------------------------------------------------------------------ the choice

export interface TopicConfigurationValue {
  /** `null`: no block; the topics are created with the target's defaults, as always. */
  transition: "afterVerification" | "none" | null;
  /** The requester states that no topic has a declarative owner (`owners: []`). */
  noOwners: boolean;
}

export const NO_TOPIC_CONFIGURATION: TopicConfigurationValue = { transition: null, noOwners: false };

/** The plan's `target.topic_configuration` value for R5's renderer, or `null` for none. */
export function topicConfigurationPlan(v: TopicConfigurationValue): { transition: "afterVerification" | "none"; owners?: [] } | null {
  if (!v.transition) return null;
  return { transition: v.transition, ...(v.noOwners ? { owners: [] as [] } : {}) };
}

export function TopicConfigurationChoice({
  value,
  onChange,
  target,
  ownersAsked = false,
}: {
  value: TopicConfigurationValue;
  onChange: (v: TopicConfigurationValue) => void;
  target: string;
  /** The preview refused TopicOwnerNotChecked: the point recorded no owner detection. */
  ownersAsked?: boolean;
}) {
  return (
    <FormGroup role="radiogroup" isStack label="Topic configuration" fieldId="topic-configuration">
      <Radio
        id="topic-configuration-source"
        name="topic-configuration"
        label="The source's, from the recovery point"
        description="Partitions, replication factor and settings as the point recorded them. The settings that could delete, compact, reject or re-stamp restored records are held at Logweir's values until the verdict."
        isChecked={value.transition !== null}
        onChange={() => onChange({ ...value, transition: "afterVerification" })}
      />
      {value.transition !== null && (
        <div className="lw-gap-below">
          <Checkbox
            id="topic-configuration-transition"
            isChecked={value.transition === "afterVerification"}
            onChange={(_e, on) => onChange({ ...value, transition: on ? "afterVerification" : "none" })}
            label="Apply the held settings once the restore passes verification"
            description="Off: the topics keep Logweir's settings; the source's are reported, not applied."
          />
          {ownersAsked && (
            <Checkbox
              id="topic-configuration-owners"
              isChecked={value.noOwners}
              onChange={(_e, on) => onChange({ ...value, noOwners: on })}
              label="No topic here is managed by an owner such as a Strimzi KafkaTopic"
              description="The recovery point did not look for owners, so the restore asks you (owners: []). It is signed into the plan."
            />
          )}
        </div>
      )}
      <Radio
        id="topic-configuration-defaults"
        name="topic-configuration"
        label={`${target}'s defaults`}
        description="The target's default replication factor and settings, as a restore always did."
        isChecked={value.transition === null}
        onChange={() => onChange({ transition: null, noOwners: false })}
      />
    </FormGroup>
  );
}

// ----------------------------------------------------------------- the preview

/** One topic's desired state as phase 0 creates it (`TopicDesired`, snake_case on the wire). */
export interface TopicDesired {
  source_topic: string;
  partitions?: number;
  replication_factor: { source?: number; target: number; reason: string };
  during_restore: Record<string, string>;
  at_creation: Record<string, string>;
  deferred: Record<string, string>;
  not_applied: { key: string; value?: string; portability: string; reason: string; target_default?: string }[];
  capped?: Record<string, { source: string; applied: string }>;
  route: string;
  owner?: { kind: string; basis: string; reference: string };
  not_recorded?: boolean;
}

export interface TopicPreview {
  topic: string;
  transition: string;
  targetLine?: string;
  diff: TopicDesired;
  atCreation?: string;
  afterVerification?: string;
  retention?: { retentionMs: number; deletesRecordsBefore: string; deletesEveryRestoredRecord: boolean } | null;
}

/** The validate-only verdict on a set the target accepts. */
const VALIDATED = "TopicCreateValidated";

/** The preview's lines, or the refusal phase 0 would make. A line it cannot read is counted. */
export function topicPreviewLines(entries: DetailEntry[]): { topics: TopicPreview[]; refusal?: string; unread: number } {
  const topics: TopicPreview[] = [];
  let refusal: string | undefined;
  let unread = 0;
  for (const e of entries) {
    const line = e.entry as Record<string, unknown> | null;
    if (!line || typeof line !== "object" || (line.check ?? e.check) !== TOPIC_CONFIGURATION_CHECK) continue;
    if (typeof line.refusal === "string") {
      refusal = line.refusal;
      continue;
    }
    const diff = line.diff as TopicDesired | undefined;
    if (typeof line.topic !== "string" || !diff || typeof diff !== "object" || !diff.replication_factor || typeof diff.route !== "string") {
      unread += 1;
      continue;
    }
    topics.push({
      topic: line.topic,
      transition: String(line.transition ?? ""),
      ...(typeof line.targetLine === "string" ? { targetLine: line.targetLine } : {}),
      diff: { ...diff, during_restore: diff.during_restore ?? {}, at_creation: diff.at_creation ?? {}, deferred: diff.deferred ?? {}, not_applied: diff.not_applied ?? [] },
      ...(typeof line.atCreation === "string" ? { atCreation: line.atCreation } : {}),
      ...(typeof line.afterVerification === "string" ? { afterVerification: line.afterVerification } : {}),
      retention: (line.retentionAfterTransition as TopicPreview["retention"]) ?? null,
    });
  }
  return { topics, ...(refusal ? { refusal } : {}), unread };
}

/** Why the factor is what it is (`replication_factor.reason`). */
export function replicationWords(rf: TopicDesired["replication_factor"]): string {
  if (rf.reason === "sourceFactor") return `${rf.target}, as the source`;
  if (rf.reason === "targetBrokers") return `${rf.target}: the source's ${rf.source ?? "factor"}, bounded by the target's ${rf.target} broker${rf.target === 1 ? "" : "s"}`;
  if (rf.reason === "notRecorded") return `${rf.target}: the source's is not recorded, so the plan's default`;
  return `${rf.target} (${rf.reason})`;
}

/** A Kubernetes object name for a topic, as `kubernetes_name` derives it; `digest` is the
 *  topic name's sha256, needed only when the name had to change. */
export function kubernetesName(topic: string, digest: string): string {
  const cleaned = topic
    .toLowerCase()
    .replace(/[^a-z0-9.-]/g, "-")
    .replace(/^[-.]+|[-.]+$/g, "");
  if (cleaned === topic && cleaned) return cleaned;
  const short = digest.replace(/^sha256:/, "").slice(0, 8) || "0";
  const stem = cleaned.slice(0, 240);
  return stem ? `${stem}-${short}` : `topic-${short}`;
}

/** The owned topic's exported settings, in the shape of the restore's desired-state document. */
export function desiredStateYaml(target: string, d: TopicDesired, digest: string): string {
  const config = Object.entries(d.deferred).sort(([a], [b]) => a.localeCompare(b));
  const configBlock = (indent: string) =>
    config.length ? [`${indent}config:`, ...config.map(([k, v]) => `${indent}  ${yamlString(k)}: ${yamlString(v)}`)] : [`${indent}config: {}`];
  if (d.owner?.kind === "strimzi") {
    const ns = d.owner.reference.includes("/") ? d.owner.reference.split("/")[0] : undefined;
    return [
      "apiVersion: kafka.strimzi.io/v1beta2",
      "kind: KafkaTopic",
      "metadata:",
      `  name: ${yamlString(kubernetesName(target, digest))}`,
      ...(ns ? [`  namespace: ${yamlString(ns)}`] : []),
      "  annotations:",
      `    logweir.dev/restored-from: ${yamlString(d.source_topic)}`,
      `    logweir.dev/source-owner: ${yamlString(d.owner.reference)}`,
      "spec:",
      `  topicName: ${yamlString(target)}`,
      ...(d.partitions !== undefined ? [`  partitions: ${d.partitions}`] : []),
      `  replicas: ${d.replication_factor.target}`,
      ...configBlock("  "),
      "",
    ].join("\n");
  }
  return [
    "apiVersion: logweir.dev/desired-state/v1",
    "kind: TopicDesiredState",
    `topic: ${yamlString(target)}`,
    `restoredFrom: ${yamlString(d.source_topic)}`,
    ...(d.owner ? ["owner:", `  kind: ${yamlString(d.owner.kind)}`, `  reference: ${yamlString(d.owner.reference)}`] : []),
    ...(d.partitions !== undefined ? [`partitions: ${d.partitions}`] : []),
    `replicationFactor: ${d.replication_factor.target}`,
    ...configBlock(""),
    "",
  ].join("\n");
}

function Keys({ values, empty = "None" }: { values: Record<string, string>; empty?: string }) {
  const entries = Object.entries(values).sort(([a], [b]) => a.localeCompare(b));
  if (entries.length === 0) return <span className="lw-subtle">{empty}</span>;
  return (
    <ul className="lw-keys">
      {entries.map(([k, v]) => (
        <li key={k}>
          {k}={msValue(k, v)}
        </li>
      ))}
    </ul>
  );
}

function OwnedYaml({ topic, d }: { topic: string; d: TopicDesired }) {
  const [open, setOpen] = useState(false);
  const digest = useSha256(topic);
  return (
    <ExpandableSection toggleText={open ? "Hide the exported YAML" : `Show the ${d.owner?.kind === "strimzi" ? "KafkaTopic" : "desired-state"} YAML`} isExpanded={open} onToggle={(_e, o) => setOpen(o)}>
      <CodeBlock className="lw-yaml">
        <CodeBlockCode>{desiredStateYaml(topic, d, digest.startsWith("sha256:") ? digest : "")}</CodeBlockCode>
      </CodeBlock>
      <Content component="p" className="lw-subtle lw-small">
        A preview. The restore writes the document to its evidence (desiredStateKey), bound by digest in the signed scorecard; apply that one
        with the topic's owner.
      </Content>
    </ExpandableSection>
  );
}

function TopicBlock({ t }: { t: TopicPreview }) {
  const d = t.diff;
  const owned = d.route === "desiredStateExport";
  const transition = t.transition === "afterVerification";
  const during = { ...d.during_restore, ...d.at_creation };
  const after = { ...d.at_creation, ...d.deferred };
  return (
    <li>
      <div className="lw-topic-config__head">
        <strong className="lw-break">{t.topic}</strong>
        <span className="lw-subtle lw-small">
          {d.source_topic !== t.topic ? `from ${d.source_topic} · ` : ""}
          {d.partitions !== undefined ? `${d.partitions} ${d.partitions === 1 ? "partition" : "partitions"} · ` : ""}replication {replicationWords(d.replication_factor)}
        </span>
        {owned && <StatusLabel tone="info" text={d.owner?.kind === "strimzi" ? "Owned by Strimzi" : "Owned elsewhere"} title={d.owner?.reference} />}
        {d.not_recorded && <StatusLabel tone="warning" text="Not recorded" title="the backup could not read this topic's configuration" />}
      </div>
      {d.not_recorded && (
        <Alert isInline isPlain variant="warning" title="The backup did not record this topic's configuration">
          It is created with Logweir's settings only, and its configuration is NOT RECORDED.
        </Alert>
      )}
      <DescriptionList isCompact className="lw-dl" orientation={{ sm: "vertical", md: "horizontal" }}>
        <DescriptionListGroup>
          <DescriptionListTerm>While restoring</DescriptionListTerm>
          <DescriptionListDescription>
            <Keys values={during} />
            <div className="lw-subtle lw-small">Logweir's settings, then the source's that are safe at creation.</div>
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>{owned ? "Exported for its owner" : transition ? "After verification" : "Held, not applied"}</DescriptionListTerm>
          <DescriptionListDescription>
            <Keys values={owned ? d.deferred : after} />
            <div className="lw-subtle lw-small">
              {owned
                ? `${d.owner?.reference ?? "Its owner"} manages this topic: these settings are written for it, never applied through Kafka.`
                : transition
                  ? "One AlterConfigs with the whole set, once the signed verdict is a pass; Logweir's settings go with it."
                  : "The topic keeps Logweir's settings; these are reported only."}
            </div>
            {owned && <OwnedYaml topic={t.topic} d={d} />}
          </DescriptionListDescription>
        </DescriptionListGroup>
        {d.capped && Object.keys(d.capped).length > 0 && (
          <DescriptionListGroup>
            <DescriptionListTerm>Changed</DescriptionListTerm>
            <DescriptionListDescription>
              <ul className="lw-keys">
                {Object.entries(d.capped).map(([k, c]) => (
                  <li key={k}>
                    {k}: the source's {c.source}, applied as {c.applied} (capped at the replication factor)
                  </li>
                ))}
              </ul>
            </DescriptionListDescription>
          </DescriptionListGroup>
        )}
        {d.not_applied.length > 0 && (
          <DescriptionListGroup>
            <DescriptionListTerm>Never copied</DescriptionListTerm>
            <DescriptionListDescription>
              <ul className="lw-keys">
                {d.not_applied.map((n) => (
                  <li key={n.key}>
                    {n.key}
                    {n.value !== undefined ? `=${msValue(n.key, n.value)}` : ""} <span className="lw-subtle">({n.reason})</span>
                    {n.target_default !== undefined && <span className="lw-subtle"> · the target's default: {msValue(n.key, n.target_default)}</span>}
                  </li>
                ))}
              </ul>
            </DescriptionListDescription>
          </DescriptionListGroup>
        )}
      </DescriptionList>
      {t.atCreation && t.atCreation !== VALIDATED && (
        <Alert isInline isPlain variant="danger" title={`The target refuses the creation set (${t.atCreation})`}>
          The restore is refused before any topic is created (TopicConfigRefusedByTarget).
        </Alert>
      )}
      {transition && !owned && t.afterVerification && t.afterVerification !== VALIDATED && (
        <Alert isInline isPlain variant="warning" title={`The target refuses the set applied after verification (${t.afterVerification})`}>
          The transition would fail for this topic; its whole desired set is then left for you to apply.
        </Alert>
      )}
      {transition && t.retention && (
        <Alert
          isInline
          isPlain
          variant={t.retention.deletesEveryRestoredRecord ? "danger" : "info"}
          title={
            t.retention.deletesEveryRestoredRecord
              ? `retention.ms ${msValue("retention.ms", String(t.retention.retentionMs))} deletes every restored record`
              : `retention.ms ${msValue("retention.ms", String(t.retention.retentionMs))} deletes restored records older than ${exactUtc(t.retention.deletesRecordsBefore)}`
          }
        >
          At the broker's next retention check after the transition.
        </Alert>
      )}
    </li>
  );
}

/** The restore preflight's configuration diff, one block per target topic. */
export function TopicConfigurationPreview({ entries }: { entries: DetailEntry[] }) {
  const { topics, refusal, unread } = topicPreviewLines(entries);
  if (refusal) {
    return (
      <Alert isInline variant="danger" title="The restore would be refused before creating anything">
        {refusal}
      </Alert>
    );
  }
  if (topics.length === 0) {
    return <Content component="p" className="lw-subtle">The readiness check returned no configuration preview{unread ? ` (${unread} line(s) could not be read)` : ""}.</Content>;
  }
  return (
    <>
      <ul className="lw-topic-config" aria-label="Topic configuration preview">
        {topics.map((t) => (
          <TopicBlock key={t.topic} t={t} />
        ))}
      </ul>
      {unread > 0 && <Content component="p" className="lw-subtle lw-small">{unread} preview line(s) could not be read.</Content>}
    </>
  );
}

// ---------------------------------------------------------------- the outcome

const RF_WORDS: Record<string, string> = { sourceFactor: "as the source", targetBrokers: "bounded by the target's brokers", notRecorded: "the source's not recorded" };

/** `Restore.topicConfiguration`: each topic before and after, and what the transition did. */
export function TopicConfigurationResult({ tc }: { tc: TopicConfiguration }) {
  const outcome = tc.transition ?? "";
  return (
    <>
      <Content component="p" className={outcome.startsWith("notRun") || outcome === "partial" ? undefined : "lw-subtle"}>
        {outcome === "applied"
          ? "Created with the source's settings; the settings held during the restore were set to the source's once it passed verification."
          : outcome === "partial"
            ? `The transition applied to ${tc.applied.length} topic(s) and failed for ${tc.failed.length}. Logweir does not retry: apply each failed topic's whole set below.`
            : outcome.startsWith("notRun")
              ? `The transition did not run (${outcome.replace(/^notRun:\s*/, "")}). The topics keep the settings they were created with.`
              : "Created with the source's settings."}{" "}
        <span className="lw-subtle lw-small">Read off the runner's log; the signed scorecard is the evidence.</span>
      </Content>
      <Table aria-label="Topic configuration before and after verification" variant="compact" gridBreakPoint="grid-md" className="lw-kv-table">
        <Thead>
          <Tr>
            <Th width={20}>Topic</Th>
            <Th width={15}>Outcome</Th>
            <Th width={15}>Replication</Th>
            <Th width={25}>While restoring</Th>
            <Th width={25}>Now</Th>
          </Tr>
        </Thead>
        <Tbody>
          {tc.topics.map((t) => {
            const failed = tc.failed.find((f) => f.topic === t.topic);
            const exported = tc.exported.includes(t.topic);
            const applied = tc.applied.includes(t.topic);
            return (
              <Tr key={t.topic}>
                <Td dataLabel="Topic" className="lw-break">
                  {t.topic}
                </Td>
                <Td dataLabel="Outcome">
                  {failed ? (
                    <StatusLabel tone="danger" text="Not applied" title={failed.error} />
                  ) : exported ? (
                    <StatusLabel tone="info" text="Exported" title="desiredStateExport" />
                  ) : applied ? (
                    <StatusLabel tone="success" text="Applied" />
                  ) : (
                    <StatusLabel tone="neutral" text="Unchanged" />
                  )}
                  {failed && <div className="lw-reason">{failed.error}</div>}
                </Td>
                <Td dataLabel="Replication">
                  {t.replicationFactor ?? <span className="lw-subtle">Not recorded</span>}
                  {t.replicationFactorReason && (
                    <div className="lw-subtle lw-small">
                      {RF_WORDS[t.replicationFactorReason] ?? t.replicationFactorReason}
                      {t.sourceReplicationFactor != null && t.sourceReplicationFactor !== t.replicationFactor ? ` (source ${t.sourceReplicationFactor})` : ""}
                    </div>
                  )}
                </Td>
                <Td dataLabel="While restoring">
                  <Keys values={t.before} />
                </Td>
                <Td dataLabel="Now">
                  <Keys values={failed ? t.before : t.after} />
                  {failed && (
                    <div className="lw-small">
                      To apply: <Keys values={failed.desired} />
                    </div>
                  )}
                </Td>
              </Tr>
            );
          })}
        </Tbody>
      </Table>
      {tc.desiredStateKey && (
        <Content component="p" className="lw-small lw-gap-top">
          Owned topics' settings are exported to <span className="lw-mono lw-break">{tc.desiredStateKey}</span> in the evidence store, for their
          owner to apply.
        </Content>
      )}
    </>
  );
}
