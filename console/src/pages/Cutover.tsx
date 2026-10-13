// Theme A's cutover (PROD-04.2): review how each consumer group's captured position
// maps onto the restored topics and what the target group looks like now (the
// Switchover preflight), then create a Switchover for the groups that are ready.
// Mock data in the shapes of decisions/PROD-04.2-cutover.md (src/mock/themeA.ts).
import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import {
  Alert,
  Breadcrumb,
  BreadcrumbItem,
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Checkbox,
  CodeBlock,
  CodeBlockCode,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  ExpandableSection,
  Flex,
  FlexItem,
  PageSection,
  ProgressStep,
  ProgressStepper,
} from "@patternfly/react-core";
import { ExpandableRowContent, Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import ExchangeAltIcon from "@patternfly/react-icons/dist/esm/icons/exchange-alt-icon";
import { useRestore } from "../api/queries";
import type { Restore } from "../api/types";
import {
  MAPPING_RULE,
  UNAVAILABLE_WORDS,
  planYaml,
  reviewFor,
  switchoverStatus,
  themeAOf,
  type MappingEntry,
  type ReviewGroup,
} from "../mock/themeA";
import { PageHeader } from "../components/PageHeader";
import { MappingStatus, OutcomeStatus, PhaseStatus, VerdictStatus } from "../components/Status";
import { Empty, ErrorState, LoadingRows } from "../components/States";
import { count } from "../components/When";
import { Ident } from "../components/Ident";
import { useSha256 } from "../components/hash";
import { CompactList, useNarrow } from "../components/CompactList";

type Step = "review" | "confirm" | "done";

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

function classCounts(entries: MappingEntry[]) {
  const n = (c: MappingEntry["class"]) => entries.filter((e) => e.class === c).length;
  return { exact: n("exact"), approximate: n("approximate"), unavailable: n("unavailable"), skipped: entries.reduce((s, e) => s + (e.skipped ?? 0), 0) };
}

/** "3 exact · 1 approximate" with the class names, for a group or a plan. */
function Positions({ entries, notCaptured }: { entries: MappingEntry[]; notCaptured?: string }) {
  if (notCaptured) return <span className="lw-subtle">No position was captured</span>;
  const c = classCounts(entries);
  const parts = [
    c.exact > 0 && `${c.exact} exact`,
    c.approximate > 0 && `${c.approximate} approximate`,
    c.unavailable > 0 && `${c.unavailable} unavailable`,
  ].filter(Boolean);
  return <span>{parts.join(" · ")}</span>;
}

/** The target group now, from the Switchover preflight. */
function targetText(g: ReviewGroup): string {
  const s = g.state;
  if (g.notCaptured || !s) return "Not checked";
  if (!s.visible) return "Hidden from Logweir's principal";
  if (!s.exists) return g.verdict === "ready" ? "Absent: created by the switchover" : "Absent";
  if (s.members > 0) return `${plural(s.members, "member")} connected (${s.state})`;
  if (Object.keys(s.committed).length) return g.verdict === "alreadyApplied" ? "Stopped; positions equal the plan" : "Stopped; positions differ from the plan";
  return "Stopped; no positions yet";
}

function why(e: MappingEntry): string {
  if (e.class === "exact") return "First restored record at or after the position";
  if (e.class === "approximate") return `${plural(e.skipped ?? 0, "unread record")} left out by the restore's end time`;
  const name = e.reason ?? "";
  return UNAVAILABLE_WORDS[name.split(":")[0]!] ?? name;
}

function Legend() {
  const [open, setOpen] = useState(false);
  return (
    <ExpandableSection toggleText="How positions are translated" isExpanded={open} onToggle={(_e, o) => setOpen(o)}>
      <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "14ch" }}>
        <DescriptionListGroup>
          <DescriptionListTerm>
            <MappingStatus cls="exact" />
          </DescriptionListTerm>
          <DescriptionListDescription>
            The group resumes at the first restored record whose original offset (its last x-original-offset header) is
            at or after the captured position. Nothing it had not read is skipped. Rule: <span className="lw-mono">{MAPPING_RULE}</span>.
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>
            <MappingStatus cls="approximate" />
          </DescriptionListTerm>
          <DescriptionListDescription>
            The same rule, but the restore's own selection (its start or end time) left out records the group had not read
            yet. The count is stated. Verification still passed.
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>
            <MappingStatus cls="unavailable" />
          </DescriptionListTerm>
          <DescriptionListDescription>
            No restored record corresponds to the position, for a named reason (for example BeforeArchive, ArchiveGap or
            PartitionNotRestored). Logweir never guesses: a group with such a partition is left out.
          </DescriptionListDescription>
        </DescriptionListGroup>
      </DescriptionList>
    </ExpandableSection>
  );
}

/** One group's partitions, on an inset panel; the same column widths in every group. */
function Partitions({ g }: { g: ReviewGroup }) {
  if (g.notCaptured) {
    return (
      <div className="lw-inset">
        <Content component="p" className="lw-inset__note">
          The backup did not capture this group (<span className="lw-mono">{g.notCaptured}</span>): streams groups are not
          captured, so there is no position to translate. Reset it by hand if its application moves to the target.
        </Content>
      </div>
    );
  }
  return (
    <div className="lw-inset">
      <Table aria-label={`Partitions of ${g.source}`} variant="compact" gridBreakPoint="grid-md">
        <Thead>
          <Tr>
            <Th width={25}>Partition</Th>
            <Th width={15} className="lw-right" modifier="nowrap">
              On the source
            </Th>
            <Th width={15} className="lw-right" modifier="nowrap">
              Resumes at
            </Th>
            <Th width={15}>Translation</Th>
            <Th width={30}>Why</Th>
          </Tr>
        </Thead>
        <Tbody>
          {g.entries.map((e) => (
            <Tr key={`${e.topic}-${e.partition}`}>
              <Td dataLabel="Partition" modifier="nowrap">
                {e.target_topic} / {e.partition}
              </Td>
              <Td dataLabel="On the source" className="lw-num lw-right">
                {count(e.position)}
              </Td>
              <Td dataLabel="Resumes at" className="lw-num lw-right">
                {e.target_offset === undefined ? "None" : count(e.target_offset)}
              </Td>
              <Td dataLabel="Translation">
                <MappingStatus cls={e.class} title={e.reason} />
              </Td>
              <Td dataLabel="Why">
                <span className="lw-subtle">{why(e)}</span>
                {e.reason && <div className="lw-reason">{e.reason}</div>}
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
      {g.activeAtCapture && (
        <Content component="p" className="lw-subtle lw-small lw-inset__note">
          This group was active when its positions were captured, so a few records it had already read may be read again.
          None is skipped.
        </Content>
      )}
    </div>
  );
}

function selectable(g: ReviewGroup) {
  return g.verdict === "ready" || g.verdict === "alreadyApplied";
}

function toggled(set: Set<string>, key: string): Set<string> {
  const next = new Set(set);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

function ReviewTable({ groups, selected, setSelected, target }: { groups: ReviewGroup[]; selected: Set<string>; setSelected: (s: Set<string>) => void; target: string }) {
  // Wide screens open a ready group and a blocked one; phones list the groups instead.
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set(["orders-billing", "orders-fraud"]));
  return (
    <Table aria-label="Consumer groups and their translated positions" isExpandable>
      <Thead>
        <Tr>
          <Th screenReaderText="Show partitions" />
          <Th screenReaderText="Include" />
          <Th width={25}>Group</Th>
          <Th width={25}>Verdict</Th>
          <Th width={20}>Positions</Th>
          <Th width={30}>On {target} now</Th>
        </Tr>
      </Thead>
      {groups.map((g, i) => {
        const isOpen = expanded.has(g.source);
        return (
          <Tbody key={g.source} isExpanded={isOpen}>
            <Tr>
              <Td expand={{ rowIndex: i, isExpanded: isOpen, onToggle: () => setExpanded(toggled(expanded, g.source)), expandId: `exp-${g.source}` }} />
              <Td
                select={{
                  rowIndex: i,
                  isSelected: selected.has(g.source),
                  isDisabled: !selectable(g),
                  onSelect: () => setSelected(toggled(selected, g.source)),
                }}
              />
              <Td dataLabel="Group">
                <strong>{g.source}</strong>
                <div className="lw-subtle lw-small">{g.groupType} group</div>
              </Td>
              <Td dataLabel="Verdict">
                <VerdictStatus verdict={g.verdict} title={g.reason} />
                {g.reason && g.verdict !== "alreadyApplied" && <div className="lw-reason">{g.reason}</div>}
              </Td>
              <Td dataLabel="Positions">
                <Positions entries={g.entries} notCaptured={g.notCaptured} />
              </Td>
              <Td dataLabel={`On ${target} now`}>{targetText(g)}</Td>
            </Tr>
            <Tr isExpanded={isOpen}>
              <Td colSpan={6} noPadding>
                <ExpandableRowContent>
                  <Partitions g={g} />
                </ExpandableRowContent>
              </Td>
            </Tr>
          </Tbody>
        );
      })}
    </Table>
  );
}

/** Phones: one block per group, its partitions behind a toggle. */
function ReviewCards({ groups, selected, setSelected, target }: { groups: ReviewGroup[]; selected: Set<string>; setSelected: (s: Set<string>) => void; target: string }) {
  const [open, setOpen] = useState<Set<string>>(new Set());
  return (
    <ul className="lw-list" aria-label="Consumer groups and their translated positions">
      {groups.map((g) => (
        <li key={g.source} className="lw-group-card">
          <Flex justifyContent={{ default: "justifyContentSpaceBetween" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "nowrap" }} gap={{ default: "gapSm" }}>
            <FlexItem className="lw-min0">
              <Checkbox
                id={`pick-${g.source}`}
                isChecked={selected.has(g.source)}
                isDisabled={!selectable(g)}
                onChange={() => setSelected(toggled(selected, g.source))}
                label={<strong>{g.source}</strong>}
              />
            </FlexItem>
            <FlexItem>
              <VerdictStatus verdict={g.verdict} title={g.reason} />
            </FlexItem>
          </Flex>
          <div className="lw-list__meta lw-gap-top">
            <Positions entries={g.entries} notCaptured={g.notCaptured} /> · {target}: {targetText(g)}
          </div>
          {g.reason && g.verdict !== "alreadyApplied" && <div className="lw-reason">{g.reason}</div>}
          <ExpandableSection
            toggleText={open.has(g.source) ? "Hide partitions" : `Show ${g.notCaptured ? "why" : plural(g.entries.length, "partition")}`}
            isExpanded={open.has(g.source)}
            onToggle={() => setOpen(toggled(open, g.source))}
          >
            {g.notCaptured ? (
              <Content component="p" className="lw-small">
                Not captured (<span className="lw-mono">{g.notCaptured}</span>): there is no position to translate.
              </Content>
            ) : (
              <CompactList
                flush
                label={`Partitions of ${g.source}`}
                items={g.entries.map((e) => ({
                  key: `${e.topic}-${e.partition}`,
                  title: `${e.target_topic} / ${e.partition}`,
                  meta: `${count(e.position)} → ${e.target_offset === undefined ? "none" : count(e.target_offset)} · ${why(e)}`,
                  status: <MappingStatus cls={e.class} title={e.reason} />,
                }))}
              />
            )}
          </ExpandableSection>
        </li>
      ))}
    </ul>
  );
}

function Review({ restore, groups, round, onCheck, checking, selected, setSelected, onContinue }: {
  restore: Restore;
  groups: ReviewGroup[];
  round: number;
  onCheck: () => void;
  checking: boolean;
  selected: Set<string>;
  setSelected: (s: Set<string>) => void;
  onContinue: () => void;
}) {
  const narrow = useNarrow(991);
  const navigate = useNavigate();
  const target = restore.target.clusterRef.name;
  const by = (v: ReviewGroup["verdict"]) => groups.filter((g) => g.verdict === v);
  const waiting = by("stopConsumers");
  const blocked = by("blocked");
  const chosen = groups.filter((g) => selected.has(g.source));
  const props = { groups, selected, setSelected, target };
  return (
    <PageSection>
      <Card>
        <CardHeader>
          <CardTitle component="h2">{plural(groups.length, "consumer group")} captured with this recovery point</CardTitle>
        </CardHeader>
        <CardBody>
          <ul className="lw-verdicts" aria-label="Groups by verdict">
            {(["ready", "alreadyApplied", "stopConsumers", "blocked"] as const).map((v) => (
              <li key={v}>
                <VerdictStatus verdict={v} />
                <strong className="lw-num">{by(v).length}</strong>
              </li>
            ))}
          </ul>
          <Legend />
          {narrow ? <ReviewCards {...props} /> : <ReviewTable {...props} />}
          {waiting.length > 0 && (
            <Alert
              isInline
              variant="warning"
              className="lw-gap-top"
              title={`${waiting.map((g) => g.source).join(", ")} still ${waiting.length === 1 ? "has" : "have"} consumers connected to ${target}`}
              actionLinks={
                <Button variant="link" isInline isLoading={checking} onClick={onCheck}>
                  {checking ? "Checking" : "Check again"}
                </Button>
              }
            >
              A switchover now would refuse {waiting.length === 1 ? "it" : "them"} (GroupActive): the broker accepts a position
              only while a group has no members. Stop those consumers, then check again.
            </Alert>
          )}
          {blocked.length > 0 && (
            <Alert isInline variant="danger" title={`${blocked.map((g) => g.source).join(" and ")} will be left out`} className="lw-gap-top">
              Each has a position with no restored record to resume from, or none was captured. Open the group to see why,
              and reset it by hand on {target} if its application moves there.
            </Alert>
          )}
          {round > 0 && waiting.length === 0 && (
            <Content component="p" className="lw-subtle lw-small lw-gap-top">
              Checked again just now: no group has consumers connected to {target}.
            </Content>
          )}
        </CardBody>
      </Card>
      <Flex className="lw-gap-top" gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
        <FlexItem>
          <Button variant="primary" isDisabled={chosen.length === 0} onClick={onContinue}>
            Continue with {plural(chosen.length, "group")}
          </Button>
        </FlexItem>
        <FlexItem>
          <Button variant="link" onClick={() => void navigate({ to: "/$ns/restores/$name", params: { ns: restore.namespace, name: restore.name } })}>
            Back to the restore
          </Button>
        </FlexItem>
      </Flex>
    </PageSection>
  );
}

function Confirm({ restore, chosen, left, onBack, onConfirm }: { restore: Restore; chosen: ReviewGroup[]; left: ReviewGroup[]; onBack: () => void; onConfirm: () => void }) {
  const [stopped, setStopped] = useState(false);
  const [showPlan, setShowPlan] = useState(false);
  const t = themeAOf(restore);
  const plan = useMemo(() => planYaml(restore, chosen, t.scorecardSha256, t.mappingSha256), [restore, chosen, t.scorecardSha256, t.mappingSha256]);
  const hash = useSha256(plan);
  const target = restore.target.clusterRef.name;
  const entries = chosen.flatMap((g) => g.entries);
  const c = classCounts(entries);
  const created = chosen.filter((g) => g.state?.exists === false);
  return (
    <PageSection>
      <Card>
        <CardHeader>
          <CardTitle component="h2">
            Switch {plural(chosen.length, "group")} over on {target}
          </CardTitle>
        </CardHeader>
        <CardBody>
          <DescriptionList isCompact className="lw-dl" orientation={{ sm: "vertical", md: "horizontal" }}>
            <DescriptionListGroup>
              <DescriptionListTerm>Groups</DescriptionListTerm>
              <DescriptionListDescription>
                <ul className="lw-plain-list">
                  {chosen.map((g) => (
                    <li key={g.source}>
                      {g.source}
                      <span className="lw-subtle">
                        {g.state?.exists === false ? " (created on the target)" : g.verdict === "alreadyApplied" ? " (already at these positions: no commit)" : ""}
                      </span>
                    </li>
                  ))}
                </ul>
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Positions</DescriptionListTerm>
              <DescriptionListDescription>
                {entries.length}: {c.exact} exact, {c.approximate} approximate
                {c.skipped > 0 && ` (${plural(c.skipped, "unread record")} left out by the restore's end time)`}
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Each partition</DescriptionListTerm>
              <DescriptionListDescription>
                Committed only if the group has no position there now (<span className="lw-mono">expected: none</span>). A group
                that moved since this review is refused whole (TargetPositionsChanged) and nothing is committed for it.
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Saved first</DescriptionListTerm>
              <DescriptionListDescription>
                Every group's current positions on {target}, in the signed switchover record, before any commit.
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Left out</DescriptionListTerm>
              <DescriptionListDescription>
                {left.length === 0
                  ? "None"
                  : left.map((g) => (
                      <div key={g.source}>
                        {g.source} <span className="lw-reason">{g.reason ?? "not chosen"}</span>
                      </div>
                    ))}
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Approval</DescriptionListTerm>
              <DescriptionListDescription>
                {restore.namespace} uses one-person confirmation: confirming here approves it. A two-person namespace would
                show a second approver every group and position first.
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Bound to</DescriptionListTerm>
              <DescriptionListDescription>
                <div>
                  Restore {restore.name}'s signed scorecard <Ident value={t.scorecardSha256} label="scorecard sha256" />
                </div>
                <div>
                  and its positions mapping <Ident value={t.mappingSha256} label="positions mapping sha256" />
                </div>
              </DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Plan</DescriptionListTerm>
              <DescriptionListDescription>
                <Ident value={hash} label="switchover plan sha256" />
              </DescriptionListDescription>
            </DescriptionListGroup>
          </DescriptionList>
          <ExpandableSection className="lw-gap-top" toggleText={showPlan ? "Hide the plan" : "Show the plan"} isExpanded={showPlan} onToggle={(_e, o) => setShowPlan(o)}>
            <CodeBlock>
              <CodeBlockCode>{plan}</CodeBlockCode>
            </CodeBlock>
          </ExpandableSection>
          <Alert isInline variant="info" title="Undoing a switchover has limits" className="lw-gap-top">
            A rollback plan built from the record puts the saved positions back only while no consumer has committed on{" "}
            {target} since; after that it is refused and names the records read.
            {created.length > 0 &&
              ` ${created.map((g) => g.source).join(", ")} had no position before, and an absent position cannot be put back through Logweir.`}
          </Alert>
          <Checkbox
            id="stopped"
            className="lw-gap-top"
            isChecked={stopped}
            onChange={(_e, on) => setStopped(on)}
            label="The consumers of these groups stay stopped until the switchover finishes."
          />
        </CardBody>
      </Card>
      <Flex className="lw-gap-top" gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
        <FlexItem>
          <Button variant="primary" icon={<ExchangeAltIcon />} isDisabled={!stopped || hash.startsWith("computing")} onClick={onConfirm}>
            Switch over {plural(chosen.length, "group")}
          </Button>
        </FlexItem>
        <FlexItem>
          <Button variant="secondary" onClick={onBack}>
            Back
          </Button>
        </FlexItem>
      </Flex>
    </PageSection>
  );
}

function Done({ restore, chosen, left, onFinished }: { restore: Restore; chosen: ReviewGroup[]; left: ReviewGroup[]; onFinished: () => void }) {
  const navigate = useNavigate();
  const [done, setDone] = useState(0);
  const t = themeAOf(restore);
  const runId = "01M3" + t.mappingSha256.slice(7, 29).toUpperCase();
  const status = switchoverStatus(chosen, done, runId);
  const finished = status.phase === "Succeeded";
  const target = restore.target.clusterRef.name;
  useEffect(() => {
    if (done >= chosen.length) {
      onFinished();
      return undefined;
    }
    const timer = window.setTimeout(() => setDone(done + 1), done === 0 ? 1100 : 800);
    return () => window.clearTimeout(timer);
  }, [done, chosen.length, onFinished]);
  const n = status.counts;
  const narrow = useNarrow();
  const note = (g: (typeof status.groups)[number]) =>
    g.outcome === null ? "Waiting its turn" : g.created ? `Group created on ${target}` : g.outcome === "alreadyApplied" ? "Positions already equal the plan; nothing committed" : "Prior positions recorded, then committed";
  return (
    <PageSection>
      <Card>
        <CardHeader>
          <Flex alignItems={{ default: "alignItemsCenter" }} gap={{ default: "gapSm" }}>
            <CardTitle component="h2">Switchover {restore.name}-sw1</CardTitle>
            <PhaseStatus phase={status.phase} />
          </Flex>
        </CardHeader>
        <CardBody>
          {narrow ? (
            <CompactList
              flush
              label="Outcome per group"
              items={status.groups.map((g) => ({
                key: g.source,
                title: g.source,
                meta: `${plural(g.partitions, "partition")} · ${note(g)}`,
                status: <OutcomeStatus outcome={g.outcome} reason={g.reason} />,
              }))}
            />
          ) : (
          <Table aria-label="Outcome per group" variant="compact" aria-live="polite">
            <Thead>
              <Tr>
                <Th width={30}>Group</Th>
                <Th width={25}>Outcome</Th>
                <Th width={15} className="lw-right">
                  Partitions
                </Th>
                <Th width={30}>Note</Th>
              </Tr>
            </Thead>
            <Tbody>
              {status.groups.map((g) => (
                <Tr key={g.source}>
                  <Td dataLabel="Group">{g.source === g.target ? g.source : `${g.source} → ${g.target}`}</Td>
                  <Td dataLabel="Outcome">
                    <OutcomeStatus outcome={g.outcome} reason={g.reason} />
                  </Td>
                  <Td dataLabel="Partitions" className="lw-num lw-right">
                    {g.partitions}
                  </Td>
                  <Td dataLabel="Note" className="lw-subtle">
                    {note(g)}
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
          )}
          <Content component="p" className="lw-subtle lw-small lw-gap-top">
            {n.applied} applied · {n.alreadyApplied} already applied · {n.refused} refused · {n.failed} failed · {n.untouched} not reached yet
          </Content>
          {finished && (
            <>
              <DescriptionList isCompact className="lw-dl lw-gap-top" orientation={{ sm: "vertical", md: "horizontal" }}>
                <DescriptionListGroup>
                  <DescriptionListTerm>Switchover record</DescriptionListTerm>
                  <DescriptionListDescription>
                    <Ident value={status.evidence?.recordKey ?? ""} label="switchover record key" />
                    <div className="lw-subtle lw-small">Signed; the controller verified it. Every partition's position before and after.</div>
                  </DescriptionListDescription>
                </DescriptionListGroup>
              </DescriptionList>
              <Alert isInline variant="success" title={`Start your consumers on ${target}`} className="lw-gap-top">
                {chosen.map((g) => g.source).join(", ")} now resume at the translated positions.
                {left.length > 0 && ` Still to do by hand: ${left.map((g) => g.source).join(", ")}.`}
              </Alert>
            </>
          )}
        </CardBody>
      </Card>
      {finished && (
        <Flex className="lw-gap-top" gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
          <FlexItem>
            <Button variant="primary" onClick={() => void navigate({ to: "/$ns/restores/$name", params: { ns: restore.namespace, name: restore.name } })}>
              Back to the restore
            </Button>
          </FlexItem>
          <FlexItem>
            <Button variant="link" onClick={() => void navigate({ to: "/$ns", params: { ns: restore.namespace } })}>
              Overview
            </Button>
          </FlexItem>
        </Flex>
      )}
    </PageSection>
  );
}

const STEPS = ["Review positions", "Confirm", "Switched over"];

export function Cutover() {
  const { ns, name } = useParams({ from: "/$ns/restores/$name/cutover" });
  const restore = useRestore(ns, name);
  const [round, setRound] = useState(0);
  const [checking, setChecking] = useState(false);
  const [step, setStep] = useState<Step>("review");
  const groups = useMemo(() => (restore.data ? reviewFor(restore.data, round) : []), [restore.data, round]);
  const [selected, setSelected] = useState<Set<string> | null>(null);
  const [finished, setFinished] = useState(false);
  const picked = selected ?? new Set(groups.filter(selectable).map((g) => g.source));

  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/restores" params={{ ns }}>Restores</Link>} />
      <BreadcrumbItem render={() => <Link to="/$ns/restores/$name" params={{ ns, name }}>{name}</Link>} />
      <BreadcrumbItem isActive>Switch consumers over</BreadcrumbItem>
    </Breadcrumb>
  );

  if (restore.isPending || restore.isError) {
    return (
      <>
        <PageHeader title="Switch consumers over" breadcrumb={breadcrumb} />
        <PageSection>{restore.isPending ? <LoadingRows rows={6} /> : <ErrorState error={restore.error} />}</PageSection>
      </>
    );
  }

  const r = restore.data;
  const t = themeAOf(r);
  const target = r.target.clusterRef.name;
  if (t.mapping !== "written") {
    const [title, body] =
      t.mapping === "sampled"
        ? ["No positions to switch over", "This restore was verified on a sample. Positions are mapped only by a complete verification (VerificationNotComplete); restore again with complete verification to switch consumers over."]
        : t.mapping === "scratch"
          ? ["A clone cannot be switched over", "This restore is a temporary clone, deleted when it expires (RestoreIsScratch)."]
          : ["The restore has not finished", "Positions are mapped when the restore passes its complete verification (RestoreNotSucceeded)."];
    return (
      <>
        <PageHeader title="Switch consumers over" breadcrumb={breadcrumb} />
        <PageSection>
          <Card>
            <Empty title={title}>{body}</Empty>
          </Card>
        </PageSection>
      </>
    );
  }

  const chosen = groups.filter((g) => picked.has(g.source) && selectable(g));
  const left = groups.filter((g) => !chosen.includes(g));
  const stepIndex = step === "review" ? 0 : step === "confirm" ? 1 : 2;

  const checkAgain = () => {
    setChecking(true);
    window.setTimeout(() => {
      const next = reviewFor(r, round + 1);
      // Groups that became ready join the selection; nothing chosen is dropped silently.
      setSelected(new Set([...picked, ...next.filter((g) => g.verdict === "ready").map((g) => g.source)]));
      setRound(round + 1);
      setChecking(false);
    }, 1200);
  };

  return (
    <>
      <PageHeader
        title="Switch consumers over"
        breadcrumb={breadcrumb}
        description={`Move consumer groups to the restored topics on ${target}. Each captured position was mapped by the restore's complete verification; the groups must be stopped. Nothing changes on ${target} until you confirm.`}
      />
      <PageSection>
        <ProgressStepper isCenterAligned aria-label="Cutover steps" className="lw-stepper">
          {STEPS.map((s, i) => (
            <ProgressStep
              key={s}
              id={`co-${i}`}
              titleId={`co-${i}-t`}
              variant={i < stepIndex || (i === 2 && finished) ? "success" : i === stepIndex ? "info" : "pending"}
              isCurrent={i === stepIndex && !finished}
            >
              {s}
            </ProgressStep>
          ))}
        </ProgressStepper>
      </PageSection>
      {step === "review" && (
        <Review
          restore={r}
          groups={groups}
          round={round}
          onCheck={checkAgain}
          checking={checking}
          selected={picked}
          setSelected={setSelected}
          onContinue={() => setStep("confirm")}
        />
      )}
      {step === "confirm" && <Confirm restore={r} chosen={chosen} left={left} onBack={() => setStep("review")} onConfirm={() => setStep("done")} />}
      {step === "done" && <Done restore={r} chosen={chosen} left={left} onFinished={() => setFinished(true)} />}
    </>
  );
}
