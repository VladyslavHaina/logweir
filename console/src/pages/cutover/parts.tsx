// The cutover's building blocks, shared by the flow and the Switchover pages: a group's
// partitions, the review table (phones: one block per group), the outcome per group,
// the plan and the rollback plan. Prototype look (Cutover.tsx at 164a328d), real data.
import { useState } from "react";
import {
  Alert,
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
} from "@patternfly/react-core";
import { ExpandableRowContent, Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import DownloadIcon from "@patternfly/react-icons/dist/esm/icons/download-icon";
import {
  UNAVAILABLE_WORDS,
  classCounts,
  fileName,
  groupsPartial,
  isTerminal,
  offered,
  plannedPositions,
  reasonWord,
  reasonWords,
  rollbackPlan,
  unavailablePositions,
  verdictOf,
  type ParsedPlan,
  type ReviewGroup,
  type ReviewPosition,
  type Switchover,
} from "../../api/switchovers";
import { MappingStatus, VerdictStatus } from "../../components/Status";
import { CompactList, useNarrow } from "../../components/CompactList";
import { count } from "../../components/When";
import { OutcomeLabel } from "./status";

export const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

/** Save text as a file: the console's downloads are made in the page, never fetched. */
export function download(name: string, text: string, type = "text/plain") {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/** "3 exact · 1 approximate · 1 unavailable". */
export function PositionCounts({ positions }: { positions: ReviewPosition[] }) {
  const c = classCounts(positions);
  const parts = [c.exact > 0 && `${c.exact} exact`, c.approximate > 0 && `${c.approximate} approximate`, c.unavailable > 0 && `${c.unavailable} unavailable`].filter(Boolean);
  return <span>{parts.length ? parts.join(" · ") : "No position"}</span>;
}

/** Why a partition resumes where it does, in words; the class's reason name beside it. */
export function why(p: ReviewPosition): string {
  if (p.class === "exact") return "The first restored record at or after the captured position";
  if (p.class === "approximate") {
    return p.skipped !== undefined
      ? `${plural(p.skipped, "unread record")} left out by the restore's own selection`
      : "The restore's own selection left out records the group had not read; the review does not state how many";
  }
  return UNAVAILABLE_WORDS[reasonWord(p.reason)] ?? "No restored record corresponds to the position.";
}

/** The target group now, as the review read it. */
export function targetNow(g: ReviewGroup): string {
  if (g.absent) return offered(g) ? "Absent: the switchover creates it" : "Absent";
  if (g.members !== undefined && g.members > 0) return `${plural(g.members, "member")} connected${g.state ? ` (${g.state})` : ""}`;
  if (g.alreadyApplied) return "Stopped; its positions equal the plan";
  if (g.positions.some((p) => p.committed !== null)) return "Stopped; it holds other positions";
  return g.state ? `Stopped (${g.state}); no positions yet` : "Stopped; no positions yet";
}

export function Legend() {
  const [open, setOpen] = useState(false);
  return (
    <ExpandableSection toggleText="How positions are translated" isExpanded={open} onToggle={(_e, o) => setOpen(o)}>
      <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "14ch" }}>
        <DescriptionListGroup>
          <DescriptionListTerm>
            <MappingStatus cls="exact" />
          </DescriptionListTerm>
          <DescriptionListDescription>
            The group resumes at the first restored record whose original offset (its x-original-offset header) is at or after the
            captured position. Nothing it had not read is skipped.
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>
            <MappingStatus cls="approximate" />
          </DescriptionListTerm>
          <DescriptionListDescription>
            The same rule, but the restore's own selection (its start or end time, or a partition subset) left out records the group
            had not read yet. Verification still passed.
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>
            <MappingStatus cls="unavailable" />
          </DescriptionListTerm>
          <DescriptionListDescription>
            No restored record corresponds to the position, for a named reason. It is never planned: that partition gets no committed
            position, so its consumer starts there from its own auto.offset.reset.
          </DescriptionListDescription>
        </DescriptionListGroup>
      </DescriptionList>
    </ExpandableSection>
  );
}

/** One group's partitions on an inset panel: where each resumes, its class, and what the target holds now. */
export function Partitions({ g }: { g: ReviewGroup }) {
  const narrow = useNarrow();
  const notes = (
    <>
      {g.reason && verdictOf(g).verdict !== "ready" && (
        <Content component="p" className="lw-inset__note">
          <span className="lw-reason">{g.reason}</span> {reasonWords(g.reason) ?? ""}
          {g.message && <span className="lw-subtle"> ({g.message})</span>}
        </Content>
      )}
      {unavailablePositions(g).length > 0 && offered(g) && (
        <Content component="p" className="lw-subtle lw-small lw-inset__note">
          {plural(unavailablePositions(g).length, "partition")} of this group {unavailablePositions(g).length === 1 ? "is" : "are"} unavailable and
          not planned: the switchover commits no position there, so its consumer starts those from its own auto.offset.reset.
        </Content>
      )}
    </>
  );
  if (narrow) {
    return (
      <div className="lw-inset">
        <CompactList
          flush
          label={`Partitions of ${g.source}`}
          items={g.positions.map((p) => ({
            key: `${p.topic}-${p.partition}`,
            title: `${p.topic} / ${p.partition}`,
            meta: `${p.offset === undefined ? "Not planned" : `Resumes at ${count(p.offset)}`} · now ${p.committed === null ? "none" : count(p.committed)} · ${why(p)}`,
            status: <MappingStatus cls={p.class} title={p.reason} />,
          }))}
        />
        {notes}
      </div>
    );
  }
  return (
    <div className="lw-inset">
      <Table aria-label={`Partitions of ${g.source}`} variant="compact">
        <Thead>
          <Tr>
            <Th width={20}>Partition</Th>
            <Th width={15} className="lw-right" modifier="nowrap">
              Resumes at
            </Th>
            <Th width={15}>Translation</Th>
            <Th width={15} className="lw-right" modifier="nowrap">
              Committed now
            </Th>
            <Th width={35}>Why</Th>
          </Tr>
        </Thead>
        <Tbody>
          {g.positions.map((p) => (
            <Tr key={`${p.topic}-${p.partition}`}>
              <Td dataLabel="Partition" className="lw-break">
                {p.topic} / {p.partition}
              </Td>
              <Td dataLabel="Resumes at" className="lw-num lw-right">
                {p.offset === undefined ? "Not planned" : count(p.offset)}
              </Td>
              <Td dataLabel="Translation">
                <MappingStatus cls={p.class} title={p.reason} />
              </Td>
              <Td dataLabel="Committed now" className="lw-num lw-right">
                {p.committed === null ? "None" : count(p.committed)}
              </Td>
              <Td dataLabel="Why">
                <span className="lw-subtle">{why(p)}</span>
                {p.reason && <div className="lw-reason">{p.reason}</div>}
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
      {notes}
    </div>
  );
}

function toggled(set: Set<string>, key: string): Set<string> {
  const next = new Set(set);
  if (next.has(key)) next.delete(key);
  else next.add(key);
  return next;
}

export interface ReviewProps {
  groups: ReviewGroup[];
  selected: Set<string>;
  setSelected: (s: Set<string>) => void;
  target: string;
}

/** Wide screens: one row per group, its partitions behind the expand toggle. */
export function ReviewTable({ groups, selected, setSelected, target }: ReviewProps) {
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set(groups.filter((g) => verdictOf(g).verdict === "blocked" || unavailablePositions(g).length > 0).map((g) => g.source)));
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
        const v = verdictOf(g);
        return (
          <Tbody key={g.source} isExpanded={isOpen}>
            <Tr>
              <Td expand={{ rowIndex: i, isExpanded: isOpen, onToggle: () => setExpanded(toggled(expanded, g.source)), expandId: `exp-${g.source}` }} />
              <Td className="pf-v6-c-table__check">
                <Checkbox
                  id={`include-${i}`}
                  aria-label={`Include ${g.source}`}
                  isChecked={selected.has(g.source)}
                  isDisabled={!offered(g)}
                  onChange={() => setSelected(toggled(selected, g.source))}
                />
              </Td>
              <Td dataLabel="Group">
                <strong className="lw-break">{g.source}</strong>
                <div className="lw-subtle lw-small">
                  {g.target !== g.source ? `as ${g.target} · ` : ""}
                  {g.type ? `${g.type} group` : "no group yet"}
                </div>
              </Td>
              <Td dataLabel="Verdict">
                <VerdictStatus verdict={v.verdict} title={v.reason} />
                {v.reason && <div className="lw-reason">{v.reason}</div>}
              </Td>
              <Td dataLabel="Positions">
                <PositionCounts positions={g.positions} />
              </Td>
              <Td dataLabel={`On ${target} now`}>{targetNow(g)}</Td>
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
export function ReviewCards({ groups, selected, setSelected, target }: ReviewProps) {
  const [open, setOpen] = useState<Set<string>>(new Set());
  return (
    <ul className="lw-list" aria-label="Consumer groups and their translated positions">
      {groups.map((g) => {
        const v = verdictOf(g);
        return (
          <li key={g.source} className="lw-group-card">
            <Flex justifyContent={{ default: "justifyContentSpaceBetween" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "nowrap" }} gap={{ default: "gapSm" }}>
              <FlexItem className="lw-min0">
                <Checkbox
                  id={`pick-${g.source}`}
                  isChecked={selected.has(g.source)}
                  isDisabled={!offered(g)}
                  onChange={() => setSelected(toggled(selected, g.source))}
                  label={<strong className="lw-break">{g.source}</strong>}
                />
              </FlexItem>
              <FlexItem>
                <VerdictStatus verdict={v.verdict} title={v.reason} />
              </FlexItem>
            </Flex>
            <div className="lw-list__meta lw-gap-top">
              <PositionCounts positions={g.positions} /> · {target}: {targetNow(g)}
            </div>
            {v.reason && <div className="lw-reason">{v.reason}</div>}
            <ExpandableSection
              toggleText={open.has(g.source) ? "Hide partitions" : `Show ${plural(g.positions.length, "partition")}`}
              isExpanded={open.has(g.source)}
              onToggle={() => setOpen(toggled(open, g.source))}
            >
              <Partitions g={g} />
            </ExpandableSection>
          </li>
        );
      })}
    </ul>
  );
}

/** One row per group: the plan's groups while the run has not reported, then each outcome. */
export function OutcomeTable({ sw, plan, target }: { sw: Switchover; plan: ParsedPlan | null; target: string }) {
  const narrow = useNarrow();
  const reported = sw.groups.length > 0;
  const rows = reported
    ? sw.groups.map((g) => ({ key: g.target, source: g.source, target: g.target, outcome: g.outcome as string | null, reason: g.reason ?? undefined, created: g.created, partitions: g.partitions }))
    : (plan?.groups ?? []).map((g) => ({ key: g.target, source: g.source, target: g.target, outcome: null, reason: undefined, created: false, partitions: g.partitions.length }));
  const note = (r: (typeof rows)[number]) => {
    if (r.outcome === null) return isTerminal(sw) ? "Not known here: the signed switchover record names its outcome" : "Waiting for the run's result";
    if (r.outcome === "applied") return r.created ? `Group created on ${target}; its positions committed` : "Its prior positions recorded, then committed";
    if (r.outcome === "alreadyApplied") return "Its positions already equalled the plan; nothing committed";
    if (r.outcome === "untouched") return "The run stopped before this group; nothing committed";
    return reasonWords(r.reason) ?? (r.outcome === "refused" ? "Nothing committed for this group" : "See the record for each partition");
  };
  if (rows.length === 0) {
    return (
      <Content component="p" className="lw-subtle">
        {sw.phase === "Refused" ? "Refused before any group: nothing was committed." : "No group has reported yet."}
      </Content>
    );
  }
  if (narrow) {
    return (
      <CompactList
        flush
        label="Outcome per group"
        items={rows.map((r) => ({
          key: r.key,
          title: r.source === r.target ? r.source : `${r.source} → ${r.target}`,
          meta: (
            <>
              {plural(r.partitions, "partition")} · {note(r)}
              {r.reason && <span className="lw-reason"> {r.reason}</span>}
            </>
          ),
          status: <OutcomeLabel outcome={r.outcome} />,
        }))}
      />
    );
  }
  return (
    <Table aria-label="Outcome per group" variant="compact" aria-live="polite">
      <Thead>
        <Tr>
          <Th width={25}>Group</Th>
          <Th width={20}>Outcome</Th>
          <Th width={15} className="lw-right">
            Partitions
          </Th>
          <Th width={40}>Note</Th>
        </Tr>
      </Thead>
      <Tbody>
        {rows.map((r) => (
          <Tr key={r.key}>
            <Td dataLabel="Group" className="lw-break">
              {r.source === r.target ? r.source : `${r.source} → ${r.target}`}
            </Td>
            <Td dataLabel="Outcome">
              <OutcomeLabel outcome={r.outcome} />
              {r.reason && <div className="lw-reason">{r.reason}</div>}
            </Td>
            <Td dataLabel="Partitions" className="lw-num lw-right">
              {r.partitions}
            </Td>
            <Td dataLabel="Note" className="lw-subtle">
              {note(r)}
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
}

/** "2 applied · 1 refused": the controller's counts, from the groups it reported. */
export function OutcomeCounts({ sw }: { sw: Switchover }) {
  const n = (o: string) => sw.groups.filter((g) => g.outcome === o).length;
  // FX-38: counts over a partial list are not known, never 0.
  if (groupsPartial(sw)) {
    return (
      <Content component="p" className="lw-subtle lw-small lw-gap-top">
        Counts not known here: the groups listed are not every group. The signed switchover record names each one.
      </Content>
    );
  }
  if (sw.groups.length === 0) return null;
  return (
    <Content component="p" className="lw-subtle lw-small lw-gap-top">
      {n("applied")} applied · {n("alreadyApplied")} already applied · {n("refused")} refused · {n("failed")} failed · {n("untouched")} not reached
    </Content>
  );
}

/** The plan's bytes, shown and saved exactly as the approver signs them. */
export function PlanBlock({ bytes, name }: { bytes: string; name: string }) {
  const [open, setOpen] = useState(false);
  return (
    <ExpandableSection toggleText={open ? "Hide the plan" : "Show the plan"} isExpanded={open} onToggle={(_e, o) => setOpen(o)}>
      <CodeBlock
        actions={
          <Button variant="link" icon={<DownloadIcon />} onClick={() => download(fileName(name, ".switchover-plan.yaml", "switchover-plan.yaml"), bytes, "application/yaml")}>
            Download
          </Button>
        }
      >
        <CodeBlockCode>{bytes}</CodeBlockCode>
      </CodeBlock>
    </ExpandableSection>
  );
}

/** What undoing this switchover would take: shown and saved, never started from here. */
export function RollbackCard({ sw, plan }: { sw: Switchover; plan: ParsedPlan | null }) {
  const rb = rollbackPlan(sw, plan);
  const appliedGroups = sw.groups.filter((g) => g.outcome === "applied").length;
  if (appliedGroups === 0 && rb.stuck.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Undoing this switchover</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p">
          A rollback is a new switchover with its own approval. Its plan is built from this switchover's signed record and commits each
          group's earlier positions back, expecting the ones applied here. It is refused for a group whose consumers committed since,
          naming the records they read.
        </Content>
        {rb.restorable.length > 0 && (
          <Table aria-label="Positions a rollback commits back" variant="compact" className="lw-gap-top">
            <Thead>
              <Tr>
                <Th>Group</Th>
                <Th>Partition</Th>
                <Th className="lw-right">Now</Th>
                <Th className="lw-right">Back to</Th>
              </Tr>
            </Thead>
            <Tbody>
              {rb.restorable.map((r) => (
                <Tr key={`${r.target}-${r.topic}-${r.partition}`}>
                  <Td dataLabel="Group">{r.target}</Td>
                  <Td dataLabel="Partition">
                    {r.topic} / {r.partition}
                  </Td>
                  <Td dataLabel="Now" className="lw-num lw-right">
                    {count(r.applied)}
                  </Td>
                  <Td dataLabel="Back to" className="lw-num lw-right">
                    {count(r.prior)}
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        )}
        {rb.notRestorable.length > 0 && (
          <Alert isInline variant="info" className="lw-gap-top" title={`${plural(rb.notRestorable.length, "position")} had none before`}>
            A commit cannot remove a position, so these cannot be put back through Logweir. With the group stopped, remove them with{" "}
            <code className="lw-mono">kafka-consumer-groups.sh --delete-offsets</code>; the download lists each command.
          </Alert>
        )}
        {rb.stuck.length > 0 && (
          <Alert isInline variant="warning" className="lw-gap-top" title={`${rb.stuck.join(", ")} failed part-way (CommitFailed)`}>
            Neither a re-run nor a rollback can take {rb.stuck.length === 1 ? "it" : "them"}: the record's per-partition "applied" says which
            positions to remove before a new switchover with the same plan.
          </Alert>
        )}
        {!plan && appliedGroups > 0 && (
          <Content component="p" className="lw-subtle lw-small lw-gap-top">
            This plan was not written by the console, so this page does not read its partitions: build the rollback plan from the
            record with the command below.
          </Content>
        )}
        <Flex className="lw-gap-top" gap={{ default: "gapMd" }}>
          <FlexItem>
            <Button variant="secondary" icon={<DownloadIcon />} onClick={() => download(fileName(sw.name, ".rollback.txt", "switchover.rollback.txt"), rb.text)}>
              Download the rollback steps
            </Button>
          </FlexItem>
        </Flex>
        <CodeBlock className="lw-gap-top">
          <CodeBlockCode>{"logweir switchover rollback-plan --record <a local copy of the switchover record> --out rollback.yaml"}</CodeBlockCode>
        </CodeBlock>
      </CardBody>
    </Card>
  );
}

/** The positions a set of groups commits, by class. */
export function planCounts(groups: ReviewGroup[]) {
  return classCounts(groups.flatMap(plannedPositions));
}
