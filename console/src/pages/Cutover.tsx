// Theme A's cutover (PROD-04.2): review how each consumer group's committed position
// translates into the restored topics, then switch the stopped groups over.
// Mock data; the documents are provisional until decisions/PROD-04.2-cutover.md.
import { useCallback, useEffect, useMemo, useState } from "react";
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
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  ExpandableSection,
  Flex,
  FlexItem,
  FormGroup,
  FormHelperText,
  HelperText,
  HelperTextItem,
  Label,
  PageSection,
  ProgressStep,
  ProgressStepper,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { ExpandableRowContent, Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import { useRestore } from "../api/queries";
import { reviewFor, verdictOf, type GroupReview } from "../mock/cutover";
import { PageHeader } from "../components/PageHeader";
import { MappingStatus, VerdictStatus } from "../components/Status";
import { ErrorState, LoadingRows } from "../components/States";
import { count } from "../components/When";

type Step = "review" | "confirm" | "done";

function summary(g: GroupReview): string {
  const n = (m: string) => g.rows.filter((r) => r.mapping === m).length;
  const parts = [
    n("exact") && `${n("exact")} exact`,
    n("approximate") && `${n("approximate")} approximate`,
    n("unavailable") && `${n("unavailable")} unavailable`,
  ].filter(Boolean);
  return parts.join(" · ");
}

function Legend() {
  const [open, setOpen] = useState(false);
  return (
    <ExpandableSection toggleText="How positions are translated" isExpanded={open} onToggle={(_e, o) => setOpen(o)}>
      <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "14ch" }}>
        <DescriptionListGroup>
          <DescriptionListTerm>
            <MappingStatus mapping="exact" />
          </DescriptionListTerm>
          <DescriptionListDescription>
            The group resumes at the first restored record whose original offset (the x-original-offset header) is at or
            after its committed offset. Nothing is skipped or read twice.
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>
            <MappingStatus mapping="approximate" />
          </DescriptionListTerm>
          <DescriptionListDescription>
            The committed offset was the end of the partition when the backup ran, so the group resumes at the end of the
            restored partition. Safe, but not tied to one record.
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>
            <MappingStatus mapping="unavailable" />
          </DescriptionListTerm>
          <DescriptionListDescription>
            No restored record corresponds to the position, or the group was not captured. Logweir never guesses: the
            group is left out, and you reset it yourself if you need it.
          </DescriptionListDescription>
        </DescriptionListGroup>
      </DescriptionList>
    </ExpandableSection>
  );
}

function ReviewTable({
  groups,
  selected,
  setSelected,
  target,
}: {
  groups: GroupReview[];
  selected: Set<string>;
  setSelected: (s: Set<string>) => void;
  target: string;
}) {
  // Wide screens open two groups to show both an exact and a blocked case; phones start closed.
  const [expanded, setExpanded] = useState<Set<string>>(() =>
    window.matchMedia?.("(min-width: 992px)").matches ? new Set(["orders-billing", "orders-fraud"]) : new Set(),
  );
  const toggle = (set: Set<string>, key: string) => {
    const next = new Set(set);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    return next;
  };
  return (
    <Table aria-label="Consumer groups and their translated positions" gridBreakPoint="grid-lg" isExpandable>
      <Thead>
        <Tr>
          <Th screenReaderText="Show partitions" />
          <Th screenReaderText="Include" />
          <Th>Group</Th>
          <Th>Partitions</Th>
          <Th>Translation</Th>
          <Th>On {target}</Th>
          <Th>Verdict</Th>
        </Tr>
      </Thead>
      {groups.map((g, i) => {
        const verdict = verdictOf(g);
        const isOpen = expanded.has(g.group);
        return (
          <Tbody key={g.group} isExpanded={isOpen}>
            <Tr>
              <Td
                expand={{ rowIndex: i, isExpanded: isOpen, onToggle: () => setExpanded(toggle(expanded, g.group)), expandId: `exp-${g.group}` }}
              />
              <Td
                select={{
                  rowIndex: i,
                  isSelected: selected.has(g.group),
                  isDisabled: verdict !== "ready",
                  onSelect: () => setSelected(toggle(selected, g.group)),
                }}
              />
              <Td dataLabel="Group" modifier="nowrap">
                <strong>{g.group}</strong>
                <div className="lw-subtle lw-small">{g.type === "other" ? "streams group" : `${g.type} group`}</div>
              </Td>
              <Td dataLabel="Partitions" className="lw-num">{g.rows.length}</Td>
              <Td dataLabel="Translation">{summary(g)}</Td>
              <Td dataLabel={`On ${target}`}>
                {g.activeMembers > 0 ? (
                  <Label isCompact status="warning">{g.activeMembers} members connected</Label>
                ) : (
                  "No members connected"
                )}
              </Td>
              <Td dataLabel="Verdict">
                <VerdictStatus verdict={verdict} />
              </Td>
            </Tr>
            <Tr isExpanded={isOpen}>
              <Td colSpan={7} noPadding>
                <ExpandableRowContent>
                  <Table aria-label={`Partitions of ${g.group}`} variant="compact" gridBreakPoint="grid-md" className="lw-nested">
                    <Thead>
                      <Tr>
                        <Th>Partition</Th>
                        <Th modifier="wrap">Committed on the source</Th>
                        <Th modifier="wrap">Resumes at on {target}</Th>
                        <Th>Translation</Th>
                        <Th width={40}>Why</Th>
                      </Tr>
                    </Thead>
                    <Tbody>
                      {g.rows.map((r) => (
                        <Tr key={`${r.topic}-${r.partition}`}>
                          <Td dataLabel="Partition">
                            {r.topic} / {r.partition}
                          </Td>
                          <Td dataLabel="Committed on the source" className="lw-num">{r.committed === null ? "Not captured" : count(r.committed)}</Td>
                          <Td dataLabel={`Resumes at on ${target}`} className="lw-num">{r.target === null ? "None" : count(r.target)}</Td>
                          <Td dataLabel="Translation">
                            <MappingStatus mapping={r.mapping} />
                          </Td>
                          <Td dataLabel="Why" className="lw-subtle">{r.why}</Td>
                        </Tr>
                      ))}
                    </Tbody>
                  </Table>
                </ExpandableRowContent>
              </Td>
            </Tr>
          </Tbody>
        );
      })}
    </Table>
  );
}

function Apply({ groups, onFinished }: { groups: GroupReview[]; onFinished: () => void }) {
  const [applied, setApplied] = useState(0);
  useEffect(() => {
    if (applied >= groups.length) {
      onFinished();
      return undefined;
    }
    const t = window.setTimeout(() => setApplied(applied + 1), 900);
    return () => window.clearTimeout(t);
  }, [applied, groups.length, onFinished]);
  return (
    <ol className="lw-checks" aria-live="polite">
      {groups.map((g, i) => (
        <li key={g.group}>
          <span>
            {g.group}: {g.rows.length} positions
          </span>
          {i < applied ? (
            <Label isCompact status="success">Committed</Label>
          ) : i === applied ? (
            <Label isCompact color="blue">Committing</Label>
          ) : (
            <Label isCompact color="grey">Waiting</Label>
          )}
        </li>
      ))}
    </ol>
  );
}

export function Cutover() {
  const { ns, name } = useParams({ from: "/$ns/restores/$name/cutover" });
  const navigate = useNavigate();
  const restore = useRestore(ns, name);
  const [groups, setGroups] = useState<GroupReview[]>(() => reviewFor(name));
  const [selected, setSelected] = useState<Set<string>>(() => new Set(groups.filter((g) => verdictOf(g) === "ready").map((g) => g.group)));
  const [step, setStep] = useState<Step>("review");
  const [checking, setChecking] = useState(false);
  const [stopped, setStopped] = useState(false);
  const [typed, setTyped] = useState("");
  const [finished, setFinished] = useState(false);
  const markFinished = useCallback(() => setFinished(true), []);

  const target = restore.data?.target.clusterRef.name ?? "orders-dr";
  const chosen = useMemo(() => groups.filter((g) => selected.has(g.group)), [groups, selected]);
  const waiting = groups.filter((g) => verdictOf(g) === "stopConsumers");
  const blocked = groups.filter((g) => verdictOf(g) === "blocked");
  const positions = chosen.flatMap((g) => g.rows);

  const checkAgain = () => {
    setChecking(true);
    window.setTimeout(() => {
      setGroups((gs) => gs.map((g) => ({ ...g, activeMembers: 0 })));
      setSelected((s) => new Set([...s, ...waiting.map((g) => g.group)]));
      setChecking(false);
    }, 1400);
  };

  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/restores" params={{ ns }}>Restores</Link>} />
      <BreadcrumbItem render={() => <Link to="/$ns/restores/$name" params={{ ns, name }}>{name}</Link>} />
      <BreadcrumbItem isActive>Switch consumers over</BreadcrumbItem>
    </Breadcrumb>
  );

  if (restore.isPending) {
    return (
      <>
        <PageHeader title="Switch consumers over" breadcrumb={breadcrumb} />
        <PageSection>
          <LoadingRows rows={6} />
        </PageSection>
      </>
    );
  }
  if (restore.isError) {
    return (
      <>
        <PageHeader title="Switch consumers over" breadcrumb={breadcrumb} />
        <PageSection>
          <ErrorState error={restore.error} />
        </PageSection>
      </>
    );
  }

  const stepIndex = step === "review" ? 0 : step === "confirm" ? 1 : 2;
  const steps = ["Review positions", "Confirm", "Switched over"];

  return (
    <>
      <PageHeader
        title="Switch consumers over"
        breadcrumb={breadcrumb}
        description={`Move consumer groups to the restored topics on ${target}. Logweir translates each committed position, checks the groups are stopped, then commits. Nothing happens to your consumers until you confirm.`}
      />
      <PageSection>
        <ProgressStepper isCenterAligned aria-label="Cutover steps" className="lw-stepper">
          {steps.map((s, i) => (
            <ProgressStep
              key={s}
              id={`co-${i}`}
              titleId={`co-${i}-t`}
              variant={i < stepIndex || (i === 2 && finished) ? "success" : i === stepIndex ? "info" : "pending"}
              isCurrent={i === stepIndex}
            >
              {s}
            </ProgressStep>
          ))}
        </ProgressStepper>
      </PageSection>

      {step === "review" && (
        <PageSection>
          <Card>
            <CardHeader>
              <CardTitle component="h2">
                {groups.length} consumer groups were captured with this recovery point
              </CardTitle>
            </CardHeader>
            <CardBody>
              <Flex gap={{ default: "gapMd" }} className="lw-chips">
                <span><VerdictStatus verdict="ready" /> {groups.filter((g) => verdictOf(g) === "ready").length} can switch now</span>
                <span><VerdictStatus verdict="stopConsumers" /> {waiting.length} still have consumers connected</span>
                <span><VerdictStatus verdict="blocked" /> {blocked.length} cannot be switched</span>
              </Flex>
              <Legend />
              <ReviewTable groups={groups} selected={selected} setSelected={setSelected} target={target} />
              {waiting.length > 0 && (
                <Alert
                  isInline
                  variant="warning"
                  title={`${waiting.map((g) => g.group).join(", ")} still ${waiting.length === 1 ? "has" : "have"} consumers connected to ${target}`}
                  actionLinks={
                    <Button variant="link" isInline isLoading={checking} onClick={checkAgain}>
                      {checking ? "Checking" : "Check again"}
                    </Button>
                  }
                >
                  A group is only switched while no consumer is connected, so a live group is never moved under its feet.
                  Stop those consumers, then check again.
                </Alert>
              )}
              {blocked.length > 0 && (
                <Alert isInline variant="danger" title={`${blocked.map((g) => g.group).join(" and ")} will be left out`} className="lw-gap-top">
                  Each has a position with no restored record to resume from. Open the group above to see why. Reset it by
                  hand on {target} if its application needs to run there.
                </Alert>
              )}
            </CardBody>
          </Card>
          <Flex className="lw-gap-top" gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
            <FlexItem>
              <Button variant="primary" isDisabled={chosen.length === 0} onClick={() => setStep("confirm")}>
                Continue with {chosen.length} {chosen.length === 1 ? "group" : "groups"}
              </Button>
            </FlexItem>
            <FlexItem>
              <Button variant="link" onClick={() => void navigate({ to: "/$ns/restores/$name", params: { ns, name } })}>
                Back to the restore
              </Button>
            </FlexItem>
          </Flex>
        </PageSection>
      )}

      {step === "confirm" && (
        <PageSection>
          <Card>
            <CardHeader>
              <CardTitle component="h2">
                Switch {chosen.length} {chosen.length === 1 ? "group" : "groups"} over on {target}
              </CardTitle>
            </CardHeader>
            <CardBody>
              <DescriptionList isHorizontal isCompact horizontalTermWidthModifier={{ default: "18ch" }}>
                <DescriptionListGroup>
                  <DescriptionListTerm>Groups</DescriptionListTerm>
                  <DescriptionListDescription>{chosen.map((g) => g.group).join(", ")}</DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Positions committed</DescriptionListTerm>
                  <DescriptionListDescription>
                    {positions.length} ({positions.filter((p) => p.mapping === "exact").length} exact,{" "}
                    {positions.filter((p) => p.mapping === "approximate").length} approximate)
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Saved first</DescriptionListTerm>
                  <DescriptionListDescription>
                    The groups' current positions on {target}, kept in the switchover record {name}-switchover.
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Left out</DescriptionListTerm>
                  <DescriptionListDescription>
                    {groups.filter((g) => !selected.has(g.group)).map((g) => g.group).join(", ") || "None"}
                  </DescriptionListDescription>
                </DescriptionListGroup>
                <DescriptionListGroup>
                  <DescriptionListTerm>Approval</DescriptionListTerm>
                  <DescriptionListDescription>One-person confirmation in {ns}: you confirm here.</DescriptionListDescription>
                </DescriptionListGroup>
              </DescriptionList>
              <Alert isInline variant="info" title="Undoing a switchover has a limit" className="lw-gap-top">
                The saved positions can be put back only until a consumer commits on {target}. Once your applications run,
                the switchover record is a history of what was there, not an undo.
              </Alert>
              <Title headingLevel="h3" size="md" className="lw-gap-top">
                Confirm
              </Title>
              <Checkbox
                id="stopped"
                isChecked={stopped}
                onChange={(_e, on) => setStopped(on)}
                label="The consumers of these groups are stopped, and stay stopped until this finishes."
              />
              <FormGroup label={`Type ${target} to confirm`} fieldId="confirm-target" className="lw-gap-top lw-confirm">
                <TextInput id="confirm-target" value={typed} onChange={(_e, v) => setTyped(v)} autoComplete="off" />
                <FormHelperText>
                  <HelperText>
                    <HelperTextItem>The cluster whose consumer group positions change.</HelperTextItem>
                  </HelperText>
                </FormHelperText>
              </FormGroup>
            </CardBody>
          </Card>
          <Flex className="lw-gap-top" gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
            <FlexItem>
              <Button variant="danger" isDisabled={!stopped || typed !== target} onClick={() => setStep("done")}>
                Switch over {chosen.length} {chosen.length === 1 ? "group" : "groups"}
              </Button>
            </FlexItem>
            <FlexItem>
              <Button variant="secondary" onClick={() => setStep("review")}>
                Back
              </Button>
            </FlexItem>
          </Flex>
        </PageSection>
      )}

      {step === "done" && (
        <PageSection>
          <Card>
            <CardHeader>
              <CardTitle component="h2">{finished ? "Switched over" : "Committing positions"}</CardTitle>
            </CardHeader>
            <CardBody>
              <Apply groups={chosen} onFinished={markFinished} />
              {finished && (
                <Alert isInline variant="success" title={`Start your consumers on ${target}`}>
                  {chosen.map((g) => g.group).join(" and ")} now resume at the translated positions. The switchover record{" "}
                  {name}-switchover lists every position before and after, and it is kept with the restore's evidence.
                </Alert>
              )}
              {finished && blocked.length > 0 && (
                <Content component="p" className="lw-gap-top">
                  Still to do by hand: {blocked.map((g) => g.group).join(", ")}.
                </Content>
              )}
            </CardBody>
          </Card>
          {finished && (
            <Flex className="lw-gap-top" gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
              <FlexItem>
                <Button variant="primary" onClick={() => void navigate({ to: "/$ns/restores/$name", params: { ns, name } })}>
                  Back to the restore
                </Button>
              </FlexItem>
              <FlexItem>
                <Button variant="link" onClick={() => void navigate({ to: "/$ns", params: { ns } })}>
                  Overview
                </Button>
              </FlexItem>
            </Flex>
          )}
        </PageSection>
      )}
    </>
  );
}
