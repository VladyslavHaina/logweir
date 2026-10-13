// One schedule: when it runs, what it backs up, what its last slot did, its
// runs and their recovery points, and retention for its destination. Every
// action on the page is bound to this schedule.
import { useState } from "react";
import { Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import {
  Alert,
  AlertActionCloseButton,
  Breadcrumb,
  BreadcrumbItem,
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Content,
  DescriptionList,
  ExpandableSection,
  Flex,
  Grid,
  GridItem,
  Label,
  LabelGroup,
  PageSection,
  Pagination,
  Stack,
} from "@patternfly/react-core";
import { ActionsColumn, Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import type { Backup } from "../../api/types";
import { ApiError } from "../../api/client";
import {
  useDestinationChoices,
  useRetentionPolicies,
  useRunHistory,
  useSchedule,
  useSetSuspension,
  type Schedule,
} from "../../api/schedules";
import { PageHeader } from "../../components/PageHeader";
import { RunStatus, StatusLabel, TriggerText } from "../../components/Status";
import { ErrorState, LoadingRows } from "../../components/States";
import { CompactList, useNarrow } from "../../components/CompactList";
import { When, count } from "../../components/When";
import { Ident } from "../../components/Ident";
import { BackUpNow } from "./BackUpNow";
import { RetentionCard } from "./Retention";
import { cadenceWords, latestPoint, outcomeOf, runsOf, zoneOf } from "./model";
import { NextRuns, ScheduleStatus, Term, WriteAlert, useGrant } from "./parts";

function Runs({ ns, runs, canRestore, label }: { ns: string; runs: Backup[]; canRestore: boolean; label: string }) {
  const narrow = useNarrow();
  const navigate = useNavigate();
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState(10);
  const shown = runs.slice((page - 1) * perPage, page * perPage);
  const pager =
    runs.length > 10 ? (
      <Pagination
        itemCount={runs.length}
        perPage={perPage}
        page={page}
        onSetPage={(_e, p) => setPage(p)}
        onPerPageSelect={(_e, pp) => {
          setPerPage(pp);
          setPage(1);
        }}
        isCompact
        titles={{ paginationAriaLabel: `${label} pages` }}
      />
    ) : null;
  const restore = (b: Backup) => void navigate({ to: "/$ns/restores/new", params: { ns }, search: { backup: b.name } });
  if (narrow) {
    return (
      <>
        <CompactList
          flush
          label={label}
          items={shown.map((b) => ({
            key: b.name,
            href: `#/${ns}/backups/${b.name}`,
            title: b.name,
            meta: (
              <>
                <When value={b.createdAt} /> · {b.trigger?.kind === "Retry" ? `retry ${b.trigger.attempt}` : (b.trigger?.kind ?? "Scheduled").toLowerCase()}
                {b.scheduleRef?.generation ? ` · g${b.scheduleRef.generation}` : ""}
              </>
            ),
            status: <RunStatus op={b.operation} />,
          }))}
        />
        {pager && <div className="lw-pager">{pager}</div>}
      </>
    );
  }
  return (
    <>
      <Table aria-label={label} variant="compact" gridBreakPoint="grid-lg">
        <Thead>
          <Tr>
            <Th>Run</Th>
            <Th>Status</Th>
            <Th>Trigger</Th>
            <Th>Started</Th>
            <Th screenReaderText="Actions" />
          </Tr>
        </Thead>
        <Tbody>
          {shown.map((b) => (
            <Tr key={b.name}>
              <Td dataLabel="Run" className="lw-name-cell">
                <Link to="/$ns/backups/$name" params={{ ns, name: b.name }} className="lw-break">
                  {b.name}
                </Link>
                <div className="lw-subtle lw-small">
                  {b.scheduleRef?.generation ? `revision g${b.scheduleRef.generation}` : "revision not recorded"}
                </div>
              </Td>
              <Td dataLabel="Status">
                <RunStatus op={b.operation} />
              </Td>
              <Td dataLabel="Trigger">
                <TriggerText b={b} />
              </Td>
              <Td dataLabel="Started">
                <When value={b.createdAt} />
              </Td>
              <Td isActionCell>
                <ActionsColumn
                  items={[
                    { title: "View details", onClick: () => void navigate({ to: "/$ns/backups/$name", params: { ns, name: b.name } }) },
                    ...(canRestore ? [{ title: "Restore this point", isDisabled: !b.operation.verifiedSuccess, onClick: () => restore(b) }] : []),
                  ]}
                />
              </Td>
            </Tr>
          ))}
        </Tbody>
      </Table>
      {pager && <div className="lw-pager">{pager}</div>}
    </>
  );
}

/** A disclosure that starts open where its runs are the page's point (a deleted schedule). */
function Section({ open, text, children }: { open: boolean; text: string; children: React.ReactNode }) {
  const [expanded, setExpanded] = useState(open);
  return (
    <ExpandableSection toggleText={text} isExpanded={expanded} onToggle={(_e, o) => setExpanded(o)}>
      {children}
    </ExpandableSection>
  );
}

function History({ ns, name, uid, canRestore }: { ns: string; name: string; uid: string | undefined; canRestore: boolean }) {
  const history = useRunHistory(ns);
  const navigate = useNavigate();
  if (history.isPending) return <LoadingRows rows={4} label="Loading the runs" />;
  if (history.isError) return <ErrorState error={history.error} onRetry={() => void history.refetch()} />;
  const groups = runsOf(name, uid, history.data.items);
  const latest = latestPoint(groups.mine);
  return (
    <Stack hasGutter>
      {groups.mine.length === 0 ? (
        <Content component="p" className="lw-subtle">
          {uid
            ? "This schedule has made no run yet. Its first one starts at its next slot, or use Back up now."
            : "This schedule no longer exists, so no run can be placed as its own; the runs that name it are below."}
        </Content>
      ) : (
        <>
          <Flex justifyContent={{ default: "justifyContentSpaceBetween" }} alignItems={{ default: "alignItemsCenter" }} gap={{ default: "gapSm" }}>
            <Content component="p" className="lw-subtle">
              {latest ? (
                <>
                  Latest recovery point: <strong>{latest.name}</strong>, <When value={latest.createdAt} />.
                </>
              ) : (
                "No run of this schedule has a verified recovery point yet."
              )}
            </Content>
            {latest && canRestore && (
              <Button variant="secondary" onClick={() => void navigate({ to: "/$ns/restores/new", params: { ns }, search: { backup: latest.name } })}>
                Restore the latest point
              </Button>
            )}
          </Flex>
          <Runs ns={ns} runs={groups.mine} canRestore={canRestore} label={`Runs of ${name}`} />
        </>
      )}
      {[...new Set(groups.earlier.map((b) => b.scheduleRef!.uid!))].map((identity, i) => {
        const runs = groups.earlier.filter((b) => b.scheduleRef?.uid === identity);
        return (
          <Section key={identity} open={!uid} text={uid ? `${runs.length} runs of an earlier schedule with this name` : `${runs.length} runs of ${name}, identity ${i + 1}`}>
            <Content component="p" className="lw-subtle">
              They carry schedule identity {identity}. A schedule deleted and created again under one name is a different
              schedule, so these are never merged with another identity's runs. Their points can still be restored.
            </Content>
            <Runs ns={ns} runs={runs} canRestore={canRestore} label={`Runs of schedule identity ${identity}`} />
          </Section>
        );
      })}
      {groups.legacy.length > 0 && (
        <Section open={!uid} text={`${groups.legacy.length} runs naming this schedule without its identity`}>
          <Content component="p" className="lw-subtle">
            They record no schedule identity, so nothing tells whether this schedule or an earlier one of the same name
            made them. They are never offered as this schedule's latest point; each can still be restored on its own.
          </Content>
          <Runs ns={ns} runs={groups.legacy} canRestore={canRestore} label="Runs naming this schedule without its identity" />
        </Section>
      )}
      {history.data.more && (
        <Content component="p" className="lw-subtle lw-small">
          Only the newest 200 runs of this namespace were read; older runs of this schedule are on the Backups page.
        </Content>
      )}
    </Stack>
  );
}

const or = (value: number | null | undefined, fallback: string, unit = " s") => (typeof value === "number" ? `${value}${unit}` : fallback);

function WhatItBacksUp({ ns, s }: { ns: string; s: Schedule }) {
  const destinations = useDestinationChoices(ns);
  const dest = s.destinationRef ? destinations.data?.find((d) => d.name === s.destinationRef!.name) : undefined;
  const exclude = s.allUserTopics?.exclude;
  return (
    <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "14ch" }}>
      <Term term="Source">
        <a href={`#/${ns}/connections`}>{s.sourceRef.name}</a>
      </Term>
      <Term term="Topics">
        {s.allUserTopics ? (
          <>
            All user topics, found when each run starts. When discovery cannot prove it saw every topic:{" "}
            {s.allUserTopics.incompleteDiscovery === "Refuse" ? "the run is refused" : "it backs up the visible topics and records that"}.
            {exclude?.topics?.length || exclude?.prefixes?.length ? (
              <div className="lw-subtle lw-small">
                Left out: {[...(exclude.topics ?? []), ...(exclude.prefixes ?? []).map((p) => `${p}…`)].join(", ")}
              </div>
            ) : null}
          </>
        ) : s.topics.length === 0 ? (
          <StatusLabel tone="danger" text="None" title="No topic and no dynamic selection: no run backs anything up" />
        ) : (
          <LabelGroup numLabels={8}>
            {s.topics.map((t) => (
              <Label key={t} isCompact variant="outline">
                {t}
              </Label>
            ))}
          </LabelGroup>
        )}
      </Term>
      <Term term="Written to">
        {s.destinationRef ? (
          <>
            <a href={`#/${ns}/destinations`}>{s.destinationRef.name}</a>
            {dest ? (
              <div className="lw-subtle lw-small lw-break">{dest.canonicalUrl}</div>
            ) : destinations.isError ? (
              <div className="lw-subtle lw-small">The destinations could not be read, so where this is cannot be shown.</div>
            ) : destinations.data ? (
              <div className="lw-subtle lw-small">No destination of this name is in the namespace now.</div>
            ) : null}
          </>
        ) : (
          <>
            <span className="lw-break lw-mono">{s.archive.url}</span>
            <div className="lw-subtle lw-small">An archive URL{s.archive.credentialRef ? `, credential Secret ${s.archive.credentialRef.name}` : ""}</div>
          </>
        )}
      </Term>
      <Term term="Overlapping runs">{s.concurrencyPolicy === "Allow" ? "Allowed" : "Never"}</Term>
      <Term term="Starting deadline">{or(s.startingDeadlineSeconds, "Default (3600 s)")}</Term>
      <Term term="Missed slots">{s.catchUpPolicy === "Latest" ? "Run the latest once" : s.catchUpPolicy === "None" ? "Count and skip" : "Default (count and skip)"}</Term>
      <Term term="Retries">
        {s.retry ? `${s.retry.maxRetries}, ${or(s.retry.delaySeconds, "300 s")} apart` : "Default (none)"}
      </Term>
      <Term term="Run deadline">{or(s.activeDeadlineSeconds, "Default (3600 s)")}</Term>
      <Term term="Created">
        <When value={s.createdAt} exact />
      </Term>
    </DescriptionList>
  );
}

function LastSlot({ ns, s }: { ns: string; s: Schedule }) {
  const slot = s.status.lastSlot;
  const missed = s.status.missedSlots;
  const ready = s.status.ready;
  const active = s.status.activeRuns;
  return (
    <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "14ch" }}>
      <Term term="Controller">
        {ready ? (
          <>
            {ready.status === "True" ? "Ready" : ready.status === "False" ? "Not ready" : "Unknown"}
            {ready.reason ? <span className="lw-reason"> {ready.reason}</span> : null}
            {ready.message && <div className="lw-subtle lw-small">{ready.message}</div>}
          </>
        ) : (
          <span className="lw-subtle">Not evaluated yet</span>
        )}
      </Term>
      <Term term="Last slot">
        {slot ? (
          <>
            {slot.disposition}
            <span className="lw-reason"> {slot.reason}</span>
            <div className="lw-subtle lw-small">
              due <When value={slot.dueAt} />, attempt {slot.attempt}
              {slot.backupRef ? (
                <>
                  {" · "}
                  <Link to="/$ns/backups/$name" params={{ ns, name: slot.backupRef.name }}>
                    {slot.backupRef.name}
                  </Link>
                </>
              ) : null}
            </div>
          </>
        ) : (
          <span className="lw-subtle">Not recorded</span>
        )}
      </Term>
      <Term term="Last fired">
        <When value={s.status.lastFireTime} />
      </Term>
      <Term term="Missed slots">
        {missed ? (
          <>
            {missed.countCapped ? `At least ${count(missed.count)}` : count(missed.count)}
            {missed.recent?.[0] && (
              <div className="lw-subtle lw-small">
                newest {missed.recent[0].slot}: <span className="lw-reason">{missed.recent[0].reason}</span>
              </div>
            )}
          </>
        ) : (
          <span className="lw-subtle">Not recorded</span>
        )}
      </Term>
      <Term term="Running now">
        {active === undefined || active === null ? (
          <span className="lw-subtle">Not recorded</span>
        ) : active.length === 0 ? (
          "None"
        ) : (
          active.map((r) => (
            <div key={r.name}>
              <Link to="/$ns/backups/$name" params={{ ns, name: r.name }}>
                {r.name}
              </Link>{" "}
              <span className="lw-subtle lw-small">{r.kind === "Retry" ? `retry ${r.attempt}` : r.kind.toLowerCase()}</span>
            </div>
          ))
        )}
      </Term>
    </DescriptionList>
  );
}

function WhenItRuns({ s }: { s: Schedule }) {
  const zone = zoneOf(s);
  const policy = s.status.policy;
  const behind = typeof s.generation === "number" && typeof s.status.observedGeneration === "number" && s.status.observedGeneration < s.generation;
  return (
    <Stack hasGutter>
      <Content component="p">
        {cadenceWords(s.preset, s.schedule)}, in {zone}. Saved as <code className="lw-mono lw-nowrap">{s.schedule}</code>.
        {!s.timeZone && " No time zone is set, so the fields are read as UTC times."}
      </Content>
      {behind && (
        <Alert isInline isPlain variant="info" title={`Revision g${s.generation} is not evaluated yet`}>
          The controller has evaluated g{s.status.observedGeneration}; the runs below are still that revision's.
        </Alert>
      )}
      <NextRuns runs={s.status.nextRuns} zone={zone} label={`The next runs of ${s.name}`} />
      <ExpandableSection toggleText="Technical details">
        <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "16ch" }}>
          <Term term="Revision">{typeof s.generation === "number" ? `g${s.generation}` : "Not recorded"}</Term>
          <Term term="In force since">
            <When value={policy?.effectiveSince} exact />
          </Term>
          <Term term="Run policy digest">{policy ? <Ident value={policy.runPolicySha256} label="run policy digest" /> : <span className="lw-subtle">Not recorded</span>}</Term>
          <Term term="Time zone database">{policy?.tzdb ?? <span className="lw-subtle">Not recorded</span>}</Term>
        </DescriptionList>
      </ExpandableSection>
    </Stack>
  );
}

function Gone({ ns, name }: { ns: string; name: string }) {
  return (
    <>
      <PageSection>
        <Alert isInline variant="info" title="This schedule no longer exists">
          Deleting a schedule stops its future slots and removes nothing else: every run it made, and every recovery point,
          is still here. A new schedule with the same name is a different schedule and does not inherit these runs.
        </Alert>
      </PageSection>
      <PageSection>
        <Card>
          <CardHeader>
            <CardTitle component="h2">Runs naming {name}</CardTitle>
          </CardHeader>
          <CardBody>
            <History ns={ns} name={name} uid={undefined} canRestore />
          </CardBody>
        </Card>
      </PageSection>
    </>
  );
}

export function ScheduleDetail() {
  const { ns, name } = useParams({ from: "/$ns/schedules/$name" });
  const search = useSearch({ from: "/$ns/schedules/$name" });
  const navigate = useNavigate();
  const schedule = useSchedule(ns, name);
  const retention = useRetentionPolicies(ns);
  const grant = useGrant(ns);
  const suspension = useSetSuspension(ns, name);
  const [backUp, setBackUp] = useState(false);
  const [flash, setFlash] = useState(search.done);

  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/schedules" params={{ ns }}>Schedules</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );

  if (schedule.isPending) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <LoadingRows rows={6} label="Loading the schedule" />
        </PageSection>
      </>
    );
  }
  if (schedule.isError) {
    const gone = schedule.error instanceof ApiError && schedule.error.status === 404;
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} status={gone ? <StatusLabel tone="neutral" text="Deleted" /> : undefined} />
        {gone ? (
          <Gone ns={ns} name={name} />
        ) : (
          <PageSection>
            <ErrorState error={schedule.error} onRetry={() => void schedule.refetch()} />
          </PageSection>
        )}
      </>
    );
  }

  const s = schedule.data;
  const toggle = () => suspension.mutate({ suspended: !s.suspended, expectedResourceVersion: s.resourceVersion });
  const suspensionOutcome = suspension.isError ? outcomeOf(suspension.error) : null;

  return (
    <>
      <PageHeader
        title={s.name}
        breadcrumb={breadcrumb}
        status={<ScheduleStatus schedule={s} />}
        description={`${cadenceWords(s.preset, s.schedule)} (${zoneOf(s)}) · ${s.allUserTopics ? "all user topics" : `${s.topics.length} topics`} from ${s.sourceRef.name}`}
        actions={
          <Flex gap={{ default: "gapSm" }}>
            {grant?.manualBackupCreate && (
              <Button variant="primary" onClick={() => setBackUp(true)}>
                Back up now
              </Button>
            )}
            {grant?.scheduleSetSuspension && (
              <Button variant="secondary" onClick={toggle} isLoading={suspension.isPending} isDisabled={suspension.isPending}>
                {s.suspended ? "Resume" : "Suspend"}
              </Button>
            )}
            {grant?.scheduleCreate && (
              <Button variant="secondary" onClick={() => void navigate({ to: "/$ns/schedules/$name/edit", params: { ns, name } })}>
                Edit policy
              </Button>
            )}
          </Flex>
        }
      />
      {(flash || suspension.isError) && (
        <PageSection>
          {flash && (
            <Alert
              isInline
              variant="success"
              title={flash === "created" ? `Schedule ${s.name} created` : `Policy saved as revision g${s.generation}`}
              actionClose={<AlertActionCloseButton onClose={() => setFlash(undefined)} />}
            >
              {flash === "created" ? "Its first run starts at the next slot below, or use Back up now." : "Runs already created keep the revision they copied."}
            </Alert>
          )}
          {suspension.isError && (
            <WriteAlert
              error={suspension.error}
              kind="command"
              subject="the schedule"
              verb={suspension.variables?.suspended === false ? "resumed" : "suspended"}
              onRetry={suspensionOutcome === "unknown" && suspension.variables ? () => suspension.mutate(suspension.variables!) : () => suspension.reset()}
              retryText={suspensionOutcome === "unknown" ? "Try again" : "Dismiss"}
            >
              {suspensionOutcome === "precondition" && <p>Somebody changed this schedule after this page read it. It has been read again: check it, then try again.</p>}
            </WriteAlert>
          )}
        </PageSection>
      )}
      <PageSection>
        <Grid hasGutter>
          <GridItem lg={7} xl={8}>
            <Stack hasGutter>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">When it runs</CardTitle>
                </CardHeader>
                <CardBody>
                  <WhenItRuns s={s} />
                </CardBody>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Runs and recovery points</CardTitle>
                </CardHeader>
                <CardBody>
                  <History ns={ns} name={s.name} uid={s.uid} canRestore={grant?.restoreCreate === true} />
                </CardBody>
              </Card>
            </Stack>
          </GridItem>
          <GridItem lg={5} xl={4}>
            <Stack hasGutter>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">What it backs up</CardTitle>
                </CardHeader>
                <CardBody>
                  <WhatItBacksUp ns={ns} s={s} />
                </CardBody>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Last slot</CardTitle>
                </CardHeader>
                <CardBody>
                  <LastSlot ns={ns} s={s} />
                </CardBody>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Retention</CardTitle>
                </CardHeader>
                <CardBody>
                  <RetentionCard schedule={s} policies={retention.data} failed={retention.isError} />
                </CardBody>
              </Card>
            </Stack>
          </GridItem>
        </Grid>
      </PageSection>
      {grant?.manualBackupCreate && <BackUpNow ns={ns} schedule={s} open={backUp} onClose={() => setBackUp(false)} />}
    </>
  );
}

