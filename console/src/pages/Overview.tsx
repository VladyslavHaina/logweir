// The first page: is this namespace recoverable right now, and what needs you?
import { useRef, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import {
  Alert,
  AlertActionCloseButton,
  AlertGroup,
  Button,
  Card,
  CardBody,
  CardFooter,
  CardHeader,
  CardTitle,
  Content,
  Dropdown,
  DropdownItem,
  DropdownList,
  Flex,
  Gallery,
  Grid,
  GridItem,
  MenuToggle,
  PageSection,
  Progress,
  ProgressMeasureLocation,
  ProgressSize,
  Stack,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import ErrorIcon from "@patternfly/react-icons/dist/esm/icons/rh-ui-error-fill-icon";
import WarningIcon from "@patternfly/react-icons/dist/esm/icons/rh-ui-warning-fill-icon";
import CheckIcon from "@patternfly/react-icons/dist/esm/icons/rh-ui-check-circle-fill-icon";
import ArrowRightIcon from "@patternfly/react-icons/dist/esm/icons/arrow-right-icon";
import { useBackUpNow, useBackups, useConnections, useProtection, useRestores, useSchedules } from "../api/queries";
import type { Backup, ProtectionPolicy, Restore, Schedule } from "../api/types";
import { PageHeader } from "../components/PageHeader";
import { HealthStatus, RunStatus } from "../components/Status";
import { ErrorState, LoadingRows } from "../components/States";
import { When, duration, useNow } from "../components/When";
import { CompactList, useNarrow } from "../components/CompactList";
import { attentionItems, useAttentionReadable, type Attention } from "../app/attention";
import { ageVsObjective } from "../app/policyAge";
import { ApiError } from "../api/client";
import { can, useSessionDoc } from "../api/session";
import { activity, backupDay, freshestPoint, protectionCount, strip } from "./overviewData";
import { GettingStarted } from "./GettingStarted";

function BackUpNow({ ns, schedules }: { ns: string; schedules: Schedule[] }) {
  const [open, setOpen] = useState(false);
  const backUp = useBackUpNow(ns);
  const [toast, setToast] = useState<{ variant: "info" | "danger"; title: string; body: string } | null>(null);
  // A request with no answer may or may not have created the run: the next click for the
  // same schedule sends the same Idempotency-Key, so it names that run, never a second one.
  const unanswered = useRef(new Map<string, string>());
  const start = (schedule: string) => {
    const key = unanswered.current.get(schedule) ?? `logweir-console.backup.${crypto.randomUUID()}`;
    backUp.mutate(
      { schedule, key },
      {
        onSuccess: (b) => {
          unanswered.current.delete(schedule);
          setToast({ variant: "info", title: `Backup ${b.name} started`, body: "It appears under Recent activity and updates as it runs." });
        },
        onError: (error) => {
          const noAnswer = !(error instanceof ApiError) || error.status === 0 || error.status >= 500;
          if (noAnswer) unanswered.current.set(schedule, key);
          else unanswered.current.delete(schedule);
          setToast({
            variant: "danger",
            title: noAnswer ? `${schedule}: the backup may or may not have started` : `${schedule}: the backup was not started`,
            body: noAnswer
              ? `${error.message} Backing up ${schedule} again is safe: it names the same run.`
              : error.message,
          });
        },
      },
    );
  };
  return (
    <>
      <Dropdown
        isOpen={open}
        onOpenChange={setOpen}
        toggle={(ref) => (
          <MenuToggle ref={ref} onClick={() => setOpen(!open)} isExpanded={open} isDisabled={schedules.length === 0 || backUp.isPending}>
            Back up now
          </MenuToggle>
        )}
      >
        <DropdownList>
          {schedules.map((s) => (
            <DropdownItem
              key={s.name}
              description={`${s.topics.length ? `${s.topics.length} topics` : "all user topics"} from ${s.sourceRef.name}`}
              onClick={() => {
                setOpen(false);
                start(s.name);
              }}
            >
              {s.name}
            </DropdownItem>
          ))}
        </DropdownList>
      </Dropdown>
      {toast && (
        <AlertGroup isToast isLiveRegion>
          <Alert
            variant={toast.variant}
            title={toast.title}
            timeout={toast.variant === "info" ? 6000 : false}
            onTimeout={() => setToast(null)}
            actionClose={<AlertActionCloseButton onClose={() => setToast(null)} />}
          >
            {toast.body}
          </Alert>
        </AlertGroup>
      )}
    </>
  );
}

function Stat({ title, value, detail, tone }: { title: string; value: string; detail: React.ReactNode; tone?: "danger" | "warning" | "success" }) {
  const Icon = tone === "danger" ? ErrorIcon : tone === "warning" ? WarningIcon : tone === "success" ? CheckIcon : null;
  return (
    <Card isFullHeight className="lw-stat" component="section" aria-label={title}>
      <CardHeader>
        <CardTitle component="h2" className="lw-stat__title">
          {title}
        </CardTitle>
      </CardHeader>
      <CardBody>
        <Flex alignItems={{ default: "alignItemsCenter" }} gap={{ default: "gapSm" }} flexWrap={{ default: "nowrap" }}>
          {Icon && <Icon className={`lw-tone-${tone}`} aria-hidden="true" />}
          <span className="lw-stat__value">{value}</span>
        </Flex>
        <div className="lw-subtle lw-stat__detail">{detail}</div>
      </CardBody>
    </Card>
  );
}

function topicsOf(p: ProtectionPolicy): string {
  // Absent means "the topics of the newest matching point" (the API's own reading).
  if (!p.protects.topics) return "topics of its newest point";
  const n = p.protects.topics.length;
  return `${n} ${n === 1 ? "topic" : "topics"}`;
}

function PolicyTable({ policies, now }: { policies: ProtectionPolicy[]; now: number }) {
  return (
    <Table aria-label="Protection policies" variant="compact" gridBreakPoint="grid-md">
      <Thead>
        <Tr>
          <Th>Policy</Th>
          <Th>Status</Th>
          <Th width={30}>Recovery point age</Th>
          <Th>Source</Th>
        </Tr>
      </Thead>
      <Tbody>
        {policies.map((p) => {
          const age = ageVsObjective(p, now);
          return (
            <Tr key={p.name}>
              <Td dataLabel="Policy">
                <strong className="lw-break">{p.name}</strong>
                <div className="lw-subtle">{topicsOf(p)}</div>
              </Td>
              <Td dataLabel="Status">
                <HealthStatus health={p.health} />
              </Td>
              <Td dataLabel="Newest point">
                <Progress
                  value={age.percent}
                  variant={age.variant}
                  size={ProgressSize.sm}
                  measureLocation={ProgressMeasureLocation.none}
                  aria-label={`${p.name}: newest recovery point ${age.text}`}
                />
                <div className="lw-subtle">
                  {p.lastAvailablePoint ? <When value={p.lastAvailablePoint.recoveryPointAt} /> : "None yet"} · objective {duration(p.objectives.maxRecoveryPointAgeSeconds)}
                </div>
              </Td>
              <Td dataLabel="Source" className="lw-break">
                {p.protects.sourceRef.name}
              </Td>
            </Tr>
          );
        })}
      </Tbody>
    </Table>
  );
}

function NeedsAttention({ items }: { items: Attention[] }) {
  if (items.length === 0) {
    return (
      <Content component="p" className="lw-subtle">
        <CheckIcon className="lw-tone-success" /> Nothing needs you right now.
      </Content>
    );
  }
  return (
    <ul className="lw-attention lw-plain-links">
      {items.map((item) => (
        <li key={item.key}>
          <span className="lw-attention__icon">
            {item.tone === "danger" ? <ErrorIcon className="lw-tone-danger" /> : <WarningIcon className="lw-tone-warning" />}
          </span>
          <div>
            <strong>{item.title}</strong>
            <div className="lw-subtle">{item.detail}</div>
            <a href={`#${item.to}`}>
              {item.action} <ArrowRightIcon />
            </a>
          </div>
        </li>
      ))}
    </ul>
  );
}

function RunStrip({ ns, schedule, runs }: { ns: string; schedule: string; runs: Backup[] }) {
  const cells = strip(runs, schedule);
  return (
    <div className="lw-strip">
      <div className="lw-strip__name">
        <strong className="lw-break">{schedule}</strong>
        <span className="lw-subtle"> · {cells.length ? `last ${cells.length} ${cells.length === 1 ? "run" : "runs"}` : "no runs yet"}</span>
      </div>
      {cells.length > 0 && (
        <ol className="lw-strip__cells" aria-label={`${schedule}: last ${cells.length} runs, oldest first`}>
          {cells.map((c) => (
            <li key={c.backup.name}>
              <Link
                to="/$ns/backups/$name"
                params={{ ns, name: c.target.name }}
                className={`lw-strip__cell lw-strip__cell--${c.cls}`}
                title={`${c.at}: ${c.word}`}
                aria-label={`${c.at}, ${c.word}`}
              />
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}

function Activity({ ns, backups, restores }: { ns: string; backups: Backup[]; restores: Restore[] }) {
  const rows = activity(backups, restores);
  if (rows.length === 0) {
    return (
      <Content component="p" className="lw-subtle">
        No backups or restores yet.
      </Content>
    );
  }
  return (
    <CompactList
      flush
      label="Recent backups and restores"
      items={rows.map((row) => ({
        key: row.kind + row.name,
        href: `#/${ns}/${row.kind === "Backup" ? "backups" : "restores"}/${row.name}`,
        title: row.name,
        meta: (
          <>
            {row.kind} · <When value={row.at} />
          </>
        ),
        status: <RunStatus op={row.op} />,
      }))}
    />
  );
}

/** Phones: a policy is one row, its name, its status and the newest point's age. */
function PolicyList({ ns, policies, now }: { ns: string; policies: ProtectionPolicy[]; now: number }) {
  return (
    <CompactList
      flush
      label="Protection policies"
      items={policies.map((p) => ({
        key: p.name,
        href: `#/${ns}/protection`,
        title: p.name,
        meta: `${ageVsObjective(p, now).text} · objective ${duration(p.objectives.maxRecoveryPointAgeSeconds)}`,
        status: <HealthStatus health={p.health} />,
      }))}
    />
  );
}

/** Fetching for the first time; a read the session does not grant is idle, never pending. */
function loading(q: { isLoading: boolean }) {
  return q.isLoading;
}

function Empty({ text }: { text: string }) {
  return (
    <Content component="p" className="lw-subtle">
      {text}
    </Content>
  );
}

export function Overview() {
  const { ns } = useParams({ from: "/$ns/" });
  const navigate = useNavigate();
  const session = useSessionDoc();
  const protection = useProtection(ns);
  const backups = useBackups(ns);
  const restores = useRestores(ns);
  const connections = useConnections(ns);
  const schedules = useSchedules(ns);
  const narrow = useNarrow();
  const now = useNow();

  // What this role may read here decides which numbers and cards exist (docs/api.md, role table).
  const may = {
    protection: can(session, ns, "protection"),
    backups: can(session, ns, "backupsRead"),
    restores: can(session, ns, "restoresRead"),
    connections: can(session, ns, "connectionsRead"),
    schedules: can(session, ns, "schedulesRead"),
    backUp: can(session, ns, "manualBackupCreate") && can(session, ns, "schedulesRead"),
    restore: can(session, ns, "restoreCreate"),
  };
  const attentionReadable = useAttentionReadable(ns);

  const queries = [protection, backups, restores, connections, schedules];
  const failed = queries.find((q) => q.isError);

  const empty = connections.data?.length === 0;
  const header = (
    <PageHeader
      title="Overview"
      description={`Whether ${ns} can be recovered right now, and what needs you.`}
      actions={
        empty || !(may.backUp || may.restore) ? undefined : (
          <Flex gap={{ default: "gapSm" }}>
            {may.backUp && <BackUpNow ns={ns} schedules={schedules.data ?? []} />}
            {may.restore && (
              <Button variant="primary" onClick={() => void navigate({ to: "/$ns/restores/new", params: { ns }, search: {} })}>
                Restore
              </Button>
            )}
          </Flex>
        )
      }
    />
  );

  if (failed) {
    return (
      <>
        {header}
        <PageSection>
          <ErrorState error={failed.error} onRetry={() => queries.forEach((q) => void q.refetch())} />
        </PageSection>
      </>
    );
  }
  if (queries.some(loading)) {
    return (
      <>
        {header}
        <PageSection>
          <Gallery hasGutter minWidths={{ default: "220px" }}>
            {[0, 1, 2, 3].map((i) => (
              <Card key={i}>
                <CardBody>
                  <LoadingRows rows={2} label="Loading the overview" />
                </CardBody>
              </Card>
            ))}
          </Gallery>
        </PageSection>
      </>
    );
  }

  const policies = protection.data ?? [];
  const runs = backups.data ?? [];
  const conns = connections.data ?? [];
  if (may.connections && conns.length === 0) {
    return (
      <>
        {header}
        <GettingStarted ns={ns} />
      </>
    );
  }

  const count = protectionCount(policies);
  const freshest = freshestPoint(policies, now);
  const day = backupDay(runs, now);
  const attention = attentionReadable ? attentionItems(ns, policies, runs, conns, now) : [];
  const scheduleNames = (schedules.data ?? []).map((s) => s.name);

  return (
    <>
      {header}
      <PageSection>
        <Gallery hasGutter minWidths={{ default: "150px", sm: "220px" }}>
          {may.protection && (
            <Stat
              title="Protection"
              value={count.total ? `${count.healthy} of ${count.total}` : "None"}
              tone={count.total === 0 ? undefined : count.healthy === count.total ? "success" : "danger"}
              detail={count.total ? "policies meet their objective" : "No protection policy yet"}
            />
          )}
          {may.protection && (
            <Stat
              title="Newest recovery point"
              value={freshest ? `${duration(freshest.ageSeconds)} old` : "None"}
              tone={freshest ? (freshest.withinObjective ? "success" : "danger") : count.total ? "danger" : undefined}
              detail={
                freshest
                  ? `${freshest.policy.name}: ${freshest.verified ? "verified" : "not verified"}, objective ${duration(freshest.policy.objectives.maxRecoveryPointAgeSeconds)}`
                  : "No available point yet"
              }
            />
          )}
          {may.backups && (
            <Stat
              title="Backups, last 24 hours"
              value={`${day.verified} verified`}
              tone={day.failed > 0 ? "warning" : day.verified > 0 ? "success" : undefined}
              detail={`${day.failed} failed${day.running ? ` · ${day.running} running` : ""}`}
            />
          )}
          {attentionReadable && (
            <Stat
              title="Needs attention"
              value={String(attention.length)}
              tone={attention.some((a) => a.tone === "danger") ? "danger" : attention.length ? "warning" : "success"}
              detail={attention.length ? "items, listed below" : "Nothing right now"}
            />
          )}
        </Gallery>
      </PageSection>
      {attentionReadable && (
        <PageSection>
          <Card>
            <CardHeader>
              <CardTitle component="h2">Needs attention</CardTitle>
            </CardHeader>
            <CardBody>
              <NeedsAttention items={attention} />
            </CardBody>
          </Card>
        </PageSection>
      )}
      <PageSection>
        <Grid hasGutter>
          {(may.protection || (may.schedules && may.backups)) && (
            <GridItem lg={7} xl={8}>
              <Stack hasGutter>
                {may.protection && (
                  <Card>
                    <CardHeader>
                      <CardTitle component="h2">Protection policies</CardTitle>
                    </CardHeader>
                    <CardBody>
                      {policies.length === 0 ? (
                        <Empty text="No protection policy names an objective for this namespace yet." />
                      ) : narrow ? (
                        <PolicyList ns={ns} policies={policies} now={now} />
                      ) : (
                        <PolicyTable policies={policies} now={now} />
                      )}
                    </CardBody>
                    <CardFooter>
                      <Link to="/$ns/$section" params={{ ns, section: "protection" }}>
                        All protection policies <ArrowRightIcon />
                      </Link>
                    </CardFooter>
                  </Card>
                )}
                {may.schedules && may.backups && (
                  <Card>
                    <CardHeader>
                      <CardTitle component="h2">Recent runs per schedule</CardTitle>
                    </CardHeader>
                    <CardBody>
                      {scheduleNames.length === 0 ? (
                        <Empty text="No schedule yet." />
                      ) : (
                        <>
                          {scheduleNames.map((name) => (
                            <RunStrip key={name} ns={ns} schedule={name} runs={runs} />
                          ))}
                          <ul className="lw-legend" aria-label="Legend">
                            <li><span className="lw-strip__cell lw-strip__cell--ok" /> Verified</li>
                            <li><span className="lw-strip__cell lw-strip__cell--retried" /> Verified on retry</li>
                            <li><span className="lw-strip__cell lw-strip__cell--failed" /> Failed</li>
                            <li><span className="lw-strip__cell lw-strip__cell--running" /> In progress</li>
                          </ul>
                        </>
                      )}
                    </CardBody>
                  </Card>
                )}
              </Stack>
            </GridItem>
          )}
          {(may.backups || may.restores) && (
            <GridItem lg={may.protection || (may.schedules && may.backups) ? 5 : 12} xl={may.protection || (may.schedules && may.backups) ? 4 : 12}>
              <Card isFullHeight>
                <CardHeader>
                  <CardTitle component="h2">Recent activity</CardTitle>
                </CardHeader>
                <CardBody>
                  <Activity ns={ns} backups={runs} restores={restores.data ?? []} />
                </CardBody>
                {may.backups && (
                  <CardFooter>
                    <Link to="/$ns/backups" params={{ ns }}>
                      All backups <ArrowRightIcon />
                    </Link>
                  </CardFooter>
                )}
              </Card>
            </GridItem>
          )}
        </Grid>
      </PageSection>
    </>
  );
}
