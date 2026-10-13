// The first page: is this namespace recoverable right now, and what needs you?
import { useState } from "react";
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
  ProgressVariant,
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
import { When, duration } from "../components/When";
import { CompactList, useNarrow } from "../components/CompactList";
import { DAY_MS, attentionItems, type Attention } from "../app/attention";
import { GettingStarted } from "./GettingStarted";

function BackUpNow({ ns, schedules }: { ns: string; schedules: Schedule[] }) {
  const [open, setOpen] = useState(false);
  const backUp = useBackUpNow(ns);
  const [started, setStarted] = useState<string | null>(null);
  return (
    <>
      <Dropdown
        isOpen={open}
        onOpenChange={setOpen}
        toggle={(ref) => (
          <MenuToggle ref={ref} onClick={() => setOpen(!open)} isExpanded={open} isDisabled={schedules.length === 0}>
            Back up now
          </MenuToggle>
        )}
      >
        <DropdownList>
          {schedules.map((s) => (
            <DropdownItem
              key={s.name}
              description={`${s.topics.length} topics from ${s.sourceRef.name}`}
              onClick={() => {
                setOpen(false);
                backUp.mutate(s.name, { onSuccess: (b) => setStarted(b.name) });
              }}
            >
              {s.name}
            </DropdownItem>
          ))}
        </DropdownList>
      </Dropdown>
      {started && (
        <AlertGroup isToast isLiveRegion>
          <Alert
            variant="info"
            title={`Backup ${started} started`}
            timeout={6000}
            onTimeout={() => setStarted(null)}
            actionClose={<AlertActionCloseButton onClose={() => setStarted(null)} />}
          >
            It appears under Recent activity and updates as it runs.
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

function ageVsObjective(p: ProtectionPolicy) {
  const age = p.lastAvailablePoint?.ageSeconds;
  const objective = p.objectives.maxRecoveryPointAgeSeconds;
  if (typeof age !== "number") return { percent: 100, variant: ProgressVariant.danger, text: "No recovery point" };
  const percent = Math.min(100, Math.round((age / objective) * 100));
  const variant = percent >= 100 ? ProgressVariant.danger : percent >= 75 ? ProgressVariant.warning : ProgressVariant.success;
  return { percent, variant, text: `${duration(age)} of ${duration(objective)}` };
}

function PolicyTable({ policies }: { policies: ProtectionPolicy[] }) {
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
          const age = ageVsObjective(p);
          return (
            <Tr key={p.name}>
              <Td dataLabel="Policy">
                <strong>{p.name}</strong>
                <div className="lw-subtle">{p.protects.topics?.length ?? 0} topics</div>
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
              <Td dataLabel="Source">{p.protects.sourceRef.name}</Td>
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
  const last = runs.filter((r) => r.trigger?.kind !== "Retry").slice(0, 24).reverse();
  const outcome = (b: Backup) => {
    const retry = runs.find((r) => r.trigger?.retryOf?.name === b.name);
    if (b.operation.state === "failed" && retry?.operation.verifiedSuccess) return { cls: "retried", word: "failed, then verified on retry", target: retry };
    if (!b.operation.terminal) return { cls: "running", word: "in progress", target: b };
    if (b.operation.verifiedSuccess) return { cls: "ok", word: "verified", target: b };
    return { cls: "failed", word: b.operation.state, target: b };
  };
  return (
    <div className="lw-strip">
      <div className="lw-strip__name">
        <strong>{schedule}</strong>
        <span className="lw-subtle"> · last {last.length} runs</span>
      </div>
      <ol className="lw-strip__cells" aria-label={`${schedule}: last ${last.length} runs, oldest first`}>
        {last.map((b) => {
          const o = outcome(b);
          const when = new Date(String(b.createdAt)).toISOString().slice(11, 16);
          return (
            <li key={b.name}>
              <Link
                to="/$ns/backups/$name"
                params={{ ns, name: o.target.name }}
                className={`lw-strip__cell lw-strip__cell--${o.cls}`}
                title={`${when} UTC: ${o.word}`}
                aria-label={`${when} UTC, ${o.word}`}
              />
            </li>
          );
        })}
      </ol>
    </div>
  );
}

function Activity({ ns, backups, restores }: { ns: string; backups: Backup[]; restores: Restore[] }) {
  const rows = [
    ...backups.map((b) => ({ kind: "Backup" as const, name: b.name, at: String(b.createdAt), op: b.operation })),
    ...restores.map((r) => ({ kind: "Restore" as const, name: r.name, at: String(r.createdAt), op: r.operation })),
  ]
    .sort((a, b) => b.at.localeCompare(a.at))
    .slice(0, 6);
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
function PolicyList({ ns, policies }: { ns: string; policies: ProtectionPolicy[] }) {
  return (
    <CompactList
      flush
      label="Protection policies"
      items={policies.map((p) => ({
        key: p.name,
        href: `#/${ns}/protection`,
        title: p.name,
        meta: `${ageVsObjective(p).text} · objective ${duration(p.objectives.maxRecoveryPointAgeSeconds)}`,
        status: <HealthStatus health={p.health} />,
      }))}
    />
  );
}

export function Overview() {
  const { ns } = useParams({ from: "/$ns/" });
  const navigate = useNavigate();
  const protection = useProtection(ns);
  const backups = useBackups(ns);
  const restores = useRestores(ns);
  const connections = useConnections(ns);
  const schedules = useSchedules(ns);
  const narrow = useNarrow();

  const queries = [protection, backups, restores, connections, schedules];
  const failed = queries.find((q) => q.isError);
  const loading = queries.some((q) => q.isPending);

  const empty = connections.data?.length === 0;
  const header = (
    <PageHeader
      title="Overview"
      description={`Whether ${ns} can be recovered right now, and what needs you.`}
      actions={
        empty ? undefined :
        <Flex gap={{ default: "gapSm" }}>
          <BackUpNow ns={ns} schedules={schedules.data ?? []} />
          <Button variant="primary" onClick={() => void navigate({ to: "/$ns/restores/new", params: { ns }, search: {} })}>
            Restore
          </Button>
        </Flex>
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
  if (loading) {
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
  if (conns.length === 0) {
    return (
      <>
        {header}
        <GettingStarted ns={ns} />
      </>
    );
  }

  const protectedCount = policies.filter((p) => p.health === "Healthy").length;
  const freshestPolicy = policies
    .filter((p) => p.lastAvailablePoint)
    .sort((a, b) => String(b.lastAvailablePoint?.recoveryPointAt).localeCompare(String(a.lastAvailablePoint?.recoveryPointAt)))[0];
  const freshest = freshestPolicy?.lastAvailablePoint;
  const lastDay = runs.filter((b) => Date.now() - Date.parse(String(b.createdAt)) < DAY_MS);
  const verified = lastDay.filter((b) => b.operation.verifiedSuccess).length;
  const failedRuns = lastDay.filter((b) => b.operation.state === "failed").length;
  const running = runs.filter((b) => !b.operation.terminal).length;
  const attention = attentionItems(ns, policies, runs, conns);

  return (
    <>
      {header}
      <PageSection>
        <Gallery hasGutter minWidths={{ default: "150px", sm: "220px" }}>
          <Stat
            title="Protection"
            value={`${protectedCount} of ${policies.length}`}
            tone={protectedCount === policies.length ? "success" : "danger"}
            detail="policies meet their objective"
          />
          <Stat
            title="Newest recovery point"
            value={freshest ? `${duration(freshest.ageSeconds ?? 0)} old` : "None"}
            tone={freshest ? "success" : "danger"}
            detail={
              freshestPolicy
                ? `${freshestPolicy.name}: verified, objective ${duration(freshestPolicy.objectives.maxRecoveryPointAgeSeconds)}`
                : "No verified point yet"
            }
          />
          <Stat
            title="Backups, last 24 hours"
            value={`${verified} verified`}
            tone={failedRuns > 0 ? "warning" : "success"}
            detail={`${failedRuns} failed${running ? ` · ${running} running` : ""}`}
          />
          <Stat
            title="Needs attention"
            value={String(attention.length)}
            tone={attention.some((a) => a.tone === "danger") ? "danger" : attention.length ? "warning" : "success"}
            detail={attention.length ? "items, listed below" : "Nothing right now"}
          />
        </Gallery>
      </PageSection>
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
      <PageSection>
        <Grid hasGutter>
          <GridItem lg={7} xl={8}>
            <Stack hasGutter>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Protection policies</CardTitle>
                </CardHeader>
                <CardBody>{narrow ? <PolicyList ns={ns} policies={policies} /> : <PolicyTable policies={policies} />}</CardBody>
                <CardFooter>
                  <Link to="/$ns/$section" params={{ ns, section: "protection" }}>
                    All protection policies <ArrowRightIcon />
                  </Link>
                </CardFooter>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Recent runs per schedule</CardTitle>
                </CardHeader>
                <CardBody>
                  {(schedules.data ?? []).map((s) => (
                    <RunStrip key={s.name} ns={ns} schedule={s.name} runs={runs.filter((b) => b.schedule === s.name || b.scheduleRef?.name === s.name)} />
                  ))}
                  <ul className="lw-legend" aria-label="Legend">
                    <li><span className="lw-strip__cell lw-strip__cell--ok" /> Verified</li>
                    <li><span className="lw-strip__cell lw-strip__cell--retried" /> Verified on retry</li>
                    <li><span className="lw-strip__cell lw-strip__cell--failed" /> Failed</li>
                    <li><span className="lw-strip__cell lw-strip__cell--running" /> In progress</li>
                  </ul>
                </CardBody>
              </Card>
            </Stack>
          </GridItem>
          <GridItem lg={5} xl={4}>
            <Card isFullHeight>
              <CardHeader>
                <CardTitle component="h2">Recent activity</CardTitle>
              </CardHeader>
              <CardBody>
                <Activity ns={ns} backups={runs} restores={restores.data ?? []} />
              </CardBody>
              <CardFooter>
                <Link to="/$ns/backups" params={{ ns }}>
                  All backups <ArrowRightIcon />
                </Link>
              </CardFooter>
            </Card>
          </GridItem>
        </Grid>
      </PageSection>
    </>
  );
}
