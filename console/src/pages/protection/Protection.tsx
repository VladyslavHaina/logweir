// Protection: is there a recovery point you could actually restore from, and is
// it young enough? A green, running schedule can still have none, so the
// policy's health and its schedules' own health sit side by side and are never
// folded into one. The page reads; protection policies are an administrator's.
import { Link, useParams } from "@tanstack/react-router";
import {
  Alert,
  Breadcrumb,
  BreadcrumbItem,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Content,
  DescriptionList,
  Grid,
  GridItem,
  Label,
  LabelGroup,
  PageSection,
  Progress,
  ProgressMeasureLocation,
  ProgressSize,
  Stack,
  Title,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import ShieldAltIcon from "@patternfly/react-icons/dist/esm/icons/shield-alt-icon";
import { useProtection } from "../../api/queries";
import { grantOf, useSessionDoc } from "../../api/session";
import { ApiError } from "../../api/client";
import { useProtectionPolicy, type ProtectionPolicy } from "../../api/schedules";
import { ageVsObjective, historyTooLarge, newestPointWords } from "../../app/policyAge";
import { PageHeader } from "../../components/PageHeader";
import { HealthStatus, StatusLabel } from "../../components/Status";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { CompactList, useNarrow } from "../../components/CompactList";
import { When, duration } from "../../components/When";
import { Term } from "../schedules/parts";
import { Prose } from "../../components/Prose";
import "../schedules/schedules.css";

/** The controller's alert kinds (crds/protection_policy.rs AlertKind), in words; a newer kind is shown as written. */
const PROBLEM_WORDS: Record<string, string> = {
  BackupFailure: "A run failed",
  Staleness: "The newest point is older than the objective",
  ArchiveUnavailable: "The catalog cannot find the point in storage",
  RehearsalFailure: "A rehearsal failed, or has not run for too long",
};

/** The ledger's informational kind: a finished recovery, recorded resolved when it happens. Not a problem (DUI-12). */
export const RECOVERY_COMPLETED = "RecoveryCompleted";

/** The ledger split in two: what went wrong, and the recoveries it recorded. */
export function ledgerOf(p: Pick<ProtectionPolicy, "alerts">) {
  const alerts = p.alerts ?? [];
  return { problems: alerts.filter((a) => a.kind !== RECOVERY_COMPLETED), recoveries: alerts.filter((a) => a.kind === RECOVERY_COMPLETED) };
}

const EVIDENCE_WORDS: Record<string, string> = {
  Valid: "Verified",
  ValidHistorical: "Verified, under a key since retired",
  Invalid: "Did not verify",
  Untrusted: "Signed by a key this installation does not trust",
  NotAttempted: "Not verified",
};

/** Open problems in the policy's ledger. */
export function openProblems(p: ProtectionPolicy): number {
  return ledgerOf(p).problems.filter((a) => a.state === "Open").length;
}

/** `Protected` has three statuses, kept apart: Unknown is not rounded down to False. */
export function protectedWords(p: ProtectionPolicy): { tone: "success" | "danger" | "neutral"; text: string; title: string } | null {
  const c = p.conditions?.find((x) => x.type === "Protected");
  if (!c) return null;
  const title = `Protected=${c.status}${c.reason ? ` ${c.reason}` : ""}`;
  if (c.status === "True") return { tone: "success", text: "Objective met", title };
  if (c.status === "Unknown") return { tone: "neutral", text: "Could not be evaluated", title };
  return { tone: "danger", text: "Objective not met", title };
}

/** The failure streak; an absent one is not known, never 0 (FX-38). */
/** A last attempt's phase and reason; "Not recorded" only when it carries neither. */
export function attemptWords(a: { phase?: string | null; reason?: string | null }): string {
  if (a.phase && a.reason) return `${a.phase} (${a.reason})`;
  return a.phase || a.reason || "Not recorded";
}

export function failedWords(p: ProtectionPolicy): string {
  const streak = typeof p.consecutiveFailedRuns === "number" ? String(p.consecutiveFailedRuns) : "Not known";
  return `${streak} (${p.objectives.maxConsecutiveFailedRuns} allowed)`;
}

function AgeBar({ p }: { p: ProtectionPolicy }) {
  const age = ageVsObjective(p);
  return (
    <>
      <Progress
        value={age.percent}
        variant={age.variant}
        size={ProgressSize.sm}
        measureLocation={ProgressMeasureLocation.none}
        aria-label={`${p.name}: newest recovery point ${age.text}`}
      />
      <div className="lw-subtle lw-small">
        {p.lastAvailablePoint ? <When value={p.lastAvailablePoint.recoveryPointAt} /> : newestPointWords(p)} · objective{" "}
        {duration(p.objectives.maxRecoveryPointAgeSeconds)}
      </div>
    </>
  );
}

function scheduleWords(p: ProtectionPolicy): string {
  const all = p.schedules ?? [];
  if (all.length === 0) return "None named";
  return all.map((s) => `${s.name}${s.suspended ? " (suspended)" : s.ready === "False" ? " (not ready)" : ""}`).join(", ");
}

export function ProtectionList() {
  const { ns } = useParams({ from: "/$ns/protection" });
  // A localAdmin console holds no roles: its one actor is the installation's administrator.
  const session = useSessionDoc();
  const administers = session.authenticationMode === "localAdmin" || grantOf(session, ns)?.roles.includes("administrator") === true;
  const policies = useProtection(ns);
  const narrow = useNarrow();
  const rows = policies.data ?? [];
  return (
    <>
      <PageHeader
        title="Protection"
        description="Whether each objective is met: a recovery point you could restore from, young enough. A schedule that runs is not the same as an objective that is met."
      />
      <PageSection>
        <Card>
          {policies.isPending ? (
            <div className="lw-pad">
              <LoadingRows rows={3} label="Loading protection policies" />
            </div>
          ) : policies.isError ? (
            <ErrorState error={policies.error} onRetry={() => void policies.refetch()} />
          ) : rows.length === 0 ? (
            <Empty title="No protection policy" icon={ShieldAltIcon}>
              Nothing here is checking whether a recoverable backup exists or how old it is.{" "}
              {/* DUI-31.11: an administrator is the one who sets it, so it is not told to ask one. */}
              {administers
                ? `This console reads protection policies and does not create them: as an administrator of ${ns}, you add one with kubectl, as a ProtectionPolicy in ${ns} that names its schedules and objectives.`
                : `Protection policies are set by whoever administers Logweir for ${ns}; this console reads them and does not create them.`}
            </Empty>
          ) : narrow ? (
            <CompactList
              label="Protection policies"
              items={rows.map((p) => ({
                key: p.name,
                href: `#/${ns}/protection/${p.name}`,
                title: p.name,
                meta: `${ageVsObjective(p).text} · objective ${duration(p.objectives.maxRecoveryPointAgeSeconds)}`,
                status: <HealthStatus health={p.health} />,
              }))}
            />
          ) : (
            <Table aria-label="Protection policies" variant="compact" gridBreakPoint="grid-lg">
              <Thead>
                <Tr>
                  <Th modifier="wrap">Policy</Th>
                  <Th modifier="wrap">Status</Th>
                  <Th width={25} modifier="wrap">Recovery point age</Th>
                  <Th modifier="wrap">Failed in a row</Th>
                  <Th modifier="wrap">Schedules</Th>
                  <Th modifier="wrap">Open problems</Th>
                </Tr>
              </Thead>
              <Tbody>
                {rows.map((p) => (
                  <Tr key={p.name}>
                    <Td dataLabel="Policy" className="lw-name-cell">
                      <Link to="/$ns/protection/$name" params={{ ns, name: p.name }} className="lw-break">
                        {p.name}
                      </Link>
                      <div className="lw-subtle lw-small">
                        {p.protects.topics?.length ? `${p.protects.topics.length} topics` : "the newest point's topics"} from {p.protects.sourceRef.name}
                      </div>
                    </Td>
                    <Td dataLabel="Status">
                      <HealthStatus health={p.health} />
                    </Td>
                    <Td dataLabel="Recovery point age">
                      <AgeBar p={p} />
                    </Td>
                    <Td dataLabel="Failed in a row" className="lw-num">
                      {failedWords(p)}
                    </Td>
                    <Td dataLabel="Schedules">{scheduleWords(p)}</Td>
                    <Td dataLabel="Open problems" className="lw-num">
                      {openProblems(p)}
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
        </Card>
      </PageSection>
    </>
  );
}

const yes = (v: boolean) => (v ? "Required" : "Not required");

export function ProtectionDetail() {
  const { ns, name } = useParams({ from: "/$ns/protection/$name" });
  const policy = useProtectionPolicy(ns, name);
  const narrow = useNarrow();
  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/protection" params={{ ns }}>Protection</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );
  if (policy.isPending || policy.isError) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          {policy.isPending ? (
            <LoadingRows rows={6} label="Loading the protection policy" />
          ) : (
            <ErrorState error={policy.error} onRetry={policy.error instanceof ApiError && policy.error.status === 404 ? undefined : () => void policy.refetch()} />
          )}
        </PageSection>
      </>
    );
  }
  const p = policy.data;
  const point = p.lastAvailablePoint;
  const met = protectedWords(p);
  const behind = typeof p.generation === "number" && typeof p.observedGeneration === "number" && p.observedGeneration < p.generation;
  const schedules = p.schedules ?? [];
  const { problems: alerts, recoveries } = ledgerOf(p);
  return (
    <>
      <PageHeader
        title={p.name}
        breadcrumb={breadcrumb}
        status={
          <>
            <HealthStatus health={p.health} />
            {met?.tone === "neutral" && <StatusLabel tone={met.tone} text={met.text} title={met.title} />}
          </>
        }
        description={
          p.evaluatedAt ? (
            <>
              Evaluated <When value={p.evaluatedAt} />, every {duration(p.evaluationIntervalSeconds)}.
            </>
          ) : (
            "Not evaluated yet. That is an absent observation, not a verdict: nothing here says this is protected or that it is not."
          )
        }
      />
      {historyTooLarge(p) && (
        <PageSection>
          <Alert isInline variant="info" title="Could not be evaluated: too many Backup objects to read">
            <p>
              <Prose text={p.conditions?.find((c) => c.type === "Protected")?.message || "The controller recorded no message."} />
            </p>
            <p>The newest point, the last attempt and the failure streak are not known until it can read them all. This is neither healthy nor a failure.</p>
          </Alert>
        </PageSection>
      )}
      {(behind || p.staleSince) && (
        <PageSection>
          {behind && (
            <Alert isInline isPlain variant="info" title="This verdict predates the current objective">
              It was computed from revision {p.observedGeneration}; the policy is at {p.generation}.
            </Alert>
          )}
          {p.staleSince && (
            <Alert isInline isPlain variant="warning" title="The objective has been missed">
              Since <When value={p.staleSince} exact />.
            </Alert>
          )}
        </PageSection>
      )}
      <PageSection>
        <Grid hasGutter>
          <GridItem lg={7} xl={8}>
            <Stack hasGutter>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Newest recovery point</CardTitle>
                </CardHeader>
                <CardBody>
                  <AgeBar p={p} />
                  {point ? (
                    <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "16ch" }} className="lw-gap-top">
                      <Term term="Capture started">
                        <When value={point.recoveryPointAt} exact />
                      </Term>
                      <Term term="Newest record">
                        <When value={point.newestRecordAt} exact />
                      </Term>
                      <Term term="Age when evaluated">{typeof point.ageSeconds === "number" ? duration(point.ageSeconds) : "Not recorded"}</Term>
                      <Term term="Evidence">
                        {point.evidence ? <span title={point.evidence}>{EVIDENCE_WORDS[point.evidence] ?? point.evidence}</span> : "Not recorded"}
                      </Term>
                      <Term term="Backup">
                        {point.backupRef ? (
                          <Link to="/$ns/backups/$name" params={{ ns, name: point.backupRef.name }}>
                            {point.backupRef.name}
                          </Link>
                        ) : (
                          <span className="lw-subtle">Not recorded</span>
                        )}
                      </Term>
                      <Term term="Topics">
                        {point.topics?.length ? `${point.topics.join(", ")}${point.topicsTruncated ? " and more" : ""}` : "Not recorded"}
                      </Term>
                      <Term term="Availability from">{p.availabilityBasis ?? "Not recorded"}</Term>
                    </DescriptionList>
                  ) : historyTooLarge(p) ? (
                    <Content component="p" className="lw-gap-top">
                      Not known: the controller could not read every Backup in this namespace, so it names no newest point.
                    </Content>
                  ) : (
                    <Content component="p" className="lw-gap-top">
                      There is no recovery point this objective can use.{" "}
                      {p.availabilityBasis === "CatalogStale"
                        ? "The catalog could not answer, which is not the same as an archive with nothing in it."
                        : `Availability was decided from ${p.availabilityBasis ?? "an unrecorded basis"}.`}
                    </Content>
                  )}
                  <Content component="p" className="lw-subtle lw-small lw-gap-top">
                    The objective is measured from when the capture started. The newest record can look older on an idle
                    topic without any gap in protection.
                  </Content>
                </CardBody>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Schedules</CardTitle>
                </CardHeader>
                <CardBody>
                  <Content component="p" className="lw-subtle">
                    Each schedule's own health, beside the objective and never folded into it.
                  </Content>
                  {schedules.length === 0 ? (
                    <Content component="p">This policy names no schedule, so no slot is expected and no missed slot is counted.</Content>
                  ) : narrow ? (
                    <CompactList
                      flush
                      label="Schedules this policy counts"
                      items={schedules.map((s) => ({
                        key: s.name,
                        href: `#/${ns}/schedules/${s.name}`,
                        title: s.name,
                        meta: s.suspended ? "Suspended" : s.nextFireTime ? <>next <When value={s.nextFireTime} /></> : "Next run not recorded",
                        status: <StatusLabel tone={s.ready === "True" ? "success" : s.ready === "False" ? "danger" : "neutral"} text={s.ready === "True" ? "Ready" : s.ready === "False" ? "Not ready" : "Not recorded"} />,
                      }))}
                    />
                  ) : (
                    <Table aria-label="Schedules this policy counts" variant="compact" gridBreakPoint="grid-md">
                      <Thead>
                        <Tr>
                          <Th modifier="wrap">Schedule</Th>
                          <Th modifier="wrap">Ready</Th>
                          <Th modifier="wrap">Suspended</Th>
                          <Th modifier="wrap">Next run</Th>
                          <Th modifier="wrap">Last missed slot</Th>
                        </Tr>
                      </Thead>
                      <Tbody>
                        {schedules.map((s) => (
                          <Tr key={s.name}>
                            <Td dataLabel="Schedule">
                              <Link to="/$ns/schedules/$name" params={{ ns, name: s.name }} search={{}}>
                                {s.name}
                              </Link>
                            </Td>
                            <Td dataLabel="Ready">
                              <StatusLabel
                                tone={s.ready === "True" ? "success" : s.ready === "False" ? "danger" : "neutral"}
                                text={s.ready === "True" ? "Ready" : s.ready === "False" ? "Not ready" : "Not recorded"}
                              />
                            </Td>
                            <Td dataLabel="Suspended">{s.suspended ? "Yes" : "No"}</Td>
                            <Td dataLabel="Next run">
                              <When value={s.nextFireTime} />
                            </Td>
                            <Td dataLabel="Last missed slot">{s.lastMissedSlot ?? <span className="lw-subtle">None recorded</span>}</Td>
                          </Tr>
                        ))}
                      </Tbody>
                    </Table>
                  )}
                </CardBody>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Problems</CardTitle>
                </CardHeader>
                <CardBody>
                  {alerts.length === 0 ? (
                    <Content component="p" className="lw-subtle">
                      This policy has recorded no problem.
                    </Content>
                  ) : (
                    <Table aria-label="Problems" variant="compact" gridBreakPoint="grid-md">
                      <Thead>
                        <Tr>
                          <Th modifier="wrap">Problem</Th>
                          <Th modifier="wrap">State</Th>
                          <Th modifier="wrap">Opened</Th>
                          <Th modifier="wrap">Resolved</Th>
                        </Tr>
                      </Thead>
                      <Tbody>
                        {alerts.map((a) => (
                          <Tr key={a.key}>
                            <Td dataLabel="Problem">
                              {PROBLEM_WORDS[a.kind] ?? a.kind}
                              <div className="lw-reason lw-small">{a.kind}</div>
                            </Td>
                            <Td dataLabel="State">
                              <StatusLabel tone={a.state === "Open" ? "warning" : a.state === "Resolved" ? "success" : "neutral"} text={a.state} />
                            </Td>
                            <Td dataLabel="Opened">
                              <When value={a.openedAt} />
                            </Td>
                            <Td dataLabel="Resolved">
                              <When value={a.resolvedAt} />
                            </Td>
                          </Tr>
                        ))}
                      </Tbody>
                    </Table>
                  )}
                  {recoveries.length > 0 && (
                    <>
                      <Title headingLevel="h3" size="md" className="lw-gap-top">
                        Recoveries recorded
                      </Title>
                      <ul className="lw-plain-list" aria-label="Recoveries recorded">
                        {recoveries.map((a) => (
                          <li key={a.key}>
                            A recovery from this policy's source finished <When value={a.openedAt} />
                          </li>
                        ))}
                      </ul>
                    </>
                  )}
                  <Content component="p" className="lw-subtle lw-small lw-gap-top">
                    A ledger of what was found, open until it clears. A finished recovery is recorded beside it, not as a problem. Nothing is sent
                    anywhere: alert delivery is not part of v1.
                  </Content>
                </CardBody>
              </Card>
            </Stack>
          </GridItem>
          <GridItem lg={5} xl={4}>
            <Stack hasGutter>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Objective</CardTitle>
                </CardHeader>
                <CardBody>
                  <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "16ch" }}>
                    <Term term="Newest point no older than">{duration(p.objectives.maxRecoveryPointAgeSeconds)}</Term>
                    <Term term="Failed runs in a row">
                      {failedWords(p)}
                    </Term>
                    <Term term="Verified evidence">{yes(p.objectives.requireVerifiedEvidence)}</Term>
                    <Term term="Still in storage (catalog)">{yes(p.objectives.requireCatalogAvailability)}</Term>
                    {typeof p.objectives.maxRehearsalAgeSeconds === "number" && (
                      <Term term="Rehearsal no older than">{duration(p.objectives.maxRehearsalAgeSeconds)}</Term>
                    )}
                  </DescriptionList>
                </CardBody>
              </Card>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">What it protects</CardTitle>
                </CardHeader>
                <CardBody>
                  <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "12ch" }}>
                    <Term term="Source">{p.protects.sourceRef.name}</Term>
                    <Term term="Topics">
                      {p.protects.topics?.length ? (
                        <LabelGroup numLabels={8} aria-label="Topics" className="lw-names">
                          {p.protects.topics.map((t) => (
                            <Label key={t} isCompact variant="outline">
                              {t}
                            </Label>
                          ))}
                        </LabelGroup>
                      ) : (
                        "The topics of the newest matching point (never all of them)"
                      )}
                    </Term>
                    <Term term="Destination">{p.protects.destinationRef?.name ?? p.protects.legacyArchive?.url ?? "Not recorded"}</Term>
                    <Term term="Catalog">{p.protects.catalogRef?.name ?? "Not recorded"}</Term>
                    <Term term="Last attempt">
                      {p.lastAttempt?.backupRef ? (
                        <>
                          <Link to="/$ns/backups/$name" params={{ ns, name: p.lastAttempt.backupRef.name }}>
                            {p.lastAttempt.backupRef.name}
                          </Link>
                          <div className="lw-subtle lw-small">
                            {/* DUI-31.6: a reason without a phase is said alone, never beside "Not recorded". */}
                            {attemptWords(p.lastAttempt)}, <When value={p.lastAttempt.at} />
                          </div>
                        </>
                      ) : (
                        <span className="lw-subtle">{historyTooLarge(p) ? "Not known" : "Not recorded"}</span>
                      )}
                    </Term>
                  </DescriptionList>
                </CardBody>
              </Card>
              {(p.rehearsalLastSucceededAt || p.rehearsalLastFailedAt || typeof p.objectives.maxRehearsalAgeSeconds === "number") && (
                <Card>
                  <CardHeader>
                    <CardTitle component="h2">Rehearsal</CardTitle>
                  </CardHeader>
                  <CardBody>
                    <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "12ch" }}>
                      <Term term="Last passed">
                        <When value={p.rehearsalLastSucceededAt} />
                      </Term>
                      <Term term="Last failed">
                        <When value={p.rehearsalLastFailedAt} />
                        {p.rehearsalLastReason && <span className="lw-reason"> {p.rehearsalLastReason}</span>}
                      </Term>
                      <Term term="Restore">
                        {p.rehearsalLastRestoreRef ? (
                          <Link to="/$ns/restores/$name" params={{ ns, name: p.rehearsalLastRestoreRef.name }}>
                            {p.rehearsalLastRestoreRef.name}
                          </Link>
                        ) : (
                          <span className="lw-subtle">Not recorded</span>
                        )}
                      </Term>
                    </DescriptionList>
                  </CardBody>
                </Card>
              )}
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Conditions</CardTitle>
                </CardHeader>
                <CardBody>
                  {(p.conditions ?? []).length === 0 ? (
                    <Content component="p" className="lw-subtle">
                      No condition recorded yet.
                    </Content>
                  ) : (
                    <ul className="lw-plain-list">
                      {(p.conditions ?? []).map((c) => (
                        <li key={c.type} className="lw-condition">
                          <strong>{c.type}</strong> {c.status}
                          {c.reason && <span className="lw-reason"> {c.reason}</span>}
                          {c.message && (
                            <div className="lw-subtle lw-small">
                              <Prose text={c.message} />
                            </div>
                          )}
                        </li>
                      ))}
                    </ul>
                  )}
                </CardBody>
              </Card>
            </Stack>
          </GridItem>
        </Grid>
      </PageSection>
    </>
  );
}
