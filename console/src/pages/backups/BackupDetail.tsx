// One backup run: where it stands, what it covered, whether the controller
// verified its signed receipt, how to check that yourself, and what to do with
// it. The run's operation view (stage, diagnoses, result, evidence) is read
// beside it and followed until the run ends and its verdict settles; leaving
// the page stops the read, never the run.
import { useMemo } from "react";
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
  Content,
  DescriptionList,
  Flex,
  FlexItem,
  Grid,
  GridItem,
  Label,
  LabelGroup,
  PageSection,
  ProgressStep,
  ProgressStepper,
  Tooltip,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import {
  useBackupDoc,
  useBackupList,
  useBackupOperation,
  useCatalogPoints,
  useCatalogs,
  useRunDestination,
  type Backup,
  type OperationView,
  type PointView,
} from "../../api/backups";
import { useOperationStream } from "../../api/stream";
import { PageHeader } from "../../components/PageHeader";
import { CaptureStatus, StatusLabel } from "../../components/Status";
import { ErrorState, LoadingRows } from "../../components/States";
import { When, count, exactUtc } from "../../components/When";
import { Ident } from "../../components/Ident";
import { CompactList, useNarrow } from "../../components/CompactList";
import { BackupStatus, Commands, Term, Value, VerdictLabel, useFlags } from "./parts";
import { Trigger } from "./BackupList";
import "./evidence.css";
import {
  HISTORICAL_SUFFIX,
  NO_EVIDENCE_KEYS,
  NO_REVISION,
  NO_TRIGGER,
  PATH_STYLE_SENTENCE,
  QUEUED_RUN_SENTENCE,
  checkCommands,
  detailVerdict,
  evidenceRoute,
  hasEvidenceKeys,
  identityWords,
  isRecoveryPoint,
  listVerdict,
  restoreOffer,
  revisionWords,
  triggerWords,
  windowWords,
} from "./verdict";

/** The refusal a page gets when the operation it read belongs to a different run than the one on screen. */
export const DIFFERENT_RUN_SENTENCE =
  "This name now refers to a different run: the object was deleted and a new one created under the same name while this page was open. Nothing about the new run is shown under the old run's facts; reload the page to read the new one.";

export const NO_DIAGNOSES_SENTENCE =
  "The controller recorded no diagnosis for this run. That is not the same as a run with nothing wrong: a diagnosis is written when there is a cause to write down.";

export const STATE_UNKNOWN_SENTENCE =
  "Unknown is what this run's status could not establish: an active stage whose last observation is old, a phase this build does not know, or no status at all yet. It is not a statement that the run failed.";

const STEPS = [
  { id: "start", title: "Started", desc: "Admitted and scheduled", at: ["pending", "queued", "admission"] },
  { id: "prepare", title: "Preparing", desc: "The runner is being set up", at: ["preparing"] },
  { id: "copy", title: "Copying", desc: "Reading the topics into the archive", at: ["running"] },
  { id: "verify", title: "Verifying", desc: "The controller checks the signed receipt", at: ["verifying"] },
] as const;

function LiveProgress({ b, op }: { b: Backup; op: OperationView | undefined }) {
  const now = op?.stage ?? b.operation.state;
  const at = STEPS.findIndex((s) => (s.at as readonly string[]).includes(now));
  return (
    <ProgressStepper isCenterAligned aria-label="Backup progress" className="lw-stepper">
      {STEPS.map((s, i) => (
        <ProgressStep
          key={s.id}
          id={`bp-${s.id}`}
          titleId={`bp-${s.id}-t`}
          variant={i < at ? "success" : i === at ? "info" : "pending"}
          isCurrent={i === at}
          description={s.desc}
        >
          {s.title}
        </ProgressStep>
      ))}
    </ProgressStepper>
  );
}

function positionsText(g: NonNullable<NonNullable<PointView["consumerPositions"]>["groups"]>[number]): string {
  if (!g.positions) return g.reason === "GroupTypeNotCaptured" ? "Streams and share groups are not captured" : (g.reason ?? "No position recorded");
  const p = g.positions;
  const extra = [
    [p.notRelated, "outside it"],
    [p.neverCommitted, "never committed"],
    [p.beyondEnd, "beyond the end"],
    [p.failed, "failed"],
    [p.notObserved, "not observed"],
  ] as const;
  return `${p.related} ${p.related === 1 ? "partition" : "partitions"} within the archived data${extra
    .filter(([n]) => n > 0)
    .map(([n, w]) => `, ${n} ${w}`)
    .join("")}`;
}

function groupType(t: string | null | undefined): string {
  return t === "other" ? "streams / share" : (t ?? "Not recorded");
}

function Groups({ point }: { point: PointView }) {
  const groups = point.consumerPositions?.groups ?? [];
  const narrow = useNarrow();
  if (!point.consumerPositions) {
    return (
      <Content component="p" className="lw-subtle">
        This point publishes no consumer positions: the backup selected no group, the point predates format 1.7.0, or
        the catalog was synced by an older runner. That is never "no positions".
      </Content>
    );
  }
  if (narrow) {
    return (
      <CompactList
        flush
        label="Consumer groups captured with this point"
        items={groups.map((g) => ({
          key: g.groupId,
          title: g.groupId,
          meta: `${groupType(g.groupType)} · ${positionsText(g)}`,
          status: <CaptureStatus captured={g.outcome === "captured"} reason={g.reason} />,
        }))}
      />
    );
  }
  return (
    <Table aria-label="Consumer groups captured with this point" variant="compact" gridBreakPoint="grid-md">
      <Thead>
        <Tr>
          <Th>Group</Th>
          <Th>Type</Th>
          <Th>Captured</Th>
          <Th>Positions</Th>
        </Tr>
      </Thead>
      <Tbody>
        {groups.map((g) => (
          <Tr key={g.groupId}>
            <Td dataLabel="Group">{g.groupId}</Td>
            <Td dataLabel="Type">{groupType(g.groupType)}</Td>
            <Td dataLabel="Captured">
              <CaptureStatus captured={g.outcome === "captured"} reason={g.reason} />
            </Td>
            <Td dataLabel="Positions">{positionsText(g)}</Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
}

/**
 * The catalog row that IS this run: the catalog indexing the run's destination, the row with the
 * run's backup set and, when the run reported the digest of its receipt, that exact receipt (a set
 * can hold more than one point).
 */
function useRunPoint(ns: string, b: Backup | undefined, receiptSha256: string | null | undefined) {
  const catalogs = useCatalogs(ns);
  const catalog = (catalogs.data ?? []).find((c) => b?.destinationRef && c.destinationRef?.name === b.destinationRef.name);
  const points = useCatalogPoints(ns, catalog?.name ?? "", false, Boolean(catalog));
  const enabled = Boolean(catalog);
  const rows = (points.data?.pages ?? []).flatMap((p) => p.items);
  const point = b
    ? rows.find((p) => p.backupId === b.backupId && (!receiptSha256 || p.receiptSha256 === receiptSha256))
    : undefined;
  return { catalog, point, pending: catalogs.isPending || (enabled && points.isPending), readable: catalogs.isSuccess };
}

function PointCard({ ns, b, receiptSha256 }: { ns: string; b: Backup; receiptSha256: string | null | undefined }) {
  const { catalog, point, pending, readable } = useRunPoint(ns, b, receiptSha256);
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Recovery point and consumer groups</CardTitle>
      </CardHeader>
      <CardBody>
        {!isRecoveryPoint(b) ? (
          <Content component="p" className="lw-subtle">
            {b.operation.terminal ? "This run made no recovery point, so no catalog lists one for it." : "This run has not finished; its recovery point, if it makes one, appears here once a catalog has synced it."}
          </Content>
        ) : pending ? (
          <LoadingRows rows={3} label="Reading the recovery catalog" />
        ) : !readable ? (
          <Content component="p" className="lw-subtle">
            The recovery catalog could not be read, so this page cannot show the point this run made.
          </Content>
        ) : !b.destinationRef ? (
          <Content component="p" className="lw-subtle">
            This run wrote to an inline archive, which no catalog here indexes by destination.
          </Content>
        ) : !catalog ? (
          <Content component="p" className="lw-subtle">
            No recovery catalog indexes destination {b.destinationRef.name}, so nothing in Kubernetes lists this run's point.{" "}
            <Link to="/$ns/catalog" params={{ ns }}>
              Connect one
            </Link>
            .
          </Content>
        ) : !point ? (
          <Content component="p" className="lw-subtle">
            Catalog{" "}
            <Link to="/$ns/catalog/$name" params={{ ns, name: catalog.name }}>
              {catalog.name}
            </Link>{" "}
            does not list this run in the part of its view this page read. That is not "missing": a run made no point
            until it succeeded, the catalog may not have synced since, or the point may be outside its window.
          </Content>
        ) : (
          <>
            <Flex gap={{ default: "gapMd" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "wrap" }}>
              <FlexItem>
                <Ident value={point.pointId} label="recovery point id" />
              </FlexItem>
              <FlexItem>
                <StatusLabel tone={point.availability === "Available" ? "success" : "warning"} text={point.availability} title="availability" />
              </FlexItem>
              <FlexItem>
                <StatusLabel
                  tone={point.verification === "Verified" || point.verification === "VerifiedHistorical" ? "success" : "warning"}
                  text={point.verification}
                  title="verification"
                />
              </FlexItem>
              <FlexItem className="lw-subtle lw-small">
                in catalog{" "}
                <Link to="/$ns/catalog/$name" params={{ ns, name: catalog.name }}>
                  {catalog.name}
                </Link>
              </FlexItem>
            </Flex>
            <Content component="p" className="lw-subtle lw-gap-top">
              The committed positions captured with this point. A restore can translate them so consumers resume on the
              restored topics. A group that is not captured has no position: it is never read as offset 0.
            </Content>
            <Groups point={point} />
          </>
        )}
      </CardBody>
    </Card>
  );
}

function Diagnoses({ op }: { op: OperationView }) {
  const list = op.diagnostics ?? [];
  if (list.length === 0) return <Content component="p" className="lw-subtle">{NO_DIAGNOSES_SENTENCE}</Content>;
  return (
    <Table aria-label="What the controller could see" variant="compact" gridBreakPoint="grid-md">
      <Thead>
        <Tr>
          <Th>Cause</Th>
          <Th>Message</Th>
          <Th>About</Th>
          <Th>Seen</Th>
        </Tr>
      </Thead>
      <Tbody>
        {list.map((d, i) => (
          <Tr key={`${d.code}-${i}`}>
            <Td dataLabel="Cause">
              <StatusLabel tone={d.severity === "Error" ? "danger" : "warning"} text={d.code} title={d.severity} />
            </Td>
            <Td dataLabel="Message" className="lw-break">
              <Value value={d.message} />
            </Td>
            <Td dataLabel="About">{d.object ? `${d.object.kind} ${d.object.name}` : <span className="lw-subtle">Not recorded</span>}</Td>
            <Td dataLabel="Seen">
              {typeof d.count === "number" ? `${d.count} ${d.count === 1 ? "time" : "times"}, last ` : ""}
              <When value={d.lastSeen} />
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
}

function OperationCard({ op }: { op: OperationView }) {
  const p = op.progress;
  return (
    <Card isFullHeight>
      <CardHeader>
        <CardTitle component="h2">Where it stands</CardTitle>
      </CardHeader>
      <CardBody className="lw-ev-stack">
        <DescriptionList isCompact orientation={{ sm: "horizontal" }} className="lw-dl">
          <Term term="State">
            {op.state}
            {op.stateReason ? <span className="lw-reason"> {op.stateReason}</span> : null}
          </Term>
          <Term term="Stage">
            <Value value={op.stage} />
          </Term>
          <Term term="Message">
            <Value value={op.message} />
          </Term>
          <Term term="Last update">
            <When value={op.lastUpdatedAt} exact />
          </Term>
          {p?.lastObservedTime && (
            <Term term="Last observed">
              <When value={p.lastObservedTime} exact />
            </Term>
          )}
          {p?.runnerPhase?.name && (
            <Term term="Runner phase">
              {p.runnerPhase.number} {p.runnerPhase.name}
            </Term>
          )}
          <Term term="Status too old">{op.stale ? <StatusLabel tone="warning" text="Yes, stale" /> : "No"}</Term>
        </DescriptionList>
        {op.state === "unknown" && (
          <Content component="p" className="lw-subtle lw-small">
            {STATE_UNKNOWN_SENTENCE}
          </Content>
        )}
        {!p && (
          <Content component="p" className="lw-subtle lw-small">
            This run carries no progress record. That is what a run reconciled by an older controller looks like; it is
            not a stalled run, and no stage is inferred from it.
          </Content>
        )}
        <div>
          <Content component="h3" className="lw-small">
            What the controller could see
          </Content>
          <Diagnoses op={op} />
        </div>
        {op.conditions.length > 0 && (
          <div>
            <Content component="h3" className="lw-small">
              Activity
            </Content>
            <ol className={op.terminal ? "lw-timeline" : "lw-timeline lw-timeline--live"} aria-live="polite">
              {[...op.conditions].reverse().map((c) => (
                <li key={c.type}>
                  <Flex justifyContent={{ default: "justifyContentSpaceBetween" }} gap={{ default: "gapSm" }}>
                    <FlexItem>
                      <strong>{c.type}</strong> <span className="lw-reason">{c.status} {c.reason ?? ""}</span>
                    </FlexItem>
                    <FlexItem className="lw-subtle lw-small">
                      <When value={c.lastTransitionTime} />
                    </FlexItem>
                  </Flex>
                  {c.message && <div className="lw-break">{c.message}</div>}
                </li>
              ))}
            </ol>
          </div>
        )}
      </CardBody>
    </Card>
  );
}

function EvidenceCard({ ns, b, op }: { ns: string; b: Backup; op: OperationView | undefined }) {
  const destination = useRunDestination(ns, b.destinationRef?.name);
  if (!op) {
    return (
      <Card isFullHeight>
        <CardHeader>
          <CardTitle component="h2">Evidence</CardTitle>
        </CardHeader>
        <CardBody>
          <LoadingRows rows={4} label="Reading the evidence" />
        </CardBody>
      </Card>
    );
  }
  const verdict = detailVerdict(op);
  const trust = op.trust as OperationView["trust"] | undefined;
  const e = op.evidence ?? {};
  const v = op.verification ?? { state: "unknown" };
  const route = evidenceRoute(b, destination.data, destination.error);
  const keys = hasEvidenceKeys(e.payloadKey, e.sidecarKey);
  // A run with no evidence at all shows the two verdict words and why, not a column of "Not recorded".
  const recorded = Boolean(e.payloadKey || e.sidecarKey || e.payloadSha256 || v.matchedKeyId || v.verifiedAt || trust?.keyState || trust?.policy);
  return (
    <Card isFullHeight>
      <CardHeader>
        <CardTitle component="h2">Evidence</CardTitle>
      </CardHeader>
      <CardBody className="lw-ev-stack">
        <div>
          <VerdictLabel verdict={verdict} />
          <Content component="p" className="lw-gap-top lw-break" title={verdict.green ? verdict.label : undefined}>
            {verdict.green
              ? `Verified by the controller at ${exactUtc(verdict.verifiedAt!)} against the key below${verdict.historical ? HISTORICAL_SUFFIX : ""}. The controller fetched this run's signed receipt with its own read-only credential and checked it; this page holds no key and checks nothing itself.`
              : `${verdict.reason} This page holds no key and checks nothing itself; the verdict is the controller's.`}
          </Content>
        </div>
        <DescriptionList isCompact orientation={{ sm: "horizontal" }} className="lw-dl">
          <Term term="Signature">
            <Value value={v.state} />
          </Term>
          <Term term="Trust">
            <Value value={trust?.state} />
            {trust?.basis && <span className="lw-reason"> basis {trust.basis}</span>}
          </Term>
{recorded && (
            <>
          <Term term="Key state">
            <Value value={trust?.keyState} />
          </Term>
          <Term term="Trust policy">
            {trust?.policy?.name ? `${trust.policy.name}${typeof trust.policy.generation === "number" ? ` g${trust.policy.generation}` : ""}` : <Value value={null} />}
          </Term>
          <Term term="Verified with key">{v.matchedKeyId ? <Ident value={v.matchedKeyId} label="signer key id" /> : <Value value={null} />}</Term>
          <Term term="Verified at">
            <When value={v.verifiedAt} exact />
          </Term>
          <Term term="Signed at">
            {trust?.signedAt ? (
              <>
                <When value={trust.signedAt} exact /> <span className="lw-subtle lw-small">(the document's own claim)</span>
              </>
            ) : (
              <Value value={trust?.signingTimeRead === "absent" ? "The document carries no signing time" : null} />
            )}
          </Term>
          <Term term="Payload type">
            <Value value={v.payloadType} />
          </Term>
          <Term term="Receipt key">{e.payloadKey ? <span className="lw-ev-key">{e.payloadKey}</span> : <Value value={null} />}</Term>
          <Term term="Receipt sha256">{e.payloadSha256 ? <Ident value={e.payloadSha256} label="receipt sha256" /> : <Value value={null} />}</Term>
          <Term term="Signature key">{e.sidecarKey ? <span className="lw-ev-key">{e.sidecarKey}</span> : <Value value={null} />}</Term>
            </>
          )}
          {v.detail && <Term term="Detail">{v.detail}</Term>}
        </DescriptionList>
        <div>
          <Content component="h3" className="lw-small">
            Check it yourself
          </Content>
          {!keys ? (
            <Content component="p" className="lw-subtle">
              {NO_EVIDENCE_KEYS}
            </Content>
          ) : (
            <>
              <Content component="p" className="lw-subtle lw-small">
                The two keys above name objects in your archive; the verifiers take local files. The first two lines
                fetch, and the last two verify: once with the Rust reader and once with the Python one.
              </Content>
              {route.missing && (
                <Alert isInline isPlain variant="warning" title="This page does not know the bucket" className="lw-ev-gap-bottom">
                  {route.missing}
                </Alert>
              )}
              {route.pathStyle && route.endpoint && (
                <Content component="p" className="lw-subtle lw-small">
                  {PATH_STYLE_SENTENCE}
                </Content>
              )}
              <Commands lines={checkCommands(route, e.payloadKey!, e.sidecarKey!)} label="the commands" />
            </>
          )}
        </div>
      </CardBody>
    </Card>
  );
}

function ResultCard({ op }: { op: OperationView }) {
  const r = op.result;
  return (
    <Card isFullHeight>
      <CardHeader>
        <CardTitle component="h2">Result</CardTitle>
      </CardHeader>
      <CardBody>
        <DescriptionList isCompact orientation={{ sm: "horizontal" }} className="lw-dl">
          <Term term="Result">
            <Value value={r?.status} />
          </Term>
          <Term term="Exit code">
            <Value value={r?.exitCode} />
          </Term>
          <Term term="Exit reason">
            <Value value={r?.exitReason} />
          </Term>
          <Term term="Last phase">
            <Value value={r?.lastPhaseCompleted} />
          </Term>
        </DescriptionList>
        <Content component="p" className="lw-subtle lw-small lw-gap-top">
          This is what the run recorded. Whether the document it produced verifies is the separate question under
          Evidence: a run that exited 0 and whose receipt was never checked is not a verified one.
        </Content>
      </CardBody>
    </Card>
  );
}

export function BackupDetail() {
  const { ns, name } = useParams({ from: "/$ns/backups/$name" });
  const navigate = useNavigate();
  const backup = useBackupDoc(ns, name);
  // While the run moves, its event stream writes the operation view and the poll stands down.
  const streaming = useOperationStream(ns, "backup", name, backup.data !== undefined && !backup.data.operation.terminal);
  const operation = useBackupOperation(ns, name, streaming);
  const runs = useBackupList(ns);
  const flags = useFlags(ns);

  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/backups" params={{ ns }}>Backups</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );
  const b = backup.data;
  const retries = useMemo(
    () => (runs.data ?? []).filter((r) => r.trigger?.retryOf?.name === name),
    [runs.data, name],
  );

  if (backup.isPending) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <LoadingRows rows={6} label="Loading the backup" />
        </PageSection>
      </>
    );
  }
  if (backup.isError || !b) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <ErrorState error={backup.error} onRetry={() => void backup.refetch()} />
        </PageSection>
      </>
    );
  }

  // The uid guard: an operation that answers to another uid is another run's, and none of it is shown.
  const op = operation.data && (!operation.data.uid || !b.uid || operation.data.uid === b.uid) ? operation.data : undefined;
  const mismatch = Boolean(operation.data && !op);
  const offer = restoreOffer(b, flags?.restoreCreate === true);
  const failed = b.operation.terminal && (b.operation.state === "failed" || b.operation.state === "refused");
  const verdict = op ? detailVerdict(op) : listVerdict(b.operation);
  const revision = revisionWords(b.scheduleRef);
  const restoreButton = (
    <Button
      variant="primary"
      isAriaDisabled={!offer.offer}
      onClick={() => offer.offer && void navigate({ to: "/$ns/restores/new", params: { ns }, search: { backup: b.name } })}
    >
      Restore this point
    </Button>
  );

  return (
    <>
      <PageHeader
        title={b.name}
        breadcrumb={breadcrumb}
        status={b.operation.state === "succeeded" ? <VerdictLabel verdict={verdict} /> : <BackupStatus b={b} />}
        description={`${b.topics.length} ${b.topics.length === 1 ? "topic" : "topics"} from ${b.sourceRef.name}, ${windowWords(b)}.`}
        actions={offer.offer ? restoreButton : <Tooltip content={offer.reason}>{restoreButton}</Tooltip>}
      />
      {mismatch && (
        <PageSection>
          <Alert variant="danger" isInline title="This name now refers to a different run">
            {DIFFERENT_RUN_SENTENCE}
          </Alert>
        </PageSection>
      )}
      {!b.operation.terminal && (
        <PageSection>
          <Card>
            <CardBody>
              <LiveProgress b={b} op={op} />
              {b.operation.state === "queued" && (
                <Content component="p" className="lw-subtle lw-gap-top">
                  {QUEUED_RUN_SENTENCE}
                </Content>
              )}
              <Content component="p" className="lw-subtle lw-small lw-gap-top">
                This page follows the run while it is open. Leaving it stops the read, never the run.
              </Content>
            </CardBody>
          </Card>
        </PageSection>
      )}
      {failed && (
        <PageSection>
          <Alert
            variant="danger"
            isInline
            title={`This run ${b.operation.state === "refused" ? "was refused" : "failed"}: ${b.operation.stateReason ?? "no reason recorded"}`}
          >
            <p>{op?.message ?? "The controller recorded no further detail."}</p>
            <p>
              No recovery point was made by this run.{" "}
              {retries.length > 0 ? (
                <>
                  It was retried as{" "}
                  {retries.map((r, i) => (
                    <span key={r.name}>
                      {i > 0 ? ", " : ""}
                      <Link to="/$ns/backups/$name" params={{ ns, name: r.name }}>
                        {r.name}
                      </Link>{" "}
                      ({(triggerWords(r.trigger) ?? "retry").toLowerCase()}, {listVerdict(r.operation).green ? "verified" : r.operation.terminal ? "not verified" : "in progress"})
                    </span>
                  ))}
                  .
                </>
              ) : (
                "No retry of it is listed here; the schedule's retry policy decides whether one runs, and the next slot runs normally."
              )}
            </p>
          </Alert>
        </PageSection>
      )}
      <PageSection>
        <Grid hasGutter>
          <GridItem lg={7}>
            <Card isFullHeight>
              <CardHeader>
                <CardTitle component="h2">What this run covered</CardTitle>
              </CardHeader>
              <CardBody>
                <DescriptionList isCompact orientation={{ sm: "horizontal" }} className="lw-dl">
                  <Term term="Source">{b.sourceRef.name}</Term>
                  <Term term="Topics">
                    <LabelGroup numLabels={6}>
                      {b.topics.map((t) => (
                        <Label key={t} isCompact variant="outline">
                          {t}
                        </Label>
                      ))}
                    </LabelGroup>
                  </Term>
                  <Term term="Records">{b.operation.terminal ? count(b.records) : "Counting while it runs"}</Term>
                  <Term term="Covers">{windowWords(b)}</Term>
                  <Term term="Destination">
                    {b.destinationRef ? (
                      <>
                        {b.destinationRef.name}
                        {b.locationDigest && (
                          <div className="lw-small lw-subtle">
                            frozen location <Ident value={b.locationDigest} label="frozen location digest" />
                          </div>
                        )}
                      </>
                    ) : (
                      <>
                        Inline archive <span className="lw-ev-key">{b.archive.url}</span>
                      </>
                    )}
                  </Term>
                  <Term term="Trigger">
                    {triggerWords(b.trigger) ? <Trigger b={b} /> : <span className="lw-subtle">{NO_TRIGGER}</span>}
                    {b.trigger?.retryOf && (
                      <div className="lw-small">
                        Retry of{" "}
                        <Link to="/$ns/backups/$name" params={{ ns, name: b.trigger.retryOf.name }}>
                          {b.trigger.retryOf.name}
                        </Link>
                      </div>
                    )}
                    <div className="lw-small lw-subtle">Started by: {b.triggeredBy}</div>
                  </Term>
                  <Term term="Schedule">
                    {b.scheduleRef?.name || b.schedule ? (
                      <>
                        <a href={`#/${ns}/schedules/${encodeURIComponent((b.scheduleRef?.name ?? b.schedule)!)}`}>
                          {b.scheduleRef?.name ?? b.schedule}
                        </a>
                        {b.slot && <span className="lw-subtle"> · slot {b.slot}</span>}
                        <div className="lw-small lw-subtle lw-break">{revision ?? NO_REVISION}</div>
                      </>
                    ) : (
                      <span className="lw-subtle">None: a run taken without a schedule</span>
                    )}
                  </Term>
                  <Term term="Started">
                    <When value={b.createdAt} exact />
                  </Term>
                  <Term term="Read as">{identityWords(b.observedAuth)}</Term>
                  <Term term="Backup set">{b.backupId ? <Ident value={b.backupId} label="backup set id" /> : <Value value={null} />}</Term>
                  <Term term="Manifest key">{b.manifestKey ? <span className="lw-ev-key">{b.manifestKey}</span> : <Value value={null} />}</Term>
                  <Term term="Deadline">{`${count(b.deadlineSeconds)} s`}</Term>
                  <Term term="Run UID">
                    <Ident value={b.uid} label="run uid" />
                  </Term>
                </DescriptionList>
              </CardBody>
            </Card>
          </GridItem>
          <GridItem lg={5}>
            {mismatch ? (
              <Card isFullHeight>
                <CardBody>
                  <Content component="p" className="lw-subtle">
                    Not shown: the evidence read belongs to a different run.
                  </Content>
                </CardBody>
              </Card>
            ) : operation.isError ? (
              <Card isFullHeight>
                <CardHeader>
                  <CardTitle component="h2">Evidence</CardTitle>
                </CardHeader>
                <ErrorState error={operation.error} onRetry={() => void operation.refetch()} />
              </Card>
            ) : (
              <EvidenceCard ns={ns} b={b} op={op} />
            )}
          </GridItem>
          {op && (
            <>
              <GridItem lg={7}>
                <OperationCard op={op} />
              </GridItem>
              <GridItem lg={5}>
                <ResultCard op={op} />
              </GridItem>
            </>
          )}
          <GridItem>
            <PointCard ns={ns} b={b} receiptSha256={op?.evidence?.payloadSha256} />
          </GridItem>
        </Grid>
      </PageSection>
    </>
  );
}
