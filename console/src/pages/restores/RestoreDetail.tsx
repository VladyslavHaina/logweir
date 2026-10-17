// One restore, live: where it is, why, what the controller could see, what the run
// recorded, whether its evidence verified, how much was compared and what it produced.
// The old console's #/operations (restore) and #/history restore detail in one page.
// It computes no state (the API's word is shown) and never merges the result with the
// evidence: "the run passed" and "its signed scorecard verifies" are two facts.
import { useEffect, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
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
  CodeBlock,
  CodeBlockCode,
  Content,
  ExpandableSection,
  Flex,
  FlexItem,
  Grid,
  GridItem,
  Modal,
  ModalBody,
  ModalFooter,
  ModalHeader,
  PageSection,
  Progress,
  ProgressMeasureLocation,
  ProgressStep,
  ProgressStepper,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import ExchangeAltIcon from "@patternfly/react-icons/dist/esm/icons/exchange-alt-icon";
import RedoIcon from "@patternfly/react-icons/dist/esm/icons/redo-icon";
import {
  useApprovalPolicy,
  useApprovalRequest,
  useCapabilities,
  useRestoreAction,
  useRestoreItem,
  useRestoreOperation,
  useVerdictFollow,
  verdictPending,
  type OperationView,
  type Restore,
} from "../../api/restores";
import { useOperationStream } from "../../api/stream";
import type { OperationStage } from "../../api/types";
import { PageHeader } from "../../components/PageHeader";
import { StatusLabel } from "../../components/Status";
import { ErrorState, LoadingRows } from "../../components/States";
import { count, exactUtc } from "../../components/When";
import { Ident } from "../../components/Ident";
import { CloneStatus } from "../cutover/CloneLifetime";
import { TopicConfigurationResult } from "../cutover/TopicConfiguration";
import { SwitchoverEntry } from "../cutover/SwitchoverEntry";
import { cutoverRefusal } from "../cutover/Cutover";
import { useSourceBackup } from "../../api/queries";
import { verdictInstantTerm } from "../backups/verdict";
import { Commands, Facts, Instant, RestoreStatus, shown } from "./parts";
import { approvalRoute } from "./model";
import {
  COMPLETION_NOT_VERIFIED,
  QUEUED_RUN_SENTENCE,
  completionCountLabels,
  RETRY_FRESH_TARGET_SENTENCE,
  SCORECARD_CLAIM_SENTENCE,
  completionGuidance,
  coverageWords,
  timestampBoundWords,
  modeMeaning,
  evidenceCase,
  evidenceVerified,
  independentCheck,
  notCovered,
  operationVerified,
  outcomeShown,
  planEvidenceBucket,
  planEvidenceRoute,
  planTimeBasis,
  refusalSentence,
  selectionWords,
  signedTimeBasisText,
  teardownLeftWords,
  verificationScopeSentence,
  CANCEL_CONFIRM_SENTENCE,
  CLEANUP_CONFIRM_SENTENCE,
  INTERRUPTED_TITLE,
  STOPPING_SENTENCE,
  cleanupWords,
  interruptionCause,
  planRateLimit,
  rateLimitWords,
  reconcileWords,
  restoreSchemasWords,
} from "./words";
import "./restores.css";
import { ErrorText, Prose } from "../../components/Prose";

const STAGES: { stage: OperationStage; title: string; desc: string }[] = [
  { stage: "admission", title: "Admitted", desc: "Plan checked and approved" },
  { stage: "preparing", title: "Preparing", desc: "Topics created on the target" },
  { stage: "running", title: "Restoring", desc: "Records replayed from the archive" },
  { stage: "verifying", title: "Verifying", desc: "Restored records compared with the archive" },
  { stage: "finished", title: "Done", desc: "Scorecard signed and verified" },
];

/** The stepper: the API's stage, never inferred. A stage this build does not know shows no step as current. */
function Stages({ op, verified }: { op: OperationView; verified: boolean }) {
  const at = STAGES.findIndex((s) => s.stage === (op.stage ?? (op.state === "queued" ? "admission" : undefined)));
  return (
    <ProgressStepper isCenterAligned aria-label="Restore progress" className="lw-stepper">
      {STAGES.map((s, i) => {
        const done = op.terminal ? (verified ? true : i < at) : i < at;
        const current = !op.terminal && i === at;
        const failedHere = op.terminal && !verified && i === Math.max(at, 0);
        return (
          <ProgressStep
            key={s.stage}
            id={`rs-${s.stage}`}
            titleId={`rs-${s.stage}-t`}
            variant={failedHere ? "danger" : done ? "success" : current ? "info" : "pending"}
            isCurrent={current}
            description={s.desc}
          >
            {s.title}
          </ProgressStep>
        );
      })}
    </ProgressStepper>
  );
}

/** A scorecard fact, shown as a claim until the evidence verified. */
function Claim({ value, verified }: { value: React.ReactNode; verified: boolean }) {
  if (value === null || value === undefined || value === "") return <span className="lw-subtle">Not recorded</span>;
  return verified ? (
    <>{value}</>
  ) : (
    <>
      {value} <StatusLabel tone="neutral" text="Unverified claim" title="Copied from a scorecard whose signature has not verified" />
    </>
  );
}

function CreationStopped({ r }: { r: Restore }) {
  const s = r.targetTopicsAppeared;
  if (!s) return null;
  const more = (list: string[], total: number) => (total > list.length ? ` and ${total - list.length} more` : "");
  return (
    <Alert variant="warning" isInline title="This restore stopped while creating its target topics, before anything was restored" id="creation-stopped">
      {s.appeared.length > 0 && (
        <p>
          {s.appeared.join(", ")}
          {more(s.appeared, s.appearedCount)}: created by someone else after this restore was admitted. The restore wrote nothing into
          {s.appeared.length === 1 ? " it." : " them."}
        </p>
      )}
      {s.left.length === 0 && s.unconfirmed.length === 0 ? (
        <p>This restore created no topic.</p>
      ) : (
        <ul className="lw-plain-list">
          {s.left.map((t) => (
            <li key={t}>
              <strong>{t}</strong>: {s.leftInstruction}.
            </li>
          ))}
          {s.unconfirmed.map((t) => (
            <li key={t}>
              <strong>{t}</strong>: {s.unconfirmedInstruction ?? "this restore asked the cluster to create it and got no definite answer; check what it holds and who writes to it before you remove it"}.
            </li>
          ))}
        </ul>
      )}
      {(s.left.length > 0 || s.unconfirmed.length > 0) && <p>Logweir never deletes a topic under a name it may not own.</p>}
    </Alert>
  );
}

/** What a restore waits for, under the workspace's approval policy. */
function Awaiting({ ws, r, op }: { ws: string; r: Restore; op: OperationView }) {
  const policy = useApprovalPolicy(ws);
  const route = approvalRoute(policy.data ?? null);
  const request = useApprovalRequest(ws, r.name, op.awaitingApproval && route === "two-person");
  if (!op.awaitingApproval) return null;
  // R6's page for this request; it refuses the link if the restore moved since (hash, approval).
  const approvals = (
    <Link to="/$ws/approvals/$name" params={{ ws, name: r.name }} search={{ hash: r.planHash, approval: r.approvalRef.name }}>
      Open its approval
    </Link>
  );
  if (route === "two-person") {
    const req = request.data;
    return (
      <Alert variant="info" isInline title="Waiting for a second person to approve" id="awaiting-approval">
        <p>
          {req?.stateSentence ? <Prose text={req.stateSentence} /> : "The console recorded your request; a second person, signed in as an approver and not you, approves it."}{" "}
          {req?.expiresAt ? (
            <>
              It expires <Instant value={req.expiresAt} />.
            </>
          ) : null}{" "}
          No Job runs until the controller verifies the approval.
        </p>
        <p>{approvals}</p>
      </Alert>
    );
  }
  if (route === "confirm" || route === "confirm-unavailable") {
    // DUI-18: a one-person workspace needs nobody else. Creating the restore confirmed it;
    // the controller verifies that confirmation, then the run starts.
    return (
      <Alert variant="info" isInline title="The controller is verifying your confirmation" id="awaiting-approval">
        <p>
          This workspace needs no second person: creating the restore confirmed it ({r.approvalRef.name}). The run starts once the
          controller has verified that confirmation, usually within half a minute.
        </p>
      </Alert>
    );
  }
  return (
    <Alert variant="info" isInline title="Waiting for an approval" id="awaiting-approval">
      <p>
        No Job runs until the controller verifies an approval of this plan ({r.approvalRef.name}).
        {route === "strict"
          ? " An approver who is not you countersigns the console's confirmation with their own key and submits it."
          : route === "legacy"
            ? " An approver who is not you signs this plan with their own key, out of band."
            : ""}
      </p>
      <p>{approvals}</p>
    </Alert>
  );
}

function CompleteCoverage({ op, verified }: { op: OperationView; verified: boolean }) {
  const c = op.verificationScope.complete;
  if (!c) return null;
  const rows = c.partitions ?? null;
  return (
    <div id="complete-coverage" data-covered={c.covered ? "true" : "false"}>
      <Facts
        items={[
          { term: "Covered", value: <Claim value={c.covered ? "yes" : "no: not a pass"} verified={verified} /> },
          { term: "Record bound", value: <Claim value={typeof c.maxRecords === "number" ? count(c.maxRecords) : null} verified={verified} /> },
          { term: "Archive segments verified", value: <Claim value={`${count(c.segmentsVerified)} of ${count(c.segments)} (failed ${count(c.segmentsFailed)}, not examined ${count(c.segmentsUnverified)})`} verified={verified} /> },
          { term: "Archived records decoded", value: <Claim value={count(c.recordsDecoded)} verified={verified} /> },
          { term: "Compaction holes", value: <Claim value={`${count(c.offsetHoles)} (disclosed, not a fault)`} verified={verified} /> },
        ]}
      />
      {rows === null ? (
        <Content component="p" className="lw-subtle lw-small">
          The signed scorecard lists {count(c.partitionCount)} partitions, more than this page is served; each one's counts are in the scorecard.
        </Content>
      ) : (
        <Table aria-label="Complete coverage by partition" variant="compact" gridBreakPoint="grid-lg" className="lw-gap-top">
          <Thead>
            <Tr>
              <Th>Topic</Th>
              <Th className="lw-num">Partition</Th>
              <Th>Compared</Th>
              <Th className="lw-num">Expected</Th>
              <Th className="lw-num">Matching</Th>
              <Th className="lw-num">Missing</Th>
              <Th className="lw-num">Unexpected</Th>
              <Th className="lw-num">Duplicates</Th>
              <Th className="lw-num">Out of order</Th>
              <Th className="lw-num">Different</Th>
            </Tr>
          </Thead>
          <Tbody>
            {rows.map((p) => (
              <Tr key={`${p.topic}/${p.partition}`}>
                <Td dataLabel="Topic">{p.topic}</Td>
                <Td dataLabel="Partition" className="lw-num">{p.partition}</Td>
                <Td dataLabel="Compared">{p.compared ? "yes" : <strong>no</strong>}</Td>
                <Td dataLabel="Expected" className="lw-num">{count(p.replay.expected)}</Td>
                <Td dataLabel="Matching" className="lw-num">{count(p.replay.matching)}</Td>
                <Td dataLabel="Missing" className="lw-num">{count(p.replay.missing)}</Td>
                <Td dataLabel="Unexpected" className="lw-num">{count(p.replay.unexpected)}</Td>
                <Td dataLabel="Duplicates" className="lw-num">{count(p.replay.duplicates)}</Td>
                <Td dataLabel="Out of order" className="lw-num">{count(p.replay.outOfOrder)}</Td>
                <Td dataLabel="Different" className="lw-num">{count(p.replay.mismatched)}</Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      )}
    </div>
  );
}

export const VERDICT_PENDING_SENTENCE =
  "The run has finished and the controller is verifying its signed scorecard. This page follows it until the verdict is recorded.";
export const VERDICT_STOPPED_SENTENCE =
  "The run finished, and after two minutes the controller has still recorded no verdict on its scorecard, so this page stopped following it. Nothing about the restore changed; read it again to see the verdict when it lands.";

/** One page per restore: moving to another restore starts its page afresh. */
export function RestoreDetail() {
  const { ws, name } = useParams({ from: "/$ws/restores/$name" });
  return <Detail key={`${ws}/${name}`} ws={ws} name={name} />;
}

function Detail({ ws, name }: { ws: string; name: string }) {
  const navigate = useNavigate();
  const caps = useCapabilities(ws);
  // DUI-38: a terminal restore whose verdict is still pending is followed until the
  // verdict lands, for at most VERDICT_FOLLOW_MS (the decision is read below and fed back).
  const [follow, setFollow] = useState(false);
  const restore = useRestoreItem(ws, name, { follow });
  // While the run moves, its event stream (R1) writes the operation view and the poll stands down.
  const streaming = useOperationStream(ws, "restore", name, restore.data !== undefined && (!restore.data.operation.terminal || follow));
  const operation = useRestoreOperation(ws, name, streaming, follow);
  const verdict = useVerdictFollow(verdictPending(restore.data?.operation) || verdictPending(operation.data));
  const source = useSourceBackup(ws, restore.data?.backupSetRef);
  useEffect(() => setFollow(verdict.following), [verdict.following]);
  const [showPlan, setShowPlan] = useState(false);
  const act = useRestoreAction(ws, name);
  const [confirm, setConfirm] = useState<"cancel" | "cleanup" | null>(null);

  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ws/restores" params={{ ws }}>Restores</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );
  if (restore.isPending || (operation.isPending && operation.fetchStatus !== "idle")) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <LoadingRows rows={6} label="Loading the restore" />
        </PageSection>
      </>
    );
  }
  if (restore.isError) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <ErrorState error={restore.error} onRetry={() => void restore.refetch()} />
        </PageSection>
      </>
    );
  }

  const r = restore.data;
  const op = operation.data ?? null;
  const verified = op ? operationVerified(op) : r.operation.verifiedSuccess && r.coverage?.covered !== false;
  // A scorecard fact is a claim until the scorecard's signature verified, pass or not.
  const signed = op ? evidenceVerified(op) : verified;
  const failed = r.operation.terminal && (r.operation.state === "failed" || r.operation.state === "refused");
  // Theme A: a restore verified completely has positions to switch over (the review decides the rest).
  const switchable = cutoverRefusal(r, source) === null;
  const complete = r.coverage?.requested === "complete";
  const scope = op?.verificationScope;
  // DUI-48.2: the completion counts' labels follow the recorded coverage.
  const labels = completionCountLabels(scope?.coverage ?? r.coverage?.recorded ?? null);
  const replayedMatch = op?.progress?.message?.match(/Replayed ([\d,]+) of ([\d,]+)/);
  const planBucket = planEvidenceBucket(r.planBytes);
  const evidenceKeys = op?.evidence.payloadKey && op.evidence.sidecarKey;
  const selection = scope?.selection ?? r.selection ?? null;
  // PROD-07.2: Cancel while it has not ended; Interrupted with its partial targets, Retry and Clean up.
  const interruption = r.interruption ?? null;
  const interrupted = r.operation.state === "interrupted";
  const stopping = !r.operation.terminal && !interrupted && r.operation.stateReason === "Stopping";
  const mayCreate = caps?.restoreCreate !== false;
  const cancellable = mayCreate && !r.operation.terminal && !interrupted && !stopping;
  // The targets it created (the partition table names the SOURCE topics they were restored from).
  const partialTopics = r.newTopics;
  const cleanupLine = cleanupWords(interruption?.cleanup, interruption?.cleanupReason);
  const schemasLine = restoreSchemasWords(r.coverage.schemas ?? null);
  const retry = () => void navigate({ to: "/$ws/restores/new", params: { ws }, search: { retryOf: r.name } });
  const run = (action: "cancel" | "cleanup") => act.mutate(action, { onSettled: () => setConfirm(null) });

  return (
    <>
      <PageHeader
        title={r.name}
        breadcrumb={breadcrumb}
        status={<RestoreStatus r={r} />}
        description={`From the point at ${exactUtc(r.pointInTime)} into ${r.target.clusterRef.name}.`}
        actions={
          cancellable ? (
            <Button variant="secondary" isDanger onClick={() => setConfirm("cancel")} id="restore-cancel">
              Cancel
            </Button>
          ) : interrupted && mayCreate ? (
            <Button variant="secondary" icon={<RedoIcon />} onClick={retry} id="retry-new-topics">
              Retry into new topics
            </Button>
          ) : failed && mayCreate ? (
            <Button variant="secondary" icon={<RedoIcon />} onClick={retry} id="retry-fresh-target">
              Retry to a fresh target
            </Button>
          ) : verified && switchable && caps?.restoreCreate !== false ? (
            <Button variant="primary" icon={<ExchangeAltIcon />} onClick={() => void navigate({ to: "/$ws/restores/$name/cutover", params: { ws, name } })}>
              Switch consumers over
            </Button>
          ) : undefined
        }
      />
      <PageSection>
        <div className="lw-stack">
          {op && <Awaiting ws={ws} r={r} op={op} />}
          {verdict.following && (
            <Content component="p" className="lw-subtle" role="status">
              {VERDICT_PENDING_SENTENCE}
            </Content>
          )}
          {verdict.stopped && (
            <Alert
              variant="info"
              isInline
              title="The verdict is still pending"
              actionLinks={
                <Button
                  variant="link"
                  isInline
                  onClick={() => {
                    verdict.again();
                    void restore.refetch();
                    void operation.refetch();
                  }}
                >
                  Read it again
                </Button>
              }
            >
              {VERDICT_STOPPED_SENTENCE}
            </Alert>
          )}
          {r.operation.state === "queued" && <Alert variant="info" isInline title="Queued">{QUEUED_RUN_SENTENCE}</Alert>}
          <CreationStopped r={r} />
          {stopping && (
            <Alert variant="info" isInline title="Stopping…" id="restore-stopping">
              {STOPPING_SENTENCE}
            </Alert>
          )}
          {interrupted && (
            <Alert
              variant="warning"
              isInline
              title={INTERRUPTED_TITLE}
              id="restore-interrupted"
              actionLinks={
                interruption?.cleanupOffered && mayCreate ? (
                  <Button variant="secondary" isDanger onClick={() => setConfirm("cleanup")} id="restore-cleanup">
                    Clean up partial topics
                  </Button>
                ) : undefined
              }
            >
              <p>{interruptionCause(interruption?.cause, interruption?.requestedBy)}</p>
              <p>{reconcileWords(interruption?.verification, interruption?.verificationReason)}</p>
              {cleanupLine && <p id="restore-cleanup-state">{cleanupLine}</p>}
            </Alert>
          )}
          {/* DUI-57: a refused Cancel or Clean up is the action's answer, not a page that failed to load. */}
          {act.isError && (
            <Alert variant="danger" isInline title={`${act.variables === "cleanup" ? "Clean up" : "Cancel"} was not done`} id="restore-action-refused" actionClose={<AlertActionCloseButton onClose={() => act.reset()} />}>
              <ErrorText error={act.error} />
            </Alert>
          )}
          {failed && (
            <Alert variant="danger" isInline title={`This restore ${r.operation.state === "refused" ? "was refused" : "failed"}${r.operation.stateReason ? `: ${r.operation.stateReason}` : ""}`}>
              {refusalSentence(op) && <p className="lw-break">{refusalSentence(op)}</p>}
              <p>{RETRY_FRESH_TARGET_SENTENCE}</p>
            </Alert>
          )}
          {notCovered(scope?.complete) && (
            <Alert variant="danger" isInline title="Complete coverage did not cover this restore">
              {verificationScopeSentence(scope)}
            </Alert>
          )}
          {op && !interrupted && (!op.terminal || verified) && (
            <Card>
              <CardBody>
                <Stages op={op} verified={signed} />
                {!op.terminal && op.stage === "running" && replayedMatch && (
                  <Progress
                    className="lw-records-progress"
                    value={(Number(replayedMatch[1]!.replace(/,/g, "")) / Number(replayedMatch[2]!.replace(/,/g, ""))) * 100}
                    title="Records restored"
                    label={`${replayedMatch[1]} of ${replayedMatch[2]}`}
                    measureLocation={ProgressMeasureLocation.outside}
                    aria-label="Records restored"
                  />
                )}
              </CardBody>
            </Card>
          )}
          {op && op.terminal && verified && (
            <Alert variant="success" isInline title="Restored and verified">
              <p>
                {complete && scope?.complete
                  ? `${count(scope.complete.replay.matching)} of ${count(scope.complete.replay.expected)} records match the archive byte for byte (complete verification).`
                  : `${count(op.completion?.recordsSampledMatching)} of ${count(op.completion?.recordsSampled)} sampled records match the archive byte for byte.`}{" "}
                The controller verified the signed scorecard.
              </p>
              {switchable && (
                <p>
                  <strong>Next:</strong> your consumer groups still point at their old positions. Review how each position translates to
                  the restored topics, then switch them over.
                </p>
              )}
            </Alert>
          )}
          {!op && caps?.operationsRead === false && (
            <Alert variant="info" isInline isPlain title="Your role here does not read operations, so progress and evidence are not shown." />
          )}
          {operation.isError && <ErrorState error={operation.error} onRetry={() => void operation.refetch()} />}
        </div>
      </PageSection>
      <PageSection>
        <Grid hasGutter>
          <GridItem lg={7}>
            <Card isFullHeight>
              <CardHeader>
                <CardTitle component="h2">Activity</CardTitle>
              </CardHeader>
              <CardBody>
                {op ? (
                  <>
                    <Facts
                      items={[
                        { term: "State", value: `${op.state}${op.stateReason ? ` (${op.stateReason})` : ""}` },
                        { term: "Stage", value: shown(op.stage) },
                        op.message ? { term: "Message", value: <Prose text={op.message} /> } : null,
                        op.stale ? { term: "Status", value: <StatusLabel tone="warning" text="Too old to believe" /> } : null,
                        { term: "Last update", value: <Instant value={op.lastUpdatedAt} /> },
                      ]}
                    />
                    <ol className={op.terminal ? "lw-timeline lw-gap-top" : "lw-timeline lw-timeline--live lw-gap-top"} aria-live="polite">
                      {[...op.conditions].reverse().map((c) => (
                        <li key={`${c.type}-${c.lastTransitionTime}`}>
                          <Flex justifyContent={{ default: "justifyContentSpaceBetween" }} gap={{ default: "gapSm" }}>
                            <FlexItem>
                              <strong>{c.type}</strong>
                              {c.status !== "True" && <span className="lw-subtle"> ({c.status})</span>}
                              {c.reason && <span className="lw-subtle lw-small"> · {c.reason}</span>}
                            </FlexItem>
                            <FlexItem className="lw-subtle lw-small">
                              <Instant value={c.lastTransitionTime} />
                            </FlexItem>
                          </Flex>
                          {c.message && (
                            <div>
                              <Prose text={c.message} />
                            </div>
                          )}
                        </li>
                      ))}
                    </ol>
                    {(op.diagnostics ?? []).length > 0 && (
                      <>
                        <Content component="h3" className="lw-gap-top">What the controller could see</Content>
                        <ul className="lw-plain-list" aria-label="Diagnoses">
                          {op.diagnostics!.map((d) => (
                            <li key={`${d.code}-${d.lastSeen}`} className="lw-gap-bottom">
                              <StatusLabel tone={d.severity === "Error" ? "danger" : "warning"} text={d.code} /> <Prose text={d.message ?? ""} />
                              <div className="lw-subtle lw-small">
                                {d.object ? `${d.object.kind} ${d.object.name}` : ""}
                                {d.count && d.count > 1 ? ` · ${d.count} times` : ""} · last <Instant value={d.lastSeen} />
                              </div>
                            </li>
                          ))}
                        </ul>
                      </>
                    )}
                    <Content component="p" className="lw-subtle lw-small lw-gap-top">
                      {op.terminal
                        ? "This run is final."
                        : streaming
                          ? "Following this restore through its event stream as it moves; you can leave this page and come back. Leaving cancels nothing."
                          : "Following this restore by re-reading it every second (no event stream is open for this session); you can leave this page and come back. Leaving cancels nothing."}
                    </Content>
                  </>
                ) : (
                  <Content component="p" className="lw-subtle">Not available.</Content>
                )}
              </CardBody>
            </Card>
          </GridItem>
          <GridItem lg={5}>
            <Card isFullHeight>
              <CardHeader>
                <CardTitle component="h2">Plan</CardTitle>
              </CardHeader>
              <CardBody>
                <Facts
                  narrow
                  items={[
                    { term: "Plan hash", value: <Ident value={r.planHash} label="plan sha256" /> },
                    { term: "Approval", value: <>{r.approvalRef.name} <span className="lw-subtle lw-small">({r.approvalSubject === "originalName" ? "subject originalName" : "subject ordinary"})</span></> },
                    { term: "Target", value: `${r.target.clusterRef.name}, ${r.target.originalName ? "original names" : `prefix ${r.target.topicPrefix}`}` },
                    { term: "Mode", value: modeMeaning(r.target.mode, r.target.originalName) },
                    { term: "Point in time", value: exactUtc(r.pointInTime) },
                    { term: "Time basis (plan)", value: planTimeBasis(r.planBytes) ? "producer time" : r.planBytes ? "not stated" : "Not recorded" },
                    { term: "Backup set", value: <Ident value={r.backupSetRef} label="backup set id" /> },
                    { term: "Archive", value: r.sourceDestinationRef ? `saved destination ${r.sourceDestinationRef.name}` : r.sourceArchive.url },
                    { term: "Deadline", value: `${Math.round(r.deadlineSeconds / 60)} min` },
                    { term: "Byte-rate limit", value: rateLimitWords(planRateLimit(r.planBytes)) },
                  ]}
                />
                {r.planBytes ? (
                  <ExpandableSection toggleText={showPlan ? "Hide the plan" : "Show the plan"} onToggle={(_e, o) => setShowPlan(o)} isExpanded={showPlan} className="lw-gap-top">
                    <CodeBlock>
                      <CodeBlockCode>{r.planBytes}</CodeBlockCode>
                    </CodeBlock>
                  </ExpandableSection>
                ) : null}
              </CardBody>
            </Card>
          </GridItem>
          {op && (
            <GridItem lg={6}>
              <Card isFullHeight>
                <CardHeader>
                  <CardTitle component="h2">Result and evidence</CardTitle>
                </CardHeader>
                <CardBody>
                  {!op.terminal ? (
                    <Content component="p" className="lw-subtle">
                      The run has not finished. What it recorded and whether its signed scorecard verified appear here when it does.
                    </Content>
                  ) : (
                  <>
                  <Content component="p">
                    {verified ? (
                      <StatusLabel tone="success" text="Verified" title={`verified at ${op.verification.verifiedAt ?? ""} against key ${op.verification.matchedKeyId ?? ""}`} />
                    ) : (
                      <StatusLabel tone={op.terminal ? "warning" : "neutral"} text="Not verified" />
                    )}{" "}
                    <span className="lw-subtle">{verified ? `${op.trust.state === "verifiedHistorical" ? "by a retired key that was valid when it signed" : "by the controller"}` : evidenceCase(op)}</span>
                  </Content>
                  <Facts
                    narrow
                    items={[
                      { term: "Result", value: `${op.result.status}${typeof op.result.exitCode === "number" ? `, exit ${op.result.exitCode}` : ""}${op.result.exitReason ? ` (${op.result.exitReason})` : ""}` },
                      { term: "Outcome", value: <Claim value={outcomeShown(op)} verified={signed} /> },
                      { term: "Last phase completed", value: shown(op.result.lastPhaseCompleted) },
                      {
                        term: "Signature",
                        value: op.verification.detail ? <Prose text={`${op.verification.state}: ${op.verification.detail}`} /> : op.verification.state,
                      },
                      // DUI-47: an instant beside a pending verdict is the attempt's, not a verdict's.
                      { term: verdictInstantTerm(op.verification.state), value: <Instant value={op.verification.verifiedAt} exact /> },
                      op.verification.matchedKeyId ? { term: "Signed by key", value: <Ident value={op.verification.matchedKeyId} label="signer key id" /> } : null,
                      { term: "Trust", value: `${op.trust.state}, basis ${op.trust.basis}${op.trust.keyState ? `, key ${op.trust.keyState}` : ""}${op.trust.policy?.name ? `, policy ${op.trust.policy.name}` : ""}` },
                      op.evidence.payloadKey ? { term: "Scorecard", value: <Ident value={op.evidence.payloadKey} label="scorecard key" /> } : { term: "Scorecard", value: shown(null) },
                      op.evidence.payloadSha256 ? { term: "Scorecard sha256", value: <Ident value={op.evidence.payloadSha256} label="scorecard sha256" /> } : null,
                      op.evidence.offsetReportKey ? { term: "Offset report", value: <Ident value={op.evidence.offsetReportKey} label="offset report key" /> } : null,
                    ]}
                  />
                  {!signed && op.terminal && <Content component="p" className="lw-subtle lw-small lw-gap-top">{SCORECARD_CLAIM_SENTENCE}</Content>}
                  <ExpandableSection toggleText="Check it yourself" className="lw-gap-top">
                    {evidenceKeys && planBucket ? (
                      <>
                        <Content component="p" className="lw-subtle lw-small">
                          Fetch the signed scorecard from the evidence bucket this restore's approved plan wrote to, then verify it with the Rust
                          reader and the Python one.
                        </Content>
                        <Commands lines={independentCheck(planBucket, op.evidence.payloadKey!, op.evidence.sidecarKey!, planEvidenceRoute(r.planBytes))} label="the commands" />
                      </>
                    ) : (
                      <Content component="p" className="lw-subtle lw-small">
                        This run recorded no scorecard and signature keys, or its plan names no evidence bucket this page can read, so there is
                        nothing to fetch.
                      </Content>
                    )}
                  </ExpandableSection>
                  </>
                  )}
                </CardBody>
              </Card>
            </GridItem>
          )}
          <GridItem lg={op ? 6 : 12}>
            <Card isFullHeight>
              <CardHeader>
                <CardTitle component="h2">Verification</CardTitle>
              </CardHeader>
              <CardBody>
                <Facts
                  narrow
                  items={[
                    {
                      term: "Asked for",
                      value: complete ? `complete, ${typeof r.coverage.completeMaxRecords === "number" ? `at most ${count(r.coverage.completeMaxRecords)} archived records` : "no record bound"}` : "sampled",
                    },
                    {
                      term: "Signed",
                      value:
                        r.coverage?.recorded ?? scope?.coverage ? (
                          <Claim value={coverageWords(r.coverage?.recorded ?? scope?.coverage, r.coverage?.requested, scope?.complete ?? { covered: r.coverage?.covered ?? undefined })} verified={signed} />
                        ) : (
                          coverageWords(null, r.coverage?.requested, null, r.operation.terminal)
                        ),
                    },
                    selectionWords(selection) ? { term: "Restored (signed)", value: <Claim value={selectionWords(selection)} verified={signed} /> } : null,
                    { term: "Time basis (signed)", value: r.timeBasis ? <Claim value={signedTimeBasisText(r.timeBasis)} verified={signed} /> : signedTimeBasisText(null) },
                    schemasLine ? { term: "Schemas (signed)", value: <Claim value={schemasLine} verified={signed} /> } : null,
                  ]}
                />
                {scope && op?.terminal && <Content component="p" className="lw-gap-top">{verificationScopeSentence(scope)}</Content>}
                {r.timeBasis?.notRecorded.length ? (
                  <Alert variant="warning" isInline isPlain className="lw-gap-top" title={`The timestamp type of ${r.timeBasis.notRecorded.join(", ")} was not recorded, so the clock this restore's point in time was read on is unknown.`} />
                ) : null}
              </CardBody>
            </Card>
          </GridItem>
          {op?.verificationScope.complete && (
            <GridItem>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Complete coverage</CardTitle>
                </CardHeader>
                <CardBody>
                  <CompleteCoverage op={op} verified={signed} />
                </CardBody>
              </Card>
            </GridItem>
          )}
          <GridItem>
            <Card>
              <CardHeader>
                <CardTitle component="h2">What this restore produced</CardTitle>
              </CardHeader>
              <CardBody>
                {op?.completion ? (
                  <>
                    <Table aria-label="Topics this restore created" variant="compact">
                      <Thead>
                        <Tr>
                          <Th>Topic</Th>
                          <Th className="lw-num">Partitions</Th>
                        </Tr>
                      </Thead>
                      <Tbody>
                        {(op.completion.newTopics ?? []).map((nt) => (
                          <Tr key={nt.name}>
                            <Td dataLabel="Topic">{nt.name}</Td>
                            <Td dataLabel="Partitions" className="lw-num">{count(nt.partitions)}</Td>
                          </Tr>
                        ))}
                      </Tbody>
                    </Table>
                    <Facts
                      items={[
                        { term: "Records expected", value: <Claim value={count(op.completion.recordsExpected)} verified={signed} /> },
                        { term: labels.window, value: <Claim value={count(op.completion.recordsRestored)} verified={signed} /> },
                        { term: labels.checked, value: <Claim value={count(op.completion.recordsSampled)} verified={signed} /> },
                        { term: labels.matching, value: <Claim value={count(op.completion.recordsSampledMatching)} verified={signed} /> },
                        { term: "Integrity level", value: <Claim value={op.completion.integrityLevel} verified={signed} /> },
                      ]}
                    />
                  </>
                ) : op?.terminal && op.state === "succeeded" ? (
                  <Content component="p" className="lw-subtle">{COMPLETION_NOT_VERIFIED}</Content>
                ) : (
                  <Facts
                    items={[
                      { term: "New topics", value: r.newTopics.length ? r.newTopics.join(", ") : shown(null) },
                      { term: "Source topics (written to by nothing)", value: r.oldTopics?.length ? r.oldTopics.join(", ") : shown(null) },
                    ]}
                  />
                )}
                {op?.terminal && op.state === "succeeded" && (
                  <Content component="p" className="lw-gap-top">{completionGuidance(r.target.mode, r.target.originalName)}</Content>
                )}
                {r.topicPreflight && (
                  <Facts
                    items={[
                      { term: "Target timestamp type", value: shown(r.topicPreflight.timestampType) },
                      { term: "Target retention.ms", value: shown(r.topicPreflight.retentionMs) },
                      { term: "Record age the target accepts", value: timestampBoundWords(r.topicPreflight.timestampBound) },
                    ]}
                  />
                )}
                {op?.teardown && (
                  <>
                    <Content component="h3" className="lw-gap-top">Teardown</Content>
                    <Facts
                      items={[
                        { term: "Deleted", value: op.teardown.deleted?.length ? `${op.teardown.deleted.join(", ")}${op.teardown.deletedTruncated ? " and more" : ""}` : shown(null) },
                        {
                          term: "Not deleted",
                          value: (
                            <>
                              {teardownLeftWords(op.teardown) && <div><strong>{teardownLeftWords(op.teardown)}</strong></div>}
                              {op.teardown.failed?.length ? op.teardown.failed.map((f) => `${f.topic}: ${f.error}`).join("; ") : "none"}
                            </>
                          ),
                        },
                        op.teardown.attestationKey ? { term: "Attestation", value: <Ident value={op.teardown.attestationKey} label="teardown attestation key" /> } : null,
                      ]}
                    />
                  </>
                )}
              </CardBody>
            </Card>
          </GridItem>
          <GridItem>
            {op?.clone ? (
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Clone</CardTitle>
                </CardHeader>
                <CardBody>
                  <CloneStatus clone={op.clone} teardown={op.teardown} restoreName={r.name} />
                </CardBody>
              </Card>
            ) : (
              r.target.mode !== "scratch" && <SwitchoverEntry ws={ws} restore={r} canCreate={caps?.restoreCreate !== false} source={source} />
            )}
          </GridItem>
          {r.topicConfiguration && (
            <GridItem>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Topic configuration</CardTitle>
                </CardHeader>
                <CardBody>
                  <TopicConfigurationResult tc={r.topicConfiguration} />
                </CardBody>
              </Card>
            </GridItem>
          )}
        </Grid>
      </PageSection>
      {confirm && (
        <Modal isOpen variant="small" onClose={() => setConfirm(null)} aria-labelledby="restore-confirm-title">
          <ModalHeader title={confirm === "cancel" ? `Cancel ${r.name}?` : "Clean up the partial topics?"} labelId="restore-confirm-title" titleIconVariant="warning" />
          <ModalBody>
            {confirm === "cancel" ? (
              <Content component="p">{CANCEL_CONFIRM_SENTENCE}</Content>
            ) : (
              <>
                {partialTopics.length > 0 ? (
                  <ul className="lw-plain-list" aria-label="Topics to delete">
                    {partialTopics.map((t) => (
                      <li key={t} className="lw-mono lw-break">{t}</li>
                    ))}
                  </ul>
                ) : (
                  <Content component="p">The topics the signed reconcile records as created by this restore.</Content>
                )}
                <Content component="p" className="lw-gap-top">{CLEANUP_CONFIRM_SENTENCE}</Content>
              </>
            )}
          </ModalBody>
          <ModalFooter>
            <Button variant="danger" onClick={() => run(confirm)} isLoading={act.isPending} isDisabled={act.isPending} id="restore-confirm">
              {confirm === "cancel" ? "Cancel the restore" : "Delete these topics"}
            </Button>
            <Button variant="link" onClick={() => setConfirm(null)}>
              {confirm === "cancel" ? "Keep it running" : "Keep them"}
            </Button>
          </ModalFooter>
        </Modal>
      )}
    </>
  );
}
