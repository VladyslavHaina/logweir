// One Restore's approval: the facts an approver needs to decide, and the one
// action the namespace's mode takes (OD-8):
//   two-person  Approve: the second person's click, with the request and its whole
//               scope (every topic, the window, the target, the plan hash) shown first;
//   strict      Countersign: the console's confirmation to copy, the command, and
//               the sidecar the approver's own key wrote;
//   unbound     Record the approval: the two files `logweir drill approve` wrote;
//   confirm     nothing: the requester's confirmation in the wizard is the whole
//               authorization, and its record is shown.
// Every value about the subject is read from the Restore itself (name, UID,
// approvalRef, and the sha256 of its plan bytes, computed here); a link that
// claims otherwise is refused. Subject kind Restore: theme A's Switchover
// approvals (R7) get their own page beside this one.
import { useEffect, useState, type ReactNode } from "react";
import { Link, useParams, useSearch } from "@tanstack/react-router";
import {
  Alert,
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
  DescriptionList,
  ExpandableSection,
  PageSection,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import SearchIcon from "@patternfly/react-icons/dist/esm/icons/search-icon";
import {
  approvalMode,
  approvalState,
  approvalSubjectWords,
  CONFIRM_ORIGINAL_NAME_SENTENCE,
  confirmationNameFor,
  consoleApprovalOffered,
  MODE_WORDS,
  ORIGINAL_NAME_APPROVAL_SENTENCE,
  outcomeOf,
  planHashOf,
  restoreProgressSentence,
  routeMismatches,
  scopeShowable,
  stateSentence,
  subjectOf,
  UNKNOWN_OUTCOME_SENTENCE,
  useApproval,
  useApprovalGrant,
  useApprovalPacket,
  useApprovalPolicy,
  useApprovalRequest,
  useConsoleApproval,
  useSubjectRestore,
  type ApprovalGrant,
  type ApprovalRequest,
  type Found,
  type Mismatch,
  type OfferView,
  type RouteClaim,
  type Subject,
  type Written,
} from "../../api/approvals";
import type { Restore } from "../../api/types";
import { PageHeader } from "../../components/PageHeader";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { When } from "../../components/When";
import { Ident } from "../../components/Ident";
import { StatusLabel } from "../../components/Status";
import { ApprovalFilesForm, CountersignForm } from "./forms";
import { ApprovalRecord, ApprovalStatus, ErrorLine, NotRecorded, Principal, Refusal, RequestStatus, ScopeCard, Term } from "./parts";
import "./approvals.css";
import { ErrorText } from "../../components/Prose";

/** The sha256 of the Restore's plan bytes, computed here; undefined while it computes. */
function usePlanHash(restore: Restore | null | undefined): string | undefined {
  const [hash, setHash] = useState<{ of: string; value: string }>();
  const key = restore ? `${restore.uid}:${restore.planHash}:${restore.planBytes?.length ?? -1}` : "";
  useEffect(() => {
    let live = true;
    if (restore) void planHashOf(restore).then((value) => live && setHash({ of: key, value }));
    return () => {
      live = false;
    };
  }, [key, restore]);
  return hash && hash.of === key ? hash.value : undefined;
}

/** Why a two-person request is not offered to this sign-in, as a refusal title. */
export const REFUSAL_TITLE: Record<string, string> = {
  requester: "You cannot approve your own request",
  notApprover: "Not offered to you",
  notSecondPerson: "You cannot approve this request",
  localAdmin: "This console cannot take approvals",
  notPending: "Nothing to approve",
  scopeIncomplete: "Not approvable here",
};

function RecordedNotice({ written }: { written: Written | null }) {
  if (!written) return null;
  return (
    <Alert variant="info" isInline title={written.replayed ? "Already recorded" : `Recorded as Approval ${written.approval.name}`} className="lw-gap-top" component="h3" id="approval-recorded">
      {written.replayed
        ? "The same approval was recorded before; nothing new was created."
        : "The controller verifies it before the restore runs. This page shows its verdict below when it has one."}
    </Alert>
  );
}

/** The approval's state in one sentence, or why it is unknown. */
function StateLine({ found, subject, approvalError, showAbsent = false }: { found: Found | null; subject: Subject; approvalError: unknown; showAbsent?: boolean }) {
  if (approvalError) {
    return (
      <Content component="p" id="approval-state">
        Approval {subject.approvalName} could not be read, so whether one exists for this Restore, and whether the controller verified it, is unknown from here. Recording one is still safe: the same content is recognised rather than duplicated, and different content is reported as a conflict and never overwritten.
        <ErrorLine error={approvalError} />
      </Content>
    );
  }
  // An absent Approval is what the action below is for; only under confirm, where
  // nothing is given here, does its absence need saying.
  return found && (found.state !== "absent" || showAbsent) ? (
    <Content component="p" id="approval-state">
      {stateSentence(found, subject)}
    </Content>
  ) : null;
}

/** The subject as read from the Restore, for every mode but two-person. */
function SubjectCard({ ns, restore, subject, mode, computed }: { ns: string; restore: Restore; subject: Subject; mode: string; computed: boolean }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">The request</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle">
          Every value below was read from Restore {restore.name} itself. They are exactly what an approval for it binds, and they are not editable.
        </Content>
        <DescriptionList isCompact orientation={{ md: "horizontal" }} horizontalTermWidthModifier={{ default: "14ch" }} className="lw-gap-top lw-approval-facts">
          <Term term="Restore">
            <Link to="/$ns/restores/$name" params={{ ns, name: restore.name }}>
              {restore.name}
            </Link>
          </Term>
          <Term term="Restore UID">
            <Ident value={subject.uid} label="restore uid" />
          </Term>
          <Term term="Into">
            {restore.target.clusterRef.name}, {restore.target.mode}
            {restore.newTopics.length > 0 && <div className="lw-subtle lw-small lw-break">{restore.newTopics.join(", ")}</div>}
          </Term>
          <Term term="Point in time">
            <When value={restore.pointInTime} exact />
          </Term>
          <Term term="Plan hash">
            <code className="lw-mono lw-break" id="subject-plan-hash">
              {subject.planHash}
            </code>
            <div className="lw-subtle lw-small">{computed ? "The sha256 of its plan bytes, computed on this page." : "As the service computed it: this read carried no plan bytes."}</div>
          </Term>
          <Term term="Approval">
            <code className="lw-mono">{subject.approvalName}</code>
          </Term>
          <Term term="Authorises">{approvalSubjectWords(restore.approvalSubject)}</Term>
          <Term term="Progress">{restoreProgressSentence(restore)}</Term>
        </DescriptionList>
        {restore.approvalSubject === "originalName" && mode !== "twoPerson" && (
          <Alert variant="warning" isInline isPlain title="Original topic names" id="original-name-approval" className="lw-gap-top" component="h3">
            {mode === "confirm" ? CONFIRM_ORIGINAL_NAME_SENTENCE : ORIGINAL_NAME_APPROVAL_SENTENCE}
          </Alert>
        )}
        {typeof restore.planBytes === "string" && (
          <ExpandableSection toggleText="Show the plan" className="lw-gap-top">
            <CodeBlock>
              <CodeBlockCode>{restore.planBytes}</CodeBlockCode>
            </CodeBlock>
          </ExpandableSection>
        )}
      </CardBody>
    </Card>
  );
}

/** The two-person request as the server shows it. Nothing of an unconfirmed request is shown. */
function RequestCard({ request, grant }: { request: ApprovalRequest; grant: ApprovalGrant }) {
  const shown = request.state !== "notConfirmed" && typeof request.requester === "string";
  return (
    <Card>
      <CardHeader actions={{ actions: <RequestStatus state={request.state} />, hasNoOffset: true }}>
        <CardTitle component="h2">The request</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle">
          A second person, signed in as an approver and not the requester, approves here. No key is involved and nothing is pasted: the console records who approved and when, and signs that.
        </Content>
        <Content component="p" id="request-state">
          {request.stateSentence}
        </Content>
        {shown && (
          <DescriptionList isCompact columnModifier={{ default: "1Col", lg: "2Col" }} className="lw-gap-top lw-approval-facts">
            <Term term="Requested by">
              <span id="request-requester">
                <Principal id={request.requester} grant={grant} />
              </span>
            </Term>
            <Term term="Plan hash">
              <code className="lw-mono lw-break" id="request-plan-hash">
                {request.planHash}
              </code>
            </Term>
            <Term term="Authorises">
              <span id="request-approval-subject">{approvalSubjectWords(request.approvalSubject ?? "ordinary")}</span>
            </Term>
            <Term term="Change ticket">{request.ticket ? request.ticket : <NotRecorded />}</Term>
            <Term term="Requested">
              <When value={request.issuedAt} exact />
            </Term>
            <Term term="Expires">
              <span id="request-expires-at">
                <When value={request.expiresAt} exact />
              </span>
            </Term>
            <Term term="Restore UID">
              <Ident value={request.restoreUid} label="restore uid" />
            </Term>
            <Term term="Policy">
              {request.policy} <Ident value={request.policyDigest} label="policy digest" />
            </Term>
            {request.state === "approved" && (
              <>
                <Term term="Approved by">
                  <span id="request-approver">
                    <Principal id={request.approver} grant={grant} />
                  </span>
                </Term>
                <Term term="Approved at">
                  <When value={request.approvedAt} exact />
                </Term>
              </>
            )}
          </DescriptionList>
        )}
      </CardBody>
    </Card>
  );
}

function MismatchRefusal({ ns, subject, mismatches }: { ns: string; subject: Subject; mismatches: Mismatch[] }) {
  return (
    <Refusal title={`This link does not match Restore ${subject.name}, so nothing will be submitted from it`} id="route-mismatch">
      <ul className="lw-plain-list">
        {mismatches.map((m) => (
          <li key={m.field} className="lw-break">
            {m.field}: the link says <code className="lw-mono">{m.claimed}</code>; the Restore carries <code className="lw-mono">{m.actual}</code>
          </li>
        ))}
      </ul>
      <p>
        Open the Restore&apos;s own approval page instead:{" "}
        <Link to="/$ns/approvals/$name" params={{ ns, name: subject.name }} search={{}}>
          {subject.name}
        </Link>
        .
      </p>
    </Refusal>
  );
}

export function ApprovalPage() {
  const { ns, name } = useParams({ from: "/$ns/approvals/$name" });
  const claim = useSearch({ from: "/$ns/approvals/$name" }) as RouteClaim;
  // One page per subject: what was approved here, and the click's own state,
  // never carry over to the next Restore's page.
  return <SubjectPage key={`${ns}/${name}`} ns={ns} name={name} claim={claim} />;
}

function SubjectPage({ ns, name, claim }: { ns: string; name: string; claim: RouteClaim }) {
  const grant = useApprovalGrant(ns);
  const policyQ = useApprovalPolicy(ns, grant.approvalsRead);
  const restoreQ = useSubjectRestore(ns, name, grant.restoresRead);
  const restore = restoreQ.data;
  const hash = usePlanHash(restore);
  const subject = restore && hash ? subjectOf(restore, hash) : null;
  const approvalName = restore?.approvalRef.name ?? claim.approval ?? "";
  const approvalQ = useApproval(ns, approvalName, grant.approvalsRead && restoreQ.isSuccess);
  const policy = policyQ.data;
  const mode = policy ? approvalMode(policy) : null;
  const requestQ = useApprovalRequest(ns, name, mode === "twoPerson" && Boolean(restore) && policy?.consoleApprovalAvailable === true);
  const confirmationQ = useApprovalPacket(ns, confirmationNameFor(approvalName), mode === "strict" && Boolean(restore) && grant.approvalPacketRead);
  const typedQ = useApprovalPacket(ns, approvalName, approvalQ.data?.approvalSubject === "originalName" && grant.approvalPacketRead);
  const approve = useConsoleApproval(ns);
  const [written, setWritten] = useState<Written | null>(null);

  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/approvals" params={{ ns }}>Approvals</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );

  const loading = (label: string) => (
    <>
      <PageHeader title={name} breadcrumb={breadcrumb} />
      <PageSection>
        <LoadingRows rows={6} label={label} />
      </PageSection>
    </>
  );

  if (!grant.known || (grant.approvalsRead && policyQ.isPending) || (grant.restoresRead && restoreQ.isPending)) return loading("Loading the approval");
  if (!grant.approvalsRead || !grant.restoresRead) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <Card>
            <Empty title="You can't read this approval" icon={SearchIcon}>
              Your sign-in holds no role in {ns} that reads restores and approvals.
            </Empty>
          </Card>
        </PageSection>
      </>
    );
  }
  if (restoreQ.isError) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <ErrorState error={restoreQ.error} onRetry={() => void restoreQ.refetch()} />
        </PageSection>
      </>
    );
  }
  if (restore === null) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <Stack hasGutter>
            <StackItem>
              <Card>
                <Empty title={`No Restore named ${name}`} icon={SearchIcon}>
                  No Restore named {name} exists in namespace {ns}, so there is nothing to approve and no form is offered. An approval is recorded for a Restore that exists.
                </Empty>
              </Card>
            </StackItem>
            {approvalQ.data && (
              <StackItem>
                <ApprovalRecord approval={approvalQ.data} packet={typedQ.data} grant={grant} />
              </StackItem>
            )}
          </Stack>
        </PageSection>
      </>
    );
  }
  if (!restore || !subject) return loading("Computing the plan hash");

  const found: Found | null = approvalQ.isError ? null : approvalQ.isPending ? null : approvalState(approvalQ.data ?? null, subject);
  const mismatches = routeMismatches(claim, subject);
  const hashDisagrees = typeof restore.planBytes === "string" && subject.planHash !== restore.planHash;
  const pending = restore.operation.state === "pending";
  const blocked = mismatches.length > 0 || hashDisagrees;

  const status = found ? <ApprovalStatus found={found} waiting /> : <StatusLabel tone="neutral" text={approvalQ.isPending ? "Reading" : "State unknown"} />;
  const description = policy ? `${MODE_WORDS[approvalMode(policy)]} · policy ${policy.name}` : "The namespace's approval policy could not be read.";

  const refusals = (
    <>
      {mismatches.length > 0 && (
        <StackItem>
          <MismatchRefusal ns={ns} subject={subject} mismatches={mismatches} />
        </StackItem>
      )}
      {hashDisagrees && (
        <StackItem>
          <Refusal title="The plan this page read is not the plan the service hashed" id="plan-hash-disagrees">
            Its bytes hash to {subject.planHash} here and to {restore.planHash} at the service. Nothing can be approved from this read: reload the page.
          </Refusal>
        </StackItem>
      )}
    </>
  );

  let body: ReactNode;
  if (policyQ.isError || !policy || !mode) {
    body = (
      <>
        <StackItem>
          <SubjectCard ns={ns} restore={restore} subject={subject} mode="unknown" computed={typeof restore.planBytes === "string"} />
        </StackItem>
        <StackItem>
          <Card>
            <CardHeader>
              <CardTitle component="h2">Approval policy</CardTitle>
            </CardHeader>
            <CardBody>
              <ErrorState error={policyQ.error ?? new Error("The approval policy could not be read.")} onRetry={() => void policyQ.refetch()} />
              <Content component="p" className="lw-subtle">
                Without the namespace&apos;s policy this page cannot say how this restore is approved, so it offers nothing.
              </Content>
            </CardBody>
          </Card>
        </StackItem>
      </>
    );
  } else if (mode === "twoPerson") {
    const request = requestQ.data;
    const refused: Mismatch[] = hashDisagrees ? [...mismatches, { field: "plan hash", claimed: subject.planHash, actual: restore.planHash }] : mismatches;
    const view: OfferView = { policy, restoreExists: true, maySubmit: grant.approvalSubmit, mismatches: refused, request, subject };
    const offered = consoleApprovalOffered(view) && !written;
    const scope = scopeShowable(request);
    let decision: ReactNode;
    if (policy.consoleApprovalAvailable !== true) {
      decision = (
        <Refusal title="This console cannot take a two-person approval" id="console-approval-unavailable">
          It is the in-cluster administrator console, whose one identity cannot be two people, or its confirmation key is not there yet. A second person approves in the shared console.
        </Refusal>
      );
    } else if (requestQ.isPending) {
      decision = <LoadingRows rows={2} label="Reading the request" />;
    } else if (requestQ.isError || !request) {
      decision = (
        <Refusal title="The request could not be read" id="console-approval-unread">
          Nothing about it is shown, and nothing can be approved from here. <ErrorLine error={requestQ.error} />
        </Refusal>
      );
    } else if (offered) {
      decision = (
        <>
          <Content component="p" id="approve-sentence">
            {request.approve.sentence}
          </Content>
          <div className="lw-decision">
            <Button variant="primary" id="approve" isLoading={approve.isPending} isDisabled={approve.isPending} onClick={() => approve.mutate(view, { onSuccess: setWritten })}>
              Approve
            </Button>
            <span className="lw-subtle lw-small">Approving sends the hash of the request shown above and nothing else.</span>
          </div>
        </>
      );
    } else if (!written) {
      const readerOnly = !grant.approvalSubmit && request.approve.offered;
      decision = (
        <Refusal title={readerOnly ? "Not offered to you" : (REFUSAL_TITLE[request.approve.refusal ?? ""] ?? "Not offered to you")} id="approve-not-offered">
          {readerOnly
            ? "This login may read the request and not approve it: that needs the approver role in this namespace, held by someone other than the requester."
            : request.approve.sentence || "This login is not offered an approval for this request."}
        </Refusal>
      );
    }
    const failure = approve.error ? (
      outcomeOf(approve.error) === "unknown" ? (
        <Alert variant="warning" isInline title="The answer did not arrive" className="lw-gap-top" component="h3">
          {UNKNOWN_OUTCOME_SENTENCE}
        </Alert>
      ) : (
        <Refusal title="Not approved" id="approve-refused">
          <ErrorText error={approve.error} />
        </Refusal>
      )
    ) : null;
    body = (
      <>
        <StackItem>
          {request ? (
            <RequestCard request={request} grant={grant} />
          ) : (
            <SubjectCard ns={ns} restore={restore} subject={subject} mode={mode} computed={typeof restore.planBytes === "string"} />
          )}
        </StackItem>
        {request && request.state !== "notConfirmed" && typeof request.requester === "string" && (
          <StackItem>
            {scope ? (
              <ScopeCard scope={scope} />
            ) : (
              <Refusal title="What this request approves cannot be shown in full" id="scope-incomplete">
                {request.scopeSentence || "What this request approves could not be shown in full, so it cannot be approved here."}
              </Refusal>
            )}
          </StackItem>
        )}
        <StackItem>
          <Card id="decision">
            <CardHeader>
              <CardTitle component="h2">Your decision</CardTitle>
            </CardHeader>
            <CardBody>
              <StateLine found={found} subject={subject} approvalError={approvalQ.error} />
              {decision}
              {failure}
              <RecordedNotice written={found?.state === "verified" ? null : written} />
            </CardBody>
          </Card>
        </StackItem>
      </>
    );
  } else {
    const absent = found?.state === "absent";
    let action: ReactNode;
    let title: string;
    if (mode === "confirm") {
      title = "Confirmed in the console";
      action = (
        <Content component="p" id="ordinary-confirmation">
          Policy {policy.name} is confirm (one person, no key): the console recorded its signed confirmation of the requester as Approval {subject.approvalName} when the Restore was created, and there is nothing for an approver to add. The state above is the controller&apos;s verdict on it.
        </Content>
      );
    } else if (mode === "strict") {
      title = "Countersign";
      const offered = pending && !blocked && absent;
      action = (
        <>
          <Content component="p">
            Policy {policy.name} is strict: this restore runs only after an approver who is not its requester countersigns the console&apos;s confirmation with their personal key.
          </Content>
          {!offered ? null : !grant.approvalSubmit ? (
            <Refusal title="Not offered to you" id="countersign-read-only">
              This login may read the governed approval and not submit one: that needs the approver role, held by someone other than the requester.
            </Refusal>
          ) : confirmationQ.isError ? (
            <Refusal title="The console's confirmation could not be read">
              <ErrorLine error={confirmationQ.error} />
            </Refusal>
          ) : grant.approvalPacketRead && confirmationQ.isPending ? (
            <LoadingRows rows={2} label="Reading the console's confirmation" />
          ) : grant.approvalPacketRead && confirmationQ.data === null ? (
            <Refusal title="Nothing to countersign" id="no-confirmation">
              No console confirmation {confirmationNameFor(subject.approvalName)} exists for this Restore. A Restore submitted through the console carries one; a Restore written directly to the cluster in a governed namespace never runs.
            </Refusal>
          ) : (
            <CountersignForm ns={ns} subject={subject} packet={confirmationQ.data ?? null} onDone={setWritten} />
          )}
        </>
      );
    } else {
      title = "Record the approval";
      const offered = pending && !blocked && subject.approvalName.length > 0 && (absent || approvalQ.isError);
      action = !offered ? null : !grant.approvalSubmit ? (
        <Refusal title="Not offered to you" id="files-read-only">
          This login may read the approval and not record one: that needs the approver role in this namespace.
        </Refusal>
      ) : (
        <ApprovalFilesForm ns={ns} subject={subject} onDone={setWritten} />
      );
    }
    body = (
      <>
        <StackItem>
          <SubjectCard ns={ns} restore={restore} subject={subject} mode={mode} computed={typeof restore.planBytes === "string"} />
        </StackItem>
        <StackItem>
          <Card id="decision">
            <CardHeader>
              <CardTitle component="h2">{title}</CardTitle>
            </CardHeader>
            <CardBody>
              <StateLine found={found} subject={subject} approvalError={approvalQ.error} showAbsent={mode === "confirm"} />
              {action}
              <RecordedNotice written={found?.state === "verified" ? null : written} />
            </CardBody>
          </Card>
        </StackItem>
      </>
    );
  }

  return (
    <>
      <PageHeader title={name} breadcrumb={breadcrumb} status={status} description={description} />
      <PageSection className="lw-approval-page">
        <Stack hasGutter>
          {refusals}
          {body}
          {approvalQ.data && (
            <StackItem>
              <ApprovalRecord approval={approvalQ.data} packet={typedQ.data} grant={grant} />
            </StackItem>
          )}
        </Stack>
      </PageSection>
    </>
  );
}
