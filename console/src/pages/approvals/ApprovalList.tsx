// The Approvals page: what waits for an approval in this workspace, and every
// Approval recorded here. Neither list takes the page away when it cannot be
// read: an approver who may read one and not the other still gets a usable page.
import { Link, useParams } from "@tanstack/react-router";
import { Card, CardBody, CardHeader, CardTitle, Content, PageSection, Stack, StackItem } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import UserCheckIcon from "@patternfly/react-icons/dist/esm/icons/user-check-icon";
import CheckCircleIcon from "@patternfly/react-icons/dist/esm/icons/check-circle-icon";
import {
  approvalMode,
  approvalState,
  modeSentence,
  noApprovalSentence,
  recordState,
  useApprovalGrant,
  useApprovalPolicy,
  useApprovalRequests,
  useApprovals,
  useWaitingRestores,
  useWaitingSwitchovers,
  type Approval,
  type ApprovalGrant,
  type ApprovalMode,
  type ApprovalRequest,
} from "../../api/approvals";
import { useSwitchoverApprovalRequests, type Switchover } from "../../api/switchovers";
import type { Restore } from "../../api/types";
import type { ReactNode } from "react";
import { PageHeader } from "../../components/PageHeader";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { When } from "../../components/When";
import { Ident } from "../../components/Ident";
import { CompactList, useNarrow } from "../../components/CompactList";
import { StatusLabel } from "../../components/Status";
import { ApprovalStatus, ErrorLine, principalText } from "./parts";
import "./approvals.css";

/**
 * Where an Approval's subject is decided: a Restore's page, or a Switchover's (theme A).
 * DUI-46: a Switchover's approval page is the two-person request, which exists only in a
 * two-person workspace; under any other mode (or one not known) its record opens the
 * Switchover's own page, as the Waiting table's link does.
 */
export function subjectHref(ws: string, approval: Approval, mode: ApprovalMode | null): string | undefined {
  if (approval.subjectRef.kind === "Restore") {
    return `#/${ws}/approvals/${encodeURIComponent(approval.subjectRef.name)}?approval=${encodeURIComponent(approval.name)}`;
  }
  if (approval.subjectRef.kind === "Switchover") return switchoverHref(ws, approval.subjectRef.name, mode);
  return undefined;
}

/** A switchover's link from Approvals: its two-person approval page, or its own page. */
export function switchoverHref(ws: string, name: string, mode: ApprovalMode | null): string {
  return mode === "twoPerson" ? `#/${ws}/approvals/switchover/${encodeURIComponent(name)}` : `#/${ws}/switchovers/${encodeURIComponent(name)}`;
}

const NOTHING_WAITS: Record<ApprovalMode, string> = {
  confirm: "Under confirm a restore or a switchover runs on its requester's own confirmation, so nothing waits for an approver here.",
  twoPerson: "When someone asks for a restore or a switchover, it waits here until a second person approves it.",
  strict: "When someone asks for a restore or a switchover, it waits here until an approver countersigns it with their key.",
  files: "A restore or a switchover created in this workspace waits here until an approver records the files logweir drill approve wrote.",
};

/** The part of a restore's or a switchover's request that says what happens next. */
type RequestRead = { data?: { state: ApprovalRequest["state"]; approve: ApprovalRequest["approve"] }; isError: boolean; isPending: boolean };

/** What happens next for one waiting restore or switchover, for this sign-in. */
function nextStep(mode: ApprovalMode | null, grant: ApprovalGrant, request: RequestRead | undefined): { text: string; mine: boolean } {
  switch (mode) {
    case "twoPerson": {
      if (!request || request.isPending) return { text: "Reading the request", mine: false };
      if (request.isError || !request.data) return { text: "The request could not be read", mine: false };
      const r = request.data;
      if (r.state === "approved") return { text: "Approved in the console", mine: false };
      if (r.state === "expired") return { text: "Expired: submit the restore again", mine: false };
      if (r.state === "notConfirmed") return { text: "Not confirmed by this console", mine: false };
      if (r.approve.offered) return { text: "Waits for you", mine: true };
      if (r.approve.refusal === "requester") return { text: "You requested it: a second person approves", mine: false };
      return { text: "Waits for another approver", mine: false };
    }
    case "strict":
      return grant.approvalSubmit ? { text: "Countersign with your key", mine: true } : { text: "Waits for an approver's countersignature", mine: false };
    case "files":
      return grant.approvalSubmit ? { text: "Record the approver's files", mine: true } : { text: "Waits for an approver's files", mine: false };
    case "confirm":
      return { text: "The console's confirmation is being verified", mine: false };
    default:
      return { text: "Open it for the workspace's approval policy", mine: false };
  }
}

/** One row of "Waiting for approval": a Restore's or a Switchover's (DUI-27). */
interface WaitingRow {
  key: string;
  name: string;
  href: string;
  link: ReactNode;
  sub: string;
  step: { text: string; mine: boolean };
  found: ReturnType<typeof approvalState> | null;
  requestedAt: string | null | undefined;
}

function byNewest<T extends { createdAt?: string | null; name: string }>(items: T[]): T[] {
  return [...items].sort((a, b) => String(b.createdAt ?? "").localeCompare(String(a.createdAt ?? "")) || a.name.localeCompare(b.name));
}

export function ApprovalList() {
  const { ws } = useParams({ from: "/$ws/approvals" });
  const grant = useApprovalGrant(ws);
  const narrow = useNarrow();
  const policy = useApprovalPolicy(ws, grant.approvalsRead);
  const approvals = useApprovals(ws, grant.approvalsRead);
  const waiting = useWaitingRestores(ws, grant.restoresRead);
  // DUI-27: a switchover held for its approval waits here too, as a restore does.
  const waitingSwitchovers = useWaitingSwitchovers(ws, grant.restoresRead);
  const mode = policy.data ? approvalMode(policy.data) : null;
  const restores = byNewest(waiting.data ?? []);
  const switchovers: Switchover[] = waitingSwitchovers.data ?? [];
  const requests = useApprovalRequests(
    ws,
    restores.map((r) => r.name),
    mode === "twoPerson",
  );
  const switchoverRequests = useSwitchoverApprovalRequests(
    ws,
    switchovers.map((s) => s.name),
    mode === "twoPerson",
  );

  const description = policy.data
    ? modeSentence(approvalMode(policy.data), policy.data.name)
    : policy.isError
      ? "The workspace's approval policy could not be read, so this page cannot say how approving works here."
      : "What waits for an approval here, and every approval recorded.";

  if (grant.known && !grant.approvalsRead) {
    return (
      <>
        <PageHeader title="Approvals" />
        <PageSection>
          <Card>
            <Empty title="You can't read approvals here" icon={UserCheckIcon}>
              Your sign-in holds no role in {ws} that reads approvals. An administrator binds the approver, operator or viewer role.
            </Empty>
          </Card>
        </PageSection>
      </>
    );
  }

  const byName = new Map<string, Approval>((approvals.data ?? []).map((a) => [a.name, a]));

  const waitingRows: WaitingRow[] = [
    ...restores.map((r: Restore, i) => ({
      key: `restore/${r.name}`,
      name: r.name,
      href: `#/${ws}/approvals/${encodeURIComponent(r.name)}`,
      link: (
        <Link to="/$ws/approvals/$name" params={{ ws, name: r.name }} search={{}} className="lw-break">
          {r.name}
        </Link>
      ),
      sub: `Restore into ${r.target.clusterRef.name} · ${r.target.originalName ? "original names" : `prefix ${r.target.topicPrefix || "none"}`}`,
      step: nextStep(mode, grant, requests[i]),
      found: approvals.isError ? null : approvalState(byName.get(r.approvalRef.name) ?? null, { name: r.name, uid: r.uid, namespace: r.namespace, planHash: "", approvalName: r.approvalRef.name }),
      requestedAt: r.createdAt,
    })),
    ...switchovers.map((s, i) => ({
      key: `switchover/${s.name}`,
      name: s.name,
      // A second person decides on the switchover's approval page; under strict or with
      // files the Switchover's own page gives the commands.
      href: switchoverHref(ws, s.name, mode),
      link:
        mode === "twoPerson" ? (
          <Link to="/$ws/approvals/switchover/$name" params={{ ws, name: s.name }} className="lw-break">
            {s.name}
          </Link>
        ) : (
          <Link to="/$ws/switchovers/$name" params={{ ws, name: s.name }} className="lw-break">
            {s.name}
          </Link>
        ),
      sub: `Switchover of restore ${s.restore.name}`,
      step: nextStep(mode, grant, switchoverRequests[i]),
      found: approvals.isError
        ? null
        : approvalState(byName.get(s.approvalName) ?? null, { name: s.name, uid: s.uid, namespace: s.namespace, planHash: "", approvalName: s.approvalName }, "Switchover"),
      requestedAt: switchoverRequests[i]?.data?.issuedAt,
    })),
  ];
  // The restores decide the card; switchovers that cannot be listed are said in a line beside them.
  const waitingPending = waiting.isPending || (waitingSwitchovers.isPending && !waitingSwitchovers.isError);
  const waitingKnown = waiting.data !== undefined && waitingSwitchovers.data !== undefined;

  const waitingCard = (
    <Card>
      <CardHeader>
        <CardTitle component="h2">
          Waiting for approval{waitingKnown ? ` (${waitingRows.length})` : ""}
        </CardTitle>
      </CardHeader>
      <CardBody>
        {approvals.isError && (
          <Content component="p" className="lw-subtle">
            The Approvals in this workspace could not be listed, so this page cannot say which restores or switchovers already have one. Each restore&apos;s own page reads its Approval directly.
            <ErrorLine error={approvals.error} />
          </Content>
        )}
        {waitingSwitchovers.isError && (
          <Content component="p" className="lw-subtle">
            The switchovers in this workspace could not be listed, so one that waits for an approval is not shown here.
            <ErrorLine error={waitingSwitchovers.error} />
          </Content>
        )}
        {!grant.restoresRead && grant.known ? (
          <Content component="p" className="lw-subtle">
            Your sign-in may not list restores or switchovers in {ws}, so none can be chosen here.
          </Content>
        ) : waitingPending ? (
          <LoadingRows rows={3} label="Loading what waits" />
        ) : waiting.isError ? (
          <ErrorState error={waiting.error} onRetry={() => void waiting.refetch()} />
        ) : waitingRows.length === 0 ? (
          waitingSwitchovers.isError ? null : (
            <Empty title="Nothing waits for approval" icon={CheckCircleIcon}>
              {mode ? NOTHING_WAITS[mode] : "No restore or switchover in this workspace waits for an approval."}
            </Empty>
          )
        ) : narrow ? (
          <CompactList
            flush
            label="Waiting for approval"
            items={waitingRows.map(({ key, name, href, sub, step, found, requestedAt }) => ({
              key,
              href,
              title: name,
              meta: (
                <>
                  {sub} · {step.mine ? <strong>{step.text}</strong> : step.text} · <When value={requestedAt} />
                </>
              ),
              status: found ? <ApprovalStatus found={found} waiting /> : <StatusLabel tone="neutral" text="Unknown" />,
            }))}
          />
        ) : (
          <Table aria-label="Waiting for approval" variant="compact" gridBreakPoint="grid-md">
            <Thead>
              <Tr>
                <Th>Subject</Th>
                <Th>Approval</Th>
                <Th>Next</Th>
                <Th modifier="nowrap">Requested</Th>
              </Tr>
            </Thead>
            <Tbody>
              {waitingRows.map(({ key, link, sub, step, found, requestedAt }) => (
                <Tr key={key}>
                  <Td dataLabel="Subject">
                    {link}
                    <div className="lw-subtle lw-small lw-break">{sub}</div>
                  </Td>
                  <Td dataLabel="Approval">{found ? <ApprovalStatus found={found} waiting /> : <StatusLabel tone="neutral" text="Unknown: not readable" />}</Td>
                  <Td dataLabel="Next">{step.mine ? <strong>{step.text}</strong> : step.text}</Td>
                  <Td dataLabel="Requested" modifier="nowrap">
                    <When value={requestedAt} />
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        )}
      </CardBody>
    </Card>
  );

  const recorded = byNewest(approvals.data ?? []);
  const recordedCard = (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Recorded approvals</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle">
          An Approval that exists is not an approval; one the controller verified is. One that did not verify stays here with its reason, as the record of a rejected attempt.
        </Content>
        <div className="lw-gap-top">
          {approvals.isPending ? (
            <LoadingRows rows={3} label="Loading approvals" />
          ) : approvals.isError ? (
            <ErrorState error={approvals.error} onRetry={() => void approvals.refetch()} />
          ) : recorded.length === 0 ? (
            <Empty title="No approvals recorded" icon={UserCheckIcon}>
              {mode ? noApprovalSentence(mode) : "No Approval in this workspace yet."}
            </Empty>
          ) : narrow ? (
            <CompactList
              flush
              label="Recorded approvals"
              items={recorded.map((a) => ({
                key: a.name,
                href: subjectHref(ws, a, mode),
                title: `${a.subjectRef.kind}/${a.subjectRef.name}`,
                meta: (
                  <>
                    {a.approver ? principalText(a.approver, grant) : "No approver recorded"} · <When value={a.createdAt} />
                  </>
                ),
                status: <ApprovalStatus found={recordState(a)} />,
              }))}
            />
          ) : (
            <Table aria-label="Recorded approvals" variant="compact" gridBreakPoint="grid-md">
              <Thead>
                <Tr>
                  <Th>Subject</Th>
                  <Th>Verdict</Th>
                  <Th>Approver</Th>
                  <Th>Matched key</Th>
                  <Th modifier="nowrap">Recorded</Th>
                </Tr>
              </Thead>
              <Tbody>
                {recorded.map((a) => {
                  const href = subjectHref(ws, a, mode);
                  return (
                    <Tr key={a.name}>
                      <Td dataLabel="Subject" className="lw-break">
                        {href ? <a href={href}>{a.subjectRef.name}</a> : a.subjectRef.name}
                        <div className="lw-subtle lw-small">
                          {a.subjectRef.kind} · Approval {a.name}
                        </div>
                      </Td>
                      <Td dataLabel="Verdict">
                        <ApprovalStatus found={recordState(a)} />
                      </Td>
                      <Td dataLabel="Approver">
                        {a.approver ? (
                          <span className="lw-break lw-mono">{principalText(a.approver, grant)}</span>
                        ) : (
                          <span className="lw-subtle">Not recorded</span>
                        )}
                      </Td>
                      <Td dataLabel="Matched key">{a.matchedKeyId ? <Ident value={a.matchedKeyId} label="matched key id" /> : <span className="lw-subtle">Not recorded</span>}</Td>
                      <Td dataLabel="Recorded" modifier="nowrap">
                        <When value={a.createdAt} />
                      </Td>
                    </Tr>
                  );
                })}
              </Tbody>
            </Table>
          )}
        </div>
      </CardBody>
    </Card>
  );

  return (
    <>
      <PageHeader title="Approvals" description={description} />
      <PageSection>
        <Stack hasGutter>
          {/* Under confirm nothing waits for an approver; the card shows only while a confirmation verifies. */}
          {!(mode === "confirm" && waitingKnown && waitingRows.length === 0) && <StackItem>{waitingCard}</StackItem>}
          <StackItem>{recordedCard}</StackItem>
        </Stack>
      </PageSection>
    </>
  );
}
