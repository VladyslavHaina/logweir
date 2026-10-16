// One Switchover (PROD-04.2 §9 screen 5): where it stands, each group's outcome, the
// signed record, the plan exactly as approved, what a two-person approver is shown,
// and what undoing it would take. A re-run is a new review of the restore (a new plan
// and a new approval), never a button that re-submits this object.
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
  CodeBlock,
  CodeBlockCode,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  PageSection,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import { useRestore } from "../../api/queries";
import { useCan } from "../../api/session";
import { ApiError } from "../../api/client";
import { shellArg } from "../../lib/shell";
import { fileName, isTerminal, reasonWords, useSwitchover, useSwitchoverApprovalRequest, type Switchover } from "../../api/switchovers";
import { useApprovalPolicy } from "../../api/approvals";
import { PageHeader } from "../../components/PageHeader";
import { ErrorState, LoadingRows } from "../../components/States";
import { Ident } from "../../components/Ident";
import { PhaseLabel } from "./status";
import { PlanBlock } from "./parts";
import { Prose } from "../../components/Prose";
import { SwitchoverResult, cutoverRefusal } from "./Cutover";
import { SwitchoverApprovalSummary } from "./SwitchoverApprovalSummary";

function ApprovalCard({ ws, sw }: { ws: string; sw: Switchover }) {
  const policy = useApprovalPolicy(ws);
  const canRead = useCan(ws, "approvalsRead");
  const mode = policy.data?.operatorMode;
  const waiting = sw.phase === "Pending" && sw.reason === "ApprovalNotVerified";
  const request = useSwitchoverApprovalRequest(ws, sw.name, waiting && mode === "two-person" && canRead);
  if (!waiting || !policy.data) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Approval</CardTitle>
      </CardHeader>
      <CardBody>
        {mode === "two-person" ? (
          <>
            <Content component="p">
              A second person approves this in{" "}
              <Link to="/$ws/approvals/switchover/$name" params={{ ws, name: sw.name }}>
                Approvals
              </Link>
              . This is what they are shown: every group and position, and nothing else is committed.
            </Content>
            {!canRead ? (
              <Content component="p" className="lw-subtle">
                Your role here cannot read approval requests.
              </Content>
            ) : request.isPending ? (
              <LoadingRows rows={3} />
            ) : request.isError ? (
              <ErrorState error={request.error} onRetry={() => void request.refetch()} />
            ) : (
              <div className="lw-gap-top">
                <SwitchoverApprovalSummary request={request.data} />
              </div>
            )}
          </>
        ) : policy.data.legacy ? (
          <>
            <Content component="p">
              This workspace has no approval policy. An approver signs the plan and uploads both files in{" "}
              <Link to="/$ws/approvals" params={{ ws }}>
                Approvals
              </Link>
              :
            </Content>
            <CodeBlock>
              <CodeBlockCode>{`logweir drill approve --subject-kind Switchover --spec ${shellArg(fileName(sw.name, ".switchover-plan.yaml", ""), "storageKey", "<the downloaded plan>")}`}</CodeBlockCode>
            </CodeBlock>
          </>
        ) : (
          <>
            <Content component="p">
              An approver countersigns the console's request ({sw.approvalName}-confirmation) with their personal key and uploads the
              signature in{" "}
              <Link to="/$ws/approvals" params={{ ws }}>
                Approvals
              </Link>
              :
            </Content>
            <CodeBlock>
              <CodeBlockCode>{`logweir drill countersign --document <request> --confirmation <console sidecar> --key <your key.pem> --out approval.sig`}</CodeBlockCode>
            </CodeBlock>
          </>
        )}
      </CardBody>
    </Card>
  );
}

export function SwitchoverPage() {
  const { ws, name } = useParams({ from: "/$ws/switchovers/$name" });
  const navigate = useNavigate();
  const sw = useSwitchover(ws, name);
  // Asked only once the Switchover names its restore (DUI-31.8: no read of an empty name).
  const restore = useRestore(ws, sw.data?.restore.name ?? "");
  // DUI-30: "Review again" starts a review, a write; a role that may not create a restore here is not offered it.
  const canWrite = useCan(ws, "restoreCreate");
  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ws/switchovers" params={{ ws }}>Switchovers</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );
  if (sw.isPending || sw.isError) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>{sw.isPending ? <LoadingRows rows={6} /> : <ErrorState error={sw.error} onRetry={() => void sw.refetch()} />}</PageSection>
      </>
    );
  }
  const s = sw.data;
  const gone = restore.isError && restore.error instanceof ApiError && restore.error.status === 404;
  const sameRestore = restore.data && restore.data.uid === s.restore.uid;
  const target = sameRestore ? restore.data!.target.clusterRef.name : undefined;
  return (
    <>
      <PageHeader
        title={name}
        breadcrumb={breadcrumb}
        status={<PhaseLabel sw={s} />}
        description={`Applies restore ${s.restore.name}'s mapped consumer positions${target ? ` on ${target}` : ""}.`}
        actions={
          // DUI-28: after any outcome, a Succeeded one included, the way back to a new review.
          isTerminal(s) && sameRestore && canWrite && !cutoverRefusal(restore.data!) ? (
            <Button variant="secondary" onClick={() => void navigate({ to: "/$ws/restores/$name/cutover", params: { ws, name: s.restore.name }, search: {} })}>
              Review again
            </Button>
          ) : undefined
        }
      />
      <PageSection>
        <Stack hasGutter>
          <StackItem>
            <SwitchoverResult ws={ws} name={name} restore={sameRestore ? restore.data : undefined} onPage />
          </StackItem>
          <StackItem>
            <ApprovalCard ws={ws} sw={s} />
          </StackItem>
          <StackItem>
            <Card>
              <CardHeader>
                <CardTitle component="h2">Details</CardTitle>
              </CardHeader>
              <CardBody>
                <DescriptionList isCompact className="lw-dl" orientation={{ sm: "vertical", md: "horizontal" }}>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Restore</DescriptionListTerm>
                    <DescriptionListDescription>
                      {gone ? (
                        <>
                          {s.restore.name} <span className="lw-subtle">(deleted since)</span>
                        </>
                      ) : (
                        <Link to="/$ws/restores/$name" params={{ ws, name: s.restore.name }}>
                          {s.restore.name}
                        </Link>
                      )}
                      {restore.data && !sameRestore && <div className="lw-subtle lw-small">A restore of that name exists now, but it is another object (another UID).</div>}
                      <div className="lw-subtle lw-small">UID {s.restore.uid}</div>
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Approval</DescriptionListTerm>
                    <DescriptionListDescription>{s.approvalName}</DescriptionListDescription>
                  </DescriptionListGroup>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Plan</DescriptionListTerm>
                    <DescriptionListDescription>
                      <Ident value={s.planHash} label="switchover plan sha256" />
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Reason</DescriptionListTerm>
                    <DescriptionListDescription>
                      {s.reason ? (
                        <>
                          <span className="lw-mono">{s.reason}</span>
                          {reasonWords(s.reason) && <div className="lw-subtle lw-small">{reasonWords(s.reason)}</div>}
                        </>
                      ) : (
                        <span className="lw-subtle">Not recorded</span>
                      )}
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                  {s.message && (
                    <DescriptionListGroup>
                      <DescriptionListTerm>Message</DescriptionListTerm>
                      <DescriptionListDescription className="lw-break">
                        <Prose text={s.message} />
                      </DescriptionListDescription>
                    </DescriptionListGroup>
                  )}
                  <DescriptionListGroup>
                    <DescriptionListTerm>Exit code</DescriptionListTerm>
                    <DescriptionListDescription>{s.exitCode ?? <span className="lw-subtle">Not recorded</span>}</DescriptionListDescription>
                  </DescriptionListGroup>
                </DescriptionList>
                {s.planBytes ? (
                  <div className="lw-gap-top">
                    <PlanBlock bytes={s.planBytes} name={s.name} />
                  </div>
                ) : (
                  <Alert isInline isPlain variant="info" title="The plan was not returned" className="lw-gap-top" />
                )}
              </CardBody>
            </Card>
          </StackItem>
        </Stack>
      </PageSection>
    </>
  );
}
