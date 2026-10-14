// One switchover's approval (subject kind Switchover, theme A): the request a second person
// is shown (every group, topic and position, from R7's SwitchoverApprovalSummary) and the
// one Approve click, which sends only the hash of what was shown. Under strict or with
// files, the Switchover's own page gives the CLI commands.
import { Link, useParams } from "@tanstack/react-router";
import { Alert, Breadcrumb, BreadcrumbItem, Button, Card, CardBody, CardHeader, CardTitle, Content, PageSection, Stack, StackItem } from "@patternfly/react-core";
import { outcomeOf, UNKNOWN_OUTCOME_SENTENCE, useApprovalGrant } from "../../api/approvals";
import { useApproveSwitchover, useSwitchoverApprovalRequest } from "../../api/switchovers";
import { PageHeader } from "../../components/PageHeader";
import { ErrorState, LoadingRows } from "../../components/States";
import { SwitchoverApprovalSummary } from "../cutover/SwitchoverApprovalSummary";
import { REFUSAL_TITLE } from "./ApprovalPage";
import { Refusal, RequestStatus, Requester } from "./parts";
import "./approvals.css";
import { ErrorText, Prose } from "../../components/Prose";

export function SwitchoverApprovalPage() {
  const { ns, name } = useParams({ from: "/$ns/approvals/switchover/$name" });
  const requestQ = useSwitchoverApprovalRequest(ns, name);
  const grant = useApprovalGrant(ns);
  const approve = useApproveSwitchover(ns, name);
  const request = requestQ.data;
  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/approvals" params={{ ns }}>Approvals</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );
  return (
    <>
      <PageHeader title={`Switchover ${name}`} breadcrumb={breadcrumb} status={request ? <RequestStatus state={request.state} /> : undefined} description="A two-person switchover, as the second person is shown it." />
      <PageSection>
        {requestQ.isPending ? (
          <LoadingRows rows={4} label="Reading the request" />
        ) : requestQ.isError || !request ? (
          <ErrorState error={requestQ.error} onRetry={() => void requestQ.refetch()} />
        ) : (
          <Stack hasGutter>
            <StackItem>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">What approving commits</CardTitle>
                </CardHeader>
                <CardBody>
                  <Content component="p" className="lw-subtle"><Prose text={request.stateSentence} /></Content>
                  {/* DUI-21: the second person sees who asked, as on a restore's request. */}
                  {request.state !== "notConfirmed" && request.requester && (
                    <Content component="p" id="switchover-requester">
                      Requested by <Requester
                        id={request.requester}
                        email={request.requesterEmail}
                        name={request.requesterName}
                        detailsNotVerified={request.requesterDetailsNotVerified === true}
                        grant={grant}
                      />
                    </Content>
                  )}
                  <SwitchoverApprovalSummary request={request} />
                </CardBody>
              </Card>
            </StackItem>
            <StackItem>
              <Card>
                <CardHeader>
                  <CardTitle component="h2">Decision</CardTitle>
                </CardHeader>
                <CardBody>
                  {approve.isSuccess ? (
                    <Alert variant="success" isInline title="Approved">
                      The Switchover runs once the controller verifies the approval.{" "}
                      <Link to="/$ns/switchovers/$name" params={{ ns, name }}>Open the Switchover</Link>
                    </Alert>
                  ) : request.approve.offered && request.confirmationSha256 ? (
                    <>
                      <Content component="p">{request.approve.sentence}</Content>
                      <div className="lw-decision">
                        <Button variant="primary" id="approve-switchover" isLoading={approve.isPending} isDisabled={approve.isPending} onClick={() => approve.mutate(request.confirmationSha256!)}>
                          Approve
                        </Button>
                        <span className="lw-subtle lw-small">Approving sends the hash of the request shown above and nothing else.</span>
                      </div>
                    </>
                  ) : (
                    <Refusal title={REFUSAL_TITLE[request.approve.refusal ?? ""] ?? "Not offered to you"} id="approve-not-offered">
                      {request.approve.sentence || "This login is not offered an approval for this request."}
                    </Refusal>
                  )}
                  {approve.error &&
                    (outcomeOf(approve.error) === "unknown" ? (
                      <Alert variant="warning" isInline title="The answer did not arrive" className="lw-gap-top" component="h3">
                        {UNKNOWN_OUTCOME_SENTENCE}
                      </Alert>
                    ) : (
                      <Refusal title="Not approved" id="approve-refused">
                        <ErrorText error={approve.error} />
                      </Refusal>
                    ))}
                </CardBody>
              </Card>
            </StackItem>
          </Stack>
        )}
      </PageSection>
    </>
  );
}
