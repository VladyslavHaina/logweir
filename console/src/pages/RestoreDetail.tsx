// One restore, live: where it is, what it wrote, whether it verified, and what comes next.
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
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Flex,
  FlexItem,
  Grid,
  GridItem,
  PageSection,
  Progress,
  ProgressMeasureLocation,
  ProgressStep,
  ProgressStepper,
} from "@patternfly/react-core";
import ExchangeAltIcon from "@patternfly/react-icons/dist/esm/icons/exchange-alt-icon";
import { useRestore, useRestoreOperation } from "../api/queries";
import type { OperationStage, OperationView } from "../api/types";
import { PageHeader } from "../components/PageHeader";
import { RunStatus } from "../components/Status";
import { ErrorState, LoadingRows } from "../components/States";
import { When, count, exactUtc } from "../components/When";
import { Ident } from "../components/Ident";

const STAGES: { stage: OperationStage; title: string; desc: string }[] = [
  { stage: "admission", title: "Admitted", desc: "Plan checked and confirmed" },
  { stage: "preparing", title: "Preparing", desc: "Topics created on the target" },
  { stage: "running", title: "Restoring", desc: "Records replayed from the archive" },
  { stage: "verifying", title: "Verifying", desc: "A sample compared with the archive" },
  { stage: "finished", title: "Done", desc: "Scorecard signed and verified" },
];

function Stages({ op }: { op: OperationView }) {
  const at = STAGES.findIndex((s) => s.stage === (op.stage ?? "admission"));
  const finished = op.terminal;
  return (
    <ProgressStepper isCenterAligned aria-label="Restore progress" className="lw-stepper">
      {STAGES.map((s, i) => {
        const done = finished ? op.verifiedSuccess || i < at : i < at;
        const current = !finished && i === at;
        const failedHere = finished && !op.verifiedSuccess && i === at;
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

export function RestoreDetail() {
  const { ns, name } = useParams({ from: "/$ns/restores/$name" });
  const navigate = useNavigate();
  const restore = useRestore(ns, name);
  const operation = useRestoreOperation(ns, name);

  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/restores" params={{ ns }}>Restores</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );

  if (restore.isPending || operation.isPending) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <LoadingRows rows={6} label="Loading the restore" />
        </PageSection>
      </>
    );
  }
  if (restore.isError || operation.isError) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <ErrorState error={restore.error ?? operation.error} onRetry={() => void restore.refetch()} />
        </PageSection>
      </>
    );
  }

  const r = restore.data;
  const op = operation.data;
  const done = op.terminal && op.verifiedSuccess;
  const completion = op.completion;
  const expected = op.verificationScope.recordsExpected ?? 0;
  const replayedMatch = op.progress?.message?.match(/Replayed ([\d,]+)/);
  const replayed = replayedMatch ? Number(replayedMatch[1]!.replace(/,/g, "")) : done ? expected : 0;

  return (
    <>
      <PageHeader
        title={r.name}
        breadcrumb={breadcrumb}
        status={<RunStatus op={r.operation} />}
        description={`From the point at ${exactUtc(r.pointInTime)} into ${r.target.clusterRef.name}.`}
        actions={
          done ? (
            <Button
              variant="primary"
              icon={<ExchangeAltIcon />}
              onClick={() => void navigate({ to: "/$ns/restores/$name/cutover", params: { ns, name } })}
            >
              Switch consumers over
            </Button>
          ) : undefined
        }
      />
      <PageSection>
        <Card>
          <CardBody>
            <Stages op={op} />
            {!op.terminal && op.stage === "running" && (
              <Progress
                className="lw-records-progress"
                value={expected ? (replayed / expected) * 100 : 0}
                title="Records restored"
                label={`${count(replayed)} of ${count(expected)}`}
                measureLocation={ProgressMeasureLocation.outside}
                aria-label="Records restored"
              />
            )}
          </CardBody>
        </Card>
      </PageSection>
      {done && (
        <PageSection>
          <Alert variant="success" isInline title="Restored and verified">
            <p>
              {count(completion?.recordsRestored)} records written to {r.target.clusterRef.name}.{" "}
              {count(completion?.recordsSampledMatching)} of {count(completion?.recordsSampled)} sampled records match the
              archive byte for byte. The controller verified the signed scorecard.
            </p>
            <p>
              <strong>Next:</strong> your consumer groups still point at their old positions. Review how each position
              translates to the restored topics, then switch them over.
            </p>
          </Alert>
        </PageSection>
      )}
      <PageSection>
        <Grid hasGutter>
          <GridItem lg={7}>
            <Card isFullHeight>
              <CardHeader>
                <CardTitle component="h2">Activity</CardTitle>
              </CardHeader>
              <CardBody>
                <ol className={op.terminal ? "lw-timeline" : "lw-timeline lw-timeline--live"} aria-live="polite">
                  {[...op.conditions].reverse().map((c) => (
                    <li key={c.type}>
                      <Flex justifyContent={{ default: "justifyContentSpaceBetween" }} gap={{ default: "gapSm" }}>
                        <FlexItem>
                          <strong>{c.type}</strong>
                        </FlexItem>
                        <FlexItem className="lw-subtle lw-small">
                          <When value={c.lastTransitionTime} />
                        </FlexItem>
                      </Flex>
                      <div>{c.message}</div>
                    </li>
                  ))}
                </ol>
                {!op.terminal && (
                  <Content component="p" className="lw-subtle lw-small lw-gap-top">
                    Updates arrive as they happen; you can leave this page and come back.
                  </Content>
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
                <DescriptionList isCompact>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Topics</DescriptionListTerm>
                    <DescriptionListDescription>
                      <ul className="lw-plain-list">
                        {r.newTopics.map((t) => (
                          <li key={t}>{t}</li>
                        ))}
                      </ul>
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Target</DescriptionListTerm>
                    <DescriptionListDescription>
                      {r.target.clusterRef.name}, {r.target.originalName ? "original names" : `prefix ${r.target.topicPrefix}`}
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Confirmed by</DescriptionListTerm>
                    <DescriptionListDescription>Alice Martin (one-person confirmation)</DescriptionListDescription>
                  </DescriptionListGroup>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Plan hash</DescriptionListTerm>
                    <DescriptionListDescription>
                      <Ident value={r.planHash} label="plan sha256" />
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                  {op.verification.matchedKeyId && (
                    <DescriptionListGroup>
                      <DescriptionListTerm>Scorecard signed by</DescriptionListTerm>
                      <DescriptionListDescription>
                        <Ident value={op.verification.matchedKeyId} label="signer key id" />
                      </DescriptionListDescription>
                    </DescriptionListGroup>
                  )}
                </DescriptionList>
              </CardBody>
            </Card>
          </GridItem>
        </Grid>
      </PageSection>
    </>
  );
}
