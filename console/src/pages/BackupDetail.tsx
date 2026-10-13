// One backup run: what it covered, whether its evidence verified, and what you can do with it.
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
  Grid,
  GridItem,
  Label,
  LabelGroup,
  PageSection,
  ProgressStep,
  ProgressStepper,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import { useBackup, usePoints } from "../api/queries";
import type { Backup, PointView } from "../api/types";
import { PageHeader } from "../components/PageHeader";
import { RunStatus } from "../components/Status";
import { ErrorState, LoadingRows } from "../components/States";
import { When, count } from "../components/When";
import { Ident } from "../components/Ident";
import { TriggerLabel, windowText } from "./Backups";

function Term({ term, children }: { term: string; children: React.ReactNode }) {
  return (
    <DescriptionListGroup>
      <DescriptionListTerm>{term}</DescriptionListTerm>
      <DescriptionListDescription>{children}</DescriptionListDescription>
    </DescriptionListGroup>
  );
}

function LiveProgress({ b }: { b: Backup }) {
  const order = ["pending", "running", "verifying"];
  const at = order.indexOf(b.operation.state);
  const steps = [
    { id: "start", title: "Started", desc: "Admitted and scheduled" },
    { id: "copy", title: "Copying", desc: "Reading the topics into the archive" },
    { id: "verify", title: "Verifying", desc: "The controller checks the signed receipt" },
    { id: "done", title: "Verified", desc: "A recovery point you can restore" },
  ];
  return (
    <Card>
      <CardBody>
        <ProgressStepper isCenterAligned aria-label="Backup progress" className="lw-stepper">
          {steps.map((s, i) => (
            <ProgressStep
              key={s.id}
              id={`bp-${s.id}`}
              titleId={`bp-${s.id}-t`}
              variant={i <= at ? "success" : i === at + 1 ? "info" : "pending"}
              isCurrent={i === at + 1}
              description={s.desc}
            >
              {s.title}
            </ProgressStep>
          ))}
        </ProgressStepper>
      </CardBody>
    </Card>
  );
}

function Groups({ point }: { point: PointView | undefined }) {
  const groups = point?.consumerPositions?.groups ?? [];
  if (!point) return <Content component="p" className="lw-subtle">This run has no recovery point.</Content>;
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
            <Td dataLabel="Type">{g.groupType === "other" ? "streams / share" : g.groupType}</Td>
            <Td dataLabel="Captured">
              {g.outcome === "captured" ? (
                <Label isCompact status="success">Captured</Label>
              ) : (
                <Label isCompact color="grey" title={g.reason ?? undefined}>
                  Not captured
                </Label>
              )}
            </Td>
            <Td dataLabel="Positions">
              {g.positions
                ? `${g.positions.related} partitions within the archived data${g.positions.notRelated ? `, ${g.positions.notRelated} outside it` : ""}`
                : g.reason === "GroupTypeNotCaptured"
                  ? "Streams groups are not captured"
                  : (g.reason ?? "None")}
            </Td>
          </Tr>
        ))}
      </Tbody>
    </Table>
  );
}

export function BackupDetail() {
  const { ns, name } = useParams({ from: "/$ns/backups/$name" });
  const navigate = useNavigate();
  const backup = useBackup(ns, name);
  const points = usePoints(ns);

  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/backups" params={{ ns }}>Backups</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
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
  if (backup.isError) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          <ErrorState error={backup.error} onRetry={() => void backup.refetch()} />
        </PageSection>
      </>
    );
  }

  const b = backup.data;
  const point = (points.data ?? []).find((p) => p.backupId === b.backupId);
  const verified = b.operation.verifiedSuccess;

  return (
    <>
      <PageHeader
        title={b.name}
        breadcrumb={breadcrumb}
        status={<RunStatus op={b.operation} />}
        description={`${b.topics.length} topics from ${b.sourceRef.name}, ${windowText(b)}.`}
        actions={
          <Button
            variant="primary"
            isDisabled={!verified}
            onClick={() => void navigate({ to: "/$ns/restores/new", params: { ns }, search: { backup: b.name } })}
          >
            Restore this point
          </Button>
        }
      />
      {!b.operation.terminal && (
        <PageSection>
          <LiveProgress b={b} />
        </PageSection>
      )}
      {b.operation.state === "failed" && (
        <PageSection>
          <Alert variant="danger" isInline title={`This run failed: ${b.operation.stateReason ?? "no reason recorded"}`}>
            <p>{b.operation.outcome ?? "The controller recorded no further detail."}</p>
            <p>
              No recovery point was made by this run.{" "}
              {b.operation.stateReason === "SourceUnreachable"
                ? "Check that the source connection is reachable, then the next slot runs normally."
                : "A retry ran after it; open the retry for its point."}
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
                <DescriptionList isHorizontal isCompact horizontalTermWidthModifier={{ default: "12ch" }}>
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
                  <Term term="Covers">{windowText(b)}</Term>
                  <Term term="Destination">{b.destinationRef?.name ?? "Inline archive"}</Term>
                  <Term term="Trigger">
                    <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }}>
                      <TriggerLabel b={b} />
                      {b.schedule && <span>from schedule {b.schedule}</span>}
                      {b.trigger?.retryOf && (
                        <span>
                          retry of{" "}
                          <Link to="/$ns/backups/$name" params={{ ns, name: b.trigger.retryOf.name }}>
                            {b.trigger.retryOf.name}
                          </Link>
                        </span>
                      )}
                    </Flex>
                  </Term>
                  <Term term="Started">
                    <When value={b.createdAt} exact />
                  </Term>
                  <Term term="Read as">{b.observedAuth?.username ?? b.observedAuth?.mode ?? "Not recorded"}</Term>
                </DescriptionList>
              </CardBody>
            </Card>
          </GridItem>
          <GridItem lg={5}>
            <Card isFullHeight>
              <CardHeader>
                <CardTitle component="h2">Evidence</CardTitle>
              </CardHeader>
              <CardBody>
                {verified && point ? (
                  <>
                    <Content component="p">
                      The controller fetched this run's signed receipt and verified it against the trusted key. This page
                      holds no key and checks nothing itself.
                    </Content>
                    <DescriptionList isCompact>
                      <Term term="Recovery point">
                        <Ident value={point.pointId} label="recovery point id" />
                      </Term>
                      <Term term="Receipt">
                        <Ident value={point.receiptSha256} label="receipt sha256" />
                      </Term>
                      <Term term="Signed by key">
                        <Ident value={String(point.signerKeyId)} label="signer key id" />
                      </Term>
                      <Term term="Format">{point.formatVersion}</Term>
                    </DescriptionList>
                  </>
                ) : (
                  <Content component="p" className="lw-subtle">
                    {b.operation.terminal
                      ? "No verified evidence: this run made no recovery point."
                      : "The receipt is signed and verified when the run finishes."}
                  </Content>
                )}
              </CardBody>
            </Card>
          </GridItem>
          <GridItem>
            <Card>
              <CardHeader>
                <CardTitle component="h2">Consumer groups</CardTitle>
              </CardHeader>
              <CardBody>
                <Content component="p" className="lw-subtle">
                  The committed positions captured with this point. A restore can translate them so consumers resume on
                  the restored topics (cutover). A group that is not captured has no position: it is never read as offset
                  0.
                </Content>
                {points.isPending ? <LoadingRows rows={3} /> : <Groups point={point} />}
              </CardBody>
            </Card>
          </GridItem>
        </Grid>
      </PageSection>
    </>
  );
}
