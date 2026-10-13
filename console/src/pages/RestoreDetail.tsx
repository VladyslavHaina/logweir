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
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import ExchangeAltIcon from "@patternfly/react-icons/dist/esm/icons/exchange-alt-icon";
import DownloadIcon from "@patternfly/react-icons/dist/esm/icons/download-icon";
import { useRestore, useRestoreOperation } from "../api/queries";
import type { OperationStage, OperationView } from "../api/types";
import { PageHeader } from "../components/PageHeader";
import { RunStatus } from "../components/Status";
import { ErrorState, LoadingRows } from "../components/States";
import { When, count, exactUtc, msValue, relative } from "../components/When";
import { Ident } from "../components/Ident";
import { PROTOTYPE } from "../api/client";
import type { Restore } from "../api/types";
import { DURING_RESTORE, TOPIC_CONFIG, mappingFor, themeAOf, type RestoreThemeA } from "../mock/themeA";
import { StatusLabel } from "../components/Status";

const STAGES: { stage: OperationStage; title: string; desc: string }[] = [
  { stage: "admission", title: "Admitted", desc: "Plan checked and confirmed" },
  { stage: "preparing", title: "Preparing", desc: "Topics created on the target" },
  { stage: "running", title: "Restoring", desc: "Records replayed from the archive" },
  { stage: "verifying", title: "Verifying", desc: "Restored records compared with the archive" },
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

/** The positions the complete verification mapped, and whether a switchover is possible (PROD-04.2). */
function PositionsCard({ r, t }: { r: Restore; t: RestoreThemeA }) {
  const entries = Object.values(mappingFor(r)).flatMap((g) => g.entries);
  const n = (c: string) => entries.filter((e) => e.class === c).length;
  const body =
    t.mapping === "written" ? (
      <>
        <Content component="p">
          {Object.keys(mappingFor(r)).length} captured groups, {entries.length} positions: {n("exact")} exact, {n("approximate")} approximate,{" "}
          {n("unavailable")} unavailable. The mapping is bound to the signed scorecard.
        </Content>
        <Ident value={t.mappingSha256} label="positions mapping sha256" />
      </>
    ) : t.mapping === "sampled" ? (
      <Content component="p" className="lw-subtle">
        Not mapped: this restore was verified on a sample. Consumer positions are mapped only by a complete verification, so
        it cannot be switched over.
      </Content>
    ) : t.mapping === "scratch" ? (
      <Content component="p" className="lw-subtle">
        A clone is not switched over: its topics are deleted when it expires.
      </Content>
    ) : (
      <Content component="p" className="lw-subtle">
        {r.coverage?.requested === "complete"
          ? "Mapped when the complete verification passes."
          : "Not mapped: this restore uses a sampled verification."}
      </Content>
    );
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Consumer positions</CardTitle>
      </CardHeader>
      <CardBody>{body}</CardBody>
    </Card>
  );
}

/** `Restore.status.clone`: when the clone's topics go, and what removed them. */
function CloneCard({ t }: { t: RestoreThemeA }) {
  const c = t.clone!;
  return (
    <Card>
      <CardHeader>
        <Flex alignItems={{ default: "alignItemsCenter" }} gap={{ default: "gapSm" }}>
          <CardTitle component="h2">Temporary clone</CardTitle>
          {c.phase === "Removed" ? <StatusLabel tone="success" text="Removed" /> : <StatusLabel tone="info" text={`Deletes ${relative(c.expiresAt)}`} />}
        </Flex>
      </CardHeader>
      <CardBody>
        <DescriptionList isCompact className="lw-dl" orientation={{ sm: "vertical", md: "horizontal" }}>
          <DescriptionListGroup>
            <DescriptionListTerm>Expires</DescriptionListTerm>
            <DescriptionListDescription>
              <When value={c.expiresAt} exact />
            </DescriptionListDescription>
          </DescriptionListGroup>
          <DescriptionListGroup>
            <DescriptionListTerm>What is deleted</DescriptionListTerm>
            <DescriptionListDescription>
              Only the topics this run created, as its signed scorecard records them, and only while each topic's ID is the one
              recorded at creation. A topic recreated since is kept (CloneTargetReplaced).
            </DescriptionListDescription>
          </DescriptionListGroup>
          <DescriptionListGroup>
            <DescriptionListTerm>Clean-up</DescriptionListTerm>
            <DescriptionListDescription>
              {c.phase === "Removed" ? "Done; the clean-up attestation is kept with the evidence." : `Not run yet (${c.attempts} attempts). Deleting this restore first runs it at once.`}
            </DescriptionListDescription>
          </DescriptionListGroup>
        </DescriptionList>
      </CardBody>
    </Card>
  );
}

/** `Restore.status.topicConfiguration`: the settings before and after the transition (PROD-05.2). */
function TopicConfigCard({ r, t }: { r: Restore; t: RestoreThemeA }) {
  const tc = t.topicConfiguration!;
  const sources = ["orders", "orders-dlq", "payments"];
  const rows = r.newTopics.map((name, i) => ({ name, model: TOPIC_CONFIG.find((m) => m.source === sources[i]) }));
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Topic configuration</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle">
          {tc.transition === "afterVerification"
            ? "Created with the source's settings. The settings that could delete, compact or re-stamp restored records were held at Logweir's values until the verdict, then set to the source's."
            : "Created with the source's settings; the restore's own settings were left in place."}
        </Content>
        <Table aria-label="Topic configuration before and after verification" variant="compact" gridBreakPoint="grid-md" className="lw-kv-table">
          <Thead>
            <Tr>
              <Th width={25}>Topic</Th>
              <Th width={15}>Outcome</Th>
              <Th width={30}>While restoring</Th>
              <Th width={30}>Now</Th>
            </Tr>
          </Thead>
          <Tbody>
            {rows.map(({ name, model }) => {
              const keys = Object.keys(model?.deferred ?? {});
              const exported = tc.exported.includes(name);
              const failed = tc.failed.find((f) => f.topic === name);
              const list = (value: (key: string) => string) => (
                <ul className="lw-keys">
                  {keys.map((key) => (
                    <li key={key}>
                      {key}={value(key)}
                    </li>
                  ))}
                </ul>
              );
              return (
                <Tr key={name}>
                  <Td dataLabel="Topic">{name}</Td>
                  <Td dataLabel="Outcome">
                    {failed ? (
                      <StatusLabel tone="danger" text="Not applied" title={failed.error} />
                    ) : exported ? (
                      <StatusLabel tone="info" text="Exported" title={model?.owner} />
                    ) : tc.applied.includes(name) ? (
                      <StatusLabel tone="success" text="Applied" />
                    ) : (
                      <StatusLabel tone="neutral" text="Not run" />
                    )}
                  </Td>
                  <Td dataLabel="While restoring">{list((key) => DURING_RESTORE[key] ?? "not set")}</Td>
                  <Td dataLabel="Now">
                    {exported ? (
                      <>
                        {list((key) => DURING_RESTORE[key] ?? "not set")}
                        <div className="lw-subtle lw-small">The owner applies: {keys.map((key) => `${key}=${msValue(key, model?.deferred[key] ?? "")}`).join(", ")}</div>
                      </>
                    ) : (
                      list((key) => msValue(key, model?.deferred[key] ?? ""))
                    )}
                  </Td>
                </Tr>
              );
            })}
          </Tbody>
        </Table>
        {tc.exported.length > 0 && tc.desiredStateKey && (
          <Flex className="lw-gap-top" gap={{ default: "gapMd" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "wrap" }}>
            <FlexItem className="lw-min0">
              <Content component="p">
                {tc.exported.join(", ")} {tc.exported.length === 1 ? "is" : "are"} owned by Strimzi, so Logweir did not change{" "}
                {tc.exported.length === 1 ? "its" : "their"} settings: apply the KafkaTopic it wrote.
              </Content>
              <Ident value={tc.desiredStateKey} label="desired-state document key" />
            </FlexItem>
            <FlexItem>
              <Button variant="secondary" icon={<DownloadIcon />} isDisabled={PROTOTYPE}>
                KafkaTopic YAML
              </Button>
            </FlexItem>
          </Flex>
        )}
      </CardBody>
    </Card>
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
  const t = themeAOf(r);
  const done = op.terminal && op.verifiedSuccess;
  const complete = r.coverage?.requested === "complete";
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
          done && t.mapping === "written" ? (
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
              {complete
                ? `All ${count(completion?.recordsSampledMatching)} match the archive byte for byte (complete verification).`
                : `${count(completion?.recordsSampledMatching)} of ${count(completion?.recordsSampled)} sampled records match the archive byte for byte.`}{" "}
              The controller verified the signed scorecard.
            </p>
            {t.mapping === "written" && (
              <p>
                <strong>Next:</strong> your consumer groups still point at their old positions. Review how each position
                translates to the restored topics, then switch them over.
              </p>
            )}
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
                <DescriptionList isCompact className="lw-dl lw-dl--narrow" orientation={{ sm: "horizontal" }}>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Topics</DescriptionListTerm>
                    <DescriptionListDescription>
                      <ul className="lw-plain-list">
                        {r.newTopics.map((topic) => (
                          <li key={topic}>{topic}</li>
                        ))}
                      </ul>
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Target</DescriptionListTerm>
                    <DescriptionListDescription>
                      {r.target.clusterRef.name}, {r.target.originalName ? "original names" : `prefix ${r.target.topicPrefix}`}
                      {r.target.mode === "scratch" ? " (a temporary clone)" : ""}
                    </DescriptionListDescription>
                  </DescriptionListGroup>
                  <DescriptionListGroup>
                    <DescriptionListTerm>Verification</DescriptionListTerm>
                    <DescriptionListDescription>
                      {complete ? "Complete: every restored record is compared" : "Sampled: 10,000 records are compared"}
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
          <GridItem>{t.clone ? <CloneCard t={t} /> : <PositionsCard r={r} t={t} />}</GridItem>
          {t.topicConfiguration && (
            <GridItem>
              <TopicConfigCard r={r} t={t} />
            </GridItem>
          )}
        </Grid>
      </PageSection>
    </>
  );
}
