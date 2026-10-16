// One connection: what the controller's probe last saw, its saved settings
// (references only, never a credential), Test connection, and its topics.
import { useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
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
  Grid,
  GridItem,
  PageSection,
} from "@patternfly/react-core";
import {
  AUTH_PHRASE,
  AUTH_WORDS,
  CONNECTION_REFUSAL_REASONS,
  PROBE_FRESH_SECONDS,
  PROBE_SENTENCE,
  REFUSAL_GLOSS,
  mayStart,
  probeState,
  useConnection,
  useFollowedPreflight,
  useGrant,
  useIntentKey,
  useStartConnectionCheck,
  type Connection,
} from "../../api/connections";
import { PageHeader } from "../../components/PageHeader";
import { ErrorState, LoadingRows } from "../../components/States";
import { Ident } from "../../components/Ident";
import { Discovery } from "./Discovery";
import { CheckResult, ConnectionUsability, Instant, LastTestLine, NotRecorded, ProbeLabels, ProbeReason, Term, WriteOutcome } from "./parts";

export const CONNECTION_CHECK_SENTENCE =
  "Test connection starts a readiness check: an isolated Job that projects this connection's own credential and dials these brokers now. What it records is that Job's own result at the moment it ran. It is not a promise about the next run, and Ready authorises nothing: every guard still runs when a backup or restore executes.";
export const CONNECTION_CHECK_NO_TOPICS_SENTENCE =
  "This check names no topic, so it says nothing about which topics this principal can describe. Discover topics below answers that.";
export const CONNECTION_CHECK_REFUSED_SENTENCE =
  "The controller has refused this connection, so there is nothing to dial with: a check would be created, fail to render a plan and record nothing. Fix the object first.";

function Probe({ c, onReRead, reading }: { c: Connection; onReRead: () => void; reading: boolean }) {
  const p = probeState(c.reachability);
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Connection probe</CardTitle>
      </CardHeader>
      <CardBody>
        <ProbeLabels state={p} />
        <DescriptionList isCompact isHorizontal className="lw-gap-top" horizontalTermWidthModifier={{ default: "10ch", md: "14ch" }}>
          <Term term={p.verdict === "probing" ? "Previously observed" : "Observed"}>
            {p.observedAt ? <Instant value={p.observedAt} exact /> : "No observation recorded"}
          </Term>
          <Term term="Cluster id">{p.clusterId ? <Ident value={p.clusterId} label="Cluster id" /> : <NotRecorded />}</Term>
          <Term term="Reason">
            <ProbeReason state={p} />
          </Term>
        </DescriptionList>
        {p.stale && (
          <Content component="p" className="lw-gap-top">
            This observation is older than the {PROBE_FRESH_SECONDS} s freshness budget, so it describes the past and not the present.
          </Content>
        )}
        <Content component="p" className="lw-subtle lw-small lw-gap-top">
          {PROBE_SENTENCE} Re-read shows the newest probe the controller has recorded; it dials nothing.
        </Content>
        <Button variant="secondary" onClick={onReRead} isLoading={reading} isDisabled={reading}>
          Re-read the probe
        </Button>
      </CardBody>
    </Card>
  );
}

function Settings({ c }: { c: Connection }) {
  const a = c.auth;
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Settings</CardTitle>
      </CardHeader>
      <CardBody>
        <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "10ch", md: "16ch" }}>
          <Term term="Role">{c.role}</Term>
          <Term term="Bootstrap servers">
            <span className="lw-break">{c.bootstrapServers.join(", ")}</span>
          </Term>
          <Term term="Authentication">{AUTH_WORDS[a.mode]}</Term>
          {a.username && <Term term="SASL username">{a.username}</Term>}
          {a.credentialRef?.name && <Term term="Credential Secret">{a.credentialRef.name}</Term>}
          {a.clientCertificateRef?.name && <Term term="Certificate Secret">{a.clientCertificateRef.name}</Term>}
          <Term term="TLS">{a.tls ? "On" : "Off"}</Term>
          <Term term="Private CA">
            {a.tlsCa ? `${a.tlsCa.kind === "secret" ? "Secret" : "ConfigMap"} ${a.tlsCa.name}, key ${a.tlsCa.key}` : "None: the runner image's own trust store"}
          </Term>
          <Term term="Marker topic">{c.markerTopic ?? <NotRecorded />}</Term>
          <Term term="Created">
            <Instant value={c.createdAt} exact />
          </Term>
          <Term term="UID">
            <Ident value={c.uid} label="Connection UID" />
          </Term>
        </DescriptionList>
        <Content component="p" className="lw-subtle lw-small lw-gap-top">
          A credential is never shown: the connection carries only the name of the Secret it lives in.
        </Content>
      </CardBody>
    </Card>
  );
}

/** Test connection: the started check's own rows, or the newest recorded one as a summary. */
export function ConnectionCheck({ ws, c, mayStart, readable }: { ws: string; c: Connection; mayStart: boolean; readable: boolean }) {
  const start = useStartConnectionCheck(ws, c.name);
  const intent = useIntentKey("connection-check");
  const [started, setStarted] = useState<string | null>(null);
  const [local, setLocal] = useState<unknown>(null);
  const { query: check, stopped } = useFollowedPreflight(ws, started);
  // FX-46: the recorded test's own rows say what the endpoint is usable for; `lastTest` carries only its id.
  const { query: recorded } = useFollowedPreflight(ws, started === null && readable ? (c.lastTest?.preflightId ?? null) : null);
  const refused = CONNECTION_REFUSAL_REASONS.includes(c.reachability.reason ?? "") ? (c.reachability.reason as string) : "";

  const run = () => {
    if (start.isPending || refused) return;
    setLocal(null);
    let key: string;
    try {
      key = intent.take();
    } catch (refusal) {
      setLocal(refusal);
      return;
    }
    start.mutate(key, {
        // The key is held until the check exists: a resend after a lost answer replays it rather than starting a second.
        onSuccess: (item) => {
          intent.settle();
          setStarted(item.id);
        },
      });
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Test connection</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle">
          {CONNECTION_CHECK_SENTENCE}
        </Content>
        <Content component="p" className="lw-subtle lw-small">
          {CONNECTION_CHECK_NO_TOPICS_SENTENCE}
        </Content>
        {!readable ? (
          <Content component="p" className="lw-subtle">
            Readiness checks are not available to you in {ws}.
          </Content>
        ) : (
          <>
            {refused && (
              <Alert isInline variant="danger" title="The controller refused this connection" className="lw-gap-top">
                <p>{CONNECTION_CHECK_REFUSED_SENTENCE}</p>
                <p>
                  Controller reason: <code>{refused}</code>
                  {REFUSAL_GLOSS[refused] ? `: ${REFUSAL_GLOSS[refused]}` : ""}.
                </p>
              </Alert>
            )}
            {mayStart ? (
              <Flex gap={{ default: "gapSm" }} className="lw-gap-top">
                <Button variant="primary" onClick={run} isLoading={start.isPending} isDisabled={start.isPending || Boolean(refused)}>
                  Test connection
                </Button>
              </Flex>
            ) : (
              <Content component="p" className="lw-subtle">
                This login may read connection checks in this workspace and not start one.
              </Content>
            )}
            <div aria-live="polite" className="lw-gap-top">
              {local !== null && <WriteOutcome error={local} subject="the check" credentials={false} unmatched={[]} />}
              {start.isError && <WriteOutcome error={start.error} subject="the check" credentials={false} unmatched={[]} />}
              {started === null ? (
                <>
                  <LastTestLine test={c.lastTest} noun="connectivity check" subject="connection" />
                  {recorded.data && <ConnectionUsability preflight={recorded.data} />}
                  {recorded.isError && (
                    <Content component="p" className="lw-subtle lw-small">
                      The recorded check could not be read, so what this endpoint is usable for is not shown.
                    </Content>
                  )}
                </>
              ) : check.isPending ? (
                <LoadingRows rows={3} label="Loading the check" />
              ) : check.isError ? (
                <ErrorState error={check.error} onRetry={() => void check.refetch()} />
              ) : (
                <>
                  <CheckResult preflight={check.data} stopped={stopped} onRunAgain={mayStart ? run : undefined} runAgainDisabled={start.isPending} />
                  <ConnectionUsability preflight={check.data} />
                </>
              )}
            </div>
          </>
        )}
      </CardBody>
    </Card>
  );
}

export function ConnectionDetail() {
  const { ws, name } = useParams({ from: "/$ws/connections/$name" });
  // One page per connection: a check started here never carries over to another.
  return <ConnectionPage key={`${ws}/${name}`} ws={ws} name={name} />;
}

function ConnectionPage({ ws, name }: { ws: string; name: string }) {
  const grant = useGrant(ws);
  const readable = grant.can?.connectionsRead !== false;
  const connection = useConnection(ws, name, readable);
  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ws/connections" params={{ ws }}>Connections</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );
  if (!readable || connection.isPending || connection.isError) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          {!readable ? (
            <Content component="p">Your roles in {ws} do not include reading connections.</Content>
          ) : connection.isPending ? (
            <LoadingRows rows={6} label="Loading the connection" />
          ) : (
            <ErrorState error={connection.error} onRetry={() => void connection.refetch()} />
          )}
        </PageSection>
      </>
    );
  }
  const c = connection.data;
  const can = grant.can;
  return (
    <>
      <PageHeader
        title={c.name}
        breadcrumb={breadcrumb}
        status={<ProbeLabels state={probeState(c.reachability)} />}
        description={`${c.role === "target" ? "Target" : c.role === "source" ? "Source" : c.role} connection to ${c.bootstrapServers.length} bootstrap ${
          c.bootstrapServers.length === 1 ? "server" : "servers"
        }, ${AUTH_PHRASE[c.auth.mode]}${c.auth.tls ? " over TLS" : ""}.`}
      />
      <PageSection>
        <Grid hasGutter>
          <GridItem lg={6}>
            <Probe c={c} onReRead={() => void connection.refetch()} reading={connection.isRefetching} />
          </GridItem>
          <GridItem lg={6}>
            <Settings c={c} />
          </GridItem>
          <GridItem>
            <ConnectionCheck ws={ws} c={c} readable={can?.preflight !== false} mayStart={mayStart(grant, "preflight")} />
          </GridItem>
          <GridItem>
            <Discovery ws={ws} name={c.name} readable={can?.topicDiscovery !== false} mayStart={mayStart(grant, "topicDiscovery")} />
          </GridItem>
        </Grid>
      </PageSection>
    </>
  );
}
