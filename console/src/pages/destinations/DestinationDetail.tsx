// One destination: its location, its four grants (references only, never a
// credential), Test access, what uses it, and Rotate access.
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
  Checkbox,
  Content,
  DescriptionList,
  Flex,
  Grid,
  GridItem,
  PageSection,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import { mayStart, useFollowedPreflight, useGrant, useIntentKey } from "../../api/connections";
import {
  DESTINATION_TEST_SENTENCE,
  GRANT_ROLES,
  NOT_JUDGED_SENTENCE,
  ROLE_WORDS,
  useDestination,
  useDestinationUsage,
  useTestDestination,
  type Destination,
  type GrantRole,
} from "../../api/destinations";
import { PageHeader } from "../../components/PageHeader";
import { ErrorState, LoadingRows } from "../../components/States";
import { StatusLabel } from "../../components/Status";
import { Ident } from "../../components/Ident";
import { CompactList, useNarrow } from "../../components/CompactList";
import { CheckResult, Field, Instant, LastTestLine, NotRecorded, Term, WriteOutcome } from "../connections/parts";
import { DestinationVerdict, TransportStatus } from "./DestinationList";
import { RotateAccess } from "./RotateAccess";
import { ErrorText, Prose } from "../../components/Prose";

const MODE_WORDS: Record<string, string> = {
  secretKeys: "Secret",
  workloadIdentity: "Workload identity",
  controllerIdentity: "Controller identity",
  archiveReadGrant: "The archive-read grant",
  inheritsArchiveWrite: "None: uses the archive-write credential",
  notConfigured: "None: verification is not attempted",
};

function Location({ d }: { d: Destination }) {
  const ca = d.transport.caBundle;
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Location</CardTitle>
      </CardHeader>
      <CardBody>
        <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "10ch", md: "18ch" }}>
          <Term term="Archive root">
            <code className="lw-break">{d.canonicalUrl}</code>
          </Term>
          <Term term="Bucket">{d.storage.bucket}</Term>
          <Term term="Prefix">{d.storage.prefix === "" ? "The bucket root" : d.storage.prefix}</Term>
          <Term term="Region">{d.storage.region ?? <NotRecorded />}</Term>
          <Term term="Endpoint">{d.storage.endpoint ?? "AWS S3"}</Term>
          <Term term="Addressing">{d.storage.addressing === "virtualHosted" ? "Virtual-hosted" : "Path-style"}</Term>
          <Term term="Transport">
            <TransportStatus security={d.transport.security} />
          </Term>
          <Term term="Private CA">
            {ca ? (
              <>
                ConfigMap {ca.configMapName}, key {ca.key}
                {ca.sha256 && (
                  <div>
                    <Ident value={ca.sha256} label="CA digest" />
                  </div>
                )}
              </>
            ) : (
              "None: the runner image's own trust store"
            )}
          </Term>
          <Term term="Write probe">{d.writeProbe === "createOnlyMarker" ? "Create-only marker" : "Disabled"}</Term>
          <Term term="Location digest">{d.locationDigest ? <Ident value={d.locationDigest} label="Location digest" /> : <NotRecorded />}</Term>
          <Term term="Generation">
            {d.generation}
            {typeof d.status.observedGeneration === "number" && d.status.observedGeneration !== d.generation && (
              <span className="lw-subtle"> (the controller last judged generation {d.status.observedGeneration})</span>
            )}
          </Term>
          <Term term="Controller message">{d.status.message ? <Prose text={d.status.message} /> : <NotRecorded />}</Term>
          <Term term="Created">
            <Instant value={d.createdAt} exact />
          </Term>
          <Term term="UID">
            <Ident value={d.uid} label="Destination UID" />
          </Term>
        </DescriptionList>
      </CardBody>
    </Card>
  );
}

function Access({ d }: { d: Destination }) {
  const narrow = useNarrow();
  const reference = (role: GrantRole) => {
    const g = d.access[role];
    return g.secretName ?? g.serviceAccountName ?? "";
  };
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Access</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle lw-small">
          Each grant is a reference: a Secret's name and the key names inside it. No value is ever returned.
        </Content>
        {narrow ? (
          <CompactList
            flush
            label="Grants"
            items={GRANT_ROLES.map((role) => ({
              key: role,
              title: ROLE_WORDS[role],
              meta: [MODE_WORDS[d.access[role].mode] ?? d.access[role].mode, reference(role), d.access[role].keys.join(", ")].filter(Boolean).join(" · "),
              status: null,
            }))}
          />
        ) : (
          <Table aria-label="Grants" variant="compact">
            <Thead>
              <Tr>
                <Th width={25}>Grant</Th>
                <Th width={35}>Source</Th>
                <Th width={40}>Reference</Th>
              </Tr>
            </Thead>
            <Tbody>
              {GRANT_ROLES.map((role) => {
                const g = d.access[role];
                return (
                  <Tr key={role}>
                    <Td dataLabel="Grant">{ROLE_WORDS[role]}</Td>
                    <Td dataLabel="Source">{MODE_WORDS[g.mode] ?? g.mode}</Td>
                    <Td dataLabel="Reference" className="lw-break">
                      {reference(role) || <span className="lw-subtle">None</span>}
                      {g.keys.length > 0 && <div className="lw-subtle lw-small">keys {g.keys.join(", ")}</div>}
                    </Td>
                  </Tr>
                );
              })}
            </Tbody>
          </Table>
        )}
      </CardBody>
    </Card>
  );
}

function TestAccess({ ws, d, mayTest }: { ws: string; d: Destination; mayTest: boolean }) {
  const test = useTestDestination(ws, d.name);
  const intent = useIntentKey("destination-test");
  const [roles, setRoles] = useState<GrantRole[]>([]);
  const [started, setStarted] = useState<string | null>(null);
  const [local, setLocal] = useState<unknown>(null);
  const { query: check, stopped } = useFollowedPreflight(ws, started);
  const run = () => {
    if (test.isPending) return;
    setLocal(null);
    let key: string;
    try {
      key = intent.take();
    } catch (refusal) {
      setLocal(refusal);
      return;
    }
    test.mutate({ key, roles }, {
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
        <CardTitle component="h2">Test access</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle">
          {DESTINATION_TEST_SENTENCE}
        </Content>
        {mayTest ? (
          <>
            <Field id="test-roles" label="Grants to exercise" role="group" help="None chosen: every grant a run uses, including Archive read and Evidence write when they fall back to the archive-write credential. Archive write itself is never written to by a test: it is dialled through Archive read or Evidence write when they fall back to it, else the run verifies it.">
              <Flex gap={{ default: "gapMd" }} flexWrap={{ default: "wrap" }}>
                {GRANT_ROLES.map((role) => (
                  <Checkbox
                    key={role}
                    id={`test-role-${role}`}
                    label={ROLE_WORDS[role]}
                    isChecked={roles.includes(role)}
                    onChange={(_e, on) => setRoles(on ? [...roles, role] : roles.filter((r) => r !== role))}
                  />
                ))}
              </Flex>
            </Field>
            <Button variant="primary" className="lw-gap-top" onClick={run} isLoading={test.isPending} isDisabled={test.isPending}>
              Test access
            </Button>
          </>
        ) : (
          <Content component="p" className="lw-subtle">
            This login may read destinations and not test them: that needs the operator or administrator role.
          </Content>
        )}
        <div aria-live="polite" className="lw-gap-top">
          {local !== null && <WriteOutcome error={local} subject="the test" credentials={false} unmatched={[]} />}
          {test.isError && <WriteOutcome error={test.error} subject="the test" credentials={false} unmatched={[]} />}
          {started === null ? (
            <LastTestLine test={d.lastTest} noun="access test" subject="destination" />
          ) : check.isPending ? (
            <LoadingRows rows={3} label="Loading the test" />
          ) : check.isError ? (
            <ErrorState error={check.error} onRetry={() => void check.refetch()} />
          ) : (
            <CheckResult preflight={check.data} stopped={stopped} onRunAgain={mayTest ? run : undefined} runAgainDisabled={test.isPending} />
          )}
        </div>
      </CardBody>
    </Card>
  );
}

function Usage({ ws, name }: { ws: string; name: string }) {
  const usage = useDestinationUsage(ws, name);
  const rows = usage.data ? [...usage.data.schedules, ...usage.data.backups] : [];
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">What uses this</CardTitle>
      </CardHeader>
      <CardBody>
        {usage.isPending ? (
          <LoadingRows rows={2} label="Loading what uses this destination" />
        ) : usage.isError ? (
          <Content component="p" className="lw-subtle">
            The usage list could not be read: <ErrorText error={usage.error} />
          </Content>
        ) : (
          <>
            {rows.length === 0 ? (
              <Content component="p">No schedule or backup the service read names it.</Content>
            ) : (
              <ul className="lw-plain-list">
                {rows.map((u) => (
                  <li key={`${u.kind}/${u.name}`} className="lw-break">
                    {u.kind === "BackupSchedule" ? (
                      <>
                        Schedule <a href={`#/${ws}/schedules/${u.name}`}>{u.name}</a>
                      </>
                    ) : (
                      <>
                        Backup{" "}
                        <Link to="/$ws/backups/$name" params={{ ws, name: u.name }}>
                          {u.name}
                        </Link>
                      </>
                    )}{" "}
                    {u.createdAt && (
                      <span className="lw-subtle lw-small">
                        created <Instant value={u.createdAt} />
                      </span>
                    )}
                  </li>
                ))}
              </ul>
            )}
            {usage.data.truncated && <Content component="p" className="lw-subtle lw-small">One of the lists was cut at its bound; there may be more.</Content>}
            <Content component="p" className="lw-subtle lw-small lw-gap-top">
              How this was read: {usage.data.basis}
            </Content>
          </>
        )}
      </CardBody>
    </Card>
  );
}

export function DestinationDetail() {
  const { ws, name } = useParams({ from: "/$ws/destinations/$name" });
  // One page per destination: a test started or a rotation shown here never carries over to another.
  return <DestinationPage key={`${ws}/${name}`} ws={ws} name={name} />;
}

function DestinationPage({ ws, name }: { ws: string; name: string }) {
  const grant = useGrant(ws);
  const readable = grant.can?.destinations !== false;
  const destination = useDestination(ws, name, readable);
  const [rotating, setRotating] = useState(false);
  const [rotated, setRotated] = useState<number | null>(null);
  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ws/destinations" params={{ ws }}>Destinations</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );
  if (!readable || destination.isPending || destination.isError) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          {!readable ? (
            <Content component="p">Your roles in {ws} do not include reading destinations.</Content>
          ) : destination.isPending ? (
            <LoadingRows rows={6} label="Loading the destination" />
          ) : (
            <ErrorState error={destination.error} onRetry={() => void destination.refetch()} />
          )}
        </PageSection>
      </>
    );
  }
  const d = destination.data;
  const mayManage = mayStart(grant, "destinations");
  const unjudged = d.status.valid === null || d.status.valid === undefined;
  return (
    <>
      <PageHeader
        title={d.name}
        breadcrumb={breadcrumb}
        status={
          <Flex gap={{ default: "gapXs" }} component="span">
            <DestinationVerdict status={d.status} />
            {d.default && <StatusLabel tone="info" text="Workspace default" />}
          </Flex>
        }
        description={d.description ?? d.canonicalUrl}
        actions={
          mayManage && grant.can?.credentialWrite ? (
            <Button variant="secondary" onClick={() => setRotating(true)}>
              Rotate access
            </Button>
          ) : undefined
        }
      />
      {(unjudged || rotated !== null) && (
        <PageSection>
          {rotated !== null && (
            <Alert isInline variant="success" title={`Access rotated. The destination is now at generation ${rotated}.`} className="lw-gap-top">
              A new credential was written to a new Secret bound to this destination; a superseded Secret is left for you to delete.
            </Alert>
          )}
          {unjudged && <Alert isInline isPlain variant="info" title="Not judged yet">{NOT_JUDGED_SENTENCE}</Alert>}
        </PageSection>
      )}
      <PageSection>
        <Grid hasGutter>
          <GridItem lg={6}>
            <Location d={d} />
          </GridItem>
          <GridItem lg={6}>
            <Access d={d} />
          </GridItem>
          <GridItem>
            <TestAccess ws={ws} d={d} mayTest={mayManage} />
          </GridItem>
          <GridItem>
            <Usage ws={ws} name={d.name} />
          </GridItem>
        </Grid>
      </PageSection>
      {rotating && (
        <RotateAccess
          ws={ws}
          destination={d}
          onClose={() => setRotating(false)}
          onReRead={() => void destination.refetch()}
          onRotated={(item) => {
            setRotating(false);
            setRotated(item.generation);
          }}
        />
      )}
    </>
  );
}
