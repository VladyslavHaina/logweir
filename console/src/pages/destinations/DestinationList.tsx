// Every saved destination in the workspace: where it writes, how securely,
// and whether the controller has judged it.
import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { Button, Card, Content, PageSection } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import CloudUploadAltIcon from "@patternfly/react-icons/dist/esm/icons/cloud-upload-alt-icon";
import { mayStart, useGrant } from "../../api/connections";
import { DEFAULT_ANNOTATION_RESIDUAL, NOT_JUDGED_SENTENCE, useDestinationList, type DestinationSummary } from "../../api/destinations";
import { PageHeader } from "../../components/PageHeader";
import { CompactList, useNarrow } from "../../components/CompactList";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { StatusLabel } from "../../components/Status";
import { AddDestination } from "./AddDestination";

/** The controller's verdict. Absent is "not judged yet", which is not the same as invalid. */
export function DestinationVerdict({ status }: { status: DestinationSummary["status"] }) {
  const reason = status.reason;
  if (status.valid === true) return <StatusLabel tone="success" text={reason && reason !== "Valid" ? `Valid (${reason})` : "Valid"} title="status.valid true" />;
  if (status.valid === false) return <StatusLabel tone="danger" text={reason && reason !== "Invalid" ? `Not valid (${reason})` : "Not valid"} title="status.valid false" />;
  return <StatusLabel tone="neutral" text="Not judged yet" title="status.valid not recorded" />;
}

/** Transport is its own fact: plaintext is a legal, explicit choice that never looks like TLS. */
export function TransportStatus({ security }: { security: string }) {
  if (security === "insecureHttp") return <StatusLabel tone="warning" text="Plaintext HTTP" title="insecureHttp" />;
  if (security === "tls") return <StatusLabel tone="success" text="TLS" title="tls" />;
  return <span className="lw-subtle">Not recorded</span>;
}

export const READ_ONLY_SENTENCE = "This login may read the destinations in this workspace and not add or change one: that needs the operator or administrator role.";

export function DestinationList() {
  const { ws } = useParams({ from: "/$ws/destinations" });
  const navigate = useNavigate();
  const grant = useGrant(ws);
  const readable = grant.can?.destinations !== false;
  const list = useDestinationList(ws);
  const narrow = useNarrow();
  const [adding, setAdding] = useState(false);
  const mayAdd = mayStart(grant, "destinations") && Boolean(grant.can?.credentialWrite);
  const items = [...(list.data ?? [])].sort((a, b) => a.name.localeCompare(b.name));
  const add = (
    <Button variant="primary" onClick={() => setAdding(true)}>
      Add a destination
    </Button>
  );

  return (
    <>
      <PageHeader
        title="Destinations"
        description="Where backups are written: one bucket and prefix, written down once. Schedules, backups and restores refer to it by name."
        actions={mayAdd ? add : undefined}
      />
      <PageSection>
        <Card>
          {!readable ? (
            <Empty title="Not available to you here" icon={CloudUploadAltIcon}>
              Your roles in {ws} do not include reading destinations.
            </Empty>
          ) : list.isPending ? (
            <div className="lw-pad">
              <LoadingRows rows={3} label="Loading destinations" />
            </div>
          ) : list.isError ? (
            <ErrorState error={list.error} onRetry={() => void list.refetch()} />
          ) : items.length === 0 ? (
            <Empty title="No destinations yet" icon={CloudUploadAltIcon} actions={mayAdd ? add : undefined}>
              A destination is the bucket backups are written to. Add one before creating a schedule.
            </Empty>
          ) : (
            <>
              {narrow ? (
                <CompactList
                  label="Destinations"
                  items={items.map((d) => ({
                    key: d.uid,
                    href: `#/${ws}/destinations/${d.name}`,
                    title: d.name,
                    meta: `${d.default ? "default · " : ""}${d.canonicalUrl}`,
                    status: <DestinationVerdict status={d.status} />,
                  }))}
                />
              ) : (
                <Table aria-label="Destinations" variant="compact" gridBreakPoint="grid-lg">
                  <Thead>
                    <Tr>
                      <Th>Name</Th>
                      <Th>Verdict</Th>
                      <Th>Location</Th>
                      <Th>Transport</Th>
                      <Th>Addressing</Th>
                    </Tr>
                  </Thead>
                  <Tbody>
                    {items.map((d) => (
                      <Tr key={d.uid}>
                        <Td dataLabel="Name">
                          <Link to="/$ws/destinations/$name" params={{ ws, name: d.name }} className="lw-break">
                            {d.name}
                          </Link>
                          {d.default && (
                            <>
                              {" "}
                              <StatusLabel tone="info" text="Default" />
                            </>
                          )}
                          {d.description && <div className="lw-subtle lw-small">{d.description}</div>}
                        </Td>
                        <Td dataLabel="Verdict">
                          <DestinationVerdict status={d.status} />
                        </Td>
                        <Td dataLabel="Location" className="lw-break">
                          <code>{d.canonicalUrl}</code>
                          <div className="lw-subtle lw-small">{d.endpoint ?? "AWS S3"}</div>
                        </Td>
                        <Td dataLabel="Transport">
                          <TransportStatus security={d.transport} />
                        </Td>
                        <Td dataLabel="Addressing">{d.addressing === "virtualHosted" ? "Virtual-hosted" : "Path-style"}</Td>
                      </Tr>
                    ))}
                  </Tbody>
                </Table>
              )}
              <div className="lw-pad">
                {!items.some((d) => d.default) && (
                  <Content component="p" className="lw-subtle lw-small">
                    No destination here is marked default, so a new schedule starts with none chosen. {DEFAULT_ANNOTATION_RESIDUAL}
                  </Content>
                )}
                {items.some((d) => d.status.valid === null || d.status.valid === undefined) && (
                  <Content component="p" className="lw-subtle lw-small">
                    {NOT_JUDGED_SENTENCE}
                  </Content>
                )}
              </div>
            </>
          )}
        </Card>
        {grant.known && readable && !mayAdd && (
          <Content component="p" className="lw-subtle lw-gap-top">
            {READ_ONLY_SENTENCE}
          </Content>
        )}
      </PageSection>
      {adding && (
        <AddDestination
          ws={ws}
          onClose={() => setAdding(false)}
          onCreated={(name) => {
            setAdding(false);
            void navigate({ to: "/$ws/destinations/$name", params: { ws, name } });
          }}
        />
      )}
    </>
  );
}
