// Every saved connection in the namespace: where it points, how it
// authenticates, and what the controller's probe last saw.
import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { Button, Card, Content, PageSection, SearchInput, Toolbar, ToolbarContent, ToolbarItem } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import PlugIcon from "@patternfly/react-icons/dist/esm/icons/plug-icon";
import SearchIcon from "@patternfly/react-icons/dist/esm/icons/search-icon";
import { AUTH_WORDS, PROBE_SENTENCE, mayAddConnection, probeState, useConnectionList, useGrant, type Connection } from "../../api/connections";
import { PageHeader } from "../../components/PageHeader";
import { CompactList, useNarrow } from "../../components/CompactList";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { AddConnection } from "./AddConnection";
import { Instant, ProbeLabels } from "./parts";

export const READ_ONLY_SENTENCE =
  "This login may read the connections in this namespace and not add one: that needs the operator or administrator role.";

/** How a connection authenticates, in one line: the mode, the principal, TLS, and the credential Secret's name. */
export function authText(c: Connection): string {
  const parts = [AUTH_WORDS[c.auth.mode]];
  if (c.auth.username) parts.push(`as ${c.auth.username}`);
  parts.push(c.auth.tls ? "TLS" : "no TLS");
  if (c.auth.credentialRef?.name) parts.push(`credential in Secret ${c.auth.credentialRef.name}`);
  if (c.auth.clientCertificateRef?.name) parts.push(`certificate in Secret ${c.auth.clientCertificateRef.name}`);
  if (c.auth.tlsCa) parts.push(`CA from ${c.auth.tlsCa.kind === "secret" ? "Secret" : "ConfigMap"} ${c.auth.tlsCa.name}`);
  return parts.join(" · ");
}

/** The first bootstrap server, and how many more: the detail page lists them all. */
export function serversText(servers: string[]): string {
  if (servers.length <= 1) return servers[0] ?? "None";
  return `${servers[0]} and ${servers.length - 1} more`;
}

function matches(c: Connection, needle: string): boolean {
  if (!needle) return true;
  const hay = [c.name, c.role, ...c.bootstrapServers, c.auth.mode, c.auth.username ?? "", c.reachability.clusterId ?? "", c.reachability.reason ?? ""]
    .join(" ")
    .toLowerCase();
  return needle
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .every((w) => hay.includes(w));
}

export function ConnectionList() {
  const { ns } = useParams({ from: "/$ns/connections" });
  const navigate = useNavigate();
  const grant = useGrant(ns);
  const readable = grant.can?.connectionsRead !== false;
  const list = useConnectionList(ns);
  const narrow = useNarrow();
  const [search, setSearch] = useState("");
  const [adding, setAdding] = useState(false);
  const mayAdd = mayAddConnection(grant);

  const rows = useMemo(() => (list.data ?? []).filter((c) => matches(c, search.trim())).sort((a, b) => a.name.localeCompare(b.name)), [list.data, search]);
  const add = (
    <Button variant="primary" onClick={() => setAdding(true)}>
      Add a connection
    </Button>
  );

  return (
    <>
      <PageHeader
        title="Connections"
        description="The Kafka clusters Logweir reads from and restores into. The credential is entered once and never shown again."
        actions={mayAdd ? add : undefined}
      />
      <PageSection>
        <Card>
          {!readable ? (
            <Empty title="Not available to you here" icon={PlugIcon}>
              Your roles in {ns} do not include reading connections.
            </Empty>
          ) : list.isPending ? (
            <div className="lw-pad">
              <LoadingRows rows={4} label="Loading connections" />
            </div>
          ) : list.isError ? (
            <ErrorState error={list.error} onRetry={() => void list.refetch()} />
          ) : list.data.length === 0 ? (
            <Empty title="No connections yet" icon={PlugIcon} actions={mayAdd ? add : undefined}>
              A connection names a Kafka cluster and how to reach it. Add the cluster you want to back up first; restores need a target
              connection too.
            </Empty>
          ) : (
            <>
              <Toolbar>
                <ToolbarContent>
                  <ToolbarItem className={narrow ? "lw-grow" : undefined}>
                    <SearchInput
                      aria-label="Find a connection"
                      placeholder="Find a connection"
                      value={search}
                      onChange={(_e, v) => setSearch(v)}
                      onClear={() => setSearch("")}
                    />
                  </ToolbarItem>
                  <ToolbarItem align={{ default: "alignEnd" }}>
                    <Button variant="secondary" onClick={() => void list.refetch()} isLoading={list.isRefetching}>
                      Re-read
                    </Button>
                  </ToolbarItem>
                </ToolbarContent>
              </Toolbar>
              {rows.length === 0 ? (
                <Empty
                  title="No connection matches"
                  icon={SearchIcon}
                  actions={
                    <Button variant="link" onClick={() => setSearch("")}>
                      Clear the search
                    </Button>
                  }
                >
                  Nothing matches this search.
                </Empty>
              ) : narrow ? (
                <CompactList
                  label="Connections"
                  items={rows.map((c) => {
                    const probe = probeState(c.reachability);
                    return {
                      key: c.uid,
                      href: `#/${ns}/connections/${c.name}`,
                      title: c.name,
                      meta: (
                        <>
                          {c.role} · {AUTH_WORDS[c.auth.mode]} · {probe.observedAt ? <Instant value={probe.observedAt} /> : "no observation"}
                        </>
                      ),
                      status: <ProbeLabels state={probe} />,
                    };
                  })}
                />
              ) : (
                <Table aria-label="Connections" variant="compact" gridBreakPoint="grid-lg">
                  <Thead>
                    <Tr>
                      <Th width={20}>Name</Th>
                      <Th width={30}>Connection probe</Th>
                      <Th width={25}>Authentication</Th>
                      <Th width={25}>Bootstrap servers</Th>
                    </Tr>
                  </Thead>
                  <Tbody>
                    {rows.map((c) => {
                      const probe = probeState(c.reachability);
                      return (
                        <Tr key={c.uid}>
                          <Td dataLabel="Name">
                            <Link to="/$ns/connections/$name" params={{ ns, name: c.name }} className="lw-break">
                              {c.name}
                            </Link>
                            <div className="lw-subtle lw-small">{c.role}</div>
                          </Td>
                          <Td dataLabel="Connection probe">
                            <ProbeLabels state={probe} />
                            <div className="lw-subtle lw-small">
                              {probe.observedAt ? <Instant value={probe.observedAt} /> : "No observation recorded"}
                              {probe.reason && probe.verdict !== "reachable" && probe.verdict !== "probing" ? (
                                <>
                                  {" · "}
                                  <code>{probe.reason}</code>
                                </>
                              ) : null}
                              {probe.clusterId && probe.verdict === "reachable" ? (
                                <>
                                  {" · cluster "}
                                  <code>{probe.clusterId}</code>
                                </>
                              ) : null}
                              {probe.laterReason ? (
                                <>
                                  {" · latest probe "}
                                  <code>{probe.laterReason}</code>
                                </>
                              ) : null}
                            </div>
                          </Td>
                          <Td dataLabel="Authentication" className="lw-break">
                            {authText(c)}
                          </Td>
                          <Td dataLabel="Bootstrap servers" className="lw-break">
                            <span title={c.bootstrapServers.join(", ")}>{serversText(c.bootstrapServers)}</span>
                          </Td>
                        </Tr>
                      );
                    })}
                  </Tbody>
                </Table>
              )}
              <div className="lw-pad">
                <Content component="p" className="lw-subtle lw-small">
                  {PROBE_SENTENCE} Re-read reads what the controller has recorded since; it dials nothing. A connection's own page has Test
                  connection, which does dial.
                </Content>
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
        <AddConnection
          ns={ns}
          onClose={() => setAdding(false)}
          onCreated={(name) => {
            setAdding(false);
            void navigate({ to: "/$ns/connections/$name", params: { ns, name } });
          }}
        />
      )}
    </>
  );
}
