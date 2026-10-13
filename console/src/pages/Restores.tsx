// Every restore in the namespace, newest first.
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { Button, Card, PageSection } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import HistoryIcon from "@patternfly/react-icons/dist/esm/icons/history-icon";
import { useRestores } from "../api/queries";
import { PageHeader } from "../components/PageHeader";
import { RunStatus } from "../components/Status";
import { Empty, ErrorState, LoadingRows } from "../components/States";
import { When, exactUtc, relative } from "../components/When";
import { CompactList, useNarrow } from "../components/CompactList";
import { themeAOf } from "../mock/themeA";
import type { Restore } from "../api/types";

/** What kind of restore this is, in a few words: names, clone expiry, verification. */
function facts(r: Restore): string {
  const clone = themeAOf(r).clone;
  const kind = clone
    ? clone.phase === "Removed"
      ? "clone, removed"
      : `clone, deletes ${relative(clone.expiresAt)}`
    : r.target.originalName
      ? "original names"
      : `prefix ${r.target.topicPrefix}`;
  const coverage = r.coverage?.requested === "complete" ? "complete verification" : "sampled verification";
  return `${kind} · ${coverage}`;
}

export function Restores() {
  const { ns } = useParams({ from: "/$ns/restores" });
  const navigate = useNavigate();
  const restores = useRestores(ns);
  const narrow = useNarrow();
  const start = () => void navigate({ to: "/$ns/restores/new", params: { ns }, search: {} });
  return (
    <>
      <PageHeader
        title="Restores"
        description="Every restore in this namespace: where it wrote, whether it verified, and its consumer cutover."
        actions={
          <Button variant="primary" onClick={start}>
            Restore a point
          </Button>
        }
      />
      <PageSection>
        <Card>
          {restores.isPending ? (
            <div className="lw-pad">
              <LoadingRows rows={4} label="Loading restores" />
            </div>
          ) : restores.isError ? (
            <ErrorState error={restores.error} onRetry={() => void restores.refetch()} />
          ) : (restores.data ?? []).length === 0 ? (
            <Empty
              title="No restores yet"
              icon={HistoryIcon}
              actions={
                <Button variant="secondary" onClick={start}>
                  Restore a point
                </Button>
              }
            >
              A restore writes a recovery point into new topics and verifies what it wrote. Start with a drill on a test
              cluster.
            </Empty>
          ) : narrow ? (
            <CompactList
              label="Restores"
              items={(restores.data ?? []).map((r) => ({
                key: r.name,
                href: `#/${ns}/restores/${r.name}`,
                title: r.name,
                meta: (
                  <>
                    <When value={r.createdAt} /> · {facts(r)}
                  </>
                ),
                status: <RunStatus op={r.operation} />,
              }))}
            />
          ) : (
            <Table aria-label="Restores" variant="compact" gridBreakPoint="grid-md">
              <Thead>
                <Tr>
                  <Th>Name</Th>
                  <Th>Status</Th>
                  <Th>Into</Th>
                  <Th modifier="nowrap">Point in time</Th>
                  <Th modifier="nowrap">Started</Th>
                </Tr>
              </Thead>
              <Tbody>
                {(restores.data ?? []).map((r) => (
                  <Tr key={r.name}>
                    <Td dataLabel="Name">
                      <Link to="/$ns/restores/$name" params={{ ns, name: r.name }} className="lw-break">
                        {r.name}
                      </Link>
                      <div className="lw-subtle lw-small">{facts(r)}</div>
                    </Td>
                    <Td dataLabel="Status">
                      <RunStatus op={r.operation} />
                    </Td>
                    <Td dataLabel="Into">
                      {r.target.clusterRef.name}
                      <div className="lw-subtle lw-small">{r.newTopics.join(", ")}</div>
                    </Td>
                    <Td dataLabel="Point in time" modifier="nowrap">{exactUtc(r.pointInTime)}</Td>
                    <Td dataLabel="Started" modifier="nowrap">
                      <When value={r.createdAt} />
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
        </Card>
      </PageSection>
    </>
  );
}
