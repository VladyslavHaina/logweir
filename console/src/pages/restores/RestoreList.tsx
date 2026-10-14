// Every restore in the namespace, newest first: where it wrote, whether it verified
// (green only by the restore badge rule), how much was verified, and a narrowed
// restore said as partial. The old console's #/history (restore half) and #/operations.
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { Button, Card, Content, PageSection } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import HistoryIcon from "@patternfly/react-icons/dist/esm/icons/history-icon";
import { useCapabilities, useRestoreList, type Restore } from "../../api/restores";
import { PageHeader } from "../../components/PageHeader";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { exactUtc } from "../../components/When";
import { CompactList, useNarrow } from "../../components/CompactList";
import { Instant, RestoreStatus } from "./parts";
import { coverageWords, selectionWords } from "./words";
import "./restores.css";

/** What kind of restore this is, in a few words: names, mode, coverage, selection. */
export function restoreFacts(r: Restore): string {
  // A clone's expiry is on its operation view, not on the list item: its page shows it.
  const kind =
    r.target.mode === "scratch"
      ? "scratch rehearsal"
      : r.target.originalName
        ? "original names"
        : `prefix ${r.target.topicPrefix}`;
  const coverage = coverageWords(r.coverage?.recorded, r.coverage?.requested, { covered: r.coverage?.covered ?? undefined });
  const partial = selectionWords(r.selection) ? " · partial" : "";
  return `${kind} · ${coverage}${partial}`;
}

export function Restores() {
  const { ns } = useParams({ from: "/$ns/restores" });
  const navigate = useNavigate();
  const restores = useRestoreList(ns);
  const caps = useCapabilities(ns);
  const narrow = useNarrow();
  const mayCreate = caps?.restoreCreate !== false;
  const start = () => void navigate({ to: "/$ns/restores/new", params: { ns }, search: {} });
  return (
    <>
      <PageHeader
        title="Restores"
        description="Every restore in this namespace: where it wrote, whether it verified, and how much was compared."
        actions={
          mayCreate ? (
            <Button variant="primary" onClick={start}>
              Restore a point
            </Button>
          ) : undefined
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
          ) : restores.data.length === 0 ? (
            <Empty
              title="No restores yet"
              icon={HistoryIcon}
              actions={
                mayCreate ? (
                  <Button variant="secondary" onClick={start}>
                    Restore a point
                  </Button>
                ) : undefined
              }
            >
              A restore writes a recovery point into new topics and verifies what it wrote. Start with a drill on a test cluster.
            </Empty>
          ) : narrow ? (
            <CompactList
              label="Restores"
              items={restores.data.map((r) => ({
                key: r.uid,
                href: `#/${ns}/restores/${r.name}`,
                title: r.name,
                meta: (
                  <>
                    <Instant value={r.createdAt} /> · {restoreFacts(r)}
                  </>
                ),
                status: <RestoreStatus r={r} />,
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
                {restores.data.map((r) => (
                  <Tr key={r.uid}>
                    <Td dataLabel="Name">
                      <Link to="/$ns/restores/$name" params={{ ns, name: r.name }} className="lw-break">
                        {r.name}
                      </Link>
                      <div className="lw-subtle lw-small">{restoreFacts(r)}</div>
                    </Td>
                    <Td dataLabel="Status">
                      <RestoreStatus r={r} />
                    </Td>
                    <Td dataLabel="Into">
                      {r.target.clusterRef.name}
                      <div className="lw-subtle lw-small lw-break">{r.newTopics.length ? r.newTopics.join(", ") : "No topic recorded yet"}</div>
                    </Td>
                    <Td dataLabel="Point in time" modifier="nowrap">
                      {exactUtc(r.pointInTime)}
                    </Td>
                    <Td dataLabel="Started" modifier="nowrap">
                      <Instant value={r.createdAt} />
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
          {restores.data && restores.data.length > 0 && (
            <Content component="p" className="lw-subtle lw-small lw-pad">
              Verified means the controller verified the restore's signed scorecard; a list row does not carry the key or the instant,
              the restore's page shows both.
            </Content>
          )}
        </Card>
      </PageSection>
    </>
  );
}
