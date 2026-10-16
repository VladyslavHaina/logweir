// Every restore in the workspace, newest first: where it wrote, whether it verified
// (green only by the restore badge rule), how much was verified, and a narrowed
// restore said as partial. The old console's #/history (restore half) and #/operations.
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { Button, Card, Content, PageSection } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import HistoryIcon from "@patternfly/react-icons/dist/esm/icons/history-icon";
import { useCapabilities, useClones, useRestoreList, type Restore } from "../../api/restores";
import type { Clone } from "../../api/switchovers";
import { cloneListWords } from "../cutover/CloneLifetime";
import { PageHeader } from "../../components/PageHeader";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { exactUtc } from "../../components/When";
import { CompactList, useNarrow } from "../../components/CompactList";
import { Instant, RestoreStatus } from "./parts";
import { coverageWords, selectionWords } from "./words";
import "./restores.css";

/** What kind of restore this is, in a few words: names, mode, coverage, selection. A kept
 *  clone (its operation view's `status.clone`) says so and when it goes (DUI-23). */
export function restoreFacts(r: Restore, clone?: Clone): string {
  const kind = clone
    ? cloneListWords(clone)
    : r.target.mode === "scratch"
      ? "scratch rehearsal"
      : r.target.originalName
        ? "original names"
        : `prefix ${r.target.topicPrefix}`;
  const coverage = coverageWords(r.coverage?.recorded, r.coverage?.requested, { covered: r.coverage?.covered ?? undefined }, r.operation.terminal);
  const partial = selectionWords(r.selection) ? " · partial" : "";
  return `${kind} · ${coverage}${partial}`;
}

export function Restores() {
  const { ws } = useParams({ from: "/$ws/restores" });
  const navigate = useNavigate();
  const restores = useRestoreList(ws);
  const clones = useClones(ws, restores.data);
  const caps = useCapabilities(ws);
  const narrow = useNarrow();
  const mayCreate = caps?.restoreCreate !== false;
  const start = () => void navigate({ to: "/$ws/restores/new", params: { ws }, search: {} });
  return (
    <>
      <PageHeader
        title="Restores"
        description="Every restore in this workspace: where it wrote, whether it verified, and how much was compared."
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
                href: `#/${ws}/restores/${r.name}`,
                title: r.name,
                meta: (
                  <>
                    <Instant value={r.createdAt} /> · {restoreFacts(r, clones.get(r.name))}
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
                      <Link to="/$ws/restores/$name" params={{ ws, name: r.name }} className="lw-break">
                        {r.name}
                      </Link>
                      <div className="lw-subtle lw-small">{restoreFacts(r, clones.get(r.name))}</div>
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
