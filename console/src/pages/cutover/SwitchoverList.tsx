// Recover → Switchovers: every Switchover in the namespace, newest first as the API
// lists them, with where each stands and what it did per group (PROD-04.2 §3.3).
import { Link } from "@tanstack/react-router";
import { useParams } from "@tanstack/react-router";
import { Card, CardBody, PageSection } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import ExchangeAltIcon from "@patternfly/react-icons/dist/esm/icons/exchange-alt-icon";
import { isTerminal, useSwitchovers, type Switchover } from "../../api/switchovers";
import { PageHeader } from "../../components/PageHeader";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { CompactList, useNarrow } from "../../components/CompactList";
import { PhaseLabel } from "./status";
import { plural } from "./parts";
import { useCan } from "../../api/session";

/** DUI-28: from an ended switchover back to a new review of its restore (the cutover page
 *  reads the restore again and says why when it cannot be switched over). */
function ReviewAgain({ ns, s }: { ns: string; s: Switchover }) {
  return (
    <Link to="/$ns/restores/$name/cutover" params={{ ns, name: s.restore.name }} search={{}} aria-label={`Review again: restore ${s.restore.name}`}>
      Review again
    </Link>
  );
}

/** "2 applied, 1 refused" from the reported groups; the phase's words before that. */
export function groupSummary(s: Switchover): string {
  if (s.groups.length === 0) {
    if (s.phase === "Refused") return "Refused before any group";
    if (s.phase === "Pending" && s.reason === "ApprovalNotVerified") return "Waiting for approval";
    return isTerminal(s) ? "No group reported" : "Not reported yet";
  }
  const n = (o: string) => s.groups.filter((g) => g.outcome === o).length;
  return [
    n("applied") && `${n("applied")} applied`,
    n("alreadyApplied") && `${n("alreadyApplied")} already applied`,
    n("refused") && `${n("refused")} refused`,
    n("failed") && `${n("failed")} failed`,
    n("untouched") && `${n("untouched")} not reached`,
  ]
    .filter(Boolean)
    .join(", ");
}

export function SwitchoverList() {
  const { ns } = useParams({ from: "/$ns/switchovers" });
  const list = useSwitchovers(ns);
  const narrow = useNarrow();
  // DUI-30: a review is a write; only a role that may create a restore here is offered one.
  const canWrite = useCan(ns, "restoreCreate");
  const header = (
    <PageHeader
      title="Switchovers"
      description="Each switchover moved consumer groups onto a verified restore's topics, at the positions the restore mapped. One starts from a restore's page: Switch consumers over."
    />
  );
  if (list.isPending || list.isError) {
    return (
      <>
        {header}
        <PageSection>
          <Card>
            <CardBody>{list.isPending ? <LoadingRows rows={4} /> : <ErrorState error={list.error} onRetry={() => void list.refetch()} />}</CardBody>
          </Card>
        </PageSection>
      </>
    );
  }
  const items = list.data;
  return (
    <>
      {header}
      <PageSection>
        <Card>
          {items.length === 0 ? (
            <Empty
              title="No switchovers yet"
              icon={ExchangeAltIcon}
              actions={
                <Link to="/$ns/restores" params={{ ns }} className="pf-v6-c-button pf-m-secondary">
                  Restores
                </Link>
              }
            >
              A switchover starts from a restore that was verified completely: open it and choose Switch consumers over.
            </Empty>
          ) : narrow ? (
            <CardBody>
              <CompactList
                label="Switchovers"
                items={items.map((s) => ({
                  key: s.uid || s.name,
                  href: `#/${ns}/switchovers/${s.name}`,
                  title: s.name,
                  meta: (
                    <>
                      {s.restore.name} · {groupSummary(s)}
                      {canWrite && isTerminal(s) && (
                        <>
                          {" · "}
                          <ReviewAgain ns={ns} s={s} />
                        </>
                      )}
                    </>
                  ),
                  status: <PhaseLabel sw={s} />,
                }))}
              />
            </CardBody>
          ) : (
            <Table aria-label="Switchovers">
              <Thead>
                <Tr>
                  <Th width={25}>Switchover</Th>
                  <Th width={25}>Restore</Th>
                  <Th width={20}>Status</Th>
                  <Th width={canWrite ? 20 : 30}>Groups</Th>
                  {canWrite && <Th width={10} screenReaderText="Next" />}
                </Tr>
              </Thead>
              <Tbody>
                {items.map((s) => (
                  <Tr key={s.uid || s.name}>
                    <Td dataLabel="Switchover" className="lw-break">
                      <Link to="/$ns/switchovers/$name" params={{ ns, name: s.name }}>
                        {s.name}
                      </Link>
                    </Td>
                    <Td dataLabel="Restore" className="lw-break">
                      <Link to="/$ns/restores/$name" params={{ ns, name: s.restore.name }}>
                        {s.restore.name}
                      </Link>
                    </Td>
                    <Td dataLabel="Status">
                      <PhaseLabel sw={s} />
                      {s.reason && s.phase !== s.reason && s.reason !== "JobCreated" && <div className="lw-reason">{s.reason}</div>}
                    </Td>
                    <Td dataLabel="Groups">
                      {groupSummary(s)}
                      {s.groups.length > 0 && <div className="lw-subtle lw-small">{plural(s.groups.reduce((n, g) => n + g.partitions, 0), "partition")}</div>}
                    </Td>
                    {canWrite && <Td dataLabel="Next">{isTerminal(s) && <ReviewAgain ns={ns} s={s} />}</Td>}
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
