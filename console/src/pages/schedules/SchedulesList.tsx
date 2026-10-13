// Every schedule in the namespace: when each runs next, whether it is active,
// and the three things you do to one (back up now, suspend or resume, edit).
import { useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { Button, Card, PageSection } from "@patternfly/react-core";
import { ActionsColumn, Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import OutlinedClockIcon from "@patternfly/react-icons/dist/esm/icons/outlined-clock-icon";
import { useSchedules } from "../../api/queries";
import { useSetSuspension, type Schedule } from "../../api/schedules";
import { PageHeader } from "../../components/PageHeader";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { CompactList, useNarrow } from "../../components/CompactList";
import { When } from "../../components/When";
import { BackUpNow } from "./BackUpNow";
import { cadenceWords, zoneOf } from "./model";
import { ScheduleStatus, WriteAlert, useGrant } from "./parts";

function nextAt(s: Schedule): string | null | undefined {
  return s.status.nextRuns?.[0]?.at ?? s.status.nextFireTime;
}

function writtenTo(s: Schedule): string {
  return s.destinationRef ? s.destinationRef.name : "an archive URL";
}

/** Suspend or resume one row; its refusal is reported above the table. */
function useRowSuspension(ns: string, s: Schedule, onFailure: (f: Failure) => void) {
  const m = useSetSuspension(ns, s.name);
  return {
    pending: m.isPending,
    run: () =>
      m.mutate(
        { suspended: !s.suspended, expectedResourceVersion: s.resourceVersion },
        { onError: (error) => onFailure({ name: s.name, error, verb: s.suspended ? "resumed" : "suspended" }) },
      ),
  };
}

type Failure = { name: string; error: unknown; verb: string };

function Row({ ns, s, onBackUp, onFailure }: { ns: string; s: Schedule; onBackUp: (s: Schedule) => void; onFailure: (f: Failure) => void }) {
  const navigate = useNavigate();
  const grant = useGrant(ns);
  const suspension = useRowSuspension(ns, s, onFailure);
  const actions = [
    { title: "View details", onClick: () => void navigate({ to: "/$ns/schedules/$name", params: { ns, name: s.name }, search: {} }) },
    ...(grant?.manualBackupCreate ? [{ title: "Back up now", onClick: () => onBackUp(s) }] : []),
    ...(grant?.scheduleSetSuspension ? [{ title: s.suspended ? "Resume" : "Suspend", onClick: suspension.run, isDisabled: suspension.pending }] : []),
    ...(grant?.scheduleCreate ? [{ title: "Edit policy", onClick: () => void navigate({ to: "/$ns/schedules/$name/edit", params: { ns, name: s.name } }) }] : []),
  ];
  return (
    <Tr>
      <Td dataLabel="Name" className="lw-name-cell">
        <Link to="/$ns/schedules/$name" params={{ ns, name: s.name }} search={{}} className="lw-break">
          {s.name}
        </Link>
        <div className="lw-subtle lw-small">
          {s.allUserTopics ? "All user topics" : `${s.topics.length} ${s.topics.length === 1 ? "topic" : "topics"}`} from {s.sourceRef.name}
        </div>
      </Td>
      <Td dataLabel="Status">
        <ScheduleStatus schedule={s} />
      </Td>
      <Td dataLabel="Runs">
        {cadenceWords(s.preset, s.schedule)}
        <div className="lw-subtle lw-small">{zoneOf(s)}</div>
      </Td>
      <Td dataLabel="Written to">{writtenTo(s)}</Td>
      <Td dataLabel="Next run">{s.suspended ? <span className="lw-subtle">Suspended</span> : <When value={nextAt(s)} />}</Td>
      <Td dataLabel="Last run">
        <When value={s.status.lastFireTime} />
      </Td>
      <Td isActionCell>
        <ActionsColumn items={actions} />
      </Td>
    </Tr>
  );
}

export function SchedulesList() {
  const { ns } = useParams({ from: "/$ns/schedules" });
  const navigate = useNavigate();
  const schedules = useSchedules(ns);
  const grant = useGrant(ns);
  const narrow = useNarrow();
  const [backUp, setBackUp] = useState<string | null>(null);
  const [failure, setFailure] = useState<Failure | null>(null);
  const create = () => void navigate({ to: "/$ns/schedules/new", params: { ns } });
  const rows = [...(schedules.data ?? [])].sort((a, b) => a.name.localeCompare(b.name));
  // The modal reads the row as listed NOW, so a re-read after a refusal reaches it.
  const chosen = rows.find((s) => s.name === backUp);

  return (
    <>
      <PageHeader
        title="Schedules"
        description={
          grant && !grant.scheduleCreate
            ? "When each backup runs. This login may read the schedules here and not create or change one."
            : "When each backup runs, what it reads and where it writes."
        }
        actions={
          grant?.scheduleCreate ? (
            <Button variant="primary" onClick={create}>
              Create a schedule
            </Button>
          ) : undefined
        }
      />
      {failure && (
        <PageSection>
          <WriteAlert error={failure.error} kind="command" subject={failure.name} verb={failure.verb} onRetry={() => setFailure(null)} retryText="Dismiss" />
        </PageSection>
      )}
      <PageSection>
        <Card>
          {schedules.isPending ? (
            <div className="lw-pad">
              <LoadingRows rows={4} label="Loading schedules" />
            </div>
          ) : schedules.isError ? (
            <ErrorState error={schedules.error} onRetry={() => void schedules.refetch()} />
          ) : rows.length === 0 ? (
            <Empty
              title="No schedules yet"
              icon={OutlinedClockIcon}
              actions={
                grant?.scheduleCreate ? (
                  <Button variant="primary" onClick={create}>
                    Create a schedule
                  </Button>
                ) : undefined
              }
            >
              A schedule backs up a connection's topics at the times you choose and writes each run to a destination.
            </Empty>
          ) : narrow ? (
            <CompactList
              label="Schedules"
              items={rows.map((s) => ({
                key: s.name,
                href: `#/${ns}/schedules/${s.name}`,
                title: s.name,
                meta: (
                  <>
                    {cadenceWords(s.preset, s.schedule)}
                    {!s.suspended && nextAt(s) && (
                      <>
                        {" · next "}
                        <When value={nextAt(s)} />
                      </>
                    )}
                  </>
                ),
                status: <ScheduleStatus schedule={s} />,
              }))}
            />
          ) : (
            <Table aria-label="Schedules" variant="compact" gridBreakPoint="grid-lg">
              <Thead>
                <Tr>
                  <Th>Name</Th>
                  <Th>Status</Th>
                  <Th>Runs</Th>
                  <Th>Written to</Th>
                  <Th>Next run</Th>
                  <Th>Last run</Th>
                  <Th screenReaderText="Actions" />
                </Tr>
              </Thead>
              <Tbody>
                {rows.map((s) => (
                  <Row key={s.name} ns={ns} s={s} onBackUp={(x) => setBackUp(x.name)} onFailure={setFailure} />
                ))}
              </Tbody>
            </Table>
          )}
        </Card>
      </PageSection>
      {chosen && <BackUpNow key={chosen.name} ns={ns} schedule={chosen} open onClose={() => setBackUp(null)} />}
    </>
  );
}
