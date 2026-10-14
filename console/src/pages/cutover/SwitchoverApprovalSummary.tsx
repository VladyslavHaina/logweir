// For R6's approval page (subject kind Switchover): EVERYTHING the second person signs
// for, from `GET .../switchovers/{name}/approval-request` — the restore the plan applies
// (bound by its plan hash, its signed scorecard and its positions mapping), and every
// group, restored topic and position with its class and the position it must hold first.
// A request the service cannot show in full (`scopeComplete: false`) is shown as that
// refusal, never as a partial list. Approve itself stays on R6's page
// (`useApproveSwitchover` in api/switchovers.ts sends the click).
import { Alert, Content, DescriptionList, DescriptionListDescription, DescriptionListGroup, DescriptionListTerm, Title } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import type { SwitchoverApprovalRequest, SwitchoverScope } from "../../api/switchovers";
import { Ident } from "../../components/Ident";
import { MappingStatus } from "../../components/Status";
import { CompactList, useNarrow } from "../../components/CompactList";
import { count } from "../../components/When";
import { plural } from "./parts";
import "./cutover.css";
import { Prose } from "../../components/Prose";

const INCOMPLETE: Record<string, string> = {
  tooManyGroups: "it names more groups than one request can show",
  tooManyPositions: "it commits more positions than one request can show",
  planUnreadable: "its plan could not be read",
};

function ScopeTable({ scope }: { scope: SwitchoverScope }) {
  const narrow = useNarrow();
  return (
    <>
      {scope.groups.map((g) => {
        const rows = g.topics.flatMap((t) => t.positions.map((p) => ({ topic: t.topic, ...p })));
        const label = g.source === g.target ? g.target : `${g.source} → ${g.target}`;
        return (
          <section key={g.target} className="lw-scope-group" aria-label={`Group ${g.target}`}>
            <Title headingLevel="h3" size="md" className="lw-break">
              {label} <span className="lw-subtle lw-small">{plural(rows.length, "position")}</span>
            </Title>
            {narrow ? (
              <CompactList
                flush
                label={`Positions of ${g.target}`}
                items={rows.map((p) => ({
                  key: `${p.topic}-${p.partition}`,
                  title: `${p.topic} / ${p.partition}`,
                  meta: `Commits ${count(p.offset)} · must hold first: ${p.expected === null || p.expected === undefined ? "none" : count(p.expected)}`,
                  status: <MappingStatus cls={p.approximate ? "approximate" : "exact"} />,
                }))}
              />
            ) : (
              <Table aria-label={`Positions of ${g.target}`} variant="compact">
                <Thead>
                  <Tr>
                    <Th width={35}>Partition</Th>
                    <Th width={20} className="lw-right">
                      Commits
                    </Th>
                    <Th width={20}>Translation</Th>
                    <Th width={25} className="lw-right" modifier="nowrap">
                      Must hold first
                    </Th>
                  </Tr>
                </Thead>
                <Tbody>
                  {rows.map((p) => (
                    <Tr key={`${p.topic}-${p.partition}`}>
                      <Td dataLabel="Partition" className="lw-break">
                        {p.topic} / {p.partition}
                      </Td>
                      <Td dataLabel="Commits" className="lw-num lw-right">
                        {count(p.offset)}
                      </Td>
                      <Td dataLabel="Translation">
                        <MappingStatus cls={p.approximate ? "approximate" : "exact"} />
                      </Td>
                      <Td dataLabel="Must hold first" className="lw-num lw-right">
                        {p.expected === null || p.expected === undefined ? "None" : count(p.expected)}
                      </Td>
                    </Tr>
                  ))}
                </Tbody>
              </Table>
            )}
          </section>
        );
      })}
    </>
  );
}

/** The whole scope of one switchover request, or why it cannot be shown. */
export function SwitchoverApprovalSummary({ request }: { request: SwitchoverApprovalRequest }) {
  if (!request.scopeComplete || !request.scope) {
    const why = request.scopeIncomplete ? (INCOMPLETE[request.scopeIncomplete] ?? request.scopeIncomplete) : "the service did not say why";
    return (
      <Alert isInline variant="danger" title="This request cannot be shown in full, so it is not approved here">
        The console shows a switchover only with every group and position it commits; {why}. <span className="lw-reason">{request.scopeIncomplete ?? "scopeIncomplete"}</span>
      </Alert>
    );
  }
  const s = request.scope;
  return (
    <div>
      <Content component="p">
        <Prose
          text={
            request.scopeSentence ??
            `Approving lets the controller commit ${plural(s.positionsCount, "position")} for ${plural(s.groupsCount, "consumer group")} on restore ${s.restore.name}'s target.`
          }
        />{" "}
        Each is committed only to a group with no members, and only where the group holds exactly what is listed first.
      </Content>
      <DescriptionList isCompact className="lw-dl lw-gap-top" orientation={{ sm: "vertical", md: "horizontal" }}>
        <DescriptionListGroup>
          <DescriptionListTerm>Restore</DescriptionListTerm>
          <DescriptionListDescription>
            {s.restore.name} <span className="lw-subtle lw-small">UID {s.restore.uid}</span>
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>Its plan</DescriptionListTerm>
          <DescriptionListDescription>
            <Ident value={s.restore.planHash} label="restore plan sha256" />
            <div className="lw-subtle lw-small">The target cluster and credentials come from it.</div>
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>Its signed scorecard</DescriptionListTerm>
          <DescriptionListDescription>
            <Ident value={s.restore.scorecardSha256} label="scorecard sha256" />
          </DescriptionListDescription>
        </DescriptionListGroup>
        <DescriptionListGroup>
          <DescriptionListTerm>Its positions mapping</DescriptionListTerm>
          <DescriptionListDescription>
            <Ident value={s.restore.mappingSha256} label="positions mapping sha256" />
          </DescriptionListDescription>
        </DescriptionListGroup>
        {request.planHash && (
          <DescriptionListGroup>
            <DescriptionListTerm>Switchover plan</DescriptionListTerm>
            <DescriptionListDescription>
              <Ident value={request.planHash} label="switchover plan sha256" />
            </DescriptionListDescription>
          </DescriptionListGroup>
        )}
        {s.rollbackOf && (
          <DescriptionListGroup>
            <DescriptionListTerm>Rolls back</DescriptionListTerm>
            <DescriptionListDescription>
              <Ident value={s.rollbackOf.key} label="switchover record key" />
              <div className="lw-gap-top">
                <Ident value={s.rollbackOf.sha256} label="switchover record sha256" />
              </div>
            </DescriptionListDescription>
          </DescriptionListGroup>
        )}
        <DescriptionListGroup>
          <DescriptionListTerm>Scope</DescriptionListTerm>
          <DescriptionListDescription>
            {plural(s.groupsCount, "group")}, {plural(s.positionsCount, "position")}
          </DescriptionListDescription>
        </DescriptionListGroup>
      </DescriptionList>
      <div className="lw-gap-top">
        <ScopeTable scope={s} />
      </div>
    </div>
  );
}
