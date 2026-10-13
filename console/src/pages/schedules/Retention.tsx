// Retention for a schedule's destination. A schedule's own spec.retention no
// longer reports; a RetentionPolicy that names the destination does. "Logweir
// never deletes from your archive" is said only when the namespace's policies
// were read and none that deletes can reach this archive.
import { Alert, Content, DescriptionList, Stack } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import type { RetentionPolicy, Schedule } from "../../api/schedules";
import CheckIcon from "@patternfly/react-icons/dist/esm/icons/check-circle-icon";
import { When, count } from "../../components/When";
import { ENFORCEMENT_WORDS, NEVER_DELETES, deletes, retentionOf } from "./model";
import { Term } from "./parts";

const GUARANTEES: Record<string, string> = {
  LogweirEnforced: "Enforced by Logweir",
  ProviderEnforcedUnverified: "Declared by your provider; Logweir cannot verify it",
  NotEnforced: "Not enforced",
};

function condition(p: RetentionPolicy, type: string) {
  return p.conditions?.find((c) => c.type === type);
}

function Covering({ policy }: { policy: RetentionPolicy }) {
  const e = policy.lastEvaluation;
  const recorded = e?.accounting === "Recorded";
  const enforced = condition(policy, "Enforced");
  const words = policy.enforcement ? ENFORCEMENT_WORDS[policy.enforcement] : undefined;
  return (
    <div>
      <Content component="p">
        <strong>{policy.name}</strong> governs this destination. {words ?? `It reports what is happening as ${policy.enforcement ?? "not recorded"}.`}
        {policy.mode === "Enforce" && policy.enforcement !== "LogweirWorker" ? " It is asked to enforce, but is not enforcing now." : ""}
      </Content>
      {policy.mode === "Enforce" && enforced?.status === "False" && (
        <Alert isInline isPlain variant="warning" title={`Not enforcing: ${enforced.reason ?? "no reason recorded"}`}>
          {enforced.message}
        </Alert>
      )}
      {policy.enforcementDegraded && (
        <Alert isInline isPlain variant="danger" title="Enforcement degraded">
          Three enforcement runs in a row failed, so this policy has stopped scheduling them until its settings change.
        </Alert>
      )}
      <DescriptionList isCompact isHorizontal horizontalTermWidthModifier={{ default: "16ch" }}>
        <Term term="Mode">{policy.mode}</Term>
        <Term term="Keeps">
          {[policy.keepLast ? `The newest ${policy.keepLast} points` : "", policy.keepDays ? `${policy.keepDays} days of points` : ""].filter(Boolean).join(" and ") || "Not recorded"}
          <div className="lw-subtle lw-small">Never fewer than {policy.minUsablePoints} usable points.</div>
        </Term>
        <Term term="Age expiry">{GUARANTEES[policy.guarantees?.ageExpiry ?? ""] ?? "Not recorded"}</Term>
        <Term term="Points in use by a restore">{GUARANTEES[policy.guarantees?.activeRestoreProtection ?? ""] ?? "Not recorded"}</Term>
        <Term term="Legal hold">{GUARANTEES[policy.guarantees?.legalHold ?? ""] ?? "Not recorded"}</Term>
        {e && typeof e.pointsEvaluated === "number" && (
          <Term term="Last evaluation">
            <When value={e.at} />: {count(e.pointsEvaluated)} points; {recorded ? `${count(e.keptCount)} kept, ` : ""}
            {count(e.candidateCount)} in this plan
            {recorded && e.truncatedByCap ? `, ${count(e.truncatedByCap)} more held back by the per-run ceiling of ${count(e.maxDeletionsPerRun)}` : ""}.
            {!recorded && " This evaluation does not record how many points it keeps."}
            {e.viewIncomplete && " The catalog view it read does not hold every point of the archive."}
          </Term>
        )}
      </DescriptionList>
      {deletes(policy) && policy.enforcement === "LogweirWorker" && (
        <Content component="p" className="lw-subtle lw-small">
          Approving a plan authorises deletion of the archive objects it names. Deletion cannot be undone.
        </Content>
      )}
    </div>
  );
}

export function RetentionCard({ schedule, policies, failed }: { schedule: Schedule; policies: RetentionPolicy[] | undefined; failed: boolean }) {
  const coverage = retentionOf(schedule, failed ? undefined : policies);
  return (
    <Stack hasGutter>
      {schedule.retention && (
        <Alert isInline isPlain variant="info" title="This schedule's own retention setting is withdrawn">
          It is stored but reports nothing; editing the policy removes it. A RetentionPolicy sets retention.
        </Alert>
      )}
      {coverage.kind === "unread" && (
        <Content component="p" className="lw-subtle">
          {failed
            ? "The retention policies of this namespace could not be read, so this page cannot say whether one deletes from this archive."
            : "Reading the retention policies."}
        </Content>
      )}
      {coverage.kind === "inline" && (
        <Content component="p">This schedule writes to an archive URL, which no RetentionPolicy names. {NEVER_DELETES}</Content>
      )}
      {coverage.kind === "none" && coverage.elsewhere.length === 0 && (
        <Content component="p">
          No RetentionPolicy names {schedule.destinationRef?.name}.{" "}
          <span className="lw-inline-icon">
            <CheckIcon className="lw-tone-success" aria-hidden="true" />
            {NEVER_DELETES}
          </span>
        </Content>
      )}
      {coverage.kind === "one" && <Covering policy={coverage.policy} />}
      {coverage.kind === "conflict" && (
        <Alert isInline variant="warning" title="More than one RetentionPolicy names this destination">
          The controller marks each of them not ready (Conflict): none evaluates and none enforces until one is removed:{" "}
          {coverage.policies.map((p) => p.name).join(", ")}.
        </Alert>
      )}
      {(coverage.kind === "none" || coverage.kind === "one") && coverage.elsewhere.length > 0 && (
        <>
          <Alert isInline isPlain variant="warning" title="A policy that deletes names another destination">
            Two destinations can point at one bucket and prefix, and retention matches points by location. This page does not
            compare locations, so it does not say that nothing deletes from this archive. Check which destination each governs.
          </Alert>
          <Table aria-label="Retention policies that delete" variant="compact">
            <Thead>
              <Tr>
                <Th>Policy</Th>
                <Th>Destination</Th>
                <Th>What is happening</Th>
              </Tr>
            </Thead>
            <Tbody>
              {coverage.elsewhere.map((p) => (
                <Tr key={p.name}>
                  <Td dataLabel="Policy">{p.name}</Td>
                  <Td dataLabel="Destination">{p.destinationRef.name}</Td>
                  <Td dataLabel="What is happening">{p.enforcement}</Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        </>
      )}
    </Stack>
  );
}
