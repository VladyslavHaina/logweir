// Trust: the installation's trust policies, read-only. Which keys exist, what
// each may do, where it is in its lifecycle, and what the controller last
// decided about it, shown only while that decision is fresh. Nothing here adds,
// retires or revokes a key: that is a cluster-admin step with kubectl.
import { Alert, Card, CardBody, CardHeader, CardTitle, Content, DescriptionList, Flex, FlexItem, Grid, GridItem, PageSection } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import KeyIcon from "@patternfly/react-icons/dist/esm/icons/key-icon";
import LockIcon from "@patternfly/react-icons/dist/esm/icons/lock-icon";
import { ApiError } from "../../api/client";
import { useTrustPolicies, type TrustPolicy } from "../../api/backups";
import { PageHeader } from "../../components/PageHeader";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { StatusLabel } from "../../components/Status";
import { When, count, exactUtc } from "../../components/When";
import { Ident } from "../../components/Ident";
import { Commands, Term, Value, useAnyFlags } from "../backups/parts";
import { FINGERPRINT_COMMAND } from "../catalog/model";
import "../backups/evidence.css";
import {
  CONFLICT_SENTENCE,
  EDIT_SENTENCE,
  FINGERPRINT_SENTENCE,
  KEYS_FORBIDDEN_SENTENCE,
  NO_TRUST_SENTENCE,
  POLICY_SNIPPET,
  UNKNOWN_IS_NOT_VALID_SENTENCE,
  compromiseGuard,
  isFresh,
  keyEvaluation,
  lifecycleWords,
  unknownReason,
  usabilityWords,
} from "./model";

function list(names: string[] | undefined): string {
  return names && names.length > 0 ? names.join(", ") : "";
}

function PolicyCard({ p }: { p: TrustPolicy }) {
  const fresh = isFresh(p);
  const why = unknownReason(p);
  const guard = compromiseGuard(p);
  const conflicts = p.conflicts ?? [];
  const keys = p.keys ?? [];
  return (
    <Card>
      <CardHeader>
        <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "wrap" }}>
          <FlexItem>
            <CardTitle component="h2">{p.name}</CardTitle>
          </FlexItem>
          {p.default && (
            <FlexItem>
              <StatusLabel tone="info" text="Installation default" />
            </FlexItem>
          )}
          <FlexItem>
            {p.loaded === true ? (
              <StatusLabel tone="success" text="Loaded" />
            ) : p.loaded === false ? (
              <StatusLabel tone="danger" text="Not loaded" />
            ) : (
              <StatusLabel tone="neutral" text="Load not recorded" />
            )}
          </FlexItem>
          <FlexItem>
            {fresh ? <StatusLabel tone="success" text="Evaluation fresh" /> : <StatusLabel tone="neutral" text="Evaluation unknown" title={why ?? undefined} />}
          </FlexItem>
        </Flex>
      </CardHeader>
      <CardBody className="lw-ev-stack">
        {conflicts.length > 0 && (
          <Alert isInline variant="danger" title="Namespaces claimed by more than one policy">
            <p>{CONFLICT_SENTENCE}</p>
            <ul className="lw-plain-list">
              {conflicts.map((c) => (
                <li key={c.namespace}>
                  {c.namespace} ({c.policies.join(", ")})
                </li>
              ))}
            </ul>
          </Alert>
        )}
        {guard && (
          <Alert isInline variant={guard.act ? "danger" : "info"} title={`Compromise guard: ${guard.reason}`}>
            {guard.message}
          </Alert>
        )}
        <DescriptionList isCompact orientation={{ sm: "horizontal" }} className="lw-dl" columnModifier={{ lg: "2Col" }}>
          <Term term="Evaluation">
            {fresh ? "Fresh" : `Unknown. ${why}`}
            <div className="lw-subtle lw-small">
              Decided by the product API against its own clock, which read {exactUtc(p.evaluation.serverTime)}: fresh means within{" "}
              {count(p.evaluation.freshWithinSeconds)} s
              {typeof p.evaluation.ageSeconds === "number" ? `, and this evaluation is ${count(p.evaluation.ageSeconds)} s old` : ""}.
            </div>
          </Term>
          <Term term="Evaluated at">
            <When value={p.evaluation.evaluatedAt} exact />
          </Term>
          <Term term="Generation">
            {p.generation ?? "Not recorded"}
            <span className="lw-subtle"> · observed {p.observedGeneration ?? "not recorded"}</span>
          </Term>
          <Term term="Claims namespaces">
            <Value value={list(p.namespaces)} />
          </Term>
          <Term term="Governs namespaces">
            <Value value={list(p.boundNamespaces)} />
          </Term>
          <Term term="Namespace lists">
            {p.namespacesFiltered ? "Narrowed to the namespaces you administer" : "Complete for this policy"}
            {p.namespacesTruncated ? "; cut short by the row limit" : ""}
          </Term>
          <Term term="Keys">
            {count(p.keyCount)}
            {p.keysTruncated ? `; ${count(keys.length)} shown, the list was cut short` : ""}
          </Term>
          <Term term="Allowed target clusters">
            {count(p.allowedTargetClusterIds)} {p.allowedTargetClusterIds === 1 ? "cluster id" : "cluster ids"} a rehearsal or restore may write to
          </Term>
        </DescriptionList>
        {(p.conditions ?? []).filter((c) => c.type !== "CompromiseGuard").length > 0 && (
          <ul className="lw-plain-list lw-small">
            {(p.conditions ?? [])
              .filter((c) => c.type !== "CompromiseGuard")
              .map((c) => (
                <li key={c.type} className="lw-break">
                  <strong>{c.type}</strong> <span className="lw-reason">{c.status} {c.reason ?? ""}</span>
                  {c.message ? <span className="lw-subtle">: {c.message}</span> : null}
                </li>
              ))}
          </ul>
        )}
        <Content component="p" className="lw-subtle lw-small">
          {UNKNOWN_IS_NOT_VALID_SENTENCE}
        </Content>
        {keys.length === 0 ? (
          <Content component="p" className="lw-subtle">
            This policy carries no key. A policy with no key verifies nothing and authorises nothing.
          </Content>
        ) : (
          <Table aria-label={`Keys of ${p.name}`} variant="compact" gridBreakPoint="grid-xl">
            <Thead>
              <Tr>
                <Th>Key</Th>
                <Th>Held by</Th>
                <Th>Valid</Th>
                <Th>Lifecycle</Th>
                <Th>Evaluation</Th>
                <Th>May</Th>
              </Tr>
            </Thead>
            <Tbody>
              {keys.map((k) => {
                const ev = keyEvaluation(p, k);
                return (
                  <Tr key={k.keyId}>
                    <Td dataLabel="Key">
                      <Ident value={k.keyId} label="key id" />
                      <div className="lw-subtle lw-small">
                        {k.algorithm} · {k.usages.join(", ")}
                      </div>
                    </Td>
                    <Td dataLabel="Held by" className="lw-break">
                      {k.principal.display ?? <span className="lw-subtle">No label</span>}
                      <div className="lw-mono lw-subtle">{k.principal.id}</div>
                    </Td>
                    <Td dataLabel="Valid" className="lw-small">
                      <span title={`${exactUtc(k.notBefore)} to ${exactUtc(k.notAfter)}`}>
                        {k.notBefore.slice(0, 10)} to {k.notAfter.slice(0, 10)}
                      </span>
                      <div className="lw-subtle">declared {k.state}</div>
                    </Td>
                    <Td dataLabel="Lifecycle" className="lw-small">
                      {lifecycleWords(p, k)}
                    </Td>
                    <Td dataLabel="Evaluation">
                      <StatusLabel tone={ev.tone} text={ev.word} title={ev.reason} />
                    </Td>
                    <Td dataLabel="May" className="lw-small">
                      {usabilityWords(p, k)}
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

export function Trust() {
  const flags = useAnyFlags();
  const allowed = flags?.trustPoliciesRead === true;
  const policies = useTrustPolicies(allowed);
  const forbidden = (flags !== null && !allowed) || (policies.error instanceof ApiError && policies.error.status === 403);
  const items = policies.data ?? [];

  return (
    <>
      <PageHeader
        title="Trust"
        description="The trust policies this installation verifies evidence and approvals against: which keys exist, what each may do, where it is in its lifecycle, and what the controller last decided about it. This page reads; it changes nothing."
      />
      <PageSection>
        {forbidden ? (
          <Card>
            <Empty title="Only administrators can view trust material" icon={LockIcon}>
              {KEYS_FORBIDDEN_SENTENCE}
            </Empty>
          </Card>
        ) : policies.isPending ? (
          <Card>
            <CardBody>
              <LoadingRows rows={6} label="Loading the trust policies" />
            </CardBody>
          </Card>
        ) : policies.isError ? (
          <Card>
            <ErrorState error={policies.error} onRetry={() => void policies.refetch()} />
          </Card>
        ) : (
          <Grid hasGutter>
            {items.length === 0 ? (
              <GridItem>
                <Card>
                  <Empty title="No trust policy" icon={KeyIcon}>
                    {NO_TRUST_SENTENCE}
                  </Empty>
                </Card>
              </GridItem>
            ) : (
              items.map((p) => (
                <GridItem key={p.uid || p.name}>
                  <PolicyCard p={p} />
                </GridItem>
              ))
            )}
            <GridItem lg={5}>
              <Card isFullHeight>
                <CardHeader>
                  <CardTitle component="h2">Check a key out of band</CardTitle>
                </CardHeader>
                <CardBody className="lw-ev-stack">
                  <Content component="p" className="lw-subtle">
                    {FINGERPRINT_SENTENCE}
                  </Content>
                  <Commands lines={[FINGERPRINT_COMMAND]} label="the fingerprint command" />
                </CardBody>
              </Card>
            </GridItem>
            <GridItem lg={7}>
              <Card isFullHeight>
                <CardHeader>
                  <CardTitle component="h2">Editing trust is a cluster-admin step</CardTitle>
                </CardHeader>
                <CardBody className="lw-ev-stack">
                  <Content component="p" className="lw-subtle">
                    {EDIT_SENTENCE}
                  </Content>
                  <Commands lines={POLICY_SNIPPET.split("\n")} label="the document" />
                </CardBody>
              </Card>
            </GridItem>
          </Grid>
        )}
      </PageSection>
    </>
  );
}
