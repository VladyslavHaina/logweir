// The pieces the Approvals pages share: the one status label for an approval's
// state, principals in full, an Approval's record, the two-person request and its
// scope, and the refusal block (a refusal reads as a refusal, never as an error).
import type { ReactNode } from "react";
import {
  Alert,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import BanIcon from "@patternfly/react-icons/dist/esm/icons/ban-icon";
import {
  afterwardsWords,
  approvalSubjectWords,
  instantText,
  OWNER_WORDS,
  ownerCase,
  recordState,
  REVOKED_AFTER_USE_WORDS,
  selfAttestedSentence,
  STATE_WORDS,
  TRANSACTION_MARKERS_SENTENCE,
  TWO_PERSON_ORIGINAL_NAME_SENTENCE,
  typedTopicsOf,
  youOf,
  type Approval,
  type ApprovalGrant,
  type ApprovalPacket,
  type ApprovalRequest,
  type ApprovalScope,
  type Found,
} from "../../api/approvals";
import { StatusLabel } from "../../components/Status";
import { Ident } from "../../components/Ident";
import { When } from "../../components/When";
import { ApiError } from "../../api/client";

export function Term({ term, children }: { term: string; children: ReactNode }) {
  return (
    <DescriptionListGroup>
      <DescriptionListTerm>{term}</DescriptionListTerm>
      <DescriptionListDescription>{children}</DescriptionListDescription>
    </DescriptionListGroup>
  );
}

export function NotRecorded() {
  return <span className="lw-subtle">Not recorded</span>;
}

/** An approval's state as the one status style. Green only when the controller verified it. */
export function ApprovalStatus({ found, waiting = false }: { found: Found; waiting?: boolean }) {
  const title = found.reason ? `${found.state}: ${found.reason}` : found.state;
  switch (found.state) {
    case "absent":
      return <StatusLabel tone="neutral" text={waiting ? "Waiting for approval" : STATE_WORDS.absent} title={title} />;
    case "awaiting-verification":
      return <StatusLabel tone="progress" text={STATE_WORDS["awaiting-verification"]} title={title} />;
    case "verified":
      return <StatusLabel tone="success" text={STATE_WORDS.verified} title={`verified by the controller against key ${found.key}`} />;
    case "revoked-after-use":
      return <StatusLabel tone="danger" text={STATE_WORDS["revoked-after-use"]} title={REVOKED_AFTER_USE_WORDS} />;
    default:
      return <StatusLabel tone="danger" text={STATE_WORDS[found.state]} title={title} />;
  }
}

/** A two-person request's own state. Never green: "approved" is the console's
 *  record that a second person approved; the controller's verdict is the Approval's. */
export function RequestStatus({ state }: { state: ApprovalRequest["state"] | string }) {
  if (state === "pending") return <StatusLabel tone="warning" text="Waiting for a second person" title="pending" />;
  if (state === "approved") return <StatusLabel tone="info" text="Approved in the console" title="approved" />;
  if (state === "expired") return <StatusLabel tone="danger" text="Expired" title="expired" />;
  if (state === "notConfirmed") return <StatusLabel tone="danger" text="Not confirmed by this console" title="notConfirmed" />;
  return <StatusLabel tone="neutral" text="Unknown" title={String(state)} />;
}

/**
 * A principal IN FULL, wrapped and never shortened, where an approval is decided
 * or audited: two principals that differ anywhere read differently without a
 * hover. "you (<name>)" comes first when it is the signed-in actor's own.
 */
export function Principal({ id, grant }: { id: string | null | undefined; grant: ApprovalGrant }) {
  if (!id) return <NotRecorded />;
  const you = youOf(id, grant);
  return (
    <span className="lw-principal">
      {you && <strong>{you} </strong>}
      <code className="lw-mono lw-break">{id}</code>
    </span>
  );
}

/** A list's short form of `<issuer>#<subject>`: the issuer's host and the subject, the full value on hover. */
export function principalShort(id: string, grant: ApprovalGrant): string {
  const you = youOf(id, grant);
  if (you) return you;
  const at = id.indexOf("#");
  if (at === -1) return id;
  const host = /^[a-z][a-z0-9+.-]*:\/\/([^/?#]+)/i.exec(id.slice(0, at))?.[1] ?? id.slice(0, at);
  const subject = id.slice(at + 1);
  return `${host} · ${subject.length > 20 ? `${subject.slice(0, 6)}…${subject.slice(-12)}` : subject}`;
}

/** A refusal: what was not done and why, in the server's words. Not an error. */
export function Refusal({ title, children, id }: { title: string; children?: ReactNode; id?: string }) {
  return (
    <Alert variant="custom" customIcon={<BanIcon />} isInline isPlain title={title} id={id} className="lw-refusal" component="h3">
      {children}
    </Alert>
  );
}

/** An error's status line, for a block that keeps the rest of the page standing. */
export function ErrorLine({ error }: { error: unknown }) {
  const api = error instanceof ApiError ? error : null;
  return (
    <span className="lw-error-meta">
      {error instanceof Error ? error.message : String(error)}
      {api && ` (${api.status} ${api.code}${api.requestId ? ` · request ${api.requestId}` : ""})`}
    </span>
  );
}

/** One Approval as recorded: the controller's verdict and every value it carries. */
export function ApprovalRecord({ approval, packet, grant, heading = "The approval record" }: { approval: Approval; packet?: ApprovalPacket | null; grant: ApprovalGrant; heading?: string }) {
  const found = recordState(approval);
  const verified = approval.conditions.find((c) => c.type === "Verified");
  const typed = typedTopicsOf(packet);
  return (
    <Card>
      <CardHeader actions={{ actions: <ApprovalStatus found={found} />, hasNoOffset: true }}>
        <CardTitle component="h2">{heading}</CardTitle>
      </CardHeader>
      <CardBody>
        <DescriptionList isCompact orientation={{ md: "horizontal" }} horizontalTermWidthModifier={{ default: "14ch" }}>
          <Term term="Approval">{approval.name}</Term>
          <Term term="Subject">
            {approval.subjectRef.kind}/{approval.subjectRef.name}
          </Term>
          <Term term="Plan hash">
            <code className="lw-mono lw-break">{approval.planHash}</code>
          </Term>
          <Term term="Verified for">
            {approval.verifiedSubject ? <Ident value={approval.verifiedSubject.uid} label="verified subject uid" /> : <NotRecorded />}
          </Term>
          <Term term="Verdict">
            {verified ? (
              <>
                {verified.reason ?? "Not recorded"}
                {verified.message && <div className="lw-subtle lw-small lw-break">{verified.message}</div>}
              </>
            ) : (
              <span className="lw-subtle">The controller has not decided yet.</span>
            )}
          </Term>
          <Term term="Matched key">{approval.matchedKeyId ? <Ident value={approval.matchedKeyId} label="matched key id" /> : <NotRecorded />}</Term>
          <Term term="Approver">
            <Principal id={approval.approver} grant={grant} />
          </Term>
          <Term term="Ticket">{approval.ticket ? approval.ticket : <NotRecorded />}</Term>
          <Term term="Authorises">{approvalSubjectWords(approval.approvalSubject)}</Term>
          {typed && (
            <Term term="Confirmation">
              <span id="typed-topics">
                Confirmed by one person with every original topic name re-typed:{" "}
                {typed.map((t, i) => (
                  <span key={`${t}-${i}`}>
                    {i > 0 && ", "}
                    <code className="lw-mono">{t}</code>
                  </span>
                ))}
              </span>
            </Term>
          )}
          {approval.authorization && (
            <Term term="Authorized under">
              {approval.authorization.policyName} ({approval.authorization.mode}), requested by{" "}
              <Principal id={approval.authorization.requester} grant={grant} />
            </Term>
          )}
          <Term term="Recorded">
            <When value={approval.createdAt} exact />
          </Term>
        </DescriptionList>
        <Content component="p" className="lw-subtle lw-small lw-gap-top">
          {selfAttestedSentence(approval.selfAttestedRisk)}
        </Content>
      </CardBody>
    </Card>
  );
}

function Storage({ s }: { s: ApprovalScope["evidence"] }) {
  return (
    <span className="lw-break">
      <code className="lw-mono">{s.location}</code>
      {s.endpoint && (
        <>
          {" "}
          at <code className="lw-mono">{s.endpoint}</code>
        </>
      )}
      {s.region && <>, region {s.region}</>}
      {s.plaintextHttp && <strong> (over plain HTTP)</strong>}
    </span>
  );
}

/** An instant to the millisecond the run uses, the recorded value in `dateTime`. */
export function Instant({ value }: { value: string | null | undefined }) {
  if (!value) return <NotRecorded />;
  return <time dateTime={value}>{instantText(value)}</time>;
}

/**
 * THE APPROVAL SCOPE, ALL OF IT: source, recovery point, target cluster, every
 * topic and the name it is restored under (original names marked, partition
 * subsets number by number), the verification and where the evidence goes.
 * Never a slice: the topic list scrolls inside the card and a long name wraps.
 */
export function ScopeCard({ scope }: { scope: ApprovalScope }) {
  const original = scope.topics.some((t) => t.originalName);
  const t = scope.target;
  return (
    <Card id="approval-scope">
      <CardHeader>
        <CardTitle component="h2">What you approve</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle">
          Everything below is read from the plan this request names by hash, which the console&apos;s signature covers. Approving approves all of it.
        </Content>
        <DescriptionList isCompact columnModifier={{ default: "1Col", lg: "2Col" }} className="lw-gap-top">
          <Term term="Restored to">
            <Instant value={scope.recovery.pointInTime} />
            {!scope.recovery.pointInTimeStated && " (the end of the check window)"}
          </Term>
          <Term term="Restored from">{scope.recovery.windowStart ? <Instant value={scope.recovery.windowStart} /> : "The archive's floor"}</Term>
          <Term term="Target cluster">
            <span id="scope-target-cluster" className="lw-break">
              {t.bootstrapServers.map((b, i) => (
                <span key={b}>
                  {i > 0 && ", "}
                  <code className="lw-mono">{b}</code>
                </span>
              ))}
            </span>
          </Term>
          <Term term="Authenticates as">
            {t.authMode}
            {t.authUsername && (
              <>
                {" "}
                as <code className="lw-mono">{t.authUsername}</code>
              </>
            )}
          </Term>
          <Term term="Target mode">
            {t.mode}, replication factor {t.replicationFactor}
          </Term>
          <Term term="Topic prefix">{t.topicPrefix ? <code className="lw-mono">{t.topicPrefix}</code> : "None: every topic is written under its own name"}</Term>
          <Term term="Afterwards">
            <span id="scope-afterwards">{afterwardsWords(t.mode, t.teardown)}</span>
          </Term>
          <Term term="Verification">
            {scope.verification.coverage} over <Instant value={scope.verification.windowStart} /> to <Instant value={scope.verification.windowEnd} />
          </Term>
          <Term term="Source archive">
            <Storage s={scope.source.storage} />
          </Term>
          <Term term="Backup set">
            <code className="lw-mono lw-break">{scope.source.backup}</code>
          </Term>
          <Term term="Recovery point">
            {scope.source.pointId ? (
              <span className="lw-break">
                <code className="lw-mono">{scope.source.pointId}</code>
                {scope.source.receiptSha256 && (
                  <span className="lw-subtle lw-small">
                    {" "}
                    receipt <Ident value={scope.source.receiptSha256} label="receipt sha256" />
                  </span>
                )}
              </span>
            ) : (
              <NotRecorded />
            )}
          </Term>
          {scope.recovery.timeBasis && <Term term="Time basis">{scope.recovery.timeBasis}</Term>}
          <Term term="Evidence">
            <Storage s={scope.evidence} />
          </Term>
          {scope.planName && (
            <Term term="Plan">
              <code className="lw-mono">{scope.planName}</code>
            </Term>
          )}
        </DescriptionList>
        {t.allowTransactionMarkers === true && (
          <Alert variant="warning" isInline isPlain title={TRANSACTION_MARKERS_SENTENCE} id="scope-transaction-markers" className="lw-gap-top" component="p" />
        )}
        {original && (
          <>
            <Alert variant="warning" isInline isPlain title="Original topic names" id="request-original-topics" className="lw-gap-top" component="h3">
              {TWO_PERSON_ORIGINAL_NAME_SENTENCE}
            </Alert>
            <OwnerStatement scope={scope} />
          </>
        )}
        <Content component="h3" className="lw-gap-top lw-approval-topics__head" id="scope-topic-count">
          {scope.topics.length} {scope.topics.length === 1 ? "topic" : "topics"}, every one listed
        </Content>
        <div className="lw-approval-topics" tabIndex={0} role="region" aria-labelledby="scope-topic-count">
          <Table aria-label="Every topic restored and the name it is restored under" variant="compact" gridBreakPoint="grid-md" id="scope-topics">
            <Thead>
              <Tr>
                <Th>Source topic</Th>
                <Th>Restored as</Th>
                <Th>Partitions</Th>
              </Tr>
            </Thead>
            <Tbody>
              {scope.topics.map((topic, i) => (
                <Tr key={`${topic.source}-${i}`}>
                  <Td dataLabel="Source topic" className="lw-break">
                    <code className="lw-mono">{topic.source}</code>
                  </Td>
                  <Td dataLabel="Restored as" className="lw-break">
                    <code className="lw-mono">{topic.target}</code>
                    {topic.originalName && <strong className="lw-original-name"> original name</strong>}
                  </Td>
                  <Td dataLabel="Partitions">{topic.partitions ? topic.partitions.join(", ") : "Every partition"}</Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        </div>
      </CardBody>
    </Card>
  );
}

/** The requester's owner statement, as the plan signs it. */
function OwnerStatement({ scope }: { scope: ApprovalScope }) {
  const statement = scope.target.ownerStatement;
  const kind = ownerCase(statement);
  const owners = statement?.owners ?? [];
  return (
    <div className="lw-inset lw-owner-statement" id="scope-owner-statement">
      <Content component="p">{OWNER_WORDS[kind]}</Content>
      {(kind === "ownerPath" || kind === "ownersWithoutPath") && owners.length > 0 && (
        <ul className="lw-plain-list">
          {owners.map((o) => (
            <li key={`${o.topic}-${o.reference}`}>
              <code className="lw-mono">{o.topic}</code> ({o.kind} <code className="lw-mono">{o.reference}</code>)
            </li>
          ))}
        </ul>
      )}
      {kind === "ownerPath" && owners.length === 0 && <Content component="p">No owner is listed.</Content>}
    </div>
  );
}
