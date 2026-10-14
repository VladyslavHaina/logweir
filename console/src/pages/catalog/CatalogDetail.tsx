// One recovery catalog: what its last sync saw, whether its view is still
// usable, every point it lists with its two verdicts kept apart, and who signed
// them. A refused sub-read is shown in its own card, never as an empty table:
// an empty table is what an empty catalog looks like.
import { quote } from "../restores/plan";
import { useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import {
  Alert,
  Breadcrumb,
  BreadcrumbItem,
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Content,
  DescriptionList,
  Grid,
  GridItem,
  PageSection,
  Switch,
  Tooltip,
} from "@patternfly/react-core";
import { ExpandableRowContent, Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import { useCatalog, useCatalogPoints, useCatalogSigners, type Catalog, type PointPage, type PointView, type SignerPage } from "../../api/backups";
import { PageHeader } from "../../components/PageHeader";
import { ErrorState, LoadingRows } from "../../components/States";
import { StatusLabel } from "../../components/Status";
import { When, count, exactUtc } from "../../components/When";
import { Ident } from "../../components/Ident";
import { Commands, MessageValue, Term, Value, useFlags } from "../backups/parts";
import { RESTORE_NEEDS_ROLE } from "../backups/verdict";
import "../backups/evidence.css";
import { ReadyStatus, whereFrom } from "./CatalogList";
import { Prose } from "../../components/Prose";
import {
  BACKUP_VERDICTS_INCOMPLETE_SENTENCE,
  CATALOG_WINDOW_SENTENCE,
  FINGERPRINT_COMMAND,
  MORE_POINTS_SENTENCE,
  NO_ONE_CLICK_TRUST_SENTENCE,
  NO_POINT_SENTENCE,
  PARTIAL_POINTS_SENTENCE,
  POINT_BINDING_REDACTED_SENTENCE,
  SCHEMA_DEPENDENCY_NOTE,
  SCHEMA_REGISTRY_NOT_CAPTURED,
  TWO_AXES_SENTENCE,
  VIEW_EXPIRED_SENTENCE,
  bestLocation,
  catalogPointOffer,
  consumerPositionsWords,
  isRedacted,
  schemaDependentTopics,
  truncatedWords,
  viewIsUsable,
  catalogSentence,
  duplicateOf,
  viewIsWindow,
} from "./model";

const COUNTS: [keyof Catalog["counts"], string][] = [
  ["total", "Total"],
  ["available", "Available"],
  ["missing", "Missing"],
  ["unreadable", "Unreadable"],
  ["unverified", "Unverified"],
  ["untrustedSigner", "Untrusted signer"],
  ["invalid", "Invalid"],
  ["conflict", "Conflict"],
  ["deleted", "Deleted"],
  ["unsupportedFormat", "Unsupported format"],
];

function ViewCard({ c }: { c: Catalog }) {
  const usable = viewIsUsable(c);
  const duplicate = duplicateOf(c);
  const days = (c.histogram ?? []).slice(-14);
  const most = Math.max(1, ...days.map((d) => d.points));
  return (
    <Card isFullHeight>
      <CardHeader>
        <CardTitle component="h2">The view</CardTitle>
      </CardHeader>
      <CardBody className="lw-ev-stack">
        {duplicate ? (
          <Alert isInline variant="warning" title="This catalog duplicates another one and never syncs">
            <Prose text={duplicate} />
          </Alert>
        ) : (
          !usable && <Alert isInline variant="warning" title="The view is not usable" children={VIEW_EXPIRED_SENTENCE} />
        )}
        <DescriptionList isCompact orientation={{ sm: "horizontal" }} className="lw-dl">
          <Term term="Synced">
            <When value={c.syncedAt} exact />
          </Term>
          <Term term="View expires">
            <When value={c.viewExpiresAt} exact />
          </Term>
          <Term term="Points in view">
            {count(c.viewPoints)} of at most {count(c.viewLimit)}
          </Term>
          <Term term="Window">{truncatedWords(c)}</Term>
          <Term term="Walk">
            {c.cursor?.complete === true ? "Complete" : c.cursor?.complete === false ? "Not complete: a budgeted walk that will continue" : <Value value={null} />}
            {c.cursor?.indexShard && <span className="lw-subtle"> · index shard {c.cursor.indexShard}</span>}
          </Term>
          <Term term="Last sync">
            {c.lastSync ? (
              <>
                <When value={c.lastSync.finishedAt ?? c.lastSync.startedAt} />
                {typeof c.lastSync.exitCode === "number" && <span className="lw-subtle"> · exit {c.lastSync.exitCode}</span>}
                {c.lastSync.refusalReason && <span className="lw-reason"> {c.lastSync.refusalReason}</span>}
              </>
            ) : (
              <Value value={null} />
            )}
          </Term>
        </DescriptionList>
        <dl className="lw-ev-counts" aria-label="What the last sync found">
          {COUNTS.map(([key, label]) => (
            <div key={key}>
              <dt>{label}</dt>
              <dd>{count(c.counts[key])}</dd>
            </div>
          ))}
        </dl>
        {days.length > 0 && (
          <div>
            <Content component="h3" className="lw-small">
              Points per day
            </Content>
            <ul className="lw-plain-list lw-ev-histogram">
              {days.map((d) => (
                <li key={d.day}>
                  <span className="lw-mono">{d.day}</span>
                  <span className="lw-ev-bar" style={{ inlineSize: `${(d.points / most) * 100}%` }} aria-hidden="true" />
                  <span className="lw-num">{count(d.points)}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
        {c.truncated && viewIsWindow(c) && (
          <Content component="p" className="lw-subtle lw-small">
            {CATALOG_WINDOW_SENTENCE.replace(/`/g, "")}
          </Content>
        )}
        {(c.conditions ?? []).length > 0 && (
          <ul className="lw-plain-list lw-small">
            {(c.conditions ?? []).map((x) => (
              <li key={x.type} className="lw-break">
                <strong>{x.type}</strong> <span className="lw-reason">{x.status} {x.reason ?? ""}</span>
                {x.message ? (
                  <span className="lw-subtle">
                    : <Prose text={x.message} />
                  </span>
                ) : null}
              </li>
            ))}
          </ul>
        )}
      </CardBody>
    </Card>
  );
}

function Availability({ p }: { p: PointView }) {
  return <StatusLabel tone={p.availability === "Available" ? "success" : p.availability === "Deleted" ? "neutral" : "warning"} text={p.availability} title="availability" />;
}

function Verification({ p }: { p: PointView }) {
  const green = p.verification === "Verified" || p.verification === "VerifiedHistorical";
  return (
    <>
      <StatusLabel tone={green ? "success" : p.verification === "Invalid" || p.verification === "Revoked" ? "danger" : "warning"} text={p.verification} title="verification" />
      {p.backupVerdict && (
        <div className="lw-ev-cell-note">
          <StatusLabel tone="danger" text={`Backup verdict ${p.backupVerdict}`} title="the controller's verdict on this point's own Backup" />
        </div>
      )}
    </>
  );
}

function restoreHref(ns: string, catalog: string, pointId: string): string {
  return `#/${ns}/restores/new?catalog=${encodeURIComponent(catalog)}&point=${encodeURIComponent(pointId)}`;
}

function PointsCard({ ns, name }: { ns: string; name: string }) {
  const [onlyRestorable, setOnlyRestorable] = useState(false);
  const [open, setOpen] = useState<Record<string, boolean>>({});
  const points = useCatalogPoints(ns, name, onlyRestorable);
  const flags = useFlags(ns);
  const mayRestore = flags?.restoreCreate === true;
  const pages: PointPage[] = points.data?.pages ?? [];
  const pageFacts = { viewExpired: pages.some((p) => p.viewExpired), backupVerdictsIncomplete: pages.find((p) => p.backupVerdictsIncomplete)?.backupVerdictsIncomplete };
  const rows = pages.flatMap((p) => p.items);
  const redacted = rows.some((p) => isRedacted(p.receiptKey) || isRedacted(p.backupId));
  const schema = rows.map((p) => ({ id: p.pointId, topics: schemaDependentTopics(p) })).filter((r) => r.topics.length > 0);

  return (
    <Card>
      <CardHeader
        actions={{
          actions: (
            <Switch
              id="points-restorable"
              label="Only points a restore can use"
              isChecked={onlyRestorable}
              onChange={(_e, v) => setOnlyRestorable(v)}
            />
          ),
          hasNoOffset: true,
        }}
      >
        <CardTitle component="h2">Recovery points</CardTitle>
      </CardHeader>
      <CardBody className="lw-ev-stack">
        <Content component="p" className="lw-subtle lw-small">
          {TWO_AXES_SENTENCE}
        </Content>
        {points.isPending ? (
          <LoadingRows rows={5} label="Loading the recovery points" />
        ) : points.isError && rows.length === 0 ? (
          <ErrorState error={points.error} onRetry={() => void points.refetch()} />
        ) : (
          <>
            {pageFacts.viewExpired && <Alert isInline variant="warning" title="This view has aged out" children={VIEW_EXPIRED_SENTENCE} />}
            {pageFacts.backupVerdictsIncomplete && (
              <Alert isInline variant="danger" title={`Backup verdicts incomplete (${pageFacts.backupVerdictsIncomplete})`} children={BACKUP_VERDICTS_INCOMPLETE_SENTENCE} />
            )}
            {pages.some((p) => p.incomplete) && (
              <Alert isInline variant="warning" title={PARTIAL_POINTS_SENTENCE}>
                A point on a line this page could not read is not listed. Read the list again.
              </Alert>
            )}
            {rows.length === 0 ? (
              <Content component="p" className="lw-subtle">
                {onlyRestorable ? "No point in this view is restorable now. Turn the filter off to see every point and why." : NO_POINT_SENTENCE}
              </Content>
            ) : (
              <Table aria-label="Recovery points" variant="compact" gridBreakPoint="grid-lg">
                <Thead>
                  <Tr>
                    <Th screenReaderText="Details" />
                    <Th>Point</Th>
                    <Th modifier="fitContent">Availability</Th>
                    <Th modifier="fitContent">Verification</Th>
                    <Th>Signed by</Th>
                    <Th screenReaderText="Restore" modifier="fitContent" />
                  </Tr>
                </Thead>
                {rows.map((p, i) => {
                  const offer = catalogPointOffer(p, pageFacts);
                  const location = bestLocation(p);
                  const positions = consumerPositionsWords(p);
                  const expanded = open[p.pointId] === true;
                  const restore = offer.offer && mayRestore;
                  return (
                    <Tbody key={`${p.pointId}-${i}`} isExpanded={expanded}>
                      <Tr>
                        <Td
                          expand={{
                            rowIndex: i,
                            isExpanded: expanded,
                            onToggle: () => setOpen({ ...open, [p.pointId]: !expanded }),
                            expandId: `point-${i}`,
                          }}
                        />
                        <Td dataLabel="Point">
                          <Ident value={p.pointId} label="recovery point id" />
                          <div className="lw-subtle lw-small">
                            {/* DUI-22: recoveryPointAt is when the capture started; the records it holds end at coveredTo. */}
                            captured <When value={p.recoveryPointAt} />
                            {p.coveredTo ? <> · records up to {exactUtc(p.coveredTo)}</> : null}
                          </div>
                        </Td>
                        <Td dataLabel="Availability">
                          <Availability p={p} />
                        </Td>
                        <Td dataLabel="Verification">
                          <Verification p={p} />
                        </Td>
                        <Td dataLabel="Signed by">{p.signerKeyId ? <Ident value={p.signerKeyId} label="signer key id" /> : <Value value={null} />}</Td>
                        <Td dataLabel="Restore" modifier="fitContent">
                          {restore ? (
                            <Button component="a" href={restoreHref(ns, name, p.pointId)} variant="secondary" size="sm">
                              Restore this point
                            </Button>
                          ) : (
                            <Tooltip content={offer.offer ? RESTORE_NEEDS_ROLE : `Not offered: ${offer.reason}`}>
                              <Button variant="secondary" size="sm" isAriaDisabled>
                                Restore this point
                              </Button>
                            </Tooltip>
                          )}
                        </Td>
                      </Tr>
                      <Tr isExpanded={expanded}>
                        <Td colSpan={6}>
                          <ExpandableRowContent>
                            <DescriptionList isCompact orientation={{ sm: "horizontal" }} className="lw-dl">
                              <Term term="Restore">{restore ? "Offered" : offer.offer ? RESTORE_NEEDS_ROLE : `Not offered: ${offer.reason}`}</Term>
                              <Term term="Remedy">
                                <MessageValue value={p.remedy} />
                              </Term>
                              <Term term="Location">
                                {location ? `${location.locationId} (${location.availability})` : <Value value={null} />}
                                {(p.locations ?? []).length > 1 && <span className="lw-subtle"> · seen at {(p.locations ?? []).length} locations</span>}
                              </Term>
                              <Term term="Covers">
                                {p.coveredFrom && p.coveredTo ? (
                                  <>
                                    <When value={p.coveredFrom} exact /> to <When value={p.coveredTo} exact />
                                  </>
                                ) : (
                                  <Value value={null} />
                                )}
                              </Term>
                              <Term term="Backup set">
                                <span className="lw-ev-key">{p.backupId}</span>
                              </Term>
                              <Term term="Receipt">
                                <span className="lw-ev-key">{p.receiptKey}</span>
                                <div>
                                  <Ident value={p.receiptSha256} label="receipt sha256" />
                                </div>
                              </Term>
                              <Term term="Format">
                                <Value value={p.formatVersion} />
                              </Term>
                              <Term term="Consumer positions">
                                <Value value={positions} />
                              </Term>
                            </DescriptionList>
                          </ExpandableRowContent>
                        </Td>
                      </Tr>
                    </Tbody>
                  );
                })}
              </Table>
            )}
            {points.hasNextPage && (
              <div>
                <Content component="p" className="lw-subtle lw-small">
                  {MORE_POINTS_SENTENCE}
                </Content>
                <Button variant="secondary" onClick={() => void points.fetchNextPage()} isLoading={points.isFetchingNextPage} isDisabled={points.isFetchingNextPage}>
                  Read the next 200 points
                </Button>
              </div>
            )}
            {points.isError && rows.length > 0 && <ErrorState error={points.error} onRetry={() => void points.fetchNextPage()} />}
            {redacted && <Alert isInline variant="warning" title="Some plan bindings are redacted" children={POINT_BINDING_REDACTED_SENTENCE} />}
            <Content component="p" className="lw-subtle lw-small">
              Restoring a point binds the plan to its id, receipt key and digest, and manifest digest; the runner checks
              that binding again before it reads anything, so a mismatch is a refusal, never a restore of something else.
            </Content>
            {schema.length > 0 && (
              <div>
                <Content component="h3" className="lw-small">
                  Schema-dependent topics
                </Content>
                <Alert isInline isPlain variant="warning" title={SCHEMA_REGISTRY_NOT_CAPTURED} />
                <ul className="lw-plain-list lw-small">
                  {schema.map((r) => (
                    <li key={r.id} className="lw-break">
                      <span className="lw-mono">{r.id}</span>: {r.topics.join(", ")}
                    </li>
                  ))}
                </ul>
                <Content component="p" className="lw-subtle lw-small">
                  {SCHEMA_DEPENDENCY_NOTE}
                </Content>
              </div>
            )}
          </>
        )}
      </CardBody>
    </Card>
  );
}

/** The document a trust administrator applies for an untrusted signer. Rendered, never submitted. */
export function trustSnippet(keyIds: string[]): string[] {
  const lines = [
    "# Add the key to the TrustPolicy that governs this namespace.",
    "# `state: Retired` verifies what it already signed and authorises nothing new.",
    "apiVersion: logweir.dev/v1alpha1",
    "kind: TrustPolicy",
    "metadata:",
    "  name: <the policy that names this namespace>",
    "spec:",
    "  keys:",
  ];
  for (const id of keyIds) {
    lines.push(
      // The server's key id enters the YAML only through the plan builder's scalar writer.
      `    - keyId: ${quote(id)}`,
      "      spkiPem: |",
      "        <the PUBLIC half, after you compared its fingerprint out of band>",
      "      algorithm: p256",
      "      usages: [EvidenceSigning]",
      '      principal: {id: "install:<who holds it>"}',
      "      state: Retired",
    );
  }
  lines.push("", "kubectl --context <ctx> apply -f trustpolicy.yml");
  return lines;
}

function SignersCard({ signers }: { signers: SignerPage }) {
  const list = signers.items;
  const untrusted = list.filter((s) => s.trusted !== true);
  return (
    <>
      {list.length === 0 ? (
        <Content component="p" className="lw-subtle">
          This catalog recorded no signer. A view with no signer is a view that verified nothing, which is not the same
          as an archive nobody signed.
        </Content>
      ) : (
        <Table aria-label="Who signed these points" variant="compact" gridBreakPoint="grid-md">
          <Thead>
            <Tr>
              <Th>Key</Th>
              <Th>Principal hint</Th>
              <Th>Points</Th>
              <Th>Trust</Th>
            </Tr>
          </Thead>
          <Tbody>
            {list.map((s) => (
              <Tr key={s.keyId}>
                <Td dataLabel="Key">
                  <Ident value={s.keyId} label="key id" />
                </Td>
                <Td dataLabel="Principal hint">
                  {s.principalHint ? (
                    <>
                      {s.principalHint} <span className="lw-subtle lw-small">(from the record, never authority)</span>
                    </>
                  ) : (
                    <Value value={null} />
                  )}
                </Td>
                <Td dataLabel="Points" className="lw-num">
                  {count(s.points)}
                </Td>
                <Td dataLabel="Trust">
                  {s.trusted === true ? (
                    <StatusLabel tone="success" text="Listed in the TrustPolicy" />
                  ) : s.trusted === false ? (
                    <StatusLabel tone="danger" text="Not listed in the TrustPolicy" />
                  ) : (
                    <StatusLabel tone="neutral" text="Could not tell" title="the catalog could not tell, which is not false and certainly not true" />
                  )}
                </Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      )}
      {typeof signers.untrustedPoints === "number" && signers.untrustedPoints > 0 && (
        <Content component="p" className="lw-small">
          {count(signers.untrustedPoints)} {signers.untrustedPoints === 1 ? "point carries" : "points carry"} a signature the bound policy does not accept.
        </Content>
      )}
      {untrusted.length > 0 && (
        <>
          <Alert isInline variant="warning" title="No one-click trust" children={NO_ONE_CLICK_TRUST_SENTENCE} />
          <Content component="p" className="lw-subtle lw-small">
            Run this against the public half the key's holder gives you, and compare the digest with the key above.
          </Content>
          {/* The console's own command, never the server's text: a command an operator copies
              carries no string the page has not shaped (the server's names the same digest). */}
          <Commands lines={[FINGERPRINT_COMMAND]} label="the fingerprint command" />
          <Content component="h3" className="lw-small">
            Trusting this key is a cluster-admin step
          </Content>
          <Content component="p" className="lw-subtle lw-small">
            This page shows the document and does not apply it: TrustPolicy objects are cluster-scoped, and the product
            API serves no trust write.
          </Content>
          <Commands lines={trustSnippet(untrusted.map((s) => s.keyId))} label="the document" />
        </>
      )}
    </>
  );
}

export function CatalogDetail() {
  const { ns, name } = useParams({ from: "/$ns/catalog/$name" });
  const catalog = useCatalog(ns, name);
  const signers = useCatalogSigners(ns, name);
  const breadcrumb = (
    <Breadcrumb className="lw-breadcrumb">
      <BreadcrumbItem render={() => <Link to="/$ns/catalog" params={{ ns }}>Recovery catalog</Link>} />
      <BreadcrumbItem isActive>{name}</BreadcrumbItem>
    </Breadcrumb>
  );
  if (catalog.isPending || catalog.isError) {
    return (
      <>
        <PageHeader title={name} breadcrumb={breadcrumb} />
        <PageSection>
          {catalog.isPending ? <LoadingRows rows={6} label="Loading the catalog" /> : <ErrorState error={catalog.error} onRetry={() => void catalog.refetch()} />}
        </PageSection>
      </>
    );
  }
  const c = catalog.data;
  return (
    <>
      <PageHeader
        title={c.name}
        breadcrumb={breadcrumb}
        status={<ReadyStatus c={c} />}
        description={catalogSentence(c)}
      />
      <PageSection>
        <Grid hasGutter>
          <GridItem lg={5}>
            <Card isFullHeight>
              <CardHeader>
                <CardTitle component="h2">The catalog</CardTitle>
              </CardHeader>
              <CardBody>
                <DescriptionList isCompact orientation={{ sm: "horizontal" }} className="lw-dl">
                  <Term term="Destination">
                    <span className="lw-break">{whereFrom(c)}</span>
                    {c.legacyArchive?.credentialRef && <div className="lw-subtle lw-small">credential Secret {c.legacyArchive.credentialRef.name}</div>}
                  </Term>
                  <Term term="Sync mode">{c.mode}</Term>
                  <Term term="Sync interval">{c.intervalSeconds === 0 ? "Manual only" : `${count(c.intervalSeconds)} s`}</Term>
                  <Term term="Deep check">{c.deepCheck}</Term>
                  <Term term="View limit">{count(c.viewLimit)} newest points</Term>
                  <Term term="Created">
                    <When value={c.createdAt} exact />
                  </Term>
                  <Term term="Generation">
                    {c.generation ?? "Not recorded"}
                    {typeof c.observedGeneration === "number" && <span className="lw-subtle"> · view built from g{c.observedGeneration}</span>}
                  </Term>
                </DescriptionList>
              </CardBody>
            </Card>
          </GridItem>
          <GridItem lg={7}>
            <ViewCard c={c} />
          </GridItem>
          <GridItem>
            <PointsCard ns={ns} name={c.name} />
          </GridItem>
          <GridItem>
            <Card>
              <CardHeader>
                <CardTitle component="h2">Who signed these points</CardTitle>
              </CardHeader>
              <CardBody className="lw-ev-stack">
                {signers.isPending ? (
                  <LoadingRows rows={2} label="Loading the signers" />
                ) : signers.isError ? (
                  <ErrorState error={signers.error} onRetry={() => void signers.refetch()} />
                ) : (
                  <SignersCard signers={signers.data} />
                )}
              </CardBody>
            </Card>
          </GridItem>
        </Grid>
      </PageSection>
    </>
  );
}

