// The five steps of the restore wizard: the recovery point, topics and time, the
// target, the readiness check, and review and create. Each step reads the one form
// (react-hook-form) and the facts the page resolved; none of them sends anything.
import { useMemo, useState, type ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import { Controller, useWatch, type Control } from "react-hook-form";
import {
  Alert,
  Button,
  Checkbox,
  Content,
  ExpandableSection,
  Flex,
  Form,
  FormGroup,
  FormSelect,
  FormSelectOption,
  Radio,
  SearchInput,
  Switch,
  TextArea,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import DownloadIcon from "@patternfly/react-icons/dist/esm/icons/download-icon";
import { ReachStatus, StatusLabel } from "../../components/Status";
import { LoadingRows } from "../../components/States";
import { count, exactUtc } from "../../components/When";
import { Ident } from "../../components/Ident";
import { useNarrow } from "../../components/CompactList";
import type { ApprovalPolicy, Backup, CatalogView, Connection, PointView, Preflight } from "../../api/restores";
import { Coded, Commands, Facts, FieldHelp, Instant, shown } from "./parts";
import { matchesQuery, pointHaystack, restorableWindow, rfc3339, type RecoveryPoint } from "./points";
import type { PlanFields, PreparedPlan } from "./plan";
import { CloneLifetime } from "../cutover/CloneLifetime";
import { TopicConfigurationChoice } from "../cutover/TopicConfiguration";
import {
  approvalRoute,
  completeChosen,
  endInstant,
  frozenTopicsOf,
  isBlockingRow,
  isDraftApprovalRow,
  originalNameChosen,
  readyButForDraftApproval,
  replicationBasisText,
  replicationMayDiffer,
  staleReasonLine,
  topicMapping,
  typedConfirmationRequired,
  type BrokerFact,
  type ReplicationChoice,
  type WizardValues,
} from "./model";
import {
  CATALOG_BINDING_SENTENCE,
  CATALOG_TOPICS_SENTENCE,
  COMPLETE_COVERAGE_COST,
  COVERAGE_BOUND_HELP,
  ORIGINAL_NAME_COVERAGE_SENTENCE,
  ORIGINAL_NAME_NO_OWNER_STATEMENT,
  ORIGINAL_NAME_SENTENCE,
  ORIGINAL_NAME_TYPED_SENTENCE,
  POINT_UNBOUND_SENTENCE,
  READINESS_CAVEAT_SENTENCE,
  READINESS_NOT_RUN_WARNING,
  REPLICATION_DIFFERS_NOTE,
  REPLICATION_UNKNOWN_WARNING,
  RESTORE_SEMANTICS_SENTENCE,
  RETRY_IDENTITY_SENTENCE,
  RETRY_OLD_RUN_SENTENCE,
  SCRATCH_MARKER_WARNING,
  SCRATCH_SENTENCE,
  TIME_BASIS_NOTE,
  TIME_BASIS_OPTED_WARNING,
  modeMeaning,
} from "./words";
import { ErrorText } from "../../components/Prose";

type Errors = Partial<Record<string, { message?: string }>>;
const err = (errors: Errors, name: string) => errors[name]?.message;

// ------------------------------------------------------------- 1. the point

export interface Offer {
  catalog: CatalogView;
  entry: PointView;
}

export function pointKeyOf(b: Backup): string {
  return `backup/${b.uid}`;
}
export function offerKeyOf(o: Offer): string {
  return `catalog/${o.catalog.name}/${o.entry.pointId}`;
}

const SHOWN = 8;

export function PointStep({
  control,
  points,
  offers,
  offerNotes,
  offersPending,
  resolution,
  retryOf,
  refusal,
  errors,
}: {
  control: Control<WizardValues>;
  points: Backup[];
  offers: Offer[];
  offerNotes: string[];
  offersPending: boolean;
  resolution: React.ReactNode;
  retryOf: string;
  refusal: string;
  errors: Errors;
}) {
  const narrow = useNarrow();
  const [query, setQuery] = useState("");
  const [limit, setLimit] = useState(SHOWN);
  const matching = useMemo(() => points.filter((b) => matchesQuery(pointHaystack(b), query)), [points, query]);
  const newest = points[0]?.uid;
  const end = (b: Backup) => {
    const w = restorableWindow(b.windowCovered);
    return w ? rfc3339(w.toMs) : null;
  };
  return (
    <>
      <Title headingLevel="h2" size="xl">Which recovery point?</Title>
      {retryOf && (
        <Alert variant="info" isInline title={`Retrying ${retryOf} to a fresh target`} className="lw-gap-top lw-gap-bottom" id="retry-banner">
          <p>This is a NEW restore. {RETRY_IDENTITY_SENTENCE}</p>
          <p>{RETRY_OLD_RUN_SENTENCE}</p>
        </Alert>
      )}
      {refusal && (
        <Alert variant="danger" isInline title="The point this link names cannot be restored" className="lw-gap-top lw-gap-bottom" id="point-refusal">
          {refusal}
        </Alert>
      )}
      <Content component="p" className="lw-subtle">
        Each point is a succeeded backup with its covered window. The newest is first; the plan restores up to the end of the
        window unless you choose an earlier moment.
      </Content>
      <SearchInput
        className="lw-point-search"
        placeholder="Filter by name, schedule, set, source or topic"
        value={query}
        onChange={(_e, v) => {
          setQuery(v);
          setLimit(SHOWN);
        }}
        onClear={() => setQuery("")}
        aria-label="Filter recovery points"
      />
      <Controller
        control={control}
        name="pointKey"
        render={({ field }) =>
          matching.length === 0 ? (
            <Content component="p" className="lw-subtle lw-gap-top">
              {points.length === 0 ? "No backup in this namespace is a recovery point yet." : "No recovery point matches that filter."}
            </Content>
          ) : narrow ? (
            <div className="lw-list lw-list--rows lw-gap-top" role="radiogroup" aria-label="Recovery points">
              {matching.slice(0, limit).map((b, i) => (
                <div key={b.uid} className="lw-list__row">
                  <div className="lw-list__main">
                    <Radio
                      id={`point-${i}`}
                      name="point"
                      isChecked={field.value === pointKeyOf(b)}
                      onChange={() => field.onChange(pointKeyOf(b))}
                      label={<strong>{end(b) ? exactUtc(end(b)!) : b.name}</strong>}
                      description={
                        <>
                          {b.name}
                          {b.uid === newest ? " · newest" : ""} · {count(b.records)} records
                        </>
                      }
                    />
                  </div>
                  <div className="lw-list__end">
                    <PointEvidence b={b} />
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <Table aria-label="Recovery points" variant="compact" className="lw-gap-top">
              <Thead>
                <Tr>
                  <Th screenReaderText="Choose" />
                  <Th>Restores up to</Th>
                  <Th>Topics</Th>
                  <Th className="lw-num">Records</Th>
                  <Th>Evidence</Th>
                </Tr>
              </Thead>
              <Tbody>
                {matching.slice(0, limit).map((b, i) => (
                  <Tr key={b.uid} isClickable isRowSelected={field.value === pointKeyOf(b)} onRowClick={() => field.onChange(pointKeyOf(b))}>
                    <Td select={{ rowIndex: i, variant: "radio", isSelected: field.value === pointKeyOf(b), onSelect: () => field.onChange(pointKeyOf(b)) }} />
                    <Td dataLabel="Restores up to">
                      <strong>{end(b) ? exactUtc(end(b)!) : <span className="lw-subtle">Not recorded</span>}</strong>
                      {b.uid === newest && <span className="lw-subtle"> · newest</span>}
                      <div className="lw-subtle lw-small lw-break">
                        {b.name} · {b.sourceRef.name} · <Instant value={b.createdAt} />
                      </div>
                    </Td>
                    <Td dataLabel="Topics">{b.topics.join(", ")}</Td>
                    <Td dataLabel="Records" className="lw-num">
                      {count(b.records)}
                    </Td>
                    <Td dataLabel="Evidence">
                      <PointEvidence b={b} />
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )
        }
      />
      {matching.length > limit && (
        <Button variant="link" isInline className="lw-gap-top" onClick={() => setLimit(limit + SHOWN)}>
          Show {Math.min(SHOWN, matching.length - limit)} more of {matching.length}
        </Button>
      )}
      <FieldHelp id="point" error={err(errors, "pointKey")} />
      <Controller
        control={control}
        name="pointKey"
        render={({ field }) => (
          offersPending || offers.length > 0 || offerNotes.length > 0 ? (
          <ExpandableSection toggleText={`From recovery catalogs${offersPending ? "" : ` (${offers.length})`}`} className="lw-gap-top" isIndented>
            <Content component="p" className="lw-subtle">
              Points a connected archive holds, whether or not a Backup in this namespace produced them: a restore from one is bound to
              the point itself.
            </Content>
            {offersPending ? (
              <LoadingRows rows={2} label="Reading the recovery catalogs" />
            ) : offers.length === 0 ? (
              <Content component="p" className="lw-subtle">No catalog offers a point.</Content>
            ) : (
              <div className="lw-list lw-list--rows" role="radiogroup" aria-label="Catalog points">
                {offers.slice(0, 50).map((o, i) => (
                  <div key={offerKeyOf(o)} className="lw-list__row">
                    <div className="lw-list__main">
                      <Radio
                        id={`offer-${i}`}
                        name="point"
                        isChecked={field.value === offerKeyOf(o)}
                        onChange={() => field.onChange(offerKeyOf(o))}
                        label={<strong>{o.entry.coveredTo ? exactUtc(o.entry.coveredTo) : o.entry.pointId}</strong>}
                        description={
                          <>
                            catalog {o.catalog.name} · set {o.entry.backupId}
                          </>
                        }
                      />
                    </div>
                    <div className="lw-list__end">
                      <StatusLabel tone="success" text="Verified" title={`${o.entry.verification}, ${o.entry.availability}`} />
                    </div>
                  </div>
                ))}
              </div>
            )}
            {offerNotes.map((n) => (
              <Content key={n} component="p" className="lw-subtle lw-small">
                {n}
              </Content>
            ))}
          </ExpandableSection>
          ) : <></>
        )}
      />
      {resolution}
    </>
  );
}

/** A point's evidence by the backup badge rule: verified only when the controller verified it. */
function PointEvidence({ b }: { b: Backup }) {
  return b.operation.verifiedSuccess && b.operation.verificationState === "valid" ? (
    <StatusLabel tone="success" text="Verified" />
  ) : (
    <StatusLabel tone="warning" text="Not verified" title={`verification ${b.operation.verificationState}`} />
  );
}

/** What the chosen point binds the plan to, and what refuses it. */
export function Resolution({
  point,
  destinationProblem,
  unboundReason,
  pending,
  error,
}: {
  point: RecoveryPoint | null;
  destinationProblem: string | null;
  unboundReason: string | null;
  pending: boolean;
  error: unknown;
}) {
  if (pending) return <LoadingRows rows={2} label="Reading the chosen point" />;
  if (error) {
    return (
      <Alert variant="danger" isInline title="The chosen point could not be read" className="lw-gap-top">
        <ErrorText error={error} />
      </Alert>
    );
  }
  if (!point) return null;
  return (
    <div className="lw-gap-top" id="point-resolution">
      {destinationProblem && (
        <Alert variant="danger" isInline title="This point cannot be restored from its saved destination" id="destination-problem">
          {destinationProblem}. A different destination is a different store; nothing was substituted.
        </Alert>
      )}
      <Facts
        items={[
          { term: "Backup set", value: <Ident value={point.backupSetRef} label="backup set id" /> },
          { term: "Window", value: `${exactUtc(point.window.fromMs)} to ${exactUtc(point.window.toMs)}` },
          { term: "Archive", value: point.destinationName ? `saved destination ${point.destinationName}` : point.archiveUrl || "Not recorded" },
          point.catalog
            ? { term: "Bound to", value: <>point <Ident value={point.catalog.entry.pointId} label="point id" /> in catalog {point.catalog.name}</> }
            : { term: "Bound to", value: <span className="lw-subtle">the backup set only{unboundReason ? <>: <Coded text={unboundReason} /></> : ""}</span> },
        ]}
      />
      <Content component="p" className="lw-subtle lw-small">
        {point.catalog ? CATALOG_BINDING_SENTENCE : POINT_UNBOUND_SENTENCE}
      </Content>
    </div>
  );
}

// ------------------------------------------------------- 2. topics and time

export function WhatStep({ control, point, errors }: { control: Control<WizardValues>; point: RecoveryPoint; errors: Errors }) {
  const v = useWatch({ control }) as WizardValues;
  const w = restorableWindow(point.window);
  const listed = point.catalog?.entry.topics ?? [];
  const layout = new Map(listed.map((t) => [t.name, t]));
  const required = originalNameChosen(v);
  return (
    <Form onSubmit={(e) => e.preventDefault()}>
      <Title headingLevel="h2" size="xl">What to restore</Title>
      {point.topics === null ? (
        <Controller
          control={control}
          name="topicsText"
          render={({ field }) => (
            <FormGroup label="Topics" fieldId="topics-text" isRequired>
              <TextInput id="topics-text" value={field.value} onChange={(_e, t) => field.onChange(t)} validated={err(errors, "topicsText") ? "error" : "default"} />
              <FieldHelp id="topics-text" error={err(errors, "topicsText")}>{CATALOG_TOPICS_SENTENCE}</FieldHelp>
            </FormGroup>
          )}
        />
      ) : (
        <Controller
          control={control}
          name="topics"
          render={({ field }) => (
            <FormGroup role="group" isStack label="Topics" fieldId="topics">
              <Flex gap={{ default: "gapMd" }}>
                <Button variant="link" isInline onClick={() => field.onChange(point.topics!.slice())}>Select all</Button>
                <Button variant="link" isInline onClick={() => field.onChange([])}>Select none</Button>
              </Flex>
              {point.topics!.map((t) => {
                const facts = layout.get(t);
                return (
                  <Checkbox
                    key={t}
                    id={`topic-${t}`}
                    label={t}
                    description={
                      facts && (facts.partitions || facts.replicationFactor)
                        ? `${facts.partitions ? `${facts.partitions} ${facts.partitions === 1 ? "partition" : "partitions"}` : "partitions not recorded"}, replication factor ${facts.replicationFactor ?? "not recorded"} at the source`
                        : undefined
                    }
                    isChecked={field.value.includes(t)}
                    onChange={(_e, on) => field.onChange(on ? [...field.value, t] : field.value.filter((x) => x !== t))}
                  />
                );
              })}
              <FieldHelp id="topics" error={err(errors, "topics")}>
                The plan lists them in the point's own order, so the order you tick them in never changes its hash.
              </FieldHelp>
            </FormGroup>
          )}
        />
      )}
      <Controller
        control={control}
        name="timeMode"
        render={({ field }) => (
          <FormGroup role="radiogroup" isStack label="Up to which moment" fieldId="time-mode">
            <Radio
              id="time-end"
              name="time-mode"
              label={`The end of this point (${w ? exactUtc(endInstant(point)) : "not recorded"})`}
              description="Everything the backup captured."
              isChecked={field.value === "end"}
              onChange={() => field.onChange("end")}
            />
            <Radio
              id="time-earlier"
              name="time-mode"
              label="An earlier moment"
              description="For example just before a bad deploy wrote bad records. Records after it are left out."
              isChecked={field.value === "earlier"}
              onChange={() => field.onChange("earlier")}
            />
          </FormGroup>
        )}
      />
      {v.timeMode === "earlier" && (
        <Controller
          control={control}
          name="pointInTime"
          render={({ field }) => (
            <FormGroup label="Instant (UTC)" fieldId="point-in-time" isRequired>
              <TextInput id="point-in-time" value={field.value} onChange={(_e, t) => field.onChange(t)} validated={err(errors, "pointInTime") ? "error" : "default"} className="lw-instant-input" />
              <FieldHelp id="point-in-time" error={err(errors, "pointInTime")}>
                {w ? `RFC 3339, between ${rfc3339(w.fromMs)} and ${rfc3339(w.toMs)}, the window this point covers.` : "This point discloses no window."}
              </FieldHelp>
            </FormGroup>
          )}
        />
      )}
      <Controller
        control={control}
        name="timeBasis"
        render={({ field }) => (
          <Checkbox id="time-basis" label="Select by producer time" description={TIME_BASIS_NOTE} isChecked={field.value} onChange={(_e, on) => field.onChange(on)} />
        )}
      />
      <Controller
        control={control}
        name="coverage"
        render={({ field }) => (
          <FormGroup role="radiogroup" isStack label="How to verify" fieldId="coverage">
            <Radio
              id="coverage-complete"
              name="coverage"
              label="Complete: compare every restored record (recommended)"
              description="Needed to switch consumer groups over: the captured positions are mapped while every record is read."
              isChecked={required || field.value === "complete"}
              onChange={() => field.onChange("complete")}
            />
            <Radio
              id="coverage-sampled"
              name="coverage"
              label="Sampled: compare 25 records per partition and the manifest's counts"
              description="Faster for a large drill. Consumer positions are not mapped."
              isChecked={!required && field.value === "sampled"}
              isDisabled={required}
              onChange={() => field.onChange("sampled")}
            />
            <FieldHelp id="coverage">{required ? ORIGINAL_NAME_COVERAGE_SENTENCE : COMPLETE_COVERAGE_COST}</FieldHelp>
          </FormGroup>
        )}
      />
      {completeChosen(v) && (
        <Controller
          control={control}
          name="completeMaxRecords"
          render={({ field }) => (
            <FormGroup label="Record bound (optional)" fieldId="complete-max-records">
              <TextInput id="complete-max-records" inputMode="numeric" value={field.value} onChange={(_e, t) => field.onChange(t)} validated={err(errors, "completeMaxRecords") ? "error" : "default"} className="lw-short-input" />
              <FieldHelp id="complete-max-records" error={err(errors, "completeMaxRecords")}>{COVERAGE_BOUND_HELP}</FieldHelp>
            </FormGroup>
          )}
        />
      )}
    </Form>
  );
}

// ------------------------------------------------------------- 3. the target

export function TargetStep({
  control,
  point,
  targets,
  others,
  connection,
  fields,
  replication,
  brokers,
  destinations,
  errors,
  ns,
  ownersAsked = false,
}: {
  control: Control<WizardValues>;
  point: RecoveryPoint;
  targets: Connection[];
  others: number;
  connection: Connection | null;
  fields: PlanFields;
  replication: ReplicationChoice;
  brokers: BrokerFact;
  destinations: { name: string; uid: string }[];
  errors: Errors;
  ns: string;
  /** The readiness check's preview refused TopicOwnerNotChecked: ask for the owner statement. */
  ownersAsked?: boolean;
}) {
  const v = useWatch({ control }) as WizardValues;
  const original = originalNameChosen(v);
  const mapping = topicMapping(fields);
  return (
    <Form onSubmit={(e) => e.preventDefault()}>
      <Title headingLevel="h2" size="xl">Where to restore</Title>
      <Controller
        control={control}
        name="target"
        render={({ field }) => (
          <FormGroup role="radiogroup" isStack label="Target connection" fieldId="target">
            {targets.length === 0 && (
              <Alert variant="warning" isInline title="No connection has the target role">
                Add a connection with the target role first; a source cluster is never written to from here.{" "}
                <Link to="/$ns/$section" params={{ ns, section: "connections" }}>Open Connections</Link>
              </Alert>
            )}
            {targets.map((c) => (
              <Radio
                key={c.uid}
                id={`target-${c.name}`}
                name="target"
                isChecked={field.value === c.name}
                onChange={() => field.onChange(c.name)}
                label={
                  <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }}>
                    <span>{c.name}</span>
                    <ReachStatus state={c.reachability.state} />
                  </Flex>
                }
                description={`${c.bootstrapServers.length} ${c.bootstrapServers.length === 1 ? "broker address" : "broker addresses"} · ${c.bootstrapServers[0] ?? ""}`}
              />
            ))}
            <FieldHelp id="target" error={err(errors, "target")}>
              Only connections with the target role are listed{others > 0 ? ` (${others} other ${others === 1 ? "connection is" : "connections are"} not)` : ""}. The probe is
              the controller's last dial, context and not a verdict: the readiness check reaches the cluster for this plan.
            </FieldHelp>
          </FormGroup>
        )}
      />
      <Controller
        control={control}
        name="naming"
        render={({ field }) => (
          <FormGroup role="radiogroup" isStack label="Topic names" fieldId="naming">
            <Radio
              id="naming-prefix"
              name="naming"
              label="New topics with a prefix (recommended)"
              description="Nothing that exists is touched. Good for drills and for inspecting data."
              isChecked={field.value === "prefix" || v.scratch}
              onChange={() => field.onChange("prefix")}
            />
            <Radio
              id="naming-original"
              name="naming"
              label="The original names"
              description="For a real recovery into a cluster where these topics do not exist. You state that no owner manages them."
              isChecked={field.value === "original" && !v.scratch}
              isDisabled={v.scratch}
              onChange={() => field.onChange("original")}
            />
          </FormGroup>
        )}
      />
      {!original && (
        <Controller
          control={control}
          name="prefix"
          render={({ field }) => (
            <FormGroup label="Prefix" fieldId="prefix">
              <TextInput id="prefix" value={field.value} placeholder={fields.target.topicMappingPrefix} onChange={(_e, t) => field.onChange(t)} validated={err(errors, "prefix") ? "error" : "default"} className="lw-prefix-input" />
              <FieldHelp id="prefix" error={err(errors, "prefix")}>
                Empty it to go back to the default for the point in time.
              </FieldHelp>
            </FormGroup>
          )}
        />
      )}
      {original && (
        <div className="lw-restore-inset">
          <Content component="p">{ORIGINAL_NAME_SENTENCE}</Content>
          <Controller
            control={control}
            name="noOwner"
            render={({ field }) => (
              <Checkbox id="no-owner" label={ORIGINAL_NAME_NO_OWNER_STATEMENT} isChecked={field.value} onChange={(_e, on) => field.onChange(on)} isRequired />
            )}
          />
          <FieldHelp id="no-owner" error={err(errors, "noOwner")}>
            Signed into the plan as owners: []. The runner refuses a plan without it (OriginalNameOwnerNotChecked).
          </FieldHelp>
        </div>
      )}
      <FormGroup label="Mapping" fieldId="mapping" role="group">
        <ul className="lw-plain-list lw-mapping" aria-label="Topic mapping" id="mapping">
          {mapping.map((row) => (
            <li key={row.source}>
              <span className="lw-mono">{row.source}</span> → <span className="lw-mono">{row.target}</span>
            </li>
          ))}
        </ul>
        <FieldHelp id="mapping">
          {original ? "Each topic is restored under its own name; the scratch prefix is kept for the runner's own probe topic." : "A target name is the prefix and the source name, and nothing else."}
        </FieldHelp>
      </FormGroup>
      <FormGroup label="Rehearsal" fieldId="scratch" role="group">
        <Controller
          control={control}
          name="scratch"
          render={({ field }) => (
            <Switch id="scratch" label="Delete the restored topics when the run ends (scratch mode)" isChecked={field.value} onChange={(_e, on) => field.onChange(on)} />
          )}
        />
        <FieldHelp id="scratch">{v.scratch ? SCRATCH_SENTENCE : modeMeaning("newTopic", original)}</FieldHelp>
        {v.scratch && connection && !connection.markerTopic && (
          <Alert variant="warning" isInline isPlain title={SCRATCH_MARKER_WARNING} />
        )}
        {v.scratch && (
          <Controller
            control={control}
            name="cloneTtl"
            render={({ field }) => (
              <CloneLifetime value={field.value} onChange={field.onChange} target={connection?.name ?? "the target"} error={err(errors, "cloneTtl")} />
            )}
          />
        )}
      </FormGroup>
      <Controller
        control={control}
        name="topicConfiguration"
        render={({ field }) => (
          <TopicConfigurationChoice
            value={field.value}
            onChange={field.onChange}
            target={connection?.name ?? "the target"}
            ownersAsked={ownersAsked}
            error={err(errors, "topicConfiguration")}
          />
        )}
      />
      <Controller
        control={control}
        name="replication"
        render={({ field }) => (
          <FormGroup label="Replication factor" fieldId="replication">
            <TextInput id="replication" inputMode="numeric" value={field.value} placeholder={String(replication.basis === "chosen" ? "" : replication.value)} onChange={(_e, t) => field.onChange(t)} validated={err(errors, "replication") ? "error" : "default"} className="lw-short-input" />
            <FieldHelp id="replication" error={err(errors, "replication")}>
              {`${Number.isFinite(replication.value) ? replication.value : "Not a number"}: ${replicationBasisText(replication, brokers)}. One factor for every topic; empty it to go back to the default.`}
            </FieldHelp>
            {replication.basis === "grammar" && <Alert variant="warning" isInline isPlain title={REPLICATION_UNKNOWN_WARNING} />}
            {replicationMayDiffer(replication) && replication.basis !== "grammar" && (
              <Content component="p" className="lw-subtle lw-small">{REPLICATION_DIFFERS_NOTE}</Content>
            )}
            {brokers.count === null && brokers.why && <Content component="p" className="lw-subtle lw-small">The target's broker count is not known: {brokers.why}.</Content>}
          </FormGroup>
        )}
      />
      {point.destinationName ? (
        <Controller
          control={control}
          name="evidenceDestination"
          render={({ field }) => (
            <FormGroup label="Evidence goes to" fieldId="evidence-destination">
              <FormSelect id="evidence-destination" value={field.value} onChange={(_e, t) => field.onChange(t)} validated={err(errors, "evidenceDestination") ? "error" : "default"}>
                {destinations.map((d) => (
                  <FormSelectOption key={d.uid || d.name} value={d.name} label={d.name === point.destinationName ? `${d.name} (the point's own)` : d.name} />
                ))}
              </FormSelect>
              <FieldHelp id="evidence-destination" error={err(errors, "evidenceDestination")}>
                The signed scorecard is written under logweir/ in this destination's bucket, with its own transport.
              </FieldHelp>
            </FormGroup>
          )}
        />
      ) : (
        <LegacyArchive control={control} errors={errors} />
      )}
    </Form>
  );
}

/** An inline archive (no saved destination): how the runner reaches it, typed here. */
function LegacyArchive({ control, errors }: { control: Control<WizardValues>; errors: Errors }) {
  const text = (name: "archiveSecret" | "storeEndpoint" | "storeRegion" | "evidenceBucket", label: string, help: string) => (
    <Controller
      control={control}
      name={name}
      render={({ field }) => (
        <FormGroup label={label} fieldId={name}>
          <TextInput id={name} value={field.value} onChange={(_e, t) => field.onChange(t)} validated={err(errors, name) ? "error" : "default"} />
          <FieldHelp id={name} error={err(errors, name)}>{help}</FieldHelp>
        </FormGroup>
      )}
    />
  );
  return (
    <FormGroup label="Inline archive" fieldId="legacy" role="group" isStack>
      <Content component="p" className="lw-subtle">
        This point was written to an archive named inline, not through a saved destination: say how the restore reaches it.
      </Content>
      {text("archiveSecret", "Archive credential Secret (name only)", "The Secret the restore Job reads the archive with. Blank for an archive reached without one.")}
      {text("storeEndpoint", "Endpoint", "Empty for AWS S3.")}
      {text("storeRegion", "Region", "")}
      <Controller control={control} name="storePathStyle" render={({ field }) => <Checkbox id="store-path-style" label="Path-style addressing" isChecked={field.value} onChange={(_e, on) => field.onChange(on)} />} />
      <Controller
        control={control}
        name="storeAllowHttp"
        render={({ field }) => (
          <Checkbox id="store-allow-http" label="Allow plain HTTP to the store" description="Off by default and never derived from the endpoint: credentials and records would cross the network unencrypted." isChecked={field.value} onChange={(_e, on) => field.onChange(on)} />
        )}
      />
      {text("evidenceBucket", "Evidence bucket", "Where the signed scorecard is written, under logweir/, with the archive's endpoint and transport. It starts as the archive's own bucket, where the controller verifies it.")}
    </FormGroup>
  );
}

// ---------------------------------------------------------- 4. readiness

export interface ReadinessState {
  result: Preflight | null;
  boundHash: string;
  boundKey: string;
  error: unknown;
  starting: boolean;
  stoppedAt: number | null;
}

export function CheckStep({
  readiness,
  refusal,
  canCheck,
  hash,
  planProblem,
  connection,
  onStart,
  onCancel,
  topicPreview,
}: {
  readiness: ReadinessState;
  refusal: string | null;
  canCheck: boolean;
  hash: string;
  planProblem: string | null;
  connection: Connection | null;
  onStart: () => void;
  onCancel: () => void;
  /** The topic configuration preview (theme A), once the check is done. */
  topicPreview?: ReactNode;
}) {
  const r = readiness.result;
  const running = r !== null && !r.terminal && readiness.stoppedAt === null;
  const rows = r?.checks ?? [];
  const ok = r !== null && refusal === null;
  return (
    <>
      <Title headingLevel="h2" size="xl">Is {connection?.name ?? "the target"} ready?</Title>
      <Content component="p" className="lw-subtle">
        The controller checks this exact plan against the target and the archive before anything is created. The check writes nothing.
      </Content>
      {!canCheck ? (
        <Alert variant="warning" isInline title="A readiness check is not available to you here">
          {READINESS_NOT_RUN_WARNING}
        </Alert>
      ) : (
        <>
          <Flex gap={{ default: "gapMd" }} alignItems={{ default: "alignItemsCenter" }} className="lw-gap-bottom">
            <Button variant={r ? "secondary" : "primary"} onClick={onStart} isDisabled={!!planProblem || readiness.starting || running} isLoading={readiness.starting} id="readiness-start">
              {r ? "Check this plan again" : "Run the readiness check"}
            </Button>
            {running && (
              <Button variant="link" onClick={onCancel}>
                Cancel the check
              </Button>
            )}
            {r && <StatusLabel tone={ok ? "success" : running ? "progress" : "danger"} text={ok ? (readyButForDraftApproval(r) ? "Ready for this draft" : "Ready") : running ? "Checking" : r.state === "cancelled" ? "Cancelled" : "Not ready"} />}
          </Flex>
          {planProblem && (
            <Alert variant="warning" isInline title="There is no plan to check yet">
              {planProblem}
            </Alert>
          )}
          {readiness.error !== null && readiness.error !== undefined && (
            <Alert variant="danger" isInline title="The check could not be started or read">
              <ErrorText error={readiness.error} />
            </Alert>
          )}
          {r && refusal && !running && (
            <Alert variant={r.stale || readiness.boundHash !== hash ? "warning" : "danger"} isInline title={r.stale || readiness.boundHash !== hash ? "Out of date" : "Not ready"} id="readiness-refusal">
              {refusal}
            </Alert>
          )}
          {readiness.stoppedAt !== null && r && !r.terminal && (
            <Alert variant="warning" isInline title="Stopped following the check">
              It did not finish in 12 minutes. Run it again.
            </Alert>
          )}
          {rows.length > 0 && (
            <ol className="lw-checks" aria-live="polite" aria-label="Readiness checks">
              {rows.map((c) => (
                <li key={c.id}>
                  <span>
                    <span className="lw-mono">{c.id}</span>
                    {c.message ? <span className="lw-subtle"> · {c.message}</span> : null}
                    {c.state !== "ready" && c.remedy ? <div className="lw-small">{c.remedy}</div> : null}
                    {c.code && c.state !== "ready" ? <div className="lw-subtle lw-small">{c.code}{!isBlockingRow(c) ? " (advisory)" : ""}</div> : null}
                  </span>
                  <CheckRowStatus c={c} />
                </li>
              ))}
            </ol>
          )}
          {r && r.warnings.length > 0 && (
            <Content component="p" className="lw-subtle lw-small">
              Advisory: {r.warnings.map((w) => `${w.id} ${w.state}${w.code ? ` (${w.code})` : ""}`).join("; ")}.
            </Content>
          )}
          {r && r.executionOnly.length > 0 && (
            <Content component="p" className="lw-subtle lw-small">
              Only the run can answer: {r.executionOnly.map((x) => `${x.id} (${x.note})`).join("; ")}. Ready never means these passed.
            </Content>
          )}
          {r && r.stale && <Content component="p" className="lw-subtle lw-small">Stale: {r.staleReasons.map(staleReasonLine).join("; ")}.</Content>}
          {running && rows.length === 0 && <LoadingRows rows={3} label="Checking" />}
          {topicPreview && (
            <>
              <Title headingLevel="h3" size="lg" className="lw-gap-top">Topic configuration</Title>
              {topicPreview}
            </>
          )}
        </>
      )}
      <Content component="p" className="lw-subtle lw-small lw-gap-top">{READINESS_CAVEAT_SENTENCE}</Content>
      {connection && (
        <Facts
          items={[
            { term: "Target probe (context)", value: <ReachStatus state={connection.reachability.state} /> },
            { term: "Cluster id", value: shown(connection.reachability.clusterId) },
            { term: "Observed", value: <Instant value={connection.reachability.observedAt} /> },
          ]}
        />
      )}
    </>
  );
}

/** One readiness row: passed, not ready (blocking or advisory), or not answered. */
function CheckRowStatus({ c }: { c: Preflight["checks"][number] }) {
  const title = `${c.id}: ${c.state}${c.code ? ` (${c.code})` : ""}`;
  if (c.state === "ready") return <StatusLabel tone="success" text="Passed" title={title} />;
  if (isDraftApprovalRow(c)) return <StatusLabel tone="neutral" text="After create" title={title} />;
  if (c.state === "notReady") return <StatusLabel tone={isBlockingRow(c) ? "danger" : "warning"} text="Not ready" title={title} />;
  return <StatusLabel tone="warning" text={c.state === "skipped" ? "Skipped" : "Not answered"} title={title} />;
}

// ------------------------------------------------------- 5. review and create

export function ReviewStep({
  control,
  point,
  fields,
  prepared,
  planProblem,
  replication,
  brokers,
  policy,
  errors,
  values,
}: {
  control: Control<WizardValues>;
  point: RecoveryPoint;
  fields: PlanFields;
  prepared: PreparedPlan | null;
  planProblem: string | null;
  replication: ReplicationChoice;
  brokers: BrokerFact;
  policy: ApprovalPolicy | null;
  errors: Errors;
  values: WizardValues;
}) {
  const [open, setOpen] = useState(false);
  const v = values;
  const original = originalNameChosen(v);
  const mapping = topicMapping(fields);
  const download = () => {
    if (!prepared) return;
    const url = URL.createObjectURL(new Blob([prepared.bytes], { type: "application/yaml" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = `${prepared.restoreName}.plan.yaml`;
    a.click();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  };
  return (
    <Form onSubmit={(e) => e.preventDefault()}>
      <Title headingLevel="h2" size="xl">Review and create</Title>
      <Facts
        items={[
          { term: "Recovery point", value: `${point.backup?.name ?? point.catalog?.entry.pointId ?? ""}, set ${point.backupSetRef}` },
          { term: "Up to", value: fields.pointInTime ? `${exactUtc(fields.pointInTime)} (${fields.pointInTime})` : "Not recorded" },
          { term: "Into", value: `${fields.target.bootstrapServers.length ? v.target : "no target"}: ${mapping.map((m) => (m.source === m.target ? m.source : `${m.source} → ${m.target}`)).join(", ")}` },
          { term: "Mode", value: modeMeaning(fields.target.mode, fields.target.originalName === true) },
          { term: "Verification", value: fields.sample.coverage === "complete" ? `complete${typeof fields.sample.completeMaxRecords === "number" ? `, at most ${count(fields.sample.completeMaxRecords)} archived records` : ", no record bound"}` : "sampled: 25 records per partition and the manifest's counts" },
          { term: "Replication factor", value: `${replication.value} (${replicationBasisText(replication, brokers)})` },
          { term: "Time basis", value: v.timeBasis ? "producer time (restore.time_basis: producerTime)" : "not stated: a LogAppendTime topic at this point is refused before anything is created" },
          { term: "Approval subject", value: original ? "originalName: only an approval signed for a restore under the original names authorises it" : "ordinary" },
          { term: "Plan", value: prepared ? <Ident value={prepared.hash} label="plan sha256" /> : <span className="lw-subtle">{planProblem ? "No plan" : "Computing the hash"}</span> },
          prepared && { term: "Names minted", value: <>{prepared.restoreName}, {prepared.approvalName}</> },
        ]}
      />
      {planProblem && (
        <Alert variant="danger" isInline title="The plan cannot be rendered from these values" id="plan-problem">
          {planProblem}. There is no hash and nothing to submit.
        </Alert>
      )}
      {prepared && (
        <ExpandableSection toggleText={open ? "Hide the plan" : "Show the plan"} onToggle={(_e, o) => setOpen(o)} isExpanded={open}>
          <Commands lines={[prepared.bytes.replace(/\n$/, "")]} exact={prepared.bytes} label="the plan" />
          <Button variant="link" icon={<DownloadIcon />} onClick={download} className="lw-gap-top">
            Download the plan
          </Button>
        </ExpandableSection>
      )}
      {original && <Alert variant="warning" isInline isPlain title={ORIGINAL_NAME_SENTENCE} />}
      {v.timeBasis && <Alert variant="warning" isInline isPlain title={TIME_BASIS_OPTED_WARNING} />}
      {fields.sample.coverage === "complete" && <Content component="p" className="lw-subtle lw-small">{COMPLETE_COVERAGE_COST}</Content>}
      <Content component="p" className="lw-subtle lw-small" id="restore-semantics">{RESTORE_SEMANTICS_SENTENCE}</Content>
      <ApprovalBlock control={control} policy={policy} errors={errors} />
      {typedConfirmationRequired(policy, v) && (
        <Controller
          control={control}
          name="typedNames"
          render={({ field }) => (
            <FormGroup label="Re-type the original topic names" fieldId="typed-names" isRequired>
              <TextArea id="typed-names" value={field.value} onChange={(_e, t) => field.onChange(t)} rows={Math.max(3, frozenTopicsOf(point, v).length)} autoComplete="off" spellCheck={false} validated={err(errors, "typedNames") ? "error" : "default"} />
              <FieldHelp id="typed-names" error={err(errors, "typedNames")}>{ORIGINAL_NAME_TYPED_SENTENCE}</FieldHelp>
            </FormGroup>
          )}
        />
      )}
    </Form>
  );
}

const APPROVE_COMMAND = "logweir drill approve --spec <file> --key <privkey> --approver <id> --ticket <id> --subject-kind Restore --out <file>";
const COUNTERSIGN_COMMAND = "logweir drill countersign --document <approvalBytes> --confirmation <sidecarBytes> --key <privkey> --out approval.sig";

/** The approval step under the namespace's EFFECTIVE policy, in the operator's three
 *  modes. A policy that could not be read shows the out-of-band (strict) text. */
function ApprovalBlock({ control, policy, errors }: { control: Control<WizardValues>; policy: ApprovalPolicy | null; errors: Errors }) {
  const route = approvalRoute(policy);
  const ticket = (
    <Controller
      control={control}
      name="ticket"
      render={({ field }) => (
        <FormGroup label="Change ticket" fieldId="ticket" isRequired={policy?.ticketRequired === true}>
          <TextInput id="ticket" maxLength={128} value={field.value} onChange={(_e, t) => field.onChange(t)} validated={err(errors, "ticket") ? "error" : "default"} className="lw-ticket-input" />
          <FieldHelp id="ticket" error={err(errors, "ticket")}>Signed into the request with the plan hash; the approver sees it before approving.</FieldHelp>
        </FormGroup>
      )}
    />
  );
  switch (route) {
    case "confirm":
      return (
        <Alert variant="info" isInline title={`${policy!.namespace} uses one-person confirmation`} id="approval-confirm">
          Creating the restore is your confirmation: the console signs, as its attestation that you asked, a document naming exactly
          this restore and this plan hash, and the restore runs once the controller verifies it. No approver and no key are involved.
          {policy!.basis === "freshInstall" ? " This installation started in confirm: every namespace without its own approval policy is confirmed in the console." : ""}
        </Alert>
      );
    case "confirm-unavailable":
      return (
        <Alert variant="danger" isInline title="Confirm: not available yet" id="approval-confirm-unavailable">
          Policy {policy!.name} is confirm, and this console cannot sign a confirmation yet: its confirmation key is not there. The
          installation's identity hook writes it once, at install; try again in a minute.
        </Alert>
      );
    case "two-person":
      return (
        <>
          <Alert variant="info" isInline title="Two-person: a second person approves in the console" id="approval-two-person">
            Creating the restore records the console's signed request naming you as the requester. It runs only after a second person,
            signed in as an approver and not you, approves it on the Approvals page. No key is involved. The request expires if nobody
            approves it in time.
          </Alert>
          {ticket}
        </>
      );
    case "two-person-unavailable":
      return (
        <Alert variant="danger" isInline title="Two-person: not available in this console" id="approval-two-person-unavailable">
          Policy {policy!.name} is two-person, and this console cannot take part: it is the in-cluster administrator console, or its
          confirmation key is not there yet. Request and approve this restore in the shared console.
        </Alert>
      );
    case "strict":
      return (
        <>
          <Alert variant="info" isInline title="Strict: an approver's personal key" id="approval-strict">
            Creating the restore records the console's confirmation of you as the requester. It runs only after an approver who is not
            you countersigns that confirmation with their own key, on their own machine, and submits it on the restore's approval page.
          </Alert>
          {ticket}
          <Commands lines={[COUNTERSIGN_COMMAND]} label="the countersign command" />
        </>
      );
    default:
      return (
        <>
          <Alert variant="info" isInline title="Approve it out of band (strict: an approver's personal key)" id="approval-legacy">
            Run this on the machine that holds the approver's private key; this page never sees it.
          </Alert>
          <Commands lines={[APPROVE_COMMAND]} label="the approve command" />
        </>
      );
  }
}
