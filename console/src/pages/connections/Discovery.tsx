// Topic discovery: a check Job dials this connection with its own credential,
// asks the broker for metadata and stores what came back. A successful listing
// is never called complete: Kafka hides topics a principal cannot describe.
import { useState } from "react";
import {
  Alert,
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Checkbox,
  Content,
  DescriptionList,
  Flex,
  Form,
  FormSelect,
  FormSelectOption,
  Grid,
  GridItem,
  TextInput,
  Title,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import BanIcon from "@patternfly/react-icons/dist/esm/icons/ban-icon";
import UnknownIcon from "@patternfly/react-icons/dist/esm/icons/unknown-icon";
import {
  CHECK_UNFINISHED_SENTENCE,
  CHECK_UNREADABLE_SENTENCE,
  carriesKeyMaterial,
  useCancelDiscovery,
  useIntentKey,
  useDiscoverySlots,
  useStartDiscovery,
  useTopicPage,
  type Stopped,
  type TopicDiscovery,
  type TopicFilters,
} from "../../api/connections";
import { StatusLabel } from "../../components/Status";
import { ErrorState, LoadingRows } from "../../components/States";
import { count } from "../../components/When";
import { Ident } from "../../components/Ident";
import { CompactList, useNarrow } from "../../components/CompactList";
import { Field, Instant, NotRecorded, Term, WriteOutcome } from "./parts";

export const VISIBILITY_UNKNOWN_SENTENCE =
  "Kafka hides topics this principal cannot describe, and hides them without saying so, so a successful listing is not proof that this is every topic. Logweir records that as unknown rather than calling it complete.";
export const VISIBILITY_LIMITED_SENTENCE =
  "An authorization omission was observed: at least one topic exists that this principal may not describe. The list is a subset, and Logweir cannot say how large a subset.";
export const EMPTY_INVENTORY_SENTENCE =
  "No visible topics. Kafka hides topics this principal cannot describe; this is not proof that the cluster is empty.";
export const STALE_INVENTORY_SENTENCE =
  "This inventory is past its freshness, or its connection changed under it. It is shown because an old fact with a date on it is more use than a blank, and labelled because an old fact presented as current is not.";
export const TRUNCATED_SENTENCE = "The inventory hit a ceiling before the cluster ran out of topics, so this is a prefix of what the broker listed, not the whole of it.";
export const SCAN_INCOMPLETE_SENTENCE =
  "This page stopped at its read budget, not at the end of the result: there is more to read. A short page is not the last page, and a filter that matches little is what makes a page short.";

export function visibilitySentence(v: TopicDiscovery["visibility"]): string {
  if (v?.state === "limited") return VISIBILITY_LIMITED_SENTENCE;
  if (v?.state === "attestedComplete") {
    return v.attestation
      ? `An administrator attests that this principal sees every topic: ${v.attestation}; not verified by Logweir.`
      : VISIBILITY_UNKNOWN_SENTENCE;
  }
  return VISIBILITY_UNKNOWN_SENTENCE;
}

function DiscoveryStatus({ d }: { d: TopicDiscovery }) {
  switch (d.state) {
    case "succeeded":
      return <StatusLabel tone="success" text="Listed" title="state succeeded" />;
    case "failed":
      return <StatusLabel tone="danger" text="Failed" title="state failed" />;
    case "cancelled":
      return <StatusLabel tone="neutral" text="Cancelled" title="state cancelled" icon={<BanIcon />} />;
    case "pending":
    case "queued":
    case "running":
      return <StatusLabel tone="progress" text={d.state === "running" ? "Running" : d.state === "queued" ? "Queued" : "Starting"} title={`state ${d.state}`} />;
    default:
      return <StatusLabel tone="neutral" text="Unknown" title={`state ${d.state}`} icon={<UnknownIcon />} />;
  }
}

/** One slot: the latest attempt or the last successful inventory, labelled so a reader never has to work out which. */
function Slot({ d, heading, stopped, onRunAgain }: { d: TopicDiscovery; heading: string; stopped: Stopped; onRunAgain?: () => void }) {
  const empty = d.state === "succeeded" && d.counts?.returned === 0;
  return (
    <section aria-label={heading}>
      <Title headingLevel="h3" size="md">
        {heading}
      </Title>
      <Flex gap={{ default: "gapSm" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "wrap" }} className="lw-gap-top">
        <DiscoveryStatus d={d} />
        {d.stale && <StatusLabel tone="warning" text={`Stale: ${d.staleReasons.join(", ") || "reason not recorded"}`} />}
        {d.truncated && <StatusLabel tone="warning" text={`Truncated: ${d.truncationReason ?? "reason not recorded"}`} />}
        <Ident value={d.id} label="Discovery id" />
      </Flex>
      {!d.terminal && stopped !== "" && (
        <Alert
          isInline
          variant="warning"
          className="lw-gap-top"
          title={stopped === "deadline" ? "This discovery did not finish" : "This discovery could not be read again"}
          actionLinks={
            onRunAgain && (
              <Button variant="link" isInline onClick={onRunAgain}>
                Run the discovery again
              </Button>
            )
          }
        >
          {(stopped === "deadline" ? CHECK_UNFINISHED_SENTENCE : CHECK_UNREADABLE_SENTENCE).split("check").join("discovery")}
        </Alert>
      )}
      {d.stale && <Content component="p" className="lw-subtle lw-small lw-gap-top">{STALE_INVENTORY_SENTENCE}</Content>}
      {d.truncated && <Content component="p" className="lw-subtle lw-small">{TRUNCATED_SENTENCE}</Content>}
      {d.state === "succeeded" && <Content component="p" className="lw-subtle lw-small">{visibilitySentence(d.visibility)}</Content>}
      {empty && (
        <Alert isInline isPlain variant="info" title={EMPTY_INVENTORY_SENTENCE} />
      )}
      <DescriptionList isCompact isHorizontal className="lw-gap-top" horizontalTermWidthModifier={{ default: "16ch" }}>
        <Term term="Observed">
          <Instant value={d.observedAt} exact />
        </Term>
        <Term term="Fresh until">
          <Instant value={d.freshUntil} exact />
        </Term>
        <Term term="Principal">{d.connection.principal ?? <NotRecorded />}</Term>
        <Term term="Cluster id">{d.clusterId ? <code>{d.clusterId}</code> : <NotRecorded />}</Term>
        <Term term="Listed / stored">{d.counts ? `${count(d.counts.listed)} / ${count(d.counts.returned)}` : <NotRecorded />}</Term>
        {d.counts && <Term term="Internal excluded">{count(d.counts.internalExcluded)}</Term>}
        {d.counts && d.counts.errored > 0 && <Term term="Metadata unreadable">{count(d.counts.errored)}</Term>}
        {d.expected && (
          <Term term="Expected topics">
            {d.expected.requested} asked: {d.expected.visible} visible, {d.expected.notAuthorized} not authorized, {d.expected.notFound} not
            found, {d.expected.unknown} unknown
          </Term>
        )}
        {d.error && (
          <Term term="Error">
            <code>{d.error.code}</code> {d.error.message}
          </Term>
        )}
      </DescriptionList>
    </section>
  );
}

const NO_FILTERS: TopicFilters = { q: "", prefix: "", internal: "exclude", errored: "include" };

/** The stored inventory of the last successful discovery, with the service's own filters and paging. */
function Topics({ ns, discovery }: { ns: string; discovery: TopicDiscovery }) {
  const narrow = useNarrow();
  const [typed, setTyped] = useState<TopicFilters>(NO_FILTERS);
  const [filters, setFilters] = useState<TopicFilters>(NO_FILTERS);
  const [cursors, setCursors] = useState<string[]>([]);
  const cursor = cursors.length > 0 ? cursors[cursors.length - 1]! : null;
  const page = useTopicPage(ns, discovery.id, filters, cursor);
  const apply = () => {
    setFilters(typed);
    setCursors([]);
  };
  return (
    <section aria-label="Stored inventory" className="lw-gap-top">
      <Title headingLevel="h3" size="md">
        Topics in the last successful inventory
      </Title>
      <Content component="p" className="lw-subtle lw-small">
        These filters change what the service reads out of the stored result. They change nothing on the cluster and start no check.
      </Content>
      <Form
        onSubmit={(e) => {
          e.preventDefault();
          apply();
        }}
        aria-label="Search the stored inventory"
      >
        <Grid hasGutter md={3}>
          <GridItem>
            <Field id="topic-q" label="Contains">
              <TextInput id="topic-q" value={typed.q} onChange={(_e, v) => setTyped({ ...typed, q: v })} />
            </Field>
          </GridItem>
          <GridItem>
            <Field id="topic-prefix" label="Prefix">
              <TextInput id="topic-prefix" value={typed.prefix} onChange={(_e, v) => setTyped({ ...typed, prefix: v })} />
            </Field>
          </GridItem>
          <GridItem>
            <Field id="topic-internal" label="Internal topics">
              <FormSelect id="topic-internal" value={typed.internal} onChange={(_e, v) => setTyped({ ...typed, internal: v as TopicFilters["internal"] })}>
                <FormSelectOption value="exclude" label="Exclude" />
                <FormSelectOption value="include" label="Include" />
              </FormSelect>
            </Field>
          </GridItem>
          <GridItem>
            <Field id="topic-errored" label="Unreadable metadata">
              <FormSelect id="topic-errored" value={typed.errored} onChange={(_e, v) => setTyped({ ...typed, errored: v as TopicFilters["errored"] })}>
                <FormSelectOption value="include" label="Include" />
                <FormSelectOption value="exclude" label="Exclude" />
                <FormSelectOption value="only" label="Only" />
              </FormSelect>
            </Field>
          </GridItem>
        </Grid>
        <div>
          <Button type="submit" variant="secondary">
            Search
          </Button>
        </div>
      </Form>
      <div className="lw-gap-top">
        {page.isPending ? (
          <LoadingRows rows={4} label="Loading topics" />
        ) : page.isError ? (
          <ErrorState error={page.error} onRetry={() => void page.refetch()} />
        ) : (
          <>
            {page.data.items.length === 0 ? (
              <Content component="p" className="lw-subtle">
                No topic on this page. That is a statement about this page and its filters, not about the cluster.
              </Content>
            ) : narrow ? (
              <CompactList
                flush
                label="Topics"
                items={page.data.items.map((t) => ({
                  key: t.name,
                  title: t.name,
                  meta: `${t.partitions} partitions${t.internal ? " · internal" : ""}${t.expected ? " · expected" : ""}`,
                  status: t.errorCode ? <StatusLabel tone="warning" text={t.errorCode} /> : <span />,
                }))}
              />
            ) : (
              <Table aria-label="Topics" variant="compact">
                <Thead>
                  <Tr>
                    <Th>Topic</Th>
                    <Th className="lw-num">Partitions</Th>
                    <Th>Internal</Th>
                    <Th>Expected</Th>
                    <Th>Metadata error</Th>
                  </Tr>
                </Thead>
                <Tbody>
                  {page.data.items.map((t) => (
                    <Tr key={t.name}>
                      <Td dataLabel="Topic" className="lw-break">
                        <code>{t.name}</code>
                      </Td>
                      <Td dataLabel="Partitions" className="lw-num">
                        {t.partitions}
                      </Td>
                      <Td dataLabel="Internal">{t.internal ? "Yes" : "No"}</Td>
                      <Td dataLabel="Expected">{t.expected ? "Yes" : "No"}</Td>
                      <Td dataLabel="Metadata error">{t.errorCode ? <code>{t.errorCode}</code> : <span className="lw-subtle">None</span>}</Td>
                    </Tr>
                  ))}
                </Tbody>
              </Table>
            )}
            {!page.data.scan.complete && <Content component="p" className="lw-subtle lw-small lw-gap-top">{SCAN_INCOMPLETE_SENTENCE}</Content>}
            <Flex gap={{ default: "gapSm" }} className="lw-gap-top">
              {cursors.length > 0 && (
                <Button variant="secondary" onClick={() => setCursors([])}>
                  First page
                </Button>
              )}
              {page.data.page.nextCursor ? (
                <Button variant="secondary" onClick={() => setCursors([...cursors, page.data.page.nextCursor!])} isLoading={page.isFetching}>
                  Read the next page
                </Button>
              ) : (
                <span className="lw-subtle lw-small">This is the end of the stored result.</span>
              )}
            </Flex>
          </>
        )}
      </div>
    </section>
  );
}

/** The discovery card on a connection's page. */
export function Discovery({ ns, name, mayStart, readable }: { ns: string; name: string; mayStart: boolean; readable: boolean }) {
  const { query: slots, stopped } = useDiscoverySlots(ns, name, readable);
  const start = useStartDiscovery(ns, name);
  const cancel = useCancelDiscovery(ns, name);
  const [includeInternal, setIncludeInternal] = useState(false);
  const [expected, setExpected] = useState("");
  const intent = useIntentKey("discovery");
  const [local, setLocal] = useState<unknown>(null);
  const [reused, setReused] = useState(false);
  const expectedProblem = carriesKeyMaterial(expected) ? "This looks like a private key; topic names only." : undefined;

  const run = () => {
    if (start.isPending || expectedProblem) return;
    setLocal(null);
    let key: string;
    try {
      // One key per click, held while the request is in flight: a double click is one discovery.
      key = intent.take();
    } catch (refusal) {
      setLocal(refusal);
      return;
    }
    start.mutate(
      { key, request: { includeInternal, expectedTopics: expected } },
      {
        onSuccess: (answer) => setReused(answer.reused === true),
        onSettled: () => intent.settle(),
      },
    );
  };

  const latest = slots.data?.latestAttempt ?? null;
  const success = slots.data?.lastSuccessful ?? null;
  const running = latest !== null && !latest.terminal;

  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Topics</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle">
          Discover topics starts a check Job that dials this connection with its own credential and asks the broker for metadata. A
          successful listing is never called complete: Kafka hides topics this principal cannot describe.
        </Content>
        {!readable ? (
          <Content component="p" className="lw-subtle">
            Topic discovery is not available to you in {ns}.
          </Content>
        ) : (
          <>
            {mayStart ? (
              <Form
                onSubmit={(e) => {
                  e.preventDefault();
                  run();
                }}
                aria-label="Discover topics"
                className="lw-gap-top"
              >
                <Grid hasGutter md={6}>
                  <GridItem>
                    <Field
                      id="discovery-expected"
                      label="Expected topics (optional)"
                      error={expectedProblem}
                      help="Names the check asks about explicitly, separated by commas. A broker refuses a name this principal cannot describe whether or not it exists, which is the only way a listing's silence becomes visible."
                    >
                      <TextInput
                        id="discovery-expected"
                        value={expected}
                        onChange={(_e, v) => setExpected(v)}
                        validated={expectedProblem ? "error" : "default"}
                        aria-describedby="discovery-expected-helper"
                      />
                    </Field>
                  </GridItem>
                  <GridItem>
                    <Checkbox
                      id="discovery-internal"
                      label="Include Kafka internal topics (__-prefixed)"
                      isChecked={includeInternal}
                      onChange={(_e, v) => setIncludeInternal(v)}
                      className="lw-gap-top"
                    />
                  </GridItem>
                </Grid>
                <Flex gap={{ default: "gapSm" }}>
                  <Button type="submit" variant="secondary" isLoading={start.isPending} isDisabled={start.isPending}>
                    Discover topics
                  </Button>
                  {running && (
                    <Button variant="link" onClick={() => cancel.mutate(latest.id)} isDisabled={cancel.isPending}>
                      Cancel the discovery
                    </Button>
                  )}
                </Flex>
                <div aria-live="polite">
                  {local !== null && <WriteOutcome error={local} subject="the discovery" credentials={false} unmatched={[]} />}
                  {start.isError && <WriteOutcome error={start.error} subject="the discovery" credentials={false} unmatched={[]} />}
                  {cancel.isError && (
                    <Alert isInline variant="danger" title="The discovery was not cancelled">
                      {cancel.error.message}
                    </Alert>
                  )}
                  {reused && start.isSuccess && (
                    <Alert isInline isPlain variant="info" title="A fresh identical inventory already existed, so no second check was started. This is that result." />
                  )}
                </div>
              </Form>
            ) : (
              <Content component="p" className="lw-subtle">
                This login may read topic discoveries in this namespace and not start one.
              </Content>
            )}
            <div className="lw-gap-top">
              {slots.isPending ? (
                <LoadingRows rows={3} label="Loading discoveries" />
              ) : slots.isError ? (
                <ErrorState error={slots.error} onRetry={() => void slots.refetch()} />
              ) : latest === null && success === null ? (
                <Content component="p" className="lw-subtle">
                  No topic discovery has run for this connection. Nothing here claims anything about its topics.
                </Content>
              ) : (
                <Grid hasGutter lg={6}>
                  {latest !== null && (
                    <GridItem>
                      <Slot
                        d={latest}
                        heading={success !== null && latest.id === success.id ? "Latest attempt, also the last successful inventory" : "Latest attempt"}
                        stopped={stopped}
                        onRunAgain={mayStart ? run : undefined}
                      />
                    </GridItem>
                  )}
                  {(latest === null || success === null || latest.id !== success.id) && (
                    <GridItem>
                      {success === null ? (
                        <Content component="p" className="lw-subtle">
                          No discovery for this connection has ever produced an inventory.
                        </Content>
                      ) : (
                        <Slot d={success} heading="Last successful inventory" stopped="" />
                      )}
                    </GridItem>
                  )}
                </Grid>
              )}
            </div>
            {success !== null && <Topics key={success.id} ns={ns} discovery={success} />}
          </>
        )}
      </CardBody>
    </Card>
  );
}
