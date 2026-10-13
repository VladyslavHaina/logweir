// Theme A on the restore page (PROD-04.2, PROD-05.2, PROD-11.1a), still on the
// prototype's mock: consumer positions, the clone and the topic configuration. R7
// replaces these cards with the theme A API's Restore.status fields; until then they
// are drawn only in mock builds, never from a live restore.
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Content,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Flex,
  FlexItem,
} from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import DownloadIcon from "@patternfly/react-icons/dist/esm/icons/download-icon";
import type { Restore } from "../../api/restores";
import { StatusLabel } from "../../components/Status";
import { When, msValue, relative } from "../../components/When";
import { Ident } from "../../components/Ident";
import { DURING_RESTORE, TOPIC_CONFIG, mappingFor, type RestoreThemeA } from "../../mock/themeA";

/** Whether this build draws theme A from the mock (every build but a live one). */
export const MOCK_THEME_A = import.meta.env.VITE_LOGWEIR_API !== "live";

/** The positions the complete verification mapped, and whether a switchover is possible (PROD-04.2). */
export function PositionsCard({ r, t }: { r: Restore; t: RestoreThemeA }) {
  const entries = Object.values(mappingFor(r)).flatMap((g) => g.entries);
  const n = (c: string) => entries.filter((e) => e.class === c).length;
  const body =
    t.mapping === "written" ? (
      <>
        <Content component="p">
          {Object.keys(mappingFor(r)).length} captured groups, {entries.length} positions: {n("exact")} exact, {n("approximate")} approximate,{" "}
          {n("unavailable")} unavailable. The mapping is bound to the signed scorecard.
        </Content>
        <Ident value={t.mappingSha256} label="positions mapping sha256" />
      </>
    ) : t.mapping === "sampled" ? (
      <Content component="p" className="lw-subtle">
        Not mapped: this restore was verified on a sample. Consumer positions are mapped only by a complete verification, so
        it cannot be switched over.
      </Content>
    ) : t.mapping === "scratch" ? (
      <Content component="p" className="lw-subtle">
        A clone is not switched over: its topics are deleted when it expires.
      </Content>
    ) : (
      <Content component="p" className="lw-subtle">
        {r.coverage?.requested === "complete"
          ? "Mapped when the complete verification passes."
          : "Not mapped: this restore uses a sampled verification."}
      </Content>
    );
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Consumer positions</CardTitle>
      </CardHeader>
      <CardBody>{body}</CardBody>
    </Card>
  );
}

/** `Restore.status.clone`: when the clone's topics go, and what removed them. */
export function CloneCard({ t }: { t: RestoreThemeA }) {
  const c = t.clone!;
  return (
    <Card>
      <CardHeader>
        <Flex alignItems={{ default: "alignItemsCenter" }} gap={{ default: "gapSm" }}>
          <CardTitle component="h2">Temporary clone</CardTitle>
          {c.phase === "Removed" ? <StatusLabel tone="success" text="Removed" /> : <StatusLabel tone="info" text={`Deletes ${relative(c.expiresAt)}`} />}
        </Flex>
      </CardHeader>
      <CardBody>
        <DescriptionList isCompact className="lw-dl" orientation={{ sm: "vertical", md: "horizontal" }}>
          <DescriptionListGroup>
            <DescriptionListTerm>Expires</DescriptionListTerm>
            <DescriptionListDescription>
              <When value={c.expiresAt} exact />
            </DescriptionListDescription>
          </DescriptionListGroup>
          <DescriptionListGroup>
            <DescriptionListTerm>What is deleted</DescriptionListTerm>
            <DescriptionListDescription>
              Only the topics this run created, as its signed scorecard records them, and only while each topic's ID is the one
              recorded at creation. A topic recreated since is kept (CloneTargetReplaced).
            </DescriptionListDescription>
          </DescriptionListGroup>
          <DescriptionListGroup>
            <DescriptionListTerm>Clean-up</DescriptionListTerm>
            <DescriptionListDescription>
              {c.phase === "Removed" ? "Done; the clean-up attestation is kept with the evidence." : `Not run yet (${c.attempts} attempts). Deleting this restore first runs it at once.`}
            </DescriptionListDescription>
          </DescriptionListGroup>
        </DescriptionList>
      </CardBody>
    </Card>
  );
}

/** `Restore.status.topicConfiguration`: the settings before and after the transition (PROD-05.2). */
export function TopicConfigCard({ r, t }: { r: Restore; t: RestoreThemeA }) {
  const tc = t.topicConfiguration!;
  const sources = ["orders", "orders-dlq", "payments"];
  const rows = r.newTopics.map((name, i) => ({ name, model: TOPIC_CONFIG.find((m) => m.source === sources[i]) }));
  return (
    <Card>
      <CardHeader>
        <CardTitle component="h2">Topic configuration</CardTitle>
      </CardHeader>
      <CardBody>
        <Content component="p" className="lw-subtle">
          {tc.transition === "afterVerification"
            ? "Created with the source's settings. The settings that could delete, compact or re-stamp restored records were held at Logweir's values until the verdict, then set to the source's."
            : "Created with the source's settings; the restore's own settings were left in place."}
        </Content>
        <Table aria-label="Topic configuration before and after verification" variant="compact" gridBreakPoint="grid-md" className="lw-kv-table">
          <Thead>
            <Tr>
              <Th width={25}>Topic</Th>
              <Th width={15}>Outcome</Th>
              <Th width={30}>While restoring</Th>
              <Th width={30}>Now</Th>
            </Tr>
          </Thead>
          <Tbody>
            {rows.map(({ name, model }) => {
              const keys = Object.keys(model?.deferred ?? {});
              const exported = tc.exported.includes(name);
              const failed = tc.failed.find((f) => f.topic === name);
              const list = (value: (key: string) => string) => (
                <ul className="lw-keys">
                  {keys.map((key) => (
                    <li key={key}>
                      {key}={value(key)}
                    </li>
                  ))}
                </ul>
              );
              return (
                <Tr key={name}>
                  <Td dataLabel="Topic">{name}</Td>
                  <Td dataLabel="Outcome">
                    {failed ? (
                      <StatusLabel tone="danger" text="Not applied" title={failed.error} />
                    ) : exported ? (
                      <StatusLabel tone="info" text="Exported" title={model?.owner} />
                    ) : tc.applied.includes(name) ? (
                      <StatusLabel tone="success" text="Applied" />
                    ) : (
                      <StatusLabel tone="neutral" text="Not run" />
                    )}
                  </Td>
                  <Td dataLabel="While restoring">{list((key) => DURING_RESTORE[key] ?? "not set")}</Td>
                  <Td dataLabel="Now">
                    {exported ? (
                      <>
                        {list((key) => DURING_RESTORE[key] ?? "not set")}
                        <div className="lw-subtle lw-small">The owner applies: {keys.map((key) => `${key}=${msValue(key, model?.deferred[key] ?? "")}`).join(", ")}</div>
                      </>
                    ) : (
                      list((key) => msValue(key, model?.deferred[key] ?? ""))
                    )}
                  </Td>
                </Tr>
              );
            })}
          </Tbody>
        </Table>
        {tc.exported.length > 0 && tc.desiredStateKey && (
          <Flex className="lw-gap-top" gap={{ default: "gapMd" }} alignItems={{ default: "alignItemsCenter" }} flexWrap={{ default: "wrap" }}>
            <FlexItem className="lw-min0">
              <Content component="p">
                {tc.exported.join(", ")} {tc.exported.length === 1 ? "is" : "are"} owned by Strimzi, so Logweir did not change{" "}
                {tc.exported.length === 1 ? "its" : "their"} settings: apply the KafkaTopic it wrote.
              </Content>
              <Ident value={tc.desiredStateKey} label="desired-state document key" />
            </FlexItem>
            <FlexItem>
              <Button variant="secondary" icon={<DownloadIcon />} isDisabled={MOCK_THEME_A}>
                KafkaTopic YAML
              </Button>
            </FlexItem>
          </Flex>
        )}
      </CardBody>
    </Card>
  );
}

