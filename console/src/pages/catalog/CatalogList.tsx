// Every recovery catalog in the namespace: the durable inventory of recovery
// points in an archive, as this cluster last managed to read it, and the one
// write this area offers, connecting an existing archive.
import { useState } from "react";
import { Link, useParams } from "@tanstack/react-router";
import { Button, Card, Content, PageSection } from "@patternfly/react-core";
import { Table, Tbody, Td, Th, Thead, Tr } from "@patternfly/react-table";
import CatalogIcon from "@patternfly/react-icons/dist/esm/icons/catalog-icon";
import { useCatalogs, type Catalog } from "../../api/backups";
import { PageHeader } from "../../components/PageHeader";
import { CompactList, useNarrow } from "../../components/CompactList";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { StatusLabel } from "../../components/Status";
import { When, count } from "../../components/When";
import { useFlags } from "../backups/parts";
import "../backups/evidence.css";
import { ConnectArchive } from "./ConnectArchive";
import { CATALOG_WINDOW_SENTENCE, DUPLICATE_CATALOG, NO_CATALOG_SENTENCE, conditionOf, countsWords, noViewWords, windowedCatalogs } from "./model";
import { Prose } from "../../components/Prose";

/** The catalog's Ready condition as a word; never read from the browser's clock. */
export function ReadyStatus({ c }: { c: Pick<Catalog, "conditions"> }) {
  const ready = conditionOf(c.conditions, "Ready");
  const title = ready ? `Ready ${ready.status}${ready.reason ? ` (${ready.reason})` : ""}${ready.message ? `: ${ready.message}` : ""}` : "no Ready condition yet";
  if (ready?.status === "True") return <StatusLabel tone="success" text="Ready" title={title} />;
  if (!ready) return <StatusLabel tone="neutral" text="Not synced yet" title={title} />;
  if (ready.reason === DUPLICATE_CATALOG) return <StatusLabel tone="warning" text="Duplicate" title={title} />;
  return <StatusLabel tone="warning" text="Not ready" title={title} />;
}

export function whereFrom(c: Pick<Catalog, "destinationRef" | "legacyArchive">): string {
  return c.destinationRef?.name ?? c.legacyArchive?.url ?? "Not recorded";
}

export const CONNECT_READ_ONLY_SENTENCE =
  "This login may read the recovery catalog in this namespace and not connect an archive to it: that needs the operator or administrator role.";

export function CatalogList() {
  const { ns } = useParams({ from: "/$ns/catalog" });
  const catalogs = useCatalogs(ns);
  const flags = useFlags(ns);
  const narrow = useNarrow();
  const [connecting, setConnecting] = useState(false);
  const mayConnect = flags?.catalogConnect === true;
  const items = catalogs.data ?? [];
  const windowed = windowedCatalogs(items);
  const connectButton = (variant: "primary" | "secondary") =>
    mayConnect ? (
      <Button variant={variant} onClick={() => setConnecting(true)}>
        Connect an archive
      </Button>
    ) : undefined;

  return (
    <>
      <PageHeader
        title="Recovery catalog"
        description="Every recovery catalog in this namespace: the inventory of recovery points in an archive, as this cluster last read it. The archive is the truth; a catalog is the view of it."
        actions={items.length > 0 ? connectButton("primary") : undefined}
      />
      {connecting && <ConnectArchive ns={ns} open={connecting} onClose={() => setConnecting(false)} />}
      <PageSection>
        <Card>
          {catalogs.isPending ? (
            <div className="lw-pad">
              <LoadingRows rows={3} label="Loading the recovery catalogs" />
            </div>
          ) : catalogs.isError ? (
            <ErrorState error={catalogs.error} onRetry={() => void catalogs.refetch()} />
          ) : items.length === 0 ? (
            <Empty title="No recovery catalog yet" icon={CatalogIcon} actions={connectButton("primary")}>
              {NO_CATALOG_SENTENCE} {mayConnect ? "" : CONNECT_READ_ONLY_SENTENCE}
            </Empty>
          ) : narrow ? (
            <CompactList
              label="Recovery catalogs"
              items={items.map((c) => ({
                key: c.uid || c.name,
                href: `#/${ns}/catalog/${c.name}`,
                title: c.name,
                meta: (
                  <>
                    {whereFrom(c)} · {countsWords(c)}
                    {c.syncedAt && (
                      <>
                        {" "}
                        · synced <When value={c.syncedAt} />
                      </>
                    )}
                  </>
                ),
                status: <ReadyStatus c={c} />,
              }))}
            />
          ) : (
            <Table aria-label="Recovery catalogs" variant="compact" gridBreakPoint="grid-lg">
              <Thead>
                <Tr>
                  <Th>Name</Th>
                  <Th>Status</Th>
                  <Th>Destination</Th>
                  <Th>Points</Th>
                  <Th>Available</Th>
                  <Th modifier="wrap">Untrusted signer</Th>
                  <Th>Synced</Th>
                  <Th modifier="wrap">View expires</Th>
                </Tr>
              </Thead>
              <Tbody>
                {items.map((c) => (
                  <Tr key={c.uid || c.name}>
                    <Td dataLabel="Name">
                      <Link to="/$ns/catalog/$name" params={{ ns, name: c.name }} className="lw-break">
                        {c.name}
                      </Link>
                      <div className="lw-subtle lw-small">
                        {c.mode} sync, {c.intervalSeconds === 0 ? "manual only" : `every ${count(c.intervalSeconds)} s`}
                      </div>
                    </Td>
                    <Td dataLabel="Status">
                      <ReadyStatus c={c} />
                    </Td>
                    <Td dataLabel="Destination" className="lw-break">
                      {whereFrom(c)}
                    </Td>
                    {noViewWords(c) !== null ? (
                      // DUI-40.2: no view, so nothing to count: said once, as the phone list says it.
                      <Td dataLabel="View" colSpan={5} className="lw-subtle">
                        {noViewWords(c)}
                      </Td>
                    ) : (
                      <>
                        <Td dataLabel="Points" className="lw-num">
                          {count(c.counts.total)}
                        </Td>
                        <Td dataLabel="Available" className="lw-num">
                          {count(c.counts.available)}
                        </Td>
                        <Td dataLabel="Untrusted signer" className="lw-num">
                          {count(c.counts.untrustedSigner)}
                        </Td>
                        <Td dataLabel="Synced">
                          <When value={c.syncedAt} />
                        </Td>
                        <Td dataLabel="View expires">
                          <When value={c.viewExpiresAt} />
                        </Td>
                      </>
                    )}
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
          {items.length > 0 && (windowed.length > 0 || !mayConnect) && (
            <Content component="p" className="lw-subtle lw-small lw-ev-note">
              {/* DUI-19: the window sentence only under a view the controller says is a window, as on the catalog's page. */}
              {windowed.length > 0 ? <Prose text={`${windowed.map((c) => c.name).join(", ")}: ${CATALOG_WINDOW_SENTENCE} `} /> : ""}
              {mayConnect ? "" : CONNECT_READ_ONLY_SENTENCE}
            </Content>
          )}
        </Card>
      </PageSection>
    </>
  );
}
