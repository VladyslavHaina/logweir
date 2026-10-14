// Every backup run in the namespace: find one, see whether the controller
// verified it, restore it, or start one now.
import { useMemo, useState } from "react";
import { Link, useNavigate, useParams, useSearch } from "@tanstack/react-router";
import {
  Button,
  Card,
  Content,
  PageSection,
  Pagination,
  SearchInput,
  ToggleGroup,
  ToggleGroupItem,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
} from "@patternfly/react-core";
import { ActionsColumn, Table, Tbody, Td, Th, Thead, Tr, type IAction, type ThProps } from "@patternfly/react-table";
import SearchIcon from "@patternfly/react-icons/dist/esm/icons/search-icon";
import CubesIcon from "@patternfly/react-icons/dist/esm/icons/cubes-icon";
import RedoIcon from "@patternfly/react-icons/dist/esm/icons/redo-icon";
import UserIcon from "@patternfly/react-icons/dist/esm/icons/user-icon";
import HistoryIcon from "@patternfly/react-icons/dist/esm/icons/history-icon";
import OutlinedClockIcon from "@patternfly/react-icons/dist/esm/icons/outlined-clock-icon";
import { useBackupList, type Backup } from "../../api/backups";
import { PageHeader } from "../../components/PageHeader";
import { CompactList, useNarrow } from "../../components/CompactList";
import { Empty, ErrorState, LoadingRows } from "../../components/States";
import { When, count } from "../../components/When";
import { BackUpNow } from "./BackUpNow";
import { BackupStatus, useFlags } from "./parts";
import "./evidence.css";
import { LIST_VERIFIED_NOTE, listVerdict, restoreOffer, triggerWords, windowShort } from "./verdict";

export type Filter = "all" | "verified" | "unverified" | "running";

/** The list's address may name a filter (DUI-31.3: Overview's "runs that did not verify"). */
export function backupsSearch(search: Record<string, unknown>): { filter?: Filter } {
  const f = search.filter;
  return f === "verified" || f === "unverified" || f === "running" ? { filter: f } : {};
}

const FILTERS: { id: Filter; label: string }[] = [
  { id: "all", label: "All" },
  { id: "verified", label: "Verified" },
  { id: "unverified", label: "Not verified" },
  { id: "running", label: "In progress" },
];

export function matches(b: Backup, filter: Filter): boolean {
  const green = listVerdict(b.operation).green;
  if (filter === "verified") return b.operation.terminal && green;
  if (filter === "unverified") return b.operation.terminal && !green;
  if (filter === "running") return !b.operation.terminal;
  return true;
}

/** What a search matches: name, source, schedule, backup set and topics; every term must appear. */
export function searchMatches(b: Backup, query: string): boolean {
  const hay = [b.name, b.sourceRef.name, b.schedule ?? "", b.scheduleRef?.name ?? "", b.backupId ?? "", ...b.topics].join(" ").toLowerCase();
  return query
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .every((term) => hay.includes(term));
}

/** Newest first by creation; a run with no creation instant sorts last, in the order it arrived. */
export function byCreated(rows: Backup[], desc: boolean): Backup[] {
  return rows
    .map((b, i) => ({ b, i }))
    .sort((x, y) => {
      const l = x.b.createdAt ?? "";
      const r = y.b.createdAt ?? "";
      if (l === r) return x.i - y.i;
      if (l === "") return 1;
      if (r === "") return -1;
      return (desc ? -1 : 1) * l.localeCompare(r);
    })
    .map((x) => x.b);
}

export function Trigger({ b }: { b: Backup }) {
  const words = triggerWords(b.trigger);
  if (words === null) return <span className="lw-subtle">Not recorded</span>;
  const Icon = b.trigger?.kind === "Retry" ? RedoIcon : b.trigger?.kind === "Manual" ? UserIcon : b.trigger?.kind === "CatchUp" ? HistoryIcon : OutlinedClockIcon;
  return (
    <span className="lw-inline-icon">
      <Icon aria-hidden="true" />
      {words}
    </span>
  );
}

export function Backups() {
  const { ns } = useParams({ from: "/$ns/backups" });
  const { filter: linked } = useSearch({ from: "/$ns/backups" });
  const navigate = useNavigate();
  const list = useBackupList(ns);
  const flags = useFlags(ns);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<Filter>(linked ?? "all");
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState(10);
  const [sortDesc, setSortDesc] = useState(true);
  const [runNow, setRunNow] = useState(false);
  const narrow = useNarrow();
  const mayRestore = flags?.restoreCreate === true;

  const all = list.data ?? [];
  const rows = useMemo(
    () => byCreated(all.filter((b) => matches(b, filter) && searchMatches(b, search.trim())), sortDesc),
    [all, search, filter, sortDesc],
  );
  const shown = rows.slice((page - 1) * perPage, page * perPage);
  const clear = () => {
    setSearch("");
    setFilter("all");
    setPage(1);
  };
  const pager = (
    <Pagination
      itemCount={rows.length}
      perPage={perPage}
      page={page}
      onSetPage={(_e, p) => setPage(p)}
      onPerPageSelect={(_e, pp) => {
        setPerPage(pp);
        setPage(1);
      }}
      isCompact
      titles={{ paginationAriaLabel: "Backups pages" }}
    />
  );
  const sortStarted: ThProps["sort"] = {
    sortBy: { index: 6, direction: sortDesc ? "desc" : "asc" },
    onSort: () => setSortDesc(!sortDesc),
    columnIndex: 6,
  };
  const actionsFor = (b: Backup): IAction[] => {
    const offer = restoreOffer(b, mayRestore);
    return [
      { title: "View details", onClick: () => void navigate({ to: "/$ns/backups/$name", params: { ns, name: b.name } }) },
      {
        title: "Restore this point",
        isAriaDisabled: !offer.offer,
        tooltipProps: offer.offer ? undefined : { content: offer.reason },
        onClick: () => void navigate({ to: "/$ns/restores/new", params: { ns }, search: { backup: b.name, uid: b.uid } }),
      },
    ];
  };

  return (
    <>
      <PageHeader
        title="Backups"
        description="Every backup run in this namespace. A run is green only when the controller has verified its signed receipt."
        actions={
          <>
            {flags?.manualBackupCreate && (
              <Button variant="secondary" onClick={() => setRunNow(true)}>
                Back up now
              </Button>
            )}{" "}
            {mayRestore && (
              <Button variant="primary" onClick={() => void navigate({ to: "/$ns/restores/new", params: { ns }, search: {} })}>
                Restore a point
              </Button>
            )}
          </>
        }
      />
      {runNow && <BackUpNow ns={ns} open={runNow} onClose={() => setRunNow(false)} />}
      <PageSection>
        <Card>
          <Toolbar clearAllFilters={clear}>
            <ToolbarContent>
              <ToolbarItem className={narrow ? "lw-grow" : undefined}>
                <SearchInput
                  aria-label="Find a backup by name, source, schedule or topic"
                  placeholder="Find by name, source or topic"
                  value={search}
                  onChange={(_e, v) => {
                    setSearch(v);
                    setPage(1);
                  }}
                  onClear={() => setSearch("")}
                />
              </ToolbarItem>
              <ToolbarItem>
                <ToggleGroup aria-label="Show">
                  {FILTERS.map((f) => (
                    <ToggleGroupItem
                      key={f.id}
                      text={f.label}
                      buttonId={`filter-${f.id}`}
                      isSelected={filter === f.id}
                      onChange={() => {
                        setFilter(f.id);
                        setPage(1);
                      }}
                    />
                  ))}
                </ToggleGroup>
              </ToolbarItem>
              {!narrow && (
                <ToolbarItem variant="pagination" align={{ default: "alignEnd" }}>
                  {pager}
                </ToolbarItem>
              )}
            </ToolbarContent>
          </Toolbar>
          {list.isPending ? (
            <div className="lw-pad">
              <LoadingRows rows={8} label="Loading backups" />
            </div>
          ) : list.isError ? (
            <ErrorState error={list.error} onRetry={() => void list.refetch()} />
          ) : all.length === 0 ? (
            <Empty
              title="No backups yet"
              icon={CubesIcon}
              actions={
                flags?.manualBackupCreate ? (
                  <Button variant="secondary" onClick={() => setRunNow(true)}>
                    Back up now
                  </Button>
                ) : undefined
              }
            >
              A schedule creates the first backup at its next slot. Runs appear here with the evidence the controller
              recorded for them.
            </Empty>
          ) : rows.length === 0 ? (
            <Empty
              title="No backup matches"
              icon={SearchIcon}
              actions={
                <Button variant="link" onClick={clear}>
                  Clear the filters
                </Button>
              }
            >
              Nothing matches these filters.
            </Empty>
          ) : narrow ? (
            <>
              <CompactList
                label="Backups"
                items={shown.map((b) => ({
                  key: b.uid || b.name,
                  href: `#/${ns}/backups/${b.name}`,
                  title: b.name,
                  meta: (
                    <>
                      <When value={b.createdAt} /> · {(triggerWords(b.trigger) ?? "trigger not recorded").toLowerCase()} · {b.sourceRef.name}
                    </>
                  ),
                  status: <BackupStatus b={b} />,
                }))}
              />
              <div className="lw-pager">{pager}</div>
            </>
          ) : (
            <Table aria-label="Backups" variant="compact" gridBreakPoint="grid-lg">
              <Thead>
                <Tr>
                  <Th>Name</Th>
                  <Th>Status</Th>
                  <Th>Trigger</Th>
                  <Th>Source</Th>
                  <Th>Records</Th>
                  <Th>Covers</Th>
                  <Th sort={sortStarted} modifier="fitContent">
                    Started
                  </Th>
                  <Th screenReaderText="Actions" />
                </Tr>
              </Thead>
              <Tbody>
                {shown.map((b) => (
                  <Tr key={b.uid || b.name}>
                    <Td dataLabel="Name" className="lw-name-cell">
                      <Link to="/$ns/backups/$name" params={{ ns, name: b.name }} className="lw-break">
                        {b.name}
                      </Link>
                      <div className="lw-subtle lw-small">
                        {b.topics.length} {b.topics.length === 1 ? "topic" : "topics"}: {b.topics.join(", ")}
                      </div>
                    </Td>
                    <Td dataLabel="Status">
                      <BackupStatus b={b} withDetail />
                    </Td>
                    <Td dataLabel="Trigger">
                      <Trigger b={b} />
                    </Td>
                    <Td dataLabel="Source">{b.sourceRef.name}</Td>
                    <Td dataLabel="Records" className="lw-num">
                      {b.operation.terminal ? count(b.records) : "Counting"}
                    </Td>
                    <Td dataLabel="Covers">{windowShort(b)}</Td>
                    <Td dataLabel="Started">
                      <When value={b.createdAt} />
                    </Td>
                    <Td isActionCell>
                      <ActionsColumn items={actionsFor(b)} />
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
          {all.length > 0 && (
            <Content component="p" className="lw-subtle lw-small lw-ev-note">
              {LIST_VERIFIED_NOTE}
            </Content>
          )}
        </Card>
      </PageSection>
    </>
  );
}
