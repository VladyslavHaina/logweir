// Every backup run in the namespace: find one, see whether it is verified, restore it.
import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import {
  Button,
  Card,
  Label,
  PageSection,
  Pagination,
  SearchInput,
  ToggleGroup,
  ToggleGroupItem,
  Toolbar,
  ToolbarContent,
  ToolbarItem,
} from "@patternfly/react-core";
import { ActionsColumn, Table, Tbody, Td, Th, Thead, Tr, type ThProps } from "@patternfly/react-table";
import SearchIcon from "@patternfly/react-icons/dist/esm/icons/search-icon";
import CubesIcon from "@patternfly/react-icons/dist/esm/icons/cubes-icon";
import { useBackups } from "../api/queries";
import type { Backup } from "../api/types";
import { PageHeader } from "../components/PageHeader";
import { RunStatus } from "../components/Status";
import { Empty, ErrorState, LoadingRows } from "../components/States";
import { When, count } from "../components/When";

type Filter = "all" | "verified" | "failed" | "running";

const FILTERS: { id: Filter; label: string }[] = [
  { id: "all", label: "All" },
  { id: "verified", label: "Verified" },
  { id: "failed", label: "Failed" },
  { id: "running", label: "In progress" },
];

function matches(b: Backup, filter: Filter): boolean {
  if (filter === "verified") return b.operation.verifiedSuccess;
  if (filter === "failed") return b.operation.terminal && !b.operation.verifiedSuccess;
  if (filter === "running") return !b.operation.terminal;
  return true;
}

export function TriggerLabel({ b }: { b: Backup }) {
  const kind = b.trigger?.kind ?? (b.triggeredBy === "manual" ? "Manual" : "Scheduled");
  if (kind === "Retry") return <Label isCompact color="orange">Retry {b.trigger?.attempt}</Label>;
  if (kind === "Manual") return <Label isCompact color="purple">Manual</Label>;
  if (kind === "CatchUp") return <Label isCompact color="teal">Catch-up</Label>;
  return <Label isCompact variant="outline">Scheduled</Label>;
}

export function windowText(b: Backup): string {
  if (!b.windowCovered) return b.operation.terminal ? "Not recorded" : "When it finishes";
  const t = (ms: number) => new Date(ms).toISOString().slice(11, 16);
  return `${t(b.windowCovered.fromMs)} to ${t(b.windowCovered.toMs)} UTC`;
}

export function Backups() {
  const { ns } = useParams({ from: "/$ns/backups" });
  const navigate = useNavigate();
  const backups = useBackups(ns);
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState<Filter>("all");
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState(10);
  const [sortDesc, setSortDesc] = useState(true);

  const rows = useMemo(() => {
    const all = backups.data ?? [];
    const needle = search.trim().toLowerCase();
    return all
      .filter((b) => matches(b, filter))
      .filter((b) => !needle || b.name.toLowerCase().includes(needle) || b.sourceRef.name.includes(needle))
      .sort((a, b) => (sortDesc ? -1 : 1) * String(a.createdAt).localeCompare(String(b.createdAt)));
  }, [backups.data, search, filter, sortDesc]);

  const shown = rows.slice((page - 1) * perPage, page * perPage);
  const sortStarted: ThProps["sort"] = {
    sortBy: { index: 6, direction: sortDesc ? "desc" : "asc" },
    onSort: () => setSortDesc(!sortDesc),
    columnIndex: 6,
  };

  return (
    <>
      <PageHeader
        title="Backups"
        description="Every backup run in this namespace. A run is green only when the controller has verified its signed receipt."
        actions={
          <Button variant="primary" onClick={() => void navigate({ to: "/$ns/restores/new", params: { ns }, search: {} })}>
            Restore a point
          </Button>
        }
      />
      <PageSection>
        <Card>
          <Toolbar clearAllFilters={() => { setSearch(""); setFilter("all"); }}>
            <ToolbarContent>
              <ToolbarItem>
                <SearchInput
                  aria-label="Find a backup by name or source"
                  placeholder="Find by name or source"
                  value={search}
                  onChange={(_e, v) => { setSearch(v); setPage(1); }}
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
                      onChange={() => { setFilter(f.id); setPage(1); }}
                    />
                  ))}
                </ToggleGroup>
              </ToolbarItem>
              <ToolbarItem variant="pagination" align={{ default: "alignEnd" }}>
                <Pagination
                  itemCount={rows.length}
                  perPage={perPage}
                  page={page}
                  onSetPage={(_e, p) => setPage(p)}
                  onPerPageSelect={(_e, pp) => { setPerPage(pp); setPage(1); }}
                  isCompact
                  titles={{ paginationAriaLabel: "Backups pages" }}
                />
              </ToolbarItem>
            </ToolbarContent>
          </Toolbar>
          {backups.isPending ? (
            <div className="lw-pad">
              <LoadingRows rows={8} label="Loading backups" />
            </div>
          ) : backups.isError ? (
            <ErrorState error={backups.error} onRetry={() => void backups.refetch()} />
          ) : (backups.data ?? []).length === 0 ? (
            <Empty title="No backups yet" icon={CubesIcon}>
              A schedule creates the first backup at its next slot, or use Back up now on the overview. Runs appear here
              with the evidence the controller recorded.
            </Empty>
          ) : rows.length === 0 ? (
            <Empty
              title="No backup matches"
              icon={SearchIcon}
              actions={
                <Button variant="link" onClick={() => { setSearch(""); setFilter("all"); }}>
                  Clear the filters
                </Button>
              }
            >
              Nothing matches these filters.
            </Empty>
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
                  <Th sort={sortStarted} modifier="fitContent">Started</Th>
                  <Th screenReaderText="Actions" />
                </Tr>
              </Thead>
              <Tbody>
                {shown.map((b) => (
                  <Tr key={b.name} isClickable={false}>
                    <Td dataLabel="Name" className="lw-name-cell">
                      <Link to="/$ns/backups/$name" params={{ ns, name: b.name }} className="lw-break">
                        {b.name}
                      </Link>
                      <div className="lw-subtle lw-small">{b.topics.length} topics: {b.topics.join(", ")}</div>
                    </Td>
                    <Td dataLabel="Status">
                      <RunStatus op={b.operation} />
                    </Td>
                    <Td dataLabel="Trigger">
                      <TriggerLabel b={b} />
                    </Td>
                    <Td dataLabel="Source">{b.sourceRef.name}</Td>
                    <Td dataLabel="Records" className="lw-num">{b.operation.terminal ? count(b.records) : "Counting"}</Td>
                    <Td dataLabel="Covers">{windowText(b)}</Td>
                    <Td dataLabel="Started">
                      <When value={b.createdAt} />
                    </Td>
                    <Td isActionCell>
                      <ActionsColumn
                        items={[
                          { title: "View details", onClick: () => void navigate({ to: "/$ns/backups/$name", params: { ns, name: b.name } }) },
                          {
                            title: "Restore this point",
                            isDisabled: !b.operation.verifiedSuccess,
                            onClick: () => void navigate({ to: "/$ns/restores/new", params: { ns }, search: { backup: b.name } }),
                          },
                        ]}
                      />
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
        </Card>
      </PageSection>
    </>
  );
}
