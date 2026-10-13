// Backups, the recovery catalog and trust (PROD-18.1 R4): every read and write
// those pages make, over the typed client. The backup list and one backup share
// the overview's query keys (`["backups", ns, ...]`), so a write anywhere
// refreshes both.
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, api, unwrap } from "./client";
import type { components } from "./schema";

type S = components["schemas"];
type Problem = S["Problem"];

export type Backup = S["Backup"];
export type OperationView = S["OperationView"];
export type OperationSummary = S["OperationSummary"];
export type ManualBackupResponse = S["ManualBackupResponse"];
export type Destination = S["Destination"];
export type DestinationSummary = S["DestinationSummary"];
export type Catalog = S["CatalogView"];
export type PointView = S["PointView"];
export type PointPage = S["PointPageResponse"];
export type SignerPage = S["SignerPageResponse"];
export type ConnectArchiveRequest = S["ConnectArchiveRequest"];
export type CatalogResponse = S["CatalogResponse"];
export type TrustPolicy = S["TrustPolicyView"];
export type TrustKey = S["TrustKeyView"];
export type FieldError = S["FieldError"];

const LIVE_MS = 1500;
/** How many list pages one read follows. 25 pages of 200 is 5,000 runs; past it the page says so. */
const MAX_PAGES = 25;

// ------------------------------------------------------------ refusals

/** An ApiError that keeps what a form needs: the field errors and, for `policy_changed`, the revision in force. */
export class ProblemError extends ApiError {
  readonly fieldErrors: FieldError[];
  readonly policy: S["PolicyChangedDetail"] | undefined;

  constructor(status: number, problem: Partial<Problem> | undefined) {
    super(status, problem);
    this.fieldErrors = Array.isArray(problem?.errors) ? problem.errors : [];
    this.policy = problem?.policy ?? undefined;
  }
}

function unwrapProblem<T>(result: { data?: T; error?: unknown; response: Response }): T {
  if (result.data !== undefined && result.response.ok) return result.data;
  throw new ProblemError(result.response.status, result.error as Partial<Problem> | undefined);
}

/** Nothing was sent: this page refused before any request. */
export class LocalRefusal extends Error {}

/**
 * What became of a write (the old console's `failureKind`):
 * - `unknown`: no answer, a timeout or a 5xx, so the object may or may not exist; resending the same key is safe;
 * - `conflict`: 409; `invalid`: 400 or 422; `refused`: any other answer; `local`: nothing was sent.
 */
export type Outcome = "unknown" | "conflict" | "invalid" | "refused" | "local";

export function outcomeOf(error: unknown): Outcome {
  if (error instanceof LocalRefusal) return "local";
  if (!(error instanceof ApiError)) return "unknown";
  if (error.status === 0 || error.status === 408 || error.status >= 500) return "unknown";
  if (error.status === 409) return "conflict";
  if (error.status === 400 || error.status === 422) return "invalid";
  return "refused";
}

/**
 * A fresh `Idempotency-Key`: `logweir-console.<kind>.` and 32 random hex characters. With no
 * random source the page refuses rather than count, which would replay one page load's write in the next.
 */
export function mintKey(kind: string): string {
  const source = globalThis.crypto;
  if (!source || typeof source.getRandomValues !== "function") {
    throw new LocalRefusal(
      "This page will not send this: the idempotency key that makes a double click or a retry one request needs the platform's random source, and it is unavailable.",
    );
  }
  const hex = Array.from(source.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, "0")).join("");
  return `logweir-console.${kind}.${hex}`;
}

function writeHeaders(key: string) {
  return { "Idempotency-Key": key, Origin: window.location.origin };
}

// ------------------------------------------------------------- listing

export interface AllPages<T> {
  items: T[];
  /** True when the list stopped at MAX_PAGES with a cursor left: the page says it shows the first part. */
  partial: boolean;
}

type ListPage<T> = { items: T[]; page: { nextCursor?: string | null } };

/** Every page of a list, following `nextCursor` up to MAX_PAGES. */
export async function readAll<T>(read: (cursor: string | undefined) => Promise<ListPage<T>>): Promise<AllPages<T>> {
  const items: T[] = [];
  let cursor: string | undefined;
  for (let n = 0; n < MAX_PAGES; n += 1) {
    const page = await read(cursor);
    items.push(...page.items);
    cursor = page.page.nextCursor ?? undefined;
    if (!cursor) return { items, partial: false };
  }
  return { items, partial: true };
}

// ------------------------------------------------------------- backups

/** Every Backup in the namespace, newest first is the page's job; re-read while one is moving. */
export function useBackupList(ns: string) {
  return useQuery({
    queryKey: ["backups", ns, "*all"],
    queryFn: ({ signal }) =>
      readAll(async (cursor) =>
        unwrap(
          await api.GET("/api/v1/namespaces/{ns}/backups", {
            params: { path: { ns }, query: { limit: 200, ...(cursor ? { cursor } : {}) } },
            signal,
          }),
        ),
      ),
    refetchInterval: (q) => (q.state.data?.items.some((b) => !b.operation.terminal) ? LIVE_MS : false),
  });
}

function notFound(error: unknown): boolean {
  return error instanceof ApiError && error.status === 404;
}

export function useBackupDoc(ns: string, name: string) {
  return useQuery({
    queryKey: ["backups", ns, name],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/backups/{name}", { params: { path: { ns, name } }, signal })).item,
    refetchInterval: (q) => (q.state.data && !q.state.data.operation.terminal ? LIVE_MS : false),
    retry: (count, error) => !notFound(error) && count < 2,
  });
}

/** How often a terminal run whose verdict is still pending is read again, and for how long. */
const SETTLE_MS = 5000;
const SETTLE_READS = 60;

/** The run's operation view: stage, diagnoses, result, evidence. Read until it is terminal and its verdict has settled. */
export function useBackupOperation(ns: string, name: string) {
  return useQuery({
    queryKey: ["operations", ns, "backup", name],
    queryFn: async ({ signal }) =>
      unwrap(
        await api.GET("/api/v1/namespaces/{ns}/operations/{kind}/{name}", {
          params: { path: { ns, kind: "backup", name } },
          signal,
        }),
      ).item,
    refetchInterval: (q) => {
      const op = q.state.data;
      if (!op) return false;
      if (!op.terminal) return LIVE_MS;
      return op.verification?.state === "pending" && q.state.dataUpdateCount < SETTLE_READS ? SETTLE_MS : false;
    },
    retry: (count, error) => !notFound(error) && count < 2,
  });
}

/** The destination a run names, read for its bucket. A refusal is the check's caveat, not the page's error. */
export function useRunDestination(ns: string, name: string | undefined) {
  return useQuery({
    queryKey: ["destinations", ns, name ?? ""],
    queryFn: async ({ signal }) =>
      unwrap(
        await api.GET("/api/v1/namespaces/{ns}/destinations/{name}", { params: { path: { ns, name: name! } }, signal }),
      ).item,
    enabled: Boolean(name),
    retry: false,
  });
}

export interface RunNow {
  schedule: string;
  /** The revision this page showed; a different one now is `409 policy_changed`, never a run under a policy nobody saw. */
  generation?: number | null;
  key: string;
}

/** Back up now from one schedule: one run per key, whatever happens to the answer. */
export function useBackUpNow(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ schedule, generation, key }: RunNow): Promise<ManualBackupResponse> =>
      unwrapProblem(
        await api.POST("/api/v1/namespaces/{ns}/backups", {
          params: { path: { ns }, header: writeHeaders(key) },
          body: { scheduleRef: { name: schedule, ...(typeof generation === "number" ? { expectedGeneration: generation } : {}) } },
        }),
      ),
    onSettled: () => client.invalidateQueries({ queryKey: ["backups", ns] }),
  });
}

// -------------------------------------------------------------- catalog

export function useCatalogs(ns: string) {
  return useQuery({
    queryKey: ["catalogs", ns],
    queryFn: ({ signal }) =>
      readAll(async (cursor) =>
        unwrap(
          await api.GET("/api/v1/namespaces/{ns}/catalogs", {
            params: { path: { ns }, query: { limit: 200, ...(cursor ? { cursor } : {}) } },
            signal,
          }),
        ),
      ),
  });
}

export function useCatalog(ns: string, name: string) {
  return useQuery({
    queryKey: ["catalogs", ns, name],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/catalogs/{name}", { params: { path: { ns, name } }, signal })).item,
    retry: (count, error) => !notFound(error) && count < 2,
  });
}

/** One bounded page of the catalog's view at a time (200 points), the next only when asked for. */
export function useCatalogPoints(ns: string, name: string, selectableOnly: boolean, enabled = true) {
  return useInfiniteQuery({
    enabled,
    queryKey: ["points", ns, name, selectableOnly ? "selectable" : "all"],
    initialPageParam: undefined as string | undefined,
    queryFn: async ({ pageParam, signal }) =>
      unwrap(
        await api.GET("/api/v1/namespaces/{ns}/catalogs/{name}/points", {
          params: {
            path: { ns, name },
            query: {
              limit: 200,
              ...(pageParam ? { cursor: pageParam } : {}),
              ...(selectableOnly ? { selectable: "true" as const } : {}),
            },
          },
          signal,
        }),
      ),
    getNextPageParam: (last) => last.page.nextCursor ?? undefined,
    retry: false,
  });
}

export function useCatalogSigners(ns: string, name: string) {
  return useQuery({
    queryKey: ["catalogs", ns, name, "signers"],
    queryFn: async ({ signal }) =>
      unwrap(await api.GET("/api/v1/namespaces/{ns}/catalogs/{name}/signers", { params: { path: { ns, name } }, signal })),
    retry: false,
  });
}

/** The namespace's saved destinations, for the connect form's pick-list. A failed read leaves a text box. */
export function useDestinationChoices(ns: string) {
  return useQuery({
    queryKey: ["destinations", ns, "*choices"],
    queryFn: ({ signal }) =>
      readAll(async (cursor) =>
        unwrap(
          await api.GET("/api/v1/namespaces/{ns}/destinations", {
            params: { path: { ns }, query: { limit: 200, ...(cursor ? { cursor } : {}) } },
            signal,
          }),
        ),
      ),
    retry: false,
  });
}

/** Connect an existing archive: one RecoveryCatalog per key. */
export function useConnectArchive(ns: string) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async ({ body, key }: { body: ConnectArchiveRequest; key: string }): Promise<CatalogResponse> =>
      unwrapProblem(
        await api.POST("/api/v1/namespaces/{ns}/catalogs", { params: { path: { ns }, header: writeHeaders(key) }, body }),
      ),
    onSettled: () => client.invalidateQueries({ queryKey: ["catalogs", ns] }),
  });
}

// ---------------------------------------------------------------- trust

/** The installation's trust policies this reader administers (cluster-scoped; no namespace). */
export function useTrustPolicies(enabled = true) {
  return useQuery({
    enabled,
    queryKey: ["trust-policies"],
    queryFn: ({ signal }) =>
      readAll(async (cursor) =>
        unwrap(await api.GET("/api/v1/trust-policies", { params: { query: { limit: 200, ...(cursor ? { cursor } : {}) } }, signal })),
      ),
    retry: (count, error) => !(error instanceof ApiError && (error.status === 403 || error.status === 404)) && count < 2,
  });
}
