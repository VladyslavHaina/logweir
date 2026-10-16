// The one product-API client: typed from schemas/logweir-api-v1.openapi.json, same
// origin, the browser's session cookie, and the CSRF token from GET /api/v1/session
// on every unsafe request. Built with VITE_LOGWEIR_API=live (every `vite build`) it
// talks to /api/v1 on the origin that served the page; otherwise (the single-file
// demo, `vite dev`, the tests) its fetch is the in-page mock and nothing leaves the page.
import createClient, { type Client, type Middleware } from "openapi-fetch";
import type { paths } from "./schema";
import type { FieldError, PolicyChangedDetail, Problem } from "./types";
import { mockFetch } from "../mock/fetch";

export const MOCK = import.meta.env.VITE_LOGWEIR_API !== "live";

/** The two codes of a 401 that mean "sign in" (docs/api.md, *The session and the CSRF token*). */
export const SIGN_IN_CODES: readonly string[] = ["unauthenticated", "session_expired"];

/** How long the boot read of the session may take before the service counts as unavailable. */
export const SESSION_TIMEOUT_MS = 5000;

/** The page size lists ask for (the API's maximum), and how many pages one list may follow. */
export const LIST_PAGE_SIZE = 200;
export const LIST_PAGE_BUDGET = 25;

const SAFE_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);

let csrfToken = "";

/** Kept in memory for the life of the page, never in browser storage. */
export function setCsrfToken(token: string | null | undefined): void {
  csrfToken = token ?? "";
}

/** `X-CSRF-Token` on every unsafe request, and on nothing else. */
export const csrf: Middleware = {
  onRequest({ request }) {
    if (!SAFE_METHODS.has(request.method) && csrfToken) {
      request.headers.set("X-CSRF-Token", csrfToken);
    }
    return request;
  },
};

export type ApiClient = Client<paths>;

/** A client over `fetch` (the browser's when absent). Tests hand in their own. */
export function makeClient(options: { fetch?: (request: Request) => Promise<Response>; baseUrl?: string } = {}): ApiClient {
  const client = createClient<paths>({
    baseUrl: options.baseUrl ?? "",
    credentials: "same-origin",
    ...(options.fetch ? { fetch: options.fetch } : {}),
  });
  client.use(csrf);
  return client;
}

export const api: ApiClient = MOCK ? makeClient({ baseUrl: "https://logweir.invalid", fetch: mockFetch }) : makeClient();

/** A refusal from the product API, or no answer at all (`status` 0), with what the error state shows. */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly requestId: string | undefined;
  /** False only when the API said repeating the same request cannot succeed. */
  readonly retryable: boolean;
  /** A refusal's field errors (`errors[]`), so a form places each beside the input it names. */
  readonly fieldErrors: FieldError[];
  /** For `policy_changed`: the schedule revision in force now. */
  readonly policy: PolicyChangedDetail | undefined;

  constructor(status: number, problem: Partial<Problem> | string | undefined) {
    const doc = typeof problem === "object" && problem !== null ? problem : undefined;
    super(doc?.detail ?? doc?.title ?? `The service answered ${status} without a problem document.`);
    this.name = "ApiError";
    this.status = status;
    this.code = String(doc?.code ?? (status === 0 ? "no_answer" : "unknown"));
    this.requestId = doc?.requestId ?? undefined;
    this.retryable = doc?.retryable ?? true;
    this.fieldErrors = Array.isArray(doc?.errors) ? doc.errors : [];
    this.policy = doc?.policy ?? undefined;
  }

  /** A 401 the product API gave because nobody is signed in, or the session ended. */
  get signedOut(): boolean {
    return this.status === 401 && SIGN_IN_CODES.includes(this.code);
  }
}

function noAnswer(code: string, detail: string): ApiError {
  return new ApiError(0, { code: code as Problem["code"], detail });
}

/** openapi-fetch's result to data, or an ApiError. */
export function unwrap<T>(result: { data?: T; error?: unknown; response: Response }): T {
  if (result.response.ok && result.data !== undefined) return result.data;
  if (result.response.ok) {
    throw noAnswer("empty_answer", `The service answered ${result.response.status} with no document.`);
  }
  throw new ApiError(result.response.status, result.error as Partial<Problem> | string | undefined);
}

/** Issue one request and unwrap it. A request that never got an answer is an ApiError with
 *  status 0; a cancelled one (the query was dropped) stays an AbortError for TanStack Query. */
export async function call<T>(request: () => Promise<{ data?: T; error?: unknown; response: Response }>): Promise<T> {
  let result: { data?: T; error?: unknown; response: Response };
  try {
    result = await request();
  } catch (cause) {
    if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
    if (cause instanceof SyntaxError) {
      throw noAnswer("not_the_api", "The service answered with something that is not the Logweir product API.");
    }
    throw noAnswer("no_answer", `The request did not reach the Logweir service (${cause instanceof Error ? cause.message : String(cause)}).`);
  }
  return unwrap(result);
}

/** The session document, bounded: no answer in SESSION_TIMEOUT_MS is an outage, not a wait. */
export async function readSession(client: ApiClient = api, timeoutMs = SESSION_TIMEOUT_MS) {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const late = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      controller.abort();
      reject(noAnswer("no_answer", `The Logweir service did not answer within ${timeoutMs / 1000} seconds.`));
    }, timeoutMs);
  });
  try {
    return await Promise.race([call(() => client.GET("/api/v1/session", { signal: controller.signal })), late]);
  } finally {
    clearTimeout(timer);
  }
}

/** Every page of a list, following `nextCursor`. A workspace larger than the budget is an
 *  error the page shows; a prefix is never presented as the whole. */
export async function listAll<T>(
  what: string,
  read: (cursor: string | undefined) => Promise<{ items: T[]; page: { nextCursor?: string | null } }>,
): Promise<T[]> {
  const items: T[] = [];
  let cursor: string | undefined;
  for (let pages = 0; pages < LIST_PAGE_BUDGET; pages += 1) {
    const page = await read(cursor);
    items.push(...page.items);
    if (!page.page.nextCursor) return items;
    cursor = page.page.nextCursor;
  }
  throw noAnswer(
    "list_too_large",
    `This workspace holds more than ${items.length} ${what}, more than the console reads in one list. It is not showing the first ${items.length} as if they were all.`,
  );
}

/** A failed write whose outcome is unknown: no answer, a timeout or a server failure. The
 *  object may or may not exist, and only resending the same Idempotency-Key can tell. A
 *  refusal the page made before sending anything is known: nothing was sent. */
export function outcomeUnknown(error: unknown): boolean {
  if (error instanceof LocalRefusal) return false;
  return !(error instanceof ApiError) || error.status === 0 || error.status === 408 || error.status >= 500;
}

/** Nothing was sent: the page refused before any request. */
export class LocalRefusal extends Error {}

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

/** The headers every durable create carries. The browser sets `Origin` itself; the
 *  typed route names it, so it is passed through unchanged. */
export function createHeaders(idempotencyKey: string) {
  return { "Idempotency-Key": idempotencyKey, Origin: window.location.origin };
}

/** The existing sign-in route, coming back to this page and route afterwards. The API
 *  accepts `next` only as a path under /ui/ (crates/logweir-api/src/auth/login.rs). */
export function signInHref(location: { pathname: string; hash: string } = window.location): string {
  return `/auth/login?next=${encodeURIComponent(location.pathname + location.hash)}`;
}

/** Sign out: the unsafe POST, with the CSRF token, then the caller reloads. */
export async function signOut(client: ApiClient = api): Promise<void> {
  const { response, error } = await client.POST("/api/v1/session/logout", {
    params: { header: { Origin: window.location.origin } },
    body: {} as never,
  });
  // A session that already ended is signed out either way.
  if (!response.ok && response.status !== 401) throw new ApiError(response.status, error as Partial<Problem>);
}
