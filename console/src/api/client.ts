// The one product-API client: typed from schemas/logweir-api-v1.openapi.json,
// same origin, cookie session, the CSRF token on every unsafe request. In the
// prototype its `fetch` is the mock (mock/fetch.ts), so nothing leaves the page.
import createClient, { type Middleware } from "openapi-fetch";
import type { paths } from "./schema";
import type { Problem } from "./types";
import { mockFetch } from "../mock/fetch";

export const PROTOTYPE = import.meta.env.VITE_LOGWEIR_API !== "live";

let csrfToken = "";

/** Kept in memory for the life of the page, never in browser storage. */
export function setCsrfToken(token: string | undefined): void {
  csrfToken = token ?? "";
}

const csrf: Middleware = {
  onRequest({ request }) {
    if (request.method !== "GET" && request.method !== "HEAD" && csrfToken) {
      request.headers.set("X-CSRF-Token", csrfToken);
    }
    return request;
  },
};

export const api = createClient<paths>(
  PROTOTYPE
    ? { baseUrl: "https://logweir.invalid", fetch: mockFetch }
    : { baseUrl: "", credentials: "same-origin" },
);
api.use(csrf);

/** A refusal from the product API, with what the error state shows. */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly requestId: string | undefined;

  constructor(status: number, problem: Partial<Problem> | undefined) {
    super(problem?.detail ?? problem?.title ?? `The service answered ${status}.`);
    this.status = status;
    this.code = String(problem?.code ?? "unknown");
    this.requestId = problem?.requestId ?? undefined;
  }
}

/** openapi-fetch's result to data, or an ApiError. */
export function unwrap<T>(result: { data?: T; error?: unknown; response: Response }): T {
  if (result.data !== undefined && result.response.ok) return result.data;
  throw new ApiError(result.response.status, result.error as Partial<Problem> | undefined);
}

/** The headers every durable create carries. `Origin` is set by the browser. */
export function createHeaders(intent: string) {
  return { "Idempotency-Key": intent, Origin: window.location.origin };
}
