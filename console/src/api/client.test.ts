// The live client: the CSRF token on unsafe methods only, every failure an ApiError the
// error state can show, every page of a list, the bounded session read, the sign-in hand-over.
import { afterEach, describe, expect, it } from "vitest";
import { ApiError, call, listAll, makeClient, readSession, setCsrfToken, signInHref, signOut } from "./client";

const BASE = "http://127.0.0.1:8484";

/** A fetch that records each request and answers with `answer(request)`. */
function recording(answer: (request: Request) => Response | Promise<Response>) {
  const seen: Request[] = [];
  const fetch = async (request: Request) => {
    seen.push(request);
    return answer(request);
  };
  return { seen, client: makeClient({ fetch, baseUrl: BASE }) };
}

const json = (status: number, body: unknown, type = "application/json") =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": type } });

const problem = (status: number, code: string, detail: string) =>
  json(status, { status, code, title: code, detail, requestId: "01REQ", retryable: false, type: `https://logweir.dev/problems/${code}` }, "application/problem+json");

afterEach(() => setCsrfToken(undefined));

describe("the CSRF token", () => {
  it("rides on unsafe methods and never on reads", async () => {
    setCsrfToken("tok-123");
    const { seen, client } = recording(() => json(201, { item: { name: "b" }, requestId: "r" }));
    await client.GET("/api/v1/namespaces/{ns}/backups", { params: { path: { ns: "team-a" } } });
    await client.POST("/api/v1/namespaces/{ns}/backups", {
      params: { path: { ns: "team-a" }, header: { "Idempotency-Key": "key-12345678", Origin: BASE } },
      body: { scheduleRef: { name: "orders-hourly" } },
    });
    await client.PUT("/api/v1/namespaces/{ns}/schedules/{name}", {
      params: { path: { ns: "team-a", name: "s" }, header: { Origin: BASE } },
      body: {} as never,
    });
    expect(seen.map((r) => [r.method, r.headers.get("x-csrf-token")])).toEqual([
      ["GET", null],
      ["POST", "tok-123"],
      ["PUT", "tok-123"],
    ]);
    expect(seen[1]!.headers.get("idempotency-key")).toBe("key-12345678");
    expect(seen[1]!.headers.get("content-type")).toBe("application/json");
    expect(seen.every((r) => r.url.startsWith(`${BASE}/api/v1/`))).toBe(true);
  });

  it("is absent while the session has none (localAdmin)", async () => {
    setCsrfToken(null);
    const { seen, client } = recording(() => new Response(null, { status: 204 }));
    await signOut(client);
    expect(seen[0]!.method).toBe("POST");
    expect(seen[0]!.headers.has("x-csrf-token")).toBe(false);
  });
});

describe("errors", () => {
  it("turn a problem document into an ApiError with its status, code and request id", async () => {
    const { client } = recording(() => problem(403, "namespace_forbidden", "team-z is not granted to you."));
    const error = await call(() => client.GET("/api/v1/namespaces/{ns}/backups", { params: { path: { ns: "team-z" } } })).catch((e) => e);
    expect(error).toBeInstanceOf(ApiError);
    expect([error.status, error.code, error.requestId, error.message, error.retryable]).toEqual([
      403,
      "namespace_forbidden",
      "01REQ",
      "team-z is not granted to you.",
      false,
    ]);
    expect(error.signedOut).toBe(false);
  });

  it("say signed out only for the two sign-in codes of a 401", async () => {
    for (const [code, signedOut] of [
      ["unauthenticated", true],
      ["session_expired", true],
      ["forbidden", false],
    ] as const) {
      const { client } = recording(() => problem(401, code, "x"));
      const error = await call(() => client.GET("/api/v1/session")).catch((e) => e);
      expect([code, error.signedOut]).toEqual([code, signedOut]);
    }
  });

  it("turn a body that is not a problem, no answer, and a non-JSON success into ApiErrors", async () => {
    const proxy = recording(() => new Response("<html>bad gateway</html>", { status: 502, headers: { "content-type": "text/html" } }));
    const a = await call(() => proxy.client.GET("/api/v1/session")).catch((e) => e);
    expect([a instanceof ApiError, a.status, a.code]).toEqual([true, 502, "unknown"]);

    const down = recording(() => Promise.reject(new TypeError("Failed to fetch")));
    const b = await call(() => down.client.GET("/api/v1/session")).catch((e) => e);
    expect([b instanceof ApiError, b.status, b.code]).toEqual([true, 0, "no_answer"]);
    expect(b.message).toContain("did not reach the Logweir service");

    const html = recording(() => new Response("<html>login</html>", { status: 200, headers: { "content-type": "text/html" } }));
    const c = await call(() => html.client.GET("/api/v1/session")).catch((e) => e);
    expect([c instanceof ApiError, c.status, c.code]).toEqual([true, 0, "not_the_api"]);
  });

  it("leave a cancelled request cancelled", async () => {
    const { client } = recording(() => Promise.reject(new DOMException("aborted", "AbortError")));
    const error = await call(() => client.GET("/api/v1/session")).catch((e) => e);
    expect(error).toBeInstanceOf(DOMException);
    expect(error.name).toBe("AbortError");
  });
});

describe("lists", () => {
  it("follow nextCursor to the last page", async () => {
    const cursors: (string | undefined)[] = [];
    const items = await listAll("backups", async (cursor) => {
      cursors.push(cursor);
      const n = cursors.length;
      return { items: [n], page: { nextCursor: n < 3 ? `c${n}` : null } };
    });
    expect(items).toEqual([1, 2, 3]);
    expect(cursors).toEqual([undefined, "c1", "c2"]);
  });

  it("refuse a list past the page budget instead of showing a prefix as the whole", async () => {
    const error = await listAll("backups", async () => ({ items: [1], page: { nextCursor: "more" } })).catch((e) => e);
    expect([error instanceof ApiError, error.code]).toEqual([true, "list_too_large"]);
  });
});

describe("the session read", () => {
  it("is an outage when the service does not answer in time", async () => {
    const { client } = recording(() => new Promise<Response>(() => undefined));
    const error = await readSession(client, 20).catch((e) => e);
    expect([error instanceof ApiError, error.status, error.code]).toEqual([true, 0, "no_answer"]);
    expect(error.message).toContain("did not answer within");
  });
});

describe("sign-in", () => {
  it("hands over to the existing route and comes back to the same page and route", () => {
    expect(signInHref({ pathname: "/ui/next/", hash: "#/team-a/backups" })).toBe(
      "/auth/login?next=%2Fui%2Fnext%2F%23%2Fteam-a%2Fbackups",
    );
  });
});
