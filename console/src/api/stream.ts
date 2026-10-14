// One run's event stream (GET …/operations/{kind}/{name}/events) into the query cache.
// `operation` and `reset` carry the bare OperationView and replace the cached one;
// `end` says why the CONNECTION closed (`settled`: the run is final; `vanished`: the
// object is gone; `maxDuration`: the server's 300 s bound, so open a new one);
// `heartbeat` only says the connection is alive. The browser reconnects an
// interrupted stream by itself and resumes with Last-Event-ID. Same origin, the
// session cookie, no token in the address.
import { useEffect, useState } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { MOCK } from "./client";
import { keys } from "./queries";
import { useCan } from "./session";
import type { OperationView } from "./types";

type Kind = "backup" | "restore";

export function streamPath(ns: string, kind: Kind, name: string): string {
  return `/api/v1/namespaces/${encodeURIComponent(ns)}/operations/${kind}/${encodeURIComponent(name)}/events`;
}

/** `end` carries `{reason}`; anything else reads as no reason. */
function endReason(data: string): string {
  try {
    return String((JSON.parse(data) as { reason?: unknown }).reason ?? "");
  } catch {
    return "";
  }
}

/** Open the stream and write what it carries into `client`. Returns the close function.
 *  `onOpen(open)` says whether a stream is carrying the run (false once it has ended for good). */
export function followOperation(
  client: QueryClient,
  ns: string,
  kind: Kind,
  name: string,
  onOpen: (open: boolean) => void,
  Source: typeof EventSource = EventSource,
): () => void {
  let source: EventSource | null = null;
  let closed = false;
  const settle = () => {
    void client.invalidateQueries({ queryKey: keys.list(`${kind}s`, ns) });
    void client.invalidateQueries({ queryKey: keys.item(`${kind}s`, ns, name) });
  };
  const close = () => {
    closed = true;
    source?.close();
    onOpen(false);
  };
  const open = () => {
    if (closed) return;
    source = new Source(streamPath(ns, kind, name));
    onOpen(true);
    const take = (event: MessageEvent<string>) => {
      let view: OperationView;
      try {
        view = JSON.parse(event.data) as OperationView;
      } catch {
        return;
      }
      client.setQueryData(keys.operation(ns, kind, name), view);
      if (view.terminal) settle();
    };
    source.addEventListener("operation", take);
    source.addEventListener("reset", take);
    source.addEventListener("end", (event: MessageEvent<string>) => {
      source?.close();
      const reason = endReason(event.data);
      if (reason === "maxDuration") {
        open();
        return;
      }
      if (reason === "settled") settle();
      close();
    });
    source.addEventListener("error", () => {
      // CLOSED means the browser gave up (a refusal, not an interruption): the poll takes over.
      if (source?.readyState === Source.CLOSED) close();
    });
  };
  open();
  return close;
}

/** Follow one run while its page is open. Returns whether the stream is carrying it, so
 *  the operation's poll (queries.ts useOperation) stands down only while it is. */
export function useOperationStream(ns: string, kind: Kind, name: string, active: boolean): boolean {
  const client = useQueryClient();
  const allowed = useCan(ns, "operationEvents");
  const [streaming, setStreaming] = useState(false);
  const usable = active && allowed && !MOCK && typeof EventSource === "function";
  useEffect(() => {
    if (!usable) return undefined;
    return followOperation(client, ns, kind, name, setStreaming);
  }, [client, ns, kind, name, usable]);
  return usable && streaming;
}
