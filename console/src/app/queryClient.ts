// The page's one query client (main.tsx makes it; the session rows make their own).
import { MutationCache, QueryCache, QueryClient } from "@tanstack/react-query";
import { ApiError } from "../api/client";
import { keys } from "../api/queries";

/**
 * A 401 asking for a sign-in on any request after the boot read means the session ended
 * under the page. The session is read again, and the gate (SessionGate.tsx) keeps the
 * page mounted, with what was typed, and shows its "session ended" dialog over it
 * (DUI-4). Once that read says signed out, later 401s ask nothing more: the dialog's
 * own "Continue here" and coming back to the tab read it again.
 */
export function makeQueryClient(): QueryClient {
  const client: QueryClient = new QueryClient({
    queryCache: new QueryCache({ onError: (error, query) => onSignedOut(error, query.queryKey) }),
    mutationCache: new MutationCache({ onError: (error) => onSignedOut(error) }),
    defaultOptions: {
      queries: {
        staleTime: 10_000,
        refetchOnWindowFocus: true,
        // A refusal is an answer: only no answer, or a retryable server failure, is tried once more.
        retry: (count, error) =>
          count < 1 && (!(error instanceof ApiError) || error.status === 0 || (error.status >= 500 && error.retryable)),
      },
      mutations: { retry: false },
    },
  });

  function onSignedOut(error: unknown, key?: readonly unknown[]) {
    if (!(error instanceof ApiError) || !error.signedOut || key?.[0] === keys.session[0]) return;
    const known = client.getQueryState(keys.session)?.error;
    if (known instanceof ApiError && known.signedOut) return;
    // Not reset: a reset drops the session document, and the gate would unmount the page.
    void client.invalidateQueries({ queryKey: keys.session }, { cancelRefetch: false });
  }

  return client;
}
