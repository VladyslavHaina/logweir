import "./csp";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { MutationCache, QueryCache, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import "@patternfly/react-core/dist/styles/base.css";
import { ApiError, MOCK } from "./api/client";
import { keys } from "./api/queries";
import { ThemeProvider } from "./app/theme";
import { SessionGate } from "./app/SessionGate";
import { createConsoleRouter } from "./router";
// After every component stylesheet, so the console's few overrides win.
import "./styles.css";

// A 401 asking for a sign-in on any later request means the session ended under the
// page: read the session again, and the gate shows the sign-in page.
function onSignedOut(error: unknown, key?: readonly unknown[]) {
  if (error instanceof ApiError && error.signedOut && key?.[0] !== keys.session[0]) {
    void queryClient.resetQueries({ queryKey: keys.session });
  }
}

const queryClient: QueryClient = new QueryClient({
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

const router = createConsoleRouter();

if (MOCK) document.title = "Logweir console (prototype)";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ThemeProvider>
      <QueryClientProvider client={queryClient}>
        <SessionGate>{(session) => <RouterProvider router={router} context={{ session }} />}</SessionGate>
      </QueryClientProvider>
    </ThemeProvider>
  </StrictMode>,
);
