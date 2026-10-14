import "./csp";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import "@patternfly/react-core/dist/styles/base.css";
import { MOCK } from "./api/client";
import { makeQueryClient } from "./app/queryClient";
import { ThemeProvider } from "./app/theme";
import { SessionGate } from "./app/SessionGate";
import { createConsoleRouter } from "./router";
// After every component stylesheet, so the console's few overrides win.
import "./styles.css";

const queryClient = makeQueryClient();

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
