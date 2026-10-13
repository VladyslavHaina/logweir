// Routes. Hash history: the address after `#` is the route, so logweir-api keeps
// serving files only and the sign-in round trip (`next=/ui/next/#/...`) comes back
// to the same route. The namespace is always in the address, so every link names
// what it shows; a visit with none lands on the session's first namespace.
import {
  Outlet,
  createHashHistory,
  createRootRouteWithContext,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/react-router";
import { landingNamespace } from "./api/session";
import type { Session } from "./api/types";
import { Shell } from "./app/Shell";
import { Overview } from "./pages/Overview";
import { Restores } from "./pages/Restores";
import { RestoreWizard } from "./pages/RestoreWizard";
import { RestoreDetail } from "./pages/RestoreDetail";
import { Cutover } from "./pages/Cutover";
import { Planned } from "./pages/Planned";
import { connectionRoutes } from "./pages/connections/routes";
import { destinationRoutes } from "./pages/destinations/routes";
import { scheduleRoutes } from "./pages/schedules/routes";
import { backupRoutes } from "./pages/backups/routes";
import { approvalRoutes } from "./pages/approvals/routes";

export type RouterContext = { session: Session };

/** Pages still drawn on the mock: shown in the mock builds; in a live build each waits
 *  for its rebuild run (R2-R7), which drops its `PROTOTYPE ?` and makes it real. The test
 *  is inline (not a helper) so a live build drops the page and the mock it reads. */
const PROTOTYPE = import.meta.env.VITE_LOGWEIR_API !== "live";

const root = createRootRouteWithContext<RouterContext>()({ component: Outlet });

const home = createRoute({
  getParentRoute: () => root,
  path: "/",
  beforeLoad: ({ context }) => {
    throw redirect({ to: "/$ns", params: { ns: landingNamespace(context.session) ?? "default" } });
  },
});

export const nsRoute = createRoute({ getParentRoute: () => root, path: "$ns", component: Shell });

const overview = createRoute({ getParentRoute: () => nsRoute, path: "/", component: Overview });
const restores = createRoute({ getParentRoute: () => nsRoute, path: "restores", component: PROTOTYPE ? Restores : Planned });
const restoreNew = createRoute({
  getParentRoute: () => nsRoute,
  path: "restores/new",
  component: PROTOTYPE ? RestoreWizard : Planned,
  validateSearch: (search: Record<string, unknown>): { backup?: string } =>
    typeof search.backup === "string" ? { backup: search.backup } : {},
});
const restore = createRoute({ getParentRoute: () => nsRoute, path: "restores/$name", component: PROTOTYPE ? RestoreDetail : Planned });
const cutover = createRoute({ getParentRoute: () => nsRoute, path: "restores/$name/cutover", component: PROTOTYPE ? Cutover : Planned });
const planned = createRoute({ getParentRoute: () => nsRoute, path: "$section", component: Planned });

const routeTree = root.addChildren([
  home,
  nsRoute.addChildren([
    overview,
    ...backupRoutes(nsRoute),
    restores,
    restoreNew,
    restore,
    cutover,
    ...connectionRoutes(nsRoute),
    ...destinationRoutes(nsRoute),
    ...scheduleRoutes(nsRoute),
    ...approvalRoutes(nsRoute),
    planned,
  ]),
]);

/** The page's router, made once by main.tsx (and afresh by each test that renders the console). */
export function createConsoleRouter() {
  return createRouter({
    routeTree,
    history: createHashHistory(),
    // Filled in by <RouterProvider context> once the session gate has a session.
    context: { session: undefined! },
    defaultPreload: false,
    scrollRestoration: true,
  });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof createConsoleRouter>;
  }
}
