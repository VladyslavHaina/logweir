// Routes. Hash history: the address after `#` is the route, so logweir-api keeps
// serving files only and the sign-in round trip (`next=/ui/#/...`) comes back
// to the same route. The workspace is always in the address, so every link names
// what it shows; a visit with none lands on the session's first workspace.
import {
  Outlet,
  createHashHistory,
  createRootRouteWithContext,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/react-router";
import { landingWorkspace } from "./api/session";
import type { Session } from "./api/types";
import { Shell } from "./app/Shell";
import { Overview } from "./pages/Overview";
import { Restores } from "./pages/restores/RestoreList";
import { RestoreWizard, restoreSearch } from "./pages/restores/RestoreWizard";
import { RestoreDetail } from "./pages/restores/RestoreDetail";
import { NotFound } from "./pages/NotFound";
import { PageError } from "./components/PageError";
import { connectionRoutes } from "./pages/connections/routes";
import { destinationRoutes } from "./pages/destinations/routes";
import { scheduleRoutes } from "./pages/schedules/routes";
import { backupRoutes } from "./pages/backups/routes";
import { approvalRoutes } from "./pages/approvals/routes";
import { cutoverRoutes } from "./pages/cutover/routes";

export type RouterContext = { session: Session };

const root = createRootRouteWithContext<RouterContext>()({ component: Outlet });

const home = createRoute({
  getParentRoute: () => root,
  path: "/",
  beforeLoad: ({ context }) => {
    throw redirect({ to: "/$ws", params: { ws: landingWorkspace(context.session) ?? "default" } });
  },
});

export const wsRoute = createRoute({ getParentRoute: () => root, path: "$ws", component: Shell });

const overview = createRoute({ getParentRoute: () => wsRoute, path: "/", component: Overview });
const restores = createRoute({ getParentRoute: () => wsRoute, path: "restores", component: Restores });
const restoreNew = createRoute({
  getParentRoute: () => wsRoute,
  path: "restores/new",
  component: RestoreWizard,
  validateSearch: restoreSearch,
});
const restore = createRoute({ getParentRoute: () => wsRoute, path: "restores/$name", component: RestoreDetail });
const notFound = createRoute({ getParentRoute: () => wsRoute, path: "$section", component: NotFound });

const routeTree = root.addChildren([
  home,
  wsRoute.addChildren([
    overview,
    ...backupRoutes(wsRoute),
    restores,
    restoreNew,
    restore,
    ...cutoverRoutes(wsRoute),
    ...connectionRoutes(wsRoute),
    ...destinationRoutes(wsRoute),
    ...scheduleRoutes(wsRoute),
    ...approvalRoutes(wsRoute),
    notFound,
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
    // Every route catches its own render errors: one page that cannot be shown leaves the shell (DUI-16).
    defaultErrorComponent: PageError,
  });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof createConsoleRouter>;
  }
}
