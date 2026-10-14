// Routes. Hash history: the address after `#` is the route, so logweir-api keeps
// serving files only and the sign-in round trip (`next=/ui/#/...`) comes back
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
import { Restores } from "./pages/restores/RestoreList";
import { RestoreWizard, restoreSearch } from "./pages/restores/RestoreWizard";
import { RestoreDetail } from "./pages/restores/RestoreDetail";
import { NotFound } from "./pages/NotFound";
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
    throw redirect({ to: "/$ns", params: { ns: landingNamespace(context.session) ?? "default" } });
  },
});

export const nsRoute = createRoute({ getParentRoute: () => root, path: "$ns", component: Shell });

const overview = createRoute({ getParentRoute: () => nsRoute, path: "/", component: Overview });
const restores = createRoute({ getParentRoute: () => nsRoute, path: "restores", component: Restores });
const restoreNew = createRoute({
  getParentRoute: () => nsRoute,
  path: "restores/new",
  component: RestoreWizard,
  validateSearch: restoreSearch,
});
const restore = createRoute({ getParentRoute: () => nsRoute, path: "restores/$name", component: RestoreDetail });
const notFound = createRoute({ getParentRoute: () => nsRoute, path: "$section", component: NotFound });

const routeTree = root.addChildren([
  home,
  nsRoute.addChildren([
    overview,
    ...backupRoutes(nsRoute),
    restores,
    restoreNew,
    restore,
    ...cutoverRoutes(nsRoute),
    ...connectionRoutes(nsRoute),
    ...destinationRoutes(nsRoute),
    ...scheduleRoutes(nsRoute),
    ...approvalRoutes(nsRoute),
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
  });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof createConsoleRouter>;
  }
}
