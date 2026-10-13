// Routes. Hash history: the address after `#` is the route, so logweir-api keeps
// serving files only and the sign-in round trip (`next=/ui/#/...`) works as today.
// The namespace is always in the address, so every link names what it shows.
import {
  Outlet,
  createHashHistory,
  createRootRoute,
  createRoute,
  createRouter,
  redirect,
} from "@tanstack/react-router";
import { Shell } from "./app/Shell";
import { Overview } from "./pages/Overview";
import { Restores } from "./pages/Restores";
import { RestoreWizard } from "./pages/RestoreWizard";
import { RestoreDetail } from "./pages/RestoreDetail";
import { Cutover } from "./pages/Cutover";
import { NotInPrototype } from "./pages/NotInPrototype";
import { backupRoutes } from "./pages/backups/routes";

const root = createRootRoute({ component: Outlet });

const home = createRoute({
  getParentRoute: () => root,
  path: "/",
  beforeLoad: () => {
    throw redirect({ to: "/$ns", params: { ns: "team-a" } });
  },
});

export const nsRoute = createRoute({ getParentRoute: () => root, path: "$ns", component: Shell });

const overview = createRoute({ getParentRoute: () => nsRoute, path: "/", component: Overview });
const restores = createRoute({ getParentRoute: () => nsRoute, path: "restores", component: Restores });
const restoreNew = createRoute({
  getParentRoute: () => nsRoute,
  path: "restores/new",
  component: RestoreWizard,
  validateSearch: (search: Record<string, unknown>): { backup?: string } =>
    typeof search.backup === "string" ? { backup: search.backup } : {},
});
const restore = createRoute({ getParentRoute: () => nsRoute, path: "restores/$name", component: RestoreDetail });
const cutover = createRoute({ getParentRoute: () => nsRoute, path: "restores/$name/cutover", component: Cutover });
const planned = createRoute({ getParentRoute: () => nsRoute, path: "$section", component: NotInPrototype });

const routeTree = root.addChildren([
  home,
  nsRoute.addChildren([overview, ...backupRoutes(nsRoute), restores, restoreNew, restore, cutover, planned]),
]);

export const router = createRouter({
  routeTree,
  history: createHashHistory(),
  defaultPreload: false,
  scrollRestoration: true,
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
