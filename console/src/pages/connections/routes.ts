// The Connections routes, under the namespace route (router.tsx registers them).
import { createRoute } from "@tanstack/react-router";
import type { nsRoute } from "../../router";
import { ConnectionList } from "./ConnectionList";
import { ConnectionDetail } from "./ConnectionDetail";

export function connectionRoutes(parent: typeof nsRoute) {
  return [
    createRoute({ getParentRoute: () => parent, path: "connections", component: ConnectionList }),
    createRoute({ getParentRoute: () => parent, path: "connections/$name", component: ConnectionDetail }),
  ] as const;
}
