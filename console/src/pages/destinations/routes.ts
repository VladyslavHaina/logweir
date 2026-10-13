// The Destinations routes, under the namespace route (router.tsx registers them).
import { createRoute } from "@tanstack/react-router";
import type { nsRoute } from "../../router";
import { DestinationList } from "./DestinationList";
import { DestinationDetail } from "./DestinationDetail";

export function destinationRoutes(parent: typeof nsRoute) {
  return [
    createRoute({ getParentRoute: () => parent, path: "destinations", component: DestinationList }),
    createRoute({ getParentRoute: () => parent, path: "destinations/$name", component: DestinationDetail }),
  ] as const;
}
