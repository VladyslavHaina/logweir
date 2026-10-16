// The Destinations routes, under the workspace route (router.tsx registers them).
import { createRoute } from "@tanstack/react-router";
import type { wsRoute } from "../../router";
import { DestinationList } from "./DestinationList";
import { DestinationDetail } from "./DestinationDetail";

export function destinationRoutes(parent: typeof wsRoute) {
  return [
    createRoute({ getParentRoute: () => parent, path: "destinations", component: DestinationList }),
    createRoute({ getParentRoute: () => parent, path: "destinations/$name", component: DestinationDetail }),
  ] as const;
}
