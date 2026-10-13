// The cutover routes, under the namespace route (router.tsx registers them): the flow
// from a restore, and Recover → Switchovers with one page per Switchover.
import { createRoute } from "@tanstack/react-router";
import type { nsRoute } from "../../router";
import { Cutover } from "./Cutover";
import { SwitchoverList } from "./SwitchoverList";
import { SwitchoverPage } from "./SwitchoverPage";

/** `?switchover=<name>`: the flow's third step, the Switchover the confirm created. */
function cutoverSearch(search: Record<string, unknown>): { switchover?: string } {
  return typeof search.switchover === "string" && search.switchover ? { switchover: search.switchover } : {};
}

export function cutoverRoutes(parent: typeof nsRoute) {
  return [
    createRoute({ getParentRoute: () => parent, path: "restores/$name/cutover", component: Cutover, validateSearch: cutoverSearch }),
    createRoute({ getParentRoute: () => parent, path: "switchovers", component: SwitchoverList }),
    createRoute({ getParentRoute: () => parent, path: "switchovers/$name", component: SwitchoverPage }),
  ] as const;
}
