// The Schedules and Protection routes (PROD-18.1 R3), under `#/<ws>/`. The
// router adds them with one line, so page groups built in parallel do not edit
// the same route list twice.
import { createRoute } from "@tanstack/react-router";
import type { wsRoute } from "../../router";
import { SchedulesList } from "./SchedulesList";
import { ScheduleCreate, ScheduleEdit } from "./ScheduleEdit";
import { ScheduleDetail } from "./ScheduleDetail";
import { ProtectionDetail, ProtectionList } from "../protection/Protection";

type Done = { done?: "created" | "saved" };

export function scheduleRoutes(parent: typeof wsRoute) {
  const getParentRoute = () => parent;
  return [
    createRoute({ getParentRoute, path: "schedules", component: SchedulesList }),
    createRoute({ getParentRoute, path: "schedules/new", component: ScheduleCreate }),
    createRoute({
      getParentRoute,
      path: "schedules/$name",
      component: ScheduleDetail,
      validateSearch: (search: Record<string, unknown>): Done =>
        search.done === "created" || search.done === "saved" ? { done: search.done } : {},
    }),
    createRoute({ getParentRoute, path: "schedules/$name/edit", component: ScheduleEdit }),
    createRoute({ getParentRoute, path: "protection", component: ProtectionList }),
    createRoute({ getParentRoute, path: "protection/$name", component: ProtectionDetail }),
  ] as const;
}
