// The Approvals routes, under the workspace route (router.tsx registers them).
import { createRoute } from "@tanstack/react-router";
import type { wsRoute } from "../../router";
import type { RouteClaim } from "../../api/approvals";
import { ApprovalList } from "./ApprovalList";
import { ApprovalPage } from "./ApprovalPage";
import { SwitchoverApprovalPage } from "./SwitchoverApprovalPage";

/** What a link may claim about the Restore it names; the page refuses a claim that disagrees. */
function claim(search: Record<string, unknown>): RouteClaim {
  return {
    ...(typeof search.hash === "string" && search.hash ? { hash: search.hash } : {}),
    ...(typeof search.approval === "string" && search.approval ? { approval: search.approval } : {}),
  };
}

export function approvalRoutes(parent: typeof wsRoute) {
  return [
    createRoute({ getParentRoute: () => parent, path: "approvals", component: ApprovalList }),
    // A Switchover's approval (subject kind Switchover, theme A).
    createRoute({ getParentRoute: () => parent, path: "approvals/switchover/$name", component: SwitchoverApprovalPage }),
    // One Restore's approval (subject kind Restore).
    createRoute({ getParentRoute: () => parent, path: "approvals/$name", component: ApprovalPage, validateSearch: claim }),
  ] as const;
}
