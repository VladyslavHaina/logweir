// The R4 routes (PROD-18.1): Backups, the Recovery catalog and Trust, under
// `#/<ns>/`. The router adds them with one line, so page groups built in
// parallel do not edit the same route list twice.
import { createRoute } from "@tanstack/react-router";
import type { nsRoute } from "../../router";
import { Backups, backupsSearch } from "./BackupList";
import { BackupDetail } from "./BackupDetail";
import { CatalogList } from "../catalog/CatalogList";
import { CatalogDetail } from "../catalog/CatalogDetail";
import { Trust } from "../trust/Trust";

export function backupRoutes(parent: typeof nsRoute) {
  const getParentRoute = () => parent;
  return [
    createRoute({ getParentRoute, path: "backups", component: Backups, validateSearch: backupsSearch }),
    createRoute({ getParentRoute, path: "backups/$name", component: BackupDetail }),
    createRoute({ getParentRoute, path: "catalog", component: CatalogList }),
    createRoute({ getParentRoute, path: "catalog/$name", component: CatalogDetail }),
    createRoute({ getParentRoute, path: "trust", component: Trust }),
  ] as const;
}
