// Binding the wizard to ONE recovery point: a Backup (bound to its catalog row when a
// catalog over its archive lists its receipt) or a catalog point, with its frozen
// destination checked against the live one. A read that FAILS throws; "could not read"
// is never "refused", and a refusal is never "the newest one instead".
import { findCatalogPoint, listCatalogs, readBackupReceiptDigest, readCatalog, readDestination } from "../../api/restores";
import {
  backupPoint,
  catalogPoint,
  catalogPointOffer,
  destinationStore,
  frozenDestinationProblem,
  isRecoveryPoint,
  type Backup,
  type Catalog,
  type Destination,
  type PointView,
  type RecoveryPoint,
} from "./points";

/** The most recovery catalogs a Backup's row is looked for in. */
export const MAX_SOURCE_CATALOGS = 4;

export type Resolved =
  | { state: "selected"; point: RecoveryPoint; destination: Destination | null; destinationProblem: string | null; unboundReason: string | null }
  | { state: "refused"; reason: string };

/** The catalog row a Backup's restore is bound to, or why there is none: its own receipt
 *  (by digest and set), in a catalog over the run's own destination or inline archive. */
export async function bindBackup(ws: string, b: Backup, signal?: AbortSignal): Promise<{ row: { name: string; entry: PointView } | null; reason: string }> {
  const own = b.operation.verificationState;
  if (own !== "valid" && own !== "notAttempted") {
    return { row: null, reason: `the controller's own verdict on this run is ${own}, and a catalog row never outranks it` };
  }
  let digest: string | null;
  try {
    digest = await readBackupReceiptDigest(ws, b.name, signal);
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") throw error;
    return { row: null, reason: `this Backup's receipt digest could not be read (${error instanceof Error ? error.message : String(error)})` };
  }
  if (!digest) return { row: null, reason: "this Backup reports no receipt digest, so no catalog point can be named for it" };
  let catalogs: Catalog[];
  try {
    catalogs = await listCatalogs(ws, signal);
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") throw error;
    return { row: null, reason: `this workspace's recovery catalogs could not be read (${error instanceof Error ? error.message : String(error)})` };
  }
  const dest = b.destinationRef?.name ?? "";
  const ours = catalogs.filter((c) => (dest ? c.destinationRef?.name === dest : c.legacyArchive?.url === b.archive.url));
  const where = dest ? `destination ${dest}` : `archive ${b.archive.url}`;
  if (ours.length === 0) return { row: null, reason: `no recovery catalog in this workspace reads ${where}` };
  for (const c of ours.slice(0, MAX_SOURCE_CATALOGS)) {
    const found = await findCatalogPoint(ws, c.name, (p) => p.backupId === b.backupId && p.receiptSha256 === digest, signal);
    if (!found.entry) continue;
    const offer = catalogPointOffer(found.entry, found);
    if (!offer.offer) return { row: null, reason: `recovery catalog ${c.name} lists this point and does not offer it: ${offer.reason}` };
    return { row: { name: c.name, entry: found.entry }, reason: "" };
  }
  return { row: null, reason: `no recovery catalog over ${where} lists this run's point yet: the catalog has not synced it` };
}

/** Resolve a Backup chosen in step 1. */
export async function resolveBackup(ws: string, b: Backup, signal?: AbortSignal): Promise<Resolved> {
  if (!isRecoveryPoint(b)) return { state: "refused", reason: `Backup ${b.name} is not a recovery point.` };
  const bound = await bindBackup(ws, b, signal);
  const point = backupPoint(b, bound.row);
  let destination: Destination | null = null;
  let problem: string | null = null;
  if (point.destinationName) {
    destination = await readDestination(ws, point.destinationName, signal);
    problem = frozenDestinationProblem(point, destination);
    if (!problem && !destinationStore(destination)) problem = `saved destination ${point.destinationName} does not publish an S3 location this page can put into a plan`;
  }
  return { state: "selected", point, destination, destinationProblem: problem, unboundReason: bound.row ? null : bound.reason };
}

/** Resolve a catalog point named by the route (`?catalog=&point=`). */
export async function resolveCatalogPoint(ws: string, catalogName: string, pointId: string, signal?: AbortSignal): Promise<Resolved> {
  if (!pointId) return { state: "refused", reason: "The link names a catalog and no recovery point." };
  const catalog = await readCatalog(ws, catalogName, signal);
  const found = await findCatalogPoint(ws, catalogName, (p) => p.pointId === pointId, signal);
  if (!found.entry) {
    return {
      state: "refused",
      reason: found.budgetExhausted
        ? "The catalog's view holds more pages than this page reads, and the point was not in the ones it read."
        : found.viewExpired
          ? "The catalog's view has aged out (viewExpired); sync the catalog again."
          : found.incomplete
            ? "The catalog's point list is partial: a page or a line of its view could not be read; reload."
            : `Catalog ${catalogName} does not list point ${pointId}.`,
    };
  }
  const offer = catalogPointOffer(found.entry, found);
  if (!offer.offer) return { state: "refused", reason: `Catalog ${catalogName} does not offer point ${pointId}: ${offer.reason}.` };
  const destName = catalog.destinationRef?.name ?? "";
  let destination: Destination | null = null;
  if (destName) {
    destination = await readDestination(ws, destName, signal);
    if (!destination) return { state: "refused", reason: `The catalog reads saved destination ${destName}, which is absent.` };
    if (!destinationStore(destination)) return { state: "refused", reason: `Saved destination ${destName} does not publish an S3 location this page can put into a plan.` };
  } else if (!catalog.legacyArchive?.url) {
    return { state: "refused", reason: "The catalog names neither a saved destination nor an archive." };
  }
  const point = catalogPoint(catalog, found.entry, destination);
  return { state: "selected", point, destination, destinationProblem: frozenDestinationProblem(point, destination), unboundReason: null };
}
