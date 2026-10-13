// Recovery points: which Backups and catalog rows a restore can be built from, and
// the one point a wizard is bound to. Pure functions over product-API documents
// (ui/pages/restore-wizard.js's selector, resolver and catalog rules, without the DOM).
import type { components } from "../../api/schema";
import type { PlanPointBinding } from "./plan";

type S = components["schemas"];
export type Backup = S["Backup"];
export type Destination = S["Destination"];
export type PointView = S["PointView"];
export type Catalog = S["CatalogView"];

/** A window as `Backup.windowCovered` writes it: epoch ms, the end exclusive. */
export interface Window {
  fromMs: number;
  toMs: number;
}

/** The one point a wizard is bound to. A Backup bound to its catalog row carries both. */
export interface RecoveryPoint {
  /** `backup/<uid>` or `catalog/<catalog>/<pointId>`: the identity the readiness attempt and the selection use. */
  key: string;
  backup: Backup | null;
  /** The catalog row the plan is bound to (`source.point`), when there is one. */
  catalog: { name: string; entry: PointView } | null;
  backupSetRef: string;
  window: Window;
  /** The frozen topic list, or null when a catalog point publishes none (the operator names them). */
  topics: string[] | null;
  /** The saved destination the archive is read through; "" for an inline archive. */
  destinationName: string;
  destinationUid: string;
  locationDigest: string;
  /** `logweir-destination://<name>` for a saved destination, else the inline archive URL. */
  archiveUrl: string;
  /** An inline archive's credential Secret NAME (never a value). */
  archiveSecretName: string;
  sourceName: string;
  createdAt: string;
}

export const DESTINATION_SENTINEL = "logweir-destination://";

/** Whether a Backup is a point a plan can be built from: succeeded, with a backup set and a window. */
export function isRecoveryPoint(b: Backup | null | undefined): b is Backup & { backupId: string; windowCovered: Window } {
  return (
    !!b &&
    b.operation.state === "succeeded" &&
    typeof b.backupId === "string" &&
    b.backupId.length > 0 &&
    typeof b.windowCovered?.fromMs === "number" &&
    typeof b.windowCovered?.toMs === "number"
  );
}

/** The namespace's recovery points, newest first (creation order; ties keep list order). */
export function recoveryPoints(backups: Backup[]): Backup[] {
  return backups
    .map((point, index) => ({ point, index }))
    .filter((e) => isRecoveryPoint(e.point))
    .sort((a, b) => {
      const l = Date.parse(String(a.point.createdAt ?? ""));
      const r = Date.parse(String(b.point.createdAt ?? ""));
      if (l === r || Number.isNaN(l) || Number.isNaN(r)) return a.index - b.index;
      return l < r ? 1 : -1;
    })
    .map((e) => e.point);
}

export type Resolution =
  | { state: "none" }
  | { state: "selected"; backup: Backup }
  | { state: "unusable"; backup: Backup }
  | { state: "missing"; name: string; uid: string; renamed: boolean };

/** WHAT A ROUTE IDENTITY RESOLVES TO. `uid` decides when given (a point deleted and
 *  recreated under the same name is a different run); the name is for the refusal.
 *  Never "the newest one instead". */
export function resolvePoint(backups: Backup[], wanted: { backup?: string; uid?: string }): Resolution {
  const uid = (wanted.uid ?? "").trim();
  const name = (wanted.backup ?? "").trim();
  if (!uid && !name) return { state: "none" };
  const found = backups.find((b) => (uid ? b.uid === uid : b.name === name));
  if (!found) return { state: "missing", name, uid, renamed: !!uid && !!name && backups.some((b) => b.name === name) };
  return isRecoveryPoint(found) ? { state: "selected", backup: found } : { state: "unusable", backup: found };
}

/** What a refused route identity says. */
export function refusalSentence(r: Resolution): string {
  if (r.state === "missing") {
    return r.renamed
      ? `Backup ${r.name} was deleted and a different run now has its name (uid ${r.uid} is gone). Choose the point you mean; nothing was substituted.`
      : `No Backup named ${r.name || r.uid} is in this namespace. Choose a recovery point below.`;
  }
  if (r.state === "unusable") {
    const b = r.backup;
    return `Backup ${b.name} is not a recovery point: it is ${b.operation.state}${
      b.operation.state === "succeeded" ? " without a recorded backup set or covered window" : ""
    }. Only a succeeded run with a backup set and a window can be restored.`;
  }
  return "";
}

/** The lowercased text one point is searched by. */
export function pointHaystack(b: Backup): string {
  return [b.name, b.scheduleRef?.name ?? b.schedule, b.slot, b.sourceRef.name, b.archive.url, b.backupId, ...b.topics]
    .filter((p): p is string => typeof p === "string" && p.length > 0)
    .join(" ")
    .toLowerCase();
}

/** Every whitespace-separated term must appear; an empty query matches everything. */
export function matchesQuery(haystack: string, query: string): boolean {
  return query
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .every((term) => haystack.includes(term));
}

/** The range a plan may name: `[fromMs + 1, toMs - 1]` (the runner accepts
 *  `oldest < pit <= newest`, and `toMs` is exclusive). Null when empty. */
export function restorableWindow(w: Partial<Window> | null | undefined): Window | null {
  if (typeof w?.fromMs !== "number" || typeof w?.toMs !== "number") return null;
  const first = w.fromMs + 1;
  const last = w.toMs - 1;
  return last < first ? null : { fromMs: first, toMs: last };
}

/** An instant as the plan writes it: RFC 3339, milliseconds only when there are some. */
export function rfc3339(ms: number): string {
  const iso = new Date(ms).toISOString();
  return iso.endsWith(".000Z") ? `${iso.slice(0, 19)}Z` : iso;
}

export function epochMs(instant: string | null | undefined): number | null {
  if (typeof instant !== "string" || instant.length === 0) return null;
  const at = Date.parse(instant);
  return Number.isNaN(at) ? null : at;
}

export function windowMessage(w: Window): string {
  return `the archive covers [${rfc3339(w.fromMs)}, ${rfc3339(w.toMs)}]; a point outside it cannot be restored`;
}

/** The bucket and prefix an object-store URL names. */
export function bucketOf(url: string): string {
  const rest = url.includes("://") ? url.slice(url.indexOf("://") + 3) : url;
  return rest.split("/")[0] ?? "";
}
export function prefixOf(url: string): string {
  const rest = url.includes("://") ? url.slice(url.indexOf("://") + 3) : url;
  const slash = rest.indexOf("/");
  return slash === -1 ? "" : rest.slice(slash + 1);
}

// ------------------------------------------------------------- catalog points

const POINT_ID = /^lwp1-[0-9a-f]{32}$/;
const DIGEST = /^sha256:[0-9a-f]{64}$/;
export const REDACTION_MARKER = "[redacted]";

/** The catalog's identity of a Backup's point: `lwp1-` and the first 32 hex of its receipt digest. */
export function pointIdOfReceiptDigest(digest: string | null | undefined): string {
  const m = /^sha256:([0-9a-f]{64})$/.exec(String(digest ?? ""));
  return m ? `lwp1-${m[1]!.slice(0, 32)}` : "";
}

/** WHETHER ONE CATALOG ROW MAY BE OFFERED, and if not, why: in the order an operator repairs them. */
export function catalogPointOffer(
  e: PointView | null | undefined,
  page: { viewExpired?: boolean; backupVerdictsIncomplete?: string | null },
): { offer: true } | { offer: false; reason: string } {
  const no = (reason: string) => ({ offer: false as const, reason });
  if (!e) return no("the catalog's view does not list this point");
  if (page.viewExpired) return no("the catalog's view has aged out (viewExpired); sync the catalog again");
  if (e.backupVerdict) {
    return no(`the controller refused this point's own Backup evidence (${e.backupVerdict}); a catalog row never outranks a verdict the controller reached`);
  }
  if (page.backupVerdictsIncomplete) {
    return no(`the product API could not read every Backup verdict in this namespace (${page.backupVerdictsIncomplete}), so it cannot say that no Backup refused this receipt`);
  }
  if (e.selectable !== true) {
    return no(
      `the catalog does not list this point as restorable: availability ${e.availability || "unknown"}, verification ${e.verification || "unknown"}${e.remedy ? ` (${e.remedy})` : ""}`,
    );
  }
  if (!POINT_ID.test(e.pointId)) return no("the point id is not `lwp1-` plus 32 lowercase hex characters");
  if (!e.receiptKey?.trim()) return no("the catalog published no receipt key for this point");
  if (e.receiptKey.includes(REDACTION_MARKER) || (e.manifestKey ?? "").includes(REDACTION_MARKER)) {
    return no("the catalog published this point's keys redacted, and a redacted key is never put into a plan binding");
  }
  if (!DIGEST.test(e.receiptSha256)) return no("the catalog published no well-formed receipt digest for this point");
  if (!DIGEST.test(e.manifestSha256 ?? "")) {
    return no("the catalog published no manifest digest for this point, and the plan binding needs one: the runner checks the manifest the receipt attests before any data moves");
  }
  if (!/^[A-Za-z0-9._-]{1,128}$/.test(e.backupId)) return no("the catalog published no usable backup set id for this point");
  if (restorableWindow(catalogWindow(e)) === null) return no("the catalog published no covered window a point in time can be chosen in");
  return { offer: true };
}

/** The catalog row's covered window, half-open as the receipt writes it. */
export function catalogWindow(e: PointView): Window | null {
  const from = epochMs(e.coveredFrom);
  const to = epochMs(e.coveredTo);
  return from === null || to === null ? null : { fromMs: from, toMs: to };
}

export function pointBindingOf(p: RecoveryPoint): PlanPointBinding | undefined {
  const e = p.catalog?.entry;
  if (!e) return undefined;
  return { pointId: e.pointId, receiptKey: e.receiptKey, receiptSha256: e.receiptSha256, manifestSha256: e.manifestSha256 ?? "" };
}

/** The point a plan over a Backup is built from: bound to its catalog row when one was
 *  found (`source.point`, the catalog's window), else the Backup alone. */
export function backupPoint(b: Backup & { backupId: string; windowCovered: Window }, row: { name: string; entry: PointView } | null): RecoveryPoint {
  const dest = b.destinationRef?.name ?? "";
  return {
    key: `backup/${b.uid}`,
    backup: b,
    catalog: row,
    backupSetRef: b.backupId,
    window: (row && catalogWindow(row.entry)) || b.windowCovered,
    topics: b.topics.slice(),
    destinationName: dest,
    destinationUid: b.destinationRef?.uid ?? "",
    locationDigest: b.locationDigest ?? "",
    archiveUrl: b.archive.url,
    archiveSecretName: dest ? "" : (b.archive.credentialRef?.name ?? ""),
    sourceName: b.sourceRef.name,
    createdAt: String(b.createdAt ?? ""),
  };
}

/** A point read from a catalog alone (no Backup in this namespace). Its destination's
 *  frozen facts are the live ones read now, so a later move is still caught at submit. */
export function catalogPoint(catalog: Catalog, entry: PointView, destination: Destination | null): RecoveryPoint {
  const name = catalog.destinationRef?.name ?? "";
  return {
    key: `catalog/${catalog.name}/${entry.pointId}`,
    backup: null,
    catalog: { name: catalog.name, entry },
    backupSetRef: entry.backupId,
    window: catalogWindow(entry)!,
    // The view's topics are a layout, not the frozen list: the operator names the topics
    // (prefilled from the layout when it is whole), and the readiness check reads the
    // manifest for exactly those names.
    topics: null,
    destinationName: name,
    destinationUid: destination?.uid ?? "",
    locationDigest: destination?.locationDigest ?? "",
    archiveUrl: name ? `${DESTINATION_SENTINEL}${name}` : (catalog.legacyArchive?.url ?? ""),
    archiveSecretName: name ? "" : (catalog.legacyArchive?.credentialRef?.name ?? ""),
    sourceName: "",
    createdAt: String(entry.recoveryPointAt ?? ""),
  };
}

/** A destination-backed point is pinned by its frozen name, uid and location digest:
 *  a replacement under the same name is not a source for it. Null when it still matches. */
export function frozenDestinationProblem(p: RecoveryPoint, live: Destination | null | undefined): string | null {
  if (!p.destinationName) return null;
  if (!p.destinationUid || !p.locationDigest) {
    return `recovery point ${p.backup?.name ?? p.key} does not publish the frozen destination UID and location digest required to restore`;
  }
  if (!live) return `saved destination ${p.destinationName} is absent`;
  if (live.name !== p.destinationName) return `saved destination ${p.destinationName} resolved as a different name`;
  if (live.uid !== p.destinationUid) {
    return `saved destination ${p.destinationName} was recreated: recovery point UID ${p.destinationUid} does not match live UID ${live.uid || "(absent)"}`;
  }
  if (live.locationDigest !== p.locationDigest) {
    return `saved destination ${p.destinationName} moved: recovery point location digest ${p.locationDigest} does not match live digest ${live.locationDigest || "(absent)"}`;
  }
  return null;
}

/** Only public destination fields become signed storage settings. Null when the
 *  destination publishes no S3 location this page can put into a plan. */
export function destinationStore(d: Destination | null | undefined) {
  if (!d || d.storage.provider !== "s3" || !d.storage.bucket || typeof d.storage.prefix !== "string") return null;
  return {
    bucket: d.storage.bucket,
    prefix: d.storage.prefix,
    region: d.storage.region ?? "",
    endpoint: d.storage.endpoint ?? "",
    pathStyle: d.storage.addressing === "pathStyle",
    allowHttp: d.transport.security === "insecureHttp",
  };
}

/** The evidence prefix is never an operator field: logweir writes only under `logweir/`. */
export const EVIDENCE_PREFIX = "logweir/";

/** A saved destination's evidence store: its bucket at `logweir/`, its own transport. */
export function evidenceStoreOf(d: Destination | null | undefined) {
  const s = destinationStore(d);
  return s && { ...s, prefix: EVIDENCE_PREFIX };
}
