// The recovery catalog's rules, carried over from the old console
// (ui/pages/catalog.js and the wizard's catalog-point offer).
//
// 1. Availability and verification are two columns, never one. Whether a point
//    may be restored from is the catalog's own `selectable`, never recomputed.
// 2. No one-click trust: an untrusted signer is shown with its key id and the
//    fingerprint command, and nothing on the page adds a key.
// 3. Nothing is hidden: every entry is listed with its exact state and remedy.
// 4. The view is a WINDOW and says so: an expired view is a missing view, not a
//    missing archive.
import type { Catalog, PointPage, PointView } from "../../api/backups";

type Condition = { type: string; status: string; reason?: string | null; message?: string | null };

export function conditionOf(conditions: Condition[] | undefined, type: string): Condition | undefined {
  return (conditions ?? []).find((c) => c.type === type);
}

/** Whether the catalog's view is one the page can list from: the controller's `Ready`, never the browser's clock. */
export function viewIsUsable(c: Pick<Catalog, "conditions">): boolean {
  return conditionOf(c.conditions, "Ready")?.status === "True";
}

export const CATALOG_WINDOW_SENTENCE =
  "This view is a bounded window over the durable catalog in object storage. Points beyond it are counted and histogrammed here and listed by `logweir catalog list` against the archive itself; none of them is hidden and none of them is deleted.";

export const VIEW_EXPIRED_SENTENCE =
  "The Kubernetes view of this catalog has expired or has never been synced. That is a missing view and not a missing archive: the points are still in object storage. Sync the catalog to get the window back.";

export const TWO_AXES_SENTENCE =
  "Availability and verification are separate. Availability is whether the archive can still serve a point; verification is whether its receipt verifies under a key this installation accepts. A point is restorable only when it is Available and Verified (or VerifiedHistorical), and that is the catalog's own judgement, not this page's. A point whose Backup the controller refused is never restorable, whatever its two columns say.";

export const NO_CATALOG_SENTENCE =
  "Without a catalog, the points in your archive are still there, but nothing in Kubernetes can list them. Connect a destination you already hold a read-only credential for.";

export const NO_POINT_SENTENCE =
  "This catalog's view holds no point. That is a statement about the view: check the sync state above before concluding anything about the archive.";

export const NO_ONE_CLICK_TRUST_SENTENCE =
  "This page offers no control that adds a key to a TrustPolicy. A key found beside an archive is a claim, never trusted by proximity: compare its fingerprint with its holder out of band, then have an administrator add it with kubectl.";

export const BACKUP_VERDICTS_INCOMPLETE_SENTENCE =
  "The product API could not read every Backup verdict in this namespace for this page, so a row's restorability reflects the catalog alone for the Backups it could not read. No restore is offered from this page while that is so: a catalog row never outranks a verdict the controller reached, and one nobody could read might be such a verdict.";

export const POINT_BINDING_REDACTED_SENTENCE =
  "At least one point here published its plan binding (its backup set id or its receipt key) as [redacted]: the catalog sync's own redactor withheld it, so what the API serves is not the value in the bucket, and no restore is offered for that point until the catalog publishes the value itself. Runners up to v0.2.0-rc.1 did this to every scheduled run's point: upgrade the runner image and sync the catalog again. The point id and both digests are unaffected; nothing in your archive is missing or unreadable because of this.";

export const MORE_POINTS_SENTENCE =
  "The view holds more points than this page has read. This is the size of one request (200 points), a different limit from the window above.";

export const CONNECT_SENTENCE =
  "Connecting an archive creates a RecoveryCatalog: a read-only view over a destination you already hold a credential for. It writes nothing to your bucket, moves nothing and deletes nothing. Points signed by a key this installation does not list come back as untrusted, and nothing here changes that.";

export const CATALOG_MODE_HELP =
  "Full rescans every catalog record under logweir/catalog/v1/points/; Index reads the day shards of the catalog index, newest first. Neither reads a backup receipt that has no catalog record: points written before the catalog existed (v0.1.5 and earlier) appear only after their records are backfilled once with `logweir catalog sync`.";

/** The sync modes the connect form offers, in the REQUEST's spelling (the CRD's `Index`/`Full` would be a 422). */
export const SYNC_MODES = ["full", "index"] as const;

/** The fingerprint command, if the API sends none. */
export const FINGERPRINT_COMMAND = "openssl pkey -pubin -outform DER -in <key>.pub.pem | openssl dgst -sha256";

// ---------------------------------------------------- the restore offer

/** The marker the archive-key redactor leaves behind. A value that carries it is not a key. */
export const REDACTION_MARKER = "[redacted]";

export function isRedacted(value: unknown): boolean {
  return typeof value === "string" && value.includes(REDACTION_MARKER);
}

const MINTED_SET_ID =
  /^(?:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-([0-9]{8})-([0-9]{6})(?:-r[1-9])?)$/;

/**
 * Whether `id` is a set id the catalog sync's redactor reads as an identity (`check_contract.rs` `is_set_id`):
 * a UUID, or `<schedule uid>-<yyyymmdd>-<hhmmss>` (with `-r<k>` for a retry) whose slot is a real instant.
 * `console/fixtures/set-ids.json` is read by this side's tests and by the Rust side's.
 */
export function isMintedSetId(id: unknown): boolean {
  const m = MINTED_SET_ID.exec(typeof id === "string" ? id : "");
  if (m === null) return false;
  if (m[1] === undefined) return true;
  const day = m[1];
  const time = m[2]!;
  const n = (s: string, from: number, to: number) => Number(s.slice(from, to));
  const [y, mo, d] = [n(day, 0, 4), n(day, 4, 6), n(day, 6, 8)];
  const [h, mi, se] = [n(time, 0, 2), n(time, 2, 4), n(time, 4, 6)];
  const t = new Date(0);
  t.setUTCFullYear(y, mo - 1, d);
  t.setUTCHours(h, mi, Math.min(se, 59), 0);
  return (
    se <= 60 &&
    t.getUTCFullYear() === y &&
    t.getUTCMonth() === mo - 1 &&
    t.getUTCDate() === d &&
    t.getUTCHours() === h &&
    t.getUTCMinutes() === mi &&
    t.getUTCSeconds() === Math.min(se, 59)
  );
}

const CHOSEN_SET_ID_RULE =
  "a set id chosen for `logweir backup run` survives when it is a public form, for example a UUID, or lower-case letters, digits, `.`, `-`, `_` and `=` under 40 characters";

/** Why a point's plan binding came back redacted, or null when neither binding field carries the marker. */
export function redactedBindingReason(e: Pick<PointView, "backupId" | "receiptKey">): string | null {
  const setGone = isRedacted(e.backupId);
  const keyGone = isRedacted(e.receiptKey);
  if (!setGone && !keyGone) return null;
  const what: string[] = [];
  if (setGone) what.push(`backup set id as \`${e.backupId}\``);
  if (keyGone) what.push(`receipt key as \`${e.receiptKey}\``);
  const head = `the catalog published this point's ${what.join(" and its ")}, the redactor's output rather than the value in the bucket, so no plan binding can name the objects the runner must re-read: `;
  if (setGone) {
    return `${head}the catalog sync did not read the set id as an identity. Runners up to v0.2.0-rc.1 withhold every scheduled run's set id (its schedule's UID, then the slot) this way; upgrade the runner image and sync the catalog again. And ${CHOSEN_SET_ID_RULE}`;
  }
  if (isMintedSetId(e.backupId)) {
    return `${head}the redactor withheld the run id in the key, which runners older than v0.2.0-rc.1 did; sync the catalog again with a current runner image`;
  }
  return `${head}its set id \`${e.backupId}\` and the run id are two chosen components, and an archive key may carry only one past the redactor; ${CHOSEN_SET_ID_RULE}`;
}

const POINT_ID_SHAPE = /^lwp1-[0-9a-f]{32}$/;
const DIGEST_SHAPE = /^sha256:[0-9a-f]{64}$/;

function epochMs(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

/** The instants a plan may name inside a point's half-open covered window, or null. */
export function restorableWindow(e: Pick<PointView, "coveredFrom" | "coveredTo">): { fromMs: number; toMs: number } | null {
  const from = epochMs(e.coveredFrom);
  const to = epochMs(e.coveredTo);
  if (from === null || to === null) return null;
  const first = from + 1;
  const last = to - 1;
  return last < first ? null : { fromMs: first, toMs: last };
}

export type Offer = { offer: true } | { offer: false; reason: string };

/**
 * WHETHER ONE CATALOG ROW MAY BE OFFERED AS A RESTORE, and if not, why (the wizard's own rule,
 * `catalogPointOffer`). The order of the refusals is the order an operator would repair them in.
 */
export function catalogPointOffer(e: PointView | null | undefined, page: Partial<Pick<PointPage, "viewExpired" | "backupVerdictsIncomplete">>): Offer {
  const no = (reason: string): Offer => ({ offer: false, reason });
  if (!e) return no("the catalog's view does not list this point");
  if (page.viewExpired === true) return no("the catalog's view has aged out (viewExpired); sync the catalog again");
  if (typeof e.backupVerdict === "string" && e.backupVerdict.length > 0) {
    return no(`the controller refused this point's own Backup evidence (${e.backupVerdict}); a catalog row never outranks a verdict the controller reached`);
  }
  if (typeof page.backupVerdictsIncomplete === "string" && page.backupVerdictsIncomplete.length > 0) {
    return no(
      `the product API could not read every Backup verdict in this namespace (${page.backupVerdictsIncomplete}), so it cannot say that no Backup refused this receipt; this page does not offer a point over a verdict nobody could read`,
    );
  }
  if (e.selectable !== true) {
    return no(
      `the catalog does not list this point as restorable: availability ${e.availability || "unknown"}, verification ${e.verification || "unknown"}${
        typeof e.remedy === "string" && e.remedy.length > 0 ? ` (${e.remedy})` : ""
      }`,
    );
  }
  if (typeof e.pointId !== "string" || !POINT_ID_SHAPE.test(e.pointId)) return no("the point id is not `lwp1-` plus 32 lowercase hex characters");
  if (typeof e.receiptKey !== "string" || e.receiptKey.trim().length === 0) return no("the catalog published no receipt key for this point");
  const redacted = redactedBindingReason(e);
  if (redacted !== null) return no(redacted);
  if (typeof e.receiptSha256 !== "string" || !DIGEST_SHAPE.test(e.receiptSha256)) {
    return no("the catalog published no well-formed receipt digest for this point");
  }
  if (typeof e.manifestSha256 !== "string" || !DIGEST_SHAPE.test(e.manifestSha256)) {
    return no("the catalog published no manifest digest for this point, and the plan binding needs one: the runner checks the manifest the receipt attests before any data moves");
  }
  if (typeof e.backupId !== "string" || !/^[A-Za-z0-9._-]{1,128}$/.test(e.backupId)) {
    return no("the catalog published no usable backup set id for this point");
  }
  if (restorableWindow(e) === null) return no("the catalog published no covered window a point in time can be chosen in");
  return { offer: true };
}

/** The location a restore would read from: the first `Available` one, or the first there is. A choice between recorded facts. */
export function bestLocation(e: Pick<PointView, "locations">): { locationId: string; availability: string } | null {
  const list = e.locations ?? [];
  return list.find((l) => l.availability === "Available") ?? list[0] ?? null;
}

/**
 * A point's consumer-position evidence in words: how long before the point the positions were read,
 * and per group its outcome and how many positions relate to archived data, with every non-zero
 * count beside it. Null when the point publishes none (never "no positions").
 */
export function consumerPositionsWords(e: Pick<PointView, "consumerPositions">): string | null {
  const cp = e.consumerPositions;
  if (!cp) return null;
  const n = (v: number | undefined | null) => (typeof v === "number" ? v : 0);
  const parts = (cp.groups ?? []).map((g) => {
    const p = g.positions;
    if (g.outcome === "captured" && p) {
      const total = n(p.related) + n(p.notRelated) + n(p.neverCommitted) + n(p.beyondEnd) + n(p.failed) + n(p.notObserved);
      const also = (
        [
          [p.neverCommitted, "never committed"],
          [p.beyondEnd, "beyond the end"],
          [p.failed, "failed"],
          [p.notObserved, "not observed"],
        ] as const
      )
        .filter(([v]) => n(v) > 0)
        .map(([v, what]) => `, ${n(v)} ${what}`)
        .join("");
      return `${g.groupId}: captured${g.active === true ? " (active)" : ""}, ${n(p.related)} of ${total} positions relate to archived data${also}`;
    }
    return `${g.groupId}: ${g.outcome} (${g.reason ?? "no reason recorded"})`;
  });
  if (typeof cp.groupsOmitted === "number") parts.push(`${cp.groupsOmitted} groups not listed here; the signed receipt names each`);
  const fresh =
    typeof cp.observedBeforeRecoveryPointMs === "number"
      ? `read ${(cp.observedBeforeRecoveryPointMs / 1000).toFixed(1)} s before this point`
      : "read at a time this view does not say";
  return `Consumer positions ${fresh} (listing ${cp.listing}), not atomic with the records: ${parts.join("; ")}`;
}

/** What the review says about a topic whose archived records carry Confluent wire-format framing. */
export const SCHEMA_REGISTRY_NOT_CAPTURED = "Registry not captured: applications may not read these records after restore.";

export const SCHEMA_DEPENDENCY_NOTE =
  "The backup that wrote these points judged their topics' archived records and found Confluent wire-format framing in their keys or values: a zero byte and a schema id that a schema registry issued. Logweir never contacts or captures a schema registry, so an application that deserializes the restored records needs the registry that issued these ids, reachable from where it runs. The restore copies the bytes unchanged either way, and nothing here blocks it.";

/** A point's schema-dependent topics, each as `topic (sides: schema ids …; basis)`. A point whose topics are not listed says nothing: not assessed, never "none". */
export function schemaDependentTopics(e: Pick<PointView, "topics">): string[] {
  return (e.topics ?? [])
    .filter((t) => t.schemaDependency?.verdict === "schemaDependent")
    .map((t) => {
      const d = t.schemaDependency!;
      const ids = (d.schemaIds ?? []).filter((id) => Number.isInteger(id) && id >= 1);
      const sides = (d.sides ?? []).filter((s) => s === "key" || s === "value");
      return `${t.name} (${sides.length > 0 ? sides.join(" and ") : "records"}: schema ids ${ids.length > 0 ? ids.join(", ") : "not listed"}${
        d.schemaIdsOmitted === true ? " and more" : ""
      }${typeof d.basis === "string" ? `; ${d.basis}` : ""})`;
    });
}

// --------------------------------------------------------- the connect form

/** The API's field paths mapped onto this form's inputs, so a 422 lands beside the control it is about. */
export function connectFieldOf(path: string): "name" | "destination" | "syncMode" | undefined {
  if (path === "name") return "name";
  if (path === "destinationRef" || path === "destinationRef.name" || path === "legacyArchive" || path.startsWith("legacyArchive.")) return "destination";
  if (path === "syncMode") return "syncMode";
  return undefined;
}

/** The answer a spent key gets, and what clears it. */
export function isIntentConflict(error: unknown): boolean {
  const code = (error as { code?: unknown } | null)?.code;
  return code === "idempotency_conflict" || code === "idempotency_key_invalid";
}

export const INTENT_USED_SENTENCE =
  "The idempotency key this form was holding was already spent on a different request, so the API refused rather than guess which of the two you meant. Submitting again sends a new key, because the request has changed.";
