// OD-23-core's API shapes (docs/to-do/decisions/OD-23-core.md §4) that the OpenAPI
// document on this branch does not carry yet, so the console can show them now:
// - PROD-07.2 (claude/od23-b3, 26dedb22): `Restore.interruption`, the `interrupted`
//   state, and `POST …/restores/{name}/cancel` and `…/cleanup`;
// - PROD-03.1/03.2 (claude/od23-b2 has no API commit yet; the record's §4): the
//   connection's `schemaRegistry`, the backup's and point's `schemaRegistry`, and the
//   restore's `schemas`.
// Once the document carries them, `npm run gen:api` writes them into schema.d.ts, the
// types below become the generated ones, the casts in the readers go, and so do the two
// route types at the end of restores.ts. Type-only: the mock imports it too.
import type { components } from "./schema";

type S = components["schemas"];

/** `Restore.interruption`, as od23-b3 projects it. */
export interface RestoreInterruption {
  /** `Cancelled`, `RunnerStopped` or `RunFailed`. */
  cause?: string | null;
  requestedBy?: string | null;
  /** `Running`, `Done` or `Failed`: the reconcile of the partial targets. */
  verification?: string | null;
  verificationReason?: string | null;
  /** Whether Clean up may be requested now. */
  cleanupOffered: boolean;
  /** `Removing`, `Removed` or `RemovalFailed`. */
  cleanup?: string | null;
  cleanupReason?: string | null;
}

/** `Restore.schemas`, copied from the scorecard (OD-23-core §1.3 step 6). */
export interface RestoreSchemas {
  imported?: { subject: string; version: number; id: number }[] | null;
  alreadyPresent?: number | null;
  /** `noTargetRegistry` or `noSnapshot`. */
  notImported?: string | null;
}

/** A backup's and a catalog point's `schemaRegistry`, from the receipt (§4). */
export interface SchemaRegistryCapture {
  captured: boolean;
  subjects?: number | null;
  versions?: number | null;
  /** `registryUnreachable`, `registryUnauthorized`, `snapshotTooLarge`, `snapshotIncomplete`. */
  incompleteReason?: string | null;
}

/** A connection's `schemaRegistry` (no password: it lives in the connection's Secret). */
export interface ConnectionRegistry {
  url: string;
  username?: string | null;
}

/** The connection create body's optional block; the password goes into the connection's own Secret. */
export interface CreateRegistry {
  url: string;
  username?: string;
  password?: string;
}

export type CreateConnectionRequest = S["CreateConnectionRequest"] & { schemaRegistry?: CreateRegistry };

export function interruptionOf(r: S["Restore"]): RestoreInterruption | null {
  return (r as { interruption?: RestoreInterruption | null }).interruption ?? null;
}

export function restoreSchemasOf(r: S["Restore"]): RestoreSchemas | null {
  return (r as { schemas?: RestoreSchemas | null }).schemas ?? null;
}

export function registryCaptureOf(x: S["Backup"] | S["PointView"]): SchemaRegistryCapture | null {
  return (x as { schemaRegistry?: SchemaRegistryCapture | null }).schemaRegistry ?? null;
}

export function connectionRegistryOf(c: S["Connection"]): ConnectionRegistry | null {
  return (c as { schemaRegistry?: ConnectionRegistry | null }).schemaRegistry ?? null;
}

/** The `interrupted` operation state, which the generated union does not list yet. */
export function isInterrupted(op: { state: string }): boolean {
  return op.state === "interrupted";
}
