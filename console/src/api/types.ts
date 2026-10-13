// Names for the product API's documents, all generated from
// schemas/logweir-api-v1.openapi.json (`npm run gen:api` writes schema.d.ts).
import type { components } from "./schema";

type S = components["schemas"];

export type Session = S["SessionResponse"];
export type NamespaceGrant = S["NamespaceGrant"];
export type Capabilities = S["Capabilities"];
export type Backup = S["Backup"];
export type Restore = S["Restore"];
export type Connection = S["Connection"];
export type Destination = S["Destination"];
export type Schedule = S["Schedule"];
export type ProtectionPolicy = S["ProtectionPolicyView"];
export type OperationView = S["OperationView"];
export type OperationState = S["OperationState"];
export type OperationStage = S["OperationStage"];
export type OperationSummary = S["OperationSummary"];
export type PointView = S["PointView"];
export type PointGroup = S["PointGroupView"];
export type Problem = S["Problem"];
export type CreateRestoreRequest = S["CreateRestoreRequest"];
