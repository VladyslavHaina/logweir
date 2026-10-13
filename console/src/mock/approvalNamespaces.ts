// Three demo namespaces, one per approval mode the default team-a (confirm) does
// not show: two-person (payments), strict (ledger) and unbound (sandbox). The
// session lists them (data.ts), and mock/approvals.ts serves their restores and
// approvals. No imports beyond types, so data.ts can read this without a cycle.
import type { Capabilities, NamespaceGrant } from "../api/types";

const NONE: Capabilities = {
  approvalPacketRead: false,
  approvalSubmit: false,
  approvalsRead: false,
  backupsRead: false,
  catalogConnect: false,
  catalogWindowQuery: false,
  catalogs: false,
  connectionCreate: false,
  connectionTest: false,
  connectionsRead: false,
  credentialWrite: false,
  destinations: false,
  manualBackupCreate: false,
  operationEvents: false,
  operationsRead: false,
  preflight: false,
  protection: false,
  restoreCreate: false,
  restoresRead: false,
  retention: false,
  scheduleCreate: false,
  scheduleSetSuspension: false,
  schedulesRead: false,
  topicDiscovery: false,
  trustAdministration: false,
  trustPoliciesRead: false,
};

/** The approver role's actions (docs/api.md, "Who holds an action"). */
const APPROVER: Capabilities = {
  ...NONE,
  approvalPacketRead: true,
  approvalSubmit: true,
  approvalsRead: true,
  backupsRead: true,
  operationEvents: true,
  operationsRead: true,
  preflight: true,
  restoresRead: true,
};

/** An operator who is also an approver: requests restores and approves other people's. */
const OPERATOR_APPROVER: Capabilities = {
  ...APPROVER,
  catalogs: true,
  catalogWindowQuery: true,
  connectionsRead: true,
  connectionTest: true,
  destinations: true,
  manualBackupCreate: true,
  protection: true,
  restoreCreate: true,
  retention: true,
  schedulesRead: true,
  topicDiscovery: true,
};

export const APPROVAL_DEMO_GRANTS: NamespaceGrant[] = [
  { name: "payments", roles: ["operator", "approver"], capabilities: OPERATOR_APPROVER },
  { name: "ledger", roles: ["approver"], capabilities: APPROVER },
  { name: "sandbox", roles: ["approver"], capabilities: APPROVER },
];
