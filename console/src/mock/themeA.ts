// Theme A's restore facts on mock data, in the API's own shapes (PROD-05.2, PROD-11.1a).
// The plan bytes decide, as they do for the controller: a verified restore whose plan
// asks for the source's topic configuration carries `Restore.topicConfiguration`, and a
// clone (`teardown: {ttl_seconds}`) carries `OperationView.clone`.
import type { components } from "../api/schema";

type S = components["schemas"];

const iso = (ms: number) => new Date(ms).toISOString();

/** The demo restores theme A's screens start from (mock/data.ts). */
export const CLONE_DEMO = "orders-clone-k3v9d";
export const CUTOVER_DEMO = "orders-recovery-q4m8x";

/** What each demo restore's plan asks for, beside what R5's mock plan states. */
export function themeAPlanOf(name: string): { teardownTtlSeconds?: number; topicConfiguration?: { transition: "afterVerification" } } {
  if (name === CLONE_DEMO) return { teardownTtlSeconds: 24 * 3600, topicConfiguration: { transition: "afterVerification" } };
  if (name === CUTOVER_DEMO) return { topicConfiguration: { transition: "afterVerification" } };
  return {};
}

const BEFORE = { "cleanup.policy": "delete", "retention.ms": "-1", "message.timestamp.type": "CreateTime" };
const SOURCE: Record<string, Record<string, string>> = {
  orders: { "cleanup.policy": "delete", "retention.ms": "604800000", "message.timestamp.type": "CreateTime" },
  "orders-dlq": { "cleanup.policy": "delete", "retention.ms": "2592000000", "message.timestamp.type": "CreateTime" },
  payments: { "cleanup.policy": "compact", "retention.ms": "-1", "message.timestamp.type": "CreateTime" },
};

/** `Restore.topicConfiguration` once the run passed, when the plan asked for it. */
export function topicConfigurationOf(planBytes: string | undefined, newTopics: string[], sources: string[], verified: boolean): S["TopicConfigurationView"] | undefined {
  const t = (planBytes ?? "").match(/\n {4}transition: "(afterVerification|none)"\n/);
  if (!t || !verified) return undefined;
  const apply = t[1] === "afterVerification";
  return {
    transition: apply ? "applied" : "notRun: transition none",
    applied: apply ? newTopics.slice() : [],
    exported: [],
    failed: [],
    topics: newTopics.map((topic, i) => ({
      topic,
      before: BEFORE,
      after: apply ? (SOURCE[sources[i] ?? ""] ?? BEFORE) : BEFORE,
      replicationFactor: 3,
      replicationFactorReason: "sourceFactor",
      route: "adminApi",
    })),
  };
}

/** `OperationView.clone` for a clone's plan: when its topics go, from when the run ended. */
export function cloneOf(planBytes: string | undefined, endedAtMs: number | null): S["CloneView"] | undefined {
  const m = (planBytes ?? "").match(/\n {4}ttl_seconds: (\d+)\n/);
  if (!m) return undefined;
  const ttl = Number(m[1]);
  if (endedAtMs === null) return { ttlSeconds: ttl, phase: "Active" };
  const expires = endedAtMs + ttl * 1000;
  const gone = expires < Date.now();
  return { ttlSeconds: ttl, expiresAt: iso(expires), phase: gone ? "Removed" : "Active", attempts: gone ? 1 : 0 };
}
