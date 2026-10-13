// Theme A's cutover review (PROD-04.2), on mock data. The product API does not
// publish these documents yet: the shapes below are PROVISIONAL and follow the
// tracker's wording (exact / approximate / unavailable; the reserved
// `Switchover` kind). The real fields come from decisions/PROD-04.2-cutover.md.

export type Mapping = "exact" | "approximate" | "unavailable";

export interface PositionRow {
  topic: string;
  partition: number;
  /** The group's committed next-to-consume offset on the source, from the recovery point. */
  committed: number | null;
  /** The offset the group will resume from on the target; null when unavailable. */
  target: number | null;
  mapping: Mapping;
  why: string;
}

export interface GroupReview {
  group: string;
  type: "classic" | "consumer" | "other";
  /** Members connected on the TARGET cluster right now. */
  activeMembers: number;
  rows: PositionRow[];
}

const EXACT = "First restored record whose x-original-offset is at or after the committed offset.";
const AT_END = "The committed offset was the partition end; it maps to the end of the restored partition.";

export function reviewFor(restore: string): GroupReview[] {
  void restore;
  return [
    {
      group: "orders-billing",
      type: "classic",
      activeMembers: 0,
      rows: [
        { topic: "orders", partition: 0, committed: 1_204_331, target: 1_198_870, mapping: "exact", why: EXACT },
        { topic: "orders", partition: 1, committed: 1_187_002, target: 1_181_455, mapping: "exact", why: EXACT },
        { topic: "orders", partition: 2, committed: 1_210_948, target: 1_205_101, mapping: "exact", why: EXACT },
        { topic: "orders-dlq", partition: 0, committed: 3_812, target: 3_790, mapping: "approximate", why: AT_END },
      ],
    },
    {
      group: "orders-shipping",
      type: "consumer",
      activeMembers: 2,
      rows: [
        { topic: "orders", partition: 0, committed: 1_203_990, target: 1_198_529, mapping: "exact", why: EXACT },
        { topic: "orders", partition: 1, committed: 1_186_877, target: 1_181_330, mapping: "exact", why: EXACT },
        { topic: "orders", partition: 2, committed: 1_211_406, target: 1_205_559, mapping: "approximate", why: AT_END },
      ],
    },
    {
      group: "orders-fraud",
      type: "classic",
      activeMembers: 0,
      rows: [
        { topic: "orders", partition: 0, committed: 1_199_870, target: 1_194_412, mapping: "exact", why: EXACT },
        {
          topic: "orders",
          partition: 1,
          committed: 402_118,
          target: null,
          mapping: "unavailable",
          why: "The committed offset is older than the restored data (the archive starts at offset 655,040), so no restored record corresponds to it.",
        },
        { topic: "orders", partition: 2, committed: 1_206_771, target: 1_200_925, mapping: "exact", why: EXACT },
      ],
    },
    {
      group: "orders-audit-stream",
      type: "other",
      activeMembers: 0,
      rows: [
        {
          topic: "orders",
          partition: 0,
          committed: null,
          target: null,
          mapping: "unavailable",
          why: "Not captured: streams groups are not captured (GroupTypeNotCaptured), so there is no position to translate.",
        },
      ],
    },
  ];
}

export type GroupVerdict = "ready" | "stopConsumers" | "blocked";

export function verdictOf(group: GroupReview): GroupVerdict {
  if (group.rows.some((r) => r.mapping === "unavailable")) return "blocked";
  if (group.activeMembers > 0) return "stopConsumers";
  return "ready";
}
