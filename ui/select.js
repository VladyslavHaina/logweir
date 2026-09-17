// select.js -- the saved-cluster selector, and the words a connection probe is
// allowed to be described with (PLAT-07.2).
//
// PURE. Every function here is a function from JSON objects -- ones the API
// server could have returned, which is what the checked-in fixtures are -- and
// an optional instant, to a plain value or an HTML string. No DOM, no network,
// no clock of its own: `probeState` takes `now` the way `approvals.js`'s age
// column does, so a freshness verdict is reproducible in a test rather than a
// function of when the test ran.
//
// WHY THIS IS ONE MODULE AND NOT THREE COPIES. Three surfaces choose a saved
// KafkaCluster -- the schedule form's source, the restore wizard's target, and
// the clusters page's own list -- and before this module each one did it by
// NAME, in its own spelling. A name is not an identity: delete a cluster and
// recreate it under the same name and every one of those references silently
// follows the new object, which is a different set of brokers reached with a
// different credential. PLAT-11.1 fixed exactly this for recovery points by
// pinning the Backup's UID in the route and REFUSING when the UID is gone; this
// module is the same rule for connections, and the refusal wording is
// deliberately parallel.
//
// THE THREE THINGS IT OWNS
//
//   1. IDENTITY. A selection is `{uid, name}`. The UID decides; the name is for
//      reading and for refusals. `resolveClusterSelection` is the ONE place
//      that turns a selection plus a list into an answer, and it has four
//      answers -- `none`, `selected`, `recreated`, `missing` -- none of which
//      is "the first one instead".
//   2. FRESHNESS. `status.reachable` is a CONNECTION PROBE and never a
//      readiness verdict: the words below say "connection probe", the badges
//      say `reachable`/`not reachable`/`refused`/`probing`/`never probed`, and
//      an observation older than the freshness budget is labelled `stale`
//      beside whatever it says. The word `ready` appears nowhere in this file
//      and `ui/tests/pages.spec.js` asserts that for every surface that renders
//      a probe.
//   3. CAPABILITY. `spec.role` travels with every option, because "which of
//      these is my target" is the question a person is actually answering. It
//      is a LABEL and not an authorisation -- the CRD says so and so does the
//      restore wizard's own sentence -- so the selector SHOWS it and never
//      filters a cluster out of the list because of it.
//
// AND IT HOLDS NO CREDENTIAL. What travels through here is a Secret's NAME, a
// data KEY's name, and a ConfigMap's name. There is no field on a KafkaCluster
// a password could be read from, and this module adds none.

import { badge, cell, esc } from "./render.js";

/** The noun every surface uses for `status.reachable`.
 *
 *  D2 section 9 fixes it: "Connection health (`status.reachable`) is labelled
 *  'connection probe', never 'ready'." A `Ready` condition on a schedule means
 *  the controller accepted the object; a probe means a pod dialled a broker
 *  once, minutes ago, and said what happened. Conflating them is how a
 *  dashboard tells someone their disaster-recovery path is fine. */
export const PROBE_NOUN = "connection probe";

/** The word this module refuses to print, held as data so
 *  `no_probe_surface_says_ready` can assert the rendered strings against it
 *  rather than against a spelling in a test. */
export const FORBIDDEN_PROBE_WORD = "ready";

/** How old an observation may be before it is labelled `stale`, in seconds.
 *
 *  DERIVED FROM THE CONTROLLER'S OWN CADENCE, not picked. `weirkeeper`'s
 *  `controllers::kafka_cluster::PROBE_TTL_SECONDS` is 300 and `RE_PROBE_SECS`
 *  is that plus a 15 s margin, so a healthy controller refreshes
 *  `status.observedAt` about every 315 s. Twice that -- 630 s -- is the point
 *  at which a missed refresh is no longer explainable by scheduling jitter, so
 *  it is the point at which this page stops presenting the observation as
 *  current. A shorter budget would flag a working installation; a longer one
 *  would keep showing a dead probe as fresh, which is the defect PLAT-07.2
 *  exists to remove.
 *
 *  It is a parameter everywhere below: every function takes `freshSeconds` and
 *  falls back to this, so an installation that re-probes on another cadence has
 *  one number to change. */
export const PROBE_FRESH_SECONDS = 630;

/** Clock skew this module tolerates before calling an observation stale for
 *  being in the FUTURE, in seconds. A viewer's laptop clock and the cluster's
 *  differ; a minute of that is not news, and more than a minute means the
 *  freshness arithmetic below cannot be trusted either way. */
export const PROBE_SKEW_SECONDS = 60;

/** The words for a connection with NO `status.observedAt` at all.
 *
 *  ITS OWN WORD, BECAUSE IT IS ITS OWN STATE. `stale` means "this reading
 *  describes the past" and is a statement ABOUT an observation; a connection
 *  the controller refused to dial has no observation for it to be about, and
 *  labelling it stale says two contradictory things at once -- "no observation
 *  recorded" and "this observation is older than the budget" -- and tells an
 *  operator to wait for a refresh that will never come, because a refused
 *  connection starts no probe Job. */
export const NEVER_OBSERVED_WORDS = "never observed";

/** The four `status.reason` values PLAT-07.1's resolver writes when it REFUSES
 *  a saved connection before any probe Job exists.
 *
 *  Byte for byte `weirkeeper::conditions`' terminal states and
 *  `controllers::kafka_cluster::PROBE_CONDITION_REASONS`' last four entries.
 *  A cluster carrying one of these has no `status.reachable` at all -- the
 *  refusal clears it -- so without this list the page would render "never
 *  probed" over a connection the controller has told it is broken. */
export const CONNECTION_REFUSAL_REASONS = Object.freeze([
  "ConnectionConfigInvalid",
  "ConnectionReferenceInvalid",
  "ConnectionFieldUnsupported",
  "CredentialNotRenderable",
]);

/** The reason the controller writes while a probe Job is in flight. */
export const PROBE_RUNNING_REASON = "ProbeRunning";

/** What each refusal reason means, in one sentence, for a reader who has the
 *  reason and not the controller source. The CONTROLLER'S OWN REASON is always
 *  printed verbatim beside these; this is a gloss and never a replacement, so a
 *  reason this map does not know still reaches the screen. */
export const REFUSAL_GLOSS = Object.freeze({
  ConnectionConfigInvalid:
    "the saved settings contradict each other -- plaintext with TLS on, a CA with TLS off, or " +
    "two CA sources -- so the controller refused to dial rather than dial something else",
  ConnectionReferenceInvalid:
    "a reference on this object is not a legal Kubernetes name or data key, so nothing could " +
    "be resolved in this namespace",
  ConnectionFieldUnsupported:
    "this object sets a field this controller does not implement; it is refused rather than " +
    "resolved without that field",
  CredentialNotRenderable:
    "the credential this connection needs cannot be rendered -- a missing SASL username or " +
    "Secret reference -- so no run could present an identity",
});

/** The verdicts [`probeState`] answers with. Never `ready`. */
export const PROBE_VERDICTS = Object.freeze([
  "reachable",
  "unreachable",
  "refused",
  "probing",
  "unknown",
  "never",
]);

/** The caption each verdict renders with, and the badge kind it takes. */
const VERDICT_WORDS = Object.freeze({
  reachable: { kind: "ok", words: "reachable" },
  unreachable: { kind: "danger", words: "not reachable" },
  refused: { kind: "danger", words: "refused" },
  probing: { kind: "warn", words: "probing" },
  unknown: { kind: "warn", words: "unknown" },
  never: { kind: "flat", words: "never probed" },
});

/** The objects a selector was handed, whatever shape they arrived in: a
 *  `KafkaClusterList`, a bare array, or one object. The same normalisation
 *  `ui/pages/clusters.js` exports as `itemsOf`, repeated here rather than
 *  imported so this module depends on no page. */
export function clustersOf(input) {
  if (input === null || input === undefined) {
    return [];
  }
  if (Array.isArray(input)) {
    return input;
  }
  if (Array.isArray(input.items)) {
    return input.items;
  }
  return [input];
}

/** One cluster's `metadata.uid`, or the empty string. */
export function clusterUid(cluster) {
  const uid = (((cluster || {}).metadata) || {}).uid;
  return typeof uid === "string" ? uid : "";
}

/** One cluster's `metadata.name`, or the empty string. */
export function clusterName(cluster) {
  const name = (((cluster || {}).metadata) || {}).name;
  return typeof name === "string" ? name : "";
}

/** One cluster's `spec.role` -- the capability label -- or the empty string. */
export function clusterRole(cluster) {
  const role = (((cluster || {}).spec) || {}).role;
  return typeof role === "string" ? role : "";
}

/** The lowercased text one cluster is searched by: its name, its role, its
 *  bootstrap addresses, its auth mode and username, the NAME of its credential
 *  Secret, the cluster id the broker gave and the probe's reason. Never a
 *  value of any Secret -- there is none on this object to read. */
export function clusterHaystack(cluster) {
  const c = cluster || {};
  const spec = c.spec || {};
  const auth = spec.auth || {};
  const status = c.status || {};
  const parts = [
    clusterName(c),
    clusterRole(c),
    Array.isArray(spec.bootstrapServers) ? spec.bootstrapServers.join(" ") : "",
    auth.mode,
    auth.username,
    ((auth.secretRef || {}).name),
    auth.tls === true ? "tls" : "no-tls",
    status.clusterId,
    status.reason,
  ];
  return parts
    .filter((part) => typeof part === "string" && part.length > 0)
    .join(" ")
    .toLowerCase();
}

/** True when every whitespace-separated word of `query` appears in `haystack`.
 *  The same "every word must match" rule the recovery-point selector uses. */
export function haystackMatches(haystack, query) {
  const text = typeof haystack === "string" ? haystack : "";
  const words = String(query === undefined || query === null ? "" : query)
    .toLowerCase()
    .split(/\s+/)
    .filter((word) => word.length > 0);
  return words.every((word) => text.indexOf(word) !== -1);
}

/** The saved clusters a selector offers, EVERY ONE OF THEM, ordered by name.
 *
 *  NO ROLE FILTER, EVER. `spec.role` is documented in the CRD as "a label the
 *  adopter picks and the controller reports", and the restore wizard already
 *  carries the sentence explaining that the runner's own guard is the gate: a
 *  `newTopic` restore into the source cluster is exactly where a point-in-time
 *  recovery belongs, and Task 28a removed a role filter here that rendered an
 *  empty select for the product's own demo. So the role travels WITH each
 *  option and decides only which one is preselected ([`preferredCluster`]).
 *
 *  Ordered by name so the list a person reads twice is the same list twice;
 *  `kubectl`'s order is creation order, which reshuffles as objects come and
 *  go. A cluster with no name is dropped: it cannot be selected by identity
 *  and cannot be rendered as an option. */
export function savedClusters(clusters) {
  return clustersOf(clusters)
    .filter((cluster) => clusterName(cluster).length > 0)
    .slice()
    .sort((a, b) => {
      const left = clusterName(a);
      const right = clusterName(b);
      if (left === right) {
        return 0;
      }
      return left < right ? -1 : 1;
    });
}

/** The cluster a fresh form starts on: the first one carrying `prefer` as its
 *  role, else the first saved cluster, else `null`. A DEFAULT and not a
 *  decision -- it is `selected` in the rendered control, visible, and
 *  changeable before anything is sent. */
export function preferredCluster(clusters, prefer) {
  const all = savedClusters(clusters);
  if (typeof prefer === "string" && prefer.length > 0) {
    for (const cluster of all) {
      if (clusterRole(cluster) === prefer) {
        return cluster;
      }
    }
  }
  return all.length > 0 ? all[0] : null;
}

/** WHAT A SAVED SELECTION RESOLVES TO against the clusters a namespace holds.
 *  Pure, and the ONE place any surface decides which cluster it is bound to.
 *
 *  `selection` is `{uid, name}`. Four answers:
 *
 *   - `none`      -- neither half was given; the caller offers the selector
 *                    with its default preselected and nothing is bound yet.
 *   - `selected`  -- resolved. `cluster` is the object, `uid`/`name` are ITS
 *                    identity (so a name-only selection is PINNED to the UID
 *                    that answered it, `pinned: true`), and `renamedFrom` is
 *                    the older name when the object has since been given
 *                    another one. A RENAME IS NOT A REFUSAL: same UID, same
 *                    brokers, same credential -- only the label moved, and the
 *                    page says so and carries on.
 *   - `recreated` -- the UID is gone AND a different object now answers to the
 *                    name. THE REFUSAL THIS MODULE EXISTS FOR: a KafkaCluster
 *                    deleted and recreated under the same name is a different
 *                    connection -- different brokers, a different credential,
 *                    a different cluster id -- and following it silently is
 *                    how a backup ends up reading somebody else's topics.
 *                    `recreatedUid` carries the impostor's UID so the refusal
 *                    can name both.
 *   - `missing`   -- the UID is gone and nothing carries the name either.
 *
 *  A name-only selection can never be `recreated`: with no recorded UID there
 *  is nothing to have changed, which is exactly what makes an EXISTING
 *  `KafkaCluster` reference -- `BackupSchedule.spec.sourceRef.name`, written
 *  before this module existed -- still usable. It resolves, it is pinned, and
 *  from then on it is an identity. */
export function resolveClusterSelection(clusters, selection) {
  const wanted = selection || {};
  const uid = typeof wanted.uid === "string" ? wanted.uid.trim() : "";
  const name = typeof wanted.name === "string" ? wanted.name.trim() : "";
  const blank = {
    state: "none",
    cluster: null,
    uid: "",
    name: "",
    role: "",
    pinned: false,
    renamedFrom: "",
    recreatedUid: "",
  };
  if (uid.length === 0 && name.length === 0) {
    return blank;
  }
  const all = savedClusters(clusters);
  let found = null;
  for (const cluster of all) {
    if (uid.length > 0 ? clusterUid(cluster) === uid : clusterName(cluster) === name) {
      found = cluster;
      break;
    }
  }
  if (found === null) {
    let impostor = "";
    if (uid.length > 0 && name.length > 0) {
      for (const cluster of all) {
        if (clusterName(cluster) === name) {
          impostor = clusterUid(cluster);
          break;
        }
      }
    }
    return {
      state: impostor.length > 0 ? "recreated" : "missing",
      cluster: null,
      uid: uid,
      name: name,
      role: "",
      pinned: false,
      renamedFrom: "",
      recreatedUid: impostor,
    };
  }
  const foundUid = clusterUid(found);
  const foundName = clusterName(found);
  return {
    state: "selected",
    cluster: found,
    uid: foundUid.length > 0 ? foundUid : uid,
    name: foundName,
    role: clusterRole(found),
    pinned: uid.length === 0,
    renamedFrom: uid.length > 0 && name.length > 0 && name !== foundName ? name : "",
    recreatedUid: "",
  };
}

/** THE CONNECTION PROBE, as a verdict plus a freshness judgement. Pure; `now`
 *  is epoch milliseconds and defaults to the caller's clock only when it is
 *  not a number, exactly as the approvals page's age column does.
 *
 *  The verdict comes off `status.reachable` and `status.reason` TOGETHER,
 *  because `reachable` is a tri-state whose absent case has several causes the
 *  CRD's own `status.reason` documentation enumerates:
 *
 *   - `reachable: true`                          -> `reachable`
 *   - `reachable: false`                         -> `unreachable`
 *   - absent, reason in the four refusals        -> `refused`
 *   - absent, reason `ProbeRunning`              -> `probing`
 *   - absent, some other reason                  -> `unknown`
 *   - absent, no reason at all                   -> `never`
 *
 *  `stale` is about the OBSERVATION and is independent of the verdict: a
 *  `reachable: true` older than the budget is a stale success, which is the
 *  case this whole task is about. No `observedAt`, an unparseable one, or one
 *  more than [`PROBE_SKEW_SECONDS`] in the future is stale too -- in the last
 *  case the arithmetic itself is untrustworthy, and presenting an untrustworthy
 *  number as current is the same defect wearing a different hat.
 *
 *  A `probing` verdict is NOT stale: a probe in flight has not observed
 *  anything yet, so there is no observation to be old. Its `observedAt` (when
 *  the controller left one in place) is reported as the PREVIOUS observation
 *  and labelled as such by the caller. */
export function probeState(cluster, now, freshSeconds) {
  const status = ((cluster || {}).status) || {};
  const reason = typeof status.reason === "string" ? status.reason : "";
  const observedAt = typeof status.observedAt === "string" ? status.observedAt : "";
  const budget =
    (typeof freshSeconds === "number" && freshSeconds > 0 ? freshSeconds : PROBE_FRESH_SECONDS) *
    1000;
  const at = typeof now === "number" ? now : Date.now();
  let verdict = "never";
  if (status.reachable === true) {
    verdict = "reachable";
  } else if (status.reachable === false) {
    verdict = "unreachable";
  } else if (CONNECTION_REFUSAL_REASONS.indexOf(reason) !== -1) {
    verdict = "refused";
  } else if (reason === PROBE_RUNNING_REASON) {
    verdict = "probing";
  } else if (reason.length > 0) {
    verdict = "unknown";
  }
  const observedMs = observedAt.length === 0 ? null : Date.parse(observedAt);
  const parsed = observedMs === null || isNaN(observedMs) ? null : observedMs;
  const ageMs = parsed === null ? null : at - parsed;
  // STALENESS IS A STATEMENT ABOUT AN OBSERVATION, so it needs one. With no
  // `observedAt` -- which is every refused connection, because the resolver
  // refuses before a probe Job exists and the refusal clears the whole reading
  // -- there is nothing for "older than the budget" to be about, and the honest
  // answer is `observed: false` and its own word (review finding F2).
  let stale = false;
  if (verdict !== "probing" && parsed !== null) {
    stale = ageMs > budget || ageMs < -PROBE_SKEW_SECONDS * 1000;
  }
  return {
    verdict: verdict,
    reason: reason,
    observedAt: observedAt,
    observedMs: parsed,
    observed: parsed !== null,
    ageMs: ageMs,
    ageSeconds: ageMs === null ? null : Math.floor(ageMs / 1000),
    stale: stale,
    freshSeconds: budget / 1000,
    clusterId: typeof status.clusterId === "string" ? status.clusterId : "",
  };
}

/** An age in seconds as a short human string, or the absent marker. */
export function ageWords(seconds) {
  if (typeof seconds !== "number" || isNaN(seconds)) {
    return cell(null);
  }
  const whole = Math.abs(Math.floor(seconds));
  const suffix = seconds < 0 ? " in the future" : " ago";
  if (whole < 120) {
    return esc(String(whole) + "s" + suffix);
  }
  if (whole < 7200) {
    return esc(String(Math.floor(whole / 60)) + "m" + suffix);
  }
  if (whole < 172800) {
    return esc(String(Math.floor(whole / 3600)) + "h" + suffix);
  }
  return esc(String(Math.floor(whole / 86400)) + "d" + suffix);
}

/** The probe as ONE badge whose words carry the state, prefixed with the noun
 *  so the badge cannot be read as a readiness verdict wherever it lands. */
export function probeBadge(state) {
  const s = state || {};
  const words = VERDICT_WORDS[s.verdict] || VERDICT_WORDS.never;
  return badge(words.kind, PROBE_NOUN + ": " + words.words);
}

/** The STALE badge, or the empty string when the observation is current. It is
 *  its own badge rather than a different colour on the first one: "reachable,
 *  and nobody has checked in two hours" is two facts and a reader needs both.
 *
 *  It can only ever appear beside an observation that EXISTS: `probeState`
 *  leaves `stale` false when there is no `observedAt`, and [`observedBadge`]
 *  carries that case instead. */
export function staleBadge(state) {
  return (state || {}).stale === true ? badge("warn", "stale") : "";
}

/** The `never observed` badge: a connection carrying no `status.observedAt`
 *  that is not already saying so in its verdict.
 *
 *  `never probed` (the verdict for a cluster the controller has written
 *  nothing about) already says it, so this does not repeat it; what it is for
 *  is the `refused` and `unknown` cases, where the verdict says what happened
 *  and this says that nothing was ever measured. */
export function observedBadge(state) {
  const s = state || {};
  if (s.observed === true || s.verdict === "never" || s.verdict === "probing") {
    return "";
  }
  return badge("flat", NEVER_OBSERVED_WORDS);
}

/** The one line that says what the probe observed and when: the badges, the
 *  observed instant with its age, and -- for anything that is not a plain
 *  success -- the CONTROLLER'S OWN `status.reason`, verbatim, with its gloss
 *  when this module has one.
 *
 *  THE REASON IS NEVER REWORDED. `ConnectionConfigInvalid` is what the operator
 *  will grep the controller logs for and what `docs/kubernetes.md` section 20
 *  documents; a page that printed "connection problem" instead would have
 *  thrown away the only handle they have. */
export function probeLine(state) {
  const s = state || {};
  const parts = [probeBadge(s)];
  const stale = staleBadge(s);
  if (stale.length > 0) {
    parts.push(stale);
  }
  const never = observedBadge(s);
  if (never.length > 0) {
    parts.push(never);
  }
  if (s.observedAt) {
    const age = s.ageSeconds === null ? "" : " (" + ageWords(s.ageSeconds) + ")";
    parts.push(
      (s.verdict === "probing" ? "previously observed " : "observed ") +
        "<time datetime=\"" + esc(s.observedAt) + "\">" + esc(s.observedAt) + "</time>" + age,
    );
  } else {
    parts.push("no observation recorded");
  }
  if (s.verdict === "reachable" && s.clusterId) {
    parts.push("cluster id " + esc(s.clusterId));
  }
  if (s.reason && s.verdict !== "reachable") {
    const gloss = REFUSAL_GLOSS[s.reason];
    parts.push(
      "reason <code>" + esc(s.reason) + "</code>" + (gloss === undefined ? "" : " -- " + esc(gloss)),
    );
  }
  if (s.stale === true) {
    parts.push(
      "this observation is older than the " + esc(String(s.freshSeconds)) +
        "s freshness budget, so it describes the past and not the present",
    );
  }
  return parts.join(" ");
}

/** The sentence every probe surface carries once, saying what the reading is
 *  and what it is not. */
export const PROBE_SENTENCE =
  "This is a connection probe: a pod the controller runs on its own cadence dialled these " +
  "brokers once and recorded what happened. It is not a readiness verdict and not a promise " +
  "about right now -- an observation older than the freshness budget is labelled stale, and a " +
  "connection the controller refused to dial at all is labelled with its own reason.";

/** The sentence the "Test connection" control carries.
 *
 *  IT SAYS WHAT THE BUTTON DOES, WHICH IS A READ. This page has no authority to
 *  make the controller dial anything: `KafkaCluster.spec` is immutable, the
 *  page's whole write surface is five creates and one suspend patch, and the
 *  re-probe cadence is the probe Job's own `ttlSecondsAfterFinished` in
 *  `weirkeeper`. So the control re-reads the object and shows the newest
 *  observation the controller has recorded -- which is what "test the
 *  connection" can honestly mean from a browser holding no execution
 *  authority. Saying otherwise would be the same class of lie as rendering a
 *  stale probe as current. */
export const TEST_CONNECTION_SENTENCE =
  "Test connection re-reads this KafkaCluster and shows the newest connection probe the " +
  "controller has recorded. The browser never dials a broker: the controller owns the probe " +
  "and re-runs it on its own cadence, so this reports its latest result rather than forcing a " +
  "new dial.";

/** The "Test connection" control for one cluster, identified by UID.
 *
 *  A BUTTON IN ITS OWN FORM, so a keyboard submit reaches it and so the page
 *  can find it again by the UID rather than by counting rows. `pending`
 *  disables it while a read is in flight -- a second click would be a second
 *  read of the same object, which is harmless and confusing. */
export function renderTestConnection(cluster, pending) {
  const uid = clusterUid(cluster);
  return (
    "<form class=\"probe-test\" data-probe-uid=\"" + esc(uid) + "\" data-probe-name=\"" +
    esc(clusterName(cluster)) + "\"" + (pending === true ? " aria-busy=\"true\"" : "") + ">" +
    "<button type=\"submit\"" + (pending === true ? " disabled" : "") + ">Test connection</button>" +
    "</form>"
  );
}

/** THE PROBE PANEL for one cluster: the badges, the observation, the reason,
 *  the capability label, and the Test connection control. */
export function renderProbePanel(cluster, view) {
  const v = view || {};
  const state = probeState(cluster, v.now, v.freshSeconds);
  return (
    "<section class=\"probe\" id=\"cluster-probe\" data-probe-uid=\"" + esc(clusterUid(cluster)) +
    "\"><h3>Connection probe</h3>" +
    "<p class=\"probe-line\" id=\"cluster-probe-line\">" + probeLine(state) + "</p>" +
    "<p class=\"note\">capability label: <code>" + cell(clusterRole(cluster)) + "</code> -- " +
    "what this cluster is to Logweir. It is a label the adopter picks, not an authorisation.</p>" +
    renderTestConnection(cluster, v.pending === true) +
    "<p class=\"note\">" + TEST_CONNECTION_SENTENCE + "</p>" +
    "<p class=\"note\">" + PROBE_SENTENCE + "</p>" +
    "</section>"
  );
}

// --------------------------------------------------------------- the selector

/** The option a refused selector opens on: no identity, marked `selected`, and
 *  saying what has to happen next. `value=""` is what
 *  [`readClusterSelection`] reads back and what the two forms' own checks
 *  refuse, so a Create click under a standing refusal sends nothing. */
export const EMPTY_OPTION =
  "<option value=\"\" selected data-name=\"\" data-search=\"\">choose a saved connection</option>";

/** The refusal a selection whose UID is gone gets. Named for the two cases it
 *  distinguishes, because they call for different things: a recreated cluster
 *  means someone rebuilt the connection and this reference has to be made
 *  again deliberately, and a missing one means the connection is simply gone. */
export function renderSelectionRefusal(id, resolved) {
  const r = resolved || {};
  const recreated = r.state === "recreated";
  const why = recreated
    ? "<p class=\"refusal\">The KafkaCluster this form had selected is gone, and a DIFFERENT " +
      "object now answers to the name <code>" + esc(r.name) + "</code> -- which is what a " +
      "connection deleted and recreated looks like. A recreated cluster is a different set of " +
      "brokers reached with a different credential, so the selection is refused rather than " +
      "moved onto it. Choose the connection you mean below.</p>"
    : "<p class=\"refusal\">No KafkaCluster in this namespace carries that uid, so the " +
      "connection this form had selected is gone. It was not replaced by another: this page " +
      "will not send a run at a connection nobody chose.</p>";
  return (
    "<div class=\"refusal-block\" id=\"" + esc(id) + "-refusal\" role=\"alert\">" + why +
    "<p class=\"note\">Selected was: KafkaCluster <code>" + esc(r.name) + "</code>, uid <code>" +
    esc(r.uid) + "</code>." +
    (recreated
      ? " The object now under that name has uid <code>" + esc(r.recreatedUid) + "</code>."
      : "") +
    "</p></div>"
  );
}

/** The note a resolved selection carries when the object has been RENAMED
 *  since it was chosen, or when a name-only reference was just pinned. */
export function renderSelectionNote(resolved) {
  const r = resolved || {};
  if (r.state !== "selected") {
    return "";
  }
  if (r.renamedFrom) {
    return (
      "<p class=\"note\" data-selection-renamed=\"true\">This connection has been renamed since " +
      "it was chosen: it was <code>" + esc(r.renamedFrom) + "</code> and is now <code>" +
      esc(r.name) + "</code>. The selection did not move -- it is the same object, uid <code>" +
      esc(r.uid) + "</code> -- so nothing about the run changes.</p>"
    );
  }
  if (r.pinned === true) {
    return (
      "<p class=\"note\" data-selection-pinned=\"true\">This reference named a connection by " +
      "name alone. It resolved to <code>" + esc(r.name) + "</code>, uid <code>" + esc(r.uid) +
      "</code>, and is pinned to that uid from here on: if that object is deleted and recreated, " +
      "this form refuses rather than following the new one.</p>"
    );
  }
  return "";
}

/** One cluster's option caption: its name, its capability label, and its probe
 *  in words. Everything a person needs to pick the right one without leaving
 *  the form. */
export function optionCaption(cluster, state) {
  const words = VERDICT_WORDS[(state || {}).verdict] || VERDICT_WORDS.never;
  return (
    clusterName(cluster) +
    " (role: " + (clusterRole(cluster).length > 0 ? clusterRole(cluster) : "unset") + ")" +
    " -- " + PROBE_NOUN + ": " + words.words +
    ((state || {}).stale === true ? ", stale" : "")
  );
}

/** THE SEARCHABLE SAVED-CLUSTER SELECTOR.
 *
 *  `view` is `{id, name, clusters, selection, now, freshSeconds, label, help,
 *  prefer, query, errors, pending}`:
 *   - `id`        -- the DOM id prefix; `<id>` is the select, `<id>-search` the
 *                    search box, `<id>-uid` the hidden identity input.
 *   - `name`      -- the form field name the select posts under. The UID is
 *                    what it carries.
 *   - `selection` -- `{uid, name}` as the draft holds it.
 *   - `prefer`    -- the role preselected when nothing is selected yet.
 *
 *  THE VALUE IS THE UID AND THE NAME RIDES ALONG. A `<select>` has exactly one
 *  value, and it has to be the identity; but a refusal has to be able to say
 *  which NAME was selected after the object is gone, and a request body still
 *  spells `sourceRef.name` because that is what the CRD takes. So the name
 *  travels in a hidden input beside the select and in each option's
 *  `data-name`, and [`readClusterSelection`] reads both back.
 *
 *  SEARCH FILTERS OPTIONS, IT DOES NOT SHORTEN THE LIST. Each option carries
 *  its own `data-search` haystack; the wiring hides the ones that do not match
 *  and never removes them, so a selected option that stops matching is still
 *  selected and still submitted. A search that narrows a form's answer without
 *  saying so is a search that changes what gets sent. */
export function renderClusterSelector(view) {
  const v = view || {};
  const id = typeof v.id === "string" && v.id.length > 0 ? v.id : "cluster-select";
  const field = typeof v.name === "string" && v.name.length > 0 ? v.name : "cluster";
  const all = savedClusters(v.clusters);
  const resolved = resolveClusterSelection(all, v.selection);
  // A REFUSAL SELECTS NOTHING (review finding F1). `recreated` and `missing`
  // are the two states in which the connection this form was bound to is not
  // there, and the page has just said so. Preselecting a DEFAULT under that
  // refusal -- which is what a `none`-shaped fallback did -- leaves a
  // submit-ready form bound to a connection nobody chose: a browser defaults an
  // unselected `<select>` to its first option, so one more click on Create sent
  // a schedule at whatever happened to sort first. So the two refusal states
  // render an explicit empty option, marked `selected`, and the empty value is
  // what `validateSchedule` and `validateRestore` refuse.
  const refused = resolved.state === "recreated" || resolved.state === "missing";
  const fallback = resolved.state === "selected" || refused
    ? resolved.cluster
    : preferredCluster(all, v.prefer);
  const chosenUid = resolved.state === "selected" ? resolved.uid : "";
  const errors = (v.errors || {})[field] || (v.errors || {})[id];
  const options = all
    .map((cluster) => {
      const uid = clusterUid(cluster);
      const state = probeState(cluster, v.now, v.freshSeconds);
      const selected =
        chosenUid.length > 0
          ? uid === chosenUid
          : resolved.state === "none" && fallback !== null && uid === clusterUid(fallback);
      return (
        "<option value=\"" + esc(uid) + "\"" + (selected ? " selected" : "") +
        " data-name=\"" + esc(clusterName(cluster)) + "\"" +
        " data-role=\"" + esc(clusterRole(cluster)) + "\"" +
        " data-probe=\"" + esc(state.verdict) + "\"" +
        " data-stale=\"" + (state.stale === true ? "true" : "false") + "\"" +
        " data-search=\"" + esc(clusterHaystack(cluster)) + "\">" +
        esc(optionCaption(cluster, state)) + "</option>"
      );
    })
    .join("");
  // THE HIDDEN PAIR IS THE REFUSED PAIR while a refusal stands: it is what the
  // refusal is ABOUT, and it is what a reader, a screenshot and a bug report
  // need. It is never a third cluster's identity, and it is never what gets
  // submitted -- `readClusterSelection` prefers the `<select>`, whose value is
  // the empty option's empty string until somebody picks a connection.
  const chosenName = refused
    ? resolved.name
    : resolved.state === "selected"
      ? resolved.name
      : fallback === null
        ? ""
        : clusterName(fallback);
  const chosenObject = resolved.state === "selected" ? resolved.cluster : (refused ? null : fallback);
  const hiddenUid = refused
    ? resolved.uid
    : (chosenUid.length > 0 ? chosenUid : (chosenObject === null ? "" : clusterUid(chosenObject)));
  const detail =
    chosenObject === null
      ? "<p class=\"note\" id=\"" + esc(id) + "-probe\">no saved connection is selected.</p>"
      : "<p class=\"probe-line\" id=\"" + esc(id) + "-probe\">" +
        probeLine(probeState(chosenObject, v.now, v.freshSeconds)) + "</p>";
  return (
    "<div class=\"cluster-selector\" data-selector=\"" + esc(id) + "\">" +
    (resolved.state === "recreated" || resolved.state === "missing"
      ? renderSelectionRefusal(id, resolved)
      : "") +
    "<div class=\"field\"><label for=\"" + esc(id) + "-search\">search saved connections</label>" +
    "<input id=\"" + esc(id) + "-search\" name=\"" + esc(field) + "Search\" value=\"" +
    esc(typeof v.query === "string" ? v.query : "") + "\">" +
    "<p class=\"help\">Filters the choices below by name, role, bootstrap address, auth mode, " +
    "username, credential Secret name, cluster id or probe reason. Every word must match; the " +
    "selected connection stays selected whether it matches or not.</p></div>" +
    "<div class=\"field\"><label for=\"" + esc(id) + "\">" +
    esc(typeof v.label === "string" && v.label.length > 0 ? v.label : "saved connection") +
    "</label>" +
    "<select id=\"" + esc(id) + "\" name=\"" + esc(field) + "\"" +
    (errors === undefined ? "" : " aria-invalid=\"true\" aria-describedby=\"" + esc(id) + "-error\"") +
    ">" +
    (all.length === 0
      ? "<option value=\"\" selected data-name=\"\">no KafkaCluster in this namespace</option>"
      : (refused ? EMPTY_OPTION : "") + options) +
    "</select>" +
    "<input type=\"hidden\" id=\"" + esc(id) + "-uid\" name=\"" + esc(field) + "Uid\" value=\"" +
    esc(hiddenUid) +
    "\">" +
    "<input type=\"hidden\" id=\"" + esc(id) + "-name\" name=\"" + esc(field) + "Name\" value=\"" +
    esc(chosenName) + "\">" +
    (typeof v.help === "string" && v.help.length > 0
      ? "<p class=\"help\">" + esc(v.help) + "</p>"
      : "") +
    "<p class=\"note\" id=\"" + esc(id) + "-no-match\" hidden>no saved connection matches that " +
    "search; the selected one is still selected.</p>" +
    detail +
    "</div>" +
    renderSelectionNote(resolved) +
    "<p class=\"note\">" + PROBE_SENTENCE + "</p>" +
    "</div>"
  );
}

// ---------------------------------------------------------------- the DOM half

/** Reads one selector's answer back out of a form: the UID the select carries
 *  and the NAME of the option that carries it. Falls back to the hidden inputs
 *  when the select is absent, which is what a form rendered with no saved
 *  cluster looks like. */
export function readClusterSelection(form, id) {
  const select = form.querySelector("#" + id);
  if (select === null || select === undefined) {
    return { uid: hiddenValue(form, id, "uid"), name: hiddenValue(form, id, "name") };
  }
  const uid = String(select.value === undefined || select.value === null ? "" : select.value);
  const option = select.options === undefined ? null : select.options[select.selectedIndex];
  const name =
    option === null || option === undefined || typeof option.getAttribute !== "function"
      ? hiddenValue(form, id, "name")
      : String(option.getAttribute("data-name") || "");
  return { uid: uid, name: name };
}

function hiddenValue(form, id, suffix) {
  const node = form.querySelector("#" + id + "-" + suffix);
  if (node === null || node === undefined) {
    return "";
  }
  return String(node.value === undefined || node.value === null ? "" : node.value);
}

/** Applies a search query to one selector's options IN PLACE: matching options
 *  are shown, non-matching ones hidden, and the SELECTED option is never
 *  hidden. Returns how many options are visible, so the caller can show the
 *  no-match note without counting again. Pure with respect to the selection:
 *  it changes no option's `selected`. */
export function filterSelectorOptions(select, query) {
  if (select === null || select === undefined || select.options === undefined) {
    return 0;
  }
  let visible = 0;
  for (let i = 0; i < select.options.length; i += 1) {
    const option = select.options[i];
    const haystack =
      typeof option.getAttribute === "function" ? String(option.getAttribute("data-search") || "") : "";
    const keep = option.selected === true || haystackMatches(haystack, query);
    option.hidden = !keep;
    if (keep) {
      visible += 1;
    }
  }
  return visible;
}
