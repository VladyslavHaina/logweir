# Signing keys: bootstrap, identity, backup and rotation

Logweir signs evidence with a P-256 or Ed25519 key and ships each signature in
a detached `.sig` sidecar. The supported Helm install bootstraps a persistent
P-256 installation identity without local key generation; external adoption
accepts either algorithm. This document also records the fixture-key recipe,
the fingerprint an auditor pins and the rotation contract.

If you are an auditor rather than a publisher, the document you want is
[`verify-a-scorecard.md`](verify-a-scorecard.md); this one is about the key's
provenance, which that one tells you to pin.

## Managed installation identity

`helm upgrade --install` runs `logweir identity bootstrap` in a short-lived
hook Job. It atomically fills retained Secret `logweir-signing-key` exactly
once and writes only public material to ConfigMap `logweir-signing-trust`.
Retries and upgrades load and validate the winner; they do not generate again.
If public trust remains but the private key is missing, bootstrap reports key
loss and stops. See [install.md](install.md#back-up-and-recover-the-installation-identity)
for backup, restore, external adoption and reinstall commands.

The bootstrap ServiceAccount can get the fixed managed Secret and ConfigMap,
get one explicitly configured external Secret, and patch only the two managed
objects. It cannot list Secrets. The long-lived controller and UI receive no
private-key read permission. Runner Jobs still mount the managed Secret because
they are the signing authority; their ServiceAccount has no API token.

“Short-lived” describes each bootstrap/distributor process and its projected
token, not automatic RBAC revocation. The narrowly resource-name-scoped
ServiceAccounts, Roles, and RoleBindings persist with the Helm release so later
upgrade, rollback, and retry hooks can run. The accounts default token automount
off; only a hook Pod opts in for its bounded lifetime.

The release namespace is always an authorized runner namespace. Additional
namespaces are explicit in `identity.authorizedRunnerNamespaces`; a separate
short-lived distributor reads only the established primary Secret and
get/patches only the fixed retained target Secret. It copies identical bytes
and fails if a different signer is already present. The current trust reference
is global, so the chart also enforces one installation identity per cluster
until PLAT-19.1 defines a multi-installation lifecycle.

Because bootstrap can read and patch the private identity, its image is
separate from ordinary `runnerImage` selection and must be pinned by digest.
Mutable/local bootstrap images require the explicit development-only override.
The publication gate executes `identity bootstrap --help` from the exact
candidate digest before promotion; the compatible reviewed digest must be
pinned in chart defaults before a release is supported.

The public ConfigMap is a publication record, not authorization. Its
`trust-reference` names the legacy roster location,
`TrustRoster/default.spec.signingKeys`; a cluster administrator decides whether
to add the key there or, where a `TrustPolicy` governs the namespace, as a key
with usage `EvidenceSigning` on that policy (below). A public key arriving
beside an archive is never trusted merely by proximity.

## Generating a fixture or externally managed keypair

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out signing.pem
openssl pkey -in signing.pem -pubout -out public.pem
```

The keypair in `e2e/fixtures/signed/` was produced by exactly these two commands and is now **pinned**: it is read, never regenerated. Running them again in this repository would orphan the `917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd` fingerprint that `verify-a-scorecard.md` teaches auditors to pin — run them only when standing up a **new** publisher's key, outside this tree.

This OpenSSL recipe is for fixtures or explicit external adoption, not a
prerequisite for the managed install. Logweir's runtime image is based on
`debian:bookworm-slim` and ships no `openssl`; bootstrap uses the same
in-process signing library as the runner.
Generate externally managed pairs on an approved workstation or in your key
system, then adopt them as [install.md](install.md) describes.

`signing.pem` is a PKCS#8 PEM private key. Logweir reads it with
`SigningKey::from_pem_file`, which **discovers** the algorithm rather than
taking a flag, so P-256 and Ed25519 keys are both accepted and a mis-set flag
cannot select the wrong one.

## The fingerprint

A key is named, in every document Logweir signs and every instruction it gives
an auditor, by the **SHA-256 of its SubjectPublicKeyInfo DER encoding**:

```bash
openssl pkey -pubin -in public.pem -outform DER | openssl dgst -sha256
```

There is exactly one definition of that value and three places it appears:
the digest `openssl` prints above, the string `SigningKey::key_id()` computes
in `crates/logweir-evidence/src/keys.rs`, and the `signatures[].keyid` field a
`.sig` sidecar carries. They are the same number by construction — which is
what lets an auditor cross-check a sidecar's own claim about which key signed
it before running any cryptography at all.

The checked-in fixture key's fingerprint is:

```
917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd
```

**That is a test fingerprint. No auditor should ever pin it for a real
publisher.** It names a throwaway key that lives in this repository's own test
suite; pinning it against a production scorecard would mean pinning a key whose
private half is public.

## Why a private key is checked in on purpose

`e2e/fixtures/signed/signing.pem` is a throwaway test key committed
deliberately. `.gitignore` ignores `*.pem` repository-wide and then un-ignores
`/e2e/fixtures/signed/*.pem` specifically, so that this one directory's keypair
is staged by `git add -A` and a fresh CI checkout does not fail on a missing
file. It signs nothing outside this repository's test suite, it has never
signed a real drill scorecard, and it must never be used to sign one. See
[`../e2e/fixtures/signed/README.md`](../e2e/fixtures/signed/README.md).

## The Logweir-side pin

Regenerating the signed fixtures **reads** the checked-in key rather than
minting a new one, so the fingerprint above survives regeneration. The code
that does it is `SigningKey::load_or_generate` in
`crates/logweir-evidence/src/keys.rs`: given a path that exists it loads that
key and leaves the file untouched; given a path that does not exist it mints a
fresh P-256 key and writes it there. A path that exists but is malformed is an
**error**, never a silent re-mint. The caller is
`crates/logweir-evidence/examples/mint_fixture.rs`, which `just fixtures-sign`
runs.

A second program reads the same pinned private key:
`crates/logweir-evidence/examples/mint_bogus_fixture.rs`, run by
`just fixtures-sign-bogus`, which loads `e2e/fixtures/signed/signing.pem` with
`SigningKey::from_pem_file` — never `load_or_generate` — so a missing key is a
loud failure rather than a silent re-mint, and which writes only
`e2e/fixtures/signed/scorecard-self-attested-bogus.json` and its `.sig`.

The practical consequence: re-running the fixture recipe can change the signed
**document** and never the **key**, so a diff in a `.sig` file means the
payload moved, and nothing else.

## Rotation

Logweir has **no** key-rotation subcommand in v0.1. Bootstrap deliberately
refuses to replace an established identity, including when a different
external Secret is configured. Rotation is operator-driven, and it is three
steps:

1. **Mint a new pair** with the recipe above, on a laptop, outside this
   repository.
2. **Publish the new `public.pem` and its fingerprint through the out-of-band
   channel** — the publisher's own website over TLS, read aloud or handed over
   in person, or your organization's trusted-key registry. The channel must be
   independent of the scorecards themselves, for the reason
   [`verify-a-scorecard.md`](verify-a-scorecard.md) gives: a key that arrives
   in the same bundle as the document it verifies proves only that the bundle
   is internally consistent.
3. **Sign forward-going scorecards with the new key. Do not re-sign anything
   already published.** A signature is over a fixed byte string; an existing
   scorecard and its existing sidecar remain valid under the old key forever,
   and re-issuing them under the new one would replace evidence an auditor may
   already have pinned. Retiring a key means "nothing new is signed with it",
   not "everything it signed is withdrawn".

Keep the retiring public key and its policy history for old archives. Routine
retirement means no new signatures and does not invalidate old evidence;
revocation is a distinct policy action that may do so. The immutable, globally
named `TrustRoster/default` cannot express an overlap update without an
administrator-managed replacement window — which is what `TrustPolicy` below
replaces. On a roster-only cluster, back up and restore the matching private
Secret, public ConfigMap and roster together on rollback. Never delete the old
public key merely because the new signer works.

## Rotation with a `TrustPolicy`, and old archives still verifying

> **IN EFFECT since PLAT-19.1 (D3 W10).** `Approval` admission, evidence
> verification, the catalog view, the restore preflight and every governed
> restore bundle resolve the namespace's trust through its `TrustPolicy`
> (falling back to `TrustRoster/default` only where no policy governs), so **a
> key retired or revoked on a policy is withdrawn**: a fresh approval refuses
> it, a fresh verification refuses it, and an object that already finished has
> its verdict re-derived when the policy changes
> ([kubernetes.md](kubernetes.md) §8, *Trust resolution*, and §15.2b). An
> earlier revision of this notice said the policy was not consulted yet; that
> stopped being true when PLAT-19.1 landed, and it is recorded here because
> older copies of this page are still in circulation.

`TrustPolicy` (cluster-scoped, PLAT-19.1) is what makes a rotation an overlap
instead of a replacement. A key on it carries a lifecycle — `notBefore`,
`notAfter`, `state: Active | Retired | Revoked`, `retiredAt`, and for a
revocation a `revocationReason`, a `revokedAt` and a `revocationEffectiveFrom`
(the CRD refuses a `Revoked` key without both instants) — and the spec
is deliberately **mutable and one-way**: keys are append-only, `notAfter` may
only be brought forward, `state` moves `Active → Retired` and
`Active | Retired → Revoked` and never backwards, and the revocation instants
are write-once. Public material can never be edited out, because a receipt
signed in March must still verify in December.

The controller holds `list`, `watch` and `patch` on the status subresource, and
`patch` on the object for one field: the `logweir.dev/compromise-revocation`
finalizer (below, *A compromise revocation outlives the policy that recorded
it*). It never edits a key's lifecycle — its one write to the object is a merge
patch of `metadata.finalizers` under a `resourceVersion` precondition, and a
test pins that body.

### Who administers a `TrustPolicy`, and through what

**`logweir-trust-admin`**, a cluster-scoped ClusterRole `logweir.yaml` ships
unbound, is the only holder of a write verb on `trustpolicies`. It carries
`get`/`list`/`watch` so the holder can read what they are about to change, and
`create`/`update`/`patch` so a policy can be written and rotated — `patch`
beside `update` because `kubectl edit` and `kubectl apply` both send one. It
carries **no `delete`**: deleting a policy does not retire a key, it removes the
binding that governs a namespace and sends every namespace it bound back to
`legacy-roster-v1`. Withdrawing trust is an edit. A policy that records a
`KeyCompromise` revocation is additionally **held** on deletion until the
revocation is recorded somewhere else — see *Replacing a `TrustPolicy`
safely*.

```bash
kubectl --context docker-desktop create clusterrolebinding logweir-trust-admin \
  --clusterrole=logweir-trust-admin --user=<security-owner>
```

Bind it to somebody who does **not** hold `logweir-operator`: an operator who
could edit a trust policy could add their own key and then approve their own
restore, which is the one separation `self_attested: false` exists to make
possible. `logweir-viewer`, `logweir-operator` and `logweir-approver` are
read-only on the kind, and a test asserts it rather than leaving it to a reader
of four files.

**Not through the product API.** `GET /api/v1/trust-policies[/{name}]` is a
read — it is what the console's keys view renders — and there is no write route
and no action that could take one: `capabilities.trustAdministration` is
`false` in this release. The console ServiceAccount's own grants stop at
`get`/`list` on `trustpolicies`, so even a defect in the service's authorization
could not produce a write. The supported administration path is `kubectl apply`
under `logweir-trust-admin`, plus the `logweir trust export|migrate-roster`
helpers; a cluster-scoped write that decides whose keys may sign an approval is
a separately reviewed admin path, not a console button.

What the read surface publishes, and what it deliberately does not: key ids,
states, windows, usages and the namespaces a policy binds, plus an evaluation
column that renders **`unknown`** — never `valid` — for a policy whose
`status.observedGeneration` lags its `metadata.generation`. An unevaluated
policy is not a trusted one.

### The supported procedure

1. **Add the new public key to the bound `TrustPolicy`** as `state: Active`,
   `usages: [EvidenceSigning]`. The overlap begins here: both keys are valid,
   both sign, both verify, and there is no window in which nothing is trusted.
2. **Point the runner at the new private key.** *Planned (PLAT-19.1, not
   shipped):* a new retained Secret selected by a chart value
   `identity.activeSigningSecretName`. **That value does not exist yet** — the
   signing Secret name is still compiled in — so today this step means
   replacing the contents of the established Secret, which
   `logweir identity bootstrap` deliberately refuses to do for you. There is no
   supported in-place runner cutover in this release. It is not a chart change
   alone: the value has to back a new controller environment variable replacing
   the compiled `SIGNING_KEY_SECRET` constant, and a `logweir identity rotate
   --confirm-current <keyId>` that mints the new pair without touching the old
   one. Step 1 and steps 3-4 below are shipped and work; only the cutover in
   this step is not.
3. **Wait for in-flight Jobs**, then set the old key `state: Retired` with
   `retiredAt: <now>`.
4. **Old archives keep verifying.** A retired key's evidence verifies with
   `trust.basis: Historical`, which is not a downgrade of `Current`: it is the
   honest answer for a key that was valid when it signed. The badge reads
   "verified by weirkeeper at `<verifiedAt>` against key `<id>` (signed before
   that key was retired)" — the console's `HISTORICAL_SUFFIX`, and
   `kubernetes.md` §15.2's wording.

Nothing in step 3 invalidates anything. The rule the controller applies is one
pure function of the key's declared history and the document's own claimed
signing time:

| key state | verdict for stored evidence |
|---|---|
| `Active`, signed inside validity | `Valid`, `trust.basis: Current` |
| `Expired`/`Retired`, signed at or before `notAfter`/`retiredAt` | `Valid`, `trust.basis: Historical` |
| `Expired`/`Retired`, claiming a later signing time | `Untrusted`, `SignedOutsideValidity` |
| `Revoked` with `Superseded`/`Unspecified` | a retirement at `revocationEffectiveFrom` |
| `Revoked` with `KeyCompromise`, controller observation before the revocation | `Untrusted`, `RecordedBeforeRevocation` — rendered with the recorded instant, never green |
| `Revoked` with `KeyCompromise`, no such observation | `Untrusted`, `Revoked` |
| key absent from the policy | `Untrusted`, `UntrustedSigner` |
| usage mismatch | `Untrusted`, `KeyUsageMismatch` |
| a status written before `signedAt` existed, document not yet re-read | `NotAttempted`, `trust.basis: Unverified` — never green, and never `Untrusted` either: nothing has been compared. One bounded re-read of the run's own receipt repairs it (`docs/kubernetes.md` §15.2c) |

Retirement and revocation are **different actions**. Retire a key you are
finished with; revoke one whose private half may be in someone else's hands,
and say which with `revocationReason`. A compromise revocation does not accept
the document's own claim about when it was signed — that claim is exactly what
an attacker holding the key can write — so the only evidence accepted is a
`verifiedAt` this installation's own controller recorded on an earlier
reconcile. **An imported archive with no such history and a compromise-revoked
signer fails closed.** There is no trusted timestamping service in this
release; that is future work, and until it exists the honest answer for
evidence this installation never observed is a refusal.

### A compromise revocation outlives the policy that recorded it

A `KeyCompromise` revocation says the private half may be in someone else's
hands. That is a fact about the **key material**, not about the policy you
wrote it on, so this controller applies it everywhere
(TRUSTPOLICY-DELETE-DROPS-REVOCATION, found by the PoC round's rehearsal R2,
where deleting the revoking policy sent the namespace back to a roster that
still listed the key and all six of its backups re-verified `Valid`):

* **Every namespace, whatever it resolves through.** Once any `TrustPolicy`
  records key `K` as `state: Revoked, revocationReason: KeyCompromise`, `K` is
  revoked for compromise in every namespace — the recording policy's own, one
  bound to another policy that still lists `K` as `Active`, the `default: true`
  policy, and a namespace that falls back to `legacy-roster-v1`. Removing a
  namespace from the policy's `spec.namespaces` therefore does not re-trust
  `K` there. Two records of one compromise take the earlier
  `revocationEffectiveFrom`. A key the answering policy does not list at all
  stays `UntrustedSigner`; nothing is added, only revoked. A `Superseded` or
  `Unspecified` revocation is NOT carried: it is a statement about your
  rotation, not about the key.
* **What reads it.** Evidence verification and re-trust (`Backup`, `Restore`,
  rehearsal Restores), the recovery catalog view and its `signers[].trusted`,
  the runner's evidence keyring, fresh approvals (`KeyRevoked`), consumed
  approvals (`RecordedBeforeRevocation`), the preflight `signer.rostered` row,
  the API's `trust.state`, and each policy's own `status.keys[]` — so the keys
  view shows `Revoked` for a key a policy still declares `Active`. A refusal
  caused by a record on another policy names it ("recorded by
  TrustPolicy/…"). Stored verdicts are re-derived on the event that records the
  compromise, in EVERY namespace. A terminal `Restore` in a namespace on the
  roster does not wait for a restart ([kubernetes.md](kubernetes.md) §8,
  *Trust resolution*).
* **The reason is sticky (CEL rule G9).** Once a revoked key's
  `revocationReason` is `KeyCompromise` it stays so; editing it to
  `Superseded` would turn "never green" into a retirement and re-verify every
  document the key signed before the instant. A supersession may still be
  escalated to a compromise.
* **The record cannot silently disappear.** The controller places the
  finalizer `logweir.dev/compromise-revocation` on every policy that records a
  compromise, and a `kubectl delete` of such a policy is **held** (the object
  stays, `Terminating`, and is still resolved through) until either another
  policy without a `deletionTimestamp` records the same revocation, or nothing
  in the cluster — no other policy, not `TrustRoster/default` — lists the key
  any more. The `CompromiseGuard` condition says which: `CompromiseRecorded`
  (guarded), `DeletionBlocked` (held, naming every source that still lists the
  key), `CompromiseInherited` (this policy lists a key another policy revoked;
  record it here too), or `NoCompromiseRecorded`. A held policy is looked at
  again every 15 s.

**The residuals, said plainly.** A policy revoked and deleted before the
controller reconciled it once (or while it was down) was never given the
finalizer, and the API server refuses a new finalizer on an object already
being deleted. Wait until `kubectl get trustpolicy <name> -o
jsonpath='{.metadata.finalizers}'` lists `logweir.dev/compromise-revocation`
before any further change. And anyone holding `patch` or `update` on
`trustpolicies` can remove the finalizer by hand: a cluster-admin (`stability.md`
O0, outside the threat boundary), a `logweir-trust-admin` holder, or the
controller's own ServiceAccount. A trust-admin holds no `delete`, so they
cannot start a deletion, but they can complete one a cluster-admin started and
the guard is holding. Removing it trusts the key again wherever it is still
listed, which is exactly what the condition's message warns.

### Replacing a `TrustPolicy` safely

`notBefore`, `usages` and the public material are immutable, so a policy
applied with a wrong one is replaced, not edited — and a replacement is the one
routine operation that deletes a policy. When the policy records a compromise:

1. **Export it as it stands now**, never from an older file:
   `kubectl --context <ctx> get trustpolicy <old> -o json | logweir trust export
   --policy <old> --stdin > successor.yaml`. The export carries every key with
   its current state, so the compromised key comes across as `Revoked,
   KeyCompromise` with its instants. A file saved before the revocation lists
   the key `Active`: this controller still refuses it (the old policy's record
   applies), but an older one would not.
2. **Give the successor a new name** and make the one correction you need.
   Apply it. If it names the same namespaces, those namespaces are contested
   until the old policy is gone — every approval and verification there is
   refused with `TrustPolicyConflict` for that window, which is the fail-closed
   side. (To avoid the window, apply the successor with no `spec.namespaces`
   first and add them after step 3.)
3. **Delete the old policy.** The finalizer is released on the next pass
   (within 15 s) because the successor records the same revocation; check with
   `kubectl --context <ctx> get trustpolicy` that the old one is gone and the
   successor reads `LOADED True` and its `CompromiseGuard` condition
   `CompromiseRecorded`.

**Going back to the roster only** (a rollback, or retiring `TrustPolicy`
altogether): first re-create `TrustRoster/default` without the compromised key
(its spec is immutable, so delete and re-apply it — namespaces on the roster
fall to `RosterNotFound` for that moment, the fail-closed side), then delete
the policies. The guard releases the last record because nothing lists the
key any more — which is also the only state a roster-only controller cannot
re-trust it from. **The release is a point-in-time check, and afterwards the
cluster keeps no memory of the compromise.** Re-creating the roster from an old
export, or a GitOps re-sync of an old roster manifest, trusts the key again.
Remove the key from every roster SOURCE (the file in Git, saved exports), not
only from the live object, for the same reason as *Export again after every
revocation* below.

**A policy stuck `Terminating`** reads `CompromiseGuard=True/DeletionBlocked`,
and its message names the key and every source still listing it
(`TrustRoster/default` or `TrustPolicy/<name>`). Any one of these releases it:
- apply a policy recording the same revocation;
- for each `TrustPolicy/<name>` named, revoke the key there for `KeyCompromise`
  (a policy cannot drop a key, so that policy becomes a carrier), or delete it
  (cluster-admin);
- when the roster is named, re-create it without the key.

Do not remove the finalizer by hand unless you mean to trust the key again.

### When a verdict is re-derived, and why the boundary is an instant

A verdict about a key is a function of the key's declared history **and of the
clock**, and nothing writes to the object when the clock crosses a boundary. So
the controllers wake at the boundary itself:

* an `Approval` that verified requeues at its **matched approver key's
  `notAfter`** — the instant `may_sign_new` starts refusing it;
* a `TrustPolicy` requeues at the earliest future `notBefore`, `notAfter`, or
  `notAfter` minus thirty days among its keys — the three instants at which
  `status.keys[].effectiveState` or the `ExpiringSoon` condition changes with
  nobody editing anything.

Both keep the five-minute heartbeat as the ceiling, so `evaluatedAt` still moves
and "stale" stays distinguishable from "stopped"; the deadline only ever brings
a wakeup **forward**, and a boundary already in the past is not re-armed.

**Why this is a correctness rule and not a tuning knob.** On a lab run an
approval whose approver key had expired read `Verified=True` across 22
consecutive samples over 2 m 35 s, at one unchanged `resourceVersion`, before
the next heartbeat re-derived it to `KeyIdExpired`. An expired key shown as a
valid authorisation is exactly what *"treat unevaluated or stale expiry
information as unknown, not valid"* forbids. Shortening the heartbeat would have
traded the lag for a permanent write-free reconcile on every object of these
kinds and still left a window; waking at the boundary costs one reconcile per
key lifetime and closes the scheduled part of it. What remains is stated, not
hidden: the 1 s requeue floor plus reconcile latency, a controller outage that
spans the boundary (the timer re-arms on restart, but nothing re-derives while
the controller is down), and clock skew between the controller and whatever
wrote `notAfter`. A reader that must never act on a stale `Verified=True` treats
a verdict whose key window has closed as unknown until re-derived — that
reader-side rule, not this timer, is what closes those gaps.

The asymmetry is deliberate: only a verdict a clock can **withdraw** carries a
deadline. A refusal that an edit would turn into a pass — a roster installed, a
key added — waits for the heartbeat, because a closed door left closed a few
minutes too long is not the failure this rule is about.

### Key usage separation

A key declares what it may do, **exactly one** of the three uses below, enforced
by the API server (CEL rule G8) because `usages` is immutable once written and
`spec.keys` is append-only — a key that both attests and authorises could never
be narrowed afterwards. A key presented for the wrong use is refused with
`KeyUsageMismatch` wherever a `TrustPolicy` governs; on the roster path the
overlap is still only *labelled*, as `selfAttestedRisk`.

| usage | who holds it | what it may do |
|---|---|---|
| `EvidenceSigning` | the runner's signing key (the installation identity) | verify receipts, scorecards, teardown attestations, catalog records |
| `GovernedApproval` | human approvers, on their own machines | sign approval and standing-authorization documents |
| `ConsoleConfirmation` | the API's confirmation key | attest the authenticated requester |

`KeyUsageMismatch` is a different refusal from a bad signature and points at a
different fix.

**PLAT-19.2 puts the third usage to work.** A namespace the installation binds
to an approval policy (`docs/kubernetes.md` §8, *Approval policy*) verifies
authorization document v2, and the keys it needs on its `TrustPolicy` are:

* the console's key, usage `ConsoleConfirmation` — generate it once
  (`openssl genpkey -algorithm ed25519 -out confirmation.key`), give the private
  half to the console as a Secret (`approvalPolicy.confirmationKeySecret`, key
  `confirmation.key`) and nothing else, and put the public half here;
  `GET /api/v1/namespaces/{ns}/approval-policy` prints the key id the console
  loaded. It attests who asked; under an `Ordinary` binding that attestation is
  the whole authorization, and under `Governed` it authorises nothing alone.
* for a `Governed` namespace, each approver's key, usage `GovernedApproval`,
  with **`principal.id` set to that approver's own `<issuer>#<subject>`** — the
  exact string the console records as a requester. Separation of duties compares
  this principal with the console-attested requester, so a key whose principal
  is a display name or an email proves nothing: it would differ from every
  requester, including its own holder. Such a key is therefore **refused**
  (`SelfApprovalRefused`) under a Governed policy: the controller fails closed on
  any `principal.id` that is not `<issuer>#<subject>`.

The legacy roster never yields a `ConsoleConfirmation` key, so a namespace must
be governed by a `TrustPolicy` before an approval policy can take effect in it.
Retiring or revoking the console key refuses every new ordinary confirmation and
governed request at once (`KeyRetired`/`KeyRevoked`), exactly as for any other
key; rotate it with an overlap, as below.

### Migrating from the roster, and rolling back

Until a `TrustPolicy` exists, a controller synthesises `legacy-roster-v1` from
`TrustRoster/default`. Behaviour is today's, with **one deliberate
tightening**: the roster's `signingKeys[].notAfter` is carried verbatim and
then *enforced*, so evidence claiming a signing time **after** an expired
signing key's `notAfter` becomes `Untrusted / SignedOutsideValidity` where the
roster path renders it green. Evidence signed at or before that instant stays
`Valid` (basis `Historical`). The blast radius is narrow — `notAfter` is
optional on a roster entry and an absent one is synthesised to
`9999-12-31T23:59:59Z`, so only rosters where somebody explicitly set a
signing-key expiry are affected at all — and the evidence it catches is
evidence signed by a key past its own declared expiry, which is the hole
PLAT-19.1 exists to close. Everything else maps across unchanged:
`approverKeys` become
`GovernedApproval`, `signingKeys` become `EvidenceSigning`,
`allowedClusterIds` becomes `allowedTargetClusterIds`, `notAfter` is carried
verbatim, every key is `Active`, and a roster with one unparseable
`approverKeys` entry still refuses every approval — a partially loaded roster
is not a roster. No `ConsoleConfirmation` key is ever synthesised.

Migration is explicit and reviewable. Nothing applies it for you:

```bash
kubectl --context <ctx> get trustroster default -o json \
  | logweir trust migrate-roster --stdin --name org-default --default \
  > trustpolicy.yaml
# read it, then:
kubectl --context <ctx> apply -f trustpolicy.yaml
```

The command reads no clock and generates no name, so re-running it over the
same roster produces byte-identical output. **It refuses a roster key that is
on both `approverKeys` and `signingKeys`**, naming the key id: a policy key
declares exactly one usage (CEL rule G8), so such a key cannot be expressed at
all, and emitting it would produce a file the API server rejects after you had
reviewed it. Issue a separate `keyId` per usage — mint a new signing key and
keep the old one as the approver. The roster keeps working unchanged until you
do. It invents no `retiredAt` and no revocation: the roster records no lifecycle event, those fields are write-once
on the CRD, and a migration that wrote one would assert something nobody
recorded and could never take back.

**The roster is not deleted.** Once a matching policy exists,
`TrustRoster/default` is marked `Superseded=True/SupersededByTrustPolicy` and
stops being consulted for bound namespaces — a status condition, with the spec
untouched. That is the whole rollback story: an older controller reads only
`TrustRoster/default`, which is still present and unchanged, so governed
approvals and evidence verification keep working.

The one thing rollback does not carry is keys added to the `TrustPolicy`
*after* the migration, which an older controller has never heard of. So:

> **Before rolling back, add every post-migration key to
> `TrustRoster/default` as well** — or accept `NotAttempted` on evidence
> signed by it. Nothing deletes public material in either direction.

**And the one thing rollback must not carry: a compromised key.** A controller
without `TrustPolicy` support (`v0.1.5`) reads only the roster and cannot
express a revocation, so it trusts every key the roster lists — including one a
policy revoked for `KeyCompromise`. The policy's `CompromiseGuard` message says
when the roster still lists such a key ("TrustRoster/default still lists …").

> **Before rolling back to a build older than the compromise guard,**
> re-create `TrustRoster/default` without every key any policy revoked for
> `KeyCompromise`, and record each such revocation on EVERY policy that still
> lists the key (a policy reading `CompromiseInherited`): an older
> `TrustPolicy`-aware build applies a revocation only in the namespaces of the
> policy that records it. The finalizer stays on the objects across the
> rollback and an older build never removes it, so a deletion made while it
> runs is held until this build is back.

### Backing the policy up

RBAC grants `delete` on `trustpolicies` to no Logweir role, but a
cluster-admin is outside the threat boundary (`stability.md` O0), so the object
needs a backup that is not the cluster:

```bash
kubectl --context <ctx> get trustpolicy org-default -o json \
  | logweir trust export --policy org-default --stdin > trustpolicy.yaml
```

The output is **public key material only**, rebuilt field by field rather than
filtered, with `status`, `managedFields`, `resourceVersion` and `uid` absent so
it re-applies cleanly onto any cluster. Re-applying it never removes a key.
An input carrying a private-key PEM is refused and nothing is written.

**Export again after every revocation.** An export is a snapshot: one taken
before a key was revoked lists it `Active`, and re-applying it as a new policy
(or onto a fresh cluster) lists the key as trusted again. On this cluster the
compromise record the live policy carries still applies; on a cluster that has
no such record, nothing does.

### Which policy governs a namespace

A namespace never names its own trust — a policy names the namespaces it
governs, for the reason `kubernetes.md` §8 gives about the roster's fixed name.
Resolution is: an exact `spec.namespaces` match, else the one policy with
`default: true`, else the synthesised `legacy-roster-v1`. **A namespace two
policies both claim resolves to nothing**, and every approval and verification
there is refused with `TrustPolicyConflict` — picking one would be a trust
decision made by a sort order. Two policies setting `default: true` are the
same fault one level up, and every namespace that would have fallen to a
default is refused the same way.

An auditor who sees a `keyid` that does not match their pinned fingerprint is
told, in `verify-a-scorecard.md`, to stop and resolve it with the publisher out
of band. Announcing a rotation before the first scorecard signed with the new
key reaches them is therefore the publisher's job, not the tooling's.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
