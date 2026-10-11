# Signing keys: bootstrap, identity, backup and rotation

Logweir signs evidence with a P-256 or Ed25519 key and ships each signature in a
detached `.sig` sidecar. The Helm install bootstraps a persistent P-256
installation identity with no local key generation; external adoption accepts
either algorithm. An auditor wants [`verify-a-scorecard.md`](verify-a-scorecard.md);
this page is about a key's provenance, which that one says to pin.

## Managed installation identity

`helm upgrade --install` runs `logweir identity bootstrap` in a short-lived hook
Job. It fills the retained Secret `logweir-signing-key` exactly once and writes
only public material to the ConfigMap `logweir-signing-trust`. Retries and
upgrades load and validate the winner and never generate again; public trust
without its private key is key loss, and bootstrap stops. Backup, restore,
adoption and reinstall are in
[install.md](install.md#back-up-and-recover-the-installation-identity).

- **Who can touch it.** The bootstrap ServiceAccount can get the fixed Secret
  and ConfigMap and one configured external Secret, and patch only the two
  managed objects; it cannot list Secrets. The controller and console get no
  read on private keys. Runner Jobs mount the Secret because they sign; their
  ServiceAccount has no API token. "Short-lived" is the process and its
  projected token: the name-scoped accounts, Roles and RoleBindings persist with
  the release so later hooks can run, with token automount off except in a hook
  Pod.
- **Other runner namespaces** are listed in
  `identity.authorizedRunnerNamespaces` (the release namespace always is). A
  short-lived distributor copies the established Secret's exact bytes into a
  fixed retained Secret there, and fails if a different signer is present. The
  trust reference is global, so the chart enforces one installation identity per
  cluster.
- **The bootstrap image** reads and patches the private identity, so it is
  separate from `runnerImage` and pinned by digest (a mutable one needs the
  development-only override). The publication gate runs `identity bootstrap
  --help` from the exact candidate digest.
- **The public ConfigMap is a publication record, not authorization.** Its
  `trust-reference` names `TrustRoster/default.spec.signingKeys`; an
  administrator decides to add the key there or, where a `TrustPolicy` governs,
  as an `EvidenceSigning` key. A public key beside an archive is never trusted
  by proximity. **Except once, on a fresh install (PROD-16.1):** the run that
  generates the identity and the console key trusts both in one default
  `TrustPolicy` it creates ([below](#the-console-key-and-the-fresh-installs-trust-prod-161)).

## Generating a fixture or externally managed keypair

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out signing.pem
openssl pkey -in signing.pem -pubout -out public.pem
```

This recipe is for fixtures or explicit external adoption, never a prerequisite
of the managed install: the runtime image, based on `debian:bookworm-slim`,
ships no `openssl`, and bootstrap uses the runner's own signing library.
Generate an externally managed pair in your key system and adopt it as
[install.md](install.md) describes. `signing.pem` is a PKCS#8 PEM private key;
`SigningKey::from_pem_file` **discovers** the algorithm, so a flag cannot select
the wrong one.

## The fingerprint

A key is named, in every signed document and every instruction to an auditor, by
the **SHA-256 of its SubjectPublicKeyInfo DER encoding**:

```bash
openssl pkey -pubin -in public.pem -outform DER | openssl dgst -sha256
```

That digest, `SigningKey::key_id()` (`crates/logweir-evidence/src/keys.rs`) and a
sidecar's `signatures[].keyid` are the same number by construction, so an
auditor can check a sidecar's claim about its key before any cryptography.

## The checked-in fixture key

`e2e/fixtures/signed/signing.pem` is a throwaway test key, committed on purpose
(`.gitignore` ignores `*.pem` and un-ignores that directory, so a fresh checkout
has it). It signs nothing outside this repository's tests
([`../e2e/fixtures/signed/README.md`](../e2e/fixtures/signed/README.md)). Its
fingerprint is:

```
917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd
```

**That is a test fingerprint. No auditor should ever pin it for a real
publisher**: its private half is public. The pair was made with the two commands
above and is **pinned**: regenerating the fixtures reads it.
`SigningKey::load_or_generate` loads a key that exists and mints one only where
none exists (a malformed file is an error, never a re-mint);
`crates/logweir-evidence/examples/mint_fixture.rs` (`just fixtures-sign`) uses
it, and `mint_bogus_fixture.rs` (`just fixtures-sign-bogus`) loads the same key
with `SigningKey::from_pem_file`, so a missing key fails loudly. A diff in a
`.sig` file therefore means the payload moved. Never run the recipe in this tree
except for a **new** publisher, outside it.

## Rotation

Logweir has **no** key-rotation subcommand, and bootstrap refuses to replace an
established identity, even for a different external Secret. Rotation is the
operator's:

1. **Mint a new pair** outside this repository.
2. **Publish the new `public.pem` and its fingerprint out of band** (your TLS
   website, in person, your trusted-key registry), never in the bundle it
   verifies ([`verify-a-scorecard.md`](verify-a-scorecard.md)).
3. **Sign forward with the new key; never re-sign what was published.** An old
   scorecard and sidecar stay valid under the old key forever. Retiring a key
   means nothing new is signed with it, not that its signatures are withdrawn.

Keep the retiring public key and its policy history for old archives; never
delete it because the new signer works. Revocation is a separate policy action
that may invalidate old evidence. On a roster-only cluster, back up and restore
the private Secret, public ConfigMap and roster together on rollback.

## Rotation with a `TrustPolicy`, and old archives still verifying

`TrustPolicy` (cluster-scoped, PLAT-19.1) makes a rotation an overlap.
`Approval` admission, evidence verification, the catalog view, the preflight and
every governed bundle resolve a namespace's trust through its policy (else
`TrustRoster/default`), so a key retired or revoked on a policy is withdrawn
everywhere ([kubernetes.md](kubernetes.md#trust-which-keys-govern-a-namespace)).
A key carries `notBefore`, `notAfter`, `state: Active | Retired | Revoked`,
`retiredAt`, and for a revocation `revocationReason`, `revokedAt` and
`revocationEffectiveFrom` (the CRD refuses `Revoked` without both instants). The
spec is **mutable and one-way**: keys are append-only, `notAfter` only comes
forward, `state` moves `Active → Retired` and `Active | Retired → Revoked` and
never back, and the revocation instants are write-once, so a receipt signed in
March still verifies in December. The controller's one write to the object is a
`resourceVersion`-preconditioned merge patch of the
`logweir.dev/compromise-revocation` finalizer.

### Who administers a `TrustPolicy`, and through what

**`logweir-trust-admin`**, cluster-scoped and shipped unbound, is the only
holder of a write verb on `trustpolicies` (`get`/`list`/`watch`,
`create`/`update`/`patch`, and **no `delete`**: deleting a policy sends its
namespaces back to `legacy-roster-v1`, so withdrawing trust is an edit). Bind it
to someone without `logweir-operator`, who could otherwise add their own key and
approve their own restore:

```bash
kubectl --context docker-desktop create clusterrolebinding logweir-trust-admin \
  --clusterrole=logweir-trust-admin --user=<security-owner>
```

**Not through the product API.** `GET /api/v1/trust-policies[/{name}]` is a
read (the keys view), `capabilities.trustAdministration` is `false`, and the
console's own grants stop at `get`/`list`. Administration is `kubectl apply`
under `logweir-trust-admin`, plus `logweir trust export|migrate-roster`. The
read publishes key ids, states, windows, usages and bound namespaces, and an
evaluation of **`unknown`**, never `valid`, while `status.observedGeneration`
lags `metadata.generation`.

### The supported procedure

1. **Add the new public key to the bound `TrustPolicy`**, `state: Active`,
   `usages: [EvidenceSigning]`. Both keys now sign and verify.
2. **Point the runner at the new private key.** There is no supported in-place
   cutover yet: the signing Secret's name is compiled in (`SIGNING_KEY_SECRET`),
   so this means replacing the established Secret's contents, which bootstrap
   refuses to do for you. A chart value `identity.activeSigningSecretName` and a
   `logweir identity rotate --confirm-current <keyId>` are planned, not shipped.
3. **Wait for in-flight Jobs**, then set the old key `state: Retired`,
   `retiredAt: <now>`.
4. **Old archives keep verifying**, with `trust.basis: Historical`: "verified by
   weirkeeper at `<verifiedAt>` against key `<id>` (signed before that key was
   retired)", a pass.

The verdict is one function of the key's history and the document's claimed
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
| a status written before `signedAt` existed, document not yet re-read | `NotAttempted`, `trust.basis: Unverified` — never green, never `Untrusted`; one bounded re-read repairs it ([kubernetes.md](kubernetes.md#when-trust-changes)) |

Retire a key you are finished with; revoke one whose private half may be in
someone else's hands, and say which with `revocationReason`. A compromise
revocation does not trust the document's own signing time (an attacker writes
it): only a `verifiedAt` this controller recorded earlier counts, so **an
imported archive with no such history and a compromise-revoked signer fails
closed.** There is no trusted timestamping service.

### A compromise revocation outlives the policy that recorded it

A `KeyCompromise` revocation is a fact about the key material, so it applies in
**every namespace**: the recording policy's, one whose policy still lists the
key `Active`, the default policy's and the roster's. Unbinding a namespace does
not re-trust the key; two records take the earlier `revocationEffectiveFrom`;
a key the answering policy does not list stays `UntrustedSigner`. A `Superseded`
or `Unspecified` revocation is not carried. Verification, the catalog's
`signers[].trusted`, the runner's keyring, fresh approvals (`KeyRevoked`),
consumed ones (`RecordedBeforeRevocation`), the preflight, the API and each
policy's `status.keys[]` all read it, and name the policy that recorded it.
Stored verdicts are re-derived when it is recorded. **The reason is sticky (CEL
rule G9):** `KeyCompromise` never becomes `Superseded`; the reverse is allowed.

**The record cannot silently disappear.** Every policy recording a compromise
gets the finalizer `logweir.dev/compromise-revocation`, and its deletion is
**held** (`Terminating`, still resolved through, rechecked every 15 s) until
another live policy records the same revocation or nothing lists the key. The
`CompromiseGuard` condition says which: `CompromiseRecorded`, `DeletionBlocked`
(naming every source still listing the key), `CompromiseInherited` (record it
here too) or `NoCompromiseRecorded`. Residuals: a policy revoked and deleted
before the controller saw it never got the finalizer (wait until `kubectl get
trustpolicy <name> -o jsonpath='{.metadata.finalizers}'` lists it), and anyone
with `patch` or `update` on `trustpolicies` (a cluster-admin, a trust-admin, the
controller's account) can remove it, which trusts the key again wherever listed.

### Replacing a `TrustPolicy` safely

`notBefore`, `usages` and the public material are immutable, so a wrong one is
replaced, which deletes a policy. When it records a compromise:

1. **Export it as it stands now**, never from an older file: `kubectl
   --context <ctx> get trustpolicy <old> -o json | logweir trust export
   --policy <old> --stdin > successor.yaml`. The compromised key comes across as
   `Revoked, KeyCompromise` with its instants.
2. **Give the successor a new name**, make the one correction, apply it. Shared
   namespaces are `TrustPolicyConflict` until the old policy is gone (apply it
   with no `spec.namespaces` first to avoid that window).
3. **Delete the old policy.** The finalizer releases within 15 s; `kubectl
   --context <ctx> get trustpolicy` shows the successor `LOADED True` and
   `CompromiseRecorded`.

**Going back to the roster only:** re-create `TrustRoster/default` without the
compromised key (delete and re-apply; its spec is immutable), then delete the
policies. Afterwards the cluster keeps no memory of the compromise, so remove the
key from every roster SOURCE too (Git, saved exports). **A policy stuck
`Terminating`** (`CompromiseGuard=True/DeletionBlocked`) is released by a policy
recording the same revocation, by revoking the key for `KeyCompromise` on each
named policy (or a cluster-admin deleting it), or by re-creating the roster
without the key. Do not remove the finalizer unless you mean to trust the key
again.

### When a verdict is re-derived, and why the boundary is an instant

Nothing writes to an object when the clock crosses a key's boundary, so the
controllers wake there: an `Approval` that verified requeues at its approver
key's `notAfter`, and a `TrustPolicy` at the earliest future `notBefore`,
`notAfter` or `notAfter` minus thirty days (`ExpiringSoon`). The five-minute
heartbeat stays the ceiling. On a lab run an expired approver key read
`Verified=True` for 2 m 35 s until the heartbeat; waking at the boundary closes
that, leaving the 1 s requeue floor, a controller outage across the boundary and
clock skew. A reader that must never act on a stale `Verified=True` treats a
verdict whose key window has closed as unknown. Only a verdict a clock can
**withdraw** gets a deadline; a refusal an edit would lift waits for the
heartbeat.

### Key usage separation

A key declares **exactly one** use, enforced by the API server (CEL rule G8)
because `usages` is immutable and `spec.keys` append-only. A key presented for
another use is `KeyUsageMismatch` wherever a `TrustPolicy` governs (on the
roster path only labelled, `selfAttestedRisk`), a different refusal from a bad
signature.

| usage | who holds it | what it may do |
|---|---|---|
| `EvidenceSigning` | the runner's signing key (the installation identity) | verify receipts, scorecards, teardown attestations, catalog records |
| `GovernedApproval` | human approvers, on their own machines | sign approval and standing-authorization documents |
| `ConsoleConfirmation` | the API's confirmation key | attest the authenticated requester |

A namespace bound to an approval policy (PLAT-19.2,
[kubernetes.md](kubernetes.md#approvals)) needs on its `TrustPolicy` the
console's `ConsoleConfirmation` key (the hook generates it since PROD-16.1; a
console naming its own Secret, `approvalPolicy.confirmationKeySecret`, holds the
private half under `confirmation.key`; `GET
/api/v1/namespaces/{ns}/approval-policy` prints the loaded key id) and, for a
`Governed` namespace, each approver's `GovernedApproval` key with
**`principal.id` = that approver's own `<issuer>#<subject>`**, the string the
console records as requester. Any other `principal.id` is refused
(`SelfApprovalRefused`): separation of duties compares it with the requester.
The roster never yields a `ConsoleConfirmation` key. Retiring or revoking the
console key refuses every new confirmation and governed request
(`KeyRetired`/`KeyRevoked`); rotate it with an overlap.

### The console key and the fresh install's trust (PROD-16.1)

With a console and the managed identity, bootstrap manages the console's key
like the identity: an Ed25519 key generated ONCE into the retained Secret
`logweir-console-confirmation` (`helm.sh/resource-policy: keep`), its public half
(`key-id`, `confirmation.pub.pem`, `algorithm`, `trust-usage:
ConsoleConfirmation`) in the retained ConfigMap `logweir-console-trust`, never
regenerated; a public half without its key is key loss. A hand-made Secret of
that name is adopted and never trusted automatically: compare its fingerprint
out of band. Back it up with `logweir-signing-key`. This holds while
`identity.bootstrapFeatures.consoleKey` is `true`.

**On a fresh install only**, the run that generates both keys with a console
creates ONE default `TrustPolicy` (`logweir-installation`) with exactly two keys:
the signer (`EvidenceSigning`, principal
`install:<release-ns>/logweir-signing-key`) and the console key
(`ConsoleConfirmation`, principal
`console:<release-ns>/logweir-console-confirmation`), `notBefore` five minutes
before each Secret's creation, `notAfter` `9999-12-31T23:59:59Z` (shorten it when
you rotate), and `allowedTargetClusterIds` from the chart. It never creates one
beside existing trust (any `TrustPolicy` or `TrustRoster/default`), never edits
one, and never runs on an upgrade. **No standing grant:** the `create
trustpolicies` it needs exists only as a `post-install` hook of the first
install (Helm 3.19+ or 4.x; the chart refuses older Helm) and is revoked by the
hook on every exit path; for those seconds, whoever can run a pod as
`<release>-identity-bootstrap` could create a policy (SECURITY.md). After a
failed first install, delete it (`docs/install.md` §5f).

### Migrating from the roster, and rolling back

Until a `TrustPolicy` exists, the controller synthesises `legacy-roster-v1` from
`TrustRoster/default`, with **one deliberate tightening**: a roster signing key's
`notAfter` is enforced, so evidence claiming a later signing time is `Untrusted /
SignedOutsideValidity` (at or before it, `Valid`, `Historical`); an absent
`notAfter` is `9999-12-31T23:59:59Z`. Otherwise `approverKeys` become
`GovernedApproval`, `signingKeys` `EvidenceSigning`, `allowedClusterIds`
`allowedTargetClusterIds`, every key `Active`; one unparseable approver key
refuses every approval, and no `ConsoleConfirmation` key is synthesised.
Migration is explicit:

```bash
kubectl --context <ctx> get trustroster default -o json \
  | logweir trust migrate-roster --stdin --name org-default --default \
  > trustpolicy.yaml
# read it, then:
kubectl --context <ctx> apply -f trustpolicy.yaml
```

It reads no clock and names nothing itself, so it is byte-reproducible. **It
refuses a roster key on both `approverKeys` and `signingKeys`**, naming it (a
policy key has one usage; mint a separate signing key), and invents no
`retiredAt` or revocation. **The roster is not deleted**: it is marked
`Superseded=True/SupersededByTrustPolicy` and stops being consulted, so an older
controller reading only the roster keeps working.

> **Before rolling back, add every post-migration key to
> `TrustRoster/default` as well** — or accept `NotAttempted` on evidence
> signed by it. Nothing deletes public material in either direction.

A controller without `TrustPolicy` support (`v0.1.5`) trusts every roster key,
including one a policy revoked for `KeyCompromise` (the `CompromiseGuard` message
says when the roster still lists one).

> **Before rolling back to a build older than the compromise guard,**
> re-create `TrustRoster/default` without every key any policy revoked for
> `KeyCompromise`, and record each such revocation on EVERY policy that still
> lists the key (a policy reading `CompromiseInherited`). The finalizer stays
> across the rollback and an older build never removes it.

### Backing the policy up

No Logweir role deletes `trustpolicies`, but a cluster-admin can, so back it up
outside the cluster:

```bash
kubectl --context <ctx> get trustpolicy org-default -o json \
  | logweir trust export --policy org-default --stdin > trustpolicy.yaml
```

The output is **public key material only**, rebuilt field by field, without
`status`, `managedFields`, `resourceVersion` or `uid`, so it re-applies onto any
cluster and never removes a key; an input carrying a private-key PEM is refused.
**Export again after every revocation**: an older export lists a revoked key
`Active`, and re-applied on a cluster with no record of the compromise it trusts
the key again. The recovery kit packs these exports
([install.md](install.md#recover-onto-a-clean-installation-with-the-kit)).

### Which policy governs a namespace

A policy names the namespaces it governs; a namespace never names its trust.
Resolution is an exact `spec.namespaces` match, else the one `default: true`
policy, else `legacy-roster-v1`. **A namespace two policies claim resolves to
nothing**, and every approval and verification there is `TrustPolicyConflict`;
two `default: true` policies refuse every namespace that would fall to a default.
An auditor whose pinned fingerprint does not match a `keyid` stops and asks the
publisher, so announcing a rotation before the first new signature reaches them
is the publisher's job.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
