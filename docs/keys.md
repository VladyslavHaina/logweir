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
`trust-reference` points at the current verifier's actual location,
`TrustRoster/default.spec.signingKeys`; a cluster administrator decides whether
to add it. A public key arriving beside an archive is never trusted merely by
proximity.

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

> **NOT YET IN EFFECT. Read this before acting on anything below it.**
>
> `TrustPolicy` is served, validated by the API server and reconciled: you can
> create one, the controller parses every key, resolves it against the clock
> and reports `status.keys[].effectiveState`, and `kubectl get trustpolicy`
> renders it. **Nothing consults it for a verification or an approval yet.**
> `Approval` admission and evidence verification both still read
> `TrustRoster/default` and nothing else.
>
> The practical consequence, stated plainly because it is the one that bites:
> **a revocation you record on a `TrustPolicy` today is not applied.** Set a key
> to `state: Revoked, revocationReason: KeyCompromise` and the policy will show
> `effectiveState: Revoked` while governed approvals signed by that key are
> still accepted, because the code that admits them has never read the policy.
> **To withdraw a key today, remove it from `TrustRoster/default`'s
> `approverKeys` / `signingKeys`** — that is still the only enforcement point.
>
> The consumer is PLAT-19.1's verification worker, which replaces `load_roster`
> in `weirkeeper::controllers::approval` and the roster read in
> `weirkeeper::verification`. Until it lands, treat everything below as the
> contract those two will implement — accurate about the shapes, and not yet
> about the enforcement. `docs/stability.md`'s rule applies: a documented
> guarantee the code does not deliver is a defect, so this notice is part of
> the document and stays until the wiring does.

`TrustPolicy` (cluster-scoped, PLAT-19.1) is what makes a rotation an overlap
instead of a replacement. A key on it carries a lifecycle — `notBefore`,
`notAfter`, `state: Active | Retired | Revoked`, `retiredAt`, and for a
revocation a `revocationReason` and a `revocationEffectiveFrom` — and the spec
is deliberately **mutable and one-way**: keys are append-only, `notAfter` may
only be brought forward, `state` moves `Active → Retired` and
`Active | Retired → Revoked` and never backwards, and the revocation instants
are write-once. Public material can never be edited out, because a receipt
signed in March must still verify in December.

The controller holds `list`, `watch` and `patch` on the status subresource and
nothing else — it never edits a key's lifecycle. **Today no Logweir ClusterRole
grants `update` on `trustpolicies` at all**, so editing one is a cluster-admin
action. *Planned (PLAT-19.1, not shipped):* a `logweir-trust-admin` ClusterRole
that is the only holder of that verb, with operator and approver read-only.
Until it exists, scope the permission yourself.

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
   supported in-place runner cutover in this release.
3. **Wait for in-flight Jobs**, then set the old key `state: Retired` with
   `retiredAt: <now>`.
4. **Old archives keep verifying.** A retired key's evidence verifies with
   `trust.basis: Historical`, which is not a downgrade of `Current`: it is the
   honest answer for a key that was valid when it signed. The badge carries
   "verified against retired key `<id>` (signed before retirement)".

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

### Key usage separation

A key declares what it may do, **exactly one** of the three uses below, enforced
by the API server (CEL rule G8) because `usages` is immutable once written and
`spec.keys` is append-only — a key that both attests and authorises could never
be narrowed afterwards. A key presented for the wrong use is refused with
`KeyUsageMismatch` **once the verification worker lands** (see the notice at the
top of this section); on the roster path the overlap is still only *labelled*,
as `selfAttestedRisk`.

| usage | who holds it | what it may do |
|---|---|---|
| `EvidenceSigning` | the runner's signing key (the installation identity) | verify receipts, scorecards, teardown attestations, catalog records |
| `GovernedApproval` | human approvers, on their own machines | sign approval and standing-authorization documents |
| `ConsoleConfirmation` | the API's confirmation key | attest the authenticated requester |

`KeyUsageMismatch` is a different refusal from a bad signature and points at a
different fix.

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
