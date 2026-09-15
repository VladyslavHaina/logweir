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
revocation is a distinct policy action that may do so. The current immutable,
globally named `TrustRoster/default` cannot express an overlap update without
an administrator-managed replacement window. PLAT-19.1 will introduce explicit
trust-policy references and overlapping validity; until then, back up and
restore the matching private Secret, public ConfigMap and roster together on
rollback. Never delete the old public key merely because the new signer works.

An auditor who sees a `keyid` that does not match their pinned fingerprint is
told, in `verify-a-scorecard.md`, to stop and resolve it with the publisher out
of band. Announcing a rotation before the first scorecard signed with the new
key reaches them is therefore the publisher's job, not the tooling's.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
