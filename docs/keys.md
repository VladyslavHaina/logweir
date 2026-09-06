# Signing keys: how to make one, how to name it, how to rotate it

Logweir signs a drill scorecard with an ECDSA P-256 key and ships the
signature in a detached `.sig` sidecar. This document is the recipe that
produced the key checked into `e2e/fixtures/signed/`, the definition of the
fingerprint an auditor pins, and the rotation story — because until now the
repository had no documented way to make a key at all.

If you are an auditor rather than a publisher, the document you want is
[`verify-a-scorecard.md`](verify-a-scorecard.md); this one is about the key's
provenance, which that one tells you to pin.

## Generating a keypair — on a laptop, never in the image

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out signing.pem
openssl pkey -in signing.pem -pubout -out public.pem
```

The keypair in `e2e/fixtures/signed/` was produced by exactly these two commands and is now **pinned**: it is read, never regenerated. Running them again in this repository would orphan the `917cf9a299872cbf8b2715999ce457464705bb8f48df0a07e9b1e19bb9f383fd` fingerprint that `verify-a-scorecard.md` teaches auditors to pin — run them only when standing up a **new** publisher's key, outside this tree.

**This is a laptop recipe, and it has to be.** Logweir's runtime image is
`debian:bookworm-slim`, which ships no `openssl`, so neither of these commands
can run inside the image and no Logweir code path invokes `openssl`. Generate
the keypair on an operator workstation (or in whatever key-management system
your organization already trusts), and mount or inject the private half into
the container at run time. Do not add `openssl` to the image to make this
document easier to follow.

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

The practical consequence: re-running the fixture recipe can change the signed
**document** and never the **key**, so a diff in a `.sig` file means the
payload moved, and nothing else.

## Rotation

Logweir has **no** key-rotation subcommand in v0.1. Rotation is entirely
operator-driven, and it is three steps:

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

An auditor who sees a `keyid` that does not match their pinned fingerprint is
told, in `verify-a-scorecard.md`, to stop and resolve it with the publisher out
of band. Announcing a rotation before the first scorecard signed with the new
key reaches them is therefore the publisher's job, not the tooling's.

---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
