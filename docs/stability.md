# Stability

## Known limitations of v0.1

- **S3 credentials come from `object_store`'s own chain, not the AWS SDK's.**
  `logweir-engine-oso`'s `Store::from_url`/`Store::read_only_from_url` build
  the S3 client with `object_store` 0.14's `AmazonS3Builder::from_env()`,
  which applies `object_store`'s OWN credential chain (static keys, then web
  identity / IRSA, ECS, EKS Pod Identity, IMDS). That is **not** the AWS SDK
  chain: `~/.aws/credentials` profiles, `AWS_PROFILE` and SSO are
  unsupported. State it plainly here because an adopter discovering it at
  drill time is a support ticket.

- **MSRV is 1.89**, raised from 1.82 by `object_store` 0.14's aws/azure/gcp/http
  feature set (Global Constraint 9 fixes that crate, version and feature set,
  so the toolchain floor moves instead). Rust edition 2024, required by that
  dependency tree's RustCrypto crates (`digest`/`crypto-common`/
  `block-buffer`), is only supported starting rustc 1.85; several other
  transitive dependencies (`crc-fast`, the `icu_*` crates, `idna_adapter`,
  pulled in through `reqwest`) then raise the floor further to 1.89.

- **Create-only evidence puts: MinIO is the only S3-compatible backend Logweir
  has actually tested.** Three facts follow, and they are stated separately
  because only the second is verified here:
  1. `object_store` 0.14 implements `PutMode::Create` on AWS S3 via the
     conditional `If-None-Match` put.
  2. MinIO is the only S3-compatible backend Logweir has tested. **Real AWS S3
     is NOT exercised in v0.1** — Global Constraint 17 forbids provisioning a
     cloud resource, and no task in this plan supplies a bucket, a region or a
     credential source. The AWS leg is `[UNVERIFIED against real S3]` and is
     confirmed by the first adopter run, not by this plan.
  3. Any other S3-compatible backend that answers `NotSupported` (or
     `NotImplemented`) to `PutMode::Create` takes the HEAD-then-PUT fallback in
     `Store::put_create_only`, which reports `create_only_enforced: false` to
     its caller. That fallback is not equivalent: between the HEAD and the PUT
     there is a window in which a concurrent writer can create the object, so
     it is a genuinely weaker guarantee, not a cosmetic difference. **The
     signed scorecard does not distinguish the two cases** — it publishes
     `create_only_enforced: false` either way, for the reason in the next
     bullet — so the only way to know which path a given backend takes is to
     test that backend, as MinIO was tested here.

- **The signed scorecard's `evidence` block makes NO claim about the upload.**
  Read this before quoting a scorecard's `evidence` block to an auditor.

  Four fields — `create_only_enforced`, `immutable`, `retain_until`,
  `version_id` — describe facts that only exist *after* the scorecard has been
  uploaded. The scorecard is signed *before* that upload (it must be: a
  signature covers bytes, and the bytes have to exist first), and it is never
  re-serialised afterwards, because re-serialising would invalidate the
  signature. So at signing time those four fields cannot be known.

  Rather than sign whatever value happened to be in them, Logweir **zeroes all
  four before signing**. Every scorecard Logweir emits in v0.1 therefore reads:

  ```json
  "evidence": { "create_only_enforced": false, "immutable": false,
                "retain_until": null, "version_id": null }
  ```

  What that does and does not mean:

  - It means **"no proof was obtainable at signing time."** It is *not* a claim
    that the object is mutable, was overwritten, or is unversioned.
  - The document **deliberately under-claims.** A run whose upload really was
    conditional still publishes `create_only_enforced: false`, because phase 8
    cannot know that yet and a signed document is not the place to guess.
  - Conversely — and this is the point — **a signed scorecard can never assert
    WORM protection or create-only enforcement that Logweir did not establish.**
    A valid signature over `immutable: true` would be indistinguishable from a
    verified fact, and nothing downstream (`logweir drill verify` included)
    could tell the difference.
  - The real post-upload readback *is* captured — `Store::put_create_only`
    reports whether the conditional put was used, and
    `Store::object_lock_readback` is consulted — but in v0.1 it is held in
    memory only and **is not published anywhere an auditor can read.** Making
    it auditor-visible requires a second signed receipt written *after* the
    upload, the same shape as the teardown attestation. That is not in v0.1.

  Independently of the above: `object_store` 0.14 — the crate, version and
  feature set Global Constraint 9 fixes — models no Object Lock / WORM API on
  any of its `aws`/`azure`/`gcp`/`http` backends, so
  `Store::object_lock_readback` returns `None` on every backend Logweir can
  build. Even the in-memory readback is therefore always "no proof" for
  `immutable`/`retain_until` today. A bucket genuinely under Object Lock will
  not be recognised as such until that API exists.

### A crashed restore is not resumable in v0.1

The rendered `restore.checkpoint_state` path is **pod-local and is never uploaded**. If the
restore process dies mid-run, there is no checkpoint to resume from: the drill re-runs from
phase 0. The presence of the `checkpoint_state` key in the rendered `restore.yaml` does not
imply resumability, and Logweir does not offer it in v0.1.
