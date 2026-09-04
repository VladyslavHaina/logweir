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
     `Store::put_create_only` and reports `evidence.create_only_enforced:
     false` in the signed scorecard. That fallback is not equivalent: between
     the HEAD and the PUT there is a window in which a concurrent writer can
     create the object, so a `false` here means "an existing object may have
     been overwritten" and should be read as a weaker guarantee, not a
     cosmetic difference.

- **`evidence.immutable` is always `false` in v0.1, on every backend.**
  Spec §6 C3 permits `true` only after a provider readback of Object Lock /
  WORM retention, and `object_store` 0.14 — the crate, version and feature set
  Global Constraint 9 fixes — models no such API on any of its
  `aws`/`azure`/`gcp`/`http` backends. `Store::object_lock_readback` therefore
  returns `None` everywhere and phase 8 publishes `immutable: false` with
  `retain_until: null`. A `false` here means "no proof was obtainable", **not**
  "the object is known to be mutable"; a bucket really under Object Lock will
  still read `false` until that readback exists. Any claim to the contrary
  supplied by a caller is overwritten, never carried through.

### A crashed restore is not resumable in v0.1

The rendered `restore.checkpoint_state` path is **pod-local and is never uploaded**. If the
restore process dies mid-run, there is no checkpoint to resume from: the drill re-runs from
phase 0. The presence of the `checkpoint_state` key in the rendered `restore.yaml` does not
imply resumability, and Logweir does not offer it in v0.1.
