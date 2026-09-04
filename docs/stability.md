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
