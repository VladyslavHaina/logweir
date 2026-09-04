# Stability

This file did not exist before Task 12b. `deny.toml`'s `RUSTSEC-2024-0370`
ignore entry (added by Task 1) already pointed at "docs/stability.md's
non-contracts section", and Task 12b's own brief instructs adding a paragraph
here "under Known limitations of v0.1" — so this file is created now, with
only the sections needed to hold what Task 12b itself is required to record.
A fuller stability document — including the "non-contracts" section
`deny.toml` refers to — is drift from an earlier task and is out of this
task's scope; see this task's report for that note.

## Known limitations of v0.1

- **S3 credentials come from `object_store`'s own chain, not the AWS SDK's.**
  `logweir-engine-oso`'s `Store::from_url`/`Store::read_only_from_url` build
  the S3 client with `object_store` 0.14's `AmazonS3Builder::from_env()`,
  which applies `object_store`'s OWN credential chain (static keys, then web
  identity / IRSA, ECS, EKS Pod Identity, IMDS). That is **not** the AWS SDK
  chain: `~/.aws/credentials` profiles, `AWS_PROFILE` and SSO are
  unsupported. State it plainly here because an adopter discovering it at
  drill time is a support ticket.

- **MSRV is 1.89, raised from 1.82 by object_store 0.14's aws/azure/gcp/http
  feature set (Global Constraint 9).** Rust edition 2024, required by that
  dependency tree's `digest`/`crypto-common`/`block-buffer` (RustCrypto)
  crates, is only supported starting rustc 1.85; once that resolved, `cargo`
  additionally reported an MSRV floor of 1.89 from `crc-fast`, the `icu_*`
  crates and `idna_adapter`, all pulled in transitively through
  `reqwest -> hyper -> ... -> object_store`. Global Constraint 9 fixes the
  `object_store` crate, version and feature set, so the toolchain pin moved
  instead — `rust-toolchain.toml`, `[workspace.package].rust-version` in the
  root `Cargo.toml`, and the `dtolnay/rust-toolchain` step in
  `.github/workflows/ci.yml` all now read `1.89.0`/`1.89`.
