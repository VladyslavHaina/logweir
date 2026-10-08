# third_party/

Provenance for two pinned things Logweir did not write — the `kafka-backup`
engine's source, and the **org root's public key** — and the recipe and patches
Logweir builds the engine with (OD-3, PROD-00.2).

| File | What it is |
| --- | --- |
| `kafka-backup-build.env` | **Logweir's build** of the engine: `ENGINE_VERSION` (`<release>+logweir.<n>`, what `kafka-backup --version` prints) and `ENGINE_DIGEST` (the build-input digest over the tarball, the patches and the version). `scripts/engine-source.sh` builds from it, and the runner image declares it in `/etc/logweir/engine-identity`. |
| `kafka-backup-patches/` | The ordered patches Logweir carries on the vendored source, and the patch-first policy (its `README.md`). |
| `kafka-backup-deny.toml` | `cargo deny`'s policy for the engine's own lockfile, after the patches (`scripts/ci-check.sh`). |
| `kafka-backup-binary.digest` | The immutable `sha256:` digest of OSO's released image of the same tag. Never a tag — Docker Hub tags are mutable (Global Constraint 7). It pins the one-release rollback (`ENGINE_SOURCE=oso`) and the compose stack's seeding engine; what ships is Logweir's build. |
| `kafka-backup-v0.23.3.tar.gz` | The upstream source at the pinned tag (`v0.23.3` = commit `afb160e7f2c69b7c3c28e1b868dd952835a5b0af`), force-added past `.gitignore` and let into the Docker build context by name (`.dockerignore`). Logweir's build compiles it. It replaced `kafka-backup-v0.21.0.tar.gz` with the PROD-00.3f bump; `xtask`'s drift gate requires exactly one tarball here. |
| `kafka-backup-v0.23.3.tar.gz.sha256` | Checksum of the tarball above. |
| `LICENSE-MIT` | The upstream MIT licence, redistributed as required (Global Constraint 15). |
| `org-root.pub.pem` | The org root's **public** key. Un-ignored by name in `/.gitignore`, exactly as `e2e/fixtures/signed/*.pem` is. |
| `org-root.fingerprint` | One `sha256:` line: the SHA-256 of the SubjectPublicKeyInfo DER encoding of the key above. `COPY`ed into **both** images at `/etc/logweir/org-root.fingerprint`. |

Upstream is MIT with no CLA/DCO, which is uncapped relicensing risk; vendoring the
source and the licence is the hedge, and since OD-3 the source is what Logweir
builds. The digest, the tarball, its checksum and the licence are produced
together by `./scripts/extract-engine.sh` (`OSO_REFRESH=1`) and must be updated
together — changing the digest without re-vendoring the tarball is a defect —
and a new tarball is a new build: re-apply the patches, bump the `<n>` of
`ENGINE_VERSION` and re-record `ENGINE_DIGEST` (`scripts/engine-source.sh digest`).
`crates/logweir/tests/engine_pin.rs` and `engine_build.rs` refuse a tree where
they disagree.

## The org-root anchor

`org-root.fingerprint` is the anchor stage-2 Task 16 calls T1. It is baked into
both images at build time so that whoever controls the cluster cannot change it
without producing a **different image** — a ConfigMap or a Secret would be
exactly the projection a compromised control plane owns. The value is
reproducible from the file beside it:

```bash
openssl pkey -pubin -in third_party/org-root.pub.pem -outform DER | openssl dgst -sha256
```

`scripts/check-dod.sh` runs that comparison, `just check-org-root` compares both
images against the checked-in file, and `scripts/check-image-weirkeeper.sh`
check 4 does it for the controller image.

**No private key is here, and the shipped anchor authorises nothing.** The
keypair was generated outside this tree with `docs/keys.md`'s recipe and the
private half was destroyed; `/.gitignore` ignores `*.pem` and `*.key`
repository-wide and un-ignores exactly one public file. **Nothing reads the
baked path yet** — Phase 0 does not open it, and
`crates/logweir/tests/manifest_lint.rs`'s `the_fingerprint_is_not_read_at_runtime`
asserts that. An adopter replaces both files with their own org root's and
rebuilds both images; `docs/kubernetes.md` §14 carries the whole story.

Logweir itself is Apache-2.0 (see `/LICENSE` and `/NOTICE`).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
