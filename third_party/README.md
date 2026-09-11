# third_party/

Provenance for two pinned things Logweir did not write: the `kafka-backup`
engine, and the **org root's public key**. Nothing here is Logweir code.

| File | What it is |
| --- | --- |
| `kafka-backup-binary.digest` | The immutable `sha256:` image digest the engine binary was extracted from. Never a tag — Docker Hub tags are mutable (Global Constraint 7). |
| `kafka-backup-v0.21.0.tar.gz` | The upstream source at the pinned tag, force-added past `.gitignore`. |
| `kafka-backup-v0.21.0.tar.gz.sha256` | Checksum of the tarball above. |
| `LICENSE-MIT` | The upstream MIT licence, redistributed as required (Global Constraint 15). |
| `org-root.pub.pem` | The org root's **public** key. Un-ignored by name in `/.gitignore`, exactly as `e2e/fixtures/signed/*.pem` is. |
| `org-root.fingerprint` | One `sha256:` line: the SHA-256 of the SubjectPublicKeyInfo DER encoding of the key above. `COPY`ed into **both** images at `/etc/logweir/org-root.fingerprint`. |

Upstream is MIT with no CLA/DCO, which is uncapped relicensing risk; vendoring the
source and the licence is the hedge. The first four files are produced together by
`./scripts/extract-engine.sh` (`just engine`) and must be updated together —
changing the digest without re-vendoring the tarball is a defect.

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
