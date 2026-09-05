# third_party/

Provenance for the pinned `kafka-backup` engine. Nothing here is Logweir code.

| File | What it is |
| --- | --- |
| `kafka-backup-binary.digest` | The immutable `sha256:` image digest the engine binary was extracted from. Never a tag — Docker Hub tags are mutable (Global Constraint 7). |
| `kafka-backup-v0.21.0.tar.gz` | The upstream source at the pinned tag, force-added past `.gitignore`. |
| `kafka-backup-v0.21.0.tar.gz.sha256` | Checksum of the tarball above. |
| `LICENSE-MIT` | The upstream MIT licence, redistributed as required (Global Constraint 15). |

Upstream is MIT with no CLA/DCO, which is uncapped relicensing risk; vendoring the
source and the licence is the hedge. All four files are produced together by
`./scripts/extract-engine.sh` (`just engine`) and must be updated together —
changing the digest without re-vendoring the tarball is a defect.

Logweir itself is Apache-2.0 (see `/LICENSE` and `/NOTICE`).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
