# Maintainers

| Name | GitHub | Company | Areas |
|---|---|---|---|
| Vladyslav | the owner of `github.com/logweir/logweir` | independent | everything |

The GitHub column names the repository owner declared in `Cargo.toml`'s
`repository` field rather than a personal handle, because this clone has no
`origin` remote configured to read one from. Replace it with the handle before
the first external contribution lands, so a contributor knows who to ping.

## How changes land

By pull request, with at least one maintainer approval and a DCO sign-off
(`git commit -s`). There is no CLA; inbound = outbound Apache-2.0. See
[CONTRIBUTING.md](CONTRIBUTING.md).

## One maintainer, one employer, and what that forecloses

The table above has one row, and the Company column exists because a foundation
would ask for it. **A single-maintainer, single-employer project does not pass
a CNCF Sandbox review on organisational diversity**, which is considered during
review; that is a 12-18 month question at best and nothing in this repository
pretends otherwise. The column is here now so that the day it has three rows,
the answer is already in the file rather than being reconstructed.

The related decision is in [CONTRIBUTING.md](CONTRIBUTING.md): **no
copyright-assignment CLA is required or accepted**, so once outside
contributors have landed code, no single party — this maintainer included — can
relicense the whole.

## Format changes

Changes to `schemas/logweir-drill-scorecard-1.0.0.json` follow the policy in
[docs/stability.md](docs/stability.md): a minor adds optional fields only, a
major changes an identity rule. A major bump needs two maintainer approvals.

Documentation is licensed [CC-BY-4.0](docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
