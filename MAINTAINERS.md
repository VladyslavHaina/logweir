# Maintainers

| Name | GitHub | Areas |
|---|---|---|
| Vladyslav | the owner of `github.com/logweir/logweir` | everything |

The GitHub column names the repository owner declared in `Cargo.toml`'s
`repository` field rather than a personal handle, because this clone has no
`origin` remote configured to read one from. Replace it with the handle before
the first external contribution lands, so a contributor knows who to ping.

## How changes land

By pull request, with at least one maintainer approval and a DCO sign-off
(`git commit -s`). There is no CLA; inbound = outbound Apache-2.0. See
[CONTRIBUTING.md](CONTRIBUTING.md).

## Format changes

Changes to `schemas/logweir-drill-scorecard-1.0.0.json` follow the policy in
[docs/stability.md](docs/stability.md): a minor adds optional fields only, a
major changes an identity rule. A major bump needs two maintainer approvals.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
