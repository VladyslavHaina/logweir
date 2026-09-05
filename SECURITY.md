# Security policy

## Supported versions

Only the latest `v0.1.x` release receives security fixes. Logweir is pre-1.0 and
has no long-term support branch.

## Reporting a vulnerability

Report privately through this repository's GitHub Security Advisories
("Report a vulnerability"). Do not open a public issue. We acknowledge within
5 working days and aim to ship a fix or a documented mitigation within 30 days.

## Scope

In scope — the `logweir` binary and the `logweir-*` crates in this repository,
and in particular anything that lets: a drill scorecard be forged or its
signature cover less than a reader would assume; a lock proof be bypassed; a
guard refusal (exit 3) be evaded; one of the three forbidden keys
(`purge_topics`, `dry_run`, `header_preflight_external`) reach an argv or a
rendered document; or Logweir write outside its own `logweir/` prefix or to a
source cluster on any path.

Out of scope — vulnerabilities in `osodevops/kafka-backup`, which Logweir shells
out to by pinned digest and never links. Report those upstream.

## Cryptography

Scorecards and approvals are signed with DSSE over PAE. A finding that the
signature does not cover what a reader would assume it covers is in scope and is
treated as high severity.

## Key material in this repository

`e2e/fixtures/signed/signing.pem` and `public.pem` are **throwaway test
fixtures**, checked in deliberately so the verifier paths have something real to
exercise. They sign nothing outside this repository's own test suite, are
regenerated on demand by `just fixtures-sign`, and must never be used to sign a
real drill scorecard. `.gitignore` excludes `*.pem` everywhere else, and no
private key material appears in any log line, test name, or error message.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
