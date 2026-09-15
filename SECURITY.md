# Security policy

## Supported versions

Only the latest `v0.1.x` release receives security fixes. Logweir is pre-1.0 and
has no long-term support branch.

## Reporting a vulnerability

Report privately through this repository's GitHub Security Advisories
("Report a vulnerability"). Do not open a public issue. We acknowledge within
5 working days and aim to ship a fix or a documented mitigation within 30 days.

## Scope

In scope — the `logweir` binary, the `weirkeeper` controller, the static `ui/`
bundle and the `logweir-*` crates in this repository, and in particular
anything that lets: a drill scorecard or a backup receipt be forged or its
signature cover less than a reader would assume; an approval be accepted that
one of the approval checks should have refused; a lock proof be bypassed; a
guard refusal (exit 3) be evaded; one of the three forbidden keys
(`purge_topics`, `dry_run`, `header_preflight_external`) reach an argv or a
rendered document; a key, a bearer token or any credential reach the UI page;
or Logweir write outside its own `logweir/` prefix, delete an archive object,
or write to a source cluster on any path.

Out of scope — vulnerabilities in `osodevops/kafka-backup`, which Logweir shells
out to by pinned digest and never links. Report those upstream.

## What this design does not protect against

These four are **accepted residuals**, not undiscovered bugs. They are stated
here, in `README.md` and in `docs/kubernetes.md` so that nobody has to discover
them; a report that one of them is true is not a vulnerability report.

- **`weirkeeper` is a signing oracle wherever it holds Job CRUD over the
  namespace that holds `logweir-signing-key`.** The controller has no `get` on
  Secrets anywhere and never reads the key — but it creates Jobs, and a Job it
  creates can mount the Secret and sign whatever it likes with no Logweir crate
  involved. Job create in that namespace is equivalent to holding the key. The
  hardened layout is to put `logweir-signing-key` in a namespace where
  `weirkeeper` has no Job CRUD, which removes the oracle.
- **A cluster-admin defeats every control described here.** They can mount the
  signing Secret and sign, edit the `TrustRoster`, or delete an admission
  policy. Nothing in this design constrains that subject, and nothing claims
  to.
- **RBAC bounds the viewer, not the page.** The UI is served by
  `kubectl proxy` under the viewer's own kubeconfig, so the four shipped
  ClusterRoles bind the *user* and bind nothing at all about the page. A
  cluster-admin kubeconfig gives the page cluster-admin. The optional Helm UI
  proxy uses its ServiceAccount instead; everyone able to reach that proxy
  receives that account's API authority.
- **`self_attested: false` means only "two different keys".** One person
  holding both keypairs satisfies it. It is not evidence of an independent
  auditor, and a scorecard that carries it should not be read as one.

## Cryptography

Scorecards and approvals are signed with DSSE over PAE. A finding that the
signature does not cover what a reader would assume it covers is in scope and is
treated as high severity.

## Key material in this repository

`e2e/fixtures/signed/signing.pem` and `public.pem`, and
`ui/tests/fixtures/approver.pem` and `approver.pub.pem`, are **throwaway test
fixtures**. They exercise signature verification and CLI/UI approval parity;
never use them for real backups, drills or approvals. The fixture READMEs
explain their regeneration and intended scope.

`.gitignore` excludes other private key files. The separately tracked
`third_party/org-root.pub.pem` is a public anchor, not a private signing key.
No private key material belongs in logs, test names or error messages.

Documentation is licensed [CC-BY-4.0](docs/LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
