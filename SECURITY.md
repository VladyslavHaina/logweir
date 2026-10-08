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

Also in scope since PROD-00.2 (OD-3) — **the `kafka-backup` engine the images
ship**. Logweir builds it from the vendored OSO source with its own patch folder
(`third_party/kafka-backup-patches/`), so a vulnerability in that build is
Logweir's to fix: in the engine's code, in its dependency graph (its own
`Cargo.lock`, which `scripts/ci-check.sh` checks with `cargo deny` under
`third_party/kafka-backup-deny.toml`), or in a patch Logweir carries. Report it
here. A fix ships as a patch as soon as its oracle passes; reporting it to OSO
as well is welcome, and is not a condition of the fix. The engine is still a
separate process Logweir never links, and the one-release rollback image
(`ENGINE_SOURCE=oso`, OSO's own binary) carries OSO's code as OSO released it:
report a defect that only it has upstream.

## What this design does not protect against

These are **accepted residuals**, not undiscovered bugs. They are stated
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
- **In the console approval modes, the console can approve alone** (PROD-16.1,
  OD-8). Under `confirm` (internal `Ordinary`) — the default of a fresh
  install with a console — whoever controls the console pod, its
  `logweir-console-confirmation` key Secret, or the identity provider can
  confirm a restore with no second person, exactly as an `Ordinary` binding
  always accepted. Namespaces that cannot accept that are bound `strict`
  (a personal-key approval), or the installation sets
  `approvalPolicy.default: strict`.
- **The in-cluster administrator console confirms as one shared identity.**
  In `localAdmin` mode the confirming principal is
  `urn:logweir:local-admin#admin`: whoever can reach that console — the
  Kubernetes permission to port-forward to it, already full console
  administrator authority — can confirm a restore alone.
- **A fresh install's trust step holds `create` on `TrustPolicy` for its
  install alone.** RBAC cannot narrow `create` by name, so the identity hook's
  grant (`<release>-identity-trust`) is rendered only on a first install with
  no existing identity and the managed console key, only on Helm 3.19+ or 4.x
  (older Helm keeps it bound when the hook fails, so the chart refuses to
  render it there), only as a `post-install` hook that Helm deletes when the
  install's hooks finish, succeeded or failed; and the hook deletes its own
  binding on every exit path it controls (after the trust step, a failed step,
  a usage error). For that window, whoever can run a pod as
  `<release>-identity-bootstrap` in the release namespace could create a
  TrustPolicy; the installer holds cluster-admin then anyway. A hook that never
  runs its code (an image that cannot be pulled, a pod never admitted) or a
  Helm client killed mid-install leaves the binding until the next hook run:
  after a failed first install, delete it
  (`kubectl delete clusterrolebinding,clusterrole <release>-identity-trust`,
  `docs/install.md` §5f; `docs/kubernetes.md` §8).
- **The controller's ServiceAccount can append a key to any existing
  `TrustPolicy`** (pre-existing, not new in PROD-16.1). Its cluster-wide
  `patch` on `trustpolicies` serves the compromise finalizer and no admission
  policy narrows it to `metadata.finalizers`, so whoever can run a pod as
  `weirkeeper` in the release namespace holds a trust administrator's power
  wherever a policy exists — on a fresh install with a console, from the first
  minute (`logweir-installation`). Owed: an admission fence on that account.
- **On a fresh install, the trust administrator and the installation
  administrator can each change approval, as before.** The fresh-install
  `confirm` default is honoured only beside the `TrustPolicy` the identity
  hook created in the same run (a bound marker: policy UID, both key ids,
  provenance); editing that policy, or the approval-policy document, changes
  approval. On an install that existed before PROD-16.1 a marker patched into
  the identity ConfigMap changes nothing.
- **A connection's credential binding is enforced by the runner, after the
  kubelet has projected the credential.** A `KafkaCluster` that names another
  connection's credential Secret cannot make Logweir present it anywhere: every
  runner compares the Secret's `logweir-binding` with the binding of the
  connection it was built for and refuses a mismatch before any client exists
  (`CredentialBindingMismatch`, [kubernetes.md](docs/kubernetes.md) §20.9).
  But the controller reads no Secret, so it is the kubelet that resolves the
  reference, and the foreign value sits in the refused pod's environment (or,
  for a client key, its projected volume) for the moment before the runner
  exits — inside a pod that mounts no ServiceAccount token and sends nothing.
  Anyone who can already read pods' environment or exec into a runner pod in
  that namespace can read any credential a run projects, bound or not. And
  anyone who can WRITE a Secret can bind a credential they put there to their
  own connection — that is their own credential, which the binding exists to
  allow. For the same reason Secret `patch` WITHOUT `get` is, for a connection
  credential, as strong as `get`: it can set a victim Secret's
  `logweir-binding` to a connection whose endpoint the patcher chose (the
  binding value is public on `status.credentialBinding`).
- **The same binding guards every other credential reference (FX-20), with the
  same residuals and three more.** A `BackupDestination` grant, a
  `RetentionPolicy` delete key, a `ProtectionPolicy` route and an inline
  archive `secretRef` are each refused (`CredentialBindingMismatch`) unless the
  Secret carries the binding of that object, route or archive location
  ([kubernetes.md](docs/kubernetes.md) §20.10); the kubelet still projects the
  value into the refused pod, and Secret `patch` is still as strong as `get`.
  Not bound: a **workload-identity grant** names a ServiceAccount, so a
  destination author who may name another team's IRSA-annotated
  ServiceAccount beside an endpoint they control gets requests signed with that
  role's temporary credentials (the session token and access key id travel);
  a destination's **CA reference** is mutable, so a principal who can both edit
  it and intercept traffic to the (immutable) endpoint can read signed
  requests; and an inline archive is bound to its **location** — every field
  that shapes the URL the runner dials (scheme, bucket, endpoint, region,
  addressing, `allowHttp`; never the prefix) — so any object in the namespace
  may use that Secret at that location, what a `BackupDestination` in the
  namespace already allows. A region that is not a region name is refused
  outright (`StorageRegionInvalid`): without an endpoint the region is part of
  the host, and before FX-20's fix round a plan keeping the victim's bucket
  could spell one that sent the requests elsewhere. A standing rehearsal
  authorization's scope does not name the storage a `Restore` reads
  (PLAT-14.3b), so under one the location binding and the region rule are
  what keep a Secret at its location.

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
