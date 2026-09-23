# Release checklist

Use this checklist for each release candidate. Its historical filename is
retained for existing links; it no longer records the status of the first tag.
Record the candidate commit, version, successful workflow URL and image digests
in the [release notes](release-notes.md)' candidate record. Previous successful
releases do not validate new bytes.

The rows below start `open`. Mark a row `closed` only with evidence for the
candidate, or `blocked: <reason>` when an external prerequisite prevents it.
A script's existence describes the check, not a successful execution. The
workflow summaries and downloaded artifacts provide execution evidence.

| # | Check | Status | Check or evidence source |
|---|---|---|---|
| 1 | Repository checks and Compose backup/restore tests pass for the exact commit | `open` | `.github/workflows/ci.yml`, `scripts/ci-check.sh` |
| 2 | Required Kubernetes changes have been exercised through installation, scheduling, restore and verification | `open` | `.github/workflows/helm-demo.yml`, `scripts/test-k8s-scram.py`, `docs/kubernetes.md` |
| 3 | Runner, controller, console and UI candidates pass image checks and their registry digests match the published set | `open` | `.github/workflows/images.yml`, `scripts/ci-images.sh`, `scripts/check-image-api.sh` |
| 4 | The version tag identifies the intended commit and the release workflow succeeds | `open` | `.github/workflows/release.yml` |
| 5 | License notices, dependency inventory and UI assets are included in the applicable artifacts | `open` | `scripts/check-image.sh`, `scripts/check-image-weirkeeper.sh`, `scripts/check-image-ui.sh`, `THIRD_PARTY_NOTICES.md` |
| 6 | Downloadable artifacts contain the independent verifier and the packaged Linux CLI completes the release drill | `open` | `.github/workflows/release-drill.yml`, `.github/workflows/release.yml`, `docs/verify_scorecard.py` |
| 7 | Both signed document schemas are current and the independent readers agree on accepted and rejected fixtures | `open` | `scripts/check-verifier-parity.sh`, `crates/logweir/tests/two_reader_parity.rs`, `crates/logweir/tests/two_reader_parity_receipt.rs` |
| 8 | Install instructions identify the published references, prerequisites and supported architectures | `open` | `docs/install.md`, `charts/logweir/README.md` |
| 9 | Release notes describe limitations and any unverified claims have an explanation | `open` | `docs/release-notes.md`, `scripts/check-unverified-labels.sh`, `docs/stability.md` |
| 10 | Upgrade and recovery notes cover changed credentials, CRDs, storage or evidence formats | `open` | `docs/release-notes.md`, `docs/kubernetes.md`, `docs/keys.md`, `docs/architecture.md` |

## What to record

- The candidate commit and version, with the successful CI and release run URLs.
- Every image digest — runner, controller, console and UI — and the supported
  architectures. A locally loaded image
  proves local execution, not that another machine can pull it.
- Results from a fresh artifact download and the packaged-binary drill.
- Any Kubernetes test performed, including its context, authentication mode,
  storage and limitations. Local SCRAM testing does not prove company EKS IAM
  or private-CA configuration.
- Required migration or recovery actions, and any checks intentionally deferred
  with a reason. Do not describe a deferred check as successful.
- The shipped task IDs, commit and image identities, tested environments,
  results, limitations and rollback in [release-handoff.md](release-handoff.md).

Every bullet but the last fills the candidate record at the top of the
release's entry in [release-notes.md](release-notes.md).

Main image publication is automatic after CI and is separate from a versioned
release. A push to `main` need not create a tag or GitHub Release. See
[gates.md](gates.md) for workflow ownership and publication order.

Project naming and announcement requirements remain in
[TRADEMARKS.md](../TRADEMARKS.md); they are not a claim established by CI.
Historical local transcripts remain useful in [kubernetes.md](kubernetes.md)
and [e2e/k8s/laptop-demo.md](../e2e/k8s/laptop-demo.md), but cannot substitute
for evidence about the current release candidate.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
