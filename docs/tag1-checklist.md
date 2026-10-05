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
| 3 | Runner, controller, console and UI candidates pass image checks, and the version tag names the same digests as the `sha-<commit>` publication it promotes | `open` | `.github/workflows/images.yml`, `scripts/ci-images.sh`, `scripts/check-image-api.sh`, `scripts/release.sh` |
| 4 | The version tag identifies the intended commit, the dry run and the release workflow succeed | `open` | `.github/workflows/release.yml` |
| 5 | License notices, dependency inventory and UI assets are included in the applicable artifacts | `open` | `scripts/check-image.sh`, `scripts/check-image-weirkeeper.sh`, `scripts/check-image-ui.sh`, `scripts/release-build.sh`, `THIRD_PARTY_NOTICES.md` |
| 6 | Downloadable artifacts contain the independent verifier, the packaged Linux CLI completes the release drill, and every archive performs `drill countersign` on its own platform | `open` | `.github/workflows/release-drill.yml`, `.github/workflows/release.yml`, `docs/verify_scorecard.py`, `scripts/release-countersign-check.py` |
| 7 | Both signed document schemas are current and the independent readers agree on accepted and rejected fixtures | `open` | `scripts/check-verifier-parity.sh`, `crates/logweir/tests/two_reader_parity.rs`, `crates/logweir/tests/two_reader_parity_receipt.rs` |
| 8 | Install instructions identify the published references, prerequisites and supported architectures | `open` | `docs/install.md`, `charts/logweir/README.md` |
| 9 | Release notes describe limitations and any unverified claims have an explanation | `open` | `docs/release-notes.md`, `scripts/check-unverified-labels.sh`, `docs/stability.md` |
| 10 | Upgrade and recovery notes cover changed credentials, CRDs, storage or evidence formats | `open` | `docs/release-notes.md`, `docs/kubernetes.md`, `docs/keys.md`, `docs/architecture.md` |
| 11 | The published release's assets verify after download, and the chart installs from the registry with its pinned digests | `open` | `scripts/release.sh`, `deploy/poc/README.md` |

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

## Cutting a release candidate

The owner approves every tag push; a tag, its images and its GitHub Release
are publications. Everything before step 4 writes nothing outside workflow
artifacts.

1. **The commit.** `main` CI is green for it, including the `publish` job, so
   its `sha-<commit>` images and chart exist; or it is a commit that differs
   from such a commit only under `docs/` (a tracker or release-notes edit).
   The tag's `tests` job is the full `ci.yml`, so no test may be failing on
   it.
2. **The dry run.** Dispatch the workflow on that commit's branch; it runs
   every job up to `assemble` and publishes nothing
   ([gates.md](gates.md#versioned-releases)):

   ```bash
   gh workflow run release.yml --ref main -f rehearsal_tag=v0.2.0-rc.1
   # On a branch with no publication of its own, name the main commit whose
   # images it rehearses against:
   gh workflow run release.yml --ref <branch> -f rehearsal_tag=v0.2.0-rc.1 -f publication=<40-hex main commit>
   gh run download <run-id> -n release-assets -D rc-dry-run
   bash scripts/release.sh verify rc-dry-run/release/assets
   ```

3. **The record.** The dry run's `release.json` names the publication commit,
   the four image digests and the chart's package digest the tag will
   publish; fill the [candidate record](release-notes.md) from it before the
   tag, and its run rows after step 4.
4. **The tag**, with the owner's approval, then watch the run:

   ```bash
   git tag -a v0.2.0-rc.1 <commit> -m "Logweir v0.2.0-rc.1"
   git push origin v0.2.0-rc.1
   gh run list --workflow release.yml --event push --limit 1
   ```

   A failed `publish-images` or `github-release` job can be re-run: a version
   tag that already names the release's digests, a chart version already
   published as the same bytes and an existing GitHub Release are verified,
   not written again; anything else is refused.
5. **Verify as a user would**, from another machine:

   ```bash
   gh release download v0.2.0-rc.1 -R VladyslavHaina/logweir -D rc
   (cd rc && sha256sum -c SHA256SUMS)        # macOS: shasum -a 256 -c SHA256SUMS
   helm pull oci://registry-1.docker.io/vladyslavhaina/logweir-chart --version 0.2.0-rc.1 -d pulled
   cmp pulled/logweir-chart-0.2.0-rc.1.tgz rc/logweir-chart-0.2.0-rc.1.tgz
   for image in weirkeeper logweir logweir-console logweir-ui; do
     docker buildx imagetools inspect "docker.io/vladyslavhaina/$image:v0.2.0-rc.1" --format '{{json .Manifest}}' | jq -r .digest
   done                                      # each equals release.json's .images.refs[<image>].digest
   ```

6. **The countersigning step.** A Governed approval needs an approver's
   `logweir drill countersign` on their own machine ([kubernetes.md](kubernetes.md),
   §8 *Approval policy*). The approver takes the archive for that machine from
   the release, checks it against `SHA256SUMS`, and countersigns with their
   own key; the release run already had every archive countersign a throwaway
   request and checked the result independently, and no release step holds an
   approver's key.
7. **The install.** Upgrade a running installation to the release's chart
   ([deploy/poc](../deploy/poc/README.md), *Upgrade to a newer publication*,
   with `LOGWEIR_CHART_VERSION` set to the release's version) and record it in
   row 11.

## What a GitHub Release carries

| Asset | What it is | How it is verified |
|---|---|---|
| `logweir-<target>.tar.xz` (three) | the CLI for Linux x86-64, Linux arm64 and macOS arm64, with `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md` and `README.md` | its `.sha256` and `SHA256SUMS`; at build, [scripts/release-build.sh](../scripts/release-build.sh) checks its exact contents and notices, that no engine is inside, that it starts and countersigns on its own platform, and measures its run-time needs; the Linux x86-64 archive drives the release drill |
| `logweir-<target>.tar.xz.sha256` | `dist`'s checksum of each archive | `SHA256SUMS` |
| `verify_scorecard.py` | the independent verifier, `docs/verify_scorecard.py` at the tag | `SHA256SUMS`; the release drill runs it on the drill's signed scorecard |
| `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md` | Logweir's licence, its notices and the generated dependency inventory | `SHA256SUMS`; identical to the tagged commit's |
| `kafka-backup-LICENSE` | the engine's MIT licence (`third_party/LICENSE-MIT`), owed because the runner image redistributes the engine | `SHA256SUMS` |
| `logweir-chart-<version>.tgz` | the chart package, pushed as these bytes to `oci://registry-1.docker.io/vladyslavhaina/logweir-chart --version <version>` | `SHA256SUMS`; its four images are pinned by digest (`assemble`); the registry serves the same bytes back (`ci-images.sh chart-push`) |
| `release.json` | the tag, commit, publication commit, the four image references with digests and platforms, the chart, and each archive's run-time needs | `SHA256SUMS` |
| `ui-files.sha256` | the page files the console and UI images ship, by digest | `SHA256SUMS`; the candidate record's `ui/` row |
| `SHA256SUMS` | the sha256 of every asset above | `scripts/release.sh verify`, on the assembled set and on the published release downloaded back |

The release's notes state where the images come from, the measured run-time
needs of each archive, and how to verify all of it.

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
