#!/usr/bin/env bash
# Build jobs smoke-test native images before uploading candidates. Only the
# promotion job moves public tags, after every platform has passed.
set -euo pipefail
cd "$(dirname "$0")/.."

: "${GITHUB_SHA:?}" "${NS:?}"
[[ "$GITHUB_SHA" =~ ^[0-9a-f]{40}$ && "$NS" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || exit 1
# All four images on both architectures. The runner joined arm64 with PROD-00.2:
# its engine is Logweir's build of the vendored source, compiled for each
# platform, where OSO's own binary existed for linux/amd64 only.
products=(weirkeeper logweir-ui logweir-console logweir)

check_image() {
  local product="$1" ref="$2"
  case "$product" in
    logweir) bash scripts/check-image.sh "$ref" ;;
    weirkeeper) bash scripts/check-image-weirkeeper.sh "$ref" ;;
    logweir-ui) bash scripts/check-image-ui.sh "$ref" ;;
    logweir-console) bash scripts/check-image-api.sh "$ref" ;;
  esac
  local revision
  revision=$(docker image inspect "$ref" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
  [[ "$revision" == "$GITHUB_SHA" ]] || { echo "Wrong revision in $ref" >&2; exit 1; }
}

# ---------------------------------------------------------------------------
# THE CHART, PUBLISHED BESIDE THE IMAGES IT NAMES (chart gap G4)
# ---------------------------------------------------------------------------
# `chart-package <dir>` writes `logweir-chart-<version>.tgz` into <dir>;
# `chart` packages, pushes it to oci://registry-1.docker.io/$NS and verifies
# the registry serves back the same bytes. Both take TAG — the image tag the
# promote step just published — and version the chart WITH it:
#
#   TAG sha-<commit> (a main push)  version <Chart.yaml version>-sha-<commit>
#   TAG v<semver>    (a release)    version <semver>
#
# and in both cases appVersion = TAG and the four Logweir image defaults are
# rewritten from `:latest` to `docker.io/$NS/<image>:$TAG`, so
# `helm install oci://registry-1.docker.io/$NS/logweir-chart --version <v>`
# installs exactly the images this run published — no `--set` needed. The
# chart is renamed `logweir-chart` in the package only: Docker Hub names a
# chart's repository after the chart, and `$NS/logweir` is the runner image.
# The bootstrap image stays the chart's reviewed digest pin (identity.*).
#
# A RELEASE CHART PINS ITS IMAGES BY DIGEST (PROD-14.0). With IMAGE_DIGESTS
# naming a JSON object {"weirkeeper": "sha256:…", "logweir": "sha256:…",
# "logweir-console": "sha256:…", "logweir-ui": "sha256:…"}, each rewritten
# default reads `docker.io/$NS/<image>:$TAG@sha256:…`: the tag says which
# release, the digest decides which bytes, and moving the tag later changes
# nothing an installed release pulls. A `v<semver>` TAG without IMAGE_DIGESTS is
# REFUSED, so no release chart can install whatever a tag points at later.
# `release.yml` passes the digests of the `sha-<commit>` publication it
# promotes (`scripts/release.sh resolve`); a `sha-` chart keeps its tag.
CHART_SRC=charts/logweir
CHART_NAME=logweir-chart
CHART_REPOSITORY="oci://registry-1.docker.io/$NS"

chart_version() {
  local base
  base=$(awk '/^version:/ { print $2; exit }' "$CHART_SRC/Chart.yaml")
  [[ "$base" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Chart.yaml version '$base' is not X.Y.Z" >&2; return 1; }
  if [[ "$TAG" == "sha-$GITHUB_SHA" ]]; then
    echo "$base-sha-$GITHUB_SHA"
  elif [[ "$TAG" =~ ^v([0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?)$ ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "TAG '$TAG' is neither sha-\$GITHUB_SHA nor a v<semver> release tag" >&2
    return 1
  fi
}

chart_package() {
  local out="$1" version work line from to digest rewritten=0
  version=$(chart_version) || return 1
  if [[ "$TAG" == v* && -z "${IMAGE_DIGESTS:-}" ]]; then
    echo "a release chart ($TAG) pins its four images by digest: set IMAGE_DIGESTS" >&2
    return 1
  fi
  work=$(mktemp -d "${TMPDIR:-/tmp}/logweir-chart.XXXXXX")
  cp -R "$CHART_SRC" "$work/$CHART_NAME"
  # Chart.yaml: the package's name. Version and appVersion are set by
  # `helm package` below, the one place both are decided.
  awk -v name="$CHART_NAME" '/^name: / && !done { print "name: " name; done = 1; next } { print }' \
    "$CHART_SRC/Chart.yaml" > "$work/$CHART_NAME/Chart.yaml"
  : > "$work/values.yaml"
  while IFS= read -r line || [[ -n "$line" ]]; do
    for image in weirkeeper logweir logweir-console logweir-ui; do
      from="docker.io/vladyslavhaina/$image:latest"
      if [[ "$line" == *"$from"* ]]; then
        to="docker.io/$NS/$image:$TAG"
        if [[ -n "${IMAGE_DIGESTS:-}" ]]; then
          digest=$(jq -er --arg image "$image" \
            '.[$image] // empty | select(test("^sha256:[0-9a-f]{64}$"))' "$IMAGE_DIGESTS") \
            || { echo "IMAGE_DIGESTS ($IMAGE_DIGESTS) has no sha256 digest for $image" >&2; rm -rf "$work"; return 1; }
          to="$to@$digest"
        fi
        # Prefix + replacement + suffix: literal on every bash, with no
        # pattern or escape rules in the replacement.
        line="${line%%"$from"*}$to${line#*"$from"}"
        rewritten=$((rewritten + 1))
      fi
    done
    printf '%s\n' "$line" >> "$work/values.yaml"
  done < "$CHART_SRC/values.yaml"
  # FOUR, EXACTLY: controllerImage, runnerImage, api.console.image, ui.image.
  # A fifth would be an image this chart does not own; three would be one left
  # at `:latest`, which the published chart must never install.
  [[ "$rewritten" -eq 4 ]] || { echo "rewrote $rewritten image defaults, expected 4" >&2; rm -rf "$work"; return 1; }
  if grep -q ':latest' "$work/values.yaml"; then
    echo "the packaged values.yaml still names a :latest image" >&2; rm -rf "$work"; return 1
  fi
  mv "$work/values.yaml" "$work/$CHART_NAME/values.yaml"
  helm lint "$work/$CHART_NAME" >/dev/null || { rm -rf "$work"; return 1; }
  helm template logweir "$work/$CHART_NAME" -n logweir-system >/dev/null || { rm -rf "$work"; return 1; }
  mkdir -p "$out"
  helm package "$work/$CHART_NAME" --version "$version" --app-version "$TAG" -d "$out" >/dev/null \
    || { rm -rf "$work"; return 1; }
  rm -rf "$work"
  [[ -f "$out/$CHART_NAME-$version.tgz" ]] || return 1
  echo "$out/$CHART_NAME-$version.tgz"
}

# Publish ONE chart package beside the images it names, and prove an anonymous
# reader gets exactly its bytes. $1 the package, $2 its version, $3 `replace`
# (a main publication: a re-run re-pushes its own version and is compared
# again) or `immutable` (a release: a version already published must already be
# these bytes, and is otherwise REFUSED rather than replaced).
#
# "NOT PUBLISHED" IS ONLY WHAT THE REGISTRY SAYS (PROD-14.0 review M-1). An
# immutable push goes ahead only when the anonymous existence read ends in the
# answer Helm prints for a version the registry does not have -- measured on
# 2026-10-05 with Helm v4.0.1, the version release.yml installs, against
# Docker Hub:
#   Error: failed to perform "FetchReference" on source: registry-1.docker.io/<ns>/logweir-chart:<version>: not found
# Any other failure -- a rate limit, a timeout, a refused connection, a
# private repository -- leaves the question open, and an open question is
# refused before any credential is used: pushing then could replace a
# published release version with other bytes.
chart_publish() {
  local package="$1" version="$2" mode="$3" dir anonymous_docker product pushed served existing pulled_log
  local push=true
  # The images this chart names must already be PUBLIC under TAG: the chart
  # is published AFTER the promote step, never before, so no published
  # chart can point at a tag that does not exist. Asked ANONYMOUSLY -- an
  # empty Docker config, not the credentials the login step left behind --
  # so "exists for the publisher" cannot pass for "pullable by anyone".
  anonymous_docker="$(mktemp -d)"
  for product in logweir weirkeeper logweir-ui logweir-console; do
    DOCKER_CONFIG="$anonymous_docker" docker buildx imagetools inspect "docker.io/$NS/$product:$TAG" >/dev/null
  done
  dir=$(mktemp -d)
  mkdir -p "$dir/login" "$dir/pulled" "$dir/anonymous" "$dir/existing"
  pushed=$(sha256sum "$package" | awk '{print $1}')
  if [[ "$mode" == immutable ]]; then
    if DOCKER_CONFIG="$anonymous_docker" HELM_REGISTRY_CONFIG="$dir/anonymous/config.json" \
      helm pull "$CHART_REPOSITORY/$CHART_NAME" --version "$version" -d "$dir/existing" > "$dir/existing.log" 2>&1; then
      existing=$(sha256sum "$dir/existing/$CHART_NAME-$version.tgz" | awk '{print $1}')
      if [[ "$existing" != "$pushed" ]]; then
        echo "$CHART_NAME $version is already published as other bytes ($existing, not $pushed);" \
          "a release version is never replaced" >&2
        return 1
      fi
      echo "$CHART_NAME $version is already published as exactly these bytes; not pushed again" >&2
      push=false
    elif grep -qxF "Error: failed to perform \"FetchReference\" on source: ${CHART_REPOSITORY#oci://}/$CHART_NAME:$version: not found" \
      "$dir/existing.log"; then
      echo "$CHART_NAME $version is not published yet (the registry says not found); pushing it" >&2
    else
      cat "$dir/existing.log" >&2
      echo "could not tell whether $CHART_NAME $version exists; nothing pushed" >&2
      rm -rf "$anonymous_docker"
      return 1
    fi
  fi
  if [[ "$push" == true ]]; then
    # The SAME credentials the image steps use, handed to Helm's own registry
    # client on stdin; nothing is written to the job log. The login is scoped
    # to its OWN registry configuration file, used for the push alone.
    printf '%s' "$DOCKERHUB_TOKEN" | HELM_REGISTRY_CONFIG="$dir/login/config.json" \
      helm registry login registry-1.docker.io --username "$DOCKERHUB_USERNAME" --password-stdin
    HELM_REGISTRY_CONFIG="$dir/login/config.json" helm push "$package" "$CHART_REPOSITORY"
  fi
  # Verify by content, not by exit code, and TRULY ANONYMOUSLY: Helm falls
  # back to Docker's stored credentials (the ones docker/login-action left)
  # when its own registry configuration has none, so the pull-back runs with
  # BOTH an empty Docker configuration directory and a Helm registry
  # configuration that does not exist, then compares the bytes with what was
  # pushed. A chart repository Docker Hub created private on its first push
  # fails here, loudly: make `$NS/logweir-chart` Public and re-run the job
  # (docs/install.md, *(c) The Helm chart*).
  pulled_log="$dir/pull.log"
  DOCKER_CONFIG="$anonymous_docker" HELM_REGISTRY_CONFIG="$dir/anonymous/config.json" \
    helm pull "$CHART_REPOSITORY/$CHART_NAME" --version "$version" -d "$dir/pulled" > "$pulled_log" 2>&1 \
    || { cat "$pulled_log" >&2; rm -rf "$anonymous_docker"; return 1; }
  rm -rf "$anonymous_docker"
  served=$(sha256sum "$dir/pulled/$CHART_NAME-$version.tgz" | awk '{print $1}')
  [[ "$pushed" == "$served" ]] || { echo "the registry serves different chart bytes" >&2; return 1; }
  echo "chart_version=$version" >> "$GITHUB_OUTPUT"
  echo "chart_sha256=$pushed" >> "$GITHUB_OUTPUT"
  # The OCI manifest digest an anonymous `helm pull` reports, when it reports one.
  existing=$(grep -E -o 'sha256:[0-9a-f]{64}' "$pulled_log" || true)
  existing="${existing%%$'\n'*}"
  if [[ -n "$existing" ]]; then
    echo "chart_digest=$existing" >> "$GITHUB_OUTPUT"
  fi
  echo "- $CHART_REPOSITORY/$CHART_NAME --version $version (appVersion $TAG) — package sha256 \`$pushed\`" >> "$GITHUB_STEP_SUMMARY"
}

case "${1:-}" in
  chart-package)
    : "${TAG:?}" "${2:?usage: ci-images.sh chart-package <dir>}"
    chart_package "$2"
    ;;
  chart)
    : "${TAG:?}" "${GITHUB_OUTPUT:?}" "${GITHUB_STEP_SUMMARY:?}"
    : "${DOCKERHUB_USERNAME:?}" "${DOCKERHUB_TOKEN:?}"
    [[ "$TAG" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]] || exit 1
    # A RELEASE TAG'S CHART PINS THE DIGESTS `promote` JUST PUBLISHED under TAG
    # (image-digests/<product>.published, written by the promote arm in this
    # same job), unless the caller named them already.
    if [[ "$TAG" == v* && -z "${IMAGE_DIGESTS:-}" ]]; then
      digests='{}'
      for product in weirkeeper logweir logweir-console logweir-ui; do
        digest=$(cat "image-digests/$product.published")
        digests=$(jq -c --arg product "$product" --arg digest "$digest" '. + {($product): $digest}' <<< "$digests")
      done
      IMAGE_DIGESTS="$(mktemp)"
      printf '%s\n' "$digests" > "$IMAGE_DIGESTS"
      export IMAGE_DIGESTS
    fi
    package=$(chart_package "$(mktemp -d)")
    version=$(chart_version)
    chart_publish "$package" "$version" replace
    ;;
  chart-push)
    # A RELEASE's chart: the package `scripts/release.sh assemble` built once,
    # pinned by digest and listed in the GitHub Release, pushed AS THOSE BYTES.
    # Re-packaging here would not reproduce them: `helm package` writes each
    # file's mtime, and a checkout's mtimes are its checkout time.
    : "${TAG:?}" "${GITHUB_OUTPUT:?}" "${GITHUB_STEP_SUMMARY:?}"
    : "${DOCKERHUB_USERNAME:?}" "${DOCKERHUB_TOKEN:?}"
    package="${2:?usage: ci-images.sh chart-push <logweir-chart-<version>.tgz>}"
    [[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] \
      || { echo "chart-push publishes a release (v<semver>) chart only, not '$TAG'" >&2; exit 1; }
    version=$(chart_version)
    [[ -f "$package" && "$(basename "$package")" == "$CHART_NAME-$version.tgz" ]] \
      || { echo "chart-push: $package is not $CHART_NAME-$version.tgz" >&2; exit 1; }
    chart_publish "$package" "$version" immutable
    ;;
  check|candidates)
    [[ "${ARCH:-}" == amd64 || "${ARCH:-}" == arm64 ]] || exit 1
    for product in "${products[@]}"; do
      if [[ "$1" == check ]]; then
        check_image "$product" "$product:check"
        continue
      fi
      : "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}"
      repo="docker.io/$NS/$product"
      ref="$repo:build-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$ARCH"
      docker tag "$product:check" "$ref"
      docker push "$ref"
      digest=$(docker buildx imagetools inspect "$ref" --format '{{json .Manifest}}' | jq -er .digest)
      [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || exit 1
      # Execute the exact digest returned by the registry on its native host.
      docker pull --platform "linux/$ARCH" "$repo@$digest"
      check_image "$product" "$repo@$digest"
      mkdir -p image-digests
      jq -n --arg product "$product" --arg arch "$ARCH" --arg digest "$digest" \
        --arg sha "$GITHUB_SHA" '{product:$product,arch:$arch,digest:$digest,sha:$sha}' \
        > "image-digests/$product-$ARCH.json"
    done
    ;;
  promote)
    : "${TAG:?}" "${GITHUB_OUTPUT:?}" "${GITHUB_STEP_SUMMARY:?}"
    [[ "$TAG" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]] || exit 1
    if [[ "${PROMOTE_LATEST:-false}" == true ]]; then
      [[ "${GITHUB_REF:-}" == refs/heads/main && "$TAG" == "sha-$GITHUB_SHA" ]] || exit 1
    fi
    products=(logweir weirkeeper logweir-ui logweir-console)
    # Validate the complete candidate set before moving any public tag.
    for product in "${products[@]}"; do
      for arch in amd64 arm64; do
        file="image-digests/$product-$arch.json"
        jq -e --arg sha "$GITHUB_SHA" --arg product "$product" --arg arch "$arch" \
          '.sha == $sha and .product == $product and .arch == $arch and (.digest | test("^sha256:[0-9a-f]{64}$"))' "$file" >/dev/null
        digest=$(jq -r .digest "$file")
        docker buildx imagetools inspect "docker.io/$NS/$product@$digest" >/dev/null
      done
    done
    for product in "${products[@]}"; do
      repo="docker.io/$NS/$product"
      refs=("$repo@$(jq -r .digest "image-digests/$product-amd64.json")"
            "$repo@$(jq -r .digest "image-digests/$product-arm64.json")")
      docker buildx imagetools create --prefer-index=false --tag "$repo:$TAG" "${refs[@]}"
      digest=$(docker buildx imagetools inspect "$repo:$TAG" --format '{{json .Manifest}}' | jq -er .digest)
      [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || exit 1
      printf '%s\n' "$digest" > "image-digests/$product.published"
      case "$product" in
        logweir) output=runner_digest ;;
        weirkeeper) output=controller_digest ;;
        logweir-ui) output=ui_digest ;;
        logweir-console) output=console_digest ;;
      esac
      echo "$output=$digest" >> "$GITHUB_OUTPUT"
      echo "- $repo:$TAG — \`$digest\`" >> "$GITHUB_STEP_SUMMARY"
    done
    if [[ "${PROMOTE_LATEST:-false}" == true ]]; then
      # A slower, older main run must not roll latest back. Promotion jobs are
      # serialized; version releases never move these rolling tags.
      head=$(git ls-remote origin refs/heads/main | awk '{print $1}')
      if [[ "$head" != "$GITHUB_SHA" ]]; then
        echo 'A newer main commit exists; SHA tags published, rolling tags left unchanged.' >> "$GITHUB_STEP_SUMMARY"
        exit 0
      fi
      for product in "${products[@]}"; do
        repo="docker.io/$NS/$product"
        digest=$(cat "image-digests/$product.published")
        docker buildx imagetools create --prefer-index=false --tag "$repo:main" --tag "$repo:latest" "$repo@$digest"
        for rolling in main latest; do
          actual=$(docker buildx imagetools inspect "$repo:$rolling" --format '{{json .Manifest}}' | jq -er .digest)
          [[ "$actual" == "$digest" ]] || { echo "Tag verification failed: $repo:$rolling" >&2; exit 1; }
        done
      done
      echo 'Verified main and latest tags for all four images.' >> "$GITHUB_STEP_SUMMARY"
    fi
    ;;
  sbom)
    # PROD-00.2: the runner candidate's SBOM, from the exact digest the registry
    # returned for this architecture (`candidates` wrote it), never from the
    # local tag. Both binaries are built with `cargo auditable`, so the image
    # scan names the Rust crates inside them; a document that lists neither
    # the engine's crate nor Logweir's is refused rather than published as an
    # SBOM that silently covers only the Debian packages.
    : "${SYFT:?}"
    [[ "${ARCH:-}" == amd64 || "${ARCH:-}" == arm64 ]] || exit 1
    digest=$(jq -er --arg sha "$GITHUB_SHA" 'select(.sha == $sha and .product == "logweir") | .digest' \
      "image-digests/logweir-$ARCH.json")
    [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || exit 1
    out="image-digests/logweir-$ARCH.spdx.json"
    "$SYFT" scan "registry:docker.io/$NS/logweir@$digest" --platform "linux/$ARCH" \
      -o "spdx-json=$out"
    for crate in kafka-backup-core kafka-backup-cli logweir-core rustls; do
      jq -e --arg c "$crate" '[.packages[] | select(.name == $c)] | length > 0' "$out" >/dev/null \
        || { echo "the SBOM of logweir@$digest lists no \`$crate\`: the binaries were not built with cargo auditable" >&2; exit 1; }
    done
    echo "- SBOM of docker.io/$NS/logweir@$digest (linux/$ARCH): $(jq '.packages | length' "$out") packages" \
      >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
    ;;
  sign)
    # PROD-00.2: keyless signatures (cosign, GitHub OIDC -> Fulcio, logged in
    # Rekor) for the four published indexes and every platform manifest in
    # them, and the runner's SBOM attested to each platform's candidate digest.
    # Only from this repository's main, on a push: images.yml is a reusable
    # workflow, and a run another repository starts by calling it must not
    # sign under its identity (the verification pins refuse such a signature
    # anyway; this keeps one from being made).
    : "${RUNNER_DIGEST:?}" "${CONTROLLER_DIGEST:?}" "${UI_DIGEST:?}" "${CONSOLE_DIGEST:?}"
    if [[ "${GITHUB_REPOSITORY:-}" != VladyslavHaina/logweir || "${GITHUB_REF:-}" != refs/heads/main \
          || "${GITHUB_EVENT_NAME:-}" != push ]]; then
      echo "sign: refusing outside VladyslavHaina/logweir main on a push" \
           "(${GITHUB_REPOSITORY:-?} ${GITHUB_REF:-?} ${GITHUB_EVENT_NAME:-?})" >&2
      exit 1
    fi
    for pair in "logweir=$RUNNER_DIGEST" "weirkeeper=$CONTROLLER_DIGEST" \
                "logweir-ui=$UI_DIGEST" "logweir-console=$CONSOLE_DIGEST"; do
      product=${pair%%=*} digest=${pair#*=}
      [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "no digest for $product" >&2; exit 1; }
      cosign sign --yes --recursive "docker.io/$NS/$product@$digest"
    done
    for arch in amd64 arm64; do
      digest=$(jq -er --arg sha "$GITHUB_SHA" 'select(.sha == $sha and .product == "logweir") | .digest' \
        "image-digests/logweir-$arch.json")
      cosign attest --yes --type spdxjson --predicate "image-digests/logweir-$arch.spdx.json" \
        "docker.io/$NS/logweir@$digest"
    done
    ;;
  verify-signatures)
    # What an adopter runs (docs/install.md, "Verify the images"), with the
    # same pins. EVERY PIN IS A LITERAL AND NONE IS A REGULAR EXPRESSION
    # (security review of PROD-00.2):
    #   * `--certificate-identity`: the certificate's SAN, which for a reusable
    #     workflow is the CALLED workflow's ref — images.yml on main — whoever
    #     called it. A public repository's reusable workflow can be called from
    #     any repository, so the SAN alone does not say whose run signed;
    #   * `--certificate-github-workflow-repository`: the repository the run
    #     belonged to (the token's `repository` claim, the CALLER's), which
    #     is what refuses a signature made by another repository's workflow
    #     calling this one;
    #   * `--certificate-github-workflow-ref` and `-trigger`: main, on a push —
    #     the only event ci.yml's `publish` job, images.yml's one caller, runs on;
    #   * `--certificate-oidc-issuer`: GitHub's.
    # cosign documents all five for `verify` and `verify-attestation` in v2.0.0
    # and in v2.5.2, the version the sign job installs
    # (https://github.com/sigstore/cosign/blob/v2.5.2/doc/cosign_verify.md,
    # .../doc/cosign_verify-attestation.md). scripts/check-cosign-verify.py
    # refuses any cosign verification in the repository without them.
    : "${RUNNER_DIGEST:?}" "${CONTROLLER_DIGEST:?}" "${UI_DIGEST:?}" "${CONSOLE_DIGEST:?}"
    for pair in "logweir=$RUNNER_DIGEST" "weirkeeper=$CONTROLLER_DIGEST" \
                "logweir-ui=$UI_DIGEST" "logweir-console=$CONSOLE_DIGEST"; do
      product=${pair%%=*} digest=${pair#*=}
      cosign verify \
        --certificate-identity "https://github.com/VladyslavHaina/logweir/.github/workflows/images.yml@refs/heads/main" \
        --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
        --certificate-github-workflow-repository "VladyslavHaina/logweir" \
        --certificate-github-workflow-ref "refs/heads/main" \
        --certificate-github-workflow-trigger "push" \
        "docker.io/$NS/$product@$digest" >/dev/null
      echo "- verified: docker.io/$NS/$product@$digest" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
    done
    for arch in amd64 arm64; do
      digest=$(jq -er --arg sha "$GITHUB_SHA" 'select(.sha == $sha and .product == "logweir") | .digest' \
        "image-digests/logweir-$arch.json")
      cosign verify-attestation --type spdxjson \
        --certificate-identity "https://github.com/VladyslavHaina/logweir/.github/workflows/images.yml@refs/heads/main" \
        --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
        --certificate-github-workflow-repository "VladyslavHaina/logweir" \
        --certificate-github-workflow-ref "refs/heads/main" \
        --certificate-github-workflow-trigger "push" \
        "docker.io/$NS/logweir@$digest" >/dev/null
      echo "- verified SBOM attestation: docker.io/$NS/logweir@$digest (linux/$arch)" \
        >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
    done
    ;;
  *) echo 'usage: ci-images.sh check|candidates|sbom|promote|sign|verify-signatures|chart|chart-package <dir>|chart-push <package>' >&2; exit 2 ;;
esac
