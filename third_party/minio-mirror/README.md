# third_party/minio-mirror/

The MinIO server and client images Logweir's test and demo stacks run, rebuilt
from upstream source and published by the project owner. **A stopgap**: it
keeps the compose stack, the chart's demo MinIO, the PoC grants Job and the
live harnesses pulling the same MinIO release they were measured against,
until MinIO is replaced by a maintained S3 server (a tracked task). Nothing in
Logweir's own images contains MinIO.

## Why it exists

MinIO withdrew its public images. It deleted `minio/minio` and `minio/mc` from
Docker Hub on 2026-09-11, and on 2026-09-24 at about 12:55 UTC its quay.io
repositories began refusing anonymous pulls. Logweir pinned

| upstream reference (withdrawn) | release |
|---|---|
| `quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e` | MinIO `RELEASE.2025-09-07T16-13-09Z` |
| `quay.io/minio/mc@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727` | mc `RELEASE.2025-08-13T08-35-41Z` |

and CI's `e2e` job began failing at `just e2e-up` (run 36015645910). Only the
arm64 halves of those indexes were cached on the author's host, and
`dl.min.io`, where upstream's own Dockerfiles download the release binaries
from, answers `410 Gone`. So the images are rebuilt from the archived upstream
source, for linux/amd64 and linux/arm64.

## What is published

| mirror | index digest (amd64 + arm64) |
|---|---|
| `docker.io/vladyslavhaina/minio-mirror:RELEASE.2025-09-07T16-13-09Z` | `@@MINIO_INDEX@@` |
| `docker.io/vladyslavhaina/mc-mirror:RELEASE.2025-08-13T08-35-41Z` | `@@MC_INDEX@@` |

Both repositories are public; every reference in this tree pins the digest,
never the tag (`crates/logweir/tests/chart_lint.rs`,
`chart_lint_every_minio_image_reference_is_the_mirror_digest`, refuses any
other MinIO reference outside this directory and `THIRD_PARTY_NOTICES.md`).

## Provenance: how the images are built

`bash third_party/minio-mirror/build.sh --push` builds both from this
directory ([Dockerfile.minio](Dockerfile.minio), [Dockerfile.mc](Dockerfile.mc)).

| | MinIO server | MinIO Client (mc) |
|---|---|---|
| source | `https://github.com/minio/minio` | `https://github.com/minio/mc` |
| tag | `RELEASE.2025-09-07T16-13-09Z` (annotated tag object `01ce918d8279a20e4706b96a64396146894adee4`) | `RELEASE.2025-08-13T08-35-41Z` (tag object `d6541ea280b73a834b64d4097e21f2be77676104`) |
| commit (checked in the build) | `07c3a429bfed433e49018cb0f78a52145d4bedeb` | `7394ce0dd2a80935aded936b09fa12cbb3cb8096` |
| toolchain | Go 1.24.6, `docker.io/library/golang:1.24.6-alpine@sha256:c8c5f95d64aa79b6547f3b626eb84b16a7ce18a139e3e9ca19a8c078b85ba80d`, `GOTOOLCHAIN=local` | same |
| build | `CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -tags kqueue -trimpath -ldflags "$LDFLAGS"` on `$BUILDPLATFORM` (cross-compiled, never emulated) | same |
| `$LDFLAGS` | upstream's `MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go`, with `GOPATH=/root/.q/sources/gopath GOROOT=/opt/go` (the release builder's values, read back out of upstream's binary) | upstream's `MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go` |
| base | `registry.access.redhat.com/ubi9/ubi-micro:9.6-1754467928@sha256:f5c5213d2969b7b11a6666fc4b849d56b48d9d7979b60a37bb853dff0255c14b` — the upstream image's own base layer (its arm64 diff_id `sha256:9dda751d…` is the upstream image's layer 1) | same |
| CA bundle | `/etc/ssl/certs/ca-certificates.crt` of the golang image above — byte-identical to upstream's | `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem` of `registry.access.redhat.com/ubi9/ubi-minimal:9.6@sha256:34880b64c07f28f64d95737f82f891516de9a3b43583f39970f7bf8e4cfa48b7` — byte-identical to upstream's |
| other contents | `mc` (the build of the column to the right), upstream's `mc.minisig`/`mc.sha256sum`, the static `curl` 8.11.0 upstream's image carried ([licenses/curl/README](licenses/curl/README)), `docker-entrypoint.sh` from the source tree | — |

The expanded ldflags, as the build prints them:

```text
minio: -s -w -X github.com/minio/minio/cmd.Version=2025-09-07T16:13:09Z -X github.com/minio/minio/cmd.CopyrightYear=2025 -X github.com/minio/minio/cmd.ReleaseTag=RELEASE.2025-09-07T16-13-09Z -X github.com/minio/minio/cmd.CommitID=07c3a429bfed433e49018cb0f78a52145d4bedeb -X github.com/minio/minio/cmd.ShortCommitID=07c3a429bfed -X github.com/minio/minio/cmd.GOPATH=/root/.q/sources/gopath -X github.com/minio/minio/cmd.GOROOT=/opt/go
mc:    -s -w -X github.com/minio/mc/cmd.Version=2025-08-13T08:35:41Z -X github.com/minio/mc/cmd.CopyrightYear=2025 -X github.com/minio/mc/cmd.ReleaseTag=RELEASE.2025-08-13T08-35-41Z -X github.com/minio/mc/cmd.CommitID=7394ce0dd2a80935aded936b09fa12cbb3cb8096 -X github.com/minio/mc/cmd.ShortCommitID=7394ce0dd2a8
```

An untracked `.mirror-build` file is created in each source tree before
`go build`. It changes no compiled byte; it makes the VCS stamp read
`vcs.modified=true` (module version `+dirty`), as upstream's release binaries'
does — `go version -m` on the upstream binaries shows it.

### How close the binaries are to upstream's

`upstream/` holds upstream's published `sha256sum` and minisign files for both
architectures (from the GitHub releases, which are still readable).

* **`mc` is byte-identical to upstream's signed release binary**, on amd64
  (`01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891`) and arm64
  (`14c8c9616cfce4636add161304353244e8de383b2e2752c0e9dad01d4c27c12c`). Both
  Dockerfiles stop unless the sha256 matches and `minisign -V` accepts
  upstream's signature under MinIO's key
  `RWTx5Zr1tiHQLwG9keckT0c45M3AGeHD6IvimQHpyRywVWGbP1aVSGav`, so the `mc` in
  both images is upstream's bytes or there is no image.
* **`minio` matches upstream's build in everything that is recorded**: the
  same size (arm64 105 251 000 bytes, amd64 110 989 496), identical
  `go version -m` output (toolchain, every dependency and its hash, `-tags`,
  `-trimpath`, `CGO_ENABLED`, `GOARM64`, the VCS stamp), and identical code
  and data except 87 bytes on arm64 and 94 on amd64: the Go build ID and the
  GNU build-id note (hashes of the build inputs), and **two pairs of
  independent instructions in one function scheduled in the opposite order** —
  `go.opentelemetry.io/otel/sdk/metric/internal/aggregate.(*expoHistogram[go.shape.int64]).measure`
  (arm64: `str x6,[sp,#152]` / `str x5,[sp,#88]`, and `ldr x0,[sp,#128]` /
  `ldr x2,[x26]`, each pair swapped; the amd64 difference is in the same
  function). Neither pair depends on the other's result, so the behaviour is
  the same. Our build is deterministic (a fresh-cache rebuild gives the same
  sha256); upstream's release tree was `+dirty` with a change that was never
  published, which is the likely cause and cannot be reproduced. Upstream's
  `minio.minisig` and `minio.sha256sum` are therefore **not** shipped in this
  image — they describe upstream's bytes, not these.

### Image configuration against upstream's

`docker image inspect` of the rebuild against the cached upstream arm64 image:
identical `Entrypoint` (`/usr/bin/docker-entrypoint.sh`, upstream's script
byte for byte), `Cmd` (`minio`), `Env` (all nine variables), `ExposedPorts`
(`9000/tcp`), `Volumes` (`/data`), `WorkingDir` (`/`), `User` (unset, root),
and for mc `Entrypoint` `mc`, `Env` `PATH` only; the base layer is the same
layer. File by file (`docker export` of both, arm64):

* **mc image: all 544 files byte-identical to upstream's, with the same modes
  and owners** — the binary, the CA bundle, `LICENSE`, `CREDITS`, and the UBI
  base.
* **server image: 548 of upstream's 551 files byte-identical, same modes and
  owners** (`/usr/bin` 0777 by upstream's own `chmod -R 777 /usr/bin`). The
  three others: `/usr/bin/minio` (above), and `minio.minisig` /
  `minio.sha256sum`, left out (above). Added: `/licenses/curl/` (eight files).

The differences, all deliberate:

| | upstream | mirror |
|---|---|---|
| label `maintainer` | `MinIO Inc <dev@min.io>` | `Logweir project mirror (docker.io/vladyslavhaina), not MinIO, Inc.` — MinIO does not maintain this image |
| `org.opencontainers.image.*` labels | none | `source`, `revision` (upstream commit), `version`, `licenses`, `title`, `description`, `url` |
| label `io.logweir.mirror.recipe-revision` | — | the Logweir commit this recipe was built from |
| `/licenses/curl/` | absent (upstream shipped curl without its notices) | curl, OpenSSL, zlib, libssh2, nghttp2, musl and static-curl licence texts |
| `/usr/bin/minio.minisig`, `minio.sha256sum` | present | absent (see above) |
| layers | mc image: `ADD` + `RUN chmod +x` (the binary twice) | one `COPY --chmod=0711` |
| `created` | 2025-09-07 | the build date |


## Verifying an image

```bash
docker run --rm --entrypoint minio docker.io/vladyslavhaina/minio-mirror@@@MINIO_INDEX@@ --version
docker run --rm docker.io/vladyslavhaina/mc-mirror@@@MC_INDEX@@ --version
bash third_party/minio-mirror/smoke.sh <minio-image> <mc-image> linux/arm64 /tmp/smoke.tsv
```

[smoke.sh](smoke.sh) is the behaviour Logweir relies on — bucket create; a
user whose least-privilege policy allows `PutObject` and denies `GetObject`,
checked from both sides; versioning; Object Lock on a bucket created with lock
enabled; the conditional create (a second `PUT` with `If-None-Match: *` is
`412`, Logweir's execution claim); HEAD, list, delete — 48 checks, each against
the value upstream gives. On 2026-09-24 it passed, with identical output, against
the upstream arm64 images and against both architectures of the mirror
(amd64 under emulation).

## Licence

MinIO and mc are licensed by MinIO, Inc. under the **GNU Affero General Public
License, version 3**; the images carry upstream's `LICENSE` and `CREDITS` under
`/licenses/`, and are labelled `org.opencontainers.image.licenses=AGPL-3.0-only`.
Most upstream source files grant "version 3 of the License, or (at your
option) any later version"; the licence texts in `/licenses/` and the source
headers are what govern, not the label. **This is an unmodified rebuild**: the
corresponding source is exactly the upstream repositories at the commits
above, and the build scripts are this directory and upstream's own
`buildscripts/gen-ldflags.go`. The curl binary's notices are in
[licenses/curl/](licenses/curl/README). The base image is Red Hat's freely
redistributable UBI 9 (`/usr/share/licenses/` in the image; the UBI EULA is
linked from its `com.redhat.license_terms` label).

MinIO is a trademark of MinIO, Inc. This mirror is not affiliated with or
endorsed by MinIO, Inc.; it exists only so that Logweir's own test stacks keep
running the release they were measured against.
