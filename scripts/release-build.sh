#!/usr/bin/env bash
# Build ONE release archive of the `logweir` CLI and check what it ships
# (PROD-14.0). `release.yml`'s build matrix runs it once per target.
#
#   RELEASE_TAG=v<semver> bash scripts/release-build.sh <target-triple>
#
# NATIVE BUILDS ONLY: the host must BE the target. rdkafka compiles librdkafka
# from C and links the target's OpenSSL, SASL and zlib, so a cross build needs a
# sysroot of every one of them; a runner of each architecture needs nothing.
# The v0.1.5 run measured all three native builds compiling (34895138327).
#
# LINUX ARCHIVES ARE BUILT INSIDE rust:1.89-bookworm, the runner image's own
# builder base (`Dockerfile`, `FROM … rust:1.89-bookworm AS builder`), with the
# packages that stage installs, minus the cross toolchain. A Linux archive
# therefore has the runner image's ABI: Debian 12's glibc, `libssl.so.3` and
# `libsasl2.so.2`. Built on the ubuntu-24.04 runner itself it would need the
# runner's newer glibc instead. release.yml starts it as
#
#   docker run --rm -e RELEASE_TAG -v "$PWD:/src" -w /src rust:1.89-bookworm \
#     bash scripts/release-build.sh <target>
#
# and runs it directly on the macOS runner.
#
# What it writes: `<target dir>/distrib/logweir-<target>.tar.xz` and its
# `.sha256` (dist), and `<target dir>/distrib/logweir-<target>.linkage.txt`
# (what the binary needs at run time, MEASURED here and quoted in the release
# notes). What it refuses: an archive whose sidecar, contents, notices,
# engine check, smoke run or linkage is not the one the release documents.
set -euo pipefail
cd "$(dirname "$0")/.."
export LC_ALL=C

target="${1:?usage: release-build.sh <target-triple>}"
: "${RELEASE_TAG:?set RELEASE_TAG to the v<semver> tag this archive is built for}"
[[ "$RELEASE_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] \
  || { echo "release-build.sh: RELEASE_TAG '$RELEASE_TAG' is not v<semver>" >&2; exit 2; }

# cargo-dist, pinned by version AND by the sha256 its own release publishes
# (axodotdev/cargo-dist v0.28.0, `sha256.sum`). The musl builds on Linux: they
# are static, so they run in any container.
DIST_VERSION=0.28.0
case "$target" in
  x86_64-unknown-linux-gnu)
    want_os=Linux want_arch=x86_64
    dist_asset=cargo-dist-x86_64-unknown-linux-musl.tar.xz
    dist_sha256=0aea6d50e86c0b9d4f91022577df84a96c4ff405cfc317472890662c2af1df35 ;;
  aarch64-unknown-linux-gnu)
    want_os=Linux want_arch=aarch64
    dist_asset=cargo-dist-aarch64-unknown-linux-musl.tar.xz
    dist_sha256=31a445ab8a584dc9384c92372045de471d2d214b797f0cf300db3c003c627b02 ;;
  aarch64-apple-darwin)
    want_os=Darwin want_arch=arm64
    dist_asset=cargo-dist-aarch64-apple-darwin.tar.xz
    dist_sha256=436e9d1e503b106e938ac8e5e8218d5ad12b161430c8a1f874934271a1f869e9 ;;
  *) echo "release-build.sh: no release archive is built for '$target'" >&2; exit 2 ;;
esac
if [[ "$(uname -s)" != "$want_os" || "$(uname -m)" != "$want_arch" ]]; then
  echo "release-build.sh: $target is built natively; this host is $(uname -s)/$(uname -m)" >&2
  exit 2
fi

sha256_of() { # file -> hex
  local line
  if command -v sha256sum >/dev/null 2>&1; then line=$(sha256sum "$1"); else line=$(shasum -a 256 "$1"); fi
  printf '%s\n' "${line%% *}"
}

work="$(mktemp -d "${TMPDIR:-/tmp}/logweir-release-build.XXXXXX")"
trap 'rm -rf "$work"' EXIT

# ---------------------------------------------------------------- toolchain
if [[ "$want_os" == Linux ]]; then
  if [[ ! -f /etc/debian_version || "$(cut -d. -f1 /etc/debian_version)" != 12 ]]; then
    echo "release-build.sh: Linux archives are built in rust:1.89-bookworm (Debian 12), not here" >&2
    exit 2
  fi
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  # The Dockerfile builder stage's packages, native: cmake (librdkafka),
  # clang/libclang (bindgen via zstd-sys), pkg-config, the -dev libraries the
  # -sys crates link, and libcurl's HEADER (librdkafka's config.h includes it
  # even with WITH_CURL=0 — see the Dockerfile). binutils for objdump, and
  # Python with `cryptography` for the countersigning check, below.
  apt-get install -y -qq --no-install-recommends \
    cmake pkg-config clang libclang-dev libsasl2-dev libssl-dev zlib1g-dev \
    libcurl4-openssl-dev binutils xz-utils curl ca-certificates \
    python3 python3-cryptography > "$work/apt.log" \
    || { cat "$work/apt.log" >&2; exit 1; }
fi
channel=$(awk -F'"' '/^channel *=/ { print $2; exit }' rust-toolchain.toml)
[[ "$channel" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "rust-toolchain.toml pins no X.Y.Z channel" >&2; exit 1; }
rustup toolchain install "$channel" --profile minimal --component rustfmt --component clippy > "$work/rustup.log" 2>&1 \
  || { cat "$work/rustup.log" >&2; exit 1; }
command -v cmake >/dev/null 2>&1 || { echo "release-build.sh: cmake is required (librdkafka)" >&2; exit 1; }

curl -fsSL --retry 3 --max-time 300 -o "$work/$dist_asset" \
  "https://github.com/axodotdev/cargo-dist/releases/download/v$DIST_VERSION/$dist_asset"
[[ "$(sha256_of "$work/$dist_asset")" == "$dist_sha256" ]] \
  || { echo "release-build.sh: $dist_asset is not the published cargo-dist $DIST_VERSION" >&2; exit 1; }
tar -xJf "$work/$dist_asset" -C "$work"
dist="$work/${dist_asset%.tar.xz}/dist"
dist_version=$("$dist" --version)
[[ "$dist_version" == "cargo-dist $DIST_VERSION" ]] \
  || { echo "release-build.sh: $dist reports '$dist_version'" >&2; exit 1; }

# -------------------------------------------------------------------- build
distrib="${CARGO_TARGET_DIR:-target}/distrib"
name="logweir-$target"
archive="$distrib/$name.tar.xz"
rm -rf "${distrib:?}/$name" "$archive" "$archive.sha256" "$distrib/$name.linkage.txt"
# dist passes no `--locked`, so the lockfile is checked around it instead: the
# release builds the graph CI tested, or nothing.
lock_before=$(sha256_of Cargo.lock)
"$dist" build --tag "$RELEASE_TAG" --force-tag --artifacts=local --target="$target" \
  --print=linkage --output-format=json > "$work/dist-build.json"
[[ "$(sha256_of Cargo.lock)" == "$lock_before" ]] \
  || { echo "release-build.sh: the build changed Cargo.lock; a release builds the locked graph" >&2; exit 1; }
[[ -f "$archive" && -f "$archive.sha256" ]] || { echo "release-build.sh: dist wrote no $archive" >&2; exit 1; }

# 1. The sidecar names this file and its digest (`<hex> *<name>`).
read -r want sidecar_name < "$archive.sha256"
[[ "${sidecar_name#\*}" == "$name.tar.xz" && "$(sha256_of "$archive")" == "$want" ]] \
  || { echo "release-build.sh: $archive.sha256 does not describe $archive" >&2; exit 1; }

# 2. Exactly the binary and the four documents NOTICE says travel with it.
tar -tJf "$archive" > "$work/listed"
sort "$work/listed" > "$work/listed.sorted"
printf '%s\n' "$name/" "$name/LICENSE" "$name/NOTICE" "$name/README.md" \
  "$name/THIRD_PARTY_NOTICES.md" "$name/logweir" | sort > "$work/expected"
if ! cmp -s "$work/expected" "$work/listed.sorted"; then
  echo "release-build.sh: $archive does not hold exactly the binary and its four notices:" >&2
  diff "$work/expected" "$work/listed.sorted" >&2 || true
  exit 1
fi
mkdir "$work/x"
tar -xJf "$archive" -C "$work/x"
for doc in LICENSE NOTICE README.md THIRD_PARTY_NOTICES.md; do
  cmp -s "$doc" "$work/x/$name/$doc" || { echo "release-build.sh: the archive's $doc is not this commit's" >&2; exit 1; }
done

# 3. No engine inside (Global Constraint 10).
bash scripts/check-no-engine-in-binary.sh "$archive"

# 4. The packaged binary starts on this host and carries the approver's
#    countersigning command (the owner's step for a Governed restore).
bin="$work/x/$name/logweir"
version=$("$bin" --version)
case "$version" in
  "logweir "*) echo "ok: $name runs: $version" ;;
  *) echo "release-build.sh: $bin --version printed '$version'" >&2; exit 1 ;;
esac
"$bin" drill countersign --help > "$work/countersign-help"
for flag in --document --confirmation --key --out; do
  grep -q -- "$flag" "$work/countersign-help" \
    || { echo "release-build.sh: drill countersign has no $flag" >&2; exit 1; }
done
echo "ok: $name carries drill countersign"
# ...and PERFORMS it: this binary countersigns a throwaway Governed request,
# checked by code that is not Logweir's (scripts/release-countersign-check.py).
if [[ -n "${LOGWEIR_PYTHON:-}" ]]; then
  py="$LOGWEIR_PYTHON"
elif [[ "$want_os" == Linux ]]; then
  py=python3
else
  python3 -m venv "$work/venv" > "$work/venv.log" 2>&1 || { cat "$work/venv.log" >&2; exit 1; }
  "$work/venv/bin/python" -m pip install --quiet --disable-pip-version-check cryptography > "$work/pip.log" 2>&1 \
    || { cat "$work/pip.log" >&2; exit 1; }
  py="$work/venv/bin/python"
fi
"$py" scripts/release-countersign-check.py "$bin"

# 5. What it needs at run time, measured, and held to what the docs state.
linkage="$distrib/$name.linkage.txt"
if [[ "$want_os" == Linux ]]; then
  objdump -p "$bin" > "$work/objdump-p"
  objdump -T "$bin" > "$work/objdump-T"
  awk '$1 == "NEEDED" { print $2 }' "$work/objdump-p" | sort > "$work/needed"
  grep -o 'GLIBC_[0-9][0-9.]*' "$work/objdump-T" | sed 's/GLIBC_//' | sort -u -V > "$work/glibc" || true
  glibc=$(tail -n 1 "$work/glibc")
  [[ -n "$glibc" ]] || { echo "release-build.sh: no GLIBC symbol version in $bin" >&2; exit 1; }
  # Debian 12 ships glibc 2.36: a binary built here can need no newer one.
  highest=$(printf '%s\n%s\n' "$glibc" 2.36 | sort -V | tail -n 1)
  [[ "$highest" == 2.36 ]] || { echo "release-build.sh: $bin needs glibc $glibc, above Debian 12's 2.36" >&2; exit 1; }
  for lib in libssl.so.3 libcrypto.so.3 libsasl2.so.2; do
    grep -qx "$lib" "$work/needed" || { echo "release-build.sh: $bin does not link $lib; the documented requirement is wrong" >&2; exit 1; }
  done
  # What a host must install, apart from glibc itself.
  grep -v -E '^(libc|libm|libdl|libpthread|librt)\.so\.[0-9]+$|^libgcc_s\.so\.1$|^ld-linux' "$work/needed" > "$work/install" || true
  grep -E '^(libc|libm|libdl|libpthread|librt)\.so\.[0-9]+$|^libgcc_s\.so\.1$|^ld-linux' "$work/needed" > "$work/system" || true
  {
    echo "target: $target"
    echo "glibc: $glibc or newer (the highest versioned symbol the binary references)"
    echo "needs: $(tr '\n' ' ' < "$work/install" | sed 's/ $//') (Debian and Ubuntu: libssl3, libsasl2-2, zlib1g)"
    echo "system: $(tr '\n' ' ' < "$work/system" | sed 's/ $//')"
  } > "$linkage"
else
  otool -L "$bin" > "$work/otool"
  awk 'NR > 1 { print $1 }' "$work/otool" | sort > "$work/dylibs"
  # The system's own libraries and frameworks, and Homebrew's OpenSSL 3 — the
  # one requirement a Mac without it must install (`brew install openssl@3`).
  # Anything else is a new requirement the release notes do not state.
  if grep -v -E '^(/usr/lib/|/System/Library/|/opt/homebrew/opt/openssl@3/lib/)' "$work/dylibs" > "$work/other"; then
    echo "release-build.sh: $bin links a library the release does not document:" >&2
    cat "$work/other" >&2
    exit 1
  fi
  grep -qx '/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib' "$work/dylibs" \
    || { echo "release-build.sh: $bin no longer links Homebrew's openssl@3; the documented requirement is wrong" >&2; exit 1; }
  otool -l "$bin" > "$work/load-commands"
  minos=$(awk '/LC_BUILD_VERSION/ { found = 1 } found && $1 == "minos" { print $2; exit }' "$work/load-commands")
  [[ "$minos" =~ ^[0-9]+(\.[0-9]+)*$ ]] || { echo "release-build.sh: no LC_BUILD_VERSION minos in $bin" >&2; exit 1; }
  grep -v -E '^(/usr/lib/|/System/Library/)' "$work/dylibs" > "$work/install" || true
  grep -E '^(/usr/lib/|/System/Library/)' "$work/dylibs" > "$work/system" || true
  {
    echo "target: $target"
    echo "macOS: $minos or newer (the binary's LC_BUILD_VERSION minos)"
    echo "needs: $(tr '\n' ' ' < "$work/install" | sed 's/ $//') (Homebrew: brew install openssl@3)"
    echo "system: $(tr '\n' ' ' < "$work/system" | sed 's/ $//')"
  } > "$linkage"
fi
echo "== $linkage =="
cat "$linkage"
