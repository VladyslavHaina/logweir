#!/usr/bin/env bash
# Global Constraint 10, on the shipped artefact: a release binary carries no
# engine. `scripts/check-no-oso.sh` proves no crate in the dependency graph is
# kafka-backup-core; this proves it of the bytes a release actually ships,
# which is the only place an engine embedded as data would show.
#
# THE SIGNAL is the string `kafka-backup-core`. The engine's own source paths
# are compiled into its panic locations, so a linked or embedded engine carries
# that string. Logweir's runtime messages also CITE the engine's source, as
# `[U:crates/kafka-backup-core/…]`, `[U/kafka-backup/crates/kafka-backup-core/…]`
# or `[VERIFIED U/…]`. Those citations are removed before the search: the
# v0.1.4 and v0.1.5 releases failed because the check they ran counted them.
#
# THE RAW BYTES ARE SEARCHED, NOT `strings` OUTPUT. Apple's `strings` reads
# Mach-O sections only, so on the macOS release runner a path appended to the
# binary never reached the search (measured); `grep -a` reads the whole file on
# every runner.
#
# Usage: check-no-engine-in-binary.sh <logweir binary | release .tar.xz/.tar.gz>
set -euo pipefail
export LC_ALL=C

target="${1:?usage: check-no-engine-in-binary.sh <logweir binary | release archive>}"
if [ ! -f "$target" ]; then
  echo "FAIL: $target is not a file" >&2
  exit 1
fi

work="$(mktemp -d "${TMPDIR:-/tmp}/logweir-no-engine.XXXXXX")"
trap 'rm -rf "$work"' EXIT

bin="$target"
case "$target" in
  *.tar.xz | *.tar.gz)
    mkdir "$work/archive"
    tar -xf "$target" -C "$work/archive"
    find "$work/archive" -type f -name logweir > "$work/binaries"
    count=$(( $(wc -l < "$work/binaries") ))
    if [ "$count" -ne 1 ]; then
      echo "FAIL: $target holds $count file(s) named logweir; expected exactly one" >&2
      exit 1
    fi
    bin="$(cat "$work/binaries")"
    ;;
esac

echo "== $(basename "$target"): no kafka-backup-core outside a source citation =="
# A file with no `logweir` in it is not this binary, and a search over it would
# pass for the wrong reason.
if ! grep -a -q 'logweir' "$bin"; then
  echo "FAIL: no \`logweir\` string in $bin; this is not the binary to check" >&2
  exit 1
fi

set +e
grep -a -o '[[:print:]]*kafka-backup-core[[:print:]]*' "$bin" > "$work/mentions"
rc=$?
set -e
if [ "$rc" -gt 1 ]; then
  echo "FAIL: grep exited $rc reading $bin" >&2
  exit 1
fi
sed -E 's#\[(VERIFIED )?U[/:][^] ]*##g' "$work/mentions" > "$work/uncited"

set +e
grep -n 'kafka-backup-core' "$work/uncited" > "$work/hits"
rc=$?
set -e
case "$rc" in
  0)
    echo "FAIL: $bin carries kafka-backup-core outside a source citation; an engine may be linked or embedded:" >&2
    sed -n '1,20p' "$work/hits" >&2
    exit 1
    ;;
  1)
    cited=$(( $(wc -l < "$work/mentions") ))
    echo "ok: $bin names kafka-backup-core only inside $cited source citation(s)"
    ;;
  *)
    echo "FAIL: grep exited $rc reading $work/uncited" >&2
    exit 1
    ;;
esac
