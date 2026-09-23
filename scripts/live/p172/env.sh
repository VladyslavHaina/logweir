# Sourced by every script in scripts/live/p172. One place for the names, the
# owner label and the binaries, so no script carries a host path or a run tag.
#
#   TS, OUT         the run stamp and its private output directory (run.sh sets both)
#   P172_OWNER      the logweir.dev/test-owner value written and checked (default plat17-2)
#   P172_PREFIX     the namespace / cluster-object prefix (default lw-p172-)
#   LOGWEIR_REPO    the checkout whose ui/ and target/ are used (default: this repo)
#   LOGWEIR_API_BIN, LOGWEIR_WEIRKEEPER_BIN   default $LOGWEIR_REPO/target/debug/…
#   LOGWEIR_PYTHON  a python3 with `cryptography` (the OIDC mock signs ES256)
#   LWTIMEOUT       a `<seconds> <command…>` timeout wrapper (default /tmp/lwtimeout,
#                   falling back to a perl alarm one-liner when it is absent)
H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${LOGWEIR_REPO:-$(cd "$H/../../.." && pwd)}"
OWNER="${P172_OWNER:-plat17-2}"
PREFIX="${P172_PREFIX:-lw-p172-}"
TS="${TS:?TS is required}"
OUT="${OUT:?OUT is required}"
A="${PREFIX}a-$TS"; B="${PREFIX}b-$TS"; CTL="${PREFIX}ctl-$TS"; Z="${PREFIX}z-$TS"
P="${PREFIX}$TS"   # cluster-scoped object prefix
K="kubectl --context docker-desktop"
PY="${LOGWEIR_PYTHON:-python3}"
API_BIN="${LOGWEIR_API_BIN:-$REPO/target/debug/logweir-api}"
WK_BIN="${LOGWEIR_WEIRKEEPER_BIN:-$REPO/target/debug/weirkeeper}"
if [ -x "${LWTIMEOUT:-/tmp/lwtimeout}" ]; then
  T="${LWTIMEOUT:-/tmp/lwtimeout}"
else
  T="perl -e alarm(shift);exec(@ARGV)"
fi
export TS OUT P172_OWNER="$OWNER" P172_PREFIX="$PREFIX" LOGWEIR_REPO="$REPO" LOGWEIR_API_BIN="$API_BIN"
