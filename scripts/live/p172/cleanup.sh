#!/usr/bin/env bash
# Delete ONLY what this run created: each namespace and cluster-scoped RBAC
# object is deleted after its logweir.dev/test-owner label reads $OWNER (and a
# namespace's UID is printed beside it), and the run's credential files
# (ServiceAccount kubeconfigs, session/cursor keys, client secret, TLS key)
# are removed from $OUT. Safe to run twice.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
for ns in "$A" "$B" "$CTL" "$Z"; do
  owner="$($K get ns "$ns" -o jsonpath='{.metadata.labels.logweir\.dev/test-owner}' 2>/dev/null)"
  uid="$($K get ns "$ns" -o jsonpath='{.metadata.uid}' 2>/dev/null)"
  if [ -z "$uid" ]; then
    echo "absent $ns"
  elif [ "$owner" = "$OWNER" ]; then
    echo "deleting $ns uid=$uid owner=$owner"
    $T 200 $K delete ns "$ns" --wait=true --timeout=180s
  else
    echo "SKIP $ns: owner label '$owner' is not $OWNER"
  fi
done
for kind in clusterrolebinding clusterrole; do
  for n in "$P-cluster-scope" "$P-api-trustpolicies" "$P-api-trustroster"; do
    owner="$($K get $kind "$n" -o jsonpath='{.metadata.labels.logweir\.dev/test-owner}' 2>/dev/null)"
    if [ "$owner" = "$OWNER" ]; then $K delete $kind "$n"; elif [ -n "$owner" ]; then echo "SKIP $kind/$n owner='$owner'"; fi
  done
done
echo "left with owner $OWNER and prefix $PREFIX:"
$K get ns,clusterrole,clusterrolebinding -l "logweir.dev/test-owner=$OWNER" -o name 2>/dev/null | grep -F "$PREFIX" || echo "  nothing"
rm -f "$OUT"/kubeconfig-* "$OUT"/session.key "$OUT"/cursor.key "$OUT"/cursor-local.key "$OUT"/client-secret "$OUT"/tls.key
echo "credential files removed from $OUT"
