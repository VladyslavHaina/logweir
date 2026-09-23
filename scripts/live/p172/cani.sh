#!/usr/bin/env bash
# kubectl auth can-i matrix for the two ServiceAccounts the chart renders, as
# emulated by setup.sh. Each row states its expected answer; a mismatch fails.
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
API="system:serviceaccount:$CTL:logweir-api"; WK="system:serviceaccount:$CTL:weirkeeper"
LAB="system:serviceaccount:logweir-scram-local:weirkeeper"
fail=0; n=0
check() { # who expected args...
  local who="$1" want="$2"; shift 2
  local got; got="$($T 30 $K auth can-i "$@" --as "$who" 2>/dev/null)"; got="${got:-no}"
  n=$((n+1))
  local verdict=ok; [ "$got" = "$want" ] || { verdict=MISMATCH; fail=1; }
  printf '%-8s %-48s %-4s (want %-3s) can-i %s\n' "$verdict" "${who##*:}@${who#system:serviceaccount:}" "$got" "$want" "$*"
}
echo "# API ServiceAccount $API (chart api-rbac.yaml, namespaced copy; bound in A and B)"
for ns in "$A" "$B"; do
  check "$API" yes create kafkaclusters -n "$ns"
  check "$API" yes create restores -n "$ns"
  check "$API" yes list backups -n "$ns"
  check "$API" yes patch backupschedules -n "$ns"
  check "$API" yes create secrets -n "$ns"      # write-only credential entry
  check "$API" yes create approvals -n "$ns"    # PLAT-19.2's approval route
done
check "$API" no get secrets -n "$A"
check "$API" no list secrets -n "$A"
check "$API" no watch secrets -n "$A"
check "$API" no create jobs -n "$A"
check "$API" no create pods -n "$A"
check "$API" no get pods --subresource=log -n "$A"
check "$API" no create pods --subresource=exec -n "$A"
check "$API" no create pods --subresource=attach -n "$A"
check "$API" no delete restores -n "$A"
check "$API" no delete backups -n "$A"
check "$API" no update backups -n "$A"
check "$API" no create kafkaclusters -n "$Z"
check "$API" no list backups -n "$Z"
check "$API" no get secrets -n "$CTL"
check "$API" no create rolebindings -n "$A"
check "$API" no bind clusterroles
check "$API" no escalate roles -n "$A"
check "$API" no create serviceaccounts --subresource=token -n "$CTL"
check "$API" no create subjectaccessreviews
check "$API" no impersonate users
check "$API" no impersonate groups
check "$API" no impersonate serviceaccounts -n "$CTL"
check "$API" no list namespaces
echo "# Scoped controller ServiceAccount $WK (chart controller-scope.yaml; watchNamespaces=[A,B], release namespace CTL)"
for ns in "$A" "$B"; do
  check "$WK" yes create jobs -n "$ns"
  check "$WK" yes watch backups -n "$ns"
  check "$WK" yes list pods -n "$ns"
  check "$WK" yes get pods --subresource=log -n "$ns"
  check "$WK" yes create configmaps -n "$ns"
  check "$WK" yes patch restores --subresource=status -n "$ns"
done
check "$WK" no create jobs -n "$CTL"
check "$WK" no list jobs -n "$CTL"
check "$WK" no create pods -n "$CTL"
check "$WK" no list pods -n "$CTL"
check "$WK" no create configmaps -n "$CTL"
check "$WK" yes get configmaps/weirkeeper-policy -n "$CTL"
check "$WK" no get configmaps/unrelated -n "$CTL"
check "$WK" no get secrets/logweir-console-keys -n "$CTL"
check "$WK" no get secrets -n "$A"
check "$WK" no create jobs -n "$Z"
check "$WK" no watch backups -n "$Z"
check "$WK" no watch backups --all-namespaces
check "$WK" no list jobs --all-namespaces
check "$WK" no create pods --subresource=exec -n "$A"
check "$WK" no delete backups -n "$A"
check "$WK" no impersonate users
# the cluster-scoped halves ARE created (renamed copies):
check "$WK" yes list trustpolicies
check "$WK" yes watch trustrosters
check "$WK" yes patch trustpolicies --subresource=status
check "$WK" no create trustpolicies
check "$WK" no delete trustrosters
check "$API" yes get trustpolicies
check "$API" yes list trustpolicies
check "$API" no create trustpolicies
check "$API" yes get trustrosters/default
check "$API" no get trustrosters/other
check "$API" no list trustrosters
echo "# Contrast: the lab's UNSCOPED controller (cluster-wide ClusterRoleBinding, the default render)"
check "$LAB" yes create jobs -n "$CTL"
check "$LAB" yes create jobs -n "$Z"
check "$LAB" yes watch backups --all-namespaces
echo "rows=$n fail=$fail"
exit $fail
