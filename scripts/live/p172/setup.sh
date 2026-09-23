#!/usr/bin/env bash
# PLAT-17.2 live proof — namespaces, emulated chart RBAC, SA kubeconfigs.
# Every kubectl names --context docker-desktop. Only namespaced objects in
# namespaces this script creates (label logweir.dev/test-owner=$OWNER, env.sh).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
for ns in "$A" "$B" "$CTL" "$Z"; do
  $K create namespace "$ns"
  $K label namespace "$ns" logweir.dev/test-owner=$OWNER
done
$K -n "$CTL" create serviceaccount logweir-api
$K -n "$CTL" create serviceaccount weirkeeper
# The console's keys live here (emulated): the object every can-i row asks about.
$K -n "$CTL" create secret generic logweir-console-keys --from-literal=placeholder=not-a-real-key
$K -n "$CTL" create configmap unrelated --from-literal=a=b

# --- the chart's API principal (templates/ui/api-rbac.yaml), namespaced copy.
# The chart renders ClusterRole logweir-api + RoleBindings; ClusterRoles are
# cluster-scoped and this run changes no cluster RBAC, so the SAME rules are
# applied as a Role in each bound namespace. (logweir-api-trustpolicies, the
# one cluster-scoped read, is not reproduced: listed for the lab refresh.)
for ns in "$A" "$B"; do
  cat <<YAML | $K apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: logweir-api, namespace: $ns, labels: {logweir.dev/test-owner: $OWNER}}
rules:
  - apiGroups: ["logweir.dev"]
    resources: [approvals, backupdestinations, backups, backupschedules, kafkaclusters, preflights, restores, topicdiscoveries]
    verbs: ["get", "list"]
  - apiGroups: ["logweir.dev"]
    resources: [approvals, backupdestinations, backups, backupschedules, kafkaclusters, preflights, restores, topicdiscoveries]
    verbs: ["create"]
  - apiGroups: ["logweir.dev"]
    resources: [backupdestinations, backupschedules, preflights, topicdiscoveries]
    verbs: ["patch"]
  - apiGroups: ["logweir.dev"]
    resources: [protectionpolicies, recoverycatalogs, rehearsalschedules, retentionpolicies]
    verbs: ["get", "list"]
  - apiGroups: ["logweir.dev"]
    resources: ["recoverycatalogs"]
    verbs: ["create"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: logweir-api, namespace: $ns, labels: {logweir.dev/test-owner: $OWNER}}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: logweir-api}
subjects: [{kind: ServiceAccount, name: logweir-api, namespace: $CTL}]
YAML
done

# --- the chart's SCOPED controller (templates/controller-scope.yaml,
# watchNamespaces=[A, B], release namespace CTL): RoleBinding weirkeeper to the
# EXISTING ClusterRole weirkeeper (rule-for-rule the chart's; diffed at run
# time) in A and B, and the one-ConfigMap Role in CTL. weirkeeper-cluster-scope
# is cluster-scoped and is not created here.
for ns in "$A" "$B"; do
  cat <<YAML | $K apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: weirkeeper, namespace: $ns, labels: {logweir.dev/test-owner: $OWNER}}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: weirkeeper}
subjects: [{kind: ServiceAccount, name: weirkeeper, namespace: $CTL}]
YAML
done
cat <<YAML | $K apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: weirkeeper-installation-policy, namespace: $CTL, labels: {logweir.dev/test-owner: $OWNER}}
rules:
  - apiGroups: [""]
    resources: ["configmaps"]
    resourceNames: ["weirkeeper-policy"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: weirkeeper-installation-policy, namespace: $CTL, labels: {logweir.dev/test-owner: $OWNER}}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: weirkeeper-installation-policy}
subjects: [{kind: ServiceAccount, name: weirkeeper, namespace: $CTL}]
YAML

# --- THE CLUSTER-SCOPED HALVES, created this time as renamed,
# owner-labelled copies of the chart's (the rules below are the 306cebf render
# of templates/controller-scope.yaml and templates/ui/api-rbac.yaml), bound to
# THIS run's ServiceAccounts. Cluster-scoped: the caller holds the lock.
cat <<YAML | $K apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: $P-cluster-scope, labels: {logweir.dev/test-owner: $OWNER}}
rules:
  - {apiGroups: ["logweir.dev"], resources: ["trustrosters"], verbs: ["get", "list", "watch"]}
  - {apiGroups: ["logweir.dev"], resources: ["trustrosters/status"], verbs: ["patch"]}
  - {apiGroups: ["logweir.dev"], resources: ["trustpolicies"], verbs: ["list", "watch"]}
  - {apiGroups: ["logweir.dev"], resources: ["trustpolicies/status"], verbs: ["patch"]}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: $P-cluster-scope, labels: {logweir.dev/test-owner: $OWNER}}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: $P-cluster-scope}
subjects: [{kind: ServiceAccount, name: weirkeeper, namespace: $CTL}]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: $P-api-trustpolicies, labels: {logweir.dev/test-owner: $OWNER}}
rules:
  - {apiGroups: ["logweir.dev"], resources: ["trustpolicies"], verbs: ["get", "list"]}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: $P-api-trustpolicies, labels: {logweir.dev/test-owner: $OWNER}}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: $P-api-trustpolicies}
subjects: [{kind: ServiceAccount, name: logweir-api, namespace: $CTL}]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: $P-api-trustroster, labels: {logweir.dev/test-owner: $OWNER}}
rules:
  - {apiGroups: ["logweir.dev"], resources: ["trustrosters"], resourceNames: ["default"], verbs: ["get"]}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: $P-api-trustroster, labels: {logweir.dev/test-owner: $OWNER}}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: $P-api-trustroster}
subjects: [{kind: ServiceAccount, name: logweir-api, namespace: $CTL}]
YAML

# --- kubeconfigs for the two ServiceAccounts (bound, 2h tokens)
server="$($K config view --raw --minify -o jsonpath='{.clusters[0].cluster.server}')"
ca="$($K config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
for sa in logweir-api weirkeeper; do
  token="$($K -n "$CTL" create token "$sa" --duration=2h)"
  umask 077
  cat > "$OUT/kubeconfig-$sa" <<KC
apiVersion: v1
kind: Config
clusters: [{name: dd, cluster: {server: "$server", certificate-authority-data: "$ca"}}]
users: [{name: $sa, user: {token: "$token"}}]
contexts: [{name: p172-$sa, context: {cluster: dd, user: $sa}}]
current-context: p172-$sa
KC
done
$K get ns -l logweir.dev/test-owner=$OWNER -o custom-columns=NAME:.metadata.name,UID:.metadata.uid,OWNER:.metadata.labels.logweir\\.dev/test-owner
