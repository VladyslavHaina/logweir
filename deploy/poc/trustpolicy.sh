#!/usr/bin/env bash
# deploy/poc/trustpolicy.sh — print the TrustPolicy that governs logweir-poc.
#
#   bash deploy/poc/trustpolicy.sh confirmation.pub.pem > trustpolicy.yaml
#   # read it, then, as a cluster administrator (logweir-trust-admin):
#   kubectl --context docker-desktop apply -f trustpolicy.yaml
#
# Two keys, one usage each (CEL rule G8):
#   * EvidenceSigning — the installation's signing key, read from the PUBLIC
#     ConfigMap `logweir-signing-trust` the chart's bootstrap wrote (never from
#     the Secret);
#   * ConsoleConfirmation — the public half of the console's confirmation key
#     (README.md step 5), which is what authorises an Ordinary restore.
# `allowedTargetClusterIds` is the demo TARGET broker's cluster id
# (logweir.values.yaml demoKafka) — the one cluster a restore may write into.
# Nothing here is private material, and the script writes nothing but stdout.
set -euo pipefail
# shellcheck disable=SC1091
. "$(dirname "$0")/versions.env"
CTX="${KUBE_CONTEXT:-docker-desktop}"
confirmation_pub="${1:?usage: trustpolicy.sh <confirmation.pub.pem>}"
TARGET_CLUSTER_ID="${TARGET_CLUSTER_ID:-tQmDMMCERvy6yIB-vuOZCQ}"

signing_id="$(kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" get configmap logweir-signing-trust \
  -o jsonpath='{.data.key-id}')"
signing_pem="$(kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" get configmap logweir-signing-trust \
  -o jsonpath='{.data.signing\.pub\.pem}')"
# THE SIGNER'S WINDOW MUST OPEN BEFORE ITS FIRST SIGNATURE, or every receipt it
# signed earlier reads `Untrusted (SignedOutsideValidity)` once this policy
# governs the namespace. The bootstrap writes the public ConfigMap at install —
# but ALSO at the upgrade that ADOPTS a key which already existed (upgrade
# rehearsal R1: a v0.1.5 hand-provisioned key had signed for minutes before the
# ConfigMap appeared, and the live round saw all four pre-upgrade receipts turn
# Untrusted). So the window opens at the EARLIER of the ConfigMap's and the
# signing Secret's creation (only metadata is printed, never the key). A key
# older than both — an identity restored from backup into a new cluster —
# needs SIGNING_NOT_BEFORE set to its real first use (RFC 3339, UTC).
cm_since="$(kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" get configmap logweir-signing-trust \
  -o jsonpath='{.metadata.creationTimestamp}')"
secret_since="$(kubectl --context "$CTX" -n "$LOGWEIR_NAMESPACE" get secret logweir-signing-key \
  -o jsonpath='{.metadata.creationTimestamp}')"
signing_since="$cm_since"
# Both are UTC RFC 3339 with a `Z`, so the string order is the time order.
if [ -n "$secret_since" ] && [[ "$secret_since" < "$cm_since" ]]; then signing_since="$secret_since"; fi
signing_since="${SIGNING_NOT_BEFORE:-$signing_since}"
confirmation_id="$(openssl pkey -pubin -in "$confirmation_pub" -outform DER | openssl dgst -sha256 | awk '{print $NF}')"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
until="$(date -u -v+1y +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+1 year' +%Y-%m-%dT%H:%M:%SZ)"
indent() { sed 's/^/        /'; }

cat <<YAML
apiVersion: logweir.dev/v1alpha1
kind: TrustPolicy
metadata:
  name: logweir-poc
spec:
  namespaces: [${POC_NAMESPACE}]
  allowedTargetClusterIds: [${TARGET_CLUSTER_ID}]
  keys:
    - keyId: ${signing_id}
      algorithm: p256
      usages: [EvidenceSigning]
      state: Active
      notBefore: "${signing_since}"
      notAfter: "${until}"
      principal:
        id: "install:${LOGWEIR_NAMESPACE}/logweir-signing-key"
        display: Logweir installation signer
      spkiPem: |
$(printf '%s\n' "$signing_pem" | indent)
    - keyId: ${confirmation_id}
      algorithm: ed25519
      usages: [ConsoleConfirmation]
      state: Active
      notBefore: "${now}"
      notAfter: "${until}"
      principal:
        id: "console:${LOGWEIR_NAMESPACE}/logweir-api"
        display: Logweir console confirmation
      spkiPem: |
$(indent < "$confirmation_pub")
YAML
