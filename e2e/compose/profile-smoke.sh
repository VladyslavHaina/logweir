#!/usr/bin/env bash
# Smoke-run the optional compose profiles (PROD-01.5) of the stack this shell
# addresses:
#
#   eval "$(e2e/compose/stack-env.sh --slot 2 --profiles auth,cluster3)"
#   just e2e-up && e2e/compose/profile-smoke.sh && just e2e-down
#
# With no argument it smokes every profile in COMPOSE_PROFILES; name profiles
# to smoke fewer. Every check prints PASS or FAIL with the evidence, and every
# positive check has a NEGATIVE CONTROL beside it (a wrong password, a missing
# client certificate, a wrong CA…) that must be refused — a listener that let
# everything in would pass the positive half alone. Exit 1 if anything failed.
#
# Two vantage points, both through throwaway containers:
#   in-network  a client on `<project>_kafka-net`, dialling service names;
#   host-side   a client whose /etc/hosts maps `localhost` to the Docker host
#               gateway (the trick e2e/fixtures/engine-docker.sh uses), so it
#               dials the PUBLISHED ports and follows the broker's
#               `localhost:<port>` advertisements exactly as a host process
#               would.
set -uo pipefail
cd "$(dirname "$0")/../.."
# shellcheck source=e2e/compose/stack-lib.sh
. e2e/compose/stack-lib.sh
lw_e2e_check_coherent || exit 1

KV=${KAFKA_VERSION:-$(sed -n 's/^KAFKA_VERSION=//p' e2e/compose/.env 2>/dev/null)}
KV=${KV:-3.7.1}
IMG="apache/kafka:$KV"
NET="${LW_E2E_PROJECT}_kafka-net"
CERTS="$PWD/.e2e/auth/$LW_E2E_PROJECT"
port() { local v=$1 d=$2; eval "printf '%s' \"\${$v:-$d}\""; }
PASSWORD=logweir-e2e-not-a-secret

fails=0
pass() { printf 'PASS  %-44s %s\n' "$1" "$2"; }
fail() { printf 'FAIL  %-44s %s\n' "$1" "$2"; fails=$((fails + 1)); }

# in-network client: innet CMD...   (stdout+stderr captured by the caller)
innet() {
  /tmp/lwtimeout 120 docker run --rm --network "$NET" -v "$CERTS:/certs:ro" \
    --entrypoint bash "$IMG" -c "$*" 2>&1
}
# host-side client: hostside CMD...
hostside() {
  /tmp/lwtimeout 120 docker run --rm --user 0:0 -v "$CERTS:/certs:ro" \
    --entrypoint bash "$IMG" -c '
      gw=$(getent hosts host.docker.internal | cut -d" " -f1 | head -1)
      [ -n "$gw" ] || { echo "no host.docker.internal" >&2; exit 97; }
      printf "%s\tlocalhost\n" "$gw" > /etc/hosts
      '"$*" 2>&1
}
T=/opt/kafka/bin
FAST="request.timeout.ms=10000\ndefault.api.timeout.ms=15000\nsocket.connection.setup.timeout.max.ms=5000"

smoke_auth() {
  local plain scram mtls
  plain=$(port LOGWEIR_E2E_AUTH_PLAIN_PORT 9102)
  scram=$(port LOGWEIR_E2E_AUTH_SCRAM256_PORT 9103)
  mtls=$(port LOGWEIR_E2E_AUTH_MTLS_PORT 9104)
  [ -s "$CERTS/ca.pem" ] || { fail auth.certs "no $CERTS/ca.pem — is the auth profile up?"; return; }
  pass auth.certs "$(ls "$CERTS" | tr '\n' ' ')"
  local cfg out
  # PLAIN over TLS -------------------------------------------------------------
  cfg="security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nssl.truststore.type=PEM\nssl.truststore.location=/certs/ca.pem\n$FAST"
  out=$(hostside "printf '$cfg\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"logweir\" password=\"$PASSWORD\";\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$plain --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -qx orders; then pass auth.plain-tls "localhost:$plain lists orders"; else fail auth.plain-tls "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  out=$(hostside "printf '$cfg\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"logweir\" password=\"wrong-password\";\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$plain --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -q -i 'authentication failed\|SaslAuthenticationException'; then pass auth.plain-tls.wrong-password-refused "$(printf '%s' "$out" | grep -i -m1 -o 'Authentication failed[^.]*')"; else fail auth.plain-tls.wrong-password-refused "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  out=$(hostside "printf '$(printf '%s' "$cfg" | sed 's|/certs/ca.pem|/certs/wrong-ca.pem|')\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"logweir\" password=\"$PASSWORD\";\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$plain --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -q -i 'SSL handshake failed\|SSLHandshakeException\|PKIX'; then pass auth.plain-tls.wrong-ca-refused "TLS handshake refused"; else fail auth.plain-tls.wrong-ca-refused "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # SCRAM-SHA-256 ------------------------------------------------------------------
  cfg="security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-256\n$FAST"
  out=$(hostside "printf '$cfg\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"logweir\" password=\"$PASSWORD\";\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$scram --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -qx orders; then pass auth.scram-sha-256 "localhost:$scram lists orders"; else fail auth.scram-sha-256 "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  out=$(hostside "printf '$cfg\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"logweir\" password=\"wrong-password\";\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$scram --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -q -i 'authentication failed\|SaslAuthenticationException'; then pass auth.scram-sha-256.wrong-password-refused "refused"; else fail auth.scram-sha-256.wrong-password-refused "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # mTLS ----------------------------------------------------------------------------
  cfg="security.protocol=SSL\nssl.truststore.type=PEM\nssl.truststore.location=/certs/ca.pem\n$FAST"
  out=$(hostside "printf '$cfg\nssl.keystore.type=PEM\nssl.keystore.location=/certs/client.keystore.pem\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$mtls --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -qx orders; then pass auth.mtls "localhost:$mtls lists orders with client.pem"; else fail auth.mtls "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  out=$(hostside "printf '$cfg\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$mtls --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -q -i 'SSL handshake failed\|SSLHandshakeException\|bad_certificate\|certificate_required'; then pass auth.mtls.no-client-cert-refused "handshake refused without a client certificate"; else fail auth.mtls.no-client-cert-refused "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
}

smoke_cluster3() {
  local out p1
  p1=$(port LOGWEIR_E2E_C3_1_PORT 9112)
  out=$(innet "$T/kafka-metadata-quorum.sh --bootstrap-server kafka-c3-1:9094 describe --status")
  if printf '%s' "$out" | grep -q 'CurrentVoters:.*"id": *1.*"id": *2.*"id": *3\|CurrentVoters:.*\[1,2,3\]'; then pass cluster3.quorum "$(printf '%s' "$out" | grep -E 'LeaderId|CurrentVoters' | tr -s ' ' | tr '\n' ' ' | cut -c1-160)"; else fail cluster3.quorum "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
  out=$(innet "$T/kafka-topics.sh --bootstrap-server kafka-c3-1:9094 --create --if-not-exists --topic smoke-rf3 --partitions 3 && $T/kafka-topics.sh --bootstrap-server kafka-c3-2:9094 --describe --topic smoke-rf3")
  if printf '%s' "$out" | grep -q 'ReplicationFactor: 3' && [ "$(printf '%s' "$out" | grep -c 'Isr: [0-9],[0-9],[0-9]')" = 3 ]; then pass cluster3.rf3-isr3 "default RF 3, every partition's ISR has 3 replicas"; else fail cluster3.rf3-isr3 "$(printf '%s' "$out" | tail -4 | tr '\n' ' ')"; fi
  out=$(innet "$T/kafka-topics.sh --bootstrap-server kafka-c3-1:9094 --describe --topic __consumer_offsets 2>/dev/null | head -1; $T/kafka-configs.sh --bootstrap-server kafka-c3-1:9094 --entity-type brokers --entity-name 1 --describe --all | grep -E 'min.insync.replicas='")
  if printf '%s' "$out" | grep -q 'min.insync.replicas=2'; then pass cluster3.min-isr-2 "broker default min.insync.replicas=2"; else fail cluster3.min-isr-2 "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # Host side: bootstrap on node 1's published port; the produce goes to every
  # partition leader through ITS advertised localhost:<port>, acks=all.
  out=$(hostside "seq 1 30 | $T/kafka-console-producer.sh --bootstrap-server localhost:$p1 --topic smoke-rf3 --producer-property acks=all && $T/kafka-get-offsets.sh --bootstrap-server localhost:$p1 --topic smoke-rf3")
  total=$(printf '%s' "$out" | awk -F: '/^smoke-rf3:/{s+=$3} END{print s+0}')
  if [ "$total" = 30 ]; then pass cluster3.host-produce "30 records acked by all ISRs via localhost:$p1 and the two other advertised ports"; else fail cluster3.host-produce "end offsets sum $total: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # Negative control: a topic with RF 4 cannot exist on three brokers.
  out=$(innet "$T/kafka-topics.sh --bootstrap-server kafka-c3-1:9094 --create --topic smoke-rf4 --replication-factor 4 --partitions 1")
  if printf '%s' "$out" | grep -q -i 'InvalidReplicationFactor\|larger than available brokers\|Replication factor: 4 larger'; then pass cluster3.rf4-refused "three brokers refuse RF 4"; else fail cluster3.rf4-refused "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
}

smoke_cluster2() {
  local out id1 id2 p2
  p2=$(port LOGWEIR_E2E_CLUSTER2_PORT 9122)
  id1=$(innet "$T/kafka-cluster.sh cluster-id --bootstrap-server kafka-broker-1:9094" | sed -n 's/^Cluster ID: *//p')
  id2=$(innet "$T/kafka-cluster.sh cluster-id --bootstrap-server kafka-cluster2:9094" | sed -n 's/^Cluster ID: *//p')
  if [ -n "$id1" ] && [ -n "$id2" ] && [ "$id1" != "$id2" ]; then pass cluster2.distinct-cluster-id "kafka-broker-1 $id1, kafka-cluster2 $id2"; else fail cluster2.distinct-cluster-id "ids '$id1' / '$id2'"; fi
  out=$(hostside "$T/kafka-topics.sh --bootstrap-server localhost:$p2 --list")
  if printf '%s' "$out" | grep -qx logweir.scratch && ! printf '%s' "$out" | grep -qx orders; then pass cluster2.host-side "localhost:$p2 serves ITS topics: the marker, and not kafka-broker-1's orders"; else fail cluster2.host-side "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
}

smoke_streams() {
  local out n last2 last1 ghost
  # A word no earlier run produced, so the counts below are THIS run's.
  n="smoke$(date +%s)"
  out=$(innet "printf '$n alpha\n$n alpha beta\n' | $T/kafka-console-producer.sh --bootstrap-server kafka-broker-1:9094 --topic streams-plaintext-input && $T/kafka-console-consumer.sh --bootstrap-server kafka-broker-1:9094 --topic streams-wordcount-output --from-beginning --timeout-ms 30000 --property print.key=true --property key.separator== --value-deserializer org.apache.kafka.common.serialization.LongDeserializer 2>/dev/null")
  # The output is a changelog: the LAST value per key is the count.
  last2=$(printf '%s\n' "$out" | grep "^$n=" | tail -1)
  last1=$(printf '%s\n' "$out" | grep "^beta=" | tail -1)
  ghost=$(printf '%s\n' "$out" | grep -c "^${n}x=")
  if [ "$last2" = "$n=2" ] && [ -n "$last1" ] && [ "$ghost" = 0 ]; then pass streams.wordcount "$last2 (latest), $last1; no count for the never-produced ${n}x"; else fail streams.wordcount "latest '$last2', beta '$last1', ghost $ghost: $(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
  out=$(innet "$T/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9094 --describe --group logweir-e2e-wordcount --state; $T/kafka-topics.sh --bootstrap-server kafka-broker-1:9094 --list | grep '^logweir-e2e-wordcount-'")
  if printf '%s' "$out" | grep -q -w 'Stable' && printf '%s' "$out" | grep -q 'changelog' && printf '%s' "$out" | grep -q 'repartition'; then pass streams.group-and-state "group Stable; internal topics: $(printf '%s' "$out" | grep '^logweir-e2e-wordcount-' | tr '\n' ' ')"; else fail streams.group-and-state "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
}

AWSCLI=amazon/aws-cli:2.37.5@sha256:fb7ccfc7b4e3a05017e6c9ded4ba959b99af622130e211fb976c68c2f4d3f224
# s3 ENDPOINT SECRET args...: aws-cli as the fixture user (or with a wrong
# secret), on the stack network so `objectstore` and `host.docker.internal`
# both resolve.
s3() {
  local ep=$1 sk=$2; shift 2
  /tmp/lwtimeout 60 docker run --rm --network "$NET" -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY="$sk" \
    -e AWS_DEFAULT_REGION=us-east-1 -e AWS_EC2_METADATA_DISABLED=true -e AWS_MAX_ATTEMPTS=1 \
    --entrypoint bash "$AWSCLI" -c "printf 'first\\n' > /tmp/a; printf 'second\\n' > /tmp/b; aws --endpoint-url $ep s3api $*" 2>&1
}

smoke_objectstore() {
  local out in="http://objectstore:8333" host until vid md5
  host="http://host.docker.internal:$(port LOGWEIR_E2E_OBJSTORE_PORT 9130)"
  out=$(s3 "$host" minioadmin list-buckets --query 'Buckets[].Name' --output text)
  if printf '%s' "$out" | grep -q kafka-backups-locked && printf '%s' "$out" | grep -q kafka-backups-2 && printf '%s' "$out" | grep -q logweir-evidence; then pass objectstore.buckets "via the PUBLISHED port ($host): $(printf '%s' "$out" | tr -s '\t\n' ' ')"; else fail objectstore.buckets "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  out=$(s3 "$in" not-the-secret list-objects-v2 --bucket kafka-backups)
  if printf '%s' "$out" | grep -q 'SignatureDoesNotMatch\|AccessDenied'; then pass objectstore.wrong-secret-refused "$(printf '%s' "$out" | grep -o 'SignatureDoesNotMatch\|AccessDenied' | head -1)"; else fail objectstore.wrong-secret-refused "$(printf '%s' "$out" | tail -1)"; fi
  out=$(s3 "$in" minioadmin "put-object --bucket kafka-backups --key smoke/claim --body /tmp/a --if-none-match '*' && aws --endpoint-url $in s3api put-object --bucket kafka-backups --key smoke/claim --body /tmp/b --if-none-match '*'")
  if printf '%s' "$out" | grep -q 'PreconditionFailed'; then pass objectstore.if-none-match "second create refused 412 PreconditionFailed"; else fail objectstore.if-none-match "$(printf '%s' "$out" | tail -1)"; fi
  until=$(date -u -v+1d +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+1 day' +%Y-%m-%dT%H:%M:%SZ)
  # Object Lock puts need Content-MD5; /tmp/a inside s3() holds "first\n".
  md5=$(printf 'first\n' | openssl dgst -md5 -binary | openssl base64)
  out=$(s3 "$in" minioadmin "put-object --bucket kafka-backups-locked --key smoke/locked --body /tmp/a --content-md5 $md5 --object-lock-mode GOVERNANCE --object-lock-retain-until-date $until --query VersionId --output text")
  vid=$(printf '%s' "$out" | tail -1 | tr -d '[:space:]')
  out=$(s3 "$in" minioadmin get-object-retention --bucket kafka-backups-locked --key smoke/locked --output text)
  if printf '%s' "$out" | grep -q GOVERNANCE && printf '%s' "$out" | grep -q "${until%Z}"; then pass objectstore.lock-retention-readback "$(printf '%s' "$out" | tr -s '\t' ' ')"; else fail objectstore.lock-retention-readback "vid '$vid': $(printf '%s' "$out" | tail -1)"; fi
  out=$(s3 "$in" minioadmin delete-object --bucket kafka-backups-locked --key smoke/locked --version-id "$vid")
  if printf '%s' "$out" | grep -q 'AccessDenied\|ObjectLocked\|InvalidRequest'; then pass objectstore.lock-delete-refused "a retained version is not deletable"; else fail objectstore.lock-delete-refused "$(printf '%s' "$out" | tail -1)"; fi
  out=$(s3 "$in" minioadmin get-object --bucket kafka-backups-locked --key smoke/locked --version-id "$vid" /tmp/got --query VersionId --output text)
  if [ "$(printf '%s' "$out" | tail -1 | tr -d '[:space:]')" = "$vid" ]; then pass objectstore.read-by-version-id "GET ?versionId=$vid"; else fail objectstore.read-by-version-id "$(printf '%s' "$out" | tail -1)"; fi
  s3 "$in" minioadmin delete-object --bucket kafka-backups-locked --key smoke/locked --version-id "$vid" --bypass-governance-retention > /dev/null
}

profiles=${*:-$(printf '%s' "${COMPOSE_PROFILES:-}" | tr ',' ' ')}
[ -n "$profiles" ] || { echo "profile-smoke: no profile named and COMPOSE_PROFILES is empty" >&2; exit 2; }
echo "# profile smoke: project $LW_E2E_PROJECT, broker image $IMG, profiles: $profiles ($(date -u +%FT%TZ))"
for p in $profiles; do
  case "$p" in
    auth) smoke_auth ;;
    cluster3) smoke_cluster3 ;;
    cluster2) smoke_cluster2 ;;
    streams) smoke_streams ;;
    objectstore) smoke_objectstore ;;
    *) fail "$p" "no smoke defined for profile $p" ;;
  esac
done
echo "# $fails failure(s)"
[ "$fails" -eq 0 ]
