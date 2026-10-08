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

# The client image is the broker's: the pinned KAFKA_IMAGE when
# `stack-env.sh --kafka` set one, else `apache/kafka:<KAFKA_VERSION>` with
# `.env`'s pin, exactly as the compose file resolves it.
KV=${KAFKA_VERSION:-$(sed -n 's/^KAFKA_VERSION=//p' e2e/compose/.env 2>/dev/null)}
KV=${KV:-3.7.1}
IMG="${KAFKA_IMAGE:-apache/kafka:$KV}"
NET="${LW_E2E_PROJECT}_kafka-net"
CERTS="$PWD/.e2e/auth/$LW_E2E_PROJECT"
DC="docker compose -f e2e/compose/docker-compose.yml"
PASSWORD=logweir-e2e-not-a-secret

# Every container and request a check starts is BOUNDED: `timeout` (GNU) or
# `gtimeout` (Homebrew coreutils) when present, this host's /tmp/lwtimeout
# otherwise. With none of them the checks run unbounded, and say so once.
if command -v timeout >/dev/null 2>&1; then BOUND=timeout
elif command -v gtimeout >/dev/null 2>&1; then BOUND=gtimeout
elif [ -x /tmp/lwtimeout ]; then BOUND=/tmp/lwtimeout
else
  BOUND=""
  echo "profile-smoke: WARNING: no timeout, gtimeout or /tmp/lwtimeout; the checks run UNBOUNDED" >&2
fi
bounded() { # seconds command...
  local s=$1; shift
  if [ -n "$BOUND" ]; then "$BOUND" "$s" "$@"; else "$@"; fi
}

fails=0
# A nonce per run: every topic, key and subject a check creates carries it,
# so a second run on the same stack starts from nothing and a check can never
# pass on what an earlier run left behind.
RUN="smoke$(date +%s)"
pass() { printf 'PASS  %-44s %s\n' "$1" "$2"; }
fail() { printf 'FAIL  %-44s %s\n' "$1" "$2"; fails=$((fails + 1)); }

# in-network client: innet CMD...   (stdout+stderr captured by the caller)
innet() {
  bounded 120 docker run --rm --network "$NET" -v "$CERTS:/certs:ro" \
    --entrypoint bash "$IMG" -c "$*" 2>&1
}
# host-side client: hostside CMD...
hostside() {
  bounded 120 docker run --rm --user 0:0 -v "$CERTS:/certs:ro" \
    --entrypoint bash "$IMG" -c '
      gw=$(getent hosts host.docker.internal | cut -d" " -f1 | head -1)
      [ -n "$gw" ] || { echo "no host.docker.internal" >&2; exit 97; }
      printf "%s\tlocalhost\n" "$gw" > /etc/hosts
      '"$*" 2>&1
}
T=/opt/kafka/bin
FAST="request.timeout.ms=10000\ndefault.api.timeout.ms=15000\nsocket.connection.setup.timeout.ms=5000\nsocket.connection.setup.timeout.max.ms=5000"

smoke_auth() {
  local plain scram mtls
  plain=$(lw_e2e_port LOGWEIR_E2E_AUTH_PLAIN_PORT)
  scram=$(lw_e2e_port LOGWEIR_E2E_AUTH_SCRAM256_PORT)
  mtls=$(lw_e2e_port LOGWEIR_E2E_AUTH_MTLS_PORT)
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
  # SCRAM-SHA-256 over TLS, on the same SASL_SSL listener (PROD-01.3) -------------
  cfg="security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-256\nssl.truststore.type=PEM\nssl.truststore.location=/certs/ca.pem\n$FAST"
  out=$(hostside "printf '$cfg\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"logweir\" password=\"$PASSWORD\";\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$plain --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -qx logweir.scratch; then pass auth.scram-sha-256-tls "localhost:$plain lists the marker topic over SCRAM-SHA-256/TLS"; else fail auth.scram-sha-256-tls "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
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
  # PROD-01.3: a client certificate signed by nobody the broker trusts.
  out=$(hostside "cat /certs/wrong-client.key /certs/wrong-client.pem > /tmp/k; printf '$cfg\nssl.keystore.type=PEM\nssl.keystore.location=/tmp/k\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$mtls --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -q -i 'SSL handshake failed\|SSLHandshakeException\|bad_certificate\|certificate_unknown\|unknown_ca'; then pass auth.mtls.wrong-client-cert-refused "handshake refused for an untrusted client certificate"; else fail auth.mtls.wrong-client-cert-refused "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
}

smoke_cluster3() {
  local out p1
  p1=$(lw_e2e_port LOGWEIR_E2E_C3_1_PORT)
  out=$(innet "$T/kafka-metadata-quorum.sh --bootstrap-server kafka-c3-1:9094 describe --status")
  if printf '%s' "$out" | grep -q 'CurrentVoters:.*"id": *1.*"id": *2.*"id": *3\|CurrentVoters:.*\[1,2,3\]'; then pass cluster3.quorum "$(printf '%s' "$out" | grep -E 'LeaderId|CurrentVoters' | tr -s ' ' | tr '\n' ' ' | cut -c1-160)"; else fail cluster3.quorum "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
  out=$(innet "$T/kafka-topics.sh --bootstrap-server kafka-c3-1:9094 --create --topic $RUN-rf3 --partitions 3 && $T/kafka-topics.sh --bootstrap-server kafka-c3-2:9094 --describe --topic $RUN-rf3")
  if printf '%s' "$out" | grep -q 'ReplicationFactor: 3' && [ "$(printf '%s' "$out" | grep -c 'Isr: [0-9],[0-9],[0-9]')" = 3 ]; then pass cluster3.rf3-isr3 "default RF 3, every partition's ISR has 3 replicas"; else fail cluster3.rf3-isr3 "$(printf '%s' "$out" | tail -4 | tr '\n' ' ')"; fi
  out=$(innet "$T/kafka-topics.sh --bootstrap-server kafka-c3-1:9094 --describe --topic __consumer_offsets 2>/dev/null | head -1; $T/kafka-configs.sh --bootstrap-server kafka-c3-1:9094 --entity-type brokers --entity-name 1 --describe --all | grep -E 'min.insync.replicas='")
  if printf '%s' "$out" | grep -q 'min.insync.replicas=2'; then pass cluster3.min-isr-2 "broker default min.insync.replicas=2"; else fail cluster3.min-isr-2 "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # Host side: bootstrap on node 1's published port; the produce goes to every
  # partition leader through ITS advertised localhost:<port>, acks=all.
  out=$(hostside "seq 1 30 | $T/kafka-console-producer.sh --bootstrap-server localhost:$p1 --topic $RUN-rf3 --producer-property acks=all && $T/kafka-get-offsets.sh --bootstrap-server localhost:$p1 --topic $RUN-rf3")
  total=$(printf '%s' "$out" | awk -F: -v t="$RUN-rf3" '$1 == t {s+=$3} END{print s+0}')
  if [ "$total" = 30 ]; then pass cluster3.host-produce "30 records acked by all ISRs via localhost:$p1 and the two other advertised ports"; else fail cluster3.host-produce "end offsets sum $total: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # Negative control: a topic with RF 4 cannot exist on three brokers.
  out=$(innet "$T/kafka-topics.sh --bootstrap-server kafka-c3-1:9094 --create --topic $RUN-rf4 --replication-factor 4 --partitions 1")
  if printf '%s' "$out" | grep -q -i 'InvalidReplicationFactor\|larger than available brokers\|Replication factor: 4 larger'; then pass cluster3.rf4-refused "three brokers refuse RF 4"; else fail cluster3.rf4-refused "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
}

smoke_cluster2() {
  local out id1 id2 p2
  p2=$(lw_e2e_port LOGWEIR_E2E_CLUSTER2_PORT)
  id1=$(innet "$T/kafka-cluster.sh cluster-id --bootstrap-server kafka-broker-1:9094" | sed -n 's/^Cluster ID: *//p')
  id2=$(innet "$T/kafka-cluster.sh cluster-id --bootstrap-server kafka-cluster2:9094" | sed -n 's/^Cluster ID: *//p')
  if [ -n "$id1" ] && [ -n "$id2" ] && [ "$id1" != "$id2" ]; then pass cluster2.distinct-cluster-id "kafka-broker-1 $id1, kafka-cluster2 $id2"; else fail cluster2.distinct-cluster-id "ids '$id1' / '$id2'"; fi
  out=$(hostside "$T/kafka-topics.sh --bootstrap-server localhost:$p2 --list")
  if printf '%s' "$out" | grep -qx logweir.scratch && ! printf '%s' "$out" | grep -qx orders; then pass cluster2.host-side "localhost:$p2 serves ITS topics: the marker, and not kafka-broker-1's orders"; else fail cluster2.host-side "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
}

smoke_streams() {
  local out n last
  # A word no earlier run produced, so every count below is THIS run's.
  n="$RUN"
  consume() { # the output topic from the start, waiting up to $1 ms for more
    innet "$T/kafka-console-consumer.sh --bootstrap-server kafka-broker-1:9094 --topic streams-wordcount-output --from-beginning --timeout-ms $1 --property print.key=true --property key.separator== --value-deserializer org.apache.kafka.common.serialization.LongDeserializer 2>/dev/null"
  }
  produce() { # lines...
    innet "printf '%s\\n' $* | $T/kafka-console-producer.sh --bootstrap-server kafka-broker-1:9094 --topic streams-plaintext-input"
  }
  # The output is a changelog: the LAST record per key is the count.
  latest() { printf '%s\n' "$1" | grep "^$n=" | tail -1; }

  # 1. Two lines through the RUNNING application: exactly 2 — a dead app says
  #    nothing, one that double-counts says more.
  produce "'$n alpha'" "'$n beta'" > /dev/null
  last=$(latest "$(consume 30000)")
  if [ "$last" = "$n=2" ]; then pass streams.wordcount "$last, exactly the two lines produced"; else fail streams.wordcount "latest '$last', want $n=2"; fi

  # 2. NEGATIVE CONTROL: the application STOPPED, a third line counts NOTHING.
  #    Were the counts written by anything but the application, the third line
  #    would still be counted here, and this fails.
  bounded 120 $DC --profile streams stop streams-wordcount > /dev/null 2>&1
  produce "'$n gamma'" > /dev/null
  last=$(latest "$(consume 10000)")
  if [ "$last" = "$n=2" ]; then pass streams.stopped-app-counts-nothing "a line produced while the app was stopped left the count at $last"; else fail streams.stopped-app-counts-nothing "latest '$last' with the app stopped, want $n=2"; fi

  # 3. Restarted, the application catches up on the line it missed: 3. (An
  #    app that lost its committed position under auto.offset.reset=latest, or
  #    never came back, stays at 2.)
  bounded 120 $DC --profile streams start streams-wordcount > /dev/null 2>&1
  for _ in $(seq 1 30); do
    [ "$(bounded 30 $DC --profile streams ps --format '{{.Health}}' streams-wordcount 2>/dev/null)" = healthy ] && break
    sleep 5
  done
  last=$(latest "$(consume 30000)")
  if [ "$last" = "$n=3" ]; then pass streams.restart-catches-up "after a restart the missed line is counted: $last"; else fail streams.restart-catches-up "latest '$last' after the restart, want $n=3"; fi

  out=$(innet "$T/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9094 --describe --group logweir-e2e-wordcount --state; $T/kafka-topics.sh --bootstrap-server kafka-broker-1:9094 --list | grep '^logweir-e2e-wordcount-'")
  if printf '%s' "$out" | grep -q -w 'Stable' && printf '%s' "$out" | grep -q 'changelog' && printf '%s' "$out" | grep -q 'repartition'; then pass streams.group-and-state "group Stable; internal topics: $(printf '%s' "$out" | grep '^logweir-e2e-wordcount-' | tr '\n' ' ')"; else fail streams.group-and-state "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
}

AWSCLI=amazon/aws-cli:2.37.5@sha256:fb7ccfc7b4e3a05017e6c9ded4ba959b99af622130e211fb976c68c2f4d3f224
# s3 ENDPOINT SECRET args...: aws-cli as the fixture user (or with a wrong
# secret), on the stack network so `objectstore` and `host.docker.internal`
# both resolve.
s3() {
  local ep=$1 sk=$2; shift 2
  bounded 60 docker run --rm --network "$NET" -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY="$sk" \
    -e AWS_DEFAULT_REGION=us-east-1 -e AWS_EC2_METADATA_DISABLED=true -e AWS_MAX_ATTEMPTS=1 \
    --entrypoint bash "$AWSCLI" -c "printf 'first\\n' > /tmp/a; printf 'second\\n' > /tmp/b; aws --endpoint-url $ep s3api $*" 2>&1
}

smoke_objectstore() {
  local out in="http://objectstore:8333" host until vid md5
  host="http://host.docker.internal:$(lw_e2e_port LOGWEIR_E2E_OBJSTORE_PORT)"
  out=$(s3 "$host" minioadmin list-buckets --query 'Buckets[].Name' --output text)
  if printf '%s' "$out" | grep -q kafka-backups-locked && printf '%s' "$out" | grep -q kafka-backups-2 && printf '%s' "$out" | grep -q logweir-evidence; then pass objectstore.buckets "via the PUBLISHED port ($host): $(printf '%s' "$out" | tr -s '\t\n' ' ')"; else fail objectstore.buckets "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  out=$(s3 "$in" not-the-secret list-objects-v2 --bucket kafka-backups)
  if printf '%s' "$out" | grep -q 'SignatureDoesNotMatch\|AccessDenied'; then pass objectstore.wrong-secret-refused "$(printf '%s' "$out" | grep -o 'SignatureDoesNotMatch\|AccessDenied' | head -1)"; else fail objectstore.wrong-secret-refused "$(printf '%s' "$out" | tail -1)"; fi
  # First create must SUCCEED (ETag) and the second be refused: a store that
  # refused everything, or a key an earlier run left, would not pass.
  out=$(s3 "$in" minioadmin "put-object --bucket kafka-backups --key $RUN/claim --body /tmp/a --if-none-match '*' && echo FIRST-CREATED && aws --endpoint-url $in s3api put-object --bucket kafka-backups --key $RUN/claim --body /tmp/b --if-none-match '*'")
  if printf '%s' "$out" | grep -q 'FIRST-CREATED' && printf '%s' "$out" | grep -q 'PreconditionFailed'; then pass objectstore.if-none-match "first create 200, second 412 PreconditionFailed"; else fail objectstore.if-none-match "$(printf '%s' "$out" | tail -1)"; fi
  until=$(date -u -v+1d +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+1 day' +%Y-%m-%dT%H:%M:%SZ)
  # Object Lock puts need Content-MD5; /tmp/a inside s3() holds "first\n".
  md5=$(printf 'first\n' | openssl dgst -md5 -binary | openssl base64)
  out=$(s3 "$in" minioadmin "put-object --bucket kafka-backups-locked --key $RUN/locked --body /tmp/a --content-md5 $md5 --object-lock-mode GOVERNANCE --object-lock-retain-until-date $until --query VersionId --output text")
  vid=$(printf '%s' "$out" | tail -1 | tr -d '[:space:]')
  out=$(s3 "$in" minioadmin get-object-retention --bucket kafka-backups-locked --key "$RUN/locked" --output text)
  if printf '%s' "$out" | grep -q GOVERNANCE && printf '%s' "$out" | grep -q "${until%Z}"; then pass objectstore.lock-retention-readback "$(printf '%s' "$out" | tr -s '\t' ' ')"; else fail objectstore.lock-retention-readback "vid '$vid': $(printf '%s' "$out" | tail -1)"; fi
  out=$(s3 "$in" minioadmin delete-object --bucket kafka-backups-locked --key "$RUN/locked" --version-id "$vid")
  if printf '%s' "$out" | grep -q 'AccessDenied\|ObjectLocked\|InvalidRequest'; then pass objectstore.lock-delete-refused "a retained version is not deletable"; else fail objectstore.lock-delete-refused "$(printf '%s' "$out" | tail -1)"; fi
  out=$(s3 "$in" minioadmin get-object --bucket kafka-backups-locked --key "$RUN/locked" --version-id "$vid" /tmp/got --query VersionId --output text)
  if [ "$(printf '%s' "$out" | tail -1 | tr -d '[:space:]')" = "$vid" ]; then pass objectstore.read-by-version-id "GET ?versionId=$vid"; else fail objectstore.read-by-version-id "$(printf '%s' "$out" | tail -1)"; fi
  s3 "$in" minioadmin delete-object --bucket kafka-backups-locked --key "$RUN/locked" --version-id "$vid" --bypass-governance-retention > /dev/null
}

smoke_registry() {
  local out n base id
  base="http://localhost:$(lw_e2e_port LOGWEIR_E2E_REGISTRY_PORT)"
  n="$RUN"
  # Host side, straight at the PUBLISHED port.
  out=$(bounded 30 curl -s -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data '{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}' \
    "$base/subjects/$n-value/versions" 2>&1)
  id=$(printf '%s' "$out" | sed -n 's/.*"id": *\([0-9]*\).*/\1/p')
  if [ -n "$id" ]; then pass registry.register "$n-value -> schema id $id at $base"; else fail registry.register "$out"; fi
  out=$(bounded 30 curl -s "$base/schemas/ids/$id" 2>&1)
  if printf '%s' "$out" | grep -q 'Order'; then pass registry.read-by-id "GET /schemas/ids/$id returns the schema"; else fail registry.read-by-id "$out"; fi
  # Negative control: BACKWARD refuses changing id's type.
  out=$(bounded 30 curl -s -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    --data '{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}' \
    "$base/subjects/$n-value/versions" 2>&1)
  if [ "$out" = 409 ]; then pass registry.incompatible-refused "HTTP 409 for an incompatible second version"; else fail registry.incompatible-refused "HTTP $out"; fi
  # The state lives in Kafka: the registration is a record in `_schemas`.
  out=$(innet "$T/kafka-console-consumer.sh --bootstrap-server kafka-broker-1:9094 --topic _schemas --from-beginning --timeout-ms 15000 --property print.key=true 2>/dev/null")
  if printf '%s' "$out" | grep -q "$n-value"; then pass registry.state-in-kafka "_schemas holds the $n-value registration"; else fail registry.state-in-kafka "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
}
smoke_acl() {
  local p s out cfg rc
  p=$(lw_e2e_port LOGWEIR_E2E_ACL_PORT)
  s=$(lw_e2e_port LOGWEIR_E2E_ACL_SASL_PORT)
  # The authorizer is ON: without one, every ACL call answers SecurityDisabled.
  out=$(innet "$T/kafka-acls.sh --bootstrap-server kafka-acl:9094 --list; echo rc=\$?")
  rc=$(printf '%s' "$out" | sed -n 's/^rc=//p' | tail -1)
  if [ "$rc" = 0 ] && ! printf '%s' "$out" | grep -q -i 'SecurityDisabled\|No Authorizer'; then pass acl.authorizer "kafka-acls --list answers (StandardAuthorizer)"; else fail acl.authorizer "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # One topic the restricted principal may Read and Describe, and nothing else.
  innet "$T/kafka-topics.sh --bootstrap-server kafka-acl:9094 --create --topic $RUN-acl --partitions 1 --replication-factor 1 --config retention.ms=3600000 && $T/kafka-acls.sh --bootstrap-server kafka-acl:9094 --add --allow-principal User:logweir --operation Read --operation Describe --topic $RUN-acl" >/dev/null
  cfg="security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\n$FAST"
  # kafka-get-offsets needs only Describe. Not `kafka-topics --describe`: it
  # also reads the topic's configuration, so it needs DescribeConfigs too
  # (measured on 3.7.1: TopicAuthorizationException).
  out=$(hostside "printf '$cfg\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"logweir\" password=\"$PASSWORD\";\n' > /tmp/c; $T/kafka-get-offsets.sh --bootstrap-server localhost:$s --command-config /tmp/c --topic $RUN-acl")
  if printf '%s' "$out" | grep -q "^$RUN-acl:0:"; then pass acl.restricted-describe "logweir on localhost:$s reads $RUN-acl's offsets (its ACL allows Describe): $(printf '%s' "$out" | grep -m1 "^$RUN-acl:0:")"; else fail acl.restricted-describe "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # Negative control: the same principal may NOT DescribeConfigs it.
  out=$(hostside "printf '$cfg\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"logweir\" password=\"$PASSWORD\";\n' > /tmp/c; $T/kafka-configs.sh --bootstrap-server localhost:$s --command-config /tmp/c --describe --entity-type topics --entity-name $RUN-acl")
  if printf '%s' "$out" | grep -q -i 'TopicAuthorizationException\|Authorization failed\|not authorized'; then pass acl.restricted-describe-configs-denied "$(printf '%s' "$out" | grep -i -m1 -o 'TopicAuthorizationException[^.]*\|Authorization failed[^.]*')"; else fail acl.restricted-describe-configs-denied "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # ...while the super user on the PLAINTEXT port may.
  out=$(hostside "$T/kafka-configs.sh --bootstrap-server localhost:$p --describe --entity-type topics --entity-name $RUN-acl")
  if printf '%s' "$out" | grep -q 'retention.ms=3600000'; then pass acl.super-user-describe-configs "ANONYMOUS on localhost:$p reads retention.ms=3600000"; else fail acl.super-user-describe-configs "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  out=$(hostside "printf '$cfg\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"logweir\" password=\"wrong-password\";\n' > /tmp/c; $T/kafka-topics.sh --bootstrap-server localhost:$s --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -q -i 'authentication failed\|SaslAuthenticationException'; then pass acl.scram.wrong-password-refused "refused"; else fail acl.scram.wrong-password-refused "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  innet "$T/kafka-acls.sh --bootstrap-server kafka-acl:9094 --remove --force --allow-principal User:logweir --operation Read --operation Describe --topic $RUN-acl; $T/kafka-topics.sh --bootstrap-server kafka-acl:9094 --delete --topic $RUN-acl" >/dev/null
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
    registry) smoke_registry ;;
    acl) smoke_acl ;;
    *) fail "$p" "no smoke defined for profile $p" ;;
  esac
done
echo "# $fails failure(s)"
[ "$fails" -eq 0 ]
