#!/usr/bin/env bash
# Smoke-run the optional compose profiles (PROD-01.5) of the stack this shell
# addresses:
#
#   eval "$(e2e/compose/stack-env.sh --slot 2 --profiles auth,cluster3)"
#   just e2e-up && e2e/compose/profile-smoke.sh && just e2e-down
#
# With no argument it smokes every profile in COMPOSE_PROFILES; name profiles
# to smoke fewer. `groups` is not a profile: name it to smoke the groups
# fixture (`e2e/compose/groups.sh`, PROD-04.0d), which it brings up itself and
# leaves up. Every check prints PASS or FAIL with the evidence, and every
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

# PROD-04.0 §3.9's visibility state on `acl` (PROD-04.0d): `groups.sh
# visibility apply`, then what the restricted principal sees, then the
# record's negative control, then `remove` — which returns the profile to the
# state smoke_acl and FX-4's rows start from.
smoke_acl_visibility() {
  local s out jaas
  s=$(lw_e2e_port LOGWEIR_E2E_ACL_SASL_PORT)
  jaas="printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\n$FAST\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"logweir\" password=\"$PASSWORD\";\n' > /tmp/c"
  as_logweir() { hostside "$jaas; $*"; }
  out=$(bounded 600 bash e2e/compose/groups.sh visibility apply 2>&1)
  if printf '%s' "$out" | grep -q 'User:ops' && printf '%s' "$out" | grep -q 'pa-hidden'; then pass acl.visibility.apply "groups pa-visible, pa-hidden; ACLs for User:ops only (group pa-hidden; cluster)"; else fail acl.visibility.apply "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; return; fi
  # Both groups EXIST: the super user lists them.
  out=$(innet "$T/kafka-consumer-groups.sh --bootstrap-server kafka-acl:9094 --list")
  if printf '%s' "$out" | grep -qx pa-visible && printf '%s' "$out" | grep -qx pa-hidden; then pass acl.visibility.super-user-lists-both "ANONYMOUS lists pa-visible and pa-hidden"; else fail acl.visibility.super-user-lists-both "$(printf '%s' "$out" | tr '\n' ' ')"; fi
  # T14: the restricted principal's listing omits pa-hidden, with no error.
  out=$(as_logweir "$T/kafka-consumer-groups.sh --bootstrap-server localhost:$s --command-config /tmp/c --list; echo rc=\$?")
  if printf '%s' "$out" | grep -qx 'rc=0' && printf '%s' "$out" | grep -qx pa-visible && ! printf '%s' "$out" | grep -qx pa-hidden; then pass acl.visibility.listing-filtered "logweir on localhost:$s lists pa-visible, not pa-hidden, rc 0"; else fail acl.visibility.listing-filtered "$(printf '%s' "$out" | tail -4 | tr '\n' ' ')"; fi
  # A targeted describe of the hidden group is REFUSED, not empty.
  out=$(as_logweir "$T/kafka-consumer-groups.sh --bootstrap-server localhost:$s --command-config /tmp/c --describe --group pa-hidden")
  if printf '%s' "$out" | grep -q 'GroupAuthorizationException\|Not authorized to access group'; then pass acl.visibility.targeted-describe-refused "$(printf '%s' "$out" | grep -o -m1 'GroupAuthorizationException[^.]*\|Not authorized to access group[^.]*')"; else fail acl.visibility.targeted-describe-refused "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
  # The cluster ACL for User:ops denies logweir Describe on the cluster.
  out=$(as_logweir "$T/kafka-acls.sh --bootstrap-server localhost:$s --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -q 'ClusterAuthorizationException\|Cluster authorization failed'; then pass acl.visibility.cluster-describe-denied "logweir's DescribeAcls: $(printf '%s' "$out" | grep -o -m1 'ClusterAuthorizationException[^.]*\|Cluster authorization failed[^.]*')"; else fail acl.visibility.cluster-describe-denied "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
  # NEGATIVE CONTROL (the record's): the same principal granted Describe on
  # the cluster gets the UNFILTERED listing — so pa-hidden's absence above
  # was the filter, not a missing group.
  innet "$T/kafka-acls.sh --bootstrap-server kafka-acl:9094 --add --allow-principal User:logweir --operation Describe --cluster" >/dev/null
  out=$(as_logweir "$T/kafka-consumer-groups.sh --bootstrap-server localhost:$s --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -qx pa-hidden && printf '%s' "$out" | grep -qx pa-visible; then pass acl.visibility.cluster-describe-unfilters "with Describe on the cluster, logweir lists pa-hidden too"; else fail acl.visibility.cluster-describe-unfilters "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
  innet "$T/kafka-acls.sh --bootstrap-server kafka-acl:9094 --remove --force --allow-principal User:logweir --operation Describe --cluster" >/dev/null
  # remove: back to the profile's baseline — no ACL at all, so logweir may
  # Describe the cluster again (allow.everyone.if.no.acl.found).
  out=$(bounded 300 bash e2e/compose/groups.sh visibility remove 2>&1)
  if printf '%s' "$out" | grep -q '^groups: removed'; then pass acl.visibility.remove "$(printf '%s' "$out" | tail -1)"; else fail acl.visibility.remove "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  out=$(as_logweir "$T/kafka-acls.sh --bootstrap-server localhost:$s --command-config /tmp/c --list; echo rc=\$?; $T/kafka-consumer-groups.sh --bootstrap-server localhost:$s --command-config /tmp/c --list")
  if printf '%s' "$out" | grep -qx 'rc=0' && ! printf '%s' "$out" | grep -q 'User:ops\|AuthorizationException' && ! printf '%s' "$out" | grep -qx 'pa-hidden\|pa-visible'; then pass acl.visibility.removed-baseline "logweir's DescribeAcls answers again, no User:ops binding, no fixture group"; else fail acl.visibility.removed-baseline "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; fi
}

# The `streams-protocol` profile (PROD-04.0d): WordCountProcessorDemo on the
# STREAMS rebalance protocol. Its group must be a Streams group, and the
# counts must be the application's — the same stop/restart controls as
# smoke_streams, on its own output topic.
smoke_streams_protocol() {
  local out n last g=logweir-e2e-streams-protocol
  n="${RUN}p"
  pconsume() {
    innet "$T/kafka-console-consumer.sh --bootstrap-server kafka-broker-1:9094 --topic streams-wordcount-processor-output --from-beginning --timeout-ms $1 --property print.key=true --property key.separator== 2>/dev/null"
  }
  pproduce() {
    innet "printf '%s\\n' $* | $T/kafka-console-producer.sh --bootstrap-server kafka-broker-1:9094 --topic streams-plaintext-input"
  }
  platest() { printf '%s\n' "$1" | grep "^$n=" | tail -1; }
  # The demo forwards its counts on STREAM-TIME punctuation (every second of
  # RECORD time), so a count reaches the output only once a later record
  # advances stream time: a tick line, produced two seconds later.
  ptick() { sleep 2; pproduce "'${n}tick'" > /dev/null; }
  # 1. The broker types the group Streams (KIP-1071) ...
  out=$(innet "$T/kafka-groups.sh --bootstrap-server kafka-broker-1:9094 --list")
  if printf '%s' "$out" | awk -v g="$g" '$1 == g && $2 == "Streams" { f = 1 } END { exit !f }'; then pass streams-protocol.group-type "$(printf '%s' "$out" | awk -v g="$g" '$1 == g' | tr -s ' ')"; else fail streams-protocol.group-type "$(printf '%s' "$out" | tail -4 | tr '\n' ' ')"; fi
  # ... and the consumer-group tooling does not know it: were the application
  # on the classic protocol, it would be listed there as a consumer group.
  out=$(innet "$T/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9094 --list")
  if ! printf '%s' "$out" | grep -qx "$g"; then pass streams-protocol.not-a-consumer-group "kafka-consumer-groups.sh --list omits $g"; else fail streams-protocol.not-a-consumer-group "$g is listed as a consumer group"; fi
  # 2. Two lines through the running application: exactly 2.
  pproduce "'$n alpha'" "'$n beta'" > /dev/null
  ptick
  last=$(platest "$(pconsume 30000)")
  if [ "$last" = "$n=2" ]; then pass streams-protocol.wordcount "$last on streams-wordcount-processor-output"; else fail streams-protocol.wordcount "latest '$last', want $n=2"; fi
  # 3. NEGATIVE CONTROL: stopped, a third line counts nothing.
  bounded 120 $DC --profile streams-protocol stop streams-protocol-wordcount > /dev/null 2>&1
  pproduce "'$n gamma'" > /dev/null
  ptick
  last=$(platest "$(pconsume 10000)")
  if [ "$last" = "$n=2" ]; then pass streams-protocol.stopped-app-counts-nothing "the count stayed at $last"; else fail streams-protocol.stopped-app-counts-nothing "latest '$last' with the app stopped, want $n=2"; fi
  out=$(innet "$T/kafka-streams-groups.sh --bootstrap-server kafka-broker-1:9094 --describe --group $g --state")
  if printf '%s' "$out" | awk -v g="$g" '$1 == g && $(NF - 1) == "Empty" { f = 1 } END { exit !f }'; then pass streams-protocol.stopped-group-empty "$g is Empty once the application stops"; else fail streams-protocol.stopped-group-empty "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  # 4. Restarted, it catches up from its committed position and changelog: 3.
  bounded 300 $DC --profile streams-protocol up -d --wait streams-protocol-wordcount > /dev/null 2>&1
  ptick
  last=$(platest "$(pconsume 30000)")
  if [ "$last" = "$n=3" ]; then pass streams-protocol.restart-catches-up "after a restart: $last"; else fail streams-protocol.restart-catches-up "latest '$last' after the restart, want $n=3"; fi
}

# The groups fixture (PROD-04.0d), run as `profile-smoke.sh groups` (it is a
# helper, not a profile): `groups.sh up`, then the set the broker's finalized
# features call for — computed here independently of the helper — with each
# group's type and state, the members' stop/start, and the share state.
smoke_groups() {
  local out feat has_c=0 has_s=0 has_t=0 g want t s rows live
  out=$(bounded 1200 bash e2e/compose/groups.sh up 2>&1)
  if [ $? = 0 ]; then pass groups.up "$(printf '%s' "$out" | grep -m1 'kafka-broker-1: classic')"; else fail groups.up "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"; return; fi
  feat=$(innet "$T/kafka-features.sh --bootstrap-server kafka-broker-1:9094 describe")
  lvl() { printf '%s\n' "$feat" | awk -v f="$1" '$1 == "Feature:" && $2 == f { for (i = 3; i < NF; i++) if ($i == "FinalizedVersionLevel:") print $(i + 1) }' | head -1; }
  [ "$(lvl group.version)" -ge 1 ] 2>/dev/null && has_c=1
  [ "$(lvl share.version)" -ge 1 ] 2>/dev/null && has_s=1
  [ "$(lvl streams.version)" -ge 1 ] 2>/dev/null && has_t=1
  rows="pa-classic-empty Classic Empty
pa-classic-live Classic Stable"
  [ $has_c = 1 ] && rows="$rows
pa-consumer-empty Consumer Empty
pa-consumer-live Consumer Stable"
  [ $has_s = 1 ] && rows="$rows
pa-share-idle Share Empty
pa-share-live Share Stable"
  [ $has_t = 1 ] && rows="$rows
logweir-e2e-streams-protocol Streams Stable"
  out=$(bounded 600 bash e2e/compose/groups.sh list 2>/dev/null)
  while read -r g t want; do
    if printf '%s' "$out" | awk -v g="$g" -v t="$t" -v w="$want" '$1 == g && $2 == t && $3 == w { f = 1 } END { exit !f }'; then pass "groups.$g" "$t, $want"; else fail "groups.$g" "want $t $want; list: $(printf '%s' "$out" | awk -v g="$g" '$1 == g' | tr -s ' ')"; fi
  done <<EOF
$rows
EOF
  if [ $has_c = 1 ] || [ $has_s = 1 ] || [ $has_t = 1 ]; then
    # NEGATIVE CONTROL for the types: the consumer-group listing — what a
    # ListConsumerGroups-only reader sees — must NOT show the share and
    # streams groups, or they are not really of those types.
    out=$(innet "$T/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9094 --list")
    if printf '%s' "$out" | grep -qx pa-classic-live && ! printf '%s' "$out" | grep -qx 'pa-share-idle\|pa-share-live\|logweir-e2e-streams-protocol'; then pass groups.consumer-listing-omits-share-streams "kafka-consumer-groups.sh --list: $(printf '%s' "$out" | grep '^pa-' | sort | tr '\n' ' ')"; else fail groups.consumer-listing-omits-share-streams "$(printf '%s' "$out" | tr '\n' ' ')"; fi
  else
    # A 3.x line: the helper made classic groups only, because a
    # consumer-protocol member cannot join here at all.
    out=$(innet "timeout 40 $T/kafka-console-consumer.sh --bootstrap-server kafka-broker-1:9094 --topic pa-orders --max-messages 1 --timeout-ms 20000 --consumer-property group.protocol=consumer --group $RUN-kip848")
    if printf '%s' "$out" | grep -q -i 'UnsupportedVersion\|UNSUPPORTED_VERSION\|not supported'; then pass groups.no-consumer-protocol-on-3x "$(printf '%s' "$out" | grep -i -o -m1 'UnsupportedVersion[A-Za-z]*[^.]*\|UNSUPPORTED_VERSION[^.]*')"; else fail groups.no-consumer-protocol-on-3x "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  fi
  # MEMBERS STOP AND START CLEANLY. While a member lives, its group refuses a
  # reset (it is active); once `stop` returns, the same reset commits and
  # reads back (PROD-04.0 §3.3's control) — and the OTHER live groups are
  # still Stable, so the bracketed pattern stopped only its own member.
  local m mg others want_others reset rb t0 dt
  # The live groups of this line, sorted: a stop must leave every OTHER one
  # Stable (an unanchored or unbracketed pattern can take a neighbour down).
  live=$(printf '%s\n' "$rows" | awk '$3 == "Stable" { print $1 }' | sort)
  for m in classic-live consumer-live; do
    [ "$m" = consumer-live ] && [ $has_c != 1 ] && continue
    mg=pa-$m
    reset="$T/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9094 --reset-offsets --group $mg --topic pa-orders:0 --to-offset 1 --execute"
    out=$(innet "$reset")
    if printf '%s' "$out" | grep -q 'inactive\|is Stable\|current state is Stable'; then pass "groups.$m.live-refuses-reset" "$(printf '%s' "$out" | grep -o -m1 'Assignments can only be reset[^.]*')"; else fail "groups.$m.live-refuses-reset" "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
    t0=$SECONDS
    out=$(bounded 300 bash e2e/compose/groups.sh stop "$m" 2>&1)
    s=$?
    dt=$((SECONDS - t0))
    others=$(bounded 600 bash e2e/compose/groups.sh list 2>/dev/null | awk -v g="$mg" '$1 != g && $3 == "Stable" { print $1 }' | sort | tr '\n' ' ')
    want_others=$(printf '%s\n' "$live" | grep -vx "$mg" | tr '\n' ' ')
    # Under 40 s: a member that did not LEAVE (killed, not closed) would hold
    # its group until session.timeout.ms, 45 s by default.
    if [ $s = 0 ] && [ "$dt" -lt 40 ] && [ "$others" = "$want_others" ]; then pass "groups.$m.stop" "$mg Empty, stop returned in $dt s; every other live group still Stable: $others"; else fail "groups.$m.stop" "rc $s in $dt s; Stable: '$others', want '$want_others'; $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
    out=$(innet "$reset")
    rb=$(innet "$T/kafka-consumer-groups.sh --bootstrap-server kafka-broker-1:9094 --describe --group $mg --offsets" | awk -v g="$mg" '$1 == g && $2 == "pa-orders" && $3 == 0 { print $4 }')
    # One stream of words: 3.9.2's reset prints its row on the header's line.
    if printf ' %s ' "$out" | tr -s '[:space:]' ' ' | grep -q -F " $mg pa-orders 0 1 " && [ "$rb" = 1 ]; then pass "groups.$m.stopped-accepts-reset" "pa-orders 0 committed at 1, read back $rb"; else fail "groups.$m.stopped-accepts-reset" "readback '$rb': $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
    out=$(bounded 300 bash e2e/compose/groups.sh start "$m" 2>&1)
    if [ $? = 0 ]; then pass "groups.$m.start" "$mg Stable again"; else fail "groups.$m.start" "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  done
  for m in share-live streams; do
    [ "$m" = share-live ] && [ $has_s != 1 ] && continue
    [ "$m" = streams ] && [ $has_t != 1 ] && continue
    t0=$SECONDS
    out=$(bounded 300 bash e2e/compose/groups.sh stop "$m" 2>&1)
    s=$?
    dt=$((SECONDS - t0))
    if [ $s = 0 ] && [ "$dt" -lt 40 ]; then pass "groups.$m.stop" "its group is Empty, stop returned in $dt s"; else fail "groups.$m.stop" "rc $s in $dt s: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
    out=$(bounded 400 bash e2e/compose/groups.sh start "$m" 2>&1)
    if [ $? = 0 ]; then pass "groups.$m.start" "Stable again"; else fail "groups.$m.start" "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
  done
  [ $has_s = 1 ] || return 0
  # SHARE STATE. The broker runs with the single-broker settings ...
  out=$(innet "$T/kafka-configs.sh --bootstrap-server kafka-broker-1:9094 --describe --entity-type brokers --entity-name 1001 --all | grep 'share.coordinator.state.topic.\\(replication.factor\\|min.isr\\)='; $T/kafka-topics.sh --bootstrap-server kafka-broker-1:9094 --describe --topic __share_group_state | head -1")
  if printf '%s' "$out" | grep -q 'replication.factor=1 ' && printf '%s' "$out" | grep -q 'min.isr=1 ' && printf '%s' "$out" | grep -q 'Topic: __share_group_state.*ReplicationFactor: 1'; then pass groups.share-state-topic "__share_group_state exists with RF 1 (broker: RF 1, min ISR 1)"; else fail groups.share-state-topic "$(printf '%s' "$out" | tr -s ' ' | tr '\n' ' ' | cut -c1-240)"; fi
  # ... so the idle group's start offsets are READABLE (with the defaults the
  # topic never exists and the tool says "has no offset information").
  out=$(innet "$T/kafka-share-groups.sh --bootstrap-server kafka-broker-1:9094 --describe --group pa-share-idle")
  if printf '%s' "$out" | awk '$1 == "pa-share-idle" && $2 == "pa-share-in" && $4 ~ /^[0-9]+$/ { f = 1 } END { exit !f }'; then pass groups.share-idle-start-offsets "$(printf '%s' "$out" | awk '$1 == "pa-share-idle" { printf "p%s=%s ", $3, $4 }')"; else fail groups.share-idle-start-offsets "$(printf '%s' "$out" | tr -s ' ' | tr '\n' ' ' | cut -c1-240)"; fi
  # NEGATIVE CONTROL for share.auto.offset.reset=earliest: a share group
  # WITHOUT it (the default, latest) reads none of pa-share-in's records.
  out=$(innet "timeout 60 $T/kafka-console-share-consumer.sh --bootstrap-server kafka-broker-1:9094 --topic pa-share-in --timeout-ms 15000 --group $RUN-latest 2>&1")
  if printf '%s' "$out" | grep -q 'Processed a total of 0 messages'; then pass groups.share-latest-reads-nothing "a share group without the earliest config: 0 messages"; else fail groups.share-latest-reads-nothing "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"; fi
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
    acl) smoke_acl; smoke_acl_visibility ;;
    streams-protocol) smoke_streams_protocol ;;
    groups) smoke_groups ;;
    *) fail "$p" "no smoke defined for profile $p" ;;
  esac
done
echo "# $fails failure(s)"
[ "$fails" -eq 0 ]
