#!/usr/bin/env bash
set -eo pipefail

# Level 3 performance run orchestrator.
# 1. builds everything (skip tests)
# 2. starts Kafka in a container (podman/docker)
# 3. starts kafka-test-client-service as consumer/producer
# 4. starts the Gatling junk-traffic flood (kafka-test-client-core, -Pperf)
# 5. runs the functional smoke + latency/correlation probes under flood
# 6. tears everything down
#
# Tunables (env): PROBES, NOISE_OPS, NOISE_MINUTES, NOISE_BATCH, P95_MS, MAX_MS,
#                  TIMEOUT_SECONDS, WARMUP_SECONDS, CONTAINER_CLI(docker|podman),
#                  KAFKA_IMAGE, ALLURE(0|1)

PROBES="${PROBES:-200}"
NOISE_OPS="${NOISE_OPS:-100}"
NOISE_MINUTES="${NOISE_MINUTES:-3}"
NOISE_BATCH="${NOISE_BATCH:-5}"
P95_MS="${P95_MS:-15000}"
MAX_MS="${MAX_MS:-20000}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"
WARMUP_SECONDS="${WARMUP_SECONDS:-30}"
KAFKA_IMAGE="${KAFKA_IMAGE:-docker.io/apache/kafka-native:latest}"
ALLURE="${ALLURE:-1}"

KAFKA_CONTAINER="perf-kafka"
NETWORK_NAME="kafka-net"
SERVICE_LOG="target/perf-service.log"
NOISE_LOG="target/perf-noise.log"
mkdir -p target

if [ -z "${CONTAINER_CLI:-}" ]; then
  if command -v podman >/dev/null 2>&1; then CONTAINER_CLI="podman"; else CONTAINER_CLI="docker"; fi
fi

SERVICE_PID=""
NOISE_PID=""

cleanup() {
  echo "== Cleanup =="
  $CONTAINER_CLI logs $KAFKA_CONTAINER > target/perf-kafka.log 2>&1 || true
  echo "broker log saved to target/perf-kafka.log"
  if [ -n "$SERVICE_PID" ]; then
    kill -- "-$SERVICE_PID" 2>/dev/null || kill -TERM "$SERVICE_PID" 2>/dev/null || true
  fi
  if [ -n "$NOISE_PID" ]; then
    kill -- "-$NOISE_PID" 2>/dev/null || kill -TERM "$NOISE_PID" 2>/dev/null || true
    wait "$NOISE_PID" 2>/dev/null || true
  fi
  $CONTAINER_CLI stop $KAFKA_CONTAINER 2>/dev/null || true
  $CONTAINER_CLI rm $KAFKA_CONTAINER 2>/dev/null || true
  $CONTAINER_CLI network rm $NETWORK_NAME 2>/dev/null || true
}

trap cleanup EXIT

echo "== 1/6 Build (tests skipped) =="
mvn -q clean install -DskipTests

echo "== 2/6 Start Kafka (${CONTAINER_CLI}) =="
$CONTAINER_CLI network create $NETWORK_NAME 2>/dev/null || true
$CONTAINER_CLI run -d \
  --name $KAFKA_CONTAINER \
  --network $NETWORK_NAME \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  $KAFKA_IMAGE

echo "Waiting for Kafka broker..."
for i in $(seq 1 90); do
  if timeout 1 bash -c '</dev/tcp/localhost/9092' 2>/dev/null; then
    if [ "$i" -ge 3 ]; then echo "Kafka port is up (${i}s)"; break; fi
  fi
  if ! $CONTAINER_CLI ps --filter "name=$KAFKA_CONTAINER" --format "{{.Status}}" | grep -q Up; then
    echo "Kafka container died:"
    $CONTAINER_CLI logs $KAFKA_CONTAINER 2>&1 | tail -30
    exit 1
  fi
  sleep 1
done

echo "== 3/6 Start kafka-test-client-service =="
mkdir -p target
setsid mvn -f kafka-test-client-service/pom.xml spring-boot:run > "$SERVICE_LOG" 2>&1 &
SERVICE_PID=$!
echo "service started, pid group $SERVICE_PID, log: $SERVICE_LOG"

echo "Waiting for Spring service to boot..."
for i in $(seq 1 120); do
  if grep -q "Started Main\|KafkaQuickstartApplication.*started\|Started .*Application" "$SERVICE_LOG" 2>/dev/null; then
    echo "Service ready (${i}s)"
    break
  fi
  if ! kill -0 "$SERVICE_PID" 2>/dev/null; then
    echo "Service process died during startup:"
    tail -40 "$SERVICE_LOG"
    exit 1
  fi
  sleep 1
done
grep -q "Started" "$SERVICE_LOG" || { echo "Service never became ready"; tail -40 "$SERVICE_LOG"; exit 1; }

echo "== 4/6 Start Gatling noise flood =="
setsid mvn -q -pl kafka-test-client-core gatling:test -Pperf \
  -Dperf.noise.ops=$NOISE_OPS \
  -Dperf.noise.minutes=$((NOISE_MINUTES + 2)) \
  -Dperf.noise.batch=$NOISE_BATCH \
  > "$NOISE_LOG" 2>&1 &
NOISE_PID=$!
echo "noise generator started, pid group $NOISE_PID, log: $NOISE_LOG"

for i in $(seq 1 60); do
  if grep -q "\[noise\] flooding" "$NOISE_LOG" 2>/dev/null; then
    echo "Noise flood running (${i}s)"
    break
  fi
  if ! kill -0 "$NOISE_PID" 2>/dev/null; then
    echo "Noise generator died during startup:"
    tail -40 "$NOISE_LOG"
    exit 1
  fi
  sleep 1
done
echo "Warming up the flood for ${WARMUP_SECONDS}s..."
sleep "$WARMUP_SECONDS"

echo "== 5/6 Functional smoke + probes in ONE JVM (kills the class-switch stall) =="
mvn -pl kafka-test-client-core test -Pperf \
  -Dtest='FunctionalRoundTripTest,PerfRoundTripTest' \
  -Dperf.probes=$PROBES \
  -Dperf.timeout.seconds=$TIMEOUT_SECONDS \
  -Dperf.p95.ms=$P95_MS \
  -Dperf.max.ms=$MAX_MS

echo "== Results =="
echo "probe latencies : target/perf-results/roundtrip.jsonl"
if [ "$NOISE_PID" ]; then
  # let the flood run out its remaining planned time (keeps the gatling report), cap the wait
  if kill -0 "$NOISE_PID" 2>/dev/null; then
    echo "waiting for the flood to finish (for the gatling report)..."
    for i in $(seq 1 300); do
      if ! kill -0 "$NOISE_PID" 2>/dev/null; then break; fi
      sleep 1
    done
    if kill -0 "$NOISE_PID" 2>/dev/null; then
      kill -TERM "$NOISE_PID" 2>/dev/null || true
    fi
  fi
  wait "$NOISE_PID" 2>/dev/null || true
fi
echo "gatling report  : target/gatling/kafkaflood-*/index.html"
if [ "$ALLURE" = "1" ]; then
  rm -rf allure-report
  mvn -q -pl kafka-test-client-core allure:report || true
  cp -r kafka-test-client-core/target/site/allure-maven-plugin allure-report 2>/dev/null || true
  echo "allure report   : ./allure-report/index.html"
fi
echo "DONE"
