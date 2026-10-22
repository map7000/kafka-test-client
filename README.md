# kafka-test-client

A JUnit integration-test framework for coordinated Kafka send/receive round-trips, plus a small
standalone Kafka client library.

The core problem it solves: many small JUnit test threads must send and receive **their own** Kafka
messages (correlated by `RqUID`) even while other tests flood the same topics at high concurrency —
without cross-talk, without lost replies, and without the world standing still for a consumer
rebalance.

## Modules

| Module | ArtifactId | Description |
|---|---|---|
| `kafka-test-client-core/` | `kafka-test-client-lib` | The library + JUnit test framework |
| `kafka-test-client-service/` | `kafka-test-client-service` | Spring Boot round-trip service (`test_topic_in` → `test_topic_out`), used by the perf pipeline |

## How the framework works

```
  test thread                        publisher thread
  ────────────                       ────────────────
  kafka.send(msg)      ──▶ Kafka ──▶ KafkaMessageConsumer (poll loop)
  kafka.awaitMessage(RqUID)          │  processes ConsumerRecords,
        ▲                            ▼  distributes to subscriptions
        └────────────── KafkaMessageSubscriber (collector, one per topic)
```

- **`KafkaClientProducer`** — thin `KafkaProducer` wrapper; producers are shared per
  `(cluster, keySerializer, valueSerializer)` key with reference counting, so closing one wrapper
  never breaks another wrapper still using the same producer. Sends are handed to the producer
  first and awaited afterwards (allows batching); any send failure throws.
  The consumer-side `awaitMessage` correlates replies by the `RqUID` header.
- **`KafkaMessagePublisher`** — a `Flow.Publisher` that owns a single consumer daemon thread.
  Subscribers added via `subscribe()` get a `KafkaMessageSubscription` wired to their topic.
- **`KafkaMessageConsumer`** — the poll loop. Dynamically adds/removes topic subscriptions as test
  classes come and go, commits synchronously after each batch, and **unsubscribes (leaves the
  consumer group) when the last subscription is cancelled** — otherwise cancelled subscribers stay
  zombie group members and stall the next subscription's rebalance for a full session timeout.
- **`KafkaMessageSubscriber`** — a `Flow.Subscriber` collector you can await against, with optional
  predicate filtering (used to drop flood traffic from non-probe senders).

### Backpressure

`KafkaMessagePublisher` is a proper Reactive Streams (`java.util.concurrent.Flow`) implementation:

1. On `onSubscribe`, a `KafkaMessageSubscriber` immediately requests unbounded demand
   (`Long.MAX_VALUE`) — delivery is driven by the consumer's poll loop, not by manual `request(n)`
   calls.
2. `KafkaMessageConsumer` honours that contract per subscription: a record is only delivered via
   `onNext` to subscriptions with outstanding demand, and the demand counter is decremented for
   each delivered message.

Cancel (`Flow.Subscription.cancel()` → `subscriber.cancel()`) stops delivery and, once the last
subscription goes away, makes the underlying consumer leave its consumer group.

## Using the test framework

Subclass `AbstractBaseTest`, declare your endpoints with class-level annotations, and drive the
round trip through `kafka`:

```java
@KafkaWriter("test_topic_in")   // topic this test produces to (optional)
@KafkaReader("test_topic_out")  // topic this test consumes from (optional)
class MyRoundTripTest extends AbstractBaseTest {

  @Test
  void roundTrip() {
    String rqUID = UUID.randomUUID().toString();
    kafka.send(KafkaMessage.<String, String>builder()
        .topic("test_topic_in")
        .key(rqUID)
        .value("payload")
        .headers(Map.of("RqUID", rqUID))
        .build());

    KafkaMessage<String, String> reply = kafka.awaitMessage(rqUID, 10);
    assertThat(reply.value()).isEqualTo("payload");
  }
}
```

Notes:

- `@KafkaWriter`/`@KafkaReader` are wired up in `AbstractBaseTest#initKafka` (`@BeforeAll`);
  annotation on the class only. Forgetting them means `kafka.send` / `kafka.awaitMessage`
  operate on `null` components.
- Tests run in parallel (`junit.jupiter.execution.parallel.enabled = true`, factor 4) — keep tests
  stateless and isolated; the `RqUID` header is the only correlation mechanism.
- Under heavy flood, use `FloodAwareBaseTest` + `PerfProbes`: subscribers keep ≤3000 messages,
  dropping the oldest when full (no wholesale reset, so in-flight replies are never erased) and the
  flood-aware predicate prevents non-probe traffic from ever reaching the collector.

## Standalone library usage

The components can be used directly, outside JUnit:

```java
KafkaMessagePublisher<String, String> publisher =
    new KafkaMessagePublisher<>(new StringMessageProcessor(), config /* or null for kafka.properties */);

KafkaMessageSubscriber<String, String> subscriber = new KafkaMessageSubscriber<>("some-topic");
publisher.subscribe(subscriber);

KafkaClientProducer<String, String> producer = new KafkaClientProducer<>(
    config, "some-topic", new StringSerializer(), new StringSerializer());
producer.send(KafkaMessage.<String, String>builder()
    .value("hello")
    .headers(Map.of("RqUID", rqUID))
    .build());

// later: messages reply with topic/partition/offset/headers populated
producer.close();
publisher.close();
```

Message content is mapped by a `KafkaMessageProcessor` (`StringMessageProcessor` decodes
`byte[]`/`String` values and copies headers into `KafkaMessage.headers()`).

## Configuration

Consumer/producer settings are read from `kafka.properties` on the classpath by
`KafkaFileConfigProvider`. Reference (from `kafka-test-client-core/src/test/resources`):

```properties
bootstrap.servers=localhost:9092
kafka.cluster.name=test-cluster
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=org.apache.kafka.common.serialization.StringSerializer
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
group.id=TEST_GROUP
auto.offset.reset=earliest
session.timeout.ms=10000
```

`KafkaMessagePublisher` and `KafkaMessageConsumer` also accept an explicit `Properties` object
(useful when bootstrap servers are dynamic, e.g. Testcontainers).

`session.timeout.ms` is deliberately kept below the default 45s: a dead test JVM must not hold the
consumer group hostage while the next test's subscription waits out the rebalance.

## Build and test

```bash
mvn clean install -DskipTests     # full build
mvn spotless:check                # format check (google-java-format + license header)
mvn spotless:apply                # auto-format
```

| Level | Command | Needs Kafka? |
|---|---|---|
| 1 — unit | `mvn test -pl kafka-test-client-core` | no |
| 2 — container | `mvn test -pl kafka-test-client-core -Pcontainer` | yes (Testcontainers + podman socket) |
| 3 — perf pipeline | `PROBES=200 NOISE_OPS=100 NOISE_MINUTES=3 bash perf/run_perf_tests.sh` | yes (real broker + running service + Gatling flood) |

Level 2 requires the podman socket:

```bash
systemctl --user start podman.socket
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/run/user/$(id -u)/podman/podman.sock
```

Level 3 floods the topics with junk traffic, runs the functional smoke + N concurrent correlation
probes, gates p95/max latency, and writes reports. The two perf test classes
(`FunctionalRoundTripTest`, `PerfRoundTripTest`) **must run in ONE surefire invocation** — separate
JVMs kill the functional consumer group membership and the next JVM's rebalance stalls ~45s.
Perf tunables are pom properties, overridable via `-D`: `perf.probes`, `perf.timeout.seconds`,
`perf.p95.ms`, `perf.max.ms`, `perf.noise.ops`, `perf.noise.minutes`, `perf.noise.batch`.

CI: `.github/workflows/performance.yml` (manual dispatch) runs unit tests + the perf pipeline and
uploads probe results, Gatling and Allure reports as artifacts.

## Maintenance

- Dependency versions live as properties in the root `pom.xml`; update them there, not in module
  poms.
- Known-latest check: `mvn versions:display-property-updates -DallowMajorUpdates=true` (ignore the
  401/422 warnings from the unconfigured `github` repo; pre-release versions must not be bumped to).
- Contributor conventions and verified gotchas (Spring Boot 4 modularization, Lombok/JDK
  compatibility, surefire+AspectJ pinning) are documented in [AGENTS.md](AGENTS.md).
