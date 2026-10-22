# AGENTS.md

## Repo layout

- Maven multi-module project; versions of all deps/plugins are properties in the root `pom.xml` — update deps there, not in module poms.
- `kafka-test-client-core/` — library + JUnit integration test framework (not just tests). The artifactId is `kafka-test-client-lib` (directory name differs).
- `kafka-test-client-service/` — Spring Boot 4 app (entrypoint `ru.mfilatov.kafkatestclient.service.Main`) that round-trips messages (`test_topic_in` → `test_topic_out`), built to run in Docker (`Dockerfile`, port 8080).
- language level is Java 24 (`maven.compiler.release=24`); a newer JDK (e.g. 27) works fine.

## Commands

- Full build: `mvn clean install -DskipTests`
- Format check / fix: `mvn spotless:check` / `mvn spotless:apply` (google-java-format + license header)
- Level 1 (unit tests, no Kafka): `mvn test -pl kafka-test-client-core`
- Level 2 (container tests, real Kafka via Testcontainers + podman socket): `mvn test -pl kafka-test-client-core -Pcontainer` — needs `DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock` and `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/run/user/$(id -u)/podman/podman.sock` plus `systemctl --user start podman.socket`.
- Level 3 (performance pipeline): `PROBES=200 NOISE_OPS=100 NOISE_MINUTES=3 bash perf/run_perf_tests.sh` — builds, starts Kafka (podman/docker), starts the service, floods the topics with Gatling junk traffic, runs the functional smoke + 200 correlation probes, gates p95/max latency, and writes reports.
- Single test: `mvn test -pl kafka-test-client-core -Dtest=ServiceTest`

## Test levels (kafka-test-client-core)

The core idea: many small JUnit test threads must send and receive **their own** Kafka messages (correlated by `RqUID`) even under heavy topic flood and high concurrency.

- `**/unit/*Test.java` — plain JVM tests (default surefire include; no broker needed).
- `**/container/*Test.java` — Testcontainers KRaft Kafka (`apache/kafka:latest`, podman socket), full producer→consumer→subscriber pipeline, 30 concurrent probes + per-topic routing isolation.
- `**/perf/*Test.java` — against a real broker **plus the running service**: `FunctionalRoundTripTest` (correctness smoke) and `PerfRoundTripTest` (N concurrent probes under Gatling flood; asserts per-probe correlation, no cross-talk, and p95/max latency budgets). MUST run both classes in ONE surefire invocation (see script step 5/6): separate JVMs kill the functional consumer group membership and the next JVM's rebalance waits out the dead member's session timeout — a ~45s stall. Tunables (`perf.probes`, `perf.timeout.seconds`, `perf.p95.ms`, `perf.max.ms`, `perf.noise.ops`, `perf.noise.minutes`, `perf.noise.batch`) are pom properties overridable via `-D`.

## Integration test environment (kafka-test-client-core)

- From 3 levels above, ONLY unit tests run on a bare `mvn test`; container & perf levels need Kafka on `localhost:9092`.
- `run_tests.sh` (legacy) and `perf/run_perf_tests.sh` spin up the whole environment with **podman**. `perf/run_perf_tests.sh` is the reference: proper KRaft env vars are required for `apache/kafka*` images when listeners are overridden (`KAFKA_NODE_ID`, `KAFKA_PROCESS_ROLES`, `KAFKA_CONTROLLER_QUORUM_VOTERS`, …), otherwise the controller listener vanishes and the broker fails with `Missing required configuration "process.roles"`.
- Test config comes from `kafka-test-client-core/src/test/resources/kafka.properties` (bootstrap, serializers, group id, `auto.offset.reset=earliest`, `session.timeout.ms=10000`).
- CI: `.github/workflows/performance.yml` (manual dispatch) runs unit tests + the perf pipeline on GitHub runners (docker) and uploads probe results, gatling and allure reports as artifacts.

## Test-framework conventions (core)

- Kafka endpoints are declared as class annotations `@KafkaWriter("topic")` / `@KafkaReader("topic")`, and wired up in `AbstractBaseTest` `@BeforeAll` when subclassing from it. New integration tests should subclass `AbstractBaseTest` and use `kafka.send(...)` / `kafka.awaitMessage(rqUID, timeout)`. Forgetting the annotations → producer/subscriber are `null`.
- Tests run in parallel (`junit.jupiter.execution.parallel.enabled = true`, `parallel-factor = 4`) — keep tests stateless / isolated.
- Under flood, subscribers must drop non-probe replies: `KafkaMessageSubscriber` keeps ≤3000 messages and then resets its list, which erases in-flight replies. The perf level uses `FloodAwareBaseTest` + `PerfProbes` (RqUID registry + subscriber predicate) for that.
- Surefire injects the AspectJ weaver as `-javaagent` for Allure; keep `aspectj.version` in sync if the Allure setup changes.
- Message correlation is via `RqUID` header (UUID) sent by the producer and matched by `awaitMessage`.
- `KafkaMessageConsumer` unsubscribes (leaves the consumer group) when the last subscription is cancelled — keep this behaviour, otherwise cancelled subscribers become zombie group members that stall the next subscription's rebalance for a full session timeout.

## Version-update gotchas (verified)

- The Spring Boot 4 BOM no longer manages `org.springframework.retry:spring-retry` — keep the explicit version pinned in `kafka-test-client-service/pom.xml`.
- Spring Boot 4 modularized autoconfiguration: `spring-kafka` alone no longer wires `KafkaTemplate`; the service needs `spring-boot-starter-kafka`. `KafkaTemplate` bean missing at startup = this same issue.
- `<spring-boot.version>` lives in the root `pom.xml` and is referenced by the service pom's BOM import — don't delete it when resolving merges.
- `io.qameta.allure:allure-junit5` was renamed to `allure-jupiter`; use the new artefactId.
- Lombok <1.18.48 crashes under installed JDK 27 (`EndPosTable` ExceptionInInitializerError) — keep `lombok.version` ≥ 1.18.48.
- Known latest-stable check: `mvn versions:display-property-updates -DallowMajorUpdates=true` (ignore the 401/422 warnings from the unconfigured `github` package repo — Maven Central still answers).
- Pre-release versions (RC/M/beta) appear as "latest" in Maven Central metadata; do not bump to them.
