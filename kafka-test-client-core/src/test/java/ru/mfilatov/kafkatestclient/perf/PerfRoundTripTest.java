/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.perf;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.mfilatov.kafkatestclient.annotations.KafkaReader;
import ru.mfilatov.kafkatestclient.annotations.KafkaWriter;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

/**
 * Level 3 (probe runner): verifies the library's core promise under load — many small test threads
 * (JUnit parallel execution) send probes concurrently and each receives exactly its own reply while
 * the Gatling flood fills the same topics with junk traffic.
 *
 * <p>Per-probe assertions: each thread receives the reply carrying its own RqUID (no cross-talk, no
 * losses) and the reply value is the doubled payload (no rerouted content).
 *
 * <p>Run-level gates: p95 and max end-to-end latency within configurable bounds (service dice-rolls
 * of 8/9 schedule replies with a 10 s delay, so the p95 tolerance accounts for the scheduled
 * bucket).
 *
 * <p>Configurable via Maven properties: {@code perf.probes}, {@code perf.timeout.seconds}, {@code
 * perf.p95.ms}, {@code perf.max.ms}. Raw results are written to {@code target/perf-results}.
 */
@Slf4j
@KafkaWriter("test_topic_in")
@KafkaReader("test_topic_out")
class PerfRoundTripTest extends FloodAwareBaseTest {

  private static final int PROBES = intProp("perf.probes", 200);
  private static final int TIMEOUT_SECONDS = intProp("perf.timeout.seconds", 60);
  private static final long P95_LIMIT_MS = longProp("perf.p95.ms", 15000L);
  private static final long MAX_LIMIT_MS = longProp("perf.max.ms", 20000L);
  private static final Path RESULTS_FILE = Path.of("target", "perf-results", "roundtrip.jsonl");

  private static final List<Probe> results = new CopyOnWriteArrayList<>();
  private static final AtomicInteger completed = new AtomicInteger();

  private record Probe(int index, String rqUid, long sentAtMs, long latencyMs) {}

  @AfterAll
  static void reportAndGate() throws IOException {
    var latencies = results.stream().mapToLong(Probe::latencyMs).sorted().toArray();
    var max = latencies.length == 0 ? 0 : latencies[latencies.length - 1];
    var p95 = percentile(95, latencies);
    log.info(
        "Perf probes completed {}/{}; latency ms: p50={} p95={} p99={} max={}",
        completed.get(),
        PROBES,
        percentile(50, latencies),
        p95,
        percentile(99, latencies),
        max);
    writeResults();

    assertThat(completed.get())
        .as("all probes must receive exactly their own reply")
        .isEqualTo(PROBES);
    assertThat(p95).as("p50 latency budget (perf.p95.ms)").isLessThanOrEqualTo(P95_LIMIT_MS);
    assertThat(max).as("max latency budget (perf.max.ms)").isLessThanOrEqualTo(MAX_LIMIT_MS);
  }

  /** Probe the pipeline: send a probe (id) and verify the reply carries its own result. */
  @ParameterizedTest
  @Execution(ExecutionMode.CONCURRENT)
  @MethodSource("probes")
  void probeUnderFlood(Integer i) throws Exception {
    var rqUid = i + ":" + UUID.randomUUID();
    registerProbe(rqUid);
    try {
      var sentAtMs = System.currentTimeMillis();
      kafka.send(
          KafkaMessage.<String, String>builder()
              .headers(Map.of("RqUID", rqUid))
              .value(i.toString())
              .build());

      var message = kafka.awaitMessage(rqUid, TIMEOUT_SECONDS);
      // correctness: this probe's own reply, and nothing else
      assertThat(message.headers()).containsEntry("RqUID", rqUid);
      assertThat(message.value())
          .as("reply must be exactly the doubled payload of probe %s", i)
          .isEqualTo(Integer.toString(i * 2));
      assertThat(message.topic()).isEqualTo(kafka.getSubscriber().getKafkaTopic());

      completed.incrementAndGet();
      results.add(new Probe(i, rqUid, sentAtMs, System.currentTimeMillis() - sentAtMs));
    } finally {
      unregisterProbe(rqUid);
    }
  }

  private static Stream<Integer> probes() {
    return IntStream.range(0, PROBES).boxed();
  }

  private static void writeResults() throws IOException {
    if (results.isEmpty()) {
      return;
    }
    Files.createDirectories(RESULTS_FILE.getParent());
    var lines =
        results.stream()
            .sorted(java.util.Comparator.comparingInt(Probe::index))
            .map(
                r ->
                    "{\"index\":%d,\"rq_uid\":\"%s\",\"sent_at_ms\":%d,\"latency_ms\":%d}"
                        .formatted(r.index(), r.rqUid(), r.sentAtMs(), r.latencyMs()))
            .toList();
    Files.write(RESULTS_FILE, lines, StandardCharsets.UTF_8);
    log.info("Wrote {} probe results to {}", lines.size(), RESULTS_FILE);
  }

  private static long percentile(int p, long[] sorted) {
    if (sorted.length == 0) {
      return -1;
    }
    int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
    return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
  }

  private static int intProp(String name, int fallback) {
    var value = System.getProperty(name);
    return value == null ? fallback : Integer.parseInt(value);
  }

  private static long longProp(String name, long fallback) {
    var value = System.getProperty(name);
    return value == null ? fallback : Long.parseLong(value);
  }
}
