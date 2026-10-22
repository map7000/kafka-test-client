/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.perf;

import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Session;
import io.gatling.javaapi.core.Simulation;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mfilatov.kafkatestclient.config.KafkaFileConfigProvider;

/**
 * Level 3 (flood generator): a Gatling simulation that continuously pushes junk traffic into the
 * round-trip topics shared with kafka-test-client-service. Noise records are numerically valid so
 * the service processes and answers them like any other message — only the correlation RqUID is
 * random, so the functional/perf probes must still receive exactly their own replies despite the
 * flood.
 *
 * <p>Tuning (system properties, wired through the {@code perf} Maven profile): {@code
 * perf.noise.ops} target ops/sec (default 100), {@code perf.noise.minutes} runtime (default 3),
 * {@code perf.noise.batch} messages per tick (default 5).
 */
public class NoiseSimulation extends Simulation {

  private static final int OPS_PER_SEC = intProp("perf.noise.ops", 100);
  private static final int MINUTES = intProp("perf.noise.minutes", 3);
  private static final int BATCH_SIZE = intProp("perf.noise.batch", 5);

  private final KafkaProducer<String, String> producer =
      new KafkaProducer<>(
          new KafkaFileConfigProvider().getKafkaConfig("kafka.properties"),
          new StringSerializer(),
          new StringSerializer());

  private static final String IN_TOPIC = "test_topic_in";

  public NoiseSimulation() {
    // each user ticks a batch of junk messages, then waits to keep the ops/sec budget
    final ChainBuilder produce =
        CoreDsl.exec(
            (Session session) -> {
              for (int i = 0; i < BATCH_SIZE; i++) {
                var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
                headers.add(
                    new org.apache.kafka.common.header.internals.RecordHeader(
                        "RqUID", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)));
                headers.add(
                    new org.apache.kafka.common.header.internals.RecordHeader(
                        "X-Noise", "true".getBytes(StandardCharsets.UTF_8)));
                producer.send(
                    new ProducerRecord<>(
                        IN_TOPIC,
                        null,
                        String.valueOf(ThreadLocalRandom.current().nextInt(1, 100)),
                        String.valueOf(ThreadLocalRandom.current().nextInt(1, 100)),
                        headers));
              }
              producer.flush();
              return session;
            });

    ScenarioBuilder scenario =
        CoreDsl.scenario("kafka-flood")
            .forever()
            .on(produce.pause(Duration.ofMillis(Math.max(1, 1000L * BATCH_SIZE / OPS_PER_SEC))));

    setUp(scenario.injectOpen(CoreDsl.constantUsersPerSec(1).during(MINUTES * 60L)))
        .maxDuration(Duration.ofMinutes(MINUTES));
  }

  @Override
  public void before() {
    System.out.printf(
        "[noise] flooding '%s': ~%d ops/sec in batches of %d for %d minute(s)%n",
        IN_TOPIC, OPS_PER_SEC, BATCH_SIZE, MINUTES);
  }

  @Override
  public void after() {
    producer.close();
    System.out.printf("[noise] flood finished after %d minute(s)%n", MINUTES);
  }

  private static int intProp(String name, int fallback) {
    var value = System.getProperty(name);
    return value == null ? fallback : Integer.parseInt(value);
  }
}
