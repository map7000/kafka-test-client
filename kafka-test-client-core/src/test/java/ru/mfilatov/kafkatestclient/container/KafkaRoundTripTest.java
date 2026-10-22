/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import ru.mfilatov.kafkatestclient.config.KafkaFileConfigProvider;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.processor.StringMessageProcessor;
import ru.mfilatov.kafkatestclient.producer.KafkaClientProducer;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessagePublisher;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber;

/**
 * Level 2: framework round trip against a real Kafka broker running in a container (Testcontainers
 * KRaft, image {@code apache/kafka}). Verifies the library's core promise: many small client
 * threads can send and receive their own messages concurrently on the same topic.
 *
 * <p>Runs with {@code mvn test -Pcontainer}. Uses podman through its socket: export {@code
 * DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock} and {@code
 * TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/run/user/$(id -u)/podman/podman.sock} when no Docker
 * daemon is available.
 */
@Testcontainers
class KafkaRoundTripTest {

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("apache/kafka:latest"))
          .waitingFor(Wait.forListeningPort());

  private static Properties testConfig() {
    var config = new KafkaFileConfigProvider().getKafkaConfig("kafka.properties");
    config.setProperty("bootstrap.servers", KAFKA.getBootstrapServers());
    config.setProperty("auto.offset.reset", "earliest");
    config.setProperty("enable.auto.commit", "true");
    config.setProperty("auto.commit.interval.ms", "100");
    config.setProperty("kafka.cluster.name", "container-test-cluster");
    return config;
  }

  private static void createTopics(String... topics) throws Exception {
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
      admin
          .createTopics(List.of(topics).stream().map(t -> new NewTopic(t, 1, (short) 1)).toList())
          .all()
          .get();
    }
  }

  @Test
  void concurrentSmallThreadsReceiveTheirOwnMessages() throws Exception {
    var topic = "rt-in-" + UUID.randomUUID();
    createTopics(topic);

    var config = testConfig();
    int probes = 30;
    var publisher = new KafkaMessagePublisher<String, String>(new StringMessageProcessor(), config);
    var subscriber = new KafkaMessageSubscriber<String, String>(topic);
    publisher.subscribe(subscriber);
    var producer =
        new KafkaClientProducer<String, String>(
            config, topic, new StringSerializer(), new StringSerializer());

    try {
      var executor = Executors.newFixedThreadPool(8);
      var failures = new CopyOnWriteArrayList<String>();
      var latenciesMs =
          IntStream.range(0, probes)
              .mapToObj(
                  i ->
                      CompletableFuture.supplyAsync(
                              () -> {
                                var rqUid = i + "-" + UUID.randomUUID();
                                var sentAt = System.nanoTime();
                                producer.send(
                                    KafkaMessage.<String, String>builder()
                                        .value(Integer.toString(i))
                                        .headers(Map.of("RqUID", rqUid))
                                        .build());
                                var start = System.currentTimeMillis();
                                await()
                                    .atMost(Duration.ofSeconds(30))
                                    .until(
                                        () ->
                                            subscriber.getMessages().stream()
                                                .anyMatch(
                                                    m -> rqUid.equals(m.headers().get("RqUID"))));
                                var found =
                                    subscriber.getMessages().stream()
                                        .filter(m -> rqUid.equals(m.headers().get("RqUID")))
                                        .findFirst()
                                        .orElseThrow();
                                // message must be exactly the probe's own record, nothing else
                                assertThat(found.value()).isEqualTo(Integer.toString(i));
                                return System.currentTimeMillis() - start;
                              },
                              executor)
                          .whenComplete(
                              (latency, error) -> {
                                if (error != null) {
                                  failures.add(i + ": " + error.getMessage());
                                }
                              }))
              .toList()
              .stream()
              .map(CompletableFuture::join)
              .toList();

      executor.shutdown();
      executor.awaitTermination(
          Duration.ofSeconds(30).toSeconds(), java.util.concurrent.TimeUnit.SECONDS);

      assertThat(failures).as("probes that lost their own message: %s", failures).isEmpty();
      assertThat(subscriber.getMessages().size()).isGreaterThanOrEqualTo(probes);
    } finally {
      producer.close();
      publisher.shutdown();
    }
  }

  @Test
  void routingIsolatedPerTopic() throws Exception {
    var topicA = "iso-a-" + UUID.randomUUID();
    var topicB = "iso-b-" + UUID.randomUUID();
    createTopics(topicA, topicB);

    var config = testConfig();
    var publisher = new KafkaMessagePublisher<String, String>(new StringMessageProcessor(), config);
    var subscriberA = new KafkaMessageSubscriber<String, String>(topicA);
    var subscriberB = new KafkaMessageSubscriber<String, String>(topicB);
    publisher.subscribe(subscriberA);
    publisher.subscribe(subscriberB);
    var producer =
        new KafkaClientProducer<String, String>(
            config, topicA, new StringSerializer(), new StringSerializer());
    try {
      producer.send(
          KafkaMessage.<String, String>builder()
              .topic(topicA)
              .value("to-a")
              .headers(Map.of("RqUID", "a"))
              .build());
      producer.send(
          KafkaMessage.<String, String>builder()
              .topic(topicB)
              .value("to-b")
              .headers(Map.of("RqUID", "b"))
              .build());

      await()
          .atMost(Duration.ofSeconds(30))
          .until(
              () -> !subscriberA.getMessages().isEmpty() && !subscriberB.getMessages().isEmpty());

      assertThat(subscriberA.getMessages())
          .singleElement()
          .satisfies(
              m -> {
                assertThat(m.topic()).isEqualTo(topicA);
                assertThat(m.value()).isEqualTo("to-a");
              });
      assertThat(subscriberB.getMessages())
          .singleElement()
          .satisfies(
              m -> {
                assertThat(m.topic()).isEqualTo(topicB);
                assertThat(m.value()).isEqualTo("to-b");
              });
    } finally {
      producer.close();
      publisher.shutdown();
    }
  }
}
