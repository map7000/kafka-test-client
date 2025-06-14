/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

// export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/run/user/1000/podman/podman.sock

@Testcontainers
class KafkaIntegrationTest {

  @Container
  private static final KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.0.0"));

  @Test
  void testProduceAndConsume() {
    // 1. Create test topic name
    String topic = "test-topic-" + UUID.randomUUID();

    // 2. Configure producer
    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", kafka.getBootstrapServers());
    producerProps.put("key.serializer", StringSerializer.class.getName());
    producerProps.put("value.serializer", StringSerializer.class.getName());

    // 3. Configure consumer
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    // 4. Test logic
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {

      // Subscribe to topic
      consumer.subscribe(Collections.singletonList(topic));

      // Send test message
      String testMessage = "Hello Kafka!";
      producer.send(new ProducerRecord<>(topic, testMessage)).get();

      // Verify message received
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                assertThat(records).hasSize(1);
                assertThat(records.iterator().next().value()).isEqualTo(testMessage);
              });
    } catch (Exception e) {
      throw new RuntimeException("Kafka test failed", e);
    }
  }
}
