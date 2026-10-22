/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.producer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

/**
 * A generic Kafka producer that supports different message formats. Manages producer instances per
 * cluster and handles message serialization.
 *
 * @param <K> The type of message keys
 * @param <V> The type of message values
 */
@Slf4j
@Getter
public class KafkaClientProducer<K, V> implements AutoCloseable {
  private final String clusterName;
  private final String defaultTopic;
  private static final Lock lock = new ReentrantLock();
  private static final ConcurrentMap<String, KafkaProducer<?, ?>> producers =
      new ConcurrentHashMap<>();

  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  /**
   * Creates a new producer with custom serializers.
   *
   * @param config Kafka configuration
   * @param defaultTopic default topic to produce to
   * @param keySerializer serializer for keys
   * @param valueSerializer serializer for values
   */
  public KafkaClientProducer(
      Properties config,
      String defaultTopic,
      Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this.clusterName = config.getProperty("kafka.cluster.name");
    this.defaultTopic = defaultTopic;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;

    lock.lock();
    try {
      String producerKey = getProducerKey(clusterName, keySerializer, valueSerializer);
      if (!producers.containsKey(producerKey)) {
        producers.put(producerKey, createProducer(config));
        log.info(
            "Created producer for cluster: {} with types K: {}, V: {}",
            clusterName,
            keySerializer.getClass().getSimpleName(),
            valueSerializer.getClass().getSimpleName());
      }
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  private KafkaProducer<K, V> getProducer() {
    String producerKey = getProducerKey(clusterName, keySerializer, valueSerializer);
    return (KafkaProducer<K, V>) producers.get(producerKey);
  }

  private KafkaProducer<K, V> createProducer(Properties config) {
    return new KafkaProducer<>(config, keySerializer, valueSerializer);
  }

  private String getProducerKey(
      String clusterName, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    return String.format(
        "%s-%s-%s",
        clusterName, keySerializer.getClass().getName(), valueSerializer.getClass().getName());
  }

  /**
   * Sends multiple messages to Kafka.
   *
   * @param messages list of messages to send
   * @return list of record metadata for sent messages
   */
  public List<RecordMetadata> send(List<KafkaMessage<K, V>> messages) {
    var metadata = new ArrayList<RecordMetadata>();
    var producer = getProducer();

    for (var message : messages) {
      var headers = new RecordHeaders();
      message.headers().entrySet().stream()
          .map(
              entry ->
                  new RecordHeader(
                      entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8)))
          .forEach(headers::add);

      ProducerRecord<K, V> record =
          new ProducerRecord<>(
              message.topic() != null ? message.topic() : defaultTopic,
              message.partition(),
              message.key(),
              message.value(),
              headers);

      try {
        var future = producer.send(record);
        metadata.add(future.get()); // Wait for send completion
      } catch (Exception e) {
        log.error("Failed to send message: {}", e.getMessage(), e);
      }
    }

    producer.flush();
    return metadata;
  }

  /**
   * Sends a single message to Kafka.
   *
   * @param message the message to send
   * @return record metadata for the sent message
   */
  public RecordMetadata send(KafkaMessage<K, V> message) {
    var metadata = send(Collections.singletonList(message));
    return metadata.isEmpty() ? null : metadata.get(0);
  }

  @Override
  public void close() {
    lock.lock();
    try {
      String producerKey = getProducerKey(clusterName, keySerializer, valueSerializer);
      KafkaProducer<?, ?> producer = producers.remove(producerKey);
      if (producer != null) {
        producer.close();
        log.info("Closed producer for cluster: {}", clusterName);
      }
    } finally {
      lock.unlock();
    }
  }
}
