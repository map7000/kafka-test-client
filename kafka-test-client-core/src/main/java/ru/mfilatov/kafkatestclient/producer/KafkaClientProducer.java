/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.producer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
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
 * <p>Underlying {@link KafkaProducer} instances are shared between wrappers with the same {@code
 * (cluster, keySerializer, valueSerializer)} key and protected by reference counting: each wrapper
 * holds one reference and closing a wrapper releases it. The real producer is only closed when the
 * last wrapper for the key is closed, so multiple test classes can use the same cluster
 * concurrently without one close() breaking another.
 *
 * @param <K> The type of message keys
 * @param <V> The type of message values
 */
@Slf4j
@Getter
public class KafkaClientProducer<K, V> implements AutoCloseable {
  private final String clusterName;
  private final String defaultTopic;

  private static final ConcurrentMap<String, SharedProducer> producers = new ConcurrentHashMap<>();

  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final SharedProducer shared;

  /**
   * A shared KafkaProducer together with the number of wrappers currently referencing it.
   *
   * <p>Held as an identity object by each wrapper: ref counts are only ever mutated inside
   * ConcurrentHashMap compute functions, which are mutually exclusive per key, so the count and the
   * map entry cannot get out of sync.
   */
  private static final class SharedProducer {
    private final Object producer;
    private final AtomicInteger refs = new AtomicInteger();

    private SharedProducer(Object producer) {
      this.producer = producer;
    }
  }

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

    String producerKey = getProducerKey(clusterName, keySerializer, valueSerializer);
    this.shared =
        producers.compute(
            producerKey,
            (key, existing) -> {
              SharedProducer candidate =
                  (existing != null) ? existing : new SharedProducer(createProducer(config));
              candidate.refs.incrementAndGet();
              log.info(
                  "Producer shared for cluster: {} (refs: {}), types K: {}, V: {}",
                  clusterName,
                  candidate.refs.get(),
                  keySerializer.getClass().getSimpleName(),
                  valueSerializer.getClass().getSimpleName());
              return candidate;
            });
  }

  @SuppressWarnings("unchecked")
  private KafkaProducer<K, V> getProducer() {
    return (KafkaProducer<K, V>) shared.producer;
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
   * Sends multiple messages to Kafka. All messages are handed to the producer first and awaited
   * afterwards, so the producer can batch them; a failure to send any message throws.
   *
   * @param messages list of messages to send
   * @return list of record metadata for sent messages, aligned with the input order
   * @throws IllegalStateException if sending any message failed
   */
  public List<RecordMetadata> send(List<KafkaMessage<K, V>> messages) {
    List<KafkaMessage<K, V>> messagesToSend = messages == null ? List.of() : messages;
    var producer = getProducer();

    var futures = new ArrayList<Future<RecordMetadata>>(messagesToSend.size());
    for (var message : messagesToSend) {
      futures.add(producer.send(toProducerRecord(message)));
    }

    var metadata = new ArrayList<RecordMetadata>(futures.size());
    try {
      for (var future : futures) {
        metadata.add(future.get()); // Wait for send completion
      }
    } catch (Exception e) {
      if (e instanceof InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("Failed to send message(s) to Kafka", e);
    }
    return metadata;
  }

  /**
   * Builds a {@link ProducerRecord} from a {@link KafkaMessage}, falling back to the default topic
   * when the message topic is null.
   */
  private ProducerRecord<K, V> toProducerRecord(KafkaMessage<K, V> message) {
    var headers = new RecordHeaders();
    if (message.headers() != null) {
      message.headers().entrySet().stream()
          .filter(entry -> entry.getValue() != null)
          .map(
              entry ->
                  new RecordHeader(
                      entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8)))
          .forEach(headers::add);
    }

    return new ProducerRecord<>(
        message.topic() != null ? message.topic() : defaultTopic,
        message.partition(),
        message.key(),
        message.value(),
        headers);
  }

  /**
   * Sends a single message to Kafka.
   *
   * @param message the message to send
   * @return record metadata for the sent message
   */
  public RecordMetadata send(KafkaMessage<K, V> message) {
    Objects.requireNonNull(message, "Message cannot be null");
    var metadata = send(List.of(message));
    return metadata.get(0);
  }

  /**
   * Releases this wrapper's reference to the shared producer. The underlying KafkaProducer is
   * closed only when the last wrapper for the same (cluster, serializers) key is released, so it is
   * safe for multiple wrappers to coexist.
   */
  @Override
  public void close() {
    String producerKey = getProducerKey(clusterName, keySerializer, valueSerializer);
    producers.computeIfPresent(
        producerKey,
        (key, candidate) -> {
          if (candidate != shared) {
            // Our shared producer was fully closed and replaced by a newer one already.
            return candidate;
          }
          if (candidate.refs.decrementAndGet() == 0) {
            log.info("Last reference released, closing producer for cluster: {}", clusterName);
            ((KafkaProducer<?, ?>) candidate.producer).close();
            return null; // Remove the entry from the map
          }
          return candidate;
        });
  }
}
