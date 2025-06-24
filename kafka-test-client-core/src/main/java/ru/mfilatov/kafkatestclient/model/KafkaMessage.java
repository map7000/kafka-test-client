/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.model;

import java.util.Map;
import lombok.Builder;

/**
 * Represents a Kafka message with generic key and value types. This record provides an immutable
 * representation of a Kafka message with its metadata and content.
 *
 * @param <K> The type of the message key
 * @param <V> The type of the message value
 */
@Builder
public record KafkaMessage<K, V>(
    String topic, Integer partition, Long offset, Map<String, String> headers, K key, V value) {

  /**
   * Creates a new KafkaMessage instance with the specified parameters.
   *
   * @param topic the Kafka topic
   * @param partition the partition number
   * @param offset the message offset in the partition
   * @param headers the message headers
   * @param key the message key of type K
   * @param value the message value of type V
   * @throws NullPointerException if topic is null
   */
  public KafkaMessage {
    java.util.Objects.requireNonNull(topic, "Topic cannot be null");
  }

  /**
   * Creates a copy of this message with a transformed value.
   *
   * @param <T> the target type for the value
   * @param transformer the function to transform the value
   * @return a new KafkaMessage with the transformed value
   */
  public <T> KafkaMessage<K, T> mapValue(java.util.function.Function<V, T> transformer) {
    return new KafkaMessage<>(topic, partition, offset, headers, key, transformer.apply(value));
  }

  /**
   * Creates a copy of this message with a transformed key.
   *
   * @param <T> the target type for the key
   * @param transformer the function to transform the key
   * @return a new KafkaMessage with the transformed key
   */
  public <T> KafkaMessage<T, V> mapKey(java.util.function.Function<K, T> transformer) {
    return new KafkaMessage<>(topic, partition, offset, headers, transformer.apply(key), value);
  }
}
