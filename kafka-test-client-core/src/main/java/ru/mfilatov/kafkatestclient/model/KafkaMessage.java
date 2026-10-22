/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.model;

import java.util.Map;

/**
 * Represents a Kafka message with generic key and value types. This record provides an immutable
 * representation of a Kafka message with its metadata and content.
 *
 * @param <K> The type of the message key
 * @param <V> The type of the message value
 */
public record KafkaMessage<K, V>(
    String topic, Integer partition, Long offset, Map<String, String> headers, K key, V value) {

  public static <K, V> KafkaMessageBuilder<K, V> builder() {
    return new KafkaMessageBuilder<>();
  }

  public static class KafkaMessageBuilder<K, V> {
    private String topic;
    private Integer partition;
    private Long offset;
    private Map<String, String> headers;
    private K key;
    private V value;

    public KafkaMessageBuilder<K, V> topic(String topic) {
      this.topic = topic;
      return this;
    }

    public KafkaMessageBuilder<K, V> partition(Integer partition) {
      this.partition = partition;
      return this;
    }

    public KafkaMessageBuilder<K, V> offset(Long offset) {
      this.offset = offset;
      return this;
    }

    public KafkaMessageBuilder<K, V> headers(Map<String, String> headers) {
      this.headers = headers;
      return this;
    }

    public KafkaMessageBuilder<K, V> key(K key) {
      this.key = key;
      return this;
    }

    public KafkaMessageBuilder<K, V> value(V value) {
      this.value = value;
      return this;
    }

    public KafkaMessage<K, V> build() {
      return new KafkaMessage<>(topic, partition, offset, headers, key, value);
    }
  }

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
