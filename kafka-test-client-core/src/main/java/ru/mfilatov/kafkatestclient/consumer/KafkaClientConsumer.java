/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.consumer;

import java.util.Properties;
import lombok.Getter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * A generic Kafka consumer wrapper that handles raw byte array messages. Supports configurable key
 * type while maintaining raw byte array values for flexible message processing.
 *
 * @param <K> The type of message keys
 */
@Getter
public class KafkaClientConsumer<K> {
  private final KafkaConsumer<K, byte[]> consumer;

  /**
   * Creates a new Kafka consumer with the specified configuration and key deserializer.
   *
   * @param config the Kafka consumer configuration
   * @param keyDeserializer deserializer for message keys
   */
  public KafkaClientConsumer(Properties config, Deserializer<K> keyDeserializer) {
    this.consumer = new KafkaConsumer<>(config, keyDeserializer, new ByteArrayDeserializer());
  }

  /**
   * Creates a new Kafka consumer with the specified configuration. Note: Configuration must include
   * key.deserializer property.
   *
   * @param config the Kafka consumer configuration
   */
  public KafkaClientConsumer(Properties config) {
    this.consumer = new KafkaConsumer<>(config);
  }

  /** Closes the underlying Kafka consumer. */
  public void close() {
    if (consumer != null) {
      consumer.close();
    }
  }
}
