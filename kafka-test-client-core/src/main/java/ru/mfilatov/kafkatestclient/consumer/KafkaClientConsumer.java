/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.consumer;

import java.util.Properties;
import lombok.Getter;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * A generic Kafka consumer wrapper that supports configurable key and value deserializers via the
 * provided configuration. Values are exposed as {@code Object} because the actual runtime type is
 * determined by the {@code value.deserializer} in the configuration.
 *
 * @param <K> The type of message keys
 */
@Getter
public class KafkaClientConsumer<K> {
  private final KafkaConsumer<K, Object> consumer;

  /**
   * Creates a new Kafka consumer with the specified configuration. Note: Configuration must include
   * key.deserializer and value.deserializer properties.
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
