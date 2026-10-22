/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.processor;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

/**
 * Processes Kafka consumer records and converts them into KafkaMessage objects. Handles message
 * deserialization and transformation.
 *
 * @param <K> The type of the message key
 * @param <V> The type of the message value
 */
public interface KafkaMessageProcessor<K, V> {
  /**
   * Processes a Kafka consumer record and converts it to a KafkaMessage.
   *
   * @param record the Kafka consumer record to process
   * @return the processed KafkaMessage
   * @throws MessageProcessingException if processing fails
   */
  KafkaMessage<K, V> processRecord(ConsumerRecord<K, byte[]> record);

  /**
   * Extracts headers from a Kafka consumer record.
   *
   * @param message the consumer record
   * @return map of header key-value pairs
   */
  default Map<String, String> getHeaders(ConsumerRecord<K, byte[]> message) {
    return Arrays.stream(message.headers().toArray())
        .collect(Collectors.toMap(Header::key, header -> new String(header.value())));
  }
}
