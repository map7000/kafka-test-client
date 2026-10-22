/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.processor;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

/**
 * Decodes consumer records into {@code String} values. Handles both raw {@code byte[]} payloads and
 * already deserialized {@code String} values, since the actual runtime type depends on the {@code
 * value.deserializer} configured for the consumer.
 */
public class StringMessageProcessor implements KafkaMessageProcessor<String, String> {
  @Override
  public KafkaMessage<String, String> processRecord(ConsumerRecord<String, Object> record) {
    Object raw = record.value();
    String value =
        raw == null
            ? null
            : raw instanceof byte[] bytes
                ? new String(bytes, StandardCharsets.UTF_8)
                : raw.toString();
    return new KafkaMessage<>(
        record.topic(),
        record.partition(),
        record.offset(),
        getHeaders(record),
        record.key(),
        value);
  }
}
