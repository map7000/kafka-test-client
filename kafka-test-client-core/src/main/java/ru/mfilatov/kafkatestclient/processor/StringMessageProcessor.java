/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.processor;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

public class StringMessageProcessor implements KafkaMessageProcessor<String, String> {
  @Override
  public KafkaMessage<String, String> processRecord(ConsumerRecord<String, byte[]> record) {
    String value = new String(record.value(), StandardCharsets.UTF_8);
    return new KafkaMessage<String, String>(
        record.topic(),
        record.partition(),
        record.offset(),
        getHeaders(record),
        record.key(),
        value);
  }
}
