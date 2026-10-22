/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.unit;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import ru.mfilatov.kafkatestclient.processor.StringMessageProcessor;

class StringMessageProcessorTest {

  @Test
  void mapsConsumerRecordIntoMessage() {
    var record = new ConsumerRecord<String, Object>("t", 1, 42L, "k", "v");
    record.headers().add(new RecordHeader("RqUID", "uid".getBytes(StandardCharsets.UTF_8)));

    var message = new StringMessageProcessor().processRecord(record);

    assertThat(message.topic()).isEqualTo("t");
    assertThat(message.partition()).isEqualTo(1);
    assertThat(message.offset()).isEqualTo(42L);
    assertThat(message.key()).isEqualTo("k");
    assertThat(message.value()).isEqualTo("v");
    assertThat(message.headers()).containsEntry("RqUID", "uid");
  }

  @Test
  void decodesRawByteArrayValues() {
    var record =
        new ConsumerRecord<String, Object>("t", 0, 0L, "k", "v".getBytes(StandardCharsets.UTF_8));
    var message = new StringMessageProcessor().processRecord(record);
    assertThat(message.value()).isEqualTo("v");
  }

  @Test
  void decodesUtf8ByteArrayPayloads() {
    var payload = "значение ☕";
    var record =
        new ConsumerRecord<String, Object>(
            "t", 0, 0L, "k", payload.getBytes(StandardCharsets.UTF_8));
    var message = new StringMessageProcessor().processRecord(record);
    assertThat(message.value()).isEqualTo(payload);
  }

  @Test
  void recordsWithoutHeadersDecodeToEmptyHeaderMap() {
    var record = new ConsumerRecord<String, Object>("t", 0, 0L, null, "v");
    var message = new StringMessageProcessor().processRecord(record);
    assertThat(message.headers()).isEmpty();
    assertThat(message.key()).isNull();
  }
}
