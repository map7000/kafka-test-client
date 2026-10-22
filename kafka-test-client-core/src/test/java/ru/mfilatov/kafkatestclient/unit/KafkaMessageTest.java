/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.unit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

class KafkaMessageTest {

  private final KafkaMessage<String, String> full =
      KafkaMessage.<String, String>builder()
          .topic("t")
          .partition(1)
          .offset(2L)
          .headers(Map.of("RqUID", "uid"))
          .key("k")
          .value("v")
          .build();

  @Test
  void builderAssignsAllFields() {
    assertThat(full.topic()).isEqualTo("t");
    assertThat(full.partition()).isEqualTo(1);
    assertThat(full.offset()).isEqualTo(2L);
    assertThat(full.headers()).containsEntry("RqUID", "uid");
    assertThat(full.key()).isEqualTo("k");
    assertThat(full.value()).isEqualTo("v");
  }

  @Test
  void topicIsOptionalForOutboundMessages() {
    var noTopic =
        KafkaMessage.<String, String>builder().value("x").headers(Map.of("RqUID", "u")).build();
    assertThat(noTopic.topic()).isNull();
    assertThat(noTopic.value()).isEqualTo("x");
  }

  @Test
  void mapValuePreservesEnvelope() {
    var mapped = full.mapValue(v -> v + "!");
    assertThat(mapped.topic()).isEqualTo("t");
    assertThat(mapped.partition()).isEqualTo(1);
    assertThat(mapped.offset()).isEqualTo(2L);
    assertThat(mapped.headers()).containsEntry("RqUID", "uid");
    assertThat(mapped.key()).isEqualTo("k");
    assertThat(mapped.value()).isEqualTo("v!");
  }

  @Test
  void mapKeyPreservesEnvelopeAndValue() {
    var mapped = full.mapKey(k -> k.toUpperCase());
    assertThat(mapped.key()).isEqualTo("K");
    assertThat(mapped.value()).isEqualTo("v");
    assertThat(mapped.topic()).isEqualTo("t");
  }

  @Test
  void recordEqualityWorksForDeduplication() {
    var copy =
        KafkaMessage.<String, String>builder()
            .topic("t")
            .partition(1)
            .offset(2L)
            .headers(Map.of("RqUID", "uid"))
            .key("k")
            .value("v")
            .build();
    assertThat(copy).isEqualTo(full);
  }
}
