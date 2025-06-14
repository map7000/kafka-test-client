/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import ru.mfilatov.kafkatestclient.config.KafkaFileConfigProvider;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.producer.KafkaClientProducer;

@Slf4j
public class ProducerTest {
  @Test
  void sendMessageTest() {
    var config = new KafkaFileConfigProvider().getKafkaConfig("kafka.properties");
    log.info(config.getProperty("kafka.cluster.name"));
    KafkaClientProducer producer = new KafkaClientProducer(config, "test");
    var message =
        KafkaMessage.builder()
            .key(UUID.randomUUID().toString())
            .value("1")
            .headers(new HashMap<>())
            .build();
    var callback = producer.send(message);
    assertThat(callback).isNotNull();
    assertThat(callback.hasOffset()).as("Message sent successfully").isTrue();
  }
}
