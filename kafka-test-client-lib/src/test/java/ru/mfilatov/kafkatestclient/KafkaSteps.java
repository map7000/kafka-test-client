/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.producer.KafkaClientProducer;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber;

@Slf4j
public class KafkaSteps {
  @Getter protected final KafkaMessageSubscriber subscriber;
  @Getter protected final KafkaClientProducer producer;

  public KafkaSteps(KafkaMessageSubscriber subscriber, KafkaClientProducer producer) {
    this.subscriber = subscriber;
    this.producer = producer;
  }

  @SneakyThrows
  private KafkaMessage getMessage(String rqUID) {
    return subscriber.getMessages().stream()
        .filter(m -> m.headers().get("RqUID").equals(rqUID))
        .findFirst()
        .orElse(null);
  }

  @SneakyThrows
  public KafkaMessage awaitMessage(String rqUID, long timeout) {
    await()
        .timeout(timeout, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(getMessage(rqUID)).isNotNull());
    return getMessage(rqUID);
  }

  public void closeSubscriber() {
    if (Objects.nonNull(subscriber)) {
      subscriber.cancel();
    }
  }

  public RecordMetadata send(KafkaMessage message) {
    if (Objects.isNull(producer)) {
      log.error("No producer");
      return null;
    }
    return producer.send(message);
  }

  public List<RecordMetadata> send(List<KafkaMessage> message) {
    if (Objects.isNull(producer)) {
      log.error("No producer");
      return null;
    }
    return producer.send(message);
  }
}
