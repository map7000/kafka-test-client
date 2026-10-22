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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.producer.KafkaClientProducer;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber;

@Slf4j
public class KafkaSteps {
  @Getter protected final KafkaMessageSubscriber<String, String> subscriber;
  @Getter protected final KafkaClientProducer<String, String> producer;

  public KafkaSteps(
      KafkaMessageSubscriber<String, String> subscriber,
      KafkaClientProducer<String, String> producer) {
    this.subscriber = subscriber;
    this.producer = producer;
  }

  private KafkaMessage<String, String> getMessage(String rqUID) {
    // rqUID first: header values of unrelated flood traffic may not contain RqUID at all
    return subscriber.getMessages().stream()
        .filter(m -> rqUID.equals(m.headers().get("RqUID")))
        .findFirst()
        .orElse(null);
  }

  public KafkaMessage<String, String> awaitMessage(String rqUID, long timeout) {
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

  public RecordMetadata send(KafkaMessage<String, String> message) {
    if (Objects.isNull(producer)) {
      log.error("No producer");
      return null;
    }
    return producer.send(message);
  }

  public List<RecordMetadata> send(List<KafkaMessage<String, String>> message) {
    if (Objects.isNull(producer)) {
      log.error("No producer");
      return null;
    }
    return producer.send(message);
  }
}
