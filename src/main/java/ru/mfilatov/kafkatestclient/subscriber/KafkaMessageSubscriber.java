/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.subscriber;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.function.Predicate;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

@Slf4j
public class KafkaMessageSubscriber implements Flow.Subscriber<KafkaMessage> {
  @Getter private final String kafkaTopic;
  @Getter private List<KafkaMessage> messages = new CopyOnWriteArrayList<>();
  @Getter private final String id = UUID.randomUUID().toString();
  @Setter private Predicate<KafkaMessage> predicate;
  private KafkaMessageSubscription subscription;

  private final long startTime = System.currentTimeMillis();

  public KafkaMessageSubscriber(String kafkaTopic) {
    this.kafkaTopic = kafkaTopic;
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    this.subscription = (KafkaMessageSubscription) subscription;
    this.subscription.setTopic(kafkaTopic);
  }

  @Override
  public void onNext(KafkaMessage message) {
    log.info("Received message: {}", message);
    if (messages.size() > 3000) {
      this.messages = new CopyOnWriteArrayList<>();
    }

    if (System.currentTimeMillis() - startTime > 1200000) {
      cancel();
    }

    if (!messages.contains(message)) {
      if (Objects.nonNull(predicate)) {
        if (predicate.test(message)) {
          messages.add(message);
        }
      } else messages.add(message);
    }
  }

  @Override
  public void onError(Throwable throwable) {}

  @Override
  public void onComplete() {}

  public void cancel() {
    subscription.cancel();
  }
}
