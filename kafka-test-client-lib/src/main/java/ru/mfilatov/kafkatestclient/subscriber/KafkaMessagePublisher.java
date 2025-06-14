/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.subscriber;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import lombok.SneakyThrows;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

public class KafkaMessagePublisher implements Flow.Publisher<KafkaMessage> {
  private static final List<KafkaMessageSubscription> subscriptions =
      Collections.synchronizedList(new ArrayList<>());
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final Future<?> consumer = executor.submit(new KafkaMessageConsumer(subscriptions));

  @Override
  @SneakyThrows
  public synchronized void subscribe(Flow.Subscriber<? super KafkaMessage> subscriber) {
    var subscription = new KafkaMessageSubscription(subscriber);
    subscriber.onSubscribe(subscription);
    subscriptions.add(subscription);
  }
}
