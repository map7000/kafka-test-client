/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.subscriber;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import ru.mfilatov.kafkatestclient.config.KafkaFileConfigProvider;
import ru.mfilatov.kafkatestclient.consumer.KafkaClientConsumer;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

@Slf4j
public class KafkaMessageConsumer implements Runnable {
  private final List<KafkaMessageSubscription> subscriptions;
  private Set<String> topics = new HashSet<>();

  public KafkaMessageConsumer(List<KafkaMessageSubscription> subscriptions) {
    this.subscriptions = subscriptions;
  }

  @Override
  @SneakyThrows
  public void run() {
    try (Consumer<String, String> consumer =
        new KafkaClientConsumer(new KafkaFileConfigProvider().getKafkaConfig("kafka.properties"))
            .getConsumer()) {
      while (!Thread.currentThread().isInterrupted()) {
        subscriptions.removeIf(KafkaMessageSubscription::isCancelRequested);

        if (subscriptions.isEmpty()) {
          TimeUnit.MILLISECONDS.sleep(500);
          continue;
        }

        var currentTopics =
            subscriptions.stream()
                .map(KafkaMessageSubscription::getTopic)
                .collect(Collectors.toSet());

        if (!topics.equals(currentTopics)) {
          topics = new HashSet<>(currentTopics);
          consumer.subscribe(topics);
        }

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(250));

        log.info("{}", records.count());

        for (var record : records) {
          subscriptions.stream()
              .filter(s -> s.getTopic().equals(record.topic()))
              .forEach(
                  a ->
                      a.getSubscriber()
                          .onNext(
                              new KafkaMessage(
                                  record.topic(),
                                  record.partition(),
                                  record.offset(),
                                  getHeaders(record),
                                  record.key(),
                                  record.value())));
        }
        consumer.commitSync();
      }
    }
  }

  public Map<String, String> getHeaders(ConsumerRecord<String, String> message) {
    return Arrays.stream(message.headers().toArray())
        .collect(Collectors.toMap(Header::key, a -> new String(a.value())));
  }
}
