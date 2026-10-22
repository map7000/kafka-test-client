/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.unit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscription;

class KafkaMessageSubscriberTest {

  private final KafkaMessageSubscriber<String, String> subscriber =
      new KafkaMessageSubscriber<>("t");

  private static KafkaMessage<String, String> message(String value, String rqUid) {
    return KafkaMessage.<String, String>builder()
        .topic("t")
        .value(value)
        .headers(Map.of("RqUID", rqUid))
        .build();
  }

  @Test
  void storesMessagesItReceives() {
    subscriber.onNext(message("a", "1"));
    subscriber.onNext(message("b", "2"));
    assertThat(subscriber.getMessages()).hasSize(2);
    assertThat(subscriber.getMessages()).allMatch(m -> m.topic().equals("t"));
  }

  @Test
  void rejectsDuplicateMessages() {
    var message = message("a", "1");
    subscriber.onNext(message);
    subscriber.onNext(message);
    assertThat(subscriber.getMessages()).hasSize(1);
  }

  @Test
  void ignoresNullMessages() {
    subscriber.onNext(null);
    assertThat(subscriber.getMessages()).isEmpty();
  }

  @Test
  void predicateFiltersIncomingMessages() {
    subscriber.setPredicate(m -> m.headers().get("RqUID").equals("keep"));
    subscriber.onNext(message("a", "keep"));
    subscriber.onNext(message("b", "drop"));
    assertThat(subscriber.getMessages()).hasSize(1);
    assertThat(subscriber.getMessages().get(0).headers()).containsEntry("RqUID", "keep");
  }

  @Test
  void messageListResetsAfterLimitReached() {
    // 3001 unique messages: the cap (3000) is hit, the stored list is reset,
    // and only the message that triggered the reset survives
    for (int i = 0; i < 3001; i++) {
      subscriber.onNext(message("v" + i, "uid-" + i));
    }
    assertThat(subscriber.getMessages().size()).isEqualTo(1);
  }

  @Test
  void cancelSignalledThroughSubscription() {
    var subscription = new KafkaMessageSubscription<>(subscriber);
    subscription.setTopic("t");
    subscriber.onSubscribe(subscription);
    subscriber.cancel();
    assertThat(subscription.isCancelRequested()).isTrue();
    assertThat(subscriber.getMessages()).isEmpty();
  }

  @Test
  void requestsUnboundedDemandOnSubscribe() {
    var subscription = new KafkaMessageSubscription<>(subscriber);
    subscriber.onSubscribe(subscription);
    // The subscriber must honour the Flow contract by requesting demand up front,
    // otherwise publishers that honour backpressure would deliver nothing.
    assertThat(subscription.hasDemand()).isTrue();
  }
}
