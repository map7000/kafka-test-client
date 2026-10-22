/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscription;

class KafkaMessageSubscriptionTest {

  private final KafkaMessageSubscriber<String, String> target = new KafkaMessageSubscriber<>("t");
  private final KafkaMessageSubscription<String, String> subscription =
      new KafkaMessageSubscription<>(target);

  @Test
  void tracksDemandAndDecrements() {
    subscription.request(2);
    assertThat(subscription.hasDemand()).isTrue();
    subscription.decrementDemand();
    assertThat(subscription.hasDemand()).isTrue();
    subscription.decrementDemand();
    assertThat(subscription.hasDemand()).isFalse();
  }

  @Test
  void demandClampsAtMax() {
    subscription.request(Long.MAX_VALUE);
    subscription.request(1);
    subscription.decrementDemand();
    assertThat(subscription.hasDemand()).isTrue();
  }

  @Test
  void requestRejectsNonPositiveViaSubscriberError() {
    var recorder = new ErrorRecordingSubscriber();
    var recording = new KafkaMessageSubscription<>(recorder);
    recording.request(0);
    recording.request(-1L);
    // non-positive requests are reported on the subscriber, not thrown at the caller
    assertThat(recorder.errors).hasSize(2);
    assertThat(recorder.errors)
        .allSatisfy(e -> assertThat(e).isInstanceOf(IllegalArgumentException.class));
    assertThat(recording.hasDemand()).isFalse();
  }

  private static final class ErrorRecordingSubscriber
      implements java.util.concurrent.Flow.Subscriber<KafkaMessage<String, String>> {
    final java.util.List<Throwable> errors = new java.util.ArrayList<>();

    @Override
    public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {}

    @Override
    public void onNext(KafkaMessage<String, String> item) {}

    @Override
    public void onError(Throwable throwable) {
      synchronized (errors) {
        errors.add(throwable);
      }
    }

    @Override
    public void onComplete() {}
  }

  @Test
  void setTopicValidatesInput() {
    assertThatThrownBy(() -> subscription.setTopic(null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> subscription.setTopic("  "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void cancelIsIdempotentAndStopsTopicAssignment() {
    subscription.cancel();
    subscription.cancel();
    assertThat(subscription.isCancelRequested()).isTrue();
    assertThatThrownBy(() -> subscription.setTopic("t")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void subscriberAssociatesTopicOnlyViaOnSubscribe() {
    subscription.setSubscriber(target);
    subscription.setTopic("custom-topic");
    subscription.request(1);
    target.onSubscribe(subscription);
    assertThat(target.getKafkaTopic()).isEqualTo("t");
  }

  @Test
  void publisherIgnoresSecondSubscribeAttempt() {
    var first = new KafkaMessageSubscription<String, String>(target);
    first.setSubscriber(target);
    first.setTopic("t");
    target.onSubscribe(first);
    assertThat(target.getMessages()).isEmpty();

    var second =
        new KafkaMessageSubscription<String, String>(
            new KafkaMessageSubscriber<String, String>("other"));
    target.onSubscribe(second);
    // newer subscription is rejected: original one stays
    assertThat(first.isCancelRequested()).isFalse();
    assertThat(second.isCancelRequested()).isTrue();

    // messages still routed to the retained subscription's subscriber
    first.decrementDemand();
    target.onNext(
        KafkaMessage.<String, String>builder().topic("t").value("m").headers(Map.of()).build());
    assertThat(target.getMessages()).hasSize(1);
  }

  @Test
  void demandCounterIsThreadSafe() throws InterruptedException {
    Thread[] producers = new Thread[2];
    for (int t = 0; t < 2; t++) {
      producers[t] =
          new Thread(
              () -> {
                for (int i = 0; i < 1000; i++) {
                  subscription.request(1);
                }
              });
      producers[t].start();
    }
    for (var t : producers) {
      t.join();
    }
    // both threads' requests must be reflected in demand
    assertThat(subscription.hasDemand()).isTrue();
    for (int i = 0; i < 2000; i++) {
      subscription.decrementDemand();
    }
    assertThat(subscription.hasDemand()).isFalse();
  }
}
