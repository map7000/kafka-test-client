/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.subscriber;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

/**
 * A generic subscription implementation for Kafka messages. Manages the relationship between a
 * publisher and subscriber with type-safe message handling.
 *
 * @param <K> The type of message keys
 * @param <V> The type of message values
 */
@Slf4j
@NoArgsConstructor
public class KafkaMessageSubscription<K, V> implements Flow.Subscription {
  private Flow.Subscriber<? super KafkaMessage<K, V>> subscriber;

  // Method to set the subscriber
  public void setSubscriber(Flow.Subscriber<? super KafkaMessage<K, V>> subscriber) {
    this.subscriber = subscriber;
  }

  /** Maximum number of messages that can be requested */
  private static final long MAX_DEMAND = Long.MAX_VALUE;

  /** Atomic flag for tracking cancellation state */
  private final AtomicBoolean cancelled = new AtomicBoolean(false);

  /** Counter for tracking demand from the subscriber */
  private final AtomicLong demand = new AtomicLong(0);

  /** The Kafka topic this subscription is listening to */
  @Getter private volatile String topic;

  /**
   * Creates a new subscription for the specified subscriber.
   *
   * @param subscriber the subscriber to receive messages
   * @throws NullPointerException if subscriber is null
   */
  public KafkaMessageSubscription(Flow.Subscriber<? super KafkaMessage<K, V>> subscriber) {
    this.subscriber = java.util.Objects.requireNonNull(subscriber, "Subscriber cannot be null");
  }

  /**
   * Sets the Kafka topic for this subscription.
   *
   * @param topic the topic to subscribe to
   * @throws IllegalArgumentException if topic is null or empty
   * @throws IllegalStateException if subscription is cancelled
   */
  public void setTopic(String topic) {
    if (topic == null || topic.trim().isEmpty()) {
      throw new IllegalArgumentException("Topic cannot be null or empty");
    }
    if (isCancelRequested()) {
      throw new IllegalStateException("Cannot set topic on cancelled subscription");
    }
    this.topic = topic;
    log.debug("Set topic to: {} for subscriber: {}", topic, subscriber);
  }

  /**
   * Requests a number of messages to be delivered to the subscriber.
   *
   * @param n the maximum number of messages to receive
   * @throws IllegalArgumentException if n is negative or zero
   */
  @Override
  public void request(long n) {
    if (n <= 0) {
      subscriber.onError(
          new IllegalArgumentException("Request for negative or zero elements: " + n));
      return;
    }

    if (!isCancelRequested()) {
      demand.updateAndGet(current -> current + n >= MAX_DEMAND ? MAX_DEMAND : current + n);
      log.trace("Updated demand to {} for subscriber: {}", demand.get(), subscriber);
    }
  }

  /** Cancels this subscription. Once cancelled, no more messages will be delivered. */
  @Override
  public void cancel() {
    if (cancelled.compareAndSet(false, true)) {
      log.debug("Cancelled subscription for topic: {} and subscriber: {}", topic, subscriber);
    }
  }

  /**
   * Checks if this subscription has been cancelled.
   *
   * @return true if the subscription is cancelled, false otherwise
   */
  public boolean isCancelRequested() {
    return cancelled.get();
  }

  /**
   * Checks if the subscriber has requested more messages.
   *
   * @return true if there is pending demand, false otherwise
   */
  public boolean hasDemand() {
    return demand.get() > 0;
  }

  /** Decrements the demand counter when a message is delivered. */
  public void decrementDemand() {
    demand.updateAndGet(current -> current > 0 ? current - 1 : 0);
  }
}
