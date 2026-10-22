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

/**
 * A subscriber implementation for consuming Kafka messages using the Reactive Streams pattern. This
 * class maintains a thread-safe list of received messages and supports message filtering through a
 * predicate. It automatically manages message storage limits and subscription lifetime.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Thread-safe message storage using {@link CopyOnWriteArrayList}
 *   <li>Configurable message filtering through predicates
 *   <li>Automatic cleanup of old messages when limit is reached
 *   <li>Automatic subscription cancellation after timeout
 *   <li>Unique subscriber identification
 * </ul>
 *
 * @author Mikhail Filatov
 * @see Flow.Subscriber
 * @see KafkaMessage
 */
@Slf4j
public class KafkaMessageSubscriber<K, V> implements Flow.Subscriber<KafkaMessage<K, V>> {
  /** Maximum number of messages to store before resetting the list */
  private static final int MAX_MESSAGES = 3000;

  /** Maximum runtime in milliseconds (20 minutes) before auto-cancellation */
  private static final long MAX_RUNTIME_MS = 1200000;

  /** The Kafka topic this subscriber is listening to */
  @Getter private final String kafkaTopic;

  /** Unique identifier for this subscriber instance */
  @Getter private final String id = UUID.randomUUID().toString();

  /** Thread-safe list of received messages */
  @Getter private volatile List<KafkaMessage<K, V>> messages = new CopyOnWriteArrayList<>();

  /** Optional predicate for filtering incoming messages */
  @Setter private Predicate<KafkaMessage<K, V>> predicate;

  /** Current subscription instance */
  private volatile KafkaMessageSubscription<K, V> subscription;

  /** Timestamp when this subscriber was created */
  private final long startTime = System.currentTimeMillis();

  /**
   * Creates a new subscriber for the specified Kafka topic.
   *
   * @param kafkaTopic the Kafka topic to subscribe to
   * @throws NullPointerException if kafkaTopic is null
   */
  public KafkaMessageSubscriber(String kafkaTopic) {
    this.kafkaTopic = Objects.requireNonNull(kafkaTopic, "Kafka topic cannot be null");
  }

  /**
   * Handles the subscription process. If a subscription already exists, the new subscription
   * request will be cancelled.
   *
   * @param subscription the subscription to be established
   * @throws ClassCastException if subscription is not a KafkaMessageSubscription
   */
  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    if (this.subscription != null) {
      subscription.cancel();
      return;
    }
    if (subscription instanceof KafkaMessageSubscription) {
      @SuppressWarnings("unchecked")
      KafkaMessageSubscription<K, V> kafkaSubscription =
          (KafkaMessageSubscription<K, V>) subscription;
      this.subscription = kafkaSubscription;
      this.subscription.setTopic(kafkaTopic);
      log.debug("Subscribed to topic: {}", kafkaTopic);
    } else {
      log.error("Subscription is not an instance of KafkaMessageSubscription");
      subscription.cancel();
    }
  }

  /**
   * Processes incoming Kafka messages. Messages are filtered based on the predicate if one is set,
   * and stored in the messages list if accepted.
   *
   * @param message the Kafka message to process
   */
  @Override
  public void onNext(KafkaMessage<K, V> message) {
    try {
      if (shouldCancel()) {
        cancel();
        return;
      }

      if (messages.size() >= MAX_MESSAGES) {
        resetMessages();
      }

      processMessage(message);
    } catch (Exception e) {
      log.error("Error processing message: {}", message, e);
      onError(e);
    }
  }

  /**
   * Handles any errors that occur during message processing.
   *
   * @param throwable the error that occurred
   */
  @Override
  public void onError(Throwable throwable) {
    log.error("Subscription error for topic {}: {}", kafkaTopic, throwable.getMessage());
  }

  /** Called when the subscription is completed normally. */
  @Override
  public void onComplete() {
    log.info("Subscription completed for topic: {}", kafkaTopic);
  }

  /** Cancels the current subscription if one exists. */
  public void cancel() {
    if (subscription != null) {
      subscription.cancel();
      log.info("Cancelled subscription for topic: {}", kafkaTopic);
    }
  }

  /**
   * Checks if the subscription should be cancelled based on runtime duration.
   *
   * @return true if the subscription has exceeded its maximum runtime
   */
  private boolean shouldCancel() {
    return System.currentTimeMillis() - startTime > MAX_RUNTIME_MS;
  }

  /** Resets the message list when the maximum size is reached. */
  private void resetMessages() {
    log.warn("Message limit reached ({}), resetting message list", MAX_MESSAGES);
    messages = new CopyOnWriteArrayList<>();
  }

  /**
   * Processes a single Kafka message, applying the predicate filter if one exists. Duplicate
   * messages are ignored.
   *
   * @param message the message to process
   */
  private void processMessage(KafkaMessage<K, V> message) {
    if (message == null || messages.contains(message)) {
      return;
    }

    if (message instanceof KafkaMessage<?, ?>) {
      KafkaMessage<K, V> typedMessage = (KafkaMessage<K, V>) message;
      if (predicate == null || predicate.test(typedMessage)) {
        messages.add(typedMessage);
        log.debug("Added message: {} for topic: {}", typedMessage.key(), kafkaTopic);
      }
    }
  }
}
