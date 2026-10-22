/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.subscriber;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
 * predicate. It keeps storage bounded by dropping the oldest message when the limit is reached and
 * automatically cancels the subscription after a configurable lifetime.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Thread-safe message storage using {@link CopyOnWriteArrayList}
 *   <li>Configurable message filtering through predicates
 *   <li>Bounded memory: the oldest message is dropped when the limit is reached (no wholesale list
 *       reset, so in-flight replies observed through {@link #getMessages()} are never erased)
 *   <li>O(1) duplicate detection via a seen-set of recent messages
 *   <li>Automatic unbounded-demand request on subscribe (Flow contract) and automatic subscription
 *       cancellation after timeout
 *   <li>Unique subscriber identification
 * </ul>
 *
 * @author Mikhail Filatov
 * @see Flow.Subscriber
 * @see KafkaMessage
 */
@Slf4j
public class KafkaMessageSubscriber<K, V> implements Flow.Subscriber<KafkaMessage<K, V>> {
  /** Default maximum number of messages to store before dropping the oldest ones */
  public static final int DEFAULT_MAX_MESSAGES = 3000;

  /** Default maximum runtime in milliseconds (20 minutes) before auto-cancellation */
  public static final long DEFAULT_MAX_RUNTIME_MS = 1200000L;

  /** Unbounded demand requested on subscription (see Flow.Subscription contract) */
  private static final long MAX_DEMAND = Long.MAX_VALUE;

  /** The Kafka topic this subscriber is listening to */
  @Getter private final String kafkaTopic;

  /** Maximum number of messages to store before dropping the oldest ones */
  private final int maxMessages;

  /** Maximum runtime in milliseconds before auto-cancellation */
  private final long maxRuntimeMs;

  /** Unique identifier for this subscriber instance */
  @Getter private final String id = UUID.randomUUID().toString();

  /** Thread-safe ordered list of received messages */
  @Getter private final List<KafkaMessage<K, V>> messages = new CopyOnWriteArrayList<>();

  /** Messages already stored or filtered, used for O(1) duplicate detection */
  private final Set<KafkaMessage<K, V>> seen = ConcurrentHashMap.newKeySet();

  /** Optional predicate for filtering incoming messages */
  @Setter private Predicate<KafkaMessage<K, V>> predicate;

  /** Current subscription instance */
  private volatile KafkaMessageSubscription<K, V> subscription;

  /** Timestamp when this subscriber was created */
  private final long startTime = System.currentTimeMillis();

  /**
   * Creates a new subscriber for the specified Kafka topic with default limits.
   *
   * @param kafkaTopic the Kafka topic to subscribe to
   * @throws NullPointerException if kafkaTopic is null
   */
  public KafkaMessageSubscriber(String kafkaTopic) {
    this(kafkaTopic, DEFAULT_MAX_MESSAGES, DEFAULT_MAX_RUNTIME_MS);
  }

  /**
   * Creates a new subscriber with configurable storage and runtime limits.
   *
   * @param kafkaTopic the Kafka topic to subscribe to
   * @param maxMessages maximum number of messages to store
   * @param maxRuntimeMs maximum runtime before auto-cancellation, in milliseconds
   * @throws NullPointerException if kafkaTopic is null
   */
  public KafkaMessageSubscriber(String kafkaTopic, int maxMessages, long maxRuntimeMs) {
    this.kafkaTopic = Objects.requireNonNull(kafkaTopic, "Kafka topic cannot be null");
    this.maxMessages = maxMessages;
    this.maxRuntimeMs = maxRuntimeMs;
  }

  /**
   * Handles the subscription process. If a subscription already exists, the new subscription
   * request will be cancelled.
   *
   * @param subscription the subscription to be established
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
      if (kafkaSubscription.getTopic() == null) {
        kafkaSubscription.setTopic(kafkaTopic);
      }
      // Honor the Flow contract: unbounded demand up front, since delivery is driven by the
      // consumer loop. Backpressure is applied at the subscription level via hasDemand().
      kafkaSubscription.request(MAX_DEMAND);
      log.debug("Subscribed to topic: {}", kafkaTopic);
    } else {
      log.error("Subscription is not an instance of KafkaMessageSubscription");
      subscription.cancel();
    }
  }

  /**
   * Processes incoming Kafka messages. Messages are filtered based on the predicate if one is set,
   * and stored in the messages list if accepted. The list never exceeds {@link #maxMessages}:
   * storing another message drops the oldest one.
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
    return System.currentTimeMillis() - startTime > maxRuntimeMs;
  }

  /**
   * Processes a single Kafka message, applying the predicate filter if one exists. Duplicate
   * messages are ignored via the seen-set; the oldest stored message is dropped first once {@link
   * #maxMessages} is reached.
   *
   * @param message the message to process
   */
  private void processMessage(KafkaMessage<K, V> message) {
    if (message == null || seen.contains(message)) {
      return;
    }

    if (predicate != null && !predicate.test(message)) {
      return;
    }

    while (messages.size() >= maxMessages) {
      dropOldest();
    }
    seen.add(message);
    messages.add(message);
    log.debug("Added message: {} for topic: {}", message.key(), kafkaTopic);
  }

  /** Removes the oldest stored message (and its seen-set entry) to make room for new ones. */
  private void dropOldest() {
    KafkaMessage<K, V> oldest = messages.remove(0);
    seen.remove(oldest);
    log.debug("Dropped oldest message (limit {} reached) for topic: {}", maxMessages, kafkaTopic);
  }
}
