/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.subscriber;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import ru.mfilatov.kafkatestclient.config.KafkaFileConfigProvider;
import ru.mfilatov.kafkatestclient.consumer.KafkaClientConsumer;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.processor.KafkaMessageProcessor;

/**
 * A Kafka message consumer that manages message consumption and distribution to subscribers. This
 * class implements the Runnable interface to run in a separate thread and handles the lifecycle of
 * Kafka consumer operations, subscription management, and message processing.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Dynamic topic subscription management
 *   <li>Automatic cleanup of cancelled subscriptions
 *   <li>Configurable polling timeouts
 *   <li>Thread-safe operation
 *   <li>Graceful shutdown handling
 * </ul>
 *
 * @author Mikhail Filatov
 * @see Runnable
 * @see KafkaMessage
 */
@Slf4j
public class KafkaMessageConsumer<K, V> implements Runnable {
  /** Timeout duration for polling Kafka messages in milliseconds */
  private static final long POLL_TIMEOUT_MS = 250;

  /** Sleep duration when no messages are available in milliseconds */
  private static final long EMPTY_POLL_SLEEP_MS = 500;

  /** Thread-safe list of active message subscriptions */
  private final List<KafkaMessageSubscription<K, V>> subscriptions;

  /** Current set of subscribed Kafka topics */
  private volatile Set<String> topics = new HashSet<>();

  /** Flag indicating whether the consumer is running */
  private volatile boolean isRunning = true;

  /** Whether the underlying consumer currently has subscribed topics */
  private boolean subscribed = false;

  /** Processor for Kafka messages */
  private final KafkaMessageProcessor<K, V> processor;

  /**
   * Optional explicit Kafka consumer configuration. When null, configuration is loaded from the
   * {@code kafka.properties} file on the classpath.
   */
  private final Properties config;

  /**
   * Creates a new Kafka message consumer with the specified subscriptions and processor. Kafka
   * configuration is loaded from the {@code kafka.properties} file on the classpath.
   *
   * @param subscriptions list of subscriptions to manage
   * @param processor message processor instance
   * @throws NullPointerException if subscriptions or processor is null
   */
  public KafkaMessageConsumer(
      List<KafkaMessageSubscription<K, V>> subscriptions, KafkaMessageProcessor<K, V> processor) {
    this(subscriptions, processor, null);
  }

  /**
   * Creates a new Kafka message consumer with the specified subscriptions, processor and explicit
   * Kafka configuration.
   *
   * @param subscriptions list of subscriptions to manage
   * @param processor message processor instance
   * @param config explicit Kafka configuration, null to load from {@code kafka.properties}
   * @throws NullPointerException if subscriptions or processor is null
   */
  public KafkaMessageConsumer(
      List<KafkaMessageSubscription<K, V>> subscriptions,
      KafkaMessageProcessor<K, V> processor,
      Properties config) {
    this.subscriptions = Objects.requireNonNull(subscriptions, "Subscriptions list cannot be null");
    this.processor = Objects.requireNonNull(processor, "Message processor cannot be null");
    this.config = config;
  }

  /**
   * Main consumer loop that processes Kafka messages and distributes them to subscribers. Handles
   * errors gracefully and supports clean shutdown.
   */
  @Override
  public void run() {
    try (Consumer<K, Object> consumer = createConsumer()) {
      while (isRunning && !Thread.currentThread().isInterrupted()) {
        try {
          processSubscriptions(consumer);
        } catch (Exception e) {
          log.error("Error processing subscriptions", e);
          sleep(EMPTY_POLL_SLEEP_MS);
        }
      }
    } catch (Exception e) {
      log.error("Fatal error in message consumer", e);
      throw new RuntimeException("Failed to process messages", e);
    }
  }

  /**
   * Creates a new Kafka consumer using the explicit configuration when provided, otherwise from the
   * {@code kafka.properties} file.
   *
   * @return configured Kafka consumer instance
   */
  private Consumer<K, Object> createConsumer() {
    if (config != null) {
      return new KafkaClientConsumer<K>(config).getConsumer();
    }
    return new KafkaClientConsumer<K>(
            new KafkaFileConfigProvider().getKafkaConfig("kafka.properties"))
        .getConsumer();
  }

  /**
   * Processes subscriptions by cleaning up cancelled ones and handling active ones.
   *
   * @param consumer the Kafka consumer instance
   */
  private void processSubscriptions(Consumer<K, Object> consumer) {
    cleanupCancelledSubscriptions();
    updateTopicSubscriptions(consumer);

    if (subscriptions.isEmpty()) {
      sleep(EMPTY_POLL_SLEEP_MS);
      return;
    }

    pollAndProcessRecords(consumer);
  }

  /** Removes cancelled subscriptions from the subscription list. */
  private void cleanupCancelledSubscriptions() {
    subscriptions.removeIf(KafkaMessageSubscription::isCancelRequested);
  }

  /**
   * Updates Kafka consumer topic subscriptions based on current subscriptions.
   *
   * @param consumer the Kafka consumer instance
   */
  private void updateTopicSubscriptions(Consumer<K, Object> consumer) {
    Set<String> currentTopics =
        subscriptions.stream().map(KafkaMessageSubscription::getTopic).collect(Collectors.toSet());

    if (currentTopics.isEmpty()) {
      if (subscribed) {
        // Leave the group explicitly: without poll() there are no heartbeats, so the consumer
        // would remain a "zombie" group member for the whole session timeout and stall the next
        // subscription's rebalance for that duration.
        consumer.unsubscribe();
        subscribed = false;
        topics = new HashSet<>();
        log.info("No active subscriptions: consumer unsubscribed and left the group");
      }
      return;
    }

    if (!topics.equals(currentTopics)) {
      topics = new HashSet<>(currentTopics);
      consumer.subscribe(topics);
      subscribed = true;
      log.info("Updated topic subscriptions: {}", topics);
    }
  }

  /**
   * Polls for new records and processes them if available.
   *
   * @param consumer the Kafka consumer instance
   */
  private void pollAndProcessRecords(Consumer<K, Object> consumer) {
    ConsumerRecords<K, Object> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

    if (!records.isEmpty()) {
      log.debug("Received {} records", records.count());
      processRecords(records);
      consumer.commitSync();
    }
  }

  /**
   * Processes received Kafka records and distributes them to subscribers.
   *
   * @param records the records to process
   */
  private void processRecords(ConsumerRecords<K, Object> records) {
    for (ConsumerRecord<K, Object> record : records) {
      try {
        Map<String, String> headers = getHeaders(record);
        log.debug("Record headers: {}", headers);
        KafkaMessage<K, V> message = processor.processRecord(record);
        notifySubscribers(record.topic(), message);
      } catch (Exception e) {
        log.error("Failed to process record: {}", record, e);
      }
    }
  }

  /**
   * Notifies all subscribers for a specific topic about a new message.
   *
   * @param topic the message topic
   * @param message the Kafka message
   */
  private void notifySubscribers(String topic, KafkaMessage<K, V> message) {
    subscriptions.stream()
        .filter(s -> s.getTopic().equals(topic))
        .forEach(s -> s.getSubscriber().onNext(message));
  }

  /**
   * Safely sleeps for the specified duration.
   *
   * @param milliseconds the time to sleep in milliseconds
   */
  private void sleep(long milliseconds) {
    try {
      TimeUnit.MILLISECONDS.sleep(milliseconds);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Initiates a graceful shutdown of the consumer. */
  public void shutdown() {
    isRunning = false;
  }

  /**
   * Extracts headers from a Kafka consumer record.
   *
   * @param message the consumer record
   * @return map of header key-value pairs
   */
  public Map<String, String> getHeaders(ConsumerRecord<K, Object> message) {
    return Arrays.stream(message.headers().toArray())
        .filter(header -> Objects.nonNull(header.value()))
        .collect(Collectors.toMap(Header::key, header -> new String(header.value())));
  }
}
