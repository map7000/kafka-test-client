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
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.processor.KafkaMessageProcessor;

/**
 * A publisher implementation for Kafka messages that follows the Reactive Streams pattern. This
 * class manages subscriptions and message distribution to subscribers using a single-threaded
 * executor. It implements both Flow.Publisher and AutoCloseable interfaces for proper resource
 * management.
 *
 * @author Mikhail Filatov
 * @see Flow.Publisher
 * @see AutoCloseable
 */
@Slf4j
public class KafkaMessagePublisher<K, V>
    implements Flow.Publisher<KafkaMessage<K, V>>, AutoCloseable {
  /** Maximum time to wait for executor shutdown in seconds */
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

  /** Thread-safe list of active subscriptions */
  private final List<KafkaMessageSubscription<K, V>> subscriptions =
      Collections.synchronizedList(new ArrayList<>());

  /** Single-threaded executor for managing message consumption */
  private final ExecutorService executor;

  /** Future representing the running consumer task */
  private final Future<?> consumer;

  /** Flag indicating whether the publisher has been shut down */
  private volatile boolean isShutdown = false;

  /**
   * Creates a new KafkaMessagePublisher and starts the message consumer. Initializes a
   * single-threaded executor with a daemon thread for message processing.
   *
   * @param processor the KafkaMessageProcessor to process messages
   */
  public KafkaMessagePublisher(KafkaMessageProcessor<K, V> processor) {
    // Create a single-threaded executor with a daemon thread
    this.executor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread thread = new Thread(r, "kafka-message-consumer");
              thread.setDaemon(true);
              return thread;
            });
    // Start the message consumer
    this.consumer = executor.submit(new KafkaMessageConsumer<K, V>(subscriptions, processor));
  }

  /**
   * Subscribes a new subscriber to receive Kafka messages. This method is synchronized to ensure
   * thread-safe subscription management.
   *
   * @param subscriber the subscriber to add
   * @throws IllegalStateException if the publisher is already shut down
   * @throws NullPointerException if the subscriber is null
   */
  @Override
  public synchronized void subscribe(Flow.Subscriber<? super KafkaMessage<K, V>> subscriber) {
    if (isShutdown) {
      throw new IllegalStateException("Publisher is shutdown");
    }

    try {
      if (subscriber == null) {
        throw new NullPointerException("Subscriber cannot be null");
      }

      // Create and register new subscription
      var subscription = new KafkaMessageSubscription<K, V>(subscriber);
      subscriber.onSubscribe(subscription);
      subscriptions.add(subscription);
      log.debug("Added new subscriber: {}", subscriber);

    } catch (Exception e) {
      log.error("Failed to add subscriber", e);
      subscriber.onError(e);
    }
  }

  /** Implements AutoCloseable interface. Calls shutdown() if not already shut down. */
  @Override
  public void close() {
    if (!isShutdown) {
      shutdown();
    }
  }

  /**
   * Performs an orderly shutdown of the publisher. Cancels all subscriptions, stops the consumer,
   * and shuts down the executor. This method is synchronized to ensure thread-safe shutdown.
   */
  public synchronized void shutdown() {
    if (isShutdown) {
      return;
    }

    isShutdown = true;
    log.info("Shutting down KafkaMessagePublisher");

    try {
      // Cancel all active subscriptions
      subscriptions.forEach(KafkaMessageSubscription::cancel);
      subscriptions.clear();

      // Stop the message consumer
      if (!consumer.isDone()) {
        consumer.cancel(true);
      }

      // Shutdown the executor service
      executor.shutdown();
      if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        log.warn("Forced shutdown of executor after timeout");
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Shutdown interrupted", e);
      executor.shutdownNow();
    } catch (Exception e) {
      log.error("Error during shutdown", e);
      executor.shutdownNow();
    }
  }

  /**
   * Checks if the publisher has been shut down.
   *
   * @return true if the publisher is shut down, false otherwise
   */
  public boolean isShutdown() {
    return isShutdown;
  }
}
