/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.service;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Schedules and manages delayed Kafka message deliveries. Uses concurrent data structures for
 * thread-safe operation.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaServiceScheduler {

  /** Maximum message age before cleanup in milliseconds */
  private static final long MAX_MESSAGE_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours

  private final Queue<KafkaMessageOnWait> messageQueue = new ConcurrentLinkedQueue<>();
  private final AtomicLong lastProcessedTime = new AtomicLong(System.currentTimeMillis());
  private final KafkaProducerService kafkaProducerService;

  /** Processes messages that are due for delivery. Runs every 250ms by default. */
  @Scheduled(fixedDelayString = "${app.kafka.scheduler.process.interval:250}")
  void processMessages() {
    long currentTime = System.currentTimeMillis();
    long previousTime = lastProcessedTime.get();

    messageQueue.stream()
        .filter(message -> isMessageDue(message, currentTime, previousTime))
        .forEach(this::sendMessage);

    lastProcessedTime.set(currentTime);
  }

  /** Cleans up old messages from the queue. Runs every 5 seconds by default. */
  @Scheduled(fixedDelayString = "${app.kafka.scheduler.cleanup.interval:5000}")
  void cleanupOldMessages() {
    long cutoffTime = System.currentTimeMillis() - MAX_MESSAGE_AGE_MS;
    int sizeBefore = messageQueue.size();

    messageQueue.removeIf(
        message -> {
          boolean isExpired = message.timestamp() < cutoffTime;
          if (isExpired) {
            log.debug("Removing expired message: {}", message.rqUid());
          }
          return isExpired;
        });

    int removed = sizeBefore - messageQueue.size();
    if (removed > 0) {
      log.info("Cleaned up {} expired messages", removed);
    }
  }

  /**
   * Adds a new message to the scheduling queue.
   *
   * @param value message content
   * @param rqUid request unique identifier
   * @param delaySeconds delay in seconds before sending
   * @throws IllegalArgumentException if delay is negative or parameters are invalid
   */
  public void scheduleMessage(String value, String rqUid, long delaySeconds) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Message value cannot be null or empty");
    }
    if (rqUid == null || rqUid.isEmpty()) {
      throw new IllegalArgumentException("Request UID cannot be null or empty");
    }
    if (delaySeconds < 0) {
      throw new IllegalArgumentException("Delay cannot be negative");
    }

    var message =
        new KafkaMessageOnWait(value, rqUid, System.currentTimeMillis() + (delaySeconds * 1000));

    messageQueue.offer(message);
    log.debug("Scheduled message with UID {} for delivery in {} seconds", rqUid, delaySeconds);
  }

  private boolean isMessageDue(KafkaMessageOnWait message, long currentTime, long previousTime) {
    return message.timestamp() <= currentTime && message.timestamp() > previousTime;
  }

  private void sendMessage(KafkaMessageOnWait message) {
    try {
      kafkaProducerService.send(message.rqUid(), message.value());
      messageQueue.remove(message);
      log.debug("Sent scheduled message: {}", message.rqUid());
    } catch (Exception e) {
      log.error("Failed to send message {}: {}", message.rqUid(), e.getMessage(), e);
    }
  }
}
