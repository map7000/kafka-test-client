/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.service;

import io.micrometer.core.annotation.Timed;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import ru.mfilatov.kafkatestclient.exception.MessageProcessingException;

/**
 * Service for consuming and processing Kafka messages. Provides message handling with retry
 * capability and metrics.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerService {
  private static final String METRIC_NAME = "kafka.message.processing";
  private static final int MAX_RETRY_ATTEMPTS = 3;
  private static final long RETRY_DELAY_MS = 1000;

  @Value("${app.kafka.service.consumer.topic}")
  private String topic;

  @Value("${app.kafka.service.consumer.group-id}")
  private String groupId;

  private final KafkaMessageHandler kafkaMessageHandler;
  private final RollTheDiceService rollTheDiceService;
  private final AtomicLong messageCounter = new AtomicLong(0);

  /**
   * Listens for messages on the configured Kafka topic. Implements retry logic and tracks metrics.
   *
   * @param message the message payload
   * @param rqUid request unique identifier
   * @param topic the source topic
   * @throws MessageProcessingException if processing fails after retries
   */
  @KafkaListener(
      topics = "${app.kafka.service.consumer.topic}",
      groupId = "${app.kafka.service.consumer.group-id}",
      containerFactory = "kafkaListenerContainerFactory")
  @Timed(value = METRIC_NAME, description = "Time spent processing Kafka messages")
  @Retryable(
      value = MessageProcessingException.class,
      maxAttempts = MAX_RETRY_ATTEMPTS,
      backoff = @Backoff(delay = RETRY_DELAY_MS))
  public void listen(
      @Payload String message,
      @Header(value = "RqUID", required = false) String rqUid,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
      @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
      @Header(KafkaHeaders.OFFSET) long offset) {

    String messageId = generateMessageId(rqUid, partition, offset);
    long count = messageCounter.incrementAndGet();

    log.debug("Processing message {} from topic {} (total: {})", messageId, topic, count);

    try {
      validateMessage(message, messageId);
      int roll = rollTheDiceService.rollTheDice();
      kafkaMessageHandler.handle(messageId, message, roll);

      log.info("Successfully processed message {} with roll {}", messageId, roll);

    } catch (Exception e) {
      log.error("Failed to process message {}: {}", messageId, e.getMessage(), e);
      throw new MessageProcessingException("Message processing failed: " + messageId, e);
    }
  }

  private void validateMessage(String message, String messageId) {
    if (message == null || message.trim().isEmpty()) {
      throw new IllegalArgumentException("Empty message received: " + messageId);
    }
  }

  private String generateMessageId(String rqUid, int partition, long offset) {
    return rqUid != null ? rqUid : String.format("%s-%d-%d", topic, partition, offset);
  }
}
