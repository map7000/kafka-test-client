/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package org.example;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {
  private final KafkaTemplate<String, String> kafkaTemplate;

  @Value("${app.kafka.service.producer.topic}")
  private String topic;

  public CompletableFuture<SendResult<String, String>> send(String rqUID, String payload) {
    ProducerRecord<String, String> message =
        new ProducerRecord<>(topic, UUID.randomUUID().toString(), payload);

    message.headers().add(new RecordHeader("RqUID", rqUID.getBytes(StandardCharsets.UTF_8)));

    log.info("message sent: {}", message);

    return kafkaTemplate.send(message);
  }
}
