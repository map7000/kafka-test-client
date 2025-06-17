/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerService {
  @Value("${app.kafka.service.consumer.topic}")
  private String topic;

  private final KafkaMessageHandler kafkaMessageHandler;
  private final RollTheDiceService rollTheDiceService;

  @KafkaListener(topics = {"${app.kafka.service.consumer.topic}"})
  public void listen(
      @Payload String message,
      @Header("RqUID") String rqUid,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
    var roll = rollTheDiceService.rollTheDice();
    kafkaMessageHandler.handle(rqUid, message, roll);
  }
}
