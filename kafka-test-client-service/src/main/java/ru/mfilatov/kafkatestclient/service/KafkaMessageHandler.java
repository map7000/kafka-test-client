/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.service;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class KafkaMessageHandler {
  private final KafkaProducerService kafkaProducerService;
  private final KafkaServiceScheduler serviceScheduler;

  public void handle(String rqUid, String message, Integer roll) {
    var result = Integer.toString(Integer.parseInt(message) * 2);
    if (roll == 0) {
      kafkaProducerService.send(rqUid, result);
    } else if (roll > 0 && roll <= 7) {
      serviceScheduler.addToList(result, rqUid, 1);
    } else {
      serviceScheduler.addToList(result, rqUid, 10);
    }
  }
}
