/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.consumer;

import java.util.Properties;
import lombok.Getter;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Getter
public class KafkaClientConsumer {
  private final KafkaConsumer<String, String> consumer;

  public KafkaClientConsumer(Properties config) {
    this.consumer = new KafkaConsumer<>(config);
  }
}
