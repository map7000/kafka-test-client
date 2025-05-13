/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.producer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

@Slf4j
@Getter
public class KafkaClientProducer {
  private final String clusterName;
  private final String defaultTopic;
  private static final Lock lock = new ReentrantLock();
  private static final ConcurrentMap<String, KafkaProducer<String, String>> producers =
      new ConcurrentHashMap<>();

  public KafkaClientProducer(Properties config, String defaultTopic) {
    this.clusterName = config.getProperty("kafka.cluster.name");
    this.defaultTopic = defaultTopic;

    lock.lock();
    if (Objects.isNull(producers.get(this.clusterName))) {
      producers.put(this.clusterName, new KafkaProducer<>(config));
      log.info("Created producer for cluster: {}", this.clusterName);
    }
    lock.unlock();
  }

  public KafkaProducer<String, String> getProducer() {
    return producers.get(this.clusterName);
  }

  public List<RecordMetadata> send(List<KafkaMessage> messages) {
    var metadata = new ArrayList<RecordMetadata>();
    var producer = producers.get(this.clusterName);
    for (var message : messages) {
      var headers = new RecordHeaders();
      message.headers().keySet().stream()
          .map(a -> new RecordHeader(a, message.headers().get(a).getBytes(StandardCharsets.UTF_8)))
          .forEach(headers::add);

      ProducerRecord<String, String> record =
          new ProducerRecord<>(
              Objects.isNull(message.topic()) ? defaultTopic : message.topic(),
              message.partition(),
              message.key(),
              message.value(),
              headers);

      producer.send(
          record,
          ((recordMetadata, e) -> {
            if (Objects.nonNull(e)) log.error(e.getMessage());
          }));
    }
    producer.flush();
    return metadata;
  }

  public RecordMetadata send(KafkaMessage message) {
    var metadata = send(Collections.singletonList(message));
    return metadata.isEmpty() ? null : metadata.getFirst();
  }
}
