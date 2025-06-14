/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient;

import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import ru.mfilatov.kafkatestclient.annotations.KafkaReader;
import ru.mfilatov.kafkatestclient.annotations.KafkaWriter;
import ru.mfilatov.kafkatestclient.config.KafkaFileConfigProvider;
import ru.mfilatov.kafkatestclient.producer.KafkaClientProducer;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessagePublisher;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractBaseTest {
  public static final KafkaMessagePublisher publisher = new KafkaMessagePublisher();
  public KafkaSteps kafka;

  @BeforeAll
  void initKafka() {
    var topicRead = getKafkaTopicRead();
    var topicWrite = getKafkaTopicWrite();

    KafkaClientProducer producer = Objects.isNull(topicWrite) ? null : createProducer(topicWrite);
    KafkaMessageSubscriber subscriber =
        Objects.isNull(topicRead) ? null : createSubscriber(topicRead);

    kafka = new KafkaSteps(subscriber, producer);
    if (Objects.nonNull(subscriber)) {
      publisher.subscribe(subscriber);
    }
  }

  @AfterAll
  void closeKafka() {
    kafka.producer.getProducer().close();
    kafka.closeSubscriber();
  }

  protected String getKafkaTopicRead() {
    var kafkaReader = this.getClass().getAnnotation(KafkaReader.class);
    return Objects.nonNull(kafkaReader) ? kafkaReader.value() : null;
  }

  protected String getKafkaTopicWrite() {
    var kafkaWriter = this.getClass().getAnnotation(KafkaWriter.class);
    return Objects.nonNull(kafkaWriter) ? kafkaWriter.value() : null;
  }

  protected KafkaMessageSubscriber createSubscriber(String topic) {
    return new KafkaMessageSubscriber(topic);
  }

  protected KafkaClientProducer createProducer(String topic) {
    return new KafkaClientProducer(new KafkaFileConfigProvider().getKafkaConfig("kafka.properties"), topic);
  }
}
