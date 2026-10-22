/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.perf;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.mfilatov.kafkatestclient.annotations.KafkaReader;
import ru.mfilatov.kafkatestclient.annotations.KafkaWriter;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

/**
 * Level 3 (functional part): end-to-end correctness check against the running
 * kafka-test-client-service — small concurrent test threads publish probe values and each thread
 * must get back exactly its own doubled reply, even while the Gatling flood is active (replies are
 * filtered through {@link FloodAwareBaseTest}).
 */
@Slf4j
@KafkaWriter("test_topic_in")
@KafkaReader("test_topic_out")
class FunctionalRoundTripTest extends FloodAwareBaseTest {

  @ParameterizedTest
  @Execution(ExecutionMode.CONCURRENT)
  @SneakyThrows
  @MethodSource("argumentsForTest")
  public void roundTripTest(Integer i) {
    var rqUid = UUID.randomUUID().toString();
    registerProbe(rqUid);
    try {
      kafka.send(
          KafkaMessage.<String, String>builder()
              .headers(Map.of("RqUID", rqUid))
              .value(i.toString())
              .build());
      var message = kafka.awaitMessage(rqUid, 60);
      assertThat(message.value())
          .as("service must double the value for RqUID %s", rqUid)
          .isEqualTo(Integer.toString(i * 2));
      assertThat(message.headers()).containsEntry("RqUID", rqUid);
      assertThat(message.topic()).isEqualTo(kafka.getSubscriber().getKafkaTopic());
    } finally {
      unregisterProbe(rqUid);
    }
  }

  private static Stream<Integer> argumentsForTest() {
    return new Random().ints(10, 1, 100).boxed();
  }
}
