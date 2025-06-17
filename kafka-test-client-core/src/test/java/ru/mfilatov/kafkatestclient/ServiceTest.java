/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient;

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

@Slf4j
@KafkaWriter("test_topic_in")
@KafkaReader("test_topic_out")
public class ServiceTest extends AbstractBaseTest {

  @ParameterizedTest
  @Execution(ExecutionMode.CONCURRENT)
  @SneakyThrows
  @MethodSource("argumentsForTest")
  public void test1Test(Integer i) {
    var rqUID = UUID.randomUUID().toString();
    kafka.send(KafkaMessage.builder().headers(Map.of("RqUID", rqUID)).value(i.toString()).build());
    var message = kafka.awaitMessage(rqUID, 60);
    assertThat(Integer.parseInt(message.value())).isEqualTo(i * 2);
  }

  private static Stream<Integer> argumentsForTest() {
    return new Random().ints(20, 1, 100).boxed();
  }
}
