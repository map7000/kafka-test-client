/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.subscriber;

import java.util.concurrent.Flow;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

@Getter
@RequiredArgsConstructor
@Slf4j
public class KafkaMessageSubscription implements Flow.Subscription {
  private boolean cancelRequested;
  private final Flow.Subscriber<? super KafkaMessage> subscriber;

  @Setter private String topic;

  @Override
  public void request(long l) {}

  @Override
  public void cancel() {
    cancelRequested = true;
  }
}
