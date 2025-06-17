/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.service;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaServiceScheduler {
  private final ArrayList<KafkaMessageOnWait> list = new ArrayList<>();
  private final ReentrantLock lock = new ReentrantLock();
  private long lastTime = System.currentTimeMillis();
  private final KafkaProducerService kafkaProducerService;

  @Scheduled(fixedDelay = 250)
  void execute() {
    var currentTime = System.currentTimeMillis();
    list.stream()
        .filter(a -> a.timestamp() < currentTime && a.timestamp() > lastTime)
        .forEach(a -> kafkaProducerService.send(a.rqUid(), a.value()));
    lastTime = currentTime;
  }

  @Scheduled(fixedDelay = 5555)
  void clean() {
    lock.lock();
    list.removeIf(a -> a.timestamp() < lastTime);
    lock.unlock();
  }

  public void addToList(String value, String rqUid, long delay) {
    lock.lock();
    list.add(new KafkaMessageOnWait(value, rqUid, System.currentTimeMillis() + delay * 1000));
    lock.unlock();
  }
}
