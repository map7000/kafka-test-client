/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.perf;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;

/**
 * Shared state for the performance test level. Tracks the RqUIDs of in-flight probes so a
 * subscriber attached to a flooded topic can drop noise replies before they enter the message list:
 * {@link ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber} keeps at most 3000 entries
 * and resets the list above, which would otherwise throw away probe replies when the Gatling flood
 * saturates the topic.
 */
public final class PerfProbes {
  private static final Set<String> TRACKED = ConcurrentHashMap.newKeySet();
  private static volatile boolean noiseFilterActive = false;

  private PerfProbes() {}

  static void enableNoiseFilter() {
    noiseFilterActive = true;
  }

  static void disableNoiseFilter() {
    noiseFilterActive = false;
  }

  static boolean isNoiseFilterActive() {
    return noiseFilterActive;
  }

  static void register(String rqUid) {
    TRACKED.add(rqUid);
  }

  static void unregister(String rqUid) {
    TRACKED.remove(rqUid);
  }

  static boolean isTracked(String rqUid) {
    return rqUid != null && TRACKED.contains(rqUid);
  }

  /**
   * Subscriber predicate for the perf level: while the noise filter is active only probe replies
   * are stored; without it (e.g. the functional smoke) everything is accepted.
   */
  public static boolean shouldTrack(KafkaMessage<?, ?> message) {
    if (!noiseFilterActive) {
      return true;
    }
    var rqUid = message.headers() == null ? null : message.headers().get("RqUID");
    return isTracked(rqUid);
  }
}
