/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.perf;

import ru.mfilatov.kafkatestclient.AbstractBaseTest;
import ru.mfilatov.kafkatestclient.model.KafkaMessage;
import ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber;

/**
 * Base for the performance test level: the reply topic is flooded by the Gatling noise simulation,
 * so the shared subscriber must drop everything that is not a tracked probe reply — {@link
 * ru.mfilatov.kafkatestclient.subscriber.KafkaMessageSubscriber} resets its list at 3000 entries,
 * which under flood would silently erase in-flight probe replies.
 *
 * <p>Probes must {@link PerfProbes#register} their RqUID before sending and should unregister in a
 * finally block.
 */
public abstract class FloodAwareBaseTest extends AbstractBaseTest {

  @org.junit.jupiter.api.BeforeAll
  static void enableNoiseFiltering() {
    PerfProbes.enableNoiseFilter();
  }

  @org.junit.jupiter.api.AfterAll
  static void disableNoiseFiltering() {
    PerfProbes.disableNoiseFilter();
  }

  @Override
  protected KafkaMessageSubscriber<String, String> createSubscriber(String topic) {
    var subscriber = new KafkaMessageSubscriber<String, String>(topic);
    subscriber.setPredicate(PerfProbes::shouldTrack);
    return subscriber;
  }

  /** Registers a probe RqUID so its reply survives flood filtering. */
  protected static void registerProbe(String rqUid) {
    PerfProbes.register(rqUid);
  }

  /** Drops a probe RqUID from tracking once its reply has been asserted. */
  protected static void unregisterProbe(String rqUid) {
    PerfProbes.unregister(rqUid);
  }

  /** Whether the noise filter accepts {@code message} (visible for per-test customizations). */
  protected static boolean isTracked(KafkaMessage<?, ?> message) {
    return PerfProbes.shouldTrack(message);
  }
}
