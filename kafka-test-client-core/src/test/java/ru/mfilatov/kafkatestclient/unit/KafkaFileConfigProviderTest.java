/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import ru.mfilatov.kafkatestclient.config.KafkaFileConfigProvider;

class KafkaFileConfigProviderTest {

  @Test
  void loadsKafkaPropertiesFromClasspathRoot() {
    var config = new KafkaFileConfigProvider().getKafkaConfig("kafka.properties");
    assertThat(config.getProperty("bootstrap.servers")).isEqualTo("localhost:9092");
    assertThat(config.getProperty("group.id")).isEqualTo("TEST_GROUP");
    assertThat(config.getProperty("client.id")).isEqualTo("test_java_client");
    assertThat(config.getProperty("enable.auto.commit")).isEqualTo("true");
  }

  @Test
  void missingFileFailsFast() {
    assertThatThrownBy(
            () -> new KafkaFileConfigProvider().getKafkaConfig("does-not-exist.properties"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("does-not-exist.properties");
  }

  @Test
  void clusterNameDefaultsToTestClusterFromProperties() {
    var config = new KafkaFileConfigProvider().getKafkaConfig("kafka.properties");
    assertThat(config.getProperty("kafka.cluster.name")).isEqualTo("test-cluster");
  }
}
