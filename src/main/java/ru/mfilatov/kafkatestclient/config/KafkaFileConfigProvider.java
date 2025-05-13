/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaFileConfigProvider {
  final String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();

  public Properties getKafkaConfig(String configName) {
    Properties config = new Properties();
    String defaultConfigPath = rootPath + configName + "properties";
    try {
      config.load(new FileInputStream(defaultConfigPath));
    } catch (IOException e) {
      log.error("Kafka config not found: {}", defaultConfigPath);
    }
    return config;
  }
}
