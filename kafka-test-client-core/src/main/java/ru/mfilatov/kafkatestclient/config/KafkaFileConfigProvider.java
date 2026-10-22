/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * Loads Kafka client configuration properties from a file on the classpath.
 *
 * <p>Files are resolved through the context classloader rather than filesystem paths, so the
 * configuration works inside JARs and with non-ASCII working directories. A missing file is a
 * programming error and fails fast — returning an empty configuration would only surface later as a
 * cryptic {@code Missing required configuration "bootstrap.servers"} from inside the Kafka client.
 */
@Slf4j
public class KafkaFileConfigProvider {

  /**
   * Loads the named configuration file from the classpath root.
   *
   * @param configName classpath resource name, e.g. {@code "kafka.properties"}
   * @return the loaded configuration
   * @throws IllegalStateException if the file is not found on the classpath
   * @throws UncheckedIOException if the file exists but cannot be read or parsed
   */
  public Properties getKafkaConfig(String configName) {
    log.debug("Loading Kafka config: {}", configName);
    ClassLoader classLoader =
        Thread.currentThread().getContextClassLoader() != null
            ? Thread.currentThread().getContextClassLoader()
            : KafkaFileConfigProvider.class.getClassLoader();
    Properties config = new Properties();
    try (InputStream inputStream = classLoader.getResourceAsStream(configName)) {
      if (inputStream == null) {
        throw new IllegalStateException("Kafka config not found on classpath: " + configName);
      }
      config.load(inputStream);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to load Kafka config: " + configName, e);
    }
    return config;
  }
}
