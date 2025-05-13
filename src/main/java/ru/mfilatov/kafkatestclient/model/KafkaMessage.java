/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.model;

import java.util.Map;
import lombok.Builder;

@Builder
public record KafkaMessage(
    String topic,
    Integer partition,
    Long offset,
    Map<String, String> headers,
    String key,
    String value) {}
