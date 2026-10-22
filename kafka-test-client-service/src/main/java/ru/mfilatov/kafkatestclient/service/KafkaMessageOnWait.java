/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.service;

/**
 * Represents a Kafka message waiting to be sent.
 *
 * @param value message content
 * @param rqUid request unique identifier
 * @param timestamp time when the message should be sent
 */
public record KafkaMessageOnWait(String value, String rqUid, long timestamp) {}
