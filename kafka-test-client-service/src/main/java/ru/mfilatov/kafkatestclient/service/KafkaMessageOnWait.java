/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.service;

public record KafkaMessageOnWait(String value, String rqUid, long timestamp) {}
