/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package org.example;

public record KafkaMessageOnWait(String value, String rqUid, long timestamp) {}
