/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package ru.mfilatov.kafkatestclient.exception;

public class MessageProcessingException extends RuntimeException {
  public MessageProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
