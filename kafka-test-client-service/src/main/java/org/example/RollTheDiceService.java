/*
* Copyright 2025 Mikhail Filatov
* SPDX-License-Identifier: Apache-2.0
*/
package org.example;

import java.util.Random;
import org.springframework.stereotype.Service;

@Service
public class RollTheDiceService {
  private final Random random = new Random();

  public int rollTheDice() {
    return random.nextInt(0, 10);
  }
}
