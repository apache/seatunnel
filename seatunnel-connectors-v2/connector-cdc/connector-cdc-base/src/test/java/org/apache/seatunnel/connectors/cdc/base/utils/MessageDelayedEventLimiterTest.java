/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.cdc.base.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class MessageDelayedEventLimiterTest {

    @Test
    public void testAcquire() throws InterruptedException {
        double permitsPerSecond = 0.5;
        Duration delayThreshold = Duration.ofMillis(1000);
        MessageDelayedEventLimiter delayedEventLimiter =
                new MessageDelayedEventLimiter(delayThreshold, permitsPerSecond);

        long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        long actualAcquiredCount = 0;
        while (System.currentTimeMillis() < endTime) {
            boolean acquired =
                    delayedEventLimiter.acquire(
                            System.currentTimeMillis() - (delayThreshold.toMillis() * 10));
            if (acquired) {
                actualAcquiredCount++;
            }
            Thread.sleep(1);
        }
        long expectedAcquiredCount = (long) (TimeUnit.SECONDS.toSeconds(10) * permitsPerSecond);

        Assertions.assertTrue(expectedAcquiredCount >= actualAcquiredCount);
    }

    @Test
    public void testNoAcquire() throws InterruptedException {
        double permitsPerSecond = 0.5;
        Duration delayThreshold = Duration.ofMillis(1000);
        MessageDelayedEventLimiter delayedEventLimiter =
                new MessageDelayedEventLimiter(delayThreshold, permitsPerSecond);

        long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
        long actualAcquiredCount = 0;
        while (System.currentTimeMillis() < endTime) {
            boolean acquired = delayedEventLimiter.acquire(System.currentTimeMillis());
            if (acquired) {
                actualAcquiredCount++;
            }
            Thread.sleep(1);
        }

        Assertions.assertTrue(actualAcquiredCount == 0);
    }
}
