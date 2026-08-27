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

import org.apache.seatunnel.shade.com.google.common.util.concurrent.RateLimiter;

import lombok.AllArgsConstructor;

import java.time.Duration;

@AllArgsConstructor
public class MessageDelayedEventLimiter {
    private final long delayMs;
    private final RateLimiter eventRateLimiter;

    public MessageDelayedEventLimiter(Duration delayThreshold) {
        this(delayThreshold, 1);
    }

    public MessageDelayedEventLimiter(Duration delayThreshold, double permitsPerSecond) {
        this.delayMs = delayThreshold.toMillis();
        this.eventRateLimiter = RateLimiter.create(permitsPerSecond);
    }

    public boolean acquire(long messageCreateTime) {
        if (isDelayed(messageCreateTime)) {
            return eventRateLimiter.tryAcquire();
        }
        return false;
    }

    private boolean isDelayed(long messageCreateTime) {
        return delayMs != 0 && System.currentTimeMillis() - messageCreateTime >= delayMs;
    }
}
