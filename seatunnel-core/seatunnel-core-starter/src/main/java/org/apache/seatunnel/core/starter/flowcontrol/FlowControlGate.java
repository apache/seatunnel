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

package org.apache.seatunnel.core.starter.flowcontrol;

import org.apache.seatunnel.shade.com.google.common.util.concurrent.RateLimiter;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.Optional;

public class FlowControlGate {

    private static final int DEFAULT_VALUE = Integer.MAX_VALUE;

    private final Optional<RateLimiter> bytesRateLimiter;
    private final Optional<RateLimiter> countRateLimiter;

    private FlowControlGate(FlowControlStrategy flowControlStrategy) {
        final int bytesPerSecond = flowControlStrategy.getBytesPerSecond();
        final int countPerSecond = flowControlStrategy.getCountPerSecond();
        this.bytesRateLimiter =
                bytesPerSecond == DEFAULT_VALUE
                        ? Optional.empty()
                        : Optional.of(RateLimiter.create(bytesPerSecond));
        this.countRateLimiter =
                countPerSecond == DEFAULT_VALUE
                        ? Optional.empty()
                        : Optional.of(RateLimiter.create(countPerSecond));
    }

    public void audit(SeaTunnelRow row) {
        bytesRateLimiter.ifPresent(rateLimiter -> acquireBytes(rateLimiter, row));
        countRateLimiter.ifPresent(RateLimiter::acquire);
    }

    /**
     * Charge the byte limiter for this row, unless the row measures zero bytes.
     *
     * <p>A row can legitimately measure zero bytes: {@link SeaTunnelRow#getBytesSize()} counts a
     * null field as 0 and a String as its length, so an all-null row, or one of only empty strings,
     * sums to 0. {@link RateLimiter#acquire(int)} rejects a non-positive permit count, so charging
     * it would abort the task instead of throttling it. Such a row consumes no byte budget; the
     * count limiter is what bounds its rate.
     */
    private void acquireBytes(RateLimiter rateLimiter, SeaTunnelRow row) {
        int bytesSize = row.getBytesSize();
        if (bytesSize > 0) {
            rateLimiter.acquire(bytesSize);
        }
    }

    public static FlowControlGate create(FlowControlStrategy flowControlStrategy) {
        return new FlowControlGate(flowControlStrategy);
    }
}
