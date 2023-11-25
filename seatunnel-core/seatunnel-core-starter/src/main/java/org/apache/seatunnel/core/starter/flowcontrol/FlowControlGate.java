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

public class FlowControlGate {

    private final RateLimiter bytesRateLimiter;
    private final RateLimiter countRateLimiter;

    private FlowControlGate(FlowControlStrategy flowControlStrategy) {
        this.bytesRateLimiter = RateLimiter.create(flowControlStrategy.getBytesPerSecond());
        this.countRateLimiter = RateLimiter.create(flowControlStrategy.getCountPreSecond());
    }

    public void audit(SeaTunnelRow row) {
        bytesRateLimiter.acquire(row.getBytesSize());
        countRateLimiter.acquire();
    }

    public static FlowControlGate create(FlowControlStrategy flowControlStrategy) {
        return new FlowControlGate(flowControlStrategy);
    }
}
