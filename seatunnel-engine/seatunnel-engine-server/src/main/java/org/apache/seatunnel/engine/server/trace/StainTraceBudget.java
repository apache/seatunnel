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

package org.apache.seatunnel.engine.server.trace;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Tracks a simple per-second budget so each worker can cap stain trace generation. */
public final class StainTraceBudget {
    private final AtomicLong second = new AtomicLong(-1L);
    private final AtomicInteger count = new AtomicInteger(0);

    /** Attempts to reserve one trace slot in the current second and resets counters on rollover. */
    public boolean tryAcquire(int maxPerSecond, long nowMs) {
        if (maxPerSecond <= 0) {
            return false;
        }
        long nowSecond = nowMs / 1000L;
        long prev = second.get();
        if (prev != nowSecond && second.compareAndSet(prev, nowSecond)) {
            count.set(0);
        }
        while (true) {
            int cur = count.get();
            if (cur >= maxPerSecond) {
                return false;
            }
            if (count.compareAndSet(cur, cur + 1)) {
                return true;
            }
        }
    }
}
