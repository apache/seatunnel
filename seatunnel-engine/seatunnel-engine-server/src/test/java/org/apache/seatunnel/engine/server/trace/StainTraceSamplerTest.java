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

import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies sampling-rate and per-second budget enforcement for trace id generation. */
public class StainTraceSamplerTest {

    @Test
    void testDeterministicSamplingAndWorkerBudget() {
        long nowMs = 1_700_000_000_000L;
        long taskId = 10L;

        ThreadSafeCounter generated = new ThreadSafeCounter("generated");
        ThreadSafeCounter throttled = new ThreadSafeCounter("throttled");
        StainTraceSampler sampler = new StainTraceSampler(true, 2, 100, 32, generated, throttled);

        Assertions.assertEquals(-1L, sampler.tryGenerateTraceId(taskId, nowMs));
        long traceId = sampler.tryGenerateTraceId(taskId, nowMs);
        Assertions.assertNotEquals(-1L, traceId);
        Assertions.assertEquals(1L, generated.getCount());
        Assertions.assertEquals(0L, throttled.getCount());
    }

    @Test
    void testBudgetThrottlesWithinSameSecondAndResetsOnSecondTick() {
        long taskId = 10L;
        long sec0 = 1_700_000_010_000L;
        long sec1 = sec0 + 1_000L;

        ThreadSafeCounter generated = new ThreadSafeCounter("generated");
        ThreadSafeCounter throttled = new ThreadSafeCounter("throttled");
        StainTraceSampler sampler = new StainTraceSampler(true, 1, 1, 32, generated, throttled);

        Assertions.assertNotEquals(-1L, sampler.tryGenerateTraceId(taskId, sec0));
        Assertions.assertEquals(-1L, sampler.tryGenerateTraceId(taskId, sec0));
        Assertions.assertEquals(1L, generated.getCount());
        Assertions.assertEquals(1L, throttled.getCount());

        Assertions.assertNotEquals(-1L, sampler.tryGenerateTraceId(taskId, sec1));
        Assertions.assertEquals(2L, generated.getCount());
        Assertions.assertEquals(1L, throttled.getCount());
    }
}
