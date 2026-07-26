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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_BARRIER_FORWARD_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_BARRIER_FORWARD_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_LOCK_WAIT_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_LOCK_WAIT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_SNAPSHOT_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_SNAPSHOT_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_SNAPSHOT_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_CHECKPOINT_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_BUDGET_EXCEEDED_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READER_CALLBACK_BUDGET_EXCEEDED_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READER_CALLBACK_MAX_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READER_CALLBACK_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_READER_CALLBACK_TOTAL;

/**
 * Verifies the Source runtime timing boundaries used to baseline legacy readers before managed
 * mailbox rollout.
 */
class SourceRuntimeMetricsTest {

    private static final long SOURCE_RUNTIME_ID = 1L;
    private static final String METRIC_SUFFIX = "#" + SOURCE_RUNTIME_ID;

    @Test
    void shouldRecordPollDurationAndSoftBudgetBoundary() {
        SeaTunnelMetricsContext context = new SeaTunnelMetricsContext();
        SourceRuntimeMetrics metrics = new SourceRuntimeMetrics(context, SOURCE_RUNTIME_ID);
        long budget = SourceRuntimeMetrics.POLL_SOFT_BUDGET_NANOS;

        metrics.recordPoll(budget);
        metrics.recordPoll(budget + 1L);
        metrics.recordPoll(1L);

        Assertions.assertEquals(3L, count(context, SOURCE_POLL_TOTAL));
        Assertions.assertEquals((budget * 2L) + 2L, count(context, SOURCE_POLL_NANOS));
        Assertions.assertEquals(budget + 1L, count(context, SOURCE_POLL_MAX_NANOS));
        Assertions.assertEquals(1L, count(context, SOURCE_POLL_BUDGET_EXCEEDED_TOTAL));
    }

    @Test
    void shouldRecordReaderCallbackDurationAndSoftBudgetBoundary() {
        SeaTunnelMetricsContext context = new SeaTunnelMetricsContext();
        SourceRuntimeMetrics metrics = new SourceRuntimeMetrics(context, SOURCE_RUNTIME_ID);
        long budget = SourceRuntimeMetrics.READER_CALLBACK_SOFT_BUDGET_NANOS;

        metrics.recordReaderCallback(budget + 2L);
        metrics.recordReaderCallback(budget);

        Assertions.assertEquals(2L, count(context, SOURCE_READER_CALLBACK_TOTAL));
        Assertions.assertEquals((budget * 2L) + 2L, count(context, SOURCE_READER_CALLBACK_NANOS));
        Assertions.assertEquals(budget + 2L, count(context, SOURCE_READER_CALLBACK_MAX_NANOS));
        Assertions.assertEquals(1L, count(context, SOURCE_READER_CALLBACK_BUDGET_EXCEEDED_TOTAL));
    }

    @Test
    void shouldKeepCheckpointStagesIndependentAndMaximumsMonotonic() {
        SeaTunnelMetricsContext context = new SeaTunnelMetricsContext();
        SourceRuntimeMetrics metrics = new SourceRuntimeMetrics(context, SOURCE_RUNTIME_ID);

        metrics.recordCheckpointLockWait(20L);
        metrics.recordCheckpointLockWait(5L);
        metrics.recordCheckpointSnapshot(30L);
        metrics.recordCheckpointSnapshot(7L);
        metrics.recordBarrierForward(40L);
        metrics.recordBarrierForward(9L);

        Assertions.assertEquals(2L, count(context, SOURCE_CHECKPOINT_TOTAL));
        Assertions.assertEquals(25L, count(context, SOURCE_CHECKPOINT_LOCK_WAIT_NANOS));
        Assertions.assertEquals(20L, count(context, SOURCE_CHECKPOINT_LOCK_WAIT_MAX_NANOS));
        Assertions.assertEquals(2L, count(context, SOURCE_CHECKPOINT_SNAPSHOT_TOTAL));
        Assertions.assertEquals(37L, count(context, SOURCE_CHECKPOINT_SNAPSHOT_NANOS));
        Assertions.assertEquals(30L, count(context, SOURCE_CHECKPOINT_SNAPSHOT_MAX_NANOS));
        Assertions.assertEquals(49L, count(context, SOURCE_BARRIER_FORWARD_NANOS));
        Assertions.assertEquals(40L, count(context, SOURCE_BARRIER_FORWARD_MAX_NANOS));
    }

    /**
     * Returns one source-scoped metric value from the task registry.
     *
     * @param context task metrics registry
     * @param metricName base metric name
     * @return current metric value
     */
    private static long count(SeaTunnelMetricsContext context, String metricName) {
        return context.counter(metricName + METRIC_SUFFIX).getCount();
    }
}
