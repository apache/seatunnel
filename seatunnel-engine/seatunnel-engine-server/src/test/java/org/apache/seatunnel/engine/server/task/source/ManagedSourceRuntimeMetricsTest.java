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

package org.apache.seatunnel.engine.server.task.source;

import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_ADMISSION_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_POLL_BYTES;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_MANAGED_POLL_RECORDS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_BUDGET_EXCEEDED_TOTAL;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_NANOS;
import static org.apache.seatunnel.api.common.metrics.MetricNames.SOURCE_POLL_TOTAL;

/** Verifies managed Source metrics remain local to one task attempt. */
class ManagedSourceRuntimeMetricsTest {

    private static final long SOURCE_RUNTIME_ID = 1L;
    private static final long ATTEMPT_ID = 101L;
    private static final long NEXT_ATTEMPT_ID = 102L;

    @Test
    void shouldRecordManagedPollMetricsForOneAttempt() {
        SeaTunnelMetricsContext context = new SeaTunnelMetricsContext();
        ManagedSourceRuntimeMetrics metrics =
                new ManagedSourceRuntimeMetrics(context, SOURCE_RUNTIME_ID, ATTEMPT_ID);

        metrics.recordPoll(7L, 2, 19L, true);
        metrics.recordPoll(5L, 1, 11L, false);

        Assertions.assertEquals(2L, count(context, SOURCE_POLL_TOTAL, ATTEMPT_ID));
        Assertions.assertEquals(12L, count(context, SOURCE_POLL_NANOS, ATTEMPT_ID));
        Assertions.assertEquals(3L, count(context, SOURCE_MANAGED_POLL_RECORDS, ATTEMPT_ID));
        Assertions.assertEquals(30L, count(context, SOURCE_MANAGED_POLL_BYTES, ATTEMPT_ID));
        Assertions.assertEquals(1L, count(context, SOURCE_POLL_BUDGET_EXCEEDED_TOTAL, ATTEMPT_ID));
    }

    @Test
    void shouldKeepManagedRuntimeMetricsAttemptLocalAfterRecovery() {
        SeaTunnelMetricsContext context = new SeaTunnelMetricsContext();
        ManagedSourceRuntimeMetrics firstAttempt =
                new ManagedSourceRuntimeMetrics(context, SOURCE_RUNTIME_ID, ATTEMPT_ID);
        ManagedSourceRuntimeMetrics nextAttempt =
                new ManagedSourceRuntimeMetrics(context, SOURCE_RUNTIME_ID, NEXT_ATTEMPT_ID);

        firstAttempt.recordAdmission(SourceCommandAdmissionStatus.ACCEPTED);
        firstAttempt.recordPoll(11L, 3, 23L, false);
        nextAttempt.recordPoll(17L, 5, 29L, false);

        Assertions.assertEquals(
                1L, admissionCount(context, SourceCommandAdmissionStatus.ACCEPTED, ATTEMPT_ID));
        Assertions.assertEquals(1L, count(context, SOURCE_POLL_TOTAL, ATTEMPT_ID));
        Assertions.assertEquals(23L, count(context, SOURCE_MANAGED_POLL_BYTES, ATTEMPT_ID));
        Assertions.assertEquals(
                0L,
                admissionCount(context, SourceCommandAdmissionStatus.ACCEPTED, NEXT_ATTEMPT_ID));
        Assertions.assertEquals(1L, count(context, SOURCE_POLL_TOTAL, NEXT_ATTEMPT_ID));
        Assertions.assertEquals(29L, count(context, SOURCE_MANAGED_POLL_BYTES, NEXT_ATTEMPT_ID));
    }

    /**
     * Returns one source-and-attempt-scoped managed metric value from the task registry.
     *
     * @param context task metrics registry
     * @param metricName base metric name
     * @param executionId immutable engine deployment identity for this task attempt
     * @return current metric value
     */
    private static long count(
            SeaTunnelMetricsContext context, String metricName, long executionId) {
        return context.counter(
                        metricName
                                + ManagedSourceRuntimeMetrics.metricSuffix(
                                        SOURCE_RUNTIME_ID, executionId))
                .getCount();
    }

    /**
     * Returns one source-and-attempt-scoped managed admission metric value.
     *
     * @param context task metrics registry
     * @param status admission status suffix
     * @param executionId immutable engine deployment identity for this task attempt
     * @return current metric value
     */
    private static long admissionCount(
            SeaTunnelMetricsContext context,
            SourceCommandAdmissionStatus status,
            long executionId) {
        return context.counter(
                        SOURCE_MANAGED_ADMISSION_TOTAL
                                + ManagedSourceRuntimeMetrics.metricSuffix(
                                        SOURCE_RUNTIME_ID, executionId)
                                + "#"
                                + status.name())
                .getCount();
    }
}
