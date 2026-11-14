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

package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.Measurement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Light-weight smoke tests for {@link PeriodicJobMetricsLogger}.
 *
 * <p>Verifying the actual log4j2 routing into {@code metricsAppender} would require {@code
 * org.apache.logging.log4j:log4j-core} on the test classpath, which this module does not currently
 * pull in. End-to-end verification of the file appender is left to integration tests that boot the
 * engine against {@code config/log4j2.properties}.
 */
public class PeriodicJobMetricsLoggerTest {

    @Test
    public void testLogJobMetricsWithMetricsDoesNotThrow() {
        Map<String, List<Measurement>> payload = new HashMap<>();
        payload.put(
                "SourceReceivedCount",
                Collections.singletonList(
                        Measurement.of("SourceReceivedCount", 100L, 0L, Collections.emptyMap())));
        JobMetrics jobMetrics = JobMetrics.of(payload);
        Assertions.assertFalse(jobMetrics.metrics().isEmpty());

        // Just exercise the path: no exception, no assertion on log content.
        Assertions.assertDoesNotThrow(
                () -> PeriodicJobMetricsLogger.logJobMetrics(42L, jobMetrics));
    }

    @Test
    public void testLogJobMetricsWithNullEmitsWarningAndDoesNotThrow() {
        // null branch must not throw; it logs a WARN.
        Assertions.assertDoesNotThrow(() -> PeriodicJobMetricsLogger.logJobMetrics(7L, null));
    }

    @Test
    public void testLogJobMetricsWithEmptyMetricsDoesNotThrow() {
        // empty JobMetrics is still a valid input; it just produces an empty body section.
        Assertions.assertDoesNotThrow(
                () -> PeriodicJobMetricsLogger.logJobMetrics(13L, JobMetrics.empty()));
    }
}
