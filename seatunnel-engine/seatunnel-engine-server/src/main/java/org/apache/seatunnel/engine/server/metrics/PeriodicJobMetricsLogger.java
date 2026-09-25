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

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Periodically dumps the {@link JobMetrics} of running jobs to the dedicated {@code
 * seatunnel-metrics.log} file.
 *
 * <p>The logger name must match the {@code logger.metrics.name} entry in {@code
 * config/log4j2.properties}; with {@code additivity=false} on that named logger, the emitted INFO
 * lines are routed <strong>only</strong> to the {@code metricsAppender} and never pollute the
 * regular engine file/console logs.
 *
 * <p>Rotate, file-name, and retention are all driven by the Log4j2 RollingFile policy declared
 * alongside this logger, so this class intentionally owns no I/O.
 */
@Slf4j
public final class PeriodicJobMetricsLogger {

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private PeriodicJobMetricsLogger() {}

    /**
     * Emit a single job's current {@link JobMetrics} to the metrics log.
     *
     * <p>The header carries the job id and the local timestamp so that consecutive snapshots can be
     * diffed at a glance from the log file.
     *
     * @param jobId the running job id
     * @param jobMetrics the freshly-collected metrics for that job; {@code null} snapshots are
     *     skipped by the caller
     */
    public static void logJobMetrics(long jobId, JobMetrics jobMetrics) {
        if (jobMetrics == null) {
            return;
        }
        log.info(
                "=== Job {} metrics ({}) ===\n{}",
                jobId,
                TIMESTAMP_FORMAT.format(Instant.now()),
                jobMetrics);
    }
}
