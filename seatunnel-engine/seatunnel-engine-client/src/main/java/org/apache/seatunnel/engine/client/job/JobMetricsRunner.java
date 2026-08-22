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

package org.apache.seatunnel.engine.client.job;

import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

@Slf4j
public class JobMetricsRunner implements Runnable {
    private final SeaTunnelClient seaTunnelClient;
    private final Long jobId;
    private LocalDateTime lastRunTime = LocalDateTime.now();
    private Long lastReadCount = 0L;
    private Long lastWriteCount = 0L;
    private Long lastCommittedCount = 0L;
    /** Monotonic clock base for the elapsed-time calculation of the average rates. */
    private long lastRunTimeNanos = System.nanoTime();

    public JobMetricsRunner(SeaTunnelClient seaTunnelClient, Long jobId) {
        this.seaTunnelClient = seaTunnelClient;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("job-metrics-runner-" + jobId);
        try {
            JobMetricsSummary jobMetricsSummary = seaTunnelClient.getJobMetricsSummary(jobId);
            LocalDateTime now = LocalDateTime.now();
            // The first run starts immediately (initialDelay=0), so the elapsed time is close to
            // 0 ms. Compute the average rates over milliseconds (clamped to at least 1 ms to avoid
            // division by zero) so the first printed rate is accurate instead of reporting the
            // absolute counts as "per second". The elapsed time is measured with a monotonic clock
            // so a wall-clock rollback cannot produce a wrong (or negative) rate.
            long elapsedMillis =
                    Math.max(
                            1L,
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastRunTimeNanos));
            long averageRead =
                    (jobMetricsSummary.getSourceReadCount() - lastReadCount) * 1000 / elapsedMillis;
            long averageWrite =
                    (jobMetricsSummary.getSinkWriteCount() - lastWriteCount) * 1000 / elapsedMillis;
            long averageCommitted =
                    (jobMetricsSummary.getSinkCommittedCount() - lastCommittedCount)
                            * 1000
                            / elapsedMillis;

            String commitRate = "N/A";
            if (jobMetricsSummary.getSinkWriteCount() > 0
                    && jobMetricsSummary.getSinkCommittedCount() >= 0) {
                double rate =
                        (double) jobMetricsSummary.getSinkCommittedCount()
                                / jobMetricsSummary.getSinkWriteCount()
                                * 100;

                rate = Math.max(0, Math.min(100, rate));
                commitRate = String.format("%.2f%%", rate);
            }

            log.info(
                    StringFormatUtils.formatTable(
                            "Job Progress Information",
                            "Job Id",
                            jobId,
                            "Read Count So Far",
                            jobMetricsSummary.getSourceReadCount(),
                            "Write Attempt Count So Far",
                            jobMetricsSummary.getSinkWriteCount(),
                            "Write Committed Count So Far",
                            jobMetricsSummary.getSinkCommittedCount(),
                            "Commit Rate",
                            commitRate,
                            "Average Read Count",
                            averageRead + "/s",
                            "Average Write Attempt Count",
                            averageWrite + "/s",
                            "Average Write Committed Count",
                            averageCommitted + "/s",
                            "Last Statistic Time",
                            DateTimeUtils.toString(
                                    lastRunTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                            "Current Statistic Time",
                            DateTimeUtils.toString(
                                    now, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)));
            lastRunTime = now;
            lastRunTimeNanos = System.nanoTime();
            lastReadCount = jobMetricsSummary.getSourceReadCount();
            lastWriteCount = jobMetricsSummary.getSinkWriteCount();
            lastCommittedCount = jobMetricsSummary.getSinkCommittedCount();
        } catch (Exception e) {
            log.warn("Failed to get job metrics summary, it maybe first-run");
        }
    }

    @Data
    @AllArgsConstructor
    public static class JobMetricsSummary {
        private long sourceReadCount;
        private long sinkWriteCount;
        private long sinkCommittedCount;
    }
}
