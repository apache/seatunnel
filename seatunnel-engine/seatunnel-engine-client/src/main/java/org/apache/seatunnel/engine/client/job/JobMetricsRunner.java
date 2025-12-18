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

import java.time.Duration;
import java.time.LocalDateTime;

@Slf4j
public class JobMetricsRunner implements Runnable {
    private final SeaTunnelClient seaTunnelClient;
    private final Long jobId;
    private LocalDateTime lastRunTime = LocalDateTime.now();
    private Long lastReadCount = 0L;
    private Long lastWriteCount = 0L;
    private Long lastReadBytes = 0L;
    private Long lastWriteBytes = 0L;

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
            long seconds = Duration.between(lastRunTime, now).getSeconds();
            if (seconds <= 0) {
                seconds = 1;
            }
            long averageReadCount =
                    (jobMetricsSummary.getSourceReadCount() - lastReadCount) / seconds;
            long averageWriteCount =
                    (jobMetricsSummary.getSinkWriteCount() - lastWriteCount) / seconds;
            long averageReadBytes =
                    (jobMetricsSummary.getSourceReadBytes() - lastReadBytes) / seconds;
            long averageWriteBytes =
                    (jobMetricsSummary.getSinkWriteBytes() - lastWriteBytes) / seconds;
            log.info(
                    StringFormatUtils.formatTable(
                            "Job Progress Information",
                            "Job Id",
                            jobId,
                            "Read Count So Far",
                            jobMetricsSummary.getSourceReadCount(),
                            "Write Count So Far",
                            jobMetricsSummary.getSinkWriteCount(),
                            "Read Bytes So Far",
                            formatBytes(jobMetricsSummary.getSourceReadBytes()),
                            "Write Bytes So Far",
                            formatBytes(jobMetricsSummary.getSinkWriteBytes()),
                            "Average Read Count",
                            averageReadCount + "/s",
                            "Average Write Count",
                            averageWriteCount + "/s",
                            "Average Read Bytes",
                            formatBytes(averageReadBytes) + "/s",
                            "Average Write Bytes",
                            formatBytes(averageWriteBytes) + "/s",
                            "Last Statistic Time",
                            DateTimeUtils.toString(
                                    lastRunTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                            "Current Statistic Time",
                            DateTimeUtils.toString(
                                    now, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)));
            lastRunTime = now;
            lastReadCount = jobMetricsSummary.getSourceReadCount();
            lastWriteCount = jobMetricsSummary.getSinkWriteCount();
            lastReadBytes = jobMetricsSummary.getSourceReadBytes();
            lastWriteBytes = jobMetricsSummary.getSinkWriteBytes();
        } catch (Exception e) {
            log.warn("Failed to get job metrics summary, it maybe first-run");
        }
    }

    /**
     * Format bytes to human-readable string.
     *
     * @param bytes bytes to format
     * @return formatted string like "1.5 MB", "2.3 GB", etc.
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        double kb = bytes / 1024.0;
        if (kb < 1024) {
            return String.format("%.2f KB", kb);
        }
        double mb = kb / 1024.0;
        if (mb < 1024) {
            return String.format("%.2f MB", mb);
        }
        double gb = mb / 1024.0;
        return String.format("%.2f GB", gb);
    }

    @Data
    @AllArgsConstructor
    public static class JobMetricsSummary {
        private long sourceReadCount;
        private long sinkWriteCount;
        private long sourceReadBytes;
        private long sinkWriteBytes;
    }
}
