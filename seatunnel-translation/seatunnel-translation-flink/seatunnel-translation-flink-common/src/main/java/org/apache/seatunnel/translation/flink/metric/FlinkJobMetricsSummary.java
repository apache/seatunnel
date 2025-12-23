/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.translation.flink.metric;

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;

import org.apache.flink.api.common.JobExecutionResult;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class FlinkJobMetricsSummary {

    private final JobExecutionResult jobExecutionResult;

    private final LocalDateTime jobStartTime;

    private final LocalDateTime jobEndTime;

    public FlinkJobMetricsSummary(
            JobExecutionResult jobExecutionResult,
            LocalDateTime jobStartTime,
            LocalDateTime jobEndTime) {
        this.jobExecutionResult = jobExecutionResult;
        this.jobStartTime = jobStartTime;
        this.jobEndTime = jobEndTime;
        log.info(
                "FlinkJobMetricsSummary created for job: {}",
                jobExecutionResult != null ? jobExecutionResult.getJobID() : "null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private JobExecutionResult jobExecutionResult;

        private long jobStartTime;

        private long jobEndTime;

        private Builder() {}

        public Builder jobExecutionResult(JobExecutionResult jobExecutionResult) {
            this.jobExecutionResult = jobExecutionResult;
            return this;
        }

        public Builder jobStartTime(long jobStartTime) {
            this.jobStartTime = jobStartTime;
            return this;
        }

        public Builder jobEndTime(long jobEndTime) {
            this.jobEndTime = jobEndTime;
            return this;
        }

        public FlinkJobMetricsSummary build() {
            return new FlinkJobMetricsSummary(
                    jobExecutionResult,
                    DateTimeUtils.parse(jobStartTime),
                    DateTimeUtils.parse(jobEndTime));
        }
    }

    public Map<String, Object> getMetrics() {
        Map<String, Object> metrics = new HashMap<>();

        if (jobExecutionResult == null) {
            log.warn("JobExecutionResult is null, cannot get metrics");
            return metrics;
        }

        try {
            Map<String, Object> accumulatorResults = jobExecutionResult.getAllAccumulatorResults();

            for (Map.Entry<String, Object> entry : accumulatorResults.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof Number) {
                    long longValue = ((Number) value).longValue();

                    if (key.contains("SinkWriteCount")) {
                        metrics.put(MetricNames.SINK_WRITE_COUNT, longValue);

                    } else if (key.contains("SinkWriteBytes")) {
                        metrics.put(MetricNames.SINK_WRITE_BYTES, longValue);

                    } else if (key.contains("SourceReceivedCount")) {
                        metrics.put(MetricNames.SOURCE_RECEIVED_COUNT, longValue);

                    } else if (key.contains("SourceReceivedBytes")) {
                        metrics.put(MetricNames.SOURCE_RECEIVED_BYTES, longValue);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to get metrics from accumulators: {}", e.getMessage(), e);
        }

        log.info("Retrieved metrics from accumulators: {}", metrics);
        return metrics;
    }

    private long getCounterValue(Map<String, Object> metrics, String name, long defaultValue) {
        Object value = metrics.get(name);
        if (value == null) {
            return defaultValue;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            log.warn(
                    "Failed to parse counter value: {} = {}, using default: {}",
                    name,
                    value,
                    defaultValue);
            return defaultValue;
        }
    }

    @Override
    public String toString() {
        Map<String, Object> metrics = getMetrics();

        long sourceReadCount = getCounterValue(metrics, MetricNames.SOURCE_RECEIVED_COUNT, 0L);
        long sourceReadBytes = getCounterValue(metrics, MetricNames.SOURCE_RECEIVED_BYTES, 0L);
        long sinkWriteCount = getCounterValue(metrics, MetricNames.SINK_WRITE_COUNT, 0L);
        long sinkWriteBytes = getCounterValue(metrics, MetricNames.SINK_WRITE_BYTES, 0L);

        log.info(
                "Final metrics - sourceRead: {}, sourceBytes: {}, sinkWrite: {}, sinkBytes: {}",
                sourceReadCount,
                sourceReadBytes,
                sinkWriteCount,
                sinkWriteBytes);

        return StringFormatUtils.formatTable(
                "Job Statistic Information",
                "Start Time",
                DateTimeUtils.toString(jobStartTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                "End Time",
                DateTimeUtils.toString(jobEndTime, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS),
                "Total Time(s)",
                Duration.between(jobStartTime, jobEndTime).getSeconds(),
                "Total Read Count",
                sourceReadCount,
                "Total Write Count",
                sinkWriteCount,
                "Total Read Bytes",
                sourceReadBytes,
                "Total Write Bytes",
                sinkWriteBytes);
    }
}
