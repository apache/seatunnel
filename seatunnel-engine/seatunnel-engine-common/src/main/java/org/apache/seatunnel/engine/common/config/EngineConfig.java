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

package org.apache.seatunnel.engine.common.config;

import org.apache.seatunnel.api.metadata.MetadataConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.config.server.CoordinatorServiceConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.config.server.QueueType;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryConfig;
import org.apache.seatunnel.engine.common.config.server.ThreadShareMode;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;

import lombok.Data;

import java.util.Collections;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkBackupCount;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;

/** Collects the engine-side runtime configuration shared by master, worker, and telemetry logic. */
@Data
public class EngineConfig {

    private int backupCount =
            ServerConfigOptions.MasterServerConfigOptions.BACKUP_COUNT.defaultValue();
    private int printExecutionInfoInterval =
            ServerConfigOptions.MasterServerConfigOptions.PRINT_EXECUTION_INFO_INTERVAL
                    .defaultValue();

    private int printJobMetricsInfoInterval =
            ServerConfigOptions.MasterServerConfigOptions.PRINT_JOB_METRICS_INFO_INTERVAL
                    .defaultValue();

    private int jobMetricsBackupInterval =
            ServerConfigOptions.MasterServerConfigOptions.JOB_METRICS_BACKUP_INTERVAL
                    .defaultValue();

    private int jobMetricsPartitionCount =
            ServerConfigOptions.MasterServerConfigOptions.JOB_METRICS_PARTITION_COUNT
                    .defaultValue();

    private ThreadShareMode taskExecutionThreadShareMode =
            ServerConfigOptions.WorkerServerConfigOptions.TASK_EXECUTION_THREAD_SHARE_MODE
                    .defaultValue();

    private SlotServiceConfig slotServiceConfig =
            ServerConfigOptions.WorkerServerConfigOptions.SLOT_SERVICE.defaultValue();

    private CheckpointConfig checkpointConfig =
            ServerConfigOptions.MasterServerConfigOptions.CHECKPOINT.defaultValue();

    private CoordinatorServiceConfig coordinatorServiceConfig =
            ServerConfigOptions.MasterServerConfigOptions.COORDINATOR_SERVICE.defaultValue();

    private ConnectorJarStorageConfig connectorJarStorageConfig =
            ServerConfigOptions.MasterServerConfigOptions.CONNECTOR_JAR_STORAGE_CONFIG
                    .defaultValue();

    private boolean classloaderCacheMode =
            ServerConfigOptions.CLASSLOADER_CACHE_MODE.defaultValue();

    private QueueType queueType =
            ServerConfigOptions.WorkerServerConfigOptions.QUEUE_TYPE.defaultValue();
    private int historyJobExpireMinutes =
            ServerConfigOptions.MasterServerConfigOptions.HISTORY_JOB_EXPIRE_MINUTES.defaultValue();

    private long stateCleanupDelayMillis =
            ServerConfigOptions.MasterServerConfigOptions.STATE_CLEANUP_DELAY_MILLIS.defaultValue();

    private ClusterRole clusterRole = ClusterRole.MASTER_AND_WORKER;

    private String eventReportHttpApi;
    private Map<String, String> eventReportHttpHeaders = Collections.emptyMap();

    private ExecutionMode mode = ExecutionMode.CLUSTER;

    private TelemetryConfig telemetryConfig = ServerConfigOptions.TELEMETRY.defaultValue();

    private ScheduleStrategy scheduleStrategy =
            ServerConfigOptions.MasterServerConfigOptions.JOB_SCHEDULE_STRATEGY.defaultValue();

    private HttpConfig httpConfig =
            ServerConfigOptions.MasterServerConfigOptions.HTTP.defaultValue();

    /**
     * Stain trace sampling and persistence knobs used by source, transform, sink, and reporters.
     */
    private boolean stainTraceEnabled = ServerConfigOptions.STAIN_TRACE_ENABLED.defaultValue();

    private int stainTraceSampleRate = ServerConfigOptions.STAIN_TRACE_SAMPLE_RATE.defaultValue();
    private int stainTraceMaxTracesPerSecondPerWorker =
            ServerConfigOptions.STAIN_TRACE_MAX_TRACES_PER_SECOND_PER_WORKER.defaultValue();
    private int stainTraceMaxEntriesPerTrace =
            ServerConfigOptions.STAIN_TRACE_MAX_ENTRIES_PER_TRACE.defaultValue();
    private boolean stainTracePropagateToAllSplits =
            ServerConfigOptions.STAIN_TRACE_PROPAGATE_TO_ALL_SPLITS.defaultValue();

    private String stainTraceFileBasePath = null;

    private int stainTraceFileMaxEventsPerFile =
            ServerConfigOptions.STAIN_TRACE_FILE_MAX_EVENTS_PER_FILE.defaultValue();

    private int stainTraceFileMaxSizeMb =
            ServerConfigOptions.STAIN_TRACE_FILE_MAX_SIZE_MB.defaultValue();

    private int stainTraceFileFlushIntervalSeconds =
            ServerConfigOptions.STAIN_TRACE_FILE_FLUSH_INTERVAL_SECONDS.defaultValue();

    private MetadataConfig metadataConfig = ServerConfigOptions.METADATA.defaultValue();

    public void setBackupCount(int newBackupCount) {
        checkBackupCount(newBackupCount, 0);
        this.backupCount = newBackupCount;
    }

    public void setScheduleStrategy(ScheduleStrategy scheduleStrategy) {
        this.scheduleStrategy = scheduleStrategy;
    }

    public void setPrintExecutionInfoInterval(int printExecutionInfoInterval) {
        checkPositive(
                printExecutionInfoInterval,
                ServerConfigOptions.MasterServerConfigOptions.PRINT_EXECUTION_INFO_INTERVAL
                        + " must be > 0");
        this.printExecutionInfoInterval = printExecutionInfoInterval;
    }

    public void setPrintJobMetricsInfoInterval(int printJobMetricsInfoInterval) {
        checkPositive(
                printJobMetricsInfoInterval,
                ServerConfigOptions.MasterServerConfigOptions.PRINT_JOB_METRICS_INFO_INTERVAL
                        + " must be > 0");
        this.printJobMetricsInfoInterval = printJobMetricsInfoInterval;
    }

    public void setJobMetricsBackupInterval(int jobMetricsBackupInterval) {
        checkPositive(
                jobMetricsBackupInterval,
                ServerConfigOptions.MasterServerConfigOptions.JOB_METRICS_BACKUP_INTERVAL
                        + " must be > 0");
        this.jobMetricsBackupInterval = jobMetricsBackupInterval;
    }

    public void setJobMetricsPartitionCount(int jobMetricsPartitionCount) {
        checkPositive(
                jobMetricsPartitionCount,
                ServerConfigOptions.MasterServerConfigOptions.JOB_METRICS_PARTITION_COUNT
                        + " must be > 0");
        this.jobMetricsPartitionCount = jobMetricsPartitionCount;
    }

    public void setTaskExecutionThreadShareMode(ThreadShareMode taskExecutionThreadShareMode) {
        checkNotNull(queueType);
        this.taskExecutionThreadShareMode = taskExecutionThreadShareMode;
    }

    public void setHistoryJobExpireMinutes(int historyJobExpireMinutes) {
        checkPositive(
                historyJobExpireMinutes,
                ServerConfigOptions.MasterServerConfigOptions.HISTORY_JOB_EXPIRE_MINUTES
                        + " must be > 0");
        this.historyJobExpireMinutes = historyJobExpireMinutes;
    }

    public void setStateCleanupDelayMillis(long stateCleanupDelayMillis) {
        if (stateCleanupDelayMillis < 0) {
            throw new IllegalArgumentException(
                    ServerConfigOptions.MasterServerConfigOptions.STATE_CLEANUP_DELAY_MILLIS
                            + " must be >= 0");
        }
        this.stateCleanupDelayMillis = stateCleanupDelayMillis;
    }

    public EngineConfig setQueueType(QueueType queueType) {
        checkNotNull(queueType);
        this.queueType = queueType;
        return this;
    }

    public enum ClusterRole {
        MASTER_AND_WORKER,
        MASTER,
        WORKER
    }

    public EngineConfig setEventReportHttpApi(String eventReportHttpApi) {
        this.eventReportHttpApi = eventReportHttpApi;
        return this;
    }

    public EngineConfig setEventReportHttpHeaders(Map<String, String> eventReportHttpHeaders) {
        this.eventReportHttpHeaders = eventReportHttpHeaders;
        return this;
    }

    public void setStainTraceSampleRate(int stainTraceSampleRate) {
        checkPositive(
                stainTraceSampleRate, ServerConfigOptions.STAIN_TRACE_SAMPLE_RATE + " must be > 0");
        this.stainTraceSampleRate = stainTraceSampleRate;
    }

    public void setStainTraceMaxTracesPerSecondPerWorker(
            int stainTraceMaxTracesPerSecondPerWorker) {
        if (stainTraceMaxTracesPerSecondPerWorker < 0) {
            throw new IllegalArgumentException(
                    ServerConfigOptions.STAIN_TRACE_MAX_TRACES_PER_SECOND_PER_WORKER
                            + " must be >= 0");
        }
        this.stainTraceMaxTracesPerSecondPerWorker = stainTraceMaxTracesPerSecondPerWorker;
    }

    public void setStainTraceMaxEntriesPerTrace(int stainTraceMaxEntriesPerTrace) {
        checkPositive(
                stainTraceMaxEntriesPerTrace,
                ServerConfigOptions.STAIN_TRACE_MAX_ENTRIES_PER_TRACE + " must be > 0");
        this.stainTraceMaxEntriesPerTrace = stainTraceMaxEntriesPerTrace;
    }

    public void setStainTracePropagateToAllSplits(boolean stainTracePropagateToAllSplits) {
        this.stainTracePropagateToAllSplits = stainTracePropagateToAllSplits;
    }

    public String getStainTraceFileBasePath() {
        return stainTraceFileBasePath;
    }

    public void setStainTraceFileBasePath(String stainTraceFileBasePath) {
        this.stainTraceFileBasePath = stainTraceFileBasePath;
    }

    public void setStainTraceFileMaxEventsPerFile(int stainTraceFileMaxEventsPerFile) {
        checkPositive(
                stainTraceFileMaxEventsPerFile,
                ServerConfigOptions.STAIN_TRACE_FILE_MAX_EVENTS_PER_FILE + " must be > 0");
        this.stainTraceFileMaxEventsPerFile = stainTraceFileMaxEventsPerFile;
    }

    public void setStainTraceFileMaxSizeMb(int stainTraceFileMaxSizeMb) {
        checkPositive(
                stainTraceFileMaxSizeMb,
                ServerConfigOptions.STAIN_TRACE_FILE_MAX_SIZE_MB + " must be > 0");
        this.stainTraceFileMaxSizeMb = stainTraceFileMaxSizeMb;
    }

    public void setStainTraceFileFlushIntervalSeconds(int stainTraceFileFlushIntervalSeconds) {
        checkPositive(
                stainTraceFileFlushIntervalSeconds,
                ServerConfigOptions.STAIN_TRACE_FILE_FLUSH_INTERVAL_SECONDS + " must be > 0");
        this.stainTraceFileFlushIntervalSeconds = stainTraceFileFlushIntervalSeconds;
    }
}
