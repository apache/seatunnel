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

import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
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

@Data
public class EngineConfig {

    private int backupCount = ServerConfigOptions.BACKUP_COUNT.defaultValue();
    private int printExecutionInfoInterval =
            ServerConfigOptions.PRINT_EXECUTION_INFO_INTERVAL.defaultValue();

    private int printJobMetricsInfoInterval =
            ServerConfigOptions.PRINT_JOB_METRICS_INFO_INTERVAL.defaultValue();

    private int jobMetricsBackupInterval =
            ServerConfigOptions.JOB_METRICS_BACKUP_INTERVAL.defaultValue();

    private ThreadShareMode taskExecutionThreadShareMode =
            ServerConfigOptions.TASK_EXECUTION_THREAD_SHARE_MODE.defaultValue();

    private SlotServiceConfig slotServiceConfig = ServerConfigOptions.SLOT_SERVICE.defaultValue();

    private CheckpointConfig checkpointConfig = ServerConfigOptions.CHECKPOINT.defaultValue();

    private ConnectorJarStorageConfig connectorJarStorageConfig =
            ServerConfigOptions.CONNECTOR_JAR_STORAGE_CONFIG.defaultValue();

    private boolean classloaderCacheMode =
            ServerConfigOptions.CLASSLOADER_CACHE_MODE.defaultValue();

    private QueueType queueType = ServerConfigOptions.QUEUE_TYPE.defaultValue();
    private int historyJobExpireMinutes =
            ServerConfigOptions.HISTORY_JOB_EXPIRE_MINUTES.defaultValue();

    private ClusterRole clusterRole = ClusterRole.MASTER_AND_WORKER;

    private String eventReportHttpApi;
    private Map<String, String> eventReportHttpHeaders = Collections.emptyMap();

    private ExecutionMode mode = ExecutionMode.CLUSTER;

    private TelemetryConfig telemetryConfig = ServerConfigOptions.TELEMETRY.defaultValue();

    private ScheduleStrategy scheduleStrategy =
            ServerConfigOptions.JOB_SCHEDULE_STRATEGY.defaultValue();

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
                ServerConfigOptions.PRINT_EXECUTION_INFO_INTERVAL + " must be > 0");
        this.printExecutionInfoInterval = printExecutionInfoInterval;
    }

    public void setPrintJobMetricsInfoInterval(int printJobMetricsInfoInterval) {
        checkPositive(
                printJobMetricsInfoInterval,
                ServerConfigOptions.PRINT_JOB_METRICS_INFO_INTERVAL + " must be > 0");
        this.printJobMetricsInfoInterval = printJobMetricsInfoInterval;
    }

    public void setJobMetricsBackupInterval(int jobMetricsBackupInterval) {
        checkPositive(
                jobMetricsBackupInterval,
                ServerConfigOptions.JOB_METRICS_BACKUP_INTERVAL + " must be > 0");
        this.jobMetricsBackupInterval = jobMetricsBackupInterval;
    }

    public void setTaskExecutionThreadShareMode(ThreadShareMode taskExecutionThreadShareMode) {
        checkNotNull(queueType);
        this.taskExecutionThreadShareMode = taskExecutionThreadShareMode;
    }

    public void setHistoryJobExpireMinutes(int historyJobExpireMinutes) {
        checkPositive(
                historyJobExpireMinutes,
                ServerConfigOptions.HISTORY_JOB_EXPIRE_MINUTES + " must be > 0");
        this.historyJobExpireMinutes = historyJobExpireMinutes;
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
}
