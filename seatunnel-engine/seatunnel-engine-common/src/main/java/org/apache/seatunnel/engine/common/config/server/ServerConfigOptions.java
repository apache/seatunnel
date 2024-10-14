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

package org.apache.seatunnel.engine.common.config.server;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Map;

public class ServerConfigOptions {

    public static final Option<Integer> BACKUP_COUNT =
            Options.key("backup-count")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The number of backup copies of each partition.");

    public static final Option<Integer> PRINT_EXECUTION_INFO_INTERVAL =
            Options.key("print-execution-info-interval")
                    .intType()
                    .defaultValue(60)
                    .withDescription(
                            "The interval (in seconds) between two consecutive executions of the print execution info task.");

    public static final Option<Integer> PRINT_JOB_METRICS_INFO_INTERVAL =
            Options.key("print-job-metrics-info-interval")
                    .intType()
                    .defaultValue(60)
                    .withDescription("The interval (in seconds) of job print metrics info");

    public static final Option<Integer> JOB_METRICS_BACKUP_INTERVAL =
            Options.key("job-metrics-backup-interval")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The interval (in seconds) of job metrics backups");

    public static final Option<ThreadShareMode> TASK_EXECUTION_THREAD_SHARE_MODE =
            Options.key("task_execution_thread_share_mode")
                    .type(new TypeReference<ThreadShareMode>() {})
                    .defaultValue(ThreadShareMode.OFF)
                    .withDescription(
                            "The thread sharing mode of TaskExecutionServer, including ALL, OFF, PART. Default is OFF");

    public static final Option<Boolean> DYNAMIC_SLOT =
            Options.key("dynamic-slot")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to use dynamic slot.");

    public static final Option<Integer> SLOT_NUM =
            Options.key("slot-num")
                    .intType()
                    .defaultValue(2)
                    .withDescription(
                            "The number of slots. Only valid when dynamic slot is disabled.");

    public static final Option<Integer> CHECKPOINT_INTERVAL =
            Options.key("interval")
                    .intType()
                    .defaultValue(300000)
                    .withDescription(
                            "The interval (in milliseconds) between two consecutive checkpoints.");

    public static final Option<Integer> CHECKPOINT_TIMEOUT =
            Options.key("timeout")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("The timeout (in milliseconds) for a checkpoint.");

    public static final Option<Integer> SCHEMA_CHANGE_CHECKPOINT_TIMEOUT =
            Options.key("schema-change-timeout")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "The timeout (in milliseconds) for a schema change checkpoint.");

    public static final Option<String> CHECKPOINT_STORAGE_TYPE =
            Options.key("type")
                    .stringType()
                    .defaultValue("localfile")
                    .withDescription("The checkpoint storage type.");

    public static final Option<Integer> CHECKPOINT_STORAGE_MAX_RETAINED =
            Options.key("max-retained")
                    .intType()
                    .defaultValue(20)
                    .withDescription("The maximum number of retained checkpoints.");

    public static final Option<QueueType> QUEUE_TYPE =
            Options.key("queue-type")
                    .type(new TypeReference<QueueType>() {})
                    .defaultValue(QueueType.BLOCKINGQUEUE)
                    .withDescription("The internal data cache queue type.");

    public static final Option<CheckpointStorageConfig> CHECKPOINT_STORAGE =
            Options.key("storage")
                    .type(new TypeReference<CheckpointStorageConfig>() {})
                    .defaultValue(new CheckpointStorageConfig())
                    .withDescription("The checkpoint storage configuration.");

    public static final Option<SlotServiceConfig> SLOT_SERVICE =
            Options.key("slot-service")
                    .type(new TypeReference<SlotServiceConfig>() {})
                    .defaultValue(new SlotServiceConfig())
                    .withDescription("The slot service configuration.");

    public static final Option<CheckpointConfig> CHECKPOINT =
            Options.key("checkpoint")
                    .type(new TypeReference<CheckpointConfig>() {})
                    .defaultValue(new CheckpointConfig())
                    .withDescription("The checkpoint configuration.");

    public static final Option<Map<String, String>> CHECKPOINT_STORAGE_PLUGIN_CONFIG =
            Options.key("plugin-config")
                    .type(new TypeReference<Map<String, String>>() {})
                    .noDefaultValue()
                    .withDescription("The checkpoint storage instance configuration.");
    public static final Option<Integer> HISTORY_JOB_EXPIRE_MINUTES =
            Options.key("history-job-expire-minutes")
                    .intType()
                    .defaultValue(1440)
                    .withDescription("The expire time of history jobs.time unit minute");

    public static final Option<Boolean> ENABLE_CONNECTOR_JAR_STORAGE =
            Options.key("enable")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription(
                            "Enable the engine server Jar package storage service,"
                                    + " automatically upload connector Jar packages and dependent third-party Jar packages"
                                    + " to the server before job execution."
                                    + " Enabling this configuration does not require the server to hold all connector Jar packages");

    public static final Option<ConnectorJarStorageMode> CONNECTOR_JAR_STORAGE_MODE =
            Options.key("connector-jar-storage-mode")
                    .enumType(ConnectorJarStorageMode.class)
                    .defaultValue(ConnectorJarStorageMode.SHARED)
                    .withDescription(
                            "The storage mode of the connector jar package, including SHARED, ISOLATED. Default is SHARED");

    public static final Option<String> CONNECTOR_JAR_STORAGE_PATH =
            Options.key("connector-jar-storage-path")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The user defined connector jar storage path.");

    public static final Option<Integer> CONNECTOR_JAR_CLEANUP_TASK_INTERVAL =
            Options.key("connector-jar-cleanup-task-interval")
                    .intType()
                    .defaultValue(3600)
                    .withDescription("The user defined connector jar cleanup task interval.");

    public static final Option<Integer> CONNECTOR_JAR_EXPIRY_TIME =
            Options.key("connector-jar-expiry-time")
                    .intType()
                    .defaultValue(600)
                    .withDescription("The user defined connector jar expiry time.");

    public static final Option<String> CONNECTOR_JAR_HA_STORAGE_TYPE =
            Options.key("type")
                    .stringType()
                    .defaultValue("localfile")
                    .withDescription("The connector jar HA storage type.");

    public static final Option<Map<String, String>> CONNECTOR_JAR_HA_STORAGE_PLUGIN_CONFIG =
            Options.key("plugin-config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("The connector jar HA storage instance configuration.");

    public static final Option<ConnectorJarHAStorageConfig> CONNECTOR_JAR_HA_STORAGE_CONFIG =
            Options.key("jar-ha-storage")
                    .type(new TypeReference<ConnectorJarHAStorageConfig>() {})
                    .defaultValue(new ConnectorJarHAStorageConfig())
                    .withDescription("The connector jar ha storage configuration.");

    public static final Option<ConnectorJarStorageConfig> CONNECTOR_JAR_STORAGE_CONFIG =
            Options.key("jar-storage")
                    .type(new TypeReference<ConnectorJarStorageConfig>() {})
                    .defaultValue(new ConnectorJarStorageConfig())
                    .withDescription("The connector jar storage configuration.");

    public static final Option<Boolean> CLASSLOADER_CACHE_MODE =
            Options.key("classloader-cache-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to use classloader cache mode. With cache mode, all jobs share the same classloader if the jars are the same");

    public static final Option<Boolean> TELEMETRY_METRIC_ENABLED =
            Options.key("enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether open metrics export.");

    public static final Option<TelemetryMetricConfig> TELEMETRY_METRIC =
            Options.key("metric")
                    .type(new TypeReference<TelemetryMetricConfig>() {})
                    .defaultValue(new TelemetryMetricConfig())
                    .withDescription("The telemetry metric configuration.");

    public static final Option<TelemetryConfig> TELEMETRY =
            Options.key("telemetry")
                    .type(new TypeReference<TelemetryConfig>() {})
                    .defaultValue(new TelemetryConfig())
                    .withDescription("The telemetry configuration.");

    public static final Option<Integer> PORT =
            Options.key("port")
                    .intType()
                    .defaultValue(8080)
                    .withDescription("The port of the http server.");

    public static final Option<Boolean> ENABLE_HTTP =
            Options.key("enable-http")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable the http server.");

    public static final Option<String> CONTEXT_PATH =
            Options.key("context-path")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The context path of the http server.");

    public static final Option<HttpConfig> HTTP =
            Options.key("http")
                    .type(new TypeReference<HttpConfig>() {})
                    .defaultValue(new HttpConfig())
                    .withDescription("The http configuration.");

    public static final String EVENT_REPORT_HTTP = "event-report-http";
    public static final String EVENT_REPORT_HTTP_URL = "url";
    public static final String EVENT_REPORT_HTTP_HEADERS = "headers";
}
