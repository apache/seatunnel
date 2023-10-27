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
}
