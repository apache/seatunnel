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

package org.apache.seatunnel.flink;

import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.flink.util.ConfigKeyName;
import org.apache.seatunnel.flink.util.EnvironmentUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.TernaryBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkEnvironment implements RuntimeEnv {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkEnvironment.class);

    private Config config;

    private StreamExecutionEnvironment environment;

    private StreamTableEnvironment tableEnvironment;

    private ExecutionEnvironment batchEnvironment;

    private BatchTableEnvironment batchTableEnvironment;

    private boolean isStreaming;

    private String jobName = "seatunnel";

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return EnvironmentUtil.checkRestartStrategy(config);
    }

    @Override
    public void prepare(Boolean isStreaming) {
        this.isStreaming = isStreaming;
        if (isStreaming) {
            createStreamEnvironment();
            createStreamTableEnvironment();
        } else {
            createExecutionEnvironment();
            createBatchTableEnvironment();
        }
        if (config.hasPath("job.name")) {
            jobName = config.getString("job.name");
        }
    }

    public String getJobName() {
        return jobName;
    }

    public boolean isStreaming() {
        return isStreaming;
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return environment;
    }

    public StreamTableEnvironment getStreamTableEnvironment() {
        return tableEnvironment;
    }

    private void createStreamTableEnvironment() {
        // use blink and streammode
        EnvironmentSettings.Builder envBuilder = EnvironmentSettings.newInstance()
                .inStreamingMode();
        if (this.config.hasPath(ConfigKeyName.PLANNER) && "blink".equals(this.config.getString(ConfigKeyName.PLANNER))) {
            envBuilder.useBlinkPlanner();
        } else {
            envBuilder.useOldPlanner();
        }
        EnvironmentSettings environmentSettings = envBuilder.build();

        tableEnvironment = StreamTableEnvironment.create(getStreamExecutionEnvironment(), environmentSettings);
        TableConfig config = tableEnvironment.getConfig();
        if (this.config.hasPath(ConfigKeyName.MAX_STATE_RETENTION_TIME) && this.config.hasPath(ConfigKeyName.MIN_STATE_RETENTION_TIME)) {
            long max = this.config.getLong(ConfigKeyName.MAX_STATE_RETENTION_TIME);
            long min = this.config.getLong(ConfigKeyName.MIN_STATE_RETENTION_TIME);
            config.setIdleStateRetentionTime(Time.seconds(min), Time.seconds(max));
        }
    }

    private void createStreamEnvironment() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        setTimeCharacteristic();

        setCheckpoint();

        EnvironmentUtil.setRestartStrategy(config, environment.getConfig());

        if (config.hasPath(ConfigKeyName.BUFFER_TIMEOUT_MILLIS)) {
            long timeout = config.getLong(ConfigKeyName.BUFFER_TIMEOUT_MILLIS);
            environment.setBufferTimeout(timeout);
        }

        if (config.hasPath(ConfigKeyName.PARALLELISM)) {
            int parallelism = config.getInt(ConfigKeyName.PARALLELISM);
            environment.setParallelism(parallelism);
        }

        if (config.hasPath(ConfigKeyName.MAX_PARALLELISM)) {
            int max = config.getInt(ConfigKeyName.MAX_PARALLELISM);
            environment.setMaxParallelism(max);
        }
    }

    public ExecutionEnvironment getBatchEnvironment() {
        return batchEnvironment;
    }

    public BatchTableEnvironment getBatchTableEnvironment() {
        return batchTableEnvironment;
    }

    private void createExecutionEnvironment() {
        batchEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        if (config.hasPath(ConfigKeyName.PARALLELISM)) {
            int parallelism = config.getInt(ConfigKeyName.PARALLELISM);
            batchEnvironment.setParallelism(parallelism);
        }
        EnvironmentUtil.setRestartStrategy(config, batchEnvironment.getConfig());
    }

    private void createBatchTableEnvironment() {
        batchTableEnvironment = BatchTableEnvironment.create(batchEnvironment);
    }

    private void setTimeCharacteristic() {
        if (config.hasPath(ConfigKeyName.TIME_CHARACTERISTIC)) {
            String timeType = config.getString(ConfigKeyName.TIME_CHARACTERISTIC);
            switch (timeType.toLowerCase()) {
                case "event-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                    break;
                case "ingestion-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
                    break;
                case "processing-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
                    break;
                default:
                    LOGGER.warn("set time-characteristic failed, unknown time-characteristic [{}],only support event-time,ingestion-time,processing-time", timeType);
                    break;
            }
        }
    }

    private void setCheckpoint() {
        if (config.hasPath(ConfigKeyName.CHECKPOINT_INTERVAL)) {
            CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
            long interval = config.getLong(ConfigKeyName.CHECKPOINT_INTERVAL);
            environment.enableCheckpointing(interval);

            if (config.hasPath(ConfigKeyName.CHECKPOINT_MODE)) {
                String mode = config.getString(ConfigKeyName.CHECKPOINT_MODE);
                switch (mode.toLowerCase()) {
                    case "exactly-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                        break;
                    case "at-least-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
                        break;
                    default:
                        LOGGER.warn("set checkpoint.mode failed, unknown checkpoint.mode [{}],only support exactly-once,at-least-once", mode);
                        break;
                }
            }

            if (config.hasPath(ConfigKeyName.CHECKPOINT_TIMEOUT)) {
                long timeout = config.getLong(ConfigKeyName.CHECKPOINT_TIMEOUT);
                checkpointConfig.setCheckpointTimeout(timeout);
            }

            if (config.hasPath(ConfigKeyName.CHECKPOINT_DATA_URI)) {
                String uri = config.getString(ConfigKeyName.CHECKPOINT_DATA_URI);
                StateBackend fsStateBackend = new FsStateBackend(uri);
                if (config.hasPath(ConfigKeyName.STATE_BACKEND)) {
                    String stateBackend = config.getString(ConfigKeyName.STATE_BACKEND);
                    if ("rocksdb".equalsIgnoreCase(stateBackend)) {
                        StateBackend rocksDBStateBackend = new RocksDBStateBackend(fsStateBackend, TernaryBoolean.TRUE);
                        environment.setStateBackend(rocksDBStateBackend);
                    }
                } else {
                    environment.setStateBackend(fsStateBackend);
                }
            }

            if (config.hasPath(ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS)) {
                int max = config.getInt(ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS);
                checkpointConfig.setMaxConcurrentCheckpoints(max);
            }

            if (config.hasPath(ConfigKeyName.CHECKPOINT_CLEANUP_MODE)) {
                boolean cleanup = config.getBoolean(ConfigKeyName.CHECKPOINT_CLEANUP_MODE);
                if (cleanup) {
                    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
                } else {
                    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

                }
            }

            if (config.hasPath(ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS)) {
                long minPause = config.getLong(ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS);
                checkpointConfig.setMinPauseBetweenCheckpoints(minPause);
            }

            if (config.hasPath(ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS)) {
                int failNum = config.getInt(ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS);
                checkpointConfig.setTolerableCheckpointFailureNumber(failNum);
            }
        }
    }

}
