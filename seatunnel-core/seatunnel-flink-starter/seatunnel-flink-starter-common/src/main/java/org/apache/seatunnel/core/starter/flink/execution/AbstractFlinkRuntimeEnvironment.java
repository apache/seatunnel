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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.flink.utils.ConfigKeyName;
import org.apache.seatunnel.core.starter.flink.utils.EnvironmentUtil;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TernaryBoolean;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractFlinkRuntimeEnvironment implements RuntimeEnvironment {

    protected Config config;
    protected StreamExecutionEnvironment environment;
    protected JobMode jobMode;
    protected String jobName = Constants.LOGO;

    protected AbstractFlinkRuntimeEnvironment(Config config) {
        this.initialize(config);
    }

    public abstract AbstractFlinkRuntimeEnvironment setConfig(Config config);

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return EnvironmentUtil.checkRestartStrategy(config);
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return environment;
    }

    protected void setCheckpoint() {
        if (jobMode == JobMode.BATCH) {
            log.warn(
                    "Disabled Checkpointing. In flink execution environment, checkpointing is not supported and not needed when executing jobs in BATCH mode");
        }
        long interval;
        if (config.hasPath(EnvCommonOptions.CHECKPOINT_INTERVAL.key())) {
            interval = config.getLong(EnvCommonOptions.CHECKPOINT_INTERVAL.key());
        } else if (config.hasPath(ConfigKeyName.CHECKPOINT_INTERVAL)) {
            log.warn(
                    "the parameter 'execution.checkpoint.interval' will be deprecated, please use common parameter 'checkpoint.interval' to set it");
            interval = config.getLong(ConfigKeyName.CHECKPOINT_INTERVAL);
        } else {
            interval = 10000L;
        }

        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        environment.enableCheckpointing(interval);

        if (config.hasPath(EnvCommonOptions.CHECKPOINT_TIMEOUT.key())) {
            long timeout = config.getLong(EnvCommonOptions.CHECKPOINT_TIMEOUT.key());
            checkpointConfig.setCheckpointTimeout(timeout);
        } else if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.CHECKPOINT_TIMEOUT)) {
            long timeout = config.getLong(ConfigKeyName.CHECKPOINT_TIMEOUT);
            checkpointConfig.setCheckpointTimeout(timeout);
        }

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.CHECKPOINT_MODE)) {
            String mode = config.getString(ConfigKeyName.CHECKPOINT_MODE);
            switch (mode.toLowerCase()) {
                case "exactly-once":
                    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                    break;
                case "at-least-once":
                    checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
                    break;
                default:
                    log.warn(
                            "set checkpoint.mode failed, unknown checkpoint.mode [{}],only support exactly-once,at-least-once",
                            mode);
                    break;
            }
        }

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.CHECKPOINT_DATA_URI)) {
            String uri = config.getString(ConfigKeyName.CHECKPOINT_DATA_URI);
            StateBackend fsStateBackend = new FsStateBackend(uri);
            if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.STATE_BACKEND)) {
                String stateBackend = config.getString(ConfigKeyName.STATE_BACKEND);
                if ("rocksdb".equalsIgnoreCase(stateBackend)) {
                    StateBackend rocksDBStateBackend =
                            new RocksDBStateBackend(fsStateBackend, TernaryBoolean.TRUE);
                    environment.setStateBackend(rocksDBStateBackend);
                }
            } else {
                environment.setStateBackend(fsStateBackend);
            }
        }

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS)) {
            int max = config.getInt(ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS);
            checkpointConfig.setMaxConcurrentCheckpoints(max);
        }

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.CHECKPOINT_CLEANUP_MODE)) {
            boolean cleanup = config.getBoolean(ConfigKeyName.CHECKPOINT_CLEANUP_MODE);
            if (cleanup) {
                checkpointConfig.enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
            } else {
                checkpointConfig.enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            }
        }

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS)) {
            long minPause = config.getLong(ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS);
            checkpointConfig.setMinPauseBetweenCheckpoints(minPause);
        }

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS)) {
            int failNum = config.getInt(ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS);
            checkpointConfig.setTolerableCheckpointFailureNumber(failNum);
        }
    }

    protected void createStreamEnvironment() {
        Configuration configuration = new Configuration();
        EnvironmentUtil.initConfiguration(config, configuration);
        environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        setTimeCharacteristic();
        setCheckpoint();

        EnvironmentUtil.setRestartStrategy(config, environment.getConfig());

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.BUFFER_TIMEOUT_MILLIS)) {
            long timeout = config.getLong(ConfigKeyName.BUFFER_TIMEOUT_MILLIS);
            environment.setBufferTimeout(timeout);
        }

        if (config.hasPath(EnvCommonOptions.PARALLELISM.key())) {
            int parallelism = config.getInt(EnvCommonOptions.PARALLELISM.key());
            environment.setParallelism(parallelism);
        } else if (config.hasPath(ConfigKeyName.PARALLELISM)) {
            log.warn(
                    "the parameter 'execution.parallelism' will be deprecated, please use common parameter 'parallelism' to set it");
            int parallelism = config.getInt(ConfigKeyName.PARALLELISM);
            environment.setParallelism(parallelism);
        }

        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.MAX_PARALLELISM)) {
            int max = config.getInt(ConfigKeyName.MAX_PARALLELISM);
            environment.setMaxParallelism(max);
        }

        if (this.jobMode.equals(JobMode.BATCH)) {
            environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
    }

    private void setTimeCharacteristic() {
        if (EnvironmentUtil.hasPathAndWaring(config, ConfigKeyName.TIME_CHARACTERISTIC)) {
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
                    log.warn(
                            "set time-characteristic failed, unknown time-characteristic [{}],only support event-time,ingestion-time,processing-time",
                            timeType);
                    break;
            }
        }
    }

    public boolean isStreaming() {
        return JobMode.STREAMING.equals(jobMode);
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    public JobMode getJobMode() {
        return jobMode;
    }

    @Override
    public void registerPlugin(List<URL> pluginPaths) {
        pluginPaths.forEach(url -> log.info("register plugins : {}", url));
        List<Configuration> configurations = new ArrayList<>();
        try {
            configurations.add(
                    (Configuration)
                            Objects.requireNonNull(
                                            ReflectionUtils.getDeclaredMethod(
                                                    StreamExecutionEnvironment.class,
                                                    "getConfiguration"))
                                    .orElseThrow(
                                            () ->
                                                    new RuntimeException(
                                                            "can't find "
                                                                    + "method: getConfiguration"))
                                    .invoke(this.environment));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        configurations.forEach(
                configuration -> {
                    List<String> jars = configuration.get(PipelineOptions.JARS);
                    if (jars == null) {
                        jars = new ArrayList<>();
                    }
                    jars.addAll(
                            pluginPaths.stream().map(URL::toString).collect(Collectors.toList()));
                    configuration.set(
                            PipelineOptions.JARS,
                            jars.stream().distinct().collect(Collectors.toList()));
                    List<String> classpath = configuration.get(PipelineOptions.CLASSPATHS);
                    if (classpath == null) {
                        classpath = new ArrayList<>();
                    }
                    classpath.addAll(
                            pluginPaths.stream().map(URL::toString).collect(Collectors.toList()));
                    configuration.set(
                            PipelineOptions.CLASSPATHS,
                            classpath.stream().distinct().collect(Collectors.toList()));
                });
    }
}
