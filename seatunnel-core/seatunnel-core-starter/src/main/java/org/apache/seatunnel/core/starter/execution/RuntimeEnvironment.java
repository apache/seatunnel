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

package org.apache.seatunnel.core.starter.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;

import java.net.URL;
import java.util.List;

/**
 * Runtime environment for each engine, such as spark flink and st-engine, used to store the engine
 * context objects
 */
public interface RuntimeEnvironment {
    RuntimeEnvironment setConfig(Config config);

    Config getConfig();

    CheckResult checkConfig();

    RuntimeEnvironment prepare();

    RuntimeEnvironment setJobMode(JobMode mode);

    JobMode getJobMode();

    void registerPlugin(List<URL> pluginPaths);

    default void initialize(Config config) {
        this.setConfig(config.getConfig("env")).setJobMode(getJobMode(config)).prepare();
    }

    static JobMode getJobMode(Config config) {
        JobMode jobMode;
        Config envConfig = config.getConfig("env");
        if (envConfig.hasPath(EnvCommonOptions.JOB_MODE.key())) {
            jobMode = envConfig.getEnum(JobMode.class, EnvCommonOptions.JOB_MODE.key());
        } else {
            jobMode = JobMode.BATCH;
        }
        return jobMode;
    }

    static boolean getEnableCheckpoint(Config config) {
        boolean enableCheckpoint;
        Config envConfig = config.getConfig("env");
        if (envConfig.hasPath(EnvCommonOptions.CHECKPOINT_INTERVAL.key())) {
            enableCheckpoint = envConfig.getInt(EnvCommonOptions.CHECKPOINT_INTERVAL.key()) > 0;
        } else {
            enableCheckpoint = false;
        }
        return enableCheckpoint || getJobMode(config) == JobMode.STREAMING;
    }
}
