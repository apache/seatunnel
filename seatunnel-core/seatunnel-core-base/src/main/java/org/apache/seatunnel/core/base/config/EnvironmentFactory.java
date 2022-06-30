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

package org.apache.seatunnel.core.base.config;

import org.apache.seatunnel.apis.base.env.RuntimeEnv;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;

import org.apache.seatunnel.apis.base.env.RuntimeEnv;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.spark.SparkEnvironment;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.List;

/**
 * Used to create the {@link RuntimeEnv}.
 *
 * @param <ENVIRONMENT> environment type
 */
public class EnvironmentFactory<ENVIRONMENT extends RuntimeEnv> {

    private static final String PLUGIN_NAME_KEY = "plugin_name";

    private final Config config;
    private final EngineType engine;

    public EnvironmentFactory(Config config, EngineType engine) {
        this.config = config;
        this.engine = engine;
    }

    // todo:put this method into submodule to avoid dependency on the engine
    public synchronized ENVIRONMENT getEnvironment() {
        Config envConfig = config.getConfig("env");
        boolean enableHive = checkIsContainHive();
        ENVIRONMENT env;
        switch (engine) {
            case SPARK:
                env = (ENVIRONMENT) new SparkEnvironment().setEnableHive(enableHive);
                break;
            case FLINK:
                env = (ENVIRONMENT) new FlinkEnvironment();
                break;
            default:
                throw new IllegalArgumentException("Engine: " + engine + " is not supported");
        }
        env.setConfig(envConfig)
            .setJobMode(getJobMode(envConfig)).prepare();
        return env;
    }

    private boolean checkIsContainHive() {
        List<? extends Config> sourceConfigList = config.getConfigList(PluginType.SOURCE.getType());
        for (Config c : sourceConfigList) {
            if (c.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }
        List<? extends Config> sinkConfigList = config.getConfigList(PluginType.SINK.getType());
        for (Config c : sinkConfigList) {
            if (c.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }
        return false;
    }

    private JobMode getJobMode(Config envConfig) {
        JobMode jobMode;
        if (envConfig.hasPath("job.mode")) {
            jobMode = envConfig.getEnum(JobMode.class, "job.mode");
        } else {
            //Compatible with previous logic
            List<? extends Config> sourceConfigList = config.getConfigList(PluginType.SOURCE.getType());
            jobMode = sourceConfigList.get(0).getString(PLUGIN_NAME_KEY).toLowerCase().endsWith("stream") ? JobMode.STREAMING : JobMode.BATCH;
        }
        return jobMode;
    }

}
