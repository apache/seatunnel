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

package org.apache.seatunnel.core.starter.flink.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.config.CheckResult;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public final class EnvironmentUtil {

    private EnvironmentUtil() {}

    public static void setRestartStrategy(Config config, ExecutionConfig executionConfig) {
        try {
            if (config.hasPath(ConfigKeyName.RESTART_STRATEGY)) {
                String restartStrategy = config.getString(ConfigKeyName.RESTART_STRATEGY);
                switch (restartStrategy.toLowerCase()) {
                    case "no":
                        executionConfig.setRestartStrategy(RestartStrategies.noRestart());
                        break;
                    case "fixed-delay":
                        int attempts = config.getInt(ConfigKeyName.RESTART_ATTEMPTS);
                        long delay = config.getLong(ConfigKeyName.RESTART_DELAY_BETWEEN_ATTEMPTS);
                        executionConfig.setRestartStrategy(
                                RestartStrategies.fixedDelayRestart(attempts, delay));
                        break;
                    case "failure-rate":
                        long failureInterval =
                                config.getLong(ConfigKeyName.RESTART_FAILURE_INTERVAL);
                        int rate = config.getInt(ConfigKeyName.RESTART_FAILURE_RATE);
                        long delayInterval = config.getLong(ConfigKeyName.RESTART_DELAY_INTERVAL);
                        executionConfig.setRestartStrategy(
                                RestartStrategies.failureRateRestart(
                                        rate,
                                        Time.of(failureInterval, TimeUnit.MILLISECONDS),
                                        Time.of(delayInterval, TimeUnit.MILLISECONDS)));
                        break;
                    default:
                        log.warn(
                                "set restart.strategy failed, unknown restart.strategy [{}],only support no,fixed-delay,failure-rate",
                                restartStrategy);
                }
            }
        } catch (Exception e) {
            log.warn("set restart.strategy in config '{}' exception", config, e);
        }
    }

    public static CheckResult checkRestartStrategy(Config config) {
        if (config.hasPath(ConfigKeyName.RESTART_STRATEGY)) {
            String restartStrategy = config.getString(ConfigKeyName.RESTART_STRATEGY);
            switch (restartStrategy.toLowerCase()) {
                case "fixed-delay":
                    if (!(config.hasPath(ConfigKeyName.RESTART_ATTEMPTS)
                            && config.hasPath(ConfigKeyName.RESTART_DELAY_BETWEEN_ATTEMPTS))) {
                        return CheckResult.error(
                                String.format(
                                        "fixed-delay restart strategy must set [%s],[%s]",
                                        ConfigKeyName.RESTART_ATTEMPTS,
                                        ConfigKeyName.RESTART_DELAY_BETWEEN_ATTEMPTS));
                    }
                    break;
                case "failure-rate":
                    if (!(config.hasPath(ConfigKeyName.RESTART_FAILURE_INTERVAL)
                            && config.hasPath(ConfigKeyName.RESTART_FAILURE_RATE)
                            && config.hasPath(ConfigKeyName.RESTART_DELAY_INTERVAL))) {
                        return CheckResult.error(
                                String.format(
                                        "failure-rate restart strategy must set [%s],[%s],[%s]",
                                        ConfigKeyName.RESTART_FAILURE_INTERVAL,
                                        ConfigKeyName.RESTART_FAILURE_RATE,
                                        ConfigKeyName.RESTART_DELAY_INTERVAL));
                    }
                    break;
                default:
                    return CheckResult.success();
            }
        }
        return CheckResult.success();
    }

    public static void initConfiguration(Config config, Configuration configuration) {
        if (config.hasPath("pipeline")) {
            Config pipeline = config.getConfig("pipeline");
            if (pipeline.hasPath("jars")) {
                configuration.setString(PipelineOptions.JARS.key(), pipeline.getString("jars"));
            }
            if (pipeline.hasPath("classpaths")) {
                configuration.setString(
                        PipelineOptions.CLASSPATHS.key(), pipeline.getString("classpaths"));
            }
        }
    }
}
