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

import org.apache.seatunnel.engine.common.config.server.scheduler.ScheduleStrategyConfig;
import org.apache.seatunnel.engine.common.config.server.scheduler.WaitConfig;
import org.apache.seatunnel.engine.common.config.server.scheduler.WaitRescheduleConfig;
import org.apache.seatunnel.engine.common.config.server.scheduler.WindowScanAgingPriorityConfig;

import com.hazelcast.config.InvalidConfigurationException;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

public enum ScheduleStrategy {
    WAIT(
            new String[] {"wait"},
            WaitConfig::new,
            config -> {
                WaitConfig waitConfig = (WaitConfig) config;
                if (waitConfig.getSleepIntervalMillis() <= 0) {
                    throw new InvalidConfigurationException(
                            "wait.sleep-interval-millis must be > 0");
                }
            }),
    WAIT_RESCHEDULE(
            new String[] {"pending-job-reschedule"},
            WaitRescheduleConfig::new,
            config -> {
                WaitRescheduleConfig waitRescheduleConfig = (WaitRescheduleConfig) config;
                if (waitRescheduleConfig.getMaxRetryTimes() <= 0) {
                    throw new InvalidConfigurationException(
                            "pending-job-reschedule.max-retry-times must be > 0");
                }
                if (waitRescheduleConfig.getSleepIntervalMillis() <= 0) {
                    throw new InvalidConfigurationException(
                            "pending-job-reschedule.sleep-interval-millis must be > 0");
                }
            }),
    WINDOW_SCAN_AGING_PRIORITY(
            new String[] {"window-scan-aging-priority"},
            WindowScanAgingPriorityConfig::new,
            config -> {
                WindowScanAgingPriorityConfig windowScanAgingPriorityConfig =
                        (WindowScanAgingPriorityConfig) config;
                if (windowScanAgingPriorityConfig.getWindowSize() <= 0) {
                    throw new InvalidConfigurationException(
                            "window-scan-aging-priority.window-size must be > 0");
                }
                if (windowScanAgingPriorityConfig.getAgingThresholdMillis() <= 0) {
                    throw new InvalidConfigurationException(
                            "window-scan-aging-priority.aging-threshold-millis must be > 0");
                }
                if (windowScanAgingPriorityConfig.getSleepIntervalMillis() <= 0) {
                    throw new InvalidConfigurationException(
                            "window-scan-aging-priority.sleep-interval-millis must be > 0");
                }
            }),
    REJECT(new String[0], null, null);

    private final String[] configSectionKeys;
    private final Supplier<? extends ScheduleStrategyConfig> configSupplier;
    private final Consumer<? super ScheduleStrategyConfig> configValidator;

    ScheduleStrategy(
            String[] configSectionKeys,
            Supplier<? extends ScheduleStrategyConfig> configSupplier,
            Consumer<? super ScheduleStrategyConfig> configValidator) {
        this.configSectionKeys = configSectionKeys;
        this.configSupplier = configSupplier;
        this.configValidator = configValidator;
    }

    public boolean isWait() {
        return this == WAIT || this == WAIT_RESCHEDULE || this == WINDOW_SCAN_AGING_PRIORITY;
    }

    public boolean supportsConfigSection() {
        return configSupplier != null;
    }

    public String[] getConfigSectionKeys() {
        return configSectionKeys;
    }

    public boolean matchesConfigSectionKey(String key) {
        return Arrays.stream(configSectionKeys).anyMatch(k -> k.equals(key));
    }

    public static Optional<ScheduleStrategy> findByConfigSectionKey(String key) {
        return Arrays.stream(values()).filter(v -> v.matchesConfigSectionKey(key)).findFirst();
    }

    public ScheduleStrategyConfig createDefaultConfig() {
        if (configSupplier == null) {
            return null;
        }
        return configSupplier.get();
    }

    public void validateConfig(ScheduleStrategyConfig config) {
        if (configValidator != null) {
            configValidator.accept(config);
        }
    }
}
