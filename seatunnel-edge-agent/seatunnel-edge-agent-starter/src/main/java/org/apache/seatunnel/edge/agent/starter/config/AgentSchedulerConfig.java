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

package org.apache.seatunnel.edge.agent.starter.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;

import lombok.Getter;

import java.io.Serializable;
import java.util.Objects;

@Getter
public class AgentSchedulerConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long idleSleepMs;
    private final int bulkMaxSize;
    private final long flushIntervalMs;

    public AgentSchedulerConfig(ReadonlyConfig config) {
        Objects.requireNonNull(config, "config");
        ConfigValidator.of(config).validate(EdgeAgentRuntimeOptionRules.runtimeRule());

        this.idleSleepMs = config.get(EdgeAgentRuntimeOptions.RUNTIME_IDLE_SLEEP_MS);
        requireAtLeast(this.idleSleepMs, 1L, "agent.idle-sleep-ms");

        this.bulkMaxSize = config.get(EdgeAgentRuntimeOptions.RUNTIME_BULK_MAX_SIZE);
        requireAtLeast(this.bulkMaxSize, 1, "agent.bulk-max-size");

        this.flushIntervalMs = config.get(EdgeAgentRuntimeOptions.RUNTIME_FLUSH_INTERVAL_MS);
        requireAtLeast(this.flushIntervalMs, 1L, "agent.flush-interval-ms");
    }

    public static AgentSchedulerConfig from(ReadonlyConfig config) {
        return new AgentSchedulerConfig(config);
    }

    private static void requireAtLeast(int value, int min, String optionPath) {
        if (value < min) {
            throw new IllegalArgumentException(optionPath + " must be >= " + min + " when set.");
        }
    }

    private static void requireAtLeast(long value, long min, String optionPath) {
        if (value < min) {
            throw new IllegalArgumentException(optionPath + " must be >= " + min + " when set.");
        }
    }
}
