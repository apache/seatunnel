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

import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkRuntimeEnvironment extends AbstractFlinkRuntimeEnvironment
        implements RuntimeEnvironment {

    private static volatile FlinkRuntimeEnvironment INSTANCE = null;

    private FlinkRuntimeEnvironment(Config config) {
        super(config);
    }

    @Override
    public FlinkRuntimeEnvironment setConfig(Config config) {
        this.config = config;
        return this;
    }

    @Override
    public FlinkRuntimeEnvironment prepare() {
        createStreamEnvironment();
        if (config.hasPath("job.name")) {
            jobName = config.getString("job.name");
        }
        return this;
    }

    @Override
    public FlinkRuntimeEnvironment setJobMode(JobMode jobMode) {
        this.jobMode = jobMode;
        return this;
    }

    public static FlinkRuntimeEnvironment getInstance(Config config) {
        if (INSTANCE == null) {
            synchronized (FlinkRuntimeEnvironment.class) {
                if (INSTANCE == null) {
                    INSTANCE = new FlinkRuntimeEnvironment(config);
                }
            }
        }
        return INSTANCE;
    }
}
