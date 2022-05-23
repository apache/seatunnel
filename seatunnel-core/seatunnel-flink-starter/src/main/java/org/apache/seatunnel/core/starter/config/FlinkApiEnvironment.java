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

package org.apache.seatunnel.core.starter.config;

import org.apache.seatunnel.apis.base.env.RuntimeEnv;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;

public class FlinkApiEnvironment implements RuntimeEnv {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkApiEnvironment.class);

    private Config config;

    @Override
    public FlinkApiEnvironment setConfig(Config config) {
        this.config = config;
        return this;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        // todo
        return null;
    }

    @Override
    public FlinkApiEnvironment prepare() {
        // todo
        return null;
    }

    @Override
    public FlinkApiEnvironment setJobMode(JobMode mode) {
        return null;
    }

    @Override
    public JobMode getJobMode() {
        return null;
    }

    @Override
    public void registerPlugin(List<URL> pluginPaths) {

    }
}
