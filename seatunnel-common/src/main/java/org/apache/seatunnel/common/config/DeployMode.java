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

package org.apache.seatunnel.common.config;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum DeployMode {
    /**
     * Spark
     */
    CLIENT("client"),
    CLUSTER("cluster"),

    /**
     * Flink
     */
    RUN("run"),
    RUN_APPLICATION("run-application");

    private final String deployMode;

    DeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getDeployMode() {
        return deployMode;
    }

    private static final Map<String, DeployMode> NAME_MAP = Arrays.stream(DeployMode.values())
        .collect(Collectors.toMap(DeployMode::getDeployMode, Function.identity()));

    public static Optional<DeployMode> from(String deployMode) {
        return Optional.ofNullable(NAME_MAP.get(deployMode.toLowerCase()));
    }
}
