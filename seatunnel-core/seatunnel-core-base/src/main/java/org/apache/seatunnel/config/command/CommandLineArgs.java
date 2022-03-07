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

package org.apache.seatunnel.config.command;

import org.apache.seatunnel.common.config.DeployMode;

import java.util.List;

public class CommandLineArgs {

    private String deployMode = DeployMode.CLIENT.getName();
    private final String configFile;
    private final boolean testConfig;
    private List<String> variables;

    public CommandLineArgs(String configFile, boolean testConfig, List<String> variables) {
        this.configFile = configFile;
        this.testConfig = testConfig;
        this.variables = variables;
    }

    public CommandLineArgs(String deployMode, String configFile, boolean testConfig) {
        this.deployMode = deployMode;
        this.configFile = configFile;
        this.testConfig = testConfig;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public String getConfigFile() {
        return configFile;
    }

    public boolean isTestConfig() {
        return testConfig;
    }

    public List<String> getVariables() {
        return variables;
    }

}
