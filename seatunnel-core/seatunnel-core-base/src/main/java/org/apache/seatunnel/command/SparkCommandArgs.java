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

package org.apache.seatunnel.command;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.config.EngineType;

import com.beust.jcommander.Parameter;

import java.util.List;

public class SparkCommandArgs implements CommandArgs {

    @Parameter(names = {"-c", "--config"},
        description = "config file",
        required = true)
    private String configFile = "application.conf";

    @Parameter(names = {"-e", "--deploy-mode"},
        description = "spark deploy mode",
        required = true,
        validateWith = DeployModeValidator.class)
    private String deployMode = DeployMode.CLIENT.getName();

    @Parameter(names = {"-m", "--master"},
        description = "spark master",
        required = true)
    private String master = null;

    @Parameter(names = {"-i", "--variable"},
        description = "variable substitution, such as -i city=beijing, or -i date=20190318")
    private List<String> variables = null;

    @Parameter(names = {"-t", "--check"},
        description = "check config")
    private boolean checkConfig = false;

    public String getConfigFile() {
        return configFile;
    }

    public DeployMode getDeployMode() {
        return DeployMode.from(deployMode);
    }

    public boolean isCheckConfig() {
        return checkConfig;
    }

    public String getMaster() {
        return master;
    }

    public List<String> getVariables() {
        return variables;
    }

    @Override
    public EngineType getEngineType() {
        return EngineType.SPARK;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public void setVariables(List<String> variables) {
        this.variables = variables;
    }

    public void setCheckConfig(boolean checkConfig) {
        this.checkConfig = checkConfig;
    }
}
