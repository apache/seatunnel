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

package org.apache.seatunnel.core.starter.spark.args;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.command.AbstractCommandArgs;
import org.apache.seatunnel.core.starter.command.DeployModeConverter;
import org.apache.seatunnel.core.starter.config.EngineType;

import com.beust.jcommander.Parameter;

public class SparkCommandArgs extends AbstractCommandArgs {

    @Parameter(names = {"-c", "--config"},
        description = "Config file",
        required = true)
    private String configFile;

    @Parameter(names = {"-e", "--deploy-mode"},
        description = "Spark deploy mode",
        required = true,
        converter = DeployModeConverter.class)
    private DeployMode deployMode;

    @Parameter(names = {"-m", "--master"},
        description = "Spark master",
        required = true)
    private String master = null;

    public String getMaster() {
        return master;
    }

    @Override
    public EngineType getEngineType() {
        return EngineType.SPARK;
    }

    @Override
    public DeployMode getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(DeployMode deployMode) {
        this.deployMode = deployMode;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    @Override
    public String getConfigFile() {
        return this.configFile;
    }

    @Override
    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

}
