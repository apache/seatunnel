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

public class FlinkCommandArgs extends AbstractCommandArgs {

    @Parameter(names = {"-r", "--run-mode"},
        description = "job run mode, run or run-application")
    private String runMode;

    @Override
    public EngineType getEngineType() {
        return EngineType.FLINK;
    }

    @Override
    public DeployMode getDeployMode() {
        return DeployMode.CLIENT;
    }

    public String getRunMode() {
        return runMode;
    }

    public void setRunMode(String runMode) {
        this.runMode = runMode;
    }
}
