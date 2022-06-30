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

<<<<<<<< HEAD:seatunnel-core/seatunnel-flink-starter/src/main/java/org/apache/seatunnel/core/starter/flink/args/FlinkCommandArgs.java
package org.apache.seatunnel.core.starter.flink.args;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.command.AbstractCommandArgs;
import org.apache.seatunnel.core.starter.config.EngineType;
import org.apache.seatunnel.core.starter.flink.config.FlinkRunMode;
========
package org.apache.seatunnel.core.flink.args;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.base.command.AbstractCommandArgs;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.core.flink.config.FlinkRunMode;
>>>>>>>> dev:seatunnel-core/seatunnel-core-flink/src/main/java/org/apache/seatunnel/core/flink/args/FlinkCommandArgs.java

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.util.List;

public class FlinkCommandArgs extends AbstractCommandArgs {

    @Parameter(names = {"-r", "--run-mode"},
        converter = RunModeConverter.class,
        description = "job run mode, run or run-application")
    private FlinkRunMode runMode = FlinkRunMode.RUN;
    /**
     * Undefined parameters parsed will be stored here as flink command parameters.
     */
    private List<String> flinkParams;

    /**
     * Undefined parameters parsed will be stored here as flink command parameters.
     */
    private List<String> flinkParams;

    @Override
    public EngineType getEngineType() {
        return EngineType.FLINK;
    }

    @Override
    public DeployMode getDeployMode() {
        return DeployMode.CLIENT;
    }

    public FlinkRunMode getRunMode() {
        return runMode;
    }

    public void setRunMode(FlinkRunMode runMode) {
        this.runMode = runMode;
    }

    public List<String> getFlinkParams() {
        return flinkParams;
    }

    public void setFlinkParams(List<String> flinkParams) {
        this.flinkParams = flinkParams;
    }

    /**
     * Used to convert the run mode string to the enum value.
     */
    private static class RunModeConverter implements IStringConverter<FlinkRunMode> {
        /**
         * If the '-r' is not set, then will not go into this convert method.
         *
         * @param value input value set by '-r' or '--run-mode'
         * @return flink run mode enum value
         */
        @Override
        public FlinkRunMode convert(String value) {
            for (FlinkRunMode flinkRunMode : FlinkRunMode.values()) {
                if (flinkRunMode.getMode().equalsIgnoreCase(value)) {
                    return flinkRunMode;
                }
            }
            throw new IllegalArgumentException(String.format("Run mode %s not supported", value));
        }
    }

}
