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

package org.apache.seatunnel.core.starter.flink.command;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.execution.FlinkExecution;
import org.apache.seatunnel.core.starter.flink.utils.ResourceUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

@Slf4j
public class FlinkTaskExecuteCommand implements Command<FlinkCommandArgs> {

    private final FlinkCommandArgs flinkCommandArgs;

    public FlinkTaskExecuteCommand(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        Path configFile =
                ResourceUtils.getConfigFile(
                        flinkCommandArgs.getMasterType(), flinkCommandArgs.getConfigFile());
        checkConfigExist(configFile);
        Config config = ConfigBuilder.of(configFile, flinkCommandArgs.getVariables());
        // if user specified job name using command line arguments, override config option
        if (!flinkCommandArgs.getJobName().equals(Constants.LOGO)) {
            config =
                    config.withValue(
                            ConfigUtil.joinPath("env", "job.name"),
                            ConfigValueFactory.fromAnyRef(flinkCommandArgs.getJobName()));
        }
        FlinkExecution seaTunnelTaskExecution =
                new FlinkExecution(
                        flinkCommandArgs.getDiscoveryType(),
                        Arrays.stream(flinkCommandArgs.getConnectors().split(";"))
                                .map(ResourceUtils::of)
                                .collect(Collectors.toList()),
                        config);
        try {
            seaTunnelTaskExecution.execute();
        } catch (Exception e) {
            throw new CommandExecuteException("Flink job executed failed", e);
        }
    }
}
