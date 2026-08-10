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

import org.apache.seatunnel.api.metalake.MetalakeConfigUtils;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.utils.EnvironmentUtil;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.ConfigValidationUtils;
import org.apache.seatunnel.core.starter.utils.FileUtils;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

/** Use to validate the configuration of the SeaTunnel API. */
@Slf4j
public class FlinkConfValidateCommand implements Command<FlinkCommandArgs> {

    private final FlinkCommandArgs flinkCommandArgs;

    public FlinkConfValidateCommand(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void execute() throws ConfigCheckException {
        Path configPath = FileUtils.getConfigPath(flinkCommandArgs);
        checkConfigExist(configPath);
        Config config =
                MetalakeConfigUtils.getMetalakeConfig(
                        ConfigBuilder.of(configPath, flinkCommandArgs.getVariables()));
        if (!flinkCommandArgs.getJobName().equals(Constants.LOGO)) {
            config =
                    config.withValue(
                            ConfigUtil.joinPath("env", "job.name"),
                            ConfigValueFactory.fromAnyRef(flinkCommandArgs.getJobName()));
        }
        ConfigValidationUtils.validate(
                config, EnvironmentUtil.checkRestartStrategy(config.getConfig("env")));
        log.info("Config validation succeeded: {}", configPath);
    }
}
