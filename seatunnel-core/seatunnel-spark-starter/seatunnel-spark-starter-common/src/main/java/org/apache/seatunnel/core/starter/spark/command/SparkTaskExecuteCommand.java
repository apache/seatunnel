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

package org.apache.seatunnel.core.starter.spark.command;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.metalake.MetalakeConfigUtils;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.starter.spark.execution.SparkExecution;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.FileUtils;

import org.apache.spark.SparkFiles;

import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import static org.apache.seatunnel.core.starter.utils.FileUtils.checkConfigExist;

@Slf4j
public class SparkTaskExecuteCommand implements Command<SparkCommandArgs> {

    private final SparkCommandArgs sparkCommandArgs;

    public SparkTaskExecuteCommand(SparkCommandArgs sparkCommandArgs) {
        this.sparkCommandArgs = sparkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        Path configFile = resolveConfigPath(sparkCommandArgs, SparkFiles::get);
        checkConfigExist(configFile);
        Config config =
                MetalakeConfigUtils.getMetalakeConfig(
                        ConfigBuilder.of(configFile, sparkCommandArgs.getVariables()));
        if (!sparkCommandArgs.getJobName().equals(Constants.LOGO)) {
            config =
                    config.withValue(
                            ConfigUtil.joinPath("env", "job.name"),
                            ConfigValueFactory.fromAnyRef(sparkCommandArgs.getJobName()));
        }
        try {
            SparkExecution seaTunnelTaskExecution = new SparkExecution(config);
            seaTunnelTaskExecution.execute();
        } catch (Exception e) {
            throw new CommandExecuteException("Run SeaTunnel on spark failed", e);
        }
    }

    static Path resolveConfigPath(
            SparkCommandArgs sparkCommandArgs, Function<String, String> sparkFilesResolver) {
        Path configFile = FileUtils.getConfigPath(sparkCommandArgs);
        if (!DeployMode.CLUSTER.equals(sparkCommandArgs.getDeployMode())) {
            log.info("Resolved SeaTunnel config file: {}", configFile.toAbsolutePath());
            return configFile;
        }

        Path fileName = configFile.getFileName();
        if (fileName == null) {
            log.info("Resolved SeaTunnel config file: {}", configFile.toAbsolutePath());
            return configFile;
        }

        String sparkFilePath;
        try {
            sparkFilePath = sparkFilesResolver.apply(fileName.toString());
        } catch (RuntimeException e) {
            log.debug("Failed to resolve SeaTunnel config file from SparkFiles", e);
            log.info("Resolved SeaTunnel config file: {}", configFile.toAbsolutePath());
            return configFile;
        }
        if (sparkFilePath == null) {
            log.info("Resolved SeaTunnel config file: {}", configFile.toAbsolutePath());
            return configFile;
        }

        Path shippedConfigFile = Paths.get(sparkFilePath);
        if (Files.exists(shippedConfigFile)) {
            log.info("Resolved SeaTunnel config file: {}", shippedConfigFile.toAbsolutePath());
            return shippedConfigFile;
        }
        if (Files.exists(configFile)) {
            log.info("Resolved SeaTunnel config file: {}", configFile.toAbsolutePath());
            return configFile;
        }
        log.warn(
                "SeaTunnel config file was not found in working directory {} or SparkFiles path {}",
                configFile.toAbsolutePath(),
                shippedConfigFile.toAbsolutePath());
        return configFile;
    }
}
