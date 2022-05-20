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

package org.apache.seatunnel.core.spark.command;

import org.apache.seatunnel.core.base.command.Command;
import org.apache.seatunnel.core.base.config.ConfigBuilder;
import org.apache.seatunnel.core.base.exception.CommandExecuteException;
import org.apache.seatunnel.core.base.utils.FileUtils;
import org.apache.seatunnel.core.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.spark.execution.SeaTunnelTaskExecution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * todo: do we need to move these class to a new module? since this may cause version conflict with the old flink version.
 * This command is used to execute the Flink job by SeaTunnel new API.
 */
public class SeaTunnelApiTaskExecuteCommand implements Command<SparkCommandArgs> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SeaTunnelApiTaskExecuteCommand.class);

    private final SparkCommandArgs sparkCommandArgs;

    public SeaTunnelApiTaskExecuteCommand(SparkCommandArgs sparkCommandArgs) {
        this.sparkCommandArgs = sparkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        Path configFile = FileUtils.getConfigPath(sparkCommandArgs);
        Config config = new ConfigBuilder(configFile).getConfig();
        try {
            SeaTunnelTaskExecution seaTunnelTaskExecution = new SeaTunnelTaskExecution(config);
            seaTunnelTaskExecution.execute();
        } catch (Exception e) {
            LOGGER.error("Run SeaTunnel on spark failed.", e);
        }
    }

}
