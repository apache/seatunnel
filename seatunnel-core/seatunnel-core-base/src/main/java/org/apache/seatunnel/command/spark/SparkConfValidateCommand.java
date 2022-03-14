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

package org.apache.seatunnel.command.spark;

import org.apache.seatunnel.command.Command;
import org.apache.seatunnel.command.SparkCommandArgs;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.config.ConfigBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

/**
 * Used to validate the spark task conf is validated.
 */
public class SparkConfValidateCommand implements Command<SparkCommandArgs> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkConfValidateCommand.class);

    @Override
    public void execute(SparkCommandArgs sparkCommandArgs) {
        String confPath;
        if (DeployMode.CLUSTER.equals(sparkCommandArgs.getDeployMode())) {
            confPath = Paths.get(sparkCommandArgs.getConfigFile()).getFileName().toString();
        } else {
            confPath = sparkCommandArgs.getConfigFile();
        }
        new ConfigBuilder(confPath, sparkCommandArgs.getEngineType()).checkConfig();
        LOGGER.info("config OK !");
    }

}
