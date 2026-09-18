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

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

class FlinkConfValidateCommandTest {

    @Test
    void shouldValidateFlinkConfig() throws URISyntaxException {
        FlinkCommandArgs commandArgs = buildArgs("/config/fake_to_inmemory.json");
        Command<?> command = commandArgs.buildCommand();

        Assertions.assertInstanceOf(FlinkConfValidateCommand.class, command);
        Assertions.assertDoesNotThrow(command::execute);
    }

    @Test
    void shouldRejectInvalidRestartStrategy() throws URISyntaxException {
        FlinkCommandArgs commandArgs = buildArgs("/config/invalid_restart_strategy.json");
        Command<?> command = commandArgs.buildCommand();

        Assertions.assertInstanceOf(FlinkConfValidateCommand.class, command);
        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(
                                "fixed-delay restart strategy must set [execution.restart.attempts],[execution.restart.delayBetweenAttempts]"));
    }

    private static FlinkCommandArgs buildArgs(String resourcePath) throws URISyntaxException {
        FlinkCommandArgs commandArgs = new FlinkCommandArgs();
        commandArgs.setDeployMode(DeployMode.RUN);
        commandArgs.setCheckConfig(true);
        commandArgs.setConfigFile(getResourcePath(resourcePath).toString());
        commandArgs.setVariables(Collections.emptyList());
        return commandArgs;
    }

    private static Path getResourcePath(String resourcePath) throws URISyntaxException {
        URL resource = FlinkConfValidateCommandTest.class.getResource(resourcePath);
        Assertions.assertNotNull(resource);
        return Paths.get(resource.toURI());
    }
}
