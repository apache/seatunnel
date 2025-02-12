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

package org.apache.seatunnel.core.starter.spark;

import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.spark.args.SparkCommandArgs;
import org.apache.seatunnel.core.starter.spark.multitable.MultiTableSinkTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;

public class SparkCommandArgsTest {
    @Test
    public void testExecuteClientCommandArgsWithPluginName()
            throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        SparkCommandArgs sparkCommandArgs = buildSparkCommands(configFile);
        sparkCommandArgs.setDeployMode(DeployMode.CLIENT);
        Assertions.assertDoesNotThrow(() -> SeaTunnel.run(sparkCommandArgs.buildCommand()));
    }

    @Test
    public void testExecuteClientCommandArgsWithoutPluginName()
            throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory_without_pluginname.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        SparkCommandArgs sparkCommandArgs = buildSparkCommands(configFile);
        sparkCommandArgs.setDeployMode(DeployMode.CLIENT);
        CommandExecuteException commandExecuteException =
                Assertions.assertThrows(
                        CommandExecuteException.class,
                        () -> SeaTunnel.run(sparkCommandArgs.buildCommand()));
        Assertions.assertEquals(
                String.format("No configuration setting found for key '%s'", PLUGIN_NAME.key()),
                commandExecuteException.getCause().getMessage());
    }

    private static SparkCommandArgs buildSparkCommands(String configFile) {
        SparkCommandArgs sparkCommandArgs = new SparkCommandArgs();
        sparkCommandArgs.setConfigFile(configFile);
        sparkCommandArgs.setCheckConfig(false);
        sparkCommandArgs.setVariables(null);
        sparkCommandArgs.setDeployMode(DeployMode.CLIENT);
        return sparkCommandArgs;
    }
}
