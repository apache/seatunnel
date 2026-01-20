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

package org.apache.seatunnel.core.starter.flink;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.multitable.MultiTableSinkTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.List;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;

public class FlinkCommandArgsTest {
    @Test
    public void testExecuteClientCommandArgsWithPluginName()
            throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        FlinkCommandArgs flinkCommandArgs = buildFlinkCommandArgs(configFile);
        Assertions.assertDoesNotThrow(() -> SeaTunnel.run(flinkCommandArgs.buildCommand()));
    }

    @Test
    public void testExecuteClientCommandArgsWithoutPluginName()
            throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory_without_pluginname.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        FlinkCommandArgs flinkCommandArgs = buildFlinkCommandArgs(configFile);
        ConfigException configException =
                Assertions.assertThrows(
                        ConfigException.class,
                        () -> SeaTunnel.run(flinkCommandArgs.buildCommand()));
        Assertions.assertEquals(
                String.format("No configuration setting found for key '%s'", PLUGIN_NAME.key()),
                configException.getMessage());
    }

    private static FlinkCommandArgs buildFlinkCommandArgs(String configFile) {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);
        return flinkCommandArgs;
    }

    @Test
    public void testBuildFlinkCommandArgsWithSavePoint() {
        FlinkStarter flinkStarter =
                new FlinkStarter(
                        new String[] {
                            "--target", "local",
                            "--deploy-mode ", "run",
                            "--config ", "/config/fake_to_inmemory1.json",
                            "--fromCheckpoint ",
                                    "hdfs:///flink/checkpoints/3c298a925d9a2a7837bbf5a8e4966b4f/chk-7902"
                        });
        List<String> commands = flinkStarter.buildCommands();
        Assertions.assertTrue(
                commands.contains("-s") || commands.contains("--fromSavepoint"),
                "The flink commands should include either `-s` or `--fromSavepoint`");
    }

    @Test
    public void testBuildFlinkCommandArgsWithoutSavePoint() {
        FlinkStarter flinkStarter =
                new FlinkStarter(
                        new String[] {
                            "--target", "local",
                            "--deploy-mode ", "run",
                            "--config ", "/config/fake_to_inmemory1.json",
                        });
        List<String> commands = flinkStarter.buildCommands();
        Assertions.assertTrue(
                !commands.contains("-s") && !commands.contains("--fromSavepoint"),
                "The flink commands should not include either `-s` or `--fromSavepoint`");
    }
}
