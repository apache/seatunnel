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

package org.apache.seatunnel.core.starter.seatunnel.args;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.seatunnel.multitable.MultiTableSinkTest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;

public class ClientCommandArgsTest {
    @Test
    public void testExecuteClientCommandArgsWithPluginName()
            throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = buildClientCommandArgs(configFile);
        Assertions.assertDoesNotThrow(() -> SeaTunnel.run(clientCommandArgs.buildCommand()));
    }

    @Test
    public void testSetJobId() throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        long jobId = 999;
        ClientCommandArgs clientCommandArgs = buildClientCommandArgs(configFile, jobId);
        Assertions.assertDoesNotThrow(() -> SeaTunnel.run(clientCommandArgs.buildCommand()));
    }

    @Test
    public void testExecuteClientCommandArgsWithoutPluginName()
            throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory_without_pluginname.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = buildClientCommandArgs(configFile);
        CommandExecuteException commandExecuteException =
                Assertions.assertThrows(
                        CommandExecuteException.class,
                        () -> SeaTunnel.run(clientCommandArgs.buildCommand()));
        Assertions.assertEquals(
                String.format(
                        "The '%s' option is not configured, please configure it.",
                        PLUGIN_NAME.key()),
                commandExecuteException.getCause().getMessage());
    }

    private static ClientCommandArgs buildClientCommandArgs(String configFile, Long jobId) {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setVariables(new ArrayList<>());
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setMasterType(MasterType.LOCAL);
        clientCommandArgs.setCheckConfig(false);
        if (jobId != null) {
            clientCommandArgs.setCustomJobId(String.valueOf(jobId));
        }
        return clientCommandArgs;
    }

    private static ClientCommandArgs buildClientCommandArgs(String configFile) {
        return buildClientCommandArgs(configFile, null);
    }
}
