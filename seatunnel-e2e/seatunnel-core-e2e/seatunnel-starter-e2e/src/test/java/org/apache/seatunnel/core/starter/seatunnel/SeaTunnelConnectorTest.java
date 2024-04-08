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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Only support for seatunnel")
@Slf4j
public class SeaTunnelConnectorTest extends TestSuiteBase implements TestResource {

    /**
     * Connectors that do not implement the Factory interface should be excluded because they cannot
     * be discovered by seatunnel-plugin-discovery todo: If these connectors implement the Factory
     * interface in the future, it should be removed from here
     */
    private static final Set<String> EXCLUDE_CONNECTOR =
            new HashSet() {
                {
                    add("TDengine");
                    add("SelectDBCloud");
                }
            };

    @Override
    public void startUp() throws Exception {}

    @Override
    public void tearDown() throws Exception {}

    @TestTemplate
    public void testExecCheck(TestContainer container) throws Exception {
        String[] case1 = {"-l -pt source"};
        execCheck(container, case1, PluginType.SOURCE);

        String[] case2 = {"-l -pt sink"};
        execCheck(container, case2, PluginType.SINK);

        String[] case3 = {"-o Paimon -pt sink"};
        Container.ExecResult execResult = container.executeConnectorCheck(case3);
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertTrue(StringUtils.isBlank(execResult.getStderr()));
        log.info(execResult.getStdout());
    }

    private static void execCheck(TestContainer container, String[] args, PluginType pluginType)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeConnectorCheck(args);
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertTrue(StringUtils.isBlank(execResult.getStderr()));
        log.info(execResult.getStdout());
        String pluginTypeStr = pluginType.getType();
        Set<String> connectorIdentifier =
                ContainerUtil.getConnectorIdentifier("seatunnel", pluginTypeStr).stream()
                        .filter(connectorIdenf -> !EXCLUDE_CONNECTOR.contains(connectorIdenf))
                        .collect(Collectors.toSet());
        Set<String> connectors =
                new TreeSet<>(
                        Arrays.asList(
                                execResult
                                        .getStdout()
                                        .trim()
                                        .replaceFirst(
                                                StringUtils.capitalize(pluginTypeStr),
                                                StringUtils.EMPTY)
                                        .trim()
                                        .toLowerCase()
                                        .split(StringUtils.SPACE)));
        Assertions.assertEquals(connectorIdentifier.size(), connectors.size());
        Set<String> diff =
                connectorIdentifier.stream()
                        .filter(
                                connectorIdentifierStr ->
                                        !connectors.contains(connectorIdentifierStr.toLowerCase()))
                        .collect(Collectors.toSet());
        Assertions.assertTrue(diff.isEmpty());
    }
}
